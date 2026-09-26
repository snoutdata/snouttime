//! A small S3 client for tiering (PLAN.md Phase 5, D11): PUT an object from a file, GET a byte
//! range of one, DELETE one. Any S3-compatible endpoint (AWS, MinIO, R2), path-style URLs.
//!
//! Requests are signed with AWS Signature Version 4, written from AWS's published description
//! of it and checked against AWS's own worked example (the test below). Bodies are sent as
//! `UNSIGNED-PAYLOAD`, which S3 accepts over TLS and which spares hashing a whole partition
//! twice. The HTTP client is blocking (`ureq`): a Postgres backend is single-threaded and has
//! no business hosting an async runtime.
//!
//! No Postgres in here: the access method hands it a `Config` and bytes.

use std::fmt::Write as _;
use std::io::Read;

use hmac::{Hmac, KeyInit, Mac};
use sha2::{Digest, Sha256};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Config {
	/// `https://s3.us-west-2.amazonaws.com`, `http://minio:9000`, ...
	pub endpoint: String,
	pub region: String,
	pub access_key: String,
	pub secret_key: String,
	pub session_token: Option<String>,
}

/// Where an object is: `s3://bucket/key`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Location {
	pub bucket: String,
	pub key: String,
}

impl Location {
	pub fn parse(url: &str) -> Result<Location, String> {
		let rest = url.strip_prefix("s3://").ok_or_else(|| format!("\"{url}\" is not an s3:// URL"))?;
		let (bucket, key) = rest.split_once('/').ok_or_else(|| format!("\"{url}\" names a bucket but no key"))?;
		if bucket.is_empty() || key.is_empty() {
			return Err(format!("\"{url}\" names a bucket but no key"));
		}
		Ok(Location { bucket: bucket.to_string(), key: key.to_string() })
	}

	pub fn url(&self) -> String {
		format!("s3://{}/{}", self.bucket, self.key)
	}
}

fn sha256_hex(data: &[u8]) -> String {
	hex(&Sha256::digest(data))
}

fn hex(b: &[u8]) -> String {
	let mut s = String::with_capacity(b.len() * 2);
	for x in b {
		let _ = write!(s, "{x:02x}");
	}
	s
}

fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
	let mut m = <Hmac<Sha256> as KeyInit>::new_from_slice(key).expect("HMAC takes a key of any length");
	m.update(data);
	m.finalize().into_bytes().to_vec()
}

/// URI-encodes everything but the unreserved characters, and `/` when `slash` is kept.
fn uri_encode(s: &str, keep_slash: bool) -> String {
	let mut out = String::with_capacity(s.len());
	for b in s.bytes() {
		match b {
			b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => out.push(b as char),
			b'/' if keep_slash => out.push('/'),
			_ => {
				let _ = write!(out, "%{b:02X}");
			}
		}
	}
	out
}

/// The Authorization header for a request, per SigV4. `headers` are the ones to sign, names in
/// lower case; `amz_date` is `YYYYMMDDTHHMMSSZ`.
pub fn authorization(
	cfg: &Config,
	method: &str,
	path: &str,
	query: &str,
	headers: &[(String, String)],
	payload_hash: &str,
	amz_date: &str,
) -> String {
	let mut hs: Vec<(String, String)> = headers.iter().map(|(k, v)| (k.to_lowercase(), v.trim().to_string())).collect();
	hs.sort();
	let canonical_headers: String = hs.iter().map(|(k, v)| format!("{k}:{v}\n")).collect();
	let signed: Vec<&str> = hs.iter().map(|(k, _)| k.as_str()).collect();
	let signed = signed.join(";");
	let canonical = format!("{method}\n{}\n{query}\n{canonical_headers}\n{signed}\n{payload_hash}", uri_encode(path, true));
	let date = &amz_date[..8];
	let scope = format!("{date}/{}/s3/aws4_request", cfg.region);
	let to_sign = format!("AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}", sha256_hex(canonical.as_bytes()));
	let k = hmac(format!("AWS4{}", cfg.secret_key).as_bytes(), date.as_bytes());
	let k = hmac(&k, cfg.region.as_bytes());
	let k = hmac(&k, b"s3");
	let k = hmac(&k, b"aws4_request");
	let signature = hex(&hmac(&k, to_sign.as_bytes()));
	format!("AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed}, Signature={signature}", cfg.access_key)
}

fn amz_now() -> String {
	// seconds since the epoch to a UTC civil time, without a date crate (days-from-civil's inverse)
	let secs = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0) as i64;
	let (days, rem) = (secs.div_euclid(86400), secs.rem_euclid(86400));
	let z = days + 719_468;
	let era = z.div_euclid(146_097);
	let doe = z - era * 146_097;
	let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
	let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
	let mp = (5 * doy + 2) / 153;
	let d = doy - (153 * mp + 2) / 5 + 1;
	let m = if mp < 10 { mp + 3 } else { mp - 9 };
	let y = yoe + era * 400 + if m <= 2 { 1 } else { 0 };
	format!("{y:04}{m:02}{d:02}T{:02}{:02}{:02}Z", rem / 3600, rem % 3600 / 60, rem % 60)
}

struct Request {
	url: String,
	headers: Vec<(String, String)>,
}

fn request(cfg: &Config, method: &str, loc: &Location, extra: &[(String, String)], payload_hash: &str) -> Request {
	request_at(cfg, method, &format!("/{}/{}", loc.bucket, loc.key), &[], extra, payload_hash)
}

/// `query` pairs are signed in the canonical order and appended to the URL.
fn request_at(
	cfg: &Config,
	method: &str,
	path: &str,
	query: &[(&str, String)],
	extra: &[(String, String)],
	payload_hash: &str,
) -> Request {
	let endpoint = cfg.endpoint.trim_end_matches('/');
	let host = endpoint.split_once("://").map_or(endpoint, |x| x.1).to_string();
	let mut q: Vec<(String, String)> = query.iter().map(|(k, v)| (uri_encode(k, false), uri_encode(v, false))).collect();
	q.sort();
	let query_string = q.iter().map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>().join("&");
	let date = amz_now();
	let mut headers = vec![
		("host".to_string(), host),
		("x-amz-content-sha256".to_string(), payload_hash.to_string()),
		("x-amz-date".to_string(), date.clone()),
	];
	if let Some(t) = &cfg.session_token {
		headers.push(("x-amz-security-token".to_string(), t.clone()));
	}
	headers.extend(extra.iter().cloned());
	let auth = authorization(cfg, method, path, &query_string, &headers, payload_hash, &date);
	headers.push(("authorization".to_string(), auth));
	headers.retain(|(k, _)| k != "host");
	let q = if query_string.is_empty() { String::new() } else { format!("?{query_string}") };
	Request { url: format!("{endpoint}{}{q}", uri_encode(path, true)), headers }
}

/// The operating system's trusted CAs, read once per backend.
fn roots() -> ureq::tls::RootCerts {
	static ROOTS: std::sync::OnceLock<ureq::tls::RootCerts> = std::sync::OnceLock::new();
	ROOTS
		.get_or_init(|| {
			let found = rustls_native_certs::load_native_certs();
			let certs: Vec<ureq::tls::Certificate<'static>> = found
				.certs
				.into_iter()
				.map(|c| {
					let der: &'static [u8] = Box::leak(c.as_ref().to_vec().into_boxed_slice());
					ureq::tls::Certificate::from_der(der)
				})
				.collect();
			ureq::tls::RootCerts::new_with_certs(&certs)
		})
		.clone()
}

fn agent() -> ureq::Agent {
	ureq::Agent::config_builder()
		.tls_config(ureq::tls::TlsConfig::builder().root_certs(roots()).build())
		.http_status_as_error(false)
		.timeout_global(Some(std::time::Duration::from_secs(600)))
		.build()
		.into()
}

fn failure(what: &str, loc: &Location, status: u16, body: &str) -> String {
	let code = body.split("<Code>").nth(1).and_then(|s| s.split("</Code>").next()).unwrap_or("");
	format!("{what} {} failed: HTTP {status}{}", loc.url(), if code.is_empty() { String::new() } else { format!(" ({code})") })
}

/// Runs `f` up to three times, backing off, while it fails with a network error or a 5xx:
/// the failures an object store asks its clients to retry.
fn retry<T>(mut f: impl FnMut() -> Result<T, String>) -> Result<T, String> {
	let mut last = String::new();
	for attempt in 0..3 {
		match f() {
			Ok(v) => return Ok(v),
			Err(e) if e.contains("HTTP 5") || !e.contains("HTTP ") => {
				last = e;
				std::thread::sleep(std::time::Duration::from_millis(200 << (2 * attempt)));
			}
			Err(e) => return Err(e),
		}
	}
	Err(last)
}

/// Uploads a file as the object (retried).
pub fn put_file(cfg: &Config, loc: &Location, path: &std::path::Path) -> Result<(), String> {
	retry(|| put_file_once(cfg, loc, path))
}

fn put_file_once(cfg: &Config, loc: &Location, path: &std::path::Path) -> Result<(), String> {
	let len = std::fs::metadata(path).map_err(|e| format!("could not read {}: {e}", path.display()))?.len();
	let r = request(cfg, "PUT", loc, &[], "UNSIGNED-PAYLOAD");
	let file = std::fs::File::open(path).map_err(|e| format!("could not read {}: {e}", path.display()))?;
	let mut req = agent().put(&r.url).header("content-length", len.to_string());
	for (k, v) in &r.headers {
		req = req.header(k, v);
	}
	let mut resp = req.send(ureq::SendBody::from_reader(&mut std::io::BufReader::new(file))).map_err(|e| format!("PUT {}: {e}", loc.url()))?;
	let status = resp.status().as_u16();
	if status / 100 != 2 {
		let body = resp.body_mut().read_to_string().unwrap_or_default();
		return Err(failure("PUT", loc, status, &body));
	}
	Ok(())
}

/// `len` bytes of the object from `start` (retried).
pub fn get_range(cfg: &Config, loc: &Location, start: u64, len: u64) -> Result<Vec<u8>, String> {
	retry(|| get_range_once(cfg, loc, start, len))
}

fn get_range_once(cfg: &Config, loc: &Location, start: u64, len: u64) -> Result<Vec<u8>, String> {
	if len == 0 {
		return Ok(Vec::new());
	}
	let range = format!("bytes={}-{}", start, start + len - 1);
	let empty = sha256_hex(b"");
	let r = request(cfg, "GET", loc, &[("range".to_string(), range)], &empty);
	let mut req = agent().get(&r.url);
	for (k, v) in &r.headers {
		req = req.header(k, v);
	}
	let mut resp = req.call().map_err(|e| format!("GET {}: {e}", loc.url()))?;
	let status = resp.status().as_u16();
	let mut body = Vec::new();
	resp.body_mut().as_reader().take(len + 1).read_to_end(&mut body).map_err(|e| format!("GET {}: {e}", loc.url()))?;
	if status != 206 && status != 200 {
		return Err(failure("GET", loc, status, &String::from_utf8_lossy(&body)));
	}
	if body.len() as u64 != len {
		return Err(format!("GET {} returned {} bytes where {len} were asked for", loc.url(), body.len()));
	}
	Ok(body)
}

fn tag<'a>(xml: &'a str, name: &str) -> Option<&'a str> {
	let open = format!("<{name}>");
	let close = format!("</{name}>");
	let start = xml.find(&open)? + open.len();
	let end = xml[start..].find(&close)? + start;
	Some(&xml[start..end])
}

/// Every object under `prefix`, as (key, last modified), following continuation tokens.
pub fn list(cfg: &Config, bucket: &str, prefix: &str) -> Result<Vec<(String, String)>, String> {
	let mut out = Vec::new();
	let mut token: Option<String> = None;
	let empty = sha256_hex(b"");
	loop {
		let mut q = vec![("list-type", "2".to_string()), ("prefix", prefix.to_string())];
		if let Some(t) = &token {
			q.push(("continuation-token", t.clone()));
		}
		let r = request_at(cfg, "GET", &format!("/{bucket}"), &q, &[], &empty);
		let mut req = agent().get(&r.url);
		for (k, v) in &r.headers {
			req = req.header(k, v);
		}
		let mut resp = req.call().map_err(|e| format!("LIST s3://{bucket}/{prefix}: {e}"))?;
		let status = resp.status().as_u16();
		let body = resp.body_mut().read_to_string().map_err(|e| format!("LIST s3://{bucket}/{prefix}: {e}"))?;
		if status != 200 {
			let loc = Location { bucket: bucket.to_string(), key: prefix.to_string() };
			return Err(failure("LIST", &loc, status, &body));
		}
		for part in body.split("<Contents>").skip(1) {
			if let (Some(k), Some(m)) = (tag(part, "Key"), tag(part, "LastModified")) {
				out.push((xml_unescape(k), m.to_string()));
			}
		}
		token = if tag(&body, "IsTruncated") == Some("true") { tag(&body, "NextContinuationToken").map(xml_unescape) } else { None };
		if token.is_none() {
			return Ok(out);
		}
	}
}

fn xml_unescape(s: &str) -> String {
	s.replace("&lt;", "<").replace("&gt;", ">").replace("&quot;", "\"").replace("&apos;", "'").replace("&amp;", "&")
}

pub fn delete(cfg: &Config, loc: &Location) -> Result<(), String> {
	let empty = sha256_hex(b"");
	let r = request(cfg, "DELETE", loc, &[], &empty);
	let mut req = agent().delete(&r.url);
	for (k, v) in &r.headers {
		req = req.header(k, v);
	}
	let mut resp = req.call().map_err(|e| format!("DELETE {}: {e}", loc.url()))?;
	let status = resp.status().as_u16();
	if status / 100 != 2 && status != 404 {
		let body = resp.body_mut().read_to_string().unwrap_or_default();
		return Err(failure("DELETE", loc, status, &body));
	}
	Ok(())
}

#[cfg(test)]
mod tests {
	use super::*;

	/// AWS's worked example for SigV4 with S3 ("GET Object", Signature Version 4 docs): a range
	/// GET of /test.txt in examplebucket, signed on 2013-05-24 with the documentation keys.
	#[test]
	fn matches_aws_worked_example() {
		let cfg = Config {
			endpoint: "https://examplebucket.s3.amazonaws.com".into(),
			region: "us-east-1".into(),
			access_key: "AKIAIOSFODNN7EXAMPLE".into(),
			secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into(),
			session_token: None,
		};
		let empty = sha256_hex(b"");
		assert_eq!(empty, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
		let headers = vec![
			("host".to_string(), "examplebucket.s3.amazonaws.com".to_string()),
			("range".to_string(), "bytes=0-9".to_string()),
			("x-amz-content-sha256".to_string(), empty.clone()),
			("x-amz-date".to_string(), "20130524T000000Z".to_string()),
		];
		let auth = authorization(&cfg, "GET", "/test.txt", "", &headers, &empty, "20130524T000000Z");
		assert_eq!(
			auth,
			"AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, \
			 SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, \
			 Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
		);
	}

	#[test]
	fn keys_are_encoded_and_urls_parsed() {
		assert_eq!(uri_encode("/b/a key+x/é", true), "/b/a%20key%2Bx/%C3%A9");
		assert_eq!(Location::parse("s3://b/p/k.snt").unwrap(), Location { bucket: "b".into(), key: "p/k.snt".into() });
		assert!(Location::parse("s3://b").is_err());
		assert!(Location::parse("http://b/k").is_err());
		assert_eq!(amz_now().len(), 16);
	}
}
