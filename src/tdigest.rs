//! Percentile sketches (PLAN.md Phase 2.3, D9): the merging t-digest.
//!
//! Written from the paper, T. Dunning and O. Ertl, "Computing Extremely Accurate Quantiles
//! Using t-Digests" (2019): values are gathered into centroids (a mean and a weight), and how
//! much weight a centroid may hold is bounded by the scale function
//! k1(q) = δ / 2π · asin(2q − 1), which keeps centroids near the tails small, so extreme
//! quantiles stay accurate. Incoming values are buffered and merged into the centroids in
//! batches. Two digests merge by pooling their centroids and compressing again, which is what
//! makes the state a rollup can keep (D9).
//!
//! SQL:
//!
//! * `percentile_sketch(value double precision [, compression integer])` aggregate → `tdigest`
//! * `percentile(tdigest, q double precision)` → the estimated q-quantile, and a `q[]` form
//! * `merge(tdigest)` aggregate → one `tdigest` from many
//!
//! NULL and NaN values are skipped. The text form is JSON, and any `tdigest` read from text
//! or disk is validated before it is used (R5: a corrupt one is an ERROR, never a crash).

use pgrx::prelude::*;
use pgrx::{InOutFuncs, StringInfo};
use serde::{Deserialize, Serialize};

pub const DEFAULT_COMPRESSION: f64 = 100.0;

/// The pure digest: no Postgres in it, tested on its own.
#[derive(Clone, Debug, Default)]
pub struct Digest {
	pub compression: f64,
	pub means: Vec<f64>,
	pub weights: Vec<f64>,
	/// Merged weight, the sum of `weights`.
	pub total: f64,
	pub min: f64,
	pub max: f64,
	/// Values not merged yet, each of weight 1.
	buffer: Vec<f64>,
}

fn k(q: f64, compression: f64) -> f64 {
	compression / (2.0 * std::f64::consts::PI) * (2.0 * q - 1.0).asin()
}

fn k_inverse(k: f64, compression: f64) -> f64 {
	((k * 2.0 * std::f64::consts::PI / compression).sin() + 1.0) / 2.0
}

impl Digest {
	pub fn new(compression: f64) -> Digest {
		Digest { compression, min: f64::INFINITY, max: f64::NEG_INFINITY, ..Default::default() }
	}

	fn buffer_cap(&self) -> usize {
		(self.compression * 5.0) as usize
	}

	pub fn add(&mut self, x: f64) {
		if x.is_nan() {
			return;
		}
		self.min = self.min.min(x);
		self.max = self.max.max(x);
		self.buffer.push(x);
		if self.buffer.len() >= self.buffer_cap() {
			self.compress();
		}
	}

	pub fn count(&self) -> f64 {
		self.total + self.buffer.len() as f64
	}

	/// Merge the buffer into the centroids.
	pub fn compress(&mut self) {
		if self.buffer.is_empty() {
			return;
		}
		let mut items: Vec<(f64, f64)> = self.means.iter().copied().zip(self.weights.iter().copied()).collect();
		items.extend(self.buffer.drain(..).map(|x| (x, 1.0)));
		self.rebuild(items);
	}

	/// Pool another digest's centroids with this one's.
	pub fn merge(&mut self, other: &Digest) {
		if other.count() == 0.0 {
			return;
		}
		self.compress();
		let mut items: Vec<(f64, f64)> = self.means.iter().copied().zip(self.weights.iter().copied()).collect();
		items.extend(other.means.iter().copied().zip(other.weights.iter().copied()));
		items.extend(other.buffer.iter().map(|&x| (x, 1.0)));
		self.min = self.min.min(other.min);
		self.max = self.max.max(other.max);
		self.rebuild(items);
	}

	/// Sort `items` by mean and merge neighbours greedily while the merged centroid stays
	/// within one unit of the scale function.
	fn rebuild(&mut self, mut items: Vec<(f64, f64)>) {
		items.sort_by(|a, b| a.0.total_cmp(&b.0));
		let total: f64 = items.iter().map(|i| i.1).sum();
		let (mut means, mut weights) = (Vec::new(), Vec::new());
		let mut so_far = 0.0;
		let mut iter = items.into_iter();
		let Some((mut mean, mut weight)) = iter.next() else {
			return;
		};
		let mut limit = total * k_inverse(k(0.0, self.compression) + 1.0, self.compression);
		for (m, w) in iter {
			if so_far + weight + w <= limit {
				weight += w;
				mean += (m - mean) * w / weight;
			} else {
				so_far += weight;
				means.push(mean);
				weights.push(weight);
				limit = total * k_inverse(k(so_far / total, self.compression) + 1.0, self.compression);
				(mean, weight) = (m, w);
			}
		}
		means.push(mean);
		weights.push(weight);
		self.means = means;
		self.weights = weights;
		self.total = total;
	}

	/// The estimated q-quantile, q in [0, 1]; None when empty. Centroids are taken as centred
	/// on their cumulative weight and the estimate interpolates between neighbouring means, with
	/// the min and max as the ends and a centroid of weight 1 treated as the exact value it is.
	pub fn quantile(&mut self, q: f64) -> Option<f64> {
		self.compress();
		let n = self.means.len();
		if n == 0 {
			return None;
		}
		if n == 1 || self.min == self.max {
			return Some(if n == 1 && self.weights[0] == 1.0 { self.means[0] } else { self.min + (self.max - self.min) * q });
		}
		let total = self.total;
		let index = q * total;
		if index < 1.0 {
			return Some(self.min);
		}
		if index > total - 1.0 {
			return Some(self.max);
		}
		let (m, w) = (&self.means, &self.weights);
		// Left of the first centroid's centre: between min and that mean.
		if w[0] > 1.0 && index < w[0] / 2.0 {
			return Some(self.min + (index - 1.0) / (w[0] / 2.0 - 1.0) * (m[0] - self.min));
		}
		// Right of the last centroid's centre.
		if w[n - 1] > 1.0 && total - index <= w[n - 1] / 2.0 {
			return Some(self.max - (total - index - 1.0) / (w[n - 1] / 2.0 - 1.0) * (self.max - m[n - 1]));
		}
		let mut so_far = w[0] / 2.0;
		for i in 0..n - 1 {
			let dw = (w[i] + w[i + 1]) / 2.0;
			if so_far + dw > index {
				// A single value is exact: within half a unit of it, it is the answer.
				let left_unit = if w[i] == 1.0 {
					if index - so_far < 0.5 {
						return Some(m[i]);
					}
					0.5
				} else {
					0.0
				};
				let right_unit = if w[i + 1] == 1.0 {
					if so_far + dw - index <= 0.5 {
						return Some(m[i + 1]);
					}
					0.5
				} else {
					0.0
				};
				let z1 = index - so_far - left_unit;
				let z2 = so_far + dw - index - right_unit;
				return Some((m[i] * z2 + m[i + 1] * z1) / (z1 + z2));
			}
			so_far += dw;
		}
		Some(self.max)
	}

	/// Everything a stored or typed-in digest must satisfy, or why not.
	pub fn check(&self) -> Result<(), &'static str> {
		if !(10.0..=10_000.0).contains(&self.compression) {
			return Err("compression must be between 10 and 10000");
		}
		if self.means.len() != self.weights.len() {
			return Err("means and weights differ in length");
		}
		if self.means.iter().any(|m| !m.is_finite()) || self.weights.iter().any(|w| !(w.is_finite() && *w > 0.0)) {
			return Err("a centroid is not finite, or has a weight that is not positive");
		}
		if self.means.windows(2).any(|p| p[0] > p[1]) {
			return Err("centroids are not in order");
		}
		let sum: f64 = self.weights.iter().sum();
		if (sum - self.total).abs() > 1e-6 * sum.max(1.0) {
			return Err("the total is not the sum of the weights");
		}
		if !self.means.is_empty()
			&& !(self.min.is_finite() && self.max.is_finite() && self.min <= self.means[0] && self.max >= self.means[self.means.len() - 1])
		{
			return Err("min and max do not bound the centroids");
		}
		Ok(())
	}
}

/// The SQL type: a compressed digest.
#[derive(Clone, Debug, Serialize, Deserialize, PostgresType)]
#[inoutfuncs]
pub struct TDigest {
	compression: f64,
	count: f64,
	min: f64,
	max: f64,
	means: Vec<f64>,
	weights: Vec<f64>,
}

impl TDigest {
	fn from_digest(mut d: Digest) -> TDigest {
		d.compress();
		let empty = d.means.is_empty();
		TDigest {
			compression: d.compression,
			count: d.total,
			min: if empty { 0.0 } else { d.min },
			max: if empty { 0.0 } else { d.max },
			means: d.means,
			weights: d.weights,
		}
	}

	/// Back to a digest, validated: this came from text or from disk.
	fn to_digest(&self) -> Digest {
		let empty = self.means.is_empty();
		let d = Digest {
			compression: self.compression,
			means: self.means.clone(),
			weights: self.weights.clone(),
			total: self.count,
			min: if empty { f64::INFINITY } else { self.min },
			max: if empty { f64::NEG_INFINITY } else { self.max },
			buffer: Vec::new(),
		};
		if let Err(why) = d.check() {
			error!("invalid tdigest: {why}");
		}
		d
	}
}

impl InOutFuncs for TDigest {
	fn input(input: &core::ffi::CStr) -> Self {
		let text = input.to_str().unwrap_or_else(|_| error!("invalid tdigest: not UTF-8"));
		let t: TDigest = serde_json::from_str(text).unwrap_or_else(|e| error!("invalid tdigest: {e}"));
		t.to_digest();
		t
	}

	fn output(&self, buffer: &mut StringInfo) {
		buffer.push_str(&serde_json::to_string(self).expect("a tdigest serialises"));
	}
}

// ---- the aggregates: an internal state holding a Digest, freed with its memory context ----

unsafe fn agg_context(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::MemoryContext {
	let mut ctx: pg_sys::MemoryContext = std::ptr::null_mut();
	if pg_sys::AggCheckCallContext(fcinfo, &mut ctx) == 0 {
		error!("snouttime sketch support functions may only be called by an aggregate");
	}
	ctx
}

unsafe fn arg(fcinfo: pg_sys::FunctionCallInfo, n: usize) -> (pg_sys::Datum, bool) {
	let a = (*fcinfo).args.as_slice(n + 1)[n];
	(a.value, a.isnull)
}

unsafe fn state(fcinfo: pg_sys::FunctionCallInfo, compression: f64) -> *mut Digest {
	let ctx = agg_context(fcinfo);
	let (s, isnull) = arg(fcinfo, 0);
	if isnull {
		pgrx::PgMemoryContexts::For(ctx).leak_and_drop_on_delete(Digest::new(compression))
	} else {
		s.cast_mut_ptr::<Digest>()
	}
}

fn compression_of(c: i32) -> f64 {
	if !(10..=10_000).contains(&c) {
		error!("a percentile sketch's compression must be between 10 and 10000, not {c}");
	}
	c as f64
}

/// (internal, value float8 [, compression int]) -> internal
unsafe fn sketch_trans(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum {
	let compression = if (*fcinfo).nargs > 2 {
		let (c, cnull) = arg(fcinfo, 2);
		if cnull {
			error!("a percentile sketch's compression must not be NULL");
		}
		compression_of(c.value() as i32)
	} else {
		DEFAULT_COMPRESSION
	};
	let d = state(fcinfo, compression);
	if (*d).compression != compression {
		error!("a percentile sketch's compression must be the same for every row of a group");
	}
	let (v, vnull) = arg(fcinfo, 1);
	if !vnull {
		(*d).add(f64::from_bits(v.value() as u64));
	}
	pg_sys::Datum::from(d)
}

/// (internal, tdigest) -> internal
unsafe fn merge_trans(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum {
	let (v, vnull) = arg(fcinfo, 1);
	let incoming = if vnull { None } else { TDigest::from_polymorphic_datum(v, false, pg_sys::InvalidOid) };
	let compression = incoming.as_ref().map(|t| t.compression).unwrap_or(DEFAULT_COMPRESSION);
	let d = state(fcinfo, compression);
	if let Some(t) = incoming {
		let other = t.to_digest();
		if (*d).count() == 0.0 {
			(*d).compression = other.compression;
		}
		(*d).merge(&other);
	}
	pg_sys::Datum::from(d)
}

/// (internal, internal) -> internal
unsafe fn combine(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum {
	let ctx = agg_context(fcinfo);
	let (a, anull) = arg(fcinfo, 0);
	let (b, bnull) = arg(fcinfo, 1);
	if bnull {
		if anull {
			(*fcinfo).isnull = true;
		}
		return a;
	}
	let b = &*b.cast_mut_ptr::<Digest>();
	let a = if anull {
		pgrx::PgMemoryContexts::For(ctx).leak_and_drop_on_delete(Digest::new(b.compression))
	} else {
		a.cast_mut_ptr::<Digest>()
	};
	if (*a).count() == 0.0 {
		(*a).compression = b.compression;
	}
	(*a).merge(b);
	pg_sys::Datum::from(a)
}

/// (internal) -> bytea, the compressed digest as the type stores it.
unsafe fn serialize(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum {
	agg_context(fcinfo);
	let d = &*arg(fcinfo, 0).0.cast_mut_ptr::<Digest>();
	let t = TDigest::from_digest(d.clone());
	let bytes = serde_cbor::to_vec(&t).expect("a tdigest serialises");
	pg_sys::Datum::from(pgrx::rust_byte_slice_to_bytea(&bytes).into_pg())
}

/// (bytea, internal) -> internal
unsafe fn deserialize(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum {
	let ctx = agg_context(fcinfo);
	let v = pg_sys::pg_detoast_datum_packed(arg(fcinfo, 0).0.cast_mut_ptr());
	let t: TDigest = serde_cbor::from_slice(pgrx::varlena::varlena_to_byte_slice(v))
		.unwrap_or_else(|e| error!("invalid serialised tdigest: {e}"));
	let d = t.to_digest();
	pg_sys::Datum::from(pgrx::PgMemoryContexts::For(ctx).leak_and_drop_on_delete(d))
}

/// (internal, ...) -> tdigest; NULL when no value was seen.
unsafe fn finish(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum {
	let (s, snull) = arg(fcinfo, 0);
	if snull || (*s.cast_mut_ptr::<Digest>()).count() == 0.0 {
		(*fcinfo).isnull = true;
		return pg_sys::Datum::from(0);
	}
	let t = TDigest::from_digest((*s.cast_mut_ptr::<Digest>()).clone());
	t.into_datum().unwrap()
}

macro_rules! c_function {
	($name:ident, $finfo:ident, $body:expr) => {
		#[no_mangle]
		pub extern "C" fn $finfo() -> &'static pg_sys::Pg_finfo_record {
			static V1: pg_sys::Pg_finfo_record = pg_sys::Pg_finfo_record { api_version: 1 };
			&V1
		}
		#[pg_guard]
		#[no_mangle]
		pub unsafe extern "C-unwind" fn $name(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum {
			($body)(fcinfo)
		}
	};
}

c_function!(snouttime_tdigest_trans, pg_finfo_snouttime_tdigest_trans, sketch_trans);
c_function!(snouttime_tdigest_merge_trans, pg_finfo_snouttime_tdigest_merge_trans, merge_trans);
c_function!(snouttime_tdigest_combine, pg_finfo_snouttime_tdigest_combine, combine);
c_function!(snouttime_tdigest_serialize, pg_finfo_snouttime_tdigest_serialize, serialize);
c_function!(snouttime_tdigest_deserialize, pg_finfo_snouttime_tdigest_deserialize, deserialize);
c_function!(snouttime_tdigest_final, pg_finfo_snouttime_tdigest_final, finish);

fn check_q(q: f64) {
	if !(0.0..=1.0).contains(&q) {
		error!("a percentile must be between 0 and 1, not {q}");
	}
}

/// The estimated q-quantile of the values the sketch saw.
#[pg_extern(immutable, parallel_safe)]
fn percentile(sketch: TDigest, q: f64) -> Option<f64> {
	check_q(q);
	sketch.to_digest().quantile(q)
}

#[pg_extern(immutable, parallel_safe, name = "percentile")]
fn percentiles(sketch: TDigest, q: Vec<f64>) -> Option<Vec<f64>> {
	let mut d = sketch.to_digest();
	q.into_iter()
		.map(|q| {
			check_q(q);
			d.quantile(q)
		})
		.collect()
}

/// How many values the sketch saw.
#[pg_extern(immutable, parallel_safe)]
fn sketch_count(sketch: TDigest) -> f64 {
	sketch.to_digest().total
}

pgrx::extension_sql!(
	r#"
CREATE FUNCTION snouttime._tdigest_trans(internal, double precision) RETURNS internal
	LANGUAGE c IMMUTABLE PARALLEL SAFE AS 'MODULE_PATHNAME', 'snouttime_tdigest_trans';
CREATE FUNCTION snouttime._tdigest_trans(internal, double precision, integer) RETURNS internal
	LANGUAGE c IMMUTABLE PARALLEL SAFE AS 'MODULE_PATHNAME', 'snouttime_tdigest_trans';
CREATE FUNCTION snouttime._tdigest_merge_trans(internal, snouttime.tdigest) RETURNS internal
	LANGUAGE c IMMUTABLE PARALLEL SAFE AS 'MODULE_PATHNAME', 'snouttime_tdigest_merge_trans';
CREATE FUNCTION snouttime._tdigest_combine(internal, internal) RETURNS internal
	LANGUAGE c IMMUTABLE PARALLEL SAFE AS 'MODULE_PATHNAME', 'snouttime_tdigest_combine';
CREATE FUNCTION snouttime._tdigest_serialize(internal) RETURNS bytea
	LANGUAGE c STRICT IMMUTABLE PARALLEL SAFE AS 'MODULE_PATHNAME', 'snouttime_tdigest_serialize';
CREATE FUNCTION snouttime._tdigest_deserialize(bytea, internal) RETURNS internal
	LANGUAGE c STRICT IMMUTABLE PARALLEL SAFE AS 'MODULE_PATHNAME', 'snouttime_tdigest_deserialize';
CREATE FUNCTION snouttime._tdigest_final(internal) RETURNS snouttime.tdigest
	LANGUAGE c IMMUTABLE PARALLEL SAFE AS 'MODULE_PATHNAME', 'snouttime_tdigest_final';
CREATE AGGREGATE snouttime.percentile_sketch(value double precision) (
	SFUNC = snouttime._tdigest_trans, STYPE = internal, FINALFUNC = snouttime._tdigest_final,
	COMBINEFUNC = snouttime._tdigest_combine,
	SERIALFUNC = snouttime._tdigest_serialize, DESERIALFUNC = snouttime._tdigest_deserialize,
	PARALLEL = SAFE);
CREATE AGGREGATE snouttime.percentile_sketch(value double precision, compression integer) (
	SFUNC = snouttime._tdigest_trans, STYPE = internal, FINALFUNC = snouttime._tdigest_final,
	COMBINEFUNC = snouttime._tdigest_combine,
	SERIALFUNC = snouttime._tdigest_serialize, DESERIALFUNC = snouttime._tdigest_deserialize,
	PARALLEL = SAFE);
CREATE AGGREGATE snouttime.merge(sketch snouttime.tdigest) (
	SFUNC = snouttime._tdigest_merge_trans, STYPE = internal, FINALFUNC = snouttime._tdigest_final,
	COMBINEFUNC = snouttime._tdigest_combine,
	SERIALFUNC = snouttime._tdigest_serialize, DESERIALFUNC = snouttime._tdigest_deserialize,
	PARALLEL = SAFE);
"#,
	name = "tdigest_aggregates",
	requires = [TDigest]
);

#[cfg(test)]
mod tests {
	use super::Digest;

	/// A deterministic spread of values: the rank error of every estimate is checked.
	fn values(n: usize) -> Vec<f64> {
		let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
		(0..n)
			.map(|_| {
				x ^= x << 13;
				x ^= x >> 7;
				x ^= x << 17;
				(x >> 11) as f64 / (1u64 << 53) as f64
			})
			.map(|u| -u.max(1e-300).ln()) // exponential: a long right tail
			.collect()
	}

	fn rank_error(sorted: &[f64], estimate: f64, q: f64) -> f64 {
		let rank = sorted.partition_point(|&v| v <= estimate) as f64 / sorted.len() as f64;
		(rank - q).abs()
	}

	#[test]
	fn quantiles_are_within_rank_error() {
		let v = values(200_000);
		let mut d = Digest::new(100.0);
		v.iter().for_each(|&x| d.add(x));
		let mut sorted = v.clone();
		sorted.sort_by(|a, b| a.total_cmp(b));
		for q in [0.001, 0.01, 0.1, 0.25, 0.5, 0.75, 0.9, 0.99, 0.999] {
			let e = rank_error(&sorted, d.quantile(q).unwrap(), q);
			// the tails are held far tighter than the middle, as k1 promises
			let bound = if q < 0.02 || q > 0.98 { 0.001 } else { 0.01 };
			assert!(e < bound, "q={q}: rank error {e}");
		}
		assert!(d.check().is_ok());
		assert_eq!(d.quantile(0.0).unwrap(), sorted[0]);
		assert_eq!(d.quantile(1.0).unwrap(), sorted[sorted.len() - 1]);
	}

	#[test]
	fn merging_halves_matches_one_digest() {
		let v = values(100_000);
		let (mut a, mut b, mut whole) = (Digest::new(100.0), Digest::new(100.0), Digest::new(100.0));
		for (i, &x) in v.iter().enumerate() {
			if i % 2 == 0 { a.add(x) } else { b.add(x) }
			whole.add(x);
		}
		a.merge(&b);
		let mut sorted = v.clone();
		sorted.sort_by(|a, b| a.total_cmp(b));
		for q in [0.01, 0.5, 0.99] {
			assert!(rank_error(&sorted, a.quantile(q).unwrap(), q) < 0.01);
		}
		assert_eq!(a.total, 100_000.0);
		assert!(a.means.len() <= 2 * 100, "{} centroids", a.means.len());
	}

	#[test]
	fn a_few_values_are_exact() {
		let mut d = Digest::new(100.0);
		for x in [3.0, 1.0, 2.0] {
			d.add(x);
		}
		assert_eq!(d.quantile(0.0), Some(1.0));
		assert_eq!(d.quantile(0.5), Some(2.0));
		assert_eq!(d.quantile(1.0), Some(3.0));
		let mut one = Digest::new(100.0);
		one.add(7.0);
		assert_eq!(one.quantile(0.3), Some(7.0));
		assert_eq!(Digest::new(100.0).quantile(0.5), None);
	}

	#[test]
	fn a_corrupt_digest_is_refused() {
		let mut d = Digest::new(100.0);
		(0..1000).for_each(|i| d.add(i as f64));
		d.compress();
		let mut bad = d.clone();
		bad.weights[0] = -1.0;
		assert!(bad.check().is_err());
		let mut bad = d.clone();
		bad.means.swap(0, 1);
		assert!(bad.check().is_err());
		let mut bad = d.clone();
		bad.total += 5.0;
		assert!(bad.check().is_err());
		let mut bad = d;
		bad.weights.pop();
		assert!(bad.check().is_err());
	}
}
