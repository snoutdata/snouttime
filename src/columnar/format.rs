//! The on-disk format of a sealed partition's column store (docs/snouttime/COLUMNAR.md §2-§3),
//! as pure Rust: no Postgres in here, so it is unit-tested and fuzzed without a server. The
//! access method (`super`) turns tuples into [`Column`]s and back and moves the bytes to and
//! from pages; everything about what the bytes MEAN is here.
//!
//! A stream of row groups, then a directory, described by a metapage:
//!
//! ```text
//! row group = varint header length | header | column chunk 0 | column chunk 1 | ...
//! header    = varint rows | varint columns | per column:
//!               varint offset (from the end of the header) | varint length | u8 encoding
//!               | varint nulls | u8 flags (1: min and max follow) | [zigzag min, zigzag max]
//!               | u32 CRC-32C of the chunk
//! chunk     = a codec::compress frame of: varint null-bitmap length | null bitmap | values
//! directory = varint groups | per group: varint offset | varint length | varint first row
//!               | varint rows | varint columns | per column: u8 flags | [zigzag min, max]
//!               | when the rows are sorted: the group's first row's sort key, then its last
//!                 row's, each value varint 0 (NULL) or length + 1 | bytes
//!               | then, when sorted by two columns or more: varint 0 (not recorded) or
//!                 runs + 1 | per run of the leading columns that ends inside the group:
//!                 varint row (its last row, from the group's first) | that row's sort key
//! ```
//!
//! Version 2 (2026-09-23) added the sort order and the flags to the metapage and the first and
//! last keys to the directory: a sealed partition is its own index on its sort key.
//!
//! Version 3 (2026-09-25) added paged integer and float chunks ([`ENC_INT_PAGED`],
//! [`ENC_FLOAT_PAGED`]), which are not one frame:
//!
//! ```text
//! paged chunk = varint head length | head | page 0 | page 1 | ...
//! head        = a codec::compress frame of: varint null-bitmap length | null bitmap
//!                 | varint rows per page | varint pages | per page: varint length | u32 CRC-32C
//! page k      = a codec::compress frame of the non-null values of rows k * rows per page up to
//!                 the next page's, in the unpaged encoding (1 or 2)
//! ```
//!
//! The row group header's checksum covers the head (its length included), and each page's is in
//! the head, so one page is read, checked and decompressed without the others. A version 3
//! reader reads version 2; the version is what keeps an older build from reading a paged chunk
//! as damage.
//!
//! Every decoder here is as strict as the codecs (R5): a length past the end, an unknown
//! encoding, a value count that disagrees with the null bitmap, or a checksum that does not
//! match is a [`CodecError`], never a panic.

use crate::codec::{self, bitmap, compress, dict, float, int, CodecError, Result};

pub const MAGIC: u32 = 0x534e_5431; // "SNT1"
pub const VERSION: u32 = 3;
/// The oldest version this build reads.
pub const OLDEST_VERSION: u32 = 2;

/// Meta flag: the relation's non-unique indexes hold only its delta store's rows, so the
/// planner must never use one to find a column-store row (PLAN.md Q5).
pub const FLAG_LATE_INDEXES: u8 = 1;

/// A row group records where its runs of the leading sort columns end only up to this many:
/// past it (a leading key with more distinct values than that in 8,192 rows) nothing is
/// recorded and a reader decodes instead. Chosen, not measured.
pub const MAX_RUNS: usize = 64;

/// Row groups larger than this many bytes of encoded values are cut early, so no chunk
/// frame approaches `compress::MAX_RAW_BYTES`. Chosen.
pub const MAX_GROUP_BYTES: usize = 32 << 20;

pub const ENC_INT: u8 = 1;
pub const ENC_FLOAT: u8 = 2;
pub const ENC_BOOL: u8 = 3;
pub const ENC_DICT: u8 = 4;
pub const ENC_PLAIN: u8 = 5;
/// [`ENC_INT`] and [`ENC_FLOAT`] cut into pages of [`PAGE_ROWS`] rows, each encoded on its own,
/// so a reader that needs a few rows decodes the pages that hold them and not the whole chunk
/// (version 3, 2026-09-25: one host's hour is 360 of a row group's 8,192 rows).
pub const ENC_INT_PAGED: u8 = 6;
pub const ENC_FLOAT_PAGED: u8 = 7;

/// Rows per page of a paged chunk. Chosen, not measured as a size: about an hour of one series
/// at a reading every few seconds, the unit a narrow query reads, and a multiple of the integer
/// codec's 128-value groups. A chunk of no more rows than this is not paged.
pub const PAGE_ROWS: usize = 1024;

/// One column of one row group, as the writer hands it over: which rows are null, and the
/// values of the others, in order.
#[derive(Debug, Clone, PartialEq)]
pub struct Column {
	pub nulls: Vec<bool>,
	pub values: Values,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Values {
	/// Integers and every time type, as their int64 value.
	Int(Vec<i64>),
	/// float4 and float8 as bit patterns (float4's in the low 32 bits).
	Float(Vec<u64>),
	Bool(Vec<bool>),
	/// Any other type, as each value's bytes.
	Bytes(Vec<Vec<u8>>),
}

impl Values {
	fn len(&self) -> usize {
		match self {
			Values::Int(v) => v.len(),
			Values::Float(v) => v.len(),
			Values::Bool(v) => v.len(),
			Values::Bytes(v) => v.len(),
		}
	}
}

/// A decoded chunk. Dictionary columns stay a dictionary, so the reader can build each
/// distinct value once and point every row at it.
#[derive(Debug, Clone, PartialEq)]
pub enum Decoded {
	Int(Vec<i64>),
	Float(Vec<u64>),
	Bool(Vec<bool>),
	Dict { entries: Vec<Vec<u8>>, indexes: Vec<u32> },
	Plain(Vec<Vec<u8>>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct DecodedColumn {
	pub nulls: Vec<bool>,
	pub values: Decoded,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MinMax {
	pub min: i64,
	pub max: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkMeta {
	pub offset: u64,
	pub length: u64,
	pub encoding: u8,
	pub nulls: u64,
	pub minmax: Option<MinMax>,
	pub crc: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupHeader {
	pub rows: u64,
	pub columns: Vec<ChunkMeta>,
	/// Where the chunks start, from the start of the row group.
	pub data_start: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupEntry {
	pub offset: u64,
	pub length: u64,
	pub first_row: u64,
	pub rows: u64,
	pub minmax: Vec<Option<MinMax>>,
	/// The sort key of the group's first row and of its last, one value per column of
	/// `Meta::order` (None: NULL), as the bytes the column store keeps; empty when unsorted.
	pub first_key: Vec<Option<Vec<u8>>>,
	pub last_key: Vec<Option<Vec<u8>>>,
	/// Where the runs of every sort column but the last end inside the group (not the last
	/// run, which ends with the group): each run's last row, from the group's first, and that
	/// row's whole sort key. `DISTINCT ON (host) ... ORDER BY host, ts DESC` on a store sorted by
	/// (host, ts) is each run's last row, so it reads them from here and decodes nothing. None
	/// when not recorded: fewer than two sort columns, or more than [`MAX_RUNS`] runs.
	pub run_ends: Option<Vec<(u32, Vec<Option<Vec<u8>>>)>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Meta {
	pub rows: u64,
	pub groups: u64,
	pub codec: u8,
	pub dir_offset: u64,
	pub dir_length: u64,
	pub dir_crc: u32,
	/// The type OID of each attribute at seal time; 0 for a dropped one.
	pub types: Vec<u32>,
	/// A tiered column store (PLAN.md Phase 5): its row groups are in an object in S3, and
	/// only this metapage and the directory are in the relation's own pages.
	pub remote: Option<Remote>,
	/// The attributes (1-based) the rows are sorted by, each ascending with NULLs last; empty
	/// when they are in the order they arrived. Row groups follow on in this order, so each
	/// group's first and last keys bound every row in it.
	pub order: Vec<u16>,
	/// [`FLAG_LATE_INDEXES`].
	pub flags: u8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Remote {
	pub url: String,
	/// The object's length: every row group lies inside it.
	pub length: u64,
}

// ---- CRC-32C (Castagnoli), what Postgres uses for its own checksummed records ----

const fn crc_table() -> [u32; 256] {
	let mut t = [0u32; 256];
	let mut i = 0;
	while i < 256 {
		let mut c = i as u32;
		let mut k = 0;
		while k < 8 {
			c = if c & 1 != 0 { 0x82F6_3B78 ^ (c >> 1) } else { c >> 1 };
			k += 1;
		}
		t[i] = c;
		i += 1;
	}
	t
}

static CRC: [u32; 256] = crc_table();

/// CRC-32C (Castagnoli), as the chunk and directory checksums are written. Checking it a byte
/// at a time from the table cost three times what decoding a float chunk does (12.7 ns a value
/// against 4.1, 2026-09-23), so where the CPU has the CRC-32C instruction (ARMv8's CRC
/// extension, x86's SSE 4.2), eight bytes go through it at once; the table stays as the
/// fallback and as the definition the tests hold the instruction to.
pub fn crc32c(data: &[u8]) -> u32 {
	#[cfg(target_arch = "aarch64")]
	if std::arch::is_aarch64_feature_detected!("crc") {
		return unsafe { !crc32c_arm(!0, data) };
	}
	#[cfg(target_arch = "x86_64")]
	if std::arch::is_x86_feature_detected!("sse4.2") {
		return unsafe { !crc32c_x86(!0, data) };
	}
	!crc32c_table(!0, data)
}

fn crc32c_table(mut c: u32, data: &[u8]) -> u32 {
	for &b in data {
		c = CRC[((c ^ b as u32) & 0xff) as usize] ^ (c >> 8);
	}
	c
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "crc")]
unsafe fn crc32c_arm(mut c: u32, data: &[u8]) -> u32 {
	use std::arch::aarch64::{__crc32cb, __crc32cd};
	let mut words = data.chunks_exact(8);
	for w in &mut words {
		c = __crc32cd(c, u64::from_le_bytes(w.try_into().unwrap()));
	}
	for &b in words.remainder() {
		c = __crc32cb(c, b);
	}
	c
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse4.2")]
unsafe fn crc32c_x86(c: u32, data: &[u8]) -> u32 {
	use std::arch::x86_64::{_mm_crc32_u64, _mm_crc32_u8};
	let mut c64 = c as u64;
	let mut words = data.chunks_exact(8);
	for w in &mut words {
		c64 = _mm_crc32_u64(c64, u64::from_le_bytes(w.try_into().unwrap()));
	}
	let mut c = c64 as u32;
	for &b in words.remainder() {
		c = _mm_crc32_u8(c, b);
	}
	c
}

// ---- small readers and writers over untrusted bytes ----

fn put(out: &mut Vec<u8>, v: u64) {
	let mut v = v;
	while v >= 0x80 {
		out.push((v as u8) | 0x80);
		v >>= 7;
	}
	out.push(v as u8);
}

fn zz(v: i64) -> u64 {
	((v << 1) ^ (v >> 63)) as u64
}

fn unzz(u: u64) -> i64 {
	((u >> 1) as i64) ^ -((u & 1) as i64)
}

struct Cur<'a> {
	d: &'a [u8],
	p: usize,
}

impl<'a> Cur<'a> {
	fn byte(&mut self) -> Result<u8> {
		let b = *self.d.get(self.p).ok_or(CodecError::Truncated)?;
		self.p += 1;
		Ok(b)
	}
	fn varint(&mut self) -> Result<u64> {
		let mut v = 0u64;
		for i in 0..10 {
			let b = self.byte()?;
			let g = (b & 0x7f) as u64;
			if i == 9 && g > 1 {
				return Err(CodecError::Corrupt("a variable-length integer overflows 64 bits"));
			}
			v |= g << (7 * i);
			if b & 0x80 == 0 {
				return Ok(v);
			}
		}
		Err(CodecError::Corrupt("a variable-length integer is longer than ten bytes"))
	}
	fn u32(&mut self) -> Result<u32> {
		let s = self.bytes(4)?;
		Ok(u32::from_le_bytes([s[0], s[1], s[2], s[3]]))
	}
	fn u64(&mut self) -> Result<u64> {
		let s = self.bytes(8)?;
		Ok(u64::from_le_bytes(s.try_into().unwrap()))
	}
	fn bytes(&mut self, n: usize) -> Result<&'a [u8]> {
		if n > self.d.len() - self.p {
			return Err(CodecError::Truncated);
		}
		let s = &self.d[self.p..self.p + n];
		self.p += n;
		Ok(s)
	}
	/// A count that must fit in `limit`, so nothing is allocated for a corrupt one.
	fn bounded(&mut self, limit: u64, what: &'static str) -> Result<u64> {
		let v = self.varint()?;
		if v > limit {
			return Err(CodecError::Corrupt(what));
		}
		Ok(v)
	}
	fn minmax(&mut self) -> Result<Option<MinMax>> {
		match self.byte()? {
			0 => Ok(None),
			1 => {
				let min = unzz(self.varint()?);
				let max = unzz(self.varint()?);
				if min > max {
					return Err(CodecError::Corrupt("a column's minimum is above its maximum"));
				}
				Ok(Some(MinMax { min, max }))
			}
			_ => Err(CodecError::Corrupt("a column's flags name something the format does not have")),
		}
	}
}

fn put_minmax(out: &mut Vec<u8>, m: Option<MinMax>) {
	match m {
		None => out.push(0),
		Some(m) => {
			out.push(1);
			put(out, zz(m.min));
			put(out, zz(m.max));
		}
	}
}

// ---- columns ----

/// Encodes one column chunk: the null bitmap and the values, in the smallest encoding that
/// fits the type, inside one compression frame; integers and floats of more than
/// [`PAGE_ROWS`] rows are paged instead (see the module's layout).
pub fn encode_column(col: &Column, codec: compress::Codec) -> Result<(Vec<u8>, u8, Option<MinMax>)> {
	let nonnull = col.nulls.iter().filter(|&&n| !n).count();
	if nonnull != col.values.len() {
		return Err(CodecError::Corrupt("a column has a different number of values than non-null rows"));
	}
	let paged = col.nulls.len() > PAGE_ROWS;
	let (encoding, values, minmax) = match &col.values {
		Values::Int(v) => {
			let mm = v.iter().copied().fold(None, |a: Option<MinMax>, x| {
				Some(match a {
					None => MinMax { min: x, max: x },
					Some(m) => MinMax { min: m.min.min(x), max: m.max.max(x) },
				})
			});
			if paged {
				return Ok((encode_pages(&col.nulls, v, int::encode, codec)?, ENC_INT_PAGED, mm));
			}
			(ENC_INT, int::encode(v)?, mm)
		}
		Values::Float(v) if paged => return Ok((encode_pages(&col.nulls, v, float::encode_bits, codec)?, ENC_FLOAT_PAGED, None)),
		Values::Float(v) => (ENC_FLOAT, float::encode_bits(v)?, None),
		Values::Bool(v) => (ENC_BOOL, bitmap::encode(v)?, None),
		Values::Bytes(v) => {
			// the plain form's size is arithmetic; it is encoded only when it wins
			let d = dict::encode(v)?;
			if d.len() < dict::plain_len(v) {
				(ENC_DICT, d, None)
			} else {
				(ENC_PLAIN, dict::encode_plain(v)?, None)
			}
		}
	};
	let nulls = bitmap::encode(&col.nulls)?;
	let mut raw = Vec::with_capacity(nulls.len() + values.len() + 4);
	put(&mut raw, nulls.len() as u64);
	raw.extend_from_slice(&nulls);
	raw.extend_from_slice(&values);
	Ok((compress::compress(codec, &raw)?, encoding, minmax))
}

fn is_paged(encoding: u8) -> bool {
	encoding == ENC_INT_PAGED || encoding == ENC_FLOAT_PAGED
}

/// The paged form of `values`, the non-null values of the rows `nulls` describes: a head frame
/// (the null bitmap and a table of the pages' lengths and checksums), then each page's values
/// encoded by `enc` and compressed, each on its own.
fn encode_pages<T>(nulls: &[bool], values: &[T], enc: impl Fn(&[T]) -> Result<Vec<u8>>, codec: compress::Codec) -> Result<Vec<u8>> {
	let mut frames = Vec::with_capacity(nulls.len().div_ceil(PAGE_ROWS));
	let mut at = 0;
	for rows in nulls.chunks(PAGE_ROWS) {
		let n = rows.iter().filter(|&&x| !x).count();
		frames.push(compress::compress(codec, &enc(&values[at..at + n])?)?);
		at += n;
	}
	let bitmap = bitmap::encode(nulls)?;
	let mut head = Vec::with_capacity(bitmap.len() + 8 * frames.len() + 8);
	put(&mut head, bitmap.len() as u64);
	head.extend_from_slice(&bitmap);
	put(&mut head, PAGE_ROWS as u64);
	put(&mut head, frames.len() as u64);
	for f in &frames {
		put(&mut head, f.len() as u64);
		head.extend_from_slice(&crc32c(f).to_le_bytes());
	}
	let head = compress::compress(codec, &head)?;
	let mut out = Vec::with_capacity(head.len() + frames.iter().map(Vec::len).sum::<usize>() + 4);
	put(&mut out, head.len() as u64);
	out.extend_from_slice(&head);
	for f in &frames {
		out.extend_from_slice(f);
	}
	Ok(out)
}

/// How many of a chunk's first bytes the row group header's checksum covers: the whole chunk,
/// or a paged chunk's head (its pages carry their own).
pub fn checked_len(chunk: &[u8], encoding: u8) -> Result<usize> {
	if is_paged(encoding) {
		head_len(chunk, chunk.len())
	} else {
		Ok(chunk.len())
	}
}

/// A paged chunk's head's length, its own length's bytes included, from the chunk's first bytes;
/// at most `chunk_len`.
pub fn head_len(first: &[u8], chunk_len: usize) -> Result<usize> {
	let mut c = Cur { d: first, p: 0 };
	let n = c.bounded(chunk_len as u64, "a chunk's head is longer than the chunk")? as usize;
	let total = c.p + n;
	if total > chunk_len {
		return Err(CodecError::Corrupt("a chunk's head is longer than the chunk"));
	}
	Ok(total)
}

/// A chunk's frame decoded whole, below its checksums: the fuzzer's entry point, and the tests'.
#[allow(dead_code)]
pub fn decode_column(frame: &[u8], encoding: u8, rows: u64) -> Result<DecodedColumn> {
	let chunk = Chunk::open(frame, encoding, rows)?;
	let values = chunk.decode_all()?;
	Ok(DecodedColumn { nulls: chunk.nulls, values })
}

/// A column chunk opened: its null bitmap read, and its values decoded when asked, all of them,
/// or a paged chunk's pages one at a time, each read, checked and decompressed only then.
#[derive(Debug)]
pub struct Chunk {
	pub nulls: Vec<bool>,
	encoding: u8,
	/// an unpaged chunk's decompressed bytes, and where the values start in them
	raw: Vec<u8>,
	values_at: usize,
	pages: Option<Pages>,
}

/// A paged chunk's rows per page; each page's place in the chunk, checksum and frame (once it
/// has been read); and how many non-null values come before each page.
#[derive(Debug)]
struct Pages {
	rows: usize,
	spans: Vec<(usize, usize)>,
	crcs: Vec<u32>,
	frames: Vec<Option<Vec<u8>>>,
	before: Vec<usize>,
	values: usize,
}

impl Chunk {
	/// The whole chunk, checked against its header entry (a paged chunk's pages each against
	/// its own checksum as well), and opened.
	pub fn open_checked(frame: &[u8], m: &ChunkMeta, rows: u64) -> Result<Chunk> {
		if frame.len() as u64 != m.length {
			return Err(CodecError::Truncated);
		}
		let n = checked_len(frame, m.encoding)?;
		if crc32c(&frame[..n]) != m.crc {
			return Err(CodecError::Corrupt("a column chunk's checksum does not match"));
		}
		let mut c = Chunk::parse(frame, n, m.encoding, rows, frame.len())?;
		c.supply_all(frame, true)?;
		Ok(c)
	}

	/// The whole chunk, opened without looking at any checksum.
	pub fn open(frame: &[u8], encoding: u8, rows: u64) -> Result<Chunk> {
		let n = checked_len(frame, encoding)?;
		let mut c = Chunk::parse(frame, n, encoding, rows, frame.len())?;
		c.supply_all(frame, false)?;
		Ok(c)
	}

	/// A paged chunk from its head alone (`head_len` bytes, checked against the header entry):
	/// its pages are read later, by `page_span` and `supply`.
	pub fn open_head(head: &[u8], m: &ChunkMeta, rows: u64) -> Result<Chunk> {
		if !is_paged(m.encoding) {
			return Err(CodecError::Corrupt("a chunk that is not paged was opened by its head"));
		}
		if crc32c(head) != m.crc {
			return Err(CodecError::Corrupt("a column chunk's checksum does not match"));
		}
		Chunk::parse(head, head.len(), m.encoding, rows, m.length as usize)
	}

	/// `first` holds at least the chunk's first `n` bytes (all of it when it is not paged); a
	/// paged chunk's pages end at `chunk_len`.
	fn parse(first: &[u8], n: usize, encoding: u8, rows: u64, chunk_len: usize) -> Result<Chunk> {
		let paged = is_paged(encoding);
		if !paged && !matches!(encoding, ENC_INT | ENC_FLOAT | ENC_BOOL | ENC_DICT | ENC_PLAIN) {
			return Err(CodecError::Corrupt("a column names an encoding the format does not have"));
		}
		let frame = if paged {
			let mut c = Cur { d: &first[..n], p: 0 };
			let len = c.bounded(n as u64, "a chunk's head is longer than the chunk")? as usize;
			let f = c.bytes(len)?;
			if c.p != n {
				return Err(CodecError::Corrupt("bytes follow a chunk's head"));
			}
			f
		} else {
			&first[..n]
		};
		let raw = compress::decompress(frame)?;
		let mut c = Cur { d: &raw, p: 0 };
		let nb = c.bounded(raw.len() as u64, "a column's null bitmap is longer than the column")? as usize;
		let nulls = bitmap::decode(c.bytes(nb)?)?;
		if nulls.len() as u64 != rows {
			return Err(CodecError::Corrupt("a column's null bitmap does not cover its row group"));
		}
		if !paged {
			let values_at = c.p;
			return Ok(Chunk { nulls, encoding, raw, values_at, pages: None });
		}
		let page_rows = c.bounded(codec::MAX_VALUES as u64, "a chunk's pages are longer than a block may be")? as usize;
		if page_rows == 0 {
			return Err(CodecError::Corrupt("a chunk's pages hold no rows"));
		}
		let count = c.bounded(rows, "a chunk has more pages than rows")? as usize;
		if count != nulls.len().div_ceil(page_rows) {
			return Err(CodecError::Corrupt("a chunk's pages do not cover its row group"));
		}
		let mut spans = Vec::with_capacity(count);
		let mut crcs = Vec::with_capacity(count);
		let mut before = Vec::with_capacity(count);
		let (mut at, mut values) = (n, 0);
		for k in 0..count {
			let len = c.bounded(chunk_len as u64, "a chunk's page is longer than the chunk")? as usize;
			crcs.push(c.u32()?);
			let end = at.checked_add(len).filter(|&e| e <= chunk_len).ok_or(CodecError::Truncated)?;
			spans.push((at, len));
			before.push(values);
			values += nulls[k * page_rows..((k + 1) * page_rows).min(nulls.len())].iter().filter(|&&x| !x).count();
			at = end;
		}
		if c.p != raw.len() {
			return Err(CodecError::Corrupt("bytes follow a chunk's page table"));
		}
		if at != chunk_len {
			return Err(CodecError::Corrupt("bytes follow a chunk's last page"));
		}
		let frames = vec![None; count];
		Ok(Chunk { nulls, encoding, raw: Vec::new(), values_at: 0, pages: Some(Pages { rows: page_rows, spans, crcs, frames, before, values }) })
	}

	/// Every page's frame from the whole chunk's bytes.
	fn supply_all(&mut self, chunk: &[u8], check: bool) -> Result<()> {
		let Some(p) = &self.pages else { return Ok(()) };
		let spans = p.spans.clone();
		for (k, (at, len)) in spans.into_iter().enumerate() {
			let bytes = chunk.get(at..at + len).ok_or(CodecError::Truncated)?;
			self.supply(k, bytes, check)?;
		}
		Ok(())
	}

	/// Rows per page, when the chunk is paged.
	pub fn page_rows(&self) -> Option<usize> {
		self.pages.as_ref().map(|p| p.rows)
	}

	/// Where page `k`'s frame is in the chunk, from its start, when it has not been read yet.
	pub fn page_span(&self, k: usize) -> Option<(usize, usize)> {
		let p = self.pages.as_ref()?;
		p.frames.get(k)?.is_none().then(|| p.spans[k])
	}

	/// Page `k`'s frame, read: checked against its checksum (`check`), and kept.
	pub fn supply(&mut self, k: usize, frame: &[u8], check: bool) -> Result<()> {
		let p = self.pages.as_mut().ok_or(CodecError::Corrupt("a chunk that is not paged was given a page"))?;
		let &(_, len) = p.spans.get(k).ok_or(CodecError::Corrupt("a page past a chunk's last was given"))?;
		if frame.len() != len {
			return Err(CodecError::Truncated);
		}
		if check && crc32c(frame) != p.crcs[k] {
			return Err(CodecError::Corrupt("a page's checksum does not match"));
		}
		p.frames[k] = Some(frame.to_vec());
		Ok(())
	}

	/// Every non-null value, in row order.
	pub fn decode_all(&self) -> Result<Decoded> {
		let values = match &self.pages {
			None => {
				let rest = &self.raw[self.values_at..];
				match self.encoding {
					ENC_INT => Decoded::Int(int::decode(rest)?),
					ENC_FLOAT => Decoded::Float(float::decode_bits(rest)?),
					ENC_BOOL => Decoded::Bool(bitmap::decode(rest)?),
					ENC_DICT => {
						let d = dict::decode(rest)?;
						Decoded::Dict { entries: d.entries.iter().map(|e| e.to_vec()).collect(), indexes: d.indexes }
					}
					ENC_PLAIN => Decoded::Plain(dict::decode_plain(rest)?.into_iter().map(|v| v.to_vec()).collect()),
					_ => return Err(CodecError::Corrupt("a column names an encoding the format does not have")),
				}
			}
			Some(p) => self.decode_pages(0..p.spans.len())?,
		};
		let count = match &values {
			Decoded::Int(v) => v.len(),
			Decoded::Float(v) => v.len(),
			Decoded::Bool(v) => v.len(),
			Decoded::Dict { indexes, .. } => indexes.len(),
			Decoded::Plain(v) => v.len(),
		};
		if count != self.nulls.iter().filter(|&&n| !n).count() {
			return Err(CodecError::Corrupt("a column holds a different number of values than its null bitmap says"));
		}
		Ok(values)
	}

	/// Page `k`'s non-null values, in row order: those of rows `k * page_rows` up to the next
	/// page's. Only for a paged chunk whose page `k` has been read.
	pub fn decode_page(&self, k: usize) -> Result<Decoded> {
		self.decode_pages(k..k + 1)
	}

	/// The non-null values of pages `pages`, in row order, in one vector. Only for a paged chunk
	/// whose pages in the range have been read.
	pub fn decode_pages(&self, pages: std::ops::Range<usize>) -> Result<Decoded> {
		let p = self.pages.as_ref().ok_or(CodecError::Corrupt("a chunk that is not paged was read by the page"))?;
		if pages.end > p.spans.len() || pages.start >= pages.end {
			return Err(CodecError::Corrupt("a page past a chunk's last was read"));
		}
		let count = p.before.get(pages.end).copied().unwrap_or(p.values) - p.before[pages.start];
		let frame = |k: usize| p.frames[k].as_deref().ok_or(CodecError::Corrupt("a page was decoded before it was read"));
		if self.encoding == ENC_INT_PAGED {
			let mut v = Vec::with_capacity(count);
			for k in pages {
				let before = v.len();
				int::decode_into(&compress::decompress(frame(k)?)?, &mut v)?;
				check_page(p, k, v.len() - before)?;
			}
			Ok(Decoded::Int(v))
		} else {
			let mut v = Vec::with_capacity(count);
			for k in pages {
				let before = v.len();
				float::decode_bits_into(&compress::decompress(frame(k)?)?, &mut v)?;
				check_page(p, k, v.len() - before)?;
			}
			Ok(Decoded::Float(v))
		}
	}
}

/// Page `k` decoded to `n` values: as many as its rows' null bitmap has non-NULLs?
fn check_page(p: &Pages, k: usize, n: usize) -> Result<()> {
	if n != p.before.get(k + 1).copied().unwrap_or(p.values) - p.before[k] {
		return Err(CodecError::Corrupt("a page holds a different number of values than its null bitmap says"));
	}
	Ok(())
}

// ---- row groups ----

pub fn encode_group(columns: &[Column], rows: u64, codec: compress::Codec) -> Result<(Vec<u8>, Vec<ChunkMeta>)> {
	let mut data = Vec::new();
	let mut metas = Vec::with_capacity(columns.len());
	for col in columns {
		if col.nulls.len() as u64 != rows {
			return Err(CodecError::Corrupt("a column does not cover its row group"));
		}
		let (frame, encoding, minmax) = encode_column(col, codec)?;
		metas.push(ChunkMeta {
			offset: data.len() as u64,
			length: frame.len() as u64,
			encoding,
			nulls: col.nulls.iter().filter(|&&n| n).count() as u64,
			minmax,
			crc: crc32c(&frame[..checked_len(&frame, encoding)?]),
		});
		data.extend_from_slice(&frame);
	}
	let mut header = Vec::new();
	put(&mut header, rows);
	put(&mut header, metas.len() as u64);
	for m in &metas {
		put(&mut header, m.offset);
		put(&mut header, m.length);
		header.push(m.encoding);
		put(&mut header, m.nulls);
		put_minmax(&mut header, m.minmax);
		header.extend_from_slice(&m.crc.to_le_bytes());
	}
	let mut out = Vec::with_capacity(header.len() + data.len() + 4);
	put(&mut out, header.len() as u64);
	out.extend_from_slice(&header);
	out.extend_from_slice(&data);
	Ok((out, metas))
}

/// Parses a row group's header from its first bytes (at most the whole group). Checks every
/// chunk lies inside `group_len`, so the chunks can be read without further checks.
pub fn decode_group_header(bytes: &[u8], group_len: u64, max_columns: usize) -> Result<GroupHeader> {
	let mut c = Cur { d: bytes, p: 0 };
	let hlen = c.bounded(group_len, "a row group's header is longer than the row group")? as usize;
	let start = c.p;
	let h = c.bytes(hlen)?;
	let mut hc = Cur { d: h, p: 0 };
	let rows = hc.bounded(codec::MAX_VALUES as u64, "a row group holds more rows than a block may")?;
	let ncols = hc.bounded(max_columns as u64, "a row group has more columns than its table")? as usize;
	let data_start = start + hlen;
	let data_len = group_len - data_start as u64;
	let mut columns = Vec::with_capacity(ncols);
	for _ in 0..ncols {
		let offset = hc.varint()?;
		let length = hc.varint()?;
		if offset.checked_add(length).is_none_or(|e| e > data_len) {
			return Err(CodecError::Corrupt("a column chunk runs past the end of its row group"));
		}
		let encoding = hc.byte()?;
		let nulls = hc.bounded(rows, "a column has more nulls than rows")?;
		let minmax = hc.minmax()?;
		let crc = hc.u32()?;
		columns.push(ChunkMeta { offset, length, encoding, nulls, minmax, crc });
	}
	if hc.p != h.len() {
		return Err(CodecError::Corrupt("bytes follow a row group's header"));
	}
	Ok(GroupHeader { rows, columns, data_start })
}

/// One column chunk read on its own, checksummed and decoded (the fuzzer's entry point; the
/// reader opens a [`Chunk`] and decodes what it needs).
#[allow(dead_code)]
pub fn decode_chunk(frame: &[u8], m: &ChunkMeta, rows: u64) -> Result<DecodedColumn> {
	let chunk = Chunk::open_checked(frame, m, rows)?;
	let values = chunk.decode_all()?;
	Ok(DecodedColumn { nulls: chunk.nulls, values })
}

/// How many bytes from a row group's start its header ends at, from its first bytes.
pub fn header_end(first: &[u8], group_len: u64) -> Result<usize> {
	let mut c = Cur { d: first, p: 0 };
	let hlen = c.bounded(group_len, "a row group's header is longer than the row group")?;
	Ok(c.p + hlen as usize)
}

/// The chunk of column `i`, checksummed and decoded. `group` is the whole row group.
#[cfg(test)]
pub fn read_column(group: &[u8], h: &GroupHeader, i: usize) -> Result<DecodedColumn> {
	let m = &h.columns[i];
	let start = h.data_start + m.offset as usize;
	let frame = group.get(start..start + m.length as usize).ok_or(CodecError::Truncated)?;
	let chunk = Chunk::open_checked(frame, m, h.rows)?;
	let values = chunk.decode_all()?;
	Ok(DecodedColumn { nulls: chunk.nulls, values })
}

// ---- the directory and the metapage ----

pub fn encode_directory(groups: &[GroupEntry]) -> Vec<u8> {
	let mut out = Vec::new();
	put(&mut out, groups.len() as u64);
	for g in groups {
		put(&mut out, g.offset);
		put(&mut out, g.length);
		put(&mut out, g.first_row);
		put(&mut out, g.rows);
		put(&mut out, g.minmax.len() as u64);
		for &m in &g.minmax {
			put_minmax(&mut out, m);
		}
		let key = |out: &mut Vec<u8>, k: &[Option<Vec<u8>>]| {
			for v in k {
				match v {
					None => put(out, 0),
					Some(b) => {
						put(out, b.len() as u64 + 1);
						out.extend_from_slice(b);
					}
				}
			}
		};
		key(&mut out, &g.first_key);
		key(&mut out, &g.last_key);
		if !g.first_key.is_empty() {
			match &g.run_ends {
				None => put(&mut out, 0),
				Some(runs) => {
					put(&mut out, runs.len() as u64 + 1);
					for (row, k) in runs {
						put(&mut out, *row as u64);
						key(&mut out, k);
					}
				}
			}
		}
	}
	out
}

/// `stream_end` is where the directory starts: every row group must lie before it, in
/// order, without overlapping, and the row numbers must follow on.
pub fn decode_directory(bytes: &[u8], meta: &Meta) -> Result<Vec<GroupEntry>> {
	if crc32c(bytes) != meta.dir_crc {
		return Err(CodecError::Corrupt("the row-group directory's checksum does not match"));
	}
	let mut c = Cur { d: bytes, p: 0 };
	let n = c.bounded(meta.groups, "the directory lists more row groups than the metapage")?;
	if n != meta.groups {
		return Err(CodecError::Corrupt("the directory lists fewer row groups than the metapage"));
	}
	let mut out = Vec::with_capacity(n as usize);
	let (mut next_offset, mut next_row) = (0u64, 0u64);
	for _ in 0..n {
		let offset = c.varint()?;
		let length = c.varint()?;
		let first_row = c.varint()?;
		let rows = c.bounded(codec::MAX_VALUES as u64, "a row group holds more rows than a block may")?;
		if offset != next_offset || first_row != next_row || rows == 0 {
			return Err(CodecError::Corrupt("the row groups in the directory do not follow on"));
		}
		next_offset = offset.checked_add(length).ok_or(CodecError::Corrupt("a row group's length overflows"))?;
		next_row += rows;
		// local: row groups lie before the directory; tiered: inside the object
		if next_offset > meta.remote.as_ref().map_or(meta.dir_offset, |r| r.length) {
			return Err(CodecError::Corrupt("a row group runs past the end of its stream"));
		}
		let ncols = c.bounded(meta.types.len() as u64, "a row group has more columns than its table")?;
		let mut minmax = Vec::with_capacity(ncols as usize);
		for _ in 0..ncols {
			minmax.push(c.minmax()?);
		}
		let width = meta.order.len();
		let first_key = read_key(&mut c, width)?;
		let last_key = read_key(&mut c, width)?;
		let run_ends = if meta.order.is_empty() {
			None
		} else {
			match c.bounded(MAX_RUNS as u64 + 1, "a row group records more runs than a group may")? {
				0 => None,
				n => {
					let mut runs = Vec::with_capacity(n as usize - 1);
					for _ in 1..n {
						let row = c.bounded(rows.saturating_sub(1), "a run ends past its row group")? as u32;
						runs.push((row, read_key(&mut c, width)?));
					}
					Some(runs)
				}
			}
		};
		out.push(GroupEntry { offset, length, first_row, rows, minmax, first_key, last_key, run_ends });
	}
	if c.p != bytes.len() {
		return Err(CodecError::Corrupt("bytes follow the directory"));
	}
	if next_row != meta.rows {
		return Err(CodecError::Corrupt("the row groups hold a different number of rows than the metapage says"));
	}
	Ok(out)
}

/// A sort key in the directory: one value per sort column, each varint 0 (NULL) or length + 1
/// then the bytes.
fn read_key(c: &mut Cur, width: usize) -> Result<Vec<Option<Vec<u8>>>> {
	let mut k = Vec::with_capacity(width);
	for _ in 0..width {
		let n = c.varint()?;
		k.push(if n == 0 {
			None
		} else {
			let len = usize::try_from(n - 1).map_err(|_| CodecError::Truncated)?;
			Some(c.bytes(len)?.to_vec())
		});
	}
	Ok(k)
}

pub fn encode_meta(m: &Meta) -> Vec<u8> {
	let mut out = Vec::new();
	out.extend_from_slice(&MAGIC.to_le_bytes());
	out.extend_from_slice(&VERSION.to_le_bytes());
	out.extend_from_slice(&m.rows.to_le_bytes());
	out.extend_from_slice(&m.groups.to_le_bytes());
	out.push(m.codec);
	out.extend_from_slice(&m.dir_offset.to_le_bytes());
	out.extend_from_slice(&m.dir_length.to_le_bytes());
	out.extend_from_slice(&m.dir_crc.to_le_bytes());
	out.extend_from_slice(&(m.types.len() as u32).to_le_bytes());
	for t in &m.types {
		out.extend_from_slice(&t.to_le_bytes());
	}
	match &m.remote {
		None => out.push(0),
		Some(r) => {
			out.push(1);
			out.extend_from_slice(&r.length.to_le_bytes());
			out.extend_from_slice(&(r.url.len() as u32).to_le_bytes());
			out.extend_from_slice(r.url.as_bytes());
		}
	}
	out.push(m.flags);
	out.extend_from_slice(&(m.order.len() as u16).to_le_bytes());
	for a in &m.order {
		out.extend_from_slice(&a.to_le_bytes());
	}
	let crc = crc32c(&out);
	out.extend_from_slice(&crc.to_le_bytes());
	out
}

/// `space` is the metapage's data area; `max_columns` bounds the type list.
pub fn decode_meta(space: &[u8], max_columns: usize) -> Result<Meta> {
	let mut c = Cur { d: space, p: 0 };
	if c.u32()? != MAGIC {
		return Err(CodecError::Corrupt("the metapage is not a SnoutTime column store's"));
	}
	if !(OLDEST_VERSION..=VERSION).contains(&c.u32()?) {
		return Err(CodecError::Corrupt("the column store was written by a format version this build does not read"));
	}
	let rows = c.u64()?;
	let groups = c.u64()?;
	let codec = c.byte()?;
	let dir_offset = c.u64()?;
	let dir_length = c.u64()?;
	let dir_crc = c.u32()?;
	let ncols = c.u32()? as usize;
	if ncols > max_columns {
		return Err(CodecError::Corrupt("the metapage lists more columns than a table may have"));
	}
	let mut types = Vec::with_capacity(ncols);
	for _ in 0..ncols {
		types.push(c.u32()?);
	}
	let remote = match c.byte()? {
		0 => None,
		1 => {
			let length = c.u64()?;
			let n = c.u32()? as usize;
			let url = std::str::from_utf8(c.bytes(n)?).map_err(|_| CodecError::Corrupt("a tiered column store's URL is not text"))?;
			Some(Remote { url: url.to_string(), length })
		}
		_ => return Err(CodecError::Corrupt("the metapage names a location the format does not have")),
	};
	let flags = c.byte()?;
	if flags & !FLAG_LATE_INDEXES != 0 {
		return Err(CodecError::Corrupt("the metapage has a flag the format does not have"));
	}
	let norder = c.bytes(2).map(|b| u16::from_le_bytes([b[0], b[1]]))? as usize;
	if norder > ncols {
		return Err(CodecError::Corrupt("the metapage sorts by more columns than it has"));
	}
	let mut order = Vec::with_capacity(norder);
	for _ in 0..norder {
		let a = c.bytes(2).map(|b| u16::from_le_bytes([b[0], b[1]]))?;
		if a == 0 || a as usize > ncols || order.contains(&a) {
			return Err(CodecError::Corrupt("the metapage sorts by a column it does not have"));
		}
		order.push(a);
	}
	let body = c.p;
	let crc = c.u32()?;
	if crc != crc32c(&space[..body]) {
		return Err(CodecError::Corrupt("the metapage's checksum does not match"));
	}
	if dir_offset.checked_add(dir_length).is_none() || groups > rows {
		return Err(CodecError::Corrupt("the metapage's sizes do not add up"));
	}
	Ok(Meta { rows, groups, codec, dir_offset, dir_length, dir_crc, types, remote, order, flags })
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::codec::compress::Codec;
	use crate::codec::testing::{survives, Rng};

	fn columns(rows: usize, rng: &mut Rng) -> Vec<Column> {
		let nulls: Vec<bool> = (0..rows).map(|_| rng.below(10) == 0).collect();
		let nonnull = nulls.iter().filter(|&&n| !n).count();
		vec![
			Column { nulls: vec![false; rows], values: Values::Int((0..rows as i64).map(|i| 1_000_000 + i * 10).collect()) },
			Column { nulls: nulls.clone(), values: Values::Float((0..nonnull).map(|i| (i as f64 / 3.0).to_bits()).collect()) },
			Column { nulls: nulls.clone(), values: Values::Bool((0..nonnull).map(|i| i % 3 == 0).collect()) },
			Column {
				nulls: nulls.clone(),
				values: Values::Bytes((0..nonnull).map(|i| format!("host_{}", i % 7).into_bytes()).collect()),
			},
			Column {
				nulls: nulls.clone(),
				values: Values::Bytes((0..nonnull).map(|_| rng.next().to_le_bytes().to_vec()).collect()),
			},
			Column { nulls: vec![true; rows], values: Values::Int(vec![]) },
		]
	}

	fn same(col: &Column, d: &DecodedColumn) -> bool {
		if col.nulls != d.nulls {
			return false;
		}
		match (&col.values, &d.values) {
			(Values::Int(a), Decoded::Int(b)) => a == b,
			(Values::Float(a), Decoded::Float(b)) => a == b,
			(Values::Bool(a), Decoded::Bool(b)) => a == b,
			(Values::Bytes(a), Decoded::Dict { entries, indexes }) => {
				a.len() == indexes.len() && a.iter().zip(indexes).all(|(v, &i)| *v == entries[i as usize])
			}
			(Values::Bytes(a), Decoded::Plain(b)) => a == b,
			_ => false,
		}
	}

	#[test]
	fn crc32c_matches_the_published_check_value() {
		assert_eq!(crc32c(b"123456789"), 0xE306_9283);
		assert_eq!(crc32c(b""), 0);
	}

	#[test]
	fn row_groups_round_trip_for_every_codec_and_size() {
		let mut rng = Rng(51);
		for rows in [1usize, 2, 100, PAGE_ROWS, PAGE_ROWS + 1, 3000, 8192] {
			for codec in [Codec::None, Codec::Lz4, Codec::Zstd] {
				let cols = columns(rows, &mut rng);
				let (bytes, metas) = encode_group(&cols, rows as u64, codec).unwrap();
				let h = decode_group_header(&bytes, bytes.len() as u64, 16).unwrap();
				assert_eq!(h.columns, metas);
				assert_eq!(h.rows, rows as u64);
				for (i, col) in cols.iter().enumerate() {
					assert!(same(col, &read_column(&bytes, &h, i).unwrap()), "rows {rows}, {codec:?}, column {i}");
				}
				assert_eq!(metas[0].minmax, Some(MinMax { min: 1_000_000, max: 1_000_000 + (rows as i64 - 1) * 10 }));
				assert_eq!(metas[3].encoding, if rows >= 100 { ENC_DICT } else { metas[3].encoding });
				assert_eq!(metas[5].nulls, rows as u64);
			}
		}
	}

	/// A paged chunk read a page at a time gives, page by page, what reading it whole gives, for
	/// any mix of NULLs (a page with none, a page of nothing but NULLs, a short last page).
	#[test]
	fn pages_decode_as_the_whole_chunk_does() {
		let mut rng = Rng(53);
		for rows in [PAGE_ROWS + 1, 2 * PAGE_ROWS, 5000, 8192] {
			for density in [0u64, 1, 5, 10] {
				let nulls: Vec<bool> = (0..rows)
					.map(|r| match density {
						0 => false,
						// the second page all NULL, the rest none
						1 => (PAGE_ROWS..2 * PAGE_ROWS).contains(&r),
						d => rng.below(d) == 0,
					})
					.collect();
				let nonnull = nulls.iter().filter(|&&n| !n).count();
				let ints = Column { nulls: nulls.clone(), values: Values::Int((0..nonnull as i64).map(|i| 1_700_000_000 + i * 10 + (i % 7)).collect()) };
				let floats = Column { nulls: nulls.clone(), values: Values::Float((0..nonnull).map(|i| ((i as f64).sin() * 100.0).to_bits()).collect()) };
				for col in [&ints, &floats] {
					for codec in [Codec::None, Codec::Lz4, Codec::Zstd] {
						let (frame, enc, _) = encode_column(col, codec).unwrap();
						assert!(enc == ENC_INT_PAGED || enc == ENC_FLOAT_PAGED);
						let chunk = Chunk::open(&frame, enc, rows as u64).unwrap();
						assert_eq!(chunk.page_rows(), Some(PAGE_ROWS));
						assert_eq!(chunk.nulls, nulls);
						let mut paged: Vec<u64> = Vec::new();
						for k in 0..rows.div_ceil(PAGE_ROWS) {
							match chunk.decode_page(k).unwrap() {
								Decoded::Int(v) => paged.extend(v.iter().map(|&x| x as u64)),
								Decoded::Float(v) => paged.extend(v),
								_ => unreachable!(),
							}
						}
						let whole: Vec<u64> = match &col.values {
							Values::Int(v) => v.iter().map(|&x| x as u64).collect(),
							Values::Float(v) => v.clone(),
							_ => unreachable!(),
						};
						assert_eq!(paged, whole, "{rows} rows, density {density}, {codec:?}");
						assert!(same(col, &decode_column(&frame, enc, rows as u64).unwrap()));
						assert!(chunk.decode_page(rows.div_ceil(PAGE_ROWS)).is_err(), "no page past the last");
					}
				}
			}
		}
	}

	/// A paged chunk read the way a narrow read reads it: its head, then only the pages asked
	/// for, each checked on its own, so a damaged page is found when it is read and the others
	/// still read.
	#[test]
	fn a_page_is_read_on_its_own() {
		let rows = 3 * PAGE_ROWS + 5;
		let nulls: Vec<bool> = (0..rows).map(|r| r % 13 == 0).collect();
		let nonnull = nulls.iter().filter(|&&n| !n).count();
		let col = Column { nulls: nulls.clone(), values: Values::Float((0..nonnull).map(|i| ((i % 97) as f64 * 1.5).to_bits()).collect()) };
		for codec in [Codec::None, Codec::Lz4, Codec::Zstd] {
			let (bytes, metas) = encode_group(std::slice::from_ref(&col), rows as u64, codec).unwrap();
			let h = decode_group_header(&bytes, bytes.len() as u64, 4).unwrap();
			let m = &metas[0];
			let chunk = &bytes[h.data_start + m.offset as usize..h.data_start + (m.offset + m.length) as usize];
			let head = &chunk[..head_len(chunk, chunk.len()).unwrap()];
			let mut c = Chunk::open_head(head, m, rows as u64).unwrap();
			assert!(c.decode_page(2).is_err(), "a page not read yet is not decoded");
			let (at, len) = c.page_span(2).unwrap();
			c.supply(2, &chunk[at..at + len], true).unwrap();
			assert_eq!(c.page_span(2), None);
			let Decoded::Float(page) = c.decode_page(2).unwrap() else { unreachable!() };
			let Values::Float(all) = &col.values else { unreachable!() };
			let first = nulls[..2 * PAGE_ROWS].iter().filter(|&&n| !n).count();
			assert_eq!(page, all[first..first + page.len()].to_vec(), "{codec:?}");
			// a flipped bit in page 1: that page is refused, page 3 still reads
			let (at1, len1) = c.page_span(1).unwrap();
			let mut bad = chunk[at1..at1 + len1].to_vec();
			bad[len1 / 2] ^= 0x04;
			assert_eq!(c.supply(1, &bad, true), Err(CodecError::Corrupt("a page's checksum does not match")));
			let (at3, len3) = c.page_span(3).unwrap();
			c.supply(3, &chunk[at3..at3 + len3], true).unwrap();
			assert!(c.decode_page(3).is_ok());
			// and the whole chunk still reads whole, checked
			assert!(same(&col, &decode_chunk(chunk, m, rows as u64).unwrap()));
		}
	}

	/// Damage to a paged chunk's page table or pages is an error, never a panic, however it is
	/// read.
	#[test]
	fn a_damaged_paged_chunk_is_an_error() {
		// two pages, the second short, with NULLs; values that encode small, so every bit of the
		// frame can be flipped in a debug build's time
		let rows = PAGE_ROWS + 76;
		let nulls: Vec<bool> = (0..rows).map(|r| r % 11 == 0).collect();
		let nonnull = nulls.iter().filter(|&&n| !n).count();
		for col in [
			Column { nulls: nulls.clone(), values: Values::Int((0..nonnull as i64).map(|i| i * 3).collect()) },
			Column { nulls: nulls.clone(), values: Values::Float((0..nonnull).map(|i| ((i % 3) as f64).to_bits()).collect()) },
		] {
			let (frame, enc, _) = encode_column(&col, Codec::None).unwrap();
			survives(&frame, |b| {
				let c = Chunk::open(b, enc, rows as u64)?;
				for k in 0..4 {
					let _ = c.decode_page(k);
				}
				c.decode_all()
			});
		}
	}

	#[test]
	fn a_flipped_bit_in_a_chunk_is_caught_by_its_checksum() {
		let mut rng = Rng(52);
		let cols = columns(500, &mut rng);
		let (bytes, _) = encode_group(&cols, 500, Codec::Lz4).unwrap();
		let h = decode_group_header(&bytes, bytes.len() as u64, 16).unwrap();
		let mut damaged = bytes.clone();
		let at = h.data_start + h.columns[3].offset as usize + 5;
		damaged[at] ^= 0x10;
		assert_eq!(read_column(&damaged, &h, 3), Err(CodecError::Corrupt("a column chunk's checksum does not match")));
		assert!(read_column(&damaged, &h, 2).is_ok(), "the other columns are untouched");
	}

	#[test]
	fn directory_and_meta_round_trip_and_check_each_other() {
		let groups: Vec<GroupEntry> = (0..5u64)
			.map(|i| GroupEntry {
				offset: i * 1000,
				length: 1000,
				first_row: i * 8192,
				rows: 8192,
				minmax: vec![Some(MinMax { min: i as i64, max: i as i64 + 9 }), None],
				first_key: vec![Some(format!("host_{i}").into_bytes()), None],
				last_key: vec![Some(format!("host_{}", i + 1).into_bytes()), Some(vec![])],
				run_ends: if i % 2 == 0 { Some(vec![(4000, vec![Some(format!("host_{i}").into_bytes()), Some(vec![1, 2])])]) } else { None },
			})
			.collect();
		let dir = encode_directory(&groups);
		let meta = Meta {
			rows: 5 * 8192,
			groups: 5,
			codec: 1,
			dir_offset: 5000,
			dir_length: dir.len() as u64,
			dir_crc: crc32c(&dir),
			types: vec![1184, 701],
			remote: None,
			order: vec![2, 1],
			flags: FLAG_LATE_INDEXES,
		};
		let mbytes = encode_meta(&meta);
		let mut page = mbytes.clone();
		page.resize(8168, 0);
		assert_eq!(decode_meta(&page, 1600).unwrap(), meta);
		assert_eq!(decode_directory(&dir, &meta).unwrap(), groups);
		// a tiered one: the metapage carries the object, and row groups are checked against it
		let tiered = Meta { remote: Some(Remote { url: "s3://b/p/1.snt".into(), length: 5000 }), dir_offset: 0, ..meta.clone() };
		let mut page = encode_meta(&tiered);
		page.resize(8168, 0);
		assert_eq!(decode_meta(&page, 1600).unwrap(), tiered);
		assert_eq!(decode_directory(&dir, &tiered).unwrap(), groups);
		let short = Meta { remote: Some(Remote { url: "s3://b/p/1.snt".into(), length: 4999 }), ..tiered.clone() };
		assert!(decode_directory(&dir, &short).is_err(), "a row group past the object's end");
		let mut wrong = meta.clone();
		wrong.rows += 1;
		wrong.dir_crc = crc32c(&dir);
		assert!(decode_directory(&dir, &wrong).is_err(), "row counts must agree");
		let mut gap = groups.clone();
		gap[2].offset += 1;
		let d2 = encode_directory(&gap);
		let m2 = Meta { dir_crc: crc32c(&d2), dir_length: d2.len() as u64, ..meta.clone() };
		assert!(decode_directory(&d2, &m2).is_err(), "row groups must follow on");
	}

	#[test]
	fn damage_is_an_error_not_a_panic() {
		let mut rng = Rng(53);
		let cols = columns(300, &mut rng);
		let (bytes, _) = encode_group(&cols, 300, Codec::None).unwrap();
		survives(&bytes, |b| {
			let h = decode_group_header(b, b.len() as u64, 16)?;
			for i in 0..h.columns.len() {
				let _ = read_column(b, &h, i);
			}
			Ok(())
		});
		// the chunks themselves, below the checksum
		let (frame, enc, _) = encode_column(&cols[3], Codec::None).unwrap();
		survives(&frame, |b| decode_column(b, enc, 300).map(drop));
		let meta = Meta {
			rows: 10,
			groups: 1,
			codec: 0,
			dir_offset: 0,
			dir_length: 0,
			dir_crc: 0,
			types: vec![23; 3],
			remote: Some(Remote { url: "s3://b/k".into(), length: 100 }),
			order: vec![3, 1],
			flags: 1,
		};
		survives(&encode_meta(&meta), |b| decode_meta(b, 1600).map(drop));
	}

	#[test]
	fn crc32c_is_castagnoli_whichever_way_it_is_computed() {
		// the standard check value, and the instruction against the table on every length
		assert_eq!(crc32c(b"123456789"), 0xE306_9283);
		let data: Vec<u8> = (0..1000u32).map(|i| (i.wrapping_mul(2_654_435_761) >> 13) as u8).collect();
		for len in 0..data.len() {
			assert_eq!(crc32c(&data[..len]), !crc32c_table(!0, &data[..len]), "length {len}");
		}
	}

	/// Encode speed of IoT-shaped columns, ns per value, per codec. A measurement, not a check:
	/// `cargo test --release --lib encode_speed -- --ignored --nocapture`.
	#[test]
	#[ignore]
	fn encode_speed() {
		use std::time::Instant;
		let n = 8192usize;
		let mut seed = 11u64;
		let mut rnd = || {
			seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
			seed >> 11
		};
		let ts: Vec<i64> = (0..n as i64).map(|i| 1_700_000_000_000_000 + i * 60_000_000 + (i % 50) * 1_000_000).collect();
		let dev: Vec<i64> = (0..n as i64).map(|i| i / 1440).collect();
		let temp: Vec<u64> = (0..n).map(|_| (((rnd() % 3000) as f64) / 100.0 + 5.0).to_bits()).collect();
		let status: Vec<bool> = (0..n).map(|i| i % 100 != 0).collect();
		let status_vals: Vec<Vec<u8>> = (0..n / 100 + 1).map(|i| ["ok", "warn", "fault", "reboot"][i % 4].as_bytes().to_vec()).collect();
		let firmware: Vec<Vec<u8>> = (0..n).map(|i| format!("v{}", (i / 1440) % 5).into_bytes()).collect();
		let cols = [
			("timestamp", Column { nulls: vec![false; n], values: Values::Int(ts) }),
			("device int4", Column { nulls: vec![false; n], values: Values::Int(dev) }),
			("float", Column { nulls: vec![false; n], values: Values::Float(temp) }),
			("sparse text", Column { nulls: status, values: Values::Bytes(status_vals) }),
			("text, 5 values", Column { nulls: vec![false; n], values: Values::Bytes(firmware) }),
		];
		for codec in [compress::Codec::Lz4, compress::Codec::Zstd] {
			for (name, c) in &cols {
				let reps = 100;
				let t = Instant::now();
				for _ in 0..reps {
					std::hint::black_box(encode_column(c, codec).unwrap());
				}
				let ns = t.elapsed().as_nanos() as f64 / (reps * n) as f64;
				eprintln!("{codec:?} {name}: {ns:.2} ns per value encoding");
			}
		}
	}

	/// Decode speed of one bench-shaped row group's chunks, ns per value. A measurement, not a
	/// check: `cargo test --release --lib decode_speed -- --ignored --nocapture`.
	#[test]
	#[ignore]
	fn decode_speed() {
		use std::time::Instant;
		let n = 8192usize;
		let mut gauge = 50.0f64;
		let mut seed = 7u64;
		let mut rnd = || {
			seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
			seed >> 11
		};
		let floats: Vec<u64> = (0..n)
			.map(|_| {
				gauge = (gauge + (rnd() % 1000) as f64 / 100.0 - 5.0).clamp(0.0, 100.0);
				((gauge * 1e6).round() / 1e6).to_bits()
			})
			.collect();
		let ts: Vec<i64> = (0..n as i64).map(|i| 1_700_000_000_000_000 + i * 10_000_000).collect();
		let hosts: Vec<Vec<u8>> = (0..n).map(|i| format!("host_{}", i / 4096).into_bytes()).collect();
		let cols = [
			("float", Column { nulls: vec![false; n], values: Values::Float(floats) }),
			("timestamp", Column { nulls: vec![false; n], values: Values::Int(ts) }),
			("text", Column { nulls: vec![false; n], values: Values::Bytes(hosts) }),
		];
		for codec in [compress::Codec::Lz4, compress::Codec::Zstd] {
			for (name, c) in &cols {
				let (frame, enc, _) = encode_column(c, codec).unwrap();
				let reps = 200;
				let t = Instant::now();
				for _ in 0..reps {
					std::hint::black_box(decode_column(&frame, enc, n as u64).unwrap());
				}
				let ns = t.elapsed().as_nanos() as f64 / (reps * n) as f64;
				let t = Instant::now();
				for _ in 0..reps {
					std::hint::black_box(crc32c(&frame));
				}
				let crc = t.elapsed().as_nanos() as f64 / (reps * n) as f64;
				eprintln!("{codec:?} {name}: {ns:.2} ns per value decoding, {crc:.2} checking ({} bytes)", frame.len());
			}
		}
	}
}
