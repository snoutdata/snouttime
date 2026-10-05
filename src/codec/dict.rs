//! Byte strings: text, and anything else stored as bytes.
//!
//! * [`encode`]: a dictionary of the distinct values, then each row as an index into it, the
//!   indexes either run-length encoded (a `segment_by`-ordered column is long runs) or
//!   bit-packed at the width the dictionary needs, whichever is smaller. For low cardinality.
//! * [`encode_plain`]: each value with its length. For everything a dictionary does not pay on.
//!
//! Both decoders return slices of their input, never copies: a dictionary of one large value
//! referenced 65,536 times decodes to 65,536 pointers, not 65,536 copies, so no input can make
//! a decoder allocate much more than its own size.
//!
//! Dictionary layout: count (varint), entries (varint), each entry as length (varint) + bytes,
//! then a mode byte. Mode 0: runs of (index varint, length varint), lengths at least 1 and
//! summing exactly to count. Mode 1: the indexes bit-packed at the width of `entries - 1`,
//! least significant bit first, zero-padded. Plain layout: count, then each length + bytes.

use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};

use super::{put_count, put_varint, BitReader, BitWriter, CodecError, Input, Result};

/// A decoded dictionary column: row `i` is `entries[indexes[i]]`.
#[derive(Debug, PartialEq, Eq)]
pub struct Dict<'a> {
	pub entries: Vec<&'a [u8]>,
	pub indexes: Vec<u32>,
}

impl<'a> Dict<'a> {
	pub fn get(&self, row: usize) -> Option<&'a [u8]> {
		self.indexes.get(row).map(|&i| self.entries[i as usize])
	}

	pub fn iter(&self) -> impl Iterator<Item = &'a [u8]> + '_ {
		self.indexes.iter().map(|&i| self.entries[i as usize])
	}
}

/// A multiply-and-rotate hash over 8 bytes at a time. The dictionary's keys are the column's
/// own values, hashed once each at the seal; SipHash's defence against chosen collisions buys
/// nothing there and cost most of encoding a text column (2026-09-23).
#[derive(Default)]
struct Quick(u64);

impl Hasher for Quick {
	fn write(&mut self, bytes: &[u8]) {
		let mut h = self.0;
		let mut chunks = bytes.chunks_exact(8);
		for c in &mut chunks {
			h = (h.rotate_left(5) ^ u64::from_le_bytes(c.try_into().unwrap())).wrapping_mul(0x51_7c_c1_b7_27_22_0a_95);
		}
		let mut tail = [0u8; 8];
		tail[..chunks.remainder().len()].copy_from_slice(chunks.remainder());
		h = (h.rotate_left(5) ^ u64::from_le_bytes(tail) ^ bytes.len() as u64).wrapping_mul(0x51_7c_c1_b7_27_22_0a_95);
		self.0 = h;
	}
	fn finish(&self) -> u64 {
		self.0
	}
}

/// The size [`encode_plain`] would give, without encoding.
pub fn plain_len<T: AsRef<[u8]>>(values: &[T]) -> usize {
	let varint = |v: u64| (64 - v.max(1).leading_zeros() as usize).div_ceil(7);
	varint(values.len() as u64) + values.iter().map(|v| varint(v.as_ref().len() as u64) + v.as_ref().len()).sum::<usize>()
}

/// The number of distinct values, which is what decides between [`encode`] and [`encode_plain`].
pub fn distinct<T: AsRef<[u8]>>(values: &[T]) -> usize {
	let mut seen = std::collections::HashSet::new();
	for v in values {
		seen.insert(v.as_ref());
	}
	seen.len()
}

pub fn encode<T: AsRef<[u8]>>(values: &[T]) -> Result<Vec<u8>> {
	let mut out = Vec::new();
	put_count(&mut out, values.len())?;
	let mut ids: HashMap<&[u8], u32, BuildHasherDefault<Quick>> = HashMap::default();
	let mut entries: Vec<&[u8]> = Vec::new();
	let mut last: Option<(&[u8], u32)> = None;
	let indexes: Vec<u32> = values
		.iter()
		.map(|v| {
			let v = v.as_ref();
			// sorted data repeats the value before: no hash for that
			if let Some((l, i)) = last {
				if l == v {
					return i;
				}
			}
			let i = *ids.entry(v).or_insert_with(|| {
				entries.push(v);
				entries.len() as u32 - 1
			});
			last = Some((v, i));
			i
		})
		.collect();
	put_varint(&mut out, entries.len() as u64);
	for e in &entries {
		put_varint(&mut out, e.len() as u64);
		out.extend_from_slice(e);
	}
	let mut runs = Vec::new();
	for group in indexes.chunk_by(|a, b| a == b) {
		put_varint(&mut runs, group[0] as u64);
		put_varint(&mut runs, group.len() as u64);
	}
	let width = width_for(entries.len());
	let mut w = BitWriter::new();
	for &i in &indexes {
		w.write(i as u64, width);
	}
	let packed = w.finish();
	if runs.len() <= packed.len() {
		out.push(0);
		out.extend(runs);
	} else {
		out.push(1);
		out.extend(packed);
	}
	Ok(out)
}

/// Bits per index for a dictionary of `n` entries.
fn width_for(n: usize) -> u32 {
	if n <= 1 {
		0
	} else {
		usize::BITS - (n - 1).leading_zeros()
	}
}

pub fn decode(data: &[u8]) -> Result<Dict<'_>> {
	let mut input = Input::new(data);
	let count = input.count()?;
	let n = input.count()?;
	if n > count || (n == 0 && count > 0) {
		return Err(CodecError::Corrupt("a dictionary has more entries than rows, or none for its rows"));
	}
	let mut entries = Vec::with_capacity(n);
	for _ in 0..n {
		let len = input.varint()?;
		let len = usize::try_from(len).map_err(|_| CodecError::Truncated)?;
		entries.push(input.bytes(len)?);
	}
	let mut indexes = Vec::with_capacity(count);
	match input.byte()? {
		0 => {
			while indexes.len() < count {
				let i = input.index(n)? as u32;
				let len = input.varint()?;
				if len == 0 || len > (count - indexes.len()) as u64 {
					return Err(CodecError::Corrupt("a dictionary run is empty or runs past the last row"));
				}
				indexes.extend(std::iter::repeat_n(i, len as usize));
			}
			input.end()?;
		}
		1 => {
			let width = width_for(n);
			let mut r = BitReader::new(input.rest());
			for _ in 0..count {
				let i = r.read(width)?;
				if i >= n as u64 {
					return Err(CodecError::Corrupt("an index points past the end of its table"));
				}
				indexes.push(i as u32);
			}
			r.end()?;
		}
		_ => return Err(CodecError::Corrupt("a dictionary names an index mode it does not have")),
	}
	Ok(Dict { entries, indexes })
}

pub fn encode_plain<T: AsRef<[u8]>>(values: &[T]) -> Result<Vec<u8>> {
	let mut out = Vec::new();
	put_count(&mut out, values.len())?;
	for v in values {
		put_varint(&mut out, v.as_ref().len() as u64);
		out.extend_from_slice(v.as_ref());
	}
	Ok(out)
}

pub fn decode_plain(data: &[u8]) -> Result<Vec<&[u8]>> {
	let mut input = Input::new(data);
	let count = input.count()?;
	let mut out = Vec::with_capacity(count);
	for _ in 0..count {
		let len = usize::try_from(input.varint()?).map_err(|_| CodecError::Truncated)?;
		out.push(input.bytes(len)?);
	}
	input.end()?;
	Ok(out)
}

#[cfg(test)]
mod tests {
	use super::super::testing::{survives, Rng};
	use super::super::MAX_VALUES;
	use super::*;

	#[test]
	fn plain_len_is_what_encode_plain_writes() {
		let mut rng = crate::codec::testing::Rng(5);
		for n in [0usize, 1, 2, 127, 128, 300, 5000] {
			let values: Vec<Vec<u8>> = (0..n).map(|_| vec![7u8; rng.below(300) as usize]).collect();
			assert_eq!(plain_len(&values), encode_plain(&values).unwrap().len(), "{n} values");
		}
	}

	fn round_trip(values: &[Vec<u8>]) -> usize {
		let e = encode(values).unwrap();
		let d = decode(&e).unwrap();
		assert_eq!(d.iter().collect::<Vec<_>>(), values.iter().map(|v| v.as_slice()).collect::<Vec<_>>());
		assert_eq!(d.entries.len(), distinct(values));
		let p = encode_plain(values).unwrap();
		assert_eq!(decode_plain(&p).unwrap(), values.iter().map(|v| v.as_slice()).collect::<Vec<_>>());
		e.len()
	}

	fn shapes() -> Vec<Vec<Vec<u8>>> {
		let mut rng = Rng(21);
		let hosts: Vec<Vec<u8>> = (0..10).map(|i| format!("host_{i}").into_bytes()).collect();
		vec![
			vec![],
			vec![vec![]],
			vec![vec![]; 300],
			vec![b"only".to_vec(); 1000],
			// sorted by host: long runs
			(0..1000).map(|i| hosts[i / 100].clone()).collect(),
			// interleaved: no runs, so bit-packed
			(0..1000).map(|_| hosts[rng.below(10) as usize].clone()).collect(),
			// every value distinct, and bytes that are not UTF-8
			(0..500).map(|i| vec![0xff, 0, i as u8, (i >> 8) as u8]).collect(),
			(0..200).map(|_| (0..rng.below(40)).map(|_| rng.next() as u8).collect()).collect(),
		]
	}

	#[test]
	fn every_shape_round_trips() {
		for s in shapes() {
			round_trip(&s);
		}
	}

	#[test]
	fn runs_and_packing_are_chosen_by_size() {
		let s = shapes();
		let sorted = encode(&s[4]).unwrap();
		assert!(sorted.len() < 120, "ten runs: {}", sorted.len());
		let interleaved = encode(&s[5]).unwrap();
		// ten entries of seven bytes, then 1,000 four-bit indexes
		assert!(interleaved.len() <= 3 + 1 + 80 + 1 + 500, "{}", interleaved.len());
	}

	#[test]
	fn one_value_repeated_decodes_to_references_not_copies() {
		let big = vec![7u8; 4096];
		let values = vec![&big[..]; MAX_VALUES];
		let e = encode(&values).unwrap();
		assert!(e.len() < 4096 + 20);
		let d = decode(&e).unwrap();
		assert_eq!(d.entries.len(), 1);
		assert!(d.iter().all(|v| v.as_ptr() == d.entries[0].as_ptr()));
	}

	#[test]
	fn damage_is_an_error_not_a_panic() {
		for s in shapes() {
			survives(&encode(&s).unwrap(), |b| decode(b).map(drop));
			survives(&encode_plain(&s).unwrap(), |b| decode_plain(b).map(drop));
		}
		// a run longer than the rows left
		let mut b = Vec::new();
		put_varint(&mut b, 3);
		put_varint(&mut b, 1);
		b.extend([1, b'a', 0, 0, 4]);
		assert!(decode(&b).is_err());
		// a length far beyond the input
		let mut b = Vec::new();
		put_varint(&mut b, 1);
		put_varint(&mut b, u64::MAX);
		assert!(decode_plain(&b).is_err());
	}
}
