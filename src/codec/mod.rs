//! Column encodings for sealed partitions. Pure Rust: nothing in this
//! module or below it touches Postgres, so it is unit-tested and fuzzed without a database.
//!
//! * [`int`]: integers and timestamps, as the value, its delta or its delta-of-delta (whichever
//!   is smallest), bit-packed in groups of 128 at each group's own width.
//! * [`float`]: floats as the XOR with the previous value (Gorilla, Pelkonen et al., VLDB 2015).
//! * [`dict`]: byte strings as a dictionary plus run-length or bit-packed indexes, and
//!   [`dict::encode_plain`] for anything with too many distinct values for a dictionary to pay.
//! * [`bitmap`]: booleans and null maps.
//! * [`compress`]: the LZ4 or zstd frame one column block is wrapped in.
//!
//! The rule for every decoder here: the bytes are untrusted (a torn page, a bad disk, a hostile
//! dump), so a decoder returns [`CodecError`] for anything it cannot make sense of. It never
//! panics, never reads out of bounds, and never allocates more than the input justifies: a
//! count is refused above [`MAX_VALUES`] before anything is allocated for it, and a decoder
//! that could expand a few bytes into many (a dictionary) hands back borrowed slices of its
//! input rather than copies. Every encoding is also strict: bytes left over after the last
//! value, or padding bits that are not zero, are corruption too, which is what lets a fuzzer
//! tell a decoder that ignores damage from one that notices it.

pub mod bitmap;
pub mod compress;
pub mod dict;
pub mod float;
pub mod int;

use std::fmt;

/// The most values one encoded column chunk may hold. Decoders refuse a larger count before
/// allocating anything for it, so a corrupt count costs an error, not memory. A format limit,
/// chosen, not measured: a block of a sealed partition holds far fewer rows.
pub const MAX_VALUES: usize = 65_536;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodecError {
	/// The input ended before the encoding said it would.
	Truncated,
	/// The input is not something the encoder could have produced; the text says what.
	Corrupt(&'static str),
	/// A count above [`MAX_VALUES`], or an encoder asked to write more than that.
	TooMany(u64),
}

impl fmt::Display for CodecError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			CodecError::Truncated => write!(f, "the data ends before its encoding says it does"),
			CodecError::Corrupt(what) => write!(f, "{what}"),
			CodecError::TooMany(n) => write!(f, "{n} values, more than the {MAX_VALUES} one block may hold"),
		}
	}
}

impl std::error::Error for CodecError {}

pub type Result<T> = std::result::Result<T, CodecError>;

/// Maps signed to unsigned so small magnitudes of either sign are small numbers.
fn zigzag(v: i64) -> u64 {
	((v << 1) ^ (v >> 63)) as u64
}

fn unzigzag(u: u64) -> i64 {
	((u >> 1) as i64) ^ -((u & 1) as i64)
}

/// LEB128, least significant group first.
fn put_varint(out: &mut Vec<u8>, mut v: u64) {
	while v >= 0x80 {
		out.push((v as u8) | 0x80);
		v >>= 7;
	}
	out.push(v as u8);
}

fn put_count(out: &mut Vec<u8>, n: usize) -> Result<()> {
	if n > MAX_VALUES {
		return Err(CodecError::TooMany(n as u64));
	}
	put_varint(out, n as u64);
	Ok(())
}

/// A cursor over untrusted bytes. Every read is checked.
struct Input<'a> {
	data: &'a [u8],
	pos: usize,
}

impl<'a> Input<'a> {
	fn new(data: &'a [u8]) -> Input<'a> {
		Input { data, pos: 0 }
	}

	fn remaining(&self) -> usize {
		self.data.len() - self.pos
	}

	fn byte(&mut self) -> Result<u8> {
		let b = *self.data.get(self.pos).ok_or(CodecError::Truncated)?;
		self.pos += 1;
		Ok(b)
	}

	fn bytes(&mut self, n: usize) -> Result<&'a [u8]> {
		if n > self.remaining() {
			return Err(CodecError::Truncated);
		}
		let s = &self.data[self.pos..self.pos + n];
		self.pos += n;
		Ok(s)
	}

	fn rest(&mut self) -> &'a [u8] {
		let s = &self.data[self.pos..];
		self.pos = self.data.len();
		s
	}

	/// A LEB128 value that fits in 64 bits, in its shortest form: an overlong encoding
	/// (a trailing zero group) is corrupt, so every value has exactly one encoding.
	fn varint(&mut self) -> Result<u64> {
		let mut v = 0u64;
		for i in 0..10 {
			let b = self.byte()?;
			let group = (b & 0x7f) as u64;
			if i == 9 && group > 1 {
				return Err(CodecError::Corrupt("a variable-length integer overflows 64 bits"));
			}
			v |= group << (7 * i);
			if b & 0x80 == 0 {
				if i > 0 && group == 0 {
					return Err(CodecError::Corrupt("a variable-length integer is not in its shortest form"));
				}
				return Ok(v);
			}
		}
		Err(CodecError::Corrupt("a variable-length integer is longer than ten bytes"))
	}

	fn count(&mut self) -> Result<usize> {
		let n = self.varint()?;
		if n > MAX_VALUES as u64 {
			return Err(CodecError::TooMany(n));
		}
		Ok(n as usize)
	}

	fn index(&mut self, below: usize) -> Result<usize> {
		let v = self.varint()?;
		if v >= below as u64 {
			return Err(CodecError::Corrupt("an index points past the end of its table"));
		}
		Ok(v as usize)
	}

	fn end(&self) -> Result<()> {
		if self.remaining() != 0 {
			return Err(CodecError::Corrupt("bytes follow the last value"));
		}
		Ok(())
	}
}

/// Bits packed least significant first into bytes.
struct BitWriter {
	out: Vec<u8>,
	acc: u128,
	n: u32,
}

impl BitWriter {
	fn new() -> BitWriter {
		BitWriter { out: Vec::new(), acc: 0, n: 0 }
	}

	/// Appends to `out` (a writer per group of a block wrote a new vector per group).
	fn after(out: Vec<u8>) -> BitWriter {
		BitWriter { out, acc: 0, n: 0 }
	}

	/// The low `width` bits of `v`, `width` in 0..=64. Eight bytes go out at a time: one byte
	/// at a time made encoding a time column 34 ns a value (2026-09-23).
	#[inline]
	fn write(&mut self, v: u64, width: u32) {
		if width == 0 {
			return;
		}
		let v = if width == 64 { v } else { v & ((1u64 << width) - 1) };
		self.acc |= (v as u128) << self.n;
		self.n += width;
		if self.n >= 64 {
			self.out.extend_from_slice(&(self.acc as u64).to_le_bytes());
			self.acc >>= 64;
			self.n -= 64;
		}
	}

	/// The bytes, the last one padded with zero bits.
	fn finish(mut self) -> Vec<u8> {
		while self.n > 0 {
			self.out.push(self.acc as u8);
			self.acc >>= 8;
			self.n = self.n.saturating_sub(8);
		}
		self.out
	}
}

struct BitReader<'a> {
	data: &'a [u8],
	pos: usize,
}

impl<'a> BitReader<'a> {
	fn new(data: &'a [u8]) -> BitReader<'a> {
		BitReader { data, pos: 0 }
	}

	fn read(&mut self, width: u32) -> Result<u64> {
		if width == 0 {
			return Ok(0);
		}
		let end = self.pos.checked_add(width as usize).ok_or(CodecError::Truncated)?;
		if end > self.data.len() * 8 {
			return Err(CodecError::Truncated);
		}
		let first = self.pos / 8;
		// A value spans at most nine bytes (seven bits of offset plus 64). Away from the end
		// of the stream they are one unaligned 16-byte load; near it, byte by byte.
		let acc = match self.data.get(first..first + 16) {
			Some(w) => u128::from_le_bytes(w.try_into().unwrap()),
			None => {
				let mut acc = 0u128;
				for (i, &b) in self.data[first..self.data.len().min(first + 9)].iter().enumerate() {
					acc |= (b as u128) << (8 * i);
				}
				acc
			}
		};
		let v = (acc >> (self.pos % 8)) as u64;
		self.pos = end;
		Ok(if width == 64 { v } else { v & ((1u64 << width) - 1) })
	}

	fn bit(&mut self) -> Result<bool> {
		let b = *self.data.get(self.pos / 8).ok_or(CodecError::Truncated)?;
		let v = (b >> (self.pos % 8)) & 1 == 1;
		self.pos += 1;
		Ok(v)
	}

	/// What was written was whole bytes with zero padding: anything else is corruption.
	fn end(&self) -> Result<()> {
		let used = self.pos.div_ceil(8);
		if used != self.data.len() {
			return Err(CodecError::Corrupt("bytes follow the last value"));
		}
		if self.pos % 8 != 0 && self.data[used - 1] >> (self.pos % 8) != 0 {
			return Err(CodecError::Corrupt("the padding after the last value is not zero"));
		}
		Ok(())
	}
}

/// Deterministic inputs for the tests of every encoding, with no dependency: splitmix64.
#[cfg(test)]
pub(crate) mod testing {
	pub struct Rng(pub u64);

	impl Rng {
		pub fn next(&mut self) -> u64 {
			self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
			let mut x = self.0;
			x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
			x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
			x ^ (x >> 31)
		}

		pub fn below(&mut self, n: u64) -> u64 {
			self.next() % n.max(1)
		}
	}

	/// Every decoder must survive these without panicking: random bytes, every truncation of a
	/// valid encoding, and every single-bit flip of it. What it returns is not checked, only
	/// that it returns.
	pub fn survives<T>(valid: &[u8], decode: impl Fn(&[u8]) -> super::Result<T>) {
		for n in 0..valid.len() {
			let _ = decode(&valid[..n]);
		}
		let mut flipped = valid.to_vec();
		for i in 0..valid.len() * 8 {
			flipped[i / 8] ^= 1 << (i % 8);
			let _ = decode(&flipped);
			flipped[i / 8] ^= 1 << (i % 8);
		}
		let mut rng = Rng(valid.len() as u64);
		for len in [0usize, 1, 2, 3, 5, 8, 17, 64, 300] {
			for _ in 0..200 {
				let junk: Vec<u8> = (0..len).map(|_| rng.next() as u8).collect();
				let _ = decode(&junk);
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn zigzag_round_trips_the_extremes() {
		for v in [0, 1, -1, 2, -2, i64::MAX, i64::MIN, i64::MAX - 1, i64::MIN + 1] {
			assert_eq!(unzigzag(zigzag(v)), v);
		}
		assert_eq!(zigzag(-1), 1);
		assert_eq!(zigzag(1), 2);
	}

	#[test]
	fn varints_round_trip_and_refuse_overlong_forms() {
		let mut rng = testing::Rng(1);
		let mut values = vec![0, 1, 127, 128, 16383, 16384, u64::MAX, u64::MAX - 1];
		values.extend((0..1000).map(|_| rng.next() >> rng.below(64)));
		for v in values {
			let mut b = Vec::new();
			put_varint(&mut b, v);
			let mut i = Input::new(&b);
			assert_eq!(i.varint(), Ok(v));
			i.end().unwrap();
		}
		assert!(Input::new(&[0x80, 0x00]).varint().is_err(), "overlong zero");
		assert!(Input::new(&[0xff; 10]).varint().is_err(), "past 64 bits");
		assert!(Input::new(&[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x02]).varint().is_err());
		assert_eq!(Input::new(&[0x80]).varint(), Err(CodecError::Truncated));
	}

	#[test]
	fn bits_round_trip_at_every_width() {
		let mut rng = testing::Rng(2);
		let mut w = BitWriter::new();
		let mut written = Vec::new();
		for _ in 0..5000 {
			let width = rng.below(65) as u32;
			let v = rng.next();
			w.write(v, width);
			written.push((width, if width == 64 { v } else { v & ((1u64 << width) - 1) }));
		}
		let bytes = w.finish();
		let mut r = BitReader::new(&bytes);
		for (width, v) in written {
			assert_eq!(r.read(width), Ok(v));
		}
		r.end().unwrap();
		assert_eq!(r.read(8), Err(CodecError::Truncated), "less than a byte of padding is left");
	}

	#[test]
	fn bit_padding_must_be_zero() {
		let mut w = BitWriter::new();
		w.write(1, 3);
		let mut bytes = w.finish();
		let mut r = BitReader::new(&bytes);
		r.read(3).unwrap();
		r.end().unwrap();
		bytes[0] |= 0x80;
		let mut r = BitReader::new(&bytes);
		r.read(3).unwrap();
		assert!(r.end().is_err());
	}
}
