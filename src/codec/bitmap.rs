//! Booleans, and which rows of a column are null.
//!
//! Layout: count (varint), then a mode byte. Mode 0: every value false. Mode 1: every value
//! true. Mode 2: one bit per value, least significant first, zero-padded; used only when the
//! values are mixed, so each bitmap has exactly one encoding.

use super::{put_count, BitReader, BitWriter, CodecError, Input, Result};

pub fn encode(values: &[bool]) -> Result<Vec<u8>> {
	let mut out = Vec::new();
	put_count(&mut out, values.len())?;
	if values.iter().all(|&v| !v) {
		out.push(0);
	} else if values.iter().all(|&v| v) {
		out.push(1);
	} else {
		out.push(2);
		let mut w = BitWriter::new();
		for &v in values {
			w.write(v as u64, 1);
		}
		out.extend(w.finish());
	}
	Ok(out)
}

pub fn decode(data: &[u8]) -> Result<Vec<bool>> {
	let mut input = Input::new(data);
	let count = input.count()?;
	let out = match input.byte()? {
		0 => vec![false; count],
		// an empty bitmap is written as mode 0; mode 1 for none is a second encoding of it
		// (found by the bitmap fuzz target, 2026-09-23)
		1 if count == 0 => return Err(CodecError::Corrupt("an empty bitmap is written out as all true")),
		1 => vec![true; count],
		2 => {
			let mut r = BitReader::new(input.rest());
			let v = (0..count).map(|_| r.bit()).collect::<Result<Vec<bool>>>()?;
			r.end()?;
			if v.iter().all(|&b| b) || v.iter().all(|&b| !b) {
				return Err(CodecError::Corrupt("a bitmap is written out where one byte says it"));
			}
			v
		}
		_ => return Err(CodecError::Corrupt("a bitmap names a mode it does not have")),
	};
	input.end()?;
	Ok(out)
}

#[cfg(test)]
mod tests {
	use super::super::testing::{survives, Rng};
	use super::super::MAX_VALUES;
	use super::*;

	#[test]
	fn every_length_and_mix_round_trips() {
		let mut rng = Rng(31);
		for n in (0..70).chain([127, 128, 129, 1000, MAX_VALUES]) {
			for fill in [0u64, 1, 2, 50, 99, 100] {
				let v: Vec<bool> = (0..n).map(|_| rng.below(100) < fill).collect();
				let e = encode(&v).unwrap();
				assert_eq!(decode(&e).unwrap(), v);
				if n < 300 {
					survives(&e, decode);
				}
			}
		}
	}

	#[test]
	fn uniform_bitmaps_are_one_byte_and_mixed_ones_a_bit_per_value() {
		assert_eq!(encode(&vec![false; 1000]).unwrap().len(), 3);
		assert_eq!(encode(&vec![true; 1000]).unwrap().len(), 3);
		let mixed: Vec<bool> = (0..1000).map(|i| i % 3 == 0).collect();
		assert_eq!(encode(&mixed).unwrap().len(), 3 + 125);
		assert!(decode(&[1, 2, 1]).is_err(), "all true written out");
		assert!(decode(&[1, 3]).is_err());
		assert!(decode(&[0, 1]).is_err(), "an empty bitmap has one encoding");
		assert_eq!(decode(&[0, 0]), Ok(vec![]));
	}
}
