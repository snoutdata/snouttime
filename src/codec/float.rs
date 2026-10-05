//! Floats: each value XORed with the one before, as in Gorilla (Pelkonen et al., VLDB 2015).
//!
//! Neighbouring readings of a gauge usually share their sign, exponent and leading mantissa
//! bits, so their XOR is mostly zeros and only the bits in between are written. Per value after
//! the first (which is written whole):
//!
//! * `0`: the same value as the previous one.
//! * `1 0` + the meaningful bits: the XOR's nonzero bits fit inside the previous window (at
//!   least as many leading and trailing zeros), so they are written at that window's position.
//! * `1 1` + 5 bits of leading zeros + 6 bits of length + the bits: a new window. A length of 64
//!   is written as 0, which a real window never has. Leading zeros are capped at 31, the most 5
//!   bits hold, as in the paper; the window then starts early and carries a few zero bits.
//!
//! This works on bit patterns, never on arithmetic, so every value round-trips exactly: NaN
//! with any payload, both zeros, infinities, subnormals. `encode_f32` and `decode_f32` carry a
//! `real` column's patterns in the low 32 bits.
//!
//! Layout: count (varint), then one bit stream, least significant bit first, zero-padded.

use super::{put_count, BitReader, BitWriter, CodecError, Input, Result};

pub fn encode_f64(values: &[f64]) -> Result<Vec<u8>> {
	encode_bits(&values.iter().map(|v| v.to_bits()).collect::<Vec<_>>())
}

pub fn decode_f64(data: &[u8]) -> Result<Vec<f64>> {
	Ok(decode_bits(data)?.into_iter().map(f64::from_bits).collect())
}

pub fn encode_f32(values: &[f32]) -> Result<Vec<u8>> {
	encode_bits(&values.iter().map(|v| v.to_bits() as u64).collect::<Vec<_>>())
}

pub fn decode_f32(data: &[u8]) -> Result<Vec<f32>> {
	decode_bits(data)?
		.into_iter()
		.map(|b| u32::try_from(b).map(f32::from_bits).map_err(|_| CodecError::Corrupt("a real value has more than 32 bits")))
		.collect()
}

pub fn encode_bits(values: &[u64]) -> Result<Vec<u8>> {
	let mut out = Vec::new();
	put_count(&mut out, values.len())?;
	let Some((&first, rest)) = values.split_first() else {
		return Ok(out);
	};
	let mut w = BitWriter::new();
	w.write(first, 64);
	let mut prev = first;
	// (leading, trailing) of the window in use; none before the first nonzero XOR
	let mut window: Option<(u32, u32)> = None;
	for &v in rest {
		let x = v ^ prev;
		prev = v;
		if x == 0 {
			w.write(0, 1);
			continue;
		}
		let lead = x.leading_zeros().min(31);
		let trail = x.trailing_zeros();
		// the control bits in one write: bits go out lowest first, so "1 then 0" is 0b01 and
		// "1, 1, lead, length" is 0b11 | lead << 2 | length << 7
		match window {
			Some((l, t)) if lead >= l && trail >= t => {
				w.write(0b01, 2);
				w.write(x >> t, 64 - l - t);
			}
			_ => {
				let len = 64 - lead - trail;
				w.write(0b11 | (lead as u64) << 2 | ((len % 64) as u64) << 7, 13);
				w.write(x >> trail, len);
				window = Some((lead, trail));
			}
		}
	}
	out.extend(w.finish());
	Ok(out)
}

pub fn decode_bits(data: &[u8]) -> Result<Vec<u64>> {
	let mut out = Vec::new();
	decode_bits_into(data, &mut out)?;
	Ok(out)
}

/// [`decode_bits`], appending to `out`: a paged chunk's pages go into one vector. On an error
/// `out` may hold part of the block.
pub fn decode_bits_into(data: &[u8], out: &mut Vec<u64>) -> Result<()> {
	let mut input = Input::new(data);
	let count = input.count()?;
	if count == 0 {
		input.end()?;
		return Ok(());
	}
	let mut r = BitReader::new(input.rest());
	out.reserve(count);
	let end = out.len() + count;
	let mut prev = r.read(64)?;
	out.push(prev);
	let mut window: Option<(u32, u32)> = None;
	while out.len() < end {
		if r.bit()? {
			let (lead, trail) = if r.bit()? {
				let lead = r.read(5)? as u32;
				let len = match r.read(6)? as u32 {
					0 => 64,
					n => n,
				};
				if lead + len > 64 {
					return Err(CodecError::Corrupt("a float window runs past 64 bits"));
				}
				let bits = r.read(len)?;
				// what the encoder writes for a new window is canonical: its last bit is set,
				// and so is its first unless the leading zeros were capped
				if bits & 1 == 0 || (lead < 31 && bits >> (len - 1) == 0) {
					return Err(CodecError::Corrupt("a float window is wider than its bits"));
				}
				let trail = 64 - lead - len;
				window = Some((lead, trail));
				prev ^= bits << trail;
				out.push(prev);
				continue;
			} else {
				window.ok_or(CodecError::Corrupt("a float reuses a window before one was set"))?
			};
			let bits = r.read(64 - lead - trail)?;
			// a reused window's bits may start with zeros, but a zero XOR is written as `0`
			if bits == 0 {
				return Err(CodecError::Corrupt("a float XOR is zero where a repeat was expected"));
			}
			prev ^= bits << trail;
		}
		out.push(prev);
	}
	r.end()?;
	Ok(())
}

#[cfg(test)]
mod tests {
	use super::super::testing::{survives, Rng};
	use super::super::MAX_VALUES;
	use super::*;

	fn round_trip(values: &[u64]) -> usize {
		let e = encode_bits(values).unwrap();
		assert_eq!(decode_bits(&e).unwrap(), values);
		e.len()
	}

	fn shapes() -> Vec<Vec<u64>> {
		let mut rng = Rng(11);
		let specials = [
			0.0f64, -0.0, 1.0, -1.0, f64::INFINITY, f64::NEG_INFINITY, f64::NAN, f64::MIN_POSITIVE,
			f64::MIN_POSITIVE / 3.0, f64::MAX, f64::MIN, f64::EPSILON,
		];
		let mut gauge = 50.0f64;
		vec![
			vec![],
			vec![0],
			vec![u64::MAX],
			specials.iter().map(|v| v.to_bits()).collect(),
			// NaNs with different payloads and signs, and signalling ones
			vec![0x7ff8_0000_0000_0001, 0xfff8_0000_0000_0000, 0x7ff0_0000_0000_0001, 0x7fff_ffff_ffff_ffff, 1, 0],
			(0..1000).map(|_| rng.next()).collect(),
			(0..1000).map(|_| {
				gauge += (rng.below(1000) as f64 - 500.0) / 1000.0;
				(gauge * 100.0).round() / 100.0
			}).map(f64::to_bits).collect(),
			vec![7.25f64.to_bits(); 1000],
			// sign flips: the XOR's top bit is set
			(0..500).map(|i| if i % 2 == 0 { 1.5f64 } else { -1.5 }.to_bits()).collect(),
			// XORs with more than 31 leading zeros: the capped window
			(0..500).map(|i| 1_000_000u64 + (i % 7)).collect(),
			(0..MAX_VALUES as u64).map(|i| (i as f64).to_bits()).collect(),
		]
	}

	#[test]
	fn every_shape_round_trips_bit_for_bit() {
		for s in shapes() {
			round_trip(&s);
		}
		let mut rng = Rng(12);
		for _ in 0..300 {
			let n = rng.below(600) as usize;
			let keep = rng.below(64);
			let base = rng.next();
			let v: Vec<u64> = (0..n).map(|_| base ^ (rng.next() >> keep) << rng.below(64 - keep + 1).min(63)).collect();
			round_trip(&v);
		}
	}

	#[test]
	fn the_float_wrappers_keep_every_pattern() {
		let v = [f64::NAN, -0.0, 0.0, f64::from_bits(0x7ff0_0000_dead_beef), 3.5];
		let d = decode_f64(&encode_f64(&v).unwrap()).unwrap();
		assert!(v.iter().zip(&d).all(|(a, b)| a.to_bits() == b.to_bits()));
		let v = [f32::NAN, -0.0, 0.0, f32::from_bits(0x7f80_beef), 3.5, f32::MAX, f32::MIN_POSITIVE];
		let d = decode_f32(&encode_f32(&v).unwrap()).unwrap();
		assert!(v.iter().zip(&d).all(|(a, b)| a.to_bits() == b.to_bits()));
		assert!(decode_f32(&encode_bits(&[1 << 40]).unwrap()).is_err());
	}

	#[test]
	fn repeats_and_slow_gauges_are_small() {
		assert!(round_trip(&vec![7.25f64.to_bits(); 1000]) <= 2 + 8 + 125);
		let mut rng = Rng(13);
		let mut g = 20.0f64;
		let gauge: Vec<u64> = (0..1000).map(|_| {
			if rng.below(4) == 0 {
				g += 0.5;
			}
			g.to_bits()
		}).collect();
		assert!(round_trip(&gauge) < 8 * 1000 / 4, "{}", encode_bits(&gauge).unwrap().len());
	}

	#[test]
	fn damage_is_an_error_not_a_panic() {
		for s in shapes().into_iter().filter(|s| s.len() < 2000) {
			survives(&encode_bits(&s).unwrap(), decode_bits);
		}
		let mut long = encode_bits(&[1, 2, 3]).unwrap();
		long.push(0);
		assert!(decode_bits(&long).is_err(), "trailing bytes");
	}
}
