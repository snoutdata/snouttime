//! Integers and timestamps: the values, their deltas or their deltas-of-deltas, whichever
//! is smallest, bit-packed.
//!
//! A timestamp column taken at a steady interval has a constant delta, so its delta-of-delta is
//! zero everywhere and costs nothing past the first two values: that is the observation behind
//! Gorilla's timestamp encoding (Pelkonen et al., VLDB 2015). Gorilla writes each delta-of-delta
//! with its own prefix code because it appends one point at a time; a sealed block is encoded
//! whole, so here the differences are bit-packed instead, in groups of [`GROUP`] at the width of
//! the group's largest value, which decodes without a branch per value. The same scheme serves
//! other integers, which is why the order is chosen per chunk: a counter is small as deltas, a
//! gauge that wanders is smallest as itself, and delta-of-delta would double its range.
//!
//! Arithmetic wraps, so every `i64`, including a jump from `i64::MIN` to `i64::MAX`, round-trips.
//!
//! Layout: count (varint), order (one byte, 0..=2), then the first `min(order, count)` values of
//! the successive difference sequences (zigzag varints), then the remaining differences of the
//! last order, zigzagged, in groups of [`GROUP`]: a width byte (0..=64) and the group's values
//! at that width, least significant bit first, zero-padded to a byte.

use super::{put_count, put_varint, unzigzag, zigzag, BitReader, BitWriter, CodecError, Input, Result};

/// Values per bit-packed group. Chosen, not measured.
pub const GROUP: usize = 128;

/// Encodes at whichever order of difference is smallest.
pub fn encode(values: &[i64]) -> Result<Vec<u8>> {
	// Each order's size follows from its groups' widths, so only the smallest is bit-packed
	// (all three were, to compare their lengths, until 2026-09-23). Ties go to the lower order,
	// as they did.
	let mut best = (0u8, usize::MAX);
	let mut cur = values.to_vec();
	for order in 0..=2u8 {
		if order > 0 {
			if cur.is_empty() {
				break;
			}
			cur = cur.windows(2).map(|w| w[1].wrapping_sub(w[0])).collect();
		}
		let size: usize = cur
			.chunks(GROUP)
			.map(|g| {
				let width = 64 - g.iter().fold(0, |a, &v| a | zigzag(v)).leading_zeros() as usize;
				1 + (g.len() * width).div_ceil(8)
			})
			.sum::<usize>()
			// the seeds, as many varints as the order; about their width
			+ order as usize * 5;
		if size < best.1 {
			best = (order, size);
		}
	}
	encode_order(values, best.0)
}

/// Encodes at one order: 0 is the values, 1 their deltas, 2 their deltas-of-deltas.
pub fn encode_order(values: &[i64], order: u8) -> Result<Vec<u8>> {
	assert!(order <= 2, "order is 0, 1 or 2");
	let mut out = Vec::new();
	put_count(&mut out, values.len())?;
	out.push(order);
	let mut cur = values.to_vec();
	for _ in 0..order {
		if cur.is_empty() {
			break;
		}
		put_varint(&mut out, zigzag(cur[0]));
		cur = cur.windows(2).map(|w| w[1].wrapping_sub(w[0])).collect();
	}
	out.reserve(cur.len() * 2);
	for group in cur.chunks(GROUP) {
		let width = 64 - group.iter().fold(0, |a, &v| a | zigzag(v)).leading_zeros();
		out.push(width as u8);
		let mut w = BitWriter::after(out);
		for &v in group {
			w.write(zigzag(v), width);
		}
		out = w.finish();
	}
	Ok(out)
}

pub fn decode(data: &[u8]) -> Result<Vec<i64>> {
	let mut out = Vec::new();
	decode_into(data, &mut out)?;
	Ok(out)
}

/// [`decode`], appending to `out`: a paged chunk's pages go into one vector. The seeds and the
/// differences are written where the values go and summed in place, one pass per order, so
/// nothing else is allocated. On an error `out` may hold part of the block.
pub fn decode_into(data: &[u8], out: &mut Vec<i64>) -> Result<()> {
	let mut input = Input::new(data);
	let count = input.count()?;
	let order = input.byte()?;
	if order > 2 {
		return Err(CodecError::Corrupt("an integer block names an order of difference above 2"));
	}
	let base = out.len();
	out.reserve(count);
	let seeds = (order as usize).min(count);
	for _ in 0..seeds {
		out.push(unzigzag(input.varint()?));
	}
	let mut left = count - seeds;
	while left > 0 {
		let n = left.min(GROUP);
		let width = input.byte()? as u32;
		if width > 64 {
			return Err(CodecError::Corrupt("an integer group is wider than 64 bits"));
		}
		let bytes = input.bytes((n * width as usize).div_ceil(8))?;
		let mut r = BitReader::new(bytes);
		for _ in 0..n {
			out.push(unzigzag(r.read(width)?));
		}
		r.end()?;
		left -= n;
	}
	input.end()?;
	// [seed 0, seed 1, d2 ...]: summing from seed k onwards undoes the difference of order k + 1,
	// last order first
	for k in (0..seeds).rev() {
		let v = &mut out[base + k..];
		let mut acc = v[0];
		for x in v[1..].iter_mut() {
			acc = acc.wrapping_add(*x);
			*x = acc;
		}
	}
	Ok(())
}

#[cfg(test)]
mod tests {
	use super::super::testing::{survives, Rng};
	use super::super::{CodecError, MAX_VALUES};
	use super::*;

	fn round_trip(values: &[i64]) -> usize {
		for order in 0..=2 {
			let e = encode_order(values, order).unwrap();
			assert_eq!(decode(&e).unwrap(), values, "order {order}, {} values", values.len());
		}
		let e = encode(values).unwrap();
		assert_eq!(decode(&e).unwrap(), values);
		e.len()
	}

	fn shapes() -> Vec<Vec<i64>> {
		let mut rng = Rng(7);
		let t0 = 1_798_761_600_000_000i64; // 2027-01-01 in Postgres microseconds since 2000
		vec![
			vec![],
			vec![42],
			vec![i64::MIN],
			vec![i64::MAX, i64::MIN],
			vec![i64::MIN, i64::MAX, i64::MIN, i64::MAX, 0, -1, 1],
			(0..1000).map(|i| t0 + i * 10_000_000).collect(),
			(0..1000).map(|i| t0 + i * 10_000_000 + rng.below(1000) as i64 - 500).collect(),
			(0..777).map(|_| rng.next() as i64).collect(),
			(0..1000).map(|i| i * i * i).collect(),
			(0..129).map(|i| if i == 64 { i64::MAX } else { 5 }).collect(),
			vec![0; MAX_VALUES],
			(0..MAX_VALUES as i64).collect(),
		]
	}

	#[test]
	fn every_shape_round_trips_at_every_order() {
		for s in shapes() {
			round_trip(&s);
		}
		let mut rng = Rng(8);
		for _ in 0..300 {
			let n = rng.below(700) as usize;
			// signed values of 1..=64 bits
			let shift = rng.below(64) as u32;
			let v: Vec<i64> = (0..n).map(|_| (rng.next() as i64) >> shift).collect();
			round_trip(&v);
		}
	}

	#[test]
	fn a_steady_clock_costs_almost_nothing() {
		let t: Vec<i64> = (0..1000).map(|i| 1_000_000_000 + i * 10_000_000).collect();
		// count, order, two seeds, then eight groups of zero width
		assert!(round_trip(&t) <= 20, "{}", encode(&t).unwrap().len());
	}

	#[test]
	fn the_smallest_order_is_chosen() {
		let gauge: Vec<i64> = {
			let mut rng = Rng(9);
			(0..1000).map(|_| rng.below(100) as i64).collect()
		};
		assert_eq!(encode(&gauge).unwrap()[2], 0, "a noisy gauge is smallest as itself");
		let counter: Vec<i64> = {
			let mut rng = Rng(10);
			let mut c = 0;
			(0..1000).map(|_| {
				c += rng.below(100) as i64;
				c
			}).collect()
		};
		assert_eq!(encode(&counter).unwrap()[2], 1, "a counter is smallest as deltas");
	}

	#[test]
	fn too_many_values_are_refused_both_ways() {
		assert_eq!(encode(&vec![0; MAX_VALUES + 1]), Err(CodecError::TooMany(MAX_VALUES as u64 + 1)));
		// a claimed count far above the limit, in three bytes: refused before any allocation
		let mut b = Vec::new();
		put_varint(&mut b, u64::MAX);
		assert!(matches!(decode(&b), Err(CodecError::TooMany(_))));
	}

	#[test]
	fn damage_is_an_error_not_a_panic() {
		for s in shapes().into_iter().filter(|s| s.len() < 2000) {
			survives(&encode(&s).unwrap(), decode);
		}
		let e = encode(&[1, 2, 3]).unwrap();
		let mut long = e.clone();
		long.push(0);
		assert!(decode(&long).is_err(), "trailing bytes");
		let mut wide = encode_order(&[1, 2, 3], 0).unwrap();
		wide[2] = 65;
		assert!(decode(&wide).is_err(), "width above 64");
	}
}
