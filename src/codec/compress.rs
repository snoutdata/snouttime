//! The frame one encoded column block is stored in: the encodings above remove what is
//! predictable about a column, and a general-purpose compressor then takes what is left.
//! LZ4 decompresses fastest; zstd is smaller. The choice is the series table's `codec`.
//!
//! Layout: the codec (one byte: 0 none, 1 LZ4, 2 zstd), the uncompressed length (varint, at
//! most [`MAX_RAW_BYTES`]), then the payload. The length is checked before anything is
//! allocated, and a payload that decompresses to any other length is corrupt, so a bad frame
//! can never make a decoder allocate more than the limit.

use super::{put_varint, CodecError, Input, Result};

/// The largest block, uncompressed. A format limit, chosen rather than measured: it bounds what a
/// corrupt length can make a decoder allocate, and the block writer splits a block before it.
pub const MAX_RAW_BYTES: usize = 64 << 20;

/// zstd's level. Chosen, not measured yet.
pub const ZSTD_LEVEL: i32 = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Codec {
	None = 0,
	Lz4 = 1,
	Zstd = 2,
}

pub fn compress(codec: Codec, raw: &[u8]) -> Result<Vec<u8>> {
	if raw.len() > MAX_RAW_BYTES {
		return Err(CodecError::Corrupt("a column block is larger than a block may be"));
	}
	let mut out = vec![codec as u8];
	put_varint(&mut out, raw.len() as u64);
	match codec {
		Codec::None => out.extend_from_slice(raw),
		Codec::Lz4 => out.extend(lz4_flex::block::compress(raw)),
		Codec::Zstd => out.extend(
			zstd::bulk::compress(raw, ZSTD_LEVEL).map_err(|_| CodecError::Corrupt("zstd could not compress a block"))?,
		),
	}
	Ok(out)
}

pub fn decompress(frame: &[u8]) -> Result<Vec<u8>> {
	let mut input = Input::new(frame);
	let codec = input.byte()?;
	let len = input.varint()?;
	if len > MAX_RAW_BYTES as u64 {
		return Err(CodecError::Corrupt("a block says it is larger than a block may be"));
	}
	let len = len as usize;
	let payload = input.rest();
	let raw = match codec {
		0 => payload.to_vec(),
		1 => lz4_flex::block::decompress(payload, len).map_err(|_| CodecError::Corrupt("an LZ4 block does not decompress"))?,
		2 => zstd::bulk::decompress(payload, len).map_err(|_| CodecError::Corrupt("a zstd block does not decompress"))?,
		_ => return Err(CodecError::Corrupt("a block names a compression it does not have")),
	};
	if raw.len() != len {
		return Err(CodecError::Corrupt("a block decompresses to a different length than it says"));
	}
	Ok(raw)
}

#[cfg(test)]
mod tests {
	use super::super::testing::{survives, Rng};
	use super::*;

	fn samples() -> Vec<Vec<u8>> {
		let mut rng = Rng(41);
		vec![
			vec![],
			vec![0],
			vec![0; 100_000],
			(0..10_000).map(|_| rng.next() as u8).collect(),
			(0..50_000u32).flat_map(|i| (i % 97).to_le_bytes()).collect(),
		]
	}

	#[test]
	fn every_codec_round_trips() {
		for raw in samples() {
			for codec in [Codec::None, Codec::Lz4, Codec::Zstd] {
				let f = compress(codec, &raw).unwrap();
				assert_eq!(decompress(&f).unwrap(), raw, "{codec:?}");
			}
		}
		let zeros = &samples()[2];
		assert!(compress(Codec::Lz4, zeros).unwrap().len() < 1000);
		assert!(compress(Codec::Zstd, zeros).unwrap().len() < 100);
	}

	#[test]
	fn damage_is_an_error_not_a_panic() {
		for raw in samples().into_iter().filter(|r| r.len() <= 10_000) {
			for codec in [Codec::None, Codec::Lz4, Codec::Zstd] {
				survives(&compress(codec, &raw).unwrap(), decompress);
			}
		}
		// a length above the limit is refused before anything is allocated
		let mut f = vec![1];
		put_varint(&mut f, u64::MAX);
		assert!(decompress(&f).is_err());
		// a frame that says 10 bytes and holds 3
		assert!(decompress(&[0, 10, 1, 2, 3]).is_err());
	}
}
