#![no_main]
//! Fuzzes the `compress` decoder: any bytes must give a value or an error, never a panic (R5).
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
	let _ = snouttime_fuzz::codec::compress::decompress(data);
});
