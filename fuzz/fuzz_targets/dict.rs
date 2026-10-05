#![no_main]
//! Fuzzes the `dict` decoder: any bytes must give a value or an error, never a panic.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
	let _ = snouttime_fuzz::codec::dict::decode(data);
	let _ = snouttime_fuzz::codec::dict::decode_plain(data);
});
