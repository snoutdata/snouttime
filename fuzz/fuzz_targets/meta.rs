#![no_main]
//! Fuzzes the `meta` decoder: any bytes must give a value or an error, never a panic (R5).
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
	use snouttime_fuzz::format;
	if let Ok(m) = format::decode_meta(data, 1600) {
		let _ = format::decode_directory(data, &m);
	}
});
