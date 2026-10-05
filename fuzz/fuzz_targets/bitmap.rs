#![no_main]
//! Fuzzes the `bitmap` decoder: any bytes must give a value or an error, never a panic.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
	if let Ok(v) = snouttime_fuzz::codec::bitmap::decode(data) {
		assert_eq!(snouttime_fuzz::codec::bitmap::encode(&v).unwrap(), data, "a bitmap has one encoding");
	}
});
