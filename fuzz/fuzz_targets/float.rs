#![no_main]
//! Fuzzes the `float` decoder: any bytes must give a value or an error, never a panic (R5).
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
	if let Ok(v) = snouttime_fuzz::codec::float::decode_bits(data) {
		let again = snouttime_fuzz::codec::float::encode_bits(&v).unwrap();
		assert_eq!(snouttime_fuzz::codec::float::decode_bits(&again).unwrap(), v);
	}
});
