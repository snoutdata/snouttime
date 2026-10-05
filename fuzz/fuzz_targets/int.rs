#![no_main]
//! Fuzzes the `int` decoder: any bytes must give a value or an error, never a panic.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
	if let Ok(v) = snouttime_fuzz::codec::int::decode(data) {
		// whatever decodes must encode again and decode to the same values
		let again = snouttime_fuzz::codec::int::encode(&v).unwrap();
		assert_eq!(snouttime_fuzz::codec::int::decode(&again).unwrap(), v);
	}
});
