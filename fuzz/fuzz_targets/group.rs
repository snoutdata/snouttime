#![no_main]
//! Fuzzes the `group` decoder: any bytes must give a value or an error, never a panic.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
	use snouttime_fuzz::format;
	if let Ok(h) = format::decode_group_header(data, data.len() as u64, 64) {
		for m in &h.columns {
			let start = h.data_start + m.offset as usize;
			if let Some(frame) = data.get(start..start + m.length as usize) {
				let _ = format::decode_chunk(frame, m, h.rows);
				// below the checksum too: the decoders must not trust a frame that passes it
				let _ = format::decode_column(frame, m.encoding, h.rows);
				// and a page at a time, as a narrow read decodes a paged chunk
				if let Ok(c) = format::Chunk::open(frame, m.encoding, h.rows) {
					for k in 0..h.rows as usize / format::PAGE_ROWS + 2 {
						let _ = c.decode_page(k);
					}
				}
			}
		}
	}
});
