//! Pages: moving the column store's byte stream to and from the relation's main fork
//! (docs/snouttime/COLUMNAR.md §2).
//!
//! Block 0 is the metapage; blocks 1 and on carry the stream, `DATA` bytes each, right after
//! the standard page header. Every page is a valid standard page with no hole (`pd_lower` =
//! `pd_upper` = the end of its data), so a full-page image in the WAL carries every byte,
//! and page checksums and `pg_checksums` work unchanged.
//!
//! Writing goes through Postgres 17's bulk-write API (C2 in COLUMNAR.md; D7 as amended): the
//! pages of a new relfilenode, WAL-logged as full-page images when the relation needs WAL,
//! and synced before commit when it does not.

use std::ffi::c_void;

use pgrx::pg_sys;

use super::format::{self, Meta};
use crate::codec::CodecError;

pub const BLCKSZ: usize = pg_sys::BLCKSZ as usize;
/// `SizeOfPageHeaderData`: the page header up to its line-pointer array.
pub const HEADER: usize = 24;
pub const DATA: usize = BLCKSZ - HEADER;

// storage/bulk_write.h, which pgrx does not bind. Opaque pointers; the functions are
// ordinary exported symbols of the server. Declared by hand, so they are called through
// pg_guard_ffi_boundary (`ffi`), which turns an ERROR in them into a Rust panic. Functions
// pgrx binds already do that themselves, and must NOT be wrapped again: an ERROR under both
// aborted the process instead of raising it (every one of them, until 2026-09-23).
unsafe extern "C-unwind" {
	fn smgr_bulk_start_rel(rel: pg_sys::Relation, forknum: pg_sys::ForkNumber::Type) -> *mut c_void;
	fn smgr_bulk_get_buf(bulkstate: *mut c_void) -> *mut c_void;
	fn smgr_bulk_write(bulkstate: *mut c_void, blocknum: pg_sys::BlockNumber, buf: *mut c_void, page_std: bool);
	fn smgr_bulk_finish(bulkstate: *mut c_void);
}

fn ffi<T>(f: impl FnOnce() -> T) -> T {
	unsafe { pg_sys::ffi::pg_guard_ffi_boundary(f) }
}

/// Writes the stream into a new, empty relfilenode, the metapage last.
pub struct Writer {
	bulk: *mut c_void,
	page: Vec<u8>,
	block: u32,
	/// Bytes of stream written so far.
	pub offset: u64,
}

impl Writer {
	/// # Safety
	/// `rel` must be open, and its main fork empty and created in this transaction.
	pub unsafe fn start(rel: pg_sys::Relation) -> Writer {
		let bulk = ffi(|| smgr_bulk_start_rel(rel, pg_sys::ForkNumber::MAIN_FORKNUM));
		Writer { bulk, page: Vec::with_capacity(DATA), block: 1, offset: 0 }
	}

	pub fn write(&mut self, mut bytes: &[u8]) {
		self.offset += bytes.len() as u64;
		while !bytes.is_empty() {
			let n = (DATA - self.page.len()).min(bytes.len());
			self.page.extend_from_slice(&bytes[..n]);
			bytes = &bytes[n..];
			if self.page.len() == DATA {
				self.emit_page();
			}
		}
	}

	fn emit_page(&mut self) {
		let block = self.block;
		self.block += 1;
		let data = std::mem::take(&mut self.page);
		self.put(block, &data);
		self.page = Vec::with_capacity(DATA);
	}

	fn put(&mut self, block: u32, data: &[u8]) {
		assert!(data.len() <= DATA);
		unsafe {
			let buf = ffi(|| smgr_bulk_get_buf(self.bulk));
			let page = buf as *mut u8;
			pg_sys::PageInit(page as pg_sys::Page, BLCKSZ, 0);
			std::ptr::copy_nonoverlapping(data.as_ptr(), page.add(HEADER), data.len());
			// No hole: the data runs from the header to pd_lower, and pd_upper meets it.
			let header = page as *mut pg_sys::PageHeaderData;
			let end = (HEADER + data.len()) as u16;
			(*header).pd_lower = end;
			(*header).pd_upper = end;
			ffi(|| smgr_bulk_write(self.bulk, block, buf, true));
		}
	}

	/// Writes what is left of the last page, then the metapage, and finishes the bulk write.
	pub fn finish(mut self, meta: &Meta) {
		if !self.page.is_empty() {
			self.emit_page();
		}
		let m = format::encode_meta(meta);
		if m.len() > DATA {
			pgrx::error!("a column store's metapage does not fit a page: the table has too many columns");
		}
		self.put(0, &m);
		ffi(|| unsafe { smgr_bulk_finish(self.bulk) });
	}
}

/// Where a column store's row groups are written: its own pages, or (tiering) a file that
/// is then uploaded as one object.
pub trait Sink {
	fn write(&mut self, bytes: &[u8]);
	fn offset(&self) -> u64;
}

impl Sink for Writer {
	fn write(&mut self, bytes: &[u8]) {
		Writer::write(self, bytes)
	}
	fn offset(&self) -> u64 {
		self.offset
	}
}

/// The stream of a tiered column store, in a temporary file under the cluster's own
/// `base/pgsql_tmp` (which Postgres empties at startup, so a crash leaves nothing behind).
pub struct FileSink {
	file: std::io::BufWriter<std::fs::File>,
	pub path: std::path::PathBuf,
	offset: u64,
}

impl FileSink {
	pub fn create(name: &str) -> FileSink {
		let dir = std::path::Path::new("base/pgsql_tmp");
		let _ = std::fs::create_dir_all(dir);
		let path = dir.join(name);
		let file = match std::fs::File::create(&path) {
			Ok(f) => f,
			Err(e) => pgrx::error!("could not create {}: {e}", path.display()),
		};
		FileSink { file: std::io::BufWriter::with_capacity(1 << 20, file), path, offset: 0 }
	}

	pub fn finish(mut self) -> std::path::PathBuf {
		use std::io::Write;
		if let Err(e) = self.file.flush() {
			pgrx::error!("could not write {}: {e}", self.path.display());
		}
		self.path
	}
}

impl Sink for FileSink {
	fn write(&mut self, bytes: &[u8]) {
		use std::io::Write;
		if let Err(e) = self.file.write_all(bytes) {
			pgrx::error!("could not write {}: {e}", self.path.display());
		}
		self.offset += bytes.len() as u64;
	}
	fn offset(&self) -> u64 {
		self.offset
	}
}

/// Reads the stream of an existing column store through shared buffers.
pub struct Reader {
	rel: pg_sys::Relation,
	pub blocks: u32,
}

/// A finished column store's block count and metapage, kept in its relation cache entry
/// (`rd_amcache`, which Postgres frees whenever the entry is invalidated, and so whenever the
/// relfilenode changes): planning one query over a partitioned table opened each sealed
/// partition's store three to five times, each time counting its blocks with an `lseek` and
/// reading and checking its metapage (2026-09-25). A column store is written once, by the
/// transaction that created its relfilenode, and never again under that relfilenode, so only a
/// store created in an earlier transaction is kept.
#[repr(C)]
struct Cached {
	magic: u32,
	blocks: u32,
	len: u32,
}

const CACHED_MAGIC: u32 = 0x534e_5443; // "SNTC"

/// The block count and metapage bytes `rel`'s cache entry holds, if it holds them.
unsafe fn cached<'a>(rel: pg_sys::Relation) -> Option<(u32, &'a [u8])> {
	let c = (*rel).rd_amcache as *const Cached;
	if c.is_null() || (*c).magic != CACHED_MAGIC {
		return None;
	}
	let bytes = std::slice::from_raw_parts((c as *const u8).add(std::mem::size_of::<Cached>()), (*c).len as usize);
	Some(((*c).blocks, bytes))
}

/// Keeps a finished store's block count and metapage bytes in its cache entry.
unsafe fn remember(rel: pg_sys::Relation, blocks: u32, meta: &[u8]) {
	if !(*rel).rd_amcache.is_null() || (*rel).rd_createSubid != 0 || (*rel).rd_firstRelfilelocatorSubid != 0 {
		return;
	}
	let size = std::mem::size_of::<Cached>() + meta.len();
	let c = pg_sys::MemoryContextAlloc(pg_sys::CacheMemoryContext, size) as *mut Cached;
	std::ptr::write(c, Cached { magic: CACHED_MAGIC, blocks, len: meta.len() as u32 });
	std::ptr::copy_nonoverlapping(meta.as_ptr(), (c as *mut u8).add(std::mem::size_of::<Cached>()), meta.len());
	(*rel).rd_amcache = c as *mut std::ffi::c_void;
}

impl Reader {
	/// # Safety
	/// `rel` must stay open for as long as the reader is used.
	pub unsafe fn new(rel: pg_sys::Relation) -> Reader {
		if let Some((blocks, _)) = cached(rel) {
			return Reader { rel, blocks };
		}
		let blocks = pg_sys::RelationGetNumberOfBlocksInFork(rel, pg_sys::ForkNumber::MAIN_FORKNUM);
		Reader { rel, blocks }
	}

	/// The data area of one page, as far as it was written.
	fn page(&self, block: u32) -> Result<Vec<u8>, CodecError> {
		self.with_page(block, |p| p.to_vec())
	}

	/// Runs `f` on a page's stream bytes while the buffer is pinned and share-locked, so a
	/// caller copies only what it needs out of shared buffers (a whole page into a new vector
	/// for every page read was a copy and an allocation per 8 kB, 2026-09-23).
	fn with_page<R>(&self, block: u32, f: impl FnOnce(&[u8]) -> R) -> Result<R, CodecError> {
		if block >= self.blocks {
			return Err(CodecError::Truncated);
		}
		unsafe {
			let buf = {
				pg_sys::ReadBufferExtended(
					self.rel,
					pg_sys::ForkNumber::MAIN_FORKNUM,
					block,
					pg_sys::ReadBufferMode::RBM_NORMAL,
					std::ptr::null_mut(),
				)
			};
			pg_sys::LockBuffer(buf, pg_sys::BUFFER_LOCK_SHARE as i32);
			let page = pg_sys::BufferGetPage(buf) as *const u8;
			let header = page as *const pg_sys::PageHeaderData;
			let lower = (*header).pd_lower as usize;
			let out = if (HEADER..=BLCKSZ).contains(&lower) {
				Ok(f(std::slice::from_raw_parts(page.add(HEADER), lower - HEADER)))
			} else {
				Err(CodecError::Corrupt("a column store page's header is not one this format writes"))
			};
			pg_sys::UnlockReleaseBuffer(buf);
			out
		}
	}

	/// The metapage, or `None` for a relation that has never been written (no blocks).
	pub fn meta(&self, max_columns: usize) -> Result<Option<Meta>, CodecError> {
		if self.blocks == 0 {
			return Ok(None);
		}
		unsafe {
			if let Some((_, bytes)) = cached(self.rel) {
				return format::decode_meta(bytes, max_columns).map(Some);
			}
		}
		let page = self.page(0)?;
		let meta = format::decode_meta(&page, max_columns)?;
		unsafe { remember(self.rel, self.blocks, &page) };
		Ok(Some(meta))
	}

	/// `length` bytes of the stream from `offset`.
	pub fn read(&self, offset: u64, length: u64) -> Result<Vec<u8>, CodecError> {
		let end = offset.checked_add(length).ok_or(CodecError::Truncated)?;
		if end > (self.blocks.saturating_sub(1) as u64) * DATA as u64 {
			return Err(CodecError::Truncated);
		}
		let mut out = Vec::with_capacity(length as usize);
		let mut at = offset;
		while at < end {
			let block = 1 + (at / DATA as u64) as u32;
			let within = (at % DATA as u64) as usize;
			let n = ((end - at) as usize).min(DATA - within);
			self.with_page(block, |page| match page.get(within..within + n) {
				Some(piece) => {
					out.extend_from_slice(piece);
					Ok(())
				}
				None => Err(CodecError::Truncated),
			})??;
			at += n as u64;
		}
		Ok(out)
	}

	/// The page a stream offset lives on, for ANALYZE's block sampling.
	pub fn block_of(offset: u64) -> u32 {
		1 + (offset / DATA as u64) as u32
	}
}
