//! Reading a column store: its metapage and directory, a row group's columns as Datums, and
//! the side tables (docs/snouttime/COLUMNAR.md §1, §4, §5).

use std::cell::RefCell;
use std::rc::Rc;

use pgrx::pg_sys;
use pgrx::prelude::*;

use super::build::relname;
use super::format::{self, GroupEntry, Meta};
use super::store::Reader;
use super::types::{self, Kind};
use crate::codec::CodecError;


/// TIDs (COLUMNAR.md §4): column-store row `n` is `(n / 256, n % 256 + 1)`; a delta-store
/// row is its heap TID with the top bit of the block number set.
pub const ROWS_PER_BLOCK: u64 = 256;
pub const DELTA_BIT: u32 = 0x8000_0000;

pub fn tid_of_row(n: u64) -> pg_sys::ItemPointerData {
	let mut t = pg_sys::ItemPointerData::default();
	set_tid(&mut t, (n / ROWS_PER_BLOCK) as u32, (n % ROWS_PER_BLOCK + 1) as u16);
	t
}

pub fn set_tid(t: &mut pg_sys::ItemPointerData, block: u32, offset: u16) {
	t.ip_blkid.bi_hi = (block >> 16) as u16;
	t.ip_blkid.bi_lo = (block & 0xffff) as u16;
	t.ip_posid = offset;
}

pub fn tid_block(t: &pg_sys::ItemPointerData) -> u32 {
	((t.ip_blkid.bi_hi as u32) << 16) | t.ip_blkid.bi_lo as u32
}

pub enum Tid {
	Row(u64),
	Delta(pg_sys::ItemPointerData),
	Invalid,
}

pub fn classify(t: &pg_sys::ItemPointerData) -> Tid {
	let block = tid_block(t);
	if block & DELTA_BIT != 0 {
		let mut d = *t;
		set_tid(&mut d, block & !DELTA_BIT, t.ip_posid);
		return Tid::Delta(d);
	}
	if t.ip_posid == 0 || t.ip_posid as u64 > ROWS_PER_BLOCK {
		return Tid::Invalid;
	}
	Tid::Row(block as u64 * ROWS_PER_BLOCK + t.ip_posid as u64 - 1)
}

pub fn to_delta_tid(t: &mut pg_sys::ItemPointerData) {
	let block = tid_block(t);
	set_tid(t, block | DELTA_BIT, t.ip_posid);
}

/// A column store's description, read once per scan or fetch.
pub struct Store {
	pub reader: Reader,
	pub meta: Option<Meta>,
	pub dir: Rc<Vec<GroupEntry>>,
	pub kinds: Vec<Kind>,
	pub rel: pg_sys::Relation,
	/// Each row group's first and last sort keys as Datums, built the first time a seek asks.
	bounds: Rc<Bounds>,
}

type KeyDatums = Vec<(Vec<Option<pg_sys::Datum>>, Vec<Option<pg_sys::Datum>>)>;

/// Per row group, where each run of the leading sort columns but the last ends (its last row,
/// from the group's first) and that row's sort key; None where the directory has none.
type RunDatums = Vec<Option<Vec<(u32, Vec<Option<pg_sys::Datum>>)>>>;

#[derive(Default)]
struct Bounds {
	datums: std::cell::OnceCell<KeyDatums>,
	runs: std::cell::OnceCell<RunDatums>,
	mem: RefCell<types::Mem>,
}

/// A column store never changes under one relfilenode (a seal, a reseal and a rewrite each
/// write a new one), so its parsed directory is kept per backend and reused by every plan and
/// scan: reading and parsing it was a third of planning a point query over three partitions
/// (IoT i3, 2026-09-23). Keyed by the relation, its relfilenode and the directory's place and
/// checksum, so a relfilenode number reused after a drop cannot match.
type DirKey = (u32, u32, u64, u64, u32);

thread_local! {
	static DIRS: RefCell<Vec<(DirKey, Rc<Vec<GroupEntry>>, Rc<Bounds>)>> = const { RefCell::new(Vec::new()) };
}

/// How many row groups' directory entries a backend keeps parsed, all stores together.
/// Chosen, not measured: about 30 MB at the most, for 1.6 billion rows of 8,192-row groups.
const DIR_CACHE_GROUPS: usize = 200_000;

/// The sort order and flags of a column store, by its relation's OID; None when it is not one,
/// or has never been written.
pub fn order_of(relid: pg_sys::Oid) -> Option<(Vec<u16>, u8)> {
	unsafe {
		let rel = pg_sys::RelationIdGetRelation(relid);
		if rel.is_null() {
			return None;
		}
		let out = if (*rel).rd_tableam == super::am::routine() {
			Reader::new(rel).meta(1600).ok().flatten().map(|m| (m.order, m.flags))
		} else {
			None
		};
		pg_sys::RelationClose(rel);
		out
	}
}

pub fn damaged(rel: pg_sys::Relation, where_: &str, e: CodecError) -> ! {
	let name = unsafe { relname(rel) };
	ereport!(
		PgLogLevel::ERROR,
		PgSqlErrorCode::ERRCODE_DATA_CORRUPTED,
		format!("the sealed partition \"{name}\" is damaged: {where_}: {e}"),
		"Restore it from a backup; SnoutTime never repairs a column store by itself."
	);
	unreachable!()
}

impl Store {
	/// # Safety
	/// `rel` must be open and stay open while the store is used.
	pub unsafe fn open(rel: pg_sys::Relation) -> Store {
		let reader = Reader::new(rel);
		let meta = match reader.meta(1600) {
			Ok(m) => m,
			Err(e) => damaged(rel, "metapage", e),
		};
		// Columns decode by the types they were STORED with, not the relation's current
		// descriptor: during an ALTER TABLE rewrite the relation already has its new types,
		// and the rewrite reads the old rows through a slot of the old descriptor.
		let kinds: Vec<Kind> = meta.as_ref().map_or(Vec::new(), |m| m.types.iter().map(|&t| types::kind_of_type(t)).collect());
		let (dir, bounds) = match &meta {
			None => (Rc::new(Vec::new()), Rc::new(Bounds::default())),
			Some(m) => {
				let key: DirKey = ((*rel).rd_id.to_u32(), (*rel).rd_locator.relNumber.to_u32(), m.dir_offset, m.dir_length, m.dir_crc);
				let hit = DIRS.with(|d| {
					let mut d = d.borrow_mut();
					let i = d.iter().position(|x| x.0 == key)?;
					let e = d.remove(i);
					let out = (Rc::clone(&e.1), Rc::clone(&e.2));
					d.insert(0, e);
					Some(out)
				});
				match hit {
					Some(x) => x,
					None => {
						let bytes = match reader.read(m.dir_offset, m.dir_length) {
							Ok(b) => b,
							Err(e) => damaged(rel, "row-group directory", e),
						};
						let dir = match format::decode_directory(&bytes, m) {
							Ok(d) => Rc::new(d),
							Err(e) => damaged(rel, "row-group directory", e),
						};
						let bounds = Rc::new(Bounds::default());
						DIRS.with(|d| {
							let mut d = d.borrow_mut();
							d.insert(0, (key, Rc::clone(&dir), Rc::clone(&bounds)));
							let mut total = 0;
							let keep = d
								.iter()
								.position(|x| {
									total += x.1.len();
									total > DIR_CACHE_GROUPS
								})
								.unwrap_or(d.len())
								.max(1);
							d.truncate(keep);
						});
						(dir, bounds)
					}
				}
			}
		};
		Store { reader, meta, dir, kinds, rel, bounds }
	}

	/// Row group `g`'s first and last sort keys, one Datum per column of the order (None: NULL).
	///
	/// # Safety
	/// The store's relation must be open.
	pub unsafe fn bounds(&self, g: usize) -> (&[Option<pg_sys::Datum>], &[Option<pg_sys::Datum>]) {
		let all = self.bounds.datums.get_or_init(|| {
			let order = self.meta.as_ref().map_or(&[][..], |m| &m.order[..]);
			let mut mem = self.bounds.mem.borrow_mut();
			let mut datums = |key: &[Option<Vec<u8>>]| -> Vec<Option<pg_sys::Datum>> {
				key.iter()
					.zip(order)
					.map(|(v, &a)| {
						let (len, byval) = types::key_layout(self.kinds[a as usize - 1]);
						v.as_ref().map(|b| types::datum_of_bytes(b, len, byval, &mut mem))
					})
					.collect()
			};
			self.dir.iter().map(|e| (datums(&e.first_key), datums(&e.last_key))).collect()
		});
		let (f, l) = &all[g];
		(f, l)
	}

	/// Row group `g`'s runs of the leading sort columns that end inside it, as the directory
	/// records them (`GroupEntry::run_ends`), with their keys as Datums; None when not recorded.
	/// The group's last run ends with the group, whose last key is `bounds(g).1`.
	///
	/// # Safety
	/// The store's relation must be open.
	pub unsafe fn runs(&self, g: usize) -> Option<&[(u32, Vec<Option<pg_sys::Datum>>)]> {
		let all = self.bounds.runs.get_or_init(|| {
			let order = self.meta.as_ref().map_or(&[][..], |m| &m.order[..]);
			let mut mem = self.bounds.mem.borrow_mut();
			self.dir
				.iter()
				.map(|e| {
					e.run_ends.as_ref().map(|runs| {
						runs.iter()
							.map(|(row, key)| {
								let datums = key
									.iter()
									.zip(order)
									.map(|(v, &a)| {
										let (len, byval) = types::key_layout(self.kinds[a as usize - 1]);
										v.as_ref().map(|b| types::datum_of_bytes(b, len, byval, &mut mem))
									})
									.collect();
								(*row, datums)
							})
							.collect()
					})
				})
				.collect()
		});
		all[g].as_deref()
	}

	pub fn rows(&self) -> u64 {
		self.meta.as_ref().map_or(0, |m| m.rows)
	}

	/// The row group holding row `n`.
	pub fn group_of(&self, n: u64) -> Option<usize> {
		if n >= self.rows() {
			return None;
		}
		let i = self.dir.partition_point(|g| g.first_row + g.rows <= n);
		(i < self.dir.len()).then_some(i)
	}

	/// Row group `g`, of which only the header has been read: a column is read and decoded the
	/// first time something asks for it ([`Group::decode`], [`Group::value`]). A tiered group
	/// is fetched whole, in one request: a round trip to S3 costs far more than the bytes of
	/// the columns a query skips. A local one reads the header (its length is its first varint)
	/// and later only the chunks wanted.
	///
	/// # Safety
	/// The store's relation must be open.
	pub unsafe fn load(store: &Rc<Store>, g: usize) -> Group {
		let entry = &store.dir[g];
		let fetched = match store.meta.as_ref().and_then(|m| m.remote.as_ref()) {
			Some(r) => match super::remote_read(&r.url, entry.offset, entry.length) {
				Ok(b) => Some(b),
				Err(e) => {
					ereport!(
						PgLogLevel::ERROR,
						PgSqlErrorCode::ERRCODE_EXTERNAL_ROUTINE_EXCEPTION,
						format!("could not read row group {g} of the tiered partition \"{}\": {e}", relname(store.rel))
					);
					unreachable!()
				}
			},
			None => None,
		};
		let mut group = Group {
			first_row: entry.first_row,
			rows: entry.rows,
			store: Rc::clone(store),
			index: g,
			head: format::GroupHeader { rows: 0, columns: Vec::new(), data_start: 0 },
			fetched,
			cols: RefCell::new(Vec::new()),
			mem: RefCell::new(types::Mem::new()),
		};
		let first = match group.read(entry.offset, entry.length.min(10)) {
			Ok(b) => b,
			Err(e) => damaged(store.rel, &group.place(None), e),
		};
		let end = match format::header_end(&first, entry.length) {
			Ok(n) => n,
			Err(e) => damaged(store.rel, &group.place(None), e),
		};
		let head = match group.read(entry.offset, end as u64) {
			Ok(b) => b,
			Err(e) => damaged(store.rel, &group.place(None), e),
		};
		let stored = store.kinds.len();
		let h = match format::decode_group_header(&head, entry.length, stored) {
			Ok(h) => h,
			Err(e) => damaged(store.rel, &group.place(None), e),
		};
		if h.rows != entry.rows || h.columns.len() != stored {
			damaged(store.rel, &group.place(None), CodecError::Corrupt("a row group's header disagrees with the directory"));
		}
		group.head = h;
		group.cols = RefCell::new((0..stored).map(|_| Col::Undecoded).collect());
		group
	}
}

/// A row group, its columns decoded as they are asked for. By-reference values live in the
/// group (`mem`), so they are valid as long as it is.
pub struct Group {
	pub first_row: u64,
	pub rows: u64,
	store: Rc<Store>,
	index: usize,
	head: format::GroupHeader,
	/// the whole row group, when it came from a tier in one request
	fetched: Option<Vec<u8>>,
	cols: RefCell<Vec<Col>>,
	mem: RefCell<types::Mem>,
}

/// A decoded column's Datums and nulls, one per row of its group (a paged one's are right only
/// in its decoded pages).
fn decoded(c: &Col) -> Option<(&[pg_sys::Datum], &[bool])> {
	match c {
		Col::Ready(d, nulls) => Some((d, nulls)),
		Col::Paged { chunk, datums, .. } => Some((datums, &chunk.nulls)),
		_ => None,
	}
}

enum Col {
	Undecoded,
	/// never read by the scan that loaded the group: every row reads as NULL
	Omitted,
	Ready(Vec<pg_sys::Datum>, Vec<bool>),
	/// a paged chunk (format version 3), opened, whose pages are decoded into `datums` as rows
	/// in them are asked for; `Ready` once every page is
	Paged { chunk: format::Chunk, datums: Vec<pg_sys::Datum>, done: Vec<bool> },
}

impl Group {
	/// Each stored column's chunk as the row group's header describes it: encoding, stored
	/// length, nulls. Read from the header alone.
	pub fn chunks(&self) -> &[format::ChunkMeta] {
		&self.head.columns
	}

	fn place(&self, col: Option<usize>) -> String {
		match col {
			None => format!("row group {}", self.index),
			Some(c) => format!("row group {}, column {}", self.index, c + 1),
		}
	}

	fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>, CodecError> {
		let entry = &self.store.dir[self.index];
		match &self.fetched {
			Some(b) => {
				let start = (offset - entry.offset) as usize;
				b.get(start..start + len as usize).map(<[u8]>::to_vec).ok_or(CodecError::Truncated)
			}
			None => self.store.reader.read(offset, len),
		}
	}

	/// Stored column `i`'s chunk, read, checked and opened.
	///
	/// # Safety
	/// The store's relation must be open.
	/// A paged chunk is opened by its head alone unless `whole`: its pages are read as they are
	/// asked for (`fetch_pages`).
	unsafe fn open_chunk(&self, i: usize, whole: bool) -> format::Chunk {
		let entry = &self.store.dir[self.index];
		let m = &self.head.columns[i];
		let at = entry.offset + self.head.data_start as u64 + m.offset;
		let paged = m.encoding == format::ENC_INT_PAGED || m.encoding == format::ENC_FLOAT_PAGED;
		let opened = if paged && !whole {
			self.read(at, m.length.min(10))
				.and_then(|first| format::head_len(&first, m.length as usize))
				.and_then(|n| self.read(at, n as u64))
				.and_then(|head| format::Chunk::open_head(&head, m, self.head.rows))
		} else {
			self.read(at, m.length).and_then(|frame| format::Chunk::open_checked(&frame, m, self.head.rows))
		};
		match opened {
			Ok(c) => c,
			Err(e) => damaged(self.store.rel, &self.place(Some(i)), e),
		}
	}

	/// Reads the pages of `pages` that `chunk` (stored column `i`'s) has not read yet, in one
	/// read from the first of them to the last, each checked against its checksum.
	///
	/// # Safety
	/// The store's relation must be open.
	unsafe fn fetch_pages(&self, i: usize, chunk: &mut format::Chunk, pages: std::ops::Range<usize>) {
		let missing: Vec<(usize, usize, usize)> = pages.filter_map(|k| chunk.page_span(k).map(|(at, len)| (k, at, len))).collect();
		let (Some(&(_, from, _)), Some(&(_, last, last_len))) = (missing.first(), missing.last()) else {
			return;
		};
		let entry = &self.store.dir[self.index];
		let m = &self.head.columns[i];
		let at = entry.offset + self.head.data_start as u64 + m.offset + from as u64;
		let got = self.read(at, (last + last_len - from) as u64).and_then(|bytes| {
			for &(k, a, len) in &missing {
				chunk.supply(k, &bytes[a - from..a - from + len], true)?;
			}
			Ok(())
		});
		if let Err(e) = got {
			damaged(self.store.rel, &self.place(Some(i)), e);
		}
	}

	/// Stored column `i` as its values, not Datums, for code that computes on them directly (the
	/// aggregate node): its values in the rows `want` (from the group's first) and around them:
	/// the rows the returned column covers, which for a paged chunk are the pages holding `want`
	/// and otherwise the whole group, with those rows' nulls and non-null values.
	///
	/// # Safety
	/// The store's relation must be open.
	pub unsafe fn raw_span(&self, i: usize, want: std::ops::Range<usize>) -> (std::ops::Range<usize>, format::DecodedColumn) {
		let rows = self.rows as usize;
		let whole = want.is_empty() || (want.start == 0 && want.end >= rows);
		let mut cols = self.cols.borrow_mut();
		let mut opened;
		let chunk = match &mut cols[i] {
			// the chunk a seek already opened, for the pages it read
			Col::Paged { chunk, .. } => chunk,
			_ => {
				opened = self.open_chunk(i, whole);
				&mut opened
			}
		};
		match chunk.page_rows() {
			Some(pr) if !whole => {
				let (k0, k1) = (want.start / pr, (want.end.min(rows) - 1) / pr);
				self.fetch_pages(i, chunk, k0..k1 + 1);
				let covered = k0 * pr..((k1 + 1) * pr).min(rows);
				let values = match chunk.decode_pages(k0..k1 + 1) {
					Ok(v) => v,
					Err(e) => damaged(self.store.rel, &self.place(Some(i)), e),
				};
				let nulls = chunk.nulls[covered.clone()].to_vec();
				(covered, format::DecodedColumn { nulls, values })
			}
			pr => {
				if let Some(pr) = pr {
					self.fetch_pages(i, chunk, 0..rows.div_ceil(pr));
				}
				match chunk.decode_all() {
					Ok(values) => (0..rows, format::DecodedColumn { nulls: chunk.nulls.clone(), values }),
					Err(e) => damaged(self.store.rel, &self.place(Some(i)), e),
				}
			}
		}
	}

	/// Stored column `att`'s value in row `n` (a row of the whole store, inside this group),
	/// decoding what holds it if nothing has; None for NULL.
	///
	/// # Safety
	/// The store's relation must be open, and `att` a stored column.
	pub unsafe fn value(&self, att: usize, n: u64) -> Option<pg_sys::Datum> {
		// a column the scan chose not to decode is decoded now all the same: a NULL here would
		// be a wrong answer, not a skipped one
		if matches!(self.cols.borrow()[att], Col::Omitted) {
			self.cols.borrow_mut()[att] = Col::Undecoded;
		}
		let i = (n - self.first_row) as usize;
		self.ensure(att, i..i + 1);
		self.cell(att, i).and_then(|(d, isnull)| (!isnull).then_some(d))
	}

	/// Row `i` (from the group's first) of stored column `att`, if it is decoded: its Datum and
	/// whether it is NULL.
	fn cell(&self, att: usize, i: usize) -> Option<(pg_sys::Datum, bool)> {
		decoded(&self.cols.borrow()[att]).map(|(d, nulls)| (d[i], nulls[i]))
	}

	/// Decodes stored column `i`, if it is not already.
	///
	/// # Safety
	/// The store's relation must be open.
	unsafe fn column(&self, i: usize) {
		self.ensure(i, 0..self.rows as usize);
	}

	/// Decodes stored column `i` in the rows `want` (from the group's first) at least: for a
	/// paged chunk the pages that hold them, for any other the whole chunk. An omitted column
	/// stays omitted.
	///
	/// # Safety
	/// The store's relation must be open.
	unsafe fn ensure(&self, i: usize, want: std::ops::Range<usize>) {
		let rows = self.rows as usize;
		if want.is_empty() || matches!(self.cols.borrow()[i], Col::Ready(..) | Col::Omitted) {
			return;
		}
		let kind = self.store.kinds[i];
		if kind == Kind::Dropped {
			self.cols.borrow_mut()[i] = Col::Ready(vec![pg_sys::Datum::from(0usize); rows], vec![true; rows]);
			return;
		}
		if matches!(self.cols.borrow()[i], Col::Undecoded) {
			let whole = want.start == 0 && want.end >= rows;
			let chunk = self.open_chunk(i, whole);
			if chunk.page_rows().is_none() || whole {
				let values = match chunk.decode_all() {
					Ok(v) => v,
					Err(e) => damaged(self.store.rel, &self.place(Some(i)), e),
				};
				let datums = match types::datums(kind, &chunk.nulls, &values, &mut self.mem.borrow_mut()) {
					Ok(d) => d,
					Err(e) => damaged(self.store.rel, &self.place(Some(i)), CodecError::Corrupt(e)),
				};
				self.cols.borrow_mut()[i] = Col::Ready(datums, chunk.nulls);
				return;
			}
			let pages = rows.div_ceil(chunk.page_rows().unwrap_or(rows.max(1)));
			self.cols.borrow_mut()[i] = Col::Paged { chunk, datums: vec![pg_sys::Datum::from(0usize); rows], done: vec![false; pages] };
		}
		let mut cols = self.cols.borrow_mut();
		let Col::Paged { chunk, datums, done } = &mut cols[i] else { return };
		let Some(pr) = chunk.page_rows() else { return };
		let (k0, k1) = (want.start / pr, (want.end.min(rows) - 1) / pr);
		self.fetch_pages(i, chunk, k0..k1 + 1);
		for k in k0..=k1 {
			if done[k] {
				continue;
			}
			let values = match chunk.decode_page(k) {
				Ok(v) => v,
				Err(e) => damaged(self.store.rel, &self.place(Some(i)), e),
			};
			let span = k * pr..((k + 1) * pr).min(rows);
			match types::datums(kind, &chunk.nulls[span.clone()], &values, &mut self.mem.borrow_mut()) {
				Ok(d) => datums[span].copy_from_slice(&d),
				Err(e) => damaged(self.store.rel, &self.place(Some(i)), CodecError::Corrupt(e)),
			}
			done[k] = true;
		}
		if done.iter().all(|&d| d) {
			let Col::Paged { chunk, datums, .. } = std::mem::replace(&mut cols[i], Col::Undecoded) else { unreachable!() };
			cols[i] = Col::Ready(datums, chunk.nulls);
		}
	}

	/// How many of stored column `att`'s values in this group are NULL, from its header.
	pub fn nulls(&self, att: usize) -> Option<u64> {
		self.head.columns.get(att).map(|c| c.nulls)
	}

	/// Which row group of its store this is.
	pub fn index(&self) -> usize {
		self.index
	}

	/// Marks the stored columns `needed` does not as never read, so they read as NULL, and
	/// leaves the others to be decoded when something first asks for them.
	pub fn omit(&self, needed: Option<&[bool]>) {
		let Some(n) = needed else { return };
		let mut cols = self.cols.borrow_mut();
		for (i, c) in cols.iter_mut().enumerate() {
			if !n.get(i).copied().unwrap_or(true) && matches!(c, Col::Undecoded) {
				*c = Col::Omitted;
			}
		}
	}

	/// About how many bytes its decoded columns take, to bound a cache of groups.
	pub fn decoded_bytes(&self) -> usize {
		let cols: usize = self
			.cols
			.borrow()
			.iter()
			.map(|c| match c {
				Col::Ready(d, n) => d.len() * 8 + n.len(),
				Col::Paged { datums, chunk, .. } => datums.len() * 8 + chunk.nulls.len(),
				_ => 0,
			})
			.sum();
		cols + self.mem.borrow().iter().map(|b| b.len() * 8).sum::<usize>()
	}

	/// Decodes the stored columns `needed` marks (all, when `None`), and marks the others as
	/// never read, so they read as NULL without being decoded.
	///
	/// # Safety
	/// The store's relation must be open.
	pub unsafe fn decode(&self, needed: Option<&[bool]>) {
		for i in 0..self.store.kinds.len() {
			if needed.is_none_or(|n| n.get(i).copied().unwrap_or(true)) {
				self.column(i);
			} else {
				self.cols.borrow_mut()[i] = Col::Omitted;
			}
		}
	}

	/// Attributes `from..to` (0-based) of row `n` into `values` and `nulls`, decoding the
	/// columns that have not been. Columns added after the seal read as their "missing" value,
	/// from `desc`: during an ALTER TABLE rewrite the relation's own descriptor no longer
	/// carries it, the old one the rewrite reads through does (found 2026-09-23).
	///
	/// # Safety
	/// The store's relation must be open; `desc` must be the slot's descriptor, and `values`
	/// and `nulls` must hold at least `to` entries.
	pub unsafe fn fill(&self, n: u64, from: usize, to: usize, desc: pg_sys::TupleDesc, values: *mut pg_sys::Datum, nulls: *mut bool) {
		let stored = self.store.kinds.len();
		let i = (n - self.first_row) as usize;
		for a in from..to.min(stored) {
			// a paged column decodes the page of this row, and the next rows' as they come
			self.ensure(a, i..i + 1);
		}
		let cols = self.cols.borrow();
		for a in from..to {
			let (d, isnull) = match cols.get(a) {
				Some(Col::Ready(d, isnull)) => (*d.get_unchecked(i), *isnull.get_unchecked(i)),
				Some(Col::Paged { chunk, datums, .. }) => (*datums.get_unchecked(i), *chunk.nulls.get_unchecked(i)),
				Some(_) => (pg_sys::Datum::from(0usize), true),
				None => {
					let mut isnull = true;
					(pg_sys::getmissingattr(desc, a as i32 + 1, &mut isnull), isnull)
				}
			};
			*values.add(a) = d;
			*nulls.add(a) = isnull;
		}
	}

	/// Which of the rows `range` (from the group's first) pass `keys` (integer and time
	/// comparisons) and `floats`, from the decoded columns; a NULL passes no comparison. Indexed
	/// from the group's first row; rows outside `range` are left true and are not looked at.
	///
	/// # Safety
	/// The store's relation must be open.
	pub unsafe fn mask(&self, keys: &[super::scan::Key], floats: &[super::scan::FloatFilter], range: std::ops::Range<usize>) -> Vec<bool> {
		use super::scan::Op;
		let rows = self.rows as usize;
		let mut keep = vec![true; rows];
		for k in keys {
			if k.att >= self.store.kinds.len() {
				continue;
			}
			self.ensure(k.att, range.clone());
			let kind = self.store.kinds[k.att];
			let cols = self.cols.borrow();
			let Some((d, nulls)) = decoded(&cols[k.att]) else { continue };
			for r in range.clone() {
				let (d, isnull) = (d[r], nulls[r]);
				let w = d.value() as u64;
				let v = match kind {
					Kind::Int16 => w as i16 as i64,
					Kind::Int32 => w as i32 as i64,
					_ => w as i64,
				};
				let ok = !isnull
					&& match k.op {
						Op::Lt => v < k.value,
						Op::Le => v <= k.value,
						Op::Eq => v == k.value,
						Op::Ge => v >= k.value,
						Op::Gt => v > k.value,
					};
				keep[r] &= ok;
			}
		}
		for f in floats {
			if f.att >= self.store.kinds.len() {
				continue;
			}
			self.ensure(f.att, range.clone());
			let f4 = self.store.kinds[f.att] == Kind::Float4;
			let cols = self.cols.borrow();
			let Some((d, nulls)) = decoded(&cols[f.att]) else { continue };
			for r in range.clone() {
				let (d, isnull) = (d[r], nulls[r]);
				let w = d.value() as u64;
				let x = if f4 { f32::from_bits(w as u32) as f64 } else { f64::from_bits(w) };
				keep[r] &= !isnull && f.admits(x);
			}
		}
		keep
	}

	/// Puts row `n` (a row of the whole store, inside this group) into a slot, whole.
	///
	/// # Safety
	/// `slot` must be a valid slot for the relation's descriptor.
	pub unsafe fn store(&self, n: u64, slot: *mut pg_sys::TupleTableSlot, relid: pg_sys::Oid) {
		pg_sys::ExecClearTuple(slot);
		let desc = (*slot).tts_tupleDescriptor;
		self.fill(n, 0, (*desc).natts as usize, desc, (*slot).tts_values, (*slot).tts_isnull);
		pg_sys::ExecStoreVirtualTuple(slot);
		(*slot).tts_tid = tid_of_row(n);
		(*slot).tts_tableOid = relid;
	}

	/// Puts row `n` into a slot without decoding anything: the slot decodes the columns the
	/// executor asks for, when it asks (super::slot). Any other kind of slot gets the row whole.
	///
	/// # Safety
	/// `slot` must be a valid slot for the relation's descriptor.
	pub unsafe fn store_lazy(self: &Rc<Group>, n: u64, slot: *mut pg_sys::TupleTableSlot, relid: pg_sys::Oid) {
		if !super::slot::is_lazy(slot) {
			self.decode(None);
			self.store(n, slot, relid);
			return;
		}
		super::slot::point(slot, Rc::downgrade(self), n);
		(*slot).tts_tid = tid_of_row(n);
		(*slot).tts_tableOid = relid;
	}
}

// ---- what a column store holds, per column ----


/// What a sealed partition's column store holds, per column and encoding: how many row groups
/// and rows, how many nulls, and the bytes its chunks take as stored (encoded, then compressed
/// with the store's codec). Read from the row groups' headers only, so nothing is decoded; a
/// tiered partition's row groups are fetched whole. No rows for a relation that is not a column
/// store (a live partition, the delta store). Needs SELECT on the relation. A column whose
/// chunks use more than one encoding (a dictionary where values repeat, plain bytes where they do
/// not) has a row per encoding. PLAN.md Phase 3, claim 2: the compression per encoding per type.
#[pg_extern(stable, parallel_safe)]
fn column_sizes(
	relation: pgrx::PgRelation,
) -> TableIterator<
	'static,
	(
		name!(attname, String),
		name!(type_name, String),
		name!(encoding, String),
		name!(row_groups, i64),
		name!(rows, i64),
		name!(nulls, i64),
		name!(stored_bytes, i64),
	),
> {
	let mut out: Vec<(String, String, String, i64, i64, i64, i64)> = Vec::new();
	unsafe {
		if pg_sys::pg_class_aclcheck(relation.oid(), pg_sys::GetUserId(), pg_sys::ACL_SELECT as pg_sys::AclMode) != pg_sys::AclResult::ACLCHECK_OK {
			ereport!(
				PgLogLevel::ERROR,
				PgSqlErrorCode::ERRCODE_INSUFFICIENT_PRIVILEGE,
				format!("permission denied for table {}", relation.name())
			);
		}
		// opened by the argument's conversion, with AccessShareLock, and closed when it drops
		let r = relation.as_ptr();
		let am = (*(*r).rd_rel).relam;
		let columnar = [c"snouttime_columnar", c"snouttime_tiered"]
			.iter()
			.any(|n| { let o = pg_sys::get_am_oid(n.as_ptr(), true); o != pg_sys::InvalidOid && o == am });
		if columnar {
			let store = Rc::new(Store::open(r));
			let desc = (*r).rd_att;
			// (attribute, encoding) -> row groups, rows, nulls, bytes; in attribute order
			let mut sums: Vec<((usize, u8), [i64; 4])> = Vec::new();
			for g in 0..store.dir.len() {
				let group = Store::load(&store, g);
				for (i, m) in group.chunks().iter().enumerate() {
					let key = (i, m.encoding);
					let at = match sums.iter().position(|x| x.0 == key) {
						Some(p) => p,
						None => {
							sums.push((key, [0; 4]));
							sums.len() - 1
						}
					};
					let t = &mut sums[at].1;
					t[0] += 1;
					t[1] += group.rows as i64;
					t[2] += m.nulls as i64;
					t[3] += m.length as i64;
				}
			}
			sums.sort_by_key(|x| x.0);
			let atts = types::attrs(desc);
			for ((i, enc), t) in sums {
				// a column dropped since the seal is still in the store, and is not reported
				let Some(att) = atts.get(i) else { continue };
				if att.attisdropped {
					continue;
				}
				let name = std::ffi::CStr::from_ptr(att.attname.data.as_ptr()).to_string_lossy().into_owned();
				let typ = std::ffi::CStr::from_ptr(pg_sys::format_type_be(att.atttypid)).to_string_lossy().into_owned();
				let encoding = match enc {
					format::ENC_INT | format::ENC_INT_PAGED => "integer",
					format::ENC_FLOAT | format::ENC_FLOAT_PAGED => "float",
					format::ENC_BOOL => "bool",
					format::ENC_DICT => "dictionary",
					format::ENC_PLAIN => "plain",
					_ => "unknown",
				};
				out.push((name, typ, encoding.to_string(), t[0], t[1], t[2], t[3]));
			}
		}
	}
	TableIterator::new(out)
}

// ---- side tables: snouttime_internal.delta_<oid> and deletes_<oid> ----

/// The delta store's and delete log's OIDs, if the relation has them.
pub fn side_tables(relid: pg_sys::Oid) -> (pg_sys::Oid, pg_sys::Oid) {
	unsafe {
		let nsp = pg_sys::get_namespace_oid(c"snouttime_internal".as_ptr(), true);
		if nsp == pg_sys::InvalidOid {
			return (pg_sys::InvalidOid, pg_sys::InvalidOid);
		}
		let d = std::ffi::CString::new(format!("delta_{}", relid.to_u32())).unwrap();
		let x = std::ffi::CString::new(format!("deletes_{}", relid.to_u32())).unwrap();
		(pg_sys::get_relname_relid(d.as_ptr(), nsp), pg_sys::get_relname_relid(x.as_ptr(), nsp))
	}
}

/// The column-store rows deleted as far as `snapshot` can see, sorted. Lock-only entries are
/// not deletes.
///
/// # Safety
/// `snapshot` must be valid.
pub unsafe fn deleted_rows(deletes: pg_sys::Oid, snapshot: pg_sys::Snapshot) -> Vec<u64> {
	if deletes == pg_sys::InvalidOid {
		return Vec::new();
	}
	let rel = pg_sys::table_open(deletes, pg_sys::AccessShareLock as i32);
	let scan = pg_sys::table_beginscan(rel, snapshot, 0, std::ptr::null_mut());
	let slot = pg_sys::table_slot_create(rel, std::ptr::null_mut());
	let mut out = Vec::new();
	while pg_sys::table_scan_getnextslot(scan, pg_sys::ScanDirection::ForwardScanDirection, slot) {
		pg_sys::slot_getsomeattrs(slot, 2);
		let v = std::slice::from_raw_parts((*slot).tts_values, 2);
		let n = std::slice::from_raw_parts((*slot).tts_isnull, 2);
		// a lock-only entry (SELECT ... FOR UPDATE) is not a delete
		if !n[0] && (n[1] || v[1].value() & 0xff == 0) {
			out.push(v[0].value() as u64);
		}
	}
	pg_sys::ExecDropSingleTupleTableSlot(slot);
	pg_sys::table_endscan(scan);
	pg_sys::table_close(rel, pg_sys::NoLock as i32);
	out.sort_unstable();
	out.dedup();
	out
}

/// The column-store rows in `lo..hi` deleted as far as `snapshot` can see, sorted, read through
/// the delete log's index on row_number; None when the log has no such index. A scan that seeks
/// into a few row groups asks this per group instead of reading the whole log: at 1% of a
/// 10M-row partition deleted, the whole log is 100,000 entries read and sorted for a query that
/// returns ten rows (bench/late.sh, 2026-09-25).
///
/// # Safety
/// `snapshot` must be valid.
pub unsafe fn deleted_in(deletes: pg_sys::Oid, snapshot: pg_sys::Snapshot, lo: u64, hi: u64) -> Option<Vec<u64>> {
	const BTREE_AM_OID: u32 = 403;
	if deletes == pg_sys::InvalidOid {
		return Some(Vec::new());
	}
	let rel = pg_sys::table_open(deletes, pg_sys::AccessShareLock as i32);
	let mut index = std::ptr::null_mut();
	for oid in pgrx::PgList::<std::ffi::c_void>::from_pg(pg_sys::RelationGetIndexList(rel)).iter_oid() {
		let ix = pg_sys::index_open(oid, pg_sys::AccessShareLock as i32);
		let form = &*(*ix).rd_index;
		if (*(*ix).rd_rel).relam.to_u32() == BTREE_AM_OID
			&& form.indisvalid
			&& form.indisready
			&& *form.indkey.values.as_ptr() == 1
			&& *(*ix).rd_opcintype == pg_sys::INT8OID
		{
			index = ix;
			break;
		}
		pg_sys::index_close(ix, pg_sys::AccessShareLock as i32);
	}
	if index.is_null() {
		pg_sys::table_close(rel, pg_sys::NoLock as i32);
		return None;
	}
	let mut keys = [pg_sys::ScanKeyData::default(), pg_sys::ScanKeyData::default()];
	// row_number >= lo AND row_number < hi, int8's own btree operators
	pg_sys::ScanKeyInit(&mut keys[0], 1, 4, pg_sys::Oid::from(pg_sys::F_INT8GE), pg_sys::Datum::from(lo as i64));
	pg_sys::ScanKeyInit(&mut keys[1], 1, 1, pg_sys::Oid::from(pg_sys::F_INT8LT), pg_sys::Datum::from(hi as i64));
	#[cfg(feature = "pg18")]
	let scan = pg_sys::index_beginscan(rel, index, snapshot, std::ptr::null_mut(), 2, 0);
	#[cfg(not(feature = "pg18"))]
	let scan = pg_sys::index_beginscan(rel, index, snapshot, 2, 0);
	pg_sys::index_rescan(scan, keys.as_mut_ptr(), 2, std::ptr::null_mut(), 0);
	let slot = pg_sys::table_slot_create(rel, std::ptr::null_mut());
	let mut out = Vec::new();
	while pg_sys::index_getnext_slot(scan, pg_sys::ScanDirection::ForwardScanDirection, slot) {
		pg_sys::slot_getsomeattrs(slot, 2);
		let v = std::slice::from_raw_parts((*slot).tts_values, 2);
		let n = std::slice::from_raw_parts((*slot).tts_isnull, 2);
		// a lock-only entry (SELECT ... FOR UPDATE) is not a delete
		if !n[0] && (n[1] || v[1].value() & 0xff == 0) {
			out.push(v[0].value() as u64);
		}
	}
	pg_sys::index_endscan(scan);
	pg_sys::ExecDropSingleTupleTableSlot(slot);
	pg_sys::index_close(index, pg_sys::AccessShareLock as i32);
	pg_sys::table_close(rel, pg_sys::NoLock as i32);
	out.sort_unstable();
	out.dedup();
	Some(out)
}

/// The delta store's rows for a scan: all of them, or, when the relation's non-unique indexes
/// hold only these rows (a store sealed without keep_indexes) and one of them leads with a
/// column the scan's keys compare, only those the index finds. The late-rows index is exactly
/// an index on the delta store, and scanning the whole store for one device was most of a
/// point query on a partition with late rows (IoT i3: 3.2 ms against 0.8, 2026-09-23).
pub struct DeltaScan {
	heap: pg_sys::Relation,
	heap_scan: pg_sys::TableScanDesc,
	index: pg_sys::Relation,
	index_scan: pg_sys::IndexScanDesc,
	pub slot: *mut pg_sys::TupleTableSlot,
	/// the index's search keys, alive as long as the scan
	_scankeys: Vec<pg_sys::ScanKeyData>,
}

impl DeltaScan {
	/// None when the relation has no delta store.
	///
	/// # Safety
	/// `rel` must be open, and stay open while the scan is used.
	pub unsafe fn begin(rel: pg_sys::Relation, delta: pg_sys::Oid, snapshot: pg_sys::Snapshot, keys: &[super::scan::Key]) -> Option<DeltaScan> {
		if delta == pg_sys::InvalidOid {
			return None;
		}
		if !keys.is_empty() && super::am::late_indexes(rel) {
			if let Some(s) = Self::through_index(rel, snapshot, keys) {
				return Some(s);
			}
		}
		let heap = pg_sys::table_open(delta, pg_sys::AccessShareLock as i32);
		Some(DeltaScan {
			heap,
			heap_scan: pg_sys::table_beginscan(heap, snapshot, 0, std::ptr::null_mut()),
			index: std::ptr::null_mut(),
			index_scan: std::ptr::null_mut(),
			slot: pg_sys::table_slot_create(heap, std::ptr::null_mut()),
			_scankeys: Vec::new(),
		})
	}

	unsafe fn through_index(rel: pg_sys::Relation, snapshot: pg_sys::Snapshot, keys: &[super::scan::Key]) -> Option<DeltaScan> {
		use super::scan::Op;
		const BTREE_AM_OID: u32 = 403;
		let atts = types::attrs((*rel).rd_att);
		for oid in pgrx::PgList::<std::ffi::c_void>::from_pg(pg_sys::RelationGetIndexList(rel)).iter_oid() {
			let index = pg_sys::index_open(oid, pg_sys::AccessShareLock as i32);
			let ix = &*(*index).rd_index;
			let first = *ix.indkey.values.as_ptr();
			let usable = (*(*index).rd_rel).relam.to_u32() == BTREE_AM_OID
				&& !ix.indisunique
				&& !ix.indisexclusion
				&& ix.indisvalid
				&& ix.indisready
				&& first > 0;
			let mut scankeys: Vec<pg_sys::ScanKeyData> = Vec::new();
			if usable {
				let att = first as usize - 1;
				let typ = atts[att].atttypid;
				let opfamily = *(*index).rd_opfamily;
				let opcintype = *(*index).rd_opcintype;
				for k in keys.iter().filter(|k| k.att == att) {
					// a value outside the column's type cannot be passed to its operator; the
					// whole delta store is read instead
					let fits = match typ {
						pg_sys::INT2OID => i16::try_from(k.value).is_ok(),
						pg_sys::INT4OID | pg_sys::DATEOID => i32::try_from(k.value).is_ok(),
						_ => true,
					};
					let strategy: i16 = match k.op {
						Op::Lt => 1,
						Op::Le => 2,
						Op::Eq => 3,
						Op::Ge => 4,
						Op::Gt => 5,
					};
					let opno = pg_sys::get_opfamily_member(opfamily, opcintype, opcintype, strategy);
					if !fits || opcintype != typ || opno == pg_sys::InvalidOid {
						scankeys.clear();
						break;
					}
					let mut sk = pg_sys::ScanKeyData::default();
					pg_sys::ScanKeyInit(&mut sk, 1, strategy as u16, pg_sys::get_opcode(opno), pg_sys::Datum::from(k.value as usize));
					scankeys.push(sk);
				}
			}
			if scankeys.is_empty() {
				pg_sys::index_close(index, pg_sys::AccessShareLock as i32);
				continue;
			}
			#[cfg(feature = "pg18")]
			let scan = pg_sys::index_beginscan(rel, index, snapshot, std::ptr::null_mut(), scankeys.len() as i32, 0);
			#[cfg(not(feature = "pg18"))]
			let scan = pg_sys::index_beginscan(rel, index, snapshot, scankeys.len() as i32, 0);
			pg_sys::index_rescan(scan, scankeys.as_mut_ptr(), scankeys.len() as i32, std::ptr::null_mut(), 0);
			return Some(DeltaScan {
				heap: std::ptr::null_mut(),
				heap_scan: std::ptr::null_mut(),
				index,
				index_scan: scan,
				slot: pg_sys::table_slot_create(rel, std::ptr::null_mut()),
				_scankeys: scankeys,
			});
		}
		None
	}

	/// The next row into `self.slot`, its TID the delta-store TID; false at the end.
	///
	/// # Safety
	/// The scan must not have ended.
	pub unsafe fn next(&mut self) -> bool {
		if !self.index_scan.is_null() {
			// the relation's own fetch returns delta rows with their delta TIDs already
			return pg_sys::index_getnext_slot(self.index_scan, pg_sys::ScanDirection::ForwardScanDirection, self.slot);
		}
		if !pg_sys::table_scan_getnextslot(self.heap_scan, pg_sys::ScanDirection::ForwardScanDirection, self.slot) {
			return false;
		}
		to_delta_tid(&mut (*self.slot).tts_tid);
		true
	}

	/// # Safety
	/// Once, after the last `next`.
	pub unsafe fn end(&mut self) {
		if !self.index_scan.is_null() {
			pg_sys::index_endscan(self.index_scan);
			pg_sys::index_close(self.index, pg_sys::AccessShareLock as i32);
			self.index_scan = std::ptr::null_mut();
		}
		if !self.heap_scan.is_null() {
			pg_sys::table_endscan(self.heap_scan);
			pg_sys::table_close(self.heap, pg_sys::NoLock as i32);
			self.heap_scan = std::ptr::null_mut();
		}
		if !self.slot.is_null() {
			pg_sys::ExecDropSingleTupleTableSlot(self.slot);
			self.slot = std::ptr::null_mut();
		}
	}
}
