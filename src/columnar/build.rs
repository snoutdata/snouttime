//! Building a column store: rows given to a relation whose
//! relfilenode was created in this transaction are buffered, sorted by
//! `snouttime.columnar_order_by`, and encoded into row groups when the buffer is flushed.
//!
//! That is how every path that fills a new column store works: `ALTER TABLE ... SET ACCESS
//! METHOD` (and any other rewrite) inserts every row into a transient relation, `CREATE TABLE
//! AS` and `COPY` into a table created in the same transaction do the same. The buffer is
//! flushed at `finish_bulk_insert`, before any scan of the relation, and at the latest just
//! before commit, because a plain `INSERT` never calls `finish_bulk_insert`. After a flush the
//! column store is final: later rows in the same transaction go to the delta store.
//!
//! The buffer is a Postgres tuplesort (or a tuplestore when there is no order), created
//! under the top transaction's memory context and resource owner, so it spills to disk past
//! `maintenance_work_mem` and survives the statement that started it.

use std::cell::RefCell;

use pgrx::pg_sys;
use pgrx::prelude::*;

use super::format::{self, GroupEntry, Meta, MinMax, Remote};
use super::store::{FileSink, Sink, Writer};
use super::types::{self, Acc, Kind};
use crate::codec::compress::Codec;

pub struct Builder {
	relid: pg_sys::Oid,
	relnumber: pg_sys::RelFileNumber,
	sort: *mut pg_sys::Tuplesortstate,
	store: *mut pg_sys::Tuplestorestate,
	rows: u64,
	/// the attributes the rows are sorted by, and the metapage's flags
	order: Vec<u16>,
	flags: u8,
	/// Rows were added inside a subtransaction that later aborted: they cannot be taken back
	/// out of the buffer, so the flush refuses rather than write them.
	poisoned: bool,
	subxact: pg_sys::SubTransactionId,
}

thread_local! {
	static BUILDERS: RefCell<Vec<Builder>> = const { RefCell::new(Vec::new()) };
	/// Relations flushed this transaction: their column store is final.
	static FLUSHED: RefCell<Vec<(pg_sys::Oid, pg_sys::RelFileNumber)>> = const { RefCell::new(Vec::new()) };
}


/// Does a row inserted now go into the buffer (true) or the delta store (false)?
///
/// # Safety
/// `rel` must be an open relation.
pub unsafe fn buffers(rel: pg_sys::Relation) -> bool {
	let r = &*rel;
	let new_here = r.rd_createSubid != 0 || r.rd_firstRelfilelocatorSubid != 0;
	// A relation with indexes gets index entries as each row goes in, pointing at its TID;
	// a buffered row has no final TID until the flush, so it goes to the delta store instead.
	let key = (r.rd_id, r.rd_locator.relNumber);
	new_here && !(*r.rd_rel).relhasindex && !FLUSHED.with(|f| f.borrow().contains(&key))
}

/// The sort order and flags for `rel`'s new column store: `snouttime.columnar_order_by`, a
/// comma-separated list of column names, and `snouttime.columnar_keep_indexes`. When the order
/// setting is empty and `rel` is the transient table of a rewrite of a column store (VACUUM
/// FULL, CLUSTER, an ALTER TABLE that rewrites), the old store's order and flags carry over, so
/// a rewrite never loses the order a seal chose. Otherwise empty keeps the order rows arrive in.
unsafe fn order_by(rel: pg_sys::Relation) -> (Vec<(i16, pg_sys::Oid, pg_sys::Oid)>, u8) {
	let desc = (*rel).rd_att;
	let setting = super::ORDER_BY.get().map(|s| s.to_string_lossy().into_owned()).unwrap_or_default();
	let atts = types::attrs(desc);
	let mut flags = if super::KEEP_INDEXES.get() { 0 } else { format::FLAG_LATE_INDEXES };
	let mut attnums: Vec<i16> = Vec::new();
	for name in setting.split(',').map(str::trim).filter(|s| !s.is_empty()) {
		let Some(att) = atts.iter().find(|a| !a.attisdropped && pgrx::name_data_to_str(&a.attname) == name) else {
			error!("snouttime.columnar_order_by names a column \"{name}\" that the table does not have");
		};
		attnums.push(att.attnum);
	}
	if attnums.is_empty() {
		if let Some(old) = relname(rel).strip_prefix("pg_temp_").and_then(|s| s.parse::<u32>().ok()) {
			if let Some((order, old_flags)) = super::read::order_of(pg_sys::Oid::from(old)) {
				flags = old_flags;
				attnums = order
					.into_iter()
					.map(|a| a as i16)
					.take_while(|&a| atts.get(a as usize - 1).is_some_and(|x| !x.attisdropped))
					.collect();
			}
		}
	}
	let mut keys = Vec::new();
	for a in attnums {
		let att = &atts[a as usize - 1];
		let tc = pg_sys::lookup_type_cache(att.atttypid, pg_sys::TYPECACHE_LT_OPR as i32);
		if (*tc).lt_opr == pg_sys::InvalidOid {
			error!(
				"column \"{}\" has no default ordering, so a column store cannot be sorted by it",
				pgrx::name_data_to_str(&att.attname)
			);
		}
		keys.push((att.attnum, (*tc).lt_opr, att.attcollation));
	}
	(keys, flags)
}

/// Buffers one row for `rel`'s new column store.
///
/// # Safety
/// `rel` and `slot` must be valid, and `buffers(rel)` true.
pub unsafe fn insert(rel: pg_sys::Relation, slot: *mut pg_sys::TupleTableSlot) {
	let r = &*rel;
	let key = (r.rd_id, r.rd_locator.relNumber);
	BUILDERS.with(|b| {
		let mut b = b.borrow_mut();
		// A relation whose relfilenode changed since (a TRUNCATE) starts again.
		b.retain(|x| x.relid != key.0 || x.relnumber == key.1);
		if !b.iter().any(|x| x.relid == key.0) {
			b.push(begin(rel));
		}
		let builder = b.iter_mut().find(|x| x.relid == key.0).unwrap();
		let old_cxt = pg_sys::MemoryContextSwitchTo(pg_sys::TopTransactionContext);
		let old_owner = pg_sys::CurrentResourceOwner;
		pg_sys::CurrentResourceOwner = pg_sys::TopTransactionResourceOwner;
		if builder.sort.is_null() {
			pg_sys::tuplestore_puttupleslot(builder.store, slot);
		} else {
			pg_sys::tuplesort_puttupleslot(builder.sort, slot);
		}
		pg_sys::CurrentResourceOwner = old_owner;
		pg_sys::MemoryContextSwitchTo(old_cxt);
		builder.rows += 1;
		builder.subxact = builder.subxact.max(pg_sys::GetCurrentSubTransactionId());
	});
}

unsafe fn begin(rel: pg_sys::Relation) -> Builder {
	let desc = (*rel).rd_att;
	let (keys, flags) = order_by(rel);
	let old_cxt = pg_sys::MemoryContextSwitchTo(pg_sys::TopTransactionContext);
	let old_owner = pg_sys::CurrentResourceOwner;
	pg_sys::CurrentResourceOwner = pg_sys::TopTransactionResourceOwner;
	let (sort, store) = if keys.is_empty() {
		(std::ptr::null_mut(), pg_sys::tuplestore_begin_heap(false, false, pg_sys::maintenance_work_mem))
	} else {
		let mut attnums: Vec<i16> = keys.iter().map(|k| k.0).collect();
		let mut ops: Vec<pg_sys::Oid> = keys.iter().map(|k| k.1).collect();
		let mut colls: Vec<pg_sys::Oid> = keys.iter().map(|k| k.2).collect();
		let mut nulls_first = vec![false; keys.len()];
		let s = {
			pg_sys::tuplesort_begin_heap(
				desc,
				keys.len() as i32,
				attnums.as_mut_ptr(),
				ops.as_mut_ptr(),
				colls.as_mut_ptr(),
				nulls_first.as_mut_ptr(),
				pg_sys::maintenance_work_mem,
				std::ptr::null_mut(),
				pg_sys::TUPLESORT_NONE as i32,
			)
		};
		(s, std::ptr::null_mut())
	};
	pg_sys::CurrentResourceOwner = old_owner;
	pg_sys::MemoryContextSwitchTo(old_cxt);
	Builder {
		relid: (*rel).rd_id,
		relnumber: (*rel).rd_locator.relNumber,
		sort,
		store,
		rows: 0,
		order: keys.iter().map(|k| k.0 as u16).collect(),
		flags,
		poisoned: false,
		subxact: 0,
	}
}

/// Writes `rel`'s buffered rows as its column store, if it has any buffered, and marks the
/// column store final. Called by `finish_bulk_insert`, before a scan, and before commit.
///
/// # Safety
/// `rel` must be open.
pub unsafe fn flush(rel: pg_sys::Relation) {
	let key = ((*rel).rd_id, (*rel).rd_locator.relNumber);
	let taken = BUILDERS.with(|b| {
		let mut b = b.borrow_mut();
		let i = b.iter().position(|x| x.relid == key.0 && x.relnumber == key.1)?;
		Some(b.swap_remove(i))
	});
	let Some(builder) = taken else {
		return;
	};
	FLUSHED.with(|f| f.borrow_mut().push(key));
	if builder.poisoned {
		error!(
			"rows were inserted into the new column store \"{}\" in a subtransaction that rolled back, and a column store cannot take them back out",
			relname(rel)
		);
	}
	write(rel, &builder);
	let old_owner = pg_sys::CurrentResourceOwner;
	pg_sys::CurrentResourceOwner = pg_sys::TopTransactionResourceOwner;
	if builder.sort.is_null() {
		pg_sys::tuplestore_end(builder.store);
	} else {
		pg_sys::tuplesort_end(builder.sort);
	}
	pg_sys::CurrentResourceOwner = old_owner;
}

pub unsafe fn relname(rel: pg_sys::Relation) -> String {
	pgrx::name_data_to_str(&(*(*rel).rd_rel).relname).to_string()
}

fn codec() -> Codec {
	match super::COMPRESSION.get() {
		super::Compression::None => Codec::None,
		super::Compression::Lz4 => Codec::Lz4,
		super::Compression::Zstd => Codec::Zstd,
	}
}

unsafe fn write(rel: pg_sys::Relation, builder: &Builder) {
	let desc = (*rel).rd_att;
	let atts = types::attrs(desc);
	let kinds: Vec<Kind> = atts.iter().map(types::kind_of).collect();
	let group_rows = super::GROUP_ROWS.get() as usize;
	let codec = codec();
	let slot = pg_sys::MakeSingleTupleTableSlot(desc, &pg_sys::TTSOpsMinimalTuple);
	let cxt = pg_sys::AllocSetContextCreateInternal(
		pg_sys::CurrentMemoryContext,
		c"snouttime column store build".as_ptr(),
		0,
		8 * 1024,
		8 * 1024 * 1024,
	);
	let old_owner = pg_sys::CurrentResourceOwner;
	pg_sys::CurrentResourceOwner = pg_sys::TopTransactionResourceOwner;
	if !builder.sort.is_null() {
		pg_sys::tuplesort_performsort(builder.sort);
	}
	pg_sys::CurrentResourceOwner = old_owner;

	// A tiered table writes its row groups to a file that becomes an object
	// in S3; everything else, its own pages.
	let tier_to = if super::is_tiered(rel) { Some(super::tier_location(rel)) } else { None };
	let mut file: Option<FileSink> = None;
	let mut pages: Option<Writer> = None;
	match &tier_to {
		Some(_) => file = Some(FileSink::create(&format!("snouttime_tier_{}_{}.tmp", std::process::id(), (*rel).rd_locator.relNumber.to_u32()))),
		None => pages = Some(Writer::start(rel)),
	}
	let writer: &mut dyn Sink = match (&mut file, &mut pages) {
		(Some(f), _) => f,
		(_, Some(w)) => w,
		_ => unreachable!(),
	};
	let mut accs: Vec<Acc> = kinds.iter().map(|&k| Acc::new(k)).collect();
	let mut groups: Vec<GroupEntry> = Vec::new();
	let mut rows_in_group = 0usize;
	let mut total = 0u64;
	let order = &builder.order;
	// every sort column but the last: where its runs end inside a group is recorded (format.rs)
	let prefix: Vec<usize> = order.iter().take(order.len().saturating_sub(1)).map(|&a| a as usize - 1).collect();
	let no_runs = || if order.len() >= 2 { Some(Vec::new()) } else { None };
	let mut run_ends: Option<Vec<(u32, Vec<Option<Vec<u8>>>)>> = no_runs();
	let emit = |accs: &mut Vec<Acc>,
	            rows: usize,
	            writer: &mut dyn Sink,
	            groups: &mut Vec<GroupEntry>,
	            total: &mut u64,
	            run_ends: Option<Vec<(u32, Vec<Option<Vec<u8>>>)>>| {
		let first_key = order.iter().map(|&a| accs[a as usize - 1].key_bytes(true)).collect();
		let last_key = order.iter().map(|&a| accs[a as usize - 1].key_bytes(false)).collect();
		let cols: Vec<format::Column> = accs.iter_mut().map(Acc::take).collect();
		let (bytes, metas) = match format::encode_group(&cols, rows as u64, codec) {
			Ok(x) => x,
			Err(e) => error!("could not encode a row group of \"{}\": {e}", relname(rel)),
		};
		groups.push(GroupEntry {
			offset: writer.offset(),
			length: bytes.len() as u64,
			first_row: *total,
			rows: rows as u64,
			minmax: metas.iter().map(|m| m.minmax).collect::<Vec<Option<MinMax>>>(),
			first_key,
			last_key,
			run_ends,
		});
		writer.write(&bytes);
		*total += rows as u64;
	};
	loop {
		let old_owner = pg_sys::CurrentResourceOwner;
		pg_sys::CurrentResourceOwner = pg_sys::TopTransactionResourceOwner;
		let got = if builder.sort.is_null() {
			pg_sys::tuplestore_gettupleslot(builder.store, true, false, slot)
		} else {
			pg_sys::tuplesort_gettupleslot(builder.sort, true, false, slot, std::ptr::null_mut())
		};
		pg_sys::CurrentResourceOwner = old_owner;
		if !got {
			break;
		}
		let old = pg_sys::MemoryContextSwitchTo(cxt);
		pg_sys::slot_getallattrs(slot);
		let values = std::slice::from_raw_parts((*slot).tts_values, atts.len());
		let nulls = std::slice::from_raw_parts((*slot).tts_isnull, atts.len());
		for (i, acc) in accs.iter_mut().enumerate() {
			acc.push(values[i], nulls[i]);
		}
		pg_sys::MemoryContextSwitchTo(old);
		// a new run of the leading sort columns starts at this row: the one before ended one
		if rows_in_group > 0 && !prefix.is_empty() && prefix.iter().any(|&a| !accs[a].last_two_equal()) {
			if let Some(runs) = run_ends.as_mut() {
				if runs.len() == format::MAX_RUNS {
					run_ends = None;
				} else {
					let key = order.iter().map(|&a| accs[a as usize - 1].key_bytes_prev()).collect();
					runs.push(((rows_in_group - 1) as u32, key));
				}
			}
		}
		rows_in_group += 1;
		let bytes: usize = accs.iter().map(|a| a.bytes).sum();
		if rows_in_group == group_rows || bytes >= format::MAX_GROUP_BYTES {
			emit(&mut accs, rows_in_group, writer, &mut groups, &mut total, std::mem::replace(&mut run_ends, no_runs()));
			rows_in_group = 0;
			pg_sys::MemoryContextReset(cxt);
		}
	}
	if rows_in_group > 0 {
		emit(&mut accs, rows_in_group, writer, &mut groups, &mut total, run_ends.take());
	}
	debug_assert_eq!(total, builder.rows);
	let dir = format::encode_directory(&groups);
	let (mut writer, dir_offset, remote) = match (file, tier_to) {
		(Some(f), Some(loc)) => {
			// the row groups go up as one object; the directory and the metapage stay here, so
			// planning and row-group skipping never touch S3
			let length = f.offset();
			let path = f.finish();
			let cfg = super::s3_config();
			let put = crate::s3::put_file(&cfg, &loc, &path);
			let _ = std::fs::remove_file(&path);
			if let Err(e) = put {
				error!("could not tier \"{}\": {e}", relname(rel));
			}
			(Writer::start(rel), 0, Some(Remote { url: loc.url(), length }))
		}
		(_, _) => {
			let w = pages.take().unwrap();
			let at = w.offset;
			(w, at, None)
		}
	};
	writer.write(&dir);
	let meta = Meta {
		rows: total,
		groups: groups.len() as u64,
		codec: codec as u8,
		dir_offset,
		dir_length: dir.len() as u64,
		dir_crc: format::crc32c(&dir),
		types: atts.iter().map(|a| if a.attisdropped { 0 } else { a.atttypid.to_u32() }).collect(),
		remote,
		order: builder.order.clone(),
		flags: builder.flags,
	};
	writer.finish(&meta);
	pg_sys::ExecDropSingleTupleTableSlot(slot);
	pg_sys::MemoryContextDelete(cxt);
}

/// Before commit: every buffer is written. At abort: every buffer is forgotten (its memory
/// and temporary files go with the transaction).
#[pg_guard]
pub unsafe extern "C-unwind" fn xact_callback(event: pg_sys::XactEvent::Type, _arg: *mut std::ffi::c_void) {
	match event {
		pg_sys::XactEvent::XACT_EVENT_PRE_COMMIT
		| pg_sys::XactEvent::XACT_EVENT_PARALLEL_PRE_COMMIT
		| pg_sys::XactEvent::XACT_EVENT_PRE_PREPARE => {
			let pending: Vec<pg_sys::Oid> = BUILDERS.with(|b| b.borrow().iter().map(|x| x.relid).collect());
			for relid in pending {
				let rel = pg_sys::RelationIdGetRelation(relid);
				if rel.is_null() {
					// created and dropped in this transaction
					BUILDERS.with(|b| b.borrow_mut().retain(|x| x.relid != relid));
					continue;
				}
				flush(rel);
				pg_sys::RelationClose(rel);
			}
			BUILDERS.with(|b| b.borrow_mut().clear());
		}
		pg_sys::XactEvent::XACT_EVENT_ABORT | pg_sys::XactEvent::XACT_EVENT_PARALLEL_ABORT => {
			BUILDERS.with(|b| b.borrow_mut().clear());
			FLUSHED.with(|f| f.borrow_mut().clear());
		}
		pg_sys::XactEvent::XACT_EVENT_COMMIT
		| pg_sys::XactEvent::XACT_EVENT_PARALLEL_COMMIT
		| pg_sys::XactEvent::XACT_EVENT_PREPARE => {
			FLUSHED.with(|f| f.borrow_mut().clear());
		}
		_ => {}
	}
}

#[pg_guard]
pub unsafe extern "C-unwind" fn subxact_callback(
	event: pg_sys::SubXactEvent::Type,
	my_subid: pg_sys::SubTransactionId,
	_parent: pg_sys::SubTransactionId,
	_arg: *mut std::ffi::c_void,
) {
	if event == pg_sys::SubXactEvent::SUBXACT_EVENT_ABORT_SUB {
		BUILDERS.with(|b| {
			for x in b.borrow_mut().iter_mut() {
				if x.subxact >= my_subid {
					x.poisoned = true;
				}
			}
		});
	}
}
