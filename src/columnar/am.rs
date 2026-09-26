//! The table access method's callbacks (docs/snouttime/COLUMNAR.md §4-§7).
//!
//! Every callback is `#[pg_guard]`: a Rust panic becomes a Postgres ERROR, never a crash.
//! Column-store rows come back in virtual slots; delta-store rows are read through the heap
//! access method and copied into the same slot type, with their TID marked (`read::DELTA_BIT`).

use std::ffi::c_void;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use std::rc::Rc;

use pgrx::pg_sys;
use pgrx::prelude::*;
use pgrx::PgMemoryContexts;

use super::build;
use super::read::{self, classify, Group, Store, Tid};
use super::store::Reader;


/// Switches the current memory context until dropped.
struct Switch(pg_sys::MemoryContext);

impl Switch {
	unsafe fn to(cxt: pg_sys::MemoryContext) -> Switch {
		Switch(pg_sys::MemoryContextSwitchTo(cxt))
	}
}

impl Drop for Switch {
	fn drop(&mut self) {
		unsafe {
			pg_sys::MemoryContextSwitchTo(self.0);
		}
	}
}

unsafe fn new_cxt(name: &'static std::ffi::CStr) -> pg_sys::MemoryContext {
	pg_sys::AllocSetContextCreateInternal(pg_sys::CurrentMemoryContext, name.as_ptr(), 0, 8 * 1024, 8 * 1024 * 1024)
}

// ---------------------------------------------------------------------------------------
// Sequential and parallel scans
// ---------------------------------------------------------------------------------------

#[repr(C)]
struct ScanDesc {
	base: pg_sys::TableScanDescData,
	state: *mut ScanState,
}

#[repr(C)]
struct ParallelDesc {
	base: pg_sys::ParallelTableScanDescData,
	next_group: AtomicU64,
	delta_claimed: AtomicU32,
}

#[derive(PartialEq)]
enum Phase {
	Column,
	Delta,
	Done,
}

struct ScanState {
	store: Rc<Store>,
	deleted: Vec<u64>,
	cxt: pg_sys::MemoryContext,
	group: Option<Rc<Group>>,
	row: u64,
	next_group: usize,
	phase: Phase,
	delta_oid: pg_sys::Oid,
	delta_rel: pg_sys::Relation,
	delta_scan: pg_sys::TableScanDesc,
	delta_slot: *mut pg_sys::TupleTableSlot,
	parallel: *mut ParallelDesc,
	analyze: Vec<usize>,
	/// The context the scan began in. What the scan opens lazily is allocated here: a
	/// caller may call getnextslot in a context it resets every row (ALTER TABLE's rewrite
	/// does), which freed the delta scan under us once (2026-09-23).
	home: pg_sys::MemoryContext,
}

impl ScanState {
	unsafe fn end_delta(&mut self) {
		if !self.delta_scan.is_null() {
			pg_sys::table_endscan(self.delta_scan);
			self.delta_scan = std::ptr::null_mut();
		}
		if !self.delta_slot.is_null() {
			pg_sys::ExecDropSingleTupleTableSlot(self.delta_slot);
			self.delta_slot = std::ptr::null_mut();
		}
		if !self.delta_rel.is_null() {
			pg_sys::table_close(self.delta_rel, pg_sys::NoLock as i32);
			self.delta_rel = std::ptr::null_mut();
		}
	}
}

#[pg_guard]
unsafe extern "C-unwind" fn slot_callbacks(_rel: pg_sys::Relation) -> *const pg_sys::TupleTableSlotOps {
	super::slot::slot_ops()
}

#[pg_guard]
unsafe extern "C-unwind" fn scan_begin(
	rel: pg_sys::Relation,
	snapshot: pg_sys::Snapshot,
	nkeys: i32,
	key: *mut pg_sys::ScanKeyData,
	pscan: pg_sys::ParallelTableScanDesc,
	flags: u32,
) -> pg_sys::TableScanDesc {
	// rows buffered for a column store built in this transaction are written first
	build::flush(rel);
	let desc = pg_sys::palloc0(std::mem::size_of::<ScanDesc>()) as *mut ScanDesc;
	let b = &mut (*desc).base;
	b.rs_rd = rel;
	b.rs_snapshot = snapshot;
	b.rs_nkeys = nkeys;
	b.rs_key = key;
	b.rs_flags = flags;
	b.rs_parallel = pscan;
	let (delta_oid, deletes_oid) = read::side_tables((*rel).rd_id);
	let analyze = flags & pg_sys::ScanOptions::SO_TYPE_ANALYZE != 0;
	let deleted = if snapshot.is_null() { Vec::new() } else { read::deleted_rows(deletes_oid, snapshot) };
	let state = ScanState {
		store: Rc::new(Store::open(rel)),
		deleted,
		cxt: new_cxt(c"snouttime column scan"),
		group: None,
		row: 0,
		next_group: 0,
		phase: Phase::Column,
		delta_oid: if analyze { pg_sys::InvalidOid } else { delta_oid },
		delta_rel: std::ptr::null_mut(),
		delta_scan: std::ptr::null_mut(),
		delta_slot: std::ptr::null_mut(),
		parallel: pscan as *mut ParallelDesc,
		analyze: Vec::new(),
		home: pg_sys::CurrentMemoryContext,
	};
	(*desc).state = PgMemoryContexts::CurrentMemoryContext.leak_and_drop_on_delete(state);
	desc as pg_sys::TableScanDesc
}

#[pg_guard]
unsafe extern "C-unwind" fn scan_end(scan: pg_sys::TableScanDesc) {
	let desc = scan as *mut ScanDesc;
	let s = &mut *(*desc).state;
	s.end_delta();
	pg_sys::MemoryContextDelete(s.cxt);
	s.cxt = std::ptr::null_mut();
	s.group = None;
	if (*scan).rs_flags & pg_sys::ScanOptions::SO_TEMP_SNAPSHOT != 0 {
		pg_sys::UnregisterSnapshot((*scan).rs_snapshot);
	}
}

#[pg_guard]
unsafe extern "C-unwind" fn scan_rescan(
	scan: pg_sys::TableScanDesc,
	_key: *mut pg_sys::ScanKeyData,
	_set_params: bool,
	_allow_strat: bool,
	_allow_sync: bool,
	_allow_pagemode: bool,
) {
	let s = &mut *(*(scan as *mut ScanDesc)).state;
	s.end_delta();
	s.group = None;
	s.row = 0;
	s.next_group = 0;
	s.phase = Phase::Column;
}

unsafe fn next_row(scan: pg_sys::TableScanDesc, slot: *mut pg_sys::TupleTableSlot) -> bool {
	let s = &mut *(*(scan as *mut ScanDesc)).state;
	let relid = (*(*scan).rs_rd).rd_id;
	loop {
		match s.phase {
			Phase::Column => {
				if let Some(g) = &s.group {
					while s.row < g.first_row + g.rows {
						let n = s.row;
						s.row += 1;
						if s.deleted.binary_search(&n).is_ok() {
							continue;
						}
						g.store_lazy(n, slot, relid);
						return true;
					}
				}
				let next = if s.parallel.is_null() {
					let n = s.next_group;
					s.next_group += 1;
					n
				} else {
					(*s.parallel).next_group.fetch_add(1, Ordering::SeqCst) as usize
				};
				if next < s.store.dir.len() {
					s.group = None;
					let g = Store::load(&s.store, next);
					s.row = g.first_row;
					s.group = Some(Rc::new(g));
				} else {
					s.group = None;
					s.phase = Phase::Delta;
				}
			}
			Phase::Delta => {
				if s.delta_scan.is_null() {
					let mine = s.parallel.is_null()
						|| (*s.parallel).delta_claimed.compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst).is_ok();
					if !mine || s.delta_oid == pg_sys::InvalidOid {
						s.phase = Phase::Done;
						continue;
					}
					let old = pg_sys::MemoryContextSwitchTo(s.home);
					s.delta_rel = pg_sys::table_open(s.delta_oid, pg_sys::AccessShareLock as i32);
					s.delta_slot = pg_sys::table_slot_create(s.delta_rel, std::ptr::null_mut());
					s.delta_scan = pg_sys::table_beginscan(s.delta_rel, (*scan).rs_snapshot, 0, std::ptr::null_mut());
					pg_sys::MemoryContextSwitchTo(old);
				}
				if pg_sys::table_scan_getnextslot(s.delta_scan, pg_sys::ScanDirection::ForwardScanDirection, s.delta_slot) {
					copy_delta(s.delta_slot, slot, relid);
					return true;
				}
				s.end_delta();
				s.phase = Phase::Done;
			}
			Phase::Done => {
				pg_sys::ExecClearTuple(slot);
				return false;
			}
		}
	}
}

/// A delta-store row into our slot, with its TID marked as a delta TID.
unsafe fn copy_delta(from: *mut pg_sys::TupleTableSlot, to: *mut pg_sys::TupleTableSlot, relid: pg_sys::Oid) {
	let mut tid = (*from).tts_tid;
	pg_sys::ExecCopySlot(to, from);
	read::to_delta_tid(&mut tid);
	(*to).tts_tid = tid;
	(*to).tts_tableOid = relid;
}

#[pg_guard]
unsafe extern "C-unwind" fn scan_getnextslot(
	scan: pg_sys::TableScanDesc,
	_direction: pg_sys::ScanDirection::Type,
	slot: *mut pg_sys::TupleTableSlot,
) -> bool {
	next_row(scan, slot)
}

#[pg_guard]
unsafe extern "C-unwind" fn parallelscan_estimate(_rel: pg_sys::Relation) -> pg_sys::Size {
	std::mem::size_of::<ParallelDesc>().next_multiple_of(8)
}

#[pg_guard]
unsafe extern "C-unwind" fn parallelscan_initialize(rel: pg_sys::Relation, pscan: pg_sys::ParallelTableScanDesc) -> pg_sys::Size {
	let p = pscan as *mut ParallelDesc;
	#[cfg(feature = "pg18")]
	{
		(*p).base.phs_locator = (*rel).rd_locator;
	}
	#[cfg(not(feature = "pg18"))]
	{
		(*p).base.phs_relid = (*rel).rd_id;
	}
	(*p).base.phs_syncscan = false;
	std::ptr::write(&raw mut (*p).next_group, AtomicU64::new(0));
	std::ptr::write(&raw mut (*p).delta_claimed, AtomicU32::new(0));
	std::mem::size_of::<ParallelDesc>().next_multiple_of(8)
}

#[pg_guard]
unsafe extern "C-unwind" fn parallelscan_reinitialize(_rel: pg_sys::Relation, pscan: pg_sys::ParallelTableScanDesc) {
	let p = pscan as *mut ParallelDesc;
	(*p).next_group.store(0, Ordering::SeqCst);
	(*p).delta_claimed.store(0, Ordering::SeqCst);
}

// ---------------------------------------------------------------------------------------
// Fetching by TID: index scans, and the executor's row re-fetches
// ---------------------------------------------------------------------------------------

/// How many decoded row groups an index fetch keeps. Chosen, not measured.
const FETCH_GROUPS: usize = 4;

#[repr(C)]
struct FetchDesc {
	base: pg_sys::IndexFetchTableData,
	state: *mut FetchState,
}

struct FetchState {
	store: Option<Rc<Store>>,
	cxt: pg_sys::MemoryContext,
	/// The last few row groups decoded, most recent first. An index walks a store in its
	/// own order, not the store's: bench q1 reads each host newest first from a store kept
	/// oldest first, and with one group cached decoded most groups twice (2026-09-23).
	groups: Vec<(usize, Rc<Group>)>,
	deletes: Option<Deletes>,
	/// The deleted rows an MVCC snapshot sees, read once for that snapshot: an index scan
	/// fetches row after row with the same one, and looking each up in the delete log was
	/// ten times the cost of the fetch (bench q1, 2026-09-23).
	deleted: Option<(pg_sys::Snapshot, pg_sys::TransactionId, pg_sys::CommandId, Vec<u64>)>,
	delta_rel: pg_sys::Relation,
	delta_fetch: *mut pg_sys::IndexFetchTableData,
	delta_slot: *mut pg_sys::TupleTableSlot,
	/// Where lazily opened things live (see ScanState::home).
	home: pg_sys::MemoryContext,
}

/// The delete log and its index, for one-row lookups.
struct Deletes {
	rel: pg_sys::Relation,
	index: pg_sys::Oid,
}

impl FetchState {
	unsafe fn new() -> FetchState {
		FetchState {
			store: None,
			cxt: new_cxt(c"snouttime column fetch"),
			groups: Vec::new(),
			deletes: None,
			deleted: None,
			delta_rel: std::ptr::null_mut(),
			delta_fetch: std::ptr::null_mut(),
			delta_slot: std::ptr::null_mut(),
			home: pg_sys::CurrentMemoryContext,
		}
	}

	unsafe fn release(&mut self) {
		if !self.delta_fetch.is_null() {
			pg_sys::table_index_fetch_end(self.delta_fetch);
			self.delta_fetch = std::ptr::null_mut();
		}
		if !self.delta_slot.is_null() {
			pg_sys::ExecDropSingleTupleTableSlot(self.delta_slot);
			self.delta_slot = std::ptr::null_mut();
		}
		if !self.delta_rel.is_null() {
			pg_sys::table_close(self.delta_rel, pg_sys::NoLock as i32);
			self.delta_rel = std::ptr::null_mut();
		}
		if let Some(d) = self.deletes.take() {
			pg_sys::table_close(d.rel, pg_sys::NoLock as i32);
		}
	}

	/// Row `n` of the column store into `slot`, if it exists and `snapshot` does not see it
	/// deleted.
	unsafe fn row(&mut self, rel: pg_sys::Relation, n: u64, snapshot: pg_sys::Snapshot, slot: *mut pg_sys::TupleTableSlot) -> bool {
		if self.store.is_none() {
			let old = pg_sys::MemoryContextSwitchTo(self.home);
			build::flush(rel);
			self.store = Some(Rc::new(Store::open(rel)));
			pg_sys::MemoryContextSwitchTo(old);
		}
		// A dirty snapshot reports back who inserted and who is deleting the row it saw, and a
		// btree's unique check waits for them; a column-store row was written by a committed
		// seal, so there is nobody. Left as they were, the fields were garbage, and a unique
		// check against a sealed row waited on a random transaction and retried forever (found
		// by tests/pg_regress/sql/columnar_seek.sql, 2026-09-23).
		if !snapshot.is_null() && (*snapshot).snapshot_type == pg_sys::SnapshotType::SNAPSHOT_DIRTY {
			(*snapshot).xmin = pg_sys::InvalidTransactionId;
			(*snapshot).xmax = pg_sys::InvalidTransactionId;
			(*snapshot).speculativeToken = 0;
		}
		let Some(g) = self.store.as_ref().unwrap().group_of(n) else {
			return false;
		};
		if !snapshot.is_null() && self.is_deleted(rel, n, snapshot) {
			return false;
		}
		match self.groups.iter().position(|x| x.0 == g) {
			Some(0) => {}
			Some(i) => {
				let hit = self.groups.remove(i);
				self.groups.insert(0, hit);
			}
			None => {
				// a group's values live in the group, so the one the slot holds now (the
				// first) is never the one dropped
				self.groups.truncate(FETCH_GROUPS - 1);
				let loaded = Store::load(self.store.as_ref().unwrap(), g);
				self.groups.insert(0, (g, Rc::new(loaded)));
			}
		}
		self.groups[0].1.store_lazy(n, slot, (*rel).rd_id);
		true
	}

	unsafe fn is_deleted(&mut self, rel: pg_sys::Relation, n: u64, snapshot: pg_sys::Snapshot) -> bool {
		// As heap answers for each kind of snapshot: SnapshotAny (the executor re-fetching
		// the row it is updating, found by tests/columnar/concurrency.sh) sees every row; a
		// dirty snapshot sees a row whose deleter has not committed; MVCC and self snapshots
		// see the deletes they can see.
		let snapshot = match (*snapshot).snapshot_type {
			pg_sys::SnapshotType::SNAPSHOT_ANY
			| pg_sys::SnapshotType::SNAPSHOT_TOAST
			| pg_sys::SnapshotType::SNAPSHOT_NON_VACUUMABLE => return false,
			pg_sys::SnapshotType::SNAPSHOT_DIRTY => &raw mut pg_sys::SnapshotSelfData,
			pg_sys::SnapshotType::SNAPSHOT_MVCC => {
				// what this snapshot sees of the log does not change while it is the same
				// snapshot at the same command: read it once
				let key = ((*snapshot).xmin, (*snapshot).curcid);
				let fresh = !matches!(&self.deleted, Some((p, x, c, _)) if *p == snapshot && (*x, *c) == key);
				if fresh {
					let (_, x) = read::side_tables((*rel).rd_id);
					let rows = read::deleted_rows(x, snapshot);
					self.deleted = Some((snapshot, key.0, key.1, rows));
				}
				return self.deleted.as_ref().unwrap().3.binary_search(&n).is_ok();
			}
			_ => snapshot,
		};
		if self.deletes.is_none() {
			let _home = Switch::to(self.home);
			let (_, x) = read::side_tables((*rel).rd_id);
			if x == pg_sys::InvalidOid {
				return false;
			}
			let drel = pg_sys::table_open(x, pg_sys::AccessShareLock as i32);
			let list = pg_sys::RelationGetIndexList(drel);
			if list.is_null() || (*list).length == 0 {
				pg_sys::table_close(drel, pg_sys::NoLock as i32);
				pgrx::error!("the delete log of \"{}\" has no index", build::relname(rel));
			}
			let index = (*(*list).elements).oid_value;
			self.deletes = Some(Deletes { rel: drel, index });
		}
		let d = self.deletes.as_ref().unwrap();
		super::dml::delete_logged(d.rel, d.index, n, snapshot)
	}

	unsafe fn delta(
		&mut self,
		rel: pg_sys::Relation,
		tid: &mut pg_sys::ItemPointerData,
		snapshot: pg_sys::Snapshot,
		slot: *mut pg_sys::TupleTableSlot,
		call_again: *mut bool,
		all_dead: *mut bool,
	) -> bool {
		if self.delta_rel.is_null() {
			let _home = Switch::to(self.home);
			let (d, _) = read::side_tables((*rel).rd_id);
			if d == pg_sys::InvalidOid {
				return false;
			}
			self.delta_rel = pg_sys::table_open(d, pg_sys::AccessShareLock as i32);
			self.delta_fetch = pg_sys::table_index_fetch_begin(self.delta_rel);
			self.delta_slot = pg_sys::table_slot_create(self.delta_rel, std::ptr::null_mut());
		}
		let found = pg_sys::table_index_fetch_tuple(self.delta_fetch, tid, snapshot, self.delta_slot, call_again, all_dead);
		if found {
			copy_delta(self.delta_slot, slot, (*rel).rd_id);
		}
		found
	}
}

#[pg_guard]
unsafe extern "C-unwind" fn index_fetch_begin(rel: pg_sys::Relation) -> *mut pg_sys::IndexFetchTableData {
	let f = pg_sys::palloc0(std::mem::size_of::<FetchDesc>()) as *mut FetchDesc;
	(*f).base.rel = rel;
	(*f).state = PgMemoryContexts::CurrentMemoryContext.leak_and_drop_on_delete(FetchState::new());
	f as *mut pg_sys::IndexFetchTableData
}

#[pg_guard]
unsafe extern "C-unwind" fn index_fetch_reset(_data: *mut pg_sys::IndexFetchTableData) {}

#[pg_guard]
unsafe extern "C-unwind" fn index_fetch_end(data: *mut pg_sys::IndexFetchTableData) {
	let s = &mut *(*(data as *mut FetchDesc)).state;
	s.release();
	s.groups.clear();
	s.store = None;
	pg_sys::MemoryContextDelete(s.cxt);
}

#[pg_guard]
unsafe extern "C-unwind" fn index_fetch_tuple(
	data: *mut pg_sys::IndexFetchTableData,
	tid: pg_sys::ItemPointer,
	snapshot: pg_sys::Snapshot,
	slot: *mut pg_sys::TupleTableSlot,
	call_again: *mut bool,
	all_dead: *mut bool,
) -> bool {
	let rel = (*data).rel;
	let s = &mut *(*(data as *mut FetchDesc)).state;
	match classify(&*tid) {
		Tid::Row(n) => {
			*call_again = false;
			if !all_dead.is_null() {
				*all_dead = false;
			}
			s.row(rel, n, snapshot, slot)
		}
		Tid::Delta(mut d) => s.delta(rel, &mut d, snapshot, slot, call_again, all_dead),
		Tid::Invalid => {
			*call_again = false;
			false
		}
	}
}

#[pg_guard]
unsafe extern "C-unwind" fn tuple_fetch_row_version(
	rel: pg_sys::Relation,
	tid: pg_sys::ItemPointer,
	snapshot: pg_sys::Snapshot,
	slot: *mut pg_sys::TupleTableSlot,
) -> bool {
	let mut s = FetchState::new();
	let found = match classify(&*tid) {
		Tid::Row(n) => s.row(rel, n, snapshot, slot),
		Tid::Delta(mut d) => {
			let mut again = false;
			let mut dead = false;
			s.delta(rel, &mut d, snapshot, slot, &mut again, &mut dead)
		}
		Tid::Invalid => false,
	};
	// the slot keeps its values: they were copied into it (delta) or live in memory the
	// executor's slot does not own (column rows), so materialize before the context goes
	if found {
		pg_sys::ExecMaterializeSlot(slot);
	}
	s.release();
	s.groups.clear();
	pg_sys::MemoryContextDelete(s.cxt);
	found
}

#[pg_guard]
unsafe extern "C-unwind" fn tuple_tid_valid(scan: pg_sys::TableScanDesc, tid: pg_sys::ItemPointer) -> bool {
	match classify(&*tid) {
		Tid::Row(n) => n < (*(*(scan as *mut ScanDesc)).state).store.rows(),
		Tid::Delta(_) => true,
		Tid::Invalid => false,
	}
}

#[pg_guard]
unsafe extern "C-unwind" fn tuple_get_latest_tid(_scan: pg_sys::TableScanDesc, _tid: pg_sys::ItemPointer) {}

#[pg_guard]
unsafe extern "C-unwind" fn tuple_satisfies_snapshot(
	rel: pg_sys::Relation,
	slot: *mut pg_sys::TupleTableSlot,
	snapshot: pg_sys::Snapshot,
) -> bool {
	let tmp = pg_sys::MakeSingleTupleTableSlot((*rel).rd_att, super::slot::slot_ops());
	let ok = tuple_fetch_row_version(rel, &raw mut (*slot).tts_tid, snapshot, tmp);
	pg_sys::ExecDropSingleTupleTableSlot(tmp);
	ok
}

#[pg_guard]
unsafe extern "C-unwind" fn index_delete_tuples(rel: pg_sys::Relation, delstate: *mut pg_sys::TM_IndexDeleteOp) -> pg_sys::TransactionId {
	// An index asks which of its entries point at rows dead to everyone. A column-store row is
	// never reported dead to an index (its deletes are rows of the delete log, cleaned out by a
	// reseal, which rebuilds the indexes), so only delta-store entries can be. Those are heap
	// rows of the delta store: hand them to heap's own answer, with their TIDs translated there
	// and back, so the horizon a standby needs is heap's, and every entry the index already
	// knew was deletable comes back as deletable, which a simple deletion requires (btree
	// asserts it: an "index_delete_tuples returns nothing" crashed a 40-round test, 2026-09-23).
	let d = &mut *delstate;
	let all = std::slice::from_raw_parts_mut(d.deltids, d.ndeltids as usize);
	let mut kept = 0usize;
	for i in 0..all.len() {
		if let Tid::Delta(t) = classify(&all[i].tid) {
			all[kept] = all[i];
			all[kept].tid = t;
			kept += 1;
		}
	}
	d.ndeltids = kept as i32;
	if kept == 0 {
		return pg_sys::InvalidTransactionId;
	}
	let (delta, _) = read::side_tables((*rel).rd_id);
	if delta == pg_sys::InvalidOid {
		d.ndeltids = 0;
		return pg_sys::InvalidTransactionId;
	}
	let drel = pg_sys::table_open(delta, pg_sys::AccessShareLock as i32);
	let horizon = pg_sys::table_index_delete_tuples(drel, delstate);
	pg_sys::table_close(drel, pg_sys::NoLock as i32);
	for e in std::slice::from_raw_parts_mut(d.deltids, d.ndeltids as usize) {
		read::to_delta_tid(&mut e.tid);
	}
	horizon
}

// ---------------------------------------------------------------------------------------
// Relation-level callbacks
// ---------------------------------------------------------------------------------------

#[pg_guard]
unsafe extern "C-unwind" fn relation_set_new_filelocator(
	rel: pg_sys::Relation,
	newrlocator: *const pg_sys::RelFileLocator,
	persistence: std::ffi::c_char,
	freeze_xid: *mut pg_sys::TransactionId,
	minmulti: *mut pg_sys::MultiXactId,
) {
	if persistence as u8 != pg_sys::RELPERSISTENCE_PERMANENT {
		error!("a column store must be a permanent table: \"{}\" is unlogged or temporary", build::relname(rel));
	}
	// A column store holds no transaction IDs, so it has nothing to freeze.
	*freeze_xid = pg_sys::InvalidTransactionId;
	*minmulti = pg_sys::InvalidTransactionId;
	let srel = pg_sys::RelationCreateStorage(*newrlocator, persistence, true);
	pg_sys::smgrclose(srel);
	// A TRUNCATE: the rows in the side tables went with the old column store.
	super::dml::clear_side((*rel).rd_id);
}

#[pg_guard]
unsafe extern "C-unwind" fn relation_nontransactional_truncate(rel: pg_sys::Relation) {
	pg_sys::RelationTruncate(rel, 0);
	super::dml::clear_side((*rel).rd_id);
}

#[pg_guard]
unsafe extern "C-unwind" fn relation_copy_data(rel: pg_sys::Relation, newrlocator: *const pg_sys::RelFileLocator) {
	// ALTER TABLE ... SET TABLESPACE: the pages move as they are.
	pg_sys::FlushRelationBuffers(rel);
	let persistence = (*(*rel).rd_rel).relpersistence;
	let dst = pg_sys::RelationCreateStorage(*newrlocator, persistence, true);
	pg_sys::RelationCopyStorage(pg_sys::RelationGetSmgr(rel), dst, pg_sys::ForkNumber::MAIN_FORKNUM, persistence);
	pg_sys::RelationDropStorage(rel);
	pg_sys::smgrclose(dst);
}

#[pg_guard]
unsafe extern "C-unwind" fn relation_copy_for_cluster(
	old: pg_sys::Relation,
	new: pg_sys::Relation,
	_old_index: pg_sys::Relation,
	_use_sort: bool,
	_oldest_xmin: pg_sys::TransactionId,
	xid_cutoff: *mut pg_sys::TransactionId,
	multi_cutoff: *mut pg_sys::MultiXactId,
	num_tuples: *mut f64,
	tups_vacuumed: *mut f64,
	tups_recently_dead: *mut f64,
) {
	// VACUUM FULL and CLUSTER rebuild the column store, folding in the delta store and
	// dropping deleted rows: what a reseal does.
	let snapshot = pg_sys::RegisterSnapshot(pg_sys::GetLatestSnapshot());
	let slot = pg_sys::MakeSingleTupleTableSlot((*old).rd_att, super::slot::slot_ops());
	let scan = scan_begin(old, snapshot, 0, std::ptr::null_mut(), std::ptr::null_mut(), pg_sys::ScanOptions::SO_TYPE_SEQSCAN);
	let mut n = 0f64;
	while next_row(scan, slot) {
		build::insert(new, slot);
		n += 1.0;
	}
	scan_end(scan);
	pg_sys::ExecDropSingleTupleTableSlot(slot);
	pg_sys::UnregisterSnapshot(snapshot);
	build::flush(new);
	super::dml::clear_side((*old).rd_id);
	*num_tuples = n;
	*tups_vacuumed = 0.0;
	*tups_recently_dead = 0.0;
	*xid_cutoff = pg_sys::InvalidTransactionId;
	*multi_cutoff = pg_sys::InvalidTransactionId;
}

#[pg_guard]
unsafe extern "C-unwind" fn relation_vacuum(
	_rel: pg_sys::Relation,
	_params: *mut pg_sys::VacuumParams,
	_bstrategy: pg_sys::BufferAccessStrategy,
) {
	// The column store is written once and holds no dead rows; the side tables are
	// ordinary heap tables, which autovacuum looks after on its own.
}

#[pg_guard]
unsafe extern "C-unwind" fn relation_size(rel: pg_sys::Relation, fork: pg_sys::ForkNumber::Type) -> u64 {
	let forks = if fork == pg_sys::ForkNumber::InvalidForkNumber { vec![pg_sys::ForkNumber::MAIN_FORKNUM] } else { vec![fork] };
	let smgr = pg_sys::RelationGetSmgr(rel);
	forks
		.into_iter()
		.map(|f| {
			if pg_sys::smgrexists(smgr, f) {
				pg_sys::smgrnblocks(smgr, f) as u64 * pg_sys::BLCKSZ as u64
			} else {
				0
			}
		})
		.sum()
}

#[pg_guard]
unsafe extern "C-unwind" fn relation_needs_toast_table(_rel: pg_sys::Relation) -> bool {
	// values are stored detoasted inside their column chunks
	false
}

#[pg_guard]
unsafe extern "C-unwind" fn relation_estimate_size(
	rel: pg_sys::Relation,
	_attr_widths: *mut i32,
	pages: *mut pg_sys::BlockNumber,
	tuples: *mut f64,
	allvisfrac: *mut f64,
) {
	let reader = Reader::new(rel);
	*pages = reader.blocks;
	let rows = match reader.meta(1600) {
		Ok(Some(m)) => {
			// a tiered store's bytes are in S3: cost it by them, not by its few local pages
			if let Some(r) = &m.remote {
				*pages += (r.length / pg_sys::BLCKSZ as u64) as pg_sys::BlockNumber;
			}
			m.rows as f64
		}
		_ => 0.0,
	};
	let (d, _) = read::side_tables((*rel).rd_id);
	let mut delta = 0.0;
	if d != pg_sys::InvalidOid {
		let drel = pg_sys::table_open(d, pg_sys::AccessShareLock as i32);
		let reltuples = (*(*drel).rd_rel).reltuples as f64;
		let blocks = pg_sys::RelationGetNumberOfBlocksInFork(drel, pg_sys::ForkNumber::MAIN_FORKNUM) as f64;
		delta = if reltuples >= 0.0 { reltuples } else { blocks * 100.0 };
		pg_sys::table_close(drel, pg_sys::AccessShareLock as i32);
	}
	*tuples = rows + delta;
	// No visibility map: an index-only scan fetches every row, and told otherwise the planner
	// chose one for count(*) and made 864,000 fetches of a sealed partition (2026-09-23).
	*allvisfrac = 0.0;
}

// ---------------------------------------------------------------------------------------
// ANALYZE: a sampled page is answered with the row groups that begin on it
// ---------------------------------------------------------------------------------------

#[pg_guard]
unsafe extern "C-unwind" fn scan_analyze_next_block(scan: pg_sys::TableScanDesc, stream: *mut pg_sys::ReadStream) -> bool {
	let buf = pg_sys::read_stream_next_buffer(stream, std::ptr::null_mut());
	if buf == pg_sys::InvalidBuffer as pg_sys::Buffer {
		return false;
	}
	let block = pg_sys::BufferGetBlockNumber(buf);
	pg_sys::ReleaseBuffer(buf);
	let s = &mut *(*(scan as *mut ScanDesc)).state;
	if s.store.meta.as_ref().is_some_and(|m| m.remote.is_some()) {
		// A tiered store has only its metapage and directory here: the first sampled page is
		// answered with eight row groups spread over the table, fetched from S3, the rest with
		// none. Enough rows for column statistics; the row count comes from the metapage.
		let n = s.store.dir.len();
		s.analyze = if s.next_group == 0 && n > 0 {
			let k = n.min(8);
			(0..k).map(|i| i * n / k).rev().collect()
		} else {
			Vec::new()
		};
		s.next_group = 1;
		s.group = None;
		return true;
	}
	s.analyze = s
		.store
		.dir
		.iter()
		.enumerate()
		.filter(|(_, g)| Reader::block_of(g.offset) == block)
		.map(|(i, _)| i)
		.rev()
		.collect();
	s.group = None;
	true
}

#[pg_guard]
unsafe extern "C-unwind" fn scan_analyze_next_tuple(
	scan: pg_sys::TableScanDesc,
	_oldest_xmin: pg_sys::TransactionId,
	liverows: *mut f64,
	_deadrows: *mut f64,
	slot: *mut pg_sys::TupleTableSlot,
) -> bool {
	let s = &mut *(*(scan as *mut ScanDesc)).state;
	let relid = (*(*scan).rs_rd).rd_id;
	loop {
		if let Some(g) = &s.group {
			if s.row < g.first_row + g.rows {
				let n = s.row;
				s.row += 1;
				g.store(n, slot, relid);
				*liverows += 1.0;
				return true;
			}
		}
		let Some(next) = s.analyze.pop() else {
			s.group = None;
			pg_sys::ExecClearTuple(slot);
			return false;
		};
		s.group = None;
		let g = Store::load(&s.store, next);
		g.decode(None);
		s.row = g.first_row;
		s.group = Some(Rc::new(g));
	}
}

// ---------------------------------------------------------------------------------------
// Index builds
// ---------------------------------------------------------------------------------------

const BRIN_AM_OID: u32 = 3580;

#[pg_guard]
unsafe extern "C-unwind" fn index_build_range_scan(
	table_rel: pg_sys::Relation,
	index_rel: pg_sys::Relation,
	index_info: *mut pg_sys::IndexInfo,
	_allow_sync: bool,
	_anyvisible: bool,
	_progress: bool,
	start_blockno: pg_sys::BlockNumber,
	numblocks: pg_sys::BlockNumber,
	callback: pg_sys::IndexBuildCallback,
	callback_state: *mut c_void,
	scan: pg_sys::TableScanDesc,
) -> f64 {
	if (*(*index_rel).rd_rel).relam.to_u32() == BRIN_AM_OID {
		error!(
			"a BRIN index is not supported on the sealed partition \"{}\": its column store already keeps the minimum and maximum of every row group",
			build::relname(table_rel)
		);
	}
	let all = numblocks == pg_sys::InvalidBlockNumber;
	// A non-unique index on a store sealed without keep_indexes holds only the delta store's
	// rows (PLAN.md Q5): the column store answers for its own rows by its sort key, and the
	// planner never sees such an index (`scan::relation_info`). A unique index, or one behind
	// an exclusion constraint, enforces something, so it always covers every row.
	// Whether it is unique is read from the index itself, not from `index_info`: a rebuild that
	// skips constraint checks (VACUUM FULL, CLUSTER) clears ii_Unique for the build, and a
	// unique index rebuilt that way held only the late rows (found 2026-09-23).
	let whole = (*(*index_rel).rd_index).indisunique || (*(*index_rel).rd_index).indisexclusion;
	let late_only = !whole && late_indexes(table_rel);
	// before anything is opened: an early return after them leaked a slot and a snapshot
	if late_only && all && scan.is_null() {
		return build_from_delta(table_rel, index_rel, index_info, callback, callback_state);
	}
	let estate = pg_sys::CreateExecutorState();
	let econtext = pg_sys::MakePerTupleExprContext(estate);
	let slot = pg_sys::table_slot_create(table_rel, std::ptr::null_mut());
	(*econtext).ecxt_scantuple = slot;
	let predicate = pg_sys::ExecPrepareQual((*index_info).ii_Predicate, estate);
	let (own, snapshot) = if scan.is_null() {
		let snap = pg_sys::RegisterSnapshot(pg_sys::GetTransactionSnapshot());
		let s = scan_begin(table_rel, snap, 0, std::ptr::null_mut(), std::ptr::null_mut(), pg_sys::ScanOptions::SO_TYPE_SEQSCAN);
		(s, snap)
	} else {
		(scan, std::ptr::null_mut())
	};
	let end_block = start_blockno as u64 + numblocks as u64;

	let natts = (*index_info).ii_NumIndexAttrs as usize;
	let mut values = vec![pg_sys::Datum::from(0usize); natts.max(1)];
	let mut isnull = vec![false; natts.max(1)];
	let mut n = 0f64;
	let mut saw_delta = false;
	let cb = callback.expect("an index build always has a callback");
	while next_row(own, slot) {
		pg_sys::MemoryContextReset((*econtext).ecxt_per_tuple_memory);
		let tid = (*slot).tts_tid;
		let block = read::tid_block(&tid);
		if block & read::DELTA_BIT != 0 {
			if !all {
				continue;
			}
			saw_delta = true;
		} else if late_only || (!all && ((block as u64) < start_blockno as u64 || block as u64 >= end_block)) {
			continue;
		}
		if !predicate.is_null() && !pg_sys::ExecQual(predicate, econtext) {
			continue;
		}
		pg_sys::FormIndexDatum(index_info, slot, estate, values.as_mut_ptr(), isnull.as_mut_ptr());
		let mut t = tid;
		cb(index_rel, &mut t, values.as_mut_ptr(), isnull.as_mut_ptr(), true, callback_state);
		n += 1.0;
	}
	if scan.is_null() {
		scan_end(own);
		pg_sys::UnregisterSnapshot(snapshot);
	}
	if saw_delta {
		// Delta-store rows are indexed as the build's snapshot sees them; an older snapshot
		// may not use the index until it is gone (the same rule a HOT chain imposes on heap).
		(*index_info).ii_BrokenHotChain = true;
	}
	pg_sys::ExecDropSingleTupleTableSlot(slot);
	pg_sys::FreeExecutorState(estate);
	n
}

/// Do `rel`'s non-unique indexes hold only its delta store's rows?
///
/// # Safety
/// `rel` must be open.
pub unsafe fn late_indexes(rel: pg_sys::Relation) -> bool {
	matches!(Reader::new(rel).meta(1600), Ok(Some(m)) if m.flags & super::format::FLAG_LATE_INDEXES != 0)
}

/// A late-rows-only index is built from the delta store alone: walking every column-store row
/// only to skip it was a pass over the whole partition per index at every seal.
unsafe fn build_from_delta(
	table_rel: pg_sys::Relation,
	index_rel: pg_sys::Relation,
	index_info: *mut pg_sys::IndexInfo,
	callback: pg_sys::IndexBuildCallback,
	callback_state: *mut c_void,
) -> f64 {
	let (delta, _) = read::side_tables((*table_rel).rd_id);
	if delta == pg_sys::InvalidOid {
		return 0.0;
	}
	let cb = callback.expect("an index build always has a callback");
	let estate = pg_sys::CreateExecutorState();
	let econtext = pg_sys::MakePerTupleExprContext(estate);
	let drel = pg_sys::table_open(delta, pg_sys::AccessShareLock as i32);
	let slot = pg_sys::table_slot_create(drel, std::ptr::null_mut());
	(*econtext).ecxt_scantuple = slot;
	let predicate = pg_sys::ExecPrepareQual((*index_info).ii_Predicate, estate);
	let snap = pg_sys::RegisterSnapshot(pg_sys::GetTransactionSnapshot());
	let scan = pg_sys::table_beginscan(drel, snap, 0, std::ptr::null_mut());
	let natts = (*index_info).ii_NumIndexAttrs as usize;
	let mut values = vec![pg_sys::Datum::from(0usize); natts.max(1)];
	let mut isnull = vec![false; natts.max(1)];
	let mut n = 0f64;
	while pg_sys::table_scan_getnextslot(scan, pg_sys::ScanDirection::ForwardScanDirection, slot) {
		pg_sys::MemoryContextReset((*econtext).ecxt_per_tuple_memory);
		if !predicate.is_null() && !pg_sys::ExecQual(predicate, econtext) {
			continue;
		}
		pg_sys::FormIndexDatum(index_info, slot, estate, values.as_mut_ptr(), isnull.as_mut_ptr());
		let mut t = (*slot).tts_tid;
		read::to_delta_tid(&mut t);
		cb(index_rel, &mut t, values.as_mut_ptr(), isnull.as_mut_ptr(), true, callback_state);
		n += 1.0;
	}
	pg_sys::table_endscan(scan);
	pg_sys::UnregisterSnapshot(snap);
	pg_sys::ExecDropSingleTupleTableSlot(slot);
	pg_sys::table_close(drel, pg_sys::NoLock as i32);
	pg_sys::FreeExecutorState(estate);
	if n > 0.0 {
		// as for a build through the relation: delta rows are indexed as this snapshot sees them
		(*index_info).ii_BrokenHotChain = true;
	}
	n
}

#[pg_guard]
unsafe extern "C-unwind" fn index_validate_scan(
	table_rel: pg_sys::Relation,
	_index_rel: pg_sys::Relation,
	_index_info: *mut pg_sys::IndexInfo,
	_snapshot: pg_sys::Snapshot,
	_state: *mut pg_sys::ValidateIndexState,
) {
	error!(
		"CREATE INDEX CONCURRENTLY is not supported on the sealed partition \"{}\"; a plain CREATE INDEX blocks only writes to it",
		build::relname(table_rel)
	);
}

#[pg_guard]
unsafe extern "C-unwind" fn scan_sample_next_block(scan: pg_sys::TableScanDesc, _s: *mut pg_sys::SampleScanState) -> bool {
	error!("TABLESAMPLE is not supported on the sealed partition \"{}\"", build::relname((*scan).rs_rd));
}

#[pg_guard]
unsafe extern "C-unwind" fn scan_sample_next_tuple(
	_scan: pg_sys::TableScanDesc,
	_s: *mut pg_sys::SampleScanState,
	_slot: *mut pg_sys::TupleTableSlot,
) -> bool {
	false
}

#[pg_guard]
unsafe extern "C-unwind" fn finish_bulk_insert(rel: pg_sys::Relation, _options: i32) {
	build::flush(rel);
	// An ALTER TABLE rewrite fills a transient relation named pg_temp_<oid of the table>
	// (make_new_heap); what that table's side tables held is in the new column store now.
	let name = build::relname(rel);
	if let Some(oid) = name.strip_prefix("pg_temp_").and_then(|s| s.parse::<u32>().ok()) {
		super::dml::clear_side(pg_sys::Oid::from(oid));
	}
}

// ---------------------------------------------------------------------------------------
// The routine
// ---------------------------------------------------------------------------------------

pub fn routine() -> *const pg_sys::TableAmRoutine {
	static mut ROUTINE: Option<pg_sys::TableAmRoutine> = None;
	unsafe {
		let r = &raw mut ROUTINE;
		if (*r).is_none() {
			let heap = &*pg_sys::GetHeapamTableAmRoutine();
			let mut t: pg_sys::TableAmRoutine = std::mem::zeroed();
			t.type_ = pg_sys::NodeTag::T_TableAmRoutine;
			t.slot_callbacks = Some(slot_callbacks);
			t.scan_begin = Some(scan_begin);
			t.scan_end = Some(scan_end);
			t.scan_rescan = Some(scan_rescan);
			t.scan_getnextslot = Some(scan_getnextslot);
			t.parallelscan_estimate = Some(parallelscan_estimate);
			t.parallelscan_initialize = Some(parallelscan_initialize);
			t.parallelscan_reinitialize = Some(parallelscan_reinitialize);
			t.index_fetch_begin = Some(index_fetch_begin);
			t.index_fetch_reset = Some(index_fetch_reset);
			t.index_fetch_end = Some(index_fetch_end);
			t.index_fetch_tuple = Some(index_fetch_tuple);
			t.tuple_fetch_row_version = Some(tuple_fetch_row_version);
			t.tuple_tid_valid = Some(tuple_tid_valid);
			t.tuple_get_latest_tid = Some(tuple_get_latest_tid);
			t.tuple_satisfies_snapshot = Some(tuple_satisfies_snapshot);
			t.index_delete_tuples = Some(index_delete_tuples);
			t.tuple_insert = Some(super::dml::tuple_insert);
			t.tuple_insert_speculative = Some(super::dml::tuple_insert_speculative);
			t.tuple_complete_speculative = Some(super::dml::tuple_complete_speculative);
			t.multi_insert = Some(super::dml::multi_insert);
			t.tuple_delete = Some(super::dml::tuple_delete);
			t.tuple_update = Some(super::dml::tuple_update);
			t.tuple_lock = Some(super::dml::tuple_lock);
			t.finish_bulk_insert = Some(finish_bulk_insert);
			t.relation_set_new_filelocator = Some(relation_set_new_filelocator);
			t.relation_nontransactional_truncate = Some(relation_nontransactional_truncate);
			t.relation_copy_data = Some(relation_copy_data);
			t.relation_copy_for_cluster = Some(relation_copy_for_cluster);
			t.relation_vacuum = Some(relation_vacuum);
			t.scan_analyze_next_block = Some(scan_analyze_next_block);
			t.scan_analyze_next_tuple = Some(scan_analyze_next_tuple);
			t.index_build_range_scan = Some(index_build_range_scan);
			t.index_validate_scan = Some(index_validate_scan);
			t.relation_size = Some(relation_size);
			t.relation_needs_toast_table = Some(relation_needs_toast_table);
			t.relation_toast_am = heap.relation_toast_am;
			t.relation_fetch_toast_slice = heap.relation_fetch_toast_slice;
			t.relation_estimate_size = Some(relation_estimate_size);
			t.scan_sample_next_block = Some(scan_sample_next_block);
			t.scan_sample_next_tuple = Some(scan_sample_next_tuple);
			*r = Some(t);
		}
		(*r).as_ref().unwrap()
	}
}
