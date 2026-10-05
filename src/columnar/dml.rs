//! Writes to a sealed partition.
//!
//! * INSERT goes to the delta store, a heap table, unless the column store itself is being
//!   built in this transaction (`build`).
//! * DELETE of a column-store row inserts its row number into the delete log, a heap table
//!   with an index on it. Two transactions deleting the same row are serialised the way heap
//!   serialises them: a heavyweight lock on the TID while deciding, a dirty read of the delete
//!   log to find an uncommitted delete, and a wait for that transaction to end. When it
//!   committed, the second one gets `TM_Deleted`, which READ COMMITTED treats as "the row is
//!   gone" and REPEATABLE READ as a serialisation failure.
//! * UPDATE is a delete plus an insert into the delta store, for column-store and delta rows
//!   alike, with the old version marked as MOVED: a concurrent updater cannot follow the row to
//!   its new version, so it gets the error Postgres gives for a row that moved partition
//!   ("already moved to another partition due to concurrent update") and retries, rather than
//!   silently updating nothing.
//! * A row lock on a column-store row (`SELECT ... FOR UPDATE`, `FOR SHARE`) is a delete-log
//!   entry marked `locked_only`, which a reader ignores and a writer waits for. Every lock
//!   strength is taken as the strongest.

use pgrx::pg_sys;
use pgrx::prelude::*;

use super::build;
use super::read::{self, classify, Tid};


fn no_side(rel: pg_sys::Relation) -> ! {
	let name = unsafe { build::relname(rel) };
	ereport!(
		PgLogLevel::ERROR,
		PgSqlErrorCode::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
		format!("the column store \"{name}\" has no delta store, so it cannot take new or changed rows"),
		"A column store made by ALTER TABLE ... SET ACCESS METHOD or snouttime.seal() has one; its event trigger creates it."
	);
	unreachable!()
}

unsafe fn open_side(rel: pg_sys::Relation, which: usize) -> pg_sys::Relation {
	let (d, x) = read::side_tables((*rel).rd_id);
	let oid = if which == 0 { d } else { x };
	if oid == pg_sys::InvalidOid {
		no_side(rel);
	}
	pg_sys::table_open(oid, pg_sys::RowExclusiveLock as i32)
}

// ---- inserts ----

unsafe fn delta_insert(rel: pg_sys::Relation, slot: *mut pg_sys::TupleTableSlot, cid: pg_sys::CommandId) {
	let drel = open_side(rel, 0);
	pg_sys::table_tuple_insert(drel, slot, cid, 0, std::ptr::null_mut());
	read::to_delta_tid(&mut (*slot).tts_tid);
	(*slot).tts_tableOid = (*rel).rd_id;
	pg_sys::table_close(drel, pg_sys::NoLock as i32);
}

#[pg_guard]
pub unsafe extern "C-unwind" fn tuple_insert(
	rel: pg_sys::Relation,
	slot: *mut pg_sys::TupleTableSlot,
	cid: pg_sys::CommandId,
	_options: i32,
	_bistate: *mut pg_sys::BulkInsertStateData,
) {
	if build::buffers(rel) {
		build::insert(rel, slot);
		(*slot).tts_tableOid = (*rel).rd_id;
		return;
	}
	delta_insert(rel, slot, cid);
}

#[pg_guard]
pub unsafe extern "C-unwind" fn multi_insert(
	rel: pg_sys::Relation,
	slots: *mut *mut pg_sys::TupleTableSlot,
	nslots: i32,
	cid: pg_sys::CommandId,
	options: i32,
	bistate: *mut pg_sys::BulkInsertStateData,
) {
	for i in 0..nslots as usize {
		tuple_insert(rel, *slots.add(i), cid, options, bistate);
	}
}

#[pg_guard]
pub unsafe extern "C-unwind" fn tuple_insert_speculative(
	rel: pg_sys::Relation,
	slot: *mut pg_sys::TupleTableSlot,
	cid: pg_sys::CommandId,
	_options: i32,
	_bistate: *mut pg_sys::BulkInsertStateData,
	token: u32,
) {
	let drel = open_side(rel, 0);
	pg_sys::table_tuple_insert_speculative(drel, slot, cid, 0, std::ptr::null_mut(), token);
	read::to_delta_tid(&mut (*slot).tts_tid);
	(*slot).tts_tableOid = (*rel).rd_id;
	pg_sys::table_close(drel, pg_sys::NoLock as i32);
}

#[pg_guard]
pub unsafe extern "C-unwind" fn tuple_complete_speculative(
	rel: pg_sys::Relation,
	slot: *mut pg_sys::TupleTableSlot,
	token: u32,
	succeeded: bool,
) {
	let drel = open_side(rel, 0);
	let marked = (*slot).tts_tid;
	if let Tid::Delta(d) = classify(&marked) {
		(*slot).tts_tid = d;
	}
	pg_sys::table_tuple_complete_speculative(drel, slot, token, succeeded);
	(*slot).tts_tid = marked;
	pg_sys::table_close(drel, pg_sys::NoLock as i32);
}

// ---- the delete log ----

/// A dirty snapshot, which sees committed rows and other transactions' uncommitted ones and
/// says which transaction an uncommitted one belongs to.
fn dirty() -> pg_sys::SnapshotData {
	let mut s: pg_sys::SnapshotData = unsafe { std::mem::zeroed() };
	s.snapshot_type = pg_sys::SnapshotType::SNAPSHOT_DIRTY;
	s
}

unsafe fn index_of(drel: pg_sys::Relation) -> pg_sys::Oid {
	let list = pg_sys::RelationGetIndexList(drel);
	if list.is_null() || (*list).length == 0 {
		error!("the delete log \"{}\" has no index", build::relname(drel));
	}
	(*(*list).elements).oid_value
}

/// The delete-log entries for row `n` visible to `snapshot`: (locked_only, moved, xmin) each.
unsafe fn entries(drel: pg_sys::Relation, index: pg_sys::Oid, n: u64, snapshot: pg_sys::Snapshot) -> Vec<(bool, bool, pg_sys::TransactionId)> {
	let mut key: pg_sys::ScanKeyData = std::mem::zeroed();
	{
		pg_sys::ScanKeyInit(
			&mut key,
			1,
			pg_sys::BTEqualStrategyNumber as u16,
			pg_sys::Oid::from(pg_sys::F_INT8EQ),
			pg_sys::Datum::from(n as i64),
		)
	};
	let scan = pg_sys::systable_beginscan(drel, index, true, snapshot, 1, &mut key);
	let mut out = Vec::new();
	loop {
		let tup = pg_sys::systable_getnext(scan);
		if tup.is_null() {
			break;
		}
		let mut isnull = false;
		let locked = pg_sys::heap_getattr(tup, 2, (*drel).rd_att, &mut isnull);
		let locked = !isnull && locked.value() & 0xff != 0;
		let moved = pg_sys::heap_getattr(tup, 3, (*drel).rd_att, &mut isnull);
		let moved = !isnull && moved.value() & 0xff != 0;
		let xmin = (*(*tup).t_data).t_choice.t_heap.t_xmin;
		out.push((locked, moved, xmin));
	}
	pg_sys::systable_endscan(scan);
	out
}

/// Is row `n` deleted as far as `snapshot` sees (a lock-only entry is not a delete)?
///
/// # Safety
/// `drel` must be the open delete log and `index` its index.
pub unsafe fn delete_logged(drel: pg_sys::Relation, index: pg_sys::Oid, n: u64, snapshot: pg_sys::Snapshot) -> bool {
	entries(drel, index, n, snapshot).iter().any(|&(locked, _, _)| !locked)
}

/// `moved`: the row was deleted by an UPDATE (or moved to another partition), so a
/// concurrent writer must be told it moved rather than that it is gone.
unsafe fn log_entry(drel: pg_sys::Relation, index: pg_sys::Oid, n: u64, locked_only: bool, moved: bool, cid: pg_sys::CommandId) {
	let mut values = [pg_sys::Datum::from(n as i64), pg_sys::Datum::from(locked_only), pg_sys::Datum::from(moved)];
	let mut nulls = [false, false, false];
	let tup = pg_sys::heap_form_tuple((*drel).rd_att, values.as_mut_ptr(), nulls.as_mut_ptr());
	pg_sys::heap_insert(drel, tup, cid, 0, std::ptr::null_mut());
	let irel = pg_sys::index_open(index, pg_sys::RowExclusiveLock as i32);
	let info = pg_sys::BuildIndexInfo(irel);
	let mut ivalues = [pg_sys::Datum::from(n as i64)];
	let mut inulls = [false];
	{
		pg_sys::index_insert(
			irel,
			ivalues.as_mut_ptr(),
			inulls.as_mut_ptr(),
			&mut (*tup).t_self,
			drel,
			pg_sys::IndexUniqueCheck::UNIQUE_CHECK_NO,
			false,
			info,
		)
	};
	pg_sys::index_close(irel, pg_sys::NoLock as i32);
}

unsafe fn conditional_lock_tuple(rel: pg_sys::Relation, tid: pg_sys::ItemPointer, mode: i32) -> bool {
	#[cfg(feature = "pg18")]
	{
		pg_sys::ConditionalLockTuple(rel, tid, mode, false)
	}
	#[cfg(not(feature = "pg18"))]
	{
		pg_sys::ConditionalLockTuple(rel, tid, mode)
	}
}

enum Claim {
	Ok,
	Deleted(pg_sys::TransactionId, bool),
	SelfDeleted,
	WouldBlock,
}

/// Decides whether this transaction may delete or lock column-store row `n`, waiting as the
/// heap would for a transaction that holds it. On `Claim::Ok` the caller writes its entry.
unsafe fn claim(
	rel: pg_sys::Relation,
	tid: pg_sys::ItemPointer,
	drel: pg_sys::Relation,
	index: pg_sys::Oid,
	n: u64,
	wait: bool,
	lock_only: bool,
) -> Claim {
	let mode = pg_sys::ExclusiveLock as i32;
	if wait {
		pg_sys::LockTuple(rel, tid, mode);
	} else if !conditional_lock_tuple(rel, tid, mode) {
		return Claim::WouldBlock;
	}
	let me = pg_sys::GetCurrentTransactionIdIfAny();
	let result = loop {
		let mut d = dirty();
		let found = entries(drel, index, n, &mut d);
		// another transaction's entry, still in progress: wait for it and look again
		if d.xmin != pg_sys::InvalidTransactionId && d.xmin != me {
			if !wait {
				break Claim::WouldBlock;
			}
			pg_sys::XactLockTableWait(d.xmin, rel, tid, pg_sys::XLTW_Oper::XLTW_Delete);
			continue;
		}
		// committed entries (and our own)
		if let Some(&(_, moved, xmin)) = found.iter().find(|&&(locked, _, _)| !locked) {
			break if xmin == me { Claim::SelfDeleted } else { Claim::Deleted(xmin, moved) };
		}
		if lock_only && found.iter().any(|&(locked, _, xmin)| locked && xmin == me) {
			// already locked by us
			break Claim::Ok;
		}
		break Claim::Ok;
	};
	pg_sys::UnlockTuple(rel, tid, mode);
	result
}

unsafe fn fail(tmfd: *mut pg_sys::TM_FailureData, tid: pg_sys::ItemPointer, xmax: pg_sys::TransactionId, cid: pg_sys::CommandId) {
	(*tmfd).ctid = *tid;
	(*tmfd).xmax = xmax;
	(*tmfd).cmax = cid;
	(*tmfd).traversed = false;
}

/// A row another transaction deleted: `TM_Deleted`, or when it was an UPDATE, `TM_Updated`
/// with the TID Postgres uses for a row that moved partition, so the executor raises "tuple
/// to be updated was already moved to another partition due to concurrent update" and the
/// client retries. An UPDATE here is a delete plus an insert elsewhere, exactly like a move,
/// and a silent "0 rows" would be a lost update.
unsafe fn gone(tmfd: *mut pg_sys::TM_FailureData, tid: pg_sys::ItemPointer, xmax: pg_sys::TransactionId, moved: bool) -> pg_sys::TM_Result::Type {
	fail(tmfd, tid, xmax, 0);
	if moved {
		read::set_tid(&mut (*tmfd).ctid, pg_sys::InvalidBlockNumber, pg_sys::MovedPartitionsOffsetNumber as u16);
		pg_sys::TM_Result::TM_Updated
	} else {
		pg_sys::TM_Result::TM_Deleted
	}
}

// ---- delete, update, lock ----

#[pg_guard]
pub unsafe extern "C-unwind" fn tuple_delete(
	rel: pg_sys::Relation,
	tid: pg_sys::ItemPointer,
	cid: pg_sys::CommandId,
	snapshot: pg_sys::Snapshot,
	crosscheck: pg_sys::Snapshot,
	wait: bool,
	tmfd: *mut pg_sys::TM_FailureData,
	changing_part: bool,
) -> pg_sys::TM_Result::Type {
	match classify(&*tid) {
		Tid::Delta(mut d) => {
			let drel = open_side(rel, 0);
			let r = pg_sys::table_tuple_delete(drel, &mut d, cid, snapshot, crosscheck, wait, tmfd, changing_part);
			if r != pg_sys::TM_Result::TM_Ok {
				read::to_delta_tid(&mut (*tmfd).ctid);
			}
			pg_sys::table_close(drel, pg_sys::NoLock as i32);
			r
		}
		Tid::Row(n) => {
			let drel = open_side(rel, 1);
			let index = index_of(drel);
			let r = match claim(rel, tid, drel, index, n, wait, false) {
				Claim::Ok => {
					log_entry(drel, index, n, false, changing_part, cid);
					pg_sys::TM_Result::TM_Ok
				}
				Claim::Deleted(x, moved) => gone(tmfd, tid, x, moved),
				Claim::SelfDeleted => {
					fail(tmfd, tid, pg_sys::GetCurrentTransactionIdIfAny(), cid);
					pg_sys::TM_Result::TM_SelfModified
				}
				Claim::WouldBlock => pg_sys::TM_Result::TM_WouldBlock,
			};
			pg_sys::table_close(drel, pg_sys::NoLock as i32);
			r
		}
		Tid::Invalid => error!("a TID of \"{}\" that no row has", build::relname(rel)),
	}
}

#[pg_guard]
pub unsafe extern "C-unwind" fn tuple_update(
	rel: pg_sys::Relation,
	otid: pg_sys::ItemPointer,
	slot: *mut pg_sys::TupleTableSlot,
	cid: pg_sys::CommandId,
	snapshot: pg_sys::Snapshot,
	crosscheck: pg_sys::Snapshot,
	wait: bool,
	tmfd: *mut pg_sys::TM_FailureData,
	lockmode: *mut pg_sys::LockTupleMode::Type,
	update_indexes: *mut pg_sys::TU_UpdateIndexes::Type,
) -> pg_sys::TM_Result::Type {
	// changing_part: the old version is marked as moved, for column-store and delta rows
	// alike, so a concurrent updater errors instead of silently finding nothing
	let r = tuple_delete(rel, otid, cid, snapshot, crosscheck, wait, tmfd, true);
	*lockmode = pg_sys::LockTupleMode::LockTupleExclusive;
	if r != pg_sys::TM_Result::TM_Ok {
		*update_indexes = pg_sys::TU_UpdateIndexes::TU_None;
		return r;
	}
	delta_insert(rel, slot, cid);
	// The new version is a new row with a new TID: every index needs an entry for it.
	*update_indexes = pg_sys::TU_UpdateIndexes::TU_All;
	pg_sys::TM_Result::TM_Ok
}

#[pg_guard]
pub unsafe extern "C-unwind" fn tuple_lock(
	rel: pg_sys::Relation,
	tid: pg_sys::ItemPointer,
	snapshot: pg_sys::Snapshot,
	slot: *mut pg_sys::TupleTableSlot,
	cid: pg_sys::CommandId,
	mode: pg_sys::LockTupleMode::Type,
	wait_policy: pg_sys::LockWaitPolicy::Type,
	flags: u8,
	tmfd: *mut pg_sys::TM_FailureData,
) -> pg_sys::TM_Result::Type {
	match classify(&*tid) {
		Tid::Delta(mut d) => {
			let drel = open_side(rel, 0);
			let hslot = pg_sys::table_slot_create(drel, std::ptr::null_mut());
			let r = pg_sys::table_tuple_lock(drel, &mut d, snapshot, hslot, cid, mode, wait_policy, flags, tmfd);
			if r == pg_sys::TM_Result::TM_Ok {
				let marked = *tid;
				pg_sys::ExecCopySlot(slot, hslot);
				(*slot).tts_tid = marked;
				(*slot).tts_tableOid = (*rel).rd_id;
			} else {
				read::to_delta_tid(&mut (*tmfd).ctid);
			}
			pg_sys::ExecDropSingleTupleTableSlot(hslot);
			pg_sys::table_close(drel, pg_sys::NoLock as i32);
			r
		}
		Tid::Row(n) => {
			let drel = open_side(rel, 1);
			let index = index_of(drel);
			let wait = wait_policy == pg_sys::LockWaitPolicy::LockWaitBlock;
			let r = match claim(rel, tid, drel, index, n, wait, true) {
				Claim::Ok => {
					log_entry(drel, index, n, true, false, cid);
					pg_sys::TM_Result::TM_Ok
				}
				// what heap's tuple_lock says of a row that moved: the executor's update path
				// relies on it (it treats TM_Updated from here as unexpected)
				Claim::Deleted(_, true) => {
					ereport!(
						PgLogLevel::ERROR,
						PgSqlErrorCode::ERRCODE_T_R_SERIALIZATION_FAILURE,
						"tuple to be locked was already moved to another partition due to concurrent update"
					);
					unreachable!()
				}
				Claim::Deleted(x, false) => gone(tmfd, tid, x, false),
				Claim::SelfDeleted => {
					fail(tmfd, tid, pg_sys::GetCurrentTransactionIdIfAny(), cid);
					pg_sys::TM_Result::TM_SelfModified
				}
				Claim::WouldBlock => {
					if wait_policy == pg_sys::LockWaitPolicy::LockWaitError {
						ereport!(
							PgLogLevel::ERROR,
							PgSqlErrorCode::ERRCODE_LOCK_NOT_AVAILABLE,
							format!("could not obtain lock on row in relation \"{}\"", build::relname(rel))
						);
					}
					pg_sys::TM_Result::TM_WouldBlock
				}
			};
			pg_sys::table_close(drel, pg_sys::NoLock as i32);
			if r == pg_sys::TM_Result::TM_Ok {
				let found = super::am_fetch_row(rel, tid, slot);
				if !found {
					return pg_sys::TM_Result::TM_Deleted;
				}
			}
			r
		}
		Tid::Invalid => error!("a TID of \"{}\" that no row has", build::relname(rel)),
	}
}

// ---- emptying the side tables when their rows have moved into a new column store ----

/// Deletes every row of a relation's delta store and delete log, transactionally. Called
/// when a rewrite (VACUUM FULL, CLUSTER, ALTER TABLE, TRUNCATE) has folded them into, or
/// dropped them with, the column store, under that command's exclusive lock.
pub fn clear_side(relid: pg_sys::Oid) {
	let (d, x) = read::side_tables(relid);
	for oid in [d, x] {
		if oid == pg_sys::InvalidOid {
			continue;
		}
		unsafe {
			let rel = pg_sys::table_open(oid, pg_sys::RowExclusiveLock as i32);
			let snapshot = pg_sys::RegisterSnapshot(pg_sys::GetLatestSnapshot());
			let scan = pg_sys::table_beginscan(rel, snapshot, 0, std::ptr::null_mut());
			let slot = pg_sys::table_slot_create(rel, std::ptr::null_mut());
			while pg_sys::table_scan_getnextslot(scan, pg_sys::ScanDirection::ForwardScanDirection, slot) {
				let mut t = (*slot).tts_tid;
				pg_sys::simple_heap_delete(rel, &mut t);
			}
			pg_sys::ExecDropSingleTupleTableSlot(slot);
			pg_sys::table_endscan(scan);
			pg_sys::UnregisterSnapshot(snapshot);
			pg_sys::table_close(rel, pg_sys::NoLock as i32);
		}
	}
}
