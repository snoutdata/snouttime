//! The slot a sealed table's rows come back in: a virtual slot that decodes a column only when
//! the executor asks for it.
//!
//! An index fetch cannot know which columns the plan will read, and a row group decoded whole
//! for one row is twelve columns decoded for the one or two a query looks at: bench q1's merge
//! compares host and time across ten million fetched rows and returns a hundred of them, q5
//! wants one float per event (2026-09-23). So a fetched row carries a reference to its row group
//! and its row number, and `getsomeattrs` decodes what is asked, the way a heap slot deforms
//! only the attributes asked for.
//!
//! The reference is weak. A group lives as long as the scan or fetch that loaded it keeps it,
//! which is until at least the next row it returns, the lifetime the executor gives any slot's
//! contents; a slot read after that is an error, never a read of freed memory. Anything that
//! keeps the row (materialize, a copy) decodes every column first.
//!
//! A heap tuple copied out of the slot keeps the row's TID: the virtual slot's own copy does
//! not, and ANALYZE sorts its sample by the copies' TIDs (an assertion failure on 50,000 rows,
//! 2026-09-23).

use std::rc::Weak;

use pgrx::pg_sys;

use super::read::Group;

#[repr(C)]
struct LazySlot {
	base: pg_sys::VirtualTupleTableSlot,
	/// the row group of the row the slot holds, while any of its columns is still undecoded
	group: Option<Weak<Group>>,
	row: u64,
}

static mut SLOT_OPS: Option<pg_sys::TupleTableSlotOps> = None;

pub fn slot_ops() -> *const pg_sys::TupleTableSlotOps {
	unsafe {
		let ops = &raw mut SLOT_OPS;
		if (*ops).is_none() {
			let mut o = pg_sys::TTSOpsVirtual;
			o.base_slot_size = std::mem::size_of::<LazySlot>();
			o.init = Some(init);
			o.release = Some(release);
			o.clear = Some(clear);
			o.getsomeattrs = Some(getsomeattrs);
			o.materialize = Some(materialize);
			o.copyslot = Some(copyslot);
			o.copy_heap_tuple = Some(copy_heap_tuple);
			o.copy_minimal_tuple = Some(copy_minimal_tuple);
			*ops = Some(o);
		}
		(*ops).as_ref().unwrap()
	}
}

/// # Safety
/// `slot` must be a valid slot.
pub unsafe fn is_lazy(slot: *mut pg_sys::TupleTableSlot) -> bool {
	(*slot).tts_ops == slot_ops()
}

/// Makes a lazy slot hold row `n` of `group`, nothing decoded yet. What the slot held before is
/// let go here rather than through ExecClearTuple, which is a call across the FFI boundary on
/// every row (the scan's row cost, 2026-09-23); only a slot that owns memory, having been
/// materialized, is cleared the long way.
///
/// # Safety
/// `slot` must be a slot of [`slot_ops`].
pub unsafe fn point(slot: *mut pg_sys::TupleTableSlot, group: Weak<Group>, n: u64) {
	if (*slot).tts_flags & pg_sys::TTS_FLAG_SHOULDFREE as u16 != 0 {
		pg_sys::ExecClearTuple(slot);
	}
	let s = slot as *mut LazySlot;
	(*s).group = Some(group);
	(*s).row = n;
	(*slot).tts_flags &= !(pg_sys::TTS_FLAG_EMPTY as u16);
	(*slot).tts_nvalid = 0;
}

fn virt() -> &'static pg_sys::TupleTableSlotOps {
	unsafe { &*&raw const pg_sys::TTSOpsVirtual }
}

unsafe fn forget(slot: *mut pg_sys::TupleTableSlot) {
	(*(slot as *mut LazySlot)).group = None;
}

/// Decodes every attribute not yet in `tts_values`, so the slot no longer needs its group.
unsafe fn complete(slot: *mut pg_sys::TupleTableSlot) {
	if (*slot).tts_flags & pg_sys::TTS_FLAG_EMPTY as u16 != 0 {
		return;
	}
	let natts = (*(*slot).tts_tupleDescriptor).natts;
	if ((*slot).tts_nvalid as i32) < natts {
		fill(slot, natts);
	}
	forget(slot);
}

unsafe fn fill(slot: *mut pg_sys::TupleTableSlot, natts: i32) {
	let s = slot as *mut LazySlot;
	let Some(group) = (*s).group.as_ref().and_then(Weak::upgrade) else {
		pgrx::error!("a sealed table's row was read after its scan moved past it");
	};
	group.fill((*s).row, (*slot).tts_nvalid as usize, natts as usize, (*slot).tts_tupleDescriptor, (*slot).tts_values, (*slot).tts_isnull);
	(*slot).tts_nvalid = natts as i16;
}

#[pgrx::pg_guard]
unsafe extern "C-unwind" fn init(slot: *mut pg_sys::TupleTableSlot) {
	std::ptr::write(&raw mut (*(slot as *mut LazySlot)).group, None);
	virt().init.unwrap()(slot);
}

#[pgrx::pg_guard]
unsafe extern "C-unwind" fn release(slot: *mut pg_sys::TupleTableSlot) {
	forget(slot);
	virt().release.unwrap()(slot);
}

#[pgrx::pg_guard]
unsafe extern "C-unwind" fn clear(slot: *mut pg_sys::TupleTableSlot) {
	forget(slot);
	virt().clear.unwrap()(slot);
}

#[pgrx::pg_guard]
unsafe extern "C-unwind" fn getsomeattrs(slot: *mut pg_sys::TupleTableSlot, natts: i32) {
	fill(slot, natts);
}

#[pgrx::pg_guard]
unsafe extern "C-unwind" fn materialize(slot: *mut pg_sys::TupleTableSlot) {
	complete(slot);
	virt().materialize.unwrap()(slot);
}

#[pgrx::pg_guard]
unsafe extern "C-unwind" fn copyslot(dst: *mut pg_sys::TupleTableSlot, src: *mut pg_sys::TupleTableSlot) {
	// the virtual copy clears the destination without going through its ops
	forget(dst);
	virt().copyslot.unwrap()(dst, src);
}

#[pgrx::pg_guard]
unsafe extern "C-unwind" fn copy_heap_tuple(slot: *mut pg_sys::TupleTableSlot) -> pg_sys::HeapTuple {
	complete(slot);
	let tuple = virt().copy_heap_tuple.unwrap()(slot);
	(*tuple).t_self = (*slot).tts_tid;
	(*tuple).t_tableOid = (*slot).tts_tableOid;
	tuple
}

#[cfg(not(feature = "pg18"))]
#[pgrx::pg_guard]
unsafe extern "C-unwind" fn copy_minimal_tuple(slot: *mut pg_sys::TupleTableSlot) -> pg_sys::MinimalTuple {
	complete(slot);
	virt().copy_minimal_tuple.unwrap()(slot)
}

#[cfg(feature = "pg18")]
#[pgrx::pg_guard]
unsafe extern "C-unwind" fn copy_minimal_tuple(slot: *mut pg_sys::TupleTableSlot, extra: pg_sys::Size) -> pg_sys::MinimalTuple {
	complete(slot);
	virt().copy_minimal_tuple.unwrap()(slot, extra)
}
