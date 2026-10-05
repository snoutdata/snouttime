//! Filling gaps: `locf(value)` and `interpolate(value [, at])`, WINDOW
//! functions used over the rows `gapfill` produced (or any ordered rows):
//!
//! ```sql
//! SELECT b, snouttime.locf(avg(v)) OVER (ORDER BY b)
//! FROM snouttime.gapfill('1 hour', lo, hi) AS b LEFT JOIN m ON snouttime.bucket('1 hour', m.ts) = b
//! GROUP BY b;
//! ```
//!
//! * `locf(value)`: the value, or when it is NULL the last non-NULL value before it in the
//!   window's order ("last observation carried forward"). NULL before the first one.
//! * `interpolate(value)`: the value, or when it is NULL the straight line between the
//!   non-NULL values either side of it, by row position; `interpolate(value, at)` does the same
//!   by the time `at` of each row, which is what buckets of unequal length (months) need. NULL
//!   where there is no value on one side.
//!
//! Both look at the whole PARTITION, not the frame, as `lag` and `lead` do: a frame clause
//! does not change them.
//!
//! They are real window functions, written against Postgres's window API (`windowapi.h`),
//! because `interpolate` has to know which row is the current one and an aggregate cannot.
//! pgrx has no bindings for that API, so the functions used are declared here.
//!
//! **Each row costs O(1), and each gap is scanned once.** The window's rows are in a
//! tuplestore whose read pointer moves a row at a time, so fetching an EARLIER row costs as many
//! steps as it is back: the first version fetched the last non-NULL row again for every row and
//! was quadratic (a 100,000-row `locf` did not finish in ten minutes). Now the current row is
//! read through `WinGetFuncArgCurrent` (no seek at all), the last non-NULL value is remembered as
//! the rows go by, and `interpolate` scans forward once per gap for the next one and remembers
//! that too. Nothing ever seeks backwards.

use std::ffi::c_void;

use pgrx::prelude::*;

const WINDOW_SEEK_HEAD: i32 = 1;

// Declared here, called only through `guarded`, so an ERROR inside Postgres unwinds the way
// pgrx expects rather than through Rust frames.
unsafe extern "C-unwind" {
	fn WinGetFuncArgInPartition(
		winobj: *mut c_void,
		argno: i32,
		relpos: i32,
		seektype: i32,
		set_mark: bool,
		isnull: *mut bool,
		isout: *mut bool,
	) -> pg_sys::Datum;
	fn WinGetFuncArgCurrent(winobj: *mut c_void, argno: i32, isnull: *mut bool) -> pg_sys::Datum;
	fn WinGetCurrentPosition(winobj: *mut c_void) -> i64;
	fn WinGetPartitionLocalMemory(winobj: *mut c_void, sz: usize) -> *mut c_void;
}

unsafe fn guarded<T>(f: impl FnOnce() -> T) -> T {
	pg_sys::ffi::pg_guard_ffi_boundary(f)
}

/// What one partition's rows have shown so far. Zeroed by Postgres when first asked for, so
/// `started` is false at the start of every partition.
#[repr(C)]
struct Scan {
	started: bool,
	/// The position of the row before the current one, to notice a row that was skipped.
	seen: i64,
	/// The last non-NULL value so far: its position (-1: none yet), and for `interpolate` its
	/// value and x. `locf`'s value lives in `Copy`, since it can be any type.
	last: i64,
	last_v: f64,
	last_x: f64,
	/// The next non-NULL row after the current one, once looked for: its position (-1: not
	/// looked for; NONE: there is none), value and x.
	next: i64,
	next_v: f64,
	next_x: f64,
}

const NONE: i64 = i64::MAX;

/// `locf`'s copy of the last non-NULL value, in the function's own memory, so it outlives the
/// window's read pointer moving on. One at a time: the previous copy is freed when replaced.
#[repr(C)]
struct Copy {
	resolved: bool,
	typlen: i16,
	typbyval: bool,
	held: bool,
	value: pg_sys::Datum,
}

struct Window {
	obj: *mut c_void,
	fcinfo: pg_sys::FunctionCallInfo,
}

impl Window {
	unsafe fn of(fcinfo: pg_sys::FunctionCallInfo) -> Window {
		let obj = (*fcinfo).context as *mut c_void;
		if obj.is_null() {
			error!("snouttime window functions must be called with OVER (...)");
		}
		Window { obj, fcinfo }
	}

	unsafe fn current(&self) -> i64 {
		guarded(|| WinGetCurrentPosition(self.obj))
	}

	/// Argument `argno` of the current row: (value, is NULL). No seek.
	unsafe fn here(&self, argno: i32) -> (pg_sys::Datum, bool) {
		let mut isnull = false;
		let d = guarded(|| WinGetFuncArgCurrent(self.obj, argno, &mut isnull));
		(d, isnull)
	}

	/// Argument `argno` at absolute position `pos`: (value, is NULL), or None past the end.
	/// Only ever asked for rows AHEAD of the ones already read.
	unsafe fn at(&self, argno: i32, pos: i64) -> Option<(pg_sys::Datum, bool)> {
		let (mut isnull, mut isout) = (false, false);
		let relpos = i32::try_from(pos).unwrap_or_else(|_| error!("a window partition of more than 2,147,483,647 rows is too large for a snouttime window function: split it with PARTITION BY"));
		let d = guarded(|| {
			WinGetFuncArgInPartition(self.obj, argno, relpos, WINDOW_SEEK_HEAD, false, &mut isnull, &mut isout)
		});
		if isout { None } else { Some((d, isnull)) }
	}

	/// This partition's scan, and whether the current row follows the last one seen. Postgres
	/// calls a window function once per row, in order; if that ever stops being true the cached
	/// neighbours could be wrong, so it is an error rather than a wrong answer.
	unsafe fn scan(&self, p: i64) -> &mut Scan {
		let s = &mut *(guarded(|| WinGetPartitionLocalMemory(self.obj, std::mem::size_of::<Scan>())) as *mut Scan);
		if !s.started {
			*s = Scan { started: true, seen: -1, last: -1, last_v: 0.0, last_x: 0.0, next: -1, next_v: 0.0, next_x: 0.0 };
		}
		if p != s.seen + 1 {
			error!("snouttime window function called out of row order ({} after {})", p, s.seen);
		}
		s.seen = p;
		s
	}

	unsafe fn copy(&self) -> &mut Copy {
		let flinfo = (*self.fcinfo).flinfo;
		if (*flinfo).fn_extra.is_null() {
			(*flinfo).fn_extra = pg_sys::MemoryContextAllocZero((*flinfo).fn_mcxt, std::mem::size_of::<Copy>());
		}
		let c = &mut *((*flinfo).fn_extra as *mut Copy);
		if !c.resolved {
			let typid = pg_sys::get_fn_expr_argtype(flinfo, 0);
			pg_sys::get_typlenbyval(typid, &mut c.typlen, &mut c.typbyval);
			c.resolved = true;
		}
		c
	}
}

unsafe fn null(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum {
	(*fcinfo).isnull = true;
	pg_sys::Datum::from(0)
}

/// The V1 calling-convention record Postgres looks for beside every C function.
macro_rules! v1 {
	($name:ident) => {
		#[no_mangle]
		pub extern "C" fn $name() -> &'static pg_sys::Pg_finfo_record {
			static V1: pg_sys::Pg_finfo_record = pg_sys::Pg_finfo_record { api_version: 1 };
			&V1
		}
	};
}

v1!(pg_finfo_snouttime_locf);
v1!(pg_finfo_snouttime_interpolate);
v1!(pg_finfo_snouttime_interpolate_at);

/// `locf(value anyelement) RETURNS anyelement`.
#[pg_guard]
#[no_mangle]
pub unsafe extern "C-unwind" fn snouttime_locf(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum {
	let w = Window::of(fcinfo);
	let p = w.current();
	let s = w.scan(p);
	let (d, isnull) = w.here(0);
	let c = w.copy();
	if !isnull {
		if c.held && !c.typbyval {
			pg_sys::pfree(c.value.cast_mut_ptr());
		}
		let old = pg_sys::MemoryContextSwitchTo((*(*fcinfo).flinfo).fn_mcxt);
		c.value = pg_sys::datumCopy(d, c.typbyval, c.typlen as i32);
		pg_sys::MemoryContextSwitchTo(old);
		c.held = true;
		s.last = p;
		return d;
	}
	if s.last < 0 {
		return null(fcinfo);
	}
	// The executor copies a by-reference result before it moves on, as for lag().
	c.value
}

/// The value at the current row, or the straight line between its non-NULL neighbours, with
/// `x` giving a row's position on the line from its (position, second argument).
unsafe fn interpolate(fcinfo: pg_sys::FunctionCallInfo, x: &dyn Fn(i64, Option<(pg_sys::Datum, bool)>) -> Option<f64>) -> pg_sys::Datum {
	let w = Window::of(fcinfo);
	let p = w.current();
	let s = w.scan(p);
	let with_at = (*fcinfo).nargs > 1;
	let x_here = x(p, if with_at { Some(w.here(1)) } else { None });
	let (d, isnull) = w.here(0);
	if !isnull {
		match (f64::from_datum(d, false), x_here) {
			(Some(v), Some(xp)) => {
				s.last = p;
				s.last_v = v;
				s.last_x = xp;
			}
			// A value with no time cannot anchor a line; it is returned, and not remembered.
			_ => {}
		}
		return d;
	}
	if s.last < 0 {
		return null(fcinfo);
	}
	if s.next != NONE && s.next <= p {
		// Look ahead once for this gap: the first row after here with a value and an x.
		let mut q = p + 1;
		s.next = NONE;
		while let Some((v, vnull)) = w.at(0, q) {
			if !vnull {
				let xq = x(q, if with_at { w.at(1, q) } else { None });
				if let (Some(v), Some(xq)) = (f64::from_datum(v, false), xq) {
					s.next = q;
					s.next_v = v;
					s.next_x = xq;
					break;
				}
			}
			q += 1;
		}
	}
	let Some(xp) = x_here else { return null(fcinfo) };
	if s.next == NONE {
		return null(fcinfo);
	}
	if s.next_x == s.last_x {
		return s.last_v.into_datum().unwrap();
	}
	(s.last_v + (s.next_v - s.last_v) * (xp - s.last_x) / (s.next_x - s.last_x)).into_datum().unwrap()
}

/// `interpolate(value double precision) RETURNS double precision`, by row position.
#[pg_guard]
#[no_mangle]
pub unsafe extern "C-unwind" fn snouttime_interpolate(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum {
	interpolate(fcinfo, &|pos, _| Some(pos as f64))
}

/// `interpolate(value double precision, at timestamptz) RETURNS double precision`, by time.
#[pg_guard]
#[no_mangle]
pub unsafe extern "C-unwind" fn snouttime_interpolate_at(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum {
	interpolate(fcinfo, &|_, at| match at {
		Some((d, false)) => Some(d.value() as i64 as f64),
		_ => None,
	})
}

pgrx::extension_sql!(
	r#"
CREATE FUNCTION snouttime.locf(value anyelement) RETURNS anyelement
	LANGUAGE c WINDOW IMMUTABLE PARALLEL SAFE AS 'MODULE_PATHNAME', 'snouttime_locf';
COMMENT ON FUNCTION snouttime.locf(anyelement) IS
	'The value, or the last non-NULL value before it in the window''s order';
CREATE FUNCTION snouttime.interpolate(value double precision) RETURNS double precision
	LANGUAGE c WINDOW IMMUTABLE PARALLEL SAFE AS 'MODULE_PATHNAME', 'snouttime_interpolate';
COMMENT ON FUNCTION snouttime.interpolate(double precision) IS
	'The value, or the straight line between its non-NULL neighbours, by row position';
CREATE FUNCTION snouttime.interpolate(value double precision, at timestamptz) RETURNS double precision
	LANGUAGE c WINDOW IMMUTABLE PARALLEL SAFE AS 'MODULE_PATHNAME', 'snouttime_interpolate_at';
COMMENT ON FUNCTION snouttime.interpolate(double precision, timestamptz) IS
	'The value, or the straight line between its non-NULL neighbours, by the time of each row';
"#,
	name = "fill_window_functions"
);
