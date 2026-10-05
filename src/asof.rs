//! The as-of join: `snouttime.asof_join(left_query, right_query, keys,
//! left_time, right_time, within, direction)`. Semantics in README.md ("As-of join").
//!
//! Both queries are opened as cursors ordered by (keys, time), and the two streams are merged in
//! one pass: O(n + m) comparisons after whatever the planner does to produce the order (an index
//! scan, or a sort). The SQL form, `LATERAL (... ORDER BY time DESC LIMIT 1)`, is an index probe
//! per left row; which wins depends on the sizes, and that is measured, not assumed.
//!
//! Comparisons use each type's default btree comparison function with the column's collation,
//! which is exactly the order `ORDER BY` produced, so the merge and the sort agree. A right row
//! that could still be the match is referenced inside the cursor's current batch, and copied only
//! when the batch is about to be freed, so the right side is not copied row by row.
//!
//! It is a C-convention SRF in materialize mode (a tuplestore filled in one call), written in
//! Rust: pgrx cannot declare a function returning `SETOF record` typed by the caller's column list.

use std::cmp::Ordering;
use std::ffi::{CStr, CString};

use pgrx::prelude::*;

const BATCH: i64 = 10_000;

/// One side: a cursor, its current batch, and where the key and time columns are.
struct Side {
	portal: pg_sys::Portal,
	table: *mut pg_sys::SPITupleTable,
	n: u64,
	i: u64,
	/// A copy of the rows' descriptor, kept for as long as the join runs.
	desc: pg_sys::TupleDesc,
	keys: Vec<i32>,
	time: i32,
}

impl Side {
	unsafe fn open(query: &str, keys: &[String], time: &str, what: &str, keep: pg_sys::MemoryContext) -> Side {
		probe(query, keys, time, what);
		let order: Vec<String> = keys.iter().chain(std::iter::once(&time.to_string())).map(|c| quote(c)).collect();
		let sql = format!("SELECT * FROM ({query}) AS _snouttime_{what} ORDER BY {}", order.join(", "));
		let sql = CString::new(sql).unwrap_or_else(|_| error!("the {what} query contains a NUL byte"));
		let portal = pg_sys::SPI_cursor_open_with_args(
			std::ptr::null(),
			sql.as_ptr(),
			0,
			std::ptr::null_mut(),
			std::ptr::null_mut(),
			std::ptr::null(),
			true,
			0,
		);
		if portal.is_null() {
			error!("could not open the {what} query");
		}
		let mut side = Side { portal, table: std::ptr::null_mut(), n: 0, i: 0, desc: std::ptr::null_mut(), keys: vec![], time: 0 };
		side.fetch();
		let desc = if side.table.is_null() { (*portal).tupDesc } else { (*side.table).tupdesc };
		let old = pg_sys::MemoryContextSwitchTo(keep);
		side.desc = pg_sys::CreateTupleDescCopy(desc);
		pg_sys::MemoryContextSwitchTo(old);
		let column = |name: &str| {
			let c = CString::new(name).unwrap();
			let n = pg_sys::SPI_fnumber(side.desc, c.as_ptr());
			if n <= 0 {
				error!("the {what} query has no column \"{name}\"");
			}
			n
		};
		side.keys = keys.iter().map(|k| column(k)).collect();
		side.time = column(time);
		side
	}

	unsafe fn fetch(&mut self) {
		if !self.table.is_null() {
			pg_sys::SPI_freetuptable(self.table);
		}
		pg_sys::SPI_cursor_fetch(self.portal, true, BATCH);
		self.table = pg_sys::SPI_tuptable;
		self.n = pg_sys::SPI_processed;
		self.i = 0;
	}

	unsafe fn current(&self) -> Option<pg_sys::HeapTuple> {
		(self.i < self.n).then(|| *(*self.table).vals.add(self.i as usize))
	}

	fn natts(&self) -> usize {
		unsafe { (*self.desc).natts as usize }
	}

	unsafe fn attr(&self, n: i32) -> &pg_sys::FormData_pg_attribute {
		crate::tuple_attr(self.desc, (n - 1) as usize)
	}
}

/// Run the query as `SELECT * FROM (query) LIMIT 0` first, so that a query that cannot be a
/// subquery (a DELETE, a syntax error) and a column that is not there are each reported against
/// the query the user wrote, not against the ORDER BY this module adds to it.
unsafe fn probe(query: &str, keys: &[String], time: &str, what: &str) {
	let sql = CString::new(format!("SELECT * FROM ({query}) AS _snouttime_probe LIMIT 0"))
		.unwrap_or_else(|_| error!("the {what} query contains a NUL byte"));
	let ran = PgTryBuilder::new(|| pg_sys::SPI_execute(sql.as_ptr(), true, 0))
		.catch_others(|e| match e {
			pg_sys::panic::CaughtError::PostgresError(ref r) | pg_sys::panic::CaughtError::ErrorReport(ref r) => error!(
				"the {what} query must be a SELECT, since it is run as SELECT * FROM (<query>): {}",
				r.message()
			),
			_ => e.rethrow(),
		})
		.execute();
	if ran < 0 || pg_sys::SPI_tuptable.is_null() {
		error!("the {what} query must be a SELECT");
	}
	let desc = (*pg_sys::SPI_tuptable).tupdesc;
	for name in keys.iter().map(String::as_str).chain(std::iter::once(time)) {
		let c = CString::new(name).unwrap();
		if pg_sys::SPI_fnumber(desc, c.as_ptr()) <= 0 {
			error!("the {what} query has no column \"{name}\"");
		}
	}
	pg_sys::SPI_freetuptable(pg_sys::SPI_tuptable);
}

fn quote(ident: &str) -> String {
	let c = CString::new(ident).unwrap_or_else(|_| error!("a column name contains a NUL byte"));
	unsafe { CStr::from_ptr(pg_sys::quote_identifier(c.as_ptr())).to_string_lossy().into_owned() }
}

/// A column's btree comparison, with the collation it was sorted in.
struct Cmp {
	finfo: *mut pg_sys::FmgrInfo,
	collation: pg_sys::Oid,
}

impl Cmp {
	unsafe fn of(left: &Side, l: i32, right: &Side, r: i32, what: &str) -> Cmp {
		let (la, ra) = (left.attr(l), right.attr(r));
		if la.atttypid != ra.atttypid {
			error!("the {what} columns must be the same type on both sides");
		}
		if la.attcollation != ra.attcollation {
			error!("the {what} columns must have the same collation on both sides");
		}
		let entry = pg_sys::lookup_type_cache(la.atttypid, pg_sys::TYPECACHE_CMP_PROC_FINFO as i32);
		if (*entry).cmp_proc_finfo.fn_oid == pg_sys::InvalidOid {
			error!("the {what} columns' type has no ordering, so it cannot be joined in order");
		}
		Cmp { finfo: &mut (*entry).cmp_proc_finfo, collation: la.attcollation }
	}

	/// NULLs last, as ORDER BY ... ASC sorted them.
	unsafe fn compare(&self, a: Option<pg_sys::Datum>, b: Option<pg_sys::Datum>) -> Ordering {
		match (a, b) {
			(None, None) => Ordering::Equal,
			(None, Some(_)) => Ordering::Greater,
			(Some(_), None) => Ordering::Less,
			(Some(a), Some(b)) => {
				let r = pg_sys::FunctionCall2Coll(self.finfo, self.collation, a, b).value() as i32;
				r.cmp(&0)
			}
		}
	}
}

unsafe fn get(tuple: pg_sys::HeapTuple, desc: pg_sys::TupleDesc, n: i32) -> Option<pg_sys::Datum> {
	let mut isnull = false;
	let d = pg_sys::SPI_getbinval(tuple, desc, n, &mut isnull);
	(!isnull).then_some(d)
}

struct Join {
	left: Side,
	right: Side,
	keys: Vec<Cmp>,
	time: Cmp,
	/// For `within`: how to turn a time datum into microseconds, and the limit.
	within: Option<(fn(pg_sys::Datum) -> i64, i64)>,
}

impl Join {
	/// The key columns of a left row against a right row.
	unsafe fn keys_cmp(&self, l: pg_sys::HeapTuple, r: pg_sys::HeapTuple) -> Ordering {
		for (i, c) in self.keys.iter().enumerate() {
			let o = c.compare(get(r, self.right.desc, self.right.keys[i]), get(l, self.left.desc, self.left.keys[i]));
			if o != Ordering::Equal {
				return o;
			}
		}
		Ordering::Equal
	}

	/// Right row against left row, in the (keys, time) order both cursors are in.
	unsafe fn cmp(&self, r: pg_sys::HeapTuple, l: pg_sys::HeapTuple) -> Ordering {
		self.keys_cmp(l, r).then_with(|| {
			self.time.compare(get(r, self.right.desc, self.right.time), get(l, self.left.desc, self.left.time))
		})
	}

	unsafe fn complete(&self, t: pg_sys::HeapTuple, side: &Side) -> bool {
		side.keys.iter().all(|&k| get(t, side.desc, k).is_some()) && get(t, side.desc, side.time).is_some()
	}

	/// Whether right row `r` is close enough in time to left row `l`.
	unsafe fn near(&self, l: pg_sys::HeapTuple, r: pg_sys::HeapTuple) -> bool {
		let Some((micros, limit)) = self.within else { return true };
		let (Some(lt), Some(rt)) = (get(l, self.left.desc, self.left.time), get(r, self.right.desc, self.right.time)) else {
			return false;
		};
		(micros(lt) - micros(rt)).abs() <= limit
	}
}

fn micros_ts(d: pg_sys::Datum) -> i64 {
	d.value() as i64
}

fn micros_date(d: pg_sys::Datum) -> i64 {
	(d.value() as i32) as i64 * 86_400_000_000
}

unsafe fn text_arg(fcinfo: pg_sys::FunctionCallInfo, n: usize, what: &str) -> Option<String> {
	let a = (*fcinfo).args.as_slice(n + 1)[n];
	if a.isnull {
		return None;
	}
	String::from_datum(a.value, false).or_else(|| error!("{what} is not text"))
}

#[no_mangle]
pub extern "C" fn pg_finfo_snouttime_asof_join() -> &'static pg_sys::Pg_finfo_record {
	static V1: pg_sys::Pg_finfo_record = pg_sys::Pg_finfo_record { api_version: 1 };
	&V1
}

/// asof_join(left_query text, right_query text, keys text[], left_time text,
///           right_time text, within interval, direction text) RETURNS SETOF record
#[pg_guard]
#[no_mangle]
pub unsafe extern "C-unwind" fn snouttime_asof_join(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum {
	let args = (*fcinfo).args.as_slice(7);
	let left_query = text_arg(fcinfo, 0, "left_query").unwrap_or_else(|| error!("left_query must not be NULL"));
	let right_query = text_arg(fcinfo, 1, "right_query").unwrap_or_else(|| error!("right_query must not be NULL"));
	let keys: Vec<String> = if args[2].isnull {
		error!("keys must not be NULL; an empty array joins on time alone");
	} else {
		Vec::<Option<String>>::from_datum(args[2].value, false)
			.unwrap_or_default()
			.into_iter()
			.map(|k| k.unwrap_or_else(|| error!("keys must name columns, and one of them is NULL")))
			.collect()
	};
	let left_time = text_arg(fcinfo, 3, "left_time").unwrap_or_else(|| error!("left_time must not be NULL"));
	let right_time = text_arg(fcinfo, 4, "right_time").unwrap_or_else(|| left_time.clone());
	let within = if args[5].isnull { None } else { Interval::from_datum(args[5].value, false) };
	let forward = match text_arg(fcinfo, 6, "direction").as_deref() {
		None | Some("backward") => false,
		Some("forward") => true,
		Some(other) => error!("direction must be 'backward' or 'forward', not '{other}'"),
	};

	pg_sys::InitMaterializedSRF(fcinfo, pg_sys::MAT_SRF_USE_EXPECTED_DESC);
	let rsinfo = (*fcinfo).resultinfo as *mut pg_sys::ReturnSetInfo;
	let (store, out_desc) = ((*rsinfo).setResult, (*rsinfo).setDesc);

	// Everything that must outlive a cursor batch lives here; `row` is reset for every left row.
	let keep = pg_sys::AllocSetContextCreateInternal(
		pg_sys::CurrentMemoryContext,
		c"snouttime asof_join".as_ptr(),
		0,
		8 * 1024,
		8 * 1024 * 1024,
	);
	let row = pg_sys::AllocSetContextCreateInternal(keep, c"snouttime asof_join row".as_ptr(), 0, 8 * 1024, 8 * 1024 * 1024);

	if pg_sys::SPI_connect() < 0 {
		error!("could not connect to SPI");
	}
	let left = Side::open(&left_query, &keys, &left_time, "left", keep);
	let right = Side::open(&right_query, &keys, &right_time, "right", keep);
	let keys_cmp: Vec<Cmp> = (0..keys.len())
		.map(|i| Cmp::of(&left, left.keys[i], &right, right.keys[i], &format!("key \"{}\"", keys[i])))
		.collect();
	let time = Cmp::of(&left, left.time, &right, right.time, "time");
	let within = within.map(|w| {
		if w.months() != 0 {
			error!("within cannot be in months: a month has no fixed length");
		}
		let limit = w.days() as i64 * 86_400_000_000 + w.micros();
		if limit < 0 {
			error!("within must not be negative");
		}
		let micros: fn(pg_sys::Datum) -> i64 = match left.attr(left.time).atttypid {
			pg_sys::TIMESTAMPTZOID | pg_sys::TIMESTAMPOID => micros_ts,
			pg_sys::DATEOID => micros_date,
			_ => error!("within needs a timestamptz, timestamp or date time column"),
		};
		(micros, limit)
	});

	// The column list after AS must be the left's columns, then the right's.
	let (ln, rn) = (left.natts(), right.natts());
	let out_n = (*out_desc).natts as usize;
	let out_attrs = crate::tuple_attrs(out_desc);
	let expected: Vec<pg_sys::Oid> = (1..=ln as i32)
		.map(|i| left.attr(i).atttypid)
		.chain((1..=rn as i32).map(|i| right.attr(i).atttypid))
		.collect();
	if out_n != ln + rn || out_attrs.iter().zip(&expected).any(|(a, &t)| a.atttypid != t) {
		let names: Vec<String> = expected
			.iter()
			.map(|&t| CStr::from_ptr(pg_sys::format_type_be(t)).to_string_lossy().into_owned())
			.collect();
		error!(
			"asof_join returns the left query's columns and then the right query's, so the column list must be ({})",
			names.join(", ")
		);
	}

	let mut join = Join { left, right, keys: keys_cmp, time, within };
	let mut values = vec![pg_sys::Datum::from(0); ln + rn];
	let mut nulls = vec![true; ln + rn];
	// The right row that is the match so far (backward): in the current batch, or copied.
	let mut matched_index: Option<u64> = None;
	let mut matched_copy: pg_sys::HeapTuple = std::ptr::null_mut();

	loop {
		let Some(l) = join.left.current() else {
			if join.left.n == 0 {
				break;
			}
			join.left.fetch();
			if join.left.n == 0 {
				break;
			}
			continue;
		};
		let old = pg_sys::MemoryContextSwitchTo(row);
		let mut matched: pg_sys::HeapTuple = std::ptr::null_mut();
		if join.complete(l, &join.left) {
			// Move the right side up to this left row.
			loop {
				let r = match join.right.current() {
					Some(r) => r,
					None if join.right.n == 0 => break,
					None => {
						// The batch is about to be freed: a match in it must be copied first.
						if let Some(i) = matched_index.take() {
							let t = *(*join.right.table).vals.add(i as usize);
							pg_sys::MemoryContextSwitchTo(keep);
							if !matched_copy.is_null() {
								pg_sys::heap_freetuple(matched_copy);
							}
							matched_copy = pg_sys::heap_copytuple(t);
							pg_sys::MemoryContextSwitchTo(row);
						}
						join.right.fetch();
						continue;
					}
				};
				let o = join.cmp(r, l);
				let consume = if forward { o == Ordering::Less } else { o != Ordering::Greater };
				if !consume {
					break;
				}
				if !forward && join.complete(r, &join.right) {
					matched_index = Some(join.right.i);
				}
				join.right.i += 1;
			}
			let candidate = if forward {
				join.right.current().filter(|&r| join.complete(r, &join.right))
			} else if let Some(i) = matched_index {
				Some(*(*join.right.table).vals.add(i as usize))
			} else {
				(!matched_copy.is_null()).then_some(matched_copy)
			};
			if let Some(r) = candidate {
				if join.keys_cmp(l, r) == Ordering::Equal && join.near(l, r) {
					matched = r;
				}
			}
		}
		pg_sys::heap_deform_tuple(l, join.left.desc, values.as_mut_ptr(), nulls.as_mut_ptr());
		if matched.is_null() {
			nulls[ln..].iter_mut().for_each(|n| *n = true);
		} else {
			pg_sys::heap_deform_tuple(matched, join.right.desc, values[ln..].as_mut_ptr(), nulls[ln..].as_mut_ptr());
		}
		pg_sys::tuplestore_putvalues(store, out_desc, values.as_ptr(), nulls.as_ptr());
		pg_sys::MemoryContextSwitchTo(old);
		pg_sys::MemoryContextReset(row);
		join.left.i += 1;
	}

	pg_sys::SPI_cursor_close(join.left.portal);
	pg_sys::SPI_cursor_close(join.right.portal);
	pg_sys::SPI_finish();
	pg_sys::MemoryContextDelete(keep);
	pg_sys::Datum::from(0)
}

pgrx::extension_sql!(
	r#"
CREATE FUNCTION snouttime.asof_join(
	left_query text, right_query text, keys text[], left_time text,
	right_time text DEFAULT NULL, within interval DEFAULT NULL, direction text DEFAULT 'backward'
) RETURNS SETOF record
	LANGUAGE c VOLATILE AS 'MODULE_PATHNAME', 'snouttime_asof_join';
COMMENT ON FUNCTION snouttime.asof_join(text, text, text[], text, text, interval, text) IS
	'For each left row, the right row with equal keys and the latest time at or before its own (or the earliest at or after, with direction => ''forward''); see the README';
"#,
	name = "asof_join"
);

// ---- window_join: an aggregate of the right rows in a window around each left row ----
//
// kdb+'s `wj`. For each left row with keys K and time t, `aggregate` over the right rows with
// keys K and a time in [t - before, t + after], of the column `value` (double precision). Both
// sides stream in (keys, time) order and the window only ever slides forward, so each right row
// enters and leaves it once. The window holds (time, value) pairs, never rows: `sum` and `avg`
// keep a running sum that is recomputed from the window whenever as many values have left it as
// are in it (so floating-point drift cannot accumulate, at O(1) amortised), `min` and `max` keep
// monotonic deques, `first` and `last` are its ends, `count` its length. NULL values are not in
// it (they are not counted, as SQL's count(value) does not count them). An empty window is NULL,
// or 0 for `count`.

use std::collections::VecDeque;

#[derive(Clone, Copy, PartialEq)]
enum Agg {
	Count,
	Sum,
	Avg,
	Min,
	Max,
	First,
	Last,
}

#[derive(Default)]
struct Window {
	items: VecDeque<(i64, f64)>,
	sum: f64,
	left_since_recompute: usize,
	/// Candidates for min and max: values increasing (min) or decreasing (max) from the front.
	mins: VecDeque<(i64, f64)>,
	maxes: VecDeque<(i64, f64)>,
}

impl Window {
	fn clear(&mut self) {
		*self = Window::default();
	}

	fn push(&mut self, t: i64, v: f64) {
		self.items.push_back((t, v));
		self.sum += v;
		while self.mins.back().is_some_and(|&(_, m)| m > v) {
			self.mins.pop_back();
		}
		self.mins.push_back((t, v));
		while self.maxes.back().is_some_and(|&(_, m)| m < v) {
			self.maxes.pop_back();
		}
		self.maxes.push_back((t, v));
	}

	/// Drop everything before `from`.
	fn trim(&mut self, from: i64) {
		while self.items.front().is_some_and(|&(t, _)| t < from) {
			let (_, v) = self.items.pop_front().unwrap();
			self.sum -= v;
			self.left_since_recompute += 1;
		}
		while self.mins.front().is_some_and(|&(t, _)| t < from) {
			self.mins.pop_front();
		}
		while self.maxes.front().is_some_and(|&(t, _)| t < from) {
			self.maxes.pop_front();
		}
		if self.left_since_recompute > self.items.len() {
			self.sum = self.items.iter().map(|&(_, v)| v).sum();
			self.left_since_recompute = 0;
		}
	}

	fn value(&self, agg: Agg) -> Option<f64> {
		if self.items.is_empty() {
			return (agg == Agg::Count).then_some(0.0);
		}
		Some(match agg {
			Agg::Count => self.items.len() as f64,
			Agg::Sum => self.sum,
			Agg::Avg => self.sum / self.items.len() as f64,
			Agg::Min => self.mins.front().unwrap().1,
			Agg::Max => self.maxes.front().unwrap().1,
			Agg::First => self.items.front().unwrap().1,
			Agg::Last => self.items.back().unwrap().1,
		})
	}
}

fn interval_micros(w: &Interval, what: &str) -> i64 {
	if w.months() != 0 {
		error!("{what} cannot be in months: a month has no fixed length");
	}
	let us = w.days() as i64 * 86_400_000_000 + w.micros();
	if us < 0 {
		error!("{what} must not be negative");
	}
	us
}

#[no_mangle]
pub extern "C" fn pg_finfo_snouttime_window_join() -> &'static pg_sys::Pg_finfo_record {
	static V1: pg_sys::Pg_finfo_record = pg_sys::Pg_finfo_record { api_version: 1 };
	&V1
}

/// window_join(left_query text, right_query text, keys text[], left_time text, value text,
///             before interval, after interval, aggregate text, right_time text) RETURNS SETOF record
#[pg_guard]
#[no_mangle]
pub unsafe extern "C-unwind" fn snouttime_window_join(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum {
	let args = (*fcinfo).args.as_slice(9);
	let left_query = text_arg(fcinfo, 0, "left_query").unwrap_or_else(|| error!("left_query must not be NULL"));
	let right_query = text_arg(fcinfo, 1, "right_query").unwrap_or_else(|| error!("right_query must not be NULL"));
	let keys: Vec<String> = if args[2].isnull {
		error!("keys must not be NULL; an empty array joins on time alone");
	} else {
		Vec::<Option<String>>::from_datum(args[2].value, false)
			.unwrap_or_default()
			.into_iter()
			.map(|k| k.unwrap_or_else(|| error!("keys must name columns, and one of them is NULL")))
			.collect()
	};
	let left_time = text_arg(fcinfo, 3, "left_time").unwrap_or_else(|| error!("left_time must not be NULL"));
	let value = text_arg(fcinfo, 4, "value").unwrap_or_else(|| error!("value must not be NULL"));
	let before = if args[5].isnull { error!("before must not be NULL") } else { Interval::from_datum(args[5].value, false).unwrap() };
	let after = if args[6].isnull { Interval::from_micros(0) } else { Interval::from_datum(args[6].value, false).unwrap() };
	let agg = match text_arg(fcinfo, 7, "aggregate").as_deref() {
		None | Some("avg") => Agg::Avg,
		Some("count") => Agg::Count,
		Some("sum") => Agg::Sum,
		Some("min") => Agg::Min,
		Some("max") => Agg::Max,
		Some("first") => Agg::First,
		Some("last") => Agg::Last,
		Some(other) => error!("aggregate must be one of count, sum, avg, min, max, first, last, not '{other}'"),
	};
	let right_time = text_arg(fcinfo, 8, "right_time").unwrap_or_else(|| left_time.clone());
	let (before, after) = (interval_micros(&before, "before"), interval_micros(&after, "after"));

	pg_sys::InitMaterializedSRF(fcinfo, pg_sys::MAT_SRF_USE_EXPECTED_DESC);
	let rsinfo = (*fcinfo).resultinfo as *mut pg_sys::ReturnSetInfo;
	let (store, out_desc) = ((*rsinfo).setResult, (*rsinfo).setDesc);
	let keep = pg_sys::AllocSetContextCreateInternal(
		pg_sys::CurrentMemoryContext,
		c"snouttime window_join".as_ptr(),
		0,
		8 * 1024,
		8 * 1024 * 1024,
	);
	let row = pg_sys::AllocSetContextCreateInternal(keep, c"snouttime window_join row".as_ptr(), 0, 8 * 1024, 8 * 1024 * 1024);

	if pg_sys::SPI_connect() < 0 {
		error!("could not connect to SPI");
	}
	let left = Side::open(&left_query, &keys, &left_time, "left", keep);
	let right = Side::open(&right_query, &keys, &right_time, "right", keep);
	let keys_cmp: Vec<Cmp> = (0..keys.len())
		.map(|i| Cmp::of(&left, left.keys[i], &right, right.keys[i], &format!("key \"{}\"", keys[i])))
		.collect();
	let time = Cmp::of(&left, left.time, &right, right.time, "time");
	let micros: fn(pg_sys::Datum) -> i64 = match left.attr(left.time).atttypid {
		pg_sys::TIMESTAMPTZOID | pg_sys::TIMESTAMPOID => micros_ts,
		pg_sys::DATEOID => micros_date,
		_ => error!("window_join needs a timestamptz, timestamp or date time column"),
	};
	let value_col = {
		let c = CString::new(value.as_str()).unwrap();
		let n = pg_sys::SPI_fnumber(right.desc, c.as_ptr());
		if n <= 0 {
			error!("the right query has no column \"{value}\"");
		}
		if right.attr(n).atttypid != pg_sys::FLOAT8OID {
			error!("the value column must be double precision; cast it in the right query");
		}
		n
	};

	let ln = left.natts();
	let out_n = (*out_desc).natts as usize;
	let out_attrs = crate::tuple_attrs(out_desc);
	let expected: Vec<pg_sys::Oid> =
		(1..=ln as i32).map(|i| left.attr(i).atttypid).chain(std::iter::once(pg_sys::FLOAT8OID)).collect();
	if out_n != ln + 1 || out_attrs.iter().zip(&expected).any(|(a, &t)| a.atttypid != t) {
		let names: Vec<String> = expected
			.iter()
			.map(|&t| CStr::from_ptr(pg_sys::format_type_be(t)).to_string_lossy().into_owned())
			.collect();
		error!(
			"window_join returns the left query's columns and then the aggregate, so the column list must be ({})",
			names.join(", ")
		);
	}

	let mut join = Join { left, right, keys: keys_cmp, time, within: None };
	let mut window = Window::default();
	// A copy of one right row of the key the window holds, to tell when the left key moves on.
	let mut window_key: pg_sys::HeapTuple = std::ptr::null_mut();
	let mut values = vec![pg_sys::Datum::from(0); ln + 1];
	let mut nulls = vec![true; ln + 1];

	loop {
		let Some(l) = join.left.current() else {
			if join.left.n == 0 {
				break;
			}
			join.left.fetch();
			if join.left.n == 0 {
				break;
			}
			continue;
		};
		let old = pg_sys::MemoryContextSwitchTo(row);
		let mut result: Option<f64> = None;
		if join.complete(l, &join.left) {
			let t = micros(get(l, join.left.desc, join.left.time).unwrap());
			if !window_key.is_null() && join.keys_cmp(l, window_key) != Ordering::Equal {
				window.clear();
				pg_sys::heap_freetuple(window_key);
				window_key = std::ptr::null_mut();
			}
			// Take in every right row up to t + after (skipping keys before this one).
			loop {
				let r = match join.right.current() {
					Some(r) => r,
					None if join.right.n == 0 => break,
					None => {
						join.right.fetch();
						continue;
					}
				};
				let k = join.keys_cmp(l, r);
				if k == Ordering::Greater {
					break;
				}
				if k == Ordering::Equal {
					let Some(rt) = get(r, join.right.desc, join.right.time) else { break };
					let rt = micros(rt);
					if rt > t + after {
						break;
					}
					if window_key.is_null() {
						pg_sys::MemoryContextSwitchTo(keep);
						window_key = pg_sys::heap_copytuple(r);
						pg_sys::MemoryContextSwitchTo(row);
					}
					if let Some(v) = get(r, join.right.desc, value_col) {
						window.push(rt, f64::from_bits(v.value() as u64));
					}
				}
				join.right.i += 1;
			}
			window.trim(t - before);
			result = if window_key.is_null() {
				(agg == Agg::Count).then_some(0.0)
			} else {
				window.value(agg)
			};
		}
		pg_sys::heap_deform_tuple(l, join.left.desc, values.as_mut_ptr(), nulls.as_mut_ptr());
		match result {
			Some(v) => {
				values[ln] = v.into_datum().unwrap();
				nulls[ln] = false;
			}
			None => nulls[ln] = true,
		}
		pg_sys::tuplestore_putvalues(store, out_desc, values.as_ptr(), nulls.as_ptr());
		pg_sys::MemoryContextSwitchTo(old);
		pg_sys::MemoryContextReset(row);
		join.left.i += 1;
	}

	pg_sys::SPI_cursor_close(join.left.portal);
	pg_sys::SPI_cursor_close(join.right.portal);
	pg_sys::SPI_finish();
	pg_sys::MemoryContextDelete(keep);
	pg_sys::Datum::from(0)
}

pgrx::extension_sql!(
	r#"
CREATE FUNCTION snouttime.window_join(
	left_query text, right_query text, keys text[], left_time text, value text,
	before interval, after interval DEFAULT '0', aggregate text DEFAULT 'avg', right_time text DEFAULT NULL
) RETURNS SETOF record
	LANGUAGE c VOLATILE AS 'MODULE_PATHNAME', 'snouttime_window_join';
COMMENT ON FUNCTION snouttime.window_join(text, text, text[], text, text, interval, interval, text, text) IS
	'For each left row, an aggregate of the right rows with equal keys whose time is within [time - before, time + after]; see the README';
"#,
	name = "window_join"
);
