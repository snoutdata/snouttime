//! `first(value, at)` and `last(value, at)`: the value at the earliest
//! or latest `at` in a group.
//!
//! * `value` is any type; `at` is `timestamptz`, `timestamp`, `date`, `bigint` or `integer`.
//! * A row whose `at` is NULL is ignored. A row whose `value` is NULL counts: if it is the
//!   earliest, `first` is NULL.
//! * Rows with the same `at` are a tie, and which of them wins is not defined (it depends on
//!   the order rows arrive in, which a parallel plan does not fix). Break a tie by making `at`
//!   unique.
//! * The state combines (a rollup is built from partial states), serialises for parallel
//!   aggregation through the value type's own binary send/receive functions, and so works for
//!   any type that has them, which is every built-in one.
//!
//! pgrx cannot express a polymorphic aggregate with an `internal` state and extra final-function
//! arguments, so these are written against the plain C calling convention, like `fill.rs`.

use pgrx::prelude::*;

/// A group's best row so far. Lives in the aggregate's memory context.
#[repr(C)]
struct Point {
	typid: pg_sys::Oid,
	typlen: i16,
	typbyval: bool,
	has: bool,
	at: i64,
	isnull: bool,
	value: pg_sys::Datum,
}

#[derive(Clone, Copy, PartialEq)]
enum Which {
	First,
	Last,
}

impl Which {
	fn wins(self, candidate: i64, held: i64) -> bool {
		match self {
			Which::First => candidate < held,
			Which::Last => candidate > held,
		}
	}
}

unsafe fn agg_context(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::MemoryContext {
	let mut ctx: pg_sys::MemoryContext = std::ptr::null_mut();
	if pg_sys::AggCheckCallContext(fcinfo, &mut ctx) == 0 {
		error!("snouttime.first/last support functions may only be called by an aggregate");
	}
	ctx
}

unsafe fn arg(fcinfo: pg_sys::FunctionCallInfo, n: usize) -> (pg_sys::Datum, bool) {
	let a = (*fcinfo).args.as_slice(n + 1)[n];
	(a.value, a.isnull)
}

unsafe fn null(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum {
	(*fcinfo).isnull = true;
	pg_sys::Datum::from(0)
}

/// The `at` argument as an i64 that orders the same way, whatever its type.
unsafe fn at_of(fcinfo: pg_sys::FunctionCallInfo, d: pg_sys::Datum) -> i64 {
	let typid = pg_sys::get_fn_expr_argtype((*fcinfo).flinfo, 2);
	if typid == pg_sys::INT4OID || typid == pg_sys::DATEOID {
		d.value() as i32 as i64
	} else {
		d.value() as i64
	}
}

/// Put `value` into `p`, copied into `ctx`, freeing what it held.
unsafe fn hold(p: &mut Point, ctx: pg_sys::MemoryContext, at: i64, value: pg_sys::Datum, isnull: bool) {
	if p.has && !p.isnull && !p.typbyval {
		pg_sys::pfree(p.value.cast_mut_ptr());
	}
	p.has = true;
	p.at = at;
	p.isnull = isnull;
	p.value = if isnull {
		pg_sys::Datum::from(0)
	} else {
		let old = pg_sys::MemoryContextSwitchTo(ctx);
		let copy = pg_sys::datumCopy(value, p.typbyval, p.typlen as i32);
		pg_sys::MemoryContextSwitchTo(old);
		copy
	};
}

unsafe fn new_point(ctx: pg_sys::MemoryContext, typid: pg_sys::Oid) -> *mut Point {
	let p = pg_sys::MemoryContextAllocZero(ctx, std::mem::size_of::<Point>()) as *mut Point;
	(*p).typid = typid;
	pg_sys::get_typlenbyval(typid, &mut (*p).typlen, &mut (*p).typbyval);
	p
}

/// (internal, value anyelement, at <time>) -> internal
unsafe fn trans(fcinfo: pg_sys::FunctionCallInfo, which: Which) -> pg_sys::Datum {
	let ctx = agg_context(fcinfo);
	let (state, state_null) = arg(fcinfo, 0);
	let p = if state_null {
		new_point(ctx, pg_sys::get_fn_expr_argtype((*fcinfo).flinfo, 1))
	} else {
		state.cast_mut_ptr::<Point>()
	};
	let (at, at_null) = arg(fcinfo, 2);
	if !at_null {
		let at = at_of(fcinfo, at);
		if !(*p).has || which.wins(at, (*p).at) {
			let (value, value_null) = arg(fcinfo, 1);
			hold(&mut *p, ctx, at, value, value_null);
		}
	}
	pg_sys::Datum::from(p)
}

/// (internal, internal) -> internal
unsafe fn combine(fcinfo: pg_sys::FunctionCallInfo, which: Which) -> pg_sys::Datum {
	let ctx = agg_context(fcinfo);
	let (a, a_null) = arg(fcinfo, 0);
	let (b, b_null) = arg(fcinfo, 1);
	if b_null {
		return if a_null { null(fcinfo) } else { a };
	}
	let b = &*b.cast_mut_ptr::<Point>();
	let a = if a_null { new_point(ctx, b.typid) } else { a.cast_mut_ptr::<Point>() };
	if b.has && (!(*a).has || which.wins(b.at, (*a).at)) {
		hold(&mut *a, ctx, b.at, b.value, b.isnull);
	}
	pg_sys::Datum::from(a)
}

// Serialised form: typid u32, has u8, isnull u8, at i64, then the value's binary send
// representation (the rest of the bytea). Little-endian: a serialised state only ever goes
// from a parallel worker to its leader on the same machine.
const HEADER: usize = 4 + 1 + 1 + 8;

/// (internal) -> bytea
unsafe fn serialize(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum {
	agg_context(fcinfo);
	let (s, _) = arg(fcinfo, 0);
	let p = &*s.cast_mut_ptr::<Point>();
	let mut bytes = Vec::with_capacity(HEADER + 16);
	bytes.extend_from_slice(&p.typid.to_u32().to_le_bytes());
	bytes.push(p.has as u8);
	bytes.push(p.isnull as u8);
	bytes.extend_from_slice(&p.at.to_le_bytes());
	if p.has && !p.isnull {
		let (mut send, mut varlena) = (pg_sys::InvalidOid, false);
		pg_sys::getTypeBinaryOutputInfo(p.typid, &mut send, &mut varlena);
		let sent = pg_sys::OidSendFunctionCall(send, p.value);
		bytes.extend_from_slice(pgrx::varlena::varlena_to_byte_slice(sent.cast()));
	}
	pg_sys::Datum::from(pgrx::rust_byte_slice_to_bytea(&bytes).into_pg())
}

/// (bytea, internal) -> internal
unsafe fn deserialize(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum {
	let ctx = agg_context(fcinfo);
	let (d, _) = arg(fcinfo, 0);
	let v = pg_sys::pg_detoast_datum_packed(d.cast_mut_ptr());
	let bytes = pgrx::varlena::varlena_to_byte_slice(v);
	if bytes.len() < HEADER {
		error!("snouttime: a serialised first/last state is too short");
	}
	let typid = pg_sys::Oid::from(u32::from_le_bytes(bytes[0..4].try_into().unwrap()));
	let p = new_point(ctx, typid);
	let (has, isnull) = (bytes[4] != 0, bytes[5] != 0);
	let at = i64::from_le_bytes(bytes[6..14].try_into().unwrap());
	if has {
		let value = if isnull {
			pg_sys::Datum::from(0)
		} else {
			// A receive function wants a StringInfo whose data is NUL-terminated.
			let rest = &bytes[HEADER..];
			let old = pg_sys::MemoryContextSwitchTo(ctx);
			let buf = pg_sys::palloc(rest.len() + 1) as *mut u8;
			std::ptr::copy_nonoverlapping(rest.as_ptr(), buf, rest.len());
			*buf.add(rest.len()) = 0;
			let mut si = pg_sys::StringInfoData {
				data: buf as *mut std::ffi::c_char,
				len: rest.len() as i32,
				maxlen: rest.len() as i32 + 1,
				cursor: 0,
			};
			let (mut recv, mut ioparam) = (pg_sys::InvalidOid, pg_sys::InvalidOid);
			pg_sys::getTypeBinaryInputInfo(typid, &mut recv, &mut ioparam);
			let value = pg_sys::OidReceiveFunctionCall(recv, &mut si, ioparam, -1);
			pg_sys::MemoryContextSwitchTo(old);
			if si.cursor != si.len {
				error!("snouttime: a serialised first/last value was not read to its end");
			}
			value
		};
		(*p).has = true;
		(*p).at = at;
		(*p).isnull = isnull;
		(*p).value = value;
	}
	pg_sys::Datum::from(p)
}

/// (internal, value anyelement, at <time>) -> anyelement
unsafe fn finish(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum {
	let (s, s_null) = arg(fcinfo, 0);
	if s_null {
		return null(fcinfo);
	}
	let p = &*s.cast_mut_ptr::<Point>();
	if !p.has || p.isnull {
		return null(fcinfo);
	}
	p.value
}

macro_rules! c_function {
	($name:ident, $finfo:ident, $body:expr) => {
		#[no_mangle]
		pub extern "C" fn $finfo() -> &'static pg_sys::Pg_finfo_record {
			static V1: pg_sys::Pg_finfo_record = pg_sys::Pg_finfo_record { api_version: 1 };
			&V1
		}
		#[pg_guard]
		#[no_mangle]
		pub unsafe extern "C-unwind" fn $name(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum {
			($body)(fcinfo)
		}
	};
}

c_function!(snouttime_first_trans, pg_finfo_snouttime_first_trans, |f| trans(f, Which::First));
c_function!(snouttime_last_trans, pg_finfo_snouttime_last_trans, |f| trans(f, Which::Last));
c_function!(snouttime_first_combine, pg_finfo_snouttime_first_combine, |f| combine(f, Which::First));
c_function!(snouttime_last_combine, pg_finfo_snouttime_last_combine, |f| combine(f, Which::Last));
c_function!(snouttime_point_serialize, pg_finfo_snouttime_point_serialize, serialize);
c_function!(snouttime_point_deserialize, pg_finfo_snouttime_point_deserialize, deserialize);
c_function!(snouttime_point_final, pg_finfo_snouttime_point_final, finish);

pgrx::extension_sql!(
	r#"
CREATE FUNCTION snouttime._point_serialize(internal) RETURNS bytea
	LANGUAGE c STRICT IMMUTABLE PARALLEL SAFE AS 'MODULE_PATHNAME', 'snouttime_point_serialize';
CREATE FUNCTION snouttime._point_deserialize(bytea, internal) RETURNS internal
	LANGUAGE c STRICT IMMUTABLE PARALLEL SAFE AS 'MODULE_PATHNAME', 'snouttime_point_deserialize';
CREATE FUNCTION snouttime._first_combine(internal, internal) RETURNS internal
	LANGUAGE c IMMUTABLE PARALLEL SAFE AS 'MODULE_PATHNAME', 'snouttime_first_combine';
CREATE FUNCTION snouttime._last_combine(internal, internal) RETURNS internal
	LANGUAGE c IMMUTABLE PARALLEL SAFE AS 'MODULE_PATHNAME', 'snouttime_last_combine';

DO $$
DECLARE
	t text;
BEGIN
	-- One aggregate per type of `at`; the C functions read that type from the call.
	FOREACH t IN ARRAY ARRAY['timestamptz', 'timestamp', 'date', 'bigint', 'integer'] LOOP
		EXECUTE format($f$
			CREATE FUNCTION snouttime._first_trans(internal, anyelement, %1$s) RETURNS internal
				LANGUAGE c IMMUTABLE PARALLEL SAFE AS 'MODULE_PATHNAME', 'snouttime_first_trans';
			CREATE FUNCTION snouttime._last_trans(internal, anyelement, %1$s) RETURNS internal
				LANGUAGE c IMMUTABLE PARALLEL SAFE AS 'MODULE_PATHNAME', 'snouttime_last_trans';
			CREATE FUNCTION snouttime._point_final(internal, anyelement, %1$s) RETURNS anyelement
				LANGUAGE c IMMUTABLE PARALLEL SAFE AS 'MODULE_PATHNAME', 'snouttime_point_final';
			CREATE AGGREGATE snouttime.first(value anyelement, at %1$s) (
				SFUNC = snouttime._first_trans, STYPE = internal,
				FINALFUNC = snouttime._point_final, FINALFUNC_EXTRA,
				COMBINEFUNC = snouttime._first_combine,
				SERIALFUNC = snouttime._point_serialize, DESERIALFUNC = snouttime._point_deserialize,
				PARALLEL = SAFE);
			CREATE AGGREGATE snouttime.last(value anyelement, at %1$s) (
				SFUNC = snouttime._last_trans, STYPE = internal,
				FINALFUNC = snouttime._point_final, FINALFUNC_EXTRA,
				COMBINEFUNC = snouttime._last_combine,
				SERIALFUNC = snouttime._point_serialize, DESERIALFUNC = snouttime._point_deserialize,
				PARALLEL = SAFE);
		$f$, t);
	END LOOP;
END
$$;
"#,
	name = "first_last"
);

// ---- histogram(value, min, max, buckets) ----
//
// Counts per bucket, as `width_bucket(value, min, max, buckets)` numbers them: slot 0 is
// below `min`, slots 1..buckets split [min, max) evenly, slot buckets+1 is `max` and above
// (NaN too, which Postgres sorts above every number). A NULL value is not counted. The
// state is the counts, which merge by adding, so it runs in parallel and will roll up.

#[repr(C)]
struct Histogram {
	lo: f64,
	hi: f64,
	n: i32,
	counts: *mut i64, // n + 2 of them, in the aggregate's context
}

unsafe fn histogram_new(ctx: pg_sys::MemoryContext, lo: f64, hi: f64, n: i32) -> *mut Histogram {
	if n < 1 || n > 100_000 {
		error!("histogram needs between 1 and 100000 buckets, not {n}");
	}
	if !(lo < hi) || !lo.is_finite() || !hi.is_finite() {
		error!("histogram needs a finite min below a finite max");
	}
	let h = pg_sys::MemoryContextAllocZero(ctx, std::mem::size_of::<Histogram>()) as *mut Histogram;
	(*h).lo = lo;
	(*h).hi = hi;
	(*h).n = n;
	(*h).counts = pg_sys::MemoryContextAllocZero(ctx, (n as usize + 2) * 8) as *mut i64;
	h
}

unsafe fn histogram_slots(h: &Histogram) -> &mut [i64] {
	std::slice::from_raw_parts_mut(h.counts, h.n as usize + 2)
}

/// width_bucket's numbering, including its rounding: the same arithmetic in the same order.
fn slot(v: f64, lo: f64, hi: f64, n: i32) -> usize {
	if v.is_nan() || v >= hi {
		n as usize + 1
	} else if v < lo {
		0
	} else {
		let s = ((n as f64) * ((v - lo) / (hi - lo))) as i64 + 1;
		s.clamp(1, n as i64) as usize
	}
}

/// (internal, value float8, min float8, max float8, buckets int) -> internal
unsafe fn histogram_trans(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum {
	let ctx = agg_context(fcinfo);
	let (state, state_null) = arg(fcinfo, 0);
	let (lo, lo_null) = arg(fcinfo, 2);
	let (hi, hi_null) = arg(fcinfo, 3);
	let (n, n_null) = arg(fcinfo, 4);
	if lo_null || hi_null || n_null {
		error!("histogram's min, max and buckets must not be NULL");
	}
	let (lo, hi, n) = (f64::from_bits(lo.value() as u64), f64::from_bits(hi.value() as u64), n.value() as i32);
	let h = if state_null { histogram_new(ctx, lo, hi, n) } else { state.cast_mut_ptr::<Histogram>() };
	if (*h).lo != lo || (*h).hi != hi || (*h).n != n {
		error!("histogram's min, max and buckets must be the same for every row of a group");
	}
	let (v, v_null) = arg(fcinfo, 1);
	if !v_null {
		histogram_slots(&*h)[slot(f64::from_bits(v.value() as u64), lo, hi, n)] += 1;
	}
	pg_sys::Datum::from(h)
}

/// (internal, internal) -> internal
unsafe fn histogram_combine(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum {
	let ctx = agg_context(fcinfo);
	let (a, a_null) = arg(fcinfo, 0);
	let (b, b_null) = arg(fcinfo, 1);
	if b_null {
		return if a_null { null(fcinfo) } else { a };
	}
	let b = &*b.cast_mut_ptr::<Histogram>();
	let a = if a_null { histogram_new(ctx, b.lo, b.hi, b.n) } else { a.cast_mut_ptr::<Histogram>() };
	if (*a).lo != b.lo || (*a).hi != b.hi || (*a).n != b.n {
		error!("histograms with different min, max or buckets cannot be combined");
	}
	for (x, y) in histogram_slots(&*a).iter_mut().zip(histogram_slots(b).iter()) {
		*x += *y;
	}
	pg_sys::Datum::from(a)
}

/// (internal) -> bytea: lo f64, hi f64, n i32, then n + 2 counts, little-endian.
unsafe fn histogram_serialize(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum {
	agg_context(fcinfo);
	let h = &*arg(fcinfo, 0).0.cast_mut_ptr::<Histogram>();
	let mut bytes = Vec::with_capacity(20 + (h.n as usize + 2) * 8);
	bytes.extend_from_slice(&h.lo.to_le_bytes());
	bytes.extend_from_slice(&h.hi.to_le_bytes());
	bytes.extend_from_slice(&h.n.to_le_bytes());
	for c in histogram_slots(h).iter() {
		bytes.extend_from_slice(&c.to_le_bytes());
	}
	pg_sys::Datum::from(pgrx::rust_byte_slice_to_bytea(&bytes).into_pg())
}

/// (bytea, internal) -> internal
unsafe fn histogram_deserialize(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum {
	let ctx = agg_context(fcinfo);
	let v = pg_sys::pg_detoast_datum_packed(arg(fcinfo, 0).0.cast_mut_ptr());
	let bytes = pgrx::varlena::varlena_to_byte_slice(v);
	let f = |i: usize| f64::from_le_bytes(bytes[i..i + 8].try_into().unwrap());
	if bytes.len() < 20 {
		error!("snouttime: a serialised histogram is too short");
	}
	let n = i32::from_le_bytes(bytes[16..20].try_into().unwrap());
	if n < 1 || bytes.len() != 20 + (n as usize + 2) * 8 {
		error!("snouttime: a serialised histogram has the wrong length");
	}
	let h = histogram_new(ctx, f(0), f(8), n);
	for (i, c) in histogram_slots(&*h).iter_mut().enumerate() {
		*c = i64::from_le_bytes(bytes[20 + i * 8..28 + i * 8].try_into().unwrap());
	}
	pg_sys::Datum::from(h)
}

/// (internal, value, min, max, buckets) -> bigint[]
unsafe fn histogram_final(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum {
	let (s, s_null) = arg(fcinfo, 0);
	if s_null {
		return null(fcinfo);
	}
	let h = &*s.cast_mut_ptr::<Histogram>();
	histogram_slots(h).to_vec().into_datum().unwrap_or_else(|| null(fcinfo))
}

c_function!(snouttime_histogram_trans, pg_finfo_snouttime_histogram_trans, histogram_trans);
c_function!(snouttime_histogram_combine, pg_finfo_snouttime_histogram_combine, histogram_combine);
c_function!(snouttime_histogram_serialize, pg_finfo_snouttime_histogram_serialize, histogram_serialize);
c_function!(snouttime_histogram_deserialize, pg_finfo_snouttime_histogram_deserialize, histogram_deserialize);
c_function!(snouttime_histogram_final, pg_finfo_snouttime_histogram_final, histogram_final);

pgrx::extension_sql!(
	r#"
CREATE FUNCTION snouttime._histogram_trans(internal, double precision, double precision, double precision, integer)
	RETURNS internal LANGUAGE c IMMUTABLE PARALLEL SAFE AS 'MODULE_PATHNAME', 'snouttime_histogram_trans';
CREATE FUNCTION snouttime._histogram_combine(internal, internal) RETURNS internal
	LANGUAGE c IMMUTABLE PARALLEL SAFE AS 'MODULE_PATHNAME', 'snouttime_histogram_combine';
CREATE FUNCTION snouttime._histogram_serialize(internal) RETURNS bytea
	LANGUAGE c STRICT IMMUTABLE PARALLEL SAFE AS 'MODULE_PATHNAME', 'snouttime_histogram_serialize';
CREATE FUNCTION snouttime._histogram_deserialize(bytea, internal) RETURNS internal
	LANGUAGE c STRICT IMMUTABLE PARALLEL SAFE AS 'MODULE_PATHNAME', 'snouttime_histogram_deserialize';
CREATE FUNCTION snouttime._histogram_final(internal, double precision, double precision, double precision, integer)
	RETURNS bigint[] LANGUAGE c IMMUTABLE PARALLEL SAFE AS 'MODULE_PATHNAME', 'snouttime_histogram_final';
CREATE AGGREGATE snouttime.histogram(value double precision, min double precision, max double precision, buckets integer) (
	SFUNC = snouttime._histogram_trans, STYPE = internal,
	FINALFUNC = snouttime._histogram_final, FINALFUNC_EXTRA,
	COMBINEFUNC = snouttime._histogram_combine,
	SERIALFUNC = snouttime._histogram_serialize, DESERIALFUNC = snouttime._histogram_deserialize,
	PARALLEL = SAFE);
"#,
	name = "histogram"
);

#[cfg(test)]
mod tests {
	use super::slot;

	#[test]
	fn slots_are_numbered_like_width_bucket() {
		assert_eq!(slot(-1.0, 0.0, 10.0, 5), 0);
		assert_eq!(slot(0.0, 0.0, 10.0, 5), 1);
		assert_eq!(slot(1.999, 0.0, 10.0, 5), 1);
		assert_eq!(slot(2.0, 0.0, 10.0, 5), 2);
		assert_eq!(slot(9.999, 0.0, 10.0, 5), 5);
		assert_eq!(slot(10.0, 0.0, 10.0, 5), 6);
		assert_eq!(slot(f64::NAN, 0.0, 10.0, 5), 6);
		assert_eq!(slot(f64::INFINITY, 0.0, 10.0, 5), 6);
		assert_eq!(slot(f64::NEG_INFINITY, 0.0, 10.0, 5), 0);
	}
}

// ---- counter_delta(value, at ORDER BY at) and counter_rate(value, at ORDER BY at) ----
//
// A monotonic counter's increase over a group, reading a drop as a reset to zero (so the new
// value is the increase since the reset). That needs the points in time order, which a plain
// GROUP BY does not give, so a point earlier than the one before it is an ERROR naming the fix
// (ORDER BY at) rather than a wrong number. Points with a NULL value or time are skipped.
//
// The state merges. It is a list of disjoint time SEGMENTS, each with its first and last
// point and the increase inside it, kept sorted: a combine function is called in whatever
// order the planner likes (a finalize step combined day 1 with day 3 before day 2, found by the
// regression test), so two states cannot be joined into one summary until it is known that
// nothing falls between them. Only the final function joins the segments, in time order,
// with the reset rule at each boundary. Two segments that overlap in time are an error.

#[repr(C)]
#[derive(Clone, Copy)]
struct Segment {
	n: i64,
	first_at: i64,
	first_v: f64,
	last_at: i64,
	last_v: f64,
	increase: f64,
}

#[repr(C)]
struct Counter {
	len: usize,
	cap: usize,
	segs: *mut Segment,
}

fn step(from: f64, to: f64) -> f64 {
	if to >= from { to - from } else { to }
}

unsafe fn counter_new(ctx: pg_sys::MemoryContext, cap: usize) -> *mut Counter {
	let c = pg_sys::MemoryContextAllocZero(ctx, std::mem::size_of::<Counter>()) as *mut Counter;
	(*c).cap = cap.max(1);
	(*c).segs = pg_sys::MemoryContextAllocZero(ctx, (*c).cap * std::mem::size_of::<Segment>()) as *mut Segment;
	c
}

unsafe fn segments(c: &Counter) -> &mut [Segment] {
	std::slice::from_raw_parts_mut(c.segs, c.len)
}

/// Put `seg` into `c` in time order; an overlap with a neighbour is an error.
unsafe fn insert(c: &mut Counter, seg: Segment) {
	let at = segments(c).partition_point(|s| s.first_at < seg.first_at);
	let segs = segments(c);
	let overlaps_before = at > 0 && segs[at - 1].last_at > seg.first_at;
	let overlaps_after = at < segs.len() && seg.last_at > segs[at].first_at;
	if overlaps_before || overlaps_after {
		error!("counter states with overlapping time ranges cannot be combined: write the aggregate as counter_delta(value, at ORDER BY at)");
	}
	if c.len == c.cap {
		c.cap *= 2;
		c.segs = pg_sys::repalloc(c.segs.cast(), c.cap * std::mem::size_of::<Segment>()) as *mut Segment;
	}
	let segs = std::slice::from_raw_parts_mut(c.segs, c.len + 1);
	segs.copy_within(at..c.len, at + 1);
	segs[at] = seg;
	c.len += 1;
}

/// (internal, value float8, at timestamptz) -> internal
unsafe fn counter_trans(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum {
	let ctx = agg_context(fcinfo);
	let (state, state_null) = arg(fcinfo, 0);
	let c = if state_null { counter_new(ctx, 1) } else { state.cast_mut_ptr::<Counter>() };
	let (v, v_null) = arg(fcinfo, 1);
	let (at, at_null) = arg(fcinfo, 2);
	if v_null || at_null {
		return pg_sys::Datum::from(c);
	}
	let (v, at) = (f64::from_bits(v.value() as u64), at.value() as i64);
	let c = &mut *c;
	// A transition state is only ever one segment, built from rows in order.
	if c.len == 0 {
		c.len = 1;
		*c.segs = Segment { n: 0, first_at: at, first_v: v, last_at: at, last_v: v, increase: 0.0 };
	}
	let seg = &mut *c.segs;
	if seg.n > 0 {
		if at < seg.last_at {
			error!("counter values must arrive in time order: write the aggregate as counter_delta(value, at ORDER BY at)");
		}
		seg.increase += step(seg.last_v, v);
	}
	(seg.last_at, seg.last_v) = (at, v);
	seg.n += 1;
	pg_sys::Datum::from(c as *mut Counter)
}

/// (internal, internal) -> internal
unsafe fn counter_combine(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum {
	let ctx = agg_context(fcinfo);
	let (a, a_null) = arg(fcinfo, 0);
	let (b, b_null) = arg(fcinfo, 1);
	let out = if a_null { counter_new(ctx, 4) } else { a.cast_mut_ptr::<Counter>() };
	if !b_null {
		let b = &*b.cast_mut_ptr::<Counter>();
		let old = pg_sys::MemoryContextSwitchTo(ctx);
		for seg in segments(b).iter().filter(|s| s.n > 0) {
			insert(&mut *out, *seg);
		}
		pg_sys::MemoryContextSwitchTo(old);
	}
	pg_sys::Datum::from(out)
}

/// (internal) -> bytea: the segments, six little-endian 8-byte fields each. The planner will
/// not split an aggregate with an `internal` state into partial ones at all (even per
/// partition, with no worker involved) unless the state can be serialised.
unsafe fn counter_serialize(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum {
	agg_context(fcinfo);
	let c = &*arg(fcinfo, 0).0.cast_mut_ptr::<Counter>();
	let mut bytes = Vec::with_capacity(c.len * 48);
	for s in segments(c).iter() {
		bytes.extend_from_slice(&s.n.to_le_bytes());
		bytes.extend_from_slice(&s.first_at.to_le_bytes());
		bytes.extend_from_slice(&s.first_v.to_le_bytes());
		bytes.extend_from_slice(&s.last_at.to_le_bytes());
		bytes.extend_from_slice(&s.last_v.to_le_bytes());
		bytes.extend_from_slice(&s.increase.to_le_bytes());
	}
	pg_sys::Datum::from(pgrx::rust_byte_slice_to_bytea(&bytes).into_pg())
}

/// (bytea, internal) -> internal
unsafe fn counter_deserialize(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum {
	let ctx = agg_context(fcinfo);
	let v = pg_sys::pg_detoast_datum_packed(arg(fcinfo, 0).0.cast_mut_ptr());
	let b = pgrx::varlena::varlena_to_byte_slice(v);
	if b.len() % 48 != 0 {
		error!("snouttime: a serialised counter state has the wrong length");
	}
	let c = counter_new(ctx, b.len() / 48);
	for chunk in b.chunks_exact(48) {
		let i = |k: usize| i64::from_le_bytes(chunk[k * 8..k * 8 + 8].try_into().unwrap());
		let f = |k: usize| f64::from_le_bytes(chunk[k * 8..k * 8 + 8].try_into().unwrap());
		let seg = Segment { n: i(0), first_at: i(1), first_v: f(2), last_at: i(3), last_v: f(4), increase: f(5) };
		let old = pg_sys::MemoryContextSwitchTo(ctx);
		insert(&mut *c, seg);
		pg_sys::MemoryContextSwitchTo(old);
	}
	pg_sys::Datum::from(c)
}

/// The segments joined in time order: (points, first_at, last_at, increase).
unsafe fn counter_total(fcinfo: pg_sys::FunctionCallInfo) -> Option<(i64, i64, i64, f64)> {
	let (s, s_null) = arg(fcinfo, 0);
	if s_null {
		return None;
	}
	let segs: Vec<Segment> = segments(&*s.cast_mut_ptr::<Counter>()).iter().filter(|s| s.n > 0).copied().collect();
	let (first, last) = (segs.first()?, segs.last()?);
	let mut increase = 0.0;
	for (i, seg) in segs.iter().enumerate() {
		if i > 0 {
			increase += step(segs[i - 1].last_v, seg.first_v);
		}
		increase += seg.increase;
	}
	Some((segs.iter().map(|s| s.n).sum(), first.first_at, last.last_at, increase))
}

/// (internal, value, at) -> float8: the increase, NULL with no points.
unsafe fn counter_delta_final(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum {
	match counter_total(fcinfo) {
		Some((_, _, _, increase)) => increase.into_datum().unwrap(),
		None => null(fcinfo),
	}
}

/// (internal, value, at) -> float8: the increase per second between the first and last point,
/// NULL with fewer than two points or none of them apart in time.
unsafe fn counter_rate_final(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum {
	match counter_total(fcinfo) {
		Some((n, first, last, increase)) if n > 1 && last > first => {
			(increase / ((last - first) as f64 / 1e6)).into_datum().unwrap()
		}
		_ => null(fcinfo),
	}
}

c_function!(snouttime_counter_trans, pg_finfo_snouttime_counter_trans, counter_trans);
c_function!(snouttime_counter_combine, pg_finfo_snouttime_counter_combine, counter_combine);
c_function!(snouttime_counter_serialize, pg_finfo_snouttime_counter_serialize, counter_serialize);
c_function!(snouttime_counter_deserialize, pg_finfo_snouttime_counter_deserialize, counter_deserialize);
c_function!(snouttime_counter_delta_final, pg_finfo_snouttime_counter_delta_final, counter_delta_final);
c_function!(snouttime_counter_rate_final, pg_finfo_snouttime_counter_rate_final, counter_rate_final);

pgrx::extension_sql!(
	r#"
CREATE FUNCTION snouttime._counter_trans(internal, double precision, timestamptz) RETURNS internal
	LANGUAGE c IMMUTABLE PARALLEL SAFE AS 'MODULE_PATHNAME', 'snouttime_counter_trans';
CREATE FUNCTION snouttime._counter_combine(internal, internal) RETURNS internal
	LANGUAGE c IMMUTABLE PARALLEL SAFE AS 'MODULE_PATHNAME', 'snouttime_counter_combine';
CREATE FUNCTION snouttime._counter_serialize(internal) RETURNS bytea
	LANGUAGE c STRICT IMMUTABLE PARALLEL SAFE AS 'MODULE_PATHNAME', 'snouttime_counter_serialize';
CREATE FUNCTION snouttime._counter_deserialize(bytea, internal) RETURNS internal
	LANGUAGE c STRICT IMMUTABLE PARALLEL SAFE AS 'MODULE_PATHNAME', 'snouttime_counter_deserialize';
-- PARALLEL RESTRICTED: a parallel worker reads rows in no time order, so its partial states
-- would overlap in time. Partial states per PARTITION (partitionwise aggregation, no worker)
-- are still allowed, and on a table partitioned by time they are exactly adjacent ranges.
CREATE FUNCTION snouttime._counter_delta_final(internal, double precision, timestamptz) RETURNS double precision
	LANGUAGE c IMMUTABLE PARALLEL SAFE AS 'MODULE_PATHNAME', 'snouttime_counter_delta_final';
CREATE FUNCTION snouttime._counter_rate_final(internal, double precision, timestamptz) RETURNS double precision
	LANGUAGE c IMMUTABLE PARALLEL SAFE AS 'MODULE_PATHNAME', 'snouttime_counter_rate_final';
CREATE AGGREGATE snouttime.counter_delta(value double precision, at timestamptz) (
	SFUNC = snouttime._counter_trans, STYPE = internal,
	FINALFUNC = snouttime._counter_delta_final, FINALFUNC_EXTRA,
	COMBINEFUNC = snouttime._counter_combine,
	SERIALFUNC = snouttime._counter_serialize, DESERIALFUNC = snouttime._counter_deserialize,
	PARALLEL = RESTRICTED);
CREATE AGGREGATE snouttime.counter_rate(value double precision, at timestamptz) (
	SFUNC = snouttime._counter_trans, STYPE = internal,
	FINALFUNC = snouttime._counter_rate_final, FINALFUNC_EXTRA,
	COMBINEFUNC = snouttime._counter_combine,
	SERIALFUNC = snouttime._counter_serialize, DESERIALFUNC = snouttime._counter_deserialize,
	PARALLEL = RESTRICTED);
"#,
	name = "counters"
);
