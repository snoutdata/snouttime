//! Time bounds the planner can prune partitions by (2026-09-25).
//!
//! `ts >= '2026-01-06 18:00+00'::timestamptz - interval '1 hour'` is the shape of nearly every
//! time filter, and Postgres will not prune a partition by it while planning: `timestamptz -
//! interval` is a STABLE operator (adding a day or a month depends on the session's time zone),
//! so its value is left to the executor, and every partition of the table is planned first.
//! On a one-host, one-hour query over six daily partitions that planning was most of the time
//! (0.23 ms, most of it spent planning partitions the WHERE clause never reaches).
//!
//! Two rewrites, both exact in meaning, of a WHERE clause's top-level comparisons of a
//! `timestamptz` column with `constant ± constant interval`:
//!
//! * When the interval has no days and no months, the sum does not depend on the time zone at
//!   all (it is microseconds added to microseconds), so it is computed now and the comparison is
//!   with that constant. Postgres then prunes by it, and a plan cached and run later under any
//!   setting gives the same answer.
//! * Otherwise the comparison is kept, and one it implies is added: the sum's value in ANY time
//!   zone lies within a range computed here (a month is 28 to 31 days, a day is 24 hours of
//!   local time, and converting to and from local time moves a value by at most the widest
//!   difference between two zones' offsets, 26 hours), so `ts >= t - interval` implies
//!   `ts >= t - (the most the interval can be)`. Postgres prunes by the added comparison; the
//!   original still decides every row.
//!
//! Left alone: a negative or infinite interval, an infinite timestamp, a sum outside the
//! timestamp range (the executor raises the error it always did), anything that is not two
//! constants (`now()`, a parameter), and comparisons below an OR or inside a function.

use pgrx::guc::GucSetting;
use pgrx::pg_sys;
use pgrx::PgList;

pub static ENABLED: GucSetting<bool> = GucSetting::<bool>::new(true);

const HOUR: i128 = 3_600_000_000;
const DAY: i128 = 24 * HOUR;
/// What converting to and from a zone's local time can move a value by: the widest difference
/// between two UTC offsets in use (UTC-12 to UTC+14).
const ZONES: i128 = 26 * HOUR;
/// The timestamp range Postgres accepts (`MIN_TIMESTAMP`, `END_TIMESTAMP`).
const MIN_TIMESTAMP: i128 = -211_813_488_000_000_000;
const END_TIMESTAMP: i128 = 9_223_371_331_200_000_000;

/// The value of `t + interval` (`minus`: `t - interval`) in every time zone, as (lowest,
/// highest, exact), or None when it is left to the executor.
fn range(t: i64, iv: &pg_sys::Interval, minus: bool) -> Option<(i64, i64, bool)> {
	if t == i64::MIN || t == i64::MAX || iv.month < 0 || iv.day < 0 || iv.time < 0 {
		return None;
	}
	if iv.month == i32::MAX || iv.day == i32::MAX || iv.time == i64::MAX {
		return None;
	}
	let exact = iv.month == 0 && iv.day == 0;
	let (lo, hi) = if exact {
		(iv.time as i128, iv.time as i128)
	} else {
		let (m, d, t) = (iv.month as i128, iv.day as i128, iv.time as i128);
		(m * 28 * DAY + d * DAY + t - ZONES, m * 31 * DAY + d * DAY + t + ZONES)
	};
	let t = t as i128;
	let (a, b) = if minus { (t - hi, t - lo) } else { (t + lo, t + hi) };
	if a < MIN_TIMESTAMP || b >= END_TIMESTAMP {
		return None;
	}
	Some((a as i64, b as i64, exact))
}

unsafe fn strip(mut n: *mut pg_sys::Node) -> *mut pg_sys::Node {
	while !n.is_null() && (*n).type_ == pg_sys::NodeTag::T_RelabelType {
		n = (*(n as *mut pg_sys::RelabelType)).arg as *mut pg_sys::Node;
	}
	n
}

/// A non-NULL constant's value.
unsafe fn constant(n: *mut pg_sys::Node) -> Option<pg_sys::Datum> {
	let n = strip(n);
	if n.is_null() || (*n).type_ != pg_sys::NodeTag::T_Const || (*(n as *mut pg_sys::Const)).constisnull {
		return None;
	}
	Some((*(n as *mut pg_sys::Const)).constvalue)
}

/// `timestamptz ± interval` (either way round for `+`) of two constants: its range.
unsafe fn sum_range(n: *mut pg_sys::Node) -> Option<(i64, i64, bool)> {
	let n = strip(n);
	if n.is_null() || (*n).type_ != pg_sys::NodeTag::T_OpExpr {
		return None;
	}
	let op = n as *mut pg_sys::OpExpr;
	let args = PgList::<pg_sys::Node>::from_pg((*op).args);
	if args.len() != 2 {
		return None;
	}
	let (a, b) = (constant(args.get_ptr(0)?)?, constant(args.get_ptr(1)?)?);
	let (t, iv, minus) = match (*op).opfuncid.to_u32() {
		pg_sys::F_TIMESTAMPTZ_PL_INTERVAL => (a, b, false),
		pg_sys::F_TIMESTAMPTZ_MI_INTERVAL => (a, b, true),
		pg_sys::F_INTERVAL_PL_TIMESTAMPTZ => (b, a, false),
		_ => return None,
	};
	range(t.value() as i64, &*(iv.cast_mut_ptr::<pg_sys::Interval>()), minus)
}

unsafe fn timestamptz_const(v: i64) -> *mut pg_sys::Node {
	pg_sys::makeConst(pg_sys::TIMESTAMPTZOID, -1, pg_sys::InvalidOid, 8, pg_sys::Datum::from(v), false, true) as *mut pg_sys::Node
}

/// `var op value` for strategy `strategy` (2: `<=`, 4: `>=`) of timestamptz's btree family.
unsafe fn compare(opf: pg_sys::Oid, var: *mut pg_sys::Node, strategy: i16, value: i64) -> Option<*mut pg_sys::Node> {
	let opno = pg_sys::get_opfamily_member(opf, pg_sys::TIMESTAMPTZOID, pg_sys::TIMESTAMPTZOID, strategy);
	if opno == pg_sys::InvalidOid {
		return None;
	}
	let e = pg_sys::make_opclause(
		opno,
		pg_sys::BOOLOID,
		false,
		pg_sys::copyObjectImpl(var as *const std::ffi::c_void) as *mut pg_sys::Expr,
		timestamptz_const(value) as *mut pg_sys::Expr,
		pg_sys::InvalidOid,
		pg_sys::InvalidOid,
	);
	pg_sys::set_opfuncid(e as *mut pg_sys::OpExpr);
	Some(e as *mut pg_sys::Node)
}

/// One top-level comparison: rewritten in place when the sum is exact, and the comparisons it
/// implies otherwise.
unsafe fn conjunct(n: *mut pg_sys::Node, opf: pg_sys::Oid, added: &mut Vec<*mut pg_sys::Node>) {
	if n.is_null() || (*n).type_ != pg_sys::NodeTag::T_OpExpr {
		return;
	}
	let op = n as *mut pg_sys::OpExpr;
	let args = (*op).args;
	if args.is_null() || (*args).length != 2 || (*op).opresulttype != pg_sys::BOOLOID {
		return;
	}
	let cell = |i: usize| (*args).elements.add(i);
	let (l, r) = ((*cell(0)).ptr_value as *mut pg_sys::Node, (*cell(1)).ptr_value as *mut pg_sys::Node);
	let is_column = |x: *mut pg_sys::Node| {
		let x = strip(x);
		!x.is_null()
			&& (*x).type_ == pg_sys::NodeTag::T_Var
			&& (*(x as *mut pg_sys::Var)).vartype == pg_sys::TIMESTAMPTZOID
			&& (*(x as *mut pg_sys::Var)).varlevelsup == 0
	};
	let (var, sum, at, commuted) = if is_column(l) {
		(l, r, 1usize, false)
	} else if is_column(r) {
		(r, l, 0usize, true)
	} else {
		return;
	};
	if pg_sys::exprType(sum) != pg_sys::TIMESTAMPTZOID {
		return;
	}
	let strategy = pg_sys::get_op_opfamily_strategy((*op).opno, opf);
	if !(1..=5).contains(&strategy) {
		return;
	}
	let strategy = if commuted { 6 - strategy } else { strategy };
	let Some((lo, hi, exact)) = sum_range(sum) else { return };
	if exact {
		(*cell(at)).ptr_value = timestamptz_const(lo) as *mut std::ffi::c_void;
		return;
	}
	// `<` or `<=` the sum: at most its highest; `>` or `>=`: at least its lowest; `=`: both
	if strategy <= 3 {
		if let Some(e) = compare(opf, var, 2, hi) {
			added.push(e);
		}
	}
	if strategy >= 3 {
		if let Some(e) = compare(opf, var, 4, lo) {
			added.push(e);
		}
	}
}

/// The conjuncts of an AND tree, in order.
unsafe fn conjuncts(n: *mut pg_sys::Node, out: &mut Vec<*mut pg_sys::Node>) {
	if !n.is_null() && (*n).type_ == pg_sys::NodeTag::T_BoolExpr && (*(n as *mut pg_sys::BoolExpr)).boolop == pg_sys::BoolExprType::AND_EXPR {
		for a in PgList::<pg_sys::Node>::from_pg((*(n as *mut pg_sys::BoolExpr)).args).iter_ptr() {
			conjuncts(a, out);
		}
	} else if !n.is_null() {
		out.push(n);
	}
}

/// Rewrites the WHERE clauses of `q`, its subqueries and its CTEs.
///
/// # Safety
/// `q` must be a query tree the planner is about to plan (which it scribbles on too).
pub unsafe fn query(q: *mut pg_sys::Query, depth: usize) {
	if q.is_null() || depth > 8 {
		return;
	}
	let tc = pg_sys::lookup_type_cache(pg_sys::TIMESTAMPTZOID, pg_sys::TYPECACHE_BTREE_OPFAMILY as i32);
	let opf = (*tc).btree_opf;
	let jt = (*q).jointree;
	if opf != pg_sys::InvalidOid && !jt.is_null() && !(*jt).quals.is_null() {
		let mut all = Vec::new();
		conjuncts((*jt).quals, &mut all);
		let mut added = Vec::new();
		for &c in &all {
			conjunct(c, opf, &mut added);
		}
		if !added.is_empty() {
			let mut list = PgList::<pg_sys::Node>::new();
			for n in all.into_iter().chain(added) {
				list.push(n);
			}
			(*jt).quals = pg_sys::make_andclause(list.into_pg()) as *mut pg_sys::Node;
		}
	}
	for rte in PgList::<pg_sys::RangeTblEntry>::from_pg((*q).rtable).iter_ptr() {
		if (*rte).rtekind == pg_sys::RTEKind::RTE_SUBQUERY {
			query((*rte).subquery, depth + 1);
		}
	}
	for cte in PgList::<pg_sys::CommonTableExpr>::from_pg((*q).cteList).iter_ptr() {
		let sub = (*cte).ctequery;
		if !sub.is_null() && (*sub).type_ == pg_sys::NodeTag::T_Query {
			query(sub as *mut pg_sys::Query, depth + 1);
		}
	}
}
