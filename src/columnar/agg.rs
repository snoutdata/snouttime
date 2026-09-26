//! Aggregates computed on a sealed table's decoded columns (PLAN.md 3.6), without forming a row
//! per input row: `SnoutTime Columnar Aggregate`.
//!
//! Postgres plans a grouped query in two halves when it can: a partial aggregate under the
//! data and a finalize above it, which combines the partial states. That is how it runs an
//! aggregate in parallel (the partial half in each worker) and over partitions (the partial
//! half in each partition, with `enable_partitionwise_aggregate`). This node is offered as the
//! partial half over a sealed relation: `create_upper_paths_hook` at the partial stage, beside
//! Postgres's own partial aggregate over a scan, costed, and the planner chooses. Postgres's
//! finalize then combines its states exactly as it would its own, so the node only has to emit
//! each aggregate's transition state in the aggregate's own format.
//!
//! What it accepts, and it declines everything else, so the query is planned as it would have
//! been without it:
//!
//! * grouping by plain columns of integer, time, bool, text or varchar (a deterministic
//!   collation, so equal is byte-equal) and uuid type, and by `snouttime.bucket(width, time
//!   [, origin])` or `date_bin(stride, time, origin)` with constant width and origin, computed
//!   natively with the same arithmetic;
//! * `count(*)`, `count(column)`, and `sum`, `avg`, `min`, `max` of integer (not int8, whose
//!   sum and average states are numerics), float and, for min and max, time columns; and
//!   SnoutTime's `first(value, at)` and `last(value, at)`, whose state is serialised exactly as
//!   `point.rs` serialises it;
//! * a WHERE clause made only of the comparisons the scan skips row groups by (`scan.rs`),
//!   which are then evaluated on every row as well.
//!
//! Rows the delete log names are left out, and the delta store's rows are aggregated the slow
//! way, from their tuples. Parallel-aware as the scan is: workers claim row groups from one
//! counter, one of them reads the delta store, and each emits its own partial groups.

use std::collections::HashMap;
use std::ffi::CStr;
use std::hash::{BuildHasherDefault, Hasher};
use std::rc::Rc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};
use pgrx::pg_sys;
use pgrx::prelude::*;
use pgrx::{IntoDatum, PgList, PgMemoryContexts};

use super::format::{Decoded, DecodedColumn};
use super::read::{self, Group, Store};
use super::scan::{self, Key, Op, PlanKey, PlanValue};
use super::seek::{self, Bound, Order, Seek, Term};
use super::types::{self, Kind};
use crate::bucket::Width;

pub static ENABLED: GucSetting<bool> = GucSetting::<bool>::new(true);
pub static PARTITIONWISE: GucSetting<bool> = GucSetting::<bool>::new(true);

const NAME: &CStr = c"SnoutTime Columnar Aggregate";

/// Group expressions one node handles. Chosen, not measured: GROUP BY rarely has more.
pub(super) const MAX_KEYS: usize = 4;

static mut PREV_HOOK: pg_sys::create_upper_paths_hook_type = None;
static mut PREV_PLANNER: pg_sys::planner_hook_type = None;

static mut PATH_METHODS: pg_sys::CustomPathMethods = pg_sys::CustomPathMethods {
	CustomName: NAME.as_ptr(),
	PlanCustomPath: Some(plan_path),
	ReparameterizeCustomPathByChild: None,
};

static mut SCAN_METHODS: pg_sys::CustomScanMethods =
	pg_sys::CustomScanMethods { CustomName: NAME.as_ptr(), CreateCustomScanState: Some(create_state) };

static mut EXEC_METHODS: pg_sys::CustomExecMethods = pg_sys::CustomExecMethods {
	CustomName: NAME.as_ptr(),
	BeginCustomScan: Some(begin),
	ExecCustomScan: Some(exec),
	EndCustomScan: Some(end),
	ReScanCustomScan: Some(rescan),
	MarkPosCustomScan: None,
	RestrPosCustomScan: None,
	EstimateDSMCustomScan: Some(estimate_dsm),
	InitializeDSMCustomScan: Some(initialize_dsm),
	ReInitializeDSMCustomScan: Some(reinitialize_dsm),
	InitializeWorkerCustomScan: Some(initialize_worker),
	ShutdownCustomScan: None,
	ExplainCustomScan: Some(explain),
};

pub fn init() {
	GucRegistry::define_bool_guc(
		c"snouttime.columnar_aggregate",
		c"Whether aggregates over sealed partitions may be computed on their columns",
		c"SnoutTime's partial aggregate reads the decoded columns of a row group and never forms a row per input row. Off: Postgres's own aggregate over a scan.",
		&ENABLED,
		GucContext::Userset,
		GucFlags::default(),
	);
	GucRegistry::define_bool_guc(
		c"snouttime.partitionwise_aggregate",
		c"Whether an aggregate over a table with sealed partitions is planned partition by partition",
		c"On: Postgres's enable_partitionwise_aggregate is turned on while such a query is planned, so each sealed partition can be aggregated on its columns. It never turns that setting off.",
		&PARTITIONWISE,
		GucContext::Userset,
		GucFlags::default(),
	);
	unsafe {
		PREV_HOOK = pg_sys::create_upper_paths_hook;
		pg_sys::create_upper_paths_hook = Some(hook);
		PREV_PLANNER = pg_sys::planner_hook;
		pg_sys::planner_hook = Some(planner);
		pg_sys::RegisterCustomScanMethods(&raw const SCAN_METHODS);
	}
}

// ---------------------------------------------------------------------------------------
// Partition by partition
// ---------------------------------------------------------------------------------------

/// Puts `enable_partitionwise_aggregate` back however planning ends, an ERROR included (pgrx
/// unwinds through this frame).
struct Restore(bool);

impl Drop for Restore {
	fn drop(&mut self) {
		unsafe { pg_sys::enable_partitionwise_aggregate = self.0 };
	}
}

/// Postgres aggregates a partitioned table's rows through one Append unless partitionwise
/// aggregation is on, and it is off by default (it costs planning time on tables with many
/// partitions). A sealed partition only gets this node when the aggregate is planned per
/// partition, so for a query that reads a partitioned table with a sealed partition in it,
/// the setting is turned on while the query is planned.
#[pg_guard]
unsafe extern "C-unwind" fn planner(
	parse: *mut pg_sys::Query,
	query_string: *const std::ffi::c_char,
	cursor_options: i32,
	bound_params: pg_sys::ParamListInfo,
) -> *mut pg_sys::PlannedStmt {
	// asked once, and only when one of the two things below would use the answer
	let mut sealed: Option<bool> = None;
	let mut reads_sealed = || *sealed.get_or_insert_with(|| reads_sealed_partitions(parse, 0));
	// bounds the planner can prune by, before it plans every partition (fold.rs)
	if super::fold::ENABLED.get() && (*parse).commandType == pg_sys::CmdType::CMD_SELECT && reads_sealed() {
		super::fold::query(parse, 0);
	}
	let _restore = if PARTITIONWISE.get()
		&& ENABLED.get()
		&& !pg_sys::enable_partitionwise_aggregate
		&& ((*parse).hasAggs || !(*parse).groupClause.is_null())
		&& reads_sealed()
	{
		pg_sys::enable_partitionwise_aggregate = true;
		Some(Restore(false))
	} else {
		None
	};
	match PREV_PLANNER {
		Some(prev) => prev(parse, query_string, cursor_options, bound_params),
		None => pg_sys::standard_planner(parse, query_string, cursor_options, bound_params),
	}
}

/// Does the query (its range table, its subqueries and CTEs) read a partitioned table any leaf
/// of which is sealed?
unsafe fn reads_sealed_partitions(q: *mut pg_sys::Query, depth: usize) -> bool {
	if q.is_null() || depth > 8 {
		return false;
	}
	for rte in PgList::<pg_sys::RangeTblEntry>::from_pg((*q).rtable).iter_ptr() {
		match (*rte).rtekind {
			pg_sys::RTEKind::RTE_RELATION if (*rte).relkind as u8 == pg_sys::RELKIND_PARTITIONED_TABLE => {
				if has_sealed_leaf((*rte).relid, 0) {
					return true;
				}
			}
			pg_sys::RTEKind::RTE_SUBQUERY => {
				if reads_sealed_partitions((*rte).subquery, depth + 1) {
					return true;
				}
			}
			_ => {}
		}
	}
	for cte in PgList::<pg_sys::CommonTableExpr>::from_pg((*q).cteList).iter_ptr() {
		let sub = (*cte).ctequery;
		if !sub.is_null() && (*sub).type_ == pg_sys::NodeTag::T_Query && reads_sealed_partitions(sub as *mut pg_sys::Query, depth + 1) {
			return true;
		}
	}
	false
}

unsafe fn has_sealed_leaf(relid: pg_sys::Oid, depth: usize) -> bool {
	if depth > 8 {
		return false;
	}
	let columnar = pg_sys::get_am_oid(c"snouttime_columnar".as_ptr(), true);
	let tiered = pg_sys::get_am_oid(c"snouttime_tiered".as_ptr(), true);
	let rel = pg_sys::RelationIdGetRelation(relid);
	if rel.is_null() {
		return false;
	}
	let desc = pg_sys::RelationGetPartitionDesc(rel, true);
	let mut found = false;
	if !desc.is_null() {
		for i in 0..(*desc).nparts as usize {
			let oid = *(*desc).oids.add(i);
			found = if *(*desc).is_leaf.add(i) {
				let am = pg_sys::get_rel_relam(oid);
				am == columnar || am == tiered
			} else {
				has_sealed_leaf(oid, depth + 1)
			};
			if found {
				break;
			}
		}
	}
	pg_sys::RelationClose(rel);
	found
}

// ---------------------------------------------------------------------------------------
// What the node computes
// ---------------------------------------------------------------------------------------

/// A group expression.
#[derive(Debug, Clone, Copy)]
pub(super) enum KeyExpr {
	/// a column, by what its values are
	Col { att: usize, typ: pg_sys::Oid, how: KeyHow },
	/// `snouttime.bucket` of a time column
	Bucket { att: usize, typ: pg_sys::Oid, width: Width, origin: i64 },
	/// `date_bin` of a time column: the stride in microseconds
	DateBin { att: usize, typ: pg_sys::Oid, stride: i64, origin: i64 },
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) enum KeyHow {
	Int,
	Bool,
	/// by its bytes: text and varchar (`len` -1) or a fixed-length type like uuid
	Bytes { len: i16 },
}

impl KeyExpr {
	pub(super) fn att(&self) -> usize {
		match *self {
			KeyExpr::Col { att, .. } | KeyExpr::Bucket { att, .. } | KeyExpr::DateBin { att, .. } => att,
		}
	}
}

/// An aggregate, and the column it reads.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Agg {
	CountStar,
	Count(usize),
	/// sum of int2 or int4: an int8 state
	SumInt(usize),
	/// sum of float4 (`f4`) or float8: a state of the same type
	SumFloat(usize, bool),
	/// avg of int2 or int4: an int8[] {count, sum}
	AvgInt(usize),
	/// avg of float4 or float8: a float8[] {N, Sx, Sxx}, as float8_accum keeps it
	AvgFloat(usize, bool),
	/// min or max of an integer or time column
	MinInt(usize),
	MaxInt(usize),
	/// min or max of float4 (`f4`) or float8
	MinFloat(usize, bool),
	MaxFloat(usize, bool),
	/// `snouttime.first(value, at)` / `last`: the value column, the time column, and the
	/// value's type, whose binary send function the serialised state carries it through
	First(usize, usize, pg_sys::Oid),
	Last(usize, usize, pg_sys::Oid),
}

impl Agg {
	/// the column whose NULLs the aggregate skips (first and last count a NULL value)
	fn att(&self) -> Option<usize> {
		match *self {
			Agg::CountStar | Agg::First(..) | Agg::Last(..) => None,
			Agg::Count(a) | Agg::SumInt(a) | Agg::AvgInt(a) | Agg::MinInt(a) | Agg::MaxInt(a) => Some(a),
			Agg::SumFloat(a, _) | Agg::AvgFloat(a, _) | Agg::MinFloat(a, _) | Agg::MaxFloat(a, _) => Some(a),
		}
	}

	/// every column it reads
	fn atts(&self) -> Vec<usize> {
		match *self {
			Agg::First(v, at, _) | Agg::Last(v, at, _) => vec![v, at],
			_ => self.att().into_iter().collect(),
		}
	}

	fn text(&self) -> String {
		match *self {
			Agg::CountStar => "CountStar".into(),
			Agg::Count(a) => format!("Count {a}"),
			Agg::SumInt(a) => format!("SumInt {a}"),
			Agg::SumFloat(a, f4) => format!("SumFloat {a} {}", f4 as u8),
			Agg::AvgInt(a) => format!("AvgInt {a}"),
			Agg::AvgFloat(a, f4) => format!("AvgFloat {a} {}", f4 as u8),
			Agg::MinInt(a) => format!("MinInt {a}"),
			Agg::MaxInt(a) => format!("MaxInt {a}"),
			Agg::MinFloat(a, f4) => format!("MinFloat {a} {}", f4 as u8),
			Agg::MaxFloat(a, f4) => format!("MaxFloat {a} {}", f4 as u8),
			Agg::First(v, at, t) => format!("First {v} {at} {}", t.to_u32()),
			Agg::Last(v, at, t) => format!("Last {v} {at} {}", t.to_u32()),
		}
	}
}

/// One column of the node's output, in target-list order.
#[derive(Debug, Clone, Copy)]
enum Output {
	Key(usize),
	Agg(usize),
}

#[derive(Debug, Clone)]
struct Spec {
	relid: pg_sys::Oid,
	/// the relation's range-table index, where the planner's clauses name it
	rti: pg_sys::Index,
	keys: Vec<KeyExpr>,
	aggs: Vec<Agg>,
	outputs: Vec<Output>,
	filters: Vec<PlanKey>,
	/// `column IS NULL` (true) or `IS NOT NULL` (false): answered from the null bitmap, and a
	/// row group whose header counts no row that can pass is skipped (IoT's `status IS NOT
	/// NULL` on a column that is 99% NULL, 2026-09-23)
	nulltests: Vec<(usize, bool)>,
	/// the seek's terms (seek.rs), when an equality or IN list on the sort key's leading columns
	/// is in the WHERE clause: those clauses are answered by the seek alone, exactly, since the
	/// rows of one value of a leading run are one run of rows. Empty otherwise. Each term's
	/// `expr` counts from the end of the runtime filters' expressions.
	seek: Vec<Term>,
}

/// `column IS [NOT] NULL` on a plain column of relation `relid`.
unsafe fn nulltest_of(clause: *mut pg_sys::Node, relid: pg_sys::Index) -> Option<(usize, bool)> {
	if clause.is_null() || (*clause).type_ != pg_sys::NodeTag::T_NullTest {
		return None;
	}
	let t = clause as *mut pg_sys::NullTest;
	let v = (*t).arg as *mut pg_sys::Node;
	if (*t).argisrow || v.is_null() || (*v).type_ != pg_sys::NodeTag::T_Var {
		return None;
	}
	let v = v as *mut pg_sys::Var;
	if (*v).varno as pg_sys::Index != relid || (*v).varattno <= 0 || (*v).varlevelsup != 0 {
		return None;
	}
	Some(((*v).varattno as usize - 1, (*t).nulltesttype == pg_sys::NullTestType::IS_NULL))
}

// ---------------------------------------------------------------------------------------
// Planning
// ---------------------------------------------------------------------------------------

unsafe fn func_is(oid: pg_sys::Oid, name: &str, schema: pg_sys::Oid) -> bool {
	if pg_sys::get_func_namespace(oid) != schema {
		return false;
	}
	let n = pg_sys::get_func_name(oid);
	!n.is_null() && CStr::from_ptr(n).to_bytes() == name.as_bytes()
}

unsafe fn our_schema() -> pg_sys::Oid {
	pg_sys::get_namespace_oid(c"snouttime".as_ptr(), true)
}

/// A Var of the relation being aggregated, as a 0-based attribute.
pub(super) unsafe fn var_att(n: *mut pg_sys::Node, relid: pg_sys::Index) -> Option<(usize, pg_sys::Oid)> {
	if n.is_null() || (*n).type_ != pg_sys::NodeTag::T_Var {
		return None;
	}
	let v = n as *mut pg_sys::Var;
	if (*v).varno as pg_sys::Index != relid || (*v).varattno <= 0 || (*v).varlevelsup != 0 {
		return None;
	}
	Some(((*v).varattno as usize - 1, (*v).vartype))
}

unsafe fn const_of(n: *mut pg_sys::Node) -> Option<pg_sys::Datum> {
	if n.is_null() || (*n).type_ != pg_sys::NodeTag::T_Const || (*(n as *mut pg_sys::Const)).constisnull {
		return None;
	}
	Some((*(n as *mut pg_sys::Const)).constvalue)
}

unsafe fn interval_of(d: pg_sys::Datum) -> pg_sys::Interval {
	*(d.cast_mut_ptr::<pg_sys::Interval>())
}

pub(super) unsafe fn key_expr(e: *mut pg_sys::Node, relid: pg_sys::Index) -> Option<KeyExpr> {
	if let Some((att, typ)) = var_att(e, relid) {
		let how = match typ {
			t if scan::int_family(t) || scan::time_type(t) => KeyHow::Int,
			pg_sys::BOOLOID => KeyHow::Bool,
			pg_sys::TEXTOID | pg_sys::VARCHAROID => {
				// equal must mean byte-equal
				let coll = pg_sys::exprCollation(e);
				if coll != pg_sys::InvalidOid && !pg_sys::get_collation_isdeterministic(coll) {
					return None;
				}
				KeyHow::Bytes { len: -1 }
			}
			pg_sys::UUIDOID => KeyHow::Bytes { len: 16 },
			_ => return None,
		};
		return Some(KeyExpr::Col { att, typ, how });
	}
	if (*e).type_ != pg_sys::NodeTag::T_FuncExpr {
		return None;
	}
	let f = e as *mut pg_sys::FuncExpr;
	let args = PgList::<pg_sys::Node>::from_pg((*f).args);
	let arg = |i: usize| args.get_ptr(i).unwrap_or(std::ptr::null_mut());
	let (att, typ) = var_att(arg(1), relid)?;
	if typ != pg_sys::TIMESTAMPTZOID && typ != pg_sys::TIMESTAMPOID {
		return None;
	}
	let width = interval_of(const_of(arg(0))?);
	if func_is((*f).funcid, "bucket", our_schema()) && (args.len() == 2 || args.len() == 3) {
		// the zone variants take text as their third argument; the origin one a time
		if args.len() == 3 && pg_sys::exprType(arg(2)) != typ {
			return None;
		}
		let w = checked_width(&width)?;
		let origin = if args.len() == 3 { const_of(arg(2))?.value() as i64 } else { w.default_origin() };
		return Some(KeyExpr::Bucket { att, typ, width: w, origin });
	}
	if func_is((*f).funcid, "date_bin", pg_sys::Oid::from(pg_sys::PG_CATALOG_NAMESPACE)) && args.len() == 3 {
		// date_bin refuses months, and a stride that is not positive, at run time: leave
		// those to it
		if width.month != 0 {
			return None;
		}
		let stride = (width.day as i64).checked_mul(86_400_000_000)?.checked_add(width.time)?;
		if stride <= 0 {
			return None;
		}
		let origin = const_of(arg(2))?.value() as i64;
		return Some(KeyExpr::DateBin { att, typ, stride, origin });
	}
	None
}

/// A bucket width the bucket function would accept, or None (and it raises its own error).
fn checked_width(i: &pg_sys::Interval) -> Option<Width> {
	let (months, days, micros) = (i.month as i64, i.day as i64, i.time);
	if months != 0 && (days != 0 || micros != 0) {
		return None;
	}
	if months != 0 {
		return (months > 0).then_some(Width::Months(months));
	}
	let us = days.checked_mul(86_400_000_000)?.checked_add(micros)?;
	(us > 0).then_some(Width::Micros(us))
}

unsafe fn agg_of(a: *mut pg_sys::Aggref, relid: pg_sys::Index) -> Option<Agg> {
	if !(*a).aggdistinct.is_null()
		|| !(*a).aggorder.is_null()
		|| !(*a).aggfilter.is_null()
		|| (*a).aggkind as u8 != b'n'
		|| (*a).agglevelsup != 0
	{
		return None;
	}
	let catalog = pg_sys::Oid::from(pg_sys::PG_CATALOG_NAMESPACE);
	let fnoid = (*a).aggfnoid;
	if (*a).aggstar {
		return func_is(fnoid, "count", catalog).then_some(Agg::CountStar);
	}
	let args = PgList::<pg_sys::TargetEntry>::from_pg((*a).args);
	if args.len() == 2 {
		let first = func_is(fnoid, "first", our_schema());
		if !first && !func_is(fnoid, "last", our_schema()) {
			return None;
		}
		let (v, vt) = var_att((*args.get_ptr(0)?).expr as *mut pg_sys::Node, relid)?;
		let (at, att) = var_att((*args.get_ptr(1)?).expr as *mut pg_sys::Node, relid)?;
		let value_ok = scan::int_family(vt)
			|| scan::time_type(vt)
			|| vt == pg_sys::FLOAT4OID
			|| vt == pg_sys::FLOAT8OID
			|| vt == pg_sys::BOOLOID
			|| vt == pg_sys::TEXTOID
			|| vt == pg_sys::VARCHAROID
			|| vt == pg_sys::UUIDOID;
		if !value_ok || !(scan::int_family(att) || scan::time_type(att)) || att == pg_sys::INT2OID || att == pg_sys::TIMEOID {
			return None;
		}
		return Some(if first { Agg::First(v, at, vt) } else { Agg::Last(v, at, vt) });
	}
	if args.len() != 1 {
		return None;
	}
	let (att, typ) = var_att((*args.get_ptr(0)?).expr as *mut pg_sys::Node, relid)?;
	let small_int = typ == pg_sys::INT2OID || typ == pg_sys::INT4OID;
	let f4 = typ == pg_sys::FLOAT4OID;
	let float = f4 || typ == pg_sys::FLOAT8OID;
	let ordered_int = scan::int_family(typ) || scan::time_type(typ);
	let is = |name: &str| func_is(fnoid, name, catalog);
	if is("count") {
		return Some(Agg::Count(att));
	}
	if is("sum") {
		return if small_int {
			Some(Agg::SumInt(att))
		} else if float {
			Some(Agg::SumFloat(att, f4))
		} else {
			None
		};
	}
	if is("avg") {
		return if small_int {
			Some(Agg::AvgInt(att))
		} else if float {
			Some(Agg::AvgFloat(att, f4))
		} else {
			None
		};
	}
	if is("min") || is("max") {
		let max = is("max");
		return if ordered_int {
			Some(if max { Agg::MaxInt(att) } else { Agg::MinInt(att) })
		} else if float {
			Some(if max { Agg::MaxFloat(att, f4) } else { Agg::MinFloat(att, f4) })
		} else {
			None
		};
	}
	None
}

/// The type a partial state of `agg` is: what Postgres's partial Aggref says it emits.
fn state_type(agg: Agg) -> pg_sys::Oid {
	match agg {
		Agg::CountStar | Agg::Count(_) | Agg::SumInt(_) => pg_sys::INT8OID,
		Agg::SumFloat(_, true) => pg_sys::FLOAT4OID,
		Agg::SumFloat(_, false) => pg_sys::FLOAT8OID,
		Agg::AvgInt(_) => pg_sys::INT8ARRAYOID,
		Agg::AvgFloat(..) => pg_sys::FLOAT8ARRAYOID,
		// same as the input: checked against the column's type instead
		Agg::MinInt(_) | Agg::MaxInt(_) | Agg::MinFloat(..) | Agg::MaxFloat(..) => pg_sys::InvalidOid,
		// an internal state, serialised
		Agg::First(..) | Agg::Last(..) => pg_sys::BYTEAOID,
	}
}

unsafe fn spec_of(
	root: *mut pg_sys::PlannerInfo,
	input: *mut pg_sys::RelOptInfo,
	output: *mut pg_sys::RelOptInfo,
	order: Option<&Order>,
) -> Option<Spec> {
	let relid = (*input).relid;
	let rte = *(*root).simple_rte_array.add(relid as usize);
	let parse = (*root).parse;
	if !(*parse).groupingSets.is_null() {
		return None;
	}
	// every restriction must be one the node evaluates itself: a key, a null test, or an
	// equality or IN list the seek answers (TSBS's one- and eight-host queries, 2026-09-25)
	let quals = PgList::<pg_sys::RestrictInfo>::from_pg((*input).baserestrictinfo);
	let (filters, _) = scan::plan_keys((*input).baserestrictinfo, relid);
	let nulltests: Vec<(usize, bool)> = quals.iter_ptr().filter_map(|q| nulltest_of((*q).clause as *mut pg_sys::Node, relid)).collect();
	let mut covered: Vec<bool> = quals
		.iter_ptr()
		.map(|q| {
			let c = (*q).clause as *mut pg_sys::Node;
			scan::key_of(c, relid).is_some() || nulltest_of(c, relid).is_some()
		})
		.collect();
	let mut seek_terms = Vec::new();
	if let Some(o) = order {
		let (terms, _, from) = seek::terms_by_clause(o, (*input).baserestrictinfo, relid);
		if terms.iter().any(|t| matches!(t.bound, Bound::Eq | Bound::In)) {
			for (t, &i) in terms.iter().zip(&from) {
				if matches!(t.bound, Bound::Eq | Bound::In) {
					covered[i] = true;
				}
			}
			seek_terms = terms;
		}
	}
	if covered.iter().any(|c| !c) {
		return None;
	}
	let mut spec =
		Spec { relid: (*rte).relid, rti: relid, keys: Vec::new(), aggs: Vec::new(), outputs: Vec::new(), filters, nulltests, seek: seek_terms };
	let exprs = PgList::<pg_sys::Node>::from_pg((*(*output).reltarget).exprs);
	for e in exprs.iter_ptr() {
		if (*e).type_ == pg_sys::NodeTag::T_Aggref {
			let a = e as *mut pg_sys::Aggref;
			if (*a).aggsplit != pg_sys::AggSplit::AGGSPLIT_INITIAL_SERIAL {
				return None;
			}
			let agg = agg_of(a, relid)?;
			let want = state_type(agg);
			if want != pg_sys::InvalidOid && (*a).aggtype != want {
				return None;
			}
			spec.outputs.push(Output::Agg(spec.aggs.len()));
			spec.aggs.push(agg);
		} else {
			let k = key_expr(e, relid)?;
			spec.outputs.push(Output::Key(spec.keys.len()));
			spec.keys.push(k);
		}
	}
	if spec.keys.len() > MAX_KEYS || spec.aggs.is_empty() {
		return None;
	}
	Some(spec)
}

/// The plan's description of the node, as String nodes: the relation, then one line per
/// output, then the filters in the scan's encoding.
unsafe fn to_private(spec: &Spec) -> *mut pg_sys::List {
	let mut list = PgList::<pg_sys::Node>::new();
	let mut push = |s: String| {
		let c = std::ffi::CString::new(s).unwrap();
		list.push(pg_sys::makeString(pg_sys::pstrdup(c.as_ptr())) as *mut pg_sys::Node);
	};
	push(format!("R {} {}", spec.relid.to_u32(), spec.rti));
	for o in &spec.outputs {
		push(match *o {
			Output::Key(i) => match spec.keys[i] {
				KeyExpr::Col { att, typ, how } => match how {
					KeyHow::Int => format!("K col {att} {} int", typ.to_u32()),
					KeyHow::Bool => format!("K col {att} {} bool", typ.to_u32()),
					KeyHow::Bytes { len } => format!("K col {att} {} bytes {len}", typ.to_u32()),
				},
				KeyExpr::Bucket { att, typ, width, origin } => match width {
					Width::Months(m) => format!("K bucket {att} {} M {m} {origin}", typ.to_u32()),
					Width::Micros(u) => format!("K bucket {att} {} U {u} {origin}", typ.to_u32()),
				},
				KeyExpr::DateBin { att, typ, stride, origin } => format!("K datebin {att} {} {stride} {origin}", typ.to_u32()),
			},
			Output::Agg(i) => format!("A {}", spec.aggs[i].text()),
		});
	}
	for k in &spec.filters {
		push(match k.value {
			PlanValue::Known(v) => format!("F {} {:?} K {}", k.att, k.op, v),
			PlanValue::Expr { index, typ } => format!("F {} {:?} R {} {}", k.att, k.op, index, typ.to_u32()),
		});
	}
	for &(att, is_null) in &spec.nulltests {
		push(format!("N {att} {}", is_null as u8));
	}
	for t in &spec.seek {
		push(format!("S {}", seek::term_text(t)));
	}
	list.into_pg()
}

fn parse_agg(s: &str) -> Option<Agg> {
	let p: Vec<&str> = s.split(' ').collect();
	let n = |i: usize| p.get(i).and_then(|x| x.parse::<u32>().ok());
	let att = || n(1).map(|x| x as usize);
	let f4 = || n(2).map(|x| x != 0);
	Some(match p[0] {
		"CountStar" => Agg::CountStar,
		"Count" => Agg::Count(att()?),
		"SumInt" => Agg::SumInt(att()?),
		"SumFloat" => Agg::SumFloat(att()?, f4()?),
		"AvgInt" => Agg::AvgInt(att()?),
		"AvgFloat" => Agg::AvgFloat(att()?, f4()?),
		"MinInt" => Agg::MinInt(att()?),
		"MaxInt" => Agg::MaxInt(att()?),
		"MinFloat" => Agg::MinFloat(att()?, f4()?),
		"MaxFloat" => Agg::MaxFloat(att()?, f4()?),
		"First" => Agg::First(att()?, n(2)? as usize, pg_sys::Oid::from(n(3)?)),
		"Last" => Agg::Last(att()?, n(2)? as usize, pg_sys::Oid::from(n(3)?)),
		_ => return None,
	})
}

fn parse_op(s: &str) -> Option<Op> {
	Some(match s {
		"Lt" => Op::Lt,
		"Le" => Op::Le,
		"Eq" => Op::Eq,
		"Ge" => Op::Ge,
		"Gt" => Op::Gt,
		_ => return None,
	})
}

unsafe fn from_private(list: *mut pg_sys::List) -> Option<Spec> {
	let items: Vec<String> = PgList::<pg_sys::String>::from_pg(list)
		.iter_ptr()
		.map(|s| CStr::from_ptr((*s).sval).to_string_lossy().into_owned())
		.collect();
	let mut spec = Spec {
		relid: pg_sys::InvalidOid,
		rti: 0,
		keys: Vec::new(),
		aggs: Vec::new(),
		outputs: Vec::new(),
		filters: Vec::new(),
		nulltests: Vec::new(),
		seek: Vec::new(),
	};
	for item in &items {
		let p: Vec<&str> = item.split(' ').collect();
		let num = |i: usize| p.get(i).and_then(|x| x.parse::<i64>().ok());
		match p.first().copied() {
			Some("R") => {
				spec.relid = pg_sys::Oid::from(num(1)? as u32);
				spec.rti = num(2)? as pg_sys::Index;
			}
			Some("K") => {
				let att = num(2)? as usize;
				let typ = pg_sys::Oid::from(num(3)? as u32);
				let k = match p[1] {
					"col" => KeyExpr::Col {
						att,
						typ,
						how: match *p.get(4)? {
							"int" => KeyHow::Int,
							"bool" => KeyHow::Bool,
							"bytes" => KeyHow::Bytes { len: num(5)? as i16 },
							_ => return None,
						},
					},
					"bucket" => KeyExpr::Bucket {
						att,
						typ,
						width: match *p.get(4)? {
							"M" => Width::Months(num(5)?),
							"U" => Width::Micros(num(5)?),
							_ => return None,
						},
						origin: num(6)?,
					},
					"datebin" => KeyExpr::DateBin { att, typ, stride: num(4)?, origin: num(5)? },
					_ => return None,
				};
				spec.outputs.push(Output::Key(spec.keys.len()));
				spec.keys.push(k);
			}
			Some("A") => {
				spec.outputs.push(Output::Agg(spec.aggs.len()));
				spec.aggs.push(parse_agg(&item[2..])?);
			}
			Some("F") => {
				let value = match *p.get(3)? {
					"K" => PlanValue::Known(num(4)?),
					"R" => PlanValue::Expr { index: num(4)? as usize, typ: pg_sys::Oid::from(num(5)? as u32) },
					_ => return None,
				};
				spec.filters.push(PlanKey { att: num(1)? as usize, op: parse_op(p.get(2)?)?, value });
			}
			Some("N") => spec.nulltests.push((num(1)? as usize, num(2)? != 0)),
			Some("S") => spec.seek.push(seek::parse_term(&item[2..])?),
			_ => return None,
		}
	}
	Some(spec)
}

#[pg_guard]
unsafe extern "C-unwind" fn hook(
	root: *mut pg_sys::PlannerInfo,
	stage: pg_sys::UpperRelationKind::Type,
	input: *mut pg_sys::RelOptInfo,
	output: *mut pg_sys::RelOptInfo,
	extra: *mut std::ffi::c_void,
) {
	if let Some(prev) = PREV_HOOK {
		prev(root, stage, input, output, extra);
	}
	if stage == pg_sys::UpperRelationKind::UPPERREL_DISTINCT && scan::ENABLED.get() {
		super::distinct::add_paths(root, input, output);
		return;
	}
	if stage != pg_sys::UpperRelationKind::UPPERREL_GROUP_AGG
		|| extra.is_null()
		|| !ENABLED.get()
		|| !scan::ENABLED.get()
		|| (*input).reloptkind != pg_sys::RelOptKind::RELOPT_BASEREL
		|| (*input).rtekind != pg_sys::RTEKind::RTE_RELATION
	{
		return;
	}
	// Postgres offers extensions only the final grouping stage; the partial one it builds for
	// itself, when it can aggregate in two halves, and its target is the partial states
	let grouped = output;
	let extra = extra as *mut pg_sys::GroupPathExtraData;
	let partial = match partial_rel(root, grouped) {
		Some(p) => p,
		None => match make_partial_rel(root, grouped, extra) {
			Some(p) => p,
			None => return,
		},
	};
	let rte = *(*root).simple_rte_array.add((*input).relid as usize);
	if (*rte).relkind as u8 == pg_sys::RELKIND_PARTITIONED_TABLE {
		partitionwise(root, grouped, extra, partial);
		return;
	}
	let Some((serial, parallel, groups)) = node_paths(root, input, partial) else {
		return;
	};
	finalize(root, grouped, extra, serial, groups);
	if let Some(p) = parallel {
		let mut rows = (*p).rows * (*p).parallel_workers as f64;
		let gather = pg_sys::create_gather_path(root, partial, p, (*partial).reltarget, std::ptr::null_mut(), &mut rows);
		finalize(root, grouped, extra, gather as *mut pg_sys::Path, groups);
	}
}

/// Partial partitionwise aggregation (`enable_partitionwise_aggregate`): Postgres has built a
/// partial-aggregate relation per partition and an Append of their cheapest paths, and asks
/// extensions only about the parent. So the node is offered to each sealed partition's partial
/// relation here, and the Append and its finalize are built again over whatever is now
/// cheapest in each partition.
unsafe fn partitionwise(
	root: *mut pg_sys::PlannerInfo,
	grouped: *mut pg_sys::RelOptInfo,
	extra: *mut pg_sys::GroupPathExtraData,
	partial: *mut pg_sys::RelOptInfo,
) {
	// the partitions' partial relations: the parents of the paths Postgres appended
	let mut members = Vec::new();
	for path in PgList::<pg_sys::Path>::from_pg((*partial).pathlist).iter_ptr() {
		if (*path).type_ == pg_sys::NodeTag::T_AppendPath {
			for sub in PgList::<pg_sys::Path>::from_pg((*(path as *mut pg_sys::AppendPath)).subpaths).iter_ptr() {
				members.push((*sub).parent);
			}
			break;
		}
	}
	// Each partition's path is chosen HERE, never added to its relation: add_path frees a path it
	// finds dominated, and Postgres's own Append (and the Finalize above it, which may still win)
	// points at the partitions' paths. Freed, that plan read garbage node tags ("unrecognized
	// node type: 65534") on a table of sealed and live partitions under run-time pruning (the
	// fairness run's snouttime-recent target, q6, 2026-09-25).
	let mut any = false;
	let mut children: Vec<(*mut pg_sys::Path, Option<*mut pg_sys::Path>)> = Vec::new();
	let mut groups = 0.0;
	for child in members {
		if child.is_null() || (*child).pathlist.is_null() || (*child).reloptkind != pg_sys::RelOptKind::RELOPT_OTHER_UPPER_REL {
			return;
		}
		let relid = pg_sys::bms_singleton_member((*child).relids);
		let input = *(*root).simple_rel_array.add(relid as usize);
		let mut whole = (*child).cheapest_total_path;
		let mut part = PgList::<pg_sys::Path>::from_pg((*child).partial_pathlist).get_ptr(0);
		if let Some((serial, parallel, g)) = node_paths(root, input, child) {
			if (*serial).total_cost < (*whole).total_cost {
				whole = serial;
			}
			if let Some(p) = parallel {
				if part.is_none_or(|q| (*p).total_cost < (*q).total_cost) {
					part = Some(p);
				}
			}
			groups += g;
			any = true;
		} else {
			groups += (*whole).rows;
		}
		children.push((whole, part));
	}
	if !any {
		return;
	}
	let mut subpaths = PgList::<pg_sys::Path>::new();
	for &(whole, _) in &children {
		subpaths.push(whole);
	}
	let append = pg_sys::create_append_path(root, partial, subpaths.into_pg(), std::ptr::null_mut(), std::ptr::null_mut(), std::ptr::null_mut(), 0, false, -1.0);
	finalize(root, grouped, extra, append as *mut pg_sys::Path, groups);
	// in parallel: a Parallel Append of the partitions' partial paths, and of the whole paths
	// of those that have none (an empty partition), which one worker each runs
	let mut partials = PgList::<pg_sys::Path>::new();
	let mut wholes = PgList::<pg_sys::Path>::new();
	let mut workers = 0;
	for &(whole, part) in &children {
		match part {
			Some(p) => {
				workers = workers.max((*p).parallel_workers);
				partials.push(p);
			}
			None if (*whole).parallel_safe => wholes.push(whole),
			None => return,
		}
	}
	if workers == 0 {
		return;
	}
	let workers = workers.min(pg_sys::max_parallel_workers_per_gather.max(1));
	let append = pg_sys::create_append_path(root, partial, wholes.into_pg(), partials.into_pg(), std::ptr::null_mut(), std::ptr::null_mut(), workers, true, -1.0);
	let mut rows = (*append).path.rows * workers as f64;
	let gather = pg_sys::create_gather_path(root, partial, append as *mut pg_sys::Path, (*partial).reltarget, std::ptr::null_mut(), &mut rows);
	finalize(root, grouped, extra, gather as *mut pg_sys::Path, groups);
}

/// The node's serial and parallel paths over one sealed relation, as the partial half of an
/// aggregate into `partial`, and its estimate of the groups; None when it does not apply.
unsafe fn node_paths(
	root: *mut pg_sys::PlannerInfo,
	input: *mut pg_sys::RelOptInfo,
	output: *mut pg_sys::RelOptInfo,
) -> Option<(*mut pg_sys::Path, Option<*mut pg_sys::Path>, f64)> {
	if ((*input).reloptkind != pg_sys::RelOptKind::RELOPT_BASEREL
		&& (*input).reloptkind != pg_sys::RelOptKind::RELOPT_OTHER_MEMBER_REL)
		|| (*input).rtekind != pg_sys::RTEKind::RTE_RELATION
		|| !(*input).lateral_relids.is_null()
	{
		return None;
	}
	let rte = *(*root).simple_rte_array.add((*input).relid as usize);
	if (*rte).relkind as u8 != pg_sys::RELKIND_RELATION || !(*rte).tablesample.is_null() {
		return None;
	}
	let r = pg_sys::RelationIdGetRelation((*rte).relid);
	if r.is_null() {
		return None;
	}
	if (*r).rd_tableam != super::am::routine() {
		pg_sys::RelationClose(r);
		return None;
	}
	let store = Store::open(r);
	let ncols = (*(*r).rd_att).natts as usize;
	let order = Order::of(&store);
	pg_sys::RelationClose(r);
	let spec = spec_of(root, input, output, order.as_ref())?;

	// cost: the pages of the row groups and columns it reads, and a little per row, well under
	// what forming a row and running the aggregate's functions on it costs. The per-row figure
	// is chosen, not measured.
	let keys: Vec<Key> = spec
		.filters
		.iter()
		.zip(scan::runtime_estimates(root, (*input).baserestrictinfo, (*input).relid))
		.filter_map(|(k, est)| {
			let value = match (k.value, est) {
				(PlanValue::Known(v), _) => v,
				(PlanValue::Expr { .. }, v) => v?,
			};
			Some(Key { att: k.att, op: k.op, value })
		})
		.collect();
	// the row groups the seek allows, for the values the planner can know now (all of them when
	// it knows none), as scan.rs costs them
	let mut allowed = vec![true; store.dir.len()];
	if let (Some(o), false) = (&order, spec.seek.is_empty()) {
		let (terms, exprs) = seek::terms(o, (*input).baserestrictinfo, (*input).relid);
		let values: Vec<Option<pg_sys::Datum>> = exprs
			.iter()
			.map(|&e| {
				let f = pg_sys::estimate_expression_value(root, e);
				let known = !f.is_null() && (*f).type_ == pg_sys::NodeTag::T_Const && !(*(f as *mut pg_sys::Const)).constisnull;
				known.then(|| (*(f as *mut pg_sys::Const)).constvalue)
			})
			.collect();
		allowed = vec![false; store.dir.len()];
		if let Some((hull, each)) = seek::seeks(o, &terms, &values) {
			let all = if each.is_empty() { vec![hull] } else { each };
			for mut s in all {
				for g in s.groups(&store) {
					allowed[g] = true;
				}
			}
		}
	}
	let kept = store.dir.iter().enumerate().filter(|&(i, g)| allowed[i] && scan::keeps(&keys, g)).count();
	let kept_frac = if store.dir.is_empty() { 1.0 } else { kept as f64 / store.dir.len() as f64 };
	let mut atts: Vec<usize> = spec.keys.iter().map(KeyExpr::att).chain(spec.aggs.iter().flat_map(Agg::atts)).collect();
	atts.extend(spec.filters.iter().map(|k| k.att));
	atts.extend(spec.nulltests.iter().map(|t| t.0));
	atts.sort_unstable();
	atts.dedup();
	// a column added after the seal reads as its default, which this node does not compute
	if atts.iter().any(|&a| a >= store.kinds.len()) {
		return None;
	}
	let col_frac = (atts.len().max(1) as f64 / ncols.max(1) as f64).clamp(0.05, 1.0);
	let rows_in = kept_frac * store.rows() as f64 + ((*input).tuples - store.rows() as f64).max(0.0);
	let per_row = pg_sys::cpu_operator_cost * (0.25 + 0.1 * spec.aggs.len() as f64);
	let io = (*input).pages as f64 * kept_frac * col_frac * pg_sys::seq_page_cost;
	// Groups: Postgres's estimate for the plain columns, times, for a time bucket, how many
	// buckets the rows' span holds, which the directory's minimum and maximum give exactly.
	// Postgres has no statistics for `date_bin(ts)` and guesses from ts's own distinct values,
	// one bucket per row or so, which prices a parallel plan's Gather out of reach (bench q3:
	// 65,448 groups estimated, 2,017 real, 230 ms serial against 82 in parallel).
	let mut col_exprs = PgList::<pg_sys::Node>::new();
	let mut buckets = 1.0f64;
	let exprs = PgList::<pg_sys::Node>::from_pg((*(*output).reltarget).exprs);
	let mut k = 0;
	for e in exprs.iter_ptr() {
		if (*e).type_ == pg_sys::NodeTag::T_Aggref {
			continue;
		}
		let width = match spec.keys[k] {
			KeyExpr::Col { .. } => None,
			KeyExpr::Bucket { width: Width::Micros(us), .. } => Some(us as f64),
			KeyExpr::Bucket { width: Width::Months(m), .. } => Some(m as f64 * 30.0 * 86_400_000_000.0),
			KeyExpr::DateBin { stride, .. } => Some(stride as f64),
		};
		let att = spec.keys[k].att();
		k += 1;
		match width {
			None => col_exprs.push(e),
			Some(w) => {
				let span = store
					.dir
					.iter()
					.enumerate()
					.filter(|&(i, g)| allowed[i] && scan::keeps(&keys, g))
					.map(|(_, g)| g)
					.filter_map(|g| g.minmax.get(att).copied().flatten())
					.fold(None, |a: Option<(i64, i64)>, m| Some(a.map_or((m.min, m.max), |(lo, hi)| (lo.min(m.min), hi.max(m.max)))));
				let n = span.map_or(1.0, |(lo, hi)| ((hi as f64 - lo as f64) / w).floor() + 1.0);
				buckets *= n.max(1.0);
			}
		}
	}
	let cols = if col_exprs.is_empty() {
		1.0
	} else {
		pg_sys::estimate_num_groups(root, col_exprs.into_pg(), (*input).rows.max(1.0), std::ptr::null_mut(), std::ptr::null_mut())
	};
	let groups = (cols * buckets).min(rows_in.max(1.0));
	let private = to_private(&spec);

	let make = |workers: i32| {
		let cp = pg_sys::palloc0(std::mem::size_of::<pg_sys::CustomPath>()) as *mut pg_sys::CustomPath;
		let p = &mut (*cp).path;
		p.type_ = pg_sys::NodeTag::T_CustomPath;
		p.pathtype = pg_sys::NodeTag::T_CustomScan;
		p.parent = output;
		p.pathtarget = (*output).reltarget;
		p.param_info = std::ptr::null_mut();
		p.parallel_aware = workers > 0;
		p.parallel_safe = (*input).consider_parallel;
		p.parallel_workers = workers;
		let divisor = if workers > 0 {
			let leader = (1.0 - 0.3 * workers as f64).max(0.0);
			workers as f64 + leader
		} else {
			1.0
		};
		// each worker emits its own groups, up to all of them
		p.rows = groups.min(rows_in / divisor).max(1.0);
		let run = (io + rows_in * per_row) / divisor + p.rows * pg_sys::cpu_tuple_cost;
		p.startup_cost = run;
		p.total_cost = run;
		p.pathkeys = std::ptr::null_mut();
		(*cp).flags = 0;
		(*cp).custom_private = private;
		(*cp).methods = &raw const PATH_METHODS;
		cp as *mut pg_sys::Path
	};
	let serial = make(0);
	let parallel = if (*input).consider_parallel && !(*input).partial_pathlist.is_null() {
		let workers = pg_sys::compute_parallel_worker(input, (*input).pages as f64, -1.0, pg_sys::max_parallel_workers_per_gather);
		(workers > 0).then(|| make(workers))
	} else {
		None
	};
	Some((serial, parallel, groups))
}

/// The partial-aggregate relation Postgres built for `grouped`, if it built one.
unsafe fn partial_rel(root: *mut pg_sys::PlannerInfo, grouped: *mut pg_sys::RelOptInfo) -> Option<*mut pg_sys::RelOptInfo> {
	let list = (*root).upper_rels[pg_sys::UpperRelationKind::UPPERREL_PARTIAL_GROUP_AGG as usize];
	for r in PgList::<pg_sys::RelOptInfo>::from_pg(list).iter_ptr() {
		if pg_sys::bms_equal((*r).relids, (*grouped).relids) {
			let target = (*r).reltarget;
			if target.is_null() || (*target).exprs.is_null() {
				return None;
			}
			return Some(r);
		}
	}
	None
}

/// The partial-aggregate relation, made here when Postgres made none: it makes one only for a
/// parallel or partitionwise plan, so a serial plan, which is what a sealed partition under
/// `min_parallel_table_scan_size` gets (compression makes that common), never had one. Its
/// target is built as the planner's own `make_partial_grouping_target` builds it (static there):
/// the grouping columns as they are, the Vars and Aggrefs of everything else and of HAVING, and
/// flat copies of the Aggrefs marked partial.
unsafe fn make_partial_rel(
	root: *mut pg_sys::PlannerInfo,
	grouped: *mut pg_sys::RelOptInfo,
	extra: *mut pg_sys::GroupPathExtraData,
) -> Option<*mut pg_sys::RelOptInfo> {
	if (*extra).flags & pg_sys::GROUPING_CAN_PARTIAL_AGG as i32 == 0 {
		return None;
	}
	let target = (*grouped).reltarget;
	let partial = pg_sys::create_empty_pathtarget();
	let mut others = PgList::<pg_sys::Node>::new();
	for (i, e) in PgList::<pg_sys::Expr>::from_pg((*target).exprs).iter_ptr().enumerate() {
		let sgref = if (*target).sortgrouprefs.is_null() { 0 } else { *(*target).sortgrouprefs.add(i) };
		if sgref != 0
			&& !(*root).processed_groupClause.is_null()
			&& !pg_sys::get_sortgroupref_clause_noerr(sgref, (*root).processed_groupClause).is_null()
		{
			pg_sys::add_column_to_pathtarget(partial, e, sgref);
		} else {
			others.push(e as *mut pg_sys::Node);
		}
	}
	if !(*extra).havingQual.is_null() {
		others.push((*extra).havingQual);
	}
	let flags = pg_sys::PVC_INCLUDE_AGGREGATES | pg_sys::PVC_RECURSE_WINDOWFUNCS | pg_sys::PVC_INCLUDE_PLACEHOLDERS;
	let pulled = pg_sys::pull_var_clause(others.into_pg() as *mut pg_sys::Node, flags as i32);
	pg_sys::add_new_columns_to_pathtarget(partial, pulled);
	let exprs = (*partial).exprs;
	let n = PgList::<pg_sys::Node>::from_pg(exprs).len();
	for i in 0..n {
		let cell = (*exprs).elements.add(i);
		let e = (*cell).ptr_value as *mut pg_sys::Node;
		if (*e).type_ == pg_sys::NodeTag::T_Aggref {
			let copy = pg_sys::palloc(std::mem::size_of::<pg_sys::Aggref>()) as *mut pg_sys::Aggref;
			std::ptr::copy_nonoverlapping(e as *const pg_sys::Aggref, copy, 1);
			pg_sys::mark_partial_aggref(copy, pg_sys::AggSplit::AGGSPLIT_INITIAL_SERIAL);
			(*cell).ptr_value = copy as *mut std::ffi::c_void;
		}
	}
	let partial = pg_sys::set_pathtarget_cost_width(root, partial);
	let rel = pg_sys::fetch_upper_rel(root, pg_sys::UpperRelationKind::UPPERREL_PARTIAL_GROUP_AGG, (*grouped).relids);
	(*rel).reltarget = partial;
	Some(rel)
}

/// A finalize aggregate over `sub` (the node, or a Gather of it) into the grouped relation.
unsafe fn finalize(
	root: *mut pg_sys::PlannerInfo,
	grouped: *mut pg_sys::RelOptInfo,
	extra: *mut pg_sys::GroupPathExtraData,
	sub: *mut pg_sys::Path,
	groups: f64,
) {
	let group_clause = (*root).processed_groupClause;
	let strategy = if group_clause.is_null() {
		pg_sys::AggStrategy::AGG_PLAIN
	} else if pg_sys::grouping_is_hashable(group_clause) {
		pg_sys::AggStrategy::AGG_HASHED
	} else {
		return;
	};
	let path = pg_sys::create_agg_path(
		root,
		grouped,
		sub,
		(*grouped).reltarget,
		strategy,
		pg_sys::AggSplit::AGGSPLIT_FINAL_DESERIAL,
		group_clause,
		(*extra).havingQual as *mut pg_sys::List,
		&raw const (*extra).agg_final_costs,
		// the grouped relation's own estimate is not made yet when extensions are asked
		if group_clause.is_null() { 1.0 } else { groups.max(1.0) },
	);
	pg_sys::add_path(grouped, path as *mut pg_sys::Path);
}

#[pg_guard]
unsafe extern "C-unwind" fn plan_path(
	root: *mut pg_sys::PlannerInfo,
	_rel: *mut pg_sys::RelOptInfo,
	best_path: *mut pg_sys::CustomPath,
	tlist: *mut pg_sys::List,
	_clauses: *mut pg_sys::List,
	_custom_plans: *mut pg_sys::List,
) -> *mut pg_sys::Plan {
	let cs = pg_sys::palloc0(std::mem::size_of::<pg_sys::CustomScan>()) as *mut pg_sys::CustomScan;
	(*cs).scan.plan.type_ = pg_sys::NodeTag::T_CustomScan;
	(*cs).scan.plan.targetlist = tlist;
	(*cs).scan.plan.qual = std::ptr::null_mut();
	// no relation scanned in the executor's sense: the output rows are the scan's tuples
	(*cs).scan.scanrelid = 0;
	(*cs).custom_scan_tlist = pg_sys::copyObjectImpl(tlist as *const std::ffi::c_void) as *mut pg_sys::List;
	(*cs).flags = (*best_path).flags;
	(*cs).custom_private = (*best_path).custom_private;
	// the filters' runtime values, found again from the relation the path aggregates
	let Some(spec) = from_private((*best_path).custom_private) else {
		pgrx::error!("a SnoutTime Columnar Aggregate path could not be read");
	};
	let relid = spec.rti;
	let mut exprs = PgList::<pg_sys::Node>::new();
	let base = *(*root).simple_rel_array.add(relid as usize);
	let (_, runtime) = scan::plan_keys((*base).baserestrictinfo, relid);
	let nruntime = runtime.len();
	for e in runtime {
		exprs.push(pg_sys::copyObjectImpl(e as *const std::ffi::c_void) as *mut pg_sys::Node);
	}
	// the seek's values follow, found again from the same clauses; the path was accepted only
	// because the seek answers its equalities, so the plan must seek by the same terms
	if !spec.seek.is_empty() {
		let r = pg_sys::RelationIdGetRelation(spec.relid);
		let order = if r.is_null() { None } else { Order::of(&Store::open(r)) };
		let (mut terms, values) = match &order {
			Some(o) => seek::terms(o, (*base).baserestrictinfo, relid),
			None => (Vec::new(), Vec::new()),
		};
		if !r.is_null() {
			pg_sys::RelationClose(r);
		}
		let same = terms.len() == spec.seek.len() && terms.iter().zip(&spec.seek).all(|(a, b)| a.pos == b.pos && a.bound == b.bound);
		if !same {
			pgrx::error!("a SnoutTime Columnar Aggregate path's seek could not be planned again");
		}
		for t in terms.iter_mut() {
			t.expr += nruntime;
		}
		for e in values {
			exprs.push(pg_sys::copyObjectImpl(e as *const std::ffi::c_void) as *mut pg_sys::Node);
		}
		let mut spec = spec;
		spec.seek = terms;
		(*cs).custom_private = to_private(&spec);
	}
	(*cs).custom_exprs = exprs.into_pg();
	(*cs).methods = &raw const SCAN_METHODS;
	cs as *mut pg_sys::Plan
}

// ---------------------------------------------------------------------------------------
// Execution
// ---------------------------------------------------------------------------------------

#[repr(C)]
struct Node {
	css: pg_sys::CustomScanState,
	state: *mut State,
}

#[repr(C)]
struct Shared {
	next_group: AtomicU64,
	delta_claimed: AtomicU32,
}

/// A group key: one word per group expression (a text value by its interned id) and which of
/// them are NULL.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Default)]
pub(super) struct GroupKey {
	pub(super) words: [u64; MAX_KEYS],
	pub(super) nulls: u8,
}

/// FxHash's mixing step: the keys are a few words, and SipHash's defences are not needed.
#[derive(Default)]
pub(super) struct Fx(u64);

impl Hasher for Fx {
	fn finish(&self) -> u64 {
		self.0
	}

	fn write(&mut self, bytes: &[u8]) {
		for b in bytes {
			self.write_u64(*b as u64);
		}
	}

	fn write_u64(&mut self, i: u64) {
		self.0 = (self.0.rotate_left(5) ^ i).wrapping_mul(0x51_7c_c1_b7_27_22_0a_95);
	}

	fn write_u8(&mut self, i: u8) {
		self.write_u64(i as u64);
	}

	fn write_usize(&mut self, i: usize) {
		self.write_u64(i as u64);
	}
}

pub(super) type FxMap<K, V> = HashMap<K, V, BuildHasherDefault<Fx>>;

/// One aggregate's running state for one group.
#[derive(Clone, Copy, Default)]
struct Cell {
	n: i64,
	i: i64,
	x: f64,
	y: f64,
	has: bool,
	/// first/last: the value held, as `Val::bits`, and whether it is NULL
	w: u64,
	vnull: bool,
}

/// A value of one input column for one row.
#[derive(Clone, Copy)]
pub(super) enum Val {
	Null,
	I(i64),
	F(f64),
	B(bool),
	/// an interned byte string
	T(u32),
}

impl Val {
	/// The value as one word: an integer, a float's bits, a bool, an interned string's id.
	pub(super) fn bits(self) -> u64 {
		match self {
			Val::Null => 0,
			Val::I(v) => v as u64,
			Val::F(f) => f.to_bits(),
			Val::B(b) => b as u64,
			Val::T(id) => id as u64,
		}
	}
}

/// A row group's input column, expanded to one entry per row.
pub(super) enum Col {
	Int(Vec<i64>, Vec<bool>),
	Float(Vec<f64>, Vec<bool>),
	Bool(Vec<bool>, Vec<bool>),
	Text(Vec<u32>, Vec<bool>),
}

impl Col {
	/// Whether the column was decoded: one left out (`columns`' `skip`) has no rows.
	#[inline]
	fn decoded(&self) -> bool {
		!match self {
			Col::Int(_, n) | Col::Float(_, n) | Col::Bool(_, n) | Col::Text(_, n) => n.is_empty(),
		}
	}

	#[inline]
	pub(super) fn get(&self, r: usize) -> Val {
		match self {
			Col::Int(v, n) => if n[r] { Val::Null } else { Val::I(v[r]) },
			Col::Float(v, n) => if n[r] { Val::Null } else { Val::F(v[r]) },
			Col::Bool(v, n) => if n[r] { Val::Null } else { Val::B(v[r]) },
			Col::Text(v, n) => if n[r] { Val::Null } else { Val::T(v[r]) },
		}
	}
}

/// An integer aggregate takes a non-NULL value: sum (int2/int4 into int8), avg's {count, sum},
/// min, max. As Postgres's own transition functions do it.
#[inline]
fn int_step(agg: Agg, c: &mut Cell, x: i64) {
	match agg {
		Agg::SumInt(_) => {
			c.i = if c.has { c.i.wrapping_add(x) } else { x };
			c.has = true;
		}
		Agg::AvgInt(_) => {
			c.n += 1;
			c.i = c.i.wrapping_add(x);
		}
		Agg::MinInt(_) => {
			if !c.has || x <= c.i {
				c.i = x;
			}
			c.has = true;
		}
		Agg::MaxInt(_) => {
			if !c.has || x >= c.i {
				c.i = x;
			}
			c.has = true;
		}
		_ => {}
	}
}

/// A float aggregate takes a non-NULL value, as float4pl/float8pl, float8_accum (Youngs-Cramer)
/// and float8_smaller/larger do it.
#[inline]
fn float_step(agg: Agg, c: &mut Cell, x: f64) {
	match agg {
		Agg::SumFloat(_, f4) => {
			if !c.has {
				c.x = x;
				c.has = true;
			} else if f4 {
				let (a, b) = (c.x as f32, x as f32);
				let r = a + b;
				if r.is_infinite() && !a.is_infinite() && !b.is_infinite() {
					float_overflow();
				}
				c.x = r as f64;
			} else {
				let r = c.x + x;
				if r.is_infinite() && !c.x.is_infinite() && !x.is_infinite() {
					float_overflow();
				}
				c.x = r;
			}
		}
		Agg::AvgFloat(..) => {
			let n_old = c.n as f64;
			let n = n_old + 1.0;
			let sx_old = c.x;
			c.x += x;
			if n_old > 0.0 {
				let tmp = x * n - c.x;
				c.y += tmp * tmp / (n * n_old);
				if c.x.is_infinite() || c.y.is_infinite() {
					if !sx_old.is_infinite() && !x.is_infinite() {
						float_overflow();
					}
					c.y = f64::NAN;
				}
			} else if x.is_nan() || x.is_infinite() {
				c.y = f64::NAN;
			}
			c.n += 1;
		}
		// float8_smaller/larger keep the new value when the two are equal
		Agg::MinFloat(..) => {
			if !c.has || !f_lt(c.x, x) {
				c.x = x;
			}
			c.has = true;
		}
		Agg::MaxFloat(..) => {
			if !c.has || !f_gt(c.x, x) {
				c.x = x;
			}
			c.has = true;
		}
		_ => {}
	}
}

/// Does every row of the group pass `k`, by its column's minimum, maximum and null count alone?
unsafe fn settled(k: &Key, entry: &super::format::GroupEntry, g: &read::Group) -> bool {
	let Some(Some(m)) = entry.minmax.get(k.att) else {
		return false;
	};
	if g.nulls(k.att) != Some(0) {
		return false;
	}
	match k.op {
		Op::Lt => m.max < k.value,
		Op::Le => m.max <= k.value,
		Op::Eq => m.min == k.value && m.max == k.value,
		Op::Ge => m.min >= k.value,
		Op::Gt => m.min > k.value,
	}
}

fn float_overflow() -> ! {
	ereport!(PgLogLevel::ERROR, PgSqlErrorCode::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE, "value out of range: overflow");
	unreachable!()
}

/// float8 comparison as Postgres orders it: NaN above everything, and equal to itself.
fn f_gt(a: f64, b: f64) -> bool {
	if a.is_nan() {
		!b.is_nan()
	} else {
		!b.is_nan() && a > b
	}
}

fn f_lt(a: f64, b: f64) -> bool {
	f_gt(b, a)
}

/// `v`, the values of rows `at..`, as all `rows` rows, `fill` in the others.
fn pad<T: Clone>(v: Vec<T>, at: usize, rows: usize, fill: T) -> Vec<T> {
	let mut out = Vec::with_capacity(rows);
	out.resize(at, fill.clone());
	out.extend(v);
	out.resize(rows, fill);
	out
}

/// Byte strings (text, uuid) as small ids, the same id for equal bytes across row groups,
/// whose dictionaries number their entries each their own way.
#[derive(Default)]
pub(super) struct Interner {
	map: FxMap<Vec<u8>, u32>,
	pub(super) strings: Vec<Vec<u8>>,
}

impl Interner {
	pub(super) fn intern(&mut self, b: &[u8]) -> u32 {
		if let Some(&id) = self.map.get(b) {
			return id;
		}
		let id = self.strings.len() as u32;
		self.strings.push(b.to_vec());
		self.map.insert(b.to_vec(), id);
		id
	}

	/// A row group's input columns, expanded to one entry per row.
	/// Columns in `skip` are not decoded: an empty column stands in for each, which the caller
	/// must not read.
	///
	/// Only the rows `span` (from the group's first) are asked for: a paged column decodes the
	/// pages that hold them, and reads as NULL in the rest.
	pub(super) unsafe fn columns(&mut self, g: &Group, kinds: &[Kind], inputs: &[usize], skip: &[usize], span: std::ops::Range<usize>) -> Vec<Col> {
		let rows = g.rows as usize;
		let mut out = Vec::with_capacity(inputs.len());
		for &att in inputs {
			if skip.contains(&att) {
				out.push(Col::Int(Vec::new(), Vec::new()));
				continue;
			}
			if att >= kinds.len() {
				// added after the seal: its "missing" value, which the node does not read;
				// such a relation was declined at planning (see `columns_stored`)
				out.push(Col::Int(vec![0; rows], vec![true; rows]));
				continue;
			}
			let (covered, DecodedColumn { nulls, values }) = g.raw_span(att, span.clone());
			let n = nulls.len();
			let col = match (values, kinds[att]) {
				// no NULLs: the values are already one per row
				(Decoded::Int(v), _) if v.len() == n => Col::Int(v, nulls),
				(Decoded::Int(v), _) => {
					let mut e = Vec::with_capacity(rows);
					let mut it = v.into_iter();
					for &n in &nulls {
						e.push(if n { 0 } else { it.next().unwrap_or(0) });
					}
					Col::Int(e, nulls)
				}
				(Decoded::Float(v), k) => {
					let f4 = k == Kind::Float4;
					let mut e = Vec::with_capacity(rows);
					let mut it = v.into_iter();
					for &n in &nulls {
						let bits = if n { 0 } else { it.next().unwrap_or(0) };
						e.push(if f4 { f32::from_bits(bits as u32) as f64 } else { f64::from_bits(bits) });
					}
					Col::Float(e, nulls)
				}
				(Decoded::Bool(v), _) => {
					let mut e = Vec::with_capacity(rows);
					let mut it = v.into_iter();
					for &n in &nulls {
						e.push(!n && it.next().unwrap_or(false));
					}
					Col::Bool(e, nulls)
				}
				(Decoded::Dict { entries, indexes }, _) => {
					let ids: Vec<u32> = entries.iter().map(|e| self.intern(e)).collect();
					let mut e = Vec::with_capacity(rows);
					let mut it = indexes.into_iter();
					for &n in &nulls {
						e.push(if n { 0 } else { ids[it.next().unwrap_or(0) as usize] });
					}
					Col::Text(e, nulls)
				}
				(Decoded::Plain(v), _) => {
					let mut e = Vec::with_capacity(rows);
					let mut it = v.into_iter();
					for &n in &nulls {
						e.push(if n { 0 } else { self.intern(&it.next().unwrap_or_default()) });
					}
					Col::Text(e, nulls)
				}
			};
			// rows outside the pages read are NULL, and never read: `group_rows` looks at `span`
			out.push(if covered.start == 0 && covered.end == rows {
				col
			} else {
				let at = covered.start;
				match col {
					Col::Int(v, n) => Col::Int(pad(v, at, rows, 0), pad(n, at, rows, true)),
					Col::Float(v, n) => Col::Float(pad(v, at, rows, 0.0), pad(n, at, rows, true)),
					Col::Bool(v, n) => Col::Bool(pad(v, at, rows, false), pad(n, at, rows, true)),
					Col::Text(v, n) => Col::Text(pad(v, at, rows, 0), pad(n, at, rows, true)),
				}
			});
		}
		out
	}

	/// Attribute `att` of a tuple in `slot` (a delta-store row), as a value.
	///
	/// # Safety
	/// `slot` must hold a tuple of `desc` with at least `att + 1` attributes deformed.
	pub(super) unsafe fn slot_val(&mut self, slot: *mut pg_sys::TupleTableSlot, desc: pg_sys::TupleDesc, att: usize) -> Val {
		if att >= (*desc).natts as usize || *(*slot).tts_isnull.add(att) {
			return Val::Null;
		}
		let d = *(*slot).tts_values.add(att);
		let typ = crate::tuple_attr(desc, att).atttypid;
		match types::kind_of_type(typ.to_u32()) {
			Kind::Int16 | Kind::Int32 | Kind::Int64 => Val::I(scan::datum_value(d, typ)),
			Kind::Float4 => Val::F(f32::from_bits(d.value() as u32) as f64),
			Kind::Float8 => Val::F(f64::from_bits(d.value() as u64)),
			Kind::Bool => Val::B(d.value() & 0xff != 0),
			Kind::Bytes { len: -1, .. } => {
				let v = pg_sys::pg_detoast_datum_packed(d.cast_mut_ptr());
				Val::T(self.intern(pgrx::varlena::varlena_to_byte_slice(v)))
			}
			Kind::Bytes { len, byval: false } if len > 0 => {
				Val::T(self.intern(std::slice::from_raw_parts(d.cast_mut_ptr::<u8>(), len as usize)))
			}
			_ => Val::Null,
		}
	}
}

struct State {
	spec: Spec,
	store: Option<Rc<Store>>,
	rel: pg_sys::Relation,
	/// which input columns the node reads, and where each attribute is among them
	inputs: Vec<usize>,
	pos: Vec<usize>,
	/// the input columns only a filter reads: not decoded in a row group whose range and null
	/// count settle every filter on them (IoT i4 decoded the time column of a whole day to
	/// check `ts >= <the day's start>`, 2026-09-23)
	filter_only: Vec<usize>,
	exprs: Vec<*mut pg_sys::ExprState>,
	econtext: *mut pg_sys::ExprContext,
	keys: Vec<Key>,
	none: bool,
	/// the store's order, the seek's terms, and this execution's seeks: one per value of an IN
	/// list, or the one seek; with the row groups they allow, in order. Without a seek every row
	/// group is a candidate and `seeks` is empty
	order: Option<Order>,
	seek_terms: Vec<Term>,
	seeks: Vec<Seek>,
	candidates: Vec<usize>,
	/// where the seeks' IN-list values live, reset by each resolve
	seek_cxt: pg_sys::MemoryContext,
	/// the deleted column-store rows `group_rows` leaves out: the current row group's, read
	/// through the delete log's index when few row groups are visited, or the whole log's, read
	/// once (as the scan does it); nothing is read before a row group is
	deleted: Vec<u64>,
	deleted_all: bool,
	deletes_oid: pg_sys::Oid,
	delta_oid: pg_sys::Oid,
	snapshot: pg_sys::Snapshot,
	shared: *mut Shared,
	next_group: usize,
	// the aggregation
	groups: FxMap<GroupKey, usize>,
	last: Option<(GroupKey, usize)>,
	/// per bucket key, the span of the last bucket computed: rows in time order fall in it
	spans: [(i64, i64); MAX_KEYS],
	group_keys: Vec<GroupKey>,
	cells: Vec<Cell>,
	names: Interner,
	done: bool,
	emitted: usize,
	/// where output rows' by-reference values are built, reset per row
	row_cxt: pg_sys::MemoryContext,
	mem: types::Mem,
	read: u64,
	skipped: u64,
}

impl State {
	unsafe fn resolve(&mut self) {
		self.keys.clear();
		self.none = false;
		for k in &self.spec.filters {
			let value = match k.value {
				PlanValue::Known(v) => v,
				PlanValue::Expr { index, typ } => {
					let st = self.exprs[index];
					let econtext = self.econtext;
					let mut isnull = false;
					let d = scan::ffi(|| {
						let old = pg_sys::MemoryContextSwitchTo((*econtext).ecxt_per_tuple_memory);
						let d = (*st).evalfunc.expect("an initialized expression")(st, econtext, &mut isnull);
						pg_sys::MemoryContextSwitchTo(old);
						d
					});
					if isnull {
						self.none = true;
						continue;
					}
					scan::datum_value(d, typ)
				}
			};
			self.keys.push(Key { att: k.att, op: k.op, value });
		}
		self.seeks.clear();
		let ngroups = self.store.as_ref().map_or(0, |st| st.dir.len());
		self.candidates = (0..ngroups).collect();
		let Some(o) = &self.order else { return };
		if self.seek_terms.is_empty() {
			return;
		}
		pg_sys::MemoryContextReset(self.seek_cxt);
		let mut values = vec![None; self.exprs.len()];
		for t in &self.seek_terms {
			let st = self.exprs[t.expr];
			let econtext = self.econtext;
			let seek_cxt = self.seek_cxt;
			let in_list = t.bound == Bound::In;
			let mut isnull = false;
			let d = scan::ffi(|| {
				let old = pg_sys::MemoryContextSwitchTo((*econtext).ecxt_per_tuple_memory);
				let d = (*st).evalfunc.expect("an initialized expression")(st, econtext, &mut isnull);
				pg_sys::MemoryContextSwitchTo(seek_cxt);
				// copied: every value's seek points into the array
				let d = if in_list && !isnull { pg_sys::datumCopy(d, false, -1) } else { d };
				pg_sys::MemoryContextSwitchTo(old);
				d
			});
			if isnull {
				// a comparison with NULL is never true
				self.none = true;
			} else {
				values[t.expr] = Some(d);
			}
		}
		if self.none {
			self.candidates.clear();
			return;
		}
		let old = pg_sys::MemoryContextSwitchTo(self.seek_cxt);
		let built = seek::seeks(o, &self.seek_terms, &values);
		pg_sys::MemoryContextSwitchTo(old);
		let store = Rc::clone(self.store.as_ref().unwrap());
		match built {
			// an IN list with no value: no row matches
			None => {
				self.none = true;
				self.candidates.clear();
			}
			Some((hull, each)) => {
				self.seeks = if each.is_empty() { vec![hull] } else { each };
				let mut ok = vec![false; ngroups];
				for s in self.seeks.iter_mut() {
					for g in s.groups(&store) {
						ok[g] = true;
					}
				}
				self.candidates = (0..ngroups).filter(|&g| ok[g]).collect();
			}
		}
	}

	fn reset(&mut self) {
		self.groups.clear();
		self.last = None;
		self.group_keys.clear();
		self.cells.clear();
		self.done = false;
		self.emitted = 0;
		self.next_group = 0;
		self.read = 0;
		self.skipped = 0;
	}

	/// Group expression `i`'s word for a value: the value itself, or its bucket. None is NULL.
	#[inline]
	fn key_word(&mut self, i: usize, v: Val) -> Option<u64> {
		let k = self.spec.keys[i];
		Some(match (v, k) {
			(Val::Null, _) => return None,
			(Val::I(t), KeyExpr::Bucket { width, origin, .. }) => {
				let (lo, hi) = self.spans[i];
				if lo <= t && t < hi {
					lo as u64
				} else if t == i64::MAX || t == i64::MIN {
					t as u64
				} else {
					let b = crate::bucket::plain(width, t, origin);
					// a bucket of fixed width is a span the next rows are likely in
					if let Width::Micros(us) = width {
						self.spans[i] = (b, b.saturating_add(us));
					}
					b as u64
				}
			}
			(Val::I(t), KeyExpr::DateBin { stride, origin, .. }) => {
				let (lo, hi) = self.spans[i];
				if lo <= t && t < hi {
					lo as u64
				} else {
					let b = date_bin(t, stride, origin);
					if t != i64::MAX && t != i64::MIN {
						self.spans[i] = (b, b.saturating_add(stride));
					}
					b as u64
				}
			}
			(v, _) => v.bits(),
		})
	}

	/// The group of `key`, made if it is new.
	#[inline]
	fn group_of(&mut self, key: GroupKey) -> usize {
		match self.last {
			// sorted data puts a group's rows together: the last row's group is the likely one
			Some((k, g)) if k == key => g,
			_ => {
				let g = match self.groups.get(&key) {
					Some(&g) => g,
					None => {
						let g = self.group_keys.len();
						self.groups.insert(key, g);
						self.group_keys.push(key);
						self.cells.resize(self.cells.len() + self.spec.aggs.len(), Cell::default());
						g
					}
				};
				self.last = Some((key, g));
				g
			}
		}
	}

	#[inline]
	fn passes(&self, vals: &[Val]) -> bool {
		self.spec.nulltests.iter().all(|&(att, is_null)| matches!(vals[self.pos[att]], Val::Null) == is_null)
			&& self.keys.iter().all(|k| match vals[self.pos[k.att]] {
				Val::I(v) => match k.op {
					Op::Lt => v < k.value,
					Op::Le => v <= k.value,
					Op::Eq => v == k.value,
					Op::Ge => v >= k.value,
					Op::Gt => v > k.value,
				},
				_ => false,
			})
	}

	/// One input row (a delta-store row), its columns in `inputs` order.
	fn add(&mut self, vals: &[Val]) {
		if !self.passes(vals) {
			return;
		}
		let mut key = GroupKey::default();
		for i in 0..self.spec.keys.len() {
			match self.key_word(i, vals[self.pos[self.spec.keys[i].att()]]) {
				None => key.nulls |= 1 << i,
				Some(w) => key.words[i] = w,
			}
		}
		let g = self.group_of(key);
		self.step(g, vals);
	}

	/// Every aggregate of group `g` takes one row.
	#[inline]
	fn step(&mut self, g: usize, vals: &[Val]) {
		for a in 0..self.spec.aggs.len() {
			self.step_one(g, a, vals);
		}
	}

	/// Aggregate `a` of group `g` takes one row.
	#[inline]
	fn step_one(&mut self, g: usize, a: usize, vals: &[Val]) {
		let agg = self.spec.aggs[a];
		let c = &mut self.cells[g * self.spec.aggs.len() + a];
		let v = match agg.att() {
			None => Val::B(true),
			Some(att) => vals[self.pos[att]],
		};
		if matches!(v, Val::Null) {
			return;
		}
		match (agg, v) {
			// point.rs's transition: the earliest (latest) non-NULL time wins, the first row
			// seen keeps a tie, and a NULL value is held like any other
			(Agg::First(value, at, _) | Agg::Last(value, at, _), _) => {
				let Val::I(t) = vals[self.pos[at]] else {
					return;
				};
				let first = matches!(agg, Agg::First(..));
				if !c.has || (first && t < c.i) || (!first && t > c.i) {
					let v = vals[self.pos[value]];
					c.i = t;
					c.has = true;
					c.vnull = matches!(v, Val::Null);
					c.w = v.bits();
				}
			}
			(Agg::CountStar | Agg::Count(_), _) => c.n += 1,
			(_, Val::I(x)) => int_step(agg, c, x),
			(_, Val::F(x)) => float_step(agg, c, x),
			_ => {}
		}
	}

	/// One row group's rows in `ranges` (from the group's first row, in order, not overlapping),
	/// a column at a time: the WHERE clause as a mask, each group expression's words, then runs
	/// of rows with one key, whose group is found once and whose aggregates each take the run in
	/// a loop over their own column. The arithmetic per row is `step`'s, so the result does not
	/// change; what goes is the per-row dispatch. Only the span from the first range's start to
	/// the last one's end is looked at: a seek for one host's hour is a few hundred of a group's
	/// 8,192 rows.
	fn group_rows(&mut self, cols: &[Col], first: u64, ranges: &[std::ops::Range<usize>], del: &mut usize, keys: &[Key]) {
		let (Some(lo), Some(hi)) = (ranges.first().map(|r| r.start), ranges.last().map(|r| r.end)) else {
			return;
		};
		// `keep[r - lo]`: row r of the group is in a range, not deleted, and passes the filters
		let mut keep = vec![false; hi - lo];
		for r in ranges {
			keep[r.start - lo..r.end - lo].iter_mut().for_each(|k| *k = true);
		}
		for (j, k) in keep.iter_mut().enumerate() {
			let n = first + (lo + j) as u64;
			while *del < self.deleted.len() && self.deleted[*del] < n {
				*del += 1;
			}
			if *del < self.deleted.len() && self.deleted[*del] == n {
				*k = false;
			}
		}
		for &f in keys {
			let Col::Int(v, nulls) = &cols[self.pos[f.att]] else {
				keep.iter_mut().for_each(|k| *k = false);
				break;
			};
			for (j, k) in keep.iter_mut().enumerate() {
				let r = lo + j;
				let x = v[r];
				let ok = !nulls[r]
					&& match f.op {
						Op::Lt => x < f.value,
						Op::Le => x <= f.value,
						Op::Eq => x == f.value,
						Op::Ge => x >= f.value,
						Op::Gt => x > f.value,
					};
				*k &= ok;
			}
		}
		for &(att, is_null) in &self.spec.nulltests {
			let c = &cols[self.pos[att]];
			for (j, k) in keep.iter_mut().enumerate() {
				*k &= matches!(c.get(lo + j), Val::Null) == is_null;
			}
		}
		let nkeys = self.spec.keys.len();
		let mut words: Vec<Vec<Option<u64>>> = Vec::with_capacity(nkeys);
		for i in 0..nkeys {
			let col = &cols[self.pos[self.spec.keys[i].att()]];
			let mut w = Vec::with_capacity(hi - lo);
			for j in 0..hi - lo {
				w.push(if keep[j] { self.key_word(i, col.get(lo + j)) } else { None });
			}
			words.push(w);
		}
		let naggs = self.spec.aggs.len();
		let aggs = self.spec.aggs.clone();
		let mut j = 0;
		while j < hi - lo {
			if !keep[j] {
				j += 1;
				continue;
			}
			let mut e = j + 1;
			while e < hi - lo && (!keep[e] || words.iter().all(|w| w[e] == w[j])) {
				e += 1;
			}
			let mut key = GroupKey::default();
			for (i, w) in words.iter().enumerate() {
				match w[j] {
					None => key.nulls |= 1 << i,
					Some(x) => key.words[i] = x,
				}
			}
			let g = self.group_of(key);
			for (a, agg) in aggs.iter().enumerate() {
				let c = &mut self.cells[g * naggs + a];
				// the run's rows of the group, and each one's place in `keep`
				let rows = lo + j..lo + e;
				match *agg {
					Agg::CountStar => c.n += keep[j..e].iter().filter(|&&k| k).count() as i64,
					Agg::Count(att) => {
						let col = &cols[self.pos[att]];
						c.n += rows.filter(|&i| keep[i - lo] && !matches!(col.get(i), Val::Null)).count() as i64;
					}
					Agg::SumFloat(att, _) | Agg::AvgFloat(att, _) | Agg::MinFloat(att, _) | Agg::MaxFloat(att, _)
						if matches!(cols[self.pos[att]], Col::Float(..)) =>
					{
						let Col::Float(v, nulls) = &cols[self.pos[att]] else { unreachable!() };
						for i in rows {
							if keep[i - lo] && !nulls[i] {
								float_step(*agg, c, v[i]);
							}
						}
					}
					Agg::SumInt(att) | Agg::AvgInt(att) | Agg::MinInt(att) | Agg::MaxInt(att)
						if matches!(cols[self.pos[att]], Col::Int(..)) =>
					{
						let Col::Int(v, nulls) = &cols[self.pos[att]] else { unreachable!() };
						for i in rows {
							if keep[i - lo] && !nulls[i] {
								int_step(*agg, c, v[i]);
							}
						}
					}
					_ => {
						// first/last: row by row, as `step` does it
						let mut vals = vec![Val::Null; self.inputs.len()];
						for i in rows {
							if keep[i - lo] {
								for (k, col) in cols.iter().enumerate() {
									// a filter's column the group's range settled was not decoded,
									// and nothing here reads it
									vals[k] = if col.decoded() { col.get(i) } else { Val::Null };
								}
								self.step_one(g, a, &vals);
							}
						}
					}
				}
			}
			j = e;
		}
	}

	/// Everything: every row group this worker claims, then the delta store if it claims it.
	unsafe fn aggregate(&mut self) {
		let store = Rc::clone(self.store.as_ref().unwrap());
		let kinds = store.kinds.clone();
		let mut vals = vec![Val::Null; self.inputs.len()];
		// Chosen as the scan's is (scan.rs): an index scan of the log per row group is cheaper
		// than reading all of it only for a few row groups, and a parallel node's are shared out.
		const DELETES_PER_GROUP: usize = 16;
		let mut per_group = self.shared.is_null() && self.candidates.len() <= DELETES_PER_GROUP;
		loop {
			let i = if self.shared.is_null() {
				let n = self.next_group;
				self.next_group += 1;
				n
			} else {
				(*self.shared).next_group.fetch_add(1, Ordering::SeqCst) as usize
			};
			if i >= self.candidates.len() {
				break;
			}
			let next = self.candidates[i];
			if self.none || !scan::keeps(&self.keys, &store.dir[next]) {
				self.skipped += 1;
				continue;
			}
			let g = Store::load(&store, next);
			// the rows each seek allows here, from the group's first row; one range for the
			// whole group without a seek. Values of an IN list are sorted, so their runs are too
			let mut ranges: Vec<std::ops::Range<usize>> = Vec::new();
			if self.seeks.is_empty() {
				ranges.push(0..g.rows as usize);
			} else {
				let o = self.order.as_ref().unwrap();
				for s in self.seeks.iter_mut() {
					let r = s.rows(o, &store, &g);
					if r.start < r.end {
						ranges.push((r.start - g.first_row) as usize..(r.end - g.first_row) as usize);
					}
				}
				if ranges.is_empty() {
					self.skipped += 1;
					continue;
				}
			}
			// a null test no row of the group can pass, from its header alone
			if self.spec.nulltests.iter().any(|&(att, is_null)| match g.nulls(att) {
				Some(n) => if is_null { n == 0 } else { n == g.rows },
				None => false,
			}) {
				self.skipped += 1;
				continue;
			}
			self.read += 1;
			// the filters this group's minimum, maximum and null count do not already settle
			let entry = &store.dir[next];
			let active: Vec<Key> = self.keys.iter().copied().filter(|k| !settled(k, entry, &g)).collect();
			let skip: Vec<usize> = self.filter_only.iter().copied().filter(|&a| active.iter().all(|k| k.att != a)).collect();
			let span = ranges[0].start..ranges[ranges.len() - 1].end;
			let cols = self.names.columns(&g, &kinds, &self.inputs.clone(), &skip, span);
			let first = g.first_row;
			if per_group {
				match read::deleted_in(self.deletes_oid, self.snapshot, first, first + g.rows) {
					Some(d) => self.deleted = d,
					// no index on the log (never, as the extension makes it): the whole log
					None => per_group = false,
				}
			}
			if !per_group && !self.deleted_all {
				self.deleted = read::deleted_rows(self.deletes_oid, self.snapshot);
				self.deleted_all = true;
			}
			let mut del = self.deleted.partition_point(|&d| d < first);
			self.group_rows(&cols, first, &ranges, &mut del, &active);
		}
		let mine = self.shared.is_null()
			|| (*self.shared).delta_claimed.compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst).is_ok();
		if mine && self.delta_oid != pg_sys::InvalidOid && !self.none {
			self.aggregate_delta(&mut vals);
		}
	}

	unsafe fn aggregate_delta(&mut self, vals: &mut [Val]) {
		let Some(mut delta) = read::DeltaScan::begin(self.rel, self.delta_oid, self.snapshot, &self.keys) else {
			return;
		};
		let slot = delta.slot;
		let desc = (*slot).tts_tupleDescriptor;
		let seek_atts: Vec<usize> = match (&self.order, self.seeks.is_empty()) {
			(Some(o), false) => o.atts.clone(),
			_ => Vec::new(),
		};
		let last = self.inputs.iter().chain(&seek_atts).max().map_or(0, |&a| a + 1);
		while delta.next() {
			pg_sys::slot_getsomeattrs(slot, last as i32);
			// the clauses the seek answers for the column store, answered by it for a late row too
			if let Some(o) = self.order.as_ref().filter(|_| !self.seeks.is_empty()) {
				let key = |i: usize| {
					let a = o.atts[i];
					(!*(*slot).tts_isnull.add(a)).then(|| *(*slot).tts_values.add(a))
				};
				if !self.seeks.iter_mut().any(|s| !s.below(&key) && !s.above(&key)) {
					continue;
				}
			}
			for (i, &att) in self.inputs.clone().iter().enumerate() {
				vals[i] = self.names.slot_val(slot, desc, att);
			}
			self.add(vals);
		}
		delta.end();
	}

	/// Output row `g` into the scan slot.
	unsafe fn emit(&mut self, g: usize, slot: *mut pg_sys::TupleTableSlot) {
		pg_sys::ExecClearTuple(slot);
		pg_sys::MemoryContextReset(self.row_cxt);
		let old = pg_sys::MemoryContextSwitchTo(self.row_cxt);
		let key = self.group_keys[g];
		let naggs = self.spec.aggs.len();
		for (o, out) in self.spec.outputs.clone().iter().enumerate() {
			let (d, isnull) = match *out {
				Output::Key(i) => {
					if key.nulls & (1 << i) != 0 {
						(pg_sys::Datum::from(0usize), true)
					} else {
						let w = key.words[i];
						match self.spec.keys[i] {
							KeyExpr::Col { how: KeyHow::Bytes { len }, .. } => {
								let b = &self.names.strings[w as usize];
								(types::datum_of_bytes(b, len, false, &mut self.mem), false)
							}
							_ => (pg_sys::Datum::from(w as usize), false),
						}
					}
				}
				Output::Agg(a) => {
					let c = self.cells[g * naggs + a];
					match self.spec.aggs[a] {
						Agg::CountStar | Agg::Count(_) => (pg_sys::Datum::from(c.n as usize), false),
						Agg::SumInt(_) | Agg::MinInt(_) | Agg::MaxInt(_) => (pg_sys::Datum::from(c.i as usize), !c.has),
						Agg::SumFloat(_, f4) | Agg::MinFloat(_, f4) | Agg::MaxFloat(_, f4) => {
							if !c.has {
								(pg_sys::Datum::from(0usize), true)
							} else if f4 {
								((c.x as f32).into_datum().unwrap(), false)
							} else {
								(c.x.into_datum().unwrap(), false)
							}
						}
						Agg::AvgInt(_) => (vec![c.n, c.i].into_datum().unwrap(), false),
						Agg::AvgFloat(..) => (vec![c.n as f64, c.x, c.y].into_datum().unwrap(), false),
						Agg::First(_, _, typ) | Agg::Last(_, _, typ) => (self.point_state(c, typ), false),
					}
				}
			};
			*(*slot).tts_values.add(o) = d;
			*(*slot).tts_isnull.add(o) = isnull;
		}
		pg_sys::MemoryContextSwitchTo(old);
		pg_sys::ExecStoreVirtualTuple(slot);
	}
}

impl State {
	/// A first/last state as `point.rs` serialises it: the value's type, whether a row was
	/// held and whether its value is NULL, the time, then the value through its type's binary
	/// send function.
	unsafe fn point_state(&mut self, c: Cell, typ: pg_sys::Oid) -> pg_sys::Datum {
		let mut bytes = Vec::with_capacity(32);
		bytes.extend_from_slice(&typ.to_u32().to_le_bytes());
		bytes.push(c.has as u8);
		bytes.push(c.vnull as u8);
		bytes.extend_from_slice(&(if c.has { c.i } else { 0 }).to_le_bytes());
		if c.has && !c.vnull {
			let value = match typ {
				pg_sys::FLOAT8OID => f64::from_bits(c.w).into_datum().unwrap(),
				pg_sys::FLOAT4OID => (f64::from_bits(c.w) as f32).into_datum().unwrap(),
				pg_sys::TEXTOID | pg_sys::VARCHAROID => types::datum_of_bytes(&self.names.strings[c.w as usize], -1, false, &mut self.mem),
				pg_sys::UUIDOID => types::datum_of_bytes(&self.names.strings[c.w as usize], 16, false, &mut self.mem),
				_ => pg_sys::Datum::from(c.w as usize),
			};
			let (mut send, mut varlena) = (pg_sys::InvalidOid, false);
			pg_sys::getTypeBinaryOutputInfo(typ, &mut send, &mut varlena);
			let sent = pg_sys::OidSendFunctionCall(send, value);
			bytes.extend_from_slice(pgrx::varlena::varlena_to_byte_slice(sent.cast()));
		}
		pg_sys::Datum::from(pgrx::rust_byte_slice_to_bytea(&bytes).into_pg())
	}
}

/// `date_bin(stride, t, origin)` on microseconds, as Postgres computes it.
fn date_bin(t: i64, stride: i64, origin: i64) -> i64 {
	if t == i64::MAX || t == i64::MIN {
		return t;
	}
	let Some(diff) = t.checked_sub(origin) else {
		ereport!(PgLogLevel::ERROR, PgSqlErrorCode::ERRCODE_DATETIME_FIELD_OVERFLOW, "interval out of range");
		unreachable!()
	};
	let mut delta = diff - diff % stride;
	if diff < 0 && diff % stride != 0 {
		delta -= stride;
	}
	match origin.checked_add(delta) {
		Some(r) => r,
		None => {
			ereport!(PgLogLevel::ERROR, PgSqlErrorCode::ERRCODE_DATETIME_FIELD_OVERFLOW, "timestamp out of range");
			unreachable!()
		}
	}
}

#[pg_guard]
unsafe extern "C-unwind" fn create_state(cscan: *mut pg_sys::CustomScan) -> *mut pg_sys::Node {
	let n = pg_sys::palloc0(std::mem::size_of::<Node>()) as *mut Node;
	(*n).css.ss.ps.type_ = pg_sys::NodeTag::T_CustomScanState;
	(*n).css.flags = (*cscan).flags;
	(*n).css.methods = &raw const EXEC_METHODS;
	n as *mut pg_sys::Node
}

#[pg_guard]
unsafe extern "C-unwind" fn begin(node: *mut pg_sys::CustomScanState, estate: *mut pg_sys::EState, _eflags: i32) {
	let cs = (*node).ss.ps.plan as *mut pg_sys::CustomScan;
	let Some(spec) = from_private((*cs).custom_private) else {
		pgrx::error!("a SnoutTime Columnar Aggregate plan could not be read");
	};
	let rel = pg_sys::table_open(spec.relid, pg_sys::AccessShareLock as i32);
	super::build::flush(rel);
	let (delta_oid, deletes_oid) = read::side_tables((*rel).rd_id);
	let snapshot = (*estate).es_snapshot;
	let mut inputs: Vec<usize> = spec.keys.iter().map(KeyExpr::att).chain(spec.aggs.iter().flat_map(Agg::atts)).collect();
	inputs.extend(spec.filters.iter().map(|k| k.att));
	inputs.extend(spec.nulltests.iter().map(|t| t.0));
	inputs.sort_unstable();
	inputs.dedup();
	let read: Vec<usize> = spec.keys.iter().map(KeyExpr::att).chain(spec.aggs.iter().flat_map(Agg::atts)).chain(spec.nulltests.iter().map(|t| t.0)).collect();
	let filter_only: Vec<usize> = inputs.iter().copied().filter(|a| !read.contains(a)).collect();
	let natts = (*(*rel).rd_att).natts as usize;
	let mut pos = vec![usize::MAX; natts.max(inputs.iter().max().map_or(0, |&a| a + 1))];
	for (i, &a) in inputs.iter().enumerate() {
		pos[a] = i;
	}
	let exprs: Vec<*mut pg_sys::ExprState> = PgList::<pg_sys::Expr>::from_pg((*cs).custom_exprs)
		.iter_ptr()
		.map(|e| pg_sys::ExecInitExpr(e, &raw mut (*node).ss.ps))
		.collect();
	let store = Rc::new(Store::open(rel));
	let order = if spec.seek.is_empty() { None } else { Order::of(&store) };
	if !spec.seek.is_empty() && order.is_none() {
		pgrx::error!("a SnoutTime Columnar Aggregate plan seeks a column store that is no longer sorted as it was planned");
	}
	let seek_terms = spec.seek.clone();
	let state = State {
		spec,
		store: Some(store),
		rel,
		inputs,
		filter_only,
		pos,
		exprs,
		econtext: (*node).ss.ps.ps_ExprContext,
		keys: Vec::new(),
		none: false,
		order,
		seek_terms,
		seeks: Vec::new(),
		candidates: Vec::new(),
		seek_cxt: pg_sys::AllocSetContextCreateInternal(
			pg_sys::CurrentMemoryContext,
			c"snouttime columnar aggregate seek".as_ptr(),
			0,
			8 * 1024,
			8 * 1024 * 1024,
		),
		deleted: Vec::new(),
		deleted_all: false,
		deletes_oid,
		delta_oid,
		snapshot,
		shared: std::ptr::null_mut(),
		next_group: 0,
		groups: FxMap::default(),
		last: None,
		spans: [(1, 0); MAX_KEYS],
		group_keys: Vec::new(),
		cells: Vec::new(),
		names: Interner::default(),
		done: false,
		emitted: 0,
		row_cxt: pg_sys::AllocSetContextCreateInternal(
			pg_sys::CurrentMemoryContext,
			c"snouttime columnar aggregate row".as_ptr(),
			0,
			8 * 1024,
			8 * 1024 * 1024,
		),
		mem: types::Mem::new(),
		read: 0,
		skipped: 0,
	};
	let state = PgMemoryContexts::CurrentMemoryContext.leak_and_drop_on_delete(state);
	(*(node as *mut Node)).state = state;
}

#[pg_guard]
unsafe extern "C-unwind" fn access(ss: *mut pg_sys::ScanState) -> *mut pg_sys::TupleTableSlot {
	let s = &mut *(*(ss as *mut Node)).state;
	let slot = (*ss).ss_ScanTupleSlot;
	if !s.done {
		// the keys' values, only now: a parameter has none at the start (see scan.rs)
		s.resolve();
		s.aggregate();
		s.done = true;
	}
	if s.emitted < s.group_keys.len() {
		let g = s.emitted;
		s.emitted += 1;
		s.emit(g, slot);
	} else {
		pg_sys::ExecClearTuple(slot);
	}
	slot
}

#[pg_guard]
unsafe extern "C-unwind" fn recheck(_ss: *mut pg_sys::ScanState, _slot: *mut pg_sys::TupleTableSlot) -> bool {
	true
}

#[pg_guard]
unsafe extern "C-unwind" fn exec(node: *mut pg_sys::CustomScanState) -> *mut pg_sys::TupleTableSlot {
	pg_sys::ExecScan(&mut (*node).ss, Some(access), Some(recheck))
}

#[pg_guard]
unsafe extern "C-unwind" fn end(node: *mut pg_sys::CustomScanState) {
	let s = &mut *(*(node as *mut Node)).state;
	s.seeks.clear();
	s.store = None;
	pg_sys::table_close(s.rel, pg_sys::NoLock as i32);
	pg_sys::MemoryContextDelete(s.row_cxt);
	pg_sys::MemoryContextDelete(s.seek_cxt);
}

#[pg_guard]
unsafe extern "C-unwind" fn rescan(node: *mut pg_sys::CustomScanState) {
	let s = &mut *(*(node as *mut Node)).state;
	s.reset();
	pg_sys::ExecScanReScan(&mut (*node).ss);
}

#[pg_guard]
unsafe extern "C-unwind" fn estimate_dsm(_node: *mut pg_sys::CustomScanState, _pcxt: *mut pg_sys::ParallelContext) -> pg_sys::Size {
	std::mem::size_of::<Shared>()
}

#[pg_guard]
unsafe extern "C-unwind" fn initialize_dsm(node: *mut pg_sys::CustomScanState, _pcxt: *mut pg_sys::ParallelContext, coordinate: *mut std::ffi::c_void) {
	let shared = coordinate as *mut Shared;
	std::ptr::write(shared, Shared { next_group: AtomicU64::new(0), delta_claimed: AtomicU32::new(0) });
	(*(*(node as *mut Node)).state).shared = shared;
}

#[pg_guard]
unsafe extern "C-unwind" fn reinitialize_dsm(node: *mut pg_sys::CustomScanState, _pcxt: *mut pg_sys::ParallelContext, coordinate: *mut std::ffi::c_void) {
	let shared = coordinate as *mut Shared;
	(*shared).next_group.store(0, Ordering::SeqCst);
	(*shared).delta_claimed.store(0, Ordering::SeqCst);
	let s = &mut *(*(node as *mut Node)).state;
	s.reset();
}

#[pg_guard]
unsafe extern "C-unwind" fn initialize_worker(node: *mut pg_sys::CustomScanState, _toc: *mut pg_sys::shm_toc, coordinate: *mut std::ffi::c_void) {
	(*(*(node as *mut Node)).state).shared = coordinate as *mut Shared;
}

#[pg_guard]
unsafe extern "C-unwind" fn explain(node: *mut pg_sys::CustomScanState, _ancestors: *mut pg_sys::List, es: *mut pg_sys::ExplainState) {
	let s = &*(*(node as *mut Node)).state;
	let name = std::ffi::CString::new(super::build::relname(s.rel)).unwrap();
	pg_sys::ExplainPropertyText(c"Relation".as_ptr(), name.as_ptr(), es);
	let keys = std::ffi::CString::new(s.spec.filters.len().to_string()).unwrap();
	pg_sys::ExplainPropertyText(c"Row Group Filters".as_ptr(), keys.as_ptr(), es);
	if !s.spec.seek.is_empty() {
		pg_sys::ExplainPropertyInteger(c"Sort Key Seek".as_ptr(), std::ptr::null(), s.spec.seek.len() as i64, es);
	}
	if (*es).analyze {
		pg_sys::ExplainPropertyInteger(c"Row Groups Read".as_ptr(), std::ptr::null(), s.read as i64, es);
		pg_sys::ExplainPropertyInteger(c"Row Groups Skipped".as_ptr(), std::ptr::null(), s.skipped as i64, es);
		pg_sys::ExplainPropertyInteger(c"Groups".as_ptr(), std::ptr::null(), s.group_keys.len() as i64, es);
	}
}
