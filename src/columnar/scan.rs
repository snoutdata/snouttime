//! Fast scans of sealed partitions: a custom scan node, `SnoutTime Columnar
//! Scan`, that decodes only the columns a query uses and skips whole row groups whose minimum
//! and maximum cannot satisfy its WHERE clause.
//!
//! Postgres does not tell a table access method which columns a query needs; the plain scan
//! (`am.rs`) decodes a column when the executor asks for it, and the executor asks for every
//! column up to the last one it reads. The planner does know: `set_rel_pathlist_hook` offers
//! this node beside the plain paths for every sealed relation, costed by the columns it will
//! decode and the row groups it will read, and the planner chooses. The node reads the same
//! column store, delete log and delta store the plain scan does; the WHERE clause is still
//! evaluated on every row it returns, so a skip is only ever an optimisation, never a filter.
//!
//! A row group is skipped by a comparison of an integer or time column (`<`, `<=`, `=`, `>=`,
//! `>`, and so `BETWEEN`) with a constant, or with an expression that has no columns and
//! nothing volatile in it (`now() - interval '1 day'`, a query parameter), which is evaluated
//! when the scan starts and on every rescan; the comparison is made against the minimum and
//! maximum the column store keeps per row group. Parallel-aware: workers claim row groups from
//! one counter.
//!
//! The node is offered only when it saves something over the plain scan (see `hook`), except
//! as a parallel path.

use std::ffi::CStr;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};
use std::rc::Rc;

use pgrx::pg_sys;
use pgrx::prelude::*;
use pgrx::{PgList, PgMemoryContexts};

use super::format::GroupEntry;
use super::read::{self, Group, Store};
use super::seek::{self, Bound, Order, Seek, Term};

pub static ENABLED: GucSetting<bool> = GucSetting::<bool>::new(true);

const NAME: &CStr = c"SnoutTime Columnar Scan";

static mut PREV_HOOK: pg_sys::set_rel_pathlist_hook_type = None;
static mut PREV_INFO_HOOK: pg_sys::get_relation_info_hook_type = None;

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
		c"snouttime.columnar_custom_scan",
		c"Whether sealed partitions may be read by SnoutTime's own scan",
		c"It decodes only the columns a query uses and skips row groups by their minimum and maximum. Off: the plain scan, which decodes everything.",
		&ENABLED,
		GucContext::Userset,
		GucFlags::default(),
	);
	unsafe {
		PREV_HOOK = pg_sys::set_rel_pathlist_hook;
		pg_sys::set_rel_pathlist_hook = Some(hook);
		PREV_INFO_HOOK = pg_sys::get_relation_info_hook;
		pg_sys::get_relation_info_hook = Some(relation_info);
		pg_sys::RegisterCustomScanMethods(&raw const SCAN_METHODS);
	}
}

// ---------------------------------------------------------------------------------------
// Skip keys: `column op value` on an integer or time column
// ---------------------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) enum Op {
	Lt,
	Le,
	Eq,
	Ge,
	Gt,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct Key {
	/// 0-based attribute index
	pub(super) att: usize,
	pub(super) op: Op,
	pub(super) value: i64,
}

impl Key {
	/// Can a row group with this column's range hold a row the key accepts?
	pub(super) fn admits(&self, min: i64, max: i64) -> bool {
		match self.op {
			Op::Lt => min < self.value,
			Op::Le => min <= self.value,
			Op::Eq => min <= self.value && self.value <= max,
			Op::Ge => max >= self.value,
			Op::Gt => max > self.value,
		}
	}
}

pub(super) fn keeps(keys: &[Key], g: &GroupEntry) -> bool {
	keys.iter().all(|k| match g.minmax.get(k.att) {
		// a group with no minimum for the column holds no non-null value in it, and a
		// comparison with NULL is never true
		Some(None) => false,
		Some(Some(m)) => k.admits(m.min, m.max),
		// a column added after the seal: not stored, cannot judge
		None => true,
	})
}

pub(super) fn int_family(t: pg_sys::Oid) -> bool {
	t == pg_sys::INT2OID || t == pg_sys::INT4OID || t == pg_sys::INT8OID
}

pub(super) fn time_type(t: pg_sys::Oid) -> bool {
	t == pg_sys::DATEOID || t == pg_sys::TIMEOID || t == pg_sys::TIMESTAMPOID || t == pg_sys::TIMESTAMPTZOID
}

/// A value of `typ` as the int64 the column store keeps for it.
pub(super) fn ffi<T>(f: impl FnOnce() -> T) -> T {
	unsafe { pg_sys::ffi::pg_guard_ffi_boundary(f) }
}

pub(super) fn datum_value(d: pg_sys::Datum, typ: pg_sys::Oid) -> i64 {
	let w = d.value() as u64;
	match typ {
		pg_sys::INT2OID => w as i16 as i64,
		pg_sys::INT4OID | pg_sys::DATEOID => w as i32 as i64,
		_ => w as i64,
	}
}

/// What a key compares its column with.
#[derive(Debug, Clone, Copy)]
pub(super) enum Side {
	Const(pg_sys::Datum),
	/// an expression with no Vars and nothing volatile, evaluated when the scan starts and on
	/// every rescan: `ts >= now() - interval '1 day'`, or a query parameter
	Runtime(*mut pg_sys::Node),
}

/// `column op constant-or-expression` on an integer or time column: the column, the operator
/// as it reads with the column on the left, and the other side.
pub(super) unsafe fn key_of(clause: *mut pg_sys::Node, relid: pg_sys::Index) -> Option<(usize, Op, Side, pg_sys::Oid)> {
	if clause.is_null() || (*clause).type_ != pg_sys::NodeTag::T_OpExpr {
		return None;
	}
	let op = clause as *mut pg_sys::OpExpr;
	// only the built-in comparison operators: a user's own "<" may mean anything
	if (*op).opno.to_u32() >= pg_sys::FirstNormalObjectId {
		return None;
	}
	let args = PgList::<pg_sys::Node>::from_pg((*op).args);
	if args.len() != 2 {
		return None;
	}
	let (a, b) = (args.get_ptr(0)?, args.get_ptr(1)?);
	let usable = |n: *mut pg_sys::Node| {
		(*n).type_ != pg_sys::NodeTag::T_Var
			&& !pg_sys::contain_var_clause(n)
			&& !pg_sys::contain_volatile_functions(n)
	};
	let (var, other, commuted) = if (*a).type_ == pg_sys::NodeTag::T_Var && usable(b) {
		(a as *mut pg_sys::Var, b, false)
	} else if (*b).type_ == pg_sys::NodeTag::T_Var && usable(a) {
		(b as *mut pg_sys::Var, a, true)
	} else {
		return None;
	};
	if (*var).varno as pg_sys::Index != relid || (*var).varattno <= 0 || (*var).varlevelsup != 0 {
		return None;
	}
	let (vt, ot) = ((*var).vartype, pg_sys::exprType(other));
	if !((int_family(vt) && int_family(ot)) || (time_type(vt) && vt == ot)) {
		return None;
	}
	let side = if (*other).type_ == pg_sys::NodeTag::T_Const {
		let c = other as *mut pg_sys::Const;
		if (*c).constisnull {
			return None;
		}
		Side::Const((*c).constvalue)
	} else {
		Side::Runtime(other)
	};
	let name = CStr::from_ptr(pg_sys::get_opname((*op).opno)).to_str().ok()?;
	let op = match (name, commuted) {
		("<", false) | (">", true) => Op::Lt,
		("<=", false) | (">=", true) => Op::Le,
		("=", _) => Op::Eq,
		(">=", false) | ("<=", true) => Op::Ge,
		(">", false) | ("<", true) => Op::Gt,
		_ => return None,
	};
	Some(((*var).varattno as usize - 1, op, side, ot))
}

/// `float column op float constant`: not a row-group key (no minimum and maximum are kept for
/// floats), but a row filter the node applies to decoded values before it forms a row, so a
/// row a WHERE clause rejects never reaches the executor (bench q4 rejects 99% of its rows).
#[derive(Debug, Clone, Copy)]
pub(super) struct FloatFilter {
	pub(super) att: usize,
	pub(super) op: Op,
	pub(super) value: f64,
}

impl FloatFilter {
	/// As float8's comparisons order values: NaN above everything and equal to itself. A
	/// float4 widened to double compares exactly as it does in float4.
	pub(super) fn admits(&self, x: f64) -> bool {
		let v = self.value;
		let gt = |a: f64, b: f64| if a.is_nan() { !b.is_nan() } else { !b.is_nan() && a > b };
		let eq = |a: f64, b: f64| if a.is_nan() { b.is_nan() } else { !b.is_nan() && a == b };
		match self.op {
			Op::Lt => gt(v, x),
			Op::Le => !gt(x, v),
			Op::Eq => eq(x, v),
			Op::Ge => !gt(v, x),
			Op::Gt => gt(x, v),
		}
	}
}

pub(super) unsafe fn float_filter_of(clause: *mut pg_sys::Node, relid: pg_sys::Index) -> Option<FloatFilter> {
	if clause.is_null() || (*clause).type_ != pg_sys::NodeTag::T_OpExpr {
		return None;
	}
	let op = clause as *mut pg_sys::OpExpr;
	if (*op).opno.to_u32() >= pg_sys::FirstNormalObjectId {
		return None;
	}
	let args = PgList::<pg_sys::Node>::from_pg((*op).args);
	if args.len() != 2 {
		return None;
	}
	let (a, b) = (args.get_ptr(0)?, args.get_ptr(1)?);
	let (var, cst, commuted) = if (*a).type_ == pg_sys::NodeTag::T_Var && (*b).type_ == pg_sys::NodeTag::T_Const {
		(a as *mut pg_sys::Var, b as *mut pg_sys::Const, false)
	} else if (*b).type_ == pg_sys::NodeTag::T_Var && (*a).type_ == pg_sys::NodeTag::T_Const {
		(b as *mut pg_sys::Var, a as *mut pg_sys::Const, true)
	} else {
		return None;
	};
	if (*var).varno as pg_sys::Index != relid || (*var).varattno <= 0 || (*var).varlevelsup != 0 || (*cst).constisnull {
		return None;
	}
	let is_float = |t: pg_sys::Oid| t == pg_sys::FLOAT4OID || t == pg_sys::FLOAT8OID;
	if !is_float((*var).vartype) || !is_float((*cst).consttype) {
		return None;
	}
	let d = (*cst).constvalue;
	let value = if (*cst).consttype == pg_sys::FLOAT4OID {
		f32::from_bits(d.value() as u32) as f64
	} else {
		f64::from_bits(d.value() as u64)
	};
	let name = CStr::from_ptr(pg_sys::get_opname((*op).opno)).to_str().ok()?;
	let op = match (name, commuted) {
		("<", false) | (">", true) => Op::Lt,
		("<=", false) | (">=", true) => Op::Le,
		("=", _) => Op::Eq,
		(">=", false) | ("<=", true) => Op::Ge,
		(">", false) | ("<", true) => Op::Gt,
		_ => return None,
	};
	Some(FloatFilter { att: (*var).varattno as usize - 1, op, value })
}

unsafe fn float_filters(clauses: *mut pg_sys::List, relid: pg_sys::Index) -> Vec<FloatFilter> {
	PgList::<pg_sys::RestrictInfo>::from_pg(clauses)
		.iter_ptr()
		.filter_map(|r| float_filter_of((*r).clause as *mut pg_sys::Node, relid))
		.collect()
}

// ---------------------------------------------------------------------------------------
// Planning
// ---------------------------------------------------------------------------------------

/// A key as the plan carries it: its value, or which of the plan's `custom_exprs` computes it.
#[derive(Debug, Clone, Copy)]
pub(super) struct PlanKey {
	pub(super) att: usize,
	pub(super) op: Op,
	pub(super) value: PlanValue,
}

#[derive(Debug, Clone, Copy)]
pub(super) enum PlanValue {
	Known(i64),
	Expr { index: usize, typ: pg_sys::Oid },
}

/// Which way a scan returns rows: in no particular order (row groups as they come, parallel
/// workers each taking some), or in the column store's sort order, forwards or backwards.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) enum Dir {
	Unordered,
	Forward,
	Backward,
}

/// What a path and its plan carry: the columns to decode, the row-group keys, the float
/// filters, the direction, and the seek's terms (seek.rs).
pub(super) struct Private {
	pub(super) needed: Option<Vec<usize>>,
	pub(super) keys: Vec<PlanKey>,
	pub(super) floats: Vec<FloatFilter>,
	pub(super) dir: Dir,
	pub(super) seek: Vec<Term>,
}

/// As String nodes (plans are copied and printed, so custom_private must be ordinary nodes):
/// the columns to decode, then one node per key, filter, direction and seek term.
unsafe fn to_private(p: &Private) -> *mut pg_sys::List {
	let mut list = PgList::<pg_sys::Node>::new();
	let mut push = |s: String| {
		let c = std::ffi::CString::new(s).unwrap();
		list.push(pg_sys::makeString(pg_sys::pstrdup(c.as_ptr())) as *mut pg_sys::Node);
	};
	push(match &p.needed {
		None => "all".into(),
		Some(v) => v.iter().map(|a| a.to_string()).collect::<Vec<_>>().join(","),
	});
	for k in &p.keys {
		push(match k.value {
			PlanValue::Known(v) => format!("{} {:?} K {}", k.att, k.op, v),
			PlanValue::Expr { index, typ } => format!("{} {:?} R {} {}", k.att, k.op, index, typ.to_u32()),
		});
	}
	for f in &p.floats {
		push(format!("{} {:?} F {}", f.att, f.op, f.value.to_bits()));
	}
	match p.dir {
		Dir::Unordered => {}
		Dir::Forward => push("O F".into()),
		Dir::Backward => push("O B".into()),
	}
	for t in &p.seek {
		push(format!("S {}", seek::term_text(t)));
	}
	list.into_pg()
}

unsafe fn from_private(list: *mut pg_sys::List) -> Private {
	let items: Vec<String> = PgList::<pg_sys::String>::from_pg(list)
		.iter_ptr()
		.map(|s| CStr::from_ptr((*s).sval).to_string_lossy().into_owned())
		.collect();
	let needed = match items.first().map(String::as_str) {
		Some("all") | None => None,
		Some("") => Some(Vec::new()),
		Some(s) => Some(s.split(',').filter_map(|x| x.parse().ok()).collect()),
	};
	let mut p = Private { needed, keys: Vec::new(), floats: Vec::new(), dir: Dir::Unordered, seek: Vec::new() };
	for s in items.iter().skip(1) {
		let mut it = s.split(' ');
		let Some(first) = it.next() else { continue };
		match first {
			"O" => {
				p.dir = if it.next() == Some("B") { Dir::Backward } else { Dir::Forward };
				continue;
			}
			"S" => {
				if let Some(t) = seek::parse_term(&s[2..]) {
					p.seek.push(t);
				}
				continue;
			}
			_ => {}
		}
		let parse = |it: &mut std::str::Split<'_, char>, att: usize, p: &mut Private| -> Option<()> {
			let op = match it.next()? {
				"Lt" => Op::Lt,
				"Le" => Op::Le,
				"Eq" => Op::Eq,
				"Ge" => Op::Ge,
				"Gt" => Op::Gt,
				_ => return None,
			};
			let value = match it.next()? {
				"F" => {
					p.floats.push(FloatFilter { att, op, value: f64::from_bits(it.next()?.parse().ok()?) });
					return Some(());
				}
				"K" => PlanValue::Known(it.next()?.parse().ok()?),
				"R" => PlanValue::Expr { index: it.next()?.parse().ok()?, typ: pg_sys::Oid::from(it.next()?.parse::<u32>().ok()?) },
				_ => return None,
			};
			p.keys.push(PlanKey { att, op, value });
			Some(())
		};
		if let Ok(att) = first.parse::<usize>() {
			parse(&mut it, att, &mut p);
		}
	}
	p
}

/// The keys of a relation's restriction clauses, and the expressions the runtime ones need, in
/// the order their `index` names them.
pub(super) unsafe fn plan_keys(clauses: *mut pg_sys::List, relid: pg_sys::Index) -> (Vec<PlanKey>, Vec<*mut pg_sys::Node>) {
	let mut keys = Vec::new();
	let mut exprs = Vec::new();
	for rinfo in PgList::<pg_sys::RestrictInfo>::from_pg(clauses).iter_ptr() {
		if let Some((att, op, side, typ)) = key_of((*rinfo).clause as *mut pg_sys::Node, relid) {
			let value = match side {
				Side::Const(d) => PlanValue::Known(datum_value(d, typ)),
				Side::Runtime(e) => {
					exprs.push(e);
					PlanValue::Expr { index: exprs.len() - 1, typ }
				}
			};
			keys.push(PlanKey { att, op, value });
		}
	}
	(keys, exprs)
}

/// For each key of `clauses`, in `plan_keys` order, the value the planner can fold a runtime
/// key's expression to (a Const key gives None here, its value is already known).
pub(super) unsafe fn runtime_estimates(root: *mut pg_sys::PlannerInfo, clauses: *mut pg_sys::List, relid: pg_sys::Index) -> Vec<Option<i64>> {
	let mut out = Vec::new();
	for rinfo in PgList::<pg_sys::RestrictInfo>::from_pg(clauses).iter_ptr() {
		if let Some((_, _, side, typ)) = key_of((*rinfo).clause as *mut pg_sys::Node, relid) {
			out.push(match side {
				Side::Const(_) => None,
				Side::Runtime(e) => {
					let folded = pg_sys::estimate_expression_value(root, e);
					if !folded.is_null() && (*folded).type_ == pg_sys::NodeTag::T_Const && !(*(folded as *mut pg_sys::Const)).constisnull {
						Some(datum_value((*(folded as *mut pg_sys::Const)).constvalue, typ))
					} else {
						None
					}
				}
			});
		}
	}
	out
}

/// A sealed relation's non-unique indexes that hold only its delta store's rows are taken out of
/// what the planner knows: an index scan on one would miss every sealed row, and
/// so would anything else the planner reads an index for (the actual minimum or maximum of a
/// column, when it estimates a range). Unique indexes, and exclusion constraints', stay.
#[pg_guard]
unsafe extern "C-unwind" fn relation_info(
	root: *mut pg_sys::PlannerInfo,
	relid: pg_sys::Oid,
	inhparent: bool,
	rel: *mut pg_sys::RelOptInfo,
) {
	if let Some(prev) = PREV_INFO_HOOK {
		prev(root, relid, inhparent, rel);
	}
	if inhparent || (*rel).indexlist.is_null() {
		return;
	}
	let r = pg_sys::RelationIdGetRelation(relid);
	if r.is_null() {
		return;
	}
	if (*r).rd_tableam == super::am::routine() && super::am::late_indexes(r) {
		let mut kept = PgList::<pg_sys::IndexOptInfo>::new();
		for info in PgList::<pg_sys::IndexOptInfo>::from_pg((*rel).indexlist).iter_ptr() {
			let whole = (*info).unique || {
				let ir = pg_sys::RelationIdGetRelation((*info).indexoid);
				let exclusion = !ir.is_null() && (*(*ir).rd_index).indisexclusion;
				if !ir.is_null() {
					pg_sys::RelationClose(ir);
				}
				exclusion
			};
			if whole {
				kept.push(info);
			}
		}
		(*rel).indexlist = kept.into_pg();
	}
	pg_sys::RelationClose(r);
}

#[pg_guard]
unsafe extern "C-unwind" fn hook(root: *mut pg_sys::PlannerInfo, rel: *mut pg_sys::RelOptInfo, rti: pg_sys::Index, rte: *mut pg_sys::RangeTblEntry) {
	if let Some(prev) = PREV_HOOK {
		prev(root, rel, rti, rte);
	}
	if !ENABLED.get()
		|| (*rte).rtekind != pg_sys::RTEKind::RTE_RELATION
		|| (*rte).relkind as u8 != pg_sys::RELKIND_RELATION
		|| !(*rel).lateral_relids.is_null()
		// TABLESAMPLE is a different scan; this node would return every row
		|| !(*rte).tablesample.is_null()
		|| ((*rel).reloptkind != pg_sys::RelOptKind::RELOPT_BASEREL
			&& (*rel).reloptkind != pg_sys::RelOptKind::RELOPT_OTHER_MEMBER_REL)
	{
		return;
	}
	let r = pg_sys::RelationIdGetRelation((*rte).relid);
	if r.is_null() {
		return;
	}
	if (*r).rd_tableam != super::am::routine() {
		pg_sys::RelationClose(r);
		return;
	}
	let store = Store::open(r);
	let ncols = (*(*r).rd_att).natts as usize;
	let order = Order::of(&store);
	pg_sys::RelationClose(r);

	// the columns the query reads: its target list and its restriction clauses
	let mut attrs: *mut pg_sys::Bitmapset = std::ptr::null_mut();
	pg_sys::pull_varattnos((*(*rel).reltarget).exprs as *mut pg_sys::Node, (*rel).relid, &mut attrs);
	let quals = PgList::<pg_sys::RestrictInfo>::from_pg((*rel).baserestrictinfo);
	for rinfo in quals.iter_ptr() {
		pg_sys::pull_varattnos((*rinfo).clause as *mut pg_sys::Node, (*rel).relid, &mut attrs);
	}
	// for the cost, a runtime key's value is what the planner can fold it to now; one it
	// cannot is assumed to skip nothing
	// A key it cannot fold (a generic plan's parameter) is costed at Postgres's own default
	// selectivities, as a fraction of the row groups kept.
	let mut unknown_frac = 1.0;
	let keys: Vec<Key> = plan_keys((*rel).baserestrictinfo, (*rel).relid)
		.0
		.iter()
		.zip(runtime_estimates(root, (*rel).baserestrictinfo, (*rel).relid))
		.filter_map(|(k, est)| {
			let value = match (k.value, est) {
				(PlanValue::Known(v), _) => v,
				(PlanValue::Expr { .. }, Some(v)) => v,
				(PlanValue::Expr { .. }, None) => {
					unknown_frac *= if k.op == Op::Eq { 0.005 } else { 1.0 / 3.0 };
					return None;
				}
			};
			Some(Key { att: k.att, op: k.op, value })
		})
		.collect();
	let mut needed: Option<Vec<usize>> = Some(Vec::new());
	let mut x = -1;
	loop {
		x = pg_sys::bms_next_member(attrs, x);
		if x < 0 {
			break;
		}
		let attno = x + pg_sys::FirstLowInvalidHeapAttributeNumber;
		if attno == 0 {
			needed = None; // a whole-row reference
			break;
		}
		if attno > 0 {
			if let Some(v) = needed.as_mut() {
				v.push(attno as usize - 1);
			}
		}
	}

	// the seek (seek.rs): the run of row groups the sort key allows, found by binary search;
	// a value the planner cannot know yet is costed like an unknown key above
	let range = match &order {
		Some(o) => {
			let (terms, exprs) = seek::terms(o, (*rel).baserestrictinfo, (*rel).relid);
			let values: Vec<Option<pg_sys::Datum>> = exprs
				.iter()
				.map(|&e| {
					let f = pg_sys::estimate_expression_value(root, e);
					let known = !f.is_null() && (*f).type_ == pg_sys::NodeTag::T_Const && !(*(f as *mut pg_sys::Const)).constisnull;
					known.then(|| (*(f as *mut pg_sys::Const)).constvalue)
				})
				.collect();
			for t in &terms {
				// an integer or time key's unknown value is already counted above
				if values[t.expr].is_none() && matches!(t.bound, Bound::Eq | Bound::In) && !int_family(o.types[t.pos]) && !time_type(o.types[t.pos]) {
					unknown_frac *= 0.005;
				}
			}
			// an IN list: the row groups any of its values allows, which is what is read
			match seek::seeks(o, &terms, &values) {
				None => Vec::new(),
				Some((mut hull, each)) if each.is_empty() => hull.groups(&store).collect(),
				Some((_, each)) => {
					let mut ok = vec![false; store.dir.len()];
					for mut s in each {
						for g in s.groups(&store) {
							ok[g] = true;
						}
					}
					(0..ok.len()).filter(|&g| ok[g]).collect()
				}
			}
		}
		None => (0..store.dir.len()).collect(),
	};
	let kept: Vec<&GroupEntry> = range.into_iter().map(|g| &store.dir[g]).filter(|g| keeps(&keys, g)).collect();
	// Offered only when it saves something. The plain scan's slot decodes a column only when
	// it is asked for, but the executor asks for every column up to the last one it reads,
	// and a custom scan costs more per row than a plain one (it always projects: Postgres
	// never gives a custom scan the physical target list). So it has to skip row groups, or
	// skip enough of the columns below the last one read.
	let skipped_cols = match &needed {
		None => 0,
		Some(v) => v.iter().max().map_or(0, |&last| (0..last).filter(|c| !v.contains(c)).count()),
	};
	// The parallel path is offered regardless: Postgres costs a parallel plain scan as if it
	// read every column's pages, and a parallel plan this path wins is as fast as the plain one
	// would have been (bench q2: 93 ms against 86 forced, 227 serial, 2026-09-23).
	let floats = float_filters((*rel).baserestrictinfo, (*rel).relid);
	let saves = kept.len() < store.dir.len() || unknown_frac < 1.0 || skipped_cols >= WORTH_SKIPPING || !floats.is_empty();

	// cost: the pages of the row groups it reads, in proportion to the columns it decodes
	let groups = store.dir.len().max(1) as f64;
	let kept_frac = unknown_frac * if store.dir.is_empty() { 1.0 } else { kept.len() as f64 / groups };
	let col_frac = match &needed {
		None => 1.0,
		Some(v) => (v.len().max(1) as f64 / ncols.max(1) as f64).clamp(0.05, 1.0),
	};
	let rows_read = unknown_frac * kept.iter().map(|g| g.rows).sum::<u64>() as f64 + ((*rel).tuples - store.rows() as f64).max(0.0);
	let pages = (*rel).pages as f64;
	let mut qual_cost = pg_sys::QualCost::default();
	pg_sys::cost_qual_eval(&mut qual_cost, (*rel).baserestrictinfo, root);
	let cpu = pg_sys::cpu_tuple_cost + qual_cost.per_tuple;
	// a hair under the plain scan when nothing is saved, so that ties go to the node that
	// decodes less
	let total = 0.99 * (pages * kept_frac * col_frac * pg_sys::seq_page_cost + rows_read * cpu) + qual_cost.startup;
	let delta_rows = ((*rel).tuples - store.rows() as f64).max(0.0);
	let private = |dir: Dir| {
		let mut needed = needed.clone();
		// an ordered scan compares every sort column, to merge in the delta store's rows
		if let (Dir::Forward | Dir::Backward, Some(v), Some(o)) = (dir, needed.as_mut(), &order) {
			for &a in &o.atts {
				if !v.contains(&a) {
					v.push(a);
				}
			}
		}
		to_private(&Private { needed, keys: Vec::new(), floats: floats.clone(), dir, seek: Vec::new() })
	};

	let make = |parallel_workers: i32, dir: Dir, pathkeys: *mut pg_sys::List| {
		let cp = pg_sys::palloc0(std::mem::size_of::<pg_sys::CustomPath>()) as *mut pg_sys::CustomPath;
		let p = &mut (*cp).path;
		p.type_ = pg_sys::NodeTag::T_CustomPath;
		p.pathtype = pg_sys::NodeTag::T_CustomScan;
		p.parent = rel;
		p.pathtarget = (*rel).reltarget;
		p.param_info = pg_sys::get_baserel_parampathinfo(root, rel, std::ptr::null_mut());
		p.parallel_aware = parallel_workers > 0;
		p.parallel_safe = (*rel).consider_parallel;
		p.parallel_workers = parallel_workers;
		let divisor = if parallel_workers > 0 {
			let leader = (1.0 - 0.3 * parallel_workers as f64).max(0.0);
			parallel_workers as f64 + leader
		} else {
			1.0
		};
		p.rows = (*rel).rows / divisor;
		p.startup_cost = qual_cost.startup;
		p.total_cost = qual_cost.startup + (total - qual_cost.startup) / divisor;
		p.pathkeys = pathkeys;
		if dir != Dir::Unordered {
			// the delta store's rows are sorted before the first row comes back, and the first
			// row costs one row group
			// only the late rows in the seek's range are merged; all of them are sorted once per
			// execution, however many times the scan is rescanned, so this is an upper bound
			let d = delta_rows * kept_frac;
			let sort = if d > 1.0 { 2.0 * pg_sys::cpu_operator_cost * d * d.log2() } else { 0.0 };
			p.startup_cost += sort + (total - qual_cost.startup) / (kept.len().max(1) as f64);
			p.total_cost += sort;
		}
		(*cp).flags = 0;
		(*cp).custom_private = private(dir);
		(*cp).methods = &raw const PATH_METHODS;
		cp as *mut pg_sys::Path
	};
	if saves {
		pg_sys::add_path(rel, make(0, Dir::Unordered, std::ptr::null_mut()));
	}
	// in the sort order, either way, when a query can use that order (ORDER BY, a merge join,
	// an ordered Append over partitions): what an index scan on the sort key would give
	if let Some(o) = &order {
		for dir in [Dir::Forward, Dir::Backward] {
			let pathkeys = order_pathkeys(root, rel, o, dir == Dir::Backward);
			if !pathkeys.is_null() {
				pg_sys::add_path(rel, make(0, dir, pathkeys));
			}
		}
	}
	if (*rel).consider_parallel {
		let workers = pg_sys::compute_parallel_worker(rel, pages, -1.0, pg_sys::max_parallel_workers_per_gather);
		if workers > 0 {
			pg_sys::add_partial_path(rel, make(workers, Dir::Unordered, std::ptr::null_mut()));
		}
	}
}

/// The pathkeys of the store's order (backwards: every column descending, NULLs first), as far
/// as the query has a use for them; NIL when it has none. A column the query fixes to one value
/// (`host = $1`) is left out, as an index's would be, so `(host, ts)` serves `ORDER BY ts`.
unsafe fn order_pathkeys(root: *mut pg_sys::PlannerInfo, rel: *mut pg_sys::RelOptInfo, o: &Order, backward: bool) -> *mut pg_sys::List {
	let mut out: *mut pg_sys::List = std::ptr::null_mut();
	for i in 0..o.atts.len() {
		let var = pg_sys::makeVar((*rel).relid as i32, o.atts[i] as i16 + 1, o.types[i], o.typmods[i], o.colls[i], 0);
		let tc = pg_sys::lookup_type_cache(o.types[i], (pg_sys::TYPECACHE_LT_OPR | pg_sys::TYPECACHE_GT_OPR) as i32);
		let opno = if backward { (*tc).gt_opr } else { (*tc).lt_opr };
		if opno == pg_sys::InvalidOid {
			break;
		}
		let l = pg_sys::build_expression_pathkey(root, var as *mut pg_sys::Expr, opno, (*rel).relids, false);
		let Some(pk) = PgList::<pg_sys::PathKey>::from_pg(l).get_ptr(0) else {
			break;
		};
		if (*(*pk).pk_eclass).ec_has_const || pg_sys::list_member_ptr(out, pk as *const std::ffi::c_void) {
			continue;
		}
		out = pg_sys::lappend(out, pk as *mut std::ffi::c_void);
	}
	pg_sys::truncate_useless_pathkeys(root, rel, out)
}

#[pg_guard]
unsafe extern "C-unwind" fn plan_path(
	root: *mut pg_sys::PlannerInfo,
	rel: *mut pg_sys::RelOptInfo,
	best_path: *mut pg_sys::CustomPath,
	tlist: *mut pg_sys::List,
	clauses: *mut pg_sys::List,
	_custom_plans: *mut pg_sys::List,
) -> *mut pg_sys::Plan {
	let cs = pg_sys::palloc0(std::mem::size_of::<pg_sys::CustomScan>()) as *mut pg_sys::CustomScan;
	(*cs).scan.plan.type_ = pg_sys::NodeTag::T_CustomScan;
	(*cs).scan.plan.targetlist = tlist;
	(*cs).scan.plan.qual = pg_sys::extract_actual_clauses(clauses, false);
	(*cs).scan.scanrelid = (*rel).relid;
	(*cs).flags = (*best_path).flags;
	let mut private = from_private((*best_path).custom_private);
	let (keys, mut exprs) = plan_keys(clauses, (*rel).relid);
	private.keys = keys;
	// the seek's values follow the keys' in custom_exprs
	let rte = *(*root).simple_rte_array.add((*rel).relid as usize);
	let r = pg_sys::RelationIdGetRelation((*rte).relid);
	if !r.is_null() {
		let store = Store::open(r);
		if let Some(o) = Order::of(&store) {
			let (mut terms, values) = seek::terms(&o, clauses, (*rel).relid);
			for t in terms.iter_mut() {
				t.expr += exprs.len();
			}
			exprs.extend(values);
			private.seek = terms;
		}
		pg_sys::RelationClose(r);
	}
	(*cs).custom_private = to_private(&private);
	let mut list = PgList::<pg_sys::Node>::new();
	for e in exprs {
		list.push(pg_sys::copyObjectImpl(e as *const std::ffi::c_void) as *mut pg_sys::Node);
	}
	(*cs).custom_exprs = list.into_pg();
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

#[derive(PartialEq)]
enum Phase {
	Column,
	Delta,
	Done,
}

/// How many columns below the last one a query reads the custom scan must avoid decoding to be
/// worth its extra cost per row, when it skips no row group. Chosen from one measurement of the
/// two costs (a float column decodes in about 5 ns a row; the custom scan costs 10 to 20 ns a
/// row more than a plain one, 2026-09-23), not measured as a threshold.
const WORTH_SKIPPING: usize = 3;

struct State {
	store: Rc<Store>,
	needed: Option<Vec<bool>>,
	/// the keys as planned, and the evaluators of the runtime ones
	plan_keys: Vec<PlanKey>,
	/// float comparisons, and the current row group's rows that pass them and the keys
	floats: Vec<FloatFilter>,
	mask: Vec<bool>,
	exprs: Vec<*mut pg_sys::ExprState>,
	econtext: *mut pg_sys::ExprContext,
	/// the keys with their values, recomputed on every rescan
	keys: Vec<Key>,
	/// a runtime key's value was NULL: no column-store row can satisfy it
	none: bool,
	/// the keys' and the seek's values are computed on the first fetch after a start or a
	/// rescan, never at the start: a LATERAL reference's parameter has no value until the
	/// nested loop above rescans this node, and EXPLAIN starts a plan it never runs (a text
	/// parameter read before it was set crashed the server, 2026-09-23)
	resolved: bool,
	/// the column store's order, the seek's terms, and the seek with this scan's values
	order: Option<Order>,
	seek_terms: Vec<Term>,
	seek: Option<Seek>,
	/// an IN list's seeks, one per value (`seek::seeks`), and the row groups any of them allows;
	/// both empty without an IN list. `seek` is then their hull, which the order and the late
	/// rows are merged by
	in_seeks: Vec<Seek>,
	group_ok: Vec<bool>,
	/// where this resolve's seeks and an IN list's values live: reset by the next resolve
	seek_cxt: pg_sys::MemoryContext,
	dir: Dir,
	/// the row groups the seek allows, and the rows of the current one
	groups: std::ops::Range<usize>,
	/// row groups already loaded, most recent first, kept across rescans within work_mem: a
	/// nested loop's inner scan (the LATERAL "last reading before") seeks into the same few
	/// groups again and again, and decoding one each time cost ten times the btree it
	/// replaced (bench q5, 2026-09-23). Each entry keeps its decoded size as last measured, and
	/// `cached` their sum, so bounding the cache costs one group's measure per load, not every
	/// group's: summing them all on every load was most of an as-of join over 1,000 hosts,
	/// whose cache holds hundreds of groups (q5 at 100M, 3,706 to 5,670 ms from 0.1.1 to 0.1.3,
	/// found 2026-09-26)
	cache: Vec<(usize, Rc<Group>, Option<Rc<Vec<u64>>>, usize)>,
	cached: usize,
	rows: std::ops::Range<u64>,
	/// the deleted column-store rows `wanted` checks: the current group's, read through the
	/// delete log's index when the scan visits few groups, or the whole log's, read once
	deleted: Rc<Vec<u64>>,
	deleted_all: Option<Rc<Vec<u64>>>,
	/// whether this resolve reads deletes per group (few groups) or the whole log once
	deleted_per_group: bool,
	deletes_oid: pg_sys::Oid,
	cxt: pg_sys::MemoryContext,
	home: pg_sys::MemoryContext,
	group: Option<Rc<Group>>,
	row: u64,
	next_group: usize,
	phase: Phase,
	shared: *mut Shared,
	delta_oid: pg_sys::Oid,
	delta: Option<read::DeltaScan>,
	/// an ordered scan's delta-store rows in the sort order (all of them, or those of one value of
	/// the first sort column); the seek's run of them; the next one; and the column-store row
	/// waiting to be compared with it
	sorted_delta: Option<Rc<Vec<DeltaRow>>>,
	/// every delta-store row, read and sorted once per scan
	delta_all: Option<Rc<Vec<DeltaRow>>>,
	/// per value of the first sort column, its delta-store rows read through the late-rows index
	delta_by_value: Vec<(pg_sys::Datum, Rc<Vec<DeltaRow>>)>,
	/// whether the late-rows index can be used for that (None: not asked yet)
	delta_index: Option<Option<pg_sys::Oid>>,
	/// whether the delta store is small enough to read whole once (None: not asked yet)
	delta_small: Option<bool>,
	delta_range: std::ops::Range<usize>,
	delta_at: usize,
	/// one value's late rows streamed from an index instead (see `DeltaStream`): whether this
	/// resolve streams, the stream, and the index it reads (None: not asked yet)
	streaming: bool,
	stream: Option<DeltaStream>,
	stream_found: Option<Option<(pg_sys::Oid, bool)>>,
	pending: Option<u64>,
	heap_slot: *mut pg_sys::TupleTableSlot,
	snapshot: pg_sys::Snapshot,
	read: u64,
	skipped: u64,
	/// late rows the stream returned, for EXPLAIN ANALYZE
	streamed: u64,
	decoded: usize,
}

struct DeltaRow {
	tuple: pg_sys::HeapTuple,
	key: Vec<Option<pg_sys::Datum>>,
}

/// One value's late rows read from a late-rows index in the scan's direction, one row ahead of
/// the merge, and no further than the scan goes. At 1% late rows, "the last 10 of one host"
/// fetched and sorted all ~1,000 of that host's late rows to return ten. Used when
/// the seek is only an equality on the first sort column and an index follows the rest of the
/// order; kept open across rescans, each moving it to its value.
struct DeltaStream {
	index: pg_sys::Relation,
	scan: pg_sys::IndexScanDesc,
	slot: *mut pg_sys::TupleTableSlot,
	dir: pg_sys::ScanDirection::Type,
	/// the value sought, copied: the index scan keeps pointing at it until the next rescan
	value: pg_sys::Datum,
	value_byval: bool,
	done: bool,
	head: Option<DeltaRow>,
	values: Vec<pg_sys::Datum>,
	nulls: Vec<bool>,
}

impl State {
	unsafe fn end_delta(&mut self) {
		if let Some(mut d) = self.delta.take() {
			d.end();
		}
	}

	unsafe fn end_stream(&mut self) {
		if let Some(mut s) = self.stream.take() {
			if let Some(h) = s.head.take() {
				pg_sys::heap_freetuple(h.tuple);
			}
			pg_sys::index_endscan(s.scan);
			pg_sys::ExecDropSingleTupleTableSlot(s.slot);
			pg_sys::index_close(s.index, pg_sys::AccessShareLock as i32);
			if !s.value_byval {
				pg_sys::pfree(s.value.cast_mut_ptr());
			}
		}
	}

	/// The late-rows index that can stream one value's rows in the store's order: a valid,
	/// non-unique btree on plain columns, whose first key column is the first sort column and
	/// whose next ones are the rest of the order, each with the order's collation, type and
	/// operator family, all ascending with NULLs last or all descending with NULLs first (read
	/// backward then). With the index and whether it runs opposite the order; asked once a scan.
	unsafe fn stream_index(&mut self) -> Option<(pg_sys::Oid, bool)> {
		if let Some(found) = self.stream_found {
			return found;
		}
		const BTREE_AM_OID: u32 = 403;
		const INDOPTION_DESC: i16 = 1;
		const INDOPTION_NULLS_FIRST: i16 = 2;
		let rel = self.store.rel;
		let mut found = None;
		if super::am::late_indexes(rel) {
			let o = self.order.as_ref().unwrap();
			for oid in PgList::<std::ffi::c_void>::from_pg(pg_sys::RelationGetIndexList(rel)).iter_oid() {
				let index = pg_sys::index_open(oid, pg_sys::AccessShareLock as i32);
				let ix = &*(*index).rd_index;
				let nkeys = ix.indnkeyatts as usize;
				let mut ok = (*(*index).rd_rel).relam.to_u32() == BTREE_AM_OID
					&& !ix.indisunique
					&& !ix.indisexclusion
					&& ix.indisvalid
					&& ix.indisready
					&& (*index).rd_indexprs.is_null()
					&& nkeys >= o.atts.len();
				let mut flipped = None;
				if ok {
					let keys = std::slice::from_raw_parts(ix.indkey.values.as_ptr(), nkeys);
					for i in 0..o.atts.len() {
						let same = keys[i] as usize == o.atts[i] + 1
							&& *(*index).rd_opfamily.add(i) == o.opfamilies[i]
							&& *(*index).rd_opcintype.add(i) == o.types[i]
							&& *(*index).rd_indcollation.add(i) == o.colls[i];
						if !same {
							ok = false;
							break;
						}
						// the first column is an equality: its direction does not matter
						if i == 0 {
							continue;
						}
						let opt = *(*index).rd_indoption.add(i);
						let f = match (opt & INDOPTION_DESC != 0, opt & INDOPTION_NULLS_FIRST != 0) {
							(false, false) => false,
							(true, true) => true,
							_ => {
								ok = false;
								break;
							}
						};
						if flipped.is_some_and(|x| x != f) {
							ok = false;
							break;
						}
						flipped = Some(f);
					}
				}
				pg_sys::index_close(index, pg_sys::AccessShareLock as i32);
				if ok {
					found = Some((oid, flipped.unwrap_or(false)));
					break;
				}
			}
		}
		self.stream_found = Some(found);
		found
	}

	/// Points the stream at value `v` of the first sort column, opening it on first use.
	unsafe fn stream_start(&mut self, oid: pg_sys::Oid, flipped: bool, v: pg_sys::Datum) {
		let old = pg_sys::MemoryContextSwitchTo(self.home);
		let backward = (self.dir == Dir::Backward) != flipped;
		let dir = if backward { pg_sys::ScanDirection::BackwardScanDirection } else { pg_sys::ScanDirection::ForwardScanDirection };
		if self.stream.is_none() {
			let rel = self.store.rel;
			let index = pg_sys::index_open(oid, pg_sys::AccessShareLock as i32);
			#[cfg(feature = "pg18")]
			let scan = pg_sys::index_beginscan(rel, index, self.snapshot, std::ptr::null_mut(), 1, 0);
			#[cfg(not(feature = "pg18"))]
			let scan = pg_sys::index_beginscan(rel, index, self.snapshot, 1, 0);
			let slot = pg_sys::table_slot_create(rel, std::ptr::null_mut());
			let natts = (*(*slot).tts_tupleDescriptor).natts as usize;
			self.stream = Some(DeltaStream {
				index,
				scan,
				slot,
				dir,
				value: pg_sys::Datum::from(0usize),
				value_byval: true,
				done: false,
				head: None,
				values: vec![pg_sys::Datum::from(0usize); natts],
				nulls: vec![false; natts],
			});
		}
		let o = self.order.as_ref().unwrap();
		let s = self.stream.as_mut().unwrap();
		if let Some(h) = s.head.take() {
			pg_sys::heap_freetuple(h.tuple);
		}
		if !s.value_byval {
			pg_sys::pfree(s.value.cast_mut_ptr());
		}
		let mut len = 0i16;
		let mut byval = false;
		pg_sys::get_typlenbyval(o.types[0], &mut len, &mut byval);
		s.value = pg_sys::datumCopy(v, byval, len as i32);
		s.value_byval = byval;
		s.dir = dir;
		s.done = false;
		let opfamily = *(*s.index).rd_opfamily;
		let opcintype = *(*s.index).rd_opcintype;
		let collation = *(*s.index).rd_indcollation;
		let opno = pg_sys::get_opfamily_member(opfamily, opcintype, opcintype, 3);
		let mut sk = pg_sys::ScanKeyData::default();
		pg_sys::ScanKeyEntryInitialize(&mut sk, 0, 1, 3, pg_sys::InvalidOid, collation, pg_sys::get_opcode(opno), s.value);
		pg_sys::index_rescan(s.scan, &mut sk, 1, std::ptr::null_mut(), 0);
		pg_sys::MemoryContextSwitchTo(old);
	}

	/// The stream's next row into its head, if it has one. Every row the index returns is the
	/// seek's: the index key is the seek's one equality, with its operator family and collation.
	unsafe fn stream_fill(&mut self) -> bool {
		let o = self.order.as_ref().unwrap();
		let s = self.stream.as_mut().unwrap();
		if s.head.is_some() {
			return true;
		}
		if s.done {
			return false;
		}
		let old = pg_sys::MemoryContextSwitchTo(self.home);
		if !pg_sys::index_getnext_slot(s.scan, s.dir, s.slot) {
			s.done = true;
			pg_sys::MemoryContextSwitchTo(old);
			return false;
		}
		let tuple = pg_sys::ExecCopySlotHeapTuple(s.slot);
		// already a delta TID (the relation's fetch returns them so)
		(*tuple).t_self = (*s.slot).tts_tid;
		pg_sys::heap_deform_tuple(tuple, (*s.slot).tts_tupleDescriptor, s.values.as_mut_ptr(), s.nulls.as_mut_ptr());
		let key = o.atts.iter().map(|&a| (!s.nulls[a]).then(|| s.values[a])).collect();
		pg_sys::MemoryContextSwitchTo(old);
		s.head = Some(DeltaRow { tuple, key });
		true
	}

	/// Loads row group `g` and its rows the seek allows; the direction decides where in them
	/// the scan starts.
	unsafe fn load(&mut self, g: usize) {
		self.group = None;
		// the group being left may have decoded more while it was current; nothing else grows
		self.measure_front();
		let group = match self.cache.iter().position(|x| x.0 == g) {
			Some(i) => {
				let hit = self.cache.remove(i);
				self.cache.insert(0, hit);
				Rc::clone(&self.cache[0].1)
			}
			None => {
				let loaded = Rc::new(Store::load(&self.store, g));
				// columns are decoded when first read, so a seek that returns one row decodes
				// the columns of that row only
				loaded.omit(self.needed.as_deref());
				self.cache.insert(0, (g, Rc::clone(&loaded), None, 0));
				self.read += 1;
				loaded
			}
		};
		self.deleted = self.deleted_for(&group);
		self.rows = if !self.in_seeks.is_empty() {
			// every row any value's seek allows here: one run, since the values' runs follow on in
			// the order; rows between two of them are read and left to the executor's filter
			let o = self.order.as_ref().unwrap();
			let (mut lo, mut hi) = (u64::MAX, 0);
			for s in self.in_seeks.iter_mut() {
				let r = s.rows(o, &self.store, &group);
				if r.start < r.end {
					lo = lo.min(r.start);
					hi = hi.max(r.end);
				}
			}
			if lo < hi { lo..hi } else { group.first_row..group.first_row }
		} else {
			match (&mut self.seek, &self.order) {
				(Some(s), Some(o)) => s.rows(o, &self.store, &group),
				_ => group.first_row..group.first_row + group.rows,
			}
		};
		// the rows the WHERE clause's comparisons reject, dropped before they are rows; the
		// executor still evaluates the whole clause on the rest. Only over the seek's rows, whose
		// pages are all that is decoded. Not in the sort order: such a scan usually stops after a
		// few rows, and the mask costs every row of the range.
		self.mask = if self.dir != Dir::Unordered || (self.floats.is_empty() && self.keys.is_empty()) {
			Vec::new()
		} else {
			let first = group.first_row;
			group.mask(&self.keys, &self.floats, (self.rows.start - first) as usize..(self.rows.end - first) as usize)
		};
		self.row = if self.dir == Dir::Backward { self.rows.end } else { self.rows.start };
		self.group = Some(group);
		self.trim_cache();
	}

	/// Measures the front group's decoded size again, keeping `cached` their sum.
	fn measure_front(&mut self) {
		if let Some(front) = self.cache.first_mut() {
			let now = front.1.decoded_bytes();
			self.cached = self.cached - front.3 + now;
			front.3 = now;
		}
	}

	/// Drops the least recently used groups past work_mem, never the current one.
	fn trim_cache(&mut self) {
		let budget = unsafe { pg_sys::work_mem } as usize * 1024;
		self.measure_front();
		while self.cached > budget && self.cache.len() > 1 {
			let (_, _, _, bytes) = self.cache.pop().unwrap();
			self.cached -= bytes;
		}
	}

	/// The deleted rows of `group` (at the front of the cache), per group or from the whole log.
	unsafe fn deleted_for(&mut self, group: &Group) -> Rc<Vec<u64>> {
		if self.deleted_per_group {
			if let Some(d) = &self.cache[0].2 {
				return Rc::clone(d);
			}
			let old = pg_sys::MemoryContextSwitchTo(self.home);
			let found = read::deleted_in(self.deletes_oid, self.snapshot, group.first_row, group.first_row + group.rows);
			pg_sys::MemoryContextSwitchTo(old);
			if let Some(d) = found {
				let d = Rc::new(d);
				self.cache[0].2 = Some(Rc::clone(&d));
				return d;
			}
			// no index on the log (never, as the extension makes it): the whole log from now on
			self.deleted_per_group = false;
		}
		if self.deleted_all.is_none() {
			let old = pg_sys::MemoryContextSwitchTo(self.home);
			self.deleted_all = Some(Rc::new(read::deleted_rows(self.deletes_oid, self.snapshot)));
			pg_sys::MemoryContextSwitchTo(old);
		}
		Rc::clone(self.deleted_all.as_ref().unwrap())
	}

	/// Is row `n` of the current group one to return (not masked out, not deleted)?
	fn wanted(&self, first_row: u64, n: u64) -> bool {
		(self.mask.is_empty() || self.mask[(n - first_row) as usize]) && self.deleted.binary_search(&n).is_err()
	}

	/// The next column-store row in the scan's direction, its group loaded; None at the end.
	unsafe fn next_column_row(&mut self) -> Option<u64> {
		loop {
			if let Some(g) = &self.group {
				let first = g.first_row;
				if self.dir == Dir::Backward {
					while self.row > self.rows.start {
						self.row -= 1;
						if self.wanted(first, self.row) {
							return Some(self.row);
						}
					}
				} else {
					while self.row < self.rows.end {
						let n = self.row;
						self.row += 1;
						if self.wanted(first, n) {
							return Some(n);
						}
					}
				}
			}
			self.group = None;
			let g = match self.dir {
				Dir::Backward => {
					if self.next_group <= self.groups.start {
						return None;
					}
					self.next_group -= 1;
					self.next_group
				}
				_ => {
					let i = if self.shared.is_null() {
						let n = self.next_group;
						self.next_group += 1;
						n
					} else {
						self.groups.start + (*self.shared).next_group.fetch_add(1, Ordering::SeqCst) as usize
					};
					if i >= self.groups.end {
						return None;
					}
					i
				}
			};
			if self.none || !keeps(&self.keys, &self.store.dir[g]) || (!self.group_ok.is_empty() && !self.group_ok[g]) {
				self.skipped += 1;
				continue;
			}
			self.load(g);
		}
	}

	unsafe fn next(&mut self, slot: *mut pg_sys::TupleTableSlot, relid: pg_sys::Oid) -> bool {
		if self.dir != Dir::Unordered {
			return self.next_ordered(slot, relid);
		}
		loop {
			match self.phase {
				Phase::Column => {
					match self.next_column_row() {
						Some(n) => {
							self.group.as_ref().unwrap().store_lazy(n, slot, relid);
							return true;
						}
						None => self.phase = Phase::Delta,
					}
				}
				Phase::Delta => {
					if self.delta.is_none() {
						let mine = self.shared.is_null()
							|| (*self.shared).delta_claimed.compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst).is_ok();
						let old = pg_sys::MemoryContextSwitchTo(self.home);
						// a NULL key value: no delta row passes either, and the executor says so
						let keys = if self.none { &[][..] } else { &self.keys[..] };
						self.delta = if mine { read::DeltaScan::begin(self.store.rel, self.delta_oid, self.snapshot, keys) } else { None };
						pg_sys::MemoryContextSwitchTo(old);
						if self.delta.is_none() {
							self.phase = Phase::Done;
							continue;
						}
					}
					let d = self.delta.as_mut().unwrap();
					if d.next() {
						let tid = (*d.slot).tts_tid;
						pg_sys::ExecCopySlot(slot, d.slot);
						(*slot).tts_tid = tid;
						(*slot).tts_tableOid = relid;
						return true;
					}
					self.end_delta();
					self.phase = Phase::Done;
				}
				Phase::Done => return false,
			}
		}
	}

	/// In the sort order: the column store's rows (already in it) merged with the delta
	/// store's, which are read once per scan and sorted, or streamed from an index.
	unsafe fn next_ordered(&mut self, slot: *mut pg_sys::TupleTableSlot, relid: pg_sys::Oid) -> bool {
		if self.phase == Phase::Done {
			return false;
		}
		if self.pending.is_none() {
			self.pending = self.next_column_row();
		}
		let back = self.dir == Dir::Backward;
		let delta = if self.streaming {
			None
		} else if back {
			(self.delta_at > self.delta_range.start).then(|| self.delta_at - 1)
		} else {
			(self.delta_at < self.delta_range.end).then_some(self.delta_at)
		};
		let has_delta = if self.streaming { self.stream_fill() } else { delta.is_some() };
		let take_delta = match (self.pending, has_delta) {
			(None, false) => {
				self.phase = Phase::Done;
				return false;
			}
			(None, true) => true,
			(Some(_), false) => false,
			(Some(n), true) => {
				let o = self.order.as_ref().unwrap();
				let g = self.group.as_ref().unwrap();
				let key = if self.streaming {
					&self.stream.as_ref().unwrap().head.as_ref().unwrap().key
				} else {
					&self.sorted_delta.as_ref().unwrap()[delta.unwrap()].key
				};
				let c = o.cmp_keys(&|i| g.value(o.atts[i], n), &|i| key[i]);
				if back { c == std::cmp::Ordering::Less } else { c == std::cmp::Ordering::Greater }
			}
		};
		if take_delta {
			// a streamed row is the slot's to free; a sorted one lives as long as the scan
			let (t, owned) = if self.streaming {
				self.streamed += 1;
				(self.stream.as_mut().unwrap().head.take().unwrap().tuple, true)
			} else {
				let d = delta.unwrap();
				self.delta_at = if back { d } else { d + 1 };
				(self.sorted_delta.as_ref().unwrap()[d].tuple, false)
			};
			let mut tid = (*t).t_self;
			pg_sys::ExecStoreHeapTuple(t, self.heap_slot, owned);
			pg_sys::ExecCopySlot(slot, self.heap_slot);
			read::to_delta_tid(&mut tid);
			(*slot).tts_tid = tid;
			(*slot).tts_tableOid = relid;
		} else {
			let n = self.pending.take().unwrap();
			self.group.as_ref().unwrap().store_lazy(n, slot, relid);
		}
		true
	}

	/// An ordered scan's delta-store rows, in the order, for this resolve.
	///
	/// When the seek fixes the first sort column (`host = 'host_7'`) and a late-rows index leads
	/// with it, only that value's rows are read, through the index, and kept per value: reading
	/// and sorting the WHOLE delta store for one host was 97 ms of a 0.24 ms lookup at 1% late
	/// rows on a 10M-row partition (measured 2026-09-24). An as-of join that rescans once
	/// per event reads each host's rows once, which is no more than the whole store once. Past
	/// DELTA_VALUES distinct values, and whenever the index cannot answer, the whole store is
	/// read and sorted once per scan, as before.
	unsafe fn pick_delta(&mut self) {
		const DELTA_VALUES: usize = 64;
		// Read whole once, the delta store serves every value, and no lookup is cheaper: a
		// LATERAL probing 100 hosts 20,811 times searched the 64 cached values, comparing text
		// through the collation, on every probe past the 64th host, of an EMPTY delta store (bench
		// q5, 99 to 160 ms from 0.1.1 to 0.1.2, found 2026-09-25). So a small delta store is read
		// whole at once, and once it has been, the per-value lists are not looked at again.
		if self.delta_all.is_none() && self.delta_is_small() {
			self.sort_delta();
		}
		if let Some(all) = &self.delta_all {
			self.sorted_delta = Some(Rc::clone(all));
			return;
		}
		if self.delta_oid != pg_sys::InvalidOid {
			if let Some(v) = self.seek.as_ref().and_then(|s| s.first_eq()) {
				let o = self.order.as_ref().unwrap();
				if let Some((_, rows)) = self.delta_by_value.iter().find(|(d, _)| o.cmp(0, Some(*d), Some(v)) == std::cmp::Ordering::Equal) {
					self.sorted_delta = Some(Rc::clone(rows));
					return;
				}
				if self.delta_by_value.len() < DELTA_VALUES {
					if let Some(rows) = self.delta_for_value(v) {
						let rows = Rc::new(rows);
						let o = self.order.as_ref().unwrap();
						let mut len = 0i16;
						let mut byval = false;
						pg_sys::get_typlenbyval(o.types[0], &mut len, &mut byval);
						let old = pg_sys::MemoryContextSwitchTo(self.home);
						let kept = pg_sys::datumCopy(v, byval, len as i32);
						pg_sys::MemoryContextSwitchTo(old);
						self.delta_by_value.push((kept, Rc::clone(&rows)));
						self.sorted_delta = Some(rows);
						return;
					}
				}
			}
		}
		self.sort_delta();
		self.sorted_delta = self.delta_all.clone();
	}

	/// Is the delta store at most a few pages? Asked once a scan: its pages only grow while the
	/// scan's snapshot is the same.
	unsafe fn delta_is_small(&mut self) -> bool {
		// Chosen, not measured: a few pages read and sorted once cost less than an index lookup
		// per value would.
		const SMALL_PAGES: u32 = 4;
		if let Some(small) = self.delta_small {
			return small;
		}
		let small = self.delta_oid == pg_sys::InvalidOid || {
			let rel = pg_sys::table_open(self.delta_oid, pg_sys::AccessShareLock as i32);
			let pages = pg_sys::RelationGetNumberOfBlocksInFork(rel, pg_sys::ForkNumber::MAIN_FORKNUM);
			pg_sys::table_close(rel, pg_sys::NoLock as i32);
			pages <= SMALL_PAGES
		};
		self.delta_small = Some(small);
		small
	}

	/// The late-rows index whose first column is the store's first sort column, if the store's
	/// non-unique indexes hold only its delta rows and one such btree exists.
	unsafe fn late_index(&mut self) -> Option<pg_sys::Oid> {
		if let Some(found) = self.delta_index {
			return found;
		}
		const BTREE_AM_OID: u32 = 403;
		let rel = self.store.rel;
		let mut found = None;
		if super::am::late_indexes(rel) {
			let first = self.order.as_ref().unwrap().atts[0] as i16 + 1;
			for oid in PgList::<std::ffi::c_void>::from_pg(pg_sys::RelationGetIndexList(rel)).iter_oid() {
				let index = pg_sys::index_open(oid, pg_sys::AccessShareLock as i32);
				let ix = &*(*index).rd_index;
				let ok = (*(*index).rd_rel).relam.to_u32() == BTREE_AM_OID
					&& !ix.indisunique
					&& !ix.indisexclusion
					&& ix.indisvalid
					&& ix.indisready
					&& *ix.indkey.values.as_ptr() == first
					&& (*index).rd_indexprs.is_null();
				pg_sys::index_close(index, pg_sys::AccessShareLock as i32);
				if ok {
					found = Some(oid);
					break;
				}
			}
		}
		self.delta_index = Some(found);
		found
	}

	/// The delta-store rows whose first sort column equals `v`, sorted; None when the index
	/// cannot be asked (none, or its operator class is not for the column's own type).
	unsafe fn delta_for_value(&mut self, v: pg_sys::Datum) -> Option<Vec<DeltaRow>> {
		let oid = self.late_index()?;
		let rel = self.store.rel;
		let o = self.order.as_ref().unwrap();
		let typ = o.types[0];
		let index = pg_sys::index_open(oid, pg_sys::AccessShareLock as i32);
		let opfamily = *(*index).rd_opfamily;
		let opcintype = *(*index).rd_opcintype;
		let collation = *(*index).rd_indcollation;
		let opno = pg_sys::get_opfamily_member(opfamily, opcintype, opcintype, 3);
		if opcintype != typ || opno == pg_sys::InvalidOid {
			pg_sys::index_close(index, pg_sys::AccessShareLock as i32);
			return None;
		}
		let old = pg_sys::MemoryContextSwitchTo(self.home);
		let mut sk = pg_sys::ScanKeyData::default();
		// with the index's collation: a text equality has no meaning without one
		pg_sys::ScanKeyEntryInitialize(&mut sk, 0, 1, 3, pg_sys::InvalidOid, collation, pg_sys::get_opcode(opno), v);
		#[cfg(feature = "pg18")]
		let scan = pg_sys::index_beginscan(rel, index, self.snapshot, std::ptr::null_mut(), 1, 0);
		#[cfg(not(feature = "pg18"))]
		let scan = pg_sys::index_beginscan(rel, index, self.snapshot, 1, 0);
		pg_sys::index_rescan(scan, &mut sk, 1, std::ptr::null_mut(), 0);
		let slot = pg_sys::table_slot_create(rel, std::ptr::null_mut());
		let desc = (*slot).tts_tupleDescriptor;
		let natts = (*desc).natts as usize;
		let mut values = vec![pg_sys::Datum::from(0usize); natts];
		let mut nulls = vec![false; natts];
		let mut rows = Vec::new();
		while pg_sys::index_getnext_slot(scan, pg_sys::ScanDirection::ForwardScanDirection, slot) {
			let tuple = pg_sys::ExecCopySlotHeapTuple(slot);
			// already a delta TID (the relation's fetch returns them so); marking it again is a no-op
			(*tuple).t_self = (*slot).tts_tid;
			pg_sys::heap_deform_tuple(tuple, desc, values.as_mut_ptr(), nulls.as_mut_ptr());
			let key = o.atts.iter().map(|&a| (!nulls[a]).then(|| values[a])).collect();
			rows.push(DeltaRow { tuple, key });
		}
		pg_sys::index_endscan(scan);
		pg_sys::ExecDropSingleTupleTableSlot(slot);
		pg_sys::index_close(index, pg_sys::AccessShareLock as i32);
		pg_sys::MemoryContextSwitchTo(old);
		rows.sort_by(|a: &DeltaRow, b: &DeltaRow| o.cmp_keys(&|i| a.key[i], &|i| b.key[i]));
		Some(rows)
	}

	/// Every delta-store row, sorted by the order, once per scan: the snapshot is the same on
	/// every rescan.
	unsafe fn sort_delta(&mut self) {
		if self.delta_all.is_some() {
			return;
		}
		let mut rows = Vec::new();
		if self.delta_oid != pg_sys::InvalidOid {
			let old = pg_sys::MemoryContextSwitchTo(self.home);
			let rel = pg_sys::table_open(self.delta_oid, pg_sys::AccessShareLock as i32);
			let desc = (*rel).rd_att;
			let natts = (*desc).natts as usize;
			let slot = pg_sys::table_slot_create(rel, std::ptr::null_mut());
			let scan = pg_sys::table_beginscan(rel, self.snapshot, 0, std::ptr::null_mut());
			let o = self.order.as_ref().unwrap();
			let mut values = vec![pg_sys::Datum::from(0usize); natts];
			let mut nulls = vec![false; natts];
			while pg_sys::table_scan_getnextslot(scan, pg_sys::ScanDirection::ForwardScanDirection, slot) {
				let tuple = pg_sys::ExecCopySlotHeapTuple(slot);
				(*tuple).t_self = (*slot).tts_tid;
				pg_sys::heap_deform_tuple(tuple, desc, values.as_mut_ptr(), nulls.as_mut_ptr());
				let key = o.atts.iter().map(|&a| (!nulls[a]).then(|| values[a])).collect();
				rows.push(DeltaRow { tuple, key });
			}
			pg_sys::table_endscan(scan);
			pg_sys::ExecDropSingleTupleTableSlot(slot);
			pg_sys::table_close(rel, pg_sys::NoLock as i32);
			pg_sys::MemoryContextSwitchTo(old);
			rows.sort_by(|a: &DeltaRow, b: &DeltaRow| o.cmp_keys(&|i| a.key[i], &|i| b.key[i]));
		}
		self.delta_all = Some(Rc::new(rows));
	}

	/// Gives every key its value for this scan, evaluating the runtime ones, and seeks.
	unsafe fn resolve(&mut self) {
		let econtext = self.econtext;
		let eval = |st: *mut pg_sys::ExprState| -> Option<pg_sys::Datum> {
			let mut isnull = false;
			let d = ffi(|| {
				let old = pg_sys::MemoryContextSwitchTo((*econtext).ecxt_per_tuple_memory);
				let d = (*st).evalfunc.expect("an initialized expression")(st, econtext, &mut isnull);
				pg_sys::MemoryContextSwitchTo(old);
				d
			});
			(!isnull).then_some(d)
		};
		self.keys.clear();
		self.none = false;
		for k in &self.plan_keys {
			let value = match k.value {
				PlanValue::Known(v) => v,
				PlanValue::Expr { index, typ } => match eval(self.exprs[index]) {
					Some(d) => datum_value(d, typ),
					None => {
						self.none = true;
						continue;
					}
				},
			};
			self.keys.push(Key { att: k.att, op: k.op, value });
		}
		self.seek = None;
		self.in_seeks.clear();
		self.group_ok.clear();
		self.groups = 0..self.store.dir.len();
		if let Some(o) = &self.order {
			if !self.seek_terms.is_empty() {
				pg_sys::MemoryContextReset(self.seek_cxt);
				let mut values = vec![None; self.exprs.len()];
				for t in &self.seek_terms {
					let mut v = eval(self.exprs[t.expr]);
					if t.bound == Bound::In {
						// copied: the executor resets per-tuple memory between rows, and every
						// value's seek points into the array
						v = v.map(|d| {
							let old = pg_sys::MemoryContextSwitchTo(self.seek_cxt);
							let c = pg_sys::datumCopy(d, false, -1);
							pg_sys::MemoryContextSwitchTo(old);
							c
						});
					}
					values[t.expr] = v;
					// a comparison with NULL is never true
					if v.is_none() {
						self.none = true;
					}
				}
				let old = pg_sys::MemoryContextSwitchTo(self.seek_cxt);
				let built = seek::seeks(o, &self.seek_terms, &values);
				pg_sys::MemoryContextSwitchTo(old);
				match built {
					// an IN list with no value: no row matches
					None => self.none = true,
					Some((mut hull, mut each)) => {
						self.groups = hull.groups(&self.store);
						if !each.is_empty() {
							let mut ok = vec![false; self.store.dir.len()];
							for s in each.iter_mut() {
								for g in s.groups(&self.store) {
									ok[g] = true;
								}
							}
							self.groups = match (ok.iter().position(|&b| b), ok.iter().rposition(|&b| b)) {
								(Some(first), Some(last)) => first..last + 1,
								_ => 0..0,
							};
							self.group_ok = ok;
							self.in_seeks = each;
						}
						self.seek = Some(hull);
					}
				}
			}
		}
		// A scan that visits a few row groups reads their deletes through the log's index as it
		// loads them; one that visits many reads the whole log once, which is cheaper than an
		// index scan per group. A parallel scan's groups are shared out, so it reads the whole log.
		const DELETES_PER_GROUP: usize = 16;
		let visits = if self.group_ok.is_empty() { self.groups.len() } else { self.group_ok.iter().filter(|&&b| b).count() };
		self.deleted_per_group = self.shared.is_null() && visits <= DELETES_PER_GROUP;
		self.streaming = false;
		if self.dir != Dir::Unordered && self.delta_oid != pg_sys::InvalidOid {
			if let Some(v) = self.seek.as_ref().and_then(|s| s.first_eq_only()) {
				if let Some((oid, flipped)) = self.stream_index() {
					self.stream_start(oid, flipped, v);
					self.streaming = true;
					self.delta_range = 0..0;
				}
			}
		}
		if self.dir != Dir::Unordered && !self.streaming {
			self.pick_delta();
			let rows = self.sorted_delta.as_ref().unwrap();
			self.delta_range = match (&mut self.seek, &self.order) {
				(Some(s), Some(_)) => {
					let n = rows.len();
					let start = seek_first(n, |d| !s.below(&|i| rows[d].key[i]));
					let end = seek_first(n, |d| s.above(&|i| rows[d].key[i]));
					start..end.max(start)
				}
				_ => 0..rows.len(),
			};
		}
	}

	fn reset(&mut self) {
		self.group = None;
		self.row = 0;
		self.pending = None;
		self.next_group = if self.dir == Dir::Backward { self.groups.end } else { self.groups.start };
		self.delta_at = if self.dir == Dir::Backward { self.delta_range.end } else { self.delta_range.start };
		self.phase = Phase::Column;
	}
}

/// The first of `0..n` for which `p` holds, `p` being false and then true.
fn seek_first(n: usize, mut p: impl FnMut(usize) -> bool) -> usize {
	let (mut lo, mut hi) = (0, n);
	while lo < hi {
		let mid = lo + (hi - lo) / 2;
		if p(mid) {
			hi = mid;
		} else {
			lo = mid + 1;
		}
	}
	lo
}

#[pg_guard]
unsafe extern "C-unwind" fn create_state(cscan: *mut pg_sys::CustomScan) -> *mut pg_sys::Node {
	let n = pg_sys::palloc0(std::mem::size_of::<Node>()) as *mut Node;
	(*n).css.ss.ps.type_ = pg_sys::NodeTag::T_CustomScanState;
	(*n).css.flags = (*cscan).flags;
	(*n).css.methods = &raw const EXEC_METHODS;
	(*n).css.slotOps = super::slot::slot_ops();
	n as *mut pg_sys::Node
}

#[pg_guard]
unsafe extern "C-unwind" fn begin(node: *mut pg_sys::CustomScanState, estate: *mut pg_sys::EState, _eflags: i32) {
	let rel = (*node).ss.ss_currentRelation;
	let cs = (*node).ss.ps.plan as *mut pg_sys::CustomScan;
	let private = from_private((*cs).custom_private);
	let (needed_list, plan_keys, floats) = (private.needed, private.keys, private.floats);
	let exprs: Vec<*mut pg_sys::ExprState> = PgList::<pg_sys::Expr>::from_pg((*cs).custom_exprs)
		.iter_ptr()
		.map(|e| pg_sys::ExecInitExpr(e, &raw mut (*node).ss.ps))
		.collect();
	let natts = (*(*rel).rd_att).natts as usize;
	let needed = needed_list.map(|v| {
		let mut m = vec![false; natts];
		for a in v {
			if a < natts {
				m[a] = true;
			}
		}
		m
	});
	let decoded = needed.as_ref().map_or(natts, |m| m.iter().filter(|&&b| b).count());
	super::build::flush(rel);
	let (delta_oid, deletes_oid) = read::side_tables((*rel).rd_id);
	let snapshot = (*estate).es_snapshot;
	let store = Rc::new(Store::open(rel));
	let order = Order::of(&store);
	// the plan's direction and seek were made for the order the store had then; a store
	// resealed since in another order is read unordered and unsought (the plan is replanned on
	// its next use, since a reseal changes the relation)
	let same = order.is_some() && private.seek.iter().all(|t| order.as_ref().is_some_and(|o| t.pos < o.atts.len()));
	let dir = if same { private.dir } else { Dir::Unordered };
	if !same && private.dir != Dir::Unordered {
		error!("the sealed partition \"{}\" was resealed in another order while a plan that relied on the old one ran; run the query again", super::build::relname(rel));
	}
	let heap_slot = if dir == Dir::Unordered {
		std::ptr::null_mut()
	} else {
		pg_sys::MakeSingleTupleTableSlot((*rel).rd_att, &pg_sys::TTSOpsHeapTuple)
	};
	let state = State {
		store,
		needed,
		plan_keys,
		floats,
		mask: Vec::new(),
		exprs,
		econtext: (*node).ss.ps.ps_ExprContext,
		keys: Vec::new(),
		none: false,
		resolved: false,
		seek_terms: if same { private.seek } else { Vec::new() },
		order,
		seek: None,
		in_seeks: Vec::new(),
		group_ok: Vec::new(),
		seek_cxt: pg_sys::AllocSetContextCreateInternal(
			pg_sys::CurrentMemoryContext,
			c"snouttime columnar seek".as_ptr(),
			0,
			8 * 1024,
			8 * 1024 * 1024,
		),
		dir,
		groups: 0..0,
		cache: Vec::new(),
		cached: 0,
		rows: 0..0,
		deleted: Rc::new(Vec::new()),
		deleted_all: None,
		deleted_per_group: false,
		deletes_oid,
		cxt: pg_sys::AllocSetContextCreateInternal(
			pg_sys::CurrentMemoryContext,
			c"snouttime columnar scan".as_ptr(),
			0,
			8 * 1024,
			8 * 1024 * 1024,
		),
		home: pg_sys::CurrentMemoryContext,
		group: None,
		row: 0,
		next_group: 0,
		phase: Phase::Column,
		shared: std::ptr::null_mut(),
		delta_oid,
		delta: None,
		sorted_delta: None,
		delta_all: None,
		delta_by_value: Vec::new(),
		delta_index: None,
		delta_small: None,
		delta_range: 0..0,
		delta_at: 0,
		streaming: false,
		stream: None,
		stream_found: None,
		pending: None,
		heap_slot,
		snapshot,
		read: 0,
		skipped: 0,
		streamed: 0,
		decoded,
	};
	let state = PgMemoryContexts::CurrentMemoryContext.leak_and_drop_on_delete(state);
	(*(node as *mut Node)).state = state;
}

#[pg_guard]
unsafe extern "C-unwind" fn access(ss: *mut pg_sys::ScanState) -> *mut pg_sys::TupleTableSlot {
	let s = &mut *(*(ss as *mut Node)).state;
	let slot = (*ss).ss_ScanTupleSlot;
	let relid = (*(*ss).ss_currentRelation).rd_id;
	if !s.resolved {
		s.resolve();
		s.reset();
		s.resolved = true;
	}
	if !s.next(slot, relid) {
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
	s.end_delta();
	s.end_stream();
	s.group = None;
	s.cache.clear();
	s.cached = 0;
	if !s.heap_slot.is_null() {
		pg_sys::ExecDropSingleTupleTableSlot(s.heap_slot);
		s.heap_slot = std::ptr::null_mut();
	}
	pg_sys::MemoryContextDelete(s.cxt);
}

#[pg_guard]
unsafe extern "C-unwind" fn rescan(node: *mut pg_sys::CustomScanState) {
	let s = &mut *(*(node as *mut Node)).state;
	s.end_delta();
	// a parameter the keys read may have changed: computed again on the next fetch
	s.resolved = false;
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
	s.end_delta();
	s.resolved = false;
}

#[pg_guard]
unsafe extern "C-unwind" fn initialize_worker(node: *mut pg_sys::CustomScanState, _toc: *mut pg_sys::shm_toc, coordinate: *mut std::ffi::c_void) {
	(*(*(node as *mut Node)).state).shared = coordinate as *mut Shared;
}

#[pg_guard]
unsafe extern "C-unwind" fn explain(node: *mut pg_sys::CustomScanState, _ancestors: *mut pg_sys::List, es: *mut pg_sys::ExplainState) {
	let s = &*(*(node as *mut Node)).state;
	let natts = (*(*(*node).ss.ss_currentRelation).rd_att).natts;
	let cols = std::ffi::CString::new(format!("{} of {}", s.decoded, natts)).unwrap();
	pg_sys::ExplainPropertyText(c"Columns Decoded".as_ptr(), cols.as_ptr(), es);
	let keys = std::ffi::CString::new(s.plan_keys.len().to_string()).unwrap();
	pg_sys::ExplainPropertyText(c"Row Group Filters".as_ptr(), keys.as_ptr(), es);
	if !s.seek_terms.is_empty() {
		let n = std::ffi::CString::new(s.seek_terms.len().to_string()).unwrap();
		pg_sys::ExplainPropertyText(c"Sort Key Seek".as_ptr(), n.as_ptr(), es);
	}
	match s.dir {
		Dir::Forward => pg_sys::ExplainPropertyText(c"Order".as_ptr(), c"sort key".as_ptr(), es),
		Dir::Backward => pg_sys::ExplainPropertyText(c"Order".as_ptr(), c"sort key, backward".as_ptr(), es),
		Dir::Unordered => {}
	}
	if (*es).analyze {
		pg_sys::ExplainPropertyInteger(c"Row Groups Read".as_ptr(), std::ptr::null(), s.read as i64, es);
		pg_sys::ExplainPropertyInteger(c"Row Groups Skipped".as_ptr(), std::ptr::null(), s.skipped as i64, es);
		if s.stream.is_some() {
			pg_sys::ExplainPropertyInteger(c"Late Rows Streamed".as_ptr(), std::ptr::null(), s.streamed as i64, es);
		}
	}
}
