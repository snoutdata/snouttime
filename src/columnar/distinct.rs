//! The last point per key of a sealed table: `SELECT DISTINCT ON (host) * ...
//! ORDER BY host, ts DESC`, the most asked question of a time series, as `SnoutTime Columnar
//! Distinct`.
//!
//! Postgres answers it by reading every row in that order (an index scan per partition, merged)
//! and keeping the first of each key: ten million rows fetched to return a hundred. This node
//! reads only the key and time columns of a sealed relation, keeps each key's best row by the
//! ORDER BY's rules (its direction, and whether NULLs come first), and then fetches just those
//! rows whole. Above it the query still sorts and takes the first per key, so over a partitioned
//! table the partitions' candidates, sorted and merged with the rows of any heap partition (read
//! in order, as Postgres reads them), are compared with each other, and the answer is exactly
//! what Postgres's plan returns.
//!
//! It is offered at the DISTINCT stage, which Postgres does let extensions add paths to, when:
//! DISTINCT ON's expressions are columns the aggregate node can key by (`agg.rs`); the ORDER BY
//! is those, then exactly one integer or time column; the WHERE clause is only the comparisons
//! the scan skips row groups by; and there is no grouping or window above it.

use std::ffi::CStr;
use std::rc::Rc;

use pgrx::pg_sys;
use pgrx::prelude::*;
use pgrx::{PgList, PgMemoryContexts};

use super::agg::{self, Col, FxMap, GroupKey, Interner, KeyExpr, KeyHow, Val, MAX_KEYS};
use super::read::{self, Group, Store};
use super::scan::{self, Key, Op, PlanKey, PlanValue};

const NAME: &CStr = c"SnoutTime Columnar Distinct";

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
	EstimateDSMCustomScan: None,
	InitializeDSMCustomScan: None,
	ReInitializeDSMCustomScan: None,
	InitializeWorkerCustomScan: None,
	ShutdownCustomScan: None,
	ExplainCustomScan: Some(explain),
};

pub fn init() {
	unsafe { pg_sys::RegisterCustomScanMethods(&raw const SCAN_METHODS) };
}

/// What one node computes: the key columns, the column that orders a key's rows, and the WHERE
/// clause's comparisons.
#[derive(Debug, Clone)]
struct Spec {
	keys: Vec<KeyCol>,
	order: usize,
	/// the best row has the largest `order` (DESC), else the smallest
	desc: bool,
	nulls_first: bool,
	filters: Vec<PlanKey>,
}

/// A DISTINCT ON column, and how the ORDER BY orders it: the node emits its rows in that order,
/// so a Merge Append above takes them without a Sort (which would copy, and so decode, every
/// candidate whole).
#[derive(Debug, Clone, Copy)]
struct KeyCol {
	att: usize,
	how: KeyHow,
	desc: bool,
	nulls_first: bool,
	/// for text: the collation and the btree comparison function
	collation: pg_sys::Oid,
	cmp: pg_sys::Oid,
}

// ---------------------------------------------------------------------------------------
// Planning
// ---------------------------------------------------------------------------------------

/// DISTINCT ON's columns and the ordering column, as expressions over the queried relation,
/// if the query has the shape this node answers.
struct Shape {
	/// each key's expression, whether it sorts descending, and whether NULLs come first
	keys: Vec<(*mut pg_sys::Node, bool, bool)>,
	order: *mut pg_sys::Node,
	desc: bool,
	nulls_first: bool,
}

unsafe fn shape(root: *mut pg_sys::PlannerInfo) -> Option<Shape> {
	let parse = (*root).parse;
	if !(*parse).hasDistinctOn
		|| (*parse).hasAggs
		|| !(*parse).groupClause.is_null()
		|| !(*parse).groupingSets.is_null()
		|| (*parse).hasWindowFuncs
		|| (*parse).hasTargetSRFs
		|| (*root).distinct_pathkeys.is_null()
		|| (*root).sort_pathkeys.is_null()
	{
		return None;
	}
	let distinct = PgList::<pg_sys::SortGroupClause>::from_pg((*parse).distinctClause);
	let sort = PgList::<pg_sys::SortGroupClause>::from_pg((*parse).sortClause);
	if distinct.is_empty() || distinct.len() > MAX_KEYS || sort.len() != distinct.len() + 1 {
		return None;
	}
	// the ORDER BY begins with DISTINCT ON's expressions (Postgres requires it) and has one more
	let refs: Vec<u32> = distinct.iter_ptr().map(|c| (*c).tleSortGroupRef).collect();
	for (i, c) in sort.iter_ptr().take(distinct.len()).enumerate() {
		if !refs.contains(&(*c).tleSortGroupRef) || i >= refs.len() {
			return None;
		}
	}
	let last = sort.get_ptr(distinct.len())?;
	let order = pg_sys::get_sortgroupclause_expr(last, (*parse).targetList) as *mut pg_sys::Node;
	let typ = pg_sys::exprType(order);
	if !(scan::int_family(typ) || scan::time_type(typ)) {
		return None;
	}
	let tc = pg_sys::lookup_type_cache(typ, (pg_sys::TYPECACHE_LT_OPR | pg_sys::TYPECACHE_GT_OPR) as i32);
	let desc = if (*last).sortop == (*tc).gt_opr {
		true
	} else if (*last).sortop == (*tc).lt_opr {
		false
	} else {
		return None;
	};
	let mut keys = Vec::new();
	for c in sort.iter_ptr().take(distinct.len()) {
		let e = pg_sys::get_sortgroupclause_expr(c, (*parse).targetList) as *mut pg_sys::Node;
		let t = pg_sys::lookup_type_cache(pg_sys::exprType(e), (pg_sys::TYPECACHE_LT_OPR | pg_sys::TYPECACHE_GT_OPR) as i32);
		let desc = if (*c).sortop == (*t).gt_opr {
			true
		} else if (*c).sortop == (*t).lt_opr {
			false
		} else {
			return None;
		};
		keys.push((e, desc, (*c).nulls_first));
	}
	Some(Shape { keys, order, desc, nulls_first: (*last).nulls_first })
}

/// The node's path over one sealed relation, if the shape fits it; `parent` translates the
/// shape's expressions when `input` is a partition.
unsafe fn node_path(
	root: *mut pg_sys::PlannerInfo,
	s: &Shape,
	input: *mut pg_sys::RelOptInfo,
	parent: *mut pg_sys::RelOptInfo,
) -> Option<*mut pg_sys::Path> {
	let relid = (*input).relid;
	let rte = *(*root).simple_rte_array.add(relid as usize);
	if (*rte).relkind as u8 != pg_sys::RELKIND_RELATION || !(*rte).tablesample.is_null() || !(*input).lateral_relids.is_null() {
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
	pg_sys::RelationClose(r);

	let translate = |e: *mut pg_sys::Node| {
		if parent.is_null() {
			e
		} else {
			pg_sys::adjust_appendrel_attrs_multilevel(root, e, input, parent)
		}
	};
	let mut keys = Vec::new();
	let mut key_exprs = PgList::<pg_sys::Node>::new();
	for &(e, desc, nulls_first) in &s.keys {
		let e = translate(e);
		match agg::key_expr(e, relid)? {
			KeyExpr::Col { att, how, typ } => {
				let (collation, cmp) = match how {
					KeyHow::Bytes { len: -1 } => {
						let t = pg_sys::lookup_type_cache(typ, pg_sys::TYPECACHE_CMP_PROC as i32);
						(pg_sys::exprCollation(e), (*t).cmp_proc)
					}
					_ => (pg_sys::InvalidOid, pg_sys::InvalidOid),
				};
				keys.push(KeyCol { att, how, desc, nulls_first, collation, cmp });
			}
			_ => return None,
		}
		key_exprs.push(e);
	}
	let (order, _) = agg::var_att(translate(s.order), relid)?;
	let quals = PgList::<pg_sys::RestrictInfo>::from_pg((*input).baserestrictinfo);
	let (filters, _) = scan::plan_keys((*input).baserestrictinfo, relid);
	if filters.len() != quals.len() {
		return None;
	}
	let mut atts: Vec<usize> = keys.iter().map(|k| k.att).chain([order]).chain(filters.iter().map(|k| k.att)).collect();
	atts.sort_unstable();
	atts.dedup();
	if atts.iter().any(|&a| a >= store.kinds.len()) {
		return None;
	}
	let spec = Spec { keys, order, desc: s.desc, nulls_first: s.nulls_first, filters };

	// cost: the pages of the columns it reads, a little per row, and a random page per row it
	// returns (each is fetched whole). The per-row figure is chosen, not measured.
	let col_frac = (atts.len() as f64 / ncols.max(1) as f64).clamp(0.05, 1.0);
	let groups = pg_sys::estimate_num_groups(root, key_exprs.into_pg(), (*input).rows.max(1.0), std::ptr::null_mut(), std::ptr::null_mut());
	let rows_in = (*input).tuples.max(1.0);
	let run = (*input).pages as f64 * col_frac * pg_sys::seq_page_cost
		+ rows_in * pg_sys::cpu_operator_cost * 0.25
		+ groups * (pg_sys::random_page_cost + pg_sys::cpu_tuple_cost);

	let cp = pg_sys::palloc0(std::mem::size_of::<pg_sys::CustomPath>()) as *mut pg_sys::CustomPath;
	let p = &mut (*cp).path;
	p.type_ = pg_sys::NodeTag::T_CustomPath;
	p.pathtype = pg_sys::NodeTag::T_CustomScan;
	p.parent = input;
	p.pathtarget = (*input).reltarget;
	p.param_info = std::ptr::null_mut();
	p.parallel_aware = false;
	p.parallel_safe = (*input).consider_parallel;
	p.parallel_workers = 0;
	p.rows = groups.max(1.0);
	p.startup_cost = run;
	p.total_cost = run;
	// emitted in the ORDER BY's order (see `State::sorted`)
	p.pathkeys = (*root).sort_pathkeys;
	(*cp).flags = 0;
	(*cp).custom_private = to_private(&spec);
	(*cp).methods = &raw const PATH_METHODS;
	Some(cp as *mut pg_sys::Path)
}

/// Called from the upper-paths hook at the DISTINCT stage.
///
/// # Safety
/// The planner's own arguments.
pub unsafe fn add_paths(root: *mut pg_sys::PlannerInfo, input: *mut pg_sys::RelOptInfo, distinct: *mut pg_sys::RelOptInfo) {
	if (*input).reloptkind != pg_sys::RelOptKind::RELOPT_BASEREL || (*input).rtekind != pg_sys::RTEKind::RTE_RELATION {
		return;
	}
	let Some(s) = shape(root) else {
		return;
	};
	let rte = *(*root).simple_rte_array.add((*input).relid as usize);
	let ncols = PgList::<pg_sys::PathKey>::from_pg((*root).distinct_pathkeys).len() as i32;
	let path = if (*rte).relkind as u8 == pg_sys::RELKIND_PARTITIONED_TABLE {
		// each partition in the ORDER BY's order, merged: this node's candidates, sorted, where
		// it applies; otherwise the partition's own rows in that order, as Postgres reads them.
		// (A Unique per partition would do for the heap ones, but a Unique finds its sort
		// columns without the partition's relids, and so cannot sort a partition's rows.)
		if (*input).part_rels.is_null() {
			return;
		}
		let mut any = false;
		let mut subpaths = PgList::<pg_sys::Path>::new();
		for i in 0..(*input).nparts as usize {
			let child = *(*input).part_rels.add(i);
			if child.is_null() || (*child).pathlist.is_null() {
				continue;
			}
			if (*child).reloptkind != pg_sys::RelOptKind::RELOPT_OTHER_MEMBER_REL || (*child).rtekind != pg_sys::RTEKind::RTE_RELATION {
				return;
			}
			if let Some(p) = node_path(root, &s, child, input) {
				subpaths.push(p);
				any = true;
				continue;
			}
			let mut sorted = pg_sys::get_cheapest_path_for_pathkeys(
				(*child).pathlist,
				(*root).sort_pathkeys,
				std::ptr::null_mut(),
				pg_sys::CostSelector::TOTAL_COST,
				false,
			);
			if sorted.is_null() {
				sorted = pg_sys::create_sort_path(root, child, (*child).cheapest_total_path, (*root).sort_pathkeys, -1.0) as *mut pg_sys::Path;
			}
			subpaths.push(sorted);
		}
		if !any {
			return;
		}
		pg_sys::create_merge_append_path(root, input, subpaths.into_pg(), (*root).sort_pathkeys, std::ptr::null_mut()) as *mut pg_sys::Path
	} else {
		match node_path(root, &s, input, std::ptr::null_mut()) {
			Some(p) => p,
			None => return,
		}
	};
	// the first row of each key
	let rows = (*path).rows.max(1.0);
	let unique = pg_sys::create_upper_unique_path(root, distinct, path, ncols, rows);
	pg_sys::add_path(distinct, unique as *mut pg_sys::Path);
}

unsafe fn to_private(spec: &Spec) -> *mut pg_sys::List {
	let mut list = PgList::<pg_sys::Node>::new();
	let mut push = |s: String| {
		let c = std::ffi::CString::new(s).unwrap();
		list.push(pg_sys::makeString(pg_sys::pstrdup(c.as_ptr())) as *mut pg_sys::Node);
	};
	push(format!("O {} {} {}", spec.order, spec.desc as u8, spec.nulls_first as u8));
	for k in &spec.keys {
		let how = match k.how {
			KeyHow::Int => "int".to_string(),
			KeyHow::Bool => "bool".to_string(),
			KeyHow::Bytes { len } => format!("bytes{len}"),
		};
		push(format!("K {} {how} {} {} {} {}", k.att, k.desc as u8, k.nulls_first as u8, k.collation.to_u32(), k.cmp.to_u32()));
	}
	for k in &spec.filters {
		push(match k.value {
			PlanValue::Known(v) => format!("F {} {:?} K {}", k.att, k.op, v),
			PlanValue::Expr { index, typ } => format!("F {} {:?} R {} {}", k.att, k.op, index, typ.to_u32()),
		});
	}
	list.into_pg()
}

unsafe fn from_private(list: *mut pg_sys::List) -> Option<Spec> {
	let mut spec = Spec { keys: Vec::new(), order: 0, desc: true, nulls_first: true, filters: Vec::new() };
	for s in PgList::<pg_sys::String>::from_pg(list).iter_ptr() {
		let item = CStr::from_ptr((*s).sval).to_string_lossy().into_owned();
		let p: Vec<&str> = item.split(' ').collect();
		let num = |i: usize| p.get(i).and_then(|x| x.parse::<i64>().ok());
		match p.first().copied() {
			Some("O") => {
				spec.order = num(1)? as usize;
				spec.desc = num(2)? != 0;
				spec.nulls_first = num(3)? != 0;
			}
			Some("K") => {
				let how = match *p.get(2)? {
					"int" => KeyHow::Int,
					"bool" => KeyHow::Bool,
					b => KeyHow::Bytes { len: b.strip_prefix("bytes")?.parse().ok()? },
				};
				spec.keys.push(KeyCol {
					att: num(1)? as usize,
					how,
					desc: num(3)? != 0,
					nulls_first: num(4)? != 0,
					collation: pg_sys::Oid::from(num(5)? as u32),
					cmp: pg_sys::Oid::from(num(6)? as u32),
				});
			}
			Some("F") => {
				let op = match *p.get(2)? {
					"Lt" => Op::Lt,
					"Le" => Op::Le,
					"Eq" => Op::Eq,
					"Ge" => Op::Ge,
					"Gt" => Op::Gt,
					_ => return None,
				};
				let value = match *p.get(3)? {
					"K" => PlanValue::Known(num(4)?),
					"R" => PlanValue::Expr { index: num(4)? as usize, typ: pg_sys::Oid::from(num(5)? as u32) },
					_ => return None,
				};
				spec.filters.push(PlanKey { att: num(1)? as usize, op, value });
			}
			_ => return None,
		}
	}
	Some(spec)
}

#[pg_guard]
unsafe extern "C-unwind" fn plan_path(
	_root: *mut pg_sys::PlannerInfo,
	rel: *mut pg_sys::RelOptInfo,
	best_path: *mut pg_sys::CustomPath,
	tlist: *mut pg_sys::List,
	clauses: *mut pg_sys::List,
	_custom_plans: *mut pg_sys::List,
) -> *mut pg_sys::Plan {
	let cs = pg_sys::palloc0(std::mem::size_of::<pg_sys::CustomScan>()) as *mut pg_sys::CustomScan;
	(*cs).scan.plan.type_ = pg_sys::NodeTag::T_CustomScan;
	(*cs).scan.plan.targetlist = tlist;
	// the node applies the WHERE clause itself before choosing; checking the rows it returns
	// again costs a hundred comparisons
	(*cs).scan.plan.qual = pg_sys::extract_actual_clauses(clauses, false);
	(*cs).scan.scanrelid = (*rel).relid;
	(*cs).flags = (*best_path).flags;
	(*cs).custom_private = (*best_path).custom_private;
	let (_, runtime) = scan::plan_keys(clauses, (*rel).relid);
	let mut exprs = PgList::<pg_sys::Node>::new();
	for e in runtime {
		exprs.push(pg_sys::copyObjectImpl(e as *const std::ffi::c_void) as *mut pg_sys::Node);
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

/// Where a key's best row is.
#[derive(Clone, Copy)]
enum Loc {
	Row(u64),
	Delta(pg_sys::ItemPointerData),
}

/// Where a leading attribute's value comes from.
#[derive(Clone, Copy)]
enum Source {
	Key(usize),
	Order,
}

#[derive(Clone, Copy)]
struct Best {
	order: Option<i64>,
	loc: Loc,
}

struct State {
	spec: Spec,
	store: Rc<Store>,
	inputs: Vec<usize>,
	pos: Vec<usize>,
	exprs: Vec<*mut pg_sys::ExprState>,
	econtext: *mut pg_sys::ExprContext,
	keys: Vec<Key>,
	none: bool,
	deleted: Vec<u64>,
	delta_oid: pg_sys::Oid,
	snapshot: pg_sys::Snapshot,
	names: Interner,
	/// each key's best row so far, by its index in `rows`
	best: FxMap<GroupKey, usize>,
	rows: Vec<Best>,
	last: Option<(GroupKey, usize)>,
	/// each key's best row, in the ORDER BY's order
	out: Vec<(GroupKey, Best)>,
	keys_of: Vec<GroupKey>,
	/// the leading attributes that are key or order columns, filled from what `choose` knows
	/// so a merge above compares rows without decoding anything
	prefix: Vec<Source>,
	mem: super::types::Mem,
	emitted: usize,
	done: bool,
	group: Option<(usize, Rc<Group>)>,
	delta_rel: pg_sys::Relation,
	delta_slot: *mut pg_sys::TupleTableSlot,
	home: pg_sys::MemoryContext,
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
	}

	/// Is `candidate` a better row for its key than `held`, by the ORDER BY? A tie keeps the
	/// row already held.
	fn better(&self, candidate: Option<i64>, held: Option<i64>) -> bool {
		match (candidate, held) {
			(None, None) => false,
			(None, Some(_)) => self.spec.nulls_first,
			(Some(_), None) => !self.spec.nulls_first,
			(Some(c), Some(h)) => {
				if self.spec.desc {
					c > h
				} else {
					c < h
				}
			}
		}
	}

	fn consider(&mut self, vals: &[Val], loc: Loc) {
		for k in &self.keys {
			let Val::I(v) = vals[self.pos[k.att]] else {
				return;
			};
			let ok = match k.op {
				Op::Lt => v < k.value,
				Op::Le => v <= k.value,
				Op::Eq => v == k.value,
				Op::Ge => v >= k.value,
				Op::Gt => v > k.value,
			};
			if !ok {
				return;
			}
		}
		let mut key = GroupKey::default();
		for (i, k) in self.spec.keys.iter().enumerate() {
			match vals[self.pos[k.att]] {
				Val::Null => key.nulls |= 1 << i,
				v => key.words[i] = v.bits(),
			}
		}
		let order = match vals[self.pos[self.spec.order]] {
			Val::I(t) => Some(t),
			_ => None,
		};
		self.consider_key(key, order, loc);
	}

	/// When the store is sorted by the DISTINCT ON columns and then the ordering column, and the
	/// best row is the last of its key (descending, NULLs first or none), every key's best row is
	/// the last row of its run, and the directory says where each run ends (format.rs,
	/// `run_ends`): the candidates are read from there and nothing is decoded (bench q1 decoded
	/// the key and time columns of every row group, 2026-09-23). False when that does not hold,
	/// and the rows are read instead: a filter, a deleted row, a group whose runs were not
	/// recorded.
	unsafe fn from_directory(&mut self, store: &Store) -> bool {
		let Some(meta) = store.meta.as_ref() else { return false };
		let o: Vec<usize> = meta.order.iter().map(|&a| a as usize - 1).collect();
		let atts = super::types::attrs((*store.rel).rd_att);
		let order_notnull = atts.get(self.spec.order).is_some_and(|a| a.attnotnull);
		if o.len() < 2
			|| !self.keys.is_empty()
			|| !self.deleted.is_empty()
			|| !self.spec.desc
			|| !(self.spec.nulls_first || order_notnull)
			|| o[o.len() - 1] != self.spec.order
			|| o.len() - 1 != self.spec.keys.len()
			|| !self.spec.keys.iter().all(|k| o[..o.len() - 1].contains(&k.att))
			|| super::seek::Order::of(store).is_none()
			|| store.dir.iter().any(|e| e.run_ends.is_none())
		{
			return false;
		}
		let width = o.len();
		let place: Vec<usize> = self.spec.keys.iter().map(|k| o.iter().position(|&a| a == k.att).unwrap()).collect();
		let int = |b: &[u8]| {
			let mut w = [0u8; 8];
			w[..b.len().min(8)].copy_from_slice(&b[..b.len().min(8)]);
			let raw = u64::from_le_bytes(w);
			match b.len() {
				2 => raw as u16 as i16 as i64,
				4 => raw as u32 as i32 as i64,
				_ => raw as i64,
			}
		};
		let dir = Rc::clone(&store.dir);
		let candidate = |this: &mut Self, key: &[Option<Vec<u8>>], row: u64| {
			let mut k = GroupKey::default();
			for (i, spec) in this.spec.keys.clone().iter().enumerate() {
				match &key[place[i]] {
					None => k.nulls |= 1 << i,
					Some(b) => {
						k.words[i] = match spec.how {
							KeyHow::Int => int(b) as u64,
							KeyHow::Bool => (b.first().copied().unwrap_or(0) != 0) as u64,
							KeyHow::Bytes { .. } => this.names.intern(b) as u64,
						}
					}
				}
			}
			let order = key[width - 1].as_deref().map(int);
			this.consider_key(k, order, Loc::Row(row));
		};
		for (g, e) in dir.iter().enumerate() {
			for (row, key) in e.run_ends.as_deref().unwrap_or(&[]) {
				candidate(self, key, e.first_row + *row as u64);
			}
			// the group's last run ends with it, unless the next group carries it on
			let carried = dir.get(g + 1).is_some_and(|next| next.first_key[..width - 1] == e.last_key[..width - 1]);
			if !carried {
				candidate(self, &e.last_key, e.first_row + e.rows - 1);
			}
		}
		self.skipped += dir.len() as u64;
		true
	}

	/// A candidate row for `key`.
	fn consider_key(&mut self, key: GroupKey, order: Option<i64>, loc: Loc) {
		// sorted data puts a key's rows together: the last row's key is the likely one
		let i = match self.last {
			Some((k, i)) if k == key => i,
			_ => {
				let i = match self.best.get(&key) {
					Some(&i) => i,
					None => {
						self.rows.push(Best { order, loc });
						self.keys_of.push(key);
						self.best.insert(key, self.rows.len() - 1);
						self.last = Some((key, self.rows.len() - 1));
						return;
					}
				};
				self.last = Some((key, i));
				i
			}
		};
		if self.better(order, self.rows[i].order) {
			self.rows[i] = Best { order, loc };
		}
	}

	/// One row group with no WHERE clause to apply: a key's rows are consecutive when the store
	/// is ordered by it, so each run of one key is scanned for its best row with no hashing, and
	/// only the run's winner is a candidate. Correct in any order; fast in the store's.
	fn runs(&mut self, cols: &[Col], first: u64, rows: usize, del: &mut usize) {
		let word = |c: &Col, r: usize| -> Option<u64> {
			match c.get(r) {
				Val::Null => None,
				v => Some(v.bits()),
			}
		};
		let key_cols: Vec<&Col> = self.spec.keys.iter().map(|k| &cols[self.pos[k.att]]).collect();
		let (order, nulls) = match &cols[self.pos[self.spec.order]] {
			Col::Int(v, n) => (v, n),
			_ => return,
		};
		if key_cols.len() == 1 {
			match key_cols[0] {
				Col::Int(v, n) => return self.runs_one(|i| v[i] as u64, n, order, nulls, first, rows, del),
				Col::Text(v, n) => return self.runs_one(|i| v[i] as u64, n, order, nulls, first, rows, del),
				Col::Bool(v, n) => return self.runs_one(|i| v[i] as u64, n, order, nulls, first, rows, del),
				Col::Float(..) => {}
			}
		}
		let same = |a: usize, b: usize| key_cols.iter().all(|c| word(c, a) == word(c, b));
		let mut r = 0;
		while r < rows {
			let mut e = r + 1;
			while e < rows && same(r, e) {
				e += 1;
			}
			let mut best: Option<(usize, Option<i64>)> = None;
			for i in r..e {
				let n = first + i as u64;
				while *del < self.deleted.len() && self.deleted[*del] < n {
					*del += 1;
				}
				if *del < self.deleted.len() && self.deleted[*del] == n {
					continue;
				}
				let o = if nulls[i] { None } else { Some(order[i]) };
				if best.is_none_or(|(_, b)| self.better(o, b)) {
					best = Some((i, o));
				}
			}
			if let Some((i, o)) = best {
				let mut key = GroupKey::default();
				for (k, c) in key_cols.iter().enumerate() {
					match word(c, i) {
						None => key.nulls |= 1 << k,
						Some(w) => key.words[k] = w,
					}
				}
				self.consider_key(key, o, Loc::Row(first + i as u64));
			}
			r = e;
		}
	}

	/// `runs` for one key column, over its words and the order column directly.
	#[allow(clippy::too_many_arguments)]
	fn runs_one(&mut self, word: impl Fn(usize) -> u64, knulls: &[bool], order: &[i64], onulls: &[bool], first: u64, rows: usize, del: &mut usize) {
		let (desc, nulls_first) = (self.spec.desc, self.spec.nulls_first);
		let deleted = std::mem::take(&mut self.deleted);
		let mut r = 0;
		while r < rows {
			let (w, kn) = (word(r), knulls[r]);
			let mut e = r + 1;
			while e < rows && word(e) == w && knulls[e] == kn {
				e += 1;
			}
			// the run's best row: rows with a NULL order value, then the others, by the rules
			// of `better`, the earliest of equals kept
			let mut best: Option<(usize, Option<i64>)> = None;
			let any_deleted = *del < deleted.len() && deleted[*del] < first + e as u64;
			for i in r..e {
				if any_deleted {
					let n = first + i as u64;
					while *del < deleted.len() && deleted[*del] < n {
						*del += 1;
					}
					if *del < deleted.len() && deleted[*del] == n {
						continue;
					}
				}
				let o = if onulls[i] { None } else { Some(order[i]) };
				let better = match (best, o) {
					(None, _) => true,
					(Some((_, None)), None) => false,
					(Some((_, Some(_))), None) => nulls_first,
					(Some((_, None)), Some(_)) => !nulls_first,
					(Some((_, Some(b))), Some(x)) => if desc { x > b } else { x < b },
				};
				if better {
					best = Some((i, o));
				}
			}
			if let Some((i, o)) = best {
				let mut key = GroupKey::default();
				if kn {
					key.nulls = 1;
				} else {
					key.words[0] = w;
				}
				self.consider_key(key, o, Loc::Row(first + i as u64));
			}
			r = e;
		}
		self.deleted = deleted;
	}

	unsafe fn choose(&mut self) {
		let store = Rc::clone(&self.store);
		let kinds = store.kinds.clone();
		let inputs = self.inputs.clone();
		let mut vals = vec![Val::Null; inputs.len()];
		if !self.none && !self.from_directory(&store) {
			for (g, entry) in store.dir.iter().enumerate() {
				if !scan::keeps(&self.keys, entry) {
					self.skipped += 1;
					continue;
				}
				self.read += 1;
				let group = Store::load(&store, g);
				let cols: Vec<Col> = self.names.columns(&group, &kinds, &inputs, &[], 0..group.rows as usize);
				let first = group.first_row;
				let mut del = self.deleted.partition_point(|&d| d < first);
				if self.keys.is_empty() {
					self.runs(&cols, first, group.rows as usize, &mut del);
					continue;
				}
				for r in 0..group.rows as usize {
					let n = first + r as u64;
					if del < self.deleted.len() && self.deleted[del] == n {
						del += 1;
						continue;
					}
					for (i, c) in cols.iter().enumerate() {
						vals[i] = c.get(r);
					}
					self.consider(&vals, Loc::Row(n));
				}
			}
		}
		// the delta store's rows compete with the column store's, however those were chosen (a
		// directory read skipped them until 2026-09-23: tests/pg_regress columnar_distinct)
		if !self.none && self.delta_oid != pg_sys::InvalidOid {
			let rel = pg_sys::table_open(self.delta_oid, pg_sys::AccessShareLock as i32);
			let scan = pg_sys::table_beginscan(rel, self.snapshot, 0, std::ptr::null_mut());
			let slot = pg_sys::table_slot_create(rel, std::ptr::null_mut());
			let desc = (*rel).rd_att;
			let last = inputs.iter().max().map_or(0, |&a| a + 1);
			while pg_sys::table_scan_getnextslot(scan, pg_sys::ScanDirection::ForwardScanDirection, slot) {
				pg_sys::slot_getsomeattrs(slot, last as i32);
				for (i, &att) in inputs.iter().enumerate() {
					vals[i] = self.names.slot_val(slot, desc, att);
				}
				self.consider(&vals, Loc::Delta((*slot).tts_tid));
			}
			pg_sys::ExecDropSingleTupleTableSlot(slot);
			pg_sys::table_endscan(scan);
			pg_sys::table_close(rel, pg_sys::NoLock as i32);
		}
		let candidates: Vec<(GroupKey, Best)> = self.keys_of.iter().copied().zip(self.rows.iter().copied()).collect();
		self.out = self.sorted(candidates);
		self.done = true;
	}

	/// The candidates in the ORDER BY's order: each key by its direction, NULLs rule and (text)
	/// collation, then the order column.
	unsafe fn sorted(&mut self, mut c: Vec<(GroupKey, Best)>) -> Vec<(GroupKey, Best)> {
		use std::cmp::Ordering;
		let keys = self.spec.keys.clone();
		let mut fns: Vec<Option<Box<pg_sys::FmgrInfo>>> = Vec::new();
		for k in &keys {
			fns.push(if k.cmp == pg_sys::InvalidOid {
				None
			} else {
				let mut f = Box::new(pg_sys::FmgrInfo::default());
				pg_sys::fmgr_info(k.cmp, &mut *f);
				Some(f)
			});
		}
		// the text keys as datums, once per candidate
		let mut text: Vec<Vec<pg_sys::Datum>> = Vec::with_capacity(c.len());
		for (key, _) in &c {
			let mut row = Vec::with_capacity(keys.len());
			for i in 0..keys.len() {
				row.push(if fns[i].is_some() && key.nulls & (1 << i) == 0 {
					super::types::datum_of_bytes(&self.names.strings[key.words[i] as usize], -1, false, &mut self.mem)
				} else {
					pg_sys::Datum::from(0usize)
				});
			}
			text.push(row);
		}
		let strings = &self.names.strings;
		let (desc, nulls_first) = (self.spec.desc, self.spec.nulls_first);
		let mut idx: Vec<usize> = (0..c.len()).collect();
		idx.sort_by(|&a, &b| {
			let (ka, kb) = (&c[a].0, &c[b].0);
			for (i, k) in keys.iter().enumerate() {
				let (na, nb) = (ka.nulls & (1 << i) != 0, kb.nulls & (1 << i) != 0);
				let o = match (na, nb) {
					(true, true) => Ordering::Equal,
					(true, false) => if k.nulls_first { Ordering::Less } else { Ordering::Greater },
					(false, true) => if k.nulls_first { Ordering::Greater } else { Ordering::Less },
					(false, false) => {
						let (wa, wb) = (ka.words[i], kb.words[i]);
						let o = match (k.how, &fns[i]) {
							(KeyHow::Bytes { .. }, Some(f)) => {
								let f = &**f as *const pg_sys::FmgrInfo as *mut pg_sys::FmgrInfo;
								let r = pg_sys::FunctionCall2Coll(f, k.collation, text[a][i], text[b][i]).value() as i32;
								r.cmp(&0)
							}
							(KeyHow::Bytes { .. }, None) => strings[wa as usize].cmp(&strings[wb as usize]),
							_ => (wa as i64).cmp(&(wb as i64)),
						};
						if k.desc { o.reverse() } else { o }
					}
				};
				if o != Ordering::Equal {
					return o;
				}
			}
			match (c[a].1.order, c[b].1.order) {
				(None, None) => Ordering::Equal,
				(None, Some(_)) => if nulls_first { Ordering::Less } else { Ordering::Greater },
				(Some(_), None) => if nulls_first { Ordering::Greater } else { Ordering::Less },
				(Some(x), Some(y)) => if desc { y.cmp(&x) } else { x.cmp(&y) },
			}
		});
		let out = idx.into_iter().map(|i| c[i]).collect();
		c.clear();
		out
	}

	unsafe fn emit(&mut self, key: GroupKey, best: Best, slot: *mut pg_sys::TupleTableSlot, rel: pg_sys::Relation) -> bool {
		match best.loc {
			Loc::Row(n) => {
				let Some(g) = self.store.group_of(n) else {
					return false;
				};
				if self.group.as_ref().map(|x| x.0) != Some(g) {
					self.group = None;
					self.group = Some((g, Rc::new(Store::load(&self.store, g))));
				}
				self.group.as_ref().unwrap().1.store_lazy(n, slot, (*rel).rd_id);
				if !self.prefix.is_empty() && super::slot::is_lazy(slot) {
					for (a, src) in self.prefix.clone().into_iter().enumerate() {
						let (d, isnull) = match src {
							Source::Order => match best.order {
								Some(t) => (pg_sys::Datum::from(t as usize), false),
								None => (pg_sys::Datum::from(0usize), true),
							},
							Source::Key(i) => {
								if key.nulls & (1 << i) != 0 {
									(pg_sys::Datum::from(0usize), true)
								} else {
									let w = key.words[i];
									match self.spec.keys[i].how {
										KeyHow::Bytes { len } => {
											(super::types::datum_of_bytes(&self.names.strings[w as usize], len, false, &mut self.mem), false)
										}
										_ => (pg_sys::Datum::from(w as usize), false),
									}
								}
							}
						};
						*(*slot).tts_values.add(a) = d;
						*(*slot).tts_isnull.add(a) = isnull;
					}
					(*slot).tts_nvalid = self.prefix.len() as i16;
				}
				true
			}
			Loc::Delta(mut tid) => {
				if self.delta_rel.is_null() {
					let old = pg_sys::MemoryContextSwitchTo(self.home);
					self.delta_rel = pg_sys::table_open(self.delta_oid, pg_sys::AccessShareLock as i32);
					self.delta_slot = pg_sys::table_slot_create(self.delta_rel, std::ptr::null_mut());
					pg_sys::MemoryContextSwitchTo(old);
				}
				if !pg_sys::table_tuple_fetch_row_version(self.delta_rel, &mut tid, self.snapshot, self.delta_slot) {
					return false;
				}
				pg_sys::ExecCopySlot(slot, self.delta_slot);
				read::to_delta_tid(&mut tid);
				(*slot).tts_tid = tid;
				(*slot).tts_tableOid = (*rel).rd_id;
				true
			}
		}
	}

	fn reset(&mut self) {
		self.best.clear();
		self.rows.clear();
		self.keys_of.clear();
		self.last = None;
		self.out.clear();
		self.emitted = 0;
		self.done = false;
		self.group = None;
		self.read = 0;
		self.skipped = 0;
	}
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
	let Some(spec) = from_private((*cs).custom_private) else {
		pgrx::error!("a SnoutTime Columnar Distinct plan could not be read");
	};
	super::build::flush(rel);
	let (delta_oid, deletes_oid) = read::side_tables((*rel).rd_id);
	let snapshot = (*estate).es_snapshot;
	let mut inputs: Vec<usize> = spec.keys.iter().map(|k| k.att).chain([spec.order]).chain(spec.filters.iter().map(|k| k.att)).collect();
	inputs.sort_unstable();
	inputs.dedup();
	let mut pos = vec![usize::MAX; inputs.iter().max().map_or(0, |&a| a + 1)];
	for (i, &a) in inputs.iter().enumerate() {
		pos[a] = i;
	}
	let exprs: Vec<*mut pg_sys::ExprState> = PgList::<pg_sys::Expr>::from_pg((*cs).custom_exprs)
		.iter_ptr()
		.map(|e| pg_sys::ExecInitExpr(e, &raw mut (*node).ss.ps))
		.collect();
	let mut prefix = Vec::new();
	loop {
		let a = prefix.len();
		let src = if a == spec.order {
			Source::Order
		} else if let Some(i) = spec.keys.iter().position(|k| k.att == a) {
			Source::Key(i)
		} else {
			break;
		};
		prefix.push(src);
	}
	let state = State {
		spec,
		store: Rc::new(Store::open(rel)),
		inputs,
		pos,
		exprs,
		econtext: (*node).ss.ps.ps_ExprContext,
		keys: Vec::new(),
		none: false,
		deleted: read::deleted_rows(deletes_oid, snapshot),
		delta_oid,
		snapshot,
		names: Interner::default(),
		best: FxMap::default(),
		rows: Vec::new(),
		last: None,
		out: Vec::new(),
		keys_of: Vec::new(),
		prefix,
		mem: super::types::Mem::new(),
		emitted: 0,
		done: false,
		group: None,
		delta_rel: std::ptr::null_mut(),
		delta_slot: std::ptr::null_mut(),
		home: pg_sys::CurrentMemoryContext,
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
		s.choose();
	}
	while s.emitted < s.out.len() {
		let (key, best) = s.out[s.emitted];
		s.emitted += 1;
		if s.emit(key, best, slot, (*ss).ss_currentRelation) {
			return slot;
		}
	}
	pg_sys::ExecClearTuple(slot);
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
	s.group = None;
	if !s.delta_slot.is_null() {
		pg_sys::ExecDropSingleTupleTableSlot(s.delta_slot);
		s.delta_slot = std::ptr::null_mut();
	}
	if !s.delta_rel.is_null() {
		pg_sys::table_close(s.delta_rel, pg_sys::NoLock as i32);
		s.delta_rel = std::ptr::null_mut();
	}
}

#[pg_guard]
unsafe extern "C-unwind" fn rescan(node: *mut pg_sys::CustomScanState) {
	let s = &mut *(*(node as *mut Node)).state;
	s.reset();
	pg_sys::ExecScanReScan(&mut (*node).ss);
}

#[pg_guard]
unsafe extern "C-unwind" fn explain(node: *mut pg_sys::CustomScanState, _ancestors: *mut pg_sys::List, es: *mut pg_sys::ExplainState) {
	let s = &*(*(node as *mut Node)).state;
	let keys = std::ffi::CString::new(s.spec.filters.len().to_string()).unwrap();
	pg_sys::ExplainPropertyText(c"Row Group Filters".as_ptr(), keys.as_ptr(), es);
	if (*es).analyze {
		pg_sys::ExplainPropertyInteger(c"Row Groups Read".as_ptr(), std::ptr::null(), s.read as i64, es);
		pg_sys::ExplainPropertyInteger(c"Row Groups Skipped".as_ptr(), std::ptr::null(), s.skipped as i64, es);
		pg_sys::ExplainPropertyInteger(c"Keys".as_ptr(), std::ptr::null(), s.best.len() as i64, es);
	}
}
