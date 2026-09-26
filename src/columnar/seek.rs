//! A sealed partition as its own index on its sort key (PLAN.md Q5).
//!
//! A column store sorted by `(a, b, ...)` keeps each row group's first and last key in its
//! directory, so the row groups that can hold a key range are a contiguous run found by binary
//! search, and inside a row group the rows are a contiguous run too. A WHERE clause gives the
//! range: equalities on a leading run of the sort columns, then `<`, `<=`, `>`, `>=` on the next
//! one, each against a constant or a value known when the scan starts (a parameter, `now()`).
//! That is what a btree on the same columns would use, and it is why a sealed partition does not
//! need one: `WHERE host = $1 AND ts <= $2 ORDER BY ts DESC LIMIT 1` reads one row group.
//!
//! One equality of the run may be an IN list (`host IN ('a', 'b')`, `host = ANY($1)`), as a btree
//! would take it: the scan seeks each value on its own and reads the union of their row groups
//! (`seeks`), where before it read every row group between them (TSBS's eight-host queries,
//! 2026-09-25).
//!
//! Comparisons are the column type's default btree operator family's, in the column's
//! collation: the order the seal sorted by (build.rs), ascending with NULLs last. A clause whose
//! operator is not in that family, or whose collation differs, is not used; the executor still
//! evaluates every clause on every row, so a seek only ever narrows what is read.

use std::cmp::Ordering;
use std::ops::Range;

use pgrx::pg_sys;
use pgrx::PgList;

use super::read::{Group, Store};
use super::types;

/// `BTORDER_PROC`: a btree operator family's comparison support function.
const BTORDER_PROC: i16 = 1;

/// The order a column store is sorted in, as its relation reads it now.
pub struct Order {
	/// 0-based attributes, in sort order
	pub atts: Vec<usize>,
	pub types: Vec<pg_sys::Oid>,
	pub typmods: Vec<i32>,
	pub colls: Vec<pg_sys::Oid>,
	/// each column type's own comparison (the type cache's, which lives as long as the backend)
	same: Vec<*mut pg_sys::FmgrInfo>,
	pub opfamilies: Vec<pg_sys::Oid>,
}

impl Order {
	/// None when the store is unsorted, or a sort column has since been dropped or changed type.
	///
	/// # Safety
	/// The store's relation must be open.
	pub unsafe fn of(store: &Store) -> Option<Order> {
		let meta = store.meta.as_ref()?;
		if meta.order.is_empty() {
			return None;
		}
		let atts = types::attrs((*store.rel).rd_att);
		let mut o = Order { atts: vec![], types: vec![], typmods: vec![], colls: vec![], same: vec![], opfamilies: vec![] };
		for &a in &meta.order {
			let i = a as usize - 1;
			let att = atts.get(i)?;
			if att.attisdropped || meta.types.get(i).copied() != Some(att.atttypid.to_u32()) {
				return None;
			}
			let tc = pg_sys::lookup_type_cache(
				att.atttypid,
				(pg_sys::TYPECACHE_CMP_PROC_FINFO | pg_sys::TYPECACHE_BTREE_OPFAMILY) as i32,
			);
			if (*tc).cmp_proc == pg_sys::InvalidOid || (*tc).btree_opf == pg_sys::InvalidOid {
				return None;
			}
			o.atts.push(i);
			o.types.push(att.atttypid);
			o.typmods.push(att.atttypmod);
			o.colls.push(att.attcollation);
			o.same.push(&raw mut (*tc).cmp_proc_finfo);
			o.opfamilies.push((*tc).btree_opf);
		}
		Some(o)
	}

	/// Two values of sort column `i`, NULLs last.
	pub unsafe fn cmp(&self, i: usize, a: Option<pg_sys::Datum>, b: Option<pg_sys::Datum>) -> Ordering {
		match (a, b) {
			(None, None) => Ordering::Equal,
			(None, _) => Ordering::Greater,
			(_, None) => Ordering::Less,
			(Some(x), Some(y)) => call(self.same[i], self.colls[i], x, y),
		}
	}

	/// Two whole keys, lexicographically.
	pub unsafe fn cmp_keys(&self, a: &dyn Fn(usize) -> Option<pg_sys::Datum>, b: &dyn Fn(usize) -> Option<pg_sys::Datum>) -> Ordering {
		for i in 0..self.atts.len() {
			match self.cmp(i, a(i), b(i)) {
				Ordering::Equal => {}
				o => return o,
			}
		}
		Ordering::Equal
	}
}

unsafe fn call(f: *mut pg_sys::FmgrInfo, coll: pg_sys::Oid, a: pg_sys::Datum, b: pg_sys::Datum) -> Ordering {
	(pg_sys::FunctionCall2Coll(f, coll, a, b).value() as i32).cmp(&0)
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Bound {
	Eq,
	/// `= ANY(array)`: an equality with one of the array's values, expanded by [`seeks`]
	In,
	Lo { strict: bool },
	Hi { strict: bool },
}

/// One clause the seek uses: `sort column pos  bound  value`, the value being which of the
/// plan's expressions computes it, compared by `proc` (the operator family's comparison for the
/// column's type and the value's).
#[derive(Debug, Clone, Copy)]
pub struct Term {
	pub pos: usize,
	pub bound: Bound,
	pub proc_: pg_sys::Oid,
	pub expr: usize,
}

/// The terms of `clauses` the order can seek by, and the value expressions they name. Only a
/// run of equalities from the first sort column, then ranges on the column after it, are kept.
///
/// # Safety
/// `clauses` must be a list of RestrictInfo of the relation `relid`.
pub unsafe fn terms(order: &Order, clauses: *mut pg_sys::List, relid: pg_sys::Index) -> (Vec<Term>, Vec<*mut pg_sys::Node>) {
	let (terms, exprs, _) = terms_by_clause(order, clauses, relid);
	(terms, exprs)
}

/// [`terms`], with the position in `clauses` of the clause each term came from.
///
/// # Safety
/// As [`terms`].
pub unsafe fn terms_by_clause(order: &Order, clauses: *mut pg_sys::List, relid: pg_sys::Index) -> (Vec<Term>, Vec<*mut pg_sys::Node>, Vec<usize>) {
	let mut found: Vec<(Term, *mut pg_sys::Node, usize)> = Vec::new();
	for (i, rinfo) in PgList::<pg_sys::RestrictInfo>::from_pg(clauses).iter_ptr().enumerate() {
		if let Some((t, e)) = term_of(order, (*rinfo).clause as *mut pg_sys::Node, relid) {
			found.push((t, e, i));
		}
	}
	// the run of equalities from the first sort column; one of them may be an IN list, where no
	// plain equality fixes that column
	let mut prefix = 0;
	let mut in_pos = None;
	loop {
		if found.iter().any(|(t, _, _)| t.pos == prefix && t.bound == Bound::Eq) {
			prefix += 1;
		} else if in_pos.is_none() && found.iter().any(|(t, _, _)| t.pos == prefix && t.bound == Bound::In) {
			in_pos = Some(prefix);
			prefix += 1;
		} else {
			break;
		}
	}
	let mut terms = Vec::new();
	let mut exprs = Vec::new();
	let mut from = Vec::new();
	let mut in_kept = false;
	for (mut t, e, i) in found {
		let keep = match t.bound {
			Bound::Eq => t.pos < prefix,
			Bound::In => {
				let first = in_pos == Some(t.pos) && !in_kept;
				in_kept |= first;
				first
			}
			_ => t.pos == prefix,
		};
		if keep {
			t.expr = exprs.len();
			exprs.push(e);
			terms.push(t);
			from.push(i);
		}
	}
	(terms, exprs, from)
}

/// A term as one line of a plan's private list, after its `S `: `pos bound proc expr`.
pub fn term_text(t: &Term) -> String {
	let bound = match t.bound {
		Bound::Eq => "Eq".to_string(),
		Bound::In => "In".to_string(),
		Bound::Lo { strict } => format!("Lo{}", strict as u8),
		Bound::Hi { strict } => format!("Hi{}", strict as u8),
	};
	format!("{} {} {} {}", t.pos, bound, t.proc_.to_u32(), t.expr)
}

/// [`term_text`]'s line read back.
pub fn parse_term(s: &str) -> Option<Term> {
	let mut it = s.split(' ');
	let pos = it.next()?.parse().ok()?;
	let bound = match it.next()? {
		"Eq" => Bound::Eq,
		"In" => Bound::In,
		"Lo0" => Bound::Lo { strict: false },
		"Lo1" => Bound::Lo { strict: true },
		"Hi0" => Bound::Hi { strict: false },
		"Hi1" => Bound::Hi { strict: true },
		_ => return None,
	};
	let proc_ = pg_sys::Oid::from(it.next()?.parse::<u32>().ok()?);
	let expr = it.next()?.parse().ok()?;
	Some(Term { pos, bound, proc_, expr })
}

unsafe fn term_of(order: &Order, clause: *mut pg_sys::Node, relid: pg_sys::Index) -> Option<(Term, *mut pg_sys::Node)> {
	if clause.is_null() {
		return None;
	}
	let strip = |n: *mut pg_sys::Node| {
		let mut n = n;
		while !n.is_null() && (*n).type_ == pg_sys::NodeTag::T_RelabelType {
			n = (*(n as *mut pg_sys::RelabelType)).arg as *mut pg_sys::Node;
		}
		n
	};
	let usable = |n: *mut pg_sys::Node| !pg_sys::contain_var_clause(n) && !pg_sys::contain_volatile_functions(n);
	if (*clause).type_ == pg_sys::NodeTag::T_ScalarArrayOpExpr {
		return in_term_of(order, clause as *mut pg_sys::ScalarArrayOpExpr, relid, &strip, &usable);
	}
	if (*clause).type_ != pg_sys::NodeTag::T_OpExpr {
		return None;
	}
	let op = clause as *mut pg_sys::OpExpr;
	let args = PgList::<pg_sys::Node>::from_pg((*op).args);
	if args.len() != 2 {
		return None;
	}
	let (a, b) = (args.get_ptr(0)?, args.get_ptr(1)?);
	let (sa, sb) = (strip(a), strip(b));
	let (var, other, commuted) = if (*sa).type_ == pg_sys::NodeTag::T_Var && usable(b) {
		(sa as *mut pg_sys::Var, b, false)
	} else if (*sb).type_ == pg_sys::NodeTag::T_Var && usable(a) {
		(sb as *mut pg_sys::Var, a, true)
	} else {
		return None;
	};
	if (*var).varno as pg_sys::Index != relid || (*var).varattno <= 0 || (*var).varlevelsup != 0 {
		return None;
	}
	let pos = order.atts.iter().position(|&x| x == (*var).varattno as usize - 1)?;
	if (*op).inputcollid != order.colls[pos] {
		return None;
	}
	let opf = order.opfamilies[pos];
	if !pg_sys::op_in_opfamily((*op).opno, opf) {
		return None;
	}
	let (mut strategy, mut left, mut right) = (0i32, pg_sys::InvalidOid, pg_sys::InvalidOid);
	pg_sys::get_op_opfamily_properties((*op).opno, opf, false, &mut strategy, &mut left, &mut right);
	if commuted {
		strategy = 6 - strategy;
		std::mem::swap(&mut left, &mut right);
	}
	let proc_ = pg_sys::get_opfamily_proc(opf, left, right, BTORDER_PROC);
	if proc_ == pg_sys::InvalidOid {
		return None;
	}
	let bound = match strategy {
		1 => Bound::Hi { strict: true },
		2 => Bound::Hi { strict: false },
		3 => Bound::Eq,
		4 => Bound::Lo { strict: false },
		5 => Bound::Lo { strict: true },
		_ => return None,
	};
	Some((Term { pos, bound, proc_, expr: 0 }, other))
}

/// `sort column = ANY(array)`, the column's own equality with an array of its own type: an IN
/// list. Anything else (`<> ALL`, a cross-type array, a comparison other than equality) is left to
/// the executor.
unsafe fn in_term_of(
	order: &Order,
	sa: *mut pg_sys::ScalarArrayOpExpr,
	relid: pg_sys::Index,
	strip: &dyn Fn(*mut pg_sys::Node) -> *mut pg_sys::Node,
	usable: &dyn Fn(*mut pg_sys::Node) -> bool,
) -> Option<(Term, *mut pg_sys::Node)> {
	if !(*sa).useOr {
		return None;
	}
	let args = PgList::<pg_sys::Node>::from_pg((*sa).args);
	if args.len() != 2 {
		return None;
	}
	let (a, b) = (strip(args.get_ptr(0)?), args.get_ptr(1)?);
	if (*a).type_ != pg_sys::NodeTag::T_Var || !usable(b) {
		return None;
	}
	let var = a as *mut pg_sys::Var;
	if (*var).varno as pg_sys::Index != relid || (*var).varattno <= 0 || (*var).varlevelsup != 0 {
		return None;
	}
	let pos = order.atts.iter().position(|&x| x == (*var).varattno as usize - 1)?;
	if (*sa).inputcollid != order.colls[pos] {
		return None;
	}
	let opf = order.opfamilies[pos];
	if !pg_sys::op_in_opfamily((*sa).opno, opf) {
		return None;
	}
	let (mut strategy, mut left, mut right) = (0i32, pg_sys::InvalidOid, pg_sys::InvalidOid);
	pg_sys::get_op_opfamily_properties((*sa).opno, opf, false, &mut strategy, &mut left, &mut right);
	// the values are sorted with the column's own comparison, so they must be of its type
	if strategy != 3 || left != right || left != order.types[pos] {
		return None;
	}
	let proc_ = pg_sys::get_opfamily_proc(opf, left, right, BTORDER_PROC);
	if proc_ == pg_sys::InvalidOid {
		return None;
	}
	Some((Term { pos, bound: Bound::In, proc_, expr: 0 }, b))
}

/// The seeks one scan makes, for its terms and their values: `hull`, one seek whose range holds
/// every row the terms allow (what a scan in the sort order merges its late rows by), and, when a
/// term is an IN list, `each`, one seek per distinct value (what row groups and rows are read by).
/// `each` is empty when `hull` is exact. None when the list holds no value: no row matches.
///
/// # Safety
/// Each value must be a Datum of its term's type (an array for an IN list), and outlive the seeks.
pub unsafe fn seeks(order: &Order, terms: &[Term], values: &[Option<pg_sys::Datum>]) -> Option<(Seek, Vec<Seek>)> {
	let Some(t_in) = terms.iter().copied().find(|t| t.bound == Bound::In) else {
		return Some((Seek::new(order, terms, values), Vec::new()));
	};
	let Some(array) = values.get(t_in.expr).copied().flatten() else {
		// not known yet (planning) or NULL: seek by the equalities before it
		let before: Vec<Term> = terms.iter().copied().filter(|t| t.pos < t_in.pos).collect();
		return Some((Seek::new(order, &before, values), Vec::new()));
	};
	let mut items = array_values(array);
	if items.is_empty() {
		return None;
	}
	items.sort_by(|a, b| order.cmp(t_in.pos, Some(*a), Some(*b)));
	items.dedup_by(|a, b| order.cmp(t_in.pos, Some(*a), Some(*b)) == Ordering::Equal);
	let mut hull_values = values.to_vec();
	let at = hull_values.len();
	hull_values.push(items.first().copied());
	hull_values.push(items.last().copied());
	let mut hull_terms: Vec<Term> = terms.iter().copied().filter(|t| t.bound == Bound::Eq && t.pos < t_in.pos).collect();
	hull_terms.push(Term { bound: Bound::Lo { strict: false }, expr: at, ..t_in });
	hull_terms.push(Term { bound: Bound::Hi { strict: false }, expr: at + 1, ..t_in });
	let hull = Seek::new(order, &hull_terms, &hull_values);
	let each_terms: Vec<Term> = terms.iter().map(|&t| if t.bound == Bound::In { Term { bound: Bound::Eq, ..t } } else { t }).collect();
	let each = items
		.into_iter()
		.map(|v| {
			let mut vs = values.to_vec();
			vs[t_in.expr] = Some(v);
			Seek::new(order, &each_terms, &vs)
		})
		.collect();
	Some((hull, each))
}

/// The non-NULL elements of a one-dimensional array Datum, pointing into it.
unsafe fn array_values(array: pg_sys::Datum) -> Vec<pg_sys::Datum> {
	let a = pg_sys::pg_detoast_datum(array.cast_mut_ptr()) as *mut pg_sys::ArrayType;
	let elem = (*a).elemtype;
	let (mut len, mut byval, mut align) = (0i16, false, 0 as std::ffi::c_char);
	pg_sys::get_typlenbyvalalign(elem, &mut len, &mut byval, &mut align);
	let (mut datums, mut nulls, mut n) = (std::ptr::null_mut(), std::ptr::null_mut(), 0i32);
	pg_sys::deconstruct_array(a, elem, len as i32, byval, align, &mut datums, &mut nulls, &mut n);
	(0..n as usize).filter(|&i| !*nulls.add(i)).map(|i| *datums.add(i)).collect()
}

/// The terms with their values: what a scan seeks by.
pub struct Seek {
	/// per sort column of the equality run, its value
	eq: Vec<(pg_sys::FmgrInfo, pg_sys::Oid, pg_sys::Datum)>,
	lo: Vec<(pg_sys::FmgrInfo, pg_sys::Oid, pg_sys::Datum, bool)>,
	hi: Vec<(pg_sys::FmgrInfo, pg_sys::Oid, pg_sys::Datum, bool)>,
}

impl Seek {
	/// `values[t.expr]` is each term's value, None when it could not be known (planning) or was
	/// NULL (the caller reads nothing then); a term without a value is left out.
	///
	/// # Safety
	/// Each value must be a Datum of its term's type.
	pub unsafe fn new(order: &Order, terms: &[Term], values: &[Option<pg_sys::Datum>]) -> Seek {
		let finfo = |p: pg_sys::Oid| {
			let mut f = pg_sys::FmgrInfo::default();
			pg_sys::fmgr_info(p, &mut f);
			f
		};
		let mut s = Seek { eq: vec![], lo: vec![], hi: vec![] };
		let mut eq: Vec<Option<(pg_sys::FmgrInfo, pg_sys::Oid, pg_sys::Datum)>> = Vec::new();
		for t in terms {
			let Some(v) = values.get(t.expr).copied().flatten() else { continue };
			let coll = order.colls[t.pos];
			match t.bound {
				Bound::Eq => {
					if eq.len() <= t.pos {
						eq.resize_with(t.pos + 1, || None);
					}
					if eq[t.pos].is_none() {
						eq[t.pos] = Some((finfo(t.proc_), coll, v));
					}
				}
				// an IN list not expanded by `seeks` is an equality whose value is unknown
				Bound::In => {
					if eq.len() <= t.pos {
						eq.resize_with(t.pos + 1, || None);
					}
				}
				Bound::Lo { .. } | Bound::Hi { .. } => {}
			}
		}
		// an equality whose value is unknown ends the run: what follows it cannot be sought
		let run = eq.iter().take_while(|e| e.is_some()).count();
		s.eq = eq.into_iter().take(run).flatten().collect();
		// the range is on the column after the run, and only there
		for t in terms.iter().filter(|t| t.pos == run) {
			let Some(v) = values.get(t.expr).copied().flatten() else { continue };
			let coll = order.colls[t.pos];
			match t.bound {
				Bound::Lo { strict } => s.lo.push((finfo(t.proc_), coll, v, strict)),
				Bound::Hi { strict } => s.hi.push((finfo(t.proc_), coll, v, strict)),
				_ => {}
			}
		}
		s
	}

	pub fn is_empty(&self) -> bool {
		self.eq.is_empty() && self.lo.is_empty() && self.hi.is_empty()
	}

	/// The value the first sort column must equal, when the seek has one.
	pub fn first_eq(&self) -> Option<pg_sys::Datum> {
		self.eq.first().map(|e| e.2)
	}

	/// The value the first sort column must equal, when that is ALL the seek asks: no equality on
	/// a later column and no range. Such a seek's rows are one value's rows in order, which a late-
	/// rows index can stream (`scan.rs`, `DeltaStream`).
	pub fn first_eq_only(&self) -> Option<pg_sys::Datum> {
		(self.eq.len() == 1 && self.lo.is_empty() && self.hi.is_empty()).then(|| self.eq[0].2)
	}

	/// Does every key at or before `key` fail the seek (is `key` below its range)?
	pub unsafe fn below(&mut self, key: &dyn Fn(usize) -> Option<pg_sys::Datum>) -> bool {
		for (i, (f, coll, v)) in self.eq.iter_mut().enumerate() {
			match key(i) {
				None => return false,
				Some(x) => match call(f, *coll, x, *v) {
					Ordering::Less => return true,
					Ordering::Greater => return false,
					Ordering::Equal => {}
				},
			}
		}
		let m = self.eq.len();
		self.lo.iter_mut().any(|(f, coll, v, strict)| match key(m) {
			None => false,
			Some(x) => match call(f, *coll, x, *v) {
				Ordering::Less => true,
				Ordering::Equal => *strict,
				Ordering::Greater => false,
			},
		})
	}

	/// Does every key at or after `key` fail the seek (is `key` above its range)?
	pub unsafe fn above(&mut self, key: &dyn Fn(usize) -> Option<pg_sys::Datum>) -> bool {
		for (i, (f, coll, v)) in self.eq.iter_mut().enumerate() {
			match key(i) {
				None => return true,
				Some(x) => match call(f, *coll, x, *v) {
					Ordering::Greater => return true,
					Ordering::Less => return false,
					Ordering::Equal => {}
				},
			}
		}
		let m = self.eq.len();
		self.hi.iter_mut().any(|(f, coll, v, strict)| match key(m) {
			None => true,
			Some(x) => match call(f, *coll, x, *v) {
				Ordering::Greater => true,
				Ordering::Equal => *strict,
				Ordering::Less => false,
			},
		})
	}

	/// The row groups that can hold a key in the range: a run, since they follow on in order.
	///
	/// # Safety
	/// The store's relation must be open.
	pub unsafe fn groups(&mut self, store: &Store) -> Range<usize> {
		let n = store.dir.len();
		if self.is_empty() {
			return 0..n;
		}
		let start = first(n, |g| !self.below(&|i| store.bounds(g).1[i]));
		let end = first(n, |g| self.above(&|i| store.bounds(g).0[i]));
		start..end.max(start)
	}

	/// The rows of `g` (numbered in the whole store) whose keys can be in the range.
	///
	/// # Safety
	/// The store's relation must be open; `order` must be the store's.
	pub unsafe fn rows(&mut self, order: &Order, store: &Store, g: &Group) -> Range<u64> {
		let (lo, hi) = (g.first_row, g.first_row + g.rows);
		if self.is_empty() {
			return lo..hi;
		}
		let (lo_key, hi_key) = store.bounds(g.index());
		// no row in the range: nothing to decode (an IN list's other values, mostly)
		if self.above(&|i| lo_key[i]) || self.below(&|i| hi_key[i]) {
			return lo..lo;
		}
		// every row in the range: nothing to decode
		if !self.below(&|i| lo_key[i]) && !self.above(&|i| hi_key[i]) {
			return lo..hi;
		}
		// Where the directory records the runs of the leading sort columns (all but the last),
		// each run's leading values are known without decoding them, so the search is over runs
		// by their last keys, then inside one run by the last sort column alone: in a store
		// sorted by host then time, one host's hour decodes the time column and never the host
		// column (TSBS's one- and eight-host queries, 2026-09-25).
		if let (Some(runs), true) = (store.runs(g.index()), order.atts.len() >= 2) {
			let lead = order.atts.len() - 1;
			let nruns = runs.len() + 1;
			let end_row = |k: usize| if k < runs.len() { runs[k].0 as usize + 1 } else { g.rows as usize };
			let start_row = |k: usize| if k == 0 { 0 } else { runs[k - 1].0 as usize + 1 };
			let end_key = |k: usize| -> &[Option<pg_sys::Datum>] { if k < runs.len() { &runs[k].1 } else { hi_key } };
			let within = |k: usize, r: usize| {
				let key = end_key(k);
				move |i: usize| if i < lead { key[i] } else { g.value(order.atts[lead], lo + r as u64) }
			};
			// the first row not below the range, and the first above it, each in the first run
			// whose last key is not below (is above) it
			let ks = first(nruns, |k| !self.below(&|i| end_key(k)[i]));
			let start = if ks == nruns {
				g.rows as usize
			} else {
				let (a, b) = (start_row(ks), end_row(ks));
				a + first(b - a, |r| !self.below(&within(ks, a + r)))
			};
			let ke = first(nruns, |k| self.above(&|i| end_key(k)[i]));
			let end = if ke == nruns {
				g.rows as usize
			} else {
				let (a, b) = (start_row(ke), end_row(ke));
				a + first(b - a, |r| self.above(&within(ke, a + r)))
			};
			return lo + start as u64..lo + end.max(start) as u64;
		}
		// a sort column whose first and last value in the group are the same holds that value
		// in every row between them, so it is read from the directory, not decoded (in a store
		// sorted by host then time, the host of most groups)
		let fixed: Vec<bool> = (0..order.atts.len()).map(|i| order.cmp(i, lo_key[i], hi_key[i]) == Ordering::Equal).collect();
		let key = |n: u64| {
			let fixed = &fixed;
			move |i: usize| if fixed[i] { lo_key[i] } else { g.value(order.atts[i], n) }
		};
		let start = lo + first(g.rows as usize, |r| !self.below(&key(lo + r as u64))) as u64;
		let end = lo + first(g.rows as usize, |r| self.above(&key(lo + r as u64))) as u64;
		start..end.max(start)
	}
}

/// The first of `0..n` for which `p` holds, `p` being false and then true (n if never).
fn first(n: usize, mut p: impl FnMut(usize) -> bool) -> usize {
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
