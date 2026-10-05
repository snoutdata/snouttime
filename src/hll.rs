//! Distinct-count sketches: HyperLogLog.
//!
//! The registers are the classic HyperLogLog of Flajolet, Fusy, Gandouet and Meunier (2007):
//! 2^p of them, each holding the most leading zeros (plus one) seen in the hashes routed to it.
//! The estimate is Otmar Ertl's improved raw estimator from "New cardinality estimation
//! algorithms for HyperLogLog sketches" (2017), which is accurate from a handful of values to
//! billions with no empirical bias tables and no switch between estimators, and which with a
//! 64-bit hash needs no large-range correction either.
//!
//! Values are hashed with their type's own 64-bit extended hash function (the one hash
//! partitioning uses), so any hashable type works and a collation is honoured. Two sketches
//! merge by taking the larger register, which is exact: the merge of two sketches is the
//! sketch of the union.
//!
//! SQL:
//!
//! * `distinct_sketch(value anyelement [, bits integer])` aggregate → `hll`: 2^bits registers,
//!   bits 4..18, default 12 (4,096 registers, a standard error of about 1.6%). Not called
//!   `precision`, which SQL reserves.
//! * `distinct_count(hll)` → bigint
//! * `merge(hll)` aggregate → one `hll` from many

use pgrx::prelude::*;
use pgrx::{InOutFuncs, StringInfo};
use serde::{Deserialize, Serialize};

pub const DEFAULT_PRECISION: u8 = 12;

/// The pure sketch: no Postgres in it, tested on its own.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, PostgresType)]
#[inoutfuncs]
pub struct Hll {
	precision: u8,
	registers: Vec<u8>,
}

impl Hll {
	pub fn new(precision: u8) -> Hll {
		if !(4..=18).contains(&precision) {
			error!("a distinct sketch's precision must be between 4 and 18, not {precision}");
		}
		Hll { precision, registers: vec![0; 1 << precision] }
	}

	/// The largest value a register can hold: every bit after the index was zero.
	fn register_max(&self) -> u8 {
		64 - self.precision + 1
	}

	pub fn add_hash(&mut self, h: u64) {
		let p = self.precision as u32;
		let index = (h >> (64 - p)) as usize;
		let rest = h << p;
		let rho = (rest.leading_zeros() + 1).min(self.register_max() as u32) as u8;
		if rho > self.registers[index] {
			self.registers[index] = rho;
		}
	}

	/// Pure, so it does not raise: the SQL layer turns the Err into an ERROR (see `merge_or_error`).
	pub fn merge(&mut self, other: &Hll) -> Result<(), String> {
		if other.precision != self.precision {
			return Err(format!(
				"distinct sketches of different precision cannot be merged ({} and {})",
				self.precision, other.precision
			));
		}
		for (a, b) in self.registers.iter_mut().zip(other.registers.iter()) {
			*a = (*a).max(*b);
		}
		Ok(())
	}

	fn merge_or_error(&mut self, other: &Hll) {
		if let Err(why) = self.merge(other) {
			error!("{why}");
		}
	}

	/// Ertl's improved raw estimator (Algorithm 6 of the paper).
	pub fn estimate(&self) -> f64 {
		let m = self.registers.len() as f64;
		let q = 64 - self.precision as usize;
		let mut c = vec![0.0f64; q + 2];
		for &r in &self.registers {
			c[r as usize] += 1.0;
		}
		let mut z = m * tau(1.0 - c[q + 1] / m);
		for k in (1..=q).rev() {
			z = 0.5 * (z + c[k]);
		}
		z += m * sigma(c[0] / m);
		m * m / (2.0 * std::f64::consts::LN_2) / z
	}

	pub fn check(&self) -> Result<(), &'static str> {
		if !(4..=18).contains(&self.precision) {
			return Err("precision must be between 4 and 18");
		}
		if self.registers.len() != 1 << self.precision {
			return Err("the number of registers does not match the precision");
		}
		if self.registers.iter().any(|&r| r > self.register_max()) {
			return Err("a register is larger than a hash can make it");
		}
		Ok(())
	}

	fn checked(self) -> Hll {
		if let Err(why) = self.check() {
			error!("invalid hll: {why}");
		}
		self
	}
}

/// σ(x) = x + Σ_{k≥1} x^(2^k) · 2^(k−1), summed until it stops changing.
fn sigma(mut x: f64) -> f64 {
	if x == 1.0 {
		return f64::INFINITY;
	}
	let (mut y, mut z) = (1.0, x);
	loop {
		x *= x;
		let before = z;
		z += x * y;
		y += y;
		if z == before {
			return z;
		}
	}
}

/// τ(x) = (1 − x − Σ_{k≥1} (1 − x^(2^−k))² · 2^−k) / 3, summed until it stops changing.
fn tau(mut x: f64) -> f64 {
	if x == 0.0 || x == 1.0 {
		return 0.0;
	}
	let (mut y, mut z) = (1.0, 1.0 - x);
	loop {
		x = x.sqrt();
		let before = z;
		y *= 0.5;
		z -= (1.0 - x).powi(2) * y;
		if z == before {
			return z / 3.0;
		}
	}
}

/// The text form: `{"precision": 12, "registers": "<hex, one byte per register>"}`.
#[derive(Serialize, Deserialize)]
struct HllText {
	precision: u8,
	registers: String,
}

impl InOutFuncs for Hll {
	fn input(input: &core::ffi::CStr) -> Self {
		let text = input.to_str().unwrap_or_else(|_| error!("invalid hll: not UTF-8"));
		let t: HllText = serde_json::from_str(text).unwrap_or_else(|e| error!("invalid hll: {e}"));
		let hex = t.registers.as_bytes();
		if hex.len() % 2 != 0 {
			error!("invalid hll: registers must be hex, two digits each");
		}
		let digit = |c: u8| match c {
			b'0'..=b'9' => c - b'0',
			b'a'..=b'f' => c - b'a' + 10,
			b'A'..=b'F' => c - b'A' + 10,
			_ => error!("invalid hll: registers must be hex"),
		};
		let registers = hex.chunks_exact(2).map(|p| digit(p[0]) * 16 + digit(p[1])).collect();
		Hll { precision: t.precision, registers }.checked()
	}

	fn output(&self, buffer: &mut StringInfo) {
		let mut hex = String::with_capacity(self.registers.len() * 2);
		for r in &self.registers {
			hex.push_str(&format!("{r:02x}"));
		}
		buffer.push_str(&serde_json::to_string(&HllText { precision: self.precision, registers: hex }).unwrap());
	}
}

// ---- the aggregates ----

unsafe fn agg_context(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::MemoryContext {
	let mut ctx: pg_sys::MemoryContext = std::ptr::null_mut();
	if pg_sys::AggCheckCallContext(fcinfo, &mut ctx) == 0 {
		error!("snouttime sketch support functions may only be called by an aggregate");
	}
	ctx
}

unsafe fn arg(fcinfo: pg_sys::FunctionCallInfo, n: usize) -> (pg_sys::Datum, bool) {
	let a = (*fcinfo).args.as_slice(n + 1)[n];
	(a.value, a.isnull)
}

unsafe fn state(fcinfo: pg_sys::FunctionCallInfo, precision: u8) -> *mut Hll {
	let ctx = agg_context(fcinfo);
	let (s, isnull) = arg(fcinfo, 0);
	if isnull {
		pgrx::PgMemoryContexts::For(ctx).leak_and_drop_on_delete(Hll::new(precision))
	} else {
		s.cast_mut_ptr::<Hll>()
	}
}

/// The value type's 64-bit extended hash, looked up once per call site.
unsafe fn hasher(fcinfo: pg_sys::FunctionCallInfo) -> *mut pg_sys::FmgrInfo {
	let flinfo = (*fcinfo).flinfo;
	if (*flinfo).fn_extra.is_null() {
		let typid = pg_sys::get_fn_expr_argtype(flinfo, 1);
		let entry = pg_sys::lookup_type_cache(typid, pg_sys::TYPECACHE_HASH_EXTENDED_PROC_FINFO as i32);
		if (*entry).hash_extended_proc_finfo.fn_oid == pg_sys::InvalidOid {
			error!("values of this type cannot be counted: it has no hash function");
		}
		(*flinfo).fn_extra = (&mut (*entry).hash_extended_proc_finfo as *mut pg_sys::FmgrInfo).cast();
	}
	(*flinfo).fn_extra.cast()
}

/// (internal, value anyelement [, precision int]) -> internal
unsafe fn sketch_trans(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum {
	let precision = if (*fcinfo).nargs > 2 {
		let (p, pnull) = arg(fcinfo, 2);
		if pnull {
			error!("a distinct sketch's bits must not be NULL");
		}
		let p = p.value() as i32;
		if !(4..=18).contains(&p) {
			error!("a distinct sketch's bits must be between 4 and 18, not {p}");
		}
		p as u8
	} else {
		DEFAULT_PRECISION
	};
	let h = state(fcinfo, precision);
	if (*h).precision != precision {
		error!("a distinct sketch's bits must be the same for every row of a group");
	}
	let (v, vnull) = arg(fcinfo, 1);
	if !vnull {
		let hash = pg_sys::FunctionCall2Coll(hasher(fcinfo), (*fcinfo).fncollation, v, pg_sys::Datum::from(0i64));
		(*h).add_hash(hash.value() as u64);
	}
	pg_sys::Datum::from(h)
}

/// (internal, hll) -> internal
unsafe fn merge_trans(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum {
	let (v, vnull) = arg(fcinfo, 1);
	let incoming = if vnull { None } else { Hll::from_polymorphic_datum(v, false, pg_sys::InvalidOid).map(Hll::checked) };
	let h = state(fcinfo, incoming.as_ref().map(|i| i.precision).unwrap_or(DEFAULT_PRECISION));
	if let Some(i) = incoming {
		(*h).merge_or_error(&i);
	}
	pg_sys::Datum::from(h)
}

/// (internal, internal) -> internal
unsafe fn combine(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum {
	let ctx = agg_context(fcinfo);
	let (a, anull) = arg(fcinfo, 0);
	let (b, bnull) = arg(fcinfo, 1);
	if bnull {
		if anull {
			(*fcinfo).isnull = true;
		}
		return a;
	}
	let b = &*b.cast_mut_ptr::<Hll>();
	if anull {
		return pg_sys::Datum::from(pgrx::PgMemoryContexts::For(ctx).leak_and_drop_on_delete(b.clone()));
	}
	(*a.cast_mut_ptr::<Hll>()).merge_or_error(b);
	a
}

/// (internal) -> bytea: the precision, then one byte per register.
unsafe fn serialize(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum {
	agg_context(fcinfo);
	let h = &*arg(fcinfo, 0).0.cast_mut_ptr::<Hll>();
	let mut bytes = Vec::with_capacity(1 + h.registers.len());
	bytes.push(h.precision);
	bytes.extend_from_slice(&h.registers);
	pg_sys::Datum::from(pgrx::rust_byte_slice_to_bytea(&bytes).into_pg())
}

/// (bytea, internal) -> internal
unsafe fn deserialize(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum {
	let ctx = agg_context(fcinfo);
	let v = pg_sys::pg_detoast_datum_packed(arg(fcinfo, 0).0.cast_mut_ptr());
	let b = pgrx::varlena::varlena_to_byte_slice(v);
	if b.is_empty() {
		error!("invalid serialised hll: empty");
	}
	let h = Hll { precision: b[0], registers: b[1..].to_vec() }.checked();
	pg_sys::Datum::from(pgrx::PgMemoryContexts::For(ctx).leak_and_drop_on_delete(h))
}

/// (internal, ...) -> hll; NULL when no row was seen at all.
unsafe fn finish(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum {
	let (s, snull) = arg(fcinfo, 0);
	if snull {
		(*fcinfo).isnull = true;
		return pg_sys::Datum::from(0);
	}
	(*s.cast_mut_ptr::<Hll>()).clone().into_datum().unwrap()
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

c_function!(snouttime_hll_trans, pg_finfo_snouttime_hll_trans, sketch_trans);
c_function!(snouttime_hll_merge_trans, pg_finfo_snouttime_hll_merge_trans, merge_trans);
c_function!(snouttime_hll_combine, pg_finfo_snouttime_hll_combine, combine);
c_function!(snouttime_hll_serialize, pg_finfo_snouttime_hll_serialize, serialize);
c_function!(snouttime_hll_deserialize, pg_finfo_snouttime_hll_deserialize, deserialize);
c_function!(snouttime_hll_final, pg_finfo_snouttime_hll_final, finish);

/// The estimated number of distinct values the sketch saw.
#[pg_extern(immutable, parallel_safe)]
fn distinct_count(sketch: Hll) -> i64 {
	sketch.checked().estimate().round() as i64
}

pgrx::extension_sql!(
	r#"
CREATE FUNCTION snouttime._hll_trans(internal, anyelement) RETURNS internal
	LANGUAGE c IMMUTABLE PARALLEL SAFE AS 'MODULE_PATHNAME', 'snouttime_hll_trans';
CREATE FUNCTION snouttime._hll_trans(internal, anyelement, integer) RETURNS internal
	LANGUAGE c IMMUTABLE PARALLEL SAFE AS 'MODULE_PATHNAME', 'snouttime_hll_trans';
CREATE FUNCTION snouttime._hll_merge_trans(internal, snouttime.hll) RETURNS internal
	LANGUAGE c IMMUTABLE PARALLEL SAFE AS 'MODULE_PATHNAME', 'snouttime_hll_merge_trans';
CREATE FUNCTION snouttime._hll_combine(internal, internal) RETURNS internal
	LANGUAGE c IMMUTABLE PARALLEL SAFE AS 'MODULE_PATHNAME', 'snouttime_hll_combine';
CREATE FUNCTION snouttime._hll_serialize(internal) RETURNS bytea
	LANGUAGE c STRICT IMMUTABLE PARALLEL SAFE AS 'MODULE_PATHNAME', 'snouttime_hll_serialize';
CREATE FUNCTION snouttime._hll_deserialize(bytea, internal) RETURNS internal
	LANGUAGE c STRICT IMMUTABLE PARALLEL SAFE AS 'MODULE_PATHNAME', 'snouttime_hll_deserialize';
CREATE FUNCTION snouttime._hll_final(internal) RETURNS snouttime.hll
	LANGUAGE c IMMUTABLE PARALLEL SAFE AS 'MODULE_PATHNAME', 'snouttime_hll_final';
CREATE AGGREGATE snouttime.distinct_sketch(value anyelement) (
	SFUNC = snouttime._hll_trans, STYPE = internal, FINALFUNC = snouttime._hll_final,
	COMBINEFUNC = snouttime._hll_combine,
	SERIALFUNC = snouttime._hll_serialize, DESERIALFUNC = snouttime._hll_deserialize,
	PARALLEL = SAFE);
CREATE AGGREGATE snouttime.distinct_sketch(value anyelement, bits integer) (
	SFUNC = snouttime._hll_trans, STYPE = internal, FINALFUNC = snouttime._hll_final,
	COMBINEFUNC = snouttime._hll_combine,
	SERIALFUNC = snouttime._hll_serialize, DESERIALFUNC = snouttime._hll_deserialize,
	PARALLEL = SAFE);
CREATE AGGREGATE snouttime.merge(sketch snouttime.hll) (
	SFUNC = snouttime._hll_merge_trans, STYPE = internal, FINALFUNC = snouttime._hll_final,
	COMBINEFUNC = snouttime._hll_combine,
	SERIALFUNC = snouttime._hll_serialize, DESERIALFUNC = snouttime._hll_deserialize,
	PARALLEL = SAFE);
"#,
	name = "hll_aggregates",
	requires = [Hll]
);

#[cfg(test)]
mod tests {
	use super::Hll;

	/// A 64-bit mixer (splitmix64), standing in for Postgres's hash in tests without a server.
	fn mix(mut x: u64) -> u64 {
		x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
		x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
		x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
		x ^ (x >> 31)
	}

	fn sketch(from: u64, n: u64) -> Hll {
		let mut h = Hll { precision: 12, registers: vec![0; 4096] };
		(from..from + n).for_each(|i| h.add_hash(mix(i)));
		h
	}

	#[test]
	fn estimates_are_close_across_ranges() {
		// 1.6% standard error at p = 12; allow four of them, and exactness when tiny
		for n in [0u64, 1, 10, 100, 1_000, 10_000, 100_000, 1_000_000] {
			let e = sketch(0, n).estimate();
			let rel = if n == 0 { e } else { (e - n as f64).abs() / n as f64 };
			// Small counts are decided by hash collisions (n = 100 in 4,096 registers has about
			// 1.1% standard error); larger ones by the 1.6% of p = 12. Four standard errors each.
			let bound = if n <= 100 { 0.05 } else { 0.065 };
			assert!(rel <= bound, "n={n}: estimate {e}");
		}
	}

	/// While most registers are empty, the estimator must agree with linear counting,
	/// m · ln(m / empty), which is what it converges to there. This checks the FORMULA, since
	/// both read the same registers.
	#[test]
	fn small_counts_agree_with_linear_counting() {
		for n in [10u64, 50, 100, 200, 400] {
			let h = sketch(0, n);
			let m = h.registers.len() as f64;
			let empty = h.registers.iter().filter(|&&r| r == 0).count() as f64;
			let lc = m * (m / empty).ln();
			let e = h.estimate();
			assert!((e - lc).abs() / lc < 0.005, "n={n}: estimate {e}, linear counting {lc}");
		}
	}

	#[test]
	fn merge_is_the_sketch_of_the_union() {
		let mut a = sketch(0, 50_000);
		let b = sketch(25_000, 50_000);
		a.merge(&b).unwrap();
		assert_eq!(a, sketch(0, 75_000));
		assert!(a.merge(&Hll { precision: 10, registers: vec![0; 1024] }).is_err());
	}

	#[test]
	fn duplicates_do_not_count() {
		let mut h = sketch(0, 1000);
		(0..1000u64).for_each(|i| h.add_hash(super::tests::mix(i)));
		assert_eq!(h, sketch(0, 1000));
	}

	#[test]
	fn a_corrupt_sketch_is_refused() {
		assert!(Hll { precision: 12, registers: vec![0; 4095] }.check().is_err());
		assert!(Hll { precision: 3, registers: vec![0; 8] }.check().is_err());
		let mut bad = sketch(0, 10);
		bad.registers[0] = 60;
		assert!(bad.check().is_err());
	}
}
