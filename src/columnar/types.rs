//! How each column's values become [`format::Values`] and come back as Datums.
//!
//! The typed encodings are for the types a time series is mostly made of: integers and every
//! time type as int64 (delta-of-delta), float4 and float8 as bit patterns (XOR), booleans as a
//! bitmap. Every other type is stored as its own bytes, so no type needs code of its own: a
//! by-value one as its `typlen` low bytes, a fixed-length by-reference one as its `typlen`
//! bytes, a varlena detoasted and decompressed without its header, and a cstring without its
//! terminator. Datums are rebuilt the way `fetch_att` builds them, sign extension included, so
//! a value read back compares equal, word for word, to one read from a heap.

use pgrx::pg_sys;

use super::format::{Decoded, Values};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
	Int16,
	Int32,
	Int64,
	Float4,
	Float8,
	Bool,
	/// Stored as bytes: `len` is `typlen`.
	Bytes { len: i16, byval: bool },
	Dropped,
}

pub fn kind_of(att: &pg_sys::FormData_pg_attribute) -> Kind {
	if att.attisdropped {
		return Kind::Dropped;
	}
	match att.atttypid {
		pg_sys::INT2OID => Kind::Int16,
		pg_sys::INT4OID | pg_sys::DATEOID => Kind::Int32,
		pg_sys::INT8OID | pg_sys::TIMEOID | pg_sys::TIMESTAMPOID | pg_sys::TIMESTAMPTZOID => Kind::Int64,
		pg_sys::FLOAT4OID => Kind::Float4,
		pg_sys::FLOAT8OID => Kind::Float8,
		pg_sys::BOOLOID => Kind::Bool,
		_ => Kind::Bytes { len: att.attlen, byval: att.attbyval },
	}
}

/// The kind of a column stored with type `typid` (0: dropped when it was stored).
pub fn kind_of_type(typid: u32) -> Kind {
	if typid == 0 {
		return Kind::Dropped;
	}
	let oid = pg_sys::Oid::from(typid);
	match oid {
		pg_sys::INT2OID => Kind::Int16,
		pg_sys::INT4OID | pg_sys::DATEOID => Kind::Int32,
		pg_sys::INT8OID | pg_sys::TIMEOID | pg_sys::TIMESTAMPOID | pg_sys::TIMESTAMPTZOID => Kind::Int64,
		pg_sys::FLOAT4OID => Kind::Float4,
		pg_sys::FLOAT8OID => Kind::Float8,
		pg_sys::BOOLOID => Kind::Bool,
		_ => {
			let mut len = 0i16;
			let mut byval = false;
			unsafe { pg_sys::get_typlenbyval(oid, &mut len, &mut byval) };
			Kind::Bytes { len, byval }
		}
	}
}

/// The attributes of a tuple descriptor.
///
/// # Safety
/// `desc` must be a valid tuple descriptor.
pub unsafe fn attrs(desc: pg_sys::TupleDesc) -> Vec<pg_sys::FormData_pg_attribute> {
	crate::tuple_attrs(desc)
}

/// A column being filled row by row.
pub struct Acc {
	pub kind: Kind,
	pub nulls: Vec<bool>,
	pub values: Values,
	/// Bytes of values so far, to cut a row group before it gets too large.
	pub bytes: usize,
}

impl Acc {
	pub fn new(kind: Kind) -> Acc {
		let values = match kind {
			Kind::Int16 | Kind::Int32 | Kind::Int64 | Kind::Dropped => Values::Int(Vec::new()),
			Kind::Float4 | Kind::Float8 => Values::Float(Vec::new()),
			Kind::Bool => Values::Bool(Vec::new()),
			Kind::Bytes { .. } => Values::Bytes(Vec::new()),
		};
		Acc { kind, nulls: Vec::new(), values, bytes: 0 }
	}

	/// # Safety
	/// `d` must be a valid Datum of this column's type unless `null`.
	pub unsafe fn push(&mut self, d: pg_sys::Datum, null: bool) {
		if null || self.kind == Kind::Dropped {
			self.nulls.push(true);
			return;
		}
		self.nulls.push(false);
		let w = d.value() as u64;
		match (&mut self.values, self.kind) {
			(Values::Int(v), Kind::Int16) => v.push(w as i16 as i64),
			(Values::Int(v), Kind::Int32) => v.push(w as i32 as i64),
			(Values::Int(v), _) => v.push(w as i64),
			(Values::Float(v), Kind::Float4) => v.push(w as u32 as u64),
			(Values::Float(v), _) => v.push(w),
			(Values::Bool(v), _) => v.push(w & 0xff != 0),
			(Values::Bytes(v), Kind::Bytes { len, byval }) => {
				let b = bytes_of(d, len, byval);
				self.bytes += b.len();
				v.push(b);
			}
			_ => unreachable!("an accumulator holds the values its kind says"),
		}
		self.bytes += 8;
	}

	/// The first row's value (`first`) or the last's, as a sort key's bytes: what
	/// [`datum_of_bytes`] turns back into the Datum, with [`key_layout`]. None: NULL.
	pub fn key_bytes(&self, first: bool) -> Option<Vec<u8>> {
		let null = if first { *self.nulls.first()? } else { *self.nulls.last()? };
		if null {
			return None;
		}
		let (len, _) = key_layout(self.kind);
		let word = |w: u64| w.to_le_bytes()[..len as usize].to_vec();
		Some(match &self.values {
			Values::Int(v) => word(*(if first { v.first() } else { v.last() })? as u64),
			Values::Float(v) => word(*(if first { v.first() } else { v.last() })?),
			Values::Bool(v) => vec![*(if first { v.first() } else { v.last() })? as u8],
			Values::Bytes(v) => (if first { v.first() } else { v.last() })?.clone(),
		})
	}

	/// Whether the last two rows pushed hold the same value: both NULL, or equal (bytes equal,
	/// for a type stored as bytes).
	pub fn last_two_equal(&self) -> bool {
		let n = self.nulls.len();
		if n < 2 {
			return true;
		}
		match (self.nulls[n - 2], self.nulls[n - 1]) {
			(true, true) => true,
			(false, false) => match &self.values {
				Values::Int(v) => v[v.len() - 1] == v[v.len() - 2],
				Values::Float(v) => v[v.len() - 1] == v[v.len() - 2],
				Values::Bool(v) => v[v.len() - 1] == v[v.len() - 2],
				Values::Bytes(v) => v[v.len() - 1] == v[v.len() - 2],
			},
			_ => false,
		}
	}

	/// The value of the row before the last, as [`Acc::key_bytes`] gives a value.
	pub fn key_bytes_prev(&self) -> Option<Vec<u8>> {
		let n = self.nulls.len();
		if n < 2 || self.nulls[n - 2] {
			return None;
		}
		let (len, _) = key_layout(self.kind);
		let word = |w: u64| w.to_le_bytes()[..len as usize].to_vec();
		// values hold the non-NULL rows only
		let back = if self.nulls[n - 1] { 1 } else { 2 };
		Some(match &self.values {
			Values::Int(v) => word(v[v.len() - back] as u64),
			Values::Float(v) => word(v[v.len() - back]),
			Values::Bool(v) => vec![v[v.len() - back] as u8],
			Values::Bytes(v) => v[v.len() - back].clone(),
		})
	}

	pub fn take(&mut self) -> super::format::Column {
		let values = std::mem::replace(&mut self.values, Acc::new(self.kind).values);
		self.bytes = 0;
		super::format::Column { nulls: std::mem::take(&mut self.nulls), values }
	}
}

unsafe fn bytes_of(d: pg_sys::Datum, len: i16, byval: bool) -> Vec<u8> {
	if byval {
		return (d.value() as u64).to_le_bytes()[..len as usize].to_vec();
	}
	match len {
		-1 => {
			let v = pg_sys::pg_detoast_datum(d.cast_mut_ptr());
			let total = varsize(v);
			std::slice::from_raw_parts((v as *const u8).add(4), total - 4).to_vec()
		}
		-2 => std::ffi::CStr::from_ptr(d.cast_mut_ptr()).to_bytes().to_vec(),
		n => std::slice::from_raw_parts(d.cast_mut_ptr::<u8>(), n as usize).to_vec(),
	}
}

/// A stored kind's `typlen` and `typbyval`, for [`datum_of_bytes`].
pub fn key_layout(kind: Kind) -> (i16, bool) {
	match kind {
		Kind::Int16 => (2, true),
		Kind::Int32 | Kind::Float4 => (4, true),
		Kind::Int64 | Kind::Float8 | Kind::Dropped => (8, true),
		Kind::Bool => (1, true),
		Kind::Bytes { len, byval } => (len, byval),
	}
}

/// VARSIZE of a 4-byte-header varlena, little-endian (the only byte order built for).
#[cfg(target_endian = "little")]
unsafe fn varsize(v: *const pg_sys::varlena) -> usize {
	let header = std::ptr::read_unaligned(v as *const u32);
	((header >> 2) & 0x3FFF_FFFF) as usize
}

/// A by-reference value's memory: 8-byte aligned (as Postgres aligns the values it hands out),
/// owned by Rust, so a decoded row group needs no Postgres memory context to outlive anything.
pub type Mem = Vec<Box<[u64]>>;

unsafe fn alloc(mem: &mut Mem, bytes: usize) -> *mut u8 {
	let words = bytes.div_ceil(8).max(1);
	let mut b = vec![0u64; words].into_boxed_slice();
	let p = b.as_mut_ptr() as *mut u8;
	mem.push(b);
	p
}

/// A value's Datum; a by-reference one is built in `mem`.
///
/// # Safety
/// The Datum is valid for as long as `mem` holds its buffer.
pub unsafe fn datum_of_bytes(b: &[u8], len: i16, byval: bool, mem: &mut Mem) -> pg_sys::Datum {
	if byval {
		let mut w = [0u8; 8];
		w[..b.len().min(8)].copy_from_slice(&b[..b.len().min(8)]);
		let raw = u64::from_le_bytes(w);
		let v = match len {
			1 => raw as u8 as i8 as i64 as u64,
			2 => raw as u16 as i16 as i64 as u64,
			4 => raw as u32 as i32 as i64 as u64,
			_ => raw,
		};
		return pg_sys::Datum::from(v as usize);
	}
	match len {
		-1 => {
			let p = alloc(mem, b.len() + 4);
			std::ptr::write_unaligned(p as *mut u32, ((b.len() + 4) as u32) << 2);
			std::ptr::copy_nonoverlapping(b.as_ptr(), p.add(4), b.len());
			pg_sys::Datum::from(p)
		}
		-2 => {
			let p = alloc(mem, b.len() + 1);
			std::ptr::copy_nonoverlapping(b.as_ptr(), p, b.len());
			pg_sys::Datum::from(p)
		}
		n => {
			let p = alloc(mem, n as usize);
			std::ptr::copy_nonoverlapping(b.as_ptr(), p, b.len().min(n as usize));
			pg_sys::Datum::from(p)
		}
	}
}

/// A whole decoded column as Datums, one per row, byref values in `mem`. A dictionary column
/// builds each distinct value once and points every row at it.
///
/// # Safety
/// The Datums are valid for as long as `mem` holds its buffers.
pub unsafe fn datums(kind: Kind, nulls: &[bool], values: &Decoded, mem: &mut Mem) -> Result<Vec<pg_sys::Datum>, &'static str> {
	let mut out = Vec::with_capacity(nulls.len());
	let mismatch = "a column's stored encoding does not fit its type";
	let zero = pg_sys::Datum::from(0usize);
	match (kind, values) {
		(Kind::Int16 | Kind::Int32 | Kind::Int64, Decoded::Int(v)) => {
			let mut it = v.iter();
			for &n in nulls {
				out.push(if n { zero } else { pg_sys::Datum::from(*it.next().unwrap() as u64 as usize) });
			}
		}
		(Kind::Float4, Decoded::Float(v)) => {
			let mut it = v.iter();
			for &n in nulls {
				out.push(if n { zero } else { pg_sys::Datum::from(*it.next().unwrap() as u32 as i32 as i64 as u64 as usize) });
			}
		}
		(Kind::Float8, Decoded::Float(v)) => {
			let mut it = v.iter();
			for &n in nulls {
				out.push(if n { zero } else { pg_sys::Datum::from(*it.next().unwrap() as usize) });
			}
		}
		(Kind::Bool, Decoded::Bool(v)) => {
			let mut it = v.iter();
			for &n in nulls {
				out.push(if n { zero } else { pg_sys::Datum::from(*it.next().unwrap() as usize) });
			}
		}
		(Kind::Bytes { len, byval }, Decoded::Dict { entries, indexes }) => {
			if len > 0 && !byval && entries.iter().any(|e| e.len() != len as usize) {
				return Err("a fixed-length value was stored with a different length");
			}
			let built: Vec<pg_sys::Datum> = entries.iter().map(|e| datum_of_bytes(e, len, byval, mem)).collect();
			let mut it = indexes.iter();
			for &n in nulls {
				out.push(if n { zero } else { built[*it.next().unwrap() as usize] });
			}
		}
		(Kind::Bytes { len, byval }, Decoded::Plain(v)) => {
			if len > 0 && !byval && v.iter().any(|e| e.len() != len as usize) {
				return Err("a fixed-length value was stored with a different length");
			}
			let mut it = v.iter();
			for &n in nulls {
				out.push(if n { zero } else { datum_of_bytes(it.next().unwrap(), len, byval, mem) });
			}
		}
		(Kind::Dropped, _) => out.resize(nulls.len(), zero),
		_ => return Err(mismatch),
	}
	Ok(out)
}
