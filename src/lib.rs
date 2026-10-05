//! SnoutTime: time-series storage for Postgres.
//!
//! The rules referenced as `R<n>` throughout this crate are in `CONTRIBUTING.md`.

use pgrx::prelude::*;

::pgrx::pg_module_magic!(name, version);

mod catalog;
mod series;
mod jobs;
mod info;
mod worker;
mod bucket;
mod fill;
mod point;
mod tdigest;
mod hll;
mod asof;
pub mod codec;
mod s3;
mod columnar;

/// Attribute `i` (0-based) of a tuple descriptor. PG 18 keeps the full attributes after a
/// compact array and reaches them through `TupleDescAttr`; PG 17 has them as an array field.
///
/// # Safety
/// `desc` must be valid and `i` below its `natts`.
pub(crate) unsafe fn tuple_attr<'a>(desc: pg_sys::TupleDesc, i: usize) -> &'a pg_sys::FormData_pg_attribute {
	#[cfg(feature = "pg18")]
	{
		&*pg_sys::TupleDescAttr(desc, i as i32)
	}
	#[cfg(not(feature = "pg18"))]
	{
		&(*desc).attrs.as_slice((*desc).natts as usize)[i]
	}
}

/// Every attribute of a tuple descriptor, copied.
///
/// # Safety
/// `desc` must be valid.
pub(crate) unsafe fn tuple_attrs(desc: pg_sys::TupleDesc) -> Vec<pg_sys::FormData_pg_attribute> {
	(0..(*desc).natts as usize).map(|i| *tuple_attr(desc, i)).collect()
}

/// Called once when the library is loaded into a backend, and by the postmaster when
/// `snouttime` is in `shared_preload_libraries` (which is what lets it register workers).
#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
	columnar::init();
	worker::init();
}

/// The version of the loaded library, which is what a bug report needs. It can differ from
/// `pg_extension.extversion` between installing a new build and running
/// `ALTER EXTENSION snouttime UPDATE`.
#[pg_extern(immutable, parallel_safe)]
fn version() -> &'static str {
	env!("CARGO_PKG_VERSION")
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
	use pgrx::prelude::*;

	#[pg_test]
	fn version_is_the_crate_version() {
		assert_eq!(crate::version(), env!("CARGO_PKG_VERSION"));
	}

	#[pg_test]
	fn version_is_callable_from_sql() {
		let v = Spi::get_one::<&str>("SELECT snouttime.version()").unwrap();
		assert_eq!(v, Some(env!("CARGO_PKG_VERSION")));
	}
}

/// Required by `cargo pgrx test`; must sit at the crate root.
#[cfg(test)]
pub mod pg_test {
	pub fn setup(_options: Vec<&str>) {}

	#[must_use]
	pub fn postgresql_conf_options() -> Vec<&'static str> {
		vec![]
	}
}

