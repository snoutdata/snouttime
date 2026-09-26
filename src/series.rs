//! Series tables (PLAN.md Phase 1.1): turning a table into a natively range-partitioned
//! one on its time column, and back out of the catalog again.
//!
//! The SQL is in `series.sql`. It is PL/pgSQL rather than Rust because every step is DDL
//! Postgres already knows how to do, run in one transaction so that any failure leaves
//! the table exactly as it was; Rust earns its place where there is real computation
//! (encodings, scans, the worker), not in string-building DDL.

pgrx::extension_sql_file!("series.sql", name = "series", requires = ["catalog"]);
