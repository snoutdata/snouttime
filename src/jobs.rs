//! The jobs a series table needs doing over time (PLAN.md Phase 1.2).
//!
//! The SQL is in `jobs.sql`; `worker.rs` is the background worker that calls it.

pgrx::extension_sql_file!("jobs.sql", name = "jobs", requires = ["catalog", "series"]);

pgrx::extension_sql_file!("seal.sql", name = "seal", requires = ["catalog", "series", "jobs", "columnar"]);
pgrx::extension_sql_file!("rollup.sql", name = "rollup", requires = ["catalog", "series", "jobs", "seal"]);
