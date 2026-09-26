//! What is there and what it costs (PLAN.md Phase 1.5): three views over the catalog.
//!
//! The SQL is in `info.sql`.

pgrx::extension_sql_file!("info.sql", name = "info", requires = ["catalog", "series", "jobs", "seal"]);
