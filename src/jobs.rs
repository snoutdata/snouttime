//! The jobs a series table needs doing over time (PLAN.md Phase 1.2).
//!
//! The SQL is in `jobs.sql`; `worker.rs` is the background worker that calls it. What lives
//! here is the one part SQL cannot do: running a job as its table's owner in a way the job
//! cannot step out of.

use pgrx::prelude::*;

pgrx::extension_sql_file!("jobs.sql", name = "jobs", requires = ["catalog", "series"]);

pgrx::extension_sql_file!("seal.sql", name = "seal", requires = ["catalog", "series", "jobs", "columnar"]);
pgrx::extension_sql_file!("rollup.sql", name = "rollup", requires = ["catalog", "series", "jobs", "seal"]);

/// Puts the caller's identity back when the job is over, however it ends. On an error this
/// runs while the error unwinds, before the subtransaction of `run_due_job()`'s EXCEPTION block
/// is rolled back (which restores the same identity, and the settings, again).
struct Identity {
	user: pg_sys::Oid,
	context: i32,
}

impl Drop for Identity {
	fn drop(&mut self) {
		unsafe { pg_sys::SetUserIdAndSecContext(self.user, self.context) };
	}
}

/// Runs one job (`snouttime._do_job`) as the owner of its table, in a security-restricted
/// operation, the way Postgres runs VACUUM, ANALYZE, CLUSTER, REINDEX and REFRESH MATERIALIZED
/// VIEW for a table's owner.
///
/// `SET ROLE` is not enough when the caller is a superuser (the worker always is): anything a
/// job runs under the owner's identity (a trigger on a rollup's materialized table fired by the
/// refresh job, a function in an index expression or a default, a rollup's query) could
/// say `RESET ROLE` and carry on as the superuser. Here the identity is switched with
/// `SECURITY_LOCAL_USERID_CHANGE | SECURITY_RESTRICTED_OPERATION`, under which Postgres refuses
/// `SET ROLE`, `RESET ROLE` and `SET SESSION AUTHORIZATION` outright, and the settings get a
/// GUC nest level of their own, rolled back afterwards, with `search_path` restricted to
/// `pg_catalog, pg_temp` as the server does for its own maintenance (Postgres 17+).
///
/// The caller must hold the privileges of the table's owner (a superuser does), so calling
/// this directly gives nobody anything they did not have.
#[pg_extern(sql = r#"
CREATE FUNCTION snouttime._run_job(job_kind text, rel regclass)
RETURNS TABLE (detail text, again boolean)
LANGUAGE c VOLATILE STRICT
AS 'MODULE_PATHNAME', 'run_job_wrapper';
"#)]
fn run_job(
	job_kind: &str,
	rel: pg_sys::Oid,
) -> TableIterator<'static, (name!(detail, Option<String>), name!(again, Option<bool>))> {
	let owner = Spi::get_one_with_args::<pg_sys::Oid>(
		"SELECT c.relowner FROM pg_catalog.pg_class c WHERE c.oid = $1",
		&[rel.into()],
	)
	.unwrap_or(None);
	let Some(owner) = owner else {
		ereport!(
			PgLogLevel::ERROR,
			PgSqlErrorCode::ERRCODE_UNDEFINED_TABLE,
			format!("the table of this {job_kind} job (OID {}) no longer exists", rel.to_u32())
		);
		unreachable!()
	};

	let detail;
	let again;
	unsafe {
		if !pg_sys::has_privs_of_role(pg_sys::GetUserId(), owner) {
			ereport!(
				PgLogLevel::ERROR,
				PgSqlErrorCode::ERRCODE_INSUFFICIENT_PRIVILEGE,
				"permission denied: only a role with the privileges of the table's owner may run its jobs"
			);
		}

		let mut user = pg_sys::InvalidOid;
		let mut context = 0i32;
		pg_sys::GetUserIdAndSecContext(&mut user, &mut context);
		let _restore = Identity { user, context };
		pg_sys::SetUserIdAndSecContext(
			owner,
			context | (pg_sys::SECURITY_LOCAL_USERID_CHANGE | pg_sys::SECURITY_RESTRICTED_OPERATION) as i32,
		);
		let nest = pg_sys::NewGUCNestLevel();
		pg_sys::RestrictSearchPath();

		let (d, a) = Spi::get_two_with_args::<String, bool>(
			"SELECT j.detail, j.again FROM snouttime._do_job($1, $2::pg_catalog.regclass) j",
			&[job_kind.into(), rel.into()],
		)
		.unwrap_or_else(|e| error!("{e}"));
		detail = d;
		again = a;

		// What the job set (a lock_timeout, anything its owner's code set) ends with it.
		pg_sys::AtEOXact_GUC(false, nest);
	}
	TableIterator::once((detail, again))
}
