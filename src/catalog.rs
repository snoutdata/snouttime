//! The catalog: what SnoutTime knows about the database it lives in (PLAN.md Phase 0.3).
//!
//! Five tables in the extension's own schema. Four of them are STATE and are marked with
//! `pg_extension_config_dump`, so `pg_dump` writes their rows and a restore keeps every
//! registration; without that, a dump would restore the tables and forget they were ever
//! series tables. `job_runs` is HISTORY and is deliberately not dumped.
//!
//! ## Time values are stored as a single int8 "time key"
//!
//! A series table's time column may be `timestamptz`, `timestamp`, `date` or an integer
//! (PLAN.md 1.1). Everything that stores a point or a range in time here (invalidations,
//! watermarks) stores it as int8: microseconds since 2000-01-01 for the time types
//! (Postgres's own internal representation, so the conversion is exact and cheap), and the
//! value itself for an integer column. `series.time_type` says which reading applies.
//!
//! ## Tables are named by regclass
//!
//! A regclass column is stored as an OID, so renaming a series table does not break its
//! registration, and `pg_dump` writes it as the table's name, so a restore into a database
//! where the OIDs differ resolves it again. What a regclass does NOT do is notice a DROP:
//! cleaning up after a dropped table is an event trigger's job (PLAN.md 1.1).
//!
//! ## Who may write
//!
//! Anyone may read the catalog. A row may be written only by a role that could have done
//! the same thing to the table the row names: the `guard` trigger checks that the current
//! user has the privileges of that table's owner (superusers always do). So a user can
//! register, change or remove their own tables and nobody else's, and a registration of
//! someone else's table cannot be forged with a plain INSERT.
//!
//! It is a trigger and not the two obvious alternatives, each for a concrete reason:
//! row-level security would make a non-superuser's plain `pg_dump` fail on these tables
//! (it runs with `row_security = off` and refuses a table a policy would filter), which is
//! exactly how a hosted database is exported; and `SECURITY DEFINER` functions cannot see
//! who called them, so they could not check ownership at all. The functions that write
//! here therefore run as their caller, and the trigger is the one place the rule lives.
//! A trigger does not change what `pg_dump` reads.
//!
//! `job_runs` is written only by the background worker and stays read-only to everyone else.

use pgrx::prelude::*;

extension_sql!(
	r#"
CREATE TABLE snouttime.series (
	relid regclass PRIMARY KEY,
	time_column name NOT NULL,
	time_type regtype NOT NULL
		CHECK (time_type IN ('timestamptz'::regtype, 'timestamp'::regtype, 'date'::regtype,
			'smallint'::regtype, 'integer'::regtype, 'bigint'::regtype)),
	-- Exactly one of these, by time_type: an interval for the time types, a width for integers.
	partition_interval interval,
	partition_width int8,
	premake int4 NOT NULL DEFAULT 4 CHECK (premake BETWEEN 1 AND 1000),
	-- Optional second key: each time partition is itself hash-partitioned on this column,
	-- which spreads one interval's writes over several tables when the series has many
	-- distinct devices, hosts or sensors (PLAN.md 1.4).
	space_column name,
	space_partitions int4 CHECK (space_partitions BETWEEN 2 AND 1024),
	retention interval,
	retention_width int8,
	-- Sealing (PLAN.md 3.4): a partition is rewritten into the column store once its range
	-- ended this long ago (the settle window). NULL: never sealed by the worker.
	seal_after interval,
	seal_after_width int8,
	seal_codec text NOT NULL DEFAULT 'lz4' CHECK (seal_codec IN ('none', 'lz4', 'zstd')),
	-- Order within a column store; NULL means the space key (if any), then time.
	seal_order_by name[],
	-- Whether a sealed partition's non-unique indexes cover every row, or only its late rows
	-- (PLAN.md Q5; unique indexes always cover every row).
	seal_keep_indexes boolean NOT NULL DEFAULT false,
	-- Tiering (PLAN.md Phase 5): a partition goes to S3 once its range ended this long ago.
	tier_after interval,
	tier_after_width int8,
	created_at timestamptz NOT NULL DEFAULT now(),
	CHECK ((partition_interval IS NULL) <> (partition_width IS NULL)),
	CHECK (partition_interval IS NULL OR partition_interval > interval '0'),
	CHECK (partition_width IS NULL OR partition_width > 0),
	CHECK (retention IS NULL OR partition_interval IS NOT NULL),
	CHECK (retention_width IS NULL OR partition_width IS NOT NULL),
	CHECK (seal_after IS NULL OR partition_interval IS NOT NULL),
	CHECK (seal_after_width IS NULL OR partition_width IS NOT NULL),
	CHECK ((space_column IS NULL) = (space_partitions IS NULL))
);

-- A rollup (PLAN.md Phase 4): a view over materialized buckets plus the raw rows newer than
-- the watermark. The source is a series table, or another rollup (a rollup of a rollup).
CREATE TABLE snouttime.rollups (
	relid regclass PRIMARY KEY,
	source regclass NOT NULL,
	materialized regclass NOT NULL,
	-- the source's time column and its type (for a rollup of a rollup: "bucket")
	time_column name NOT NULL,
	time_type regtype NOT NULL,
	bucket_interval interval,
	bucket_width int8,
	select_list text NOT NULL,
	group_by text,
	-- Every bucket strictly before this time key is materialized; the rest is read raw.
	watermark int8,
	retention interval,
	retention_width int8,
	created_at timestamptz NOT NULL DEFAULT now(),
	CHECK ((bucket_interval IS NULL) <> (bucket_width IS NULL))
);

-- Time ranges written since a rollup's last refresh, one row per statement per rollup that
-- depends on the table written (inclusive time keys).
CREATE TABLE snouttime.invalidations (
	rollup regclass NOT NULL,
	lo int8 NOT NULL,
	hi int8 NOT NULL,
	logged_at timestamptz NOT NULL DEFAULT now(),
	CHECK (lo <= hi)
);
CREATE INDEX invalidations_rollup_lo ON snouttime.invalidations (rollup, lo);

-- What a table took as heap just before its first seal, so "how much did sealing save" has an
-- answer after the heap is gone (PLAN.md Phase 8). Kept through a reseal and a tier, which
-- start from a column store; dropped by an unseal and with the table.
CREATE TABLE snouttime.seal_sizes (
	relid regclass PRIMARY KEY,
	bytes_before int8 NOT NULL,
	sealed_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE snouttime.jobs (
	kind text NOT NULL
		CHECK (kind IN ('premake', 'migrate', 'retention', 'seal', 'reseal', 'refresh', 'tier')),
	target regclass NOT NULL,
	schedule interval NOT NULL CHECK (schedule >= interval '1 second'),
	next_run timestamptz NOT NULL DEFAULT now(),
	enabled boolean NOT NULL DEFAULT true,
	config jsonb NOT NULL DEFAULT '{}',
	PRIMARY KEY (kind, target)
);

CREATE TABLE snouttime.job_runs (
	kind text NOT NULL,
	target regclass,
	started_at timestamptz NOT NULL,
	finished_at timestamptz,
	ok boolean,
	detail text
);
CREATE INDEX job_runs_started_at ON snouttime.job_runs (started_at);

SELECT pg_catalog.pg_extension_config_dump('snouttime.series', '');
SELECT pg_catalog.pg_extension_config_dump('snouttime.rollups', '');
SELECT pg_catalog.pg_extension_config_dump('snouttime.seal_sizes', '');
SELECT pg_catalog.pg_extension_config_dump('snouttime.invalidations', '');
SELECT pg_catalog.pg_extension_config_dump('snouttime.jobs', '');

-- Does the current user hold the privileges of this table's owner? The same test Postgres
-- applies before it lets anyone ALTER or DROP a table.
CREATE FUNCTION snouttime._may_manage(rel regclass) RETURNS boolean
LANGUAGE sql STABLE
SET search_path = pg_catalog, pg_temp
AS $$
	SELECT pg_has_role(current_user, c.relowner, 'USAGE')
	FROM pg_class c WHERE c.oid = rel
$$;

CREATE FUNCTION snouttime._guard() RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
	row_ record;
	rels regclass[];
	rel regclass;
BEGIN
	IF TG_OP = 'DELETE' THEN row_ := OLD; ELSE row_ := NEW; END IF;
	-- IF, not CASE: a CASE naming row_.relid fails on a table without that column even in
	-- a branch that is never taken.
	IF TG_TABLE_NAME IN ('series', 'seal_sizes') THEN
		rels := ARRAY[row_.relid];
	ELSIF TG_TABLE_NAME = 'rollups' THEN
		rels := ARRAY[row_.relid, row_.source];
	ELSIF TG_TABLE_NAME = 'invalidations' THEN
		rels := ARRAY[row_.rollup];
	ELSE
		rels := ARRAY[row_.target];
	END IF;
	FOREACH rel IN ARRAY rels LOOP
		-- A NULL answer means the table no longer exists. Removing a row about a table that
		-- is gone harms nobody (it is how cleanup after a DROP works); writing one is refused.
		IF TG_OP = 'DELETE' AND snouttime._may_manage(rel) IS NULL THEN
			CONTINUE;
		END IF;
		IF NOT coalesce(snouttime._may_manage(rel), false) THEN
			RAISE EXCEPTION 'permission denied: only the owner of % may change its SnoutTime settings', rel
				USING ERRCODE = 'insufficient_privilege';
		END IF;
	END LOOP;
	IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
	RETURN NEW;
END
$$;

CREATE TRIGGER guard BEFORE INSERT OR UPDATE OR DELETE ON snouttime.series
	FOR EACH ROW EXECUTE FUNCTION snouttime._guard();
CREATE TRIGGER guard BEFORE INSERT OR UPDATE OR DELETE ON snouttime.rollups
	FOR EACH ROW EXECUTE FUNCTION snouttime._guard();
CREATE TRIGGER guard BEFORE INSERT OR UPDATE OR DELETE ON snouttime.invalidations
	FOR EACH ROW EXECUTE FUNCTION snouttime._guard();
CREATE TRIGGER guard BEFORE INSERT OR UPDATE OR DELETE ON snouttime.jobs
	FOR EACH ROW EXECUTE FUNCTION snouttime._guard();
CREATE TRIGGER guard BEFORE INSERT OR UPDATE OR DELETE ON snouttime.seal_sizes
	FOR EACH ROW EXECUTE FUNCTION snouttime._guard();

-- Everyone may look; the guard trigger decides who may write.
GRANT USAGE ON SCHEMA snouttime TO PUBLIC;

REVOKE ALL ON snouttime.series, snouttime.rollups, snouttime.invalidations,
	snouttime.jobs, snouttime.job_runs, snouttime.seal_sizes FROM PUBLIC;
GRANT SELECT ON snouttime.series, snouttime.rollups, snouttime.invalidations,
	snouttime.jobs, snouttime.job_runs, snouttime.seal_sizes TO PUBLIC;
GRANT INSERT, UPDATE, DELETE ON snouttime.series, snouttime.rollups, snouttime.invalidations,
	snouttime.jobs, snouttime.seal_sizes TO PUBLIC;
"#,
	name = "catalog",
);

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
	use pgrx::prelude::*;

	#[pg_test]
	fn catalog_tables_exist() {
		let n = Spi::get_one::<i64>(
			"SELECT count(*) FROM pg_tables WHERE schemaname = 'snouttime'
			 AND tablename IN ('series', 'rollups', 'invalidations', 'jobs', 'job_runs')",
		)
		.unwrap();
		assert_eq!(n, Some(5));
	}

	#[pg_test]
	fn state_is_dumped_and_history_is_not() {
		let dumped = Spi::get_one::<Vec<String>>(
			"SELECT array_agg(c.relname::text ORDER BY c.relname)
			 FROM pg_extension e, unnest(e.extconfig) AS t(oid)
			 JOIN pg_class c ON c.oid = t.oid
			 WHERE e.extname = 'snouttime'",
		)
		.unwrap()
		.unwrap();
		assert_eq!(dumped, vec!["invalidations", "jobs", "rollups", "seal_sizes", "series"]);
	}

	#[pg_test(error = "new row for relation \"series\" violates check constraint \"series_check\"")]
	fn series_needs_exactly_one_partition_size() {
		Spi::run(
			"CREATE TABLE t (ts timestamptz);
			 INSERT INTO snouttime.series (relid, time_column, time_type)
			 VALUES ('t', 'ts', 'timestamptz')",
		)
		.unwrap();
	}
}
