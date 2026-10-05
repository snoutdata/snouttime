-- What a role that is not a superuser can and cannot do (from a security
-- review). S3 credentials are unreadable and unsettable, the object collector is not
-- callable, and the extension's SECURITY DEFINER helpers act only for a table's owner.
SET client_min_messages = warning;
SET snouttime.s3_secret_access_key = 'a secret';
CREATE ROLE st_plain LOGIN;
CREATE SCHEMA sec AUTHORIZATION st_plain;
CREATE TABLE sec_other (a int);
-- load the library, as a real session would have by now
SELECT snouttime.version() IS NOT NULL AS loaded;
SET ROLE st_plain;
SET search_path = sec, public;
\set ON_ERROR_STOP 0
SHOW snouttime.s3_secret_access_key;
SHOW snouttime.s3_access_key_id;
SELECT count(*) AS visible FROM pg_settings WHERE name = 'snouttime.s3_secret_access_key' AND setting = 'a secret';
SET snouttime.s3_secret_access_key = 'mine';
SET snouttime.tier_to = 's3://elsewhere/x';
SELECT snouttime.tier_gc();
-- the helpers behind the event triggers, called directly on somebody else's table
SELECT snouttime._columnar_sync('sec_other');
SELECT snouttime._columnar_drop_side('sec_other'::regclass::oid);
SELECT snouttime._columnar_forget('delta_1', 'deletes_1');
-- what a column store holds is read only by a role that may read the table
SELECT * FROM snouttime.column_sizes('sec_other');
\set ON_ERROR_STOP 1
-- tiering with credentials in settings, not preloaded: refused, since they would be readable
RESET ROLE;
SET snouttime.tier_to = 's3://b/p';
SET snouttime.s3_access_key_id = 'k';
CREATE TABLE sec_tier (a int);
INSERT INTO sec_tier VALUES (1);
\set ON_ERROR_STOP 0
SELECT snouttime.tier('sec_tier');
\set ON_ERROR_STOP 1
SET ROLE st_plain;
SET search_path = sec, public;
-- what the role CAN do: seal a table of its own
CREATE TABLE sec_mine (a int);
INSERT INTO sec_mine SELECT generate_series(1, 10);
SELECT snouttime.seal('sec_mine');
SELECT sum(a) FROM sec_mine;
INSERT INTO sec_mine VALUES (11);
SELECT sum(a) FROM sec_mine;
SELECT attname, encoding, rows FROM snouttime.column_sizes('sec_mine');
-- ...and DROP tables, which it could not until 0.1.5: the sql_drop event trigger read
-- snouttime_internal through to_regclass, which needs USAGE this role does not have, so every
-- DROP TABLE in the database failed (found on SnoutData Cloud, whose owner is such a role).
CREATE TABLE sec_plain (a int);
DROP TABLE sec_plain;
SELECT 'sec_mine'::regclass::oid AS mine_oid \gset
DROP TABLE sec_mine;
SELECT count(*) AS side_tables_left FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'snouttime_internal' AND c.relname IN ('delta_' || :mine_oid, 'deletes_' || :mine_oid);
-- and a series of its own, down to dropping its default partition and the whole table
CREATE TABLE sec_series (ts timestamptz NOT NULL, v int);
SELECT snouttime.create_series('sec_series', 'ts', partition_interval => '1 day', premake => 1);
SELECT snouttime.make_partitions('sec_series', '2026-01-01', '2026-01-03');
INSERT INTO sec_series SELECT timestamptz '2026-01-01+00' + g * interval '1 minute', g FROM generate_series(0, 2879) g;
SELECT snouttime.drop_default('sec_series');
SELECT snouttime.seal('sec_series_p20260101');
-- ...and its seal job, which the worker runs as the table's owner: once nothing is left to
-- seal it asks how much each sealed partition has changed, and until 0.1.6 that read the side
-- tables in SQL and failed on every run for a role like this one
INSERT INTO sec_series VALUES ('2026-01-01 12:00:30+00', -1);
SELECT snouttime._changed_since_seal('sec_series_p20260101', 100) AS changed;
SELECT snouttime.set_sealing('sec_series', interval '1 day');
SELECT detail FROM snouttime._do_job('seal', 'sec_series');
SELECT detail FROM snouttime._do_job('seal', 'sec_series');
SELECT detail FROM snouttime._do_job('seal', 'sec_series');
DROP TABLE sec_series;
SELECT count(*) AS left_behind FROM pg_class WHERE relname LIKE 'sec\_series%';
RESET ROLE;

-- ---- 0.1.7: a job runs as its table's owner, and cannot step back out to the worker ----
-- The worker is a superuser. Until 0.1.7 it ran each job after SET LOCAL ROLE to the table's
-- owner, which is no boundary: code of the owner's that the job runs (a trigger, an index
-- expression, a rollup's query) could say RESET ROLE and carry on as the superuser. Now a job
-- runs in a security-restricted operation as the owner, as VACUUM and REFRESH MATERIALIZED
-- VIEW do. Run here the way the worker runs it: run_due_job() called by a superuser.
UPDATE snouttime.jobs SET enabled = false;
CREATE ROLE st_other;
CREATE SCHEMA sec2 AUTHORIZATION st_other;
GRANT st_other TO st_plain;
SET ROLE st_plain;
SET search_path = sec, public;
-- a series table with an index expression and a trigger of the owner's, and old rows to move
CREATE FUNCTION sec_norm(text) RETURNS text LANGUAGE sql IMMUTABLE AS 'SELECT lower($1)';
CREATE TABLE sec_seen (what text, who text);
CREATE FUNCTION sec_note() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
	INSERT INTO sec.sec_seen VALUES (TG_TABLE_NAME, current_user);
	RETURN NULL;
END $$;
CREATE TABLE sec_job (ts timestamptz NOT NULL, host text NOT NULL, v int);
CREATE INDEX sec_job_host ON sec_job (sec.sec_norm(host));
CREATE TRIGGER sec_note AFTER INSERT ON sec_job FOR EACH STATEMENT EXECUTE FUNCTION sec.sec_note();
INSERT INTO sec_job SELECT timestamptz '2020-03-01+00' + g * interval '1 hour', 'Host' || (g % 3), g
FROM generate_series(0, 71) g;
SELECT snouttime.create_series('sec_job', 'ts', partition_interval => '1 day', premake => 1);
SELECT snouttime.set_sealing('sec_job', interval '1 day');
SELECT snouttime.create_rollup('sec_daily', 'sec_job', interval '1 day', select_list => 'count(*) AS n, sum(v) AS total');
CREATE TRIGGER sec_note AFTER INSERT ON sec_daily_materialized FOR EACH STATEMENT EXECUTE FUNCTION sec.sec_note();
DELETE FROM sec_seen;
RESET ROLE;
SELECT current_setting('search_path') AS search_path_before \gset
-- every job the table has, until none is due: migrate, premake, seal, refresh
UPDATE snouttime.jobs SET enabled = true, next_run = '-infinity'
WHERE target IN ('sec.sec_job'::regclass, 'sec.sec_daily'::regclass);
DO $$
BEGIN
	FOR i IN 1..40 LOOP
		EXIT WHEN NOT snouttime.run_due_job();
	END LOOP;
END $$;
-- the seal job again, now that the rows are in their partitions
UPDATE snouttime.jobs SET next_run = '-infinity' WHERE kind = 'seal' AND target = 'sec.sec_job'::regclass;
DO $$
BEGIN
	FOR i IN 1..40 LOOP
		EXIT WHEN NOT snouttime.run_due_job();
	END LOOP;
END $$;
SELECT kind, ok, count(*) AS runs FROM snouttime.job_runs
WHERE target IN ('sec.sec_job'::regclass, 'sec.sec_daily'::regclass) GROUP BY kind, ok ORDER BY kind, ok;
SELECT kind, detail FROM snouttime.job_runs
WHERE target IN ('sec.sec_job'::regclass, 'sec.sec_daily'::regclass) AND NOT ok;
-- the rows all moved, the old partitions are sealed, the rollup counts every row
SELECT count(*) AS left_in_default FROM sec.sec_job_default;
SELECT count(*) AS sealed FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
JOIN pg_am am ON am.oid = c.relam WHERE i.inhparent = 'sec.sec_job'::regclass AND am.amname = 'snouttime_columnar';
SELECT sum(n) AS rolled_up, sum(total) AS total FROM sec.sec_daily_materialized;
-- the index expression is in use on the moved rows
SET enable_seqscan = off;
SELECT count(*) AS host1 FROM sec.sec_job WHERE sec.sec_norm(host) = 'host1';
RESET enable_seqscan;
-- the owner's trigger on the materialized table ran during the refresh job, as the owner
SELECT DISTINCT what, who FROM sec.sec_seen ORDER BY what;
-- and the worker is itself again, search_path included
SELECT current_user = session_user AS worker_identity,
	current_setting('search_path') = :'search_path_before' AS same_search_path;
-- late rows, past a tenth of the partition and 10,000, so the seal job reseals; tiering and
-- retention run as the owner too
INSERT INTO sec.sec_job SELECT timestamptz '2020-03-01 12:00:30+00', 'late', g FROM generate_series(1, 10001) g;
UPDATE snouttime.jobs SET next_run = '-infinity' WHERE kind = 'seal' AND target = 'sec.sec_job'::regclass;
SELECT snouttime.run_due_job();
SELECT kind, ok, regexp_replace(detail, '[0-9]{8}', 'N', 'g') AS detail FROM snouttime.job_runs
WHERE target = 'sec.sec_job'::regclass ORDER BY started_at DESC LIMIT 1;
SELECT detail FROM snouttime._run_job('tier', 'sec.sec_job');
SET ROLE st_plain;
SELECT snouttime.set_retention('sec.sec_job', interval '30 days');
RESET ROLE;
UPDATE snouttime.jobs SET next_run = '-infinity' WHERE kind = 'retention' AND target = 'sec.sec_job'::regclass;
SELECT snouttime.run_due_job();
SELECT kind, ok, detail FROM snouttime.job_runs
WHERE target = 'sec.sec_job'::regclass ORDER BY started_at DESC LIMIT 1;
SELECT count(*) AS rows_left FROM sec.sec_job;

-- The owner's code tries to leave the job's identity: the rollup's query calls a function of
-- the owner's that says RESET ROLE and would then make its owner a superuser.
SET ROLE st_plain;
CREATE TABLE sec_src (ts timestamptz NOT NULL, v int);
SELECT snouttime.create_series('sec_src', 'ts', partition_interval => '1 day', premake => 1);
INSERT INTO sec_src VALUES ('2020-03-01 00:00:00+00', 1);
CREATE FUNCTION sec_escape() RETURNS int LANGUAGE plpgsql AS $$
BEGIN
	INSERT INTO sec.sec_seen VALUES ('escape', current_user);
	RESET ROLE;
	ALTER ROLE st_plain SUPERUSER;
	RETURN 1;
END $$;
SELECT snouttime.create_rollup('sec_escape_daily', 'sec_src', interval '1 day',
	select_list => 'count(*) AS n, sec.sec_escape() AS e');
RESET ROLE;
UPDATE snouttime.jobs SET enabled = true, next_run = '-infinity'
WHERE kind = 'refresh' AND target = 'sec.sec_escape_daily'::regclass;
SELECT snouttime.run_due_job();
-- refused, recorded as failed with Postgres's own error, and nothing it did is left
SELECT kind, ok, detail FROM snouttime.job_runs WHERE target = 'sec.sec_escape_daily'::regclass;
SELECT rolsuper FROM pg_roles WHERE rolname = 'st_plain';
SELECT count(*) AS escape_rows FROM sec.sec_seen WHERE what = 'escape';
SELECT count(*) AS materialized FROM sec.sec_escape_daily_materialized;
SELECT next_run > now() AS rescheduled FROM snouttime.jobs
WHERE kind = 'refresh' AND target = 'sec.sec_escape_daily'::regclass;
SELECT current_user = session_user AS worker_identity,
	current_setting('search_path') = :'search_path_before' AS same_search_path;
-- ...and the same for SET ROLE to a role the owner is a member of
SET ROLE st_plain;
CREATE OR REPLACE FUNCTION sec_escape() RETURNS int LANGUAGE plpgsql AS $$
BEGIN
	SET ROLE st_other;
	CREATE TABLE sec2.sec_escaped (a int);
	RETURN 1;
END $$;
RESET ROLE;
UPDATE snouttime.jobs SET next_run = '-infinity' WHERE kind = 'refresh' AND target = 'sec.sec_escape_daily'::regclass;
SELECT snouttime.run_due_job();
SELECT kind, ok, detail FROM snouttime.job_runs
WHERE target = 'sec.sec_escape_daily'::regclass ORDER BY started_at DESC LIMIT 1;
SELECT to_regclass('sec2.sec_escaped') AS escaped;
SELECT current_user = session_user AS worker_identity;
UPDATE snouttime.jobs SET enabled = false WHERE target = 'sec.sec_escape_daily'::regclass;

-- Someone else's jobs: a role that is not the owner neither runs them nor touches their rows.
SELECT 'sec.sec_src'::regclass::oid AS src_oid \gset
UPDATE snouttime.jobs SET enabled = true, next_run = '-infinity' WHERE target = 'sec.sec_src'::regclass;
SELECT count(*) AS runs_before FROM snouttime.job_runs \gset
SET ROLE st_other;
SET search_path = sec2, public;
SELECT snouttime.run_due_job() AS ran_something;
CREATE TABLE sec2.theirs (ts timestamptz NOT NULL);
\set ON_ERROR_STOP 0
SELECT snouttime._run_job('premake', :src_oid::regclass);
INSERT INTO snouttime.jobs (kind, target, schedule) VALUES ('retention', :src_oid::regclass, interval '1 hour');
UPDATE snouttime.jobs SET enabled = false WHERE target = :src_oid::regclass;
DELETE FROM snouttime.jobs WHERE target = :src_oid::regclass;
-- re-pointing a row about someone else's table at one's own: refused since 0.1.7
UPDATE snouttime.series SET relid = 'sec2.theirs'::regclass WHERE relid = :src_oid::regclass;
UPDATE snouttime.jobs SET target = 'sec2.theirs'::regclass WHERE target = :src_oid::regclass;
-- a rollup of one's own whose materialized table is someone else's: refused since 0.1.7
CREATE VIEW sec2.v AS SELECT now() AS bucket;
INSERT INTO snouttime.rollups (relid, source, materialized, time_column, time_type, bucket_interval, select_list)
VALUES ('sec2.v', 'sec2.theirs', (SELECT materialized FROM snouttime.rollups WHERE source = :src_oid::regclass),
	'ts', 'timestamptz', '1 day', 'count(*) AS n');
\set ON_ERROR_STOP 1
RESET ROLE;
SELECT count(*) - :runs_before AS runs_added FROM snouttime.job_runs;
SELECT kind, enabled, next_run = '-infinity' AS still_due FROM snouttime.jobs
WHERE target = 'sec.sec_src'::regclass ORDER BY kind;
SELECT count(*) AS still_a_series FROM snouttime.series WHERE relid = 'sec.sec_src'::regclass;

-- The invalidation trigger runs as the extension's owner, so it acts only as the triggers
-- create_rollup makes. Attached by hand, without transition tables, `new_rows` would have been
-- this temporary view, run as the superuser.
SET ROLE st_plain;
SET search_path = sec, public;
CREATE TABLE sec_fake (ts timestamptz NOT NULL);
CREATE TEMP VIEW new_rows AS SELECT now() AS ts, sec.sec_escape() AS e;
CREATE TRIGGER sec_fake AFTER INSERT ON sec_fake FOR EACH STATEMENT
	EXECUTE FUNCTION snouttime._invalidate('ts', 'timestamptz');
\set ON_ERROR_STOP 0
INSERT INTO sec_fake VALUES (now());
\set ON_ERROR_STOP 1
DROP VIEW new_rows;
DROP TABLE sec_fake;
-- the real ones still log what a write touched
INSERT INTO sec_src VALUES ('2020-03-02 00:00:00+00', 2);
SELECT count(*) AS invalidations FROM snouttime.invalidations
WHERE rollup = 'sec.sec_escape_daily'::regclass;

-- Adding a column with a default to a sealed table: the delta store takes the default as the
-- value Postgres stored, not by evaluating the owner's expression again as the extension's
-- owner (until 0.1.7 it did, so the function below ran twice, the second time as the superuser).
CREATE TABLE sec_col (a int);
INSERT INTO sec_col SELECT generate_series(1, 10);
SELECT snouttime.seal('sec_col');
INSERT INTO sec_col VALUES (11);
CREATE FUNCTION sec_default() RETURNS int LANGUAGE plpgsql STABLE AS $$
BEGIN
	RAISE NOTICE 'default evaluated as its owner: %', current_user = 'st_plain';
	RETURN 7;
END $$;
SET client_min_messages = notice;
ALTER TABLE sec_col ADD COLUMN b int DEFAULT sec.sec_default();
SET client_min_messages = warning;
SELECT a, b FROM sec_col WHERE a IN (1, 11) ORDER BY a;
RESET ROLE;
SELECT rolsuper FROM pg_roles WHERE rolname = 'st_plain';
RESET ROLE;
-- every SECURITY DEFINER function the extension defines pins its search_path
SELECT p.proname FROM pg_proc p JOIN pg_depend d ON d.objid = p.oid AND d.deptype = 'e'
JOIN pg_extension e ON e.oid = d.refobjid AND e.extname = 'snouttime'
WHERE p.prolang <> (SELECT oid FROM pg_language WHERE lanname = 'c')
	AND p.prosecdef AND NOT EXISTS (SELECT 1 FROM unnest(p.proconfig) c WHERE c LIKE 'search_path=%')
ORDER BY 1;
SELECT p.proname AS definer FROM pg_proc p JOIN pg_depend d ON d.objid = p.oid AND d.deptype = 'e'
JOIN pg_extension e ON e.oid = d.refobjid AND e.extname = 'snouttime' WHERE p.prosecdef ORDER BY 1;
