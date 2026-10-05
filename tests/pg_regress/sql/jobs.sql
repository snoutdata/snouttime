-- The jobs, run by hand. The worker that calls them on a timer has its own
-- test (src/worker.rs), because a regression test cannot wait for one.
SET client_min_messages = notice;
SET timezone = 'UTC';
-- Every regression file shares one database, so another file's series tables have jobs of
-- their own and run_due_job() would pick whichever is most overdue. Park them.
UPDATE snouttime.jobs SET enabled = false WHERE target::text NOT LIKE 'j\_%';

-- ---- what create_series schedules ----
CREATE TABLE j_metrics (ts timestamptz NOT NULL, v float8);
INSERT INTO j_metrics
SELECT timestamptz '2020-03-01 00:00:00+00' + g * interval '6 hours', g
FROM generate_series(0, 11) AS g;
SELECT snouttime.create_series('j_metrics', 'ts', partition_interval => '1 day', premake => 1);
-- premake every tenth of the interval (capped at an hour), and migrate while the default
-- partition still holds rows
SELECT kind, target, schedule, enabled FROM snouttime.jobs
WHERE target::text LIKE 'j\_%' ORDER BY kind;

-- ---- migrate: one partition per run, then the job removes itself ----
SELECT snouttime.run_due_job();
SELECT kind, target, ok, detail FROM snouttime.job_runs
WHERE target::text LIKE 'j\_%' ORDER BY started_at;
SELECT count(*) AS still_in_default FROM j_metrics_default;
-- Each run is its own transaction on purpose: now() is frozen inside one, so a job that
-- has just been rescheduled is not due again until the next statement. `-infinity` is how
-- the test says "due now" without waiting a minute for the schedule.
UPDATE snouttime.jobs SET next_run = '-infinity' WHERE kind = 'migrate';
SELECT snouttime.run_due_job();
UPDATE snouttime.jobs SET next_run = '-infinity' WHERE kind = 'migrate';
SELECT snouttime.run_due_job();
UPDATE snouttime.jobs SET next_run = '-infinity' WHERE kind = 'migrate';
SELECT snouttime.run_due_job();
-- the default partition is empty now; the next run is the one that finds that and takes
-- the job off the list
SELECT count(*) AS still_in_default FROM j_metrics_default;
UPDATE snouttime.jobs SET next_run = '-infinity' WHERE kind = 'migrate';
SELECT snouttime.run_due_job();
SELECT kind FROM snouttime.jobs WHERE target::text LIKE 'j\_%' ORDER BY kind;
SELECT count(*) AS partitions FROM pg_inherits WHERE inhparent = 'j_metrics'::regclass;
SELECT count(*) AS rows_kept FROM j_metrics;

-- ---- retention drops whole partitions, and only ones SnoutTime made ----
SELECT snouttime.set_retention('j_metrics', interval '30 days');
SELECT kind, schedule FROM snouttime.jobs WHERE target::text LIKE 'j\_%' ORDER BY kind;
-- somebody else's partition, in range, must be left alone
CREATE TABLE j_metrics_theirs PARTITION OF j_metrics
	FOR VALUES FROM ('2019-01-01 00:00:00+00') TO ('2019-01-02 00:00:00+00');
INSERT INTO j_metrics VALUES ('2019-01-01 12:00:00+00', 1);
SELECT snouttime.apply_retention('j_metrics');
-- The partitions around today are named by date, so they are shown as days from today.
SELECT CASE WHEN c.relname ~ '_p[0-9]{8}$'
	THEN 'j_metrics_p<today' || to_char(to_date(right(c.relname, 8), 'YYYYMMDD')
		- (now() AT TIME ZONE 'UTC')::date, 'SG0') || '>'
	ELSE c.relname END AS relname
FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
WHERE i.inhparent = 'j_metrics'::regclass ORDER BY 1;
SELECT count(*) AS rows_kept FROM j_metrics;
-- a retention that keeps everything drops nothing
SELECT snouttime.set_retention('j_metrics', NULL::interval);
SELECT snouttime.apply_retention('j_metrics');
SELECT kind FROM snouttime.jobs WHERE target::text LIKE 'j\_%' ORDER BY kind;

-- ---- integer series: retention counts back from the largest value ----
CREATE TABLE j_ticks (n bigint NOT NULL);
INSERT INTO j_ticks SELECT g FROM generate_series(0, 4999) AS g;
SELECT snouttime.create_series('j_ticks', 'n', partition_width => 1000, premake => 1);
CALL snouttime.migrate('j_ticks');
SELECT snouttime.set_retention('j_ticks', 2000::bigint);
SELECT snouttime.apply_retention('j_ticks');
SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
WHERE i.inhparent = 'j_ticks'::regclass ORDER BY 1;
SELECT min(n), max(n) FROM j_ticks;

-- ---- a failing job is recorded and rescheduled, and does not stop the others ----
CREATE TABLE j_broken (ts timestamptz NOT NULL);
SELECT snouttime.create_series('j_broken', 'ts', partition_interval => '1 day', premake => 1);
-- An ordinary table squatting on the name tomorrow's partition wants: the premake job
-- cannot create it and must say so rather than die.
-- (A DO block rather than \gexec, which would echo today's date into the expected output.)
DO $$ BEGIN
	EXECUTE format('CREATE TABLE %I (x int)',
		'j_broken_p' || to_char((now() AT TIME ZONE 'UTC') + interval '2 days', 'YYYYMMDD'));
END $$;
-- create_series already made today's and tomorrow's, so reach one day further
UPDATE snouttime.series SET premake = 2 WHERE relid = 'j_broken'::regclass;
DELETE FROM snouttime.job_runs;
UPDATE snouttime.jobs SET next_run = '-infinity' WHERE target = 'j_broken'::regclass;
SELECT snouttime.run_due_job();
SELECT kind, target, ok, detail LIKE '%already exists%' AS says_why
FROM snouttime.job_runs WHERE target::text LIKE 'j\_%' ORDER BY kind, started_at;
SELECT kind, target, next_run > now() AS rescheduled FROM snouttime.jobs
WHERE target = 'j_broken'::regclass ORDER BY kind;
-- and the next job still runs
UPDATE snouttime.jobs SET next_run = '-infinity' WHERE target = 'j_metrics'::regclass;
SELECT snouttime.run_due_job();
SELECT kind, target, ok FROM snouttime.job_runs WHERE target::text LIKE 'j\_%'
ORDER BY started_at DESC LIMIT 1;

-- ---- only a superuser may start a worker ----
CREATE ROLE snouttime_jobs_other;
GRANT CREATE ON SCHEMA public TO snouttime_jobs_other;
SET ROLE snouttime_jobs_other;
SELECT snouttime.start_worker();
RESET ROLE;
REVOKE CREATE ON SCHEMA public FROM snouttime_jobs_other;
DROP ROLE snouttime_jobs_other;

-- ---- a row that arrives AFTER the conversion's sweep is swept too ----
-- The migrate job removes itself once the default partition is empty, so the premake job,
-- which runs on every series table, is what notices a late row and puts it back (found by
-- tests/soak/soak.sh: before, a far-future or far-past insert stayed in the default
-- partition for good).
CREATE TABLE j_late (ts timestamptz NOT NULL, v int);
SELECT snouttime.create_series('j_late', 'ts', partition_interval => '1 day', premake => 1);
SELECT kind FROM snouttime.jobs WHERE target = 'j_late'::regclass ORDER BY kind;
INSERT INTO j_late VALUES ('2020-06-15 12:00:00+00', 1);
UPDATE snouttime.jobs SET next_run = 'infinity' WHERE target <> 'j_late'::regclass;
UPDATE snouttime.jobs SET next_run = '-infinity' WHERE kind = 'premake' AND target = 'j_late'::regclass;
SELECT snouttime.run_due_job();
SELECT detail FROM snouttime.job_runs WHERE target = 'j_late'::regclass AND kind = 'premake';
SELECT kind FROM snouttime.jobs WHERE target = 'j_late'::regclass ORDER BY kind;
UPDATE snouttime.jobs SET next_run = '-infinity' WHERE kind = 'migrate' AND target = 'j_late'::regclass;
SELECT snouttime.run_due_job();
SELECT count(*) AS late_rows_still_in_default FROM j_late_default;
SELECT tableoid::regclass AS now_in FROM j_late;

-- ---- drop_before: a one-off retention, same rules ----
CREATE TABLE j_once (ts timestamptz NOT NULL, v int);
SELECT snouttime.create_series('j_once', 'ts', partition_interval => '1 day', premake => 1);
SELECT snouttime.make_partitions('j_once', '2020-01-01', '2020-01-05');
INSERT INTO j_once SELECT timestamptz '2020-01-01+00' + g * interval '1 hour', g FROM generate_series(0, 95) g;
-- 01-01 and 01-02 end at or before 01-03 00:00; 01-03 does not, although it starts there
SELECT snouttime.drop_before('j_once', timestamptz '2020-01-03 00:00+00') AS dropped;
SELECT min(ts), count(*) FROM j_once;
SELECT snouttime.drop_before('j_once', timestamptz '2020-01-03 12:00+00') AS dropped_nothing_partial;
\set ON_ERROR_STOP 0
SELECT snouttime.drop_before('j_once', 100::bigint);
\set ON_ERROR_STOP 1
