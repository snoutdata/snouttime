-- Rollups. The property checked everywhere: the rollup's view returns exactly what the
-- same aggregate computed from the raw rows returns, before a refresh, after one, and after
-- late, updated, deleted and truncated rows, and through a rollup of a rollup.
SET client_min_messages = warning;
SET timezone = 'UTC';
UPDATE snouttime.jobs SET enabled = false WHERE target::text NOT LIKE 'r\_%';

CREATE TABLE r_src (ts timestamptz NOT NULL, host text NOT NULL, v float8);
SELECT snouttime.create_series('r_src', 'ts', partition_interval => '1 day', premake => 1);
SELECT snouttime.make_partitions('r_src', '2026-01-01', '2026-01-04');
INSERT INTO r_src SELECT timestamptz '2026-01-01+00' + g * interval '1 minute', 'h' || (g % 2), g % 97
FROM generate_series(0, 2 * 1440 - 1) AS g;

SELECT snouttime.create_rollup('r_hourly', 'r_src', interval '1 hour',
	select_list => 'host, max(v) AS max_v, count(*) AS n, sum(v) AS total', group_by => 'host');

CREATE FUNCTION r_same() RETURNS TABLE (extra bigint, missing bigint, total bigint)
LANGUAGE sql AS $$
	WITH truth AS (SELECT snouttime.bucket('1 hour', ts) AS bucket, host, max(v) AS max_v, count(*) AS n, sum(v) AS total
		FROM r_src GROUP BY 1, 2)
	SELECT (SELECT count(*) FROM (SELECT * FROM r_hourly EXCEPT ALL SELECT * FROM truth) x),
		(SELECT count(*) FROM (SELECT * FROM truth EXCEPT ALL SELECT * FROM r_hourly) x),
		(SELECT count(*) FROM r_hourly)
$$;

-- before any refresh the view is all live
SELECT * FROM r_same();
SELECT count(*) AS materialized FROM r_hourly_materialized;
SELECT snouttime.refresh_rollup('r_hourly') AS ranges;
SELECT count(*) AS materialized FROM r_hourly_materialized;
SELECT * FROM r_same();
SELECT snouttime.refresh_rollup('r_hourly') AS nothing_to_do;

-- a late row into a materialized bucket: logged, and visible at once through the view
INSERT INTO r_src VALUES ('2026-01-01 05:30:00.5+00', 'h0', 1000);
SELECT rollup::text, snouttime._key_text(lo, 'timestamptz') AS lo FROM snouttime.invalidations;
SELECT * FROM r_same();
SELECT max_v FROM r_hourly WHERE bucket = '2026-01-01 05:00+00' AND host = 'h0';
SELECT snouttime.refresh_rollup('r_hourly') AS ranges;
SELECT count(*) AS pending FROM snouttime.invalidations;
SELECT * FROM r_same();

-- updates (both the old and the new time) and deletes
UPDATE r_src SET ts = ts + interval '20 hours' WHERE ts BETWEEN '2026-01-01 01:00' AND '2026-01-01 01:05';
DELETE FROM r_src WHERE ts >= '2026-01-01 10:00' AND ts < '2026-01-01 10:30' AND host = 'h1';
SELECT * FROM r_same();
SELECT snouttime.refresh_rollup('r_hourly') AS ranges;
SELECT * FROM r_same();

-- a rollup of a rollup: daily from hourly, merging the hourly aggregates
SELECT snouttime.create_rollup('r_daily', 'r_hourly', interval '1 day',
	select_list => 'host, max(max_v) AS max_v, sum(n) AS n', group_by => 'host');
CREATE FUNCTION r_daily_same() RETURNS TABLE (extra bigint, missing bigint, total bigint)
LANGUAGE sql AS $$
	WITH truth AS (SELECT snouttime.bucket('1 day', ts) AS bucket, host, max(v) AS max_v, count(*)::numeric AS n
		FROM r_src GROUP BY 1, 2)
	SELECT (SELECT count(*) FROM (SELECT * FROM r_daily EXCEPT ALL SELECT * FROM truth) x),
		(SELECT count(*) FROM (SELECT * FROM truth EXCEPT ALL SELECT * FROM r_daily) x),
		(SELECT count(*) FROM r_daily)
$$;
SELECT * FROM r_daily_same();
SELECT snouttime.refresh_rollup('r_daily') AS ranges;
SELECT * FROM r_daily_same();
-- a late raw row reaches the daily rollup at once, through the chain
INSERT INTO r_src VALUES ('2026-01-02 12:00+00', 'h1', 5000);
SELECT count(*) AS pending_for_daily FROM snouttime.invalidations WHERE rollup = 'r_daily'::regclass;
SELECT * FROM r_daily_same();
SELECT snouttime.refresh_rollup('r_hourly'), snouttime.refresh_rollup('r_daily');
SELECT * FROM r_daily_same();
SELECT * FROM r_same();

-- the refresh job
UPDATE snouttime.jobs SET next_run = '-infinity' WHERE kind = 'refresh';
INSERT INTO r_src VALUES ('2026-01-01 23:59+00', 'h0', 7);
SELECT snouttime.run_due_job();
SELECT snouttime.run_due_job();
SELECT kind, target::text, ok, detail FROM snouttime.job_runs WHERE kind = 'refresh' ORDER BY started_at;

-- retention on the source drops a partition; the rollup keeps its buckets, and a late row for
-- that day (landing in the default partition) does not overwrite them with a fragment
SELECT count(*) AS hourly_buckets_of_jan_1 FROM r_hourly_materialized WHERE bucket < '2026-01-02';
UPDATE snouttime.series SET retention = now() - timestamptz '2026-01-02 00:00+00' WHERE relid = 'r_src'::regclass;
SELECT snouttime.apply_retention('r_src');
SELECT count(*) AS hourly_buckets_of_jan_1 FROM r_hourly_materialized WHERE bucket < '2026-01-02';
INSERT INTO r_src VALUES ('2026-01-01 03:00+00', 'h0', 1);
SELECT n FROM r_hourly WHERE bucket = '2026-01-01 03:00+00' AND host = 'h0';
SELECT snouttime.refresh_rollup('r_hourly') >= 0 AS refreshed;
SELECT n FROM r_hourly WHERE bucket = '2026-01-01 03:00+00' AND host = 'h0';

-- a rollup's own retention
SELECT snouttime.set_rollup_retention('r_hourly', now() - timestamptz '2026-01-01 12:00+00');
SELECT snouttime.refresh_rollup('r_hourly') >= 0 AS refreshed;
SELECT min(bucket) FROM r_hourly_materialized;

-- mergeable or refused
\set ON_ERROR_STOP 0
SELECT snouttime.create_rollup('r_bad', 'r_hourly', interval '1 day', select_list => 'avg(max_v)');
SELECT snouttime.create_rollup('r_bad', 'r_hourly', interval '1 day', select_list => 'count(DISTINCT host)');
SELECT snouttime.create_rollup('r_bad', 'r_src', select_list => 'max(v)');
SELECT snouttime.create_rollup('r_bad', 'r_src', interval '1 hour');
-- a group_by the select list does not return (0.1.1): its rows could not be told apart
SELECT snouttime.create_rollup('r_bad', 'r_src', interval '1 hour', select_list => 'max(v) AS max_v', group_by => 'host');
SELECT snouttime.create_rollup('r_bad', 'r_src', interval '1 hour', select_list => 'max(v) AS max_v', group_by => '"host"');
SELECT snouttime.drop_rollup('r_hourly');
\set ON_ERROR_STOP 1

-- an integer time column
CREATE TABLE r_ticks (n int8 NOT NULL, v int);
SELECT snouttime.create_series('r_ticks', 'n', partition_width => 1000, premake => 1);
INSERT INTO r_ticks SELECT g, g % 10 FROM generate_series(0, 4999) g;
CALL snouttime.migrate('r_ticks');
SELECT snouttime.create_rollup('r_tick_sums', 'r_ticks', bucket_width => 100, select_list => 'sum(v) AS s');
SELECT snouttime.refresh_rollup('r_tick_sums') AS ranges;
INSERT INTO r_ticks VALUES (150, 1000);
SELECT count(*) AS buckets, sum(s) FROM r_tick_sums;
SELECT count(*), sum(v) FROM r_ticks;

-- TRUNCATE empties a rollup too
TRUNCATE r_ticks;
SELECT count(*) AS buckets_after_truncate FROM r_tick_sums;
SELECT snouttime.refresh_rollup('r_tick_sums') >= 0 AS refreshed;
SELECT count(*) AS materialized_after_truncate FROM r_tick_sums_materialized;

-- dropping: the chain from the top, and the triggers go with the last rollup
SELECT snouttime.drop_rollup('r_daily');
SELECT snouttime.drop_rollup('r_hourly');
SELECT count(*) AS triggers_left FROM pg_trigger WHERE tgrelid = 'r_src'::regclass AND tgname LIKE 'snouttime_invalidate%';
SELECT count(*) AS rollups_left FROM snouttime.rollups WHERE source = 'r_src'::regclass;
