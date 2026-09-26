-- Phase 3.4: sealing partitions of a series table, by hand and by the worker's job, and what
-- a sealed partition does afterwards. Everything is compared with a heap twin.
SET client_min_messages = warning;
SET timezone = 'UTC';
UPDATE snouttime.jobs SET enabled = false WHERE target::text NOT LIKE 's\_%';

CREATE TABLE s_cpu (host text NOT NULL, ts timestamptz NOT NULL, usage float8, n int);
CREATE INDEX ON s_cpu (ts);
SELECT snouttime.create_series('s_cpu', 'ts', partition_interval => '1 day', premake => 1);
SELECT snouttime.make_partitions('s_cpu', '2026-01-01', '2026-01-05');
INSERT INTO s_cpu
SELECT 'host_' || (g % 5), timestamptz '2026-01-01+00' + g * interval '1 minute', g / 10.0, g
FROM generate_series(0, 4 * 1440 - 1) AS g;
CREATE TABLE s_twin AS SELECT * FROM s_cpu;

-- by hand: one partition
SELECT snouttime.seal('s_cpu_p20260101');
SELECT snouttime.seal('s_cpu_p20260101') AS again_is_nothing;
SELECT name, state FROM snouttime.partition_info WHERE series = 's_cpu'::regclass AND (name LIKE 's_cpu_p202601%' OR name = 's_cpu_default') ORDER BY name;
-- the series table's order: time, since there is no space key
SELECT count(*) AS out_of_order FROM (
	SELECT ts, lag(ts) OVER (ORDER BY ctid) AS prev FROM s_cpu_p20260101) x WHERE prev > ts;

CREATE FUNCTION s_same() RETURNS TABLE (extra bigint, missing bigint, total bigint)
LANGUAGE sql AS $$
	SELECT (SELECT count(*) FROM (SELECT * FROM s_cpu EXCEPT ALL SELECT * FROM s_twin) x),
		(SELECT count(*) FROM (SELECT * FROM s_twin EXCEPT ALL SELECT * FROM s_cpu) x),
		(SELECT count(*) FROM s_cpu)
$$;
SELECT * FROM s_same();
-- pruning still works: a query for the sealed day reads only it
EXPLAIN (COSTS OFF) SELECT count(*) FROM s_cpu WHERE ts >= '2026-01-01' AND ts < '2026-01-01 12:00';
SELECT count(*), sum(n) FROM s_cpu WHERE ts >= '2026-01-01' AND ts < '2026-01-01 12:00';

-- late writes into a sealed day, deletes and updates through the parent
INSERT INTO s_cpu VALUES ('late', '2026-01-01 06:30:30+00', 1, -1);
INSERT INTO s_twin VALUES ('late', '2026-01-01 06:30:30+00', 1, -1);
DELETE FROM s_cpu WHERE ts < '2026-01-01 01:00' AND host = 'host_1';
DELETE FROM s_twin WHERE ts < '2026-01-01 01:00' AND host = 'host_1';
UPDATE s_cpu SET usage = -usage WHERE ts BETWEEN '2026-01-01 02:00' AND '2026-01-01 02:10';
UPDATE s_twin SET usage = -usage WHERE ts BETWEEN '2026-01-01 02:00' AND '2026-01-01 02:10';
-- an update that moves a row out of the sealed day into a live one
UPDATE s_cpu SET ts = ts + interval '2 days' WHERE n = 100;
UPDATE s_twin SET ts = ts + interval '2 days' WHERE n = 100;
SELECT * FROM s_same();
SELECT snouttime._changed_since_seal('s_cpu_p20260101', 1000) AS changed;

-- a reseal folds them in
SELECT snouttime.reseal('s_cpu_p20260101');
SELECT snouttime._changed_since_seal('s_cpu_p20260101', 1000) AS changed_after;
SELECT * FROM s_same();

-- the job: seal everything whose day ended more than a day ago, oldest first, one per run
SELECT snouttime.set_sealing('s_cpu', interval '1 day', codec => 'zstd');
SELECT kind, schedule FROM snouttime.jobs WHERE target = 's_cpu'::regclass ORDER BY kind;
UPDATE snouttime.jobs SET next_run = '-infinity' WHERE kind = 'seal' AND target = 's_cpu'::regclass;
SELECT snouttime.run_due_job();
UPDATE snouttime.jobs SET next_run = '-infinity' WHERE kind = 'seal' AND target = 's_cpu'::regclass;
SELECT snouttime.run_due_job();
UPDATE snouttime.jobs SET next_run = '-infinity' WHERE kind = 'seal' AND target = 's_cpu'::regclass;
SELECT snouttime.run_due_job();
UPDATE snouttime.jobs SET next_run = '-infinity' WHERE kind = 'seal' AND target = 's_cpu'::regclass;
SELECT snouttime.run_due_job();
SELECT kind, ok, detail FROM snouttime.job_runs WHERE target = 's_cpu'::regclass ORDER BY started_at;
SELECT name, state FROM snouttime.partition_info WHERE series = 's_cpu'::regclass AND (name LIKE 's_cpu_p202601%' OR name = 's_cpu_default') ORDER BY name;
SELECT * FROM s_same();

-- unseal gives heap back
SELECT snouttime.unseal('s_cpu_p20260102');
SELECT name, state FROM snouttime.partition_info WHERE series = 's_cpu'::regclass AND name = 's_cpu_p20260102';
SELECT * FROM s_same();

-- retention drops sealed partitions and their side tables with them
SELECT 's_cpu_p20260101'::regclass::oid AS p1 \gset
SELECT snouttime.set_retention('s_cpu', interval '1 year');
UPDATE snouttime.series SET retention = now() - timestamptz '2026-01-02 00:00+00' WHERE relid = 's_cpu'::regclass;
SELECT snouttime.apply_retention('s_cpu');
SELECT count(*) AS side_tables_left FROM pg_class WHERE relname IN ('delta_' || :p1, 'deletes_' || :p1);

-- with a space key, the leaves of a time partition are sealed, in (space key, time) order
CREATE TABLE s_dev (dev int NOT NULL, ts timestamptz NOT NULL, v float8);
SELECT snouttime.create_series('s_dev', 'ts', partition_interval => '1 day', premake => 1,
	space_column => 'dev', space_partitions => 2);
SELECT snouttime.make_partitions('s_dev', '2026-01-01', '2026-01-02');
INSERT INTO s_dev SELECT g % 10, timestamptz '2026-01-01+00' + g * interval '1 minute', g FROM generate_series(0, 1439) g;
SELECT snouttime.seal('s_dev_p20260101');
SELECT name, state FROM snouttime.partition_info WHERE series = 's_dev'::regclass AND name = 's_dev_p20260101';
SELECT count(*) AS out_of_order FROM (
	SELECT dev, ts, lag(dev) OVER w AS pd, lag(ts) OVER w AS pt FROM s_dev_p20260101_h0 WINDOW w AS (ORDER BY ctid)) x
WHERE (pd, pt) > (dev, ts);
SELECT count(*), sum(v) FROM s_dev;

-- sealing, resealing and unsealing move rows without firing the user's row triggers
CREATE TABLE s_trig (id int, v int);
CREATE TABLE s_trig_log (op text);
CREATE FUNCTION s_trig_fn() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN INSERT INTO s_trig_log VALUES (TG_OP); RETURN NULL; END $$;
CREATE TRIGGER s_trig_row AFTER INSERT OR UPDATE OR DELETE ON s_trig FOR EACH ROW EXECUTE FUNCTION s_trig_fn();
INSERT INTO s_trig SELECT g, g FROM generate_series(1, 100) g;
SELECT snouttime.seal('s_trig'), snouttime.reseal('s_trig'), snouttime.unseal('s_trig'), snouttime.seal('s_trig');
SELECT op, count(*) FROM s_trig_log GROUP BY op;
DELETE FROM s_trig WHERE id <= 2;
SELECT op, count(*) FROM s_trig_log GROUP BY op ORDER BY op;

-- a table that is not a partition is its own only leaf
CREATE TABLE s_plain (a int);
INSERT INTO s_plain SELECT generate_series(1, 10);
SELECT snouttime.seal('s_plain') AS sealed, snouttime._is_sealed('s_plain') AS is_sealed;
SELECT snouttime.reseal('s_plain') AS resealed, snouttime.unseal('s_plain') AS unsealed, sum(a) FROM s_plain;

-- refusals
\set ON_ERROR_STOP 0
SELECT snouttime.seal('s_cpu');
SELECT snouttime.seal('s_dev_default');
SELECT snouttime.set_sealing('s_cpu', interval '-1 day');
SELECT snouttime.set_sealing('s_cpu', interval '1 day', codec => 'gzip');
\set ON_ERROR_STOP 1
