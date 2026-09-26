-- Phase 1.4 (a space key) and 1.5 (what is there and what it costs).
SET client_min_messages = warning;
SET timezone = 'UTC';

CREATE TABLE i_metrics (ts timestamptz NOT NULL, host text NOT NULL, v float8);
INSERT INTO i_metrics
SELECT timestamptz '2021-05-01 00:00:00+00' + g * interval '6 hours', 'h' || (g % 4), g
FROM generate_series(0, 11) AS g;
SELECT snouttime.create_series('i_metrics', 'ts', partition_interval => '1 day', premake => 1,
	space_column => 'host', space_partitions => 2);
CALL snouttime.migrate('i_metrics');

-- Each day is itself split in two by the space key, and a row still finds its way down.
SELECT name, state, range_start, range_end, children
FROM snouttime.partition_info WHERE series = 'i_metrics'::regclass AND name LIKE '%2021%'
ORDER BY name;
SELECT count(*) AS leaves FROM pg_class WHERE relname LIKE 'i\_metrics\_p2021%\_h%';
SELECT count(*) AS rows_kept FROM i_metrics;
SELECT tableoid::regclass::text ~ '_h[01]$' AS in_a_hash_partition
FROM i_metrics WHERE ts = '2021-05-01 06:00:00+00';

-- The shape, without scanning anything.
SET client_min_messages = warning;
ANALYZE i_metrics;
SELECT time_column, partition_size, space_column, space_partitions, partitions,
	foreign_partitions, oldest_range,
	-- premade around today, so as a distance from today: an absolute date made this test
	-- fail on every day but the one its expected output was recorded on
	newest_range::timestamptz - date_trunc('day', now()) AS newest_range_from_today,
	rows_in_default, estimated_rows,
	bytes > 0 AS has_bytes
FROM snouttime.series_info WHERE series = 'i_metrics'::regclass;

-- A partition somebody else made is reported, and named as not ours.
CREATE TABLE i_metrics_theirs PARTITION OF i_metrics
	FOR VALUES FROM ('2019-01-01 00:00:00+00') TO ('2019-01-02 00:00:00+00')
	PARTITION BY HASH (host);
CREATE TABLE i_metrics_theirs_h0 PARTITION OF i_metrics_theirs
	FOR VALUES WITH (MODULUS 2, REMAINDER 0);
CREATE TABLE i_metrics_theirs_h1 PARTITION OF i_metrics_theirs
	FOR VALUES WITH (MODULUS 2, REMAINDER 1);
SELECT name, state FROM snouttime.partition_info
WHERE series = 'i_metrics'::regclass AND name = 'i_metrics_theirs';
SELECT foreign_partitions FROM snouttime.series_info WHERE series = 'i_metrics'::regclass;

-- Jobs, with what happened last time.
SELECT kind, target, schedule, last_ok FROM snouttime.job_info
WHERE target = 'i_metrics'::regclass ORDER BY kind;
UPDATE snouttime.jobs SET next_run = '-infinity' WHERE target = 'i_metrics'::regclass;
SELECT snouttime.run_due_job();
SELECT kind, target, last_ok, last_detail, last_took IS NOT NULL AS timed
FROM snouttime.job_info WHERE target = 'i_metrics'::regclass AND last_ok IS NOT NULL;

-- ---- the default partition can be dropped, once it is empty ----
-- A row from outside every partition lands in the default one, and while it is there the
-- default cannot be dropped.
INSERT INTO i_metrics VALUES ('2018-07-04 00:00:00+00', 'h2', 7);
SELECT snouttime.drop_default('i_metrics');
CALL snouttime.migrate('i_metrics');
SELECT snouttime.drop_default('i_metrics');
-- calling it again says there is nothing to drop rather than failing
SELECT snouttime.drop_default('i_metrics');
SELECT count(*) FILTER (WHERE state = 'default') AS defaults
FROM snouttime.partition_info WHERE series = 'i_metrics'::regclass;
SELECT count(*) AS rows_kept FROM i_metrics;
-- and from now on a row outside every partition has nowhere to go
INSERT INTO i_metrics VALUES ('1999-01-01 00:00:00+00', 'h1', 1);

-- ---- what a space key refuses ----
CREATE TABLE i_bad (ts timestamptz NOT NULL, host text NOT NULL, id int UNIQUE);
SELECT snouttime.create_series('i_bad', 'ts', partition_interval => '1 day', space_column => 'host');
SELECT snouttime.create_series('i_bad', 'ts', partition_interval => '1 day',
	space_column => 'nope', space_partitions => 2);
SELECT snouttime.create_series('i_bad', 'ts', partition_interval => '1 day',
	space_column => 'ts', space_partitions => 2);
SELECT snouttime.create_series('i_bad', 'ts', partition_interval => '1 day',
	space_column => 'host', space_partitions => 1);
-- the unique index includes neither partition column
SELECT snouttime.create_series('i_bad', 'ts', partition_interval => '1 day',
	space_column => 'host', space_partitions => 2);

-- ---- what a seal saved (Phase 8): the heap's size is kept when it is gone ----
CREATE TABLE i_sz (ts timestamptz NOT NULL, host text NOT NULL, v int8);
SELECT snouttime.create_series('i_sz', 'ts', partition_interval => '1 day', premake => 1);
SELECT snouttime.make_partitions('i_sz', '2022-01-01', '2022-01-03');
INSERT INTO i_sz SELECT timestamptz '2022-01-01+00' + g * interval '10 seconds', 'h' || (g % 5), g
FROM generate_series(0, 2 * 8640 - 1) g;
-- never sealed: nothing to report
SELECT name, state, bytes_before FROM snouttime.partition_info
WHERE series = 'i_sz'::regclass AND name = 'i_sz_p20220101';
SELECT pg_total_relation_size('i_sz_p20220101') AS heap_bytes \gset
SELECT snouttime.seal('i_sz_p20220101');
SELECT state, bytes_before = :heap_bytes AS is_the_heap, bytes < bytes_before AS smaller
FROM snouttime.partition_info WHERE series = 'i_sz'::regclass AND name = 'i_sz_p20220101';
SELECT sealed_bytes_before = :heap_bytes AS series_before, sealed_bytes > 0 AS series_after
FROM snouttime.series_info WHERE series = 'i_sz'::regclass;
-- a reseal keeps the size from the first seal
SELECT snouttime.reseal('i_sz_p20220101');
SELECT bytes_before = :heap_bytes AS still_the_first FROM snouttime.partition_info
WHERE series = 'i_sz'::regclass AND name = 'i_sz_p20220101';
-- an unseal forgets it, and so does a drop
SELECT snouttime.unseal('i_sz_p20220101');
SELECT bytes_before FROM snouttime.partition_info
WHERE series = 'i_sz'::regclass AND name = 'i_sz_p20220101';
SELECT snouttime.seal('i_sz_p20220102');
SELECT 'i_sz_p20220101'::regclass::oid AS p1, 'i_sz_p20220102'::regclass::oid AS p2 \gset
SELECT count(*) AS recorded FROM snouttime.seal_sizes WHERE relid::oid IN (:p1, :p2);
DROP TABLE i_sz;
SELECT count(*) AS recorded FROM snouttime.seal_sizes WHERE relid::oid IN (:p1, :p2);

-- ---- the oldest and newest range are ordered as numbers on an integer series ----
CREATE TABLE i_int (k int8 NOT NULL, v int);
SELECT snouttime.create_series('i_int', 'k', partition_width => 5000, premake => 1);
SELECT snouttime.make_partitions('i_int', '5000', '15000');
SELECT oldest_range, newest_range FROM snouttime.series_info WHERE series = 'i_int'::regclass;
