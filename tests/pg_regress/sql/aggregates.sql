-- first and last. Semantics in README.md ("first and last").
SET client_min_messages = warning;
SET timezone = 'UTC';

CREATE TABLE a_points (host text, at timestamptz, v numeric, label text);
INSERT INTO a_points VALUES
	('a', '2026-09-22 10:00+00', 1.5, 'one'), ('a', '2026-09-22 09:00+00', 0.5, 'zero'),
	('a', '2026-09-22 11:00+00', 2.5, 'two'), ('a', NULL, 99, 'ignored: no time'),
	('b', '2026-09-22 08:00+00', NULL, 'b-first'), ('b', '2026-09-22 12:00+00', 7, NULL);
SELECT host, snouttime.first(v, at), snouttime.last(v, at), snouttime.first(label, at) AS first_label,
	snouttime.last(label, at) AS last_label
FROM a_points GROUP BY host ORDER BY host;
-- no rows, or only rows without a time: NULL
SELECT snouttime.first(v, at) FROM a_points WHERE false;
SELECT snouttime.first(v, at) FROM a_points WHERE at IS NULL;
-- every kind of time
SELECT snouttime.first(x, t) AS by_timestamp, snouttime.last(x, t::date) AS by_date,
	snouttime.first(x, n) AS by_bigint, snouttime.last(x, n::int) AS by_integer
FROM (VALUES ('p', timestamp '2026-01-02', 5::bigint), ('q', timestamp '2026-01-01', 9::bigint),
	('r', timestamp '2026-01-03', -3::bigint)) AS s(x, t, n);

-- ---- in parallel: partial states computed in workers, serialised, combined ----
CREATE TABLE a_big (host int, at timestamptz, v numeric, label text);
INSERT INTO a_big
SELECT g % 7, timestamptz '2026-01-01+00' + ((g * 7919) % 200000) * interval '1 minute',
	g::numeric / 3, 'row ' || g
FROM generate_series(1, 200000) AS g;
ANALYZE a_big;
SET max_parallel_workers_per_gather = 2;
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
SET min_parallel_table_scan_size = 0;
SET parallel_leader_participation = off;
SET enable_hashagg = off;
-- the plan really is partial (workers aggregate, the leader combines)
CREATE FUNCTION a_plan(q text) RETURNS SETOF text LANGUAGE plpgsql AS $$
BEGIN
	RETURN QUERY EXECUTE 'EXPLAIN (COSTS OFF) ' || q;
END $$;
SELECT count(*) > 0 AS partial_aggregate_used
FROM a_plan('SELECT host, snouttime.first(v, at), snouttime.last(label, at) FROM a_big GROUP BY host') AS line
WHERE line LIKE '%Partial%Aggregate%';
-- and gives exactly what a plain ordered aggregate gives
SELECT count(*) AS groups_that_differ
FROM (SELECT host, snouttime.first(v, at) AS f, snouttime.last(v, at) AS l,
		snouttime.first(label, at) AS fl, snouttime.last(label, at) AS ll
	FROM a_big GROUP BY host) AS s
JOIN (SELECT host, (array_agg(v ORDER BY at))[1] AS f, (array_agg(v ORDER BY at DESC))[1] AS l,
		(array_agg(label ORDER BY at))[1] AS fl, (array_agg(label ORDER BY at DESC))[1] AS ll
	FROM a_big GROUP BY host) AS r USING (host)
WHERE s.f IS DISTINCT FROM r.f OR s.l IS DISTINCT FROM r.l
	OR s.fl IS DISTINCT FROM r.fl OR s.ll IS DISTINCT FROM r.ll;
-- histogram in parallel equals histogram without
SELECT (SELECT snouttime.histogram((v % 1000)::float8, 0, 1000, 10) FROM a_big)
	= (SELECT array_agg(c ORDER BY slot) FROM (SELECT slot, count(b) AS c FROM generate_series(0, 11) AS slot
		LEFT JOIN (SELECT width_bucket((v % 1000)::float8, 0, 1000, 10) AS b FROM a_big) x ON x.b = slot
		GROUP BY slot) y) AS parallel_histogram_equals_width_bucket;
RESET ALL;
SET client_min_messages = warning;

-- ---- histogram(value, min, max, buckets): width_bucket's slots, counted ----
-- slot 0 is below min, 1..5 split [0, 10), slot 6 is 10 and above
SELECT snouttime.histogram(v, 0, 10, 5)
FROM (VALUES (-1::float8), (0), (1.999), (2), (9.999), (10), (1e9), (NULL)) AS s(v);
-- the same counts as grouping by Postgres's own width_bucket, edges included
CREATE TABLE a_vals AS
SELECT ((g * 7919) % 2400)::float8 / 100 - 6 AS v FROM generate_series(1, 5000) AS g
UNION ALL SELECT unnest(ARRAY[-5, 17, -5.000001, 16.999999, -5 + 22.0 / 7, -5 + 44.0 / 7]);
SELECT snouttime.histogram(v, -5, 17, 7) = (SELECT array_agg(c ORDER BY slot)
		FROM (SELECT slot, count(b) AS c FROM generate_series(0, 8) AS slot
			LEFT JOIN (SELECT width_bucket(v, -5, 17, 7) AS b FROM a_vals) x ON x.b = slot GROUP BY slot) y)
	AS equals_width_bucket
FROM a_vals;
-- no rows: NULL
SELECT snouttime.histogram(v, 0, 1, 2) FROM a_vals WHERE false;
\set ON_ERROR_STOP 0
SELECT snouttime.histogram(v, 0, 10, 0) FROM a_vals;
SELECT snouttime.histogram(v, 10, 0, 5) FROM a_vals;
SELECT snouttime.histogram(v, 0, 'infinity', 5) FROM a_vals;
SELECT snouttime.histogram(v, 0, NULL, 5) FROM a_vals;
SELECT snouttime.histogram(v, 0, v + 100, 5) FROM a_vals;
\set ON_ERROR_STOP 1

-- ---- counter_delta and counter_rate: increase, with a drop read as a reset ----
CREATE TABLE a_counter (host text, at timestamptz, v float8);
INSERT INTO a_counter VALUES
	('a', '2026-09-22 00:00+00', 100), ('a', '2026-09-22 00:01+00', 130),
	('a', '2026-09-22 00:02+00', 20),   -- reset: counted as +20, not -110
	('a', '2026-09-22 00:03+00', 50), ('a', '2026-09-22 00:04+00', NULL),
	('b', '2026-09-22 00:00+00', 5);
SELECT host, snouttime.counter_delta(v, at ORDER BY at) AS delta,
	snouttime.counter_rate(v, at ORDER BY at) AS per_second
FROM a_counter GROUP BY host ORDER BY host;
-- out of order is an error that names the fix, never a wrong number
\set ON_ERROR_STOP 0
SELECT snouttime.counter_delta(v, at ORDER BY at DESC) FROM a_counter WHERE host = 'a';
\set ON_ERROR_STOP 1
-- The state merges across time ranges that do not overlap, which is what a rollup does:
-- partitionwise aggregation computes one partial state per day and combines them, here with
-- a reset exactly on a partition boundary.
CREATE TABLE a_counted (host text, at timestamptz NOT NULL, v float8) PARTITION BY RANGE (at);
CREATE TABLE a_counted_1 PARTITION OF a_counted FOR VALUES FROM ('2026-09-01+00') TO ('2026-09-02+00');
CREATE TABLE a_counted_2 PARTITION OF a_counted FOR VALUES FROM ('2026-09-02+00') TO ('2026-09-03+00');
CREATE TABLE a_counted_3 PARTITION OF a_counted FOR VALUES FROM ('2026-09-03+00') TO ('2026-09-04+00');
INSERT INTO a_counted
SELECT 'h' || (g % 3), timestamptz '2026-09-01+00' + g * interval '1 minute',
	-- counts up, and resets at the start of day 3 (minute 2880)
	CASE WHEN g < 2880 THEN g * 2 ELSE (g - 2880) * 2 + 1 END
FROM generate_series(0, 3 * 1440 - 1) AS g;
ANALYZE a_counted;
SET enable_partitionwise_aggregate = on;
SET max_parallel_workers_per_gather = 0;
SELECT count(*) > 0 AS partial_per_partition
FROM a_plan('SELECT host, snouttime.counter_delta(v, at) FROM a_counted GROUP BY host') AS line
WHERE line LIKE '%Partial%Aggregate%';
SELECT host, snouttime.counter_delta(v, at) AS combined,
	(SELECT snouttime.counter_delta(v, at ORDER BY at) FROM a_counted c WHERE c.host = a.host) AS in_one_go
FROM a_counted a GROUP BY host ORDER BY host;
RESET enable_partitionwise_aggregate;
RESET max_parallel_workers_per_gather;
