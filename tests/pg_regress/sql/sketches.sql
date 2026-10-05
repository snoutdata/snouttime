-- Percentile and distinct-count sketches. Semantics in README.md ("Sketches").
-- Estimates are checked against the exact answer with a bound, not printed, so the file does
-- not depend on the last digit of a float.
SET client_min_messages = warning;

CREATE TABLE s_values AS
SELECT g AS n, g % 10 AS grp, ((g * 7919) % 100000)::float8 AS v, 'user-' || (g % 25000) AS who
FROM generate_series(1, 200000) AS g;
ANALYZE s_values;

-- ---- percentile_sketch / percentile ----
-- v is 0..99999, each twice: the median is about 50000, the 99th percentile about 99000
SELECT abs(snouttime.percentile(snouttime.percentile_sketch(v), 0.5) - 50000) < 1000 AS median_close,
	abs(snouttime.percentile(snouttime.percentile_sketch(v), 0.99) - 99000) < 200 AS p99_close,
	snouttime.percentile(snouttime.percentile_sketch(v), 0) AS minimum,
	snouttime.percentile(snouttime.percentile_sketch(v), 1) AS maximum
FROM s_values;
-- rank error of every quantile asked for, against the exact rank
WITH s AS (SELECT snouttime.percentile_sketch(v) AS d FROM s_values),
q AS (SELECT unnest(ARRAY[0.001, 0.01, 0.1, 0.5, 0.9, 0.99, 0.999]) AS q)
SELECT q.q, abs((SELECT count(*) FROM s_values WHERE v <= snouttime.percentile(s.d, q.q))::float8 / 200000 - q.q)
	< CASE WHEN q.q < 0.02 OR q.q > 0.98 THEN 0.001 ELSE 0.01 END AS within_rank_error
FROM s, q ORDER BY q.q;
-- several at once, and the count
SELECT array_length(snouttime.percentile(snouttime.percentile_sketch(v), ARRAY[0.1, 0.5, 0.9]), 1) AS three,
	snouttime.sketch_count(snouttime.percentile_sketch(v)) AS counted
FROM s_values;
-- a few values are exact; none is NULL; NULL and NaN are skipped
SELECT snouttime.percentile(snouttime.percentile_sketch(x), 0.5) AS middle_of_three
FROM (VALUES (3.0), (1.0), (2.0), (NULL), ('NaN')) AS t(x);
SELECT snouttime.percentile_sketch(x) IS NULL AS empty_is_null FROM (VALUES (NULL::float8)) AS t(x);
-- merge: sketches per group, merged, answer like one sketch of everything
SELECT abs(snouttime.percentile(snouttime.merge(d), 0.5) - 50000) < 1000 AS merged_median_close,
	snouttime.sketch_count(snouttime.merge(d)) AS merged_count
FROM (SELECT grp, snouttime.percentile_sketch(v) AS d FROM s_values GROUP BY grp) g;
-- the text form round-trips
SELECT snouttime.percentile(snouttime.percentile_sketch(v)::text::snouttime.tdigest, 0.5)
	= snouttime.percentile(snouttime.percentile_sketch(v), 0.5) AS text_round_trip
FROM s_values;
SELECT snouttime.percentile_sketch(x, 10) AS tiny FROM (VALUES (1.0), (2.0)) AS t(x);

-- ---- distinct_sketch / distinct_count ----
-- 25,000 distinct users; 1.6% standard error at the default precision, four of them allowed
SELECT abs(snouttime.distinct_count(snouttime.distinct_sketch(who)) - 25000) < 1600 AS users_close,
	abs(snouttime.distinct_count(snouttime.distinct_sketch(n)) - 200000) < 13000 AS bigints_close,
	abs(snouttime.distinct_count(snouttime.distinct_sketch(v)) - 100000) < 6500 AS floats_close
FROM s_values;
-- small counts are close to exact, duplicates and NULLs do not count
SELECT snouttime.distinct_count(snouttime.distinct_sketch(x)) AS three
FROM (VALUES ('a'), ('b'), ('c'), ('a'), (NULL)) AS t(x);
SELECT snouttime.distinct_count(snouttime.distinct_sketch(x)) AS none FROM (VALUES (NULL::int)) AS t(x);
-- merge is exactly the sketch of the union
SELECT (SELECT snouttime.merge(h)::text FROM (SELECT grp, snouttime.distinct_sketch(who) AS h FROM s_values GROUP BY grp) g)
	= (SELECT snouttime.distinct_sketch(who)::text FROM s_values) AS merge_is_union;
-- more precision, less error
SELECT abs(snouttime.distinct_count(snouttime.distinct_sketch(n, 16)) - 200000) < 3300 AS p16_close FROM s_values;
-- the text form round-trips
SELECT snouttime.distinct_count(snouttime.distinct_sketch(who)::text::snouttime.hll)
	= snouttime.distinct_count(snouttime.distinct_sketch(who)) AS hll_text_round_trip
FROM s_values;

-- ---- in parallel, with the leader not participating ----
SET max_parallel_workers_per_gather = 2;
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
SET min_parallel_table_scan_size = 0;
SET parallel_leader_participation = off;
CREATE FUNCTION s_plan(q text) RETURNS SETOF text LANGUAGE plpgsql AS $$
BEGIN
	RETURN QUERY EXECUTE 'EXPLAIN (COSTS OFF) ' || q;
END $$;
SELECT count(*) > 0 AS partial_aggregate_used
FROM s_plan('SELECT snouttime.percentile_sketch(v), snouttime.distinct_sketch(who) FROM s_values') AS line
WHERE line LIKE '%Partial%Aggregate%';
SELECT abs(snouttime.percentile(snouttime.percentile_sketch(v), 0.5) - 50000) < 1000 AS parallel_median_close,
	snouttime.distinct_sketch(who)::text = (SELECT snouttime.distinct_sketch(who)::text FROM s_values) AS parallel_hll_identical
FROM s_values;
RESET ALL;
SET client_min_messages = warning;

-- ---- refusals: a corrupt sketch is an error, never a crash ----
\set ON_ERROR_STOP 0
SELECT snouttime.percentile(snouttime.percentile_sketch(v), 1.5) FROM s_values;
SELECT snouttime.percentile_sketch(v, 5) FROM s_values;
SELECT '{"compression":100,"count":3,"min":1,"max":3,"means":[3,1,2],"weights":[1,1,1]}'::snouttime.tdigest;
SELECT '{"compression":100,"count":9,"min":1,"max":3,"means":[1,2,3],"weights":[1,1,1]}'::snouttime.tdigest;
SELECT '{"compression":100,"count":2,"min":1,"max":3,"means":[1,2],"weights":[1,-1]}'::snouttime.tdigest;
SELECT 'not json'::snouttime.tdigest;
SELECT '{"precision":4,"registers":"00"}'::snouttime.hll;
SELECT '{"precision":4,"registers":"ff000000000000000000000000000000"}'::snouttime.hll;
SELECT '{"precision":4,"registers":"zz000000000000000000000000000000"}'::snouttime.hll;
SELECT snouttime.distinct_sketch(x, 3) FROM (VALUES (1)) AS t(x);
SELECT snouttime.merge(h) FROM (SELECT snouttime.distinct_sketch(1, 10) UNION ALL SELECT snouttime.distinct_sketch(1, 12)) AS t(h);
SELECT snouttime.distinct_sketch(p) FROM (VALUES (point(1, 2))) AS t(p);
\set ON_ERROR_STOP 1
-- the smallest valid ones are accepted
SELECT snouttime.distinct_count('{"precision":4,"registers":"00000000000000000000000000000000"}'::snouttime.hll) AS empty_hll;
SELECT snouttime.percentile('{"compression":100,"count":3,"min":1,"max":3,"means":[1,2,3],"weights":[1,1,1]}'::snouttime.tdigest, 0.5) AS typed_in;
