-- SnoutTime's own scan of sealed tables. It must return exactly what the plain
-- scan returns, for any WHERE clause, while decoding fewer columns and skipping row groups.
SET client_min_messages = warning;
SET timezone = 'UTC';
SET snouttime.columnar_group_rows = 1000;

CREATE TABLE cs (host text, ts timestamptz, d date, n int4, big int8, usage float8, note text);
INSERT INTO cs
SELECT 'host_' || (g % 10), timestamptz '2026-01-01+00' + g * interval '1 minute', date '2026-01-01' + g / 1440,
	g, g::int8 * 1000000000, g / 7.0, CASE WHEN g % 100 = 0 THEN NULL ELSE md5(g::text) END
FROM generate_series(0, 49999) AS g;
CREATE TABLE cs_heap AS SELECT * FROM cs;
SET snouttime.columnar_order_by = 'ts';
ALTER TABLE cs SET ACCESS METHOD snouttime_columnar;
RESET snouttime.columnar_order_by;
ANALYZE cs;
-- late rows and a delete, so the delta store and delete log are read too
INSERT INTO cs VALUES ('late', '2026-01-10 00:00:30+00', '2026-01-10', -1, -1, 0, 'late');
INSERT INTO cs_heap VALUES ('late', '2026-01-10 00:00:30+00', '2026-01-10', -1, -1, 0, 'late');
DELETE FROM cs WHERE n BETWEEN 100 AND 199;
DELETE FROM cs_heap WHERE n BETWEEN 100 AND 199;

-- only this node's own lines, so the output is the same on every Postgres version (18 prints
-- fractional row counts and buffers by default)
CREATE FUNCTION cs_explain(q text) RETURNS SETOF text LANGUAGE plpgsql AS $$
DECLARE
	l text;
BEGIN
	FOR l IN EXECUTE 'EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) ' || q LOOP
		IF l ~ 'SnoutTime Columnar Scan|Columns Decoded|Row Group' THEN
			RETURN NEXT regexp_replace(btrim(l), ' \(actual.*\)$', '');
		END IF;
	END LOOP;
END $$;
EXPLAIN (COSTS OFF) SELECT count(*), sum(usage) FROM cs WHERE ts >= '2026-01-10' AND ts < '2026-01-11';
SELECT cs_explain($$SELECT count(*), sum(usage) FROM cs WHERE ts >= '2026-01-10' AND ts < '2026-01-11'$$);

-- equal to the heap, and to the plain scan, for a set of queries
CREATE FUNCTION cs_check(q text) RETURNS text LANGUAGE plpgsql AS $$
DECLARE
	a text; b text; c text;
BEGIN
	EXECUTE format('SELECT md5(string_agg(x::text, '','' ORDER BY x::text)) FROM (%s) x', replace(q, '$t', 'cs')) INTO a;
	SET LOCAL snouttime.columnar_custom_scan = off;
	EXECUTE format('SELECT md5(string_agg(x::text, '','' ORDER BY x::text)) FROM (%s) x', replace(q, '$t', 'cs')) INTO b;
	SET LOCAL snouttime.columnar_custom_scan = on;
	EXECUTE format('SELECT md5(string_agg(x::text, '','' ORDER BY x::text)) FROM (%s) x', replace(q, '$t', 'cs_heap')) INTO c;
	RETURN CASE WHEN a IS NOT DISTINCT FROM b AND b IS NOT DISTINCT FROM c THEN 'same' ELSE 'DIFFERENT' END;
END $$;
SELECT q, cs_check(q) FROM (VALUES
	('SELECT * FROM $t'),
	('SELECT host, n FROM $t WHERE ts >= ''2026-01-10'' AND ts < ''2026-01-11'''),
	('SELECT n FROM $t WHERE ts = ''2026-01-05 12:00+00'''),
	('SELECT n FROM $t WHERE ''2026-01-05 12:00+00'' > ts AND n > 7000'),
	('SELECT count(*) FROM $t WHERE n < 0'),
	('SELECT count(*) FROM $t WHERE n <= 150'),
	('SELECT count(*) FROM $t WHERE big BETWEEN 3000000000000 AND 4000000000000'),
	('SELECT count(*) FROM $t WHERE d = ''2026-01-20'''),
	('SELECT count(*) FROM $t WHERE n > 49990 OR host = ''late'''),
	('SELECT note FROM $t WHERE n BETWEEN 990 AND 1010'),
	('SELECT $t FROM $t WHERE n < 5'),
	('SELECT ctid IS NOT NULL, n FROM $t WHERE n < 3'),
	('SELECT count(*), max(usage) FROM $t'),
	('SELECT host, count(*) FROM $t WHERE ts < ''2026-01-02'' GROUP BY host')
) AS v(q);

-- parallel: every row once (the scan's own; the aggregate node has its test in columnar_agg)
SET snouttime.columnar_aggregate = off;
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
SET min_parallel_table_scan_size = 0;
SET max_parallel_workers_per_gather = 2;
EXPLAIN (COSTS OFF) SELECT count(*), sum(n) FROM cs WHERE ts < '2026-01-20';
SELECT count(*), sum(n) FROM cs WHERE ts < '2026-01-20';
SELECT count(*), sum(n) FROM cs_heap WHERE ts < '2026-01-20';
RESET parallel_setup_cost;
RESET parallel_tuple_cost;
RESET min_parallel_table_scan_size;
RESET max_parallel_workers_per_gather;
RESET snouttime.columnar_aggregate;

-- through a series table: pruning first, then row groups
CREATE TABLE cs_series (ts timestamptz NOT NULL, v float8);
SELECT snouttime.create_series('cs_series', 'ts', partition_interval => '1 day', premake => 1);
SELECT snouttime.make_partitions('cs_series', '2026-01-01', '2026-01-03');
INSERT INTO cs_series SELECT timestamptz '2026-01-01+00' + g * interval '10 seconds', g FROM generate_series(0, 2 * 8640 - 1) g;
SELECT snouttime.seal('cs_series_p20260101');
SELECT cs_explain($$SELECT count(*) FROM cs_series WHERE ts >= '2026-01-01 06:00' AND ts < '2026-01-01 07:00'$$);
SELECT count(*) FROM cs_series WHERE ts >= '2026-01-01 06:00' AND ts < '2026-01-01 07:00';

-- Keys whose value is known only when the scan starts (2026-09-23): a stable expression, a
-- generic plan's parameter (NULL too), and a correlated subquery that rescans the node with a
-- different value each time. Every one skips row groups and returns what the heap returns.
SELECT cs_explain($$SELECT count(*) FROM cs WHERE ts >= timestamptz '2026-01-30+00' + interval '1 day'$$);
SELECT q, cs_check(q) FROM (VALUES
	('SELECT count(*), sum(n) FROM $t WHERE ts >= timestamptz ''2026-01-30+00'' + interval ''1 day'''),
	('SELECT count(*) FROM $t WHERE ts < now() - interval ''100 years'''),
	('SELECT n FROM $t WHERE n = 40000 + 1'),
	('SELECT o.g, (SELECT count(*) FROM $t WHERE n >= o.g * 10000 AND n < o.g * 10000 + 50) FROM generate_series(0, 5) AS o(g)')
) AS v(q);
SET plan_cache_mode = force_generic_plan;
PREPARE p(timestamptz) AS SELECT count(*), sum(n) FROM cs WHERE ts >= $1;
EXECUTE p('2026-01-31+00');
SELECT count(*), sum(n) FROM cs_heap WHERE ts >= '2026-01-31+00';
EXECUTE p(NULL);
SELECT cs_explain('EXECUTE p(''2026-01-31+00'')');
DEALLOCATE p;
RESET plan_cache_mode;

-- Float comparisons applied to decoded values before a row is formed (2026-09-23): NaN above
-- everything and equal to itself, -0 equal to 0, float4 against float8 constants, and NULLs.
CREATE TABLE fl (id int, d float8, r float4);
INSERT INTO fl SELECT g, CASE g % 50 WHEN 0 THEN 'NaN' WHEN 1 THEN '-0' WHEN 2 THEN NULL WHEN 3 THEN 'Infinity' WHEN 4 THEN '-Infinity' ELSE g / 7.0 END,
	CASE g % 40 WHEN 0 THEN 'NaN'::float4 WHEN 1 THEN NULL ELSE (g / 3.0)::float4 END
FROM generate_series(0, 9999) g;
CREATE TABLE fl_heap AS SELECT * FROM fl;
SELECT snouttime.seal('fl');
CREATE FUNCTION fl_check(q text) RETURNS text LANGUAGE plpgsql AS $$
DECLARE
	a text; b text;
BEGIN
	EXECUTE format('SELECT md5(string_agg(x::text, '','' ORDER BY x::text)) FROM (%s) x', replace(q, '$t', 'fl')) INTO a;
	EXECUTE format('SELECT md5(string_agg(x::text, '','' ORDER BY x::text)) FROM (%s) x', replace(q, '$t', 'fl_heap')) INTO b;
	RETURN CASE WHEN a IS NOT DISTINCT FROM b THEN 'same' ELSE 'DIFFERENT' END;
END $$;
SELECT q, fl_check(q) FROM (VALUES
	('SELECT id, d FROM $t WHERE d > 1000'),
	('SELECT id, d FROM $t WHERE d >= ''NaN'''),
	('SELECT id, d FROM $t WHERE d = ''NaN'''),
	('SELECT id, d FROM $t WHERE d < ''NaN'''),
	('SELECT id, d FROM $t WHERE d = 0'),
	('SELECT id, d FROM $t WHERE d <= 0'),
	('SELECT id, d FROM $t WHERE 500.5 < d'),
	('SELECT id, r FROM $t WHERE r > 3000'),
	('SELECT id, r FROM $t WHERE r = ''NaN''::float4'),
	('SELECT id, r FROM $t WHERE r <= 10.5::float4'),
	('SELECT id FROM $t WHERE d > 100 AND r < 2000 AND id % 3 = 0')
) AS v(q);
SELECT cs_explain('SELECT * FROM fl WHERE d > 1000');
