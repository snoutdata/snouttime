-- Aggregates computed on a sealed table's columns (`SnoutTime Columnar Aggregate`),
-- as the partial half of Postgres's own two-phase aggregate. Every query must return exactly
-- what the same query returns on a heap twin, with and without the node, in parallel and per
-- partition. Floats are multiples of a quarter, so their sums do not depend on the order rows
-- are added in.
SET client_min_messages = warning;
SET timezone = 'UTC';
SET snouttime.columnar_group_rows = 500;

CREATE TABLE ag (ts timestamptz NOT NULL, host text, n int4, s int2, v float8, r float4, ok bool, u uuid);
INSERT INTO ag
SELECT timestamptz '2026-01-01+00' + g * interval '37 seconds',
	CASE WHEN g % 50 = 0 THEN NULL ELSE 'host_' || (g % 7) END,
	CASE WHEN g % 13 = 0 THEN NULL ELSE g % 1000 - 300 END,
	(g % 30)::int2,
	CASE WHEN g % 17 = 0 THEN NULL ELSE (g % 400) / 4.0 END,
	((g % 80) / 4.0)::float4,
	g % 3 = 0,
	('00000000-0000-0000-0000-00000000000' || (g % 4))::uuid
FROM generate_series(0, 19999) AS g;
CREATE TABLE ag_heap AS SELECT * FROM ag;
SELECT snouttime.seal('ag');
-- late rows and deletes, so the delta store and the delete log count
INSERT INTO ag VALUES ('2026-01-03 00:00:01+00', 'late', 5, 1, 2.5, 1.25, true, NULL), ('2026-01-03 00:00:02+00', NULL, NULL, NULL, NULL, NULL, NULL, NULL);
INSERT INTO ag_heap VALUES ('2026-01-03 00:00:01+00', 'late', 5, 1, 2.5, 1.25, true, NULL), ('2026-01-03 00:00:02+00', NULL, NULL, NULL, NULL, NULL, NULL, NULL);
DELETE FROM ag WHERE n BETWEEN 100 AND 140;
DELETE FROM ag_heap WHERE n BETWEEN 100 AND 140;
ANALYZE ag;
ANALYZE ag_heap;

-- parallel, so Postgres plans a partial aggregate under a Gather
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
SET min_parallel_table_scan_size = 0;
SET max_parallel_workers_per_gather = 2;

CREATE FUNCTION ag_nodes(q text) RETURNS SETOF text LANGUAGE plpgsql AS $$
DECLARE
	l text;
BEGIN
	FOR l IN EXECUTE 'EXPLAIN (COSTS OFF) ' || q LOOP
		IF l ~ 'Aggregate|Gather|Append' THEN
			RETURN NEXT regexp_replace(btrim(l), '\s+on .*$', '');
		END IF;
	END LOOP;
END $$;

CREATE FUNCTION ag_check(q text) RETURNS text LANGUAGE plpgsql AS $$
DECLARE
	a text; b text; c text;
BEGIN
	EXECUTE format('SELECT md5(string_agg(x::text, '','' ORDER BY x::text)) FROM (%s) x', replace(q, '$t', 'ag')) INTO a;
	SET LOCAL snouttime.columnar_aggregate = off;
	EXECUTE format('SELECT md5(string_agg(x::text, '','' ORDER BY x::text)) FROM (%s) x', replace(q, '$t', 'ag')) INTO b;
	SET LOCAL snouttime.columnar_aggregate = on;
	EXECUTE format('SELECT md5(string_agg(x::text, '','' ORDER BY x::text)) FROM (%s) x', replace(q, '$t', 'ag_heap')) INTO c;
	RETURN CASE WHEN a IS NOT DISTINCT FROM b AND b IS NOT DISTINCT FROM c THEN 'same' ELSE 'DIFFERENT' END;
END $$;

SELECT ag_nodes('SELECT host, count(*), avg(v) FROM ag GROUP BY host');
SELECT q, ag_check(q) FROM (VALUES
	('SELECT count(*) FROM $t'),
	('SELECT host, count(*), count(n), sum(n), avg(n), min(n), max(n) FROM $t GROUP BY host'),
	('SELECT host, sum(v), avg(v), min(v), max(v), sum(r), avg(r), min(r), max(r) FROM $t GROUP BY host'),
	('SELECT s, sum(s), avg(s) FROM $t GROUP BY s'),
	('SELECT ok, u, count(*), min(ts), max(ts) FROM $t GROUP BY ok, u'),
	('SELECT date_bin(''1 hour'', ts, ''2000-01-01''), avg(v), max(n) FROM $t GROUP BY 1'),
	('SELECT snouttime.bucket(''15 minutes'', ts), host, count(*), sum(v) FROM $t GROUP BY 1, 2'),
	('SELECT snouttime.bucket(''1 day'', ts, timestamptz ''2026-01-01 06:00+00''), count(*) FROM $t GROUP BY 1'),
	('SELECT snouttime.bucket(''1 month'', ts), count(*) FROM $t GROUP BY 1'),
	('SELECT host, count(*), avg(v) FROM $t WHERE ts >= ''2026-01-02'' AND ts < ''2026-01-02 12:00'' GROUP BY host'),
	('SELECT host, max(v) FROM $t WHERE ts >= timestamptz ''2026-01-05+00'' - interval ''1 day'' GROUP BY host'),
	('SELECT count(*), sum(n) FROM $t WHERE n > 500'),
	('SELECT count(*) FROM $t WHERE n IS NULL'),
	('SELECT host, count(*) FROM $t GROUP BY host HAVING avg(v) > 50'),
	('SELECT host, count(DISTINCT n) FROM $t GROUP BY host'),
	('SELECT host, count(*) FROM $t WHERE host LIKE ''host_1%'' GROUP BY host'),
	('SELECT host, snouttime.first(v, ts), snouttime.last(v, ts), snouttime.first(host, ts), snouttime.last(u, ts) FROM $t GROUP BY host'),
	('SELECT ok, snouttime.first(r, ts), snouttime.last(ok, ts) FROM $t GROUP BY ok'),
	-- an integer time; n is unique in these rows, since a tie's winner is not defined
	('SELECT snouttime.first(v, n), snouttime.last(host, n) FROM $t WHERE ts < ''2026-01-01 10:00'''),
	('SELECT snouttime.first(n, ts), snouttime.last(n, ts) FROM $t WHERE ts < ''2026-01-02'''),
	-- null tests, answered from the null bitmap
	('SELECT host, count(*), sum(v) FROM $t WHERE v IS NOT NULL GROUP BY host'),
	('SELECT ok, count(*) FROM $t WHERE u IS NULL AND ts >= ''2026-01-02'' GROUP BY ok'),
	('SELECT host, count(*) FROM $t WHERE host IS NULL GROUP BY host'),
	('SELECT count(*) FROM $t WHERE n IS NOT NULL AND v IS NULL')
) AS v(q);
SELECT ag_nodes('SELECT host, count(*) FROM ag WHERE v IS NOT NULL GROUP BY host');
SELECT ag_nodes('SELECT host, snouttime.last(v, ts) FROM ag GROUP BY host');

-- the aggregate node over only the column store: every row group read, none formed into rows
SELECT ag_nodes('SELECT host, count(*) FROM ag WHERE n > 500 GROUP BY host');

-- a series, aggregated partition by partition
CREATE TABLE sa (ts timestamptz NOT NULL, host text NOT NULL, v float8);
SELECT snouttime.create_series('sa', 'ts', partition_interval => '1 day', premake => 1);
SELECT snouttime.make_partitions('sa', '2026-01-01', '2026-01-04');
INSERT INTO sa SELECT timestamptz '2026-01-01+00' + g * interval '1 minute', 'h' || (g % 5), (g % 100) / 2.0 FROM generate_series(0, 3 * 1440 - 1) g;
CREATE TABLE sa_heap AS SELECT * FROM sa;
SELECT snouttime.seal('sa_p20260101'), snouttime.seal('sa_p20260102');
ANALYZE sa;
SET enable_partitionwise_aggregate = on;
SELECT ag_nodes('SELECT host, avg(v) FROM sa GROUP BY host');
SELECT (SELECT md5(string_agg(x::text, ',' ORDER BY x::text)) FROM (SELECT host, count(*), avg(v), max(v) FROM sa GROUP BY host) x)
	= (SELECT md5(string_agg(x::text, ',' ORDER BY x::text)) FROM (SELECT host, count(*), avg(v), max(v) FROM sa_heap GROUP BY host) x) AS partitionwise_same;
SELECT (SELECT md5(string_agg(x::text, ',' ORDER BY x::text)) FROM (SELECT date_bin('5 minutes', ts, '2000-01-01'), avg(v) FROM sa WHERE ts >= '2026-01-01 12:00' GROUP BY 1) x)
	= (SELECT md5(string_agg(x::text, ',' ORDER BY x::text)) FROM (SELECT date_bin('5 minutes', ts, '2000-01-01'), avg(v) FROM sa_heap WHERE ts >= '2026-01-01 12:00' GROUP BY 1) x) AS buckets_same;
-- a bound known only at run time over sealed and live partitions: the node's paths are chosen
-- per partition without adding them to the partition's relation, whose paths Postgres's own
-- Append still points at (a freed one read garbage node tags at 10M rows, 2026-09-25)
SELECT (SELECT md5(string_agg(x::text, ',' ORDER BY x::text)) FROM (SELECT host, count(*), avg(v) FROM sa WHERE ts >= timestamptz '2026-01-03 12:00+00' - interval '2 days' GROUP BY host) x)
	= (SELECT md5(string_agg(x::text, ',' ORDER BY x::text)) FROM (SELECT host, count(*), avg(v) FROM sa_heap WHERE ts >= timestamptz '2026-01-03 12:00+00' - interval '2 days' GROUP BY host) x) AS runtime_pruned_same;
RESET enable_partitionwise_aggregate;

-- without asking: SnoutTime turns partitionwise aggregation on while it plans a query over a
-- table with a sealed partition, and leaves the setting as it was
SHOW enable_partitionwise_aggregate;
SELECT ag_nodes('SELECT host, avg(v) FROM sa GROUP BY host');
SHOW enable_partitionwise_aggregate;
SET snouttime.partitionwise_aggregate = off;
SELECT ag_nodes('SELECT host, avg(v) FROM sa GROUP BY host');
RESET snouttime.partitionwise_aggregate;

-- serial: Postgres makes no partial-aggregate relation without parallelism or partitions, so
-- the node makes its own (a small sealed partition never qualifies for parallelism)
SET max_parallel_workers_per_gather = 0;
SELECT ag_nodes('SELECT host, count(*), avg(v) FROM ag GROUP BY host');
SELECT q, ag_check(q) FROM (VALUES
	('SELECT count(*) FROM $t'),
	('SELECT host, count(*), avg(v), sum(n), max(ts) FROM $t GROUP BY host'),
	('SELECT date_bin(''1 hour'', ts, ''2000-01-01''), avg(r) FROM $t WHERE n > 0 GROUP BY 1'),
	('SELECT host, snouttime.last(v, ts) FROM $t GROUP BY host HAVING count(*) > 100')
) AS v(q);
RESET max_parallel_workers_per_gather;
