-- A sealed partition is its own index on its sort key. The scan seeks by the
-- first and last keys the directory keeps per row group, returns rows in the sort order either
-- way (merging in late rows), and a non-unique index on a sealed partition holds only its late
-- rows. Every query must return what Postgres's own plan returns, and what a heap twin with a
-- btree returns, in the same order.
SET client_min_messages = warning;
SET timezone = 'UTC';
SET snouttime.columnar_group_rows = 300;

CREATE TABLE sk (ts timestamptz NOT NULL, host text, dev int4, v float8);
SELECT snouttime.create_series('sk', 'ts', partition_interval => '1 day', premake => 1);
-- premake made partitions around TODAY, which the plans below would name: dropped, so the
-- expected output does not change with the date the suite runs on (it failed on 2026-09-24).
DO $$
DECLARE p regclass;
BEGIN
	FOR p IN SELECT partition FROM snouttime.partition_info
		WHERE series = 'sk'::regclass AND range_start::timestamptz >= '2026-06-01' LOOP
		EXECUTE format('DROP TABLE %s', p);
	END LOOP;
END
$$;
SELECT snouttime.make_partitions('sk', '2026-01-01', '2026-01-05');
SELECT snouttime.set_sealing('sk', NULL::interval, order_by => '{host,ts}');
CREATE INDEX sk_host_ts ON sk (host, ts DESC);
CREATE INDEX sk_dev ON sk (dev);
INSERT INTO sk
SELECT timestamptz '2026-01-01+00' + g * interval '41 seconds',
	CASE WHEN g % 101 = 0 THEN NULL ELSE 'h' || (g % 13) END,
	g % 7,
	g / 8.0
FROM generate_series(0, 4 * 2100) AS g;
CREATE TABLE sk_heap AS SELECT * FROM sk;
CREATE INDEX ON sk_heap (host, ts DESC);
SELECT snouttime.seal('sk_p20260101'), snouttime.seal('sk_p20260102'), snouttime.seal('sk_p20260103');
-- late rows (some for a host that sorts first, some last, some NULL) and deletes
INSERT INTO sk VALUES ('2026-01-01 00:00:03+00', 'h0', 1, 0.5), ('2026-01-02 23:59:59+00', 'h9', 2, 1.5),
	('2026-01-01 12:00:01+00', NULL, 3, 2.5), ('2026-01-03 06:00:00.5+00', 'a', 4, 3.5), ('2026-01-02 06:00:00.5+00', 'zz', 5, 4.5);
INSERT INTO sk_heap VALUES ('2026-01-01 00:00:03+00', 'h0', 1, 0.5), ('2026-01-02 23:59:59+00', 'h9', 2, 1.5),
	('2026-01-01 12:00:01+00', NULL, 3, 2.5), ('2026-01-03 06:00:00.5+00', 'a', 4, 3.5), ('2026-01-02 06:00:00.5+00', 'zz', 5, 4.5);
DELETE FROM sk WHERE v BETWEEN 300 AND 320;
DELETE FROM sk_heap WHERE v BETWEEN 300 AND 320;
ANALYZE sk;
ANALYZE sk_heap;

CREATE FUNCTION sk_nodes(q text) RETURNS SETOF text LANGUAGE plpgsql AS $$
DECLARE
	l text;
BEGIN
	FOR l IN EXECUTE 'EXPLAIN (COSTS OFF) ' || q LOOP
		IF l ~ 'Limit|Sort|Append|Custom Scan|Index|Seq Scan|Order:|Seek' THEN
			RETURN NEXT regexp_replace(btrim(l), '\s+on (\w+) \w+$', ' on \1');
		END IF;
	END LOOP;
END $$;

-- the result in its own order, against the plain plan and the heap twin
CREATE FUNCTION sk_check(q text) RETURNS text LANGUAGE plpgsql AS $$
DECLARE
	a text; b text; c text;
BEGIN
	EXECUTE format('SELECT md5(string_agg(x::text, '';'')) FROM (%s) x', replace(q, '$t', 'sk')) INTO a;
	SET LOCAL snouttime.columnar_custom_scan = off;
	EXECUTE format('SELECT md5(string_agg(x::text, '';'')) FROM (%s) x', replace(q, '$t', 'sk')) INTO b;
	SET LOCAL snouttime.columnar_custom_scan = on;
	EXECUTE format('SELECT md5(string_agg(x::text, '';'')) FROM (%s) x', replace(q, '$t', 'sk_heap')) INTO c;
	RETURN CASE WHEN a IS NOT DISTINCT FROM b AND b IS NOT DISTINCT FROM c THEN 'same' ELSE 'DIFFERENT' END;
END $$;

-- the non-unique indexes of a sealed partition hold only its late rows, and the planner does
-- not see them
SELECT c.relname, (SELECT count(*) FROM sk_p20260101) AS rows, pg_relation_size(c.oid) <= 16384 AS late_rows_only
FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
WHERE i.indrelid = 'sk_p20260101'::regclass ORDER BY 1;
SELECT sk_nodes('SELECT * FROM sk_p20260101 WHERE dev = 3');

-- seeks and order, one partition and across partitions
SELECT sk_nodes('SELECT * FROM sk_p20260102 WHERE host = ''h3'' ORDER BY ts DESC LIMIT 3');
SELECT sk_nodes('SELECT ts, v FROM sk WHERE host = ''h3'' AND ts <= ''2026-01-02 12:00'' ORDER BY ts DESC LIMIT 1');
SELECT sk_nodes('SELECT * FROM sk ORDER BY host, ts LIMIT 5');
SELECT q, sk_check(q) FROM (VALUES
	('SELECT * FROM $t WHERE host = ''h3'' ORDER BY ts'),
	('SELECT * FROM $t WHERE host = ''h3'' ORDER BY ts DESC LIMIT 7'),
	('SELECT ts, v FROM $t WHERE host = ''h3'' AND ts <= ''2026-01-02 12:00'' ORDER BY ts DESC LIMIT 1'),
	('SELECT ts, v FROM $t WHERE host = ''h9'' AND ts > ''2026-01-02 12:00'' AND ts < ''2026-01-03 01:00'' ORDER BY ts'),
	('SELECT host, ts FROM $t WHERE host >= ''h5'' AND host < ''h8'' ORDER BY host, ts'),
	('SELECT host, ts FROM $t WHERE host > ''h7'' ORDER BY host DESC, ts DESC LIMIT 50'),
	('SELECT host, ts, v FROM $t ORDER BY host, ts LIMIT 40'),
	('SELECT host, ts, v FROM $t ORDER BY host DESC, ts DESC LIMIT 40'),
	('SELECT host, ts FROM $t WHERE host IS NULL ORDER BY ts'),
	('SELECT host, ts FROM $t WHERE host = ''zz'' OR host = ''a'' ORDER BY host, ts'),
	('SELECT count(*), sum(v) FROM $t WHERE host = ''h11'''),
	('SELECT count(*) FROM $t WHERE host = ''nope'''),
	('SELECT * FROM $t WHERE host = ''h0'' AND ts < ''2026-01-01 00:10'' ORDER BY ts'),
	-- late rows found through the late-rows index on dev, by the aggregate node and the scan
	('SELECT dev, count(*), sum(v) FROM $t WHERE dev = 4 GROUP BY dev'),
	('SELECT count(*), max(v) FROM $t WHERE dev >= 3 AND dev < 5'),
	('SELECT * FROM $t WHERE dev = 5 AND v > 100 ORDER BY ts'),
	('SELECT count(*) FROM $t WHERE dev = 99')
) AS v(q);

-- the as-of join, the SQL way: for each event, the latest reading of its host at or before it
CREATE TABLE ev AS SELECT timestamptz '2026-01-01+00' + g * interval '17 minutes' AS ts, 'h' || (g % 14) AS host FROM generate_series(0, 330) g;
ANALYZE ev;
SELECT sk_nodes('SELECT e.ts, c.v FROM ev e CROSS JOIN LATERAL (SELECT v FROM sk WHERE sk.host = e.host AND sk.ts <= e.ts ORDER BY sk.ts DESC LIMIT 1) c');
SELECT sk_check('SELECT e.ts, e.host, c.v FROM ev e LEFT JOIN LATERAL (SELECT v FROM $t WHERE $t.host = e.host AND $t.ts <= e.ts ORDER BY $t.ts DESC LIMIT 1) c ON true ORDER BY e.ts, e.host') AS asof_same;

-- one value's late rows streamed from the late-rows index: many late rows and
-- deletes for one host, read in either direction, stopped by a LIMIT or not, and through a
-- LATERAL that rescans once per host. sk_host_ts runs against the order (ts DESC), so this is
-- the stream read backward to go forward
INSERT INTO sk SELECT timestamptz '2026-01-02+00' + g * interval '97 seconds' + interval '0.25 seconds', 'h5', 100 + g % 3, -g
FROM generate_series(0, 300) g;
INSERT INTO sk_heap SELECT timestamptz '2026-01-02+00' + g * interval '97 seconds' + interval '0.25 seconds', 'h5', 100 + g % 3, -g
FROM generate_series(0, 300) g;
DELETE FROM sk WHERE host = 'h5' AND (v BETWEEN 400 AND 420 OR v BETWEEN -40 AND -30);
DELETE FROM sk_heap WHERE host = 'h5' AND (v BETWEEN 400 AND 420 OR v BETWEEN -40 AND -30);
ANALYZE sk;
ANALYZE sk_heap;
SELECT q, sk_check(q) FROM (VALUES
	('SELECT * FROM $t WHERE host = ''h5'' ORDER BY ts'),
	('SELECT * FROM $t WHERE host = ''h5'' ORDER BY ts DESC'),
	('SELECT * FROM $t WHERE host = ''h5'' ORDER BY ts DESC LIMIT 10'),
	('SELECT * FROM $t WHERE host = ''h5'' ORDER BY ts LIMIT 10'),
	('SELECT ts, v FROM $t WHERE host = ''h9'' ORDER BY ts DESC LIMIT 3'),
	('SELECT ts, v FROM $t WHERE host = ''zz'' ORDER BY ts'),
	('SELECT h.host, c.ts, c.v FROM (VALUES (''h5''), (''h9''), (''h0''), (''a''), (''zz''), (''h5''), (''nope'')) h(host)
		LEFT JOIN LATERAL (SELECT ts, v FROM $t WHERE $t.host = h.host ORDER BY ts DESC LIMIT 2) c ON true ORDER BY 1, 2')
) AS v(q);

-- and the stream is what answered, no further than the scan went: of h5's 290 late rows, a
-- LIMIT 10 takes only those merged into its ten (none backward, where the column store's rows
-- are later), and the whole host takes all of them
CREATE FUNCTION sk_streamed(q text) RETURNS SETOF text LANGUAGE plpgsql AS $$
DECLARE
	l text;
BEGIN
	FOR l IN EXECUTE 'EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) ' || q LOOP
		IF l ~ 'Late Rows Streamed' THEN
			RETURN NEXT btrim(l);
		END IF;
	END LOOP;
END $$;
SELECT sk_streamed('SELECT * FROM sk_p20260102 WHERE host = ''h5'' ORDER BY ts DESC LIMIT 10');
SELECT sk_streamed('SELECT * FROM sk_p20260102 WHERE host = ''h5'' ORDER BY ts LIMIT 10');
SELECT sk_streamed('SELECT * FROM sk_p20260102 WHERE host = ''h5'' ORDER BY ts');

-- an IN list on the first sort column seeks each value and reads the union of their row groups
-- (TSBS's eight-host queries read every row group between the values before, 2026-09-25); the
-- executor still filters, so rows between two values in one row group are read and dropped
SELECT q, sk_check(q) FROM (VALUES
	('SELECT * FROM $t WHERE host IN (''h3'', ''h9'', ''zz'') ORDER BY host, ts'),
	('SELECT host, ts, v FROM $t WHERE host IN (''h3'', ''h11'') AND ts >= ''2026-01-02 03:00'' AND ts < ''2026-01-02 05:00'' ORDER BY host, ts'),
	('SELECT host, ts FROM $t WHERE host = ANY(''{h0,h5,nope}'') ORDER BY host DESC, ts DESC LIMIT 20'),
	('SELECT host, ts, v FROM $t WHERE host IN (''a'', ''zz'', ''h5'') ORDER BY host, ts'),
	('SELECT count(*), sum(v) FROM $t WHERE host IN (''h1'', ''h7'', ''h12'')'),
	('SELECT count(*) FROM $t WHERE host IN (NULL, ''h3'')'),
	('SELECT count(*) FROM $t WHERE host = ANY(''{}''::text[])'),
	('SELECT count(*) FROM $t WHERE host = ANY(NULL::text[])'),
	('SELECT h.x, c.ts FROM (VALUES (1), (2)) h(x) CROSS JOIN LATERAL (SELECT ts FROM $t WHERE host = ANY(CASE WHEN h.x = 1 THEN ''{h2,h4}''::text[] ELSE ''{h6}''::text[] END) ORDER BY ts DESC LIMIT 3) c ORDER BY 1, 2')
) AS v(q);
CREATE FUNCTION sk_groups(q text) RETURNS SETOF text LANGUAGE plpgsql AS $$
DECLARE
	l text;
BEGIN
	FOR l IN EXECUTE 'EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) ' || q LOOP
		IF l ~ 'Row Groups Read|Sort Key Seek' THEN
			RETURN NEXT btrim(l);
		END IF;
	END LOOP;
END $$;
SELECT sk_groups('SELECT * FROM sk_p20260102 WHERE host IN (''h3'', ''h9'')');
SELECT sk_groups('SELECT * FROM sk_p20260102 WHERE host BETWEEN ''h3'' AND ''h9''');
PREPARE some_hosts(text[]) AS SELECT host, count(*) FROM sk WHERE host = ANY($1) GROUP BY host ORDER BY host;
SET plan_cache_mode = force_generic_plan;
EXECUTE some_hosts('{h4,h10}');
EXECUTE some_hosts('{}');
RESET plan_cache_mode;
SELECT host, count(*) FROM sk_heap WHERE host IN ('h4', 'h10') GROUP BY host ORDER BY host;

-- the aggregate node answers an equality or IN list on the sort key by the seek, aggregating only
-- the rows it finds (TSBS's one- and eight-host queries went through the executor row by row,
-- 2026-09-25); late rows are held to the same seek, and deleted rows are left out
CREATE FUNCTION sk_agg(q text) RETURNS SETOF text LANGUAGE plpgsql AS $$
DECLARE
	l text;
BEGIN
	FOR l IN EXECUTE 'EXPLAIN (COSTS OFF) ' || q LOOP
		IF l ~ 'Columnar Aggregate|Columnar Scan|Sort Key Seek' THEN
			RETURN NEXT regexp_replace(btrim(l), '\s+on (\w+) \w+$', ' on \1');
		END IF;
	END LOOP;
END $$;
SELECT sk_agg('SELECT date_bin(''10 minutes'', ts, ''2000-01-01'') b, max(v) FROM sk_p20260102 WHERE host IN (''h3'', ''h9'') AND ts >= ''2026-01-02 03:00'' AND ts < ''2026-01-02 05:00'' GROUP BY 1');
SELECT sk_agg('SELECT count(*), sum(v) FROM sk_p20260102 WHERE host = ''h5''');
SELECT q, sk_check(q) FROM (VALUES
	('SELECT date_bin(''10 minutes'', ts, ''2000-01-01'') b, max(v), count(*) FROM $t WHERE host IN (''h3'', ''h9'', ''zz'') AND ts >= ''2026-01-02 03:00'' AND ts < ''2026-01-02 05:00'' GROUP BY 1 ORDER BY 1'),
	('SELECT count(*), sum(v), avg(v), min(ts), max(ts) FROM $t WHERE host = ''h5'''),
	('SELECT host, count(*), sum(v), min(v), max(v) FROM $t WHERE host IN (''a'', ''zz'', ''h0'', ''h5'') GROUP BY host ORDER BY host'),
	('SELECT host, date_bin(''1 hour'', ts, ''2000-01-01'') b, count(*), max(v) FROM $t WHERE host = ''h9'' AND ts > ''2026-01-02 12:00'' AND ts <= ''2026-01-03 01:00'' GROUP BY 1, 2 ORDER BY 1, 2'),
	('SELECT count(*), max(v) FROM $t WHERE host = ANY(''{h1,h1,h7}'') AND dev = 3'),
	('SELECT count(*), max(v) FROM $t WHERE host = ANY(NULL::text[])'),
	('SELECT count(*), max(v) FROM $t WHERE host = ANY(''{}''::text[])'),
	('SELECT count(*), max(v) FROM $t WHERE host IN (NULL, ''h3'')'),
	('SELECT count(*), max(v) FROM $t WHERE host = ''nope'''),
	('SELECT h.host, (SELECT count(*) FROM $t WHERE $t.host = h.host), (SELECT max(v) FROM $t WHERE $t.host = ANY(ARRAY[h.host, ''h2''])) FROM (VALUES (''h5''), (''zz''), (''nope''), (''h9'')) h(host) ORDER BY 1')
) AS v(q);
SELECT sk_groups('SELECT count(*) FROM sk_p20260102 WHERE host IN (''h3'', ''h9'')');

-- the same through an index that runs WITH the order (host, ts), on a table of its own
CREATE TABLE st (ts timestamptz NOT NULL, host text, v float8);
SELECT snouttime.create_series('st', 'ts', partition_interval => '1 day', premake => 1);
DO $$
DECLARE p regclass;
BEGIN
	FOR p IN SELECT partition FROM snouttime.partition_info
		WHERE series = 'st'::regclass AND range_start::timestamptz >= '2026-06-01' LOOP
		EXECUTE format('DROP TABLE %s', p);
	END LOOP;
END
$$;
SELECT snouttime.make_partitions('st', '2026-01-01', '2026-01-02');
SELECT snouttime.set_sealing('st', NULL::interval, order_by => '{host,ts}');
CREATE INDEX st_host_ts ON st (host, ts);
INSERT INTO st SELECT timestamptz '2026-01-01+00' + g * interval '23 seconds', 'h' || (g % 5), g FROM generate_series(0, 3000) g;
CREATE TABLE st_heap AS SELECT * FROM st;
SELECT snouttime.seal('st_p20260101');
INSERT INTO st SELECT timestamptz '2026-01-01+00' + g * interval '71 seconds' + interval '0.5 seconds', 'h2', -g FROM generate_series(0, 400) g;
INSERT INTO st_heap SELECT timestamptz '2026-01-01+00' + g * interval '71 seconds' + interval '0.5 seconds', 'h2', -g FROM generate_series(0, 400) g;
DELETE FROM st WHERE v BETWEEN 1000 AND 1100 OR v BETWEEN -200 AND -150;
DELETE FROM st_heap WHERE v BETWEEN 1000 AND 1100 OR v BETWEEN -200 AND -150;
ANALYZE st;
ANALYZE st_heap;
CREATE FUNCTION st_check(q text) RETURNS text LANGUAGE plpgsql AS $$
DECLARE
	a text; b text; c text;
BEGIN
	EXECUTE format('SELECT md5(string_agg(x::text, '';'')) FROM (%s) x', replace(q, '$t', 'st')) INTO a;
	SET LOCAL snouttime.columnar_custom_scan = off;
	EXECUTE format('SELECT md5(string_agg(x::text, '';'')) FROM (%s) x', replace(q, '$t', 'st')) INTO b;
	SET LOCAL snouttime.columnar_custom_scan = on;
	EXECUTE format('SELECT md5(string_agg(x::text, '';'')) FROM (%s) x', replace(q, '$t', 'st_heap')) INTO c;
	RETURN CASE WHEN a IS NOT DISTINCT FROM b AND b IS NOT DISTINCT FROM c THEN 'same' ELSE 'DIFFERENT' END;
END $$;
SELECT q, st_check(q) FROM (VALUES
	('SELECT * FROM $t WHERE host = ''h2'' ORDER BY ts'),
	('SELECT * FROM $t WHERE host = ''h2'' ORDER BY ts DESC LIMIT 10'),
	('SELECT * FROM $t WHERE host = ''h2'' ORDER BY ts LIMIT 25'),
	('SELECT * FROM $t WHERE host = ''h3'' ORDER BY ts DESC LIMIT 5'),
	('SELECT h.host, c.ts, c.v FROM (VALUES (''h2''), (''h0''), (''h2''), (''h4'')) h(host)
		LEFT JOIN LATERAL (SELECT ts, v FROM $t WHERE $t.host = h.host ORDER BY ts LIMIT 3) c ON true ORDER BY 1, 2')
) AS v(q);

-- what each column costs sealed (0.1.2): per encoding, from the row-group headers; nothing for
-- a partition that is not sealed
SELECT attname, type_name, encoding, row_groups, rows, nulls, stored_bytes > 0 AS stored
FROM snouttime.column_sizes('st_p20260101');
SELECT count(*) FROM snouttime.column_sizes('st_heap');

-- a generic plan: the seek's values come from parameters, on every execution
PREPARE last_before(text, timestamptz) AS SELECT ts, v FROM sk WHERE host = $1 AND ts <= $2 ORDER BY ts DESC LIMIT 2;
SET plan_cache_mode = force_generic_plan;
EXECUTE last_before('h4', '2026-01-02 03:00');
EXECUTE last_before('h12', '2026-01-03 23:00');
EXECUTE last_before(NULL, '2026-01-03 23:00');
RESET plan_cache_mode;
SELECT ts, v FROM sk_heap WHERE host = 'h4' AND ts <= '2026-01-02 03:00' ORDER BY ts DESC LIMIT 2;
SELECT ts, v FROM sk_heap WHERE host = 'h12' AND ts <= '2026-01-03 23:00' ORDER BY ts DESC LIMIT 2;

-- VACUUM FULL folds the late rows in and keeps the order and the index choice
VACUUM FULL sk_p20260101;
SELECT sk_nodes('SELECT * FROM sk_p20260101 WHERE host = ''h3'' ORDER BY ts DESC LIMIT 3');
SELECT sk_nodes('SELECT * FROM sk_p20260101 WHERE dev = 3');
SELECT sk_check('SELECT * FROM $t WHERE host = ''h0'' AND ts < ''2026-01-01 06:00'' ORDER BY ts DESC');

-- keep_indexes: every index covers every row, and the planner may use them
SELECT snouttime.set_sealing('sk', NULL::interval, order_by => '{host,ts}', keep_indexes => true);
SELECT snouttime.seal('sk_p20260104');
SELECT sk_nodes('SELECT * FROM sk_p20260104 WHERE dev = 3');
SELECT sk_check('SELECT * FROM $t WHERE dev = 3 AND ts >= ''2026-01-04'' ORDER BY ts');

-- a unique index covers every row of a sealed table, enforces, and stays whole through VACUUM
-- FULL (which rebuilds indexes with the unique flag cleared for the build)
CREATE TABLE uq (ts timestamptz PRIMARY KEY, host text, n int);
CREATE INDEX uq_n ON uq (n);
INSERT INTO uq SELECT timestamptz '2026-01-01+00' + g * interval '1 minute', 'h' || (g % 5), g FROM generate_series(0, 999) g;
SET snouttime.columnar_order_by = 'host,ts';
SELECT snouttime.seal('uq');
RESET snouttime.columnar_order_by;
INSERT INTO uq VALUES ('2026-01-01 00:07+00', 'dup', -1);
VACUUM FULL uq;
INSERT INTO uq VALUES ('2026-01-01 00:07+00', 'dup', -1);
SELECT c.relname, pg_relation_size(c.oid) > 16384 AS every_row
FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid WHERE i.indrelid = 'uq'::regclass ORDER BY 1;
SET enable_seqscan = off;
SET snouttime.columnar_custom_scan = off;
SELECT count(*) FROM uq WHERE ts < '2026-01-01 06:00';
RESET enable_seqscan;
RESET snouttime.columnar_custom_scan;

-- paged chunks (format version 3): a row group of 5,000 rows is five pages of 1,024, and a seek
-- decodes only the pages its rows are in (TSBS's one- and eight-host queries, 2026-09-25). The
-- answers must not change: NULLs inside pages, a host's rows across two row groups, late rows,
-- deletes, a float filter, and a day with more runs per row group than the directory records
-- (1,000 hosts), where the seek decodes the host column instead of reading the runs
SET snouttime.columnar_group_rows = 5000;
CREATE TABLE pgd (ts timestamptz NOT NULL, host text, v float8, n int8);
SELECT snouttime.create_series('pgd', 'ts', partition_interval => '1 day', premake => 1);
DO $$
DECLARE p regclass;
BEGIN
	FOR p IN SELECT partition FROM snouttime.partition_info
		WHERE series = 'pgd'::regclass AND range_start::timestamptz >= '2026-06-01' LOOP
		EXECUTE format('DROP TABLE %s', p);
	END LOOP;
END
$$;
SELECT snouttime.make_partitions('pgd', '2026-01-01', '2026-01-03');
SELECT snouttime.set_sealing('pgd', NULL::interval, order_by => '{host,ts}');
CREATE INDEX pgd_host_ts ON pgd (host, ts DESC);
INSERT INTO pgd SELECT timestamptz '2026-01-01+00' + g * interval '3 seconds', 'h' || (g % 20),
	CASE WHEN g % 17 = 0 THEN NULL ELSE g / 4.0 END, CASE WHEN g % 23 = 0 THEN NULL ELSE g END
FROM generate_series(0, 28799) g;
INSERT INTO pgd SELECT timestamptz '2026-01-02+00' + g * interval '3 seconds', 'h' || (g % 1000),
	CASE WHEN g % 13 = 0 THEN NULL ELSE g / 8.0 END, g
FROM generate_series(0, 28799) g;
CREATE TABLE pgd_heap AS SELECT * FROM pgd;
SELECT snouttime.seal('pgd_p20260101'), snouttime.seal('pgd_p20260102');
RESET snouttime.columnar_group_rows;
INSERT INTO pgd VALUES ('2026-01-01 05:10:00.5+00', 'h7', 1.5, 7), ('2026-01-02 03:00:00.5+00', 'h150', NULL, 150), ('2026-01-01 23:59:59+00', 'h19', 2.5, NULL);
INSERT INTO pgd_heap VALUES ('2026-01-01 05:10:00.5+00', 'h7', 1.5, 7), ('2026-01-02 03:00:00.5+00', 'h150', NULL, 150), ('2026-01-01 23:59:59+00', 'h19', 2.5, NULL);
DELETE FROM pgd WHERE n BETWEEN 6000 AND 6100 OR n BETWEEN 40000 AND 40050;
DELETE FROM pgd_heap WHERE n BETWEEN 6000 AND 6100 OR n BETWEEN 40000 AND 40050;
ANALYZE pgd;
ANALYZE pgd_heap;
SELECT encoding, sum(row_groups) AS row_groups FROM snouttime.column_sizes('pgd_p20260101') GROUP BY 1 ORDER BY 1;
CREATE FUNCTION pgd_check(q text) RETURNS text LANGUAGE plpgsql AS $$
DECLARE
	a text; b text;
BEGIN
	EXECUTE format('SELECT md5(string_agg(x::text, '';'')) FROM (%s) x', replace(q, '$t', 'pgd')) INTO a;
	EXECUTE format('SELECT md5(string_agg(x::text, '';'')) FROM (%s) x', replace(q, '$t', 'pgd_heap')) INTO b;
	RETURN CASE WHEN a IS NOT DISTINCT FROM b THEN 'same' ELSE 'DIFFERENT' END;
END $$;
SELECT q, pgd_check(q) FROM (VALUES
	('SELECT * FROM $t WHERE host = ''h7'' AND ts >= ''2026-01-01 05:00'' AND ts < ''2026-01-01 06:00'' ORDER BY ts'),
	('SELECT count(*), count(v), sum(v), max(n), min(ts), max(ts) FROM $t WHERE host = ''h7'' AND ts >= ''2026-01-01 05:00'' AND ts < ''2026-01-01 06:30'''),
	('SELECT host, date_bin(''5 minutes'', ts, ''2000-01-01'') b, count(*), max(v), sum(n) FROM $t WHERE host IN (''h3'', ''h17'', ''h9'') AND ts >= ''2026-01-01 02:00'' AND ts < ''2026-01-01 04:00'' GROUP BY 1, 2 ORDER BY 1, 2'),
	('SELECT host, date_bin(''5 minutes'', ts, ''2000-01-01'') b, count(*), max(v), sum(n) FROM $t WHERE host IN (''h3'', ''h150'', ''h999'') AND ts >= ''2026-01-02 02:00'' AND ts < ''2026-01-02 04:00'' GROUP BY 1, 2 ORDER BY 1, 2'),
	('SELECT count(*), sum(v), max(n) FROM $t WHERE host IN (''h3'', ''h9'', ''h150'')'),
	('SELECT * FROM $t WHERE host = ''h150'' ORDER BY ts DESC LIMIT 5'),
	('SELECT * FROM $t WHERE host = ''h19'' ORDER BY ts DESC LIMIT 3'),
	('SELECT host, ts, v FROM $t WHERE host = ''h12'' AND ts > ''2026-01-01 11:58'' AND ts < ''2026-01-01 12:03'' ORDER BY ts'),
	('SELECT count(*), sum(n) FROM $t WHERE v > 5000 AND ts < ''2026-01-01 03:00'''),
	('SELECT host, ts FROM $t WHERE host > ''h18'' AND host < ''h2'' ORDER BY host, ts LIMIT 30'),
	('SELECT host, count(*), sum(v), max(n) FROM $t GROUP BY host ORDER BY host LIMIT 7'),
	('SELECT date_bin(''1 hour'', ts, ''2000-01-01'') b, max(v), min(n), count(*) FROM $t GROUP BY 1 ORDER BY 1'),
	('SELECT e.h, c.ts, c.v FROM (VALUES (''h7'', timestamptz ''2026-01-01 05:10:30+00''), (''h150'', ''2026-01-02 03:01+00''), (''h0'', ''2026-01-01 00:00+00'')) e(h, t)
		LEFT JOIN LATERAL (SELECT ts, v FROM $t WHERE $t.host = e.h AND $t.ts <= e.t ORDER BY $t.ts DESC LIMIT 1) c ON true ORDER BY 1')
) AS v(q);
