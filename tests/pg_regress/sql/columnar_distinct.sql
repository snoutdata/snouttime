-- The last point per key (`SnoutTime Columnar Distinct`): DISTINCT ON over a sealed
-- relation chooses each key's row from the key and time columns alone, then fetches only those
-- rows. Every query must return what Postgres's own plan returns, and what a heap twin returns.
-- The ordering column is unique within a key, since DISTINCT ON's choice between rows that tie
-- is not defined.
SET client_min_messages = warning;
SET timezone = 'UTC';
SET snouttime.columnar_group_rows = 400;

CREATE TABLE ld (ts timestamptz NOT NULL, host text, dev int4, seq int8, v float8, note text);
SELECT snouttime.create_series('ld', 'ts', partition_interval => '1 day', premake => 1);
-- premake made partitions around TODAY, which the plans below would name: dropped, so the
-- expected output does not change with the date the suite runs on (it failed on 2026-09-24).
DO $$
DECLARE p regclass;
BEGIN
	FOR p IN SELECT partition FROM snouttime.partition_info
		WHERE series = 'ld'::regclass AND range_start::timestamptz >= '2026-06-01' LOOP
		EXECUTE format('DROP TABLE %s', p);
	END LOOP;
END
$$;
SELECT snouttime.make_partitions('ld', '2026-01-01', '2026-01-05');
INSERT INTO ld
SELECT timestamptz '2026-01-01+00' + g * interval '29 seconds',
	CASE WHEN g % 97 = 0 THEN NULL ELSE 'h' || (g % 9) END,
	g % 4,
	-- one NULL per dev, so NULLS FIRST has a single row to choose
	CASE WHEN g IN (31, 62, 93, 124) THEN NULL ELSE g END,
	g / 4.0,
	md5(g::text)
FROM generate_series(0, 4 * 2979) AS g;
CREATE TABLE ld_heap AS SELECT * FROM ld;
-- three of four partitions sealed; the fourth stays heap
SELECT snouttime.seal('ld_p20260101'), snouttime.seal('ld_p20260102'), snouttime.seal('ld_p20260103');
-- late rows into a sealed partition, and deletes
INSERT INTO ld VALUES ('2026-01-02 23:59:59+00', 'h1', 1, 999999, 1.5, 'late'), ('2026-01-01 12:00:01+00', 'late', 2, 999998, 2.5, 'late');
INSERT INTO ld_heap VALUES ('2026-01-02 23:59:59+00', 'h1', 1, 999999, 1.5, 'late'), ('2026-01-01 12:00:01+00', 'late', 2, 999998, 2.5, 'late');
DELETE FROM ld WHERE seq BETWEEN 5900 AND 5990;
DELETE FROM ld_heap WHERE seq BETWEEN 5900 AND 5990;
ANALYZE ld;
ANALYZE ld_heap;

CREATE FUNCTION ld_nodes(q text) RETURNS SETOF text LANGUAGE plpgsql AS $$
DECLARE
	l text;
BEGIN
	FOR l IN EXECUTE 'EXPLAIN (COSTS OFF) ' || q LOOP
		IF l ~ 'Unique|Sort|Append|Custom Scan|Scan on' THEN
			RETURN NEXT regexp_replace(btrim(l), '\s+on (\w+) \w+$', ' on \1');
		END IF;
	END LOOP;
END $$;

-- the text of the result, in its own order: DISTINCT ON's answer includes its ORDER BY
CREATE FUNCTION ld_check(q text) RETURNS text LANGUAGE plpgsql AS $$
DECLARE
	a text; b text; c text;
BEGIN
	EXECUTE format('SELECT md5(string_agg(x::text, '';'')) FROM (%s) x', replace(q, '$t', 'ld')) INTO a;
	SET LOCAL snouttime.columnar_custom_scan = off;
	EXECUTE format('SELECT md5(string_agg(x::text, '';'')) FROM (%s) x', replace(q, '$t', 'ld')) INTO b;
	SET LOCAL snouttime.columnar_custom_scan = on;
	EXECUTE format('SELECT md5(string_agg(x::text, '';'')) FROM (%s) x', replace(q, '$t', 'ld_heap')) INTO c;
	RETURN CASE WHEN a IS NOT DISTINCT FROM b AND b IS NOT DISTINCT FROM c THEN 'same' ELSE 'DIFFERENT' END;
END $$;

SELECT ld_nodes('SELECT DISTINCT ON (host) * FROM ld ORDER BY host, ts DESC');
SELECT q, ld_check(q) FROM (VALUES
	('SELECT DISTINCT ON (host) * FROM $t ORDER BY host, ts DESC'),
	('SELECT DISTINCT ON (host) * FROM $t ORDER BY host, ts'),
	('SELECT DISTINCT ON (host) host, ts, v FROM $t ORDER BY host DESC, ts DESC'),
	('SELECT DISTINCT ON (dev) dev, seq, note FROM $t ORDER BY dev, seq DESC'),
	('SELECT DISTINCT ON (dev) dev, seq, note FROM $t ORDER BY dev, seq DESC NULLS LAST'),
	('SELECT DISTINCT ON (dev) dev, seq FROM $t ORDER BY dev, seq ASC NULLS FIRST'),
	('SELECT DISTINCT ON (host, dev) host, dev, ts, v FROM $t ORDER BY host, dev, ts DESC'),
	('SELECT DISTINCT ON (host) host, ts, v FROM $t WHERE ts < ''2026-01-02 06:00'' ORDER BY host, ts DESC'),
	('SELECT DISTINCT ON (host) host, ts FROM $t WHERE ts >= timestamptz ''2026-01-04+00'' - interval ''2 days'' ORDER BY host, ts DESC'),
	('SELECT DISTINCT ON (host) host, v * 2, upper(note) FROM $t ORDER BY host, ts DESC')
) AS v(q);

-- one sealed table on its own
CREATE TABLE lp AS SELECT * FROM ld_heap;
CREATE TABLE lp_heap AS SELECT * FROM ld_heap;
SELECT snouttime.seal('lp');
SELECT ld_nodes('SELECT DISTINCT ON (host) * FROM lp ORDER BY host, ts DESC');
SELECT (SELECT md5(string_agg(x::text, ';')) FROM (SELECT DISTINCT ON (host) * FROM lp ORDER BY host, ts DESC) x)
	= (SELECT md5(string_agg(x::text, ';')) FROM (SELECT DISTINCT ON (host) * FROM lp_heap ORDER BY host, ts DESC) x) AS alone_same;

-- sealed in (key, time) order: each key's last row is read from the directory (format.rs
-- run_ends), nothing decoded. Row groups of 64 rows, so runs cross group boundaries and a group
-- holds several; one table has a key with more distinct values per group than the directory
-- records (the rows are read instead); NULL keys and NULL times; late rows; then deletes and a
-- filter, which also send it back to reading rows.
CREATE TABLE lk (ts timestamptz, host text, dev int4, v float8);
INSERT INTO lk
-- a NULL time for three hosts, once each: DISTINCT ON's choice between ties is not defined
SELECT CASE WHEN g IN (100, 201, 302) THEN NULL ELSE timestamptz '2026-01-01+00' + g * interval '7 seconds' END,
	CASE WHEN g % 97 = 0 THEN NULL ELSE 'h' || (g % 23) END,
	g % 5,
	g / 2.0
FROM generate_series(0, 20000) AS g;
CREATE TABLE lk_heap AS SELECT * FROM lk;
CREATE TABLE lk_many AS SELECT * FROM lk;
CREATE TABLE lk_many_heap AS SELECT * FROM lk;
SET snouttime.columnar_group_rows = 64;
SET snouttime.columnar_order_by = 'host,ts';
SELECT snouttime.seal('lk');
SET snouttime.columnar_order_by = 'dev,host,ts';
SELECT snouttime.seal('lk_many');
RESET snouttime.columnar_order_by;
RESET snouttime.columnar_group_rows;
ANALYZE lk; ANALYZE lk_many;
CREATE FUNCTION lk_same(q text) RETURNS text LANGUAGE plpgsql AS $$
DECLARE
	a text; b text;
BEGIN
	EXECUTE format('SELECT md5(string_agg(x::text, '';'')) FROM (%s) x', replace(q, '$t', 'lk')) INTO a;
	EXECUTE format('SELECT md5(string_agg(x::text, '';'')) FROM (%s) x', replace(q, '$t', 'lk_heap')) INTO b;
	RETURN CASE WHEN a IS NOT DISTINCT FROM b THEN 'same' ELSE 'DIFFERENT' END;
END $$;
SELECT q, lk_same(q) FROM (VALUES
	('SELECT DISTINCT ON (host) * FROM $t ORDER BY host, ts DESC'),
	('SELECT DISTINCT ON (host) host, ts, v FROM $t ORDER BY host DESC, ts DESC'),
	('SELECT DISTINCT ON (host) host, v FROM $t ORDER BY host, ts DESC NULLS LAST')
) AS v(q);
-- the directory path: no row group read; then the same after late rows (which compete with it)
CREATE FUNCTION groups_read(q text) RETURNS SETOF text LANGUAGE plpgsql AS $$
DECLARE
	l text;
BEGIN
	FOR l IN EXECUTE 'EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) ' || q LOOP
		IF l ~ 'Row Groups Read' THEN
			RETURN NEXT btrim(l);
		END IF;
	END LOOP;
END $$;
SELECT ld_nodes('SELECT DISTINCT ON (host) * FROM lk ORDER BY host, ts DESC');
SELECT groups_read('SELECT DISTINCT ON (host) * FROM lk ORDER BY host, ts DESC');
SELECT groups_read('SELECT DISTINCT ON (dev, host) * FROM lk_many ORDER BY dev, host, ts DESC');
INSERT INTO lk VALUES ('2027-01-01+00', 'h3', 1, -1), (NULL, 'h4', 2, -2), ('2027-01-01+00', NULL, 3, -3), ('2027-01-01+00', 'new', 4, -4);
INSERT INTO lk_heap VALUES ('2027-01-01+00', 'h3', 1, -1), (NULL, 'h4', 2, -2), ('2027-01-01+00', NULL, 3, -3), ('2027-01-01+00', 'new', 4, -4);
SELECT lk_same('SELECT DISTINCT ON (host) * FROM $t ORDER BY host, ts DESC') AS late_same;
-- three sort columns: DISTINCT ON the first two
SELECT (SELECT md5(string_agg(x::text, ';')) FROM (SELECT DISTINCT ON (dev, host) * FROM lk_many ORDER BY dev, host, ts DESC) x)
	= (SELECT md5(string_agg(x::text, ';')) FROM (SELECT DISTINCT ON (dev, host) * FROM lk_many_heap ORDER BY dev, host, ts DESC) x) AS two_keys_same;
-- deletes and a filter: read, not from the directory
DELETE FROM lk WHERE v IN (10000, 5000.5, -1);
DELETE FROM lk_heap WHERE v IN (10000, 5000.5, -1);
SELECT lk_same('SELECT DISTINCT ON (host) * FROM $t ORDER BY host, ts DESC') AS deleted_same;
SELECT groups_read('SELECT DISTINCT ON (host) * FROM lk ORDER BY host, ts DESC');
SELECT lk_same('SELECT DISTINCT ON (host) * FROM $t WHERE ts < ''2026-01-01 12:00'' ORDER BY host, ts DESC') AS filtered_same;
-- a key with more runs in a row group than the directory records (64): the rows are read
CREATE TABLE lw AS SELECT (v * 2)::int4 % 3000 AS n, ts, host FROM lk_heap WHERE v >= 0;
CREATE TABLE lw_heap AS SELECT * FROM lw;
SET snouttime.columnar_group_rows = 500;
SET snouttime.columnar_order_by = 'n,ts';
SELECT snouttime.seal('lw');
RESET snouttime.columnar_order_by;
RESET snouttime.columnar_group_rows;
SELECT ld_nodes('SELECT DISTINCT ON (n) n, ts, host FROM lw ORDER BY n, ts DESC');
SELECT groups_read('SELECT DISTINCT ON (n) n, ts, host FROM lw ORDER BY n, ts DESC');
SELECT (SELECT md5(string_agg(x::text, ';')) FROM (SELECT DISTINCT ON (n) n, ts, host FROM lw ORDER BY n, ts DESC) x)
	= (SELECT md5(string_agg(x::text, ';')) FROM (SELECT DISTINCT ON (n) n, ts, host FROM lw_heap ORDER BY n, ts DESC) x) AS many_runs_same;
