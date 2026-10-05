-- The snouttime_columnar access method. The property everything here checks is
-- that a sealed table returns exactly what its heap twin returns, through every path: seq scan,
-- index scan, parallel scan, after inserts, deletes, updates, rollbacks and rewrites.
SET client_min_messages = warning;
SET snouttime.columnar_group_rows = 100;

CREATE TABLE c_heap (
	id int8 NOT NULL, at timestamptz, d date, t time, ts timestamp, s int2, i int4,
	f4 float4, f8 float8, b bool, host text, v varchar(10), n numeric, u uuid, j jsonb,
	by bytea, iv interval, ch "char", nm name
);
INSERT INTO c_heap
SELECT g, timestamptz '2026-01-01+00' + g * interval '10 seconds', date '2026-01-01' + g % 30,
	time '00:00' + g * interval '1 second', timestamp '2026-01-01' + g * interval '1 minute',
	(g % 30000 - 15000)::int2, CASE WHEN g % 11 = 0 THEN NULL ELSE g * 7 - 5000 END,
	g / 3.0, CASE g % 97 WHEN 0 THEN 'NaN'::float8 WHEN 1 THEN '-0'::float8 WHEN 2 THEN 'Infinity' ELSE g / 7.0 END,
	CASE WHEN g % 5 = 0 THEN NULL ELSE g % 2 = 0 END, 'host_' || (g % 7),
	CASE WHEN g % 13 = 0 THEN NULL ELSE left(md5(g::text), 1 + g % 10) END, g * 1.5 - 1000,
	md5(g::text)::uuid, jsonb_build_object('g', g, 'odd', g % 2 = 1), decode(md5(g::text), 'hex'),
	g * interval '1 minute', chr(65 + g % 26)::"char", ('n' || g)::name
FROM generate_series(1, 1000) AS g;
-- a long value, stored compressed and out of line in the heap
INSERT INTO c_heap (id, host) VALUES (1001, repeat('long value ', 5000));

CREATE TABLE c_col (LIKE c_heap);
INSERT INTO c_col SELECT * FROM c_heap;
CREATE INDEX ON c_col (at);
CREATE INDEX ON c_col (host, id);
-- every index whole, so the index scans below read column-store rows through them (by default
-- a non-unique index on a sealed table holds only its late rows: columnar_seek.sql)
SET snouttime.columnar_keep_indexes = on;
ALTER TABLE c_col SET ACCESS METHOD snouttime_columnar;
RESET snouttime.columnar_keep_indexes;

SELECT am.amname FROM pg_class c JOIN pg_am am ON am.oid = c.relam WHERE c.oid = 'c_col'::regclass;
SELECT count(*) AS side_tables FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'snouttime_internal' AND c.relname IN ('delta_' || 'c_col'::regclass::oid, 'deletes_' || 'c_col'::regclass::oid);

-- identical, as multisets, both ways
CREATE FUNCTION c_same(a regclass, b regclass) RETURNS TABLE (extra bigint, missing bigint, total bigint)
LANGUAGE plpgsql AS $$
BEGIN
	EXECUTE format('SELECT count(*) FROM (SELECT * FROM %s EXCEPT ALL SELECT * FROM %s) x', a, b) INTO extra;
	EXECUTE format('SELECT count(*) FROM (SELECT * FROM %s EXCEPT ALL SELECT * FROM %s) x', b, a) INTO missing;
	EXECUTE format('SELECT count(*) FROM %s', a) INTO total;
	RETURN NEXT;
END $$;
SELECT * FROM c_same('c_col', 'c_heap');
-- bit patterns of floats survive (NaN and -0 compare equal to anything above)
SELECT count(*) AS float_bits_differ FROM c_col c JOIN c_heap h USING (id)
WHERE float8send(c.f8) <> float8send(h.f8) OR float4send(c.f4) <> float4send(h.f4);
SELECT length(host) FROM c_col WHERE id = 1001;
-- smaller than the heap
SELECT pg_relation_size('c_col') < pg_relation_size('c_heap') / 2 AS smaller;

-- ---- index scans ----
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET snouttime.columnar_custom_scan = off;
SELECT id, host FROM c_col WHERE at = timestamptz '2026-01-01+00' + 500 * interval '10 seconds';
SELECT count(*) FROM c_col WHERE host = 'host_3' AND id < 300;
EXPLAIN (COSTS OFF) SELECT id FROM c_col WHERE host = 'host_3' AND id < 300;
RESET enable_seqscan;
RESET enable_bitmapscan;
RESET snouttime.columnar_custom_scan;

-- ---- parallel scans give every row once ----
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
SET min_parallel_table_scan_size = 0;
SET max_parallel_workers_per_gather = 2;
SELECT count(*), sum(id) FROM c_col;
RESET parallel_setup_cost;
RESET parallel_tuple_cost;
RESET min_parallel_table_scan_size;
RESET max_parallel_workers_per_gather;

-- ---- writes after the seal: the same statements on both tables ----
CREATE FUNCTION c_both(sql text) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
	EXECUTE replace(sql, '$t', 'c_heap');
	EXECUTE replace(sql, '$t', 'c_col');
END $$;
SELECT c_both($$INSERT INTO $t (id, at, host, i) SELECT g, now(), 'late', g FROM generate_series(2001, 2050) g$$);
SELECT c_both($$DELETE FROM $t WHERE id % 10 = 3$$);
SELECT c_both($$UPDATE $t SET host = 'moved', i = -i WHERE id % 10 = 4$$);
SELECT c_both($$UPDATE $t SET i = 0 WHERE id BETWEEN 2001 AND 2010$$);
SELECT c_both($$DELETE FROM $t WHERE id BETWEEN 2011 AND 2015$$);
SELECT * FROM c_same('c_col', 'c_heap');
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT count(*) AS moved_by_index FROM c_col WHERE host = 'moved' AND id > 0;
SELECT count(*) AS late_by_index FROM c_col WHERE host = 'late' AND id > 0;
RESET enable_seqscan;
RESET enable_bitmapscan;

-- a rolled-back delete and update leave no trace
BEGIN;
DELETE FROM c_col WHERE id < 100;
UPDATE c_col SET host = 'gone' WHERE id < 200;
ROLLBACK;
SELECT * FROM c_same('c_col', 'c_heap');

-- a row cannot be deleted twice, and a deleted row cannot be updated
BEGIN;
DELETE FROM c_col WHERE id = 5;
WITH x AS (DELETE FROM c_col WHERE id = 5 RETURNING 1) SELECT count(*) AS second_delete FROM x;
UPDATE c_col SET i = 1 WHERE id = 5;
ROLLBACK;

-- row locks on column-store rows
BEGIN;
SELECT id FROM c_col WHERE id IN (6, 7) ORDER BY id FOR UPDATE;
SELECT id FROM c_col WHERE id = 6;
COMMIT;
SELECT * FROM c_same('c_col', 'c_heap');

-- ---- rewrites fold the side tables in ----
VACUUM FULL c_col;
SELECT * FROM c_same('c_col', 'c_heap');
SELECT snouttime._changed_since_seal('c_col', 1000) AS side_rows;

-- a column added after the seal, with and without a default
SELECT c_both($$ALTER TABLE $t ADD COLUMN extra int DEFAULT 42$$);
SELECT c_both($$ALTER TABLE $t ADD COLUMN extra2 text$$);
SELECT c_both($$INSERT INTO $t (id, extra, extra2) VALUES (3001, 7, 'x')$$);
SELECT * FROM c_same('c_col', 'c_heap');
SELECT c_both($$ALTER TABLE $t DROP COLUMN v$$);
SELECT c_both($$INSERT INTO $t (id, extra2) VALUES (3002, 'after drop')$$);
SELECT * FROM c_same('c_col', 'c_heap');
-- a type change rewrites the column store
SELECT c_both($$ALTER TABLE $t ALTER COLUMN i TYPE int8$$);
SELECT * FROM c_same('c_col', 'c_heap');

-- sorted on the way in
SET snouttime.columnar_order_by = 'host, id';
CREATE TABLE c_sorted USING snouttime_columnar AS SELECT id, host FROM c_heap WHERE id <= 1000;
RESET snouttime.columnar_order_by;
SELECT count(*) AS out_of_order FROM (
	SELECT host, id, lag(host) OVER w AS ph, lag(id) OVER w AS pi FROM c_sorted WINDOW w AS (ORDER BY ctid)
) x WHERE (ph, pi) > (host, id);

-- every codec
SET snouttime.columnar_compression = 'zstd';
CREATE TABLE c_zstd USING snouttime_columnar AS SELECT * FROM c_heap;
SET snouttime.columnar_compression = 'none';
CREATE TABLE c_none USING snouttime_columnar AS SELECT * FROM c_heap;
RESET snouttime.columnar_compression;
SELECT * FROM c_same('c_zstd', 'c_heap');
SELECT * FROM c_same('c_none', 'c_heap');

-- a table created and filled in one transaction, read before commit
BEGIN;
CREATE TABLE c_tx (id int, s text) USING snouttime_columnar;
INSERT INTO c_tx SELECT g, g::text FROM generate_series(1, 250) g;
SELECT count(*), sum(id) FROM c_tx;
INSERT INTO c_tx VALUES (251, 'after the flush');
SELECT count(*), sum(id) FROM c_tx;
COMMIT;
SELECT count(*), sum(id) FROM c_tx;

-- ANALYZE sees the rows
ANALYZE c_col;
SELECT reltuples BETWEEN 500 AND 2000 AS estimated FROM pg_class WHERE oid = 'c_col'::regclass;

-- TRUNCATE empties the side tables too
TRUNCATE c_tx;
SELECT count(*) FROM c_tx;
INSERT INTO c_tx VALUES (1, 'one');
SELECT * FROM c_tx;

-- unsealing gives a heap table with the same rows, and the side tables go
ALTER TABLE c_col SET ACCESS METHOD heap;
SELECT * FROM c_same('c_col', 'c_heap');
SELECT count(*) AS side_tables FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'snouttime_internal' AND c.relname IN ('delta_' || 'c_col'::regclass::oid, 'deletes_' || 'c_col'::regclass::oid);

-- dropping a column-store table drops its side tables
SELECT 'c_zstd'::regclass::oid AS zoid \gset
DROP TABLE c_zstd;
SELECT count(*) AS side_tables_left FROM pg_class WHERE relname IN ('delta_' || :zoid, 'deletes_' || :zoid);

-- ---- refusals ----
\set ON_ERROR_STOP 0
CREATE UNLOGGED TABLE c_unlogged (a int) USING snouttime_columnar;
CREATE INDEX ON c_none USING brin (id);
CREATE INDEX CONCURRENTLY ON c_none (id);
SELECT * FROM c_none TABLESAMPLE SYSTEM (10);
\set ON_ERROR_STOP 1
