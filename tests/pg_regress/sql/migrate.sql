-- A migration moves several ranges per transaction and attaches them
-- after ONE validated constraint on the default partition, so Postgres does not scan the
-- default partition once per partition attached. Counted here with the default partition's
-- own sequential-scan statistic.
SET client_min_messages = warning;

-- 72 hourly ranges of history, all in the default partition after conversion
CREATE TABLE mg (at timestamptz NOT NULL, v int);
CREATE INDEX ON mg (at);
INSERT INTO mg SELECT timestamptz '2026-01-01+00' + g * interval '1 minute', g FROM generate_series(0, 72 * 60 - 1) AS g;
SELECT snouttime.create_series('mg', 'at', partition_interval => '1 hour', premake => 1);
SELECT snouttime._default_partition('mg')::text AS def \gset
SELECT pg_stat_force_next_flush();
SELECT seq_scan AS scans_before FROM pg_stat_user_tables WHERE relid = :'def'::regclass \gset

CALL snouttime.migrate('mg');

SELECT pg_stat_force_next_flush();
SELECT pg_stat_clear_snapshot();
-- about eight batches, each a few scans (21 when written), not one scan per partition
-- attached (72): at debug1 every ATTACH says the constraint is "implied by existing constraints"
SELECT seq_scan - :scans_before < 36 AS fewer_scans_than_partitions FROM pg_stat_user_tables WHERE relid = :'def'::regclass;
SELECT count(*) AS rows_kept, sum(v) = (72 * 60 - 1) * (72 * 60) / 2 AS same_rows FROM mg;
SELECT count(*) AS left_in_default FROM ONLY :def;
SELECT count(*) FILTER (WHERE c.relname ~ '_p2026010[1-3]_') AS partitions_for_history
FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid WHERE i.inhparent = 'mg'::regclass;
-- nothing of the move is left behind on the default partition
SELECT count(*) AS stray_constraints FROM pg_constraint WHERE conrelid = :'def'::regclass AND contype = 'c';
-- every row is in the partition its time says
SELECT count(*) AS misplaced FROM mg
WHERE tableoid::regclass::text <> 'mg_p' || to_char(at AT TIME ZONE 'UTC', 'YYYYMMDD_HH24') || '0000';

-- batches: an explicit count of ranges per transaction still ends in the same place
CREATE TABLE mg2 (at timestamptz NOT NULL, v int);
INSERT INTO mg2 SELECT timestamptz '2026-01-01+00' + g * interval '1 hour', g FROM generate_series(0, 9) AS g;
SELECT snouttime.create_series('mg2', 'at', partition_interval => '1 hour', premake => 1);
SELECT * FROM snouttime._migrate_batch('mg2', 3);
SELECT * FROM snouttime._migrate_batch('mg2', 100);
SELECT * FROM snouttime._migrate_batch('mg2');
SELECT count(*) AS rows_kept FROM mg2;

-- a range covered by somebody else's partition: those rows go back, the rest are placed
CREATE TABLE mg3 (at timestamptz NOT NULL, v int);
INSERT INTO mg3 SELECT timestamptz '2026-01-01+00' + g * interval '1 hour', g FROM generate_series(0, 5) AS g;
SELECT snouttime.create_series('mg3', 'at', partition_interval => '1 hour', premake => 1);
SELECT snouttime._default_partition('mg3')::text AS def3 \gset
-- theirs covers 02:00 to 03:30, so our hourly range for 03:45 overlaps it
ALTER TABLE mg3 DETACH PARTITION :def3;
CREATE TABLE mg3_theirs (LIKE mg3);
WITH moved AS (DELETE FROM :def3 WHERE at >= '2026-01-01 02:00+00' AND at < '2026-01-01 03:30+00' RETURNING *)
INSERT INTO mg3_theirs SELECT * FROM moved;
INSERT INTO :def3 VALUES ('2026-01-01 03:45+00', 101);
ALTER TABLE mg3 ATTACH PARTITION mg3_theirs FOR VALUES FROM ('2026-01-01 02:00+00') TO ('2026-01-01 03:30+00');
ALTER TABLE mg3 ATTACH PARTITION :def3 DEFAULT;
SELECT moved, stopped_at FROM snouttime._migrate_batch('mg3', 10);
SELECT at, v FROM ONLY :def3 ORDER BY at;
SELECT count(*) AS rows_kept FROM mg3;
SELECT count(*) AS stray_constraints FROM pg_constraint WHERE conrelid = :'def3'::regclass AND contype = 'c';
