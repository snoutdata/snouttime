-- create_series, migrate, drop_series, and the refusals.
SET client_min_messages = notice;
SET timezone = 'America/New_York';   -- partitions must still be aligned in UTC

-- ---- an empty table ----
CREATE TABLE empty_metrics (ts timestamptz NOT NULL, host text, v float8);
SELECT snouttime.create_series('empty_metrics', 'ts', partition_interval => '1 day', premake => 3);
-- now()'s partition plus three ahead, plus the default
SELECT count(*) FROM pg_inherits WHERE inhparent = 'empty_metrics'::regclass;
INSERT INTO empty_metrics VALUES (now(), 'a', 1);
SELECT tableoid::regclass::text LIKE 'empty_metrics_p%' AS routed_to_a_partition FROM empty_metrics;
-- Only this file's tables: pg_regress runs every file in the same database.
SELECT relid, time_column, time_type, partition_interval, premake FROM snouttime.series
WHERE relid IN ('empty_metrics'::regclass);

-- ---- a table with data, indexes, a trigger, a foreign key, grants and a comment ----
CREATE TABLE hosts (name text PRIMARY KEY);
INSERT INTO hosts VALUES ('a'), ('b');
CREATE TABLE metrics (
	ts timestamptz NOT NULL,
	host text NOT NULL REFERENCES hosts (name),
	v float8 CHECK (v >= 0),
	PRIMARY KEY (host, ts)
);
CREATE INDEX metrics_by_time ON metrics (ts DESC);
COMMENT ON TABLE metrics IS 'readings';
CREATE ROLE snouttime_regress_reader;
GRANT SELECT ON metrics TO snouttime_regress_reader;
CREATE FUNCTION announce() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
	IF TG_OP = 'DELETE' THEN RAISE NOTICE 'deleted %', OLD.ts; RETURN OLD; END IF;
	RAISE NOTICE 'inserted %', NEW.ts; RETURN NEW;
END $$;
CREATE TRIGGER announce BEFORE INSERT OR DELETE ON metrics FOR EACH ROW EXECUTE FUNCTION announce();
SET client_min_messages = warning;
INSERT INTO metrics
SELECT timestamptz '2020-03-01 00:00:00+00' + g * interval '6 hours', CASE WHEN g % 2 = 0 THEN 'a' ELSE 'b' END, g
FROM generate_series(0, 11) AS g;
SET client_min_messages = notice;

SELECT snouttime.create_series('metrics', 'ts', partition_interval => '1 day', premake => 1);
-- the old table is now the default partition and still holds the 2020 rows
SELECT count(*) FROM metrics_default;
-- moving rows fires no user trigger: no 'deleted' or 'inserted' notices here
CALL snouttime.migrate('metrics');
SELECT count(*) FROM metrics_default;
-- three days of 2020 data, three partitions named and bounded in UTC
SELECT c.relname, pg_get_expr(c.relpartbound, c.oid)
FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
WHERE i.inhparent = 'metrics'::regclass AND c.relname LIKE 'metrics_p2020%'
ORDER BY 1;
SELECT count(*), min(ts), max(ts) FROM metrics;
-- what carried over
SELECT indexrelid::regclass FROM pg_index WHERE indrelid = 'metrics'::regclass ORDER BY 1;
-- (NOT NULL is left out: Postgres 18 lists it in pg_constraint and 17 does not)
SELECT conname, pg_get_constraintdef(oid) FROM pg_constraint
WHERE conrelid = 'metrics'::regclass AND contype <> 'n' ORDER BY 1;
SELECT tgname FROM pg_trigger WHERE tgrelid = 'metrics'::regclass AND NOT tgisinternal;
SELECT obj_description('metrics'::regclass, 'pg_class');
SELECT has_table_privilege('snouttime_regress_reader', 'metrics', 'SELECT');
-- the trigger fires for a row in a new partition, and the foreign key is enforced
INSERT INTO metrics VALUES ('2020-03-02 12:00:00+00', 'a', 1) ON CONFLICT DO NOTHING;
INSERT INTO metrics VALUES ('2020-03-02 13:00:00+00', 'nobody', 1);

-- ---- partitions for data that is not from today ----
CREATE TABLE backfill (ts timestamptz NOT NULL);
SELECT snouttime.create_series('backfill', 'ts', partition_interval => '1 day', premake => 1);
-- [lo, hi): the range holding hi is not made
SELECT snouttime.make_partitions('backfill', '2022-02-01T00:00:00+00', '2022-02-04T00:00:00+00');
SELECT name FROM snouttime.partition_info
WHERE series = 'backfill'::regclass AND name LIKE '%2022%' ORDER BY name;
-- a bulk load then lands in partitions instead of the default one
INSERT INTO backfill SELECT timestamptz '2022-02-01 00:00:00+00' + g * interval '8 hours'
FROM generate_series(0, 8) AS g;
SELECT count(*) AS in_default FROM backfill_default;
-- and it is idempotent
SELECT snouttime.make_partitions('backfill', '2022-02-01T00:00:00+00', '2022-02-04T00:00:00+00');

-- ---- integer time ----
CREATE TABLE ticks (n bigint NOT NULL, v float8);
INSERT INTO ticks SELECT g, g FROM generate_series(1, 2500) AS g;
SELECT snouttime.create_series('ticks', 'n', partition_width => 1000, premake => 1);
CALL snouttime.migrate('ticks');
SELECT c.relname, pg_get_expr(c.relpartbound, c.oid)
FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
WHERE i.inhparent = 'ticks'::regclass ORDER BY 1;

-- ---- a month interval ----
CREATE TABLE monthly (d date NOT NULL);
INSERT INTO monthly VALUES ('2021-01-31'), ('2021-02-01'), ('2021-03-15');
SELECT snouttime.create_series('monthly', 'd', partition_interval => '1 month', premake => 1);
CALL snouttime.migrate('monthly');
SELECT c.relname, pg_get_expr(c.relpartbound, c.oid)
FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
WHERE i.inhparent = 'monthly'::regclass AND c.relname LIKE 'monthly_p2021%' ORDER BY 1;

-- ---- refusals, each a sentence, each leaving the table untouched ----
CREATE TABLE r1 (ts timestamptz);
SELECT snouttime.create_series('r1', 'nope', partition_interval => '1 day');
SELECT snouttime.create_series('r1', 'ts');
SELECT snouttime.create_series('r1', 'ts', partition_interval => '1 month 2 days');
SELECT snouttime.create_series('r1', 'ts', partition_width => 10);
CREATE TABLE r2 (label text);
SELECT snouttime.create_series('r2', 'label', partition_interval => '1 day');
CREATE TABLE r3 (id int UNIQUE, ts timestamptz);
SELECT snouttime.create_series('r3', 'ts', partition_interval => '1 day');
CREATE TABLE r4 (ts timestamptz);
INSERT INTO r4 VALUES (NULL);
SELECT snouttime.create_series('r4', 'ts', partition_interval => '1 day');
CREATE TABLE r5 (ts timestamptz);
CREATE VIEW r5_view AS SELECT * FROM r5;
SELECT snouttime.create_series('r5', 'ts', partition_interval => '1 day');
CREATE TABLE r6 (id bigint GENERATED ALWAYS AS IDENTITY, ts timestamptz);
SELECT snouttime.create_series('r6', 'ts', partition_interval => '1 day');
CREATE UNLOGGED TABLE r7 (ts timestamptz);
SELECT snouttime.create_series('r7', 'ts', partition_interval => '1 day');
SELECT snouttime.create_series('metrics', 'ts', partition_interval => '1 day');
-- nothing was registered or renamed by any of them
SELECT relid FROM snouttime.series
WHERE relid::text ~ '^(empty_metrics|metrics|ticks|monthly|theirs|r[0-9])$' ORDER BY relid::text;
SELECT count(*) FROM pg_class WHERE relname LIKE 'r__default';

-- ---- only the owner ----
CREATE ROLE snouttime_regress_other;
CREATE TABLE theirs (ts timestamptz NOT NULL);
SET ROLE snouttime_regress_other;
SELECT snouttime.create_series('theirs', 'ts', partition_interval => '1 day');
INSERT INTO snouttime.series (relid, time_column, time_type, partition_interval)
	VALUES ('theirs', 'ts', 'timestamptz', '1 day');
DELETE FROM snouttime.series WHERE relid = 'metrics'::regclass;
RESET ROLE;

-- ---- drop_series and DROP TABLE ----
SELECT snouttime.drop_series('ticks');
SELECT relkind FROM pg_class WHERE oid = 'ticks'::regclass;
DROP TABLE monthly;
SELECT relid FROM snouttime.series
WHERE relid::text ~ '^(empty_metrics|metrics|ticks|monthly|theirs)$' ORDER BY relid::text;

DROP OWNED BY snouttime_regress_reader;
DROP ROLE snouttime_regress_reader;
DROP OWNED BY snouttime_regress_other;
DROP ROLE snouttime_regress_other;
