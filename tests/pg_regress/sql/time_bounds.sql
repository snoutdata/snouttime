-- `ts >= <constant> - interval '1 hour'` is a stable expression, which
-- Postgres will not prune partitions by while planning, so every partition was planned. With no
-- days or months in the interval the sum is computed while planning; otherwise a comparison it
-- implies in every time zone is added. Either way the answers must not change, whatever the
-- time zone, now or when a cached plan is run again under another one.
SET client_min_messages = warning;
SET timezone = 'UTC';
CREATE TABLE tb (ts timestamptz NOT NULL, host text, v float8);
SELECT snouttime.create_series('tb', 'ts', partition_interval => '1 day', premake => 1);
DO $$
DECLARE p regclass;
BEGIN
	FOR p IN SELECT partition FROM snouttime.partition_info
		WHERE series = 'tb'::regclass AND range_start::timestamptz >= '2026-06-01' LOOP
		EXECUTE format('DROP TABLE %s', p);
	END LOOP;
END
$$;
SELECT snouttime.make_partitions('tb', '2026-01-01', '2026-01-07');
SELECT snouttime.make_partitions('tb', '2026-03-05', '2026-03-12');
INSERT INTO tb SELECT timestamptz '2026-01-01+00' + g * interval '7 minutes', 'h' || (g % 3), g FROM generate_series(0, 6 * 24 * 60 / 7 - 1) g;
INSERT INTO tb SELECT timestamptz '2026-03-05+00' + g * interval '7 minutes', 'h' || (g % 3), g FROM generate_series(0, 7 * 24 * 60 / 7 - 1) g;
CREATE TABLE tb_heap AS SELECT * FROM tb;
SELECT snouttime.seal(partition) FROM snouttime.partition_info
	WHERE series = 'tb'::regclass AND range_start::timestamptz < '2026-03-10' ORDER BY 1;
ANALYZE tb;
ANALYZE tb_heap;

-- the partitions a plan names, and how many the executor removed
CREATE FUNCTION tb_parts(q text) RETURNS text LANGUAGE plpgsql AS $$
DECLARE
	plan text := '';
	l text;
BEGIN
	FOR l IN EXECUTE 'EXPLAIN (COSTS OFF) ' || q LOOP
		plan := plan || l || E'\n';
	END LOOP;
	RETURN (SELECT coalesce(string_agg(DISTINCT m[1], ' ' ORDER BY m[1]), 'none') FROM regexp_matches(plan, '(tb_p\d+)', 'g') m)
		|| coalesce(' (' || substring(plan from 'Subplans Removed: \d+') || ')', '');
END $$;
-- a plan's first filter
CREATE FUNCTION tb_filter(q text) RETURNS text LANGUAGE plpgsql AS $$
DECLARE
	l text;
BEGIN
	FOR l IN EXECUTE 'EXPLAIN (COSTS OFF) ' || q LOOP
		IF l ~ 'Filter: ' THEN
			RETURN btrim(l);
		END IF;
	END LOOP;
	RETURN NULL;
END $$;
CREATE FUNCTION tb_check(q text) RETURNS text LANGUAGE plpgsql AS $$
DECLARE
	a text; b text;
BEGIN
	EXECUTE format('SELECT md5(string_agg(x::text, '';'')) FROM (%s) x', replace(q, '$t', 'tb')) INTO a;
	EXECUTE format('SELECT md5(string_agg(x::text, '';'')) FROM (%s) x', replace(q, '$t', 'tb_heap')) INTO b;
	RETURN CASE WHEN a IS NOT DISTINCT FROM b THEN 'same' ELSE 'DIFFERENT' END;
END $$;

-- an hour: computed while planning, so one partition is planned
SELECT tb_parts('SELECT count(*) FROM tb WHERE ts >= ''2026-01-03 12:00+00''::timestamptz - interval ''1 hour'' AND ts < ''2026-01-03 12:00+00''::timestamptz');
SELECT tb_filter('SELECT ts FROM tb WHERE ts >= ''2026-01-03 12:00+00''::timestamptz - interval ''1 hour'' AND ts < ''2026-01-03 12:00+00''::timestamptz');
-- a day: the comparison kept, and one it implies in every zone added (a day and 26 hours)
SELECT tb_parts('SELECT count(*) FROM tb WHERE ts >= ''2026-01-05 12:00+00''::timestamptz - interval ''1 day'' AND ts < ''2026-01-06''');
SELECT tb_filter('SELECT ts FROM tb WHERE ts >= ''2026-01-05 12:00+00''::timestamptz - interval ''1 day'' AND ts < ''2026-01-06''');
-- not rewritten: now(), a negative interval, below an OR
SELECT tb_parts('SELECT count(*) FROM tb WHERE ts >= now() - interval ''1 hour''');
SELECT tb_parts('SELECT count(*) FROM tb WHERE ts >= ''2026-01-03 12:00+00''::timestamptz - interval ''-1 hour''');
SELECT tb_parts('SELECT count(*) FROM tb WHERE ts < ''2026-01-02''::timestamptz OR ts >= ''2026-01-06 12:00+00''::timestamptz - interval ''1 hour''');
-- and off
SET snouttime.plan_time_bounds = off;
SELECT tb_parts('SELECT count(*) FROM tb WHERE ts >= ''2026-01-03 12:00+00''::timestamptz - interval ''1 hour'' AND ts < ''2026-01-03 12:00+00''::timestamptz');
RESET snouttime.plan_time_bounds;

-- the answers, in zones from UTC-11 to UTC+14 and across New York's DST change (2026-03-08)
CREATE TABLE tb_q (q text);
INSERT INTO tb_q VALUES
	('SELECT count(*), sum(v) FROM $t WHERE ts >= ''2026-01-03 12:00+00''::timestamptz - interval ''1 hour'' AND ts < ''2026-01-03 12:00+00''::timestamptz'),
	('SELECT count(*), sum(v) FROM $t WHERE ts >= ''2026-01-05 12:00+00''::timestamptz - interval ''1 day'''),
	('SELECT count(*), sum(v) FROM $t WHERE ts > ''2026-03-09 00:30''::timestamptz - interval ''1 day 2 hours'' AND ts <= ''2026-03-08 03:00''::timestamptz + interval ''1 day'''),
	('SELECT count(*), sum(v) FROM $t WHERE ts < interval ''2 days'' + ''2026-03-06 06:00''::timestamptz'),
	('SELECT count(*), sum(v) FROM $t WHERE ts >= ''2026-04-06''::timestamptz - interval ''1 month'''),
	('SELECT count(*), sum(v) FROM $t WHERE ts = ''2026-01-02 00:00+00''::timestamptz + interval ''7 minutes'''),
	('SELECT count(*), sum(v) FROM $t WHERE ''2026-01-03 12:00+00''::timestamptz - interval ''30 minutes'' <= ts AND ts < ''2026-01-03 13:00+00'''),
	('SELECT host, count(*), max(v) FROM $t WHERE ts >= ''2026-03-10''::timestamptz - interval ''1 day 12 hours'' AND host = ''h1'' GROUP BY host'),
	('SELECT count(*) FROM $t WHERE ts >= ''infinity''::timestamptz - interval ''1 day'''),
	('SELECT count(*) FROM $t WHERE ts >= ''-infinity''::timestamptz + interval ''1 hour'''),
	('SELECT count(*) FROM $t WHERE ts >= ''2026-01-03''::timestamptz - interval ''-2 days''');
SET timezone = 'UTC';
SELECT 'UTC' AS zone, left(q, 70), tb_check(q) FROM tb_q;
SET timezone = 'America/New_York';
SELECT 'New York' AS zone, left(q, 70), tb_check(q) FROM tb_q;
SET timezone = 'Pacific/Kiritimati';
SELECT 'UTC+14' AS zone, left(q, 70), tb_check(q) FROM tb_q;
SET timezone = 'Pacific/Pago_Pago';
SELECT 'UTC-11' AS zone, left(q, 70), tb_check(q) FROM tb_q;

-- a sum outside the timestamp range still raises the error it always did
SET timezone = 'UTC';
SELECT count(*) FROM tb WHERE ts >= '294276-12-31'::timestamptz + interval '10 years';

-- a generic plan made in one zone and run in others: the bound added in the first holds in all
SET plan_cache_mode = force_generic_plan;
PREPARE tb_day AS SELECT count(*), sum(v) FROM tb WHERE ts >= '2026-03-09 00:30+00'::timestamptz - interval '1 day' AND ts < '2026-03-09 00:30+00'::timestamptz;
PREPARE tb_day_heap AS SELECT count(*), sum(v) FROM tb_heap WHERE ts >= '2026-03-09 00:30+00'::timestamptz - interval '1 day' AND ts < '2026-03-09 00:30+00'::timestamptz;
SET timezone = 'UTC';
EXECUTE tb_day;
EXECUTE tb_day_heap;
SET timezone = 'America/New_York';
EXECUTE tb_day;
EXECUTE tb_day_heap;
SET timezone = 'Pacific/Kiritimati';
EXECUTE tb_day;
EXECUTE tb_day_heap;
RESET plan_cache_mode;
RESET timezone;
