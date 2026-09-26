-- Phase 6: what a role that is not a superuser can and cannot do (PLAN.md Phase 6's security
-- review). S3 credentials are unreadable and unsettable, the object collector is not
-- callable, and the extension's SECURITY DEFINER helpers act only for a table's owner.
SET client_min_messages = warning;
SET snouttime.s3_secret_access_key = 'a secret';
CREATE ROLE st_plain LOGIN;
CREATE SCHEMA sec AUTHORIZATION st_plain;
CREATE TABLE sec_other (a int);
-- load the library, as a real session would have by now
SELECT snouttime.version() IS NOT NULL AS loaded;
SET ROLE st_plain;
SET search_path = sec, public;
\set ON_ERROR_STOP 0
SHOW snouttime.s3_secret_access_key;
SHOW snouttime.s3_access_key_id;
SELECT count(*) AS visible FROM pg_settings WHERE name = 'snouttime.s3_secret_access_key' AND setting = 'a secret';
SET snouttime.s3_secret_access_key = 'mine';
SET snouttime.tier_to = 's3://elsewhere/x';
SELECT snouttime.tier_gc();
-- the helpers behind the event triggers, called directly on somebody else's table
SELECT snouttime._columnar_sync('sec_other');
SELECT snouttime._columnar_drop_side('sec_other'::regclass::oid);
SELECT snouttime._columnar_forget('delta_1', 'deletes_1');
-- what a column store holds is read only by a role that may read the table
SELECT * FROM snouttime.column_sizes('sec_other');
\set ON_ERROR_STOP 1
-- tiering with credentials in settings, not preloaded: refused, since they would be readable
RESET ROLE;
SET snouttime.tier_to = 's3://b/p';
SET snouttime.s3_access_key_id = 'k';
CREATE TABLE sec_tier (a int);
INSERT INTO sec_tier VALUES (1);
\set ON_ERROR_STOP 0
SELECT snouttime.tier('sec_tier');
\set ON_ERROR_STOP 1
SET ROLE st_plain;
SET search_path = sec, public;
-- what the role CAN do: seal a table of its own
CREATE TABLE sec_mine (a int);
INSERT INTO sec_mine SELECT generate_series(1, 10);
SELECT snouttime.seal('sec_mine');
SELECT sum(a) FROM sec_mine;
INSERT INTO sec_mine VALUES (11);
SELECT sum(a) FROM sec_mine;
SELECT attname, encoding, rows FROM snouttime.column_sizes('sec_mine');
-- ...and DROP tables, which it could not until 0.1.5: the sql_drop event trigger read
-- snouttime_internal through to_regclass, which needs USAGE this role does not have, so every
-- DROP TABLE in the database failed (found on SnoutData Cloud, whose owner is such a role).
CREATE TABLE sec_plain (a int);
DROP TABLE sec_plain;
SELECT 'sec_mine'::regclass::oid AS mine_oid \gset
DROP TABLE sec_mine;
SELECT count(*) AS side_tables_left FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'snouttime_internal' AND c.relname IN ('delta_' || :mine_oid, 'deletes_' || :mine_oid);
-- and a series of its own, down to dropping its default partition and the whole table
CREATE TABLE sec_series (ts timestamptz NOT NULL, v int);
SELECT snouttime.create_series('sec_series', 'ts', partition_interval => '1 day', premake => 1);
SELECT snouttime.make_partitions('sec_series', '2026-01-01', '2026-01-03');
INSERT INTO sec_series SELECT timestamptz '2026-01-01+00' + g * interval '1 minute', g FROM generate_series(0, 2879) g;
SELECT snouttime.drop_default('sec_series');
SELECT snouttime.seal('sec_series_p20260101');
-- ...and its seal job, which the worker runs as the table's owner: once nothing is left to
-- seal it asks how much each sealed partition has changed, and until 0.1.6 that read the side
-- tables in SQL and failed on every run for a role like this one
INSERT INTO sec_series VALUES ('2026-01-01 12:00:30+00', -1);
SELECT snouttime._changed_since_seal('sec_series_p20260101', 100) AS changed;
SELECT snouttime.set_sealing('sec_series', interval '1 day');
SELECT detail FROM snouttime._do_job('seal', 'sec_series');
SELECT detail FROM snouttime._do_job('seal', 'sec_series');
SELECT detail FROM snouttime._do_job('seal', 'sec_series');
DROP TABLE sec_series;
SELECT count(*) AS left_behind FROM pg_class WHERE relname LIKE 'sec\_series%';
RESET ROLE;
-- every SECURITY DEFINER function the extension defines pins its search_path
SELECT p.proname FROM pg_proc p JOIN pg_depend d ON d.objid = p.oid AND d.deptype = 'e'
JOIN pg_extension e ON e.oid = d.refobjid AND e.extname = 'snouttime'
WHERE p.prolang <> (SELECT oid FROM pg_language WHERE lanname = 'c')
	AND p.prosecdef AND NOT EXISTS (SELECT 1 FROM unnest(p.proconfig) c WHERE c LIKE 'search_path=%')
ORDER BY 1;
SELECT p.proname AS definer FROM pg_proc p JOIN pg_depend d ON d.objid = p.oid AND d.deptype = 'e'
JOIN pg_extension e ON e.oid = d.refobjid AND e.extname = 'snouttime' WHERE p.prosecdef ORDER BY 1;
