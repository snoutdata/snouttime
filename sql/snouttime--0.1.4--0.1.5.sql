-- SnoutTime 0.1.4 -> 0.1.5 (docs/snouttime/PLAN.md Phase 6, U1).
--
-- The sql_drop event trigger asked to_regclass('snouttime_internal.delta_<oid>'), which needs
-- USAGE on snouttime_internal. A database's owner on SnoutData Cloud is not a superuser and does
-- not have it, so with SnoutTime installed EVERY DROP TABLE in the database failed with
-- "permission denied for schema snouttime_internal", a plain table's included. It reads the
-- catalog now, which needs no privilege. The function below is src/columnar/mod.rs's text.

\echo Use "ALTER EXTENSION snouttime UPDATE TO '0.1.5'" to load this file. \quit

CREATE OR REPLACE FUNCTION snouttime._columnar_drop() RETURNS event_trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
	r record;
BEGIN
	-- The catalog, not to_regclass: to_regclass needs USAGE on snouttime_internal, which a
	-- database's owner does not have on SnoutData Cloud, so every DROP TABLE in a database with
	-- SnoutTime failed with "permission denied for schema snouttime_internal" (0.1.5, found
	-- 2026-09-26; the suite ran as a superuser and never saw it).
	FOR r IN SELECT d.objid FROM pg_event_trigger_dropped_objects() d
		WHERE d.object_type = 'table' AND d.schema_name <> 'snouttime_internal'
			AND EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
				WHERE n.nspname = 'snouttime_internal'
					AND c.relname IN ('delta_' || d.objid, 'deletes_' || d.objid))
	LOOP
		PERFORM snouttime._columnar_drop_side(r.objid);
	END LOOP;
END
$$;
