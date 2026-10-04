-- SnoutTime 0.1.6 -> 0.1.7 (docs/snouttime/PLAN.md Phase 6, U1; the Log, 2026-10-04).
--
-- A job runs as its table's owner in a security-restricted operation. The worker is a
-- superuser, and run_due_job() used to switch to the owner with SET LOCAL ROLE, which code of
-- the owner's that a job runs (a trigger, an index expression, a rollup's query) could undo
-- with RESET ROLE and act as the superuser. _run_job() (Rust, src/jobs.rs) switches the way
-- VACUUM and REFRESH MATERIALIZED VIEW do, under which Postgres refuses any change of role,
-- and run_due_job() takes only jobs on tables its caller may manage.
--
-- With it, from the same review of everything that runs as the extension's owner:
--   _guard          an UPDATE is checked against the row before it as well as after, and a
--                   rollup's materialized table is checked too;
--   _invalidate     acts only as the triggers create_rollup makes (with their transition
--                   tables) and only on the table's real time column;
--   _columnar_sync  copies an added column's default as a value instead of evaluating the
--                   owner's expression again, and will not cast rows of a delta store with
--                   code that is not the server's own.

\echo Use "ALTER EXTENSION snouttime UPDATE TO '0.1.7'" to load this file. \quit

CREATE FUNCTION snouttime._run_job(job_kind text, rel regclass)
RETURNS TABLE (detail text, again boolean)
LANGUAGE c VOLATILE STRICT
AS 'MODULE_PATHNAME', 'run_job_wrapper';

CREATE OR REPLACE FUNCTION snouttime.run_due_job() RETURNS boolean
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
	j record;
	started timestamptz := pg_catalog.clock_timestamp();
	outcome record;
	detail text;
	again boolean := false;
BEGIN
	SELECT * INTO j FROM snouttime.jobs
	WHERE enabled AND next_run <= pg_catalog.now()
		AND coalesce(snouttime._may_manage(target), false)
	ORDER BY next_run
	FOR UPDATE SKIP LOCKED
	LIMIT 1;
	IF NOT FOUND THEN
		RETURN false;
	END IF;

	BEGIN
		SELECT * INTO outcome FROM snouttime._run_job(j.kind, j.target);
		detail := outcome.detail;
		again := outcome.again;
		INSERT INTO snouttime.job_runs (kind, target, started_at, finished_at, ok, detail)
		VALUES (j.kind, j.target, started, pg_catalog.clock_timestamp(), true, detail);
	EXCEPTION WHEN OTHERS THEN
		-- A job that fails is recorded and rescheduled; it must not stop the others, and
		-- the transaction has to survive to write the log line. Leaving this block rolls
		-- back everything the job did and puts the caller's identity back.
		INSERT INTO snouttime.job_runs (kind, target, started_at, finished_at, ok, detail)
		VALUES (j.kind, j.target, started, pg_catalog.clock_timestamp(), false, SQLERRM);
	END;

	-- A job with more of the same to do is due again at once, so a backlog drains in this
	-- pass rather than one item per wake-up.
	UPDATE snouttime.jobs
	SET next_run = CASE WHEN again THEN pg_catalog.now() ELSE pg_catalog.now() + schedule END
	WHERE kind = j.kind AND target = j.target;
	RETURN true;
END
$$;

CREATE OR REPLACE FUNCTION snouttime._guard() RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
	row_ record;
	rels regclass[];
	rel regclass;
	pass int;
BEGIN
	-- Pass 1 is the row as it was (UPDATE, DELETE), pass 2 the row as it will be (INSERT,
	-- UPDATE). An UPDATE is checked both ways: checking only the new row let a role re-point a
	-- row about somebody else's table at a table of its own, which removed that registration
	-- (found 2026-10-04).
	FOR pass IN 1..2 LOOP
		IF pass = 1 THEN
			CONTINUE WHEN TG_OP = 'INSERT';
			row_ := OLD;
		ELSE
			EXIT WHEN TG_OP = 'DELETE';
			row_ := NEW;
		END IF;
		-- IF, not CASE: a CASE naming row_.relid fails on a table without that column even in
		-- a branch that is never taken.
		IF TG_TABLE_NAME IN ('series', 'seal_sizes') THEN
			rels := ARRAY[row_.relid];
		ELSIF TG_TABLE_NAME = 'rollups' THEN
			-- the materialized table too: refresh_rollup and drop_rollup write and drop it
			rels := ARRAY[row_.relid, row_.source, row_.materialized];
		ELSIF TG_TABLE_NAME = 'invalidations' THEN
			rels := ARRAY[row_.rollup];
		ELSE
			rels := ARRAY[row_.target];
		END IF;
		FOREACH rel IN ARRAY rels LOOP
			-- A NULL answer means the table no longer exists. Removing or changing a row about a
			-- table that is gone harms nobody (it is how cleanup after a DROP works); a row that
			-- names one is never written.
			IF pass = 1 AND snouttime._may_manage(rel) IS NULL THEN
				CONTINUE;
			END IF;
			IF NOT coalesce(snouttime._may_manage(rel), false) THEN
				RAISE EXCEPTION 'permission denied: only the owner of % may change its SnoutTime settings', rel
					USING ERRCODE = 'insufficient_privilege';
			END IF;
		END LOOP;
	END LOOP;
	IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
	RETURN NEW;
END
$$;

CREATE OR REPLACE FUNCTION snouttime._invalidate() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
	col name;
	t regtype;
	tg record;
	lo text;
	hi text;
	lo2 text;
	hi2 text;
BEGIN
	SELECT g.tgname, g.tgnewtable, g.tgoldtable INTO tg
	FROM pg_trigger g WHERE g.tgrelid = TG_RELID AND g.tgname = TG_NAME;
	IF tg.tgname IS NULL OR TG_NAME <> 'snouttime_invalidate_' || lower(TG_OP)
		OR (TG_OP IN ('INSERT', 'UPDATE') AND tg.tgnewtable IS DISTINCT FROM 'new_rows')
		OR (TG_OP IN ('DELETE', 'UPDATE') AND tg.tgoldtable IS DISTINCT FROM 'old_rows') THEN
		RAISE EXCEPTION 'snouttime._invalidate() runs only as the triggers snouttime.create_rollup() makes'
			USING ERRCODE = 'insufficient_privilege';
	END IF;
	SELECT a.attname, a.atttypid::regtype INTO col, t
	FROM pg_attribute a
	WHERE a.attrelid = TG_RELID AND a.attname = TG_ARGV[0] AND a.attnum > 0 AND NOT a.attisdropped
		AND a.atttypid IN ('timestamptz'::regtype, 'timestamp'::regtype, 'date'::regtype,
			'smallint'::regtype, 'integer'::regtype, 'bigint'::regtype);
	IF col IS NULL THEN
		RAISE EXCEPTION 'snouttime._invalidate(): % has no time column %', TG_RELID::regclass, TG_ARGV[0];
	END IF;
	IF TG_OP = 'TRUNCATE' THEN
		-- everything: refresh_rollup reads the extreme keys as "from the first bucket it has"
		INSERT INTO snouttime.invalidations (rollup, lo, hi)
		SELECT d, -9223372036854775807, 9223372036854775807 FROM snouttime._dependents(TG_RELID) d;
		RETURN NULL;
	END IF;
	IF TG_OP IN ('INSERT', 'UPDATE') THEN
		EXECUTE format('SELECT pg_catalog.min(%I)::text, pg_catalog.max(%I)::text FROM new_rows', col, col) INTO lo, hi;
	END IF;
	IF TG_OP IN ('DELETE', 'UPDATE') THEN
		EXECUTE format('SELECT pg_catalog.min(%I)::text, pg_catalog.max(%I)::text FROM old_rows', col, col) INTO lo2, hi2;
	END IF;
	IF lo IS NULL AND lo2 IS NULL THEN
		RETURN NULL;
	END IF;
	INSERT INTO snouttime.invalidations (rollup, lo, hi)
	SELECT d, least(snouttime._key(lo, t), snouttime._key(lo2, t)),
		greatest(snouttime._key(hi, t), snouttime._key(hi2, t))
	FROM snouttime._dependents(TG_RELID) d;
	RETURN NULL;
END
$$;

CREATE FUNCTION snouttime._base_type(typ oid) RETURNS oid
LANGUAGE sql STABLE
SET search_path = pg_catalog, pg_temp
AS $$
	WITH RECURSIVE b(t, d) AS (
		SELECT typ, 0
		UNION ALL
		SELECT y.typbasetype, b.d + 1 FROM b JOIN pg_type y ON y.oid = b.t WHERE y.typtype = 'd'
	)
	SELECT t FROM b ORDER BY d DESC LIMIT 1
$$;

CREATE OR REPLACE FUNCTION snouttime._columnar_sync(rel regclass) RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
	columnar boolean;
	delta text := 'delta_' || rel::oid;
	deletes text := 'deletes_' || rel::oid;
	have regclass;
	a record;
	cols text := '';
	dropped text[] := '{}';
	dropped_col text;
	busy boolean;
BEGIN
	SELECT am.amname IN ('snouttime_columnar', 'snouttime_tiered') INTO columnar
	FROM pg_class c LEFT JOIN pg_am am ON am.oid = c.relam WHERE c.oid = rel;
	have := to_regclass(format('snouttime_internal.%I', delta));

	IF NOT coalesce(columnar, false) THEN
		IF have IS NOT NULL THEN
			PERFORM snouttime._columnar_forget(delta, deletes);
		END IF;
		RETURN;
	END IF;

	IF have IS NULL THEN
		FOR a IN SELECT attnum, attname, attisdropped, format_type(atttypid, atttypmod) AS typ,
				CASE WHEN attcollation <> 0 THEN (SELECT format(' COLLATE %I.%I', n.nspname, co.collname)
					FROM pg_collation co JOIN pg_namespace n ON n.oid = co.collnamespace WHERE co.oid = attcollation) END AS coll
			FROM pg_attribute WHERE attrelid = rel AND attnum > 0 ORDER BY attnum
		LOOP
			IF a.attisdropped THEN
				cols := cols || format(', %I int', '_dropped_' || a.attnum);
				dropped := dropped || ('_dropped_' || a.attnum);
			ELSE
				cols := cols || format(', %I %s%s', a.attname, a.typ, coalesce(a.coll, ''));
			END IF;
		END LOOP;
		-- USING heap, always: pg_restore sets default_table_access_method to this very access
		-- method before it creates a sealed table, and a delta store that was itself a column
		-- store took the server down (tests/dump/roundtrip.sh, 2026-09-23).
		EXECUTE format('CREATE TABLE snouttime_internal.%I (%s) USING heap', delta, substr(cols, 3));
		FOREACH dropped_col IN ARRAY dropped LOOP
			EXECUTE format('ALTER TABLE snouttime_internal.%I DROP COLUMN %I', delta, dropped_col);
		END LOOP;
		EXECUTE format('CREATE TABLE snouttime_internal.%I (row_number int8 NOT NULL, '
			'locked_only boolean NOT NULL DEFAULT false, moved boolean NOT NULL DEFAULT false) USING heap', deletes);
		EXECUTE format('CREATE INDEX ON snouttime_internal.%I (row_number)', deletes);
		EXECUTE format('ALTER EXTENSION snouttime ADD TABLE snouttime_internal.%I', delta);
		EXECUTE format('ALTER EXTENSION snouttime ADD TABLE snouttime_internal.%I', deletes);
		RETURN;
	END IF;

	-- In step: every attribute number of the table has one in the delta store.
	FOR a IN SELECT t.attnum, t.attname, t.attisdropped, format_type(t.atttypid, t.atttypmod) AS typ,
			dd.attname AS dname, dd.attisdropped AS ddropped, format_type(dd.atttypid, dd.atttypmod) AS dtyp,
			t.atttypid AS typid, dd.atttypid AS dtypid
		FROM pg_attribute t
		LEFT JOIN pg_attribute dd ON dd.attrelid = have AND dd.attnum = t.attnum
		WHERE t.attrelid = rel AND t.attnum > 0 ORDER BY t.attnum
	LOOP
		IF a.dname IS NULL THEN
			IF a.attisdropped THEN
				EXECUTE format('ALTER TABLE %s ADD COLUMN %I int', have, '_dropped_' || a.attnum);
				EXECUTE format('ALTER TABLE %s DROP COLUMN %I', have, '_dropped_' || a.attnum);
			ELSE
				EXECUTE format('ALTER TABLE %s ADD COLUMN %I %s', have, a.attname, a.typ);
				-- the default, as a value, never as the owner's expression (see above)
				UPDATE pg_attribute d SET atthasmissing = true, attmissingval = p.attmissingval
				FROM pg_attribute p
				WHERE p.attrelid = rel AND p.attnum = a.attnum AND p.atthasmissing
					AND d.attrelid = have AND d.attnum = a.attnum;
			END IF;
		ELSIF a.attisdropped AND NOT a.ddropped THEN
			EXECUTE format('ALTER TABLE %s DROP COLUMN %I', have, a.dname);
		ELSIF NOT a.attisdropped AND a.typ <> a.dtyp THEN
			-- Rows already in the delta store are converted only when that runs none of the
			-- owner's code: a new typmod, a domain to its base type, a binary-coercible cast.
			IF NOT (a.typid = a.dtypid OR a.typid = snouttime._base_type(a.dtypid)
				OR EXISTS (SELECT 1 FROM pg_cast c WHERE c.castsource = a.dtypid
					AND c.casttarget = a.typid AND c.castmethod = 'b')) THEN
				EXECUTE format('SELECT EXISTS (SELECT 1 FROM %s)', have) INTO busy;
				IF busy THEN
					RAISE EXCEPTION 'the delta store of % holds rows that changing % to % would have to cast', rel, a.attname, a.typ
						USING ERRCODE = 'object_not_in_prerequisite_state',
						HINT = 'Reseal the table first (snouttime.reseal()), then change the column.';
				END IF;
			END IF;
			EXECUTE format('ALTER TABLE %s ALTER COLUMN %I TYPE %s', have, a.dname, a.typ);
		END IF;
	END LOOP;
END
$$;
