-- The jobs (PLAN.md Phase 1.2): making partitions ahead, emptying the default partition,
-- and dropping what is past its retention.
--
-- The background worker (worker.rs) does nothing but call `snouttime.run_due_job()` in a
-- loop, one job per transaction. That is deliberate: the same function is the whole
-- feature when called by hand, from pg_cron, or from anything else, and a database with no
-- worker running is a database whose jobs are simply not running rather than one that
-- behaves differently.
--
-- A job runs as the OWNER of the table it acts on (`SET LOCAL ROLE`), never as the
-- superuser the worker happens to be. So a job can do exactly what its owner could do by
-- hand, and a row in `jobs` naming someone else's table cannot exist in the first place
-- (the catalog's guard trigger).


-- Reconstruct a partition's range from the name SnoutTime gave it. Partitions made by
-- somebody else return NULL and are left alone, the same rule `_make_partition` follows.
CREATE FUNCTION snouttime._bounds_of(s snouttime.series, part regclass,
	OUT lo text, OUT hi text)
LANGUAGE plpgsql STABLE
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
	parent_name text;
	suffix text;
	anchor text;
	r record;
BEGIN
	SELECT c.relname INTO parent_name FROM pg_class c WHERE c.oid = s.relid;
	SELECT substring(c.relname FROM '_p(m?[0-9_]+)$') INTO suffix
	FROM pg_class c WHERE c.oid = part;
	IF suffix IS NULL THEN
		RETURN;
	END IF;

	IF s.partition_width IS NOT NULL THEN
		IF left(suffix, 1) = 'm' THEN
			anchor := '-' || substr(suffix, 2);
		ELSE
			anchor := suffix;
		END IF;
		IF anchor !~ '^-?[0-9]+$' THEN
			RETURN;
		END IF;
	ELSIF suffix ~ '^[0-9]{8}$' THEN
		anchor := substr(suffix, 1, 4) || '-' || substr(suffix, 5, 2) || '-' || substr(suffix, 7, 2);
		IF s.time_type = 'timestamptz'::regtype THEN
			anchor := anchor || 'T00:00:00+00';
		END IF;
	ELSIF suffix ~ '^[0-9]{8}_[0-9]{6}$' THEN
		anchor := substr(suffix, 1, 4) || '-' || substr(suffix, 5, 2) || '-' || substr(suffix, 7, 2)
			|| 'T' || substr(suffix, 10, 2) || ':' || substr(suffix, 12, 2) || ':' || substr(suffix, 14, 2);
		IF s.time_type = 'timestamptz'::regtype THEN
			anchor := anchor || '+00';
		END IF;
	ELSE
		RETURN;
	END IF;

	SELECT * INTO r FROM snouttime._range_for(s, anchor);
	-- A name that does not round-trip is not one of ours.
	IF r.suffix <> 'p' || suffix THEN
		RETURN;
	END IF;
	lo := r.lo;
	hi := r.hi;
END
$$;


-- How much time a series table keeps, as a cutoff: every partition whose range ends at or
-- before this is past its retention. NULL when the table keeps everything.
CREATE FUNCTION snouttime._retention_cutoff(s snouttime.series) RETURNS text
LANGUAGE plpgsql STABLE
AS $$
DECLARE
	top int8;
BEGIN
	IF s.retention IS NOT NULL THEN
		RETURN CASE s.time_type
			WHEN 'timestamptz'::regtype THEN (pg_catalog.now() - s.retention)::text
			ELSE ((pg_catalog.now() AT TIME ZONE 'UTC') - s.retention)::text
		END;
	END IF;
	IF s.retention_width IS NOT NULL THEN
		-- For an integer time column "now" is the largest value the table holds.
		EXECUTE pg_catalog.format('SELECT pg_catalog.max(%I)::int8 FROM %s', s.time_column, s.relid)
			INTO top;
		IF top IS NULL THEN
			RETURN NULL;
		END IF;
		RETURN (top - s.retention_width)::text;
	END IF;
	RETURN NULL;
END
$$;


CREATE FUNCTION snouttime.set_retention(relation regclass, keep interval) RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
BEGIN
	IF keep IS NOT NULL AND keep <= interval '0' THEN
		RAISE EXCEPTION 'retention must be greater than zero' USING ERRCODE = 'invalid_parameter_value';
	END IF;
	UPDATE snouttime.series SET retention = keep WHERE relid = relation;
	IF NOT FOUND THEN
		RAISE EXCEPTION '% is not a series table', relation;
	END IF;
	IF keep IS NULL THEN
		DELETE FROM snouttime.jobs WHERE kind = 'retention' AND target = relation;
	ELSE
		INSERT INTO snouttime.jobs (kind, target, schedule) VALUES ('retention', relation, interval '1 hour')
		ON CONFLICT (kind, target) DO NOTHING;
	END IF;
END
$$;

CREATE FUNCTION snouttime.set_retention(relation regclass, keep bigint) RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
BEGIN
	IF keep IS NOT NULL AND keep <= 0 THEN
		RAISE EXCEPTION 'retention must be greater than zero' USING ERRCODE = 'invalid_parameter_value';
	END IF;
	UPDATE snouttime.series SET retention_width = keep WHERE relid = relation;
	IF NOT FOUND THEN
		RAISE EXCEPTION '% is not a series table', relation;
	END IF;
	IF keep IS NULL THEN
		DELETE FROM snouttime.jobs WHERE kind = 'retention' AND target = relation;
	ELSE
		INSERT INTO snouttime.jobs (kind, target, schedule) VALUES ('retention', relation, interval '1 hour')
		ON CONFLICT (kind, target) DO NOTHING;
	END IF;
END
$$;


-- Drop whole partitions that end at or before `cutoff` (a value of the table's time type, as
-- text). It never deletes rows from a partition it cannot drop whole, and it never touches
-- the default partition or a partition SnoutTime did not make. Returns how many it dropped.
CREATE FUNCTION snouttime._drop_before(relation regclass, cutoff text) RETURNS integer
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
	s snouttime.series;
	def regclass;
	part regclass;
	b record;
	dropped int := 0;
BEGIN
	SELECT * INTO s FROM snouttime.series WHERE relid = relation;
	IF NOT FOUND THEN
		RAISE EXCEPTION '% is not a series table', relation;
	END IF;
	IF cutoff IS NULL THEN
		RETURN 0;
	END IF;
	def := snouttime._default_partition(relation);

	FOR part IN
		SELECT i.inhrelid FROM pg_inherits i
		WHERE i.inhparent = relation AND i.inhrelid IS DISTINCT FROM def
	LOOP
		SELECT * INTO b FROM snouttime._bounds_of(s, part);
		CONTINUE WHEN b.hi IS NULL;
		IF s.partition_width IS NOT NULL THEN
			CONTINUE WHEN b.hi::int8 > cutoff::int8;
		ELSIF s.time_type = 'timestamptz'::regtype THEN
			CONTINUE WHEN b.hi::timestamptz > cutoff::timestamptz;
		ELSE
			CONTINUE WHEN b.hi::timestamp > cutoff::timestamp;
		END IF;
		EXECUTE format('DROP TABLE %s', part);
		dropped := dropped + 1;
	END LOOP;
	RETURN dropped;
END
$$;

-- Drop whole partitions that are entirely past the retention. Returns how many it dropped.
CREATE FUNCTION snouttime.apply_retention(relation regclass) RETURNS integer
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
	s snouttime.series;
BEGIN
	SELECT * INTO s FROM snouttime.series WHERE relid = relation;
	IF NOT FOUND THEN
		RAISE EXCEPTION '% is not a series table', relation;
	END IF;
	RETURN snouttime._drop_before(relation, snouttime._retention_cutoff(s));
END
$$;

-- A one-off retention (PLAN.md 1.3): drop every whole partition that ends at or before
-- `before`, by the same rules. Returns how many it dropped.
CREATE FUNCTION snouttime.drop_before(relation regclass, before timestamptz) RETURNS integer
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
	t regtype;
BEGIN
	SELECT time_type INTO t FROM snouttime.series WHERE relid = relation;
	IF NOT FOUND THEN
		RAISE EXCEPTION '% is not a series table', relation;
	END IF;
	IF t NOT IN ('timestamptz'::regtype, 'timestamp'::regtype, 'date'::regtype) THEN
		RAISE EXCEPTION '% has an integer time column: give drop_before a number', relation
			USING ERRCODE = 'datatype_mismatch';
	END IF;
	RETURN snouttime._drop_before(relation, CASE t WHEN 'timestamptz'::regtype THEN before::text
		ELSE (before AT TIME ZONE 'UTC')::text END);
END
$$;

CREATE FUNCTION snouttime.drop_before(relation regclass, before bigint) RETURNS integer
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
	t regtype;
BEGIN
	SELECT time_type INTO t FROM snouttime.series WHERE relid = relation;
	IF NOT FOUND THEN
		RAISE EXCEPTION '% is not a series table', relation;
	END IF;
	IF t IN ('timestamptz'::regtype, 'timestamp'::regtype, 'date'::regtype) THEN
		RAISE EXCEPTION '% has a % time column: give drop_before a time', relation, t
			USING ERRCODE = 'datatype_mismatch';
	END IF;
	RETURN snouttime._drop_before(relation, before::text);
END
$$;



CREATE FUNCTION snouttime._do_job(job_kind text, rel regclass,
	OUT detail text, OUT again boolean)
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
	-- The parameters are NOT called kind and target: those are columns of
	-- snouttime.jobs, and a bare reference to one in a statement against that table is
	-- ambiguous (found by the jobs regression test, 2026-09-22).
	s snouttime.series;
	def regclass;
	mb record;
	n int;
	found_rows boolean;
	part regclass;
BEGIN
	again := false;
	IF job_kind = 'refresh' THEN
		-- a rollup (rollup.sql): the target is its view, not a series table
		n := snouttime.refresh_rollup(rel);
		detail := n || CASE n WHEN 1 THEN ' range of buckets recomputed' ELSE ' ranges of buckets recomputed' END;
		RETURN;
	END IF;
	SELECT * INTO s FROM snouttime.series WHERE relid = rel;
	IF NOT FOUND THEN
		detail := 'skipped: not a series table';
		RETURN;
	END IF;

	IF job_kind = 'premake' THEN
		n := snouttime.premake(rel);
		detail := n || ' partitions made';
		-- Rows that arrive AFTER the table was converted (a late write, a far-future one) land
		-- in the default partition too, and the migrate job that swept the conversion's rows
		-- removed itself when it was done. So this job, which runs on every series table on a
		-- schedule, is what notices them and puts the migrate job back. EXISTS reads at most
		-- one row. (Found 2026-09-23 by tests/soak/soak.sh: until then, only the rows present
		-- at create_series were ever swept.)
		def := snouttime._default_partition(rel);
		IF def IS NOT NULL THEN
			EXECUTE format('SELECT EXISTS (SELECT 1 FROM %s)', def) INTO found_rows;
			IF found_rows THEN
				INSERT INTO snouttime.jobs (kind, target, schedule) VALUES ('migrate', rel, interval '1 minute')
				ON CONFLICT (kind, target) DO UPDATE SET next_run = least(snouttime.jobs.next_run, now());
				detail := detail || '; rows waiting in the default partition, migrate scheduled';
			END IF;
		END IF;
		RETURN;
	ELSIF job_kind = 'retention' THEN
		n := snouttime.apply_retention(rel);
		detail := n || ' partitions dropped';
		-- Dropping is cheap and a retention backlog is usually many partitions at once.
		again := n > 0;
		RETURN;
	ELSIF job_kind = 'seal' THEN
		-- One partition per run (seal.sql). A seal waits at most five seconds for its lock
		-- and gives up rather than queue behind a user's long transaction (D10); the next
		-- run tries again.
		PERFORM set_config('lock_timeout', '5s', true);
		part := snouttime._next_to_seal(s);
		IF part IS NOT NULL THEN
			n := snouttime.seal(part);
			detail := 'sealed ' || part::text || CASE WHEN n > 1 THEN ' (' || n || ' tables)' ELSE '' END;
			again := true;
			RETURN;
		END IF;
		part := snouttime._next_to_reseal(s);
		IF part IS NOT NULL THEN
			PERFORM snouttime.reseal(part);
			detail := 'resealed ' || part::text;
			again := true;
			RETURN;
		END IF;
		detail := 'nothing to seal';
		RETURN;
	ELSIF job_kind = 'tier' THEN
		-- One partition per run, like sealing, and the same lock rule (D10).
		PERFORM set_config('lock_timeout', '5s', true);
		part := snouttime._next_to_tier(s);
		IF part IS NOT NULL THEN
			n := snouttime.tier(part);
			detail := 'tiered ' || part::text;
			again := true;
			RETURN;
		END IF;
		IF current_setting('snouttime.tier_gc', true) = 'on' THEN
			detail := 'nothing to tier; ' || snouttime.tier_gc() || ' orphaned objects deleted';
		ELSE
			detail := 'nothing to tier';
		END IF;
		RETURN;
	ELSIF job_kind = 'migrate' THEN
		-- One batch of partitions per run (_migrate_batch: about an eighth of the default
		-- partition's span), so a job never holds its locks for the whole move. When the
		-- default partition is empty there is nothing left to do and the job removes itself.
		def := snouttime._default_partition(rel);
		IF def IS NULL THEN
			DELETE FROM snouttime.jobs j WHERE j.kind = 'migrate' AND j.target = rel;
			detail := 'no default partition';
			RETURN;
		END IF;
		SELECT * INTO mb FROM snouttime._migrate_batch(rel);
		IF mb.stopped_at IS NOT NULL THEN
			DELETE FROM snouttime.jobs j WHERE j.kind = 'migrate' AND j.target = rel;
			detail := 'stopped: rows from ' || mb.stopped_at || ' fall in a range covered by a partition SnoutTime did not make';
			RETURN;
		END IF;
		IF mb.moved = 0 THEN
			DELETE FROM snouttime.jobs j WHERE j.kind = 'migrate' AND j.target = rel;
			detail := 'default partition is empty';
			RETURN;
		END IF;
		detail := 'moved rows into ' || mb.moved || CASE mb.moved WHEN 1 THEN ' partition' ELSE ' partitions' END;
		again := true;   -- there may be more in the default partition; do not wait a pass
		RETURN;
	END IF;
	detail := 'skipped: unknown job';
END
$$;


-- Run the one job that is most overdue, as the owner of its table, and record it. Returns
-- true when it ran something, false when nothing was due: a caller loops until false.
--
-- The row is taken with FOR UPDATE SKIP LOCKED, so several workers (or a worker and a
-- person) can call this at once without doing the same job twice.
CREATE FUNCTION snouttime.run_due_job() RETURNS boolean
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
	j record;
	owner_name text;
	started timestamptz := pg_catalog.clock_timestamp();
	outcome record;
	detail text;
	again boolean := false;
BEGIN
	SELECT * INTO j FROM snouttime.jobs
	WHERE enabled AND next_run <= pg_catalog.now()
	ORDER BY next_run
	FOR UPDATE SKIP LOCKED
	LIMIT 1;
	IF NOT FOUND THEN
		RETURN false;
	END IF;

	SELECT pg_catalog.pg_get_userbyid(c.relowner) INTO owner_name
	FROM pg_catalog.pg_class c WHERE c.oid = j.target;

	BEGIN
		IF owner_name IS NOT NULL THEN
			EXECUTE pg_catalog.format('SET LOCAL ROLE %I', owner_name);
		END IF;
		SELECT * INTO outcome FROM snouttime._do_job(j.kind, j.target);
		detail := outcome.detail;
		again := outcome.again;
		RESET ROLE;
		INSERT INTO snouttime.job_runs (kind, target, started_at, finished_at, ok, detail)
		VALUES (j.kind, j.target, started, pg_catalog.clock_timestamp(), true, detail);
	EXCEPTION WHEN OTHERS THEN
		-- A job that fails is recorded and rescheduled; it must not stop the others, and
		-- the transaction has to survive to write the log line.
		RESET ROLE;
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

-- The worker (and anyone else) may run jobs; who may CHANGE them is the guard trigger's
-- business, and what a job may do is its table owner's privileges.
GRANT EXECUTE ON FUNCTION snouttime.run_due_job() TO PUBLIC;
