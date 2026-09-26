-- SnoutTime 0.0.0 -> 0.1.0 (docs/snouttime/PLAN.md Phase 6, U1 and U6).
--
-- 0.1.0 is the first versioned catalog. Before it, every build called itself 0.0.0, and two
-- shapes of 0.0.0 ran in SnoutData Cloud: the first rollout (2026-09-23, before seal sizes) and
-- the second (the same day, with them). So every statement here is safe on either: a table is
-- created only if missing, a trigger only if missing, and functions and views are replaced with
-- their 0.1.0 text, which on the second shape they already are. tests/upgrade/extension.sh runs
-- it from both and checks the result is the catalog a fresh 0.1.0 install makes.
--
-- Hand-written, as every delta from here on is: pgrx generates only the full install script.

\echo Use "ALTER EXTENSION snouttime UPDATE TO '0.1.0'" to load this file. \quit

-- What a table took as heap just before its first seal (Phase 8).
CREATE TABLE IF NOT EXISTS snouttime.seal_sizes (
	relid regclass PRIMARY KEY,
	bytes_before int8 NOT NULL,
	sealed_at timestamptz NOT NULL DEFAULT now()
);
SELECT pg_catalog.pg_extension_config_dump('snouttime.seal_sizes', '');

CREATE OR REPLACE FUNCTION snouttime._guard() RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
	row_ record;
	rels regclass[];
	rel regclass;
BEGIN
	IF TG_OP = 'DELETE' THEN row_ := OLD; ELSE row_ := NEW; END IF;
	-- IF, not CASE: a CASE naming row_.relid fails on a table without that column even in
	-- a branch that is never taken.
	IF TG_TABLE_NAME IN ('series', 'seal_sizes') THEN
		rels := ARRAY[row_.relid];
	ELSIF TG_TABLE_NAME = 'rollups' THEN
		rels := ARRAY[row_.relid, row_.source];
	ELSIF TG_TABLE_NAME = 'invalidations' THEN
		rels := ARRAY[row_.rollup];
	ELSE
		rels := ARRAY[row_.target];
	END IF;
	FOREACH rel IN ARRAY rels LOOP
		-- A NULL answer means the table no longer exists. Removing a row about a table that
		-- is gone harms nobody (it is how cleanup after a DROP works); writing one is refused.
		IF TG_OP = 'DELETE' AND snouttime._may_manage(rel) IS NULL THEN
			CONTINUE;
		END IF;
		IF NOT coalesce(snouttime._may_manage(rel), false) THEN
			RAISE EXCEPTION 'permission denied: only the owner of % may change its SnoutTime settings', rel
				USING ERRCODE = 'insufficient_privilege';
		END IF;
	END LOOP;
	IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
	RETURN NEW;
END
$$;

DO $upgrade$
BEGIN
	IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid = 'snouttime.seal_sizes'::regclass AND tgname = 'guard') THEN
		CREATE TRIGGER guard BEFORE INSERT OR UPDATE OR DELETE ON snouttime.seal_sizes
			FOR EACH ROW EXECUTE FUNCTION snouttime._guard();
	END IF;
END
$upgrade$;

REVOKE ALL ON snouttime.seal_sizes FROM PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON snouttime.seal_sizes TO PUBLIC;

CREATE OR REPLACE FUNCTION snouttime._rewrite(leaf regclass, method text, order_by text, codec text, keep_indexes boolean) RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
	old_order text := current_setting('snouttime.columnar_order_by', true);
	old_codec text := current_setting('snouttime.columnar_compression', true);
	old_keep text := current_setting('snouttime.columnar_keep_indexes', true);
BEGIN
	-- the heap's size, the first time it leaves the heap (a reseal passes through heap within
	-- one call, and keeps the size recorded when it was first sealed)
	IF method <> 'heap' AND (SELECT a.amname FROM pg_class c JOIN pg_am a ON a.oid = c.relam WHERE c.oid = leaf) = 'heap' THEN
		INSERT INTO snouttime.seal_sizes (relid, bytes_before)
		VALUES (leaf, pg_total_relation_size(leaf))
		ON CONFLICT (relid) DO NOTHING;
	END IF;
	PERFORM set_config('snouttime.columnar_order_by', coalesce(order_by, ''), true);
	PERFORM set_config('snouttime.columnar_compression', coalesce(codec, 'lz4'), true);
	PERFORM set_config('snouttime.columnar_keep_indexes', CASE WHEN keep_indexes THEN 'on' ELSE 'off' END, true);
	EXECUTE format('ALTER TABLE %s SET ACCESS METHOD %I', leaf, method);
	PERFORM set_config('snouttime.columnar_order_by', coalesce(old_order, ''), true);
	PERFORM set_config('snouttime.columnar_compression', coalesce(old_codec, 'lz4'), true);
	PERFORM set_config('snouttime.columnar_keep_indexes', coalesce(old_keep, 'off'), true);
END
$$;

CREATE OR REPLACE FUNCTION snouttime.unseal(partition regclass) RETURNS integer
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
	leaf regclass;
	n int := 0;
BEGIN
	FOR leaf IN
		SELECT l FROM snouttime._leaves(partition) l WHERE snouttime._is_sealed(l)
	LOOP
		EXECUTE format('ALTER TABLE %s SET ACCESS METHOD heap', leaf);
		DELETE FROM snouttime.seal_sizes WHERE relid = leaf;
		n := n + 1;
	END LOOP;
	RETURN n;
END
$$;

CREATE OR REPLACE FUNCTION snouttime._on_drop() RETURNS event_trigger
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
	dropped oid[];
BEGIN
	SELECT array_agg(objid) INTO dropped
	FROM pg_event_trigger_dropped_objects()
	WHERE classid = 'pg_class'::regclass AND objsubid = 0;
	IF dropped IS NULL THEN
		RETURN;
	END IF;
	DELETE FROM snouttime.invalidations WHERE rollup::oid = ANY (dropped);
	DELETE FROM snouttime.jobs WHERE target::oid = ANY (dropped);
	DELETE FROM snouttime.seal_sizes WHERE relid::oid = ANY (dropped);
	DELETE FROM snouttime.rollups WHERE relid::oid = ANY (dropped) OR source::oid = ANY (dropped)
		OR materialized::oid = ANY (dropped);
	DELETE FROM snouttime.series WHERE relid::oid = ANY (dropped);
END
$$;

CREATE OR REPLACE VIEW snouttime.partition_info AS
SELECT
	s.relid AS series,
	c.oid AS partition,
	c.relname AS name,
	CASE
		WHEN c.oid = snouttime._default_partition(s.relid) THEN 'default'
		WHEN b.lo IS NULL THEN 'foreign'   -- a partition SnoutTime did not make and will not touch
		-- rewritten into the column store (PLAN.md 3.4); with a space key, every leaf is
		WHEN EXISTS (SELECT 1 FROM pg_partition_tree(c.oid) t WHERE snouttime._is_tiered(t.relid))
			AND NOT EXISTS (SELECT 1 FROM pg_partition_tree(c.oid) t JOIN pg_class l ON l.oid = t.relid
				WHERE l.relkind = 'r' AND NOT snouttime._is_tiered(t.relid)) THEN 'tiered'
		WHEN EXISTS (SELECT 1 FROM pg_partition_tree(c.oid) t WHERE snouttime._is_sealed(t.relid))
			AND NOT EXISTS (SELECT 1 FROM pg_partition_tree(c.oid) t JOIN pg_class l ON l.oid = t.relid
				WHERE l.relkind = 'r' AND NOT snouttime._is_sealed(t.relid)) THEN 'sealed'
		WHEN c.relkind = 'p' THEN 'spread' -- hash-partitioned by the space key
		ELSE 'live'
	END AS state,
	b.lo AS range_start,
	b.hi AS range_end,
	-- Summed over the whole tree under this partition: with a space key a partition is
	-- itself partitioned, and a partitioned table has no storage and no rows of its own.
	(SELECT coalesce(sum(greatest(leaf.reltuples, 0)), 0)::int8
	 FROM pg_partition_tree(c.oid) AS t
	 JOIN pg_class leaf ON leaf.oid = t.relid AND leaf.relkind <> 'p') AS estimated_rows,
	(SELECT coalesce(sum(pg_total_relation_size(t.relid)), 0)
	 FROM pg_partition_tree(c.oid) AS t) AS bytes,
	(SELECT count(*) FROM pg_inherits h WHERE h.inhparent = c.oid)::int AS children,
	-- what its leaves took as heap just before they were sealed; NULL for a partition never
	-- sealed. With `bytes` it is the compression a seal bought.
	(SELECT sum(z.bytes_before)
	 FROM pg_partition_tree(c.oid) AS t
	 JOIN snouttime.seal_sizes z ON z.relid = t.relid)::int8 AS bytes_before
FROM snouttime.series s
JOIN pg_inherits i ON i.inhparent = s.relid
JOIN pg_class c ON c.oid = i.inhrelid
LEFT JOIN LATERAL snouttime._bounds_of(s, c.oid) AS b ON true;

CREATE OR REPLACE VIEW snouttime.series_info AS
SELECT
	s.relid AS series,
	s.time_column,
	s.time_type,
	coalesce(s.partition_interval::text, s.partition_width::text) AS partition_size,
	s.space_column,
	s.space_partitions,
	coalesce(s.retention::text, s.retention_width::text) AS retention,
	count(*) FILTER (WHERE p.state <> 'default')::int AS partitions,
	count(*) FILTER (WHERE p.state = 'foreign')::int AS foreign_partitions,
	-- The bounds are text (timestamps and integer keys alike), so they are ordered as what they
	-- are: min() on text put '9000' after '10000' on an integer series.
	(array_agg(p.range_start ORDER BY
		CASE WHEN s.partition_width IS NOT NULL THEN p.range_start::numeric END,
		CASE WHEN s.partition_width IS NULL THEN p.range_start::timestamptz END)
		FILTER (WHERE p.state <> 'default' AND p.range_start IS NOT NULL))[1] AS oldest_range,
	(array_agg(p.range_end ORDER BY
		CASE WHEN s.partition_width IS NOT NULL THEN p.range_end::numeric END DESC,
		CASE WHEN s.partition_width IS NULL THEN p.range_end::timestamptz END DESC)
		FILTER (WHERE p.state <> 'default' AND p.range_end IS NOT NULL))[1] AS newest_range,
	coalesce(sum(p.estimated_rows) FILTER (WHERE p.state = 'default'), 0) AS rows_in_default,
	coalesce(sum(p.estimated_rows), 0) AS estimated_rows,
	(SELECT coalesce(sum(pg_total_relation_size(t.relid)), 0)
	 FROM pg_partition_tree(s.relid) AS t) AS bytes,
	-- the sealed partitions (not tiered ones, whose bytes are in S3), now and as heap
	coalesce(sum(p.bytes) FILTER (WHERE p.state = 'sealed' AND p.bytes_before IS NOT NULL), 0)::int8 AS sealed_bytes,
	coalesce(sum(p.bytes_before) FILTER (WHERE p.state = 'sealed'), 0)::int8 AS sealed_bytes_before
FROM snouttime.series s
LEFT JOIN snouttime.partition_info p ON p.series = s.relid
GROUP BY s.relid, s.time_column, s.time_type, s.partition_interval, s.partition_width,
	s.space_column, s.space_partitions, s.retention, s.retention_width;
