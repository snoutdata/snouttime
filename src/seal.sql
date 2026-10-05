-- Sealing: rewriting old partitions into the column store, by hand or by the
-- worker, and undoing it.
--
-- A seal is `ALTER TABLE <leaf> SET ACCESS METHOD snouttime_columnar` with the series table's
-- order and codec in force for that one statement. With a space key a time partition is
-- itself partitioned, and it is its leaves that are sealed.


-- The series table a partition belongs to: its parent, or with a space key its grandparent.
CREATE FUNCTION snouttime._series_of(part regclass) RETURNS snouttime.series
LANGUAGE sql STABLE
SET search_path = pg_catalog, pg_temp
AS $$
	SELECT s.* FROM snouttime.series s
	WHERE s.relid IN (
		SELECT i.inhparent FROM pg_inherits i WHERE i.inhrelid = part
		UNION ALL
		SELECT i2.inhparent FROM pg_inherits i JOIN pg_inherits i2 ON i2.inhrelid = i.inhparent
		WHERE i.inhrelid = part)
	LIMIT 1
$$;

-- The order within a column store: the series table's own choice, or the space key then time.
CREATE FUNCTION snouttime._seal_order(s snouttime.series) RETURNS text
LANGUAGE sql IMMUTABLE
AS $$
	SELECT CASE
		WHEN s.seal_order_by IS NOT NULL THEN pg_catalog.array_to_string(s.seal_order_by, ',')
		WHEN s.space_column IS NOT NULL THEN s.space_column || ',' || s.time_column
		ELSE s.time_column::text
	END
$$;

-- Sealed: in a column store, here or tiered to S3 (a tiered partition was sealed first).
CREATE FUNCTION snouttime._is_sealed(rel regclass) RETURNS boolean
LANGUAGE sql STABLE
SET search_path = pg_catalog, pg_temp
AS $$
	SELECT EXISTS (SELECT 1 FROM pg_class c JOIN pg_am am ON am.oid = c.relam
		WHERE c.oid = rel AND am.amname IN ('snouttime_columnar', 'snouttime_tiered'))
$$;

CREATE FUNCTION snouttime._is_tiered(rel regclass) RETURNS boolean
LANGUAGE sql STABLE
SET search_path = pg_catalog, pg_temp
AS $$
	SELECT EXISTS (SELECT 1 FROM pg_class c JOIN pg_am am ON am.oid = c.relam
		WHERE c.oid = rel AND am.amname = 'snouttime_tiered')
$$;

-- The tables a seal rewrites: a plain table is its own only leaf; a partitioned one has the
-- leaves of its tree. (pg_partition_tree returns nothing for a table that is neither a
-- partition nor partitioned, so seal() on one silently did nothing until 2026-09-23.)
CREATE FUNCTION snouttime._leaves(rel regclass) RETURNS SETOF regclass
LANGUAGE sql STABLE
SET search_path = pg_catalog, pg_temp
AS $$
	SELECT t.relid FROM pg_partition_tree(rel) t JOIN pg_class c ON c.oid = t.relid WHERE c.relkind = 'r'
	UNION
	SELECT c.oid::regclass FROM pg_class c WHERE c.oid = rel AND c.relkind = 'r'
$$;

-- Whether a seal keeps non-unique indexes whole: the series table's choice, or for a table
-- that is not a partition of one, snouttime.columnar_keep_indexes.
CREATE FUNCTION snouttime._keep_indexes(s snouttime.series) RETURNS boolean
LANGUAGE sql STABLE
AS $$
	SELECT CASE WHEN s.relid IS NULL
		THEN coalesce(current_setting('snouttime.columnar_keep_indexes', true), 'off')::boolean
		ELSE s.seal_keep_indexes END
$$;

-- Without the library in shared_preload_libraries, a session plans its first query before
-- SnoutTime's planner hooks exist, and the worker does not run at all.
CREATE FUNCTION snouttime._note_unless_preloaded() RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
BEGIN
	IF NOT snouttime._preloaded() THEN
		RAISE NOTICE 'snouttime is not in shared_preload_libraries'
			USING DETAIL = 'Without it the worker does not seal, roll up or tier anything, and a session''s first query is planned without SnoutTime''s fast paths.',
			HINT = 'Add snouttime to shared_preload_libraries in postgresql.conf and restart.';
	END IF;
END
$$;

-- Rewrites one leaf table with the given access method, the given order, codec and index
-- choice in force for that statement only.
CREATE FUNCTION snouttime._rewrite(leaf regclass, method text, order_by text, codec text, keep_indexes boolean) RETURNS void
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

-- Seals a partition (all its leaves). Returns how many tables it rewrote; 0 when every leaf
-- was sealed already. Any table can be sealed; a partition of a series table is sealed in
-- that table's order and with its codec.
CREATE FUNCTION snouttime.seal(partition regclass) RETURNS integer
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
	s snouttime.series;
	leaf regclass;
	n int := 0;
BEGIN
	IF EXISTS (SELECT 1 FROM snouttime.series WHERE relid = partition) THEN
		RAISE EXCEPTION '% is a series table; seal one of its partitions', partition
			USING HINT = 'snouttime.set_sealing() has the worker seal partitions as they age.';
	END IF;
	IF partition = snouttime._default_partition((SELECT i.inhparent FROM pg_inherits i WHERE i.inhrelid = partition)) THEN
		RAISE EXCEPTION '% is a default partition, which is never sealed: its rows are moved out, not kept', partition;
	END IF;
	PERFORM snouttime._note_unless_preloaded();
	s := snouttime._series_of(partition);
	FOR leaf IN
		SELECT l FROM snouttime._leaves(partition) l WHERE NOT snouttime._is_sealed(l)
	LOOP
		PERFORM snouttime._rewrite(leaf, 'snouttime_columnar',
			CASE WHEN s.relid IS NULL THEN current_setting('snouttime.columnar_order_by', true) ELSE snouttime._seal_order(s) END,
			CASE WHEN s.relid IS NULL THEN current_setting('snouttime.columnar_compression', true) ELSE s.seal_codec END,
			snouttime._keep_indexes(s));
		n := n + 1;
	END LOOP;
	RETURN n;
END
$$;

-- Tiering: the partition's column store goes to S3 as one object, and
-- only its metapage and directory stay here; queries read the row groups they need from S3.
-- Late writes and deletes still work (delta store, delete log). Returns tables rewritten.
CREATE FUNCTION snouttime.tier(partition regclass) RETURNS integer
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
	s snouttime.series := snouttime._series_of(partition);
	leaf regclass;
	n int := 0;
BEGIN
	IF current_setting('snouttime.tier_to', true) IS NULL OR current_setting('snouttime.tier_to', true) = '' THEN
		RAISE EXCEPTION 'tiering needs a destination: set snouttime.tier_to to s3://bucket/prefix'
			USING ERRCODE = 'object_not_in_prerequisite_state';
	END IF;
	FOR leaf IN
		SELECT l FROM snouttime._leaves(partition) l WHERE NOT snouttime._is_tiered(l)
	LOOP
		PERFORM snouttime._rewrite(leaf, 'snouttime_tiered',
			CASE WHEN s.relid IS NULL THEN current_setting('snouttime.columnar_order_by', true) ELSE snouttime._seal_order(s) END,
			CASE WHEN s.relid IS NULL THEN current_setting('snouttime.columnar_compression', true) ELSE s.seal_codec END,
			snouttime._keep_indexes(s));
		n := n + 1;
	END LOOP;
	RETURN n;
END
$$;

-- Brings a tiered partition back: a column store in the table's own pages again.
CREATE FUNCTION snouttime.recall(partition regclass) RETURNS integer
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
	s snouttime.series := snouttime._series_of(partition);
	leaf regclass;
	n int := 0;
BEGIN
	FOR leaf IN
		SELECT l FROM snouttime._leaves(partition) l WHERE snouttime._is_tiered(l)
	LOOP
		PERFORM snouttime._rewrite(leaf, 'snouttime_columnar',
			CASE WHEN s.relid IS NULL THEN current_setting('snouttime.columnar_order_by', true) ELSE snouttime._seal_order(s) END,
			CASE WHEN s.relid IS NULL THEN current_setting('snouttime.columnar_compression', true) ELSE s.seal_codec END,
			snouttime._keep_indexes(s));
		n := n + 1;
	END LOOP;
	RETURN n;
END
$$;

CREATE FUNCTION snouttime.set_tiering(relation regclass, after interval) RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
BEGIN
	IF after IS NOT NULL AND after < interval '0' THEN
		RAISE EXCEPTION 'the tiering age cannot be negative' USING ERRCODE = 'invalid_parameter_value';
	END IF;
	UPDATE snouttime.series SET tier_after = after WHERE relid = relation;
	IF NOT FOUND THEN
		RAISE EXCEPTION '% is not a series table', relation;
	END IF;
	IF after IS NULL THEN
		DELETE FROM snouttime.jobs WHERE kind = 'tier' AND target = relation;
	ELSE
		INSERT INTO snouttime.jobs (kind, target, schedule) VALUES ('tier', relation, interval '1 hour')
		ON CONFLICT (kind, target) DO NOTHING;
	END IF;
END
$$;

-- The oldest partition past its tiering age that is not wholly tiered, or NULL.
CREATE FUNCTION snouttime._next_to_tier(s snouttime.series) RETURNS regclass
LANGUAGE plpgsql STABLE
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
	cutoff text;
	def regclass := snouttime._default_partition(s.relid);
	part regclass;
	b record;
	best regclass;
	best_hi text;
BEGIN
	IF s.tier_after IS NULL THEN
		RETURN NULL;
	END IF;
	cutoff := CASE s.time_type
		WHEN 'timestamptz'::regtype THEN (now() - s.tier_after)::text
		ELSE ((now() AT TIME ZONE 'UTC') - s.tier_after)::text
	END;
	FOR part IN
		SELECT i.inhrelid FROM pg_inherits i WHERE i.inhparent = s.relid AND i.inhrelid IS DISTINCT FROM def
	LOOP
		SELECT * INTO b FROM snouttime._bounds_of(s, part);
		CONTINUE WHEN b.hi IS NULL;
		IF s.time_type = 'timestamptz'::regtype THEN
			CONTINUE WHEN b.hi::timestamptz > cutoff::timestamptz;
		ELSE
			CONTINUE WHEN b.hi::timestamp > cutoff::timestamp;
		END IF;
		CONTINUE WHEN NOT EXISTS (SELECT 1 FROM snouttime._leaves(part) l WHERE NOT snouttime._is_tiered(l));
		IF best IS NULL OR (CASE WHEN s.time_type = 'timestamptz'::regtype THEN b.hi::timestamptz < best_hi::timestamptz
				ELSE b.hi::timestamp < best_hi::timestamp END) THEN
			best := part;
			best_hi := b.hi;
		END IF;
	END LOOP;
	RETURN best;
END
$$;

-- Turns a sealed partition back into heap: the column store plus the delta store, minus
-- what was deleted. Returns how many tables it rewrote.
CREATE FUNCTION snouttime.unseal(partition regclass) RETURNS integer
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

-- Rebuilds a sealed partition's column store with its delta store folded in and its deletes
-- applied. Two rewrites: back to heap, then into a new column store.
CREATE FUNCTION snouttime.reseal(partition regclass) RETURNS integer
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
	s snouttime.series := snouttime._series_of(partition);
	leaf regclass;
	n int := 0;
BEGIN
	FOR leaf IN
		SELECT l FROM snouttime._leaves(partition) l WHERE snouttime._is_sealed(l)
	LOOP
		EXECUTE format('ALTER TABLE %s SET ACCESS METHOD heap', leaf);
		PERFORM snouttime._rewrite(leaf, 'snouttime_columnar',
			CASE WHEN s.relid IS NULL THEN current_setting('snouttime.columnar_order_by', true) ELSE snouttime._seal_order(s) END,
			CASE WHEN s.relid IS NULL THEN current_setting('snouttime.columnar_compression', true) ELSE s.seal_codec END,
			snouttime._keep_indexes(s));
		n := n + 1;
	END LOOP;
	RETURN n;
END
$$;

-- snouttime._changed_since_seal(leaf, upto), which the reseal check below asks, is in
-- src/columnar/read.rs: it reads the side tables, which SQL run as a table's owner cannot.


CREATE FUNCTION snouttime.set_sealing(relation regclass, after interval,
	codec text DEFAULT 'lz4', order_by name[] DEFAULT NULL, keep_indexes boolean DEFAULT false) RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
BEGIN
	IF after IS NOT NULL AND after < interval '0' THEN
		RAISE EXCEPTION 'the settle window cannot be negative' USING ERRCODE = 'invalid_parameter_value';
	END IF;
	IF codec NOT IN ('none', 'lz4', 'zstd') THEN
		RAISE EXCEPTION 'codec must be none, lz4 or zstd, not %', codec USING ERRCODE = 'invalid_parameter_value';
	END IF;
	UPDATE snouttime.series SET seal_after = after, seal_codec = codec, seal_order_by = order_by,
		seal_keep_indexes = keep_indexes
	WHERE relid = relation;
	IF NOT FOUND THEN
		RAISE EXCEPTION '% is not a series table', relation;
	END IF;
	PERFORM snouttime._schedule_sealing(relation, after IS NOT NULL);
END
$$;

CREATE FUNCTION snouttime.set_sealing(relation regclass, after bigint,
	codec text DEFAULT 'lz4', order_by name[] DEFAULT NULL, keep_indexes boolean DEFAULT false) RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
BEGIN
	IF after IS NOT NULL AND after < 0 THEN
		RAISE EXCEPTION 'the settle window cannot be negative' USING ERRCODE = 'invalid_parameter_value';
	END IF;
	IF codec NOT IN ('none', 'lz4', 'zstd') THEN
		RAISE EXCEPTION 'codec must be none, lz4 or zstd, not %', codec USING ERRCODE = 'invalid_parameter_value';
	END IF;
	UPDATE snouttime.series SET seal_after_width = after, seal_codec = codec, seal_order_by = order_by,
		seal_keep_indexes = keep_indexes
	WHERE relid = relation;
	IF NOT FOUND THEN
		RAISE EXCEPTION '% is not a series table', relation;
	END IF;
	PERFORM snouttime._schedule_sealing(relation, after IS NOT NULL);
END
$$;

CREATE FUNCTION snouttime._schedule_sealing(relation regclass, active boolean) RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
BEGIN
	IF active THEN
		INSERT INTO snouttime.jobs (kind, target, schedule) VALUES ('seal', relation, interval '10 minutes')
		ON CONFLICT (kind, target) DO NOTHING;
	ELSE
		DELETE FROM snouttime.jobs WHERE kind = 'seal' AND target = relation;
	END IF;
END
$$;

-- The oldest partition past its settle window that is not wholly sealed, or NULL.
CREATE FUNCTION snouttime._next_to_seal(s snouttime.series) RETURNS regclass
LANGUAGE plpgsql STABLE
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
	cutoff text;
	top int8;
	def regclass := snouttime._default_partition(s.relid);
	part regclass;
	b record;
	best regclass;
	best_hi text;
BEGIN
	IF s.seal_after IS NOT NULL THEN
		cutoff := CASE s.time_type
			WHEN 'timestamptz'::regtype THEN (now() - s.seal_after)::text
			ELSE ((now() AT TIME ZONE 'UTC') - s.seal_after)::text
		END;
	ELSIF s.seal_after_width IS NOT NULL THEN
		EXECUTE format('SELECT max(%I)::int8 FROM %s', s.time_column, s.relid) INTO top;
		IF top IS NULL THEN
			RETURN NULL;
		END IF;
		cutoff := (top - s.seal_after_width)::text;
	ELSE
		RETURN NULL;
	END IF;
	FOR part IN
		SELECT i.inhrelid FROM pg_inherits i WHERE i.inhparent = s.relid AND i.inhrelid IS DISTINCT FROM def
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
		CONTINUE WHEN NOT EXISTS (
			SELECT 1 FROM pg_partition_tree(part) t JOIN pg_class c ON c.oid = t.relid
			WHERE c.relkind = 'r' AND NOT snouttime._is_sealed(t.relid));
		IF best IS NULL OR (CASE WHEN s.partition_width IS NOT NULL THEN b.hi::int8 < best_hi::int8
				WHEN s.time_type = 'timestamptz'::regtype THEN b.hi::timestamptz < best_hi::timestamptz
				ELSE b.hi::timestamp < best_hi::timestamp END) THEN
			best := part;
			best_hi := b.hi;
		END IF;
	END LOOP;
	RETURN best;
END
$$;

-- A sealed leaf whose delta store and delete log together hold more than a tenth of its rows
-- (and at least 10,000), which the worker reseals. NULL when there is none.
CREATE FUNCTION snouttime._next_to_reseal(s snouttime.series) RETURNS regclass
LANGUAGE plpgsql STABLE
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
	leaf regclass;
	threshold int8;
BEGIN
	FOR leaf IN
		SELECT t.relid FROM pg_partition_tree(s.relid) t WHERE snouttime._is_sealed(t.relid)
	LOOP
		threshold := greatest(10000, (SELECT (greatest(reltuples, 0) / 10)::int8 FROM pg_class WHERE oid = leaf));
		IF snouttime._changed_since_seal(leaf, threshold + 1) > threshold THEN
			RETURN leaf;
		END IF;
	END LOOP;
	RETURN NULL;
END
$$;
