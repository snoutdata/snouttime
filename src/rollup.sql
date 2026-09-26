-- Rollups (PLAN.md Phase 4, D8, D9): an aggregate over time buckets of a series table, kept
-- materialized by refreshing only what changed, and read through a view that is never stale.
--
--   snouttime.create_rollup('cpu_hourly', 'cpu', interval '1 hour',
--       select_list => 'host, max(usage) AS max_usage, count(*) AS n', group_by => 'host')
--
-- makes three things in the source's schema:
--   cpu_hourly_materialized  one row per (bucket, group): the aggregates, as of the last refresh
--   cpu_hourly               the view: materialized buckets before the watermark, UNION ALL
--                            the same aggregate computed live over raw rows from the watermark on
--   invalidation triggers    on the source (statement-level, transition tables): every INSERT,
--                            UPDATE, DELETE and TRUNCATE logs the time range it touched, once per
--                            statement per dependent rollup, into snouttime.invalidations
--
-- A refresh (the worker's `refresh` job, or snouttime.refresh_rollup) recomputes only the
-- buckets those ranges touch, plus the buckets that have closed since the last refresh, and
-- moves the watermark to the start of the bucket still filling. Racing inserts lose nothing: an
-- invalidation a refresh did not see is not deleted by it and is picked up by the next one.
--
-- A rollup of a rollup (hourly from minutely) reads the source rollup's view, and a refresh of
-- the source passes the ranges it recomputed on to the rollups built on it. Its aggregates must
-- MERGE the source's (a sum of sums, a max of maxes, merge() of sketches): create_rollup refuses
-- the ones that do not, with the alternative.
--
-- Time keys (catalog.rs): microseconds since 2000-01-01 for time types, the value for integers.


CREATE FUNCTION snouttime._key(v text, t regtype) RETURNS int8
LANGUAGE sql IMMUTABLE
AS $$
	SELECT CASE
		WHEN t = 'timestamptz'::regtype THEN
			((extract(epoch FROM v::timestamptz) - 946684800) * 1000000)::int8
		WHEN t = 'timestamp'::regtype THEN
			((extract(epoch FROM v::timestamp) - 946684800) * 1000000)::int8
		WHEN t = 'date'::regtype THEN
			((extract(epoch FROM v::date::timestamp) - 946684800) * 1000000)::int8
		ELSE v::int8
	END
$$;

CREATE FUNCTION snouttime._key_text(k int8, t regtype) RETURNS text
LANGUAGE sql IMMUTABLE
AS $$
	SELECT CASE
		WHEN k IS NULL THEN NULL
		WHEN t = 'timestamptz'::regtype THEN
			(timestamptz '2000-01-01 00:00:00+00' + k * interval '1 microsecond')::text
		WHEN t = 'timestamp'::regtype THEN (timestamp '2000-01-01' + k * interval '1 microsecond')::text
		WHEN t = 'date'::regtype THEN (date '2000-01-01' + (k / 86400000000)::int)::text
		ELSE k::text
	END
$$;

-- The bucket expression for a rollup, over `col`: time types by interval, integers by width.
CREATE FUNCTION snouttime._bucket_sql(r snouttime.rollups, col text) RETURNS text
LANGUAGE sql IMMUTABLE
AS $$
	SELECT CASE
		WHEN r.bucket_width IS NOT NULL THEN
			format('snouttime.bucket(%s::int8, %s::int8)', r.bucket_width, col)
		ELSE format('snouttime.bucket(%L::interval, %s)', r.bucket_interval, col)
	END
$$;

-- The one end of a bucket: its start plus the width.
CREATE FUNCTION snouttime._bucket_end_sql(r snouttime.rollups, start_sql text) RETURNS text
LANGUAGE sql IMMUTABLE
AS $$
	SELECT CASE
		WHEN r.bucket_width IS NOT NULL THEN format('(%s + %s::int8)', start_sql, r.bucket_width)
		ELSE format('(%s + %L::interval)::%s', start_sql, r.bucket_interval,
			CASE WHEN r.time_type = 'date'::regtype THEN 'date' ELSE r.time_type::text END)
	END
$$;

-- The aggregate over [lo, hi) of the source, as a query. The bounds are SQL expressions
-- (NULL: open), so the view can bound it by a subquery and a refresh by literals.
CREATE FUNCTION snouttime._rollup_query(r snouttime.rollups, lo_sql text, hi_sql text) RETURNS text
LANGUAGE sql STABLE
AS $$
	SELECT format('SELECT %s AS bucket, %s FROM %s WHERE true%s%s GROUP BY 1%s',
		snouttime._bucket_sql(r, quote_ident(r.time_column)), r.select_list, r.source,
		CASE WHEN lo_sql IS NULL THEN '' ELSE format(' AND %I >= %s', r.time_column, lo_sql) END,
		CASE WHEN hi_sql IS NULL THEN '' ELSE format(' AND %I < %s', r.time_column, hi_sql) END,
		CASE WHEN r.group_by IS NULL THEN '' ELSE ', ' || r.group_by END)
$$;

CREATE FUNCTION snouttime._lit(v text, r snouttime.rollups) RETURNS text
LANGUAGE sql IMMUTABLE
AS $$ SELECT format('%L::%s', v, r.time_type) $$;

-- Invalidated time-key ranges (inclusive, sorted by lo) as whole buckets [lo, hi), merged,
-- ending before `upto`, and never before the source's retention cutoff: a bucket whose raw rows
-- retention may have dropped is never recomputed, what is materialized is all that is left.
CREATE FUNCTION snouttime._rollup_ranges(r snouttime.rollups, los int8[], his int8[], upto text)
RETURNS TABLE (lo text, hi text)
LANGUAGE plpgsql STABLE
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
	s snouttime.series;
	floor_ text;
	t text := r.time_type::text;
	i int;
	cur_lo int8;
	cur_hi int8;
	ranges int8[] := '{}';
	first_ text;
BEGIN
	IF los IS NULL OR upto IS NULL THEN
		RETURN;
	END IF;
	-- a TRUNCATE invalidated everything: from the first bucket materialized
	IF los[1] <= -9223372036854775807 THEN
		EXECUTE format('SELECT min(bucket)::text FROM %s', r.materialized) INTO first_;
		IF first_ IS NULL THEN
			RETURN;
		END IF;
		los[1] := snouttime._key(first_, r.time_type);
		his := (SELECT array_agg(least(h, snouttime._key(upto, r.time_type) - 1) ORDER BY o)
			FROM unnest(his) WITH ORDINALITY AS u(h, o));
	END IF;
	FOR i IN 1 .. array_length(los, 1) LOOP
		IF cur_lo IS NULL THEN
			cur_lo := los[i]; cur_hi := his[i];
		ELSIF los[i] <= cur_hi + 1 THEN
			cur_hi := greatest(cur_hi, his[i]);
		ELSE
			ranges := ranges || ARRAY[cur_lo, cur_hi];
			cur_lo := los[i]; cur_hi := his[i];
		END IF;
	END LOOP;
	ranges := ranges || ARRAY[cur_lo, cur_hi];

	SELECT * INTO s FROM snouttime.series WHERE relid = r.source;
	IF s.relid IS NOT NULL THEN
		floor_ := snouttime._retention_cutoff(s);
	END IF;
	FOR i IN 1 .. array_length(ranges, 1) / 2 LOOP
		EXECUTE format('SELECT %s::text, %s::text',
			snouttime._bucket_sql(r, snouttime._lit(snouttime._key_text(ranges[2 * i - 1], r.time_type), r)),
			snouttime._bucket_end_sql(r, snouttime._bucket_sql(r, snouttime._lit(snouttime._key_text(ranges[2 * i], r.time_type), r))))
			INTO lo, hi;
		IF snouttime._key(hi, r.time_type) > snouttime._key(upto, r.time_type) THEN
			hi := upto;
		END IF;
		IF floor_ IS NOT NULL AND snouttime._key(lo, r.time_type) < snouttime._key(floor_, r.time_type) THEN
			-- the first whole bucket at or after the cutoff
			EXECUTE format('SELECT CASE WHEN %1$s = %2$s THEN %1$s ELSE %3$s END::text',
				snouttime._bucket_sql(r, snouttime._lit(floor_, r)), snouttime._lit(floor_, r),
				snouttime._bucket_end_sql(r, snouttime._bucket_sql(r, snouttime._lit(floor_, r)))) INTO lo;
		END IF;
		CONTINUE WHEN snouttime._key(lo, r.time_type) >= snouttime._key(hi, r.time_type);
		RETURN NEXT;
	END LOOP;
END
$$;

-- The buckets before the watermark that have pending invalidations: the view reads these
-- live instead of from the materialized table, so a late row shows up at once.
CREATE FUNCTION snouttime._pending(mat regclass, sample anyelement)
RETURNS TABLE (lo anyelement, hi anyelement)
LANGUAGE plpgsql STABLE
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
	r snouttime.rollups;
	los int8[];
	his int8[];
	x record;
BEGIN
	SELECT * INTO r FROM snouttime.rollups WHERE materialized = mat;
	IF r.watermark IS NULL THEN
		RETURN;
	END IF;
	SELECT array_agg(i.lo ORDER BY i.lo), array_agg(i.hi ORDER BY i.lo) INTO los, his
	FROM snouttime.invalidations i WHERE i.rollup = r.relid;
	FOR x IN SELECT * FROM snouttime._rollup_ranges(r, los, his, snouttime._key_text(r.watermark, r.time_type)) LOOP
		EXECUTE format('SELECT %L::%s, %L::%s', x.lo, pg_typeof(sample), x.hi, pg_typeof(sample)) INTO lo, hi;
		RETURN NEXT;
	END LOOP;
END
$$;

-- The watermark of the rollup materialized in `mat`, as the source's time type: everything
-- before it is read from `mat`, everything from it on is computed live. Before the first
-- refresh it is minus infinity (or the smallest integer), so a new rollup is all live.
CREATE FUNCTION snouttime._watermark(mat regclass, sample anyelement) RETURNS anyelement
LANGUAGE plpgsql STABLE
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
	w text;
	result sample%TYPE;
BEGIN
	SELECT snouttime._key_text(r.watermark, r.time_type) INTO w FROM snouttime.rollups r WHERE r.materialized = mat;
	IF w IS NULL THEN
		w := CASE WHEN pg_typeof(sample) IN ('timestamptz'::regtype, 'timestamp'::regtype, 'date'::regtype)
			THEN '-infinity' WHEN pg_typeof(sample) = 'integer'::regtype THEN '-2147483648'
			WHEN pg_typeof(sample) = 'smallint'::regtype THEN '-32768' ELSE '-9223372036854775808' END;
	END IF;
	EXECUTE format('SELECT %L::%s', w, pg_typeof(sample)) INTO result;
	RETURN result;
END
$$;


-- ---- invalidation ----

-- Every rollup built on `source`, directly or through other rollups (hourly on raw, daily on
-- hourly): a write to the source invalidates them all at once, so none of them is ever stale.
-- Time keys are the same unit all the way down the chain.
CREATE FUNCTION snouttime._dependents(source regclass) RETURNS SETOF regclass
LANGUAGE sql STABLE
SET search_path = pg_catalog, pg_temp
AS $$
	WITH RECURSIVE d(relid) AS (
		SELECT r.relid FROM snouttime.rollups r WHERE r.source = _dependents.source
		UNION
		SELECT r.relid FROM snouttime.rollups r JOIN d ON r.source = d.relid
	)
	SELECT relid FROM d
$$;

-- The statement-level trigger on a rollup's source: log the time range the statement touched,
-- once per rollup built on this table. SECURITY DEFINER because the writer need not own the
-- rollups whose invalidation log this writes to; it writes nothing but ranges of this table.
CREATE FUNCTION snouttime._invalidate() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
	col text := TG_ARGV[0];
	t regtype := TG_ARGV[1]::regtype;
	lo text;
	hi text;
	lo2 text;
	hi2 text;
BEGIN
	IF TG_OP = 'TRUNCATE' THEN
		-- everything: refresh_rollup reads the extreme keys as "from the first bucket it has"
		INSERT INTO snouttime.invalidations (rollup, lo, hi)
		SELECT d, -9223372036854775807, 9223372036854775807 FROM snouttime._dependents(TG_RELID) d;
		RETURN NULL;
	END IF;
	IF TG_OP IN ('INSERT', 'UPDATE') THEN
		EXECUTE format('SELECT min(%I)::text, max(%I)::text FROM new_rows', col, col) INTO lo, hi;
	END IF;
	IF TG_OP IN ('DELETE', 'UPDATE') THEN
		EXECUTE format('SELECT min(%I)::text, max(%I)::text FROM old_rows', col, col) INTO lo2, hi2;
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

CREATE FUNCTION snouttime._install_invalidation(source regclass, col name, t regtype) RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
BEGIN
	IF EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid = source AND tgname = 'snouttime_invalidate_insert') THEN
		RETURN;
	END IF;
	EXECUTE format('CREATE TRIGGER snouttime_invalidate_insert AFTER INSERT ON %s '
		'REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION snouttime._invalidate(%L, %L)', source, col, t);
	EXECUTE format('CREATE TRIGGER snouttime_invalidate_update AFTER UPDATE ON %s '
		'REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION snouttime._invalidate(%L, %L)', source, col, t);
	EXECUTE format('CREATE TRIGGER snouttime_invalidate_delete AFTER DELETE ON %s '
		'REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION snouttime._invalidate(%L, %L)', source, col, t);
	EXECUTE format('CREATE TRIGGER snouttime_invalidate_truncate AFTER TRUNCATE ON %s '
		'FOR EACH STATEMENT EXECUTE FUNCTION snouttime._invalidate(%L, %L)', source, col, t);
END
$$;

CREATE FUNCTION snouttime._uninstall_invalidation(source regclass) RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
#variable_conflict use_variable
DECLARE
	t text;
BEGIN
	IF EXISTS (SELECT 1 FROM snouttime.rollups WHERE rollups.source = _uninstall_invalidation.source) THEN
		RETURN;
	END IF;
	FOREACH t IN ARRAY ARRAY['insert', 'update', 'delete', 'truncate'] LOOP
		EXECUTE format('DROP TRIGGER IF EXISTS %I ON %s', 'snouttime_invalidate_' || t, source);
	END LOOP;
END
$$;


-- ---- creating, refreshing, dropping ----

CREATE FUNCTION snouttime.create_rollup(name text, source regclass, bucket interval DEFAULT NULL,
	select_list text DEFAULT NULL, group_by text DEFAULT NULL, bucket_width bigint DEFAULT NULL)
RETURNS regclass
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
#variable_conflict use_variable
DECLARE
	s snouttime.series;
	src snouttime.rollups;
	r snouttime.rollups;
	nsp text;
	view_name regclass;
	mat regclass;
	vt text;
	bad text;
	item text;
	col text;
BEGIN
	IF select_list IS NULL OR btrim(select_list) = '' THEN
		RAISE EXCEPTION 'a rollup needs a select list: the aggregates to keep per bucket'
			USING ERRCODE = 'invalid_parameter_value', HINT = 'For example: max(usage) AS max_usage, count(*) AS n';
	END IF;
	SELECT n.nspname INTO nsp FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.oid = source;
	SELECT * INTO s FROM snouttime.series WHERE relid = source;
	SELECT * INTO src FROM snouttime.rollups WHERE relid = source;
	IF s.relid IS NULL AND src.relid IS NULL THEN
		RAISE EXCEPTION '% is neither a series table nor a rollup', source
			USING HINT = 'snouttime.create_series() makes a table a series table.';
	END IF;
	r.relid := NULL;
	r.source := source;
	r.select_list := select_list;
	r.group_by := group_by;
	r.bucket_interval := bucket;
	r.bucket_width := bucket_width;
	IF s.relid IS NOT NULL THEN
		r.time_column := s.time_column;
		r.time_type := s.time_type;
	ELSE
		r.time_column := 'bucket';
		r.time_type := src.time_type;
		-- a rollup of a rollup merges the source's aggregates, so each must be mergeable
		bad := substring(lower(select_list) FROM '\m(avg|percentile_cont|percentile_disc|mode|median|stddev[a-z_]*|var[a-z_]*|count\s*\(\s*distinct)\M');
		bad := regexp_replace(bad, '\s*\(\s*distinct$', '(DISTINCT)');
		IF bad IS NOT NULL THEN
			RAISE EXCEPTION 'a rollup of a rollup merges its source''s aggregates, and % of them is not % of the rows', bad, bad
				USING ERRCODE = 'invalid_parameter_value',
				HINT = CASE
					WHEN bad = 'avg' THEN 'Keep sum() and count() in the source rollup and divide: sum(total) / sum(n).'
					WHEN bad LIKE 'percentile%' OR bad IN ('mode', 'median') THEN
						'Keep snouttime.percentile_sketch() in the source rollup, then merge() it here and read snouttime.percentile().'
					WHEN bad LIKE 'count%' THEN
						'Keep snouttime.distinct_sketch() in the source rollup, then merge() it here and read snouttime.distinct_count().'
					ELSE 'Keep sum(), sum of squares and count() in the source rollup and compute it from those.'
				END;
		END IF;
	END IF;
	IF r.time_type IN ('timestamptz'::regtype, 'timestamp'::regtype, 'date'::regtype) THEN
		IF bucket IS NULL OR bucket_width IS NOT NULL THEN
			RAISE EXCEPTION 'a rollup over a % column needs a bucket interval', r.time_type USING ERRCODE = 'invalid_parameter_value';
		END IF;
	ELSIF bucket_width IS NULL OR bucket IS NOT NULL THEN
		RAISE EXCEPTION 'a rollup over an integer time column needs bucket_width' USING ERRCODE = 'invalid_parameter_value';
	END IF;

	EXECUTE format('CREATE TABLE %I.%I AS %s WITH NO DATA', nsp, name || '_materialized',
		snouttime._rollup_query(r, NULL, NULL));
	mat := format('%I.%I', nsp, name || '_materialized')::regclass;
	-- Every column group_by names must come back from the select list, or the rollup's rows
	-- cannot be told apart: group_by => 'host' with aggregates only made twenty rows an hour
	-- and no host (2026-09-24, 0.1.1). Plain names only, quoted or not; a group_by with a
	-- function call in it is left to its author, since its commas are not all separators.
	IF group_by IS NOT NULL AND strpos(group_by, '(') = 0 THEN
		FOR item IN SELECT btrim(x) FROM regexp_split_to_table(group_by, ',') AS x LOOP
			IF item ~ '^[A-Za-z_][A-Za-z0-9_$]*$' THEN
				col := lower(item);
			ELSIF item ~ '^"([^"]|"")+"$' THEN
				col := replace(substr(item, 2, length(item) - 2), '""', '"');
			ELSE
				CONTINUE;
			END IF;
			IF NOT EXISTS (SELECT 1 FROM pg_attribute
					WHERE attrelid = mat AND attname = col AND attnum > 0 AND NOT attisdropped) THEN
				RAISE EXCEPTION 'group_by names %, but the select list does not return it, so the rollup''s rows could not be told apart', item
					USING ERRCODE = 'invalid_parameter_value',
					HINT = format('Put %s in select_list too, for example: %s, %s', item, item, btrim(select_list));
			END IF;
		END LOOP;
	END IF;
	EXECUTE format('CREATE INDEX ON %s (bucket)', mat);
	r.materialized := mat;
	-- Three parts, the watermark and the pending ranges each computed once per query:
	--   materialized buckets before the watermark, less those with pending invalidations;
	--   the aggregate over raw rows from the watermark on;
	--   the aggregate over raw rows of each pending range (a late row shows up at once).
	-- So the view is never stale, only partly materialized (D8).
	vt := CASE WHEN r.bucket_width IS NOT NULL THEN 'int8' ELSE r.time_type::text END;
	EXECUTE format('CREATE VIEW %I.%I AS '
		'WITH w AS MATERIALIZED (SELECT snouttime._watermark(%L, NULL::%s) AS v), '
		'p AS MATERIALIZED (SELECT lo, hi FROM snouttime._pending(%L, NULL::%s)) '
		'SELECT m.* FROM %s m WHERE m.bucket < (SELECT v FROM w) '
		'AND NOT EXISTS (SELECT 1 FROM p WHERE m.bucket >= p.lo AND m.bucket < p.hi) '
		'UNION ALL %s '
		'UNION ALL SELECT x.* FROM p, LATERAL (%s) x',
		nsp, name, mat::text, vt, mat::text, vt, mat,
		snouttime._rollup_query(r, '(SELECT v FROM w)', NULL),
		snouttime._rollup_query(r, 'p.lo', 'p.hi'));
	view_name := format('%I.%I', nsp, name)::regclass;
	r.relid := view_name;
	INSERT INTO snouttime.rollups (relid, source, materialized, time_column, time_type, bucket_interval,
		bucket_width, select_list, group_by)
	VALUES (r.relid, r.source, r.materialized, r.time_column, r.time_type, r.bucket_interval,
		r.bucket_width, r.select_list, r.group_by);
	IF s.relid IS NOT NULL THEN
		PERFORM snouttime._install_invalidation(source, s.time_column, s.time_type);
	END IF;
	-- the whole history is due for a first refresh
	INSERT INTO snouttime.jobs (kind, target, schedule) VALUES ('refresh', view_name, interval '1 minute')
	ON CONFLICT (kind, target) DO NOTHING;
	RETURN view_name;
END
$$;

-- Brings a rollup up to date: recomputes every bucket an invalidation touched and every bucket
-- that has closed since the last refresh, and moves the watermark to the start of the bucket
-- still filling. Returns how many ranges of buckets it recomputed.
CREATE FUNCTION snouttime.refresh_rollup(rollup regclass) RETURNS integer
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
	r snouttime.rollups;
	t text;
	now_sql text;
	new_wm text;
	old_wm text;
	los int8[];
	his int8[];
	x record;
	n int := 0;
BEGIN
	SELECT * INTO r FROM snouttime.rollups WHERE relid = rollup;
	IF NOT FOUND THEN
		RAISE EXCEPTION '% is not a rollup', rollup;
	END IF;
	-- one refresh of a rollup at a time
	PERFORM pg_advisory_xact_lock(hashtext('snouttime.refresh_rollup'), rollup::oid::int);
	t := r.time_type::text;

	-- the new watermark: the start of the bucket still filling
	IF r.bucket_width IS NULL THEN
		now_sql := CASE r.time_type WHEN 'timestamptz'::regtype THEN 'now()'
			WHEN 'timestamp'::regtype THEN '(now() AT TIME ZONE ''UTC'')'
			ELSE '(now() AT TIME ZONE ''UTC'')::date' END;
		EXECUTE format('SELECT %s::text', snouttime._bucket_sql(r, now_sql)) INTO new_wm;
	ELSE
		EXECUTE format('SELECT %s::text FROM %s', snouttime._bucket_sql(r, format('max(%I)', r.time_column)), r.source)
			INTO new_wm;
		-- an empty source (a TRUNCATE) has no newest value: keep the watermark where it is, and
		-- still apply what was invalidated, which is what empties the materialized buckets
		new_wm := coalesce(new_wm, snouttime._key_text(r.watermark, r.time_type));
		IF new_wm IS NULL THEN
			RETURN 0;
		END IF;
	END IF;
	old_wm := snouttime._key_text(r.watermark, r.time_type);
	IF old_wm IS NULL THEN
		EXECUTE format('SELECT %s::text FROM %s', snouttime._bucket_sql(r, format('min(%I)', r.time_column)), r.source)
			INTO old_wm;
	END IF;

	-- what changed: the invalidations this refresh takes (one it does not see stays for the
	-- next), and the buckets that closed since the last refresh
	WITH d AS (DELETE FROM snouttime.invalidations WHERE invalidations.rollup = refresh_rollup.rollup RETURNING lo, hi)
	SELECT array_agg(lo ORDER BY lo), array_agg(hi ORDER BY lo) INTO los, his FROM d;
	IF old_wm IS NOT NULL AND snouttime._key(old_wm, r.time_type) < snouttime._key(new_wm, r.time_type) THEN
		SELECT array_agg(l ORDER BY l), array_agg(h ORDER BY l) INTO los, his
		FROM unnest(coalesce(los, '{}') || snouttime._key(old_wm, r.time_type),
			coalesce(his, '{}') || (snouttime._key(new_wm, r.time_type) - 1)) AS u(l, h);
	END IF;
	FOR x IN SELECT * FROM snouttime._rollup_ranges(r, los, his, new_wm) LOOP
		EXECUTE format('DELETE FROM %s WHERE bucket >= %s AND bucket < %s', r.materialized,
			snouttime._lit(x.lo, r), snouttime._lit(x.hi, r));
		EXECUTE format('INSERT INTO %s %s', r.materialized,
			snouttime._rollup_query(r, snouttime._lit(x.lo, r), snouttime._lit(x.hi, r)));
		-- the rollups built on this one recompute the same range
		INSERT INTO snouttime.invalidations (rollup, lo, hi)
		SELECT d.relid, snouttime._key(x.lo, d.time_type), snouttime._key(x.hi, d.time_type) - 1
		FROM snouttime.rollups d WHERE d.source = rollup;
		n := n + 1;
	END LOOP;

	UPDATE snouttime.rollups SET watermark = snouttime._key(new_wm, r.time_type)
	WHERE relid = rollup AND (watermark IS NULL OR watermark < snouttime._key(new_wm, r.time_type));

	-- a rollup's own retention, independent of its source's
	IF r.retention IS NOT NULL THEN
		EXECUTE format('DELETE FROM %s WHERE bucket < %L', r.materialized,
			CASE r.time_type WHEN 'timestamptz'::regtype THEN (now() - r.retention)::text
				ELSE ((now() AT TIME ZONE 'UTC') - r.retention)::text END);
	ELSIF r.retention_width IS NOT NULL THEN
		EXECUTE format('DELETE FROM %s WHERE bucket < %s', r.materialized,
			snouttime._key(new_wm, r.time_type) - r.retention_width);
	END IF;
	RETURN n;
END
$$;

CREATE FUNCTION snouttime.set_rollup_retention(rollup regclass, keep interval) RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
BEGIN
	IF keep IS NOT NULL AND keep <= interval '0' THEN
		RAISE EXCEPTION 'retention must be greater than zero' USING ERRCODE = 'invalid_parameter_value';
	END IF;
	UPDATE snouttime.rollups SET retention = keep WHERE relid = rollup;
	IF NOT FOUND THEN
		RAISE EXCEPTION '% is not a rollup', rollup;
	END IF;
END
$$;

CREATE FUNCTION snouttime.set_rollup_retention(rollup regclass, keep bigint) RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
BEGIN
	IF keep IS NOT NULL AND keep <= 0 THEN
		RAISE EXCEPTION 'retention must be greater than zero' USING ERRCODE = 'invalid_parameter_value';
	END IF;
	UPDATE snouttime.rollups SET retention_width = keep WHERE relid = rollup;
	IF NOT FOUND THEN
		RAISE EXCEPTION '% is not a rollup', rollup;
	END IF;
END
$$;

CREATE FUNCTION snouttime.drop_rollup(rollup regclass) RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
	r snouttime.rollups;
BEGIN
	SELECT * INTO r FROM snouttime.rollups WHERE relid = rollup;
	IF NOT FOUND THEN
		RAISE EXCEPTION '% is not a rollup', rollup;
	END IF;
	IF EXISTS (SELECT 1 FROM snouttime.rollups WHERE source = rollup) THEN
		RAISE EXCEPTION 'rollups are built on %; drop them first', rollup
			USING ERRCODE = 'dependent_objects_still_exist';
	END IF;
	DELETE FROM snouttime.invalidations WHERE invalidations.rollup = drop_rollup.rollup;
	DELETE FROM snouttime.jobs WHERE target = rollup;
	DELETE FROM snouttime.rollups WHERE relid = rollup;
	EXECUTE format('DROP VIEW %s', rollup);
	EXECUTE format('DROP TABLE %s', r.materialized);
	PERFORM snouttime._uninstall_invalidation(r.source);
END
$$;
