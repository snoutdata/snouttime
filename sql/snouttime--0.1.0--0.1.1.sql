-- SnoutTime 0.1.0 -> 0.1.1.
--
-- create_rollup refuses a group_by whose columns the select list does not return: before, it
-- built a rollup whose rows could not be told apart (group_by => 'host' with aggregates only
-- gave twenty rows an hour and no host). Rollups already made that way are left as they are.
--
-- The function below is src/rollup.sql's text, unchanged, so tests/upgrade/extension.sh finds
-- the upgraded catalog identical to a fresh 0.1.1 install.

\echo Use "ALTER EXTENSION snouttime UPDATE TO '0.1.1'" to load this file. \quit

CREATE OR REPLACE FUNCTION snouttime.create_rollup(name text, source regclass, bucket interval DEFAULT NULL,
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
