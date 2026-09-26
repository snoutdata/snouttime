-- Series tables (PLAN.md Phase 1.1). See series.rs for why this is SQL.
--
-- How a table becomes a series table:
--
--   1. The ORIGINAL table is renamed to <name>_default and becomes the DEFAULT partition
--      of a new partitioned table that takes the original name. No row moves, so this is
--      as fast on a billion rows as on none, and nothing is ever copied twice.
--   2. Partitions for now() and `premake` intervals ahead are created. Each one takes the
--      rows of its range out of the default partition as it is made.
--   3. Whatever older data is left in the default partition moves into proper partitions
--      one partition per transaction (`CALL snouttime.migrate(...)`, and later the worker),
--      so no lock is held for the whole copy.
--
-- Every function here runs as its CALLER (no SECURITY DEFINER): it can do only what the
-- caller could do to the table by hand, and the catalog's guard trigger enforces the same
-- rule on the registration (catalog.rs, "Who may write"). The one exception is the drop
-- cleanup at the end, which only ever deletes rows about tables that no longer exist.


-- The partition [lo, hi) that holds a value, as literals the time column's type accepts,
-- and the suffix that partition is named with.
--
-- Partitions of the time types are aligned in UTC to 2000-01-01 (date_bin's origin), or to
-- month starts for an interval made of months, so the same interval always produces the
-- same boundaries whatever the session's time zone.
CREATE FUNCTION snouttime._range_for(s snouttime.series, v text,
	OUT lo text, OUT hi text, OUT suffix text)
LANGUAGE plpgsql STABLE
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
	x int8;
	w int8;
	t timestamptz;
	b timestamptz;
	e timestamptz;
	months int;
	k int;
	fmt text;
BEGIN
	IF s.partition_width IS NOT NULL THEN
		x := v::int8;
		w := s.partition_width;
		x := x - (((x % w) + w) % w);
		lo := x::text;
		hi := (x + w)::text;
		suffix := CASE WHEN x < 0 THEN 'pm' || (-x)::text ELSE 'p' || x::text END;
		RETURN;
	END IF;

	t := CASE s.time_type
		WHEN 'timestamptz'::regtype THEN v::timestamptz
		WHEN 'timestamp'::regtype THEN v::timestamp AT TIME ZONE 'UTC'
		ELSE v::date::timestamp AT TIME ZONE 'UTC'
	END;

	months := extract(year FROM s.partition_interval)::int * 12
		+ extract(month FROM s.partition_interval)::int;
	IF months > 0 THEN
		k := (extract(year FROM t AT TIME ZONE 'UTC')::int - 2000) * 12
			+ extract(month FROM t AT TIME ZONE 'UTC')::int - 1;
		k := k - (((k % months) + months) % months);
		b := make_timestamptz(2000 + floor(k / 12.0)::int, ((k % 12) + 12) % 12 + 1, 1, 0, 0, 0, 'UTC');
		e := ((b AT TIME ZONE 'UTC') + s.partition_interval) AT TIME ZONE 'UTC';
	ELSE
		b := date_bin(s.partition_interval, t, timestamptz '2000-01-01 00:00:00+00');
		e := b + s.partition_interval;
	END IF;

	-- ISO 8601 with an explicit offset: parsed the same way whatever DateStyle says.
	IF s.time_type = 'timestamptz'::regtype THEN
		lo := to_char(b AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US') || '+00';
		hi := to_char(e AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US') || '+00';
	ELSIF s.time_type = 'timestamp'::regtype THEN
		lo := to_char(b AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US');
		hi := to_char(e AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US');
	ELSE
		lo := to_char(b AT TIME ZONE 'UTC', 'YYYY-MM-DD');
		hi := to_char(e AT TIME ZONE 'UTC', 'YYYY-MM-DD');
	END IF;

	fmt := CASE WHEN s.partition_interval < interval '1 day' THEN 'YYYYMMDD"_"HH24MISS' ELSE 'YYYYMMDD' END;
	suffix := 'p' || to_char(b AT TIME ZONE 'UTC', fmt);
END
$$;


-- The default partition of a partitioned table, or NULL.
CREATE FUNCTION snouttime._default_partition(parent regclass) RETURNS regclass
LANGUAGE sql STABLE
SET search_path = pg_catalog, pg_temp
AS $$
	SELECT nullif(partdefid, 0)::regclass FROM pg_partitioned_table WHERE partrelid = parent
$$;


-- Make sure the partition holding value `v` exists, moving that range's rows out of the
-- default partition as it is made. Returns the partition, or NULL when a partition that
-- SnoutTime did not make already covers part of the range (it is left alone).
CREATE FUNCTION snouttime._make_partition(parent regclass, v text, attach boolean DEFAULT true)
RETURNS regclass
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
	s snouttime.series;
	r record;
	nsp text;
	base text;
	owner_name text;
	part_name text;
	part regclass;
	def regclass;
	col text;
	paused name[];
	t name;
	h int;
	published boolean;
	hold text;
BEGIN
	SELECT * INTO s FROM snouttime.series WHERE relid = parent;
	IF NOT FOUND THEN
		RAISE EXCEPTION '% is not a series table', parent;
	END IF;
	SELECT * INTO r FROM snouttime._range_for(s, v);

	SELECT n.nspname, c.relname, pg_get_userbyid(c.relowner)
	INTO nsp, base, owner_name
	FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
	WHERE c.oid = parent;
	part_name := left(base, 62 - length(r.suffix)) || '_' || r.suffix;

	SELECT c.oid INTO part
	FROM pg_class c JOIN pg_inherits i ON i.inhrelid = c.oid
	WHERE i.inhparent = parent AND c.relname = part_name AND c.relnamespace = (
		SELECT relnamespace FROM pg_class WHERE oid = parent);
	IF FOUND THEN
		RETURN part;
	END IF;

	col := quote_ident(s.time_column);
	def := snouttime._default_partition(parent);

	-- Is anything replicating this table logically? It changes HOW rows are moved, below.
	published := EXISTS (
		SELECT 1 FROM pg_publication_rel pr
		WHERE pr.prrelid IN (parent, coalesce(def, parent))
	) OR EXISTS (SELECT 1 FROM pg_publication WHERE puballtables);

	EXECUTE format('CREATE TABLE %I.%I (LIKE %s INCLUDING DEFAULTS INCLUDING CONSTRAINTS '
		'INCLUDING STORAGE INCLUDING COMPRESSION INCLUDING GENERATED)%s', nsp, part_name, parent,
		CASE WHEN s.space_column IS NULL THEN ''
			ELSE format(' PARTITION BY HASH (%I)', s.space_column) END);
	part := format('%I.%I', nsp, part_name)::regclass;
	-- A space key means this partition is itself partitioned, so it needs children of its
	-- own before a row can land in it (PLAN.md 1.4).
	IF s.space_column IS NOT NULL THEN
		FOR h IN 0 .. s.space_partitions - 1 LOOP
			EXECUTE format('CREATE TABLE %I.%I PARTITION OF %s '
				'FOR VALUES WITH (MODULUS %s, REMAINDER %s)',
				nsp, left(part_name, 58) || '_h' || h, part, s.space_partitions, h);
			EXECUTE format('ALTER TABLE %I.%I OWNER TO %I', nsp, left(part_name, 58) || '_h' || h,
				owner_name);
		END LOOP;
	END IF;
	-- With this constraint in place ATTACH does not have to scan the new partition.
	EXECUTE format('ALTER TABLE %s ADD CONSTRAINT snouttime_bounds CHECK (%s >= %L AND %s < %L)',
		part, col, r.lo, col, r.hi);
	IF def IS NOT NULL THEN
		-- Moving a row is not deleting it and inserting a new one, so the user's triggers
		-- must not see it: an audit trigger would log every migrated row as a deletion.
		-- The new partition has no triggers until it is attached; the default partition's
		-- enabled user triggers are switched off for the move and back on after it. Both
		-- ALTERs lock out concurrent writers to the default partition until commit, so no
		-- other session's write can slip through while they are off.
		SELECT array_agg(tgname) INTO paused FROM pg_trigger
		WHERE tgrelid = def AND NOT tgisinternal AND tgenabled <> 'D';
		IF paused IS NOT NULL THEN
			FOREACH t IN ARRAY paused LOOP
				EXECUTE format('ALTER TABLE %s DISABLE TRIGGER %I', def, t);
			END LOOP;
		END IF;
		IF published THEN
			-- A logical subscriber must end up with the same rows, and the straight move
			-- loses them: the new partition is not attached yet, so it is not part of the
			-- publication, and its inserts are never decoded while the deletes from the
			-- default partition are. The subscriber ends up short exactly the rows that
			-- moved (found by tests/worker/logical_replication.sh, 2026-09-22: 73 rows
			-- became 19).
			--
			-- So when something IS replicating this table, the rows go out to an unlogged
			-- holding table first, the partition is attached empty, and they come back in
			-- THROUGH THE PARENT, which is what the publication names. It costs one extra
			-- copy of the batch, which is why it is not the path for everyone else.
			hold := left(part_name, 57) || '_hold';
			EXECUTE format('CREATE UNLOGGED TABLE %I.%I (LIKE %s)', nsp, hold, parent);
			EXECUTE format('WITH moved AS (DELETE FROM %s WHERE %s >= %L AND %s < %L RETURNING *) '
				'INSERT INTO %I.%I SELECT * FROM moved', def, col, r.lo, col, r.hi, nsp, hold);
		ELSE
			EXECUTE format('WITH moved AS (DELETE FROM %s WHERE %s >= %L AND %s < %L RETURNING *) '
				'INSERT INTO %s SELECT * FROM moved', def, col, r.lo, col, r.hi, part);
		END IF;
		IF paused IS NOT NULL THEN
			FOREACH t IN ARRAY paused LOOP
				EXECUTE format('ALTER TABLE %s ENABLE TRIGGER %I', def, t);
			END LOOP;
		END IF;
	END IF;
	IF NOT attach THEN
		-- _migrate_batch attaches it, with others, after one scan of the default partition.
		-- The published path cannot be left half done like this, so it never asks.
		IF published THEN
			RAISE EXCEPTION 'internal: a replicated table''s partition cannot be left unattached';
		END IF;
		EXECUTE format('ALTER TABLE %s OWNER TO %I', part, owner_name);
		RETURN part;
	END IF;
	BEGIN
		EXECUTE format('ALTER TABLE %s ATTACH PARTITION %s FOR VALUES FROM (%L) TO (%L)',
			parent, part, r.lo, r.hi);
	EXCEPTION WHEN invalid_object_definition THEN
		-- "would overlap partition": somebody else's partition already covers this range.
		-- Put the rows back where they came from and leave that partition alone.
		IF def IS NOT NULL THEN
			IF published THEN
				EXECUTE format('INSERT INTO %s SELECT * FROM %I.%I', def, nsp, hold);
				EXECUTE format('DROP TABLE %I.%I', nsp, hold);
			ELSE
				EXECUTE format('INSERT INTO %s SELECT * FROM %s', def, part);
			END IF;
		END IF;
		EXECUTE format('DROP TABLE %s', part);
		RETURN NULL;
	END;
	EXECUTE format('ALTER TABLE %s DROP CONSTRAINT snouttime_bounds', part);
	EXECUTE format('ALTER TABLE %s OWNER TO %I', part, owner_name);

	IF published AND hold IS NOT NULL THEN
		-- Back in through the parent, so the rows are published as the table the
		-- subscriber knows. The partition's own triggers (cloned from the parent when it
		-- was attached) are off for this, for the same reason as above: this is a move.
		SELECT array_agg(tgname) INTO paused FROM pg_trigger
		WHERE tgrelid = part AND NOT tgisinternal AND tgenabled <> 'D';
		IF paused IS NOT NULL THEN
			FOREACH t IN ARRAY paused LOOP
				EXECUTE format('ALTER TABLE %s DISABLE TRIGGER %I', part, t);
			END LOOP;
		END IF;
		EXECUTE format('INSERT INTO %s SELECT * FROM %I.%I', parent, nsp, hold);
		IF paused IS NOT NULL THEN
			FOREACH t IN ARRAY paused LOOP
				EXECUTE format('ALTER TABLE %s ENABLE TRIGGER %I', part, t);
			END LOOP;
		END IF;
		EXECUTE format('DROP TABLE %I.%I', nsp, hold);
	END IF;

	RETURN part;
END
$$;


-- Make the partition holding now() and `premake` more after it. For an integer time
-- column there is no "now", so it counts ahead from the largest value in the table, and
-- does nothing on an empty one. Returns how many partitions it created.
CREATE FUNCTION snouttime.premake(relation regclass) RETURNS integer
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
	s snouttime.series;
	before int;
	anchor text;
	top int8;
	i int;
BEGIN
	SELECT * INTO s FROM snouttime.series WHERE relid = relation;
	IF NOT FOUND THEN
		RAISE EXCEPTION '% is not a series table', relation;
	END IF;
	SELECT count(*) INTO before FROM pg_inherits WHERE inhparent = relation;

	IF s.partition_width IS NOT NULL THEN
		EXECUTE format('SELECT max(%I)::int8 FROM %s', s.time_column, relation) INTO top;
		IF top IS NULL THEN
			RETURN 0;
		END IF;
		FOR i IN 0 .. s.premake LOOP
			PERFORM snouttime._make_partition(relation, (top + i * s.partition_width)::text);
		END LOOP;
	ELSE
		FOR i IN 0 .. s.premake LOOP
			anchor := CASE s.time_type
				WHEN 'timestamptz'::regtype THEN (now() + i * s.partition_interval)::text
				ELSE ((now() AT TIME ZONE 'UTC') + i * s.partition_interval)::text
			END;
			PERFORM snouttime._make_partition(relation, anchor);
		END LOOP;
	END IF;

	RETURN (SELECT count(*) FROM pg_inherits WHERE inhparent = relation) - before;
END
$$;


-- Make every partition covering [lo, hi), for loading data that is not from today.
--
-- `premake` only ever looks ahead of now(), which is right for a table being written to
-- and wrong for a bulk load of last year's data: without this, every row lands in the
-- default partition and is migrated out afterwards, one partition at a time. Values are
-- given as text and read as the time column's own type. Returns how many it made.
CREATE FUNCTION snouttime.make_partitions(relation regclass, lo text, hi text) RETURNS integer
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
	s snouttime.series;
	before int;
	r record;
	cursor_ text;
	guard int := 0;
BEGIN
	SELECT * INTO s FROM snouttime.series WHERE relid = relation;
	IF NOT FOUND THEN
		RAISE EXCEPTION '% is not a series table', relation;
	END IF;
	SELECT count(*) INTO before FROM pg_inherits WHERE inhparent = relation;

	cursor_ := lo;
	LOOP
		SELECT * INTO r FROM snouttime._range_for(s, cursor_);
		-- Past the end? The range holding `hi` is not included: [lo, hi).
		IF s.partition_width IS NOT NULL THEN
			EXIT WHEN r.lo::int8 >= hi::int8;
		ELSIF s.time_type = 'timestamptz'::regtype THEN
			EXIT WHEN r.lo::timestamptz >= hi::timestamptz;
		ELSE
			EXIT WHEN r.lo::timestamp >= hi::timestamp;
		END IF;

		PERFORM snouttime._make_partition(relation, cursor_);
		cursor_ := r.hi;

		guard := guard + 1;
		IF guard > 100000 THEN
			RAISE EXCEPTION 'make_partitions: more than 100000 partitions between % and %', lo, hi
				USING HINT = 'That is almost always the wrong partition_interval rather than the intent.';
		END IF;
	END LOOP;

	RETURN (SELECT count(*) FROM pg_inherits WHERE inhparent = relation) - before;
END
$$;


-- The jobs every series table wants doing: partitions made ahead of time, and, while its
-- default partition still holds rows, one partition's worth moved out per run. Both are
-- idempotent, and `jobs.sql` is what runs them.
CREATE FUNCTION snouttime._ensure_jobs(relation regclass) RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
	s snouttime.series;
	every interval;
	def regclass;
	rows_left boolean;
BEGIN
	SELECT * INTO s FROM snouttime.series WHERE relid = relation;
	IF NOT FOUND THEN
		RAISE EXCEPTION '% is not a series table', relation;
	END IF;
	-- Often enough that a partition is never missing, rarely enough to cost nothing: a
	-- tenth of the partition interval, between a minute and an hour.
	IF s.partition_interval IS NULL THEN
		every := interval '1 hour';
	ELSE
		every := greatest(interval '1 minute', least(interval '1 hour', s.partition_interval / 10));
	END IF;
	INSERT INTO snouttime.jobs (kind, target, schedule) VALUES ('premake', relation, every)
	ON CONFLICT (kind, target) DO UPDATE SET schedule = excluded.schedule;

	def := snouttime._default_partition(relation);
	rows_left := false;
	IF def IS NOT NULL THEN
		EXECUTE format('SELECT EXISTS (SELECT 1 FROM %s)', def) INTO rows_left;
	END IF;
	IF rows_left THEN
		INSERT INTO snouttime.jobs (kind, target, schedule) VALUES ('migrate', relation, interval '1 minute')
		ON CONFLICT (kind, target) DO NOTHING;
	END IF;
END
$$;


CREATE FUNCTION snouttime.create_series(
	relation regclass,
	time_column name,
	partition_interval interval DEFAULT NULL,
	partition_width bigint DEFAULT NULL,
	premake integer DEFAULT 4,
	space_column name DEFAULT NULL,
	space_partitions integer DEFAULT NULL
) RETURNS regclass
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
	c pg_class;
	att pg_attribute;
	space_att pg_attribute;
	typ regtype;
	months int;
	nsp text;
	rel_name text;
	owner_name text;
	default_name text;
	parent regclass;
	def regclass;
	found_list text;
	has_nulls boolean;
	fk_defs text[];
	trigger_defs text[];
	index_names text[];
	index_oids oid[];
	new_index oid;
	new_index_name text;
	i int;
	stmt text;
	rec record;
	left_over int8;
BEGIN
	PERFORM snouttime._note_unless_preloaded();
	IF NOT coalesce(snouttime._may_manage(relation), false) THEN
		RAISE EXCEPTION 'permission denied: only the owner of % may make it a series table', relation
			USING ERRCODE = 'insufficient_privilege';
	END IF;
	EXECUTE format('LOCK TABLE %s IN ACCESS EXCLUSIVE MODE', relation);

	IF EXISTS (SELECT 1 FROM snouttime.series s WHERE s.relid = relation) THEN
		RAISE EXCEPTION '% is already a series table', relation;
	END IF;

	SELECT * INTO c FROM pg_class WHERE oid = relation;
	SELECT n.nspname INTO nsp FROM pg_namespace n WHERE n.oid = c.relnamespace;
	rel_name := c.relname;
	owner_name := pg_get_userbyid(c.relowner);

	-- ---- the time column and the partition size ----

	SELECT * INTO att FROM pg_attribute a
	WHERE a.attrelid = relation AND a.attname = time_column AND a.attnum > 0 AND NOT a.attisdropped;
	IF NOT FOUND THEN
		RAISE EXCEPTION '% has no column named %', relation, time_column
			USING ERRCODE = 'undefined_column';
	END IF;
	typ := att.atttypid::regtype;
	IF typ NOT IN ('timestamptz'::regtype, 'timestamp'::regtype, 'date'::regtype,
		'smallint'::regtype, 'integer'::regtype, 'bigint'::regtype) THEN
		RAISE EXCEPTION 'column % of % is %, and a series table needs a timestamptz, timestamp, date or integer time column',
			time_column, relation, typ
			USING ERRCODE = 'datatype_mismatch';
	END IF;

	IF typ IN ('smallint'::regtype, 'integer'::regtype, 'bigint'::regtype) THEN
		IF partition_width IS NULL OR partition_interval IS NOT NULL THEN
			RAISE EXCEPTION 'an integer time column is partitioned by partition_width, a number of units per partition'
				USING ERRCODE = 'invalid_parameter_value';
		END IF;
		IF partition_width <= 0 THEN
			RAISE EXCEPTION 'partition_width must be greater than zero'
				USING ERRCODE = 'invalid_parameter_value';
		END IF;
	ELSE
		IF partition_interval IS NULL OR partition_width IS NOT NULL THEN
			RAISE EXCEPTION 'a % time column is partitioned by partition_interval, an interval such as ''1 day''', typ
				USING ERRCODE = 'invalid_parameter_value';
		END IF;
		IF partition_interval <= interval '0' THEN
			RAISE EXCEPTION 'partition_interval must be greater than zero'
				USING ERRCODE = 'invalid_parameter_value';
		END IF;
		months := extract(year FROM partition_interval)::int * 12 + extract(month FROM partition_interval)::int;
		IF months > 0 AND partition_interval <> make_interval(months => months) THEN
			RAISE EXCEPTION 'partition_interval % mixes months with days or hours; use one or the other', partition_interval
				USING ERRCODE = 'invalid_parameter_value';
		END IF;
		IF typ = 'date'::regtype AND months = 0
			AND extract(epoch FROM partition_interval)::numeric % 86400 <> 0 THEN
			RAISE EXCEPTION 'a date time column needs a partition_interval of whole days'
				USING ERRCODE = 'invalid_parameter_value';
		END IF;
	END IF;
	IF premake IS NULL OR premake < 1 OR premake > 1000 THEN
		RAISE EXCEPTION 'premake must be between 1 and 1000'
			USING ERRCODE = 'invalid_parameter_value';
	END IF;

	-- The optional space key (PLAN.md 1.4): each time partition is hash-partitioned on it,
	-- which spreads one interval's writes and indexes over several tables.
	IF (space_column IS NULL) <> (space_partitions IS NULL) THEN
		RAISE EXCEPTION 'a space key needs both space_column and space_partitions'
			USING ERRCODE = 'invalid_parameter_value';
	END IF;
	IF space_column IS NOT NULL THEN
		SELECT * INTO space_att FROM pg_attribute a
		WHERE a.attrelid = relation AND a.attname = space_column AND a.attnum > 0
			AND NOT a.attisdropped;
		IF NOT FOUND THEN
			RAISE EXCEPTION '% has no column named %', relation, space_column
				USING ERRCODE = 'undefined_column';
		END IF;
		IF space_column = time_column THEN
			RAISE EXCEPTION 'the space key must be a different column from the time column'
				USING ERRCODE = 'invalid_parameter_value';
		END IF;
		IF space_partitions < 2 OR space_partitions > 1024 THEN
			RAISE EXCEPTION 'space_partitions must be between 2 and 1024'
				USING ERRCODE = 'invalid_parameter_value';
		END IF;
	END IF;

	-- ---- an already partitioned table is adopted, not converted ----

	IF c.relkind = 'p' THEN
		IF NOT EXISTS (
			SELECT 1 FROM pg_partitioned_table pt
			WHERE pt.partrelid = relation AND pt.partstrat = 'r' AND pt.partnatts = 1
				AND pt.partattrs[0] = att.attnum
		) THEN
			RAISE EXCEPTION '% is already partitioned, but not by range on % alone', relation, time_column
				USING ERRCODE = 'wrong_object_type';
		END IF;
		INSERT INTO snouttime.series (relid, time_column, time_type, partition_interval,
			partition_width, premake, space_column, space_partitions)
		VALUES (relation, time_column, typ, partition_interval, partition_width, premake,
			space_column, space_partitions);
		PERFORM snouttime.premake(relation);
		PERFORM snouttime._ensure_jobs(relation);
		RETURN relation;
	END IF;

	-- ---- what cannot be carried over, refused before anything changes ----

	IF c.relkind <> 'r' THEN
		RAISE EXCEPTION '% is not a table', relation USING ERRCODE = 'wrong_object_type';
	END IF;
	IF c.relpersistence = 't' THEN
		RAISE EXCEPTION 'a temporary table cannot be a series table' USING ERRCODE = 'wrong_object_type';
	END IF;
	IF c.relpersistence = 'u' THEN
		RAISE EXCEPTION '% is unlogged, and Postgres does not allow an unlogged partitioned table', relation
			USING ERRCODE = 'wrong_object_type';
	END IF;
	IF c.reloftype <> 0 THEN
		RAISE EXCEPTION '% is a typed table, which cannot become a partitioned table', relation
			USING ERRCODE = 'wrong_object_type';
	END IF;
	IF c.relhassubclass OR EXISTS (SELECT 1 FROM pg_inherits WHERE inhrelid = relation) THEN
		RAISE EXCEPTION '% takes part in table inheritance, which a series table cannot', relation
			USING ERRCODE = 'wrong_object_type';
	END IF;

	SELECT string_agg(DISTINCT v.oid::regclass::text, ', ') INTO found_list
	FROM pg_depend d
	JOIN pg_rewrite rw ON rw.oid = d.objid
	JOIN pg_class v ON v.oid = rw.ev_class
	WHERE d.classid = 'pg_rewrite'::regclass AND d.refobjid = relation AND v.oid <> relation;
	IF found_list IS NOT NULL THEN
		RAISE EXCEPTION '% is used by % and they would go on reading the old table', relation, found_list
			USING ERRCODE = 'dependent_objects_still_exist',
			HINT = 'Drop them, run create_series, then create them again.';
	END IF;

	SELECT string_agg(DISTINCT conrelid::regclass::text, ', ') INTO found_list
	FROM pg_constraint WHERE confrelid = relation AND contype = 'f';
	IF found_list IS NOT NULL THEN
		RAISE EXCEPTION 'foreign keys in % point at %, and they would go on pointing at the old table', found_list, relation
			USING ERRCODE = 'dependent_objects_still_exist',
			HINT = 'Drop those foreign keys, run create_series, then add them again.';
	END IF;

	IF EXISTS (SELECT 1 FROM pg_publication_rel WHERE prrelid = relation) THEN
		RAISE EXCEPTION '% is in a publication; remove it, run create_series, then add the new table with publish_via_partition_root', relation
			USING ERRCODE = 'feature_not_supported';
	END IF;
	IF c.relrowsecurity THEN
		RAISE EXCEPTION '% has row-level security, which create_series does not carry over yet', relation
			USING ERRCODE = 'feature_not_supported';
	END IF;
	IF c.relreplident = 'i' THEN
		RAISE EXCEPTION '% uses an index as its replica identity, which create_series does not carry over yet', relation
			USING ERRCODE = 'feature_not_supported';
	END IF;

	SELECT string_agg(quote_ident(a.attname), ', ') INTO found_list
	FROM pg_attribute a
	WHERE a.attrelid = relation AND a.attnum > 0 AND NOT a.attisdropped AND a.attidentity <> '';
	IF found_list IS NOT NULL THEN
		RAISE EXCEPTION 'identity column % cannot move into a partitioned table yet', found_list
			USING ERRCODE = 'feature_not_supported',
			HINT = 'A bigserial or a DEFAULT nextval(...) works.';
	END IF;

	SELECT string_agg(i.indexrelid::regclass::text, ', ') INTO found_list
	FROM pg_index i
	WHERE i.indrelid = relation AND i.indisunique AND NOT (att.attnum = ANY (i.indkey::int2[]));
	IF found_list IS NOT NULL THEN
		RAISE EXCEPTION 'unique index % does not include %', found_list, time_column
			USING ERRCODE = 'invalid_table_definition',
			HINT = 'Postgres requires every unique index on a partitioned table to include the partition column.';
	END IF;
	IF space_column IS NOT NULL THEN
		-- The same rule applies again one level down, where the key is the space column.
		SELECT string_agg(i.indexrelid::regclass::text, ', ') INTO found_list
		FROM pg_index i
		WHERE i.indrelid = relation AND i.indisunique
			AND NOT (space_att.attnum = ANY (i.indkey::int2[]));
		IF found_list IS NOT NULL THEN
			RAISE EXCEPTION 'unique index % does not include the space key %', found_list, space_column
				USING ERRCODE = 'invalid_table_definition',
				HINT = 'Every unique index has to include both partition columns.';
		END IF;
	END IF;

	IF NOT att.attnotnull THEN
		EXECUTE format('SELECT EXISTS (SELECT 1 FROM %s WHERE %I IS NULL)', relation, time_column) INTO has_nulls;
		IF has_nulls THEN
			RAISE EXCEPTION '% has rows where % is NULL, and a series table needs a time on every row', relation, time_column
				USING ERRCODE = 'not_null_violation';
		END IF;
		EXECUTE format('ALTER TABLE %s ALTER COLUMN %I SET NOT NULL', relation, time_column);
	END IF;

	-- ---- capture what LIKE does not copy, then take it off the old table ----

	-- Foreign keys this table holds, and its own triggers. Their definitions are written
	-- with the table's NAME, which the new partitioned table is about to take over, so
	-- running them again later puts them on the new table.
	SELECT array_agg(format('ALTER TABLE %I.%I ADD CONSTRAINT %I %s', nsp, rel_name, conname,
		pg_get_constraintdef(oid)) ORDER BY conname)
	INTO fk_defs
	FROM pg_constraint WHERE conrelid = relation AND contype = 'f';
	SELECT array_agg(pg_get_triggerdef(oid) ORDER BY tgname)
	INTO trigger_defs
	FROM pg_trigger WHERE tgrelid = relation AND NOT tgisinternal;

	FOR rec IN SELECT conname FROM pg_constraint WHERE conrelid = relation AND contype = 'f' LOOP
		EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I', relation, rec.conname);
	END LOOP;
	FOR rec IN SELECT tgname FROM pg_trigger WHERE tgrelid = relation AND NOT tgisinternal LOOP
		EXECUTE format('DROP TRIGGER %I ON %s', rec.tgname, relation);
	END LOOP;

	-- The old table's index names, so the new table's matching indexes can take them.
	-- The old ones are renamed out of the way first, or LIKE would invent `..._pkey1`.
	-- Both arrays are in the same order, and an index keeps its OID through a rename, so
	-- after the attach each old index can say what its new parent index should be called.
	SELECT array_agg(c2.oid ORDER BY c2.oid), array_agg(c2.relname ORDER BY c2.oid)
	INTO index_oids, index_names
	FROM pg_index i JOIN pg_class c2 ON c2.oid = i.indexrelid WHERE i.indrelid = relation;
	IF index_oids IS NOT NULL THEN
		FOR i IN 1 .. array_length(index_oids, 1) LOOP
			EXECUTE format('ALTER INDEX %s RENAME TO %I', index_oids[i]::regclass,
				left(index_names[i], 52) || '_' || substr(md5(index_names[i]), 1, 10));
		END LOOP;
	END IF;

	-- ---- the conversion ----

	default_name := left(rel_name, 55) || '_default';
	EXECUTE format('ALTER TABLE %s RENAME TO %I', relation, default_name);
	def := relation;

	EXECUTE format('CREATE TABLE %I.%I (LIKE %s INCLUDING ALL) PARTITION BY RANGE (%I)',
		nsp, rel_name, def, time_column);
	parent := format('%I.%I', nsp, rel_name)::regclass;
	EXECUTE format('ALTER TABLE %s OWNER TO %I', parent, owner_name);
	EXECUTE format('ALTER TABLE %s ATTACH PARTITION %s DEFAULT', parent, def);

	-- Each old index is now attached to the matching new one; give the new one its name
	-- back, unless LIKE already gave it exactly that name.
	IF index_oids IS NOT NULL THEN
		FOR i IN 1 .. array_length(index_oids, 1) LOOP
			SELECT pi.inhparent, c2.relname INTO new_index, new_index_name
			FROM pg_inherits pi JOIN pg_class c2 ON c2.oid = pi.inhparent
			WHERE pi.inhrelid = index_oids[i];
			IF FOUND AND new_index_name <> index_names[i] THEN
				EXECUTE format('ALTER INDEX %s RENAME TO %I', new_index::regclass, index_names[i]);
			END IF;
		END LOOP;
	END IF;

	-- Comment, privileges and replica identity, which LIKE does not carry.
	IF obj_description(def, 'pg_class') IS NOT NULL THEN
		EXECUTE format('COMMENT ON TABLE %s IS %L', parent, obj_description(def, 'pg_class'));
	END IF;
	FOR rec IN
		SELECT a.privilege_type, a.is_grantable,
			CASE WHEN a.grantee = 0 THEN 'PUBLIC' ELSE quote_ident(pg_get_userbyid(a.grantee)) END AS grantee
		FROM pg_class cc, aclexplode(cc.relacl) a
		WHERE cc.oid = def AND a.grantee <> cc.relowner
	LOOP
		EXECUTE format('GRANT %s ON %s TO %s%s', rec.privilege_type, parent, rec.grantee,
			CASE WHEN rec.is_grantable THEN ' WITH GRANT OPTION' ELSE '' END);
	END LOOP;
	FOR rec IN
		SELECT at.attname, a.privilege_type, a.is_grantable,
			CASE WHEN a.grantee = 0 THEN 'PUBLIC' ELSE quote_ident(pg_get_userbyid(a.grantee)) END AS grantee
		FROM pg_attribute at, aclexplode(at.attacl) a
		WHERE at.attrelid = def AND at.attnum > 0 AND NOT at.attisdropped AND at.attacl IS NOT NULL
	LOOP
		EXECUTE format('GRANT %s (%I) ON %s TO %s%s', rec.privilege_type, rec.attname, parent, rec.grantee,
			CASE WHEN rec.is_grantable THEN ' WITH GRANT OPTION' ELSE '' END);
	END LOOP;
	IF c.relreplident = 'f' THEN
		EXECUTE format('ALTER TABLE %s REPLICA IDENTITY FULL', parent);
	ELSIF c.relreplident = 'n' THEN
		EXECUTE format('ALTER TABLE %s REPLICA IDENTITY NOTHING', parent);
	END IF;

	-- Triggers and foreign keys, now on the new table (and, for row triggers and foreign
	-- keys, cloned by Postgres onto every partition including the old table).
	IF trigger_defs IS NOT NULL THEN
		FOREACH stmt IN ARRAY trigger_defs LOOP
			EXECUTE stmt;
		END LOOP;
	END IF;
	IF fk_defs IS NOT NULL THEN
		FOREACH stmt IN ARRAY fk_defs LOOP
			EXECUTE stmt;
		END LOOP;
	END IF;

	INSERT INTO snouttime.series (relid, time_column, time_type, partition_interval,
		partition_width, premake, space_column, space_partitions)
	VALUES (parent, time_column, typ, partition_interval, partition_width, premake,
		space_column, space_partitions);
	PERFORM snouttime.premake(parent);
	PERFORM snouttime._ensure_jobs(parent);

	EXECUTE format('SELECT count(*) FROM %s', def) INTO left_over;
	IF left_over > 0 THEN
		-- RAISE has one placeholder, %, and no %L: the literal is quoted here instead.
		RAISE NOTICE '% rows outside the premade partitions are in %; the worker moves them a partition at a time, or CALL snouttime.migrate(%) moves them now',
			left_over, def, quote_literal(parent::text);
	END IF;
	RETURN parent;
END
$$;


-- How many ranges one migration transaction moves (PLAN.md 1.2, option C, chosen
-- 2026-09-23): enough that the whole default partition is moved in about eight
-- transactions, whatever its span. Estimated from the oldest and newest row, which the
-- time index answers without a scan.
CREATE FUNCTION snouttime._migrate_width(s snouttime.series, oldest text, newest text)
RETURNS integer
LANGUAGE plpgsql STABLE
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
	ranges numeric;
BEGIN
	IF s.partition_width IS NOT NULL THEN
		ranges := (newest::numeric - oldest::numeric) / s.partition_width + 1;
	ELSE
		ranges := extract(epoch FROM newest::timestamptz - oldest::timestamptz)
			/ greatest(extract(epoch FROM s.partition_interval), 1) + 1;
	END IF;
	RETURN greatest(1, ceil(ranges / 8))::int;
END
$$;


-- Move the rows of up to `max_ranges` partition ranges out of the default partition, in
-- the caller's transaction (PLAN.md 1.2, option C).
--
-- Attaching a partition to a table that has a default partition makes Postgres scan the
-- default partition, to prove no row there belongs to the new range, unless a VALIDATED
-- constraint on it already says so. Attached one per transaction, N partitions cost N
-- scans of the original table (moved rows stay behind as dead tuples until vacuum):
-- measured at 4.2x the cost of the copy itself for 278 hourly partitions. So the rows of
-- several ranges are moved first, ONE constraint excluding all of them is added to the
-- default partition (one scan), every partition is attached without a scan, and the
-- constraint is dropped again. No row is ever invisible to a query on the table.
--
-- Returns how many partitions were attached, and the first value it could not place,
-- when a partition SnoutTime did not make covers it.
CREATE FUNCTION snouttime._migrate_batch(parent regclass, max_ranges integer DEFAULT NULL,
	OUT moved integer, OUT stopped_at text)
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
	s snouttime.series;
	def regclass;
	col text;
	oldest text;
	newest text;
	k int;
	r record;
	part regclass;
	parts regclass[] := '{}';
	los text[] := '{}';
	his text[] := '{}';
	failed regclass[] := '{}';
	cond text;
	i int;
BEGIN
	moved := 0;
	SELECT * INTO s FROM snouttime.series WHERE relid = parent;
	def := snouttime._default_partition(parent);
	IF def IS NULL THEN
		RETURN;
	END IF;
	col := quote_ident(s.time_column);
	EXECUTE format('SELECT min(%s)::text, max(%s)::text FROM %s', col, col, def) INTO oldest, newest;
	IF oldest IS NULL THEN
		RETURN;
	END IF;

	-- A replicated table moves one range at a time through the parent (see _make_partition).
	IF EXISTS (SELECT 1 FROM pg_publication_rel pr WHERE pr.prrelid IN (parent, def))
		OR EXISTS (SELECT 1 FROM pg_publication WHERE puballtables) THEN
		part := snouttime._make_partition(parent, oldest);
		IF part IS NULL THEN
			stopped_at := oldest;
		ELSE
			moved := 1;
		END IF;
		RETURN;
	END IF;

	k := coalesce(max_ranges, snouttime._migrate_width(s, oldest, newest));
	WHILE oldest IS NOT NULL AND coalesce(array_length(parts, 1), 0) < k LOOP
		SELECT * INTO r FROM snouttime._range_for(s, oldest);
		part := snouttime._make_partition(parent, oldest, attach => false);
		IF EXISTS (SELECT 1 FROM pg_inherits WHERE inhrelid = part) THEN
			-- a partition by this name is already attached: nothing of ours to move
			EXIT;
		END IF;
		parts := parts || part;
		los := los || r.lo;
		his := his || r.hi;
		EXECUTE format('SELECT min(%s)::text FROM %s', col, def) INTO oldest;
	END LOOP;
	IF coalesce(array_length(parts, 1), 0) = 0 THEN
		RETURN;
	END IF;

	SELECT string_agg(format('(%s < %L OR %s >= %L)', col, los[j], col, his[j]), ' AND ')
	INTO cond FROM generate_subscripts(los, 1) AS j;
	EXECUTE format('ALTER TABLE %s ADD CONSTRAINT snouttime_moving CHECK (%s)', def, cond);
	FOR i IN 1 .. array_length(parts, 1) LOOP
		BEGIN
			EXECUTE format('ALTER TABLE %s ATTACH PARTITION %s FOR VALUES FROM (%L) TO (%L)',
				parent, parts[i], los[i], his[i]);
			EXECUTE format('ALTER TABLE %s DROP CONSTRAINT snouttime_bounds', parts[i]);
			moved := moved + 1;
		EXCEPTION WHEN invalid_object_definition THEN
			-- "would overlap partition": somebody else's partition covers this range
			failed := failed || parts[i];
			stopped_at := coalesce(stopped_at, los[i]);
		END;
	END LOOP;
	EXECUTE format('ALTER TABLE %s DROP CONSTRAINT snouttime_moving', def);
	-- Rows that could not be placed go back where they came from.
	FOREACH part IN ARRAY failed LOOP
		EXECUTE format('INSERT INTO %s SELECT * FROM %s', def, part);
		EXECUTE format('DROP TABLE %s', part);
	END LOOP;
END
$$;


-- Move rows out of a series table's default partition into proper partitions, oldest
-- first, several partitions per transaction (_migrate_batch), so the whole move is about
-- eight transactions and no lock is held for all of it.
-- `batches` limits how many transactions it runs this call (NULL: until the default is empty).
--
-- Call it outside an explicit transaction block, or it cannot commit between batches. It
-- has no SET search_path, unlike everything else here, because Postgres refuses COMMIT
-- inside a procedure that has one; every name in it is schema-qualified instead.
CREATE PROCEDURE snouttime.migrate(relation regclass, batches integer DEFAULT NULL)
LANGUAGE plpgsql
AS $$
DECLARE
	s snouttime.series;
	def regclass;
	oldest text;
	done int := 0;
	b record;
BEGIN
	SELECT * INTO s FROM snouttime.series WHERE relid = relation;
	IF NOT FOUND THEN
		RAISE EXCEPTION '% is not a series table', relation;
	END IF;
	def := snouttime._default_partition(relation);
	IF def IS NULL THEN
		RETURN;
	END IF;
	LOOP
		EXIT WHEN batches IS NOT NULL AND done >= batches;
		SELECT * INTO b FROM snouttime._migrate_batch(relation);
		IF b.stopped_at IS NOT NULL THEN
			RAISE EXCEPTION 'rows in % from % onward fall in a range covered by a partition SnoutTime did not make', def, b.stopped_at;
		END IF;
		EXIT WHEN b.moved = 0;
		done := done + 1;
		COMMIT;
	END LOOP;
END
$$;


-- Drop the default partition, once nothing is left in it.
--
-- It is worth doing and it is not free either way, so it is a decision rather than a
-- default. What it buys: an empty default partition cannot be pruned, so EVERY query that
-- probes the table per row pays an extra index scan for it. Measured at 10M rows on
-- 2026-09-22: an as-of join of 20,811 events took 2,477 ms with the default partition
-- attached and 248 ms without it, ten times faster for a partition holding nothing.
--
-- What it costs: the default partition is the safety net. Without one, an INSERT whose
-- time falls outside every existing partition FAILS instead of landing somewhere and
-- being tidied up later. The worker keeps partitions ahead of now(), so ordinary writes
-- are fine; a backfill of old data is not, and wants `make_partitions` first.
CREATE FUNCTION snouttime.drop_default(relation regclass) RETURNS boolean
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
	def regclass;
	left_over int8;
BEGIN
	IF NOT EXISTS (SELECT 1 FROM snouttime.series WHERE relid = relation) THEN
		RAISE EXCEPTION '% is not a series table', relation;
	END IF;
	def := snouttime._default_partition(relation);
	IF def IS NULL THEN
		RETURN false;
	END IF;
	EXECUTE format('SELECT count(*) FROM %s', def) INTO left_over;
	IF left_over > 0 THEN
		RAISE EXCEPTION '% still holds % rows', def, left_over
			USING ERRCODE = 'object_not_in_prerequisite_state',
			HINT = 'CALL snouttime.migrate(...) moves them into partitions first.';
	END IF;
	EXECUTE format('ALTER TABLE %s DETACH PARTITION %s', relation, def);
	EXECUTE format('DROP TABLE %s', def);
	RETURN true;
END
$$;


-- Stop treating a table as a series table. Its partitions and data stay exactly as they
-- are: it is an ordinary partitioned table afterwards.
CREATE FUNCTION snouttime.drop_series(relation regclass) RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
BEGIN
	IF NOT EXISTS (SELECT 1 FROM snouttime.series WHERE relid = relation) THEN
		RAISE EXCEPTION '% is not a series table', relation;
	END IF;
	IF EXISTS (SELECT 1 FROM snouttime.rollups WHERE source = relation) THEN
		RAISE EXCEPTION '% has rollups; drop them first', relation
			USING ERRCODE = 'dependent_objects_still_exist';
	END IF;
	DELETE FROM snouttime.jobs WHERE target = relation;
	DELETE FROM snouttime.series WHERE relid = relation;
END
$$;


-- When a table is dropped, forget it. A regclass does not notice a DROP by itself, and a
-- stale OID could one day be reused by an unrelated table. SECURITY DEFINER because the
-- dropping user may not be the catalog's owner; it is safe because the OIDs come from
-- pg_event_trigger_dropped_objects(), which a caller cannot forge.
CREATE FUNCTION snouttime._on_drop() RETURNS event_trigger
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

CREATE EVENT TRIGGER snouttime_on_drop ON sql_drop EXECUTE FUNCTION snouttime._on_drop();
