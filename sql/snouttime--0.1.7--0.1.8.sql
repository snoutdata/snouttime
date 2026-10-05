-- SnoutTime 0.1.7 -> 0.1.8 (2026-10-05).
--
-- No change in behaviour. The comments inside four functions are reworded so they explain
-- themselves; function bodies are part of the catalog, so the new text arrives this way and an
-- upgraded database ends with exactly the catalog a fresh install makes.

\echo Use "ALTER EXTENSION snouttime UPDATE TO '0.1.8'" to load this file. \quit

CREATE OR REPLACE FUNCTION snouttime._make_partition(parent regclass, v text, attach boolean DEFAULT true)
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
	-- own before a row can land in it.
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

CREATE OR REPLACE FUNCTION snouttime.create_series(
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

	-- The optional space key: each time partition is hash-partitioned on it,
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

CREATE OR REPLACE FUNCTION snouttime._do_job(job_kind text, rel regclass,
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
		-- and gives up rather than queue behind a user's long transaction; the next
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
		-- One partition per run, like sealing, and the same lock rule.
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
	-- So the view is never stale, only partly materialized.
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
