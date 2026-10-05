-- Everything the extension owns, as text with no OIDs in it, one line per fact, sorted: two
-- databases whose outputs are identical have the same SnoutTime catalog. Used by extension.sh to
-- compare an UPGRADED database with a FRESH install of the same version.
-- pg_dump cannot do this: it writes CREATE EXTENSION for an extension and nothing it owns.
WITH owned AS (
	SELECT d.classid, d.objid
	FROM pg_depend d JOIN pg_extension e ON e.oid = d.refobjid
	WHERE e.extname = 'snouttime' AND d.deptype = 'e'
),
facts AS (
	-- membership: every object, by the name Postgres gives it
	SELECT 'member ' || pg_describe_object(classid, objid, 0) AS line FROM owned
	UNION ALL
	-- functions and procedures: their whole definition, security and settings included
	SELECT 'function ' || p.oid::regprocedure::text || E'\n' || pg_get_functiondef(p.oid)
		|| E'\nacl ' || coalesce(p.proacl::text, '')
	FROM owned o JOIN pg_proc p ON o.classid = 'pg_proc'::regclass AND p.oid = o.objid
	WHERE p.prokind IN ('f', 'p', 'w')
	UNION ALL
	SELECT 'aggregate ' || p.oid::regprocedure::text || ' ' || concat_ws(' ', a.aggtransfn::text,
		a.aggfinalfn::text, a.aggcombinefn::text, a.aggserialfn::text, a.aggdeserialfn::text,
		format_type(a.aggtranstype, NULL), a.agginitval, a.aggkind::text)
		|| ' acl ' || coalesce(p.proacl::text, '')
	FROM owned o JOIN pg_proc p ON o.classid = 'pg_proc'::regclass AND p.oid = o.objid
	JOIN pg_aggregate a ON a.aggfnoid = p.oid
	UNION ALL
	-- relations: kind and ACL, then columns, view bodies, indexes, constraints, triggers
	SELECT 'relation ' || c.oid::regclass::text || ' ' || c.relkind::text || ' acl ' || coalesce(c.relacl::text, '')
	FROM owned o JOIN pg_class c ON o.classid = 'pg_class'::regclass AND c.oid = o.objid
	UNION ALL
	SELECT 'column ' || c.oid::regclass::text || '.' || a.attname || ' ' || a.attnum || ' '
		|| format_type(a.atttypid, a.atttypmod) || CASE WHEN a.attnotnull THEN ' not null' ELSE '' END
		|| coalesce(' default ' || pg_get_expr(ad.adbin, ad.adrelid), '')
	FROM owned o JOIN pg_class c ON o.classid = 'pg_class'::regclass AND c.oid = o.objid
	JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
	LEFT JOIN pg_attrdef ad ON ad.adrelid = c.oid AND ad.adnum = a.attnum
	UNION ALL
	SELECT 'view ' || c.oid::regclass::text || E'\n' || pg_get_viewdef(c.oid)
	FROM owned o JOIN pg_class c ON o.classid = 'pg_class'::regclass AND c.oid = o.objid
	WHERE c.relkind IN ('v', 'm')
	UNION ALL
	SELECT 'index ' || pg_get_indexdef(i.indexrelid)
	FROM owned o JOIN pg_index i ON o.classid = 'pg_class'::regclass AND i.indrelid = o.objid
	UNION ALL
	SELECT 'constraint ' || con.conrelid::regclass::text || ' ' || con.conname || ' ' || pg_get_constraintdef(con.oid)
	FROM owned o JOIN pg_constraint con ON o.classid = 'pg_class'::regclass AND con.conrelid = o.objid
	UNION ALL
	SELECT 'trigger ' || pg_get_triggerdef(t.oid)
	FROM owned o JOIN pg_trigger t ON o.classid = 'pg_class'::regclass AND t.tgrelid = o.objid
	WHERE NOT t.tgisinternal
	UNION ALL
	SELECT 'event trigger ' || evtname || ' ' || evtevent || ' ' || evtfoid::regproc::text
	FROM owned o JOIN pg_event_trigger et ON o.classid = 'pg_event_trigger'::regclass AND et.oid = o.objid
	UNION ALL
	-- what pg_dump keeps of the extension's own tables (pg_extension_config_dump)
	SELECT 'config ' || string_agg(c::regclass::text, ', ' ORDER BY c::regclass::text)
	FROM pg_extension e, unnest(e.extconfig) AS c
	WHERE e.extname = 'snouttime'
	UNION ALL
	SELECT 'schema acl ' || coalesce(n.nspacl::text, '') FROM pg_namespace n WHERE n.nspname = 'snouttime'
)
-- snouttime_internal holds each sealed table's own delta and delete stores: the user's data,
-- made by a seal, not the catalog, so a database with sealed tables would never match a fresh one
SELECT line FROM facts WHERE line NOT LIKE '%snouttime_internal.%' ORDER BY line;
