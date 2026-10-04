//! Sealed partitions: the `snouttime_columnar` table access method (PLAN.md Phase 3,
//! docs/snouttime/COLUMNAR.md).
//!
//! * `format`: the bytes (pure Rust, tested without a server).
//! * `store`: pages in and out.
//! * `types`: Datums in and out.
//! * `build`: filling a new column store.
//! * `read`: reading one.
//! * `am`: the access method's callbacks.
//! * `dml`: inserts, deletes, updates and row locks after the seal.
//! * `scan`: the custom scan that decodes only what a query reads (3.6).

pub mod format;
pub(crate) mod am;
mod agg;
mod build;
mod distinct;
mod dml;
mod fold;
mod read;
mod scan;
mod seek;
mod slot;
mod store;
mod types;

use std::ffi::CString;

use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting, PostgresGucEnum};
use pgrx::pg_sys;
use pgrx::prelude::*;

#[derive(PostgresGucEnum, Clone, Copy, PartialEq, Eq, Debug)]
pub enum Compression {
	None,
	Lz4,
	Zstd,
}

pub static COMPRESSION: GucSetting<Compression> = GucSetting::<Compression>::new(Compression::Lz4);
pub static ORDER_BY: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);
pub static GROUP_ROWS: GucSetting<i32> = GucSetting::<i32>::new(8192);
/// Off (the default): a new column store's non-unique indexes hold only its late rows, and the
/// column store answers for the rest from its sort key and row-group ranges (PLAN.md Q5).
pub static KEEP_INDEXES: GucSetting<bool> = GucSetting::<bool>::new(false);

// Tiering (PLAN.md Phase 5, D11). Credentials come from these settings or from the standard
// AWS environment variables, never from a table: only a superuser can set or read them.
static TIER_TO: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);
static S3_ENDPOINT: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);
static S3_REGION: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);
static S3_ACCESS_KEY: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);
static S3_SECRET_KEY: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);
static S3_SESSION_TOKEN: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);
pub static TIER_GC: GucSetting<bool> = GucSetting::<bool>::new(false);

/// Whether this library was loaded by shared_preload_libraries. Only then are the settings
/// above defined before anything can read them: without it a value given in postgresql.conf
/// is a plain placeholder until the library loads, and any user can SHOW it (found by
/// tests/pg_regress/sql/security.sql, 2026-09-23).
static mut PRELOADED: bool = false;

/// Whether SnoutTime was loaded by shared_preload_libraries: `seal()` and `create_series()` say
/// so when it was not (PLAN.md Q7). In Rust because only a superuser may read that setting.
#[pgrx::pg_extern(stable, parallel_safe)]
fn _preloaded() -> bool {
	unsafe { PRELOADED }
}

pub fn init() {
	unsafe {
		PRELOADED = pg_sys::process_shared_preload_libraries_in_progress;
	}
	GucRegistry::define_enum_guc(
		c"snouttime.columnar_compression",
		c"How a new column store's blocks are compressed: none, lz4 or zstd",
		c"LZ4 decompresses fastest; zstd is smaller. snouttime.seal() sets it from the series table.",
		&COMPRESSION,
		GucContext::Userset,
		GucFlags::default(),
	);
	GucRegistry::define_string_guc(
		c"snouttime.columnar_order_by",
		c"Columns a new column store is sorted by, comma-separated",
		c"Empty keeps the order rows arrive in. snouttime.seal() sets it to the space key, then time.",
		&ORDER_BY,
		GucContext::Userset,
		GucFlags::default(),
	);
	GucRegistry::define_int_guc(
		c"snouttime.columnar_group_rows",
		c"Rows per row group in a new column store",
		c"A row group is the unit that is decoded, skipped and checksummed together.",
		&GROUP_ROWS,
		1,
		crate::codec::MAX_VALUES as i32,
		GucContext::Userset,
		GucFlags::default(),
	);
	GucRegistry::define_bool_guc(
		c"snouttime.columnar_keep_indexes",
		c"Whether a new column store's non-unique indexes cover every row",
		c"Off: they hold only rows written after the seal, and the column store finds the rest by its sort key. Unique indexes always cover every row. snouttime.seal() sets it from the series table.",
		&KEEP_INDEXES,
		GucContext::Userset,
		GucFlags::default(),
	);
	let secret = GucFlags::SUPERUSER_ONLY | GucFlags::NO_SHOW_ALL;
	GucRegistry::define_string_guc(c"snouttime.tier_to", c"Where tiered partitions go: s3://bucket/prefix",
		c"Each tiered partition becomes one object under it.", &TIER_TO, GucContext::Suset, GucFlags::default());
	GucRegistry::define_string_guc(c"snouttime.s3_endpoint", c"The S3 endpoint, e.g. https://s3.us-west-2.amazonaws.com",
		c"Defaults to AWS_ENDPOINT_URL, then to AWS S3 in snouttime.s3_region.", &S3_ENDPOINT, GucContext::Suset, GucFlags::default());
	GucRegistry::define_string_guc(c"snouttime.s3_region", c"The S3 region", c"Defaults to AWS_REGION, then us-east-1.",
		&S3_REGION, GucContext::Suset, GucFlags::default());
	GucRegistry::define_string_guc(c"snouttime.s3_access_key_id", c"The S3 access key id", c"Defaults to AWS_ACCESS_KEY_ID.",
		&S3_ACCESS_KEY, GucContext::Suset, secret);
	GucRegistry::define_string_guc(c"snouttime.s3_secret_access_key", c"The S3 secret key", c"Defaults to AWS_SECRET_ACCESS_KEY.",
		&S3_SECRET_KEY, GucContext::Suset, secret);
	GucRegistry::define_string_guc(c"snouttime.s3_session_token", c"An S3 session token", c"Defaults to AWS_SESSION_TOKEN.",
		&S3_SESSION_TOKEN, GucContext::Suset, secret);
	GucRegistry::define_bool_guc(
		c"snouttime.plan_time_bounds",
		c"Whether a timestamptz compared with a constant plus or minus a constant interval is given bounds partitions can be pruned by while planning",
		c"On: with no days or months in the interval the sum is computed while planning, and otherwise a comparison it implies in every time zone is added. Applies to queries over a table with sealed partitions.",
		&fold::ENABLED,
		GucContext::Userset,
		GucFlags::default(),
	);
	GucRegistry::define_bool_guc(c"snouttime.tier_gc", c"Whether the tier job deletes objects no partition points at",
		c"Off by default: a restored copy of this database may still point at them.", &TIER_GC, GucContext::Suset, GucFlags::default());
	scan::init();
	agg::init();
	distinct::init();
	unsafe {
		pg_sys::RegisterXactCallback(Some(build::xact_callback), std::ptr::null_mut());
		pg_sys::RegisterSubXactCallback(Some(build::subxact_callback), std::ptr::null_mut());
	}
}

fn setting(g: &GucSetting<Option<CString>>, env: &str) -> Option<String> {
	g.get()
		.map(|s| s.to_string_lossy().into_owned())
		.filter(|s| !s.is_empty())
		.or_else(|| std::env::var(env).ok().filter(|s| !s.is_empty()))
}

/// The S3 client's settings, or an ERROR saying which one is missing.
pub fn s3_config() -> crate::s3::Config {
	let region = setting(&S3_REGION, "AWS_REGION").unwrap_or_else(|| "us-east-1".into());
	let endpoint = setting(&S3_ENDPOINT, "AWS_ENDPOINT_URL").unwrap_or_else(|| format!("https://s3.{region}.amazonaws.com"));
	let from_settings = [&S3_ACCESS_KEY, &S3_SECRET_KEY, &S3_SESSION_TOKEN]
		.iter()
		.any(|g| g.get().is_some_and(|s| !s.is_empty()));
	if from_settings && !unsafe { PRELOADED } {
		error!(
			"S3 credentials in snouttime.s3_* settings are readable by every user unless snouttime is in shared_preload_libraries; \
			 add it there, or give the credentials in the server's environment (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)"
		);
	}
	let Some(access_key) = setting(&S3_ACCESS_KEY, "AWS_ACCESS_KEY_ID") else {
		error!("tiering needs S3 credentials: set snouttime.s3_access_key_id (or AWS_ACCESS_KEY_ID in the server's environment)");
	};
	let Some(secret_key) = setting(&S3_SECRET_KEY, "AWS_SECRET_ACCESS_KEY") else {
		error!("tiering needs S3 credentials: set snouttime.s3_secret_access_key (or AWS_SECRET_ACCESS_KEY in the server's environment)");
	};
	crate::s3::Config { endpoint, region, access_key, secret_key, session_token: setting(&S3_SESSION_TOKEN, "AWS_SESSION_TOKEN") }
}

/// Is `rel` a tiered column store (the snouttime_tiered access method)?
///
/// # Safety
/// `rel` must be open.
pub unsafe fn is_tiered(rel: pg_sys::Relation) -> bool {
	let oid = pg_sys::get_am_oid(c"snouttime_tiered".as_ptr(), true);
	oid != pg_sys::InvalidOid && (*(*rel).rd_rel).relam == oid
}

/// Where a tiered relfilenode's object goes: `<snouttime.tier_to>/<system id>/<database>/<relfilenode>.snt`.
/// The relfilenode, not the table's OID: a rewrite makes a new object, and the old one is known
/// to be garbage by its relfilenode no longer being any table's.
///
/// # Safety
/// `rel` must be open.
pub unsafe fn tier_location(rel: pg_sys::Relation) -> crate::s3::Location {
	let Some(prefix) = TIER_TO.get().map(|s| s.to_string_lossy().into_owned()).filter(|s| !s.is_empty()) else {
		error!("tiering needs a destination: set snouttime.tier_to to s3://bucket/prefix");
	};
	let base = match crate::s3::Location::parse(&format!("{}/x", prefix.trim_end_matches('/'))) {
		Ok(l) => l,
		Err(e) => error!("snouttime.tier_to: {e}"),
	};
	let key = format!(
		"{}{}/{}/{}.snt",
		base.key.strip_suffix('x').unwrap_or(""),
		pg_sys::GetSystemIdentifier(),
		pg_sys::MyDatabaseId.to_u32(),
		(*rel).rd_locator.relNumber.to_u32()
	);
	crate::s3::Location { bucket: base.bucket, key }
}

/// `len` bytes of a tiered object from `offset`.
pub fn remote_read(url: &str, offset: u64, len: u64) -> Result<Vec<u8>, String> {
	let loc = crate::s3::Location::parse(url)?;
	crate::s3::get_range(&s3_config(), &loc, offset, len)
}

/// Deletes the objects under this database's prefix that no tiered table points at any more
/// and that are older than `grace` (so a tiering in flight is never hit). Returns how many.
#[pg_extern(volatile)]
fn tier_gc(grace: default!(pgrx::datum::Interval, "'1 hour'")) -> i64 {
	let prefix = TIER_TO.get().map(|s| s.to_string_lossy().into_owned()).unwrap_or_default();
	if prefix.is_empty() {
		return 0;
	}
	let base = match crate::s3::Location::parse(&format!("{}/x", prefix.trim_end_matches('/'))) {
		Ok(l) => l,
		Err(e) => error!("snouttime.tier_to: {e}"),
	};
	let dir = unsafe {
		format!("{}{}/{}/", base.key.strip_suffix('x').unwrap_or(""), pg_sys::GetSystemIdentifier(), pg_sys::MyDatabaseId.to_u32())
	};
	let cfg = s3_config();
	let objects = match crate::s3::list(&cfg, &base.bucket, &dir) {
		Ok(o) => o,
		Err(e) => error!("could not list {}: {e}", prefix),
	};
	let live: Vec<i64> = Spi::connect(|c| {
		c.select(
			"SELECT pg_relation_filenode(c.oid)::int8 FROM pg_class c JOIN pg_am a ON a.oid = c.relam \
			 WHERE a.amname = 'snouttime_tiered'",
			None,
			&[],
		)
		.map(|t| t.filter_map(|r| r.get::<i64>(1).ok().flatten()).collect())
	})
	.unwrap_or_default();
	let mut n = 0;
	for (key, modified) in objects {
		let Some(num) = key.strip_prefix(&dir).and_then(|k| k.strip_suffix(".snt")).and_then(|k| k.parse::<i64>().ok()) else {
			continue;
		};
		if live.contains(&num) {
			continue;
		}
		let old = Spi::get_one_with_args::<bool>("SELECT $1::timestamptz < now() - $2", &[modified.as_str().into(), grace.into()])
			.ok()
			.flatten()
			.unwrap_or(false);
		if !old {
			continue;
		}
		if let Err(e) = crate::s3::delete(&cfg, &crate::s3::Location { bucket: base.bucket.clone(), key }) {
			warning!("{e}");
			continue;
		}
		n += 1;
	}
	n
}

// It acts on the bucket every tiered table shares: the extension's owner decides who may.
extension_sql!(
	"REVOKE EXECUTE ON FUNCTION snouttime.tier_gc(interval) FROM PUBLIC;",
	name = "tier_gc_grant",
	requires = [tier_gc]
);

/// Row `tid` of `rel` into `slot`, as the latest snapshot sees it (for a row just locked).
///
/// # Safety
/// All three must be valid.
pub unsafe fn am_fetch_row(rel: pg_sys::Relation, tid: pg_sys::ItemPointer, slot: *mut pg_sys::TupleTableSlot) -> bool {
	let snapshot = pg_sys::RegisterSnapshot(pg_sys::GetLatestSnapshot());
	let fetch = (*am::routine()).tuple_fetch_row_version.unwrap();
	let found = fetch(rel, tid, snapshot, slot);
	pg_sys::UnregisterSnapshot(snapshot);
	found
}

#[no_mangle]
pub extern "C" fn pg_finfo_snouttime_tiered_handler() -> &'static pg_sys::Pg_finfo_record {
	static V1: pg_sys::Pg_finfo_record = pg_sys::Pg_finfo_record { api_version: 1 };
	&V1
}

/// The tiered access method is the columnar one: the same callbacks, which look at the
/// relation's access method where the two differ (where the row groups are written and read).
#[no_mangle]
#[pg_guard]
pub unsafe extern "C-unwind" fn snouttime_tiered_handler(_fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum {
	pg_sys::Datum::from(am::routine())
}

#[no_mangle]
pub extern "C" fn pg_finfo_snouttime_columnar_handler() -> &'static pg_sys::Pg_finfo_record {
	static V1: pg_sys::Pg_finfo_record = pg_sys::Pg_finfo_record { api_version: 1 };
	&V1
}

#[no_mangle]
#[pg_guard]
pub unsafe extern "C-unwind" fn snouttime_columnar_handler(_fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum {
	pg_sys::Datum::from(am::routine())
}

extension_sql!(
	r#"
-- The side tables of sealed partitions (docs/snouttime/COLUMNAR.md §1): a delta store and a
-- delete log per partition, named after its OID. Each is made a MEMBER of the extension when
-- it is created, which is what keeps pg_dump from dumping it: a partition's rows are dumped
-- once, through the partition (a schema belonging to the extension is not enough; the tables
-- in it were dumped, found by tests/dump/roundtrip.sh on 2026-09-23).
CREATE SCHEMA snouttime_internal;

CREATE FUNCTION snouttime._columnar_handler(internal) RETURNS table_am_handler
	LANGUAGE c AS 'MODULE_PATHNAME', 'snouttime_columnar_handler';
CREATE ACCESS METHOD snouttime_columnar TYPE TABLE HANDLER snouttime._columnar_handler;
COMMENT ON ACCESS METHOD snouttime_columnar IS
	'SnoutTime sealed partitions: compressed column store, with late rows in a delta store';
CREATE FUNCTION snouttime._tiered_handler(internal) RETURNS table_am_handler
	LANGUAGE c AS 'MODULE_PATHNAME', 'snouttime_tiered_handler';
CREATE ACCESS METHOD snouttime_tiered TYPE TABLE HANDLER snouttime._tiered_handler;
COMMENT ON ACCESS METHOD snouttime_tiered IS
	'SnoutTime tiered partitions: a column store whose row groups are an object in S3';

-- Keeps a column-store table's side tables in step with it: made when a table becomes
-- columnar, given the table's columns (dropped ones included, so attribute numbers match),
-- extended when a column is added, and dropped when the table stops being columnar.
--
-- SECURITY DEFINER because the side tables live in the extension's own schema, where a
-- table's owner has no CREATE privilege, and they belong to the extension's owner. It cannot
-- tell who called it (inside a definer, current_user is the definer), and it need not: all it
-- does is make a table's side tables match its access method and columns, which is the same
-- whoever asks. The side tables hold no data a caller could not read through the table.
--
-- And because it runs as the extension's owner, it never evaluates anything the table's owner
-- wrote (2026-10-04). It used to give an added column the table's DEFAULT expression, which an
-- ALTER TABLE then evaluated here, so a default calling a function of the owner's ran it as the
-- superuser. The rows already in the delta store now get the column's missing value copied from
-- the table, where Postgres stored it when the owner's own ALTER TABLE evaluated the default as
-- the owner; and a type change that would have to cast rows in the delta store with a cast of
-- someone else's is refused rather than run (it cannot arise: a change that casts rows rewrites
-- the table, and a rewrite empties its side tables first).
-- Takes side tables out of the extension and drops them. A member of an extension cannot be
-- dropped on its own. Only called by the two SECURITY DEFINER functions here.
CREATE FUNCTION snouttime._columnar_forget(delta text, deletes text) RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
	t text;
BEGIN
	FOREACH t IN ARRAY ARRAY[delta, deletes] LOOP
		CONTINUE WHEN to_regclass(format('snouttime_internal.%I', t)) IS NULL;
		IF EXISTS (SELECT 1 FROM pg_depend d JOIN pg_extension e ON e.oid = d.refobjid
			WHERE d.classid = 'pg_class'::regclass AND d.objid = format('snouttime_internal.%I', t)::regclass
				AND e.extname = 'snouttime' AND d.deptype = 'e') THEN
			EXECUTE format('ALTER EXTENSION snouttime DROP TABLE snouttime_internal.%I', t);
		END IF;
		EXECUTE format('DROP TABLE snouttime_internal.%I', t);
	END LOOP;
END
$$;
REVOKE EXECUTE ON FUNCTION snouttime._columnar_forget(text, text) FROM PUBLIC;

-- The type a domain is ultimately over (a type that is not a domain is its own).
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

CREATE FUNCTION snouttime._columnar_sync(rel regclass) RETURNS void
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

CREATE FUNCTION snouttime._columnar_ddl() RETURNS event_trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
	r record;
BEGIN
	FOR r IN SELECT DISTINCT objid FROM pg_event_trigger_ddl_commands()
		WHERE object_type = 'table' AND classid = 'pg_class'::regclass
	LOOP
		IF EXISTS (SELECT 1 FROM pg_class WHERE oid = r.objid AND relkind = 'r')
			AND r.objid NOT IN (SELECT c.oid FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
				WHERE n.nspname = 'snouttime_internal') THEN
			PERFORM snouttime._columnar_sync(r.objid::regclass);
		END IF;
	END LOOP;
END
$$;
CREATE EVENT TRIGGER snouttime_columnar_ddl ON ddl_command_end
	WHEN TAG IN ('CREATE TABLE', 'CREATE TABLE AS', 'SELECT INTO', 'ALTER TABLE')
	EXECUTE FUNCTION snouttime._columnar_ddl();

-- Drops the side tables of a table that no longer exists. SECURITY DEFINER for the same
-- reason as _columnar_sync; safe for anyone to call, since it refuses a table that exists.
CREATE FUNCTION snouttime._columnar_drop_side(relid oid) RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
BEGIN
	IF EXISTS (SELECT 1 FROM pg_class WHERE oid = relid) THEN
		RAISE EXCEPTION 'table % still exists', relid::regclass;
	END IF;
	PERFORM snouttime._columnar_forget('delta_' || relid, 'deletes_' || relid);
END
$$;

CREATE FUNCTION snouttime._columnar_drop() RETURNS event_trigger
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
CREATE EVENT TRIGGER snouttime_columnar_drop ON sql_drop
	EXECUTE FUNCTION snouttime._columnar_drop();
"#,
	name = "columnar",
	requires = ["catalog"]
);
