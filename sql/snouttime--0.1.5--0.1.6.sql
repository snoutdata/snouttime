-- SnoutTime 0.1.5 -> 0.1.6 (docs/snouttime/PLAN.md Phase 6, U1).
--
-- The seal job runs as its table's owner, and once there is nothing left to seal it asks
-- _changed_since_seal() of every sealed partition to decide what to reseal. That was plpgsql
-- reading snouttime_internal.delta_<oid> and deletes_<oid>, which needs USAGE on the schema and
-- SELECT on the side tables. A database's owner on SnoutData Cloud is not a superuser and has
-- neither, so the job failed on every run with "permission denied for schema
-- snouttime_internal" once a series had a sealed partition. It is Rust now
-- (src/columnar/read.rs), reads the side tables directly, and asks for SELECT on the table.

\echo Use "ALTER EXTENSION snouttime UPDATE TO '0.1.6'" to load this file. \quit

DROP FUNCTION snouttime._changed_since_seal(regclass, int8);

CREATE FUNCTION snouttime."_changed_since_seal"(
	"leaf" regclass,
	"upto" bigint
) RETURNS bigint
STRICT STABLE
LANGUAGE c
AS 'MODULE_PATHNAME', '_changed_since_seal_wrapper';
