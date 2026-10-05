-- SnoutTime 0.1.1 -> 0.1.2.
--
-- snouttime.column_sizes(partition): what each column of a sealed partition costs, per encoding,
-- read from its row groups' headers. The rest of 0.1.2 is in the library and changes no catalog:
-- a point lookup on a sealed partition with late rows reads only its row groups' deletes and
-- streams one value's late rows from the index, and a GROUP BY over sealed and live partitions
-- under a bound known only at run time no longer plans over a freed path.
--
-- The function below is the text pgrx generates for a fresh 0.1.2 install, so
-- tests/upgrade/extension.sh finds the upgraded catalog identical to it.

\echo Use "ALTER EXTENSION snouttime UPDATE TO '0.1.2'" to load this file. \quit

CREATE FUNCTION snouttime."column_sizes"(
	"relation" regclass
) RETURNS TABLE (
	"attname" TEXT,
	"type_name" TEXT,
	"encoding" TEXT,
	"row_groups" bigint,
	"rows" bigint,
	"nulls" bigint,
	"stored_bytes" bigint
)
STRICT STABLE PARALLEL SAFE
LANGUAGE c
AS 'MODULE_PATHNAME', 'column_sizes_wrapper';
