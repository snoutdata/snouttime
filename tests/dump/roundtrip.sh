#!/usr/bin/env bash
# PLAN.md 0.3: a pg_dump + pg_restore of a database using SnoutTime keeps every catalog
# row, and resolves each regclass to the restored table rather than to a stale OID.
#
# Runs inside the dev container (scripts/test.sh calls it). Uses the pgrx-managed
# Postgres 17, installs the current build into it, and leaves it stopped.
set -euo pipefail

bin="$(dirname "$(ls -d ~/.pgrx/${PGV:-17}.*/pgrx-install/bin/pg_config)")"
port=$((28800 + ${PGV:-17}))
psql() { "$bin/psql" -X -q -v ON_ERROR_STOP=1 -h localhost -p "$port" "$@"; }

cargo pgrx install --pg-config "$bin/pg_config" >/dev/null 2>&1
cargo pgrx start "pg${PGV:-17}" >/dev/null 2>&1
trap 'cargo pgrx stop "pg${PGV:-17}" >/dev/null 2>&1' EXIT

for db in st_dump_src st_dump_dst; do
	"$bin/dropdb" -h localhost -p "$port" --if-exists "$db"
	"$bin/createdb" -h localhost -p "$port" "$db"
done

psql -d st_dump_src <<'SQL'
CREATE EXTENSION snouttime;
-- Pushes the OIDs of the tables below away from the ones a fresh restore will assign.
CREATE TABLE padding (x int); DROP TABLE padding;
CREATE TABLE metrics (ts timestamptz NOT NULL, v float8);
CREATE TABLE ticks (n int8 NOT NULL, v float8);
CREATE TABLE metrics_hourly (bucket timestamptz, v float8);
INSERT INTO snouttime.series (relid, time_column, time_type, partition_interval, retention)
	VALUES ('metrics', 'ts', 'timestamptz', '1 day', '30 days');
INSERT INTO snouttime.series (relid, time_column, time_type, partition_width)
	VALUES ('ticks', 'n', 'bigint', 1000000);
INSERT INTO snouttime.rollups (relid, source, materialized, time_column, time_type, bucket_interval, watermark, select_list)
	VALUES ('metrics_hourly', 'metrics', 'metrics_hourly', 'ts', 'timestamptz', '1 hour', 42, 'max(v)');
INSERT INTO snouttime.invalidations (rollup, lo, hi) VALUES ('metrics_hourly', 10, 20);
INSERT INTO snouttime.jobs (kind, target, schedule) VALUES ('retention', 'metrics', '1 hour');
INSERT INTO snouttime.job_runs (kind, target, started_at) VALUES ('retention', 'metrics', now());
SQL

snapshot() {
	psql -d "$1" -At <<'SQL'
SELECT 'series', relid::text, time_column, time_type::text, partition_interval, partition_width, retention FROM snouttime.series ORDER BY 2;
SELECT 'rollups', relid::text, source::text, bucket_interval, watermark, select_list FROM snouttime.rollups ORDER BY 2;
SELECT 'invalidations', rollup::text, lo, hi FROM snouttime.invalidations ORDER BY 2, 3;
SELECT 'jobs', kind, target::text, schedule FROM snouttime.jobs ORDER BY 2, 3;
SELECT 'resolves', count(*) FROM snouttime.series s JOIN pg_class c ON c.oid = s.relid;
SQL
}

"$bin/pg_dump" -h localhost -p "$port" -Fc -f /tmp/st_dump.pgdump st_dump_src
"$bin/pg_restore" -h localhost -p "$port" -d st_dump_dst --exit-on-error /tmp/st_dump.pgdump

before="$(snapshot st_dump_src)"
after="$(snapshot st_dump_dst)"
if [ "$before" != "$after" ]; then
	echo "dump roundtrip: catalog differs after restore" >&2
	diff <(echo "$before") <(echo "$after") >&2 || true
	exit 1
fi
if ! grep -q '^resolves|2$' <<<"$after"; then
	echo "dump roundtrip: series rows do not resolve to restored tables" >&2
	exit 1
fi
runs="$(psql -d st_dump_dst -At -c 'SELECT count(*) FROM snouttime.job_runs')"
if [ "$runs" != 0 ]; then
	echo "dump roundtrip: job_runs is history and must not be dumped, found $runs rows" >&2
	exit 1
fi
echo "PASS dump roundtrip"

# PLAN.md 3.7: a sealed table survives dump and restore with the same rows, counted once.
# Its side tables are in the extension's schema and must not be dumped: the rows in them are
# dumped through the table itself, and dumping both would restore every late row twice.
for db in st_seal_src st_seal_dst; do
	"$bin/dropdb" -h localhost -p "$port" --if-exists "$db"
	"$bin/createdb" -h localhost -p "$port" "$db"
done
psql -d st_seal_src <<'SQL'
CREATE EXTENSION snouttime;
SET snouttime.columnar_group_rows = 500;
CREATE TABLE sealed (id int8, host text, v float8);
INSERT INTO sealed SELECT g, 'h' || (g % 9), g / 3.0 FROM generate_series(1, 5000) g;
ALTER TABLE sealed SET ACCESS METHOD snouttime_columnar;
INSERT INTO sealed VALUES (9001, 'late', 1), (9002, 'late', 2);
DELETE FROM sealed WHERE id % 100 = 0;
UPDATE sealed SET v = -v WHERE id BETWEEN 10 AND 20;
-- a real rollup: series table, view, materialized buckets, triggers
SET client_min_messages = warning;
CREATE TABLE ro (ts timestamptz NOT NULL, v int);
SELECT snouttime.create_series('ro', 'ts', partition_interval => '1 day', premake => 1);
SELECT snouttime.make_partitions('ro', '2026-01-01', '2026-01-03');
INSERT INTO ro SELECT timestamptz '2026-01-01+00' + g * interval '1 minute', g FROM generate_series(0, 2879) g;
SELECT snouttime.create_rollup('ro_hourly', 'ro', interval '1 hour', select_list => 'sum(v) AS s, count(*) AS n');
SELECT snouttime.refresh_rollup('ro_hourly');
INSERT INTO ro VALUES ('2026-01-01 05:30+00', 1000000);
SQL
rows() { psql -d "$1" -At -c "SELECT count(*), md5(string_agg(id || ':' || host || ':' || v, ',' ORDER BY id)) FROM sealed"; }
"$bin/pg_dump" -h localhost -p "$port" -Fc -f /tmp/st_seal.pgdump st_seal_src
if "$bin/pg_restore" -l /tmp/st_seal.pgdump | grep -q 'snouttime_internal'; then
	echo "dump roundtrip: the side tables of a sealed table were dumped" >&2
	exit 1
fi
"$bin/pg_restore" -h localhost -p "$port" -d st_seal_dst --exit-on-error /tmp/st_seal.pgdump
before="$(rows st_seal_src)"
after="$(rows st_seal_dst)"
am="$(psql -d st_seal_dst -At -c "SELECT amname FROM pg_class c JOIN pg_am a ON a.oid = c.relam WHERE c.relname = 'sealed'")"
if [ "$before" != "$after" ] || [ "$am" != snouttime_columnar ]; then
	echo "dump roundtrip: sealed table differs after restore ($before / $after, $am)" >&2
	exit 1
fi
# restored rows arrive as late rows; a reseal folds them into a column store
resealed="$(psql -d st_seal_dst -At -c "SELECT snouttime.reseal('sealed')")"
left="$(psql -d st_seal_dst -At -c "SELECT snouttime._changed_since_seal('sealed', 100000)")"
if [ "$resealed" != 1 ] || [ "$left" != 0 ]; then
	echo "dump roundtrip: the reseal did not fold the restored rows in (resealed $resealed, $left rows left)" >&2
	exit 1
fi
if [ "$(rows st_seal_dst)" != "$before" ]; then
	echo "dump roundtrip: sealed table differs after reseal" >&2
	exit 1
fi
echo "PASS dump roundtrip of a sealed table ($before)"

# the rollup: same answer after restore, and still invalidated by writes
ro() { psql -d "$1" -At -c "SELECT count(*), sum(s), sum(n) FROM ro_hourly"; }
[ "$(ro st_seal_src)" = "$(ro st_seal_dst)" ] || { echo "dump roundtrip: the rollup differs after restore" >&2; exit 1; }
psql -d st_seal_dst -At -c "INSERT INTO ro VALUES ('2026-01-02 01:00+00', 5)" >/dev/null
psql -d st_seal_src -At -c "INSERT INTO ro VALUES ('2026-01-02 01:00+00', 5)" >/dev/null
[ "$(ro st_seal_src)" = "$(ro st_seal_dst)" ] || { echo "dump roundtrip: the restored rollup misses a new write" >&2; exit 1; }
psql -d st_seal_dst -At -c "SELECT snouttime.refresh_rollup('ro_hourly')" >/dev/null
[ "$(ro st_seal_src)" = "$(ro st_seal_dst)" ] || { echo "dump roundtrip: the restored rollup refreshes wrong" >&2; exit 1; }
echo "PASS dump roundtrip of a rollup ($(ro st_seal_dst))"
