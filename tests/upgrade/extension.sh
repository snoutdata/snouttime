#!/usr/bin/env bash
# An existing database takes a new catalog with
# `ALTER EXTENSION snouttime UPDATE`, keeps its data, and ends with EXACTLY the catalog a fresh
# install of the new version makes.
#
# For every earlier catalog in tests/upgrade/fixtures (each the install script a release, or a
# rollout, generated; `snouttime--<version>-<label>.sql`):
#   1. install it, with TODAY's library under it (a new library must serve the old catalog
#      until the UPDATE runs, which is the state every pod is in after an image rollout);
#   2. give it data: a series with a sealed and a live partition, late rows, a rollup, its jobs;
#   3. ALTER EXTENSION snouttime UPDATE;
#   4. the data reads back the same, the new catalog works (a seal records its size), and
#   5. tests/upgrade/fingerprint.sql is identical to a fresh install's, line for line.
# Tiering is not covered here: it needs an S3 endpoint (tests/tier/minio.sh).
#
# A server of its own, on its own port, as tests/worker/preload.sh does.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
bin="$(dirname "$(ls -d ~/.pgrx/${PGV:-17}.*/pgrx-install/bin/pg_config)")"
port=28819
data="$(mktemp -d)"
psql() { "$bin/psql" -X -q -v ON_ERROR_STOP=1 -h localhost -p "$port" "$@"; }

cargo pgrx install --pg-config "$bin/pg_config" >/dev/null 2>&1
extdir="$("$bin/pg_config" --sharedir)/extension"
version="$(sed -n 's/^version = "\(.*\)"/\1/p' "$here/../../Cargo.toml" | head -1)"
[ -f "$extdir/snouttime--$version.sql" ] || { echo "upgrade: no install script for $version in $extdir" >&2; exit 1; }

"$bin/initdb" -D "$data" -A trust >/dev/null
opts="-p $port -c shared_preload_libraries=snouttime -c unix_socket_directories=$data"
"$bin/pg_ctl" -D "$data" -o "$opts" -l "$data/log" -w start >/dev/null
trap '"$bin/pg_ctl" -D "$data" -m immediate stop >/dev/null 2>&1; rm -rf "$data"; rm -f "$extdir"/snouttime--0.0.0.sql' EXIT

"$bin/createdb" -h localhost -p "$port" st_fresh
psql -d st_fresh -c 'CREATE EXTENSION snouttime' 2>/dev/null
psql -d st_fresh -At -f "$here/fingerprint.sql" >"$data/fresh.txt"

fail=0
for fixture in "$here"/fixtures/snouttime--*.sql; do
	file="$(basename "$fixture" .sql)"
	from="${file#snouttime--}"
	from="${from%%-*}"
	label="${file#snouttime--$from-}"
	# the version as well as the label: two releases both labelled "release" shared a database
	db="st_up_$(echo "${from}_$label" | tr -c 'a-z0-9\n' '_')"
	# The fixture stands in for that version's install script, which pgrx no longer writes.
	cp "$fixture" "$extdir/snouttime--$from.sql"
	"$bin/createdb" -h localhost -p "$port" "$db"
	psql -d "$db" -c "CREATE EXTENSION snouttime VERSION '$from'" 2>/dev/null
	psql -d "$db" <<'SQL' >/dev/null 2>&1
SET client_min_messages = warning;
CREATE TABLE u_m (ts timestamptz NOT NULL, host text NOT NULL, v int8);
SELECT snouttime.create_series('u_m', 'ts', partition_interval => '1 day', premake => 1);
SELECT snouttime.make_partitions('u_m', '2026-01-01', '2026-01-03');
INSERT INTO u_m SELECT timestamptz '2026-01-01+00' + g * interval '1 minute', 'h' || (g % 4), g
FROM generate_series(0, 2 * 1440 - 1) g;
SELECT snouttime.seal('u_m_p20260101');
INSERT INTO u_m VALUES ('2026-01-01 12:00:30+00', 'late', 1000000);
SELECT snouttime.create_rollup('u_hourly', 'u_m', interval '1 hour', select_list => 'max(v) AS max_v, count(*) AS n');
SELECT snouttime.refresh_rollup('u_hourly');
SELECT snouttime.set_retention('u_m', interval '3650 days');
SQL
	before="$(psql -d "$db" -At -c "SELECT count(*) || ' ' || sum(v) FROM u_m")"
	rolled="$(psql -d "$db" -At -c "SELECT sum(n) FROM u_hourly")"

	psql -d "$db" -c 'ALTER EXTENSION snouttime UPDATE'
	rm -f "$extdir/snouttime--$from.sql"

	now="$(psql -d "$db" -At -c "SELECT extversion FROM pg_extension WHERE extname = 'snouttime'")"
	after="$(psql -d "$db" -At -c "SELECT count(*) || ' ' || sum(v) FROM u_m")"
	rolled_after="$(psql -d "$db" -At -c "SELECT sum(n) FROM u_hourly")"
	# the new catalog is in use: sealing the other day records its heap size
	psql -d "$db" -c "SELECT snouttime.seal('u_m_p20260102')" >/dev/null
	sized="$(psql -d "$db" -At -c "SELECT bytes_before > bytes FROM snouttime.partition_info WHERE name = 'u_m_p20260102'")"
	psql -d "$db" -At -f "$here/fingerprint.sql" >"$data/$db.txt"

	problems=()
	[ "$now" = "$version" ] || problems+=("extversion is $now, not $version")
	[ "$after" = "$before" ] || problems+=("rows changed: $before before, $after after")
	[ "$rolled_after" = "$rolled" ] || problems+=("the rollup changed: $rolled before, $rolled_after after")
	[ "$sized" = t ] || problems+=("a seal after the upgrade recorded no size ($sized)")
	if ! diff -u "$data/fresh.txt" "$data/$db.txt" >"$data/$db.diff"; then
		problems+=("the catalog differs from a fresh $version install:")
	fi
	if [ ${#problems[@]} -eq 0 ]; then
		echo "PASS upgrade: $from ($label) -> $version, rows (count and sum: $after) and the rollup intact, catalog identical to a fresh install"
	else
		fail=1
		echo "FAIL upgrade: $from ($label) -> $version" >&2
		printf '  %s\n' "${problems[@]}" >&2
		[ -s "$data/$db.diff" ] && head -60 "$data/$db.diff" >&2
	fi
done
exit $fail
