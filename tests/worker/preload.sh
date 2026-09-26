#!/usr/bin/env bash
# PLAN.md 1.2: the worker the POSTMASTER starts (snouttime in shared_preload_libraries)
# survives starting before the extension exists, which is the normal order: preload the
# library, restart, then CREATE EXTENSION. It used to call run_due_job() at once, fail on
# the missing schema and exit for good, so the database's jobs silently never ran (found
# 2026-09-23 by tests/soak/soak.sh; every other test starts its worker by hand, after the
# extension exists, and so never saw it).
#
# A server of its own, on its own port and data directory, because the one `cargo pgrx
# start` manages is not preloaded.
set -euo pipefail

bin="$(dirname "$(ls -d ~/.pgrx/${PGV:-17}.*/pgrx-install/bin/pg_config)")"
port=28818
data="$(mktemp -d)"
psql() { "$bin/psql" -X -q -v ON_ERROR_STOP=1 -h localhost -p "$port" "$@"; }

cargo pgrx install --pg-config "$bin/pg_config" >/dev/null 2>&1
"$bin/initdb" -D "$data" -A trust >/dev/null
opts="-p $port -c shared_preload_libraries=snouttime -c snouttime.databases=st_pre -c snouttime.interval=1 -c unix_socket_directories=$data"
"$bin/pg_ctl" -D "$data" -o "$opts" -l "$data/log" -w start >/dev/null
trap '"$bin/pg_ctl" -D "$data" -m immediate stop >/dev/null 2>&1; rm -rf "$data"' EXIT

# The database first, then a restart, so the worker the postmaster starts connects to a
# database that exists and has no SnoutTime in it yet: the exact state that killed it.
"$bin/createdb" -h localhost -p "$port" st_pre
"$bin/pg_ctl" -D "$data" -o "$opts" -l "$data/log" -w restart >/dev/null
for _ in $(seq 1 20); do
	grep -q 'waiting for CREATE EXTENSION snouttime' "$data/log" && break
	sleep 0.5
done
if ! grep -q 'waiting for CREATE EXTENSION snouttime' "$data/log"; then
	echo "preload: the worker never said it was waiting for the extension" >&2
	tail -20 "$data/log" >&2
	exit 1
fi

psql -d st_pre <<'SQL'
CREATE EXTENSION snouttime;
CREATE TABLE p_metrics (ts timestamptz NOT NULL, v float8);
INSERT INTO p_metrics
SELECT timestamptz '2020-03-01 00:00:00+00' + g * interval '6 hours', g
FROM generate_series(0, 7) AS g;
SET client_min_messages = warning;
SELECT snouttime.create_series('p_metrics', 'ts', partition_interval => '1 day', premake => 1);
UPDATE snouttime.jobs SET schedule = interval '1 second', next_run = '-infinity';
SQL

left=""
for _ in $(seq 1 60); do
	left="$(psql -d st_pre -At -c 'SELECT count(*) FROM p_metrics_default')"
	[ "$left" = 0 ] && break
	sleep 0.5
done
if [ "$left" != 0 ]; then
	echo "preload: the preloaded worker did not run the jobs ($left rows still in the default partition)" >&2
	grep -E 'snouttime|ERROR|FATAL' "$data/log" | tail -20 >&2
	exit 1
fi
if grep -qE 'snouttime: st_pre (ERROR|FATAL)' "$data/log"; then
	echo "preload: the worker logged an error" >&2
	grep -E 'snouttime: st_pre (ERROR|FATAL)' "$data/log" >&2
	exit 1
fi

echo "PASS preloaded worker waits for CREATE EXTENSION, then runs the jobs"
