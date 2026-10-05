#!/usr/bin/env bash
# The background worker really does run the jobs.
#
# This cannot be a #[pg_test] (each of those runs inside one transaction that is rolled
# back, so a worker in another process would never see the rows) and it cannot be a
# regression test (pg_regress cannot wait). So it is a script: start a server, make a
# series table with old data in it, start a worker, and watch the default partition empty
# itself with nobody calling anything.
set -euo pipefail

bin="$(dirname "$(ls -d ~/.pgrx/${PGV:-17}.*/pgrx-install/bin/pg_config)")"
port=$((28800 + ${PGV:-17}))
psql() { "$bin/psql" -X -q -v ON_ERROR_STOP=1 -h localhost -p "$port" "$@"; }

cargo pgrx install --pg-config "$bin/pg_config" >/dev/null 2>&1
cargo pgrx start "pg${PGV:-17}" >/dev/null 2>&1
trap 'cargo pgrx stop "pg${PGV:-17}" >/dev/null 2>&1' EXIT

"$bin/dropdb" -h localhost -p "$port" --if-exists st_worker
"$bin/createdb" -h localhost -p "$port" st_worker

psql -d st_worker <<'SQL'
CREATE EXTENSION snouttime;
CREATE TABLE w_metrics (ts timestamptz NOT NULL, v float8);
INSERT INTO w_metrics
SELECT timestamptz '2020-03-01 00:00:00+00' + g * interval '6 hours', g
FROM generate_series(0, 11) AS g;
SET client_min_messages = warning;
SELECT snouttime.create_series('w_metrics', 'ts', partition_interval => '1 day', premake => 1);
-- The migrate job runs a minute apart by default and the worker wakes every ten
-- seconds (snouttime.interval, a config-file setting a session cannot change), so the
-- schedule is what this test shortens.
UPDATE snouttime.jobs SET schedule = interval '1 second', next_run = '-infinity';
SELECT snouttime.start_worker();
SQL

# Wait for the whole thing to settle, not just for the rows to move: the job takes itself
# off the list on the run AFTER the one that empties the default partition.
left=""
jobs_left=""
for _ in $(seq 1 90); do
	left="$(psql -d st_worker -At -c 'SELECT count(*) FROM w_metrics_default')"
	jobs_left="$(psql -d st_worker -At -c "SELECT count(*) FROM snouttime.jobs WHERE kind = 'migrate'")"
	if [ "$left" = 0 ] && [ "$jobs_left" = 0 ]; then
		break
	fi
	sleep 1
done

if [ "$left" != 0 ]; then
	echo "worker: default partition still holds $left rows after 90s" >&2
	psql -d st_worker -c 'SELECT kind, ok, detail, started_at FROM snouttime.job_runs ORDER BY started_at' >&2
	exit 1
fi

rows="$(psql -d st_worker -At -c 'SELECT count(*) FROM w_metrics')"
if [ "$rows" != 12 ]; then
	echo "worker: expected 12 rows after the move, found $rows" >&2
	exit 1
fi
ran="$(psql -d st_worker -At -c "SELECT count(*) FROM snouttime.job_runs WHERE kind = 'migrate' AND ok")"
if [ "$ran" -lt 3 ]; then
	echo "worker: expected at least 3 successful migrate runs, found $ran" >&2
	exit 1
fi
gone="$(psql -d st_worker -At -c "SELECT count(*) FROM snouttime.jobs WHERE kind = 'migrate'")"
if [ "$gone" != 0 ]; then
	echo "worker: the migrate job should have removed itself once the default was empty" >&2
	exit 1
fi

echo "PASS worker runs jobs"
