#!/usr/bin/env bash
# PLAN.md 1.6: inserts from several sessions keep working, and lose nothing, while the
# worker is making partitions, moving rows out of the default partition and dropping old
# ones underneath them.
#
# This is the test that would catch a lock held too long or a row moved into a partition
# that is then dropped: eight writers, a worker on a one-second schedule, and an exact
# count at the end.
set -euo pipefail

bin="$(dirname "$(ls -d ~/.pgrx/${PGV:-17}.*/pgrx-install/bin/pg_config)")"
port=$((28800 + ${PGV:-17}))
writers=8
per_writer=200
psql() { "$bin/psql" -X -q -v ON_ERROR_STOP=1 -h localhost -p "$port" "$@"; }

cargo pgrx install --pg-config "$bin/pg_config" >/dev/null 2>&1
cargo pgrx start "pg${PGV:-17}" >/dev/null 2>&1
trap 'cargo pgrx stop "pg${PGV:-17}" >/dev/null 2>&1' EXIT

"$bin/dropdb" -h localhost -p "$port" --if-exists st_conc
"$bin/createdb" -h localhost -p "$port" st_conc

psql -d st_conc <<'SQL'
CREATE EXTENSION snouttime;
SET client_min_messages = warning;
CREATE TABLE c_metrics (ts timestamptz NOT NULL, writer int NOT NULL, n int NOT NULL);
-- Old rows to migrate out of the default partition while the writers are writing. They
-- sit inside a few partitions on purpose: migrate moves ONE partition per run, so rows
-- spread over hundreds of ten-minute ranges would still be arriving when the test ended.
INSERT INTO c_metrics
SELECT now() - interval '40 days' + g * interval '20 seconds', 0, g FROM generate_series(1, 500) AS g;
-- Ten-minute partitions, so the writers cross partition boundaries as they go.
SELECT snouttime.create_series('c_metrics', 'ts', partition_interval => '10 minutes', premake => 2);
UPDATE snouttime.jobs SET schedule = interval '1 second', next_run = '-infinity';
SELECT snouttime.start_worker();
SQL

# Each writer inserts its own rows, spread over a week so some land in the default
# partition and are then migrated under it.
for w in $(seq 1 "$writers"); do
	psql -d st_conc -c "
		INSERT INTO c_metrics
		SELECT now() - (g % 10080) * interval '1 minute', $w, g
		FROM generate_series(1, $per_writer) AS g;" >/dev/null &
done
wait

# Let the worker keep working after the writers stop, then turn retention on and watch it
# drop what it should while the counts still add up.
psql -d st_conc -c "SELECT snouttime.set_retention('c_metrics', interval '5 days')" >/dev/null
psql -d st_conc -c "UPDATE snouttime.jobs SET schedule = interval '1 second', next_run = '-infinity'" >/dev/null
sleep 25

kept="$(psql -d st_conc -At -c "SELECT count(*) FROM c_metrics WHERE writer > 0")"
failures="$(psql -d st_conc -At -c "SELECT count(*) FROM snouttime.job_runs WHERE NOT ok")"
# Rows well inside the retention window must all still be there. Ones near its edge are
# left out of the check on purpose: whether a partition on the boundary has been dropped
# yet is a matter of timing, not correctness.
missing="$(psql -d st_conc -At -c "
	SELECT count(*) FROM (
		SELECT writer, n FROM generate_series(1, $writers) AS writer, generate_series(1, $per_writer) AS n
		WHERE (n % 10080) < 6000
	) AS should
	WHERE NOT EXISTS (
		SELECT 1 FROM c_metrics c WHERE c.writer = should.writer AND c.n = should.n
	)")"

if [ "$failures" != 0 ]; then
	echo "concurrency: $failures job runs failed" >&2
	psql -d st_conc -c "SELECT kind, detail FROM snouttime.job_runs WHERE NOT ok LIMIT 5" >&2
	exit 1
fi
if [ "$missing" != 0 ]; then
	echo "concurrency: $missing rows inside the retention window went missing" >&2
	exit 1
fi
if [ "$kept" -lt 1 ]; then
	echo "concurrency: expected rows to survive, found $kept" >&2
	exit 1
fi

# The 40-day-old seed rows were migrated out of the default partition and then dropped by
# retention. Rows still IN the default partition are never dropped by retention, by design:
# it only ever drops whole partitions.
seed="$(psql -d st_conc -At -c 'SELECT count(*) FROM c_metrics WHERE writer = 0')"
if [ "$seed" != 0 ]; then
	echo "concurrency: retention should have dropped the 40-day-old seed rows, $seed left" >&2
	exit 1
fi

echo "PASS worker concurrency ($kept rows from $writers writers survived, none missing)"
