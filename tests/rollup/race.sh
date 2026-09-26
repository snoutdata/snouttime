#!/usr/bin/env bash
# PLAN.md 4: a refresh racing inserts loses nothing. Eight sessions write rows at random times
# in the last few hours (so into buckets a refresh has already materialized), in small
# transactions, while another session refreshes the rollup over and over. Afterwards one more
# refresh, and the materialized buckets must equal the aggregate of the raw rows exactly: an
# invalidation a refresh did not see must have been left for the next one.
set -euo pipefail

bin="$(dirname "$(ls -d ~/.pgrx/${PGV:-17}.*/pgrx-install/bin/pg_config)")"
port=$((28800 + ${PGV:-17}))
psql() { "$bin/psql" -X -q -v ON_ERROR_STOP=1 -h localhost -p "$port" -d st_race "$@"; }
cargo pgrx install --pg-config "$bin/pg_config" >/dev/null 2>&1
cargo pgrx start "pg${PGV:-17}" >/dev/null 2>&1
trap 'cargo pgrx stop "pg${PGV:-17}" >/dev/null 2>&1' EXIT
"$bin/dropdb" -h localhost -p "$port" --if-exists --force st_race
"$bin/createdb" -h localhost -p "$port" st_race

psql <<'SQL' >/dev/null
CREATE EXTENSION snouttime;
SET client_min_messages = warning;
CREATE TABLE m (ts timestamptz NOT NULL, host int NOT NULL, v int NOT NULL);
SELECT snouttime.create_series('m', 'ts', partition_interval => '1 hour', premake => 2);
SELECT snouttime.make_partitions('m', (now() - interval '8 hours')::text, now()::text);
INSERT INTO m SELECT now() - interval '6 hours' + g * interval '1 second', g % 5, 1 FROM generate_series(0, 5 * 3600) g;
SELECT snouttime.create_rollup('m_5min', 'm', interval '5 minutes', select_list => 'host, count(*) AS n, sum(v) AS s', group_by => 'host');
SELECT snouttime.refresh_rollup('m_5min');
SQL

writer() {
	for _ in $(seq 1 60); do
		psql -c "INSERT INTO m SELECT now() - interval '5 hours' + random() * interval '4 hours', (random() * 4)::int, 1
			FROM generate_series(1, 20)" 2>/dev/null || true
	done
}
for w in $(seq 1 8); do writer & done
refreshes=0
for _ in $(seq 1 40); do
	psql -At -c "SELECT snouttime.refresh_rollup('m_5min')" >/dev/null && refreshes=$((refreshes + 1))
done
wait
psql -At -c "SELECT snouttime.refresh_rollup('m_5min')" >/dev/null

diff="$(psql -At <<'SQL'
WITH w AS (SELECT snouttime._watermark('m_5min_materialized', NULL::timestamptz) AS v),
truth AS (SELECT snouttime.bucket('5 minutes', ts) AS bucket, host, count(*) AS n, sum(v) AS s
	FROM m WHERE ts < (SELECT v FROM w) GROUP BY 1, 2),
mat AS (SELECT * FROM m_5min_materialized WHERE bucket < (SELECT v FROM w))
SELECT (SELECT count(*) FROM (SELECT * FROM mat EXCEPT ALL SELECT * FROM truth) x) || ' '
	|| (SELECT count(*) FROM (SELECT * FROM truth EXCEPT ALL SELECT * FROM mat) x) || ' '
	|| (SELECT count(*) FROM m) || ' ' || (SELECT count(*) FROM snouttime.invalidations)
SQL
)"
read -r extra missing rows pending <<<"$diff"
if [ "$extra" != 0 ] || [ "$missing" != 0 ] || [ "$pending" != 0 ]; then
	echo "FAIL rollup race: materialized differs from the raw rows ($extra extra, $missing missing, $pending pending)" >&2
	exit 1
fi
echo "PASS rollup race: $rows rows, 9,600 of them written by 8 sessions during $refreshes refreshes; every bucket exact"
