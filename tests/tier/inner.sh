#!/usr/bin/env bash
# The tiering test proper, inside the dev container with MinIO reachable (tests/tier/minio.sh).
# A tiered table returns exactly what its heap twin returns, through every scan, after late
# writes and deletes; recall brings it back; the collector deletes only orphaned objects.
set -euo pipefail
bin="$(dirname "$(ls -d ~/.pgrx/${PGV:-17}.*/pgrx-install/bin/pg_config)")"
port=$((28800 + ${PGV:-17}))
psql() { "$bin/psql" -X -q -v ON_ERROR_STOP=1 -h localhost -p "$port" -d st_tier "$@"; }
cargo pgrx install --pg-config "$bin/pg_config" >/dev/null 2>&1
# a fresh server, so it inherits the AWS_* credentials this container was given
cargo pgrx stop "pg${PGV:-17}" >/dev/null 2>&1 || true
cargo pgrx start "pg${PGV:-17}" >/dev/null 2>&1
trap 'cargo pgrx stop "pg${PGV:-17}" >/dev/null 2>&1' EXIT
"$bin/dropdb" -h localhost -p "$port" --if-exists --force st_tier
"$bin/createdb" -h localhost -p "$port" st_tier
fail() { echo "FAIL tier: $*" >&2; exit 1; }

psql -c "ALTER SYSTEM SET snouttime.tier_to = 's3://tiered/test'" \
	-c "ALTER SYSTEM SET snouttime.s3_endpoint = '$SNOUTTIME_TEST_S3_ENDPOINT'" \
	-c "SELECT pg_reload_conf()" >/dev/null

psql <<'SQL' >/dev/null
CREATE EXTENSION snouttime;
SET snouttime.columnar_group_rows = 1000;
CREATE TABLE heap_t (id int8, ts timestamptz, host text, v float8);
INSERT INTO heap_t SELECT g, timestamptz '2026-01-01+00' + g * interval '1 minute', 'h' || (g % 7), g / 3.0
FROM generate_series(1, 20000) g;
CREATE TABLE t (LIKE heap_t);
INSERT INTO t SELECT * FROM heap_t;
CREATE INDEX ON t (ts);
SQL
same() {
	psql -At -c "SELECT (SELECT count(*) FROM (SELECT * FROM t EXCEPT ALL SELECT * FROM heap_t) a) + (SELECT count(*) FROM (SELECT * FROM heap_t EXCEPT ALL SELECT * FROM t) b)"
}
psql -At -c "SELECT snouttime.tier('t')" >/dev/null
[ "$(psql -At -c "SELECT amname FROM pg_class c JOIN pg_am a ON a.oid = c.relam WHERE relname = 't'")" = snouttime_tiered ] || fail "not tiered"
[ "$(same)" = 0 ] || fail "a tiered table differs from its heap twin"
local_bytes="$(psql -At -c "SELECT pg_relation_size('t')")"
[ "$local_bytes" -le 32768 ] || fail "a tiered table keeps $local_bytes bytes here"
echo "PASS tier: 20,000 rows in S3, $local_bytes bytes of metapage and directory here, equal to heap"

# the custom scan skips row groups here too, and an index scan works
q="SELECT count(*), sum(v) FROM t WHERE ts >= '2026-01-05' AND ts < '2026-01-06'"
[ "$(psql -At -c "$q")" = "$(psql -At -c "${q/FROM t/FROM heap_t}")" ] || fail "a range query differs"
skipped="$(psql -At -c "SET enable_indexscan = off" -c "SET enable_bitmapscan = off" -c "SET max_parallel_workers_per_gather = 0" \
	-c "EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) $q" | grep -o 'Row Groups Skipped: [0-9]*' || true)"
[ -n "$skipped" ] || fail "the columnar scan was not used on a tiered table"
if [ -n "${SHOW:-}" ]; then psql -At -c "SET max_parallel_workers_per_gather = 0" -c "SET enable_indexscan = off" -c "SET enable_bitmapscan = off" -c "EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) $q" -c "SELECT snouttime._key_text(1, 'timestamptz')" >&2; fi
psql -At -c "SET enable_seqscan = off" -c "SELECT id FROM t WHERE ts = '2026-01-02 00:00+00'" | grep -qx 1440 || fail "an index scan of a tiered table"
echo "PASS tier: range query equal ($skipped), index scan equal"

# late writes and deletes, then recall
psql -c "INSERT INTO t VALUES (99999, '2026-02-01 00:00+00', 'late', 1)" -c "DELETE FROM t WHERE id % 100 = 0" >/dev/null
psql -c "INSERT INTO heap_t VALUES (99999, '2026-02-01 00:00+00', 'late', 1)" -c "DELETE FROM heap_t WHERE id % 100 = 0" >/dev/null
psql -c "UPDATE t SET v = -1 WHERE id = 5" -c "UPDATE heap_t SET v = -1 WHERE id = 5" >/dev/null
[ "$(same)" = 0 ] || fail "late writes on a tiered table"
psql -c "ANALYZE t" >/dev/null
[ "$(psql -At -c "SELECT snouttime.recall('t')")" = 1 ] || fail "recall"
[ "$(same)" = 0 ] || fail "a recalled table differs"
echo "PASS tier: late rows, deletes and an update on a tiered table; recalled equal"

# tier again, then the collector deletes the first object (orphaned by recall) and not the second
psql -At -c "SELECT snouttime.tier('t')" >/dev/null
[ "$(psql -At -c "SELECT snouttime.tier_gc('0 seconds')")" = 1 ] || fail "the collector should delete exactly the orphaned object"
[ "$(same)" = 0 ] || fail "the collector deleted a live object"
[ "$(psql -At -c "SELECT snouttime.tier_gc('0 seconds')")" = 0 ] || fail "the collector deleted twice"
echo "PASS tier: the collector deleted the orphan and left the live object"

# a series table: the tier job moves its old partitions
psql <<'SQL' >/dev/null
SET client_min_messages = warning;
UPDATE snouttime.jobs SET enabled = false;
CREATE TABLE m (ts timestamptz NOT NULL, v int);
SELECT snouttime.create_series('m', 'ts', partition_interval => '1 day', premake => 1);
SELECT snouttime.make_partitions('m', '2026-01-01', '2026-01-04');
INSERT INTO m SELECT timestamptz '2026-01-01+00' + g * interval '1 minute', g FROM generate_series(0, 3 * 1440 - 1) g;
SELECT snouttime.set_tiering('m', interval '1 day');
UPDATE snouttime.jobs SET next_run = '-infinity', enabled = true WHERE kind = 'tier';
SQL
for _ in 1 2 3 4; do psql -At -c "UPDATE snouttime.jobs SET next_run = '-infinity' WHERE kind = 'tier'" -c "SELECT snouttime.run_due_job()" >/dev/null; done
tiered="$(psql -At -c "SELECT count(*) FROM snouttime.partition_info WHERE series = 'm'::regclass AND state = 'tiered'")"
[ "$tiered" = 3 ] || fail "the tier job tiered $tiered of 3 partitions"
[ "$(psql -At -c "SELECT count(*), sum(v) FROM m")" = "4320|9329040" ] || fail "a tiered series table lost rows"
echo "PASS tier: the tier job moved 3 partitions of a series table to S3; every row still there"
