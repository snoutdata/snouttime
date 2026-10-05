#!/usr/bin/env bash
# Two sessions writing the same rows of a sealed table behave like heap rows do,
# or fail safely where they cannot. For a column-store row and for a delta-store row:
#
#   delete, delete    the second waits for the first; after its COMMIT, the second deletes
#                     nothing; after its ROLLBACK, the second deletes the row
#   update, update    the second waits, then gets Postgres's "moved to another partition due to
#                     concurrent update" error: an UPDATE here is a delete plus an insert, like a
#                     move, and silently updating nothing would lose the update
#   repeatable read   a delete of a row deleted since the snapshot is a serialisation failure
#   FOR UPDATE        a row lock makes a delete wait until the locking transaction ends
set -euo pipefail

bin="$(dirname "$(ls -d ~/.pgrx/${PGV:-17}.*/pgrx-install/bin/pg_config)")"
port=$((28800 + ${PGV:-17}))
psql() { "$bin/psql" -X -q -h localhost -p "$port" -d st_conc "$@"; }
# without -q, so a statement's tag (DELETE 1) is printed
talk() { "$bin/psql" -X -h localhost -p "$port" -d st_conc "$@"; }
cargo pgrx install --pg-config "$bin/pg_config" >/dev/null 2>&1
RUST_BACKTRACE=1 cargo pgrx start "pg${PGV:-17}" >/dev/null 2>&1
trap 'cargo pgrx stop "pg${PGV:-17}" >/dev/null 2>&1' EXIT
"$bin/dropdb" -h localhost -p "$port" --if-exists --force st_conc
"$bin/createdb" -h localhost -p "$port" st_conc
out="$(mktemp -d)"
fail() { echo "FAIL concurrency: $*" >&2; exit 1; }

psql -v ON_ERROR_STOP=1 <<'SQL' >/dev/null
CREATE EXTENSION snouttime;
CREATE TABLE t (id int PRIMARY KEY, v int);
INSERT INTO t SELECT g, g FROM generate_series(1, 1000) g;
ALTER TABLE t SET ACCESS METHOD snouttime_columnar;
INSERT INTO t VALUES (5001, 1), (5002, 2), (5003, 3), (5004, 4), (5005, 5);
SQL

# Session A holds its change for two seconds; session B starts half a second in, so it has
# to wait for A. Prints what B's statement said.
race() {
	local a_sql="$1" a_end="$2" b_sql="$3" b_iso="${4:-read committed}"
	psql -c "BEGIN" -c "$a_sql" -c "SELECT pg_sleep(2)" -c "$a_end" >"$out/a" 2>&1 &
	sleep 0.5
	talk -c "BEGIN ISOLATION LEVEL $b_iso" -c "SELECT count(*) FROM t" -c "SELECT pg_sleep(0.1)" \
		-c "$b_sql" -c "COMMIT" >"$out/b" 2>&1 &
	sleep 1
	waiting="$(psql -At -c "SELECT count(*) FROM pg_stat_activity WHERE datname = 'st_conc' AND wait_event_type = 'Lock'")"
	wait
	echo "waited=$waiting $(grep -E '^(DELETE|UPDATE)|ERROR' "$out/b" | head -1)"
	if [ -n "${SHOW:-}" ]; then { echo "--- a"; cat "$out/a"; echo "--- b"; cat "$out/b"; } >&2; fi
}

check() {
	local what="$1" got="$2" want="$3"
	[ "$got" = "$want" ] || fail "$what: got '$got', want '$want'"
	echo "PASS $what ($got)"
}

for row in 10 5001; do
	kind="$([ "$row" = 10 ] && echo column-store || echo delta-store)"
	check "delete then delete, first commits, $kind row" \
		"$(race "DELETE FROM t WHERE id = $row" COMMIT "DELETE FROM t WHERE id = $row")" "waited=1 DELETE 0"
	next=$((row + 1))
	check "delete then delete, first rolls back, $kind row" \
		"$(race "DELETE FROM t WHERE id = $next" ROLLBACK "DELETE FROM t WHERE id = $next")" "waited=1 DELETE 1"
	next=$((row + 2))
	got="$(race "UPDATE t SET v = -1 WHERE id = $next" COMMIT "UPDATE t SET v = -2 WHERE id = $next")"
	check "update then update, $kind row" "$(sed 's/ERROR: .*moved to another partition.*/ERROR moved/' <<<"$got")" "waited=1 ERROR moved"
	next=$((row + 3))
	got="$(race "DELETE FROM t WHERE id = $next" COMMIT "DELETE FROM t WHERE id = $next" "repeatable read")"
	check "delete under repeatable read, $kind row" "$(sed 's/ERROR: .*could not serialize.*/ERROR serialize/' <<<"$got")" "waited=1 ERROR serialize"
done

check "FOR UPDATE, then delete" \
	"$(race "SELECT id FROM t WHERE id = 20 FOR UPDATE" COMMIT "DELETE FROM t WHERE id = 20")" "waited=1 DELETE 1"

# What is left is what the statements that succeeded say: 1,005 rows (v = id; 5001-5005 carry
# 1-5) less the seven successful deletes (10, 11, 13, 5001, 5002, 5004, 20) is 998, and the sum
# 500,515 less those rows' values (10+11+13+1+2+4+20 = 61) and less the committed updates of 12
# and 5003 to -1 (13 + 4) is 500,437.
left="$(psql -At -c "SELECT count(*), sum(v) FROM t")"
check "rows left" "$left" "998|500437"
