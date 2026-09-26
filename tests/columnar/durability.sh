#!/usr/bin/env bash
# PLAN.md 3.7: a sealed partition survives what a database has to survive. In throwaway
# clusters of the pgrx-built Postgres 17 (none of it touches the test cluster):
#
#   crash    kill -9 the postmaster in the middle of a seal: after recovery the table is
#            whole, sealed or not; and a committed delete from a sealed table survives
#            WAL replay without a checkpoint
#   replica  a streaming standby returns exactly what the primary does after a seal
#   pitr     a base backup plus archived WAL restored to before the seal gives the heap
#            table, and to between the seal and a later delete gives the sealed one
#   damage   a byte flipped in a column store page is an ERROR naming the damage, and the
#            server carries on
#
# Runs inside the dev container (scripts/test.sh). The WAL path is the same one pgBackRest
# uses (archive_command, a base backup, restore_command); pgBackRest itself is exercised by
# the pod image's own tests.
set -euo pipefail

bin="$(dirname "$(ls -d ~/.pgrx/${PGV:-17}.*/pgrx-install/bin/pg_config)")"
cargo pgrx install --pg-config "$bin/pg_config" >/dev/null 2>&1
base="$(mktemp -d)"
clusters=()
cleanup() {
	for d in "${clusters[@]}"; do "$bin/pg_ctl" -D "$d" stop -m immediate >/dev/null 2>&1 || true; done
	rm -rf "$base"
}
trap cleanup EXIT

port_of() { case "$1" in primary) echo 28830 ;; replica) echo 28831 ;; before) echo 28832 ;; between) echo 28833 ;; plain) echo 28834 ;; esac; }
q() { "$bin/psql" -X -q -v ON_ERROR_STOP=1 -h "$base" -p "$(port_of "$1")" -d postgres -At "${@:2}"; }
fail() { echo "FAIL durability: $*" >&2; exit 1; }
start() { "$bin/pg_ctl" -D "$base/$1" -l "$base/$1.log" -w start >/dev/null; }

conf() {
	cat >>"$base/$1/postgresql.conf" <<EOF
port = $(port_of "$1")
listen_addresses = ''
unix_socket_directories = '$base'
wal_level = replica
archive_mode = on
archive_command = 'cp %p $base/archive/%f'
max_wal_senders = 5
fsync = on
EOF
}

mkdir -p "$base/archive"
"$bin/initdb" -D "$base/primary" -A trust --data-checksums >/dev/null
conf primary
clusters+=("$base/primary")
start primary

q primary <<'SQL'
CREATE EXTENSION snouttime;
SET snouttime.columnar_group_rows = 1000;
CREATE TABLE t (id int8, host text, v float8);
INSERT INTO t SELECT g, 'h' || (g % 13), g / 7.0 FROM generate_series(1, 200000) g;
CREATE INDEX ON t (id);
SQL
sig() { q "$1" -c "SELECT count(*) || ':' || md5(string_agg(id || ',' || host || ',' || v, ';' ORDER BY id)) FROM t"; }
am() { q "$1" -c "SELECT amname FROM pg_class c JOIN pg_am a ON a.oid = c.relam WHERE relname = 't'"; }
heap_sig="$(sig primary)"

# ---- pitr, part 1: a base backup of the heap table ----
"$bin/pg_basebackup" -h "$base" -p "$(port_of primary)" -D "$base/backup" -X stream >/dev/null

# ---- replica ----
"$bin/pg_basebackup" -h "$base" -p "$(port_of primary)" -D "$base/replica" -R -X stream >/dev/null
sed -i '/^archive_mode/d; /^archive_command/d' "$base/replica/postgresql.conf"
echo "port = $(port_of replica)" >>"$base/replica/postgresql.conf"
clusters+=("$base/replica")
start replica

# ---- seal, then change it ----
q primary -c "SELECT snouttime.seal('t')" >/dev/null
sealed_sig="$(sig primary)"
[ "$sealed_sig" = "$heap_sig" ] || fail "sealing changed the rows"
q primary -c "SELECT pg_switch_wal()" >/dev/null
sleep 1.1
between="$(q primary -c "SELECT now()")"
sleep 1.1
q primary -c "DELETE FROM t WHERE id % 10 = 0" >/dev/null
q primary -c "INSERT INTO t VALUES (999999, 'late', 1)" >/dev/null
changed_sig="$(sig primary)"
q primary -c "SELECT pg_switch_wal()" >/dev/null

# the replica catches up and agrees
lsn="$(q primary -c "SELECT pg_current_wal_lsn()")"
for _ in $(seq 1 60); do
	[ "$(q replica -c "SELECT pg_last_wal_replay_lsn() >= '$lsn'")" = t ] && break
	sleep 0.5
done
[ "$(am replica)" = snouttime_columnar ] || fail "the replica's table is not sealed: '$(am replica)', replayed $(q replica -c "SELECT pg_last_wal_replay_lsn()") of $lsn; $(tail -5 "$base/replica.log")"
[ "$(sig replica)" = "$changed_sig" ] || fail "the replica returns different rows from the primary"
echo "PASS replica: a sealed table, its deletes and late rows replay on a standby"

# ---- pitr, part 2: restore to between the seal and the delete ----
for target in between; do
	cp -a "$base/backup" "$base/$target"
	sed -i '/^archive_mode/d; /^archive_command/d' "$base/$target/postgresql.conf"
	cat >>"$base/$target/postgresql.conf" <<EOF
port = $(port_of "$target")
restore_command = 'cp $base/archive/%f %p'
recovery_target_time = '$between'
recovery_target_action = 'promote'
EOF
	touch "$base/$target/recovery.signal"
	clusters+=("$base/$target")
	start "$target"
	for _ in $(seq 1 60); do
		[ "$(q "$target" -c "SELECT pg_is_in_recovery()")" = f ] && break
		sleep 0.5
	done
done
[ "$(am between)" = snouttime_columnar ] || fail "restored to after the seal, the table is not sealed"
[ "$(sig between)" = "$sealed_sig" ] || fail "restored to between seal and delete, the rows differ"
echo "PASS pitr: restored to after the seal and before a delete, the sealed rows are exactly those"

# ---- crash: a committed delete survives, a seal in flight leaves the table whole ----
q primary -c "CHECKPOINT" >/dev/null
q primary -c "DELETE FROM t WHERE id BETWEEN 100 AND 199" >/dev/null
after_delete="$(sig primary)"
q primary <<'SQL' >/dev/null
CREATE TABLE big (id int8, s text);
INSERT INTO big SELECT g, md5(g::text) FROM generate_series(1, 3000000) g;
SQL
big_sig="$(q primary -c "SELECT count(*) || ':' || sum(id) FROM big")"
( q primary -c "SELECT snouttime.seal('big')" >/dev/null 2>&1 || true ) &
# kill it while the seal is visibly running, so this is a seal in flight and not one done
for _ in $(seq 1 200); do
	[ "$(q primary -c "SELECT count(*) FROM pg_stat_activity WHERE query LIKE 'SELECT snouttime.seal%' AND state = 'active'")" = 1 ] && break
	sleep 0.05
done
sleep 0.2
# the postmaster AND its children: a backend outlives a killed postmaster long enough to
# finish its statement and commit, which is not a crash (the first version of this test
# "crashed" a seal that then committed)
pm="$(head -1 "$base/primary/postmaster.pid")"
kill -9 "$pm" $(pgrep -P "$pm") 2>/dev/null || true
wait || true
sleep 2
start primary
[ "$(sig primary)" = "$after_delete" ] || fail "after a crash, a committed delete from a sealed table was lost"
state="$(q primary -c "SELECT amname FROM pg_class c JOIN pg_am a ON a.oid = c.relam WHERE relname = 'big'")"
[ "$(q primary -c "SELECT count(*) || ':' || sum(id) FROM big")" = "$big_sig" ] || fail "after a crash mid-seal, the table lost rows ($state)"
[ "$state" = heap ] || fail "the seal finished before the kill ($state), so a seal in flight was not tested"
echo "PASS crash: a committed delete survived WAL replay; a seal killed in flight left the table whole ($state)"

# ---- damage: a flipped byte is an ERROR, not a crash (a cluster without page checksums,
# so it is the column store's own checksum that notices) ----
# Postgres 18's initdb turns page checksums on unless told not to (17's has no such flag)
nocheck=""
if "$bin/initdb" --help | grep -q -- --no-data-checksums; then nocheck="--no-data-checksums"; fi
"$bin/initdb" -D "$base/plain" -A trust $nocheck >/dev/null
echo "port = $(port_of plain)" >>"$base/plain/postgresql.conf"
echo "listen_addresses = ''" >>"$base/plain/postgresql.conf"
echo "unix_socket_directories = '$base'" >>"$base/plain/postgresql.conf"
clusters+=("$base/plain")
start plain
q plain <<'SQL' >/dev/null
CREATE EXTENSION snouttime;
CREATE TABLE d (id int8, s text);
INSERT INTO d SELECT g, md5(g::text) FROM generate_series(1, 50000) g;
SELECT snouttime.seal('d');
CHECKPOINT;
SQL
file="$base/plain/$(q plain -c "SELECT pg_relation_filepath('d')")"
"$bin/pg_ctl" -D "$base/plain" -w stop >/dev/null
# a byte in the middle of page 1's data, inside the first row group's chunks
python3 - "$file" <<'PY'
import sys
p = sys.argv[1]
b = bytearray(open(p, 'rb').read())
b[8192 + 3000] ^= 0x5a
open(p, 'wb').write(b)
PY
start plain
if out="$(q plain -c "SELECT sum(length(s)) FROM d" 2>&1)"; then
	fail "a damaged column store was read without an error: $out"
fi
grep -q 'is damaged' <<<"$out" || fail "the error does not say the partition is damaged: $out"
[ "$(q plain -c "SELECT 1")" = 1 ] || fail "the server did not survive reading a damaged block"
echo "PASS damage: $(sed 's/^.*ERROR: *//' <<<"$out" | head -1)"
