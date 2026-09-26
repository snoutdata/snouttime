#!/usr/bin/env bash
# PLAN.md 1.6: a series table replicates to a plain, unpartitioned Postgres table.
#
# A publication with publish_via_partition_root says "publish this as if it were one
# table", so the subscriber does not have to know it is partitioned at all. What this test
# pins down is that SnoutTime's own work underneath (making partitions, moving rows out of
# the default partition) does not change what the subscriber ends up with.
#
# It also pins down the one place where the two DO diverge, which is Postgres's behaviour
# and not ours: dropping a partition is DDL, and DDL is not replicated, so rows retention
# drops on the publisher stay on the subscriber. That is written down here rather than
# discovered by a user.
set -euo pipefail

bin="$(dirname "$(ls -d ~/.pgrx/${PGV:-17}.*/pgrx-install/bin/pg_config)")"
port=$((28800 + ${PGV:-17}))
data="$(ls -d ~/.pgrx/data-${PGV:-17})"
psql() { "$bin/psql" -X -q -v ON_ERROR_STOP=1 -h localhost -p "$port" "$@"; }

cargo pgrx install --pg-config "$bin/pg_config" >/dev/null 2>&1
cargo pgrx start "pg${PGV:-17}" >/dev/null 2>&1
trap 'cargo pgrx stop "pg${PGV:-17}" >/dev/null 2>&1' EXIT

if [ "$(psql -d postgres -At -c 'SHOW wal_level')" != logical ]; then
	echo "wal_level = logical" >>"$data/postgresql.conf"
	cargo pgrx stop "pg${PGV:-17}" >/dev/null 2>&1
	cargo pgrx start "pg${PGV:-17}" >/dev/null 2>&1
fi

for db in st_pub st_sub; do
	"$bin/dropdb" -h localhost -p "$port" --if-exists --force "$db"
	"$bin/createdb" -h localhost -p "$port" "$db"
done

psql -d st_pub <<'SQL'
CREATE EXTENSION snouttime;
SET client_min_messages = warning;
CREATE TABLE r_metrics (ts timestamptz NOT NULL, host text NOT NULL, v float8, PRIMARY KEY (host, ts));
INSERT INTO r_metrics
SELECT now() - interval '3 days' + g * interval '1 hour', 'h' || (g % 3), g
FROM generate_series(0, 71) AS g;
SELECT snouttime.create_series('r_metrics', 'ts', partition_interval => '1 day', premake => 1);
CREATE PUBLICATION r_pub FOR TABLE r_metrics WITH (publish_via_partition_root = true);
SQL

psql -d st_sub <<'SQL'
CREATE TABLE r_metrics (ts timestamptz NOT NULL, host text NOT NULL, v float8, PRIMARY KEY (host, ts));
SQL
# The slot is made on the publisher first, and the subscription told not to make one.
# Publisher and subscriber are the same cluster here, and CREATE SUBSCRIPTION creating its
# own slot would wait for every running transaction to finish, including its own: it hangs
# forever. Two clusters would not need this, but a test that needs two is a test nobody
# runs.
psql -d st_pub -c "SELECT pg_create_logical_replication_slot('r_slot', 'pgoutput')" >/dev/null
psql -d st_sub -c "CREATE SUBSCRIPTION r_sub
	CONNECTION 'host=localhost port=$port dbname=st_pub'
	PUBLICATION r_pub WITH (create_slot = false, slot_name = 'r_slot')" >/dev/null

settled() {
	for _ in $(seq 1 60); do
		if [ "$(psql -d st_sub -At -c 'SELECT count(*) FROM r_metrics')" = "$1" ]; then
			return 0
		fi
		sleep 1
	done
	return 1
}

if ! settled 72; then
	echo "replication: the initial copy did not arrive: $(psql -d st_sub -At -c 'SELECT count(*) FROM r_metrics')" >&2
	exit 1
fi

# Now the part that matters: move every row out of the default partition underneath it.
psql -d st_pub -c "CALL snouttime.migrate('r_metrics')" >/dev/null
psql -d st_pub -c "INSERT INTO r_metrics VALUES (now(), 'h9', 1)" >/dev/null

if ! settled 73; then
	echo "replication: after migrating, the subscriber holds $(psql -d st_sub -At -c 'SELECT count(*) FROM r_metrics')" >&2
	psql -d st_sub -c 'SELECT * FROM pg_stat_subscription' >&2
	exit 1
fi

pub_sum="$(psql -d st_pub -At -c 'SELECT coalesce(sum(v), 0)::int8 FROM r_metrics')"
sub_sum="$(psql -d st_sub -At -c 'SELECT coalesce(sum(v), 0)::int8 FROM r_metrics')"
if [ "$pub_sum" != "$sub_sum" ]; then
	echo "replication: the rows differ after migrating (publisher $pub_sum, subscriber $sub_sum)" >&2
	exit 1
fi

# The documented divergence: retention drops partitions, and DDL is not replicated.
psql -d st_pub -c "SELECT snouttime.set_retention('r_metrics', interval '1 day')" >/dev/null
dropped="$(psql -d st_pub -At -c "SELECT snouttime.apply_retention('r_metrics')")"
if [ "$dropped" -lt 1 ]; then
	echo "replication: expected retention to drop at least one partition, dropped $dropped" >&2
	exit 1
fi
sleep 2
pub_rows="$(psql -d st_pub -At -c 'SELECT count(*) FROM r_metrics')"
sub_rows="$(psql -d st_sub -At -c 'SELECT count(*) FROM r_metrics')"
if [ "$sub_rows" != 73 ] || [ "$pub_rows" -ge 73 ]; then
	echo "replication: expected the subscriber to keep all 73 rows and the publisher to hold fewer" >&2
	echo "  publisher $pub_rows, subscriber $sub_rows" >&2
	exit 1
fi

psql -d st_sub -c 'ALTER SUBSCRIPTION r_sub DISABLE' >/dev/null
psql -d st_sub -c "ALTER SUBSCRIPTION r_sub SET (slot_name = NONE)" >/dev/null
psql -d st_sub -c 'DROP SUBSCRIPTION r_sub' >/dev/null
psql -d st_pub -c "SELECT pg_drop_replication_slot('r_slot')" >/dev/null
echo "PASS logical replication (publisher $pub_rows rows, subscriber keeps $sub_rows: dropped partitions are DDL)"
