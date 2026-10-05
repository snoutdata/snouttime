#!/usr/bin/env bash
# pg_upgrade from Postgres 17 to 18 with a sealed table (column store, delta store
# and delete log), a series table with a sealed partition, and a rollup. pg_upgrade carries the
# relation files over as they are, so what this proves is that the new server reads the old
# column store, and that the extension's own objects (the side tables are extension members)
# come across with it. Throwaway clusters of the pgrx-built servers; runs in the dev container.
set -euo pipefail

old="$(dirname "$(ls -d ~/.pgrx/17.*/pgrx-install/bin/pg_config)")"
new="$(dirname "$(ls -d ~/.pgrx/18.*/pgrx-install/bin/pg_config)")"
cargo pgrx install --pg-config "$old/pg_config" >/dev/null 2>&1
cargo pgrx install --pg-config "$new/pg_config" >/dev/null 2>&1
base="$(mktemp -d)"
trap '"$old/pg_ctl" -D "$base/old" stop -m immediate >/dev/null 2>&1 || true; "$new/pg_ctl" -D "$base/new" stop -m immediate >/dev/null 2>&1 || true; rm -rf "$base"' EXIT
fail() { echo "FAIL upgrade: $*" >&2; exit 1; }

for v in old new; do
	bin="$([ $v = old ] && echo "$old" || echo "$new")"
	# the new cluster must match the old one's page checksums, and 18's initdb turns them on
	flags=""
	if [ $v = new ]; then flags="--no-data-checksums"; fi
	"$bin/initdb" -D "$base/$v" -A trust $flags >/dev/null
	printf "port = %s\nlisten_addresses = ''\nunix_socket_directories = '%s'\n" \
		"$([ $v = old ] && echo 28840 || echo 28841)" "$base" >>"$base/$v/postgresql.conf"
done
"$old/pg_ctl" -D "$base/old" -l "$base/old.log" -w start >/dev/null
q() { "$1/psql" -X -q -v ON_ERROR_STOP=1 -h "$base" -p "$2" -d postgres -At "${@:3}"; }
q "$old" 28840 <<'SQL' >/dev/null
CREATE EXTENSION snouttime;
SET client_min_messages = warning;
SET snouttime.columnar_group_rows = 1000;
CREATE TABLE t (id int8, host text, v float8);
INSERT INTO t SELECT g, 'h' || (g % 9), g / 3.0 FROM generate_series(1, 10000) g;
CREATE INDEX ON t (id);
SELECT snouttime.seal('t');
INSERT INTO t VALUES (99999, 'late', 1);
DELETE FROM t WHERE id % 100 = 0;
CREATE TABLE m (ts timestamptz NOT NULL, v int);
SELECT snouttime.create_series('m', 'ts', partition_interval => '1 day', premake => 1);
SELECT snouttime.make_partitions('m', '2026-01-01', '2026-01-03');
INSERT INTO m SELECT timestamptz '2026-01-01+00' + g * interval '1 minute', g FROM generate_series(0, 2879) g;
SELECT snouttime.seal('m_p20260101');
SELECT snouttime.create_rollup('m_hourly', 'm', interval '1 hour', select_list => 'sum(v) AS s, count(*) AS n');
SELECT snouttime.refresh_rollup('m_hourly');
INSERT INTO m VALUES ('2026-01-01 05:30+00', 1000000);
SQL
sig() {
	q "$1" "$2" -c "SELECT (SELECT count(*) || ':' || md5(string_agg(id || ',' || host || ',' || v, ';' ORDER BY id)) FROM t)
		|| ' ' || (SELECT count(*) || ':' || sum(v) FROM m)
		|| ' ' || (SELECT count(*) || ':' || sum(s) || ':' || sum(n) FROM m_hourly)"
}
before="$(sig "$old" 28840)"
"$old/pg_ctl" -D "$base/old" -w stop >/dev/null

(cd "$base" && "$new/pg_upgrade" -b "$old" -B "$new" -d "$base/old" -D "$base/new" -p 28840 -P 28841 >"$base/upgrade.log" 2>&1) \
	|| fail "pg_upgrade failed: $(tail -20 "$base/upgrade.log")"
"$new/pg_ctl" -D "$base/new" -l "$base/new.log" -w start >/dev/null
after="$(sig "$new" 28841)"
[ "$before" = "$after" ] || fail "rows differ after the upgrade: $before / $after"
am="$(q "$new" 28841 -c "SELECT string_agg(c.relname || '=' || a.amname, ',' ORDER BY c.relname) FROM pg_class c JOIN pg_am a ON a.oid = c.relam WHERE c.relname IN ('t', 'm_p20260101')")"
[ "$am" = "m_p20260101=snouttime_columnar,t=snouttime_columnar" ] || fail "sealed tables after the upgrade: $am"
# still writable, still resealable, still refreshing
q "$new" 28841 -c "INSERT INTO t VALUES (100000, 'after', 2)" -c "SELECT snouttime.reseal('t')" \
	-c "SELECT snouttime.refresh_rollup('m_hourly')" >/dev/null
[ "$(q "$new" 28841 -c "SELECT count(*) FROM t")" = "$(( $(cut -d: -f1 <<<"$before") + 1 ))" ] || fail "a write after the upgrade"
echo "PASS upgrade: Postgres 17 to 18 with a sealed table, a sealed partition and a rollup ($after)"
