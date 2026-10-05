#!/usr/bin/env bash
# Everything that must pass, in the order that fails fastest:
#   1. licences of every dependency (deny.toml)
#   2. Rust tests, plain and in-database (#[test] and #[pg_test])
#   3. SQL regression tests (tests/pg_regress/sql → tests/pg_regress/expected)
#
#   bash scripts/test.sh            the whole lot, on every supported Postgres (17 and 18)
#   bash scripts/test.sh <filter>   only Rust tests whose name contains <filter>, no regress
#   SNOUTTIME_PG_VERSIONS=17 bash scripts/test.sh   one version
#
# Tiering runs against MinIO and so needs containers of its own: bash tests/tier/minio.sh
# (PGV=18 for Postgres 18).
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ $# -gt 0 ]; then
	exec bash "$here/dev.sh" cargo pgrx test pg17 "$1"
fi

# Every Postgres version SnoutTime supports, one after the other.
versions="${SNOUTTIME_PG_VERSIONS:-17 18}"
exec bash "$here/dev.sh" bash -c '
	set -euo pipefail
	cargo deny check licenses
	for v in '"$versions"'; do
		echo "=== Postgres $v"
		cargo pgrx test "pg$v"
		# --resetdb: every file runs against a fresh database, so one cannot leave state that
		# another would see (they all share one database otherwise).
		cargo pgrx regress "pg$v" --resetdb
		export PGV="$v"
		bash tests/dump/roundtrip.sh
		bash tests/worker/runs_jobs.sh
		bash tests/worker/preload.sh
		bash tests/worker/concurrency.sh
		bash tests/worker/logical_replication.sh
		bash tests/columnar/concurrency.sh
		bash tests/columnar/durability.sh
		bash tests/rollup/race.sh
		bash tests/soak/memory.sh 40
		bash tests/upgrade/extension.sh
	done
	# from the oldest supported version to the newest, with sealed tables and a rollup
	case "'"$versions"'" in *17*18*) bash tests/columnar/upgrade.sh ;; esac
'
