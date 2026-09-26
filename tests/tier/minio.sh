#!/usr/bin/env bash
# PLAN.md 5: tiering against a real S3 implementation, MinIO, which checks every signature.
# Runs on the HOST (it starts containers): MinIO and the dev container on one network, then
# tests/tier/inner.sh inside the dev container.
#
#   bash tests/tier/minio.sh
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(cd "$here/../.." && pwd)"
engine="${SNOUTTIME_ENGINE:-docker}"
net=snouttime-tier-test
minio=snouttime-minio
cleanup() { "$engine" rm -fv "$minio" >/dev/null 2>&1 || true; "$engine" network rm "$net" >/dev/null 2>&1 || true; }
trap cleanup EXIT
cleanup
"$engine" network create "$net" >/dev/null
"$engine" run -d --name "$minio" --network "$net" -e MINIO_ROOT_USER=snouttime -e MINIO_ROOT_PASSWORD=snouttime-secret \
	quay.io/minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e server /data >/dev/null
for _ in $(seq 1 60); do
	"$engine" run --rm --network "$net" --entrypoint sh quay.io/minio/mc@sha256:a7fe349ef4bd8521fb8497f55c6042871b2ae640607cf99d9bede5e9bdf11727 -c \
		"mc alias set m http://$minio:9000 snouttime snouttime-secret >/dev/null && mc mb -p m/tiered >/dev/null" && break
	sleep 1
done
# credentials in the environment: in settings they need snouttime preloaded (see s3_config)
SNOUTTIME_DEV_ARGS="--network $net -e SNOUTTIME_TEST_S3_ENDPOINT=http://$minio:9000 -e SHOW=${SHOW:-} -e PGV=${PGV:-17} -e AWS_ACCESS_KEY_ID=snouttime -e AWS_SECRET_ACCESS_KEY=snouttime-secret" \
	bash "$root/scripts/dev.sh" bash tests/tier/inner.sh
