#!/usr/bin/env bash
# Run one command inside the SnoutTime dev container.
#
#   bash scripts/dev.sh cargo pgrx test pg17
#
# The image is tagged by a hash of the Containerfile, so editing it rebuilds on the
# next run and an unchanged one is reused. Build output and the cargo registry live in
# named volumes: a bind-mounted target directory on Windows is slow enough to matter.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(cd "$here/.." && pwd)"
engine="${SNOUTTIME_ENGINE:-docker}"

tag="$(sha256sum "$root/container/Containerfile" | cut -c1-12)"
image="snouttime-dev:$tag"

if ! "$engine" image inspect "$image" >/dev/null 2>&1; then
	echo "building $image (first run after a Containerfile change takes a while)" >&2
	"$engine" build -t "$image" -f "$root/container/Containerfile" "$root/container" >&2
fi

# Docker Desktop on Windows wants a Windows path for a bind mount.
src="$root"
if command -v cygpath >/dev/null 2>&1; then
	src="$(cygpath -w "$root")"
fi

tty=()
if [ -t 0 ] && [ -t 1 ]; then
	tty=(-it)
fi

# Extra arguments for docker run (a test that needs a network, an environment variable).
read -r -a extra <<<"${SNOUTTIME_DEV_ARGS:-}"

MSYS_NO_PATHCONV=1 exec "$engine" run --rm "${tty[@]}" "${extra[@]}" \
	-v "$src:/work" \
	-v snouttime-target:/cache/target \
	-v snouttime-registry:/usr/local/cargo/registry \
	"$image" "$@"
