#!/usr/bin/env bash
# Fuzz every decoder of untrusted bytes (docs/snouttime/PLAN.md 3.2, R5).
#
#   bash scripts/fuzz.sh [seconds per target, default 60] [target ...]
#
# All targets run at once, each for the given time, in a container with the pinned nightly
# (container/fuzz.Containerfile). A crash leaves its input in fuzz/artifacts/<target>/ and
# fails the script. Corpora are kept in fuzz/corpus/ between runs (ignored by git).
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(cd "$here/.." && pwd)"
engine="${SNOUTTIME_ENGINE:-docker}"
seconds="${1:-60}"
shift || true
targets=("$@")
if [ ${#targets[@]} -eq 0 ]; then
	targets=(int float dict bitmap compress group meta)
fi
hash_of() { if command -v sha256sum >/dev/null 2>&1; then sha256sum "$@"; else shasum -a 256 "$@"; fi; }

bash "$here/dev.sh" true # builds the dev image when its Containerfile changed
dev="snouttime-dev:$(hash_of "$root/container/Containerfile" | cut -c1-12)"
image="snouttime-fuzz:$(cat "$root/container/Containerfile" "$root/container/fuzz.Containerfile" | hash_of | cut -c1-12)"
if ! "$engine" image inspect "$image" >/dev/null 2>&1; then
	echo "building $image" >&2
	"$engine" build --build-arg DEV_IMAGE="$dev" -t "$image" -f "$root/container/fuzz.Containerfile" "$root/container" >&2
fi

"$engine" run --rm -v "$root:/work" -v snouttime-fuzz-target:/cache/fuzz-target \
	-v snouttime-registry:/usr/local/cargo/registry -e CARGO_TARGET_DIR=/cache/fuzz-target \
	-w /work/fuzz "$image" bash -c '
	set -euo pipefail
	seconds="$1"; shift
	cargo +"$SNOUTTIME_NIGHTLY" fuzz build -O >&2 # every target
	bin="/cache/fuzz-target/$(rustc +"$SNOUTTIME_NIGHTLY" -vV | sed -n "s/^host: //p")/release"
	for t in "$@"; do
		mkdir -p "corpus/$t" "artifacts/$t"
		"$bin/$t" "corpus/$t" -max_total_time="$seconds" -artifact_prefix="artifacts/$t/" \
			-print_final_stats=1 >"artifacts/$t.log" 2>&1 &
	done
	failed=0
	for t in "$@"; do wait -n || failed=1; done
	for t in "$@"; do
		runs="$(grep -m1 "stat::number_of_executed_units" "artifacts/$t.log" | awk "{print \$2}")"
		# find, not ls: a glob that matches nothing made ls fail and hid a real crash once
		if [ -n "$(find "artifacts/$t" -type f \( -name "crash-*" -o -name "oom-*" -o -name "timeout-*" -o -name "leak-*" \))" ]; then
			echo "FAIL $t: $(ls "artifacts/$t")"; failed=1
		else
			echo "ok   $t: ${runs:-?} inputs in '"$seconds"' s, no crash"
		fi
	done
	exit $failed
' fuzz "$seconds" "${targets[@]}"
