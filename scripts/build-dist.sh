#!/usr/bin/env bash
# Builds SnoutTime for one Postgres major and puts what an installation needs under <out>:
#
#   <out>/lib/snouttime.so                      stripped
#   <out>/extension/snouttime.control, snouttime--<version>.sql
#   <out>/debug/snouttime.so.debug              its symbols, for backtraces; not shipped
#
#   bash scripts/build-dist.sh 17 /out        # inside a rust:<version>-bookworm container
#
# This is the one recipe for a SnoutTime build that ships (PLAN.md Phase 7): the SnoutData Cloud
# pod image runs it in a build stage, with this package as a named build context, so the pod
# image depends on this package's build output and never the reverse (R3). It expects Debian
# bookworm with the Rust toolchain rust-toolchain.toml names, and fetches the rest: the pgdg
# server headers for the major, and cargo-pgrx at the version Cargo.toml pins.
set -euo pipefail

major="${1:?usage: scripts/build-dist.sh <postgres major> <out dir>}"
out="${2:?usage: scripts/build-dist.sh <postgres major> <out dir>}"
src="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

case "$major" in
	17 | 18) ;;
	*) echo "SnoutTime supports Postgres 17 and 18, not $major" >&2; exit 2 ;;
esac

# The toolchain this image has must be the one the package pins: a different rustc builds a
# different binary from the same source, and nothing downstream would notice.
want="$(sed -n 's/^channel *= *"\(.*\)"/\1/p' "$src/rust-toolchain.toml")"
have="$(rustc --version | awk '{print $2}')"
if [ "$want" != "$have" ]; then
	echo "rust-toolchain.toml pins $want; this image has rustc $have" >&2
	exit 1
fi
pgrx="$(sed -n 's/^pgrx *= *"=\{0,1\}\([0-9.]*\)".*/\1/p' "$src/Cargo.toml" | head -1)"
if [ -z "$pgrx" ]; then
	echo "could not read the pgrx version from Cargo.toml" >&2
	exit 1
fi

export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y --no-install-recommends build-essential clang libclang-dev pkg-config ca-certificates curl gnupg
install -d /usr/share/postgresql-common/pgdg
curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc
echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] https://apt.postgresql.org/pub/repos/apt bookworm-pgdg main" \
	>/etc/apt/sources.list.d/pgdg.list
apt-get update
apt-get install -y --no-install-recommends "postgresql-server-dev-${major}"
rm -rf /var/lib/apt/lists/*

pg_config="/usr/lib/postgresql/${major}/bin/pg_config"
cargo install --locked cargo-pgrx --version "$pgrx"
cargo pgrx init "--pg${major}" "$pg_config"

# A copy to build in, so the build context stays read-only. `cargo pgrx install --release` is
# what bench/container/Containerfile does, so the build that is benchmarked is the build that
# ships.
work="$(mktemp -d)"
# sql/ holds the upgrade scripts (snouttime--<from>--<to>.sql), which `cargo pgrx install` copies
# beside the generated install script; without them an existing database cannot ALTER EXTENSION.
cp -r "$src/Cargo.toml" "$src/Cargo.lock" "$src/rust-toolchain.toml" "$src/snouttime.control" "$src/src" "$src/sql" "$work/"
(cd "$work" && cargo pgrx install --release --pg-config "$pg_config" --no-default-features --features "pg${major}")

mkdir -p "$out/lib" "$out/extension" "$out/debug"
# Shipped stripped: the symbol table names every function and type, which a binary in a private
# image has no need to carry, and it is most of the file. The full symbols go to <out>/debug,
# which the pod image does not copy: keep it beside the image's digest to read a backtrace.
so="$("$pg_config" --pkglibdir)/snouttime.so"
objcopy --only-keep-debug "$so" "$out/debug/snouttime.so.debug"
strip --strip-unneeded -o "$out/lib/snouttime.so" "$so"
cp "$("$pg_config" --sharedir)/extension/"snouttime* "$out/extension/"
ls -l "$out/lib" "$out/extension"
