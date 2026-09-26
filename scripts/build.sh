#!/usr/bin/env bash
# Build the extension for Postgres 17.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec bash "$here/dev.sh" cargo build --features pg17
