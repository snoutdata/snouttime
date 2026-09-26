# Contributing to SnoutTime

Issues and pull requests are welcome. Run `bash scripts/test.sh` before sending one; it needs
only Docker or Podman. The rules below are what every change follows.

## R1: Clean room

No Timescale Licence code is read, ported, translated or rearranged. A different arrangement of
their code is still their code: whether something is a derivative work depends on what was
copied, not on how different the result looks. The Apache-2.0 part of TimescaleDB is not copied
either, because it would drag a second licence and a NOTICE file into the package for no gain.
Hydra columnar and Citus columnar are AGPL and are not read.

**Free to use:** published papers; public documentation of what a feature does; the Postgres
source, its docs and its APIs; code under PostgreSQL, MIT, BSD, ISC or Apache-2.0 licences
(pg_partman is PostgreSQL-licensed and may be read; pgrx is MIT/Apache).

## R2: Ideas are free; only code and marks are not

Time series is decades older than TimescaleDB, and an idea is not their IP. Never design away from
what TimescaleDB does just because they do it: if their approach, behaviour or naming is the
natural one, use it. Every decision is made on technical merit alone. R1 protects their code;
this rule protects their trademarks (`TimescaleDB`, `hypertable`), which are not used as our names.
Conventional function names (`time_bucket`-style, `first`, `last`, `locf`, `interpolate`) are
fine where they are the obvious name.

## R3: Self-contained package

SnoutTime has its own `Cargo.toml` (not a workspace member of anything), its own build
container, its own scripts and its own README, and depends on nothing outside this repository.

## R4: Every dependency must be compatible with MIT

Enforced by `cargo deny check licenses` (`deny.toml`). GPL, AGPL, LGPL, SSPL, BSL and
source-available licences fail the build.

## R5: A corrupt block errors, it never crashes

Every decoder takes untrusted bytes. A bad length, a truncated block or a garbage dictionary
index returns a Postgres `ERROR` naming the relation and block; it never panics a backend and
never reads out of bounds.

## R6: Claims discipline

Nothing outside the repo says "faster than TimescaleDB", "drop-in replacement" or "compatible
with TimescaleDB" unless the benchmark harness has measured it and the number is recorded in the
plan.

## R7: This work is published from

It becomes a paper, so every measured number carries its date, hardware, dataset, scale and the
exact command that produced it, and results where SnoutTime loses are kept. Numbers we chose
(defaults) are recorded apart from numbers we measured and are never quoted as measurements.
