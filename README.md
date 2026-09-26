# SnoutTime

A time-series extension for Postgres, written in Rust with [pgrx](https://github.com/pgcentralfoundation/pgrx).
Time partitioning on Postgres's own declarative partitions, columnar compression of closed
partitions, incrementally maintained rollups, retention, tiering to object storage, and as-of
joins.

SnoutTime runs in every [SnoutData Cloud](https://snoutdata.com/cloud) project
(`CREATE EXTENSION snouttime`), and its source is here under the Apache License 2.0. Series
tables, the time functions, sketches, as-of and window joins, sealed (columnar) partitions,
rollups and tiering work and are tested on Postgres 17 and 18. Documentation for using it is at
[docs.snoutdata.com/cloud/timeseries](https://docs.snoutdata.com/cloud/timeseries/overview); the
rules every change follows are in `CONTRIBUTING.md`.

It is developed in SnoutData's own repository and mirrored here, one commit for each upstream
commit. Comments in the code cite `PLAN.md`, the project's internal plan: a decision number (D3),
a phase (3.6) or a date points there, and is kept as written so the code stays the same on both
sides. Issues and pull requests are welcome here; an accepted pull request is applied upstream
with you as co-author and comes back through the mirror.

## Time buckets

`snouttime.bucket(width, time)` is the start of the `width`-sized interval that holds `time`.

```sql
SELECT snouttime.bucket('15 minutes', ts), avg(v) FROM metrics GROUP BY 1;
SELECT snouttime.bucket('1 day', ts, 'Europe/Berlin'), sum(v) FROM metrics GROUP BY 1;
SELECT snouttime.bucket('1 month', ts, origin => timestamptz '2000-01-15 00:00+00') ...
```

| Form | Returns |
|---|---|
| `bucket(width interval, ts timestamptz [, origin timestamptz])` | `timestamptz`, bucketed in UTC |
| `bucket(width interval, ts timestamptz, timezone text [, origin timestamp])` | `timestamptz`, bucketed on the local clock of `timezone` |
| `bucket(width interval, ts timestamp [, origin timestamp])` | `timestamp`, as written |
| `bucket(width interval, day date [, origin date])` | `date`; the width must be whole days or months |
| `bucket(width bigint, value bigint [, offset bigint])`, and the same for `integer` | the column's own units |

The rules:

- **Without a time zone, a `timestamptz` is bucketed in UTC**, whatever the session's
  `TimeZone`, which is how series partitions are aligned too, so a query gives the same buckets in
  every session. Pass a zone to bucket on a local clock.
- **A width is months (and years) or days and time, never both.** `'1 month 1 day'` has no fixed
  length and no obvious meaning, so it is an error, as it is for a partition interval.
- **Month widths are calendar arithmetic that never drifts.** Bucket *k* starts at *origin + k ×
  width* months, with the day clamped to the month's length and always counted from the origin, so
  an origin on the 31st gives Jan 31, Feb 29, Mar 31, Apr 30, May 31.
- **The default origin** is Monday 2000-01-03 for day and time widths, so `'1 week'` starts on
  Mondays, and 2000-01-01 for month widths. Times before the origin round down, never towards it.
- **With a time zone, whole days and months start at local midnight.** They equal Postgres's own
  `date_trunc(unit, ts, zone)`, so a day bucket over a DST change is 23 or 25 hours long.
- **With a time zone, any other width** gives the latest instant at or before the time whose
  local clock reads a grid time (*origin + k × width*, local). So bucketing is monotone and never
  after the time, and every bucket starts on the local grid: in the hour that happens twice there
  are two hourly buckets both labelled 01:00, the hour that never happens has none, and a bucket
  beside a change is longer or shorter by the shift. On Lord Howe Island, whose clocks move by 30
  minutes, hourly buckets still start on the local hour.
- `NULL` gives `NULL`; `infinity` and `-infinity` are returned unchanged; a result outside
  Postgres's range is an error.

Every rule is checked by `tests/pg_regress/sql/bucket.sql`, mostly as properties over tens of
thousands of times in New York, Berlin and Lord Howe.

## Filling gaps

`snouttime.gapfill(width, start, finish [, timezone] [, origin])` returns every bucket that
overlaps `[start, finish)`, in order, with the same rules as `bucket`. The first is the bucket
`start` falls in, and each one after is the next bucket however long it is, so months are months
and the hour that happens twice is two rows. LEFT JOIN onto it to get a row for every bucket:

```sql
SELECT b, avg(m.v),
       snouttime.locf(avg(m.v)) OVER (ORDER BY b),
       snouttime.interpolate(avg(m.v), b) OVER (ORDER BY b)
FROM snouttime.gapfill('1 hour', now() - interval '1 day', now()) AS b
LEFT JOIN metrics m ON snouttime.bucket('1 hour', m.ts) = b
GROUP BY b ORDER BY b;
```

Two window functions fill what is empty:

- `locf(value)`, any type: the value, or the last non-NULL value before it in the window's order.
  NULL before the first one.
- `interpolate(value double precision)`: the value, or the straight line between the non-NULL
  values either side of it, by row position. `interpolate(value, at timestamptz)` does the same
  by each row's time, which is what buckets of unequal length need (between Jan 1 and Apr 1, Feb 1
  is 31/90 of the way, not 1/3). NULL where one side has no value.

Like `lag` and `lead` they look at the whole partition, so a frame clause does not change them,
and `PARTITION BY` keeps one series from filling another. Each row costs the same however long a
gap is: nothing is ever re-read.

## first and last

`snouttime.first(value, at)` and `snouttime.last(value, at)` are the value at the earliest and
latest `at` in a group: `SELECT host, snouttime.last(cpu, ts) FROM metrics GROUP BY host`.

- `value` is any type; `at` is `timestamptz`, `timestamp`, `date`, `bigint` or `integer`.
- A row whose `at` is NULL is ignored. A NULL `value` counts: if it is the earliest, `first` is NULL.
- Rows with the same `at` are a tie and which one wins is not defined, since a parallel plan does
  not fix the order rows arrive in. Make `at` unique to break it.
- They run in parallel (partial states in workers, combined in the leader), and the partial state
  merges, which is what rollups will build on.

## histogram

`snouttime.histogram(value, min, max, buckets)` counts values into the slots Postgres's own
`width_bucket(value, min, max, buckets)` numbers: a `bigint[]` of `buckets + 2`, where the first
is below `min`, the last is `max` and above (NaN too, where `width_bucket` would raise an error),
and the ones between split `[min, max)` evenly. NULL values are not counted; `min`, `max` and
`buckets` must be the same on every row of a group. It runs in parallel, and the counts merge by
adding.

## Counters

`snouttime.counter_delta(value, at ORDER BY at)` is how much a monotonic counter went up over a
group, reading a drop as a reset to zero (100, 130, 20, 50 is +80, not -50).
`snouttime.counter_rate(value, at ORDER BY at)` is that per second between the first and last
point; NULL with fewer than two points.

- The points must be in time order, hence `ORDER BY at`: one earlier than the one before it is an
  error that says so, never a wrong number. NULL values or times are skipped.
- The state merges across time ranges that do not overlap, joined at each boundary with the same
  reset rule, which is what rolling adjacent buckets up does. Partitionwise aggregation over a
  table partitioned by time uses it now. Parallel workers do not (they read rows in no time order),
  so the aggregate is `PARALLEL RESTRICTED`.

## Sketches

Two approximate aggregates whose partial states merge, so a sketch per hour can become a sketch
per day without the rows (and, in Phase 4, a rollup can keep them).

**Percentiles: `tdigest`.** `snouttime.percentile_sketch(value [, compression])` builds a merging
t-digest (Dunning and Ertl, 2019); `snouttime.percentile(sketch, q)` estimates the q-quantile, and
takes a `q[]` too; `snouttime.merge(sketch)` merges many into one; `snouttime.sketch_count(sketch)`
is how many values it saw. NULL and NaN are skipped. Compression is 10 to 10,000, default 100.

**Distinct counts: `hll`.** `snouttime.distinct_sketch(value [, bits])` is a HyperLogLog of 2^bits
registers (bits 4 to 18, default 12) over the value type's own 64-bit hash, so any hashable type
works; `snouttime.distinct_count(sketch)` estimates, with Ertl's improved estimator (2017), which
needs no bias tables and is accurate from a handful of values up; `snouttime.merge(sketch)` is
exactly the sketch of the union. Sketches of different `bits` do not merge.

How wrong they are, measured against exact answers (2026-09-23):

| Sketch | Size | Error |
|---|---|---|
| `tdigest`, compression 100 | ~70 centroids, ~1 KB | rank error at most 0.17% at any of nine quantiles on 1,000,000 values from three distributions; at most 0.031% at the 0.1% and 99.9% tails |
| `tdigest`, compression 500 | ~300 centroids, ~4 KB | rank error at most 0.012% |
| `hll`, 12 bits (default) | 4 KB | 0.8-1.7% RMSE from 100 to 1,000,000 distinct values (theory: 1.62%) |
| `hll`, 14 bits | 16 KB | 0.3-0.8% RMSE (theory: 0.81%) |
| `hll`, 16 bits | 64 KB | 0.25-0.32% RMSE (theory: 0.41%) |

Rank error is |F(estimate) − q|: the estimate for the median of a million values being off by
0.17% means it sits between the 49.83rd and 50.17th percentiles. The HyperLogLog RMSEs are over 10
trials each, so they scatter around the theory rather than match it. Both types have a JSON text
form, and anything read from text or from disk is validated before use.

## As-of join

For each row on the left, the latest row on the right with the same keys at or before its time
(kdb+'s `aj`). The SQL way is a `LATERAL` subquery with `ORDER BY time DESC LIMIT 1`, one index
probe per left row; `snouttime.asof_join` reads both sides once, in order, and merges them.

```sql
SELECT * FROM snouttime.asof_join(
    'SELECT host, ts, kind FROM events',          -- left_query
    'SELECT host, ts, usage_user FROM cpu',       -- right_query
    keys => ARRAY['host'], left_time => 'ts', right_time => 'ts',
    within => interval '5 minutes')
  AS (host text, ts timestamptz, kind int, r_host text, r_ts timestamptz, usage_user float8);
```

The semantics, which the regression test `asof` holds it to:

- **The result is every left row once**, followed by the columns of its matching right row, or by
  NULLs where there is none (a left outer join). The columns are all of the left query's, then all
  of the right query's, and the column list after `AS` must say so: that is how a function returning
  `record` is typed in SQL.
- **The match is the right row with equal keys and the latest time at or before the left row's**
  (`direction => 'backward'`, the default). **Equal times match.** `direction => 'forward'` takes the
  earliest at or after instead.
- **`within`** is the furthest a match may be from the left row's time; beyond it there is none.
  It needs a `timestamptz`, `timestamp` or `date` time, and no months (a month has no fixed length).
- **NULLs:** a left row with a NULL key or time matches nothing and is still returned. A right row
  with a NULL key or time is never a match. Keys compare as SQL `=` does, so NULL never equals NULL.
- **Ties on the right** (several rows with the same keys and time): which one is the match is not
  defined. Make the time unique, or pre-aggregate the right side, to decide it.
- **Order:** rows come out ordered by the left's keys and time, not in the left query's order.
- Keys may be several columns of any type with a default btree ordering; each must be the same
  type and collation on both sides, as must the two time columns.
- The queries run as the caller, read-only, exactly as if typed at the prompt.

## Window join

For each left row, an aggregate of the right rows with the same keys in a window of time around
it (kdb+'s `wj`):

```sql
SELECT * FROM snouttime.window_join(
    'SELECT host, ts, kind FROM events', 'SELECT host, ts, usage_user FROM cpu',
    keys => ARRAY['host'], left_time => 'ts', value => 'usage_user',
    before => interval '5 minutes', after => interval '0', aggregate => 'max')
  AS (host text, ts timestamptz, kind int, max_usage float8);
```

- The window is `[time − before, time + after]`, both ends included; `after` defaults to 0.
- `aggregate` is `count`, `sum`, `avg` (the default), `min`, `max`, `first` or `last`, over the
  right query's `value` column, which must be `double precision` (cast it in the query).
- NULL values are not in the window (as SQL's `count(value)` does not count them). An empty window
  is NULL, or 0 for `count`. A left row with a NULL key or time gets the same.
- The result is every left row once, its columns followed by the aggregate, in (keys, time) order.
  Keys and times follow the as-of join's rules.
- Both sides are read once, in order, and each right row enters and leaves the window once:
  `sum` and `avg` keep a running sum that is recomputed from the window regularly so floating
  point drift cannot build up, and `min` and `max` keep monotonic deques.

## Sealed partitions

A partition whose time has passed can be SEALED: rewritten into `snouttime_columnar`, a table
access method that stores it column by column, encoded (delta-of-delta integers and times, XOR
floats, dictionaries for repeated text) and compressed (LZ4 by default, or zstd), with a
checksum on every block.

```sql
-- by the worker: seal each partition a day after its range ends, sorted by host then time
SELECT snouttime.set_sealing('cpu', interval '1 day', codec => 'lz4', order_by => '{host,ts}');
-- or by hand
SELECT snouttime.seal('cpu_p20260101');
SELECT snouttime.unseal('cpu_p20260101');   -- back to heap
SELECT snouttime.reseal('cpu_p20260101');   -- fold in late rows and deletes
```

- A sealed partition answers every query a heap one does, through the same SQL. Queries on it
  read only the columns they use, and skip every group of rows whose minimum and maximum cannot
  match the WHERE clause (EXPLAIN shows "Row Groups Skipped"), in parallel when it pays.
- It is its own index on its sort key (`order_by`). `WHERE host = 'a' AND ts <= $1 ORDER BY ts
  DESC LIMIT 1` finds its row by binary search, as a btree on `(host, ts)` would, and rows come
  back in that order either way, so the usual SQL for "the last reading before" is fast without
  an index (EXPLAIN shows "Sort Key Seek" and "Order"). An IN list on the sort key
  (`host IN ('a', 'b')`) is sought one value at a time, and an aggregate over a few keys
  (`SELECT max(v) ... WHERE host IN (...) AND ts > now() - interval '1 hour'`) is computed on
  the rows the seek finds, decoding only the 1,024-row pages they are in (0.1.3).
- So its non-unique indexes hold only the rows written after the seal, and the planner uses
  them only for those: a sealed partition is not made larger by an index its sort key already
  is. Unique indexes and exclusion constraints cover every row and keep enforcing. To keep
  every index whole (for lookups by a column outside the sort key), seal with
  `set_sealing(..., keep_indexes => true)`.
- It still takes writes. A late INSERT goes to a small heap beside it (its delta store), and a
  DELETE or UPDATE is recorded in its delete log; both are ordinary MVCC, so rollback and
  snapshots behave as on heap. The worker reseals a partition when these reach a tenth of it.
  An UPDATE that races another UPDATE of the same row fails with the error Postgres gives for a
  row moved to another partition, and the client retries: it never silently updates nothing.
- It is WAL-logged, so it survives a crash and is identical on a streaming replica and after a
  point-in-time restore. `pg_dump` dumps its rows; a restore brings it back as a sealed table.
- `snouttime.column_sizes(partition)` says what each column costs sealed: per column and
  encoding (integer, float, bool, dictionary, plain), how many row groups, rows and nulls, and
  the bytes its chunks take as stored. It reads the row groups' headers only (0.1.2).

  ```sql
  SELECT attname, encoding, stored_bytes FROM snouttime.column_sizes('cpu_p20260101');
  ```
- Not supported on a sealed partition, each refused with a sentence: BRIN indexes (every block
  already carries its minimum and maximum), `CREATE INDEX CONCURRENTLY` and `TABLESAMPLE`.

## Rollups

An aggregate over time buckets, kept up to date by recomputing only what changed:

```sql
SELECT snouttime.create_rollup('cpu_hourly', 'cpu', interval '1 hour',
    select_list => 'host, max(usage) AS max_usage, count(*) AS n, sum(usage) AS total',
    group_by => 'host');
SELECT * FROM cpu_hourly WHERE bucket >= now() - interval '1 day';
```

- Every column in `group_by` must also be in `select_list`, as `host` is above, or the rollup's
  rows could not be told apart; `create_rollup` refuses it with a sentence saying which (since
  0.1.1). A `group_by` with a function call in it is not checked.
- `cpu_hourly` is a view. It returns materialized buckets where nothing has changed, and computes
  the rest from the raw rows: the buckets since the last refresh, and any bucket a late, updated
  or deleted row has touched since. So it is never stale, only partly materialized.
- The worker refreshes it every minute (`snouttime.refresh_rollup` does it by hand), recomputing
  only the buckets something changed, found by statement-level triggers on `cpu`.
- A rollup of a rollup (`create_rollup('cpu_daily', 'cpu_hourly', interval '1 day', ...)`) merges
  the source's aggregates, so only mergeable ones are accepted: `max(max_usage)`,
  `sum(total) / sum(n)` rather than `avg`, `merge()` of sketches rather than percentiles.
- `snouttime.set_rollup_retention('cpu_hourly', interval '1 year')` keeps a rollup longer (or
  shorter) than its source. When retention drops raw partitions, the rollup keeps their buckets.
- A write straight into one partition, rather than through the series table, is not seen.

## Tiering to S3

A partition nobody reads often can go to object storage (AWS S3 or anything that speaks its
API: MinIO, Cloudflare R2):

```sql
ALTER SYSTEM SET snouttime.tier_to = 's3://my-bucket/snouttime';
-- credentials: snouttime.s3_access_key_id / s3_secret_access_key (superuser-only), or the
-- server's AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY; snouttime.s3_endpoint for non-AWS stores
SELECT snouttime.set_tiering('cpu', interval '90 days');   -- by the worker
SELECT snouttime.tier('cpu_p20260101');                     -- or by hand
SELECT snouttime.recall('cpu_p20260101');                   -- and back
```

- A tiered partition is its sealed column store, uploaded as one object; only its metapage and
  its directory of row groups (with every column's minimum and maximum) stay in the database.
  Queries work as before, fetching from S3 only the row groups they cannot skip.
- It still takes late writes, deletes and updates, as a sealed partition does.
- `snouttime.tier_gc()` deletes objects no partition points at any more (after a recall, or a
  second tiering). The worker calls it only with `snouttime.tier_gc = on`, because a restored
  copy of the database may still point at them.

## Versions and upgrades

A database keeps the catalog of the version it was created with until it runs
`ALTER EXTENSION snouttime UPDATE`, which moves it to the version the installed library
carries, through every script in between (`sql/snouttime--<from>--<to>.sql`). SnoutData Cloud
runs that for you after a pod starts on a newer image. Each step is tested from every earlier
version to a catalog identical to a fresh install (`tests/upgrade/extension.sh`).

**There is no downgrade.** `ALTER EXTENSION snouttime UPDATE TO` an older version fails, because
no script goes backwards, and none will: a newer catalog may hold what an older one cannot read.
The way back is a restore from a backup taken before the update (in SnoutData Cloud, a
point-in-time restore from the project's Settings).

## Settings

| Setting | Default | Range | Effect |
|---|---|---|---|
| `snouttime.databases` | none | comma-separated database names | Where the preloaded worker runs jobs. Read once at server start; needs `snouttime` in `shared_preload_libraries`. |
| `snouttime.interval` | `10` | 1 to 3600 seconds | Time between the worker's passes over the job list. |
| `snouttime.columnar_compression` | `lz4` | `none`, `lz4`, `zstd` | How a new column store's blocks are compressed. `seal()` sets it from the series table's codec. |
| `snouttime.columnar_order_by` | empty | column names, comma-separated | Order of rows within a new column store. `seal()` sets it to the space key, then time. |
| `snouttime.columnar_group_rows` | `8192` | 1 to 65536 | Rows per row group: the unit decoded, skipped and checksummed together. |
| `snouttime.columnar_keep_indexes` | `off` | boolean | Whether a new column store's non-unique indexes cover every row (on) or only rows written after it (off). `seal()` sets it from `set_sealing`'s `keep_indexes`. |
| `snouttime.plan_time_bounds` | `on` | boolean | For queries over a table with sealed partitions: `ts >= <constant> - interval '1 hour'` is computed while planning, so only the partitions it can reach are planned (Postgres leaves it to run time and plans them all). With days or months in the interval, the comparison is kept and a bound it implies in every time zone is added. |
| `snouttime.columnar_custom_scan` | `on` | boolean | Whether sealed partitions may be read by SnoutTime's own scans: the column scan (only the columns used, row groups skipped) and the last-point scan for `DISTINCT ON (key) ... ORDER BY key, time`. |
| `snouttime.columnar_aggregate` | `on` | boolean | Whether aggregates over sealed partitions may be computed on their decoded columns, as the partial half of Postgres's two-phase aggregate (`count`, `sum`, `avg`, `min`, `max`; grouping by columns and time buckets). |
| `snouttime.partitionwise_aggregate` | `on` | boolean | Whether `enable_partitionwise_aggregate` is turned on while a query over a table with sealed partitions is planned, so each sealed partition can be aggregated on its columns. It never turns that setting off. | Takes effect once the library is loaded: preload SnoutTime (`shared_preload_libraries` or `session_preload_libraries`), or a session's first query is planned without it.
| `snouttime.tier_to` | none | `s3://bucket/prefix` | Where tiered partitions go. Superuser only. |
| `snouttime.s3_endpoint` | `AWS_ENDPOINT_URL`, then AWS S3 in the region | URL | The S3 endpoint, for any S3-compatible store. Superuser only. |
| `snouttime.s3_region` | `AWS_REGION`, then `us-east-1` | region name | The region requests are signed for. Superuser only. |
| `snouttime.s3_access_key_id`, `s3_secret_access_key`, `s3_session_token` | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` | text | Credentials. Superuser only, never shown to anyone else, and refused unless `snouttime` is in `shared_preload_libraries` (before it loads, Postgres would show them to any user). |
| `snouttime.tier_gc` | `off` | boolean | Whether the tier job deletes objects no partition points at. Off because a restored copy of the database may still point at them. |

## Building and testing

Everything runs in a Linux container, so the only thing a machine needs is Docker (or Podman,
with `SNOUTTIME_ENGINE=podman`). The first run builds the toolchain image, which compiles
Postgres 17 with assertions on and takes several minutes; after that it is reused until
`container/Containerfile` changes.

```
bash scripts/build.sh            # compile the extension
bash scripts/test.sh             # licences, Rust tests (plain + in-database), SQL regression
bash scripts/test.sh <filter>    # only the Rust tests whose name contains <filter>
bash scripts/dev.sh <command>    # anything else, inside the container
```

SQL regression tests are `tests/pg_regress/sql/<name>.sql` with the expected output in
`tests/pg_regress/expected/<name>.out`.

## Benchmarks

The benchmark harness and its records are part of our research and are published with it, at
[snoutdata.com/research](https://snoutdata.com/research), rather than kept in this repository.

## License

Apache License 2.0 (`LICENSE`, `NOTICE`). SnoutTime was written from scratch; `CONTRIBUTING.md`,
R1, is the clean-room rule it was written under.
