-- What is there, and what it costs.
--
-- Both views read the catalogs and the planner's own statistics only. Nothing here scans a
-- table, so asking what a series table looks like is cheap however much data it holds, and
-- the row counts are Postgres's estimates (`reltuples`, as of the last ANALYZE), never a
-- count(*). A view that took a minute on a big table would not get looked at.

-- One row per partition, including the default one and any partition somebody else made.
CREATE VIEW snouttime.partition_info AS
SELECT
	s.relid AS series,
	c.oid AS partition,
	c.relname AS name,
	CASE
		WHEN c.oid = snouttime._default_partition(s.relid) THEN 'default'
		WHEN b.lo IS NULL THEN 'foreign'   -- a partition SnoutTime did not make and will not touch
		-- rewritten into the column store; with a space key, every leaf is
		WHEN EXISTS (SELECT 1 FROM pg_partition_tree(c.oid) t WHERE snouttime._is_tiered(t.relid))
			AND NOT EXISTS (SELECT 1 FROM pg_partition_tree(c.oid) t JOIN pg_class l ON l.oid = t.relid
				WHERE l.relkind = 'r' AND NOT snouttime._is_tiered(t.relid)) THEN 'tiered'
		WHEN EXISTS (SELECT 1 FROM pg_partition_tree(c.oid) t WHERE snouttime._is_sealed(t.relid))
			AND NOT EXISTS (SELECT 1 FROM pg_partition_tree(c.oid) t JOIN pg_class l ON l.oid = t.relid
				WHERE l.relkind = 'r' AND NOT snouttime._is_sealed(t.relid)) THEN 'sealed'
		WHEN c.relkind = 'p' THEN 'spread' -- hash-partitioned by the space key
		ELSE 'live'
	END AS state,
	b.lo AS range_start,
	b.hi AS range_end,
	-- Summed over the whole tree under this partition: with a space key a partition is
	-- itself partitioned, and a partitioned table has no storage and no rows of its own.
	(SELECT coalesce(sum(greatest(leaf.reltuples, 0)), 0)::int8
	 FROM pg_partition_tree(c.oid) AS t
	 JOIN pg_class leaf ON leaf.oid = t.relid AND leaf.relkind <> 'p') AS estimated_rows,
	(SELECT coalesce(sum(pg_total_relation_size(t.relid)), 0)
	 FROM pg_partition_tree(c.oid) AS t) AS bytes,
	(SELECT count(*) FROM pg_inherits h WHERE h.inhparent = c.oid)::int AS children,
	-- what its leaves took as heap just before they were sealed; NULL for a partition never
	-- sealed. With `bytes` it is the compression a seal bought.
	(SELECT sum(z.bytes_before)
	 FROM pg_partition_tree(c.oid) AS t
	 JOIN snouttime.seal_sizes z ON z.relid = t.relid)::int8 AS bytes_before
FROM snouttime.series s
JOIN pg_inherits i ON i.inhparent = s.relid
JOIN pg_class c ON c.oid = i.inhrelid
LEFT JOIN LATERAL snouttime._bounds_of(s, c.oid) AS b ON true;

COMMENT ON VIEW snouttime.partition_info IS
	'One row per partition of every series table. Estimates from the planner, no scans.';

-- One row per series table.
CREATE VIEW snouttime.series_info AS
SELECT
	s.relid AS series,
	s.time_column,
	s.time_type,
	coalesce(s.partition_interval::text, s.partition_width::text) AS partition_size,
	s.space_column,
	s.space_partitions,
	coalesce(s.retention::text, s.retention_width::text) AS retention,
	count(*) FILTER (WHERE p.state <> 'default')::int AS partitions,
	count(*) FILTER (WHERE p.state = 'foreign')::int AS foreign_partitions,
	-- The bounds are text (timestamps and integer keys alike), so they are ordered as what they
	-- are: min() on text put '9000' after '10000' on an integer series.
	(array_agg(p.range_start ORDER BY
		CASE WHEN s.partition_width IS NOT NULL THEN p.range_start::numeric END,
		CASE WHEN s.partition_width IS NULL THEN p.range_start::timestamptz END)
		FILTER (WHERE p.state <> 'default' AND p.range_start IS NOT NULL))[1] AS oldest_range,
	(array_agg(p.range_end ORDER BY
		CASE WHEN s.partition_width IS NOT NULL THEN p.range_end::numeric END DESC,
		CASE WHEN s.partition_width IS NULL THEN p.range_end::timestamptz END DESC)
		FILTER (WHERE p.state <> 'default' AND p.range_end IS NOT NULL))[1] AS newest_range,
	coalesce(sum(p.estimated_rows) FILTER (WHERE p.state = 'default'), 0) AS rows_in_default,
	coalesce(sum(p.estimated_rows), 0) AS estimated_rows,
	(SELECT coalesce(sum(pg_total_relation_size(t.relid)), 0)
	 FROM pg_partition_tree(s.relid) AS t) AS bytes,
	-- the sealed partitions (not tiered ones, whose bytes are in S3), now and as heap
	coalesce(sum(p.bytes) FILTER (WHERE p.state = 'sealed' AND p.bytes_before IS NOT NULL), 0)::int8 AS sealed_bytes,
	coalesce(sum(p.bytes_before) FILTER (WHERE p.state = 'sealed'), 0)::int8 AS sealed_bytes_before
FROM snouttime.series s
LEFT JOIN snouttime.partition_info p ON p.series = s.relid
GROUP BY s.relid, s.time_column, s.time_type, s.partition_interval, s.partition_width,
	s.space_column, s.space_partitions, s.retention, s.retention_width;

COMMENT ON VIEW snouttime.series_info IS
	'One row per series table: its shape, how many partitions it has, and what it costs.';

-- The last thing each job did, which is the question asked when something looks stuck.
CREATE VIEW snouttime.job_info AS
SELECT
	j.kind,
	j.target,
	j.schedule,
	j.next_run,
	j.enabled,
	r.started_at AS last_run,
	r.finished_at - r.started_at AS last_took,
	r.ok AS last_ok,
	r.detail AS last_detail
FROM snouttime.jobs j
LEFT JOIN LATERAL (
	SELECT * FROM snouttime.job_runs runs
	WHERE runs.kind = j.kind AND runs.target = j.target
	ORDER BY runs.started_at DESC
	LIMIT 1
) AS r ON true;

COMMENT ON VIEW snouttime.job_info IS
	'Every scheduled job with what happened the last time it ran.';

GRANT SELECT ON snouttime.partition_info, snouttime.series_info, snouttime.job_info TO PUBLIC;
