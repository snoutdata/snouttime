-- SnoutTime 0.1.2 -> 0.1.3 (docs/snouttime/PLAN.md 3.6, TSBS's one- and eight-host queries).
--
-- Nothing in the catalog changes: 0.1.3 is in the library. A column store sealed by it is
-- format version 3, whose integer and float chunks are cut into pages of 1,024 rows, so a seek
-- decodes only the pages its rows are in; 0.1.2 cannot read one (it refuses version 3), and
-- 0.1.3 reads every store 0.1.2 wrote. The aggregate node answers an equality or IN list on
-- the sort key by the seek, and the seek finds a value's rows inside a row group by the runs
-- the directory records, without decoding the sort key's leading columns. And, with
-- snouttime.plan_time_bounds (on by default), `ts >= <constant> - interval` gives the planner
-- bounds it can prune partitions by, where Postgres plans every partition.

\echo Use "ALTER EXTENSION snouttime UPDATE TO '0.1.3'" to load this file. \quit
