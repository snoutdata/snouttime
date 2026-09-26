-- SnoutTime 0.1.3 -> 0.1.4 (docs/snouttime/PLAN.md 3.6, found by the published re-run).
--
-- Nothing in the catalog changes: 0.1.4 is in the library, and reads and writes what 0.1.3
-- does (format version 3). A scan kept its decoded row groups within work_mem by summing every
-- cached group's size each time it loaded one; an as-of join over 1,000 hosts, whose rescans
-- keep hundreds of groups, spent most of its time there (q5 at 100M rows: 3,706 ms on 0.1.1,
-- 5,670 on 0.1.3). Each cached group now keeps its size and the scan their sum.

\echo Use "ALTER EXTENSION snouttime UPDATE TO '0.1.4'" to load this file. \quit
