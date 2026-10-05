-- gapfill, locf and interpolate. Semantics in README.md ("Filling gaps").
SET client_min_messages = warning;
SET timezone = 'UTC';

-- ---- gapfill: every bucket between two times ----
SELECT b FROM snouttime.gapfill('1 hour', timestamptz '2026-09-22 10:20:00+00', timestamptz '2026-09-22 13:00:00+00') AS b;
-- months are months, not 30 days
SELECT b::date FROM snouttime.gapfill('1 month', timestamptz '2024-01-15 00:00:00+00', timestamptz '2024-05-01 00:00:00+00') AS b;
-- the hour that happens twice in New York is two buckets
SELECT b AS b_utc, b AT TIME ZONE 'America/New_York' AS b_local
FROM snouttime.gapfill('1 hour', timestamptz '2026-11-01 04:00:00+00', timestamptz '2026-11-01 08:00:00+00',
	'America/New_York') AS b;
-- The property: gapfill is exactly the distinct buckets of a dense series of times, for
-- widths and zones where buckets are NOT all one width apart (months, DST, 90 minutes).
SELECT z.zone, w.width,
	(SELECT count(*) FROM (
		SELECT b FROM snouttime.gapfill(w.width, timestamptz '2026-02-20 00:00:00+00', timestamptz '2026-11-10 00:00:00+00', z.zone) AS b
		EXCEPT
		SELECT DISTINCT snouttime.bucket(w.width, t, z.zone) FROM generate_series(timestamptz '2026-02-20 00:00:00+00',
			timestamptz '2026-11-10 00:00:00+00' - interval '1 microsecond', interval '5 minutes') AS t) x) AS extra,
	(SELECT count(*) FROM (
		SELECT DISTINCT snouttime.bucket(w.width, t, z.zone) FROM generate_series(timestamptz '2026-02-20 00:00:00+00',
			timestamptz '2026-11-10 00:00:00+00' - interval '1 microsecond', interval '5 minutes') AS t
		EXCEPT
		SELECT b FROM snouttime.gapfill(w.width, timestamptz '2026-02-20 00:00:00+00', timestamptz '2026-11-10 00:00:00+00', z.zone) AS b) x) AS missing
FROM (VALUES ('America/New_York'), ('Australia/Lord_Howe'), ('UTC')) AS z(zone),
	(VALUES (interval '15 minutes'), ('1 hour'), ('90 minutes'), ('1 day'), ('1 month')) AS w(width)
ORDER BY z.zone, w.width;
-- and it is in order
SELECT bool_and(b > prev) AS strictly_increasing
FROM (SELECT b, lag(b) OVER (ORDER BY ord) AS prev
	FROM snouttime.gapfill('90 minutes', timestamptz '2026-10-30 00:00:00+00', timestamptz '2026-11-03 00:00:00+00',
		'America/New_York') WITH ORDINALITY AS g(b, ord)) s
WHERE prev IS NOT NULL;

-- ---- locf and interpolate over a gapfilled series ----
CREATE TABLE f_readings (ts timestamptz NOT NULL, host text NOT NULL, v float8);
INSERT INTO f_readings VALUES
	('2026-09-22 01:10+00', 'a', 10), ('2026-09-22 01:50+00', 'a', 20),
	('2026-09-22 04:30+00', 'a', 50),
	('2026-09-22 02:15+00', 'b', 7);
SELECT b, avg(r.v) AS avg,
	snouttime.locf(avg(r.v)) OVER (ORDER BY b) AS carried,
	snouttime.interpolate(avg(r.v)) OVER (ORDER BY b) AS by_position,
	snouttime.interpolate(avg(r.v), b) OVER (ORDER BY b) AS by_time
FROM snouttime.gapfill('1 hour', timestamptz '2026-09-22 00:00+00', timestamptz '2026-09-22 07:00+00') AS b
LEFT JOIN f_readings r ON r.host = 'a' AND snouttime.bucket('1 hour', r.ts) = b
GROUP BY b ORDER BY b;
-- per host: each partition carries its own values, and never another's
SELECT h.host, b, snouttime.locf(avg(r.v)) OVER (PARTITION BY h.host ORDER BY b) AS carried
FROM (VALUES ('a'), ('b')) AS h(host)
CROSS JOIN snouttime.gapfill('1 hour', timestamptz '2026-09-22 01:00+00', timestamptz '2026-09-22 05:00+00') AS b
LEFT JOIN f_readings r ON r.host = h.host AND snouttime.bucket('1 hour', r.ts) = b
GROUP BY h.host, b ORDER BY h.host, b;
-- by time, over months of unequal length: halfway in TIME, not in rows
SELECT b::date, v, snouttime.interpolate(v) OVER (ORDER BY b) AS by_position,
	round(snouttime.interpolate(v, b) OVER (ORDER BY b)::numeric, 3) AS by_time
FROM (VALUES (timestamptz '2025-01-01+00', 0::float8), ('2025-02-01+00', NULL), ('2025-03-01+00', NULL),
	('2025-04-01+00', 90)) AS s(b, v);
-- locf of a by-reference type, and a long run of NULLs
SELECT n, snouttime.locf(label) OVER (ORDER BY n) AS carried
FROM (VALUES (1, NULL::text), (2, 'first'), (3, NULL), (4, NULL), (5, 'second'), (6, NULL)) AS s(n, label);
SELECT count(*) FILTER (WHERE carried = 1) AS carried_ones, count(*) FILTER (WHERE carried IS NULL) AS nulls
FROM (SELECT snouttime.locf(CASE WHEN n = 3 THEN 1 END) OVER (ORDER BY n) AS carried
	FROM generate_series(1, 100000) AS n) s;
-- the frame does not change them: like lag and lead, they look at the whole partition
SELECT n, snouttime.interpolate(v) OVER (ORDER BY n ROWS BETWEEN CURRENT ROW AND CURRENT ROW) AS framed
FROM (VALUES (1, 1::float8), (2, NULL), (3, 3)) AS s(n, v);

-- ---- refusals ----
\set ON_ERROR_STOP 0
SELECT * FROM snouttime.gapfill('1 hour', timestamptz '-infinity', timestamptz '2026-01-01+00');
SELECT snouttime.locf(1);
\set ON_ERROR_STOP 1
