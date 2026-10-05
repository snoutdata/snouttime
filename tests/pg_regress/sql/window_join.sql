-- snouttime.window_join. Semantics in README.md ("Window join").
SET client_min_messages = warning;

CREATE TABLE w_right AS
SELECT 'h' || (g % 40) AS host, timestamptz '2026-01-01+00' + (g * 7) * interval '1 second' AS ts,
	CASE WHEN g % 97 = 0 THEN NULL ELSE ((g * 7919) % 1000)::float8 / 10 END AS v
FROM generate_series(1, 50000) AS g;
CREATE INDEX ON w_right (host, ts);
CREATE TABLE w_left AS
SELECT 'h' || (g % 43) AS host, timestamptz '2026-01-01+00' + (g * 17 + 5) * interval '1 second' AS ts, g AS id
FROM generate_series(1, 20000) AS g;
ANALYZE w_left, w_right;

-- a tiny, readable one
SELECT * FROM snouttime.window_join(
	$$SELECT k, timestamptz '2026-01-01+00' + s * interval '1 second' AS t FROM (VALUES ('a', 10), ('a', 20), ('b', 5)) AS l(k, s)$$,
	$$SELECT k, t::timestamptz AS t, v FROM (VALUES
		('a', '2026-01-01 00:00:05+00', 1.0::float8), ('a', '2026-01-01 00:00:09+00', 2.0),
		('a', '2026-01-01 00:00:12+00', NULL), ('a', '2026-01-01 00:00:19+00', 4.0)) AS r(k, t, v)$$,
	keys => ARRAY['k'], left_time => 't', value => 'v', before => '5 seconds', aggregate => 'sum')
	AS (k text, t timestamptz, total float8);

-- ---- the property: every aggregate equals its SQL form ----
CREATE FUNCTION w_check(agg text, before interval, after interval) RETURNS TABLE (differ bigint, total bigint)
LANGUAGE plpgsql AS $f$
DECLARE
	expr text := CASE agg
		WHEN 'first' THEN '(array_agg(r.v ORDER BY r.ts) FILTER (WHERE r.v IS NOT NULL))[1]'
		WHEN 'last' THEN '(array_agg(r.v ORDER BY r.ts DESC) FILTER (WHERE r.v IS NOT NULL))[1]'
		WHEN 'count' THEN 'count(r.v)::float8'
		ELSE agg || '(r.v)' END;
BEGIN
	RETURN QUERY EXECUTE format($q$
		WITH ours AS (
			SELECT * FROM snouttime.window_join('SELECT * FROM w_left', 'SELECT * FROM w_right', ARRAY['host'], 'ts', 'v',
				before => %1$L, after => %2$L, aggregate => %3$L) AS (host text, ts timestamptz, id int, a float8)
		), ref AS (
			SELECT l.id, (SELECT %4$s FROM w_right r WHERE r.host = l.host
				AND r.ts BETWEEN l.ts - %1$L::interval AND l.ts + %2$L::interval) AS a
			FROM w_left l
		)
		SELECT count(*) FILTER (WHERE (o.a IS NULL) <> (r.a IS NULL)
				OR abs(o.a - r.a) > 1e-9 * greatest(1, abs(r.a))),
			count(*)
		FROM ours o JOIN ref r USING (id)$q$, before, after, agg, expr);
END
$f$;
SELECT a.agg, w.before, w.after, c.*
FROM (VALUES ('count'), ('sum'), ('avg'), ('min'), ('max'), ('first'), ('last')) AS a(agg),
	(VALUES (interval '1 minute', interval '0'), (interval '30 seconds', interval '30 seconds'), (interval '0', interval '0')) AS w(before, after),
	LATERAL w_check(a.agg, w.before, w.after) AS c
ORDER BY a.agg, w.before, w.after;
-- a key with no rows at all on the right: NULL, and 0 for count
SELECT id, a FROM snouttime.window_join('SELECT * FROM w_left WHERE host = ''h41'' ORDER BY id LIMIT 2', 'SELECT * FROM w_right',
	ARRAY['host'], 'ts', 'v', before => '1 hour', aggregate => 'count') AS (host text, ts timestamptz, id int, a float8);

-- ---- refusals ----
\set ON_ERROR_STOP 0
SELECT * FROM snouttime.window_join('SELECT * FROM w_left', 'SELECT host, ts, v::int AS v FROM w_right', ARRAY['host'], 'ts', 'v', '1 minute')
	AS (host text, ts timestamptz, id int, a float8);
SELECT * FROM snouttime.window_join('SELECT * FROM w_left', 'SELECT * FROM w_right', ARRAY['host'], 'ts', 'v', '1 minute', aggregate => 'median')
	AS (host text, ts timestamptz, id int, a float8);
SELECT * FROM snouttime.window_join('SELECT * FROM w_left', 'SELECT * FROM w_right', ARRAY['host'], 'ts', 'v', '-1 minute')
	AS (host text, ts timestamptz, id int, a float8);
SELECT * FROM snouttime.window_join('SELECT * FROM w_left', 'SELECT * FROM w_right', ARRAY['host'], 'ts', 'v', '1 minute')
	AS (host text, ts timestamptz, id int);
\set ON_ERROR_STOP 1
