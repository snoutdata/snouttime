-- The extension installs into its own schema and reports the crate version.
SELECT extname, nspname FROM pg_extension e JOIN pg_namespace n ON n.oid = e.extnamespace WHERE extname = 'snouttime';
SELECT snouttime.version() = (SELECT extversion FROM pg_extension WHERE extname = 'snouttime') AS matches;
