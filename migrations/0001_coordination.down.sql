-- Manual destructive rollback only, after all Solo executors are stopped.
-- squawk-ignore-file ban-drop-table
DROP TABLE public.solo_successes;
DROP TABLE public.solo_leases;
DROP TABLE public.solo_schema_version;
