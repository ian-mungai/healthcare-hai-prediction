-- Per-process engine settings from the same launch plan as the container limit. No default is allowed.
set memory_limit = getenv('DUCKDB_MEMORY_LIMIT');
set threads = cast(getenv('JOB_THREADS') as integer);
