#!/bin/sh
# Run the catalog database migration, retrying while the database starts, and end the task when every try fails.
# Usage: openmetadata_migrate.sh COMMAND [ARGUMENT...]
# After OM_MIGRATE_TRIES failed tries (default 30, OM_MIGRATE_SLEEP seconds apart, default 10) the script writes OM_ABORT_FILE and exits 1. The deadline wrapper of
# the database and the search index watches that file, so the task stops at once instead of running paid sidecars until its deadline.
tries=${OM_MIGRATE_TRIES:-30}
pause=${OM_MIGRATE_SLEEP:-10}
abort_file=${OM_ABORT_FILE:-/run/om/abort}
failed=0
until "$@"; do
  failed=$((failed + 1))
  if [ "$failed" -ge "$tries" ]; then
    echo "openmetadata_migrate: the migration failed $failed times" >&2
    printf 'migration failed after %s tries\n' "$failed" > "$abort_file"
    exit 1
  fi
  sleep "$pause"
done
