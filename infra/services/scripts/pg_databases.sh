#!/bin/sh
# Roles and databases of the postgres task, with the restore of an empty database from the newest completed dump (postgres image, `sh -ec`).
# It runs after pg_restore_fetch has written RESTORE_DIR/<database>.dump for each database that the newest completed backup holds.
# PG_ROLES is "role:PASSWORD_VARIABLE ..."; PG_DATABASES is "database:owner ..." (polaris exists from the image's first start, so it is the one database that
# may exist and be empty). For each database: one that has any relation is left as it is. A missing one with a dump is restored into <database>__restore
# (one transaction, stop at the first error), checked against the dump's table list and renamed only when every database of the set passed, so a consumer
# can never connect to half-restored data and a failure leaves each database whole or absent. An empty existing one with a dump is restored in place in
# one transaction. A missing one with no dump is created empty, as before. Every restore has a deadline (RESTORE_SECONDS). The dumps are removed when done.
: "${PG_DATABASES:?PG_DATABASES is not set}"
restore_dir=${RESTORE_DIR:-/restore}
limit=${RESTORE_SECONDS:-1200}
staged=""
inplace=""

sql() { psql -X -v ON_ERROR_STOP=1 -At "$@"; }

drop_staged() {
  for name in $staged; do sql -d postgres -c "DROP DATABASE IF EXISTS \"${name}__restore\"" >/dev/null || true; done
}

refuse() {
  echo "postgres_databases: restore refused: $1" >&2
  drop_staged
  exit 1
}

roles() {
  printf '%s\n' '\set ON_ERROR_STOP on' '\set VERBOSITY terse' "SET log_statement = 'none';" "SET log_min_error_statement = 'panic';" 'SET log_min_duration_statement = -1;'
  for pair in $PG_ROLES; do
    role=${pair%%:*}
    variable=${pair#*:}
    printf '%s\n' "\\getenv pw_$role $variable"
    printf '%s\n' "SELECT 'CREATE ROLE $role LOGIN' WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '$role') \\gexec"
    printf '%s\n' "ALTER ROLE $role PASSWORD :'pw_$role';"
  done
}

exists() {
  found=$(sql -d postgres -c "SELECT 1 FROM pg_database WHERE datname = '$1'") || refuse "database $1: the server cannot be read"
  [ "$found" = 1 ]
}

filled() {
  count=$(sql -d "$1" -c "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relkind IN ('r','p','v','m','S','f') AND n.nspname NOT IN ('pg_catalog','information_schema') AND n.nspname NOT LIKE 'pg_toast%'") || refuse "database $1: its relations cannot be counted"
  [ "$count" -gt 0 ]
}

restore_into() {
  timeout "$limit" pg_restore --no-password --single-transaction --exit-on-error --dbname="$1" "$2" || refuse "database $3: pg_restore failed or passed its ${limit} second deadline"
}

verify() {
  expected=$(pg_restore --list "$2" | awk '$4 == "TABLE" && $5 != "DATA"' | wc -l | tr -d ' ')
  found=$(sql -d "$1" -c "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relkind IN ('r','p') AND n.nspname NOT IN ('pg_catalog','information_schema') AND n.nspname NOT LIKE 'pg_toast%'")
  if [ "$expected" -lt 1 ] || [ "$expected" != "$found" ]; then refuse "database $3: the restored database has $found tables, the dump lists $expected"; fi
}

roles | psql -X

for pair in $PG_DATABASES; do
  name=${pair%%:*}
  owner=${pair#*:}
  dump="$restore_dir/$name.dump"
  if exists "$name"; then
    if filled "$name"; then
      echo "postgres_databases: database $name has data, left as it is"
    elif [ -s "$dump" ]; then
      inplace="$inplace $name"
    else
      echo "postgres_databases: database $name is empty and has no completed dump"
    fi
  elif [ -s "$dump" ]; then
    staged="$staged $name"
    sql -d postgres -c "DROP DATABASE IF EXISTS \"${name}__restore\"" >/dev/null
    sql -d postgres -c "CREATE DATABASE \"${name}__restore\" OWNER \"$owner\" ENCODING 'UTF8'" >/dev/null
    restore_into "${name}__restore" "$dump" "$name"
    verify "${name}__restore" "$dump" "$name"
  else
    sql -d postgres -c "CREATE DATABASE \"$name\" OWNER \"$owner\" ENCODING 'UTF8'" >/dev/null
    echo "postgres_databases: database $name created empty, no completed dump"
  fi
done

for name in $inplace; do
  for schema in ${RESET_SCHEMAS:-}; do sql -d "$name" -c "DROP SCHEMA IF EXISTS $schema" >/dev/null || refuse "database $name: the empty schema $schema cannot be reset"; done
  restore_into "$name" "$restore_dir/$name.dump" "$name"
  verify "$name" "$restore_dir/$name.dump" "$name"
  echo "postgres_databases: restored database $name in place"
done

for name in $staged; do
  sql -d postgres -c "ALTER DATABASE \"${name}__restore\" RENAME TO \"$name\"" >/dev/null || refuse "database $name: the restored database cannot be renamed"
  echo "postgres_databases: restored database $name"
done

sql -d polaris -c "CREATE SCHEMA IF NOT EXISTS polaris_schema" >/dev/null
rm -f "$restore_dir"/*.dump "$restore_dir/RUN"
