#!/bin/sh
# Dump step of the Postgres backup service (postgres image, `sh -ec`): every INTERVAL seconds dump each database in PG_DATABASES with pg_dump -Fc into
# BACKUP_DIR/<UTC run>.ready for the upload step. The set is written in <run>.partial, so the upload step never sees a half set.
# A database is dumped only when its dump reads back completely (pg_restore --file=/dev/null) and lists at least one table; otherwise the run is refused,
# nothing is left for the upload step and last_result.json names the database and the reason. Every dump has a deadline (DUMP_SECONDS). PG_BACKUP_ONCE=1 runs
# one pass and exits 0 on success or 1 on a refusal.
: "${PG_DATABASES:?PG_DATABASES is not set}"
backups=${BACKUP_DIR:-/backup}
interval=${BACKUP_INTERVAL_SECONDS:-21600}
retry=${BACKUP_RETRY_SECONDS:-900}
first=${BACKUP_FIRST_DELAY_SECONDS:-600}
limit=${DUMP_SECONDS:-900}
keep_minutes=${BACKUP_KEEP_MINUTES:-2880}

result() {
  printf '{"result":"%s","run":"%s","reason":"%s","at_utc":"%s"}\n' "$1" "$2" "$3" "$(date -u +%Y%m%dT%H%M%SZ)" >"$backups/last_result.json"
}

dump_set() {
  run=$(date -u +%Y%m%dT%H%M%SZ)
  work="$backups/$run.partial"
  rm -rf "$work"
  mkdir -p "$work"
  for name in $PG_DATABASES; do
    if ! timeout "$limit" pg_dump --format=custom --no-password --dbname="$name" --file="$work/$name.dump"; then
      fail "database $name: pg_dump failed or passed its ${limit} second deadline" "$run" "$work"
      return 1
    fi
    if ! timeout "$limit" pg_restore --file=/dev/null "$work/$name.dump" >/dev/null; then
      fail "database $name: the dump is unreadable (truncated or damaged)" "$run" "$work"
      return 1
    fi
    tables=$(pg_restore --list "$work/$name.dump" | awk '$4 == "TABLE" && $5 != "DATA"' | wc -l | tr -d ' ')
    if [ "$tables" -lt 1 ]; then
      fail "database $name: the dump is empty (no table)" "$run" "$work"
      return 1
    fi
    bytes=$(wc -c <"$work/$name.dump" | tr -d ' ')
    printf '%s\t%s\t%s\n' "$name" "$tables" "$bytes" >>"$work/manifest.tsv"
  done
  mv "$work" "$backups/$run.ready"
  result dumped "$run" ""
  echo "pg_backup_dump: dumped $run"
}

fail() {
  echo "pg_backup_dump: refused: $1" >&2
  rm -rf "$3"
  result refused "$2" "$1"
}

mkdir -p "$backups"
find "$backups" -maxdepth 1 -name '*.partial' -exec rm -rf {} +
sleep "$first"
while true; do
  find "$backups" -maxdepth 1 \( -name '*.refused' -o -name '*.failed' \) -mmin "+$keep_minutes" -exec rm -rf {} +
  status=0
  dump_set || status=1
  if [ "${PG_BACKUP_ONCE:-}" = 1 ]; then exit "$status"; fi
  if [ "$status" -eq 0 ]; then sleep "$interval"; else sleep "$retry"; fi
done
