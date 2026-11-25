#!/bin/sh
# Dump step of the Postgres backup service (postgres image, `sh -ec`): every INTERVAL seconds dump each database in PG_DATABASES with pg_dump -Fc into
# BACKUP_DIR/<UTC run>.ready for the upload step. The set is written in <run>.partial, so the upload step never sees a half set. A run key whose .ready set
# still exists (two passes in one second) is refused like any other failure; the earlier set stays untouched.
# A database is dumped only when its dump reads back completely (pg_restore --file=/dev/null) and lists at least one table; otherwise the run is refused,
# nothing is left for the upload step and last_result.json names the database and the reason. Every dump has a deadline (DUMP_SECONDS). PG_BACKUP_ONCE=1 runs
# one pass and exits 0 on success or 1 on a refusal. last_result.json is written under another name and renamed, so the upload step never reads a half written
# pointer, and a pass that cannot publish it exits 1. It names the task (PG_BACKUP_TASK_ID, set by the final backup launch) so the upload step of another task never takes it.
: "${PG_DATABASES:?PG_DATABASES is not set}"
backups=${BACKUP_DIR:-/backup}
interval=${BACKUP_INTERVAL_SECONDS:-21600}
retry=${BACKUP_RETRY_SECONDS:-900}
first=${BACKUP_FIRST_DELAY_SECONDS:-600}
limit=${DUMP_SECONDS:-900}
keep_minutes=${BACKUP_KEEP_MINUTES:-2880}

result() {
  if printf '{"result":"%s","run":"%s","reason":"%s","task":"%s","at_utc":"%s"}\n' "$1" "$2" "$3" "${PG_BACKUP_TASK_ID:-}" "$(date -u +%Y%m%dT%H%M%SZ)" >"$backups/last_result.json.new" \
    && mv -f "$backups/last_result.json.new" "$backups/last_result.json"; then
    return 0
  fi
  echo "pg_backup_dump: the result file cannot be published" >&2
  rm -f "$backups/last_result.json.new"
  return 1
}

dump_set() {
  run=$(date -u +%Y%m%dT%H%M%SZ)
  work="$backups/$run.partial"
  rm -rf "$work"
  # dump_set runs inside an || list in the loop below, where set -e is off: every step that can fail is checked here, so no failed write is followed by a rename.
  if ! mkdir -p "$work"; then
    fail "the work folder $work cannot be created" "$run" "$work"
    return 1
  fi
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
    if ! printf '%s\t%s\t%s\n' "$name" "$tables" "$bytes" >>"$work/manifest.tsv"; then
      fail "database $name: the manifest line cannot be written" "$run" "$work"
      return 1
    fi
  done
  # mv moves a folder into an existing one, so a set of the same second (an earlier pass that the upload step has not taken) must be refused, never nested.
  if [ -e "$backups/$run.ready" ]; then
    fail "the set $run.ready already exists, so this run key is taken" "$run" "$work"
    return 1
  fi
  if ! mv "$work" "$backups/$run.ready"; then
    fail "the finished set cannot be renamed to $run.ready" "$run" "$work"
    return 1
  fi
  if ! result dumped "$run" ""; then
    return 1
  fi
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
