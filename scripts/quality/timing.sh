#!/usr/bin/env bash
# Per-command timing for the CI scripts (scripts/run_ci.sh and the acquisition scripts). Source this file; it runs nothing.
#   timed <label> <command> [arguments...]
# prints "[timing] start <label> at <UTC>" and "[timing] end <label> at <UTC>: <seconds> s, exit <code>", appends one tab-separated row
# (label, start, end, seconds, exit code) to the file named by CI_TIMING_FILE and returns the command's own exit code, so set -e still
# stops the caller. Wrap one command or one pipeline only: a shell function that holds several commands runs with set -e ignored.
#   timing_table <file>... prints the rows of the files as a Markdown table (header included) on standard output.
# E2E: .venv/bin/python -m scripts.quality.run_ci_groups_e2e

timing_clock() { date -u +%Y-%m-%dT%H:%M:%SZ; }

timed() {
  local label="$1" status=0 start_epoch end_epoch start_text end_text seconds
  shift
  if [[ ! "$label" =~ ^[A-Za-z0-9\ _.:/=-]+$ ]]; then
    printf 'timed: the label "%s" holds a character outside letters, digits, space and _ . : / = -\n' "$label" >&2
    return 2
  fi
  start_epoch="$(date -u +%s)"
  start_text="$(timing_clock)"
  printf '[timing] start %s at %s\n' "$label" "$start_text"
  "$@" || status=$?
  end_epoch="$(date -u +%s)"
  end_text="$(timing_clock)"
  seconds=$((end_epoch - start_epoch))
  if ((seconds < 0)); then
    seconds=0
  fi
  printf '[timing] end %s at %s: %s s, exit %s\n' "$label" "$end_text" "$seconds" "$status"
  printf '%s\t%s\t%s\t%s\t%s\n' "$label" "$start_text" "$end_text" "$seconds" "$status" >>"${CI_TIMING_FILE:-/dev/null}"
  return "$status"
}

# For a script that is a CI entry on its own: the rows go to a temporary file and the exit trap adds the table to the job summary. Under
# scripts/run_ci.sh (which sets CI_TIMING_FILE and writes the table itself) it changes nothing.
timing_entry() {
  if [[ -n "${CI_TIMING_FILE:-}" ]]; then
    return 0
  fi
  CI_TIMING_FILE="$(mktemp "${TMPDIR:-/tmp}/ci_timing.XXXXXX")"
  export CI_TIMING_FILE
  trap 'timing_table "$CI_TIMING_FILE" >>"${GITHUB_STEP_SUMMARY:-/dev/null}"; rm -f "$CI_TIMING_FILE"' EXIT
}

timing_table() {
  local file label start end seconds status
  printf '\n| Command | Start (UTC) | End (UTC) | Seconds | Exit |\n| --- | --- | --- | ---: | ---: |\n'
  for file in "$@"; do
    [[ -f "$file" ]] || continue
    while IFS=$'\t' read -r label start end seconds status; do
      printf '| %s | %s | %s | %s | %s |\n' "$label" "$start" "$end" "$seconds" "$status"
    done <"$file"
  done
}
