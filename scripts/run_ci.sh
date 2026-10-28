#!/usr/bin/env bash
set -euo pipefail

# CI_PART selects what runs: "all" (the default, every part), "core" (everything except the launcher E2E, the
# acquisition gate and the four lakehouse layers), "launcher" (the gates and the launcher E2E, one LAUNCHER_SHARD=K/N of it
# if set), "bronze", "silver", "gold" or "features" (that layer), "feature_sources" or "feature_columns" (one feature suite), or "gates" (the gates only).
# FEATURE_SHARD=K/N selects one hosted feature shard. Without it the selected feature suites run all their shards and combine their evidence. GitHub CI runs
# "core", the launcher shards and the four layers as parallel jobs; "gates" is the quick part of a reused pull request result.
# CI_UNITS selects the units of the gold layer (checks, writer_setup, model_metadata_e2e; default all three). GitHub CI runs the model
# metadata E2E in its own job. A unit marked alone below (writer_setup: it has timing assertions) runs by itself, before the others.
# CI_SCOPE selects the check groups (scripts/quality/ci_groups.py, config/quality/ci_groups.json): "full" (the default,
# every group: the pre-push run escalates to it when the selector or the CI scripts change; run it on demand or when hosted CI is unavailable), "staged"
# (the groups the staged paths touch, on demand) or "range" (the groups RANGE_START..RANGE_END touches: GitHub CI and the pre-push hook). The privacy, secret,
# credential, data-file, writing, Markdown lint and documentation gates run in every scope and in every part, also when no group is selected.
# CI_LIST_ONLY=1 prints the plan and runs nothing. Each worker (a group, a gold unit, a feature shard, a hook partition, an acquisition shard or a combining step) runs in its own
# session and has a deadline: the timeout of the hosted job that runs the same work (worker_limits in config/quality/ci_groups.json), or CI_WORKER_LIMIT_SECONDS for every
# worker. A worker is overdue only when the whole seconds since its start exceed its limit, so it is never stopped before its limit. At the deadline the dispatcher stops every member
# of the worker's session (scripts/process.py stop_sessions: TERM, then KILL after CI_WORKER_GRACE_SECONDS, default 5), which includes the groups that the worker's descendants
# started through scripts/process.py, confirms that no member is left and fails the run with the worker's name. An exit, Ctrl-C or TERM of the dispatcher stops every live worker
# session the same way. Each group's seconds and each command's start, end and seconds (scripts/quality/timing.sh)
# are printed and added to the job summary (GITHUB_STEP_SUMMARY).
# The check groups' functions are not in this file: scripts/ci/groups/<group>.sh holds the functions of one group of config/quality/ci_groups.json. Each group
# runs in its own shell process (run_group) that loads only scripts/quality/timing.sh and that one file, so the shell state of a group (functions, variables, options,
# working directory) cannot reach another group or this file. Files and signals are shared (docs/quality_checks.md, Check group files). A missing file fails the run with its name (require_group_file). Only this file, the selector and the shared workflow parts select every group; a
# body file selects its own group. In the full scope of part "all" the acquisition gate runs as GitHub CI runs it: four
# shards in the parallel set (each with its own folder, ACQUISITION_PART=shard) and then the combining step (ACQUISITION_PART=combine) with the 90% gate.
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
bash -n scripts/run_ci.sh scripts/infrastructure/*.sh scripts/ci/groups/*.sh
# ShellCheck runs without -x here and checks timing.sh as its own input.
# shellcheck source=/dev/null
source "$repo_root/scripts/quality/timing.sh"
python_bin="${PYTHON_BIN:-$repo_root/.venv/bin/python}"
part="${CI_PART:-all}"
case "$part" in
  all | core | launcher | bronze | silver | gold | features | feature_sources | feature_columns | gates) ;;
  *) printf 'CI_PART must be all, core, launcher, bronze, silver, gold, features, feature_sources, feature_columns or gates, not %s\n' "$part" >&2; exit 2 ;;
esac
gold_units="checks writer_setup model_metadata_e2e"
units="${CI_UNITS:-$gold_units}"
for unit in $units; do
  case " $gold_units " in
    *" $unit "*) ;;
    *) printf 'CI_UNITS must name units of the gold layer (%s), not %s\n' "$gold_units" "$unit" >&2; exit 2 ;;
  esac
done
scope="${CI_SCOPE:-full}"
case "$scope" in
  full | staged | range) ;;
  *) printf 'CI_SCOPE must be full, staged or range, not %s\n' "$scope" >&2; exit 2 ;;
esac
summary="${GITHUB_STEP_SUMMARY:-/dev/null}"
scratch="$(mktemp -d "${TMPDIR:-/tmp}/hai_ci.XXXXXX")"
# Every command's row goes to its own file (the main shell and each parallel unit), so rows never interleave; the exit trap writes them all
# to the job summary in a fixed order, also when a gate or a unit failed.
export CI_TIMING_FILE="$scratch/main.timing"
timing_files=("$CI_TIMING_FILE")
# Each worker leads its own session: a short Python step (worker_session_step) calls setsid, resets SIGINT and SIGQUIT (a background job of a shell without job
# control starts with them ignored), sets PROCESS_LAUNCH_SESSION=1 (a scripts/process.py launch inside the worker stays in its session) and starts a new bash. setsid cannot run
# inside a bash subshell, so that bash reads the functions and the variables a worker uses from a private state file (write_worker_state). The step stays as the session's leader
# and watchdog: it ends with the bash's status, and when its parent (this script) has died, even by KILL, it stops every other member of its session (its arguments: the repository
# and the grace seconds) within 0.2 s plus the grace period plus the 5 s confirmation. The live workers are listed in worker_pids; each pid is also the worker's session id.
worker_pids=()
worker_state="$scratch/worker_state.sh"
# shellcheck disable=SC2016
worker_session_step='import os, signal, sys, time
os.setsid()
signal.signal(signal.SIGINT, signal.SIG_DFL)
signal.signal(signal.SIGQUIT, signal.SIG_DFL)
os.environ["PROCESS_LAUNCH_SESSION"] = "1"
os.environ.pop("BASH_ENV", None)
os.environ.pop("ENV", None)
dispatcher = os.getppid()
worker = os.fork()
if worker == 0:
    os.execv(sys.argv[3], sys.argv[3:])
while os.getppid() == dispatcher:
    done, status = os.waitpid(worker, os.WNOHANG)
    if done:
        code = os.waitstatus_to_exitcode(status)
        sys.exit(128 - code if code < 0 else code)
    time.sleep(0.2)
for number in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
    signal.signal(number, signal.SIG_IGN)
sys.path.insert(0, sys.argv[1])
from scripts.process import stop_sessions
sys.exit(1 if stop_sessions({os.getpid()}, float(sys.argv[2]), allow_own=True) else 0)'
# The worker's bash: $1 state file, $2 item, $3 "stream" (a single item: its output also goes to the log through tee) or "buffer".
# shellcheck disable=SC2016
worker_body='set -euo pipefail
source "$1"
if [[ "$3" == stream ]]; then run_timed_item "$2" 2>&1 | tee "$scratch/$2.log"; else run_timed_item "$2" >"$scratch/$2.log" 2>&1; fi'
worker_grace="${CI_WORKER_GRACE_SECONDS:-5}"
worker_override="${CI_WORKER_LIMIT_SECONDS:-}"
if [[ ! "$worker_grace" =~ ^[1-9][0-9]*$ ]] || [[ -n "$worker_override" && ! "$worker_override" =~ ^[1-9][0-9]*$ ]]; then
  printf 'CI_WORKER_GRACE_SECONDS and CI_WORKER_LIMIT_SECONDS must be whole numbers above 0, not %s and %s\n' "$worker_grace" "${worker_override:-unset}" >&2
  exit 2
fi
# stop_sessions <grace seconds> <session>...: stop every member of the worker sessions with scripts/process.py stop_sessions (TERM, KILL after the grace period, then a
# confirmation of at most 5 s). Fails when a member survives KILL or when the sessions cannot be listed; the leaders' groups are still stopped then. An alarm ends the step
# after the grace period plus 25 s (the listing and the confirmation of both phases and of the fallback), so the exit trap cannot wait on it forever.
# shellcheck disable=SC2016
stop_sessions() {
  "$python_bin" -I -c 'import signal, sys
signal.alarm(int(float(sys.argv[2])) + 25)
sys.path.insert(0, sys.argv[1])
from scripts.process import stop_sessions
sys.exit(1 if stop_sessions({int(session) for session in sys.argv[4:]}, float(sys.argv[2]), parent=int(sys.argv[3])) else 0)' "$repo_root" "$1" "$$" "${@:2}"
}
# forget_worker <pid>: take a worker that has ended out of the live list.
forget_worker() {
  local pid keep=()
  for pid in ${worker_pids[@]+"${worker_pids[@]}"}; do
    if [[ "$pid" != "$1" ]]; then keep+=("$pid"); fi
  done
  worker_pids=(${keep[@]+"${keep[@]}"})
}
# stop_workers: stop every member of every live worker session (the exit trap, so also after Ctrl-C and TERM). It waits 2 s after TERM, less than the 5 s that scripts/process.py
# gives this script after its own TERM. The stop step is a new process in this script's session, which an outer session stop signals too, so TERM, INT and HUP are ignored first:
# the step inherits that and finishes its KILL; an outer KILL still ends it.
stop_workers() {
  if ((${#worker_pids[@]} == 0)); then return 0; fi
  trap '' TERM INT HUP
  local pids=("${worker_pids[@]}")
  worker_pids=()
  if ! stop_sessions 2 "${pids[@]}"; then
    printf 'Local CI could not stop every worker session: %s\n' "${pids[*]}" >&2
  fi
}
finish() {
  stop_workers
  if [[ -s "$scratch/main.timing" ]]; then
    timing_table "${timing_files[@]}" >>"$summary"
  fi
  rm -rf "$scratch"
}
# Only a run that sets ci_finished=1 exits 0: bash 3.2 (macOS /bin/bash) reports status 0 to this trap when an unbound
# variable ends the script, so the trap fails every other exit itself.
trap 'finish; [[ "${ci_finished:-0}" == 1 ]] || exit 1' EXIT
# Workers are in their own sessions, so a signal to this script's group does not reach them: these traps end the script, and the exit trap stops them.
trap 'exit 130' INT
trap 'exit 143' TERM
trap 'exit 129' HUP

plan_args=(--scope "$scope" --part "$part" --format shell)
if [[ "$scope" == "range" ]]; then
  plan_args+=(--from "${RANGE_START:-}" --to "${RANGE_END:-}")
fi
plan="$("$python_bin" -m scripts.quality.ci_groups plan "${plan_args[@]}")"
selected=" "
run_groups=()
while read -r kind group rest; do
  case "$kind" in
    reason) printf 'Check groups (scope %s, part %s): %s %s\n' "$scope" "$part" "$group" "$rest" | tee -a "$summary" ;;
    run) selected+="$group "; run_groups+=("$group"); printf 'run %s\n' "$group" ;;
    skip) printf 'skipped %s: %s\n' "$group" "$rest" | tee -a "$summary" ;;
  esac
done <<<"$plan"
if [[ "${CI_LIST_ONLY:-0}" == "1" ]]; then
  ci_finished=1
  exit 0
fi
is_selected() { [[ "$selected" == *" $1 "* ]]; }
# require_group_file <group>: a missing or unreadable body file of a check group fails the run with its name.
require_group_file() {
  local file="scripts/ci/groups/$1.sh"
  if [[ ! -f "$repo_root/$file" || ! -r "$repo_root/$file" ]]; then
    printf 'The body file %s of the check group %s is missing or not readable.\n' "$file" "$1" >&2
    exit 1
  fi
}
# run_group <group> <entry>: run the entry function of a check group in its own shell process. The process starts clean (no BASH_ENV, no profile) and gets
# exactly these variables beside the exported environment of this run: python_bin, repo_root, scratch, quality_output, e2e_output and CI_TIMING_FILE. It loads
# only scripts/quality/timing.sh (timed) and the body file of the group, and runs with the same options as every item (set -euo pipefail).
# shellcheck disable=SC2016
run_group() {
  env -u BASH_ENV -u ENV python_bin="$python_bin" repo_root="$repo_root" scratch="$scratch" quality_output="$quality_output" e2e_output="$e2e_output" \
    CI_TIMING_FILE="$CI_TIMING_FILE" "$BASH" --noprofile --norc -c \
    'set -euo pipefail; source "$repo_root/scripts/quality/timing.sh"; source "$repo_root/scripts/ci/groups/$1.sh"; "$2" "${@:3}"' check_group "$@"
}
# run_item <item>: a check group's entry function runs through run_group, anything else is a function of this file.
run_item() {
  local group_variable="item_group_$1" entry_variable="item_entry_$1" argument_variable="item_argument_$1"
  if [[ -n "${!group_variable:-}" ]]; then
    run_group "${!group_variable}" "${!entry_variable:-$1}" "${!argument_variable:-}"
  else
    "$1"
  fi
}
printf '\n| Step | Seconds |\n| --- | ---: |\n' >>"$summary"
record() { printf '%s: %s s\n' "$1" "$2"; printf '| %s | %s |\n' "$1" "$2" >>"$summary"; }


# Each gate is one command (or one pipeline in a function), wrapped in timed so its start, end and seconds are printed and listed.
credential_files() { git ls-files -z --cached --others --exclude-standard | xargs -0 "$python_bin" -m scripts.quality.repo_checks credential-files; }
data_files() { git ls-files -z --cached --others --exclude-standard | xargs -0 "$python_bin" -m scripts.quality.repo_checks data-files; }
# pre-commit's types: [markdown] is the extensions md and markdown in any letter case; the hook excludes data/.
markdown_lint() {
  git ls-files -z --cached --others --exclude-standard -- ':(icase)*.md' ':(icase)*.markdown' ':(exclude)data' \
    | xargs -0 "$repo_root/.tools/markdownlint-cli2/node_modules/.bin/markdownlint-cli2" --config .markdownlint-cli2.jsonc
}

started=$SECONDS
timed "gates: secret scan" "$python_bin" -m scripts.quality.scan_secrets
# The local dataset folders moved into data/datasets/; an old folder that appears again would split the data (failure mode 227).
timed "gates: moved dataset folders" "$python_bin" -c 'import json, pathlib, sys; old = [p for p in json.load(open("config/data_paths.json"))["moved"] if pathlib.Path(p).exists()]; sys.exit("old dataset folders exist again: " + ", ".join(old) if old else 0)'
# Repository checks over every file Git tracks or would track; the same checks run as pre-commit hooks.
timed "gates: lint settings" "$python_bin" -m scripts.quality.repo_checks lint-settings
timed "gates: env example" "$python_bin" -m scripts.quality.repo_checks env-example
timed "gates: privacy scan" "$python_bin" -m scripts.quality.repo_checks privacy-scan
timed "gates: writing check" "$python_bin" -m scripts.quality.repo_checks writing-check --block dash --block slash
timed "gates: credential files" credential_files
timed "gates: data files" data_files
timed "gates: markdown lint" markdown_lint
if [[ "${GITHUB_ACTIONS:-false}" == "true" ]]; then
  timed "gates: documentation review" "$python_bin" -m scripts.quality.documentation_review check-untracked
else
  timed "gates: documentation review" "$python_bin" -m scripts.quality.documentation_review check
fi
timed "gates: fast source checks" "$python_bin" -m scripts.quality.lint_sources fast
record gates $((SECONDS - started))

if [[ "$part" == "gates" ]]; then
  printf '\nThe gates part passed.\n'
  ci_finished=1
  exit 0
fi

# Every variable that run_group passes to a group is set here, before the first group runs, in every part and scope (the hosted launcher job exports neither folder).
e2e_output="${E2E_ARTIFACT_DIR:-$repo_root/data/e2e/configuration/$(date -u +%Y%m%dT%H%M%SZ)_$$}"
quality_output="${QUALITY_ARTIFACT_DIR:-$repo_root/data/e2e/quality/$(date -u +%Y%m%dT%H%M%SZ)_$$}"

if [[ "$part" != "core" ]] && is_selected launcher; then
  require_group_file launcher
  started=$SECONDS
  timed "launcher E2E${LAUNCHER_SHARD:+ shard $LAUNCHER_SHARD}" run_group launcher launcher_e2e
  record "launcher${LAUNCHER_SHARD:+ shard $LAUNCHER_SHARD}" $((SECONDS - started))
fi
if [[ "$part" == "launcher" ]]; then
  printf '\nThe launcher part passed.\n'
  ci_finished=1
  exit 0
fi

# The acquisition gate in the full scope of part all: four shards (the parallel set) and the combining step, as the GitHub jobs run them. Each shard has
# its own folder; the combining step merges the four folders, as the artifact download of GitHub CI does, and enforces the 90% coverage gate.
acquisition_shards=4
acquisition_dir="$quality_output/acquisition_shards"
acquisition_shard() { ACQUISITION_PART=shard ACQUISITION_SHARD="$1/$acquisition_shards" ACQUISITION_SHARD_DIR="$acquisition_dir/shard_$1" bash scripts/acquisition/run_checks.sh; }
acquisition_shard_1() { acquisition_shard 1; }
acquisition_shard_2() { acquisition_shard 2; }
acquisition_shard_3() { acquisition_shard 3; }
acquisition_shard_4() { acquisition_shard 4; }
acquisition_combine() {
  local merged="$acquisition_dir/merged" index
  rm -rf "$merged"
  mkdir -p "$merged"
  for ((index = 1; index <= acquisition_shards; index++)); do
    cp -R "$acquisition_dir/shard_$index/." "$merged/"
  done
  ACQUISITION_PART=combine ACQUISITION_SHARD_COUNT="$acquisition_shards" ACQUISITION_SHARD_DIR="$merged" bash scripts/acquisition/run_checks.sh
}

feature_counts="$("$python_bin" -c 'import json; c=json.load(open("config/quality/ci_groups.json"))["feature_shards"]; print(c["sources"], c["columns"])')"
read -r feature_sources_shards feature_columns_shards <<<"$feature_counts"
# worker_limit_seconds_<kind>: the deadline in seconds of each kind of worker (worker_limits in config/quality/ci_groups.json, in minutes).
worker_limits="$("$python_bin" -c 'import json; [print(kind, round(60 * entry["minutes"])) for kind, entry in json.load(open("config/quality/ci_groups.json"))["worker_limits"].items()]')"
while read -r kind seconds; do
  printf -v "worker_limit_seconds_$kind" '%s' "$seconds"
done <<<"$worker_limits"
feature_shard="${FEATURE_SHARD:-}"
if [[ -n "$feature_shard" ]]; then
  if [[ "$part" != "feature_sources" && "$part" != "feature_columns" ]]; then
    printf 'FEATURE_SHARD requires CI_PART=feature_sources or feature_columns.\n' >&2
    exit 2
  fi
  suite="${part#feature_}"
  count_variable="feature_${suite}_shards"
  if [[ ! "$feature_shard" =~ ^[1-9][0-9]*/[1-9][0-9]*$ ]] || [[ "${feature_shard##*/}" != "${!count_variable}" ]] || (( ${feature_shard%%/*} > ${!count_variable} )); then
    printf 'FEATURE_SHARD must be K/%s with 1 <= K <= %s, not %s.\n' "${!count_variable}" "${!count_variable}" "$feature_shard" >&2
    exit 2
  fi
fi
hook_partition() {
  timed "repository hooks: $1" "$python_bin" -m scripts.quality.ci_hooks --group "$1" --from "${RANGE_START:-}" --to "${RANGE_END:-}" \
    --output-directory "$quality_output/hooks/$1"
}
hook_privacy() { hook_partition privacy; }
hook_documentation() { hook_partition documentation; }
hook_code() { hook_partition code; }
combine_feature_sources() { run_group lakehouse_features lakehouse_features_combine "sources/$feature_sources_shards"; }
combine_feature_columns() { run_group lakehouse_features lakehouse_features_combine "columns/$feature_columns_shards"; }

# The work items: one per group, and for the gold layer one per selected unit. An alone item runs by itself, before the others.
alone_items=" lakehouse_gold_writer_setup "
alone=()
shared=()
for group in ${run_groups[@]+"${run_groups[@]}"}; do
  if [[ "$group" == "launcher" || "$group" == "acquisition" || "$group" == "repository_hooks" ]]; then
    continue
  fi
  require_group_file "$group"
  if [[ "$group" == "lakehouse_features" ]]; then
    for suite in sources columns; do
      if [[ "$part" == "feature_sources" || "$part" == "feature_columns" ]] && [[ "$part" != "feature_$suite" ]]; then
        continue
      fi
      count_variable="feature_${suite}_shards"
      count="${!count_variable}"
      for ((index = 1; index <= count; index++)); do
        if [[ -n "$feature_shard" && "${feature_shard%%/*}" != "$index" ]]; then
          continue
        fi
        item="lakehouse_features_${suite}_$index"
        printf -v "item_group_$item" '%s' "$group"
        printf -v "item_entry_$item" '%s' "lakehouse_features_$suite"
        printf -v "item_argument_$item" '%s' "$index/$count"
        shared+=("$item")
      done
    done
    continue
  fi
  if [[ "$group" != "lakehouse_gold" ]]; then
    printf -v "item_group_$group" '%s' "$group"
    shared+=("$group")
    continue
  fi
  for unit in $gold_units; do
    case " $units " in
      *" $unit "*)
        printf -v "item_group_lakehouse_gold_$unit" '%s' "$group"
        if [[ "$alone_items" == *" lakehouse_gold_$unit "* ]]; then
          alone+=("lakehouse_gold_$unit")
        else
          shared+=("lakehouse_gold_$unit")
        fi
        ;;
    esac
  done
done
if [[ "$part" == "all" ]] && is_selected acquisition; then
  shared+=(acquisition_shard_1 acquisition_shard_2 acquisition_shard_3 acquisition_shard_4)
fi

if [[ "$part" == "all" ]] && is_selected repository_hooks; then
  shared+=(hook_privacy hook_documentation hook_code)
fi

run_timed_item() {
  set -euo pipefail
  local item="${1:?}" item_started=$SECONDS
  export CI_TIMING_FILE="$scratch/$item.timing"
  run_item "$item"
  printf '%s\n' $((SECONDS - item_started)) >"$scratch/$item.seconds"
}

failed=()
# set_worker_limit <item>: set worker_limit to the deadline in seconds of the item. A kind without a limit stops the run before anything of it starts.
set_worker_limit() {
  local kind="${1%_[0-9]*}" variable
  variable="worker_limit_seconds_$kind"
  if [[ -z "${!variable:-}" ]]; then
    printf 'The worker %s has no limit: add "%s" to worker_limits in config/quality/ci_groups.json.\n' "$1" "$kind" >&2
    exit 1
  fi
  worker_limit="${worker_override:-${!variable}}"
}
# worker_group <item>: the check group of an item, or the item itself when it is not a group's.
worker_group() {
  local variable="item_group_$1"
  printf '%s' "${!variable:-$1}"
}
# worker_overdue <started seconds> <limit seconds>: true when a worker has run past its limit. SECONDS counts whole wall-clock seconds, so the start and the check are each
# truncated: the difference exceeds the limit only when the worker's real age is above it (at most about 1.5 s late with the poll), while ">=" would stop it up to 1 s early.
worker_overdue() { ((SECONDS - $1 > $2)); }
# write_worker_state: write the functions of this script and the variables that workers read to the private state file (mode 600 in the private scratch folder).
write_worker_state() {
  local name
  (
    umask 077
    {
      declare -f
      declare -p python_bin repo_root scratch quality_output e2e_output acquisition_dir acquisition_shards feature_sources_shards feature_columns_shards
      for name in $(compgen -v item_); do declare -p "$name"; done
    } >"$worker_state"
  )
}
# run_items <item>...: stream a single item, buffer parallel items in order, and retain each log and duration. Each item is a worker in its own session with a deadline.
run_items() {
  local item index pid status pids=() starts=() limits=() items=("$@") worker_limit pending mode=buffer
  if ((${#items[@]} == 1)); then mode=stream; fi
  write_worker_state
  for item in "$@"; do
    set_worker_limit "$item"
    limits+=("$worker_limit")
    starts+=("$SECONDS")
    if [[ "$mode" == stream ]]; then printf '\n== %s ==\n' "$item"; fi
    "$python_bin" -I -c "$worker_session_step" "$repo_root" "$worker_grace" "$BASH" --noprofile --norc -c "$worker_body" ci_worker "$worker_state" "$item" "$mode" </dev/null &
    pids+=("$!")
    worker_pids+=("$!")
    timing_files+=("$scratch/$item.timing")
  done
  pending=${#pids[@]}
  while ((pending > 0)); do
    for index in ${pids[@]+"${!pids[@]}"}; do
      pid="${pids[$index]}"
      if [[ -z "$pid" ]]; then continue; fi
      if ! kill -0 "$pid" 2>/dev/null; then
        status=0
        wait "$pid" || status=$?
        # A worker that ended can leave members in its session (a background process): they are stopped before the worker is forgotten.
        if ! stop_sessions "$worker_grace" "$pid"; then
          printf 'The ended worker %s left members in its session %s that survived KILL, or the session could not be listed.\n' "${items[$index]}" "$pid" >&2
          status=1
        fi
        if ((status != 0)); then
          failed+=("${items[$index]}")
        fi
      elif worker_overdue "${starts[$index]}" "${limits[$index]}"; then
        printf 'Local CI worker %s (check group %s) ran past its deadline of %s s: stopping its session %s.\n' \
          "${items[$index]}" "$(worker_group "${items[$index]}")" "${limits[$index]}" "$pid" >&2
        if stop_sessions "$worker_grace" "$pid"; then
          printf 'Stopped: no process of the session %s is left.\n' "$pid" >&2
        else
          printf 'The session %s still has members after KILL, or it could not be listed.\n' "$pid" >&2
        fi
        wait "$pid" 2>/dev/null || true
        failed+=("${items[$index]} (deadline ${limits[$index]} s)")
      else
        continue
      fi
      forget_worker "$pid"
      pids[index]=""
      pending=$((pending - 1))
    done
    if ((pending > 0)); then sleep 0.5; fi
  done
  for item in "$@"; do
    if ((${#items[@]} != 1)); then
      printf '\n== %s ==\n' "$item"
      cat "$scratch/$item.log"
    fi
    record "$item" "$(cat "$scratch/$item.seconds" 2>/dev/null || printf 'failed')"
  done
}
# A kind of worker without a limit stops the run before the first worker starts.
for item in ${alone[@]+"${alone[@]}"} ${shared[@]+"${shared[@]}"}; do
  set_worker_limit "$item"
done
if is_selected lakehouse_features; then
  set_worker_limit combine_feature_sources
  set_worker_limit combine_feature_columns
fi
if [[ "$part" == "all" ]] && is_selected acquisition; then
  set_worker_limit acquisition_combine
fi
if ((${#alone[@]})); then
  run_items "${alone[@]}"
fi
if ((${#failed[@]})); then
  printf '\nLocal CI failed in: %s\n' "${failed[*]}" >&2
  exit 1
fi
if ((${#shared[@]})); then
  run_items "${shared[@]}"
fi
if ((${#failed[@]})); then
  printf '\nLocal CI failed in: %s\n' "${failed[*]}" >&2
  exit 1
fi

if is_selected lakehouse_features && [[ -z "$feature_shard" ]]; then
  combines=()
  if [[ "$part" != "feature_columns" ]]; then
    combines+=(combine_feature_sources)
  fi
  if [[ "$part" != "feature_sources" ]]; then
    combines+=(combine_feature_columns)
  fi
  run_items "${combines[@]}"
  if ((${#failed[@]})); then
    printf '\nLocal CI failed in: %s\n' "${failed[*]}" >&2
    exit 1
  fi
fi

if [[ "$part" == "all" ]] && is_selected acquisition; then
  run_items acquisition_combine
  if ((${#failed[@]})); then
    printf '\nLocal CI failed in: %s\n' "${failed[*]}" >&2
    exit 1
  fi
fi

printf '\nAll local CI checks passed without AWS credentials.\n'
ci_finished=1
