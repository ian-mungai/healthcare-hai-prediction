#!/usr/bin/env bash
# E2E for scripts/acquisition/run_checks_ci.sh, the GitHub CI entry for the acquisition checks. Run from any folder:
#   bash scripts/acquisition/run_checks_ci_e2e.sh
# It copies the working versions of the CI entry and .pre-commit-config.yaml into a scratch Git worktree at HEAD,
# replaces run_checks.sh there with a stub that records each run, then runs the CI entry over committed ranges and
# synthetic cases (failure modes in data/conformance/20261005/ci_path_filter_failure_modes.md). The stub keeps each case
# under a few seconds; the real checks are not run. Results go to data/e2e/acquisition_ci_gate/<UTC>/results.txt.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"
python_bin="${PYTHON_BIN:-$repo_root/.venv/bin/python}"
out="$repo_root/data/e2e/acquisition_ci_gate/$(date -u +%Y%m%dT%H%M%SZ)_$$"
mkdir -p "$out"
results="$out/results.txt"
scratch="$(mktemp -d "${TMPDIR:-/tmp}/acquisition_ci_gate.XXXXXX")"
work="$scratch/worktree"
# The removal and the folder check use the resolved path (on macOS /var is /private/var).
work_resolved="$(cd "$scratch" && pwd -P)/worktree"
# The registered worktrees are listed for the report only; nothing decides on them.
registered() { git worktree list --porcelain | sed -n 's/^worktree //p' | sort; }
before="$(registered)" || before="(git worktree list failed)"
shielded() ( trap '' INT TERM HUP ALRM; "$@" )
cleanup() {
  local status=$? after interrupted=0 failed=0
  trap 'interrupted=1' INT TERM HUP ALRM
  # Only Git's own answers about this run's worktree count, because other agents add and remove their own worktrees in the same repository meanwhile.
  if ! shielded git worktree remove --force "$work_resolved" >/dev/null 2>"$out/worktree_remove.err"; then
    echo "FAIL scratch_worktree_removed: git worktree remove failed for $work; see $out/worktree_remove.err" | tee -a "$results" >&2
    failed=1
    # A failed deletion must not stop the cleanup under set -e: the folder check below reports what is left.
    shielded rm -rf "$work_resolved" || true
  fi
  if ! shielded git worktree prune; then
    echo "FAIL scratch_worktree_removed: git worktree prune failed" | tee -a "$results" >&2
    failed=1
  fi
  if [[ -e "$work_resolved" ]]; then
    echo "FAIL scratch_worktree_removed: the folder of this run's worktree $work still exists" | tee -a "$results" >&2
    failed=1
  fi
  shielded rm -rf "$scratch" || echo "scratch worktree: could not delete $scratch" >&2
  after="$(shielded registered)" || after="(git worktree list failed)"
  printf '%s\n' "$before" >"$out/worktrees_before.txt"
  printf '%s\n' "$after" >"$out/worktrees_after.txt"
  if ((failed)); then
    status=1
  else
    echo "PASS scratch_worktree_removed" | tee -a "$results"
    if [[ "$after" != "$before" ]]; then
      echo "NOTE the registered worktrees changed during the run (other runs add and remove their own; report only); see $out/worktrees_before.txt and $out/worktrees_after.txt" | tee -a "$results"
    fi
  fi
  if ((interrupted)) && [[ "$status" == 0 ]]; then status=130; fi
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM HUP ALRM

GIT_LFS_SKIP_SMUDGE=1 git worktree add --quiet --detach "$work" HEAD
cp scripts/acquisition/run_checks_ci.sh scripts/acquisition/run_checks_combine.sh scripts/acquisition/shards.py "$work/scripts/acquisition/"
mkdir -p "$work/scripts/quality" "$work/config/acquisition"
cp scripts/quality/timing.sh "$work/scripts/quality/timing.sh"
cp config/acquisition/shard_weights.json config/acquisition/coverage.ini "$work/config/acquisition/"
cp .pre-commit-config.yaml "$work/.pre-commit-config.yaml"
cat >"$work/scripts/acquisition/run_checks.sh" <<'STUB'
#!/usr/bin/env bash
echo "stub run" >>"$STUB_MARKER"
printf 'ran\n' >>"${ACQUISITION_RAN_MARKER:-/dev/null}"
exit "${STUB_EXIT:-0}"
STUB
# Staged, so the pre-commit stash of unstaged changes leaves them in place.
git -C "$work" add scripts/acquisition/run_checks_ci.sh scripts/acquisition/run_checks_combine.sh scripts/acquisition/shards.py scripts/acquisition/run_checks.sh \
  scripts/quality/timing.sh config/acquisition/shard_weights.json config/acquisition/coverage.ini .pre-commit-config.yaml

failures=0
# case <name> <expected exit> <expected stub runs> <expected output text> [VAR=value ...]
case_run() {
  local name="$1" want_exit="$2" want_runs="$3" want_text="$4"
  shift 4
  local marker="$scratch/$name.marker" log="$out/$name.log" code=0 runs
  : >"$marker"
  (cd "$work" && env -u RANGE_START -u RANGE_END PYTHON_BIN="$python_bin" PRE_COMMIT_HOME="$repo_root/.tools/pre_commit_cache" \
    GITHUB_STEP_SUMMARY="$out/$name.summary" STUB_MARKER="$marker" "$@" bash scripts/acquisition/run_checks_ci.sh) >"$log" 2>&1 || code=$?
  runs="$(wc -l <"$marker" | tr -d ' ')"
  if [[ "$code" == "$want_exit" && "$runs" == "$want_runs" ]] && grep -q -- "$want_text" "$log"; then
    echo "PASS $name (exit $code, stub runs $runs)" | tee -a "$results"
  else
    echo "FAIL $name: exit $code (want $want_exit), stub runs $runs (want $want_runs), text '$want_text' found: $(grep -c -- "$want_text" "$log" || true)" | tee -a "$results"
    failures=$((failures + 1))
  fi
}

docs_only=9c22c1e      # README, .gitignore and the architecture files only
acquisition=7a967b2    # requirements.txt and config/acquisition/runtime_dependencies.json
ci_only=f9ef312        # ci.yml with quality scripts, no acquisition path before this change
missing=0123456789abcdef0123456789abcdef01234567

case_run docs_only_range_skips 0 0 "Skipped" RANGE_START="$docs_only^" RANGE_END="$docs_only"
case_run acquisition_range_runs 0 1 "Passed" RANGE_START="$acquisition^" RANGE_END="$acquisition"
case_run ci_workflow_range_runs 0 1 "Passed" RANGE_START="$ci_only^" RANGE_END="$ci_only"
case_run acquisition_failure_fails 1 1 "Failed" RANGE_START="$acquisition^" RANGE_END="$acquisition" STUB_EXIT=3
case_run first_push_runs_all 0 1 "no usable range" RANGE_START=0000000000000000000000000000000000000000 RANGE_END="$acquisition"
case_run rewritten_history_runs_all 0 1 "no usable range" RANGE_START="$missing" RANGE_END="$acquisition"
case_run manual_run_runs_all 0 1 "no usable range"
case_run full_run_failure_fails 3 1 "no usable range" STUB_EXIT=3
# Shards and the combining job: a range that does not reach the checks gives a skipped manifest per shard and a combining job that accepts only that.
# The shard count is the count of the CI jobs (shards.CI_SHARD_COUNT), not a number written here.
count="$("$python_bin" -c 'from scripts.acquisition import shards; print(shards.CI_SHARD_COUNT)')"
if ! [[ "$count" =~ ^[1-9][0-9]*$ ]]; then
  echo "FAIL shard_count_known: shards.CI_SHARD_COUNT gave '$count'" | tee -a "$results"
  exit 1
fi
shards_ok="$scratch/shards_ok"
shards_mixed="$scratch/shards_mixed"
for ((index = 1; index <= count; index++)); do
  case_run "shard_${index}_skips_with_a_manifest" 0 0 "does not reach the acquisition checks" RANGE_START="$docs_only^" RANGE_END="$docs_only" \
    ACQUISITION_PART=shard ACQUISITION_SHARD="$index/$count" ACQUISITION_SHARD_DIR="$shards_ok"
done
case_run combine_accepts_all_shards_skipped 0 0 "All $count shards were skipped" RANGE_START="$docs_only^" RANGE_END="$docs_only" ACQUISITION_PART=combine ACQUISITION_SHARD_DIR="$shards_ok"
case_run mixed_shard_1_skips 0 0 "does not reach the acquisition checks" RANGE_START="$docs_only^" RANGE_END="$docs_only" \
  ACQUISITION_PART=shard ACQUISITION_SHARD="1/$count" ACQUISITION_SHARD_DIR="$shards_mixed"
case_run combine_rejects_a_missing_shard 1 0 "shard 2 of $count is missing" RANGE_START="$docs_only^" RANGE_END="$docs_only" ACQUISITION_PART=combine ACQUISITION_SHARD_DIR="$shards_mixed"
case_run combine_rejects_a_count_other_than_the_shards 1 0 "shard 1 of $((count + 1)) is missing" RANGE_START="$docs_only^" RANGE_END="$docs_only" \
  ACQUISITION_PART=combine ACQUISITION_SHARD_COUNT="$((count + 1))" ACQUISITION_SHARD_DIR="$shards_ok"
case_run shard_runs_when_the_range_reaches_the_checks 0 1 "Passed" RANGE_START="$acquisition^" RANGE_END="$acquisition" \
  ACQUISITION_PART=shard ACQUISITION_SHARD="1/$count" ACQUISITION_SHARD_DIR="$scratch/shards_run"
case_run combine_runs_when_the_range_reaches_the_checks 0 1 "Passed" RANGE_START="$acquisition^" RANGE_END="$acquisition" ACQUISITION_PART=combine ACQUISITION_SHARD_DIR="$scratch/shards_run"
if grep -q "range" "$out/docs_only_range_skips.summary" 2>/dev/null; then
  echo "PASS job_summary_names_range" | tee -a "$results"
else
  echo "FAIL job_summary_names_range: the job summary does not name the range" | tee -a "$results"
  failures=$((failures + 1))
fi

echo "$failures failed; results $results"
[[ "$failures" == 0 ]]
