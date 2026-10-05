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
cleanup() {
  git worktree remove --force "$work" >/dev/null 2>&1 || true
  rm -rf "$scratch"
}
trap cleanup EXIT

GIT_LFS_SKIP_SMUDGE=1 git worktree add --quiet --detach "$work" HEAD
cp scripts/acquisition/run_checks_ci.sh "$work/scripts/acquisition/run_checks_ci.sh"
cp .pre-commit-config.yaml "$work/.pre-commit-config.yaml"
cat >"$work/scripts/acquisition/run_checks.sh" <<'STUB'
#!/usr/bin/env bash
echo "stub run" >>"$STUB_MARKER"
exit "${STUB_EXIT:-0}"
STUB
# Staged, so the pre-commit stash of unstaged changes leaves them in place.
git -C "$work" add scripts/acquisition/run_checks_ci.sh scripts/acquisition/run_checks.sh .pre-commit-config.yaml

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
if grep -q "range" "$out/docs_only_range_skips.summary" 2>/dev/null; then
  echo "PASS job_summary_names_range" | tee -a "$results"
else
  echo "FAIL job_summary_names_range: the job summary does not name the range" | tee -a "$results"
  failures=$((failures + 1))
fi

echo "$failures failed; results $results"
[[ "$failures" == 0 ]]
