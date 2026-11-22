#!/usr/bin/env bash
# E2E for scripts/acquisition/run_checks_ci.sh, the GitHub CI entry for the acquisition checks. Run from any folder:
#   bash scripts/acquisition/run_checks_ci_e2e.sh
# It copies the working version of the CI entry into a scratch Git worktree at HEAD, replaces run_checks.sh there with a stub that
# records each run, then runs the CI entry and checks that it starts run_checks.sh once and passes its exit status on. The stub keeps each case
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
      echo "NOTE the registered worktrees changed during the run (other runs add and remove their own). This note is for the report only. See $out/worktrees_before.txt and $out/worktrees_after.txt" | tee -a "$results"
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
cat >"$work/scripts/acquisition/run_checks.sh" <<'STUB'
#!/usr/bin/env bash
echo "stub run" | tee -a "$STUB_MARKER"
exit "${STUB_EXIT:-0}"
STUB
# Staged, so the pre-commit stash of unstaged changes leaves them in place.
git -C "$work" add scripts/acquisition/run_checks_ci.sh scripts/acquisition/run_checks_combine.sh scripts/acquisition/shards.py scripts/acquisition/run_checks.sh \
  scripts/quality/timing.sh config/acquisition/shard_weights.json config/acquisition/coverage.ini

failures=0
# case <name> <expected exit> <expected stub runs> <expected output text> [VAR=value ...]
case_run() {
  local name="$1" want_exit="$2" want_runs="$3" want_text="$4"
  shift 4
  local marker="$scratch/$name.marker" log="$out/$name.log" code=0 runs
  : >"$marker"
  (cd "$work" && env -u RANGE_START -u RANGE_END PYTHON_BIN="$python_bin" \
    GITHUB_STEP_SUMMARY="$out/$name.summary" STUB_MARKER="$marker" "$@" bash scripts/acquisition/run_checks_ci.sh) >"$log" 2>&1 || code=$?
  runs="$(wc -l <"$marker" | tr -d ' ')"
  if [[ "$code" == "$want_exit" && "$runs" == "$want_runs" ]] && grep -q -- "$want_text" "$log"; then
    echo "PASS $name (exit $code, stub runs $runs)" | tee -a "$results"
  else
    echo "FAIL $name: exit $code (want $want_exit), stub runs $runs (want $want_runs), text '$want_text' found: $(grep -c -- "$want_text" "$log" || true)" | tee -a "$results"
    failures=$((failures + 1))
  fi
}

case_run whole_run_runs_the_checks 0 1 "stub run"
case_run whole_run_failure_fails 3 1 "stub run" STUB_EXIT=3
echo "$failures failed; results $results"
[[ "$failures" == 0 ]]
