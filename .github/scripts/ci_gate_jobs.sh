#!/usr/bin/env bash
# The job listing check of the CI Checks job of .github/workflows/ci.yml. needs.<job>.result is success for a matrix leg that was cancelled before a runner picked it up, so the
# gate also lists the latest result of every job of its run (a partial rerun carries the jobs of earlier attempts forward) (gh api .../actions/runs/<run>/jobs?filter=latest) and pipes one line per job here: name, status and conclusion, separated by tabs.
# REUSE is the plan's answer, EXPECTED_JOBS the number of jobs CI Checks needs: the listing holds at least one job for each of them (a matrix gives one per leg), so fewer fails. It fails and names each job that is not completed, or whose conclusion is not success; a skip is allowed only for the jobs that a verified reuse
# skips (every job but the quick gates when REUSE is true), and the quick gates of a reused result must be skipped without reuse and must have succeeded with it. The job CI Checks
# itself is left out. GATE_JOBS_FILE names a file to read instead of standard input (for scripts/quality/run_ci_reuse_e2e.py).
set -euo pipefail

reuse="${REUSE:-}"
expected="${EXPECTED_JOBS:-0}"
input="${GATE_JOBS_FILE:-/dev/stdin}"
quick="Quick Gates of a Reused Result"
self="CI Checks"
checked=0
bad=""

while IFS=$'\t' read -r name status conclusion || [[ -n "${name:-}" ]]; do
  if [[ -z "$name" || "$name" == "$self" ]]; then
    continue
  fi
  checked=$((checked + 1))
  if [[ "$status" != "completed" ]]; then
    bad+="  $name: status '$status', not completed"$'\n'
    continue
  fi
  if [[ "$name" == "$quick" ]]; then
    if [[ "$reuse" == "true" ]]; then wanted="success"; else wanted="skipped"; fi
    if [[ "$conclusion" != "$wanted" ]]; then
      bad+="  $name: conclusion '$conclusion', this run needs '$wanted'"$'\n'
    fi
  elif [[ "$conclusion" == "success" ]]; then
    :
  elif [[ "$conclusion" == "skipped" && "$reuse" == "true" ]]; then
    :
  else
    bad+="  $name: conclusion '$conclusion'"$'\n'
  fi
done <"$input"

if ((checked == 0)); then
  bad="  no job of this run was listed"$'\n'
elif ((checked < expected)); then
  bad+="  only $checked jobs other than CI Checks were listed, but at least $expected are expected (one for each job this job needs): $((expected - checked)) missing from the listing"$'\n'
fi
if [[ -n "$bad" ]]; then
  {
    echo "These jobs of this run attempt did not finish as this run requires (reuse answer: '$reuse'), so the gate fails:"
    printf '%s' "$bad"
  } | tee -a "${GITHUB_STEP_SUMMARY:-/dev/null}"
  exit 1
fi
echo "All $checked other jobs of this run attempt finished as this run requires."
