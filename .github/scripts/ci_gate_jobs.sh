#!/usr/bin/env bash
# The job listing check of the CI Checks job of .github/workflows/ci.yml. needs.<job>.result is success for a matrix leg that was cancelled before a runner picked it up, so the
# gate also lists the latest result of every job of its run (a partial rerun carries the jobs of earlier attempts forward) (gh api .../actions/runs/<run>/jobs?filter=latest) and
# passes one line per job to this script: name, status and conclusion, separated by tabs.
# NAMES: the listing must hold exactly the job names of the registry (JOBS_REGISTRY, default config/quality/ci_jobs.json: a JSON list of names), each once, and nothing else. A matrix leg is
# one name, so a missing leg, a missing standalone job, a name listed more than once and a name that is not in the registry each fail and are named, also for an empty listing (every
# expected name is then printed).
# STATUS: it fails and names each job that is not completed, or whose conclusion is not success. Every run is a full run, so no job may be skipped. The job CI Checks itself is left out.
# GATE_JOBS_FILE names a file to read instead of standard input (for the cases of .github/scripts/workflow_input_cases.sh).
set -euo pipefail
export LC_ALL=C

input="${GATE_JOBS_FILE:-/dev/stdin}"
registry="${JOBS_REGISTRY:-config/quality/ci_jobs.json}"
self="CI Checks"
checked=0
bad=""
listed_names=""

while IFS=$'\t' read -r name status conclusion || [[ -n "${name:-}" ]]; do
  if [[ -z "$name" || "$name" == "$self" ]]; then
    continue
  fi
  checked=$((checked + 1))
  listed_names+="$name"$'\n'
  if [[ "$status" != "completed" ]]; then
    bad+="  $name: status '$status', not completed"$'\n'
    continue
  fi
  if [[ "$conclusion" != "success" ]]; then
    bad+="  $name: conclusion '$conclusion'"$'\n'
  fi
done <"$input"

if ((checked == 0)); then
  bad+="  no job of this run was listed"$'\n'
fi
expected_names="$(jq -r --arg self "$self" '.[] | select(. != $self)' "$registry" | sort -u)"
listed_sorted="$(printf '%s' "$listed_names" | sort)"
while IFS= read -r name; do
  [[ -n "$name" ]] && bad+="  $name: expected by $registry, but not listed"$'\n'
done < <(comm -23 <(printf '%s\n' "$expected_names") <(printf '%s\n' "$listed_sorted" | sort -u))
while IFS= read -r name; do
  [[ -n "$name" ]] && bad+="  $name: listed more than once"$'\n'
done < <(printf '%s\n' "$listed_sorted" | uniq -d)
while IFS= read -r name; do
  [[ -n "$name" ]] && bad+="  $name: listed, but not in $registry"$'\n'
done < <(comm -13 <(printf '%s\n' "$expected_names") <(printf '%s\n' "$listed_sorted" | sort -u))
if [[ -n "$bad" ]]; then
  {
    echo "These jobs of this run attempt did not finish as this run requires, so the gate fails:"
    printf '%s' "$bad"
  } | tee -a "${GITHUB_STEP_SUMMARY:-/dev/null}"
  exit 1
fi
echo "All $checked other jobs of this run attempt finished as this run requires."
