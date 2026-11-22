#!/usr/bin/env bash
# The job listing check of the CI Checks job of .github/workflows/ci.yml. needs.<job>.result is success for a matrix leg that was cancelled before a runner picked it up, so the
# gate also lists the latest result of every job of its run (a partial rerun carries the jobs of earlier attempts forward) (gh api .../actions/runs/<run>/jobs?filter=latest) and
# passes one line per job to this script: name, status and conclusion, separated by tabs.
# NAMES: without a verified reuse (REUSE is not true) the listing must hold exactly the job names of the registry (JOBS_REGISTRY, default config/quality/ci_jobs.json: its
# required_pull_request_jobs and skipped_in_pull_request_runs), each once, and nothing else. A matrix leg is one name, so a missing leg, a missing standalone job, a name listed
# more than once and a name that is not in the registry each fail and are named. With a verified reuse the skipped matrix jobs are listed unexpanded, so names are not checked.
# STATUS: it fails and names each job that is not completed, or whose conclusion is not success; a skip is allowed only for the jobs that a verified reuse skips (every job but
# the quick gates when REUSE is true), and the quick gates of a reused result must be skipped without reuse and must have succeeded with it. The job CI Checks itself is left out.
# REUSE is the plan's answer. GATE_JOBS_FILE names a file to read instead of standard input (for scripts/quality/run_ci_reuse_e2e.py).
set -euo pipefail

reuse="${REUSE:-}"
input="${GATE_JOBS_FILE:-/dev/stdin}"
registry="${JOBS_REGISTRY:-config/quality/ci_jobs.json}"
quick="Quick Gates of a Reused Result"
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
elif [[ "$reuse" != "true" ]]; then
  expected_names="$(jq -r --arg self "$self" '(.required_pull_request_jobs + .skipped_in_pull_request_runs)[] | select(. != $self)' "$registry" | sort -u)"
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
fi
if [[ -n "$bad" ]]; then
  {
    echo "These jobs of this run attempt did not finish as this run requires (reuse answer: '$reuse'), so the gate fails:"
    printf '%s' "$bad"
  } | tee -a "${GITHUB_STEP_SUMMARY:-/dev/null}"
  exit 1
fi
echo "All $checked other jobs of this run attempt finished as this run requires."
