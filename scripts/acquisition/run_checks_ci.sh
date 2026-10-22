#!/usr/bin/env bash
# GitHub CI entry for the acquisition checks: they run only when the pushed range changes a
# path the local acquisition-checks hook watches. pre-commit applies that hook's own files pattern, so CI and the local
# hook share one path list. Without a usable range (a manual run, a push with no earlier commit, rewritten history), every check
# runs. ACQUISITION_PART selects what runs: empty is the whole run (run_checks.sh), "shard" (with ACQUISITION_SHARD=K/N) is one shard of
# the pytest run and "combine" is the combining job (scripts/acquisition/shards.py). A shard whose range does not reach the checks writes a
# skipped manifest, and the combining job then requires every manifest to say skipped, so a skipped shard can never pass beside a shard that ran.
# E2E: bash scripts/acquisition/run_checks_ci_e2e.sh
#   RANGE_START=<sha> RANGE_END=<sha> bash scripts/acquisition/run_checks_ci.sh
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"
python_bin="${PYTHON_BIN:-$repo_root/.venv/bin/python}"
summary="${GITHUB_STEP_SUMMARY:-/dev/null}"
start="${RANGE_START:-}"
end="${RANGE_END:-}"
part="${ACQUISITION_PART:-}"
directory="${ACQUISITION_SHARD_DIR:-$repo_root/data/e2e/acquisition_shards}"

if [[ -z "$start" || -z "$end" || "$start" =~ ^0+$ ]] || ! git cat-file -e "$start^{commit}" 2>/dev/null || ! git cat-file -e "$end^{commit}" 2>/dev/null; then
  echo "Acquisition checks: no usable range (start '${start:-none}'), so every check runs." | tee -a "$summary"
  exec bash scripts/acquisition/run_checks.sh
fi
echo "Acquisition checks: range $start..$end; they run only if it changes a path the acquisition-checks hook watches." | tee -a "$summary"
marker="$(mktemp "${TMPDIR:-/tmp}/acquisition_ran.XXXXXX")"
trap 'rm -f "$marker"' EXIT
export ACQUISITION_RAN_MARKER="$marker"
status=0
"$python_bin" -m pre_commit run acquisition-checks --hook-stage pre-commit --from-ref "$start" --to-ref "$end" || status=$?
if [[ "$status" == 0 && -n "$part" && ! -s "$marker" ]]; then
  reason="the range $start..$end does not reach the acquisition checks"
  echo "Acquisition checks ($part): $reason." | tee -a "$summary"
  if [[ "$part" == "shard" ]]; then
    "$python_bin" -m scripts.acquisition.shards skip --directory "$directory" --shard "${ACQUISITION_SHARD:?}" --reason "$reason"
  else
    ACQUISITION_EXPECT=skip bash scripts/acquisition/run_checks_combine.sh
  fi
fi
exit "$status"
