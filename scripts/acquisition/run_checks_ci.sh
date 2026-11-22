#!/usr/bin/env bash
# GitHub CI entry for the acquisition checks. Every hosted run is a full run, so this entry starts scripts/acquisition/run_checks.sh unchanged.
# ACQUISITION_PART selects what runs: empty is the whole run, "shard" (with ACQUISITION_SHARD=K/N) is one shard of the pytest run and "combine" is the
# combining job (scripts/acquisition/shards.py); run_checks.sh dispatches on it.
# E2E: bash scripts/acquisition/run_checks_ci_e2e.sh
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"
exec bash scripts/acquisition/run_checks.sh
