#!/usr/bin/env bash
# Build or restore the host Python environment (.venv) of a hosted CI job. Run from the repository root by the "Install shared dependencies" steps of .github/workflows/ci.yml:
#   bash scripts/quality/hosted_python_env.sh <true|false: the environment cache hit> [scripts/quality/install_tools.py arguments...]
# The native tools (uv, gitleaks and the rest) are downloaded from the pins of config/quality_tools.json and checked by SHA-256 on every run; nothing restored from a cache replaces that check.
# A restored .venv is used only after `lock_env verify host` (scripts/quality/lock_env.py) finds exactly the pinned packages with every installed file at its recorded hash. pip accepts a package
# that a restored environment already holds without looking at its hashes, so any other state (a miss, no restored folder, a failed verification) builds the environment fresh with the
# hash-pinned install (--require-hashes, wheels only). Every command runs with the interpreter of the job (setup-python), and the verification reads the restored files without running the restored interpreter. E2E: scripts/quality/run_lock_e2e.py.
set -euo pipefail

restored="${1:?the first argument is true when the environment cache was restored, else false}"
shift

if [[ "$restored" == true && ! -x .venv/bin/python ]]; then
  restored=false
fi
if [[ "$restored" != true ]]; then
  python -m venv .venv
fi
python scripts/quality/install_tools.py "$@"
if [[ "$restored" == true ]]; then
  if python -m scripts.quality.lock_env verify host --environment .venv; then
    echo "The restored Python environment matches the lock: no install."
    exit 0
  fi
  echo "The restored Python environment differs from the lock: it is rebuilt from the hash-pinned requirements."
  rm -rf .venv
  python -m venv .venv
fi
python -m scripts.quality.lock_env export host --output "$RUNNER_TEMP/host_lock"
.venv/bin/python -m pip install --no-cache-dir --require-hashes --only-binary=:all: -r "$RUNNER_TEMP/host_lock/requirements.txt"
