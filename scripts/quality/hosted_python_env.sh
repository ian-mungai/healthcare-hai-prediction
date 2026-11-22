#!/usr/bin/env bash
# Build the host Python environment (.venv) of a hosted CI job from the lock, using a cache of the downloaded wheel files. Run from the repository root by the "Install shared
# dependencies" steps of .github/workflows/ci.yml, after scripts/quality/install_tools.py has installed uv:
#   bash scripts/quality/hosted_python_env.sh <true|false: the wheel cache hit> <wheel folder>
# The environment is always built fresh with the interpreter of the job (setup-python): nothing of an earlier or restored environment is kept, so a cache can hold no code that runs.
# pip installs the host group of uv.lock with --require-hashes --only-binary=:all: and checks every wheel against the lock hashes itself. On a hit the wheel folder is the only
# source (--no-index --find-links), so the install makes no network call (the job's own pip drives the install into the new environment, pip --python); if pip rejects it (a wheel is missing or fails its hash) the folder is cleared and the wheels are
# downloaded again. On a miss, or with no folder, pip downloads the wheels into the folder (hash-checked) and the install then reads them from it, so the workflow saves exactly
# the locked wheels. HOSTED_ENV_GROUP (default host), HOSTED_ENV_VENV (default .venv) and HOSTED_ENV_WHEELS (the wheel folder when no second argument is given) are for scripts/quality/run_lock_e2e.py. E2E: scripts/quality/run_lock_e2e.py.
set -euo pipefail

hit="${1:?the first argument is true when the wheel cache was restored, else false}"
wheelhouse="${2:-${HOSTED_ENV_WHEELS:?the second argument (or HOSTED_ENV_WHEELS) is the wheel folder}}"
group="${HOSTED_ENV_GROUP:-host}"
venv="${HOSTED_ENV_VENV:-.venv}"
temp="${RUNNER_TEMP:-${TMPDIR:-/tmp}}"
flags=(--no-cache-dir --require-hashes --only-binary=:all:)

fresh_environment() {
  rm -rf "$venv"
  python -m venv "$venv"
}
fresh_environment
python -m scripts.quality.lock_env export "$group" --output "$temp/${group}_lock"
requirements="$temp/${group}_lock/requirements.txt"
install_from_wheels() {
  python -m pip --python "$venv/bin/python" install "${flags[@]}" --no-index --find-links "$wheelhouse" -r "$requirements"
}
if [[ "$hit" == true && -d "$wheelhouse" ]]; then
  if install_from_wheels; then
    echo "Installed the locked packages offline from the cached wheels."
    exit 0
  fi
  echo "::warning::pip rejected the cached wheels; they are downloaded again."
  rm -rf "$wheelhouse"
  fresh_environment
fi
mkdir -p "$wheelhouse"
python -m pip download "${flags[@]}" --dest "$wheelhouse" -r "$requirements"
install_from_wheels
