#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"
python_bin="${PYTHON_BIN:-$repo_root/.venv/bin/python}"
bash -n scripts/acquisition/run_checks.sh
"$python_bin" -m ruff check --no-respect-gitignore scripts/acquisition tests/test_source_registry.py
"$python_bin" -m mypy --check-untyped-defs --disallow-untyped-defs scripts/acquisition tests/test_source_registry.py --ignore-missing-imports
"$python_bin" -m scripts.acquisition.source_registry
"$python_bin" -m pytest scripts/acquisition/tests tests/test_source_registry.py --cov=scripts.acquisition \
  --cov-config=config/acquisition/coverage.ini --cov-report=term-missing --cov-fail-under=90 -q
