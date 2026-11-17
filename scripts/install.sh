#!/usr/bin/env bash
# Install this checkout: the environment, the pinned tools, the hash-locked host dependencies, the hooks, the push gate and a first .env file.
#
#   bash scripts/install.sh
#
# Safe to run again: an installed checkout changes nothing and the script succeeds. Each step stops the script and names itself when it fails. It never
# overwrites an existing .env. It checks its prerequisites first and names every one that is missing. HAI_DOCKER_APP names another Docker Desktop folder (the
# install tests set it).
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
python="$repo_root/.venv/bin/python"
terraform_version="1.16.1"
node_major="22"

missing=()
for program in python3.12 git terraform aws node; do
  command -v "$program" > /dev/null 2>&1 || missing+=("$program")
done
docker_app="${HAI_DOCKER_APP:-/Applications/Docker.app}"
[[ -d "$docker_app" ]] || missing+=("Docker Desktop (not found at $docker_app)")
if command -v terraform > /dev/null 2>&1; then
  found="$(terraform version 2> /dev/null | sed -n '1s/^Terraform v//p')"
  [[ "$found" == "$terraform_version" ]] || missing+=("terraform $terraform_version (found ${found:-none})")
fi
if command -v node > /dev/null 2>&1; then
  found="$(node --version 2> /dev/null | sed -n 's/^v\([0-9][0-9]*\)\..*/\1/p')"
  [[ -n "$found" && "$found" -ge "$node_major" ]] || missing+=("node $node_major or later (found ${found:-none})")
fi
if ((${#missing[@]})); then
  printf 'install: missing prerequisites: %s\n' "${missing[*]}" >&2
  exit 1
fi

scratch="$(mktemp -d "${TMPDIR:-/tmp}/hai_install.XXXXXX")"
trap 'rm -rf "$scratch"' EXIT
total=13
number=0

# step <name> <command...>: run one command, name it first, and stop the script with its name when it fails.
step() {
  local name="$1"
  shift
  number=$((number + 1))
  printf 'install: step %d of %d: %s\n' "$number" "$total" "$name"
  if ! "$@"; then
    printf 'install: FAILED at step %d of %d (%s). Fix the cause and run bash scripts/install.sh again.\n' "$number" "$total" "$name" >&2
    exit 1
  fi
}

make_environment() {
  [[ -x "$python" ]] || python3.12 -m venv .venv
}
export_lock() {
  "$python" -m scripts.quality.lock_env export host --output "$scratch/host_lock"
}
install_host_dependencies() {
  "$python" -m pip install --no-cache-dir --require-hashes --only-binary=:all: -r "$scratch/host_lock/requirements.txt"
}
fetch_dbt_archive() {
  "$python" -m scripts.lakehouse.dbt_package_archive verify > /dev/null 2>&1 || "$python" -m scripts.lakehouse.dbt_package_archive fetch
}
install_hooks() {
  PRE_COMMIT_HOME="$repo_root/.tools/pre_commit_cache" "$python" -m pre_commit install
}
make_env_file() {
  [[ -e .env || -L .env ]] || cp .env.example .env
}

step "virtual environment" make_environment
step "pinned tools (install_tools)" "$python" scripts/quality/install_tools.py
step "host lock export" export_lock
step "pip install" install_host_dependencies
step "pip check" "$python" -m pip check
step "install_markdownlint" "$python" -m scripts.quality.install_markdownlint
step "install_sqlfluff" "$python" -m scripts.quality.install_sqlfluff
step "install_dvc" "$python" -m scripts.quality.install_dvc
step "install_lint_tools" "$python" -m scripts.quality.install_lint_tools
step "dbt package archive" fetch_dbt_archive
step "pre-commit hooks" install_hooks
step ".env file" make_env_file
step "push gate (pre_push install-shim)" "$python" -m scripts.quality.pre_push install-shim

printf 'install: done. Fill every value in .env (Git ignores it); .env.example lists the settings.\n'
