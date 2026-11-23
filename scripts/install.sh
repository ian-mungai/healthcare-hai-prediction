#!/usr/bin/env bash
# Install this checkout: the environment, the pinned tools, the hash-locked host dependencies, the hooks, the push gate and a first .env file.
#
#   bash scripts/install.sh
#
# Safe to run again: an installed checkout changes nothing and the script succeeds. Each step stops the script and names itself when it fails. It never
# overwrites an existing .env. It checks its prerequisites first and names every one that is missing. HAI_DOCKER_APP names another Docker Desktop folder (the
# install tests set it).
set -euo pipefail

# Follow a symbolic link to the real script first (old Bash on macOS has no readlink -f), then take the repository root from the real location.
source_path="${BASH_SOURCE[0]}"
while [[ -L "$source_path" ]]; do
  link_directory="$(cd -P "$(dirname "$source_path")" && pwd)"
  source_path="$(readlink "$source_path")"
  [[ "$source_path" == /* ]] || source_path="$link_directory/$source_path"
done
repo_root="$(cd -P "$(dirname "$source_path")/.." && pwd)"
cd "$repo_root"
python="$repo_root/.venv/bin/python"
terraform_version="1.16.1" # the stacks require "~> 1.16.1": this release or a later 1.16 release
node_major="22"
git_minimum="2.54" # the same minimum as MINIMUM_GIT in scripts/quality/pre_push.py (the push gate refuses an older Git)

# Every prerequisite check is read-only. A problem goes on the list and the script names the whole list before it installs anything.
missing=()
tool_output=""
tool_state="absent"
# probe <program> <arguments...>: run a version command. tool_state becomes absent (no such program), failed (the command failed) or ok (tool_output holds
# its first output line). It always returns 0, so a failing probe never stops the script before the message.
probe() {
  tool_output=""
  if ! command -v "$1" > /dev/null 2>&1; then
    tool_state="absent"
    return 0
  fi
  if tool_output="$("$@" 2> /dev/null)"; then
    tool_state="ok"
    tool_output="${tool_output%%$'\n'*}"
  else
    tool_state="failed"
  fi
  return 0
}
# unusable <name> <requirement>: add the problem found by the last probe (absent or failed) to the list.
unusable() {
  if [[ "$tool_state" == "absent" ]]; then
    missing+=("$1 (not found)")
  else
    missing+=("$1 (its version command failed)")
  fi
}

probe python3.12 --version
if [[ "$tool_state" != "ok" ]]; then
  unusable python3.12
elif [[ "$tool_output" != "Python 3.12."* ]]; then
  missing+=("python3.12 (found ${tool_output:-no version})")
fi

probe git --version
if [[ "$tool_state" != "ok" ]]; then
  unusable git
else
  git_found="$(printf '%s\n' "$tool_output" | sed -n 's/^git version \([0-9][0-9]*\)\.\([0-9][0-9]*\).*/\1 \2/p')"
  read -r git_major git_minor <<< "$git_found" || true
  if [[ -z "$git_found" ]]; then
    missing+=("git (its version could not be read)")
  elif ((git_major < ${git_minimum%%.*} || (git_major == ${git_minimum%%.*} && git_minor < ${git_minimum##*.}))); then
    missing+=("Git $git_minimum or later (found $git_major.$git_minor)")
  fi
fi

probe terraform version
if [[ "$tool_state" != "ok" ]]; then
  unusable terraform
else
  terraform_found="${tool_output#Terraform v}"
  terraform_minor="${terraform_version%.*}"
  terraform_patch="${terraform_found#"$terraform_minor".}"
  if [[ "$terraform_found" != "$terraform_minor".* || ! "$terraform_patch" =~ ^[0-9]+$ ]] || ((terraform_patch < ${terraform_version##*.})); then
    missing+=("terraform $terraform_version or a later $terraform_minor release (found $terraform_found)")
  fi
fi

probe aws --version
if [[ "$tool_state" != "ok" ]]; then
  unusable aws
elif [[ "$tool_output" != aws-cli/2.* ]]; then
  missing+=("AWS CLI v2 (found ${tool_output%% *})")
fi

probe session-manager-plugin --version
if [[ "$tool_state" != "ok" ]]; then
  unusable session-manager-plugin
fi

probe node --version
if [[ "$tool_state" != "ok" ]]; then
  unusable node
else
  node_found="$(printf '%s\n' "$tool_output" | sed -n 's/^v\([0-9][0-9]*\)\..*/\1/p')"
  if [[ -z "$node_found" ]]; then
    missing+=("node (its version could not be read)")
  elif ((node_found < node_major)); then
    missing+=("node $node_major or later (found $node_found)")
  fi
fi

docker_app="${HAI_DOCKER_APP:-/Applications/Docker.app}"
[[ -d "$docker_app" ]] || missing+=("Docker Desktop (not found at $docker_app)")

if ((${#missing[@]})); then
  printf 'install: missing prerequisites:\n' >&2
  printf '  %s\n' "${missing[@]}" >&2
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
