"""The owner's script that runs the code-staging bootstrap: its shape, its inputs from .env, its masked output and its stop, run with a stand-in tool."""

from __future__ import annotations

import os
import re
import shutil
from pathlib import Path

import pytest

from scripts.process import CompletedProcess, run_command
from tests.support import check

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = "scripts/infrastructure/owner/code_staging_github.sh"
REGION, PROFILE, BUCKET, PROJECT = "us-west-2", "stand-in-profile", "stand-in-bucket-name", "stand_in_project"
BASH = shutil.which("bash")


def block() -> str:
    return (ROOT / SCRIPT).read_text()


def test_the_script_is_a_bash_script_that_stops_on_error_names_no_placeholder_and_reads_only_names_the_example_env_has() -> None:
    body = block()
    check(body.startswith("#!/usr/bin/env bash\n"), "a bash script")
    check("set -euo pipefail" in body and re.search(r"trap '.*' ERR", body) is not None, "it stops on error and traps ERR")
    check(re.search(r"<[A-Za-z_]+>", re.sub(r"<(bucket|profile|project|region)>", "", body)) is None, "no placeholder is left for the owner to fill")
    names = set(re.findall(r"\bvalue ([A-Z0-9_]+)\b", body))
    example = {line.split("=")[0] for line in (ROOT / ".env.example").read_text().splitlines() if "=" in line and not line.startswith("#")}
    check(names == {"AWS_REGION", "AWS_PROFILE", "S3_BUCKET", "PROJECT_NAME"} and names <= example, f"names read from .env: {names}")
    check("github_code_staging_bootstrap" in body, "it runs the bootstrap script")


def test_the_script_passes_the_bootstrap_no_option_because_it_takes_everything_from_env_and_terraform() -> None:
    line = next(item for item in block().splitlines() if "github_code_staging_bootstrap" in item)
    check("--" not in line.split("github_code_staging_bootstrap", 1)[1].replace("2>&1", ""), f"the script passes an option the bootstrap does not take: {line}")


TOOLS = ("git", "grep", "head", "sed", "bash")
ISOLATION = ("GIT_CONFIG_NOSYSTEM", "GIT_CONFIG_GLOBAL")


def stand_in(tmp_path: Path, env_text: str, signed_in: bool = True) -> Path:
    """A repository folder whose .venv python and ``gh`` are stand-ins, and a child environment that reaches no other ``gh``, GitHub login, token or Git
    configuration. The environment isolates PATH, the ``gh`` login and the Git configuration files. It does not isolate the network."""
    for name in ("home", "xdg", "empty_gh_config"):
        (tmp_path / name).mkdir()
    (tmp_path / "empty_gitconfig").write_text("")
    run_command("git", ["init", "-q", str(tmp_path)], env={"PATH": os.environ["PATH"], **git_isolation(tmp_path)}, check=True)
    (tmp_path / ".env").write_text(env_text)
    (tmp_path / ".venv/bin").mkdir(parents=True)
    tool = tmp_path / ".venv/bin/python"
    script = [
        "#!/bin/sh",
        f'echo "$@" >> "{tmp_path}/calls.txt"',
        f'case "$*" in *github_code_staging_bootstrap*) echo "profile=$AWS_PROFILE {PROFILE} {BUCKET} {PROJECT} {REGION}" >&2;; esac',
    ]
    tool.write_text("\n".join([*script, "echo 'read back: every part matches'", ""]))
    tool.chmod(0o755)
    (tmp_path / "gh_bin").mkdir()
    gh = tmp_path / "gh_bin/gh"
    gh.write_text(
        "\n".join(["#!/bin/sh", f'echo "$@" >> "{tmp_path}/gh_calls.txt"', f'[ "$*" = "auth status" ] && exit {0 if signed_in else 1}', "exit 99", ""])
    )
    gh.chmod(0o755)
    (tmp_path / "tool_bin").mkdir()
    for name in TOOLS:
        found = shutil.which(name)
        if found is None:
            pytest.fail(f"{name} is not installed")
        wrapper = tmp_path / "tool_bin" / name
        wrapper.write_text(f'#!/bin/sh\nexec "{found}" "$@"\n')
        wrapper.chmod(0o755)
    (tmp_path / "block.sh").write_text(block())
    return tmp_path


def git_isolation(folder: Path) -> dict[str, str]:
    """Git reads no system or global configuration: the system file is off, the global file is an empty one and HOME and XDG_CONFIG_HOME are empty folders."""
    return {
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_CONFIG_GLOBAL": str(folder / "empty_gitconfig"),
        "HOME": str(folder / "home"),
        "XDG_CONFIG_HOME": str(folder / "xdg"),
    }


def child_environment(folder: Path) -> dict[str, str]:
    """The child's whole environment: only the stand-in ``gh`` and the wrapped tools on PATH, an empty GitHub configuration folder, no token and no Git
    configuration outside the repository. It sets no network limit."""
    return {"PATH": f"{folder / 'gh_bin'}:{folder / 'tool_bin'}", "GH_CONFIG_DIR": str(folder / "empty_gh_config"), **git_isolation(folder)}


def run_block(folder: Path) -> CompletedProcess[str]:
    return run_command(str(BASH), ["block.sh"], cwd=folder, env=child_environment(folder), timeout=60)


FULL = f'PROJECT_NAME={PROJECT}\nAWS_REGION="{REGION}"\nAWS_PROFILE={PROFILE}\nS3_BUCKET={BUCKET}\nOTHER=1\n'


@pytest.mark.skipif(BASH is None, reason="bash is not installed")
def test_the_script_gives_the_bootstrap_the_profile_of_env_and_masks_every_env_value_in_the_output(tmp_path: Path) -> None:
    folder = stand_in(tmp_path, FULL)
    done = run_block(folder)
    shown = done.stdout + done.stderr
    calls = (folder / "calls.txt").read_text().splitlines()
    check(done.returncode == 0, f"the block ran: {shown}")
    check(any("github_code_staging_bootstrap" in call for call in calls), f"the script was not run: {calls}")
    check((folder / "gh_calls.txt").read_text().splitlines() == ["auth status"], "the sign-in check reached the stand-in gh once and nothing else")
    check(all(value not in shown for value in (PROFILE, BUCKET, PROJECT, REGION)), f"the output shows a value it must mask: {shown}")


@pytest.mark.skipif(BASH is None, reason="bash is not installed")
def test_a_gh_that_is_not_signed_in_stops_the_script_before_the_bootstrap_runs_and_says_to_sign_in(tmp_path: Path) -> None:
    folder = stand_in(tmp_path, FULL, signed_in=False)
    done = run_block(folder)
    check(done.returncode != 0 and "gh auth login" in done.stderr, f"it stopped and said to sign in: {done.returncode} {done.stderr}")
    check((folder / "gh_calls.txt").read_text().splitlines() == ["auth status"], "the stand-in gh saw the sign-in check")
    check(not (folder / "calls.txt").exists(), "the script did not run")


@pytest.mark.skipif(BASH is None, reason="bash is not installed")
def test_a_missing_env_name_stops_the_script_before_the_bootstrap_runs_and_names_the_missing_name(tmp_path: Path) -> None:
    folder = stand_in(tmp_path, FULL.replace(f"S3_BUCKET={BUCKET}\n", ""))
    done = run_block(folder)
    check(done.returncode != 0 and "S3_BUCKET" in done.stderr, f"it stopped and named the name: {done.returncode} {done.stderr}")
    check(not (folder / "calls.txt").exists(), "the script did not run")


@pytest.mark.skipif(BASH is None, reason="bash is not installed")
def test_the_child_reads_no_system_or_global_git_setting_even_where_one_is_planted(tmp_path: Path) -> None:
    folder = stand_in(tmp_path, FULL)
    system = tmp_path / "planted_system_config"
    system.write_text("[probe]\n\tsystem = planted\n")
    (folder / "home/.gitconfig").write_text("[probe]\n\thome = planted\n")
    (folder / "xdg/git").mkdir(parents=True)
    (folder / "xdg/git/config").write_text("[probe]\n\txdg = planted\n")
    planted = {**child_environment(folder), "GIT_CONFIG_SYSTEM": str(system)}
    listing = ["-c", "git config --list --show-origin"]
    child = run_command(str(BASH), listing, cwd=folder, env=planted, timeout=60)
    check(child.returncode == 0 and "probe." not in child.stdout, f"the child read a planted setting: {child.stdout} {child.stderr}")
    control = {name: value for name, value in planted.items() if name not in ISOLATION}
    seen = run_command(str(BASH), listing, cwd=folder, env=control, timeout=60).stdout
    check(all(marker in seen for marker in ("probe.system=planted", "probe.home=planted", "probe.xdg=planted")), f"the planted settings are not real: {seen}")
