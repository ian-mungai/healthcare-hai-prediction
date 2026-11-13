"""The owner's block that runs the code-staging bootstrap: its shape, its inputs from .env, its masked output and its stop, run with a stand-in tool."""

from __future__ import annotations

import json
import re
import shutil
from pathlib import Path

import pytest

from scripts.process import run_command
from tests.support import check

ROOT = Path(__file__).resolve().parents[1]
TITLE = "Code staging setup on GitHub"
REGION, PROFILE, BUCKET, PROJECT = "us-west-2", "stand-in-profile", "stand-in-bucket-name", "stand_in_project"
ZSH = shutil.which("zsh")


def block() -> str:
    text = json.loads((ROOT / "config/aws/owner_command_lists.json").read_text())[TITLE]
    found = re.search(r"```sh\n(.*?)```", text, re.S)
    if found is None:
        pytest.fail("the list holds no fenced block")
    return found.group(1)


def test_the_block_is_one_zsh_heredoc_that_stops_on_error_names_no_placeholder_and_reads_only_names_the_example_env_has() -> None:
    body = block()
    check(body.startswith("zsh <<'OWNER_BLOCK'\n") and body.rstrip().endswith("OWNER_BLOCK"), "one quoted heredoc for zsh")
    check("set -euo pipefail" in body and re.search(r"trap '.*' ERR", body) is not None, "it stops on error and traps ERR")
    check(re.search(r"<[A-Za-z_]+>", re.sub(r"<(bucket|profile|project|region)>", "", body)) is None, "no placeholder is left for the owner to fill")
    names = set(re.findall(r"\bvalue ([A-Z0-9_]+)\b", body))
    example = {line.split("=")[0] for line in (ROOT / ".env.example").read_text().splitlines() if "=" in line and not line.startswith("#")}
    check(names == {"AWS_REGION", "AWS_PROFILE", "S3_BUCKET", "PROJECT_NAME"} and names <= example, f"names read from .env: {names}")
    check("github_code_staging_bootstrap" in body, "it runs the bootstrap script")


def test_the_block_passes_the_script_no_option_because_it_takes_everything_from_env_and_terraform() -> None:
    line = next(item for item in block().splitlines() if "github_code_staging_bootstrap" in item)
    check("--" not in line.split("github_code_staging_bootstrap", 1)[1].replace("2>&1", ""), f"the block passes an option the script does not take: {line}")


def stand_in(tmp_path: Path, env_text: str) -> Path:
    run_command("git", ["init", "-q", str(tmp_path)], check=True)
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
    return tmp_path


FULL = f'PROJECT_NAME={PROJECT}\nAWS_REGION="{REGION}"\nAWS_PROFILE={PROFILE}\nS3_BUCKET={BUCKET}\nOTHER=1\n'


@pytest.mark.skipif(ZSH is None, reason="zsh is not installed")
def test_the_block_gives_the_script_the_profile_of_env_and_masks_every_env_value_in_the_output(tmp_path: Path) -> None:
    folder = stand_in(tmp_path, FULL)
    (folder / "block.zsh").write_text(block())
    done = run_command("zsh", ["block.zsh"], cwd=folder, timeout=60)
    shown = done.stdout + done.stderr
    calls = (folder / "calls.txt").read_text().splitlines()
    check(done.returncode == 0, f"the block ran: {shown}")
    check(any("github_code_staging_bootstrap" in call for call in calls), f"the script was not run: {calls}")
    check(all(value not in shown for value in (PROFILE, BUCKET, PROJECT, REGION)), f"the output shows a value it must mask: {shown}")


@pytest.mark.skipif(ZSH is None, reason="zsh is not installed")
def test_a_missing_env_name_stops_the_block_before_the_script_runs_and_names_the_missing_name(tmp_path: Path) -> None:
    folder = stand_in(tmp_path, FULL.replace(f"S3_BUCKET={BUCKET}\n", ""))
    (folder / "block.zsh").write_text(block())
    done = run_command("zsh", ["block.zsh"], cwd=folder, timeout=60)
    check(done.returncode != 0 and "S3_BUCKET" in done.stderr, f"it stopped and named the name: {done.returncode} {done.stderr}")
    check(not (folder / "calls.txt").exists(), "the script did not run")
