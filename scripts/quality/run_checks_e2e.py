"""E2E test of the pre-commit checks: each hook rejects a bad sample and passes a good one.

Run from the repository root after setup (``.venv`` and ``.tools`` present):
``.venv/bin/python -m scripts.quality.run_checks_e2e``. Each case builds a scratch Git repository with this repository's
hook configuration and scripts, links in ``.venv`` and ``.tools``, stages the sample and runs the hook through
``pre-commit`` itself, so a hook that is not wired in cannot pass. Samples that look like secrets or suppressions are
assembled at run time, so this file does not trip the checks it tests. The CI log is the run's artifact.
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

from scripts.process import clear_git_environment, run_command

ROOT = Path(__file__).resolve().parents[2]
PRE_COMMIT = ROOT / ".venv" / "bin" / "pre-commit"
COPIED = [".pre-commit-config.yaml", "pyproject.toml", ".env.example", "scripts/run_ci.sh"]
NOQA = "no" + "qa"
TYPE_IGNORE = "type" + ": ignore"
FAKE_PAT = "gh" + "p_" + "Zx7Qm2Lp9Rt4Wv8Ks3Nd6Hy1Bc5Fj0Ga2TeQ"
ENVIRON = "os." + "environ"
ADD_ARGUMENT = "add_" + "argument"
PROCESS_MODULE = "sub" + "process"
LAUNCHER_SOURCE = (ROOT / "scripts" / "process.py").read_text()
CI_SOURCE = (ROOT / "scripts" / "run_ci.sh").read_text()


@dataclass
class Case:
    """One hook run: files to stage (or delete) in a scratch repository and whether the hook must pass."""

    name: str
    hook: str
    expect_pass: bool
    files: dict[str, str | bytes] = field(default_factory=dict)
    committed: dict[str, str] = field(default_factory=dict)
    delete: list[str] = field(default_factory=list)
    message: str | None = None


RUFF_STANDARD = '[tool.ruff]\nline-length = 160\ntarget-version = "py312"\n\n[tool.ruff.lint]\nselect = ["E", "F", "I", "B", "UP", "S", "SIM", "T20"]\n'

CASES = [
    Case("clean file", "gitleaks", True, {"notes.md": "Nothing secret here.\n"}),
    Case("GitHub token", "gitleaks", False, {"config.py": f'GITHUB = "{FAKE_PAT}"\n'}),
    Case(".env.example", "credential-files", True, {".env.example": "AWS_REGION=your-aws-region\n"}),
    Case(".env", "credential-files", False, {".env": "AWS_REGION=us-west-2\n"}),
    Case("Terraform state", "credential-files", False, {"infra/terraform.tfstate": "{}\n"}),
    Case("saved Terraform plan", "credential-files", False, {"infra/reviewed.tfplan": "placeholder\n"}),
    Case("private key file", "credential-files", False, {"keys/deploy.pem": "placeholder\n"}),
    Case("CSV in the declared folder", "data-files", True, {"tests/fixtures/result.csv": "a,b\n1,2\n"}),
    Case("CSV outside the declared folder", "data-files", False, {"data/hospitals.csv": "a,b\n1,2\n"}),
    Case("file over 5 MB", "data-files", False, {"docs/big.bin": b"\0" * (5 * 1024 * 1024 + 1)}),
    Case("file at 5 MB", "data-files", True, {"docs/ok.bin": b"\0" * (5 * 1024 * 1024)}),
    Case("approved S603 in the launcher", "suppressions", True, {"scripts/process.py": LAUNCHER_SOURCE + f"x = 1  # {NOQA}: S603 - reason\n"}),
    Case("S603 outside the launcher", "suppressions", False, {"scripts/tool.py": f"x = 1  # {NOQA}: S603 - reason\n"}),
    Case("blanket noqa", "suppressions", False, {"scripts/tool.py": f"x = 1  # {NOQA}\n"}),
    Case("type ignore", "suppressions", False, {"scripts/tool.py": f"x: int = 'a'  # {TYPE_IGNORE}\n"}),
    Case("launcher import", "subprocess-imports", True, {"scripts/tool.py": "from scripts.process import run_command\n"}),
    Case("process module import", "subprocess-imports", False, {"scripts/tool.py": f"import {PROCESS_MODULE}\n"}),
    Case("standard Ruff settings", "lint-settings", True, {"pyproject.toml": RUFF_STANDARD}),
    Case("Ruff rule set removed", "lint-settings", False, {"pyproject.toml": RUFF_STANDARD.replace(', "T20"', "")}),
    Case("Ruff ignore added", "lint-settings", False, {"pyproject.toml": RUFF_STANDARD + 'ignore = ["E501"]\n'}),
    Case(
        "Ruff per-file ignore added",
        "lint-settings",
        False,
        {"pyproject.toml": RUFF_STANDARD + '\n[tool.ruff.lint.per-file-ignores]\n"tests/*.py" = ["S101"]\n'},
    ),
    Case("Ruff line length raised", "lint-settings", False, {"pyproject.toml": RUFF_STANDARD.replace("160", "200")}),
    Case(
        "MyPy flag removed", "lint-settings", False, {"pyproject.toml": RUFF_STANDARD, "scripts/run_ci.sh": CI_SOURCE.replace(" --disallow-untyped-defs", "")}
    ),
    Case("MyPy loosened in settings", "lint-settings", False, {"pyproject.toml": RUFF_STANDARD + "\n[tool.mypy]\nignore_errors = true\n"}),
    Case("documented variable", "env-example", True, {"scripts/tool.py": "import os\n\nNAME = " + ENVIRON + '.get("AWS_REGION", "")\n'}),
    Case("undocumented variable", "env-example", False, {"scripts/tool.py": "import os\n\nNAME = " + ENVIRON + '["NEW_SETTING"]\n'}),
    Case("script removed with its docs", "removed-names", True, committed={"scripts/old_tool.py": "X = 1\n"}, delete=["scripts/old_tool.py"]),
    Case(
        "script removed, docs still name it",
        "removed-names",
        False,
        committed={"scripts/old_tool.py": "X = 1\n", "README.md": "Run `scripts/old_tool.py`.\n"},
        delete=["scripts/old_tool.py"],
    ),
    Case(
        "flag removed, docs still name it",
        "removed-names",
        False,
        committed={
            "scripts/tool.py": "import argparse\n\nP = argparse.ArgumentParser()\nP." + ADD_ARGUMENT + '("--legacy-mode")\n',
            "README.md": "Use `--legacy-mode`.\n",
        },
        files={"scripts/tool.py": "import argparse\n\nP = argparse.ArgumentParser()\n"},
    ),
    Case("typed subject with scope", "commit-msg", True, message="feat(infra): tag the data bucket\n\nBody.\n"),
    Case("typed subject without scope", "commit-msg", True, message="fix: handle an empty file\n"),
    Case("breaking change marker", "commit-msg", True, message="feat(infra)!: drop a stack\n"),
    Case("untyped subject", "commit-msg", False, message="Update README\n"),
    Case("missing space after colon", "commit-msg", False, message="fix:handle empty input\n"),
    Case("unknown type", "commit-msg", False, message="update(infra): tag the data bucket\n"),
    Case("empty description", "commit-msg", False, message="docs(readme): \n"),
]


def git(repo: Path, *args: str) -> str:
    """Run Git in the scratch repository and return its output."""
    return run_command("git", args, cwd=repo, check=True).stdout


def write(repo: Path, files: dict[str, str | bytes] | dict[str, str]) -> None:
    """Write files into the scratch repository, creating folders as needed."""
    for relative, content in files.items():
        path = repo / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        if isinstance(content, bytes):
            path.write_bytes(content)
        else:
            path.write_text(content)


def scratch_repo(root: Path, case: Case) -> Path:
    """Create a repository holding this repository's hook setup and scripts plus the case's committed files."""
    repo = root / "repo"
    repo.mkdir()
    git(repo, "init", "-q", "-b", "main")
    git(repo, "config", "user.name", "check test")
    git(repo, "config", "user.email", "test@example.invalid")
    # Copy only files Git tracks or would track, so ignored local code never enters the scratch repository.
    listed = run_command("git", ["ls-files", "--cached", "--others", "--exclude-standard", "scripts"], cwd=ROOT, check=True).stdout.splitlines()
    for relative in [*COPIED, *listed]:
        if (ROOT / relative).is_file():
            (repo / relative).parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(ROOT / relative, repo / relative)
    for tool in (".venv", ".tools"):
        (repo / tool).symlink_to(ROOT / tool)
    (repo / ".git" / "info" / "exclude").write_text(".venv\n.tools\n__pycache__/\n")
    write(repo, case.committed)
    git(repo, "add", "-A")
    git(repo, "commit", "-q", "-m", "chore: scratch setup")
    return repo


def run_case(case: Case) -> tuple[bool, str]:
    """Run one case and return whether the hook behaved as expected, with its output."""
    with tempfile.TemporaryDirectory(prefix="hai_check_test_") as scratch:
        repo = scratch_repo(Path(scratch), case)
        write(repo, case.files)
        for relative in case.delete:
            git(repo, "rm", "-q", relative)
        git(repo, "add", "-A")
        args = ["run", case.hook]
        if case.message is not None:
            message = repo / ".git" / "COMMIT_EDITMSG"
            message.write_text(case.message)
            args += ["--hook-stage", "commit-msg", "--commit-msg-filename", str(message)]
        environment = {**os.environ, "PRE_COMMIT_HOME": str(ROOT / ".tools" / "pre_commit_cache")}
        result = run_command(str(PRE_COMMIT), args, cwd=repo, env=environment, timeout=180)
        passed = result.returncode == 0
        return passed == case.expect_pass, result.stdout + result.stderr


def main() -> int:
    """Run every case and report the ones whose hook did not behave as expected."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--hook-probe", action="store_true", help="run only the clean sample, for the commit-hook E2E")
    args = parser.parse_args()
    clear_git_environment()
    cases = CASES[:1] if args.hook_probe else CASES
    if not PRE_COMMIT.exists() or not (ROOT / ".tools" / "bin" / "gitleaks").exists():
        sys.stderr.write("setup missing: create .venv from requirements.txt and run scripts/quality/install_tools.py (README, Install)\n")
        return 1
    failures = 0
    for case in cases:
        ok, output = run_case(case)
        expected = "pass" if case.expect_pass else "block"
        sys.stdout.write(f"{'ok' if ok else 'WRONG':<6} {case.hook:<19} {case.name} (expected {expected})\n")
        if not ok:
            failures += 1
            sys.stdout.write("".join(f"       {line}\n" for line in output.strip().splitlines()[-12:]))
    sys.stdout.write(f"{len(cases) - failures} of {len(cases)} cases behaved as expected\n")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
