"""E2E test of the pre-commit checks: each hook rejects a bad sample and passes a good one.

Run from the repository root after setup (``.venv`` and ``.tools`` present):
``.venv/bin/python -m scripts.quality.run_checks_e2e``. Each case builds a scratch Git repository with this repository's
hook configuration and scripts, links in ``.venv`` and ``.tools``, stages the sample and runs the hook through
``pre-commit`` itself, so a hook that is not wired in cannot pass. Samples that look like secrets, suppressions or
personal data are assembled at run time, so this file does not trip the checks it tests. The CI log is the run's artifact.
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
COPIED = [".pre-commit-config.yaml", "pyproject.toml", ".env.example", ".privacy_allowlist", ".writing_allowlist", "scripts/run_ci.sh"]
NOQA = "no" + "qa"
TYPE_IGNORE = "type" + ": ignore"
FAKE_PAT = "gh" + "p_" + "Zx7Qm2Lp9Rt4Wv8Ks3Nd6Hy1Bc5Fj0Ga2TeQ"
ENVIRON = "os." + "environ"
ADD_ARGUMENT = "add_" + "argument"
CREDIT = "Co-Authored" + "-By"
MADE_WITH = "Generated" + " with"
SESSION = "Claude" + "-Session"
SCISSORS = "# ------------------------ >8 ------------------------"
PROCESS_MODULE = "sub" + "process"
LAUNCHER_SOURCE = (ROOT / "scripts" / "process.py").read_text()
CI_SOURCE = (ROOT / "scripts" / "run_ci.sh").read_text()
ALLOWLIST = ".privacy_allowlist"
ALLOWLIST_SOURCE = (ROOT / ALLOWLIST).read_text()
VENDOR_ADDRESS = "noreply" + "@" + "anthropic.com"  # The attribution check matches this vendor address.
HOME_PATH = "/Us" + "ers/jdoe/projects/app/run.log"
TEMP_PATH = "/var/" + "folders/zq/k3j9x0000gn/T/run_1"
PERSONAL_EMAIL = "jane.doe" + "@" + "gmail.com"
PHONE = "(206) 555" + "-0100"
ACCOUNT = "1234" + "56789012"
AWS_ARN = f"arn:aws:iam::{ACCOUNT}:role/deploy"
ENV_VALUE = "acme" + "-admin-profile"
PROJECT_NAME = "acme" + "-analytics"
# No privacy finding may print the value it found.
PRIVACY_VALUES = [HOME_PATH, TEMP_PATH, PERSONAL_EMAIL, PHONE, ACCOUNT, ENV_VALUE]
WRITING_ALLOWLIST = ".writing_allowlist"
ISO_DATE = "2026" + "-09-30"
EM_DASH = "\u2014"
CLEAN_WRITING = (
    f"---\nlast_updated: {ISO_DATE}\n---\n\n# Notes\n\n"
    "Keep secrets out of logs or commits and run a privacy scan. Lists read red, green and blue.\n"
    f"Dates read Sep 30 2026; code keeps `{ISO_DATE}`, `a`, `b` and [the log](https://example.invalid/{ISO_DATE}).\n"
    f"An em dash {EM_DASH} allowed outside articles.\n\n```text\nrun {ISO_DATE}, and done\n```\n"
)
CLAUSE_COMMA = "Secrets never go into logs or commits" + ", and a privacy scan runs on every commit.\n"
SERIAL_COMMA = "The check reads Markdown, articles" + ", and commit messages.\n"
NOR_COMMA = "It is not fast" + ", nor is it cheap.\n"


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
    Case(
        "clean privacy sample",
        "privacy-scan",
        True,
        {"notes.md": "Logs live under ~/projects and $HOME/app; see /Users/<name>/app. Contact test@example.invalid. 51,539,607,552 bytes.\n"},
    ),
    Case("home path", "privacy-scan", False, {"notes.md": f"Log at {HOME_PATH}\n"}),
    Case("temp path", "privacy-scan", False, {"notes.md": f"Scratch at {TEMP_PATH}\n"}),
    Case("personal email", "privacy-scan", False, {"notes.md": f"Ask {PERSONAL_EMAIL}\n"}),
    Case("phone number", "privacy-scan", False, {"notes.md": f"Call {PHONE}\n"}),
    Case("AWS account ARN", "privacy-scan", False, {"infra/policy.json": f'{{"Resource": "{AWS_ARN}"}}\n'}),
    Case("AWS account ID field", "privacy-scan", False, {"tests/test_config.py": f'CONFIG = {{"account_id": "{ACCOUNT}"}}\n'}),
    Case("value declared in .env", "privacy-scan", False, {".env": f"AWS_PROFILE={ENV_VALUE}\n", "scripts/deploy.sh": f"aws s3 ls --profile {ENV_VALUE}\n"}),
    Case(
        "profile named after the project",
        "privacy-scan",
        True,
        {".env": f"PROJECT_NAME={PROJECT_NAME}\nAWS_PROFILE={PROJECT_NAME.replace('-', '_')}\n", "scripts/deploy.sh": f"aws s3 ls --profile {PROJECT_NAME}\n"},
    ),
    Case(
        "profile equal to the project name",
        "privacy-scan",
        True,
        {".env": f"PROJECT_NAME={PROJECT_NAME}\nAWS_PROFILE={PROJECT_NAME}\n", "scripts/deploy.sh": f"aws s3 ls --profile {PROJECT_NAME}\n"},
    ),
    Case(
        "bucket named after the project",
        "privacy-scan",
        False,
        {".env": f"PROJECT_NAME={PROJECT_NAME}\nAWS_BUCKET={PROJECT_NAME}-raw\n", "scripts/deploy.sh": f"aws s3 ls s3://{PROJECT_NAME}-raw\n"},
    ),
    Case(
        "allowlisted email",
        "privacy-scan",
        True,
        {"notes.md": f"Ask {PERSONAL_EMAIL}\n", ALLOWLIST: ALLOWLIST_SOURCE + "email notes.md -- synthetic contact used in a demo\n"},
    ),
    Case(
        "allowlist is by type",
        "privacy-scan",
        False,
        {"notes.md": f"Call {PHONE}\n", ALLOWLIST: ALLOWLIST_SOURCE + "email notes.md -- synthetic contact used in a demo\n"},
    ),
    Case(
        "allowlist is by path",
        "privacy-scan",
        False,
        {"docs/notes.md": f"Ask {PERSONAL_EMAIL}\n", ALLOWLIST: ALLOWLIST_SOURCE + "email notes.md -- synthetic contact used in a demo\n"},
    ),
    Case("allowlist entry without reason", "privacy-scan", False, {"notes.md": "Nothing here.\n", ALLOWLIST: ALLOWLIST_SOURCE + "email notes.md\n"}),
    Case("allowlist entry with unknown type", "privacy-scan", False, {ALLOWLIST: ALLOWLIST_SOURCE + "address notes.md -- demo\n"}),
    Case("unreadable file listed", "privacy-scan", True, {"docs/diagram.png": b"\x89PNG\r\n\x1a\n\x00\x00"}),
    Case("clean writing through the hook", "writing-check", True, {"notes.md": CLEAN_WRITING}),
    Case("writing finding blocked by the hook", "writing-check", False, {"notes.md": SERIAL_COMMA}),
    Case("typed subject with scope", "commit-msg", True, message="feat(infra): tag the data bucket\n\nBody.\n"),
    Case("typed subject without scope", "commit-msg", True, message="fix: handle an empty file\n"),
    Case("breaking change marker", "commit-msg", True, message="feat(infra)!: drop a stack\n"),
    Case("untyped subject", "commit-msg", False, message="Update README\n"),
    Case("missing space after colon", "commit-msg", False, message="fix:handle empty input\n"),
    Case("unknown type", "commit-msg", False, message="update(infra): tag the data bucket\n"),
    Case("empty description", "commit-msg", False, message="docs(readme): \n"),
    Case("human co-author", "commit-msg", True, message=f"fix(api): handle retries\n\n{CREDIT}: Jane Doe <jane@example.invalid>\n"),
    Case("agent named without credit", "commit-msg", True, message="docs(loaders): add the Codex loader\n\nClaude Code and Codex read the same files.\n"),
    Case("files generated with a script", "commit-msg", True, message="docs(diagram): refresh the PNG\n\nGenerated with the render script.\n"),
    Case(
        "git comments and verbose diff",
        "commit-msg",
        True,
        message=f"fix: tidy\n# {CREDIT}: Claude <x@example.invalid>\n{SCISSORS}\n+{CREDIT}: Claude <x@example.invalid>\n",
    ),
    Case("agent credit trailer", "commit-msg", False, message=f"docs(readme): update setup\n\nBody.\n\n{CREDIT}: Claude Opus 5.5 <{VENDOR_ADDRESS}>\n"),
    Case("agent credit, lower case", "commit-msg", False, message=f"fix: tidy\n\n{CREDIT.lower()}:codex <codex@example.invalid>\n"),
    Case("vendor address only", "commit-msg", False, message=f"fix: tidy\n\n{CREDIT}: Assistant <{VENDOR_ADDRESS}>\n"),
    Case("generated-with line", "commit-msg", False, message=f"fix: tidy\n\n{MADE_WITH} [Claude Code](https://claude.com/claude-code)\n"),
    Case("agent session trailer", "commit-msg", False, message=f"fix: tidy\n\n{SESSION}: https://example.invalid/session\n"),
    Case("assisted-by credit", "commit-msg", False, message="fix: tidy\n\nAssisted-By: GitHub Copilot\n"),
    Case("emoji generated credit", "commit-msg", False, message="fix: tidy\n\n🤖 Generated by ChatGPT\n"),
    Case("scissors cannot hide actual credit", "commit-msg", False, message=f"fix: tidy\n\n{SCISSORS}\n{CREDIT}: Claude\n"),
    Case("agent substring in human name", "commit-msg", True, message=f"fix: tidy\n\n{CREDIT}: Claudette Example <human@example.invalid>\n"),
]
# Privacy cases must be blocked for their intended cause, shown by file, line and type.
BLOCK_REASONS = {
    "home path": "notes.md:1: home-directory path with a user name",
    "temp path": "notes.md:1: machine temporary path",
    "personal email": "notes.md:1: email address",
    "phone number": "notes.md:1: phone number",
    "AWS account ARN": "infra/policy.json:1: AWS account ID",
    "AWS account ID field": "tests/test_config.py:1: AWS account ID",
    "value declared in .env": "scripts/deploy.sh:1: value declared in .env",
    "bucket named after the project": "scripts/deploy.sh:1: value declared in .env",
    "allowlist is by type": "notes.md:1: phone number",
    "allowlist is by path": "docs/notes.md:1: email address",
    "allowlist entry without reason": ".privacy_allowlist:{line}: allowlist entry without a reason",
    "allowlist entry with unknown type": ".privacy_allowlist:{line}: allowlist entry with an unknown type",
    "writing finding blocked by the hook": "notes.md:1: comma before a final 'and', 'or' or 'nor'",
}
# Good cases that must also print a marker, so a skipped check cannot pass silently.
PASS_MARKERS = {
    "unreadable file listed": "docs/diagram.png: unreviewed: cannot be read as text",
}


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
        args = ["run", case.hook, *(["--verbose"] if case.name in PASS_MARKERS else [])]  # pre-commit hides passing output
        if case.message is not None:
            message = repo / ".git" / "COMMIT_EDITMSG"
            message.write_text(case.message)
            args += ["--hook-stage", "commit-msg", "--commit-msg-filename", str(message)]
        environment = {**os.environ, "PRE_COMMIT_HOME": str(ROOT / ".tools" / "pre_commit_cache"), "GIT_EDITOR": ":"}
        result = run_command(str(PRE_COMMIT), args, cwd=repo, env=environment, timeout=180)
        passed = result.returncode == 0
        output = result.stdout + result.stderr
        correct = passed == case.expect_pass and "Traceback (most recent call last)" not in output
        if not case.expect_pass and case.hook == "commit-msg":
            correct = correct and any(marker in output for marker in ("not a Conventional Commit:", "AI attribution in commit message"))
        if case.hook == "privacy-scan":
            added = len(ALLOWLIST_SOURCE.splitlines()) + 1
            reason = BLOCK_REASONS.get(case.name, "").format(line=added)
            marker = PASS_MARKERS.get(case.name, "")
            correct = correct and reason in output and marker in output and not any(value in output for value in PRIVACY_VALUES)
        if case.hook == "writing-check":
            correct = correct and BLOCK_REASONS.get(case.name, "") in output
        return correct, output


def run_range_case() -> tuple[bool, str]:
    """Exercise CI's entry point on full stored messages, including credit after a scissors line."""
    with tempfile.TemporaryDirectory(prefix="hai_commit_range_") as scratch:
        repo = scratch_repo(Path(scratch), Case("range setup", "commit-msg", True))
        base = git(repo, "rev-parse", "HEAD").strip()
        git(repo, "commit", "--allow-empty", "-qm", f"fix: human credit\n\n{CREDIT}: Jane Doe <jane@example.invalid>")
        good = run_command(str(ROOT / ".venv/bin/python"), ["-m", "scripts.quality.repo_checks", "commit-range", base, "HEAD"], cwd=repo)
        git(repo, "commit", "--allow-empty", "-qm", f"fix: bad credit\n\n{SCISSORS}\n{CREDIT}: Claude <{VENDOR_ADDRESS}>")
        bad = run_command(str(ROOT / ".venv/bin/python"), ["-m", "scripts.quality.repo_checks", "commit-range", base, "HEAD"], cwd=repo)
        invalid = run_command(str(ROOT / ".venv/bin/python"), ["-m", "scripts.quality.repo_checks", "commit-range", "missing-ref", "HEAD"], cwd=repo)
        ok = good.returncode == 0 and bad.returncode == 1 and "AI attribution in commit message" in bad.stderr and invalid.returncode != 0
        return ok, f"clean range exit={good.returncode}; attributed range exit={bad.returncode}; missing revision exit={invalid.returncode}\n" + bad.stderr


def run_verbose_case() -> tuple[bool, str]:
    """Use an installed commit-msg hook to distinguish verbose patch context from actual message text."""
    with tempfile.TemporaryDirectory(prefix="hai_verbose_commit_") as scratch:
        sample = f"before\n{CREDIT}: Claude <example@example.invalid>\nafter\n"
        repo = scratch_repo(Path(scratch), Case("verbose setup", "commit-msg", True, committed={"sample.txt": sample}))
        write(repo, {"sample.txt": sample.replace("after", "changed")})
        git(repo, "add", "--", "sample.txt")
        run_command(str(PRE_COMMIT), ["install", "--hook-type", "commit-msg"], cwd=repo, check=True)
        editor = repo / ".git" / "sample_editor.sh"
        editor.write_text('#!/bin/sh\n{ printf "fix: verbose edit\\n\\n"; cat "$1"; } > "$1.tmp"\nmv "$1.tmp" "$1"\n')
        editor.chmod(0o700)
        verbose = run_command("env", [f"GIT_EDITOR={editor}", "git", "commit", "-v"], cwd=repo)
        stored = git(repo, "log", "-1", "--format=%B")
        direct = run_command("git", ["commit", "--allow-empty", "-qm", f"fix: bad credit\n\n{SCISSORS}\n{CREDIT}: Claude"], cwd=repo)
        ok = verbose.returncode == 0 and CREDIT not in stored and direct.returncode == 1 and "AI attribution in commit message" in direct.stdout + direct.stderr
        return ok, f"verbose commit exit={verbose.returncode}; direct attributed commit exit={direct.returncode}\n" + verbose.stdout + verbose.stderr


def run_privacy_scope_case() -> tuple[bool, str]:
    """Ignored files are skipped by default and scanned with --all; --warn reports findings but exits 0; values never print."""
    with tempfile.TemporaryDirectory(prefix="hai_privacy_scope_") as scratch:
        repo = scratch_repo(Path(scratch), Case("privacy setup", "privacy-scan", True))
        (repo / ".git" / "info" / "exclude").write_text(".venv\n.tools\n__pycache__/\nlocal_notes.md\n")
        write(repo, {"local_notes.md": f"Log at {HOME_PATH}\n"})
        command = ["-m", "scripts.quality.repo_checks", "privacy-scan"]
        default = run_command(str(ROOT / ".venv/bin/python"), command, cwd=repo)
        full = run_command(str(ROOT / ".venv/bin/python"), [*command, "--all"], cwd=repo)
        warned = run_command(str(ROOT / ".venv/bin/python"), [*command, "--all", "--warn"], cwd=repo)
        marker = "local_notes.md:1: home-directory path with a user name"
        leaked = HOME_PATH in default.stdout + default.stderr + full.stdout + full.stderr + warned.stdout + warned.stderr
        ok = (
            default.returncode == 0
            and full.returncode == 1
            and marker in full.stderr
            and warned.returncode == 0
            and f"WARN {marker}" in warned.stderr
            and not leaked
        )
        return ok, f"default exit={default.returncode}; --all exit={full.returncode}; --warn exit={warned.returncode}; value leaked={leaked}\n" + full.stderr


# Each sample runs the check directly, without --warn: (name, files, articles, expected exit, marker in the output).
WRITING_SAMPLES = [
    ("clean writing sample", {"notes.md": CLEAN_WRITING}, [], 0, ""),
    ("comma before a final clause", {"notes.md": CLAUSE_COMMA}, [], 1, "notes.md:1: comma before a final 'and', 'or' or 'nor'"),
    ("Oxford comma in a list", {"notes.md": SERIAL_COMMA}, [], 1, "notes.md:1: comma before a final 'and', 'or' or 'nor'"),
    ("comma before nor", {"notes.md": NOR_COMMA}, [], 1, "notes.md:1: comma before a final 'and', 'or' or 'nor'"),
    ("ISO date in prose", {"notes.md": f"Released on {ISO_DATE} after review.\n"}, [], 1, "notes.md:1: ISO date in prose"),
    ("declared data folder skipped", {"tests/fixtures/report.md": SERIAL_COMMA}, [], 0, ""),
    (
        "allowlisted ISO date",
        {"CHANGELOG.md": f"## v1.0.0 ({ISO_DATE})\n", WRITING_ALLOWLIST: "iso-date CHANGELOG.md -- release headings are machine-readable\n"},
        [],
        0,
        "",
    ),
    ("writing allowlist entry without reason", {WRITING_ALLOWLIST: "iso-date notes.md\n"}, [], 1, ".writing_allowlist:1: allowlist entry without a reason"),
    ("em dash outside articles", {"notes.md": f"Rules help {EM_DASH} when they are checked.\n"}, [], 0, ""),
    (
        "em dash in a declared article",
        {"articles/agents.md": f"Rules help {EM_DASH} when they are checked.\n"},
        ["articles/*.md"],
        1,
        "articles/agents.md:1: em dash in article text",
    ),
]


def run_writing_case() -> tuple[bool, str]:
    """Each writing sample exits as expected for its intended cause; --warn reports the same finding but exits 0."""
    results = []
    for name, files, articles, expected, marker in WRITING_SAMPLES:
        with tempfile.TemporaryDirectory(prefix="hai_writing_") as scratch:
            repo = scratch_repo(Path(scratch), Case("writing setup", "writing-check", True))
            write(repo, files)
            command = ["-m", "scripts.quality.repo_checks", "writing-check", *(f"--articles={glob}" for glob in articles)]
            result = run_command(str(ROOT / ".venv/bin/python"), command, cwd=repo)
            warned = run_command(str(ROOT / ".venv/bin/python"), [*command, "--warn"], cwd=repo)
            ok = result.returncode == expected and marker in result.stderr and warned.returncode == 0 and (not marker or f"WARN {marker}" in warned.stderr)
            results.append((ok, f"{'ok' if ok else 'WRONG'} {name}: exit={result.returncode} (expected {expected}); --warn exit={warned.returncode}"))
            if not ok:
                results.append((False, result.stderr + warned.stderr))
    return all(ok for ok, _ in results), "\n".join(line for _, line in results) + "\n"


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
    extra = 0
    if not args.hook_probe:
        for label, runner in (
            ("commit-range", run_range_case),
            ("verbose commit", run_verbose_case),
            ("privacy scope", run_privacy_scope_case),
            ("writing samples", run_writing_case),
        ):
            extra += 1
            ok, output = runner()
            sys.stdout.write(f"{'ok' if ok else 'WRONG':<6} {label}\n")
            if not ok:
                failures += 1
                sys.stdout.write(output)
    sys.stdout.write(f"{len(cases) + extra - failures} of {len(cases) + extra} cases behaved as expected\n")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
