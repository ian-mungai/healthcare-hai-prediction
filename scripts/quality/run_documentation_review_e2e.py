"""Real-hook E2E for the documentation review: good evidence passes and every declared failure is rejected.

Run from the repository root: ``.venv/bin/python -m scripts.quality.run_documentation_review_e2e``. Each scenario
builds a synthetic Git repository with this repository's hook configuration and checker, keeps evidence local and runs the
``documentation-review`` hook through ``pre-commit``. The failure analysis was written before the checker; the CI log is
the run's artifact.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shlex
import shutil
import sys
import tempfile
from pathlib import Path
from typing import cast

from scripts.process import clear_git_environment, run_command

ROOT = Path(__file__).resolve().parents[2]
RECORD = ".documentation_review.json"
SCENARIOS = (
    "valid",
    "updated",
    "historical",
    "missing",
    "invalid_json",
    "invalid_encoding",
    "duplicate_key",
    "duplicate_document",
    "wrong_schema",
    "float_schema",
    "no_reviewer",
    "invalid_time",
    "empty_notes",
    "pending",
    "missing_document",
    "extra_document",
    "wrong_blob",
    "source_changed",
    "document_changed",
    "document_added",
    "document_removed",
    "untracked_document",
    "tracked_record",
    "unstaged_document_edit",
    "symlink_document",
    "extra_format",
    "unsafe_extra_path",
    "prepare_retry",
    "prepare_draft",
    "empty_document_inventory",
    "unmerged_index",
    "nothing_staged",
    "prepare_refresh",
    "folder_document_missing",
    "conventional_document_missing",
    "carry_forward_kept",
    "carry_forward_changed_document",
    "carry_forward_invalid_prior",
    "carry_forward_prefix_only_notes",
    "carry_forward_needs_refresh",
    "record_not_ignored",
)
GOOD = {
    "valid",
    "updated",
    "historical",
    "unstaged_document_edit",
    "extra_format",
    "prepare_retry",
    "nothing_staged",
    "empty_document_inventory",
    "carry_forward_kept",
}
PROVENANCE = "Carried forward from the review at 2026-09-26T00:00:00Z (blob unchanged): "


def git(repo: Path, *args: str) -> str:
    """Run one Git operation in the synthetic repository."""
    return run_command("git", args, cwd=repo, check=True).stdout


def fixture(repo: Path) -> None:
    """Copy the actual production hook configuration into a synthetic project, using existing pinned tools."""
    git(repo, "init", "-q", "-b", "main")
    git(repo, "config", "user.name", "documentation review test")
    git(repo, "config", "user.email", "test@example.invalid")
    for relative in ("scripts/quality/documentation_review.py", "scripts/process.py"):
        (repo / relative).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(ROOT / relative, repo / relative)
    (repo / ".venv").symlink_to(ROOT / ".venv")
    (repo / ".git" / "info" / "exclude").write_text(".venv\n__pycache__/\n.documentation_review.json\n")
    shutil.copy(ROOT / ".pre-commit-config.yaml", repo / ".pre-commit-config.yaml")
    (repo / "README.md").write_text("# Example\n\nThe command returns one.\n")
    (repo / "app.py").write_text("VALUE = 1\n")
    git(repo, "add", ".pre-commit-config.yaml", "scripts", "README.md", "app.py")


def evidence(repo: Path, extra: bool) -> dict[str, object]:
    """Create the schema-1 fixture independently of the implementation's functions."""
    rows = []
    documents = []
    for entry in git(repo, "ls-files", "--stage", "-z").split("\0"):
        if not entry:
            continue
        metadata, path = entry.split("\t", 1)
        mode, blob, stage = metadata.split()
        if stage != "0":
            raise ValueError("fixture has an unmerged entry")
        if path == RECORD:
            continue
        rows.append([path, mode, blob])
        if path == "README.md" or (extra and path == "manual.custom"):
            documents.append({"path": path, "blob": blob, "outcome": "current", "notes": "Compared the synthetic document with VALUE = 1."})
    payload = json.dumps(sorted(rows), ensure_ascii=True, separators=(",", ":")).encode()
    return {
        "schema_version": 1,
        "reviewer": "synthetic E2E reviewer",
        "reviewed_at_utc": "2026-09-26T00:00:00Z",
        "snapshot_sha256": hashlib.sha256(payload).hexdigest(),
        "extra_documents": ["manual.custom"] if extra else [],
        "documents": documents,
    }


def mutate(repo: Path, scenario: str, record: dict[str, object]) -> None:
    """Apply a declared failure scenario before staging the evidence."""
    documents = record["documents"]
    if scenario == "empty_document_inventory":
        return
    if not isinstance(documents, list) or not documents or not isinstance(documents[0], dict):
        raise ValueError("invalid fixture documents")
    first = documents[0]
    if scenario in {"updated", "historical", "pending"}:
        first["outcome"] = scenario
    if scenario == "duplicate_document":
        documents.append(dict(first))
    if scenario == "wrong_schema":
        record["schema_version"] = 2
    if scenario == "float_schema":
        record["schema_version"] = 1.0
    if scenario == "no_reviewer":
        record["reviewer"] = ""
    if scenario == "invalid_time":
        record["reviewed_at_utc"] = "yesterday"
    if scenario == "empty_notes":
        first["notes"] = " "
    if scenario == "missing_document":
        record["documents"] = []
    if scenario == "extra_document":
        documents.append({"path": "missing.md", "blob": "0" * 40, "outcome": "current", "notes": "Not present."})
    if scenario == "wrong_blob":
        first["blob"] = "0" * 40
    if scenario == "unsafe_extra_path":
        record["extra_documents"] = ["../outside.md"]
    if scenario == "source_changed":
        (repo / "app.py").write_text("VALUE = 2\n")
        git(repo, "add", "app.py")
    if scenario == "document_changed":
        (repo / "README.md").write_text("# Changed after review\n")
        git(repo, "add", "README.md")
    if scenario == "document_added":
        (repo / "guide.rst").write_text("New unreviewed guide\n")
        git(repo, "add", "guide.rst")
    if scenario == "document_removed":
        git(repo, "rm", "-q", "--cached", "README.md")
        (repo / "README.md").unlink()
    if scenario == "untracked_document":
        (repo / "guide.md").write_text("Untracked guide\n")


def run_case(scenario: str) -> tuple[bool, str]:
    """Run the actual hook twice where needed; distinguish a policy rejection from a broken hook."""
    with tempfile.TemporaryDirectory(prefix="documentation_review_e2e_") as folder:
        repo = Path(folder)
        fixture(repo)
        if scenario == "empty_document_inventory":
            git(repo, "rm", "-q", "--cached", "README.md")
            (repo / "README.md").unlink()
        if scenario == "folder_document_missing":
            (repo / "docs").mkdir()
            (repo / "docs" / "schema.json").write_text('{"description": "Documentation"}\n')
            git(repo, "add", "docs/schema.json")
        if scenario == "conventional_document_missing":
            (repo / "LICENSE").write_text("Synthetic license document\n")
            git(repo, "add", "LICENSE")
        if scenario == "extra_format":
            (repo / "manual.custom").write_text("Custom documentation\n")
            git(repo, "add", "manual.custom")
        if scenario == "symlink_document":
            (repo / "README.md").unlink()
            (repo / "README.md").symlink_to("app.py")
            git(repo, "add", "README.md")
        record = evidence(repo, scenario == "extra_format")
        mutate(repo, scenario, record)
        content = json.dumps(record, indent=2) + "\n"
        if scenario == "invalid_json":
            content = "not JSON\n"
        if scenario == "duplicate_key":
            content = content.replace('"schema_version": 1,', '"schema_version": 9, "schema_version": 1,')
        if scenario != "missing":
            (repo / RECORD).write_text(content)
            if scenario == "invalid_encoding":
                (repo / RECORD).write_bytes(b"\xff\xfeinvalid")

        if scenario == "tracked_record":
            git(repo, "add", "-f", RECORD)
        if scenario == "unstaged_document_edit":
            (repo / "README.md").write_text("Unstaged text is not the committed document.\n")
        if scenario == "prepare_retry":
            args = ["-m", "scripts.quality.documentation_review", "prepare", "--reviewer", "synthetic E2E reviewer", "--reviewed-at", "2026-09-26T00:00:00Z"]
            before = (repo / RECORD).read_bytes()
            first_run = run_command(str(repo / ".venv/bin/python"), args, cwd=repo)
            second_run = run_command(str(repo / ".venv/bin/python"), args, cwd=repo)
            if first_run.returncode != 0 or second_run.returncode != 0 or (repo / RECORD).read_bytes() != before:
                return False, first_run.stdout + first_run.stderr + second_run.stdout + second_run.stderr
        if scenario in {"prepare_draft", "prepare_refresh"}:
            args = ["-m", "scripts.quality.documentation_review", "prepare", "--reviewer", "synthetic E2E reviewer", "--reviewed-at", "2026-09-26T00:00:00Z"]
            if scenario == "prepare_draft":
                (repo / RECORD).unlink()
            else:
                (repo / "app.py").write_text("VALUE = 2\n")
                git(repo, "add", "app.py")
                stale_run = run_command(str(repo / ".venv/bin/python"), args, cwd=repo)
                if stale_run.returncode == 0 or (repo / RECORD).read_text() != content:
                    return False, "stale prepare without --refresh must reject and preserve the existing record"
                args.append("--refresh")
            first_run = run_command(str(repo / ".venv/bin/python"), args, cwd=repo)
            if first_run.returncode or not (repo / RECORD).exists():
                return False, first_run.stdout + first_run.stderr
            before = (repo / RECORD).read_bytes()
            second_run = run_command(str(repo / ".venv/bin/python"), args, cwd=repo)
            if second_run.returncode or (repo / RECORD).read_bytes() != before:
                return False, second_run.stdout + second_run.stderr
            draft = json.loads(before)
            if scenario == "prepare_refresh" and draft["snapshot_sha256"] == record["snapshot_sha256"]:
                return False, "refresh must bind the new staged source snapshot"
            if any(item["outcome"] != "pending" or item["notes"] for item in draft["documents"]):
                return False, "prepare must draft pending outcomes, never attest a completed review"

        if scenario.startswith("carry_forward") or scenario == "record_not_ignored":
            failure = carry_forward_case(repo, scenario, record)
            if failure is not None:
                return False, failure
            if scenario in {"carry_forward_needs_refresh", "record_not_ignored"}:
                return True, ""

        if scenario == "unmerged_index":
            base = git(repo, "write-tree").strip()
            (repo / "app.py").write_text("VALUE = 2\n")
            git(repo, "add", "app.py")
            ours = git(repo, "write-tree").strip()
            (repo / "app.py").write_text("VALUE = 3\n")
            git(repo, "add", "app.py")
            theirs = git(repo, "write-tree").strip()
            git(repo, "read-tree", "--reset", ours)
            git(repo, "read-tree", "-i", "-m", base, ours, theirs)
            result = run_command(str(repo / ".venv/bin/python"), ["-m", "scripts.quality.documentation_review", "check"], cwd=repo)
            output = result.stdout + result.stderr
            return result.returncode != 0 and "unresolved index conflict" in output and "Traceback" not in output, output
        if scenario == "nothing_staged":
            git(repo, "commit", "-q", "-m", "test: complete scratch snapshot")
        environment = {**os.environ, "PRE_COMMIT_HOME": str(ROOT / ".tools" / "pre_commit_cache")}
        result = run_command(str(ROOT / ".venv/bin/pre-commit"), ["run", "documentation-review", "--verbose"], cwd=repo, env=environment, timeout=180)
        output = result.stdout + result.stderr
        expected = scenario in GOOD
        valid_pass = result.returncode == 0 and "documentation review: PASS" in output
        valid_rejection = result.returncode != 0 and "documentation review: BLOCK" in output and "Traceback" not in output
        return (valid_pass if expected else valid_rejection), output


def carry_forward_case(repo: Path, scenario: str, record: dict[str, object]) -> str | None:
    """Draft with --refresh --carry-forward after a staged change; return a failure description or None [702] to [708]."""
    python = str(repo / ".venv/bin/python")
    module = ["-m", "scripts.quality.documentation_review"]
    if scenario == "record_not_ignored":
        (repo / ".git" / "info" / "exclude").write_text(".venv\n__pycache__/\n")
        result = run_command(python, [*module, "check-untracked"], cwd=repo)
        output = result.stdout + result.stderr
        return (
            None if result.returncode != 0 and "ignored" in output and "Traceback" not in output else "a record that is not ignored must be refused\n" + output
        )
    documents = cast(list[dict[str, str]], record["documents"])
    if scenario == "carry_forward_invalid_prior":
        record["reviewer"] = ""
    if scenario == "carry_forward_prefix_only_notes":
        documents[0]["notes"] = PROVENANCE
    prior = json.dumps(record, indent=2) + "\n"
    (repo / RECORD).write_text(prior)
    if scenario == "carry_forward_changed_document":
        (repo / "README.md").write_text("# Example\n\nThe command returns two.\n")
        git(repo, "add", "README.md")
    (repo / "app.py").write_text("VALUE = 2\n")
    git(repo, "add", "app.py")
    args = [*module, "prepare", "--reviewer", "synthetic E2E reviewer", "--reviewed-at", "2026-09-27T00:00:00Z", "--carry-forward"]
    if scenario == "carry_forward_needs_refresh":
        result = run_command(python, args, cwd=repo)
        output = result.stdout + result.stderr
        unchanged = (repo / RECORD).read_text() == prior
        return None if result.returncode != 0 and "--refresh" in output and unchanged else "--carry-forward without --refresh must be refused\n" + output
    result = run_command(python, [*args, "--refresh"], cwd=repo)
    if result.returncode:
        return result.stdout + result.stderr
    readme = next(item for item in json.loads((repo / RECORD).read_text())["documents"] if item["path"] == "README.md")
    if scenario == "carry_forward_kept":
        expected = PROVENANCE + "Compared the synthetic document with VALUE = 1."
        return None if readme["outcome"] == "current" and readme["notes"] == expected else f"unchanged README must carry with provenance once: {readme}"
    return None if readme["outcome"] == "pending" and not readme["notes"] else f"{scenario}: README must stay pending: {readme}"


def commit_hook_case() -> tuple[bool, str]:
    """Commit through a real hook in a disposable linked worktree; foreign fixtures must preserve parent state."""
    with tempfile.TemporaryDirectory(prefix="documentation_commit_e2e_") as folder:
        parent = Path(folder) / "parent"
        parent.mkdir()
        git(parent, "init", "-q", "-b", "main")
        git(parent, "config", "user.name", "parent fixture")
        git(parent, "config", "user.email", "parent@example.invalid")
        (parent / "seed").write_text("parent state must remain unchanged\n")
        git(parent, "add", "seed")
        git(parent, "commit", "-q", "-m", "test: seed parent")
        worktree = Path(folder) / "linked"
        git(parent, "worktree", "add", "-q", "-b", "hook_case", str(worktree))
        (worktree / "candidate").write_text("only intended linked-worktree change\n")
        git(worktree, "add", "candidate")
        parent_head = git(parent, "rev-parse", "HEAD").strip()
        parent_tree = git(parent, "write-tree").strip()
        candidate_tree = git(worktree, "write-tree").strip()
        parent_index = parent / ".git" / "index"
        before_parent_index = parent_index.read_bytes()
        config = parent / ".git" / "config"
        before_config = config.read_bytes()
        hook = parent / ".git" / "hooks" / "pre-commit"
        commands = [
            shlex.join([str(ROOT / ".venv/bin/python"), "-m", module, "--hook-probe"])
            for module in ("scripts.quality.run_checks_e2e", "scripts.quality.run_documentation_review_e2e")
        ]
        # The hook runs inside the linked worktree, so the modules are found through PYTHONPATH.
        hook.write_text("#!/bin/sh\nset -eu\n" + f"export PYTHONPATH={shlex.quote(str(ROOT))}\n" + "\n".join(commands) + "\n")
        hook.chmod(0o755)
        result = run_command("git", ["commit", "-q", "-m", "test: exercise real hook"], cwd=worktree, timeout=120)
        output = result.stdout + result.stderr
        if config.read_bytes() != before_config:
            return False, "Parent repository configuration changed during the hook.\n" + output
        if parent_index.read_bytes() != before_parent_index:
            return False, "Parent repository index bytes changed during the hook.\n" + output
        if result.returncode or "1 of 1 cases behaved as expected" not in output or "1/1 documentation-review scenarios passed" not in output:
            return False, output
        unchanged_parent = git(parent, "rev-parse", "HEAD").strip() == parent_head and git(parent, "write-tree").strip() == parent_tree
        intended_commit = git(worktree, "rev-parse", "HEAD^").strip() == parent_head
        intended_tree = git(worktree, "rev-parse", "HEAD^{tree}").strip() == candidate_tree and git(worktree, "write-tree").strip() == candidate_tree
        clean = not git(parent, "status", "--porcelain").strip() and not git(worktree, "status", "--porcelain").strip()
        return unchanged_parent and intended_commit and intended_tree and clean, output


def main() -> int:
    """Report every scenario and fail when any actual hook result differs from its declared behavior."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    selection = parser.add_mutually_exclusive_group()
    selection.add_argument("--hook-probe", action="store_true", help="run one valid sample inside the commit-hook E2E")
    selection.add_argument("--commit-hook-only", action="store_true", help="run only the linked-worktree commit-hook regression")
    args = parser.parse_args()
    clear_git_environment()
    scenarios: tuple[str, ...] = ("valid",) if args.hook_probe else SCENARIOS
    if args.commit_hook_only:
        scenarios = ()
    failures = 0
    for scenario in scenarios:
        ok, output = run_case(scenario)
        sys.stdout.write(f"{'PASS' if ok else 'FAIL'} {scenario} (expected {'pass' if scenario in GOOD else 'block'})\n")
        if not ok:
            failures += 1
            sys.stdout.write(output)
    count = len(scenarios)
    if not args.hook_probe:
        ok, output = commit_hook_case()
        count += 1
        sys.stdout.write(f"{'PASS' if ok else 'FAIL'} commit_hook_environment (expected pass)\n")
        if not ok:
            failures += 1
            sys.stdout.write(output)
    sys.stdout.write(f"{count - failures}/{count} documentation-review scenarios passed\n")
    return int(failures != 0)


if __name__ == "__main__":
    sys.exit(main())
