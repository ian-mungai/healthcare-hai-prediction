"""E2E for the check-group selection of scripts/run_ci.sh (scripts/quality/ci_groups.py and config/quality/ci_groups.json).

Run from the repository root: ``.venv/bin/python -m scripts.quality.run_ci_groups_e2e``. It adds a scratch Git worktree at HEAD,
copies the working check inputs into it, then checks which groups each scope selects: committed ranges
of known content, unusable ranges, staged changes (a deletion and a rename included), explicit paths, the full scope and
invalid input. Real runs of the CI entry check that the unconditional gates run in every part (also with an empty selection)
and before the launcher E2E, that a failing gate fails every part, that the full Markdown lint runs as a gate in every scope
and that the entry plan alone (``CI_LIST_ONLY=1``) runs nothing. One case runs this runner under a real pre-commit hook that
inherits an alternate ``GIT_INDEX_FILE`` and checks that the caller's index, staged diff and working files survive. Failure
modes CS1 to CS31 (plans, local). The report goes to data/e2e/ci_groups/<UTC>_<pid>/report.json with the revision, the SHA-256
of the files under test and the tool versions; the run fails when a file under test changes while it runs.

``--selection-only`` runs the selector and acquisition-hook selection cases as an iteration run and not commit evidence.
It skips every case that runs scripts/run_ci.sh or this runner. Acquisition selection uses a scratch replacement of the
heavy gate body and the real hook configuration.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.process import clear_git_environment, find_program, run_command

ROOT = Path(__file__).resolve().parents[2]
PYTHON = ROOT / ".venv" / "bin" / "python"
RUNNER = "scripts/quality/run_ci_groups_e2e.py"
COPIED = (
    ".github/workflows/ci.yml",
    ".markdownlint-cli2.jsonc",
    ".pre-commit-config.yaml",
    "README.md",
    "config/quality/ci_groups.json",
    "config/quality/docker_calls_reviewed.json",
    "config/quality/mypy_runtimes.json",
    "dbt/models/silver/intermediate/_intermediate__models.yml",
    "docs/quality_checks.md",
    "scripts/acquisition/run_checks.sh",
    "scripts/lakehouse/archive_saved_models.py",
    "scripts/lakehouse/run_bronze_e2e.py",
    "scripts/lakehouse/run_launcher_e2e.py",
    "scripts/lakehouse/run_pins_e2e.py",
    "scripts/lakehouse/run_provenance_e2e.py",
    "scripts/orchestration/broker.py",
    "scripts/orchestration/offline_checks.py",
    "scripts/quality/ci_groups.py",
    "scripts/quality/documentation_review.py",
    "scripts/quality/repo_checks.py",
    "scripts/quality/run_checks_e2e.py",
    RUNNER,
    "scripts/quality/run_documentation_review_e2e.py",
    "scripts/quality/typecheck.py",
    "scripts/run_ci.sh",
)
LAUNCHER_E2E = "scripts/lakehouse/run_launcher_e2e.py"
DOCS_ONLY = "9c22c1e"
INFRA_ONLY = "063cbba"
DBT_AND_README = "114c3e0"
REQUIREMENTS = "7a967b2"
ZERO = "0" * 40
MISSING = "0123456789abcdef0123456789abcdef01234567"
MARKDOWN_PROBES = ("ci_groups_probe.MD", "ci_groups_probe_two.Markdown")
CORE = ["python_lint", "lakehouse_checks", "orchestration_checks", "repository_code_checks", "configuration_checks", "sql_style", "terraform_checks"]
PART_GROUPS = {"core": CORE, "launcher": ["launcher"], "all": [*CORE, "launcher", "acquisition"]}
PASS_LINES = {"core": "All local CI checks passed", "all": "All local CI checks passed", "launcher": "The launcher part passed."}
MARKDOWNLINT = ROOT / ".tools" / "markdownlint-cli2" / "node_modules" / ".bin" / "markdownlint-cli2"


def git(work: Path, *args: str, env: dict[str, str] | None = None) -> str:
    result = run_command("git", ["-C", str(work), *args], env={**os.environ, "GIT_LFS_SKIP_SMUDGE": "1", **(env or {})}, timeout=300)
    if result.returncode != 0:
        raise RuntimeError(f"git {' '.join(args)}: {result.stderr.strip()}")
    return result.stdout


def sha256_of(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest() if path.exists() else ""


def select(work: Path, *args: str) -> tuple[int, dict[str, Any] | None, str]:
    result = run_command(str(PYTHON), ["-m", "scripts.quality.ci_groups", "plan", *args], cwd=work, timeout=120)
    try:
        plan = json.loads(result.stdout) if result.returncode == 0 else None
    except json.JSONDecodeError:
        plan = None
    return result.returncode, plan, result.stderr.strip()


def selection_case(work: Path, name: str, args: list[str], want_run: list[str], want_reason: str = "") -> dict[str, Any]:
    code, plan, error = select(work, *args)
    run = plan["run"] if plan else None
    reason = plan["reason"] if plan else ""
    skips_named = plan is not None and set(plan["skip"]) == set(plan["groups"]) - set(run or [])
    passed = code == 0 and run == want_run and want_reason in reason and skips_named
    return {"case": name, "passed": passed, "exit": code, "run": run, "want_run": want_run, "reason": reason, "error": error[-400:]}


def install_copies(work: Path) -> None:
    for path in COPIED:
        if (ROOT / path).exists():
            (work / path).parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(ROOT / path, work / path)


def restore(work: Path) -> None:
    git(work, "reset", "--quiet", "--hard", "HEAD")
    install_copies(work)


def staged_case(work: Path, name: str, setup: list[list[str]], want_run: list[str], unstaged: str = "") -> dict[str, Any]:
    try:
        if unstaged:
            path = work / unstaged
            path.write_text(path.read_text(encoding="utf-8") + "\n", encoding="utf-8")
        for command in setup:
            git(work, *command)
        return selection_case(work, name, ["--scope", "staged", "--part", "core"], want_run)
    finally:
        restore(work)


def stage_readme(work: Path) -> None:
    readme = work / "README.md"
    readme.write_text(readme.read_text(encoding="utf-8") + "\nThis paragraph comes from a docs-only test change.\n", encoding="utf-8")
    git(work, "add", "README.md")


def stage_launcher_path(work: Path) -> Path:
    copy = work / "orchestration" / "staged_copy.py"
    shutil.copy2(work / "orchestration" / "launcher.py", copy)
    git(work, "add", "orchestration/staged_copy.py")
    return copy


def run_entry(work: Path, out: Path, label: str, extra: dict[str, str]) -> tuple[int, str, str]:
    summary = out / f"{label}_summary.md"
    env = {**os.environ, "GITHUB_STEP_SUMMARY": str(summary), "GITHUB_ACTIONS": "true", "PYTHON_BIN": str(PYTHON), **extra}
    result = run_command("bash", ["scripts/run_ci.sh"], cwd=work, env=env, timeout=1800)
    log = result.stdout + result.stderr
    (out / f"{label}.log").write_text(log, encoding="utf-8")
    return result.returncode, log, summary.read_text(encoding="utf-8") if summary.exists() else ""


def docs_only_case(work: Path, out: Path, part: str) -> dict[str, Any]:
    name = "docs_only_runs_gates" if part == "core" else f"docs_only_runs_gates_{part}_part"
    stage_readme(work)
    try:
        code, log, text = run_entry(work, out, name, {"CI_SCOPE": "staged", "CI_PART": part})
    finally:
        restore(work)
    checks = {
        "exit_zero": code == 0,
        "reached_the_end": PASS_LINES[part] in log,
        "every_group_skipped_with_reason": all(f"skipped {group}:" in log for group in PART_GROUPS[part]),
        "gates_ran": re.search(r"^gates: \d+ s$", log, re.MULTILINE) is not None,
        "markdown_lint_ran": "markdownlint-cli2 v" in log,
        "no_group_ran": "\n== " not in log,
        "summary_has_durations": "| Step | Seconds |" in text and re.search(r"^\| gates \| \d+ \|$", text, re.MULTILINE) is not None,
    }
    return {"case": name, "passed": all(checks.values()), "checks": checks, "exit": code}


def break_gate(work: Path) -> Path:
    moved = next(iter(json.loads((ROOT / "config" / "data_paths.json").read_text(encoding="utf-8"))["moved"]))
    folder = work / moved
    folder.mkdir(parents=True)
    return folder


def gate_failure_case(work: Path, out: Path, part: str) -> dict[str, Any]:
    name = f"gate_failure_fails_{part}_part"
    stage_readme(work)
    folder = break_gate(work)
    try:
        code, log, _ = run_entry(work, out, name, {"CI_SCOPE": "staged", "CI_PART": part})
    finally:
        shutil.rmtree(folder, ignore_errors=True)
        restore(work)
    checks = {
        "exit_nonzero": code != 0,
        "intended_gate_message": f"old dataset folders exist again: {folder.relative_to(work)}" in log,
        "no_pass_line": all(line not in log for line in set(PASS_LINES.values())),
        "no_group_ran": "\n== " not in log,
    }
    return {"case": name, "passed": all(checks.values()), "checks": checks, "exit": code}


def install_launcher_stub(work: Path) -> Path:
    stub = work / LAUNCHER_E2E
    stub.write_text(
        'import sys\nfrom pathlib import Path\n\nmarker = Path(__file__).resolve().parents[2] / "data" / "launcher_stub.marker"\n'
        'marker.parent.mkdir(exist_ok=True)\nmarker.write_text(" ".join(sys.argv[1:]))\n',
        encoding="utf-8",
    )
    marker = work / "data" / "launcher_stub.marker"
    marker.unlink(missing_ok=True)
    return marker


def gates_before_launcher_case(work: Path, out: Path, part: str, gate_fails: bool) -> dict[str, Any]:
    name = f"gate_failure_stops_launcher_{part}_part" if gate_fails else "gates_run_before_launcher"
    marker = install_launcher_stub(work)
    copy = stage_launcher_path(work)
    folder = break_gate(work) if gate_fails else None
    try:
        code, log, text = run_entry(work, out, name, {"CI_SCOPE": "staged", "CI_PART": part, "LAUNCHER_SHARD": "1/2"})
    finally:
        if folder:
            shutil.rmtree(folder, ignore_errors=True)
        restore(work)
        copy.unlink(missing_ok=True)
    launcher_selected = "run launcher" in log
    if gate_fails:
        checks = {
            "launcher_selected": launcher_selected,
            "exit_nonzero": code != 0,
            "intended_gate_message": "old dataset folders exist again" in log,
            "launcher_never_started": not marker.exists(),
            "no_pass_line": all(line not in log for line in set(PASS_LINES.values())),
        }
    else:
        rows = re.findall(r"^\| (gates|launcher[^|]*) \| \d+ \|$", text, re.MULTILINE)
        checks = {
            "launcher_selected": launcher_selected,
            "exit_zero": code == 0,
            "reached_the_end": PASS_LINES[part] in log,
            "launcher_ran_with_its_shard": marker.exists() and marker.read_text(encoding="utf-8") == "--shard 1/2",
            "gates_before_launcher_in_summary": rows == ["gates", "launcher shard 1/2"],
        }
    return {"case": name, "passed": all(checks.values()), "checks": checks, "exit": code}


def list_only_case(work: Path, out: Path, part: str) -> dict[str, Any]:
    marker = install_launcher_stub(work)
    extra = {"CI_LIST_ONLY": "1", "CI_SCOPE": "range", "CI_PART": part, "RANGE_START": f"{DBT_AND_README}^", "RANGE_END": DBT_AND_README}
    try:
        code, log, _ = run_entry(work, out, f"list_only_{part}", extra)
    finally:
        restore(work)
    wanted = "run lakehouse_checks" if part == "core" else "run launcher"
    checks = {
        "exit_zero": code == 0,
        "plan_printed": wanted in log,
        "nothing_ran": not marker.exists() and "gates:" not in log and "\n== " not in log and all(line not in log for line in set(PASS_LINES.values())),
    }
    return {"case": f"list_only_prints_plan_{part}", "passed": all(checks.values()), "checks": checks, "exit": code}


def write_markdown_probes(work: Path) -> list[Path]:
    probes = [work / "docs" / name for name in MARKDOWN_PROBES]
    for probe in probes:
        probe.write_text("# Probe\n\nThis line of ordinary words runs past forty characters in total.\n", encoding="utf-8")
    return probes


def markdown_case(work: Path, out: Path, name: str, replacement: str, want_failure: bool, form: str = "root") -> dict[str, Any]:
    extra: list[Path] = []
    if form == "nested":
        (work / "docs" / ".markdownlint.json").write_text(f'{{"default": true, {replacement.rstrip(",")}}}\n', encoding="utf-8")
        git(work, "add", "docs/.markdownlint.json")
    elif form == "dependency":
        config = work / "docs" / ".markdownlint.json"
        config.write_text('{"ext\\u0065nds": "markdownlint_rules.json"}\n', encoding="utf-8")
        extra.append(config)
        (work / "docs" / "markdownlint_rules.json").write_text(f'{{"default": true, {replacement.rstrip(",")}}}\n', encoding="utf-8")
        git(work, "add", "docs/markdownlint_rules.json")
    else:
        config = work / ".markdownlint-cli2.jsonc"
        text = config.read_text(encoding="utf-8")
        config.write_text(text.replace('"MD013": false,', replacement, 1), encoding="utf-8")
        git(work, "add", ".markdownlint-cli2.jsonc")
    probes = write_markdown_probes(work)
    try:
        code, log, _ = run_entry(work, out, name, {"CI_SCOPE": "staged", "CI_PART": "core"})
    finally:
        restore(work)
        for path in [*probes, *extra]:
            path.unlink(missing_ok=True)
    if want_failure:
        unchanged = "README.md" if form == "root" else "docs/"
        checks = {
            "exit_nonzero": code != 0,
            "failed_in_the_gates": re.search(r"^gates: \d+ s$", log, re.MULTILINE) is None and "\n== " not in log,
            "names_the_rule_and_the_unchanged_document": "MD013" in log and unchanged in log,
            "lints_upper_case_extensions": all(f"docs/{probe}" in log for probe in MARKDOWN_PROBES),
        }
    else:
        checks = {"exit_zero": code == 0, "lint_ran": "markdownlint-cli2 v" in log, "no_violation": "Summary: 0 issues" in log and "MD0" not in log}
    return {"case": name, "passed": all(checks.values()), "checks": checks, "exit": code}


def caller_state(work: Path, index: Path) -> dict[str, str]:
    env = {"GIT_INDEX_FILE": str(index)}
    state = {
        "index_entries": git(work, "ls-files", "--stage", env=env),
        "staged_diff": git(work, "diff", "--cached", env=env),
        "unstaged_diff": git(work, "diff", env=env),
    }
    state |= {f"file {name}": sha256_of(work / name) for name in ("README.md", ".gitattributes")}
    state["index_sha256"] = sha256_of(index)  # last: the diffs above may refresh the index's cached file data
    return state


def inherited_context_case(work: Path, out: Path, scratch: Path, name: str, hook_entry: str, all_files: bool) -> dict[str, Any]:
    index = scratch / f"{name}.index"
    env = {"GIT_INDEX_FILE": str(index)}
    git(work, "read-tree", "HEAD", env=env)
    readme = work / "README.md"
    readme.write_text(readme.read_text(encoding="utf-8") + "\nstaged line\n", encoding="utf-8")
    git(work, "add", "README.md", env=env)
    readme.write_text(readme.read_text(encoding="utf-8") + "unstaged line\n", encoding="utf-8")
    attributes = work / ".gitattributes"
    attributes.write_text(attributes.read_text(encoding="utf-8") + "# unstaged only\n", encoding="utf-8")
    caller_state(work, index)  # settle the index's cached file data before the snapshot
    before = caller_state(work, index)
    config = scratch / f"{name}.yaml"
    entry = json.dumps(hook_entry)
    config.write_text(
        f"repos:\n  - repo: local\n    hooks:\n      - id: probe\n        name: probe\n        entry: {entry}\n        language: system\n"
        "        pass_filenames: false\n        always_run: true\n        stages: [manual]\n",
        encoding="utf-8",
    )
    hook_env = {**os.environ, **env, "PRE_COMMIT_HOME": str(scratch / f"{name}.pre_commit_home")}
    try:
        result = run_command(
            str(ROOT / ".venv" / "bin" / "pre-commit"),
            ["run", "--config", str(config), "--hook-stage", "manual", "--verbose", *(["--all-files"] if all_files else [])],
            cwd=work,
            env=hook_env,
            timeout=900,
        )
        after = caller_state(work, index)
    finally:
        restore(work)
    log = result.stdout + result.stderr
    (out / f"{name}.log").write_text(log, encoding="utf-8")
    changed = sorted(key for key in before if before[key] != after[key] and (all_files or key != "index_sha256"))
    child = re.search(r"(\d+)/(\d+) passed", log)
    checks = {
        "hook_exit_zero": result.returncode == 0,
        "caller_state_unchanged": not changed,
        "staged_diff_present_before": "staged line" in before["staged_diff"],
    }
    if "run_ci_groups_e2e" in hook_entry:
        checks["child_ran_all_cases"] = child is not None and child.group(1) == child.group(2) and int(child.group(2)) >= 16
    return {"case": name, "passed": all(checks.values()), "checks": checks, "changed_after_hook": changed, "exit": result.returncode}


def inherited_context_cases(work: Path, out: Path, scratch: Path) -> list[dict[str, Any]]:
    runner_entry = f"{PYTHON} -m scripts.quality.run_ci_groups_e2e --selection-only"
    control = find_program("true") or "true"
    cases = []
    for suffix, all_files in (("all_files", True), ("commit_mode", False)):
        cases.append(inherited_context_case(work, out, scratch, f"inherited_index_control_{suffix}", control, all_files))
        cases.append(inherited_context_case(work, out, scratch, f"inherited_index_survives_{suffix}", runner_entry, all_files))
    return cases


def code_identity(work: Path) -> dict[str, Any]:
    files = [*COPIED, "scripts/process.py"]
    versions = {
        "python": run_command(str(PYTHON), ["--version"], timeout=60).stdout.strip(),
        "git": run_command("git", ["--version"], timeout=60).stdout.strip(),
        "pre-commit": run_command(str(ROOT / ".venv" / "bin" / "pre-commit"), ["--version"], timeout=60).stdout.strip(),
        "markdownlint-cli2": "installed" if MARKDOWNLINT.exists() else "missing",
    }
    return {"revision": git(work, "rev-parse", "HEAD").strip(), "sha256": {path: sha256_of(ROOT / path) for path in files}, "tools": versions}


def acquisition_selection_case(work: Path, path: str, wanted: bool) -> dict[str, Any]:
    entry = work / "scripts/acquisition/run_checks.sh"
    original = entry.read_bytes()
    marker = "acquisition-selection-probe"
    try:
        entry.write_text(f"#!/usr/bin/env bash\nprintf '%s\\n' {marker}\n")
        result = run_command(
            str(PYTHON),
            ["-m", "pre_commit", "run", "acquisition-checks", "--hook-stage", "pre-commit", "--verbose", "--files", path],
            cwd=work,
            env={**os.environ, "PRE_COMMIT_HOME": str(ROOT / ".tools/pre_commit_cache")},
            timeout=180,
        )
    finally:
        entry.write_bytes(original)
    output = result.stdout + result.stderr
    passed = result.returncode == 0 and ((marker in output) if wanted else ("no files to check" in output and marker not in output))
    return {"case": f"acquisition_selection_{path}", "passed": passed, "exit": result.returncode, "wanted": wanted, "output": output}


def selection_cases(work: Path) -> list[dict[str, Any]]:
    cases = [
        selection_case(work, "docs_only_range_selects_none", ["--scope", "range", "--from", f"{DOCS_ONLY}^", "--to", DOCS_ONLY, "--part", "core"], []),
        selection_case(
            work,
            "infra_range",
            ["--scope", "range", "--from", f"{INFRA_ONLY}^", "--to", INFRA_ONLY, "--part", "core"],
            ["configuration_checks", "terraform_checks"],
        ),
        selection_case(
            work,
            "dbt_and_script_range",
            ["--scope", "range", "--from", f"{DBT_AND_README}^", "--to", DBT_AND_README, "--part", "core"],
            ["python_lint", "lakehouse_checks", "repository_code_checks", "sql_style"],
        ),
        selection_case(
            work,
            "global_trigger_runs_all",
            ["--scope", "range", "--from", f"{REQUIREMENTS}^", "--to", REQUIREMENTS, "--part", "core"],
            CORE,
            "selects every group",
        ),
        selection_case(work, "zero_start_runs_all", ["--scope", "range", "--from", ZERO, "--to", INFRA_ONLY, "--part", "core"], CORE, "no usable range"),
        selection_case(work, "missing_commit_runs_all", ["--scope", "range", "--from", MISSING, "--to", INFRA_ONLY, "--part", "core"], CORE, "no usable range"),
        selection_case(work, "empty_range_runs_all", ["--scope", "range", "--part", "core"], CORE, "no usable range"),
        selection_case(work, "unclassified_path_runs_all", ["--scope", "paths", "--path", "newtop/tool.bin", "--part", "core"], CORE, "unclassified"),
        selection_case(
            work,
            "launcher_path",
            ["--scope", "paths", "--path", "orchestration/launcher.py", "--part", "all"],
            ["python_lint", "lakehouse_checks", "orchestration_checks", "repository_code_checks", "launcher"],
        ),
        selection_case(
            work,
            "dag_python_selects_types",
            ["--scope", "paths", "--path", "dags/broker_probe_dag.py", "--part", "core"],
            ["python_lint", "orchestration_checks", "repository_code_checks"],
        ),
        selection_case(
            work,
            "service_python_selects_types",
            ["--scope", "paths", "--path", "services/hai/deadline_run.py", "--part", "core"],
            ["python_lint", "lakehouse_checks", "repository_code_checks"],
        ),
        selection_case(
            work,
            "type_manifest_selects_types",
            ["--scope", "paths", "--path", "config/quality/mypy_runtimes.json", "--part", "core"],
            ["python_lint", "lakehouse_checks", "repository_code_checks", "configuration_checks"],
        ),
        selection_case(work, "launcher_part_skips_docs", ["--scope", "paths", "--path", "docs/data_collection.md", "--part", "launcher"], []),
        selection_case(work, "full_scope_runs_all", ["--scope", "full", "--part", "all"], [*CORE, "launcher", "acquisition"], "full scope"),
        selection_case(
            work, "markdown_only_change_selects_none", ["--scope", "paths", "--path", "README.md", "--path", "docs/project_notes.md", "--part", "core"], []
        ),
        selection_case(
            work,
            "markdown_config_selects_its_checker_only",
            ["--scope", "paths", "--path", ".markdownlint-cli2.jsonc", "--part", "core"],
            ["repository_code_checks"],
        ),
        selection_case(
            work,
            "markdown_pin_selects_installer_checks",
            ["--scope", "paths", "--path", "scripts/quality/markdownlint/package-lock.json", "--part", "core"],
            ["repository_code_checks", "configuration_checks"],
        ),
        selection_case(
            work,
            "nested_markdown_config_selects_none",
            ["--scope", "paths", "--path", "docs/.markdownlint.json", "--path", "docs/guides/.markdownlint-cli2.yaml", "--part", "core"],
            [],
        ),
        selection_case(
            work, "sqlfluff_config_selects_sql_style", ["--scope", "paths", "--path", ".sqlfluff", "--part", "core"], ["repository_code_checks", "sql_style"]
        ),
        staged_case(
            work,
            "staged_scope_reads_index",
            [["update-index", "--add", "--cacheinfo", f"100644,{git(work, 'rev-parse', 'HEAD:infra/iam/main.tf').strip()},infra/iam/staged_copy.tf"]],
            ["configuration_checks", "terraform_checks"],
            unstaged="dbt/dbt_project.yml",
        ),
        staged_case(work, "deleted_file_counts", [["rm", "--quiet", "--cached", "infra/iam/main.tf"]], ["configuration_checks", "terraform_checks"]),
        staged_case(work, "renamed_file_counts_old_name", [["mv", "infra/iam/main.tf", "docs/renamed_main.tf"]], ["configuration_checks", "terraform_checks"]),
    ]
    code, _, _ = select(work, "--scope", "bogus")
    cases.append({"case": "invalid_scope_refused", "passed": code == 2, "exit": code})
    code, _, _ = select(work, "--scope", "full", "--part", "bogus")
    cases.append({"case": "invalid_part_refused", "passed": code == 2, "exit": code})
    cases += [
        acquisition_selection_case(work, "scripts/quality/typecheck.py", True),
        acquisition_selection_case(work, "config/quality/mypy_runtimes.json", True),
        acquisition_selection_case(work, "docs/quality_checks.md", False),
    ]
    return cases


def real_run_cases(work: Path, out: Path, scratch: Path) -> list[dict[str, Any]]:
    cases: list[dict[str, Any]] = []
    for part in ("all", "core", "launcher"):
        cases.append(list_only_case(work, out, part))
    for part in ("core", "all", "launcher"):
        cases.append(docs_only_case(work, out, part))
    for part in ("core", "all", "launcher"):
        cases.append(gate_failure_case(work, out, part))
    cases.append(gates_before_launcher_case(work, out, "launcher", gate_fails=False))
    cases += [gates_before_launcher_case(work, out, part, gate_fails=True) for part in ("launcher", "all")]
    cases.append(markdown_case(work, out, "markdown_config_only_change_lints_unchanged_documents", '"MD013": { "line_length": 40 },', want_failure=True))
    cases.append(markdown_case(work, out, "markdown_config_comment_only_passes", '"MD013": false, // unchanged rule', want_failure=False))
    cases.append(
        markdown_case(work, out, "markdown_config_nested_lints_unchanged_documents", '"MD013": { "line_length": 40 },', want_failure=True, form="nested")
    )
    cases.append(
        markdown_case(work, out, "markdown_config_dependency_only_change_lints", '"MD013": { "line_length": 40 },', want_failure=True, form="dependency")
    )
    cases += inherited_context_cases(work, out, scratch)
    return cases


def main() -> int:
    clear_git_environment()  # a hook's GIT_INDEX_FILE, GIT_DIR and GIT_WORK_TREE must never reach the scratch repositories
    selection_only = "--selection-only" in sys.argv[1:]
    out = ROOT / "data" / "e2e" / "ci_groups" / f"{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}_{os.getpid()}"
    out.mkdir(parents=True)
    scratch = Path(tempfile.mkdtemp(prefix="ci_groups_e2e."))
    work = scratch / "worktree"
    run_command(
        "git",
        ["-C", str(ROOT), "worktree", "add", "--quiet", "--detach", str(work), "HEAD"],
        env={**os.environ, "GIT_LFS_SKIP_SMUDGE": "1"},
        timeout=600,
        check=True,
    )
    results: list[dict[str, Any]] = []
    identity: dict[str, Any] = {}
    try:
        for name in (".venv", ".tools"):
            (work / name).symlink_to(ROOT / name)
        install_copies(work)
        identity = code_identity(work)
        results += selection_cases(work)
        if not selection_only:
            results += real_run_cases(work, out, scratch)
    finally:
        run_command("git", ["-C", str(ROOT), "worktree", "remove", "--force", str(work)], timeout=600)
        shutil.rmtree(scratch, ignore_errors=True)
    after = code_identity(ROOT) if identity else {}
    if identity and identity["sha256"] != after["sha256"]:
        results.append(
            {
                "case": "code_unchanged_during_run",
                "passed": False,
                "changed": sorted(k for k in identity["sha256"] if identity["sha256"][k] != after["sha256"][k]),
            }
        )
    failed = [result["case"] for result in results if not result["passed"]]
    skipped = ["list_only_prints_plan_*", "docs_only_runs_gates*", "gate_failure_*", "gates_run_before_launcher", "markdown_config_*", "inherited_index_*"]
    mode = {"mode": "iteration", "skipped": skipped, "note": "not commit evidence"} if selection_only else {"mode": "full"}
    report = {"started": out.name, **mode, "code": identity, "cases": results, "passed": len(results) - len(failed), "total": len(results), "failed": failed}
    (out / "report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    for result in results:
        sys.stdout.write(("PASS " if result["passed"] else "FAIL ") + result["case"] + "\n")
    sys.stdout.write(f"{report['passed']}/{report['total']} passed; report {out / 'report.json'}\n")
    return 0 if not failed else 1


if __name__ == "__main__":
    raise SystemExit(main())
