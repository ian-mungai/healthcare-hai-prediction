"""E2E for the check-group selection of scripts/run_ci.sh (scripts/quality/ci_groups.py and config/quality/ci_groups.json).

Run from the repository root: ``.venv/bin/python -m scripts.quality.run_ci_groups_e2e``. It adds a scratch Git worktree at HEAD,
copies the working check inputs into it, then checks which groups each scope selects: committed ranges
of known content, unusable ranges, staged changes (a deletion and a rename included), explicit paths, the full scope and
invalid input. Real runs of the CI entry check that the unconditional gates run in every part (also with an empty selection)
and before the launcher E2E, that a failing gate fails every part, that the full Markdown lint runs as a gate in every scope
and that the entry plan alone (``CI_LIST_ONLY=1``) runs nothing. One case runs this runner under a real pre-commit hook that
inherits an alternate ``GIT_INDEX_FILE``, ``GIT_DIR`` and ``GIT_WORK_TREE`` and checks that the caller's index, staged diff and working files survive. Failure
modes CS1 to CS31 (plans, local). The report goes to data/e2e/ci_groups/<UTC>_<pid>/report.json with the revision, the SHA-256
of the files under test and the tool versions; the run fails when a file under test changes while it runs.

``--selection-only`` runs the selector and acquisition-hook selection cases as an iteration run and not commit evidence.
It skips every case that runs scripts/run_ci.sh or this runner. Acquisition selection uses a scratch replacement of the
heavy gate body and the real hook configuration.
"""

from __future__ import annotations

import ast
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
from scripts.quality import ci_groups
from scripts.quality.scratch_worktree import ScratchWorktrees

ROOT = Path(__file__).resolve().parents[2]
PYTHON = ROOT / ".venv" / "bin" / "python"
RUNNER = "scripts/quality/run_ci_groups_e2e.py"
COPIED = (
    ".github/workflows/ci.yml",
    ".markdownlint-cli2.jsonc",
    ".pre-commit-config.yaml",
    ".sqlfluff",
    ".yamllint",
    "README.md",
    "config/quality/ci_groups.json",
    "config/quality/docker_calls_reviewed.json",
    "config/quality/mypy_runtimes.json",
    "config/quality_tools.json",
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
    "scripts/orchestration/run_pipeline_light.py",
    "scripts/orchestration/run_publish_contracts.py",
    "orchestration/graph.py",
    "orchestration/network_policy.py",
    "services/orchestration/egress_proxy.json",
    "scripts/quality/ci_groups.py",
    "scripts/quality/lint_sources.py",
    "scripts/quality/documentation_review.py",
    "scripts/quality/repo_checks.py",
    "scripts/quality/run_checks_e2e.py",
    "scripts/quality/run_lint_coverage_e2e.py",
    "dags/gold_maintenance_dag.py",
    "dags/gold_transform_dag.py",
    "dags/silver_transform_dag.py",
    "orchestration/dag_runtime.py",
    "orchestration/interface.py",
    "orchestration/pipeline_protocol.py",
    "orchestration/pipeline_render.py",
    "scripts/lakehouse/gold_maintenance.py",
    "scripts/lakehouse/pipeline_build.py",
    "scripts/lakehouse/pipeline_quality.py",
    "scripts/lakehouse/silver_export.py",
    "scripts/lakehouse/silver_input.py",
    "scripts/lakehouse/silver_publish.py",
    "scripts/orchestration/in_image_checks.py",
    "scripts/orchestration/pipeline_control.py",
    "scripts/orchestration/publish_dag_fixtures.py",
    "scripts/orchestration/run_control.py",
    "scripts/orchestration/run_network_classes_e2e.py",
    "scripts/orchestration/run_publish_dags_e2e.py",
    "scripts/orchestration/run_transform_e2e.py",
    "scripts/orchestration/supervisor.py",
    "scripts/orchestration/traffic_probe.py",
    "scripts/orchestration/transform_fixtures.py",
    "scripts/quality/run_ci_groups_e2e.py",
    "tests/orchestration/fixtures/combined_dag.py",
    RUNNER,
    "scripts/quality/scratch_worktree.py",
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
LAYERS = ["lakehouse_bronze", "lakehouse_silver", "lakehouse_gold"]
CORE = ["python_lint", "orchestration_checks", "repository_code_checks", "configuration_checks", "sql_style", "terraform_checks"]
WITH_LAYERS = [CORE[0], *LAYERS, *CORE[1:]]
PART_GROUPS = {
    "core": CORE,
    "launcher": ["launcher"],
    "bronze": LAYERS[:1],
    "silver": LAYERS[1:2],
    "gold": LAYERS[2:],
    "core+layers": WITH_LAYERS,
    "all": [*WITH_LAYERS, "launcher", "acquisition"],
}
PASS_LINES = {part: "All local CI checks passed" for part in PART_GROUPS} | {"launcher": "The launcher part passed."}
LAKEHOUSE_SCRIPT = ["python_lint", *LAYERS, "orchestration_checks", "repository_code_checks"]
QUALITY_SCRIPT = ["python_lint", "orchestration_checks", "repository_code_checks", "configuration_checks"]
QUALITY_CONFIG = [*LAYERS, "orchestration_checks", "repository_code_checks", "configuration_checks"]
ORCHESTRATION_SHARED = {
    "dbt/profiles.yml": [*LAYERS, "orchestration_checks", "repository_code_checks", "sql_style"],
    "data_contracts/lakehouse/silver_publish.json": [*LAYERS, "orchestration_checks"],
    "scripts/lakehouse/pipeline_build.py": LAKEHOUSE_SCRIPT,
    "scripts/lakehouse/silver_input.py": LAKEHOUSE_SCRIPT,
    "scripts/lakehouse/silver_publish.py": LAKEHOUSE_SCRIPT,
    "scripts/lakehouse/gold_maintenance.py": ["python_lint", "lakehouse_gold", "orchestration_checks", "repository_code_checks"],
    "scripts/lakehouse/run_lock.py": LAKEHOUSE_SCRIPT,
    "scripts/lakehouse/fake_docker.py": LAKEHOUSE_SCRIPT,
    "scripts/lakehouse/run_launcher_e2e.py": LAKEHOUSE_SCRIPT,
    "scripts/lakehouse/pins.py": LAKEHOUSE_SCRIPT,
    "scripts/lakehouse/provenance.py": LAKEHOUSE_SCRIPT,
    "scripts/lakehouse/memory_budget.py": LAKEHOUSE_SCRIPT,
    "scripts/lakehouse/dbt_package_archive.py": LAKEHOUSE_SCRIPT,
    "scripts/lakehouse/__init__.py": LAKEHOUSE_SCRIPT,
    "docker-compose.yaml": [*LAYERS, "orchestration_checks"],
    "scripts/quality/typecheck.py": QUALITY_SCRIPT,
    "scripts/quality/docker_calls.py": QUALITY_SCRIPT,
    "scripts/quality/repo_checks.py": QUALITY_SCRIPT,
    "config/quality/docker_calls_reviewed.json": QUALITY_CONFIG,
}
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
    part = args[args.index("--part") + 1] if "--part" in args else "all"
    scope = args[args.index("--scope") + 1] if "--scope" in args else ""
    if part != "launcher":
        unconditional = {"sql_style"} if scope in {"full", "range"} and "sql_style" in PART_GROUPS[part] else set()
        want_run = [group for group in PART_GROUPS[part] if group in {*want_run, *unconditional}]
    code, plan, error = select(work, *["all" if argument == "core+layers" else argument for argument in args])
    run = plan["run"] if plan else None
    if run is not None and part == "core+layers":
        run = [group for group in run if group in PART_GROUPS[part]]
    reason = plan["reason"] if plan else ""
    skips_named = plan is not None and set(plan["skip"]) == set(plan["groups"]) - set(plan["run"])
    passed = code == 0 and run == want_run and want_reason in reason and skips_named
    return {"case": name, "passed": passed, "exit": code, "run": run, "want_run": want_run, "reason": reason, "error": error[-400:]}


def install_copies(work: Path) -> None:
    for path in COPIED:
        if (ROOT / path).exists():
            (work / path).parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(ROOT / path, work / path)
    if not (ROOT / "dags/transform_dag.py").exists():
        (work / "dags/transform_dag.py").unlink(missing_ok=True)


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
        return selection_case(work, name, ["--scope", "staged", "--part", "core+layers"], want_run)
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
        "conditional_groups_skipped_with_reason": all(f"skipped {group}:" in log for group in PART_GROUPS[part] if group != "repository_code_checks"),
        "gates_ran": re.search(r"^gates: \d+ s$", log, re.MULTILINE) is not None,
        "markdown_lint_ran": "markdownlint-cli2 v" in log,
        # sql_style is selected by path in the staged scope (always_except_commits), so a README-only change skips it with a reason.
        "required_groups_only": re.findall(r"^== (\w+) ==$", log, re.MULTILINE) == (["repository_code_checks"] if part in {"core", "all"} else []),
        "summary_has_durations": "| Step | Seconds |" in text and re.search(r"^\| gates \| \d+ \|$", text, re.MULTILINE) is not None,
    }
    return {"case": name, "passed": all(checks.values()), "checks": checks, "exit": code}


def layer_skip_case(work: Path, out: Path) -> dict[str, Any]:
    """A layer job whose layer the staged change does not reach reports success, names the reason in the job summary and still runs the gates."""
    (work / "dbt" / "models" / "gold").mkdir(parents=True, exist_ok=True)
    shutil.copy2(work / "dbt" / "dbt_project.yml", work / "dbt" / "models" / "gold" / "staged_copy.yml")
    git(work, "add", "dbt/models/gold/staged_copy.yml")
    try:
        code, log, text = run_entry(work, out, "layer_skip_bronze", {"CI_SCOPE": "staged", "CI_PART": "bronze"})
        listed_code, listed, _ = run_entry(work, out, "layer_skip_gold_plan", {"CI_SCOPE": "staged", "CI_PART": "gold", "CI_LIST_ONLY": "1"})
    finally:
        restore(work)
        (work / "dbt" / "models" / "gold" / "staged_copy.yml").unlink(missing_ok=True)
    checks = {
        "exit_zero": code == 0,
        "reached_the_end": PASS_LINES["bronze"] in log,
        "reason_in_the_job_summary": "skipped lakehouse_bronze: no changed path is on its list or on an upstream layer's list" in text,
        "gates_ran": re.search(r"^gates: \d+ s$", log, re.MULTILINE) is not None,
        "no_layer_group_ran": "\n== " not in log,
        "the_downstream_layer_is_selected": listed_code == 0 and "run lakehouse_gold" in listed and "skipped lakehouse_gold" not in listed,
    }
    return {"case": "layer_not_reached_reports_success_with_reason", "passed": all(checks.values()), "checks": checks, "exit": code}


INTERRUPT_PROBE = """
import os
import signal
import sys
from pathlib import Path

from scripts.quality.scratch_worktree import ScratchWorktrees

signal.signal(signal.SIGINT, signal.default_int_handler)
trees = ScratchWorktrees(Path(sys.argv[1]))
trees.add(Path(sys.argv[2]))
removal = trees.remove_one


def interrupted_removal(path: Path) -> None:
    os.kill(os.getpid(), signal.SIGINT)
    removal(path)


trees.remove_one = interrupted_removal
try:
    trees.close()
except KeyboardInterrupt:
    sys.stdout.write("interrupt delivered after the cleanup\\n")
"""


def scratch_cleanup_interrupt_case(scratch: Path) -> dict[str, Any]:
    """A Ctrl-C that arrives while the scratch worktree is being removed is held until the removal and the check of the registered worktrees are done."""
    repo = scratch / "interrupt_repository"
    repo.mkdir()
    git(repo, "init", "--quiet")
    git(repo, "-c", "user.name=scratch", "-c", "user.email=scratch@example.invalid", "commit", "--quiet", "--allow-empty", "-m", "scratch")
    result = run_command(str(PYTHON), ["-c", INTERRUPT_PROBE, str(repo), str(scratch / "interrupt_worktree")], cwd=ROOT, timeout=120)
    registered = [line for line in git(repo, "worktree", "list", "--porcelain").splitlines() if line.startswith("worktree ")]
    checks = {
        "the_interrupt_arrives_after_the_cleanup": result.returncode == 0 and "interrupt delivered after the cleanup" in result.stdout,
        "no_worktree_is_left_registered": len(registered) == 1,
        "the_folder_is_gone": not (scratch / "interrupt_worktree").exists(),
        "no_problem_reported": "scratch worktree:" not in result.stderr,
    }
    return {
        "case": "scratch_worktree_cleanup_holds_an_interrupt_until_it_is_done",
        "passed": all(checks.values()),
        "checks": checks,
        "error": result.stderr[-400:],
    }


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
    wanted = {"core": "run sql_style", "bronze": "run lakehouse_bronze", "silver": "run lakehouse_silver", "gold": "run lakehouse_gold"}.get(
        part, "run launcher"
    )
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
    state["index_sha256"] = sha256_of(index)
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
    caller_state(work, index)
    before = caller_state(work, index)
    config = scratch / f"{name}.yaml"
    entry = json.dumps(hook_entry)
    config.write_text(
        f"repos:\n  - repo: local\n    hooks:\n      - id: probe\n        name: probe\n        entry: {entry}\n        language: system\n"
        "        pass_filenames: false\n        always_run: true\n        stages: [manual]\n",
        encoding="utf-8",
    )
    hook_env = {
        **os.environ,
        **env,
        "GIT_DIR": git(work, "rev-parse", "--absolute-git-dir").strip(),
        "GIT_WORK_TREE": str(work),
        "PRE_COMMIT_HOME": str(scratch / f"{name}.pre_commit_home"),
    }
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
    return {
        "revision": git(work, "rev-parse", "HEAD").strip(),
        "sha256": {path: sha256_of(ROOT / path) for path in files},
        "tools": versions,
        "deleted_sources": ["dags/transform_dag.py"] if not (ROOT / "dags/transform_dag.py").exists() else [],
    }


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


def layer_cases(work: Path) -> list[dict[str, Any]]:
    """The lakehouse layers: a change selects its layer and every layer downstream; shared code selects all three."""

    def path_case(name: str, path: str, groups: list[str]) -> dict[str, Any]:
        return selection_case(work, name, ["--scope", "paths", "--path", path, "--part", "core+layers"], groups)

    blob = git(work, "rev-parse", "HEAD:dbt/dbt_project.yml").strip()
    silver_gold = ["lakehouse_silver", "lakehouse_gold"]
    cases = [
        selection_case(work, "core_part_excludes_the_layers", ["--scope", "full", "--part", "core"], CORE, "full scope"),
        *(
            selection_case(work, f"{part}_part_is_its_layer_only", ["--scope", "full", "--part", part], groups, "full scope")
            for part, groups in (("bronze", LAYERS[:1]), ("silver", LAYERS[1:2]), ("gold", LAYERS[2:]))
        ),
        path_case(
            "layer_bronze_only_change_selects_every_layer",
            "scripts/lakehouse/bronze.py",
            ["python_lint", *LAYERS, "orchestration_checks", "repository_code_checks"],
        ),
        path_case(
            "layer_silver_only_change_selects_silver_and_gold",
            "scripts/lakehouse/silver.py",
            ["python_lint", *silver_gold, "orchestration_checks", "repository_code_checks"],
        ),
        path_case(
            "layer_gold_only_change_selects_gold_only",
            "scripts/lakehouse/gold_publish.py",
            ["python_lint", "lakehouse_gold", "orchestration_checks", "repository_code_checks"],
        ),
        path_case(
            "layer_shared_change_selects_every_layer",
            "scripts/lakehouse/memory_budget.py",
            ["python_lint", *LAYERS, "orchestration_checks", "repository_code_checks"],
        ),
        path_case(
            "layer_shared_gold_checks_select_every_layer",
            "scripts/lakehouse/gold_checks.py",
            ["python_lint", *LAYERS, "orchestration_checks", "repository_code_checks"],
        ),
        path_case(
            "layer_gold_model_selects_gold_only", "dbt/models/gold/dimensions/_dimensions__models.yml", ["lakehouse_gold", "orchestration_checks", "sql_style"]
        ),
        path_case(
            "layer_silver_model_selects_silver_and_gold", "dbt/models/silver/staging/_staging__models.yml", [*silver_gold, "orchestration_checks", "sql_style"]
        ),
        path_case("layer_gold_seed_selects_gold_only", "dbt/seeds/gold/identity_splits.csv", ["lakehouse_gold", "orchestration_checks", "sql_style"]),
        path_case("layer_gold_macro_read_by_bronze_selects_every_layer", "dbt/macros/gold_facts.sql", [*LAYERS, "orchestration_checks", "sql_style"]),
        path_case(
            "layer_gold_identity_macro_read_by_bronze_selects_every_layer", "dbt/macros/gold_identity.sql", [*LAYERS, "orchestration_checks", "sql_style"]
        ),
        path_case(
            "layer_gold_dimension_read_by_bronze_selects_every_layer",
            "dbt/models/gold/dimensions/dim_release.sql",
            [*LAYERS, "orchestration_checks", "sql_style"],
        ),
        path_case(
            "layer_gold_dimension_no_layer_above_reads_selects_gold_only",
            "dbt/models/gold/dimensions/dim_county.sql",
            ["lakehouse_gold", "orchestration_checks", "sql_style"],
        ),
        path_case(
            "layer_staging_model_read_by_bronze_selects_every_layer",
            "dbt/models/silver/staging/stg_bronze__file_copies.sql",
            [*LAYERS, "orchestration_checks", "sql_style"],
        ),
        path_case(
            "layer_build_marker_model_read_by_bronze_selects_every_layer",
            "dbt/models/silver/intermediate/int_silver_build_marker.sql",
            [*LAYERS, "orchestration_checks", "sql_style"],
        ),
        path_case(
            "layer_staging_model_nothing_upstream_reads_selects_silver_and_gold",
            "dbt/models/silver/staging/stg_acs_dp02.sql",
            [*silver_gold, "orchestration_checks", "sql_style"],
        ),
        path_case("layer_shared_macro_selects_every_layer", "dbt/macros/file_metadata.sql", [*LAYERS, "orchestration_checks", "sql_style"]),
        path_case("layer_gold_contract_is_read_by_silver_first", "data_contracts/lakehouse/gold_contract.json", [*silver_gold, "orchestration_checks"]),
        path_case("layer_gold_suite_is_read_by_bronze_first", "data_contracts/great_expectations/gold/gold_keys.json", [*LAYERS, "orchestration_checks"]),
        path_case("layer_saved_silver_suite_selects_silver_and_gold", "features/great_expectations/suites/silver_spatial.json", silver_gold),
        path_case("layer_suite_baselines_are_read_by_bronze_first", "data_contracts/great_expectations/baselines.json", [*LAYERS, "orchestration_checks"]),
        path_case(
            "layer_feature_model_is_read_by_gold_only",
            "dbt/models/features/int_spine_county_context.sql",
            ["lakehouse_gold", "orchestration_checks", "sql_style"],
        ),
        path_case(
            "layer_feature_recipe_seed_selects_gold_only", "dbt/seeds/features/mup_measures_recipe.csv", ["lakehouse_gold", "orchestration_checks", "sql_style"]
        ),
        path_case("layer_silver_columns_select_silver_and_gold", "data_contracts/lakehouse/silver_columns.json", [*silver_gold, "orchestration_checks"]),
        path_case("layer_bronze_table_map_selects_every_layer", "config/lakehouse/bronze_tables.json", [*LAYERS, "configuration_checks"]),
        staged_case(
            work,
            "layer_staged_gold_model_selects_gold_only",
            [["update-index", "--add", "--cacheinfo", f"100644,{blob},dbt/models/gold/staged_copy.yml"]],
            ["lakehouse_gold", "orchestration_checks", "sql_style"],
        ),
        staged_case(
            work,
            "layer_staged_silver_model_selects_silver_and_gold",
            [["update-index", "--add", "--cacheinfo", f"100644,{blob},dbt/models/silver/staged_copy.yml"]],
            [*silver_gold, "orchestration_checks", "sql_style"],
        ),
    ]
    return [*cases, *reader_coverage_cases(work)]


def module_source(module: str) -> str:
    return (ROOT / f"{module.replace('.', '/')}.py").read_text(encoding="utf-8")


def fills_template(template: list[str | None], path: str, names: set[str]) -> bool:
    """True when ``path`` is the f-string ``template`` (constant parts, ``None`` per placeholder) with each placeholder replaced by a string of ``names``."""

    def from_part(index: int, position: int) -> bool:
        if index == len(template):
            return position == len(path)
        part = template[index]
        if part is not None:
            return path.startswith(part, position) and from_part(index + 1, position + len(part))
        return any(path[position:end] in names and from_part(index + 1, end) for end in range(position + 1, len(path) + 1))

    return from_part(0, 0)


def source_inputs(source: str, tracked: set[str]) -> dict[str, str]:
    """Every tracked file that a string anywhere in ``source`` names, in module-level constants and inside functions alike, with the text that names it.
    A string constant, or a ``/`` join of strings (``Path(..)``, names assigned a join anywhere in the module), names a file or the files in a folder when it
    holds a slash; a join that is only created (the receiver of ``mkdir``, ``write_text``, ``write_bytes``, ``touch`` or ``unlink``) is not read. An f-string
    names every tracked file it matches when each placeholder is replaced by a string constant of the module (``f"dbt/models/{folder}/{name}.sql"``).
    Docstrings and one-component names are not read as paths. The source is read, not imported."""
    tree = ast.parse(source)
    in_fstring = {id(part) for node in ast.walk(tree) if isinstance(node, ast.JoinedStr) for part in node.values}
    docstrings = {id(node.value) for node in ast.walk(tree) if isinstance(node, ast.Expr)}
    strings = {
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant) and isinstance(node.value, str) and id(node) not in in_fstring and id(node) not in docstrings
    }
    created = {
        id(node.func.value)
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr in {"mkdir", "write_text", "write_bytes", "touch", "unlink"}
    }
    known = {"ROOT": "", "REPO_ROOT": ""}

    def operand(node: ast.expr) -> str | None:
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value
        if isinstance(node, ast.Name):
            return known.get(node.id)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "Path" and len(node.args) == 1:
            return operand(node.args[0])
        return None

    def joined(top: ast.BinOp) -> list[str]:
        parts: list[str | None] = []
        node: ast.expr = top
        while isinstance(node, ast.BinOp) and isinstance(node.op, ast.Div):
            parts.append(operand(node.right))
            node = node.left
        parts.append(operand(node))
        runs: list[str] = []
        run: list[str] = []
        for part in [*reversed(parts), None]:
            if part is None:
                if len(run) > 1:
                    runs.append("/".join(piece.strip("/") for piece in run if piece.strip("/")))
                run = []
            else:
                run.append(part)
        return runs

    assigned = [
        (node.targets[0].id, node.value)
        for node in ast.walk(tree)
        if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name) and node.targets[0].id not in known
    ]
    for _ in range(3):
        for name, value in assigned:
            if isinstance(value, ast.BinOp) and isinstance(value.op, ast.Div):
                if len(runs := joined(value)) == 1:
                    known[name] = runs[0]
            elif (text := operand(value)) is not None:
                known[name] = text
    inner = {id(node.left) for node in ast.walk(tree) if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Div)}
    names = set(strings)
    for node in ast.walk(tree):
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Div) and id(node) not in inner and id(node) not in created:
            names.update(joined(node))
    templates: list[list[str | None]] = [
        [part.value if isinstance(part, ast.Constant) and isinstance(part.value, str) else None for part in node.values]
        for node in ast.walk(tree)
        if isinstance(node, ast.JoinedStr)
    ]
    found: dict[str, str] = {}
    for path in sorted(tracked):
        for text in sorted(names):
            base = text.strip("/")
            if "/" in base and (path == base or path.startswith(f"{base}/")):
                found.setdefault(path, text)
        for template in templates:
            if None in template and any(template) and fills_template(template, path, strings):
                found.setdefault(path, 'f"' + "{}".join(part or "" for part in template) + '"')
    return found


def reader_inputs(modules: tuple[str, ...], tracked: set[str]) -> dict[str, str]:
    """Every tracked file that ``modules`` name anywhere in their source, with the module and the text that names it."""
    found: dict[str, str] = {}
    for name in modules:
        found |= {path: f"{name}: {text}" for path, text in source_inputs(module_source(name), tracked).items()}
    return found


def layer_modules(function: str) -> tuple[str, ...]:
    """The modules the check-group function ``function`` of scripts/run_ci.sh starts with ``-m``."""
    body = re.search(rf"^{function}\(\) \{{\n(.*?)^\}}", (ROOT / "scripts/run_ci.sh").read_text(encoding="utf-8"), re.DOTALL | re.MULTILINE)
    return tuple(re.findall(r"-m (scripts\.[\w.]+)", body.group(1))) if body else ()


SCAN_SAMPLE = """
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
MACROS = ROOT / "dbt" / "macros"
FOLDERS = ("gold/dimensions", "silver/staging")
NAMES = ("dim_release", "stg_bronze__files")


def read(root: Path, name: str, folder: str) -> str:
    \"\"\"Read dbt/models/gold/facts_county/fct_acs_county_value.sql, which is only a sentence.\"\"\"
    macro = (root / "dbt/macros/gold_facts.sql").read_text()
    joined = (root / "dbt" / "macros" / "file_metadata.sql").read_text()
    model = (root / f"dbt/models/{folder}/{name}.sql").read_text()
    (root / "scripts" / "quality").mkdir(parents=True)
    return macro + joined + model
"""


def reader_scan_case(tracked: set[str]) -> dict[str, Any]:
    """The reader scan finds a path written as one string inside a function, a ``/`` join inside a function and an f-string whose placeholders are the
    module's own strings, and does not read a folder that a function only creates, a sentence in a docstring or a model no string names."""
    found = set(source_inputs(SCAN_SAMPLE, tracked))
    checks = {
        "a_module_level_folder_names_its_files": "dbt/macros/cmi_files.sql" in found,
        "a_path_string_inside_a_function": "dbt/macros/gold_facts.sql" in found,
        "a_join_inside_a_function_names_its_file": "dbt/macros/file_metadata.sql" in found,
        "an_f_string_with_the_modules_own_strings": {
            "dbt/models/gold/dimensions/dim_release.sql",
            "dbt/models/silver/staging/stg_bronze__files.sql",
        }
        <= found,
        "a_folder_that_a_function_only_creates_is_not_read": not any(path.startswith("scripts/quality/") for path in found),
        "a_docstring_is_not_read": "dbt/models/gold/facts_county/fct_acs_county_value.sql" not in found,
        "a_model_no_string_names_is_not_read": "dbt/models/gold/dimensions/dim_county.sql" not in found,
    }
    return {"case": "reader_scan_finds_paths_built_inside_functions", "passed": all(checks.values()), "checks": checks}


def reader_coverage_cases(work: Path) -> list[dict[str, Any]]:
    """Each check group must be selected by a change to any tracked file its readers name, wherever the reader builds the path (module-level constants,
    ``/`` joins and f-strings inside functions). The readers are the modules each layer's function starts (read from scripts/run_ci.sh) and the modules they
    call for other layers' inputs (the bronze storage runner renders dbt models through the storage metadata comparison; the silver suite check calls the
    gold checks). A group whose readers name a file its list does not select is listed with that file and the text that names it.
    Every file this runner copies must select the repository group, which runs the runner's selection cases."""
    tracked = set(git(work, "ls-files").splitlines())
    readers = {
        "lakehouse_bronze": (*layer_modules("lakehouse_bronze"), "scripts.lakehouse.storage_metadata_comparison"),
        "lakehouse_silver": (*layer_modules("lakehouse_silver"), "scripts.lakehouse.gold_checks"),
        "lakehouse_gold": layer_modules("lakehouse_gold"),
    }
    missed: dict[str, list[str]] = {}
    counted: dict[str, int] = {}
    for group, modules in readers.items():
        inputs = reader_inputs(modules, tracked)
        counted[group] = len(inputs)
        for path, source in sorted(inputs.items()):
            if group not in ci_groups.plan("paths", "all", paths=[path])["run"]:
                missed.setdefault(group, []).append(f"{path} ({source})")
    not_selecting = [path for path in sorted(COPIED) if "repository_code_checks" not in ci_groups.plan("paths", "all", paths=[path])["run"]]
    return [
        reader_scan_case(tracked),
        {
            "case": "layer_readers_are_selected_by_their_inputs",
            "passed": not missed and all(counted[group] for group in ("lakehouse_silver", "lakehouse_gold")),
            "inputs_checked": counted,
            "missed": {group: items[:6] for group, items in missed.items()},
        },
        {
            "case": "files_the_runner_copies_select_the_repository_group",
            "passed": not not_selecting and len(COPIED) > 0,
            "checked": len(COPIED),
            "not_selecting": not_selecting[:6],
        },
    ]


def selection_cases(work: Path) -> list[dict[str, Any]]:
    cases = [
        selection_case(
            work,
            "docs_fixture_range_selects_reader_checks",
            ["--scope", "range", "--from", f"{DOCS_ONLY}^", "--to", DOCS_ONLY, "--part", "core+layers"],
            ["repository_code_checks"],
        ),
        selection_case(
            work,
            "infra_range",
            ["--scope", "range", "--from", f"{INFRA_ONLY}^", "--to", INFRA_ONLY, "--part", "core+layers"],
            ["configuration_checks", "terraform_checks"],
        ),
        selection_case(
            work,
            "dbt_and_script_range",
            ["--scope", "range", "--from", f"{DBT_AND_README}^", "--to", DBT_AND_README, "--part", "core+layers"],
            ["python_lint", *LAYERS, "orchestration_checks", "repository_code_checks", "sql_style"],
        ),
        selection_case(
            work,
            "global_trigger_runs_all",
            ["--scope", "range", "--from", f"{REQUIREMENTS}^", "--to", REQUIREMENTS, "--part", "core+layers"],
            WITH_LAYERS,
            "selects every group",
        ),
        selection_case(
            work, "zero_start_runs_all", ["--scope", "range", "--from", ZERO, "--to", INFRA_ONLY, "--part", "core+layers"], WITH_LAYERS, "no usable range"
        ),
        selection_case(
            work,
            "missing_commit_runs_all",
            ["--scope", "range", "--from", MISSING, "--to", INFRA_ONLY, "--part", "core+layers"],
            WITH_LAYERS,
            "no usable range",
        ),
        selection_case(work, "empty_range_runs_all", ["--scope", "range", "--part", "core+layers"], WITH_LAYERS, "no usable range"),
        selection_case(
            work, "unclassified_path_runs_all", ["--scope", "paths", "--path", "newtop/tool.bin", "--part", "core+layers"], WITH_LAYERS, "unclassified"
        ),
        selection_case(
            work,
            "launcher_path",
            ["--scope", "paths", "--path", "orchestration/launcher.py", "--part", "all"],
            ["python_lint", *LAYERS, "orchestration_checks", "repository_code_checks", "launcher"],
        ),
        selection_case(
            work,
            "dag_python_selects_types",
            ["--scope", "paths", "--path", "dags/broker_probe_dag.py", "--part", "core+layers"],
            ["python_lint", "orchestration_checks", "repository_code_checks"],
        ),
        selection_case(
            work,
            "service_python_selects_types",
            ["--scope", "paths", "--path", "services/hai/deadline_run.py", "--part", "core+layers"],
            ["python_lint", *LAYERS, "repository_code_checks"],
        ),
        selection_case(
            work,
            "type_manifest_selects_types",
            ["--scope", "paths", "--path", "config/quality/mypy_runtimes.json", "--part", "core+layers"],
            ["python_lint", *LAYERS, "orchestration_checks", "repository_code_checks", "configuration_checks"],
        ),
        selection_case(work, "launcher_part_skips_docs", ["--scope", "paths", "--path", "docs/data_collection.md", "--part", "launcher"], []),
        selection_case(work, "full_scope_runs_all", ["--scope", "full", "--part", "all"], PART_GROUPS["all"], "full scope"),
        selection_case(
            work,
            "readme_selects_reader_checks",
            ["--scope", "paths", "--path", "README.md", "--path", "docs/project_notes.md", "--part", "core+layers"],
            ["repository_code_checks"],
        ),
        selection_case(
            work,
            "markdown_config_selects_its_checker_only",
            ["--scope", "paths", "--path", ".markdownlint-cli2.jsonc", "--part", "core+layers"],
            ["repository_code_checks"],
        ),
        selection_case(
            work,
            "markdown_pin_selects_installer_checks",
            ["--scope", "paths", "--path", "scripts/quality/markdownlint/package-lock.json", "--part", "core+layers"],
            ["repository_code_checks", "configuration_checks"],
        ),
        selection_case(
            work,
            "nested_markdown_config_selects_no_group",
            ["--scope", "paths", "--path", "docs/.markdownlint.json", "--path", "docs/guides/.markdownlint-cli2.yaml", "--part", "core+layers"],
            [],
        ),
        selection_case(
            work,
            "sqlfluff_config_selects_sql_style",
            ["--scope", "paths", "--path", ".sqlfluff", "--part", "core+layers"],
            ["repository_code_checks", "sql_style"],
        ),
        staged_case(
            work,
            "staged_scope_reads_index",
            [["update-index", "--add", "--cacheinfo", f"100644,{git(work, 'rev-parse', 'HEAD:infra/iam/main.tf').strip()},infra/iam/staged_copy.tf"]],
            ["configuration_checks", "terraform_checks"],
            unstaged="dbt/dbt_project.yml",
        ),
        staged_case(
            work,
            "staged_dbt_change_selects_sql_style",
            [["update-index", "--add", "--cacheinfo", f"100644,{git(work, 'rev-parse', 'HEAD:dbt/dbt_project.yml').strip()},dbt/staged_copy.yml"]],
            [*LAYERS, "orchestration_checks", "sql_style"],
        ),
        staged_case(work, "deleted_file_counts", [["rm", "--quiet", "--cached", "infra/iam/main.tf"]], ["configuration_checks", "terraform_checks"]),
        staged_case(work, "renamed_file_counts_old_name", [["mv", "infra/iam/main.tf", "docs/renamed_main.tf"]], ["configuration_checks", "terraform_checks"]),
    ]
    cases += [
        *(
            selection_case(work, f"copied_input_{path}", ["--scope", "paths", "--path", path, "--part", "core+layers"], wanted)
            for path, wanted in (
                (".env.example", ["repository_code_checks", "configuration_checks"]),
                ("dbt/dbt_project.yml", [*LAYERS, "orchestration_checks", "repository_code_checks", "sql_style"]),
                ("dbt/profiles.yml", [*LAYERS, "orchestration_checks", "repository_code_checks", "sql_style"]),
                ("README.md", ["repository_code_checks"]),
                (
                    "dbt/models/silver/intermediate/_intermediate__models.yml",
                    ["lakehouse_silver", "lakehouse_gold", "orchestration_checks", "repository_code_checks", "sql_style"],
                ),
                ("docs/quality_checks.md", ["repository_code_checks"]),
            )
        ),
        selection_case(
            work,
            "dbt_archive_config_selects_sql_style",
            ["--scope", "paths", "--path", "config/lakehouse/dbt_package_archive.json", "--part", "core+layers"],
            [*LAYERS, "configuration_checks", "sql_style"],
        ),
        selection_case(
            work,
            "sqlfluff_pins_select_sql_style",
            ["--scope", "paths", "--path", "scripts/quality/sqlfluff/requirements.txt", "--part", "core+layers"],
            [*LAYERS, "repository_code_checks", "configuration_checks", "sql_style"],
        ),
        selection_case(
            work,
            "sqlfluff_installer_selects_metadata_checks",
            ["--scope", "paths", "--path", "scripts/quality/install_sqlfluff.py", "--part", "core+layers"],
            ["python_lint", *LAYERS, "repository_code_checks", "configuration_checks", "sql_style"],
        ),
        selection_case(
            work,
            "range_keeps_sql_style_without_sql_change",
            ["--scope", "range", "--from", f"{INFRA_ONLY}^", "--to", INFRA_ONLY, "--part", "core+layers"],
            ["configuration_checks", "sql_style", "terraform_checks"],
        ),
        selection_case(
            work,
            "new_python_folder_selects_types",
            ["--scope", "paths", "--path", "new_folder/source.py", "--part", "core+layers"],
            ["python_lint", "repository_code_checks"],
        ),
        selection_case(
            work,
            "service_sql_selects_runtime_style",
            ["--scope", "paths", "--path", "services/analytics/resources.sql", "--part", "core+layers"],
            [*LAYERS, "sql_style"],
        ),
        selection_case(
            work,
            "feature_sql_selects_inventory",
            ["--scope", "paths", "--path", "dbt/macros/features/hospital_alignment.sql", "--part", "core+layers"],
            ["lakehouse_gold", "orchestration_checks", "sql_style"],
        ),
        selection_case(
            work,
            "yaml_profile_selects_hook_e2e",
            ["--scope", "paths", "--path", ".yamllint", "--part", "core+layers"],
            ["repository_code_checks"],
        ),
        selection_case(
            work,
            "native_pins_select_install_and_hook_e2e",
            ["--scope", "paths", "--path", "config/quality_tools.json", "--part", "core+layers"],
            [*LAYERS, "repository_code_checks", "configuration_checks", "terraform_checks"],
        ),
    ]
    cases += [
        selection_case(work, f"orchestration_shared_{Path(path).stem}", ["--scope", "paths", "--path", path, "--part", "core+layers"], groups)
        for path, groups in ORCHESTRATION_SHARED.items()
    ]
    cases += [
        selection_case(
            work,
            "storage_harness_dependency_" + Path(path).stem,
            ["--scope", "paths", "--path", path, "--part", "core+layers"],
            ["python_lint", *LAYERS, "repository_code_checks", "configuration_checks"]
            if not path.startswith("scripts/acquisition/")
            else ["python_lint", *LAYERS, "repository_code_checks"],
        )
        for path in (
            "scripts/acquisition/s3_store.py",
            "scripts/acquisition/tests/test_capture.py",
            "scripts/infrastructure/render_project_config.py",
            "tests/support.py",
        )
    ]
    cases += layer_cases(work)
    code, _, _ = select(work, "--scope", "bogus")
    cases.append({"case": "invalid_scope_refused", "passed": code == 2, "exit": code})
    code, _, _ = select(work, "--scope", "full", "--part", "bogus")
    cases.append({"case": "invalid_part_refused", "passed": code == 2, "exit": code})
    cases += [
        acquisition_selection_case(work, "scripts/quality/typecheck.py", True),
        acquisition_selection_case(work, "config/quality/mypy_runtimes.json", True),
        acquisition_selection_case(work, "config/lakehouse/bronze_tables.json", True),
        acquisition_selection_case(work, "docs/quality_checks.md", False),
    ]
    return cases


def real_run_cases(work: Path, out: Path, scratch: Path) -> list[dict[str, Any]]:
    cases: list[dict[str, Any]] = []
    for part in ("all", "core", "launcher", "bronze", "silver", "gold"):
        cases.append(list_only_case(work, out, part))
    for part in ("core", "all", "launcher", "gold"):
        cases.append(docs_only_case(work, out, part))
    for part in ("core", "all", "launcher", "silver"):
        cases.append(gate_failure_case(work, out, part))
    cases.append(layer_skip_case(work, out))
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
    clear_git_environment()
    selection_only = "--selection-only" in sys.argv[1:]
    out = ROOT / "data" / "e2e" / "ci_groups" / f"{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}_{os.getpid()}"
    out.mkdir(parents=True)
    scratch = Path(tempfile.mkdtemp(prefix="ci_groups_e2e."))
    trees = ScratchWorktrees(ROOT)
    results: list[dict[str, Any]] = []
    identity: dict[str, Any] = {}
    try:
        work = trees.add(scratch / "worktree")
        for name in (".venv", ".tools"):
            (work / name).symlink_to(ROOT / name)
        install_copies(work)
        identity = code_identity(work)
        results.append(scratch_cleanup_interrupt_case(scratch))
        results += selection_cases(work)
        if not selection_only:
            results += real_run_cases(work, out, scratch)
    finally:
        worktrees = trees.close()
        shutil.rmtree(scratch, ignore_errors=True)
    results.append({"case": "scratch_worktree_removed", "passed": not worktrees["problems"], **worktrees})
    after = code_identity(ROOT) if identity else {}
    if identity and (identity["sha256"] != after["sha256"] or identity["deleted_sources"] != after["deleted_sources"]):
        results.append(
            {
                "case": "code_unchanged_during_run",
                "passed": False,
                "changed": sorted(k for k in identity["sha256"] if identity["sha256"][k] != after["sha256"][k])
                + (["dags/transform_dag.py"] if identity["deleted_sources"] != after["deleted_sources"] else []),
            }
        )
    failed = [result["case"] for result in results if not result["passed"]]
    skipped = [
        "list_only_prints_plan_*",
        "docs_only_runs_gates*",
        "gate_failure_*",
        "layer_not_reached_*",
        "gates_run_before_launcher",
        "markdown_config_*",
        "inherited_index_*",
    ]
    mode = {"mode": "iteration", "skipped": skipped, "note": "not commit evidence"} if selection_only else {"mode": "full"}
    report = {"started": out.name, **mode, "code": identity, "cases": results, "passed": len(results) - len(failed), "total": len(results), "failed": failed}
    (out / "report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    for result in results:
        sys.stdout.write(("PASS " if result["passed"] else "FAIL ") + result["case"] + "\n")
    sys.stdout.write(f"{report['passed']}/{report['total']} passed; report {out / 'report.json'}\n")
    return 0 if not failed else 1


if __name__ == "__main__":
    raise SystemExit(main())
