"""E2E for the check-group selection of scripts/run_ci.sh (scripts/quality/ci_groups.py and config/quality/ci_groups.json).

Run from the repository root: ``.venv/bin/python -m scripts.quality.run_ci_groups_e2e``. It adds a scratch Git worktree at HEAD,
copies the working check inputs into it, then checks which groups each scope selects: committed ranges
of known content, unusable ranges, staged changes (a deletion and a rename included), explicit paths, the full scope and
invalid input. Real runs of the CI entry check that the unconditional gates run in every part (also with an empty selection)
and before the launcher E2E, that a failing gate fails every part, that the full Markdown lint runs as a gate in every scope
and that the entry plan alone (``CI_LIST_ONLY=1``) runs nothing.
A static case requires the group functions in scripts/ci/groups/ to start the secret store E2E, the gold writer set-up mode and the scratch worktree E2E.
Body-file cases: a staged edit to each body file selects its group (and the layers downstream of it) and the repository group, never another group;
the pull request template selects none and the dispatcher, the workflow and the setup action select every group; planted drift for each rule of the guard
scripts/quality/ci_group_files.py fails with the file and group named. Dry-run cases run the dispatcher with a stand-in ``timed`` that only prints the
command: a missing or unreadable body file stops the run with its name, the full local run names the same groups, gold units and acquisition shards as
the GitHub jobs (a planted workflow with three shards differs), a failed shard stops the run before the combining step and a missing shard fails it.
One case runs this runner under a real pre-commit hook that inherits an alternate ``GIT_INDEX_FILE``, ``GIT_DIR`` and ``GIT_WORK_TREE`` and checks that
the caller's index, staged diff and working files survive. The report goes to
data/e2e/ci_groups/<UTC>_<pid>/report.json with the revision, the SHA-256 of the files under test and the tool versions;
the run fails when a file under test changes while it runs.

The reuse, shard and gate stage cases are the check group reuse_gate_checks (hosted job CI Reuse and Gate E2E): ``--reuse-only`` and ``--gate-stage-only``
run them, ``--selection-only`` (Local CI Core) leaves them out and a run without a mode runs all.

Hook-stage cases (selection-only mode runs them): the commit stage holds exactly the fast hooks, the pre-push stage holds ``local_ci_pushed`` (the shim check)
and ``acquisition-checks``, every hook of the manual stage that hosted CI runs is unchanged (pinned fingerprints) and every hook that left the commit stage is
in a hosted partition that the pre-push plan runs. Pre-push cases build small repositories with stand-ins for the commands: the plan selects the range with
the hosted environment and escalates to the full scope for selector, CI script and workflow paths, an unusable base runs the full scope, a failing step is
named, a step that outlives its deadline fails visibly, and the acquisition hook runs once over the computed range when the range reaches its path list (the
native hook of the push skips, the full scope runs it in its shards). Pushes through Git, the shim and pre-commit in scratch repositories cover a new branch,
a lightweight tag and an annotated tag in ``origin/main`` (recorded, nothing checked), an annotated tag and a force push at the checked-out commit (checked
once), a ref that pre-commit does not dispatch for, several refs, deletions, a tag of a tree, an up-to-date push, a tracked change and a missing, disabled or
changed shim; an inherited ``SKIP`` and the other pre-commit selection variables never reach the gate or a step. Planted defects (a heavy hook back in the
commit stage, a dropped privacy hook, a missing install type, an edited manual entry, a hook in no partition, an escalation gap) are caught by named cases.

The Docker release cases run the gate against a stand-in launcher (``orchestration/launcher.py`` in the scratch repository). A failing step, a raising step
runner and a passing gate each release once with ``--since`` the start of the run. A refused, failed, unreadable or too slow release never changes the exit
code and is recorded as ``docker_release`` in ``report.json``. ``scripts/run_ci.sh`` skips its own release under the gate, on hosted CI, off macOS, in a
list-only run and in the gates part (``release_docker`` runs on its own with a stand-in interpreter).
"""

from __future__ import annotations

import copy
import hashlib
import importlib
import json
import os
import re
import shutil
import sys
import tempfile
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest import mock

from scripts.process import clear_git_environment, find_program, run_command
from scripts.quality import ci_groups
from scripts.quality import pre_push as pre_push_module
from scripts.quality.repo_checks import load_yaml
from scripts.quality.scratch_worktree import ScratchWorktrees
from scripts.quality.source_reads import source_inputs

ROOT = Path(__file__).resolve().parents[2]
PYTHON = ROOT / ".venv" / "bin" / "python"
RUNNER = "scripts/quality/run_ci_groups_e2e.py"
GROUP_FILES = "scripts/ci/groups"
BODY_FILES = tuple(sorted(path.relative_to(ROOT).as_posix() for path in (ROOT / GROUP_FILES).glob("*.sh")))
COPIED = (
    ".env.example",
    ".github/workflows/ci.yml",
    ".markdownlint-cli2.jsonc",
    ".pre-commit-config.yaml",
    ".sqlfluff",
    ".yamllint",
    "README.md",
    "config/quality/ci_groups.json",
    "config/quality/launcher_inputs.json",
    "config/quality/feature_cases.json",
    "scripts/lakehouse/feature_shards.py",
    "scripts/lakehouse/run_feature_sources_e2e.py",
    "scripts/lakehouse/run_feature_columns_e2e.py",
    "scripts/quality/ci_hooks.py",
    "config/quality/docker_calls_reviewed.json",
    "config/quality/mypy_runtimes.json",
    "config/quality_tools.json",
    "dbt/models/silver/intermediate/_intermediate__models.yml",
    "docs/quality_checks.md",
    "scripts/acquisition/run_checks.sh",
    "scripts/acquisition/run_checks_pushed.sh",
    "scripts/quality/pre_push.py",
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
    "scripts/quality/ci_main_passes.py",
    "scripts/quality/launcher_inputs.py",
    "scripts/quality/source_reads.py",
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
    "scripts/quality/run_scratch_worktree_e2e.py",
    "scripts/acquisition/run_checks_ci_e2e.sh",
    "scripts/quality/run_documentation_review_e2e.py",
    "scripts/quality/typecheck.py",
    "scripts/run_ci.sh",
    *BODY_FILES,
    "scripts/quality/ci_group_files.py",
    "scripts/quality/timing.sh",
    "scripts/quality/ci_reuse.py",
    "config/quality/ci_jobs.json",
    "scripts/quality/run_ci_reuse_e2e.py",
    "scripts/quality/run_model_metadata_e2e.py",
    "scripts/acquisition/run_checks_shard.sh",
    "scripts/acquisition/run_checks_combine.sh",
)
LAUNCHER_E2E = "scripts/lakehouse/run_launcher_e2e.py"
DOCS_ONLY = "9c22c1e"
INFRA_ONLY = "063cbba"
DBT_AND_README = "114c3e0"
REQUIREMENTS = "7a967b2"
ZERO = "0" * 40
MISSING = "0123456789abcdef0123456789abcdef01234567"
MARKDOWN_PROBES = ("ci_groups_probe.MD", "ci_groups_probe_two.Markdown")
LAYERS = ["lakehouse_bronze", "lakehouse_silver", "lakehouse_gold", "lakehouse_features"]
CORE = ["python_lint", "orchestration_checks", "repository_code_checks", "configuration_checks", "sql_style", "terraform_checks"]
WITH_LAYERS = [CORE[0], *LAYERS, *CORE[1:]]
PART_GROUPS = {
    "core": CORE,
    "launcher": ["launcher"],
    "reuse_gate": ["reuse_gate_checks"],
    "bronze": LAYERS[:1],
    "silver": LAYERS[1:2],
    "gold": LAYERS[2:3],
    "features": LAYERS[3:],
    "feature_sources": LAYERS[3:],
    "feature_columns": LAYERS[3:],
    "feature_store": LAYERS[3:],
    "core+layers": WITH_LAYERS,
    "all": [*WITH_LAYERS, "launcher", "reuse_gate_checks", "acquisition", "repository_hooks"],
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
    "scripts/lakehouse/gold_maintenance.py": ["python_lint", *LAYERS[2:], "orchestration_checks", "repository_code_checks"],
    "scripts/lakehouse/run_lock.py": LAKEHOUSE_SCRIPT,
    "scripts/lakehouse/fake_docker.py": LAKEHOUSE_SCRIPT,
    "scripts/lakehouse/run_launcher_e2e.py": LAKEHOUSE_SCRIPT,
    "scripts/lakehouse/pins.py": LAKEHOUSE_SCRIPT,
    "scripts/lakehouse/provenance.py": LAKEHOUSE_SCRIPT,
    "scripts/lakehouse/memory_budget.py": LAKEHOUSE_SCRIPT,
    "scripts/lakehouse/dbt_package_archive.py": LAKEHOUSE_SCRIPT,
    "scripts/lakehouse/__init__.py": LAKEHOUSE_SCRIPT,
    "docker-compose.yaml": [*LAYERS, "orchestration_checks", "repository_code_checks"],
    "scripts/quality/typecheck.py": QUALITY_SCRIPT,
    "scripts/quality/docker_calls.py": QUALITY_SCRIPT,
    "scripts/quality/repo_checks.py": QUALITY_SCRIPT,
    "config/quality/docker_calls_reviewed.json": QUALITY_CONFIG,
    "scripts/process_group.py": ["python_lint", "orchestration_checks", "repository_code_checks"],
    "services/hai/bootstrap/supervisor.py": LAKEHOUSE_SCRIPT,
    "orchestration/worker_stderr.py": LAKEHOUSE_SCRIPT,
    "data_contracts/orchestration/examples/sdk_worker_cases/case_matrix.json": [*LAYERS, "orchestration_checks"],
    "data_contracts/orchestration/examples/sdk_worker_cases/mutation_cases.json": [*LAYERS, "orchestration_checks"],
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
    tracked = set(git(work, "ls-files").splitlines())
    new_files = [path for path in COPIED if (work / path).exists() and path not in tracked]
    if new_files:
        git(work, "add", "--intent-to-add", "--", *new_files)


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
    # The launcher is selected by every path (inventory_reads pattern "."), so a part that holds it starts the stand-in suite, never the real one.
    launcher_part = "launcher" in PART_GROUPS[part]
    marker = (install_launcher_group_stub(work) if "repository_code_checks" in PART_GROUPS[part] else install_launcher_stub(work)) if launcher_part else None
    try:
        code, log, text = run_entry(work, out, name, {"CI_SCOPE": "staged", "CI_PART": part})
    finally:
        restore(work)
    checks = {
        "exit_zero": code == 0,
        "reached_the_end": PASS_LINES[part] in log,
        "conditional_groups_skipped_with_reason": all(
            f"skipped {group}:" in log for group in PART_GROUPS[part] if group not in {"repository_code_checks", "launcher"}
        ),
        "launcher_selected_and_started": (marker is not None and "run launcher" in log and marker.exists()) if launcher_part else "run launcher" not in log,
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
    moved: str = next(iter(json.loads((ROOT / "config" / "data_paths.json").read_text(encoding="utf-8"))["moved"]))
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


def install_launcher_group_stub(work: Path) -> Path:
    """A launcher stub for a part that also runs repository_code_checks: it replaces the group body, because the process start guard there rejects an edited
    run_launcher_e2e.py, whose hash its reviewed entries record."""
    # The stub still names the module, because the launcher input check compares the body with its entry points.
    run = ": -m scripts.lakehouse.run_launcher_e2e >data/launcher_stub.marker"
    body = f"#!/usr/bin/env bash\n# shellcheck disable=SC2154,SC2329\nlauncher_e2e() {{\n  mkdir -p data\n  {run}\n}}\n"
    (work / GROUP_FILES / "launcher.sh").write_text(body, encoding="utf-8")
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
    wanted = {
        "core": "run sql_style",
        "bronze": "run lakehouse_bronze",
        "silver": "run lakehouse_silver",
        "gold": "run lakehouse_gold",
        "features": "run lakehouse_features",
        "feature_sources": "run lakehouse_features",
        "feature_columns": "run lakehouse_features",
        "feature_store": "run lakehouse_features",
    }.get(part, "run launcher")
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
        "bash (the group-file guard)": run_command("/bin/bash", ["--version"], timeout=60).stdout.splitlines()[0],
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
            ["-m", "pre_commit", "run", "acquisition-checks", "--hook-stage", "pre-push", "--verbose", "--files", path],
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
    """The lakehouse layers: a change selects its layer and every layer downstream; shared code selects every layer."""

    def path_case(name: str, path: str, groups: list[str]) -> dict[str, Any]:
        return selection_case(work, name, ["--scope", "paths", "--path", path, "--part", "core+layers"], groups)

    blob = git(work, "rev-parse", "HEAD:dbt/dbt_project.yml").strip()
    silver_downstream = LAYERS[1:]
    cases = [
        selection_case(work, "core_part_excludes_the_layers", ["--scope", "full", "--part", "core"], CORE, "full scope"),
        *(
            selection_case(work, f"{part}_part_is_its_layer_only", ["--scope", "full", "--part", part], groups, "full scope")
            for part, groups in (("bronze", LAYERS[:1]), ("silver", LAYERS[1:2]), ("gold", LAYERS[2:3]), ("features", LAYERS[3:]))
        ),
        path_case(
            "layer_bronze_only_change_selects_every_layer",
            "scripts/lakehouse/bronze.py",
            ["python_lint", *LAYERS, "orchestration_checks", "repository_code_checks"],
        ),
        path_case(
            "layer_silver_only_change_selects_silver_gold_and_features",
            "scripts/lakehouse/silver.py",
            ["python_lint", *silver_downstream, "orchestration_checks", "repository_code_checks"],
        ),
        path_case(
            "layer_gold_only_change_selects_gold_and_features",
            "scripts/lakehouse/gold_publish.py",
            ["python_lint", *LAYERS[2:], "orchestration_checks", "repository_code_checks"],
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
            "layer_gold_model_selects_gold_and_features",
            "dbt/models/gold/dimensions/_dimensions__models.yml",
            [*LAYERS[2:], "orchestration_checks", "sql_style"],
        ),
        path_case(
            "layer_silver_model_selects_silver_gold_and_features",
            "dbt/models/silver/staging/_staging__models.yml",
            [*silver_downstream, "orchestration_checks", "sql_style"],
        ),
        path_case("layer_gold_seed_selects_gold_and_features", "dbt/seeds/gold/identity_splits.csv", [*LAYERS[2:], "orchestration_checks", "sql_style"]),
        path_case(
            "layer_county_change_seed_selects_gold_and_features",
            "dbt/seeds/census_county_change_events.csv",
            [*LAYERS[2:], "orchestration_checks", "sql_style"],
        ),
        path_case(
            "layer_county_change_pin_selects_gold_and_features", "config/acquisition/census_county_change_golden.json", [*LAYERS[2:], "configuration_checks"]
        ),
        path_case(
            "layer_county_change_seed_check_selects_gold_and_features",
            "scripts/lakehouse/county_change_seed.py",
            ["python_lint", *LAYERS[2:], "orchestration_checks", "repository_code_checks"],
        ),
        *(
            path_case(
                f"layer_training_targets_{Path(path).stem}_selects_gold_and_features",
                path,
                ["python_lint", *LAYERS[2:], "repository_code_checks", "configuration_checks"],
            )
            for path in ("scripts/training/__init__.py", "scripts/training/targets.py", "scripts/training/run_targets_e2e.py")
        ),
        path_case("layer_training_targets_config_selects_gold_and_features", "config/training/targets.json", [*LAYERS[2:], "configuration_checks"]),
        path_case(
            "layer_training_targets_bridge_selects_gold_and_features",
            "dbt/seeds/gold/hai_outcome_measures_bridge.csv",
            [*LAYERS[2:], "orchestration_checks", "sql_style"],
        ),
        path_case("layer_county_label_seed_selects_every_layer", "dbt/seeds/alaska_county_code_labels.csv", [*LAYERS, "orchestration_checks", "sql_style"]),
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
            "layer_gold_dimension_no_layer_above_reads_selects_gold_and_features",
            "dbt/models/gold/dimensions/dim_county.sql",
            [*LAYERS[2:], "orchestration_checks", "sql_style"],
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
            "layer_staging_model_nothing_upstream_reads_selects_silver_gold_and_features",
            "dbt/models/silver/staging/stg_acs_dp02.sql",
            [*silver_downstream, "orchestration_checks", "sql_style"],
        ),
        path_case("layer_shared_macro_selects_every_layer", "dbt/macros/file_metadata.sql", [*LAYERS, "orchestration_checks", "sql_style"]),
        path_case("layer_gold_contract_is_read_by_bronze_first", "data_contracts/lakehouse/gold_contract.json", [*LAYERS, "orchestration_checks"]),
        path_case("layer_gold_suite_is_read_by_bronze_first", "data_contracts/great_expectations/gold/gold_keys.json", [*LAYERS, "orchestration_checks"]),
        path_case("layer_saved_silver_suite_selects_silver_gold_and_features", "features/great_expectations/suites/silver_spatial.json", silver_downstream),
        path_case("layer_suite_baselines_are_read_by_bronze_first", "data_contracts/great_expectations/baselines.json", [*LAYERS, "orchestration_checks"]),
        path_case(
            "layer_feature_model_is_read_by_gold_and_features",
            "dbt/models/features/int_spine_county_context.sql",
            [*LAYERS[2:], "orchestration_checks", "sql_style"],
        ),
        path_case(
            "layer_feature_recipe_seed_selects_gold_and_features",
            "dbt/seeds/features/mup_measures_recipe.csv",
            [*LAYERS[2:], "orchestration_checks", "sql_style"],
        ),
        path_case("layer_silver_columns_read_by_bronze_select_every_layer", "data_contracts/lakehouse/silver_columns.json", [*LAYERS, "orchestration_checks"]),
        path_case("layer_bronze_table_map_selects_every_layer", "config/lakehouse/bronze_tables.json", [*LAYERS, "configuration_checks"]),
        staged_case(
            work,
            "layer_staged_gold_model_selects_gold_and_features",
            [["update-index", "--add", "--cacheinfo", f"100644,{blob},dbt/models/gold/staged_copy.yml"]],
            [*LAYERS[2:], "orchestration_checks", "sql_style"],
        ),
        staged_case(
            work,
            "layer_staged_silver_model_selects_silver_gold_and_features",
            [["update-index", "--add", "--cacheinfo", f"100644,{blob},dbt/models/silver/staged_copy.yml"]],
            [*silver_downstream, "orchestration_checks", "sql_style"],
        ),
        path_case(
            "layer_feast_job_only_change_selects_features_only",
            "scripts/features/feast_job.py",
            ["python_lint", "lakehouse_features", "repository_code_checks"],
        ),
        path_case(
            "layer_feast_definitions_select_features_only", "feature_store/definitions.py", ["python_lint", "lakehouse_features", "repository_code_checks"]
        ),
        path_case("layer_feature_schema_selects_features_only", "data_contracts/features/feature_schema.json", ["lakehouse_features"]),
        path_case(
            "layer_features_publication_contract_read_by_bronze_selects_every_layer",
            "data_contracts/lakehouse/features_publish.json",
            [*LAYERS, "orchestration_checks"],
        ),
        path_case("layer_feast_image_folder_selects_features_only", "services/feature_store/requirements.in", ["lakehouse_features"]),
        path_case(
            "layer_generated_matrix_model_selects_gold_and_features",
            "dbt/models/features/matrix/feature_matrix_hospital.sql",
            [*LAYERS[2:], "orchestration_checks", "sql_style"],
        ),
    ]
    return [*cases, *reader_coverage_cases(work), *entry_point_cases(), *launcher_selection_cases(work), *launcher_always_cases(work)]


LAUNCHER_IMPORTED = (
    "scripts/lakehouse/silver.py",
    "orchestration/network_policy.py",
    "scripts/quality/docker_calls.py",
    "scripts/acquisition/new_probe.py",
    "dbt/models/gold/new_probe.sql",
    "scripts/quality/lint_sources.py",
    "scripts/quality/ci_hooks.py",
    "dags/gold_transform_dag.py",
    "tests/orchestration/fixtures/combined_dag.py",
    "scripts/quality/future_probe.py",
)
# Paths no import or read of the suite reaches. Followed modules that list files carry an ``inventory_reads`` entry with the pattern ``.``, so the group is
# selected by every path (owner decision 1): the cases expect it until a module's entry is narrowed by a reviewed change, one module at a time.
LAUNCHER_ALWAYS_SELECTED = (
    "infra/main.tf",
    "config/quality/mypy_runtimes.json",
    "docs/data_collection.md",
    ".github/pull_request_template.md",
)
# The reviewer's reproduction: an exact read added beside the reviewed mount comparison of ROOT / "config" in the suite's docker_calls_check case.
LAUNCHER_EXACT_READ = ("    from scripts.quality import docker_calls\n", '    json.loads((ROOT / "config/quality/mypy_runtimes.json").read_text())\n')
# The recheck's reproductions: inventories whose root the round 1 scan did not read (a wrapped root, a keyword argument, a listing program by path).
LAUNCHER_INVENTORY_READS = {
    "wrapped_rglob": '    for path in Path(ROOT).rglob("*.json"):\n        if path.is_file():\n            json.loads(path.read_text())\n',
    "walk_keyword": "    for _folder, _names, files in os.walk(top=ROOT):\n        json.dumps(files)\n",
    "find_by_path": '    run_command("/usr/bin/find", [str(ROOT), "-name", "*.json"], timeout=60, check=True)\n',
}
SAMPLE_REPOSITORY = {
    "entry.py": (
        "from pathlib import Path\nfrom pkg import used\n\nROOT = Path(__file__).resolve().parent\n\n\ndef run():\n    import late\n"
        '    data = (ROOT / "config/a.json").read_text()\n    compose = (ROOT / "compose.yaml").read_text()\n'
        '    files = sorted((ROOT / "folder").glob("*"))\n    return used, late, data, compose, files\n'
    ),
    "pkg/__init__.py": "",
    "pkg/used.py": "VALUE = 1\n",
    "late.py": "VALUE = 2\n",
    "config/a.json": "{}\n",
    "compose.yaml": "services:\n  worker:\n    entrypoint: [python, -m, svc.worker]\n",
    "svc/worker.py": "VALUE = 3\n",
    "folder/x.txt": "x\n",
    "folder/y.txt": "y\n",
    "unrelated.py": "VALUE = 4\n",
    "other/z.py": "VALUE = 5\n",
    "scripts/ci/groups/launcher.sh": 'launcher_e2e() {\n  "$python_bin" -m entry\n}\n',
}
SAMPLE_ENTRY = {"entry_points": ["entry.py"], "extra_inputs": []}
FOLDER_LINE = 'files = sorted((ROOT / "folder").glob("*"))'


def sample_cases(scratch: Path) -> list[dict[str, Any]]:
    """The tool on a small repository: the computed list has the imports, reads, module names in text files and whole-folder reads, and a planted defect
    against the committed list is named by ``problems``."""
    try:
        from scripts.quality import launcher_inputs
    except ImportError as error:
        return [{"case": "launcher_inputs_tool_exists", "passed": False, "error": str(error)}]
    root = scratch / "launcher_sample"
    for name, text in SAMPLE_REPOSITORY.items():
        (root / name).parent.mkdir(parents=True, exist_ok=True)
        (root / name).write_text(text, encoding="utf-8")
    tracked = set(SAMPLE_REPOSITORY)
    computed = launcher_inputs.compute(root, tracked, SAMPLE_ENTRY["entry_points"], SAMPLE_ENTRY["extra_inputs"])
    committed = {
        **SAMPLE_ENTRY,
        "files": computed["files"],
        "dynamic_sites": computed["dynamic_sites"],
        "folders": computed["folders"],
        "inventory_sites": computed["inventory_sites"],
        "inventory_reads": [{"module": "entry.py", "pattern": ".", "why": "sample: the glob of the folder it reads whole is not reviewed"}],
    }
    wanted = {"entry.py", "pkg/__init__.py", "pkg/used.py", "late.py", "config/a.json", "compose.yaml", "svc/worker.py", "folder/x.txt", "folder/y.txt"}
    cases = [
        {
            "case": "launcher_inputs_sample_closure",
            "passed": set(computed["files"]) == wanted,
            "missing": sorted(wanted - set(computed["files"])),
            "extra": sorted(set(computed["files"]) - wanted),
        },
        {"case": "launcher_inputs_sample_matches_itself", "passed": launcher_inputs.problems(root, tracked, committed) == []},
        {"case": "launcher_inputs_sample_folder_read_whole", "passed": computed["folders"] == ["folder"], "folders": computed["folders"]},
    ]

    def planted(name: str, change: Callable[[dict[str, Any]], dict[str, Any]], named: str, files: dict[str, str] | None = None) -> dict[str, Any]:
        changed_tracked = tracked | set(files or {})
        for path, text in (files or {}).items():
            (root / path).write_text(text, encoding="utf-8")
        try:
            found = launcher_inputs.problems(root, changed_tracked, change(copy.deepcopy(committed)))
        finally:
            for path in files or {}:
                (root / path).write_text(SAMPLE_REPOSITORY.get(path, ""), encoding="utf-8")
        return {"case": name, "passed": any(named in line for line in found), "named": named, "problems": found[:4]}

    def drop(path: str) -> Callable[[dict[str, Any]], dict[str, Any]]:
        return lambda listing: {**listing, "files": [item for item in listing["files"] if item != path]}

    def add(path: str) -> Callable[[dict[str, Any]], dict[str, Any]]:
        return lambda listing: {**listing, "files": sorted([*listing["files"], path])}

    cases += [
        planted("launcher_list_missing_imported_module_is_named", drop("late.py"), "late.py"),
        planted("launcher_list_missing_package_init_is_named", drop("pkg/__init__.py"), "pkg/__init__.py"),
        planted("launcher_list_missing_read_file_is_named", drop("config/a.json"), "config/a.json"),
        planted("launcher_list_missing_module_named_in_text_is_named", drop("svc/worker.py"), "svc/worker.py"),
        planted("launcher_list_missing_file_of_a_folder_read_is_named", drop("folder/y.txt"), "folder/y.txt"),
        planted("launcher_list_missing_folder_is_named", lambda listing: {**listing, "folders": []}, "folder/"),
        planted("launcher_list_stale_entry_is_named", add("unrelated.py"), "unrelated.py"),
        planted("launcher_list_deleted_entry_is_named", add("gone.py"), "gone.py"),
        planted(
            "launcher_new_import_is_named_until_regenerated",
            lambda listing: listing,
            "other/z.py",
            {"late.py": "import other.z\nVALUE = 2\n"},
        ),
        planted(
            "launcher_new_dynamic_import_is_named",
            lambda listing: listing,
            "late.py",
            {"late.py": "import importlib\n\n\ndef load(name):\n    return importlib.import_module(name)\n"},
        ),
        planted(
            "launcher_stale_ignored_read_is_named",
            lambda listing: {**listing, "ignored_reads": [{"module": "entry.py", "text": "nothing/here", "why": "planted"}]},
            "nothing/here",
        ),
        planted("launcher_entry_point_must_match_the_group_body", lambda listing: {**listing, "entry_points": ["unrelated.py"]}, "launcher.sh"),
        planted(
            "launcher_undeclared_inventory_read_is_named",
            lambda listing: listing,
            "late.py: lists files by inventory",
            {"late.py": 'LISTING = ["git", "ls-files", "--", "*.py"]\n'},
        ),
        planted(
            "launcher_undeclared_bare_listdir_is_named",
            lambda listing: listing,
            "late.py: lists files by inventory",
            {"late.py": 'from os import listdir\n\nNAMES = listdir(".")\n'},
        ),
        planted(
            "launcher_stale_inventory_read_is_named",
            lambda listing: {**listing, "inventory_reads": [{"module": "late.py", "pattern": "\\.py$", "why": "planted"}]},
            "late.py: inventory_reads",
        ),
        planted(
            "launcher_new_occurrence_of_an_ignored_text_is_named",
            lambda listing: {**listing, "ignored_reads": [{"module": "entry.py", "text": "folder", "why": "planted", "lines": [FOLDER_LINE]}]},
            "entry.py: ignored read 'folder'",
            {"entry.py": SAMPLE_REPOSITORY["entry.py"] + 'COPY = ROOT / "folder"\n'},
        ),
    ]
    ignoring = launcher_inputs.compute(root, tracked, SAMPLE_ENTRY["entry_points"], [], [{"module": "entry.py", "text": "folder", "why": "planted"}])
    (root / "entry.py").write_text(SAMPLE_REPOSITORY["entry.py"] + 'EXACT = (ROOT / "folder/x.txt").read_text()\n', encoding="utf-8")
    try:
        exact = launcher_inputs.compute(root, tracked, SAMPLE_ENTRY["entry_points"], [], [{"module": "entry.py", "text": "folder", "why": "planted"}])
    finally:
        (root / "entry.py").write_text(SAMPLE_REPOSITORY["entry.py"], encoding="utf-8")
    cases += [
        {
            "case": "launcher_trace_names_a_file_the_list_misses",
            "passed": launcher_inputs.trace_gaps(["other/z.py", "entry.py", "/outside/x"], tracked, computed["files"]) == ["other/z.py"],
        },
        {
            "case": "launcher_ignored_read_drops_its_folder",
            "passed": "folder/x.txt" not in ignoring["files"] and ignoring["folders"] == [] and ("entry.py", "folder") in ignoring["ignored"],
        },
        {
            "case": "launcher_ignored_read_keeps_an_exact_read_of_its_folder",
            "passed": "folder/x.txt" in exact["files"] and "folder/y.txt" not in exact["files"] and exact["folders"] == [],
            "files": [path for path in exact["files"] if path.startswith("folder/")],
            "folders": exact["folders"],
        },
    ]
    return cases


def overlay_problems(tracked: set[str], committed: dict[str, Any], line: str) -> list[str]:
    """The list check on the suite with ``line`` added after the anchor of LAUNCHER_EXACT_READ (in docker_calls_check): the source is changed only for this
    process's reads, never on disk."""
    from scripts.quality import launcher_inputs

    target = ROOT / LAUNCHER_E2E
    original = target.read_text(encoding="utf-8")
    anchor = LAUNCHER_EXACT_READ[0]
    if anchor not in original:
        return []
    changed = original.replace(anchor, anchor + line, 1)
    read_text = Path.read_text

    def overlay(path: Path, *args: Any, **kwargs: Any) -> str:
        return changed if path == target else read_text(path, *args, **kwargs)

    with mock.patch.object(Path, "read_text", overlay):
        return launcher_inputs.problems(ROOT, tracked, committed)


def launcher_always_cases(work: Path) -> list[dict[str, Any]]:
    """The always-select pattern (``.``) selects the launcher for every changed path and takes no part in classifying paths: it is not in a group list, so a
    path no list names still selects every group (LS27), an empty change list selects no launcher (LS28) and the configuration note states the
    exception (LS30)."""
    config = ci_groups.load()
    listed = [pattern for patterns in config["groups"].values() for pattern in patterns] + config["passive"]
    always = config.get("launcher_always", [])
    code, plan, error = select(work, "--scope", "paths", "--part", "all")
    note = json.loads((ROOT / ci_groups.CONFIG).read_text(encoding="utf-8"))["note"]
    stale = ("no longer selects it", "never another group")
    return [
        {
            "case": "launcher_always_pattern_is_not_a_classifier",
            "passed": bool(always) and not any(re.search(pattern, "newtop/tool.bin") for pattern in listed),
            "launcher_always": always,
            "matching": [pattern for pattern in listed if re.search(pattern, "newtop/tool.bin")][:4],
        },
        {
            "case": "empty_change_list_selects_no_launcher",
            "passed": code == 0 and plan is not None and "launcher" not in plan["run"],
            "run": plan["run"] if plan else None,
            "error": error[-400:],
        },
        {
            "case": "ci_groups_note_states_the_launcher_exception",
            "passed": "launcher_always" in note and "inventory_reads" in note and not any(phrase in note for phrase in stale),
            "stale_phrases": [phrase for phrase in stale if phrase in note],
        },
    ]


def launcher_selection_cases(work: Path) -> list[dict[str, Any]]:
    """The launcher group is selected by what its suite imports or reads, and by every path while a followed module that lists files carries the ``.`` pattern
    of its ``inventory_reads`` entry (owner decision 1: every such module always selects the suite until a reviewed entry narrows it). A change to a module
    the suite imports selects it, so does a path the suite never reaches, the committed list equals the computed one, and every computed file is selected by
    the group's patterns."""
    cases: list[dict[str, Any]] = [
        selection_case(work, f"launcher_selected_by_{Path(path).stem}", ["--scope", "paths", "--path", path, "--part", "launcher"], ["launcher"])
        for path in LAUNCHER_IMPORTED
    ]
    cases += [
        selection_case(work, f"launcher_always_selected_by_{Path(path).stem}", ["--scope", "paths", "--path", path, "--part", "launcher"], ["launcher"])
        for path in LAUNCHER_ALWAYS_SELECTED
    ]
    try:
        from scripts.quality import launcher_inputs
    except ImportError as error:
        return [*cases, {"case": "launcher_inputs_tool_exists", "passed": False, "error": str(error)}]
    tracked = set(git(ROOT, "ls-files").splitlines())
    committed = json.loads((ROOT / launcher_inputs.LIST).read_text(encoding="utf-8"))
    config = ci_groups.load()
    gaps = launcher_inputs.selection_gaps(config, committed["files"], tracked)
    exact = overlay_problems(tracked, committed, LAUNCHER_EXACT_READ[1])
    inventories = {name: overlay_problems(tracked, committed, line) for name, line in LAUNCHER_INVENTORY_READS.items()}
    with tempfile.TemporaryDirectory(prefix="launcher_sample.") as folder:
        sample = sample_cases(Path(folder))
    return [
        *cases,
        {
            "case": "launcher_list_equals_computed",
            "passed": launcher_inputs.problems(ROOT, tracked, committed) == [],
            "problems": launcher_inputs.problems(ROOT, tracked, committed)[:6],
        },
        {
            "case": "launcher_list_names_an_exact_read_beside_an_ignored_folder",
            "passed": any(line.startswith("config/quality/mypy_runtimes.json: the launcher suite needs it") for line in exact),
            "problems": exact[:4],
        },
        *(
            {
                "case": f"launcher_list_names_an_inventory_by_{name}",
                "passed": any(line.startswith(f"{LAUNCHER_E2E}: ") and "inventory sites" in line for line in found),
                "problems": found[:4],
            }
            for name, found in inventories.items()
        ),
        {
            "case": "launcher_list_is_selected_by_the_group",
            "passed": not gaps and len(committed["files"]) > 0,
            "not_selected": gaps[:6],
            "files": len(committed["files"]),
        },
        *sample,
    ]


def module_source(module: str) -> str:
    return (ROOT / f"{module.replace('.', '/')}.py").read_text(encoding="utf-8")


def reader_inputs(modules: tuple[str, ...], tracked: set[str]) -> dict[str, str]:
    """Every tracked file that ``modules`` name anywhere in their source, with the module and the text that names it."""
    found: dict[str, str] = {}
    for name in modules:
        found |= {path: f"{name}: {text}" for path, text in source_inputs(module_source(name), tracked).items()}
    return found


def layer_modules(function: str) -> tuple[str, ...]:
    """The modules the check-group function ``function`` (in its body file of scripts/ci/groups/) starts with ``-m``; a group split into units
    (``lakehouse_gold_checks``) also counts the functions of its units."""
    text = ci_script()
    names = [name for name in re.findall(r"^([a-z_]+)\(\) \{$", text, re.MULTILINE) if name == function or name.startswith(f"{function}_")]
    bodies = [body for name in names if (body := function_text(text, name))]
    return tuple(module for body in bodies for module in re.findall(r"-m (scripts\.[\w.]+)", body))


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


HOSTED_ENTRY_POINTS = {
    "lakehouse_features_store": (
        ("scripts.features.feature_schema check", ("scripts/features/feature_schema.py", "data_contracts/features/feature_schema_rules.json")),
        ("scripts.features.run_feature_store_e2e --mode static", ("scripts/features/run_feature_store_e2e.py", "scripts/features/feature_schema.py")),
        ("scripts.features.feast_cases --part local", ("scripts/features/feast_cases.py", "scripts/features/feast_job.py", "feature_store/feature_store.yaml")),
        ("scripts.features.online_cases --part fake", ("scripts/features/online_cases.py", "scripts/features/online_protocol.py")),
        ("scripts.orchestration.run_features_pipeline_light", ("scripts/orchestration/run_features_pipeline_light.py",)),
    ),
    "repository_code_checks": (
        (
            "scripts.quality.run_scratch_worktree_e2e",
            ("scripts/quality/run_scratch_worktree_e2e.py", "scripts/quality/scratch_worktree.py", "scripts/acquisition/run_checks_ci_e2e.sh"),
        ),
    ),
    "configuration_checks": (
        ("scripts.infrastructure.run_secret_store_e2e", ("scripts/infrastructure/run_secret_store_e2e.py", "scripts/infrastructure/secret_store.py")),
        ("scripts.infrastructure.run_host_session_e2e", ("scripts/infrastructure/run_host_session_e2e.py", "scripts/infrastructure/host_session.py")),
        ("scripts.infrastructure.host_session_plants", ("scripts/infrastructure/host_session_plants.py", "scripts/infrastructure/host_session.py")),
        ("scripts.infrastructure.run_aws_tasks_e2e", ("scripts/infrastructure/run_aws_tasks_e2e.py", "scripts/infrastructure/aws_tasks.py")),
        ("scripts.infrastructure.aws_tasks_plants", ("scripts/infrastructure/aws_tasks_plants.py", "scripts/infrastructure/aws_tasks.py")),
        (
            'scripts.infrastructure.run_iam_preflight_e2e --output-directory "$quality_output/iam_preflight"',
            (
                "scripts/infrastructure/run_iam_preflight_e2e.py",
                "scripts/infrastructure/iam_preflight.py",
                "scripts/infrastructure/iam_preflight_policies.py",
                "scripts/infrastructure/iam_preflight_stand_in.py",
            ),
        ),
    ),
    "lakehouse_gold_writer_setup": (
        (
            "scripts.lakehouse.run_gold_publish_e2e --mode writer_setup",
            ("scripts/lakehouse/run_gold_publish_e2e.py", "scripts/lakehouse/gold_writer_setup.py", "scripts/lakehouse/principal_credentials.py"),
        ),
    ),
}


def entry_point_cases() -> list[dict[str, Any]]:
    """The offline entry points that only a check-group function runs: each is started by its group function in its body file of scripts/ci/groups/ and
    a change to its sources selects that group, so a hosted run cannot pass without it."""
    text = ci_script()
    missing: list[str] = []
    not_selecting: list[str] = []
    for function, entries in HOSTED_ENTRY_POINTS.items():
        body = re.search(rf"^{function}\(\) \{{\n(.*?)^\}}", text, re.DOTALL | re.MULTILINE)
        for command, sources in entries:
            if not body or not re.search(rf"^\s*timed \"[^\"]+\" \"\$python_bin\" -m {re.escape(command)}$", body.group(1), re.MULTILINE):
                missing.append(f"{function}: {command}")
            group = next((name for name in ("lakehouse_gold", "lakehouse_features") if function.startswith(name)), function)
            not_selecting += [path for path in sources if group not in ci_groups.plan("paths", "all", paths=[path])["run"]]
    return [
        {
            "case": "offline_entry_points_run_in_their_groups",
            "passed": not missing and not not_selecting,
            "not_run_by_the_group": missing,
            "sources_not_selecting_the_group": not_selecting,
        }
    ]


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
        "lakehouse_features": (*layer_modules("lakehouse_features"), "scripts.lakehouse.feature_allowlist"),
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
            "passed": not missed and all(counted[group] for group in ("lakehouse_silver", "lakehouse_gold", "lakehouse_features")),
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
            "unclassified_path_runs_all_in_part_all",
            ["--scope", "paths", "--path", "newtop/tool.bin", "--part", "all"],
            [*WITH_LAYERS, "launcher", "reuse_gate_checks"],
            "unclassified",
        ),
        selection_case(
            work,
            "launcher_path",
            ["--scope", "paths", "--path", "orchestration/launcher.py", "--part", "all"],
            ["python_lint", *LAYERS, "orchestration_checks", "repository_code_checks", "configuration_checks", "launcher"],
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
            ["python_lint", *LAYERS, "orchestration_checks", "repository_code_checks"],
        ),
        selection_case(
            work,
            "type_manifest_selects_types",
            ["--scope", "paths", "--path", "config/quality/mypy_runtimes.json", "--part", "core+layers"],
            ["python_lint", *LAYERS, "orchestration_checks", "repository_code_checks", "configuration_checks"],
        ),
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
                    [*LAYERS[1:], "orchestration_checks", "repository_code_checks", "sql_style"],
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
            # The bronze provenance runner seals every dbt macro, so a feature macro also runs bronze and the layers below it.
            [*LAYERS, "orchestration_checks", "sql_style"],
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
    cases += body_file_cases(work)
    cases += guard_cases()
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
    for part in ("all", "core", "launcher", "bronze", "silver", "gold", "features", "feature_sources", "feature_columns", "feature_store"):
        cases.append(list_only_case(work, out, part))
    for part in ("core", "all", "launcher", "gold", "features", "feature_sources", "feature_columns"):
        cases.append(docs_only_case(work, out, part))
    for part in ("core", "all", "launcher", "silver", "features", "feature_sources", "feature_columns"):
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
    cases += speed_real_cases(work, out)
    cases += inherited_context_cases(work, out, scratch)
    return cases


GOLD_UNIT_FUNCTIONS = ("lakehouse_gold_checks", "lakehouse_gold_writer_setup", "lakehouse_gold_model_metadata_e2e")
GOLD_UNITS = ("checks", "writer_setup", "model_metadata_e2e")
FEATURE_COMMANDS = (
    "scripts.lakehouse.feature_sources --check",
    "scripts.lakehouse.run_feature_sources_e2e",
    "scripts.lakehouse.feature_columns --check",
    "scripts.lakehouse.run_feature_columns_e2e",
)
OLD_GOLD_COMMANDS = (
    "scripts.lakehouse.bridge_references --check",
    "scripts.lakehouse.run_bridge_references_e2e",
    "scripts.lakehouse.identity_splits --check",
    "scripts.lakehouse.source_labels --check",
    "scripts.lakehouse.county_change_seed --check",
    "scripts.lakehouse.run_identity_seeds_e2e",
    'scripts.lakehouse.run_county_change_seed_e2e --output "$quality_output/county_change_seed.json"',
    "scripts.training.targets --check",
    "scripts.training.run_targets_e2e",
    "scripts.lakehouse.feature_sources --check",
    "scripts.lakehouse.run_feature_sources_e2e",
    "scripts.lakehouse.feature_columns --check",
    "scripts.lakehouse.run_feature_columns_e2e",
    "scripts.lakehouse.gold_checks check-suites",
    "scripts.lakehouse.gold_checks check-models",
    "scripts.lakehouse.run_gold_publish_e2e --mode contract",
    'scripts.lakehouse.run_catalog_registration_e2e --out "$quality_output/catalog_registration" --mutations',
    "scripts.lakehouse.run_gold_publish_e2e --mode writer_setup",
    "scripts.quality.model_metadata",
    "scripts.quality.run_model_metadata_e2e",
)
TIMING = "scripts/quality/timing.sh"
ACQUISITION_PYTEST_PATHS = "scripts/acquisition/tests tests/test_source_registry.py"
DURATIONS_HEADING = "## Measured durations"


def workflow_document() -> dict[str, Any]:
    loaded = load_yaml((ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        raise ValueError(".github/workflows/ci.yml is not a mapping")
    return loaded


def ci_script() -> str:
    """The dispatcher and every body file as one text, so a function is found wherever it is defined."""
    return "\n".join((ROOT / path).read_text(encoding="utf-8") for path in ("scripts/run_ci.sh", *BODY_FILES))


def function_text(text: str, name: str) -> str | None:
    found = re.search(rf"^{name}\(\) \{{\n(.*?)^\}}", text, re.DOTALL | re.MULTILINE)
    return found.group(1) if found else None


def timed_commands(body: str) -> list[str]:
    """The commands that a function starts through ``timed``, without the label, the python launcher and the worker setting."""
    commands: list[str] = []
    for line in re.findall(r'^\s*timed "[^"]+" (.+)$', re.sub(r"\\\n\s*", " ", body), re.MULTILINE):
        command = re.sub(r'^"\$python_bin" -m ', "", line)
        command = re.sub(r' --shard "\$shard"\s+--output-directory "[^"]+"', "", command)
        commands.append(re.sub(r' --workers "\$\{MODEL_METADATA_WORKERS:-auto\}"', "", command).strip())
    return commands


def single_command_cases() -> list[dict[str, Any]]:
    """Every function that the dispatcher or a body file wraps in ``timed`` holds one command or one pipeline (several would run with set -e ignored)."""
    text = ci_script()
    wrapped = set(re.findall(r'^\s*timed "[^"]+" ([a-z_]+)$', text, re.MULTILINE))
    wrapped &= set(re.findall(r"^([a-z_]+)\(\) \{", text, re.MULTILINE))
    several: dict[str, int] = {}
    for name in sorted(wrapped):
        one_liner = re.search(rf"^{name}\(\) \{{ (.*) \}}$", text, re.MULTILINE)
        body = one_liner.group(1) if one_liner else function_text(text, name) or ""
        statements = [line for line in re.sub(r"\\\n\s*", " ", body).splitlines() if line.strip() and not line.strip().startswith("#")]
        parts = [part for line in statements for part in re.split(r"\s*(?:;|&&|\|\|)\s*", line) if part.strip()]
        if len(statements) != 1 or len(parts) != 1:
            several[name] = max(len(statements), len(parts))
    return [
        {
            "case": "timed_calls_wrap_single_commands",
            "passed": not several and len(wrapped) >= 6,
            "wrapped_functions": sorted(wrapped),
            "holding_several": several,
        }
    ]


def timing_function_cases(scratch: Path) -> list[dict[str, Any]]:
    """The wrapper of scripts/quality/timing.sh, run by a real bash: a failing command keeps its exit code and its row, a bad label is refused, the
    lines print without a summary file and the rows of several files are listed together."""
    rows = scratch / "timing_rows.tsv"
    other = scratch / "timing_rows_two.tsv"
    other.write_text("second unit\t2026-10-22T10:00:00Z\t2026-10-22T10:00:05Z\t5\t0\n", encoding="utf-8")
    environment = {
        **os.environ,
        "TIMING_SH": str(ROOT / TIMING),
        "TIMING_ROWS": str(rows),
        "TIMING_OTHER": str(other),
        "TIMING_ABSENT": str(scratch / "timing_absent.tsv"),
    }
    failing = run_command(
        "bash",
        [
            "-c",
            'source "$TIMING_SH"; CI_TIMING_FILE="$TIMING_ROWS"; timed "a failing command" bash -c "exit 3"; echo "status=$?"; timed "a passing command" true',
        ],
        env=environment,
        timeout=60,
    )
    text = rows.read_text(encoding="utf-8") if rows.exists() else ""
    refused = run_command("bash", ["-c", 'source "$TIMING_SH"; timed "bad;label" true; echo "status=$?"'], env=environment, timeout=60)
    bare = run_command("bash", ["-c", 'source "$TIMING_SH"; unset CI_TIMING_FILE; timed "no file" true; echo "status=$?"'], env=environment, timeout=60)
    table_run = run_command("bash", ["-c", 'source "$TIMING_SH"; timing_table "$TIMING_ROWS" "$TIMING_OTHER" "$TIMING_ABSENT"'], env=environment, timeout=60)
    table = table_run.stdout
    return [
        {
            "case": "timing_failed_command_keeps_exit_code",
            "passed": "status=3" in failing.stdout
            and re.search(r"\[timing\] start a failing command at \d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ", failing.stdout) is not None
            and re.search(r"\[timing\] end a failing command at \S+: \d+ s, exit 3", failing.stdout) is not None
            and re.search(r"^a failing command\t\S+\t\S+\t\d+\t3$", text, re.MULTILINE) is not None
            and re.search(r"^a passing command\t\S+\t\S+\t\d+\t0$", text, re.MULTILINE) is not None,
            "stdout": failing.stdout[-300:],
        },
        {
            "case": "timing_label_is_validated",
            "passed": "status=2" in refused.stdout and "holds a character outside" in refused.stderr and "[timing] start" not in refused.stdout,
            "stderr": refused.stderr[-200:],
        },
        {
            "case": "timing_lines_print_without_a_summary_file",
            "passed": "[timing] end no file" in bare.stdout and "status=0" in bare.stdout,
            "stdout": bare.stdout[-200:],
        },
        {
            "case": "timing_rows_of_several_files_are_listed_in_order",
            "passed": [
                line.split(" | ")[0].lstrip("| ") for line in table.splitlines() if line.startswith("| ") and "---" not in line and "Command" not in line
            ]
            == ["a failing command", "a passing command", "second unit"],
            "table": table[-500:],
        },
        {
            "case": "timing_clamps_negative_durations",
            "passed": re.search(r"if \(\(seconds < 0\)\); then\n\s+seconds=0", (ROOT / TIMING).read_text(encoding="utf-8")) is not None,
        },
    ]


def gold_unit_cases() -> list[dict[str, Any]]:
    """The gold layer's units hold every gold command once, start no launcher mode, share no report folder or fixed port and are covered by the jobs."""
    text = ci_script()
    bodies = {name: function_text(text, name) for name in GOLD_UNIT_FUNCTIONS}
    found = [command for body in bodies.values() for command in timed_commands(body or "")]
    modes = re.findall(r"run_gold_publish_e2e --mode (\w+)", " ".join(found))
    folders = {
        "scripts/lakehouse/run_bridge_references_e2e.py": "data/e2e/bridge_references",
        "scripts/lakehouse/run_identity_seeds_e2e.py": "data/e2e/gold_identity_seeds",
        "scripts/training/run_targets_e2e.py": "data/e2e/training_targets",
        "scripts/lakehouse/run_feature_sources_e2e.py": "data/e2e/feature_sources",
        "scripts/lakehouse/run_feature_columns_e2e.py": "data/e2e/feature_columns",
        "scripts/quality/run_model_metadata_e2e.py": "data/e2e/model_metadata",
        "scripts/lakehouse/run_gold_publish_e2e.py": "data/e2e/gold_publish",
    }
    publish = (ROOT / "scripts/lakehouse/run_gold_publish_e2e.py").read_text(encoding="utf-8")
    missing_folders = [
        path
        for path, folder in folders.items()
        if folder not in (ROOT / path).read_text(encoding="utf-8") and not (path.endswith("gold_publish_e2e.py") and "e2e/gold_publish" in publish)
    ]
    stand_in = (ROOT / "scripts/infrastructure/secret_store_stand_in.py").read_text(encoding="utf-8")
    ports = [
        path
        for path in (
            "scripts/lakehouse/run_gold_publish_e2e.py",
            "scripts/lakehouse/run_bridge_references_e2e.py",
            "scripts/lakehouse/run_identity_seeds_e2e.py",
            "scripts/quality/run_model_metadata_e2e.py",
        )
        if re.search(r"\b(?:port|PORT)\s*=\s*\d{2,5}\b|:\d{4,5}['\"/]", (ROOT / path).read_text(encoding="utf-8"))
    ]
    workflow = workflow_document()
    gold_jobs = [entry for entry in workflow["jobs"]["lakehouse"]["strategy"]["matrix"]["include"] if entry["part"] == "gold"]
    metadata_units = workflow["jobs"]["gold_metadata"]["steps"][-2]["env"]["CI_UNITS"]
    covered = [*gold_jobs[0]["units"].split(), *metadata_units.split()]
    unit_line = re.search(r'^gold_units="([^"]+)"$', text, re.MULTILINE)
    return [
        {
            "case": "gold_units_hold_every_gold_command_once",
            "passed": sorted(found) == sorted(command for command in OLD_GOLD_COMMANDS if command not in FEATURE_COMMANDS)
            and all(body is not None for body in bodies.values()),
            "missing": sorted(set(OLD_GOLD_COMMANDS) - set(FEATURE_COMMANDS) - set(found)),
            "extra_or_repeated": sorted(
                command for command in found if found.count(command) > 1 or command not in OLD_GOLD_COMMANDS or command in FEATURE_COMMANDS
            ),
        },
        {"case": "gold_units_start_no_launcher_modes", "passed": sorted(modes) == ["contract", "writer_setup"], "modes": modes},
        {
            "case": "gold_and_feature_checks_write_distinct_report_folders",
            "passed": not missing_folders
            and "f\"{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}-{args.mode}\"" in publish
            and len(set(folders.values())) == len(folders),
            "missing": missing_folders,
        },
        {"case": "gold_units_open_no_fixed_ports", "passed": 'StandInServer(("127.0.0.1", 0)' in stand_in and not ports, "files_with_a_port": ports},
        {
            "case": "gold_jobs_cover_every_unit_once",
            "passed": sorted(covered) == sorted(GOLD_UNITS) and unit_line is not None and unit_line.group(1).split() == list(GOLD_UNITS),
            "covered": covered,
        },
    ]


def feature_split_cases() -> list[dict[str, Any]]:
    text = ci_script()
    feature_commands = [command for suite in ("sources", "columns") for command in timed_commands(function_text(text, f"lakehouse_features_{suite}") or "")]
    gold_commands = [command for name in GOLD_UNIT_FUNCTIONS for command in timed_commands(function_text(text, name) or "")]
    sources = (
        "scripts/lakehouse/feature_allowlist.py",
        "scripts/lakehouse/feature_sources.py",
        "scripts/lakehouse/run_feature_sources_e2e.py",
        "scripts/lakehouse/feature_columns.py",
        "scripts/lakehouse/run_feature_columns_e2e.py",
    )
    cases = [
        {
            "case": "feature_group_holds_every_feature_command_once",
            "passed": sorted(feature_commands) == sorted(FEATURE_COMMANDS),
            "found": feature_commands,
            "expected": list(FEATURE_COMMANDS),
        },
        {
            "case": "gold_and_feature_groups_preserve_every_original_command_once",
            "passed": sorted([*gold_commands, *feature_commands]) == sorted(OLD_GOLD_COMMANDS),
            "found": [*gold_commands, *feature_commands],
        },
    ]
    for scope, path in [("full", ""), *(("paths", path) for path in sources)]:
        arguments = ["--scope", scope, "--part", "features", *(["--path", path] if path else [])]
        code, plan, error = select(ROOT, *arguments)
        cases.append(
            {
                "case": f"feature_part_selects_its_group_{Path(path).stem if path else 'full'}",
                "passed": code == 0 and plan is not None and plan["run"] == ["lakehouse_features"],
                "exit": code,
                "plan": plan,
                "error": error,
            }
        )
    plan_outputs = workflow_document()["jobs"]["plan"].get("outputs", {})
    for suite in ("sources", "columns"):
        definition = workflow_document()["jobs"][f"feature_{suite}"]
        steps = [step for step in definition["steps"] if step.get("run") == "bash scripts/run_ci.sh"]
        cases.append(
            {
                "case": f"feature_{suite}_job_uses_range_selection_and_the_plan_reuse_condition",
                "passed": definition.get("needs") == "plan"
                and definition.get("if") == "${{ needs.plan.outputs.reuse != 'true' }}"
                and len(steps) == 1
                and steps[0].get("env", {}).get("CI_PART") == f"feature_{suite}"
                and steps[0].get("env", {}).get("CI_SCOPE") == "range"
                and steps[0].get("env", {}).get("RANGE_START") == "${{ needs.plan.outputs.range_start }}"
                and steps[0].get("env", {}).get("RANGE_END") == "${{ needs.plan.outputs.range_end }}"
                and plan_outputs.get("range_start") == "${{ steps.range.outputs.start }}"
                and plan_outputs.get("range_end") == "${{ steps.range.outputs.end }}",
            }
        )
        for path in sources:
            cases.append(
                selection_case(
                    ROOT, f"{suite}_part_selects_{Path(path).stem}", ["--scope", "paths", "--part", f"feature_{suite}", "--path", path], ["lakehouse_features"]
                )
            )
    return cases


def acquisition_script_cases() -> list[dict[str, Any]]:
    """The acquisition scripts use the timing wrapper, the unsharded run stays one run with the 90% gate, and the combining job keeps the checks that
    the single run has beside the merged gate."""
    names = ("run_checks.sh", "run_checks_shard.sh", "run_checks_combine.sh")
    texts = {name: (ROOT / "scripts/acquisition" / name).read_text(encoding="utf-8") for name in names}
    single = texts["run_checks.sh"]
    combine = texts["run_checks_combine.sh"]
    shard = texts["run_checks_shard.sh"]

    def commands(text: str) -> set[str]:
        return set(re.findall(r'timed "acquisition(?: combine)?: [^"]+" (.+?)(?: \\)?$', text, re.MULTILINE))

    prefixes = ('"$python_bin" -m ruff ', '"$python_bin" -m scripts.quality.typecheck ', '"$python_bin" -m scripts.acquisition.source_registry')
    shared = {command for command in commands(single) if command.startswith(prefixes)}
    return [
        {
            "case": "acquisition_scripts_use_the_timing_wrapper",
            "passed": all('source "$repo_root/scripts/quality/timing.sh"' in text and "timed " in text and "timing_entry" in text for text in texts.values()),
        },
        {
            "case": "local_acquisition_run_stays_unsharded",
            "passed": "--shard" not in single.replace("ACQUISITION_SHARD", "")
            and "--cov-fail-under=90" in single
            and "--cov-report=term-missing" in single
            and ACQUISITION_PYTEST_PATHS in single,
        },
        {
            "case": "combine_job_runs_lint_types_and_registry",
            "passed": len(shared) == 3
            and shared <= commands(combine)
            and "shards gate" in combine
            and "--fail-under 90" in combine
            and ACQUISITION_PYTEST_PATHS in combine,
            "single_run_checks": sorted(shared),
        },
        {
            "case": "shard_runs_collect_without_a_target_and_seal",
            "passed": "coverage/shard${index}.coverage" in shard
            and "--cov-fail-under=0" in shard
            and "--cov-report= " in shard
            and "shards seal" in shard
            and ACQUISITION_PYTEST_PATHS in shard
            and "-p scripts.acquisition.shards" in shard,
        },
    ]


def table_rows(text: str, heading: str) -> list[list[str]]:
    section = text.split(heading, 1)[1].split("\n## ", 1)[0] if heading in text else ""
    rows = [[cell.strip() for cell in line.strip().strip("|").split("|")] for line in section.splitlines() if line.startswith("|")]
    return [row for row in rows[2:] if len(row) >= 3]


def fill_matrix(template: str, entry: dict[str, Any]) -> str:
    return re.sub(r"\$\{\{\s*matrix\.(\w+)\s*\}\}", lambda found: str(entry.get(found.group(1), "")), template)


def workflow_timeouts() -> dict[str, int]:
    """The timeout in minutes of each job of the workflow by its expanded name."""
    jobs = workflow_document()["jobs"]
    timeouts: dict[str, int] = {}
    for name in jobs:
        definition = jobs[name]
        for entry in matrix_entries(definition):
            label = fill_matrix(str(definition["name"]), entry)
            minutes = int(fill_matrix(str(definition["timeout-minutes"]), entry))
            timeouts[label] = minutes
    return timeouts


def matrix_entries(definition: dict[str, Any]) -> list[dict[str, Any]]:
    """The combinations of a job's matrix: its ``include`` list, or one entry for each value of its list-valued key; one empty entry without a matrix."""
    matrix = definition.get("strategy", {}).get("matrix")
    if not matrix:
        return [{}]
    if "include" in matrix:
        return list(matrix["include"])
    entries: list[dict[str, Any]] = []
    for key in matrix:
        for choice in matrix[key]:
            entries.append({key: choice})
    return entries


def timeout_cases() -> list[dict[str, Any]]:
    """Each job's timeout equals the one in the table of measured durations and is between 1.5 and 2.6 times the measured time (at least 5 minutes)."""
    rows = table_rows((ROOT / "docs/quality_checks.md").read_text(encoding="utf-8"), DURATIONS_HEADING)
    recorded = {row[0]: (float(row[1]), int(row[2])) for row in rows}
    timeouts = workflow_timeouts()
    off = {name: (recorded[name], timeouts.get(name)) for name in recorded if timeouts.get(name) != recorded[name][1]}
    ratio = {name: round(timeout / measured, 2) for name, (measured, timeout) in recorded.items() if not (1.5 * measured <= timeout <= max(2.6 * measured, 5))}
    return [
        {
            "case": "job_timeouts_match_the_measured_durations",
            "passed": set(recorded) == set(timeouts) and not off and not ratio and len(recorded) >= 10,
            "not_in_the_table": sorted(set(timeouts) - set(recorded)),
            "not_in_the_workflow": sorted(set(recorded) - set(timeouts)),
            "different_timeout": off,
            "outside_the_ratio": ratio,
        }
    ]


def unit_refusal_case(work: Path, out: Path) -> dict[str, Any]:
    code, log, _ = run_entry(work, out, "unknown_unit", {"CI_UNITS": "bogus", "CI_SCOPE": "full", "CI_PART": "gold", "CI_LIST_ONLY": "1"})
    return {
        "case": "unknown_unit_is_refused",
        "passed": code == 2 and "CI_UNITS must name units of the gold layer" in log and "run lakehouse_gold" not in log,
        "exit": code,
    }


def feature_combination_cases(scratch: Path) -> list[dict[str, Any]]:
    """Use synthetic case outcomes to exercise the real combiner's complete membership and candidate checks."""
    results = []
    for suite, count in (("sources", 8), ("columns", 4)):
        plan = run_command(str(PYTHON), ["-m", "scripts.lakehouse.feature_shards", "plan", "--suite", suite, "--count", str(count)], cwd=ROOT, timeout=60)
        if plan.returncode != 0:
            results.append({"case": f"feature_{suite}_combine_plan", "passed": False, "error": plan.stderr[-400:]})
            continue
        manifest = json.loads(plan.stdout)
        folder = scratch / f"feature_{suite}_reports"
        reports = {}
        for index in range(1, count + 1):
            shard = f"{index}/{count}"
            names = manifest["shards"][shard]
            reports[index] = {
                "suite": suite,
                "shard": shard,
                "case_inventory": manifest["case_inventory"],
                "selected_cases": names,
                "source_hashes": manifest["source_hashes"],
                "problems": [],
                "status": "pass",
                "cases": [{"case": name, "ok": True} for name in names],
            }
        mutations = ("control", "dropped_shard", "duplicate_case", "missing_case", "failed_shard", "other_candidate")
        for mutation in mutations:
            for index, report in reports.items():
                path = folder / f"shard_{index}" / "report.json"
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(json.dumps(report))
            target = folder / "shard_2/report.json"
            damaged = copy.deepcopy(reports[2])
            if mutation == "dropped_shard":
                target.unlink()
            elif mutation == "duplicate_case":
                damaged["cases"].append(copy.deepcopy(damaged["cases"][0]))
            elif mutation == "missing_case":
                damaged["cases"].pop()
            elif mutation == "failed_shard":
                damaged["status"] = "fail"
            elif mutation == "other_candidate":
                damaged["source_hashes"] = {}
            if mutation != "dropped_shard":
                target.write_text(json.dumps(damaged))
            run = run_command(
                str(PYTHON),
                ["-m", "scripts.lakehouse.feature_shards", "combine", "--suite", suite, "--count", str(count), "--directory", str(folder)],
                cwd=ROOT,
                timeout=60,
            )
            combined = json.loads((folder / "combined.json").read_text())
            intended = {
                "control": "",
                "dropped_shard": "missing or invalid shard 2/",
                "duplicate_case": "repeated",
                "missing_case": "missing",
                "failed_shard": "did not pass",
                "other_candidate": "candidate or case inventory differs",
            }[mutation]
            passed = run.returncode == 0 and combined["status"] == "pass" and len(combined["cases"]) == len(manifest["case_inventory"])
            if mutation != "control":
                passed = run.returncode != 0 and combined["status"] == "fail" and any(intended in problem for problem in combined["problems"])
            results.append(
                {
                    "case": f"feature_{suite}_combine_{mutation}",
                    "passed": passed,
                    "exit": run.returncode,
                    "problems": combined["problems"],
                    "evidence": "synthetic outcomes, real CLI",
                }
            )
    return results


HOOK_DEADLINE_PROGRAM = """
import json
import sys
from pathlib import Path
from unittest.mock import patch
from scripts.quality import ci_hooks
starts = Path(sys.argv[sys.argv.index('--root') + 1]) / 'process_starts.jsonl'
original_run = ci_hooks.run_command
def recorded_run(executable, *args, **kwargs):
    with starts.open('a') as handle:
        handle.write(json.dumps(executable) + '\\n')
    return original_run(executable, *args, **kwargs)
ci_hooks.run_command = recorded_run
with patch.object(ci_hooks.time, 'time', return_value=1000):
    raise SystemExit(ci_hooks.main())
"""


HOOK_TIMEOUT_PROGRAM = """
from unittest.mock import patch
from scripts.quality import ci_hooks
with patch.object(ci_hooks, 'group_deadline_seconds', return_value=1):
    raise SystemExit(ci_hooks.main())
"""


def hook_timeout_case(scratch: Path) -> dict[str, Any]:
    """A real hook process times out after partial output, and the CLI must retain failed evidence and stop the group."""
    root = scratch / "hung_hook"
    for name in ("config/quality/ci_groups.json", ".pre-commit-config.yaml", ".github/workflows/ci.yml"):
        target = root / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ROOT / name, target)
    config = json.loads((root / "config/quality/ci_groups.json").read_text())
    hook = config["hook_groups"]["code"][0]
    binary = root / ".venv/bin/pre-commit"
    binary.parent.mkdir(parents=True)
    binary.write_text(
        f"#!{PYTHON}\nimport sys,time\nsys.stdout.write('hook partial stdout\\n');sys.stdout.flush()\n"
        "sys.stderr.write('hook partial stderr\\n');sys.stderr.flush()\ntime.sleep(60)\n"
    )
    binary.chmod(0o755)
    output = root / "output"
    result = run_command(
        str(PYTHON), ["-c", HOOK_TIMEOUT_PROGRAM, "--root", str(root), "--group", "code", "--output-directory", str(output)], cwd=ROOT, timeout=10
    )
    report_path = output / "report.json"
    report = json.loads(report_path.read_text()) if report_path.exists() else {}
    log_path = output / f"{hook}.log"
    log = log_path.read_text() if log_path.exists() else ""
    rows = report.get("hooks", [])
    checks = {
        "failed_exit": result.returncode != 0,
        "failed_report": report.get("status") == "fail",
        "only_hung_hook_recorded": len(rows) == 1 and rows[0]["hook"] == hook and rows[0]["exit"] == 1,
        "partial_output_retained": "hook partial stdout" in log and "hook partial stderr" in log,
        "visible_remaining_budget": "remaining" in log and "budget" in log and "remaining" in result.stderr and "budget" in result.stderr,
        "bounded_test_deadline": report.get("deadline_seconds") == 1,
        "no_traceback": "Traceback" not in result.stderr,
    }
    return {
        "case": "hung_hook_retains_partial_output_and_failed_report",
        "passed": all(checks.values()),
        "checks": checks,
        "exit": result.returncode,
        "error": result.stderr[-1200:],
        "evidence": "real hook CLI and process, group deadline shortened to one second",
    }


def dispatcher_output_cases(base: Path, scratch: Path) -> list[dict[str, Any]]:
    """Observe the real dispatcher's stdout before acknowledging a single item, and reject interleaved parallel output."""
    results = []
    for label, part, shard, fail in (
        ("bronze", "bronze", "", False),
        ("silver", "silver", "", False),
        ("gold", "gold", "", False),
        ("source_shard", "feature_sources", "2/8", False),
        ("column_shard", "feature_columns", "2/4", False),
        ("failed_shard", "feature_sources", "2/8", True),
        ("parallel", "features", "", False),
    ):
        tree = scratch / f"output_{label}"
        shutil.copytree(base, tree)
        quality = tree / "quality"
        ack = quality / "output_ack"
        ack.mkdir(parents=True)
        parallel = label == "parallel"
        body = '  local key="${1:-layer}"\n  key="${key//\\//_}"\n  printf \'OUTPUT first @ENTRY@_%s\\n\' "$key"\n'
        signal = f"{ack}/@ENTRY@_$key"
        body += f"  local deadline=$((SECONDS + {'2' if parallel else '4'}))\n"
        body += f'  while [[ ! -f "{signal}" && $SECONDS -lt $deadline ]]; do sleep 0.02; done\n'
        if parallel:
            body += f"  if [[ -f \"{signal}\" ]]; then printf 'parallel output interleaved\\n' >&2; return 3; fi\n"
        else:
            body += f"  if [[ ! -f \"{signal}\" ]]; then printf 'output acknowledgment deadline exceeded\\n' >&2; return 3; fi\n"
        body += "  printf 'OUTPUT last @ENTRY@_%s\\n' \"$key\"\n" + ("  return 3\n" if fail else "")
        group = "lakehouse_features" if part.startswith("feature") else f"lakehouse_{part}"
        entries = (
            ["lakehouse_features_sources", "lakehouse_features_columns", "lakehouse_features_combine", "lakehouse_features_store"]
            if group == "lakehouse_features"
            else ["lakehouse_gold_checks" if part == "gold" else group]
        )
        (tree / GROUP_FILES / f"{group}.sh").write_text(
            "#!/usr/bin/env bash\n" + "\n".join(f"{entry}() {{\n{body.replace('@ENTRY@', entry)}}}\n" for entry in entries)
        )
        dispatcher = tree / "scripts/run_ci.sh"
        dispatcher.write_text(
            dispatcher.read_text().replace(
                '  rm -rf "$scratch"',
                '  mkdir -p "$quality_output/saved_item_logs"\n  cp "$scratch"/*.log "$quality_output/saved_item_logs/"\n  rm -rf "$scratch"',
                1,
            )
        )
        summary = tree / "summary.md"
        env = {
            **os.environ,
            "CI_SCOPE": "full",
            "CI_PART": part,
            "CI_UNITS": "checks",
            "FEATURE_SHARD": shard,
            "GITHUB_ACTIONS": "true",
            "GITHUB_STEP_SUMMARY": str(summary),
            "PYTHON_BIN": str(PYTHON),
            "QUALITY_ARTIFACT_DIR": str(quality),
            "E2E_ARTIFACT_DIR": str(tree / "e2e"),
        }
        observer = (
            'set -o pipefail; bash scripts/run_ci.sh | while IFS= read -r line; do printf "%s\\n" "$line"; '
            'case "$line" in "OUTPUT first "*) : > "$QUALITY_ARTIFACT_DIR/output_ack/${line#OUTPUT first }" ;; esac; done'
        )
        run = run_command("bash", ["-c", observer], cwd=tree, env=env, timeout=30)
        log = run.stdout + run.stderr
        (scratch / f"output_{label}.log").write_text(log)
        saved = sorted((quality / "saved_item_logs").glob("*.log"))
        outputs = [line for line in run.stdout.splitlines() if line.startswith("OUTPUT ")]
        retained = [line for path in saved for line in path.read_text().splitlines() if line.startswith("OUTPUT ")]
        expected_count = 15 if parallel else 1
        checks = {
            "expected_exit": (run.returncode != 0) == fail,
            "acknowledged_output": len(list(ack.iterdir())) == expected_count,
            "no_output_deadline": "output acknowledgment deadline exceeded" not in log,
            "parallel_output_buffered": "parallel output interleaved" not in log,
            "one_pair_per_item": len(outputs) == expected_count * 2
            and all(first.replace("OUTPUT first ", "") == last.replace("OUTPUT last ", "") for first, last in zip(outputs[::2], outputs[1::2], strict=True)),
            "item_logs_retained": len(saved) == expected_count and sorted(retained) == sorted(outputs),
            "summary_retained": summary.exists()
            and "| Step | Seconds |" in summary.read_text()
            and all(f"| {path.stem} |" in summary.read_text() for path in saved),
            "failure_propagates": not fail or ("Local CI failed in: lakehouse_features_sources_2" in log and PASS_LINES["all"] not in log),
        }
        results.append(
            {
                "case": f"dispatcher_output_{label}",
                "passed": all(checks.values()),
                "checks": checks,
                "exit": run.returncode,
                "evidence": "real dispatcher and stdout observer, stand-in group bodies",
            }
        )
    return results


FEATURE_TIMING_PROGRAM = """
import sys
from pathlib import Path
from types import SimpleNamespace
from scripts.lakehouse import run_feature_sources_e2e as source
from scripts.lakehouse import run_feature_columns_e2e as column
class ProgressOutput:
    pending = False
    def write(self, value):
        if value.startswith('[timing]'):
            self.pending = True
        return sys.__stdout__.write(value)
    def flush(self):
        self.pending = False
        sys.__stdout__.flush()
progress = ProgressOutput()
sys.stdout = progress
def ready(value=None):
    if progress.pending:
        raise RuntimeError('case timing output was not flushed before case work or completion')
    return value
suite = sys.argv.pop(1)
if suite == 'sources':
    source.admission_cases = lambda: [('light_admission', lambda root: None, None)]
    source.resolved_cases = lambda: [('light_resolved', lambda root: None, None)]
    source.cases = lambda: [('light_static', lambda root: None, None)]
    source.pass_cases = lambda: []
    source.shell.build_shell = lambda *args, **kwargs: Path()
    source.run_resolved = lambda *args, **kwargs: ready(([], ['dbt run tag:features', 'dbt compile tag:features']))
    source.copy_scratch = lambda root: None
    source.sources.check = lambda root: ready(([], {}))
    source.shell.run_features = lambda *args, **kwargs: ready(SimpleNamespace(database=Path(), relations={}))
    source.shell.step = lambda *args: None
    source.shell.read_relations = lambda *args: {f'main.synthetic_{i}': '1 rows, md5 ' + 'a' * 32 for i in range(31)}
    relation_problems = source.sources.relation_problems
    source.sources.relation_problems = lambda *args: ready(relation_problems(*args))
    result = source.main()
else:
    docs = {
        column.MODEL: {column.COLUMN: {'data_type': 'varchar', 'description': 'Synthetic column.'}},
        column.MATRIX_MODEL: {column.MATRIX_COLUMN: {'data_type': 'double', 'description': 'Synthetic matrix column.'}},
    }
    column.columns.sql_models = lambda root: {column.MODEL, column.MATRIX_MODEL}
    documented_all = column.columns.documented_all
    column.columns.documented_all = lambda root: (docs, []) if root == column.columns.REPO_ROOT else documented_all(root)
    column.columns.read_columns = lambda *args: {column.MODEL: [(column.COLUMN, 'varchar')], column.MATRIX_MODEL: [(column.MATRIX_COLUMN, 'double')]}
    def build(*args, **kwargs):
        ready()
        if kwargs.get('contract'):
            raise column.columns.BuildError('dbt run tag:features failed: contract planted_extra_column')
        return Path()
    column.columns.build_database = build
    column.scratch_root = lambda folder: folder
    column.columns.check = lambda root: ready(([f'{column.MODEL}.planted_extra_column (integer) is an output column that the YAML does not document'], {}))
    column.rejected_project_case = lambda folder: ready({'case': 'column_check_does_not_build_a_rejected_project', 'ok': True})
    column.hooked_filter_case = lambda folder: ready({'case': 'post_hook_on_a_filter_model_leaves_its_table_alone', 'ok': True})
    compare = column.columns.compare
    column.columns.compare = lambda *args: ready(compare(*args))
    result = column.main()
ready()
raise SystemExit(result)
"""


def feature_case_timing_cases(scratch: Path) -> list[dict[str, Any]]:
    """Run every case control path with substituted feature builds and observe one flushed timing pair per selected case."""
    results = []
    inventories = json.loads((ROOT / "config/quality/feature_cases.json").read_text())
    for suite in ("sources", "columns"):
        output = scratch / f"timing_{suite}"
        selected = (
            [
                "light_admission",
                "light_resolved",
                "all_feature_views_have_checksums",
                "unreadable_relation_fails_after",
                "unreadable_relation_fails_before",
                "light_static",
            ]
            if suite == "sources"
            else inventories[suite]
        )
        args = ["-c", FEATURE_TIMING_PROGRAM, suite, "--output-directory", str(output), "--shard", "1/1"]
        if suite == "sources":
            for name in ("light_admission", "light_resolved", "light_static", "unreadable_relation_fails", "all_feature_views_have_checksums"):
                args += ["--only", name]
        run = run_command(str(PYTHON), args, cwd=ROOT, timeout=30)
        report = json.loads((output / "report.json").read_text()) if (output / "report.json").exists() else {}
        starts = re.findall(rf"^\[timing\] start feature {suite} case (\w+) at \S+$", run.stdout, re.MULTILINE)
        ends = re.findall(rf"^\[timing\] end feature {suite} case (\w+) at \S+: [0-9.]+ s$", run.stdout, re.MULTILINE)
        checks = {
            "successful_control_run": run.returncode == 0,
            "one_start_per_case": starts == selected,
            "one_end_per_case": ends == selected,
            "case_seconds_recorded": bool(report.get("cases")) and all("seconds" in row for row in report["cases"]),
        }
        results.append(
            {
                "case": f"feature_{suite}_prints_each_case_start_and_end",
                "passed": all(checks.values()),
                "checks": checks,
                "starts": starts,
                "ends": ends,
                "exit": run.returncode,
                "error": run.stderr[-1200:],
                "evidence": "real runner CLI, substituted feature builds",
            }
        )
    return results


def hook_partition_cases(scratch: Path) -> list[dict[str, Any]]:
    """The real hook runner rejects missing and repeated gates and preserves hook-stage and range arguments."""
    root = scratch / "hook_partition"
    (root / "config/quality").mkdir(parents=True)
    config = json.loads((ROOT / "config/quality/ci_groups.json").read_text())
    shutil.copy2(ROOT / ".pre-commit-config.yaml", root / ".pre-commit-config.yaml")
    (root / ".github/workflows").mkdir(parents=True)
    shutil.copy2(ROOT / ".github/workflows/ci.yml", root / ".github/workflows/ci.yml")
    target = root / "config/quality/ci_groups.json"
    job = workflow_document()["jobs"]["hooks"]
    marker = root / "github_env"
    before = int(datetime.now(UTC).timestamp())
    recorded = run_command("bash", ["-c", job["steps"][0].get("run", "")], cwd=root, env={**os.environ, "GITHUB_ENV": str(marker)}, timeout=10)
    after = int(datetime.now(UTC).timestamp())
    values = dict(line.split("=", 1) for line in marker.read_text().splitlines()) if marker.exists() else {}
    value = values.get("JOB_STARTED", "")
    results = [{"case": "hook_job_records_start_before_checkout", "passed": recorded.returncode == 0 and value.isdigit() and before <= int(value) <= after}]
    for label in ("control", "missing_hook", "repeated_hook", "moved_hook_control", "moved_hook_missing_hook"):
        damaged = copy.deepcopy(config)
        if label.startswith("moved_hook"):
            source = next(group for group, hooks in damaged["hook_groups"].items() if "writing-check" in hooks)
            destination = next(group for group in damaged["hook_groups"] if group not in {source, "documentation"})
            damaged["hook_groups"][source].remove("writing-check")
            damaged["hook_groups"][destination].append("writing-check")
        if label.endswith("missing_hook"):
            next(hooks for hooks in damaged["hook_groups"].values() if "writing-check" in hooks).remove("writing-check")
        if label == "repeated_hook":
            damaged["hook_groups"]["code"].append("writing-check")
        target.write_text(json.dumps(damaged))
        run = run_command(str(PYTHON), ["-m", "scripts.quality.ci_hooks", "--root", str(root), "--group", "privacy", "--list"], cwd=ROOT, timeout=60)
        wanted = "missing=['writing-check']" if label.endswith("missing_hook") else "repeated or unknown=['writing-check']"
        passed = run.returncode == 0 if label.endswith("control") else run.returncode != 0 and wanted in run.stderr
        results.append({"case": f"hook_partition_{label}", "passed": passed, "exit": run.returncode, "error": run.stderr[-400:]})
        if label.endswith("control") and not passed:
            return results
    target.write_text(json.dumps(config))
    binary = root / ".venv/bin/pre-commit"
    binary.parent.mkdir(parents=True)
    calls = root / "calls.jsonl"
    body = (
        f"#!{PYTHON}\nimport json,sys\nfrom pathlib import Path\np=Path({str(calls)!r})\n"
        "with p.open('a') as f: f.write(json.dumps(sys.argv[1:])+'\\n')\n"
        f"sys.exit(3 if sys.argv[2] == 'ruff' and Path({str(root / 'fail')!r}).exists() else 0)\n"
    )
    binary.write_text(body)
    binary.chmod(0o755)
    timeout_seconds = next(int(row["timeout"]) * 60 for row in job["strategy"]["matrix"]["include"] if row["group"] == "code")
    scenarios = (
        ("runs_the_unchanged_manual_entries", False, "700", timeout_seconds - 120 - 300),
        ("continues_and_fails_after_a_bad_hook", True, "700", timeout_seconds - 120 - 300),
        ("local_without_job_start_uses_invocation_budget", False, None, timeout_seconds - 120),
        ("exhausted_budget_refuses_hook", False, str(1000 - timeout_seconds), 0),
    )
    for label, failed_hook, job_started, expected_budget in scenarios:
        calls.unlink(missing_ok=True)
        starts = root / "process_starts.jsonl"
        starts.unlink(missing_ok=True)
        (root / "fail").unlink(missing_ok=True)
        if failed_hook:
            (root / "fail").touch()
        output = root / label
        environment = {key: value for key, value in os.environ.items() if key != "JOB_STARTED"}
        if job_started is not None:
            environment["JOB_STARTED"] = job_started
        run = run_command(
            str(PYTHON),
            ["-c", HOOK_DEADLINE_PROGRAM, "--root", str(root), "--group", "code", "--output-directory", str(output)],
            cwd=ROOT,
            env=environment,
            timeout=120,
        )
        actual = [json.loads(line) for line in calls.read_text().splitlines()] if calls.exists() else []
        processes = [json.loads(line) for line in starts.read_text().splitlines()] if starts.exists() else []
        expected = (
            [["run", hook, "--all-files", "--hook-stage", "manual", "--show-diff-on-failure"] for hook in config["hook_groups"]["code"]]
            if expected_budget
            else []
        )
        report_path = output / "report.json"
        report = json.loads(report_path.read_text()) if report_path.exists() else {}
        failed = failed_hook or expected_budget == 0
        checks = {
            "all_hooks_reached": actual == expected,
            "admitted_processes_match": processes == [str(binary)] * len(expected),
            "status_matches": report.get("status") == ("fail" if failed else "pass"),
            "workflow_deadline_with_margin": report.get("deadline_seconds") == expected_budget,
            "exit_matches": (run.returncode != 0) == failed,
            "intended_failure": not failed_hook or any(row["hook"] == "ruff" and row["exit"] == 3 for row in report.get("hooks", [])),
            "exhausted_admission_message": expected_budget != 0 or ("no time left for hook" in run.stderr and "budget is exhausted" in run.stderr),
            "no_traceback": "Traceback" not in run.stderr,
        }
        results.append(
            {
                "case": f"hook_group_{label}",
                "passed": all(checks.values()),
                "checks": checks,
                "evidence": "recording pre-commit stand-in, real group CLI",
            }
        )
    return results


def parallel_plan_cases(scratch: Path) -> list[dict[str, Any]]:
    """Dry-run the real dispatcher using print-only commands in a minimal tree, without checking out data or running gates."""
    base = scratch / "parallel_plan_base"
    names = [
        "scripts/run_ci.sh",
        "scripts/process.py",
        "scripts/quality/ci_groups.py",
        "config/quality/ci_groups.json",
        "config/quality/launcher_inputs.json",
        "config/lakehouse/bronze_tables.json",
        "config/lakehouse/aws_bronze_grants.json",
        *BODY_FILES,
    ]
    names += [path.relative_to(ROOT).as_posix() for path in (ROOT / "scripts/infrastructure").glob("*.sh")]
    for name in names:
        target = base / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ROOT / name, target)
    (base / TIMING).write_text(DRY_TIMING)
    gate = base / "scripts/acquisition/run_checks.sh"
    gate.parent.mkdir(parents=True)
    gate.write_text(DRY_ACQUISITION.replace("@FAIL@", "").replace("@SKIP@", ""))
    results = []
    normal: dict[str, Any] = {}
    for label in ("control", "repeat", "feature_shard_failure", "hook_failure"):
        tree = scratch / f"parallel_plan_{label}"
        shutil.copytree(base, tree)
        if label == "feature_shard_failure":
            path = tree / GROUP_FILES / "lakehouse_features.sh"
            text = path.read_text().replace('  local shard="${1:?}"', '  local shard="${1:?}"\n  if [[ "$shard" == 2/8 ]]; then return 3; fi', 1)
            path.write_text(text)
        if label == "hook_failure":
            path = tree / "scripts/run_ci.sh"
            path.write_text(path.read_text().replace("hook_code() { hook_partition code; }", "hook_code() { return 3; }"))
        code, log, _ = run_entry(
            tree,
            scratch,
            f"parallel_{label}",
            {"CI_SCOPE": "full", "CI_PART": "all", "QUALITY_ARTIFACT_DIR": str(tree / "quality"), "E2E_ARTIFACT_DIR": str(tree / "e2e")},
        )
        local = local_plan_of(log)
        if label == "control":
            normal = local
            differences = mirror_differences(github_plan_of(workflow_document()), local)
            results.append(
                {
                    "case": "parallel_local_plan_matches_all_hosted_feature_shards_and_hook_groups",
                    "passed": code == 0 and not differences,
                    "differences": differences,
                    "exit": code,
                }
            )
            planted = {name: mirror_differences(github_plan_of(document), local) for name, document in planted_workflows().items()}
            results.append({"case": "parallel_plan_catches_dropped_feature_shard_and_hook_group", "passed": all(planted.values()), "differences": planted})
        elif label == "repeat":
            differences = mirror_differences(normal, local)
            results.append({"case": "parallel_local_plan_is_repeatable", "passed": code == 0 and not differences, "differences": differences})
        else:
            intended = "lakehouse_features_sources_2" if label == "feature_shard_failure" else "hook_code"
            checks = {
                "exit_nonzero": code != 0,
                "names_failed_item": f"Local CI failed in: {intended}" in log,
                "no_feature_combine": "TIMED lakehouse_features: combine" not in log,
                "no_pass": PASS_LINES["all"] not in log,
            }
            results.append({"case": f"parallel_{label}_prevents_a_combined_pass", "passed": all(checks.values()), "checks": checks, "exit": code})
        (scratch / f"parallel_{label}.log").write_text(log)
    return [*results, *dispatcher_output_cases(base, scratch)]


def feature_listing(suite: str, *arguments: str) -> tuple[int, list[str], str]:
    if suite == "sources":
        result = run_command(str(PYTHON), ["-m", "scripts.lakehouse.run_feature_sources_e2e", "--list-cases", *arguments], cwd=ROOT, timeout=60)
    else:
        result = run_command(str(PYTHON), ["-m", "scripts.lakehouse.run_feature_columns_e2e", "--list-cases", *arguments], cwd=ROOT, timeout=60)
    try:
        names = json.loads(result.stdout)
    except json.JSONDecodeError:
        names = []
    return result.returncode, names, result.stderr


def parallel_ci_cases(scratch: Path) -> list[dict[str, Any]]:
    """List the real feature runners without builds and verify complete, disjoint hosted shards and manual hooks."""
    inventory = json.loads((ROOT / "config/quality/feature_cases.json").read_text())
    workflow = workflow_document()
    results: list[dict[str, Any]] = []
    for suite, count in (("sources", 8), ("columns", 4)):
        all_code, names, all_error = feature_listing(suite)
        shards: list[list[str]] = []
        for index in range(1, count + 1):
            listed_code, listed_names, _ = feature_listing(suite, "--shard", f"{index}/{count}")
            shards.append(listed_names if listed_code == 0 else [])
        flattened = [name for shard in shards for name in shard]
        definition = workflow["jobs"].get(f"feature_{suite}", {})
        configured = definition.get("strategy", {}).get("matrix", {}).get("shard", [])
        results.append(
            {
                "case": f"feature_{suite}_shards_cover_every_original_case_once",
                "passed": all_code == 0
                and names == inventory[suite]
                and bool(names)
                and sorted(flattened) == sorted(names)
                and len(flattened) == len(set(flattened))
                and configured == list(range(1, count + 1)),
                "listed": len(names),
                "expected": len(inventory[suite]),
                "shard_sizes": [len(shard) for shard in shards],
                "workflow_shards": configured,
                "error": all_error[-400:],
            }
        )
        for invalid in ("0/2", "3/2", "1/0", "one/two"):
            refused_code, _, refused_error = feature_listing(suite, "--shard", invalid)
            results.append(
                {
                    "case": f"feature_{suite}_refuses_invalid_shard_{invalid}",
                    "passed": refused_code != 0 and "1 <= K <= N" in refused_error,
                    "exit": refused_code,
                    "error": refused_error[-300:],
                }
            )
    hook_document = load_yaml((ROOT / ".pre-commit-config.yaml").read_text())
    if not isinstance(hook_document, dict):
        raise ValueError(".pre-commit-config.yaml must contain a mapping")
    hooks = hook_document["repos"][0]["hooks"]
    expected = sorted(hook["id"] for hook in hooks if "manual" in hook.get("stages", ["pre-commit", "manual"]) and hook["id"] != "local_ci")
    actual: list[str] = []
    exits = []
    for group in ("privacy", "documentation", "code", "samples"):
        listed = run_command(str(PYTHON), ["-m", "scripts.quality.ci_hooks", "--group", group, "--list"], cwd=ROOT, timeout=60)
        exits.append(listed.returncode)
        try:
            actual.extend(json.loads(listed.stdout))
        except json.JSONDecodeError:
            actual.append("<invalid hook list>")
    results.append(
        {
            "case": "hook_groups_cover_every_manual_hook_exactly_once",
            "passed": exits == [0, 0, 0, 0] and sorted(actual) == expected,
            "missing": sorted(set(expected) - set(actual)),
            "repeated": sorted(name for name in set(actual) if actual.count(name) > 1),
            "exits": exits,
        }
    )
    return [*results, *feature_combination_cases(scratch), *hook_partition_cases(scratch), hook_timeout_case(scratch), *feature_case_timing_cases(scratch)]


def speed_static_cases(scratch: Path) -> list[dict[str, Any]]:
    return [
        *parallel_ci_cases(scratch),
        *single_command_cases(),
        *timing_function_cases(scratch),
        *gold_unit_cases(),
        *feature_split_cases(),
        *acquisition_script_cases(),
        *timeout_cases(),
    ]


STUB_UNIT = """#!/usr/bin/env bash
# Stand-in for a CI command: logs its start and end with the time to a file, and sleeps.
now() { perl -MTime::HiRes=time -e 'printf "%.3f", time'; }
printf '%s start %s\\n' "$1" "$(now)" >>"$STUB_LOG"
sleep "$2"
printf '%s end %s\\n' "$1" "$(now)" >>"$STUB_LOG"
"""


def replaced_function(text: str, name: str, body: str) -> str:
    found = re.search(rf"^{name}\(\) \{{\n.*?^\}}", text, re.DOTALL | re.MULTILINE)
    if not found:
        raise RuntimeError(f"no function {name} in the text")
    return text[: found.start()] + f"{name}() {{\n{body}\n}}" + text[found.end() :]


def stub_functions(work: Path, bodies: dict[str, str]) -> None:
    """Replace the bodies of functions of the dispatcher or of a body file in the scratch worktree; everything else stays as it is."""
    for name, body in bodies.items():
        path = next(
            path for path in (work / "scripts/run_ci.sh", *sorted((work / GROUP_FILES).glob("*.sh"))) if function_text(path.read_text(encoding="utf-8"), name)
        )
        path.write_text(replaced_function(path.read_text(encoding="utf-8"), name, body), encoding="utf-8")
    (work / "stub_unit.sh").write_text(STUB_UNIT, encoding="utf-8")


def stub_log(path: Path) -> dict[str, dict[str, float]]:
    seen: dict[str, dict[str, float]] = {}
    for line in path.read_text(encoding="utf-8").splitlines() if path.exists() else []:
        name, kind, stamp = line.split()
        seen.setdefault(name, {})[kind] = float(stamp)
    return seen


GOLD_STUBS = {
    "lakehouse_gold_checks": '  timed "stub: checks" bash "$repo_root/stub_unit.sh" checks 2',
    "lakehouse_gold_writer_setup": '  timed "stub: writer setup" bash "$repo_root/stub_unit.sh" writer 2',
    "lakehouse_gold_model_metadata_e2e": '  timed "stub: model metadata" bash "$repo_root/stub_unit.sh" metadata 2',
}


def units_run_case(work: Path, out: Path, name: str, units: str, want: set[str]) -> dict[str, Any]:
    stub_functions(work, GOLD_STUBS)
    log = work / "data" / "stub_units.log"
    log.parent.mkdir(exist_ok=True)
    log.unlink(missing_ok=True)
    env = {"CI_SCOPE": "full", "CI_PART": "gold", "STUB_LOG": str(log), **({"CI_UNITS": units} if units else {})}
    try:
        code, text, summary = run_entry(work, out, name, env)
    finally:
        restore(work)
    seen = stub_log(log)
    rows = re.findall(r"^\| (stub: [a-z ]+) \| \S+ \| \S+ \| (\d+) \| (\d+) \|$", summary, re.MULTILINE)
    alone_first = True
    overlap = True
    if {"writer", "checks", "metadata"} <= set(seen):
        alone_first = seen["writer"]["end"] <= seen["checks"]["start"] and seen["writer"]["end"] <= seen["metadata"]["start"]
        overlap = seen["checks"]["start"] < seen["metadata"]["end"] and seen["metadata"]["start"] < seen["checks"]["end"]
    labels = {"checks": "stub: checks", "writer": "stub: writer setup", "metadata": "stub: model metadata"}
    expected = {labels[key] for key in want}
    checks = {
        "exit_zero": code == 0 and PASS_LINES["gold"] in text,
        "only_the_wanted_units_ran": set(seen) == want,
        "writer_set_up_ended_before_the_others_started": alone_first,
        "the_other_units_ran_together": overlap,
        "every_command_has_its_row_in_the_summary": {label for label, _seconds, _status in rows} == expected,
        "rows_show_seconds_and_exit_zero": bool(rows) and all(int(seconds) >= 2 and status == "0" for _label, seconds, status in rows),
        "start_and_end_lines_in_the_log": all(f"[timing] start {label} at" in text and f"[timing] end {label} at" in text for label in expected),
    }
    return {"case": name, "passed": all(checks.values()), "checks": checks, "exit": code, "seen": seen}


def failing_unit_case(work: Path, out: Path) -> dict[str, Any]:
    stub_functions(work, {**GOLD_STUBS, "lakehouse_gold_checks": '  timed "stub: failing command" bash -c "exit 3"'})
    log = work / "data" / "stub_units.log"
    log.parent.mkdir(exist_ok=True)
    log.unlink(missing_ok=True)
    try:
        code, text, summary = run_entry(work, out, "failing_unit", {"CI_SCOPE": "full", "CI_PART": "gold", "STUB_LOG": str(log)})
    finally:
        restore(work)
    checks = {
        "exit_nonzero": code != 0,
        "failure_names_the_unit": "Local CI failed in: lakehouse_gold_checks" in text,
        "log_shows_exit_3": "[timing] end stub: failing command" in text and ", exit 3" in text,
        "summary_row_shows_exit_3": re.search(r"^\| stub: failing command \| \S+ \| \S+ \| \d+ \| 3 \|$", summary, re.MULTILINE) is not None,
        "no_pass_line": PASS_LINES["gold"] not in text,
    }
    return {"case": "timing_failed_command_keeps_exit_code_in_the_run", "passed": all(checks.values()), "checks": checks, "exit": code}


def interrupted_unit_case(work: Path, out: Path) -> dict[str, Any]:
    stub_functions(work, {**GOLD_STUBS, "lakehouse_gold_checks": '  timed "stub: interrupted" bash -c \'kill -TERM "$0"; sleep 3\' "$$"'})
    log = work / "data" / "stub_units.log"
    log.parent.mkdir(exist_ok=True)
    log.unlink(missing_ok=True)
    try:
        code, text, _ = run_entry(work, out, "interrupted_unit", {"CI_SCOPE": "full", "CI_PART": "gold", "STUB_LOG": str(log)})
    finally:
        restore(work)
    checks = {"exit_nonzero": code != 0, "no_pass_line": PASS_LINES["gold"] not in text and "All local CI checks passed" not in text}
    return {"case": "timing_interrupted_run_fails", "passed": all(checks.values()), "checks": checks, "exit": code}


def gates_part_cases(work: Path, out: Path) -> list[dict[str, Any]]:
    code, log, text = run_entry(work, out, "gates_part", {"CI_SCOPE": "full", "CI_PART": "gates"})
    folder = break_gate(work)
    try:
        failed_code, failed_log, _ = run_entry(work, out, "gates_part_failure", {"CI_SCOPE": "full", "CI_PART": "gates"})
    finally:
        shutil.rmtree(folder, ignore_errors=True)
        restore(work)
    return [
        {
            "case": "gates_part_runs_gates_only",
            "passed": code == 0
            and "The gates part passed." in log
            and "\n== " not in log
            and re.search(r"^gates: \d+ s$", log, re.MULTILINE) is not None
            and "| gates: privacy scan |" in text,
            "exit": code,
        },
        {
            "case": "gates_part_fails_when_a_gate_fails",
            "passed": failed_code != 0 and "old dataset folders exist again" in failed_log and "The gates part passed." not in failed_log,
            "exit": failed_code,
        },
    ]


MODEL_METADATA_CASES = ("control", "review8_p01_public", "review8_p01_confidential", "review8_p02_public")


def model_metadata_cases(work: Path, out: Path) -> list[dict[str, Any]]:
    """The model metadata E2E with cases at the same time: the outcomes equal the serial outcomes (``--prove-parallel-equal``), and a program that passes its
    deadline fails the run with the case named."""
    arguments = [argument for name in MODEL_METADATA_CASES for argument in ("--case", name)]
    folder = out / "model_metadata"
    equal = run_command(
        str(PYTHON),
        ["-m", "scripts.quality.run_model_metadata_e2e", *arguments, "--workers", "2", "--prove-parallel-equal", "--output-directory", str(folder / "equal")],
        cwd=work,
        timeout=600,
    )
    slow = run_command(
        str(PYTHON),
        [
            "-m",
            "scripts.quality.run_model_metadata_e2e",
            *arguments[:4],
            "--workers",
            "2",
            "--case-seconds",
            "1",
            "--output-directory",
            str(folder / "deadline"),
        ],
        cwd=work,
        timeout=600,
    )
    equal_text, slow_text = equal.stdout + equal.stderr, slow.stdout + slow.stderr
    named = [name for name in MODEL_METADATA_CASES[:2] if re.search(rf"WRONG\s+{name}\b", slow_text)]
    return [
        {
            "case": "model_metadata_workers_match_serial",
            "passed": equal.returncode == 0 and "serial and 2 workers: 4 cases, 0 different" in equal_text and "4 of 4 cases behaved as expected" in equal_text,
            "exit": equal.returncode,
            "output_tail": equal_text[-300:],
        },
        {
            "case": "model_metadata_workers_deadline_names_the_cases",
            "passed": slow.returncode != 0
            and named == list(MODEL_METADATA_CASES[:2])
            and "deadline: a program of this case ran longer than 1 seconds" in slow_text,
            "exit": slow.returncode,
            "named": named,
            "output_tail": slow_text[-300:],
        },
    ]


def speed_real_cases(work: Path, out: Path) -> list[dict[str, Any]]:
    cases = [unit_refusal_case(work, out)]
    cases.append(units_run_case(work, out, "gold_units_run_alone_then_together", "", {"writer", "checks", "metadata"}))
    cases.append(units_run_case(work, out, "gold_unit_selection_runs_only_the_named_unit", "model_metadata_e2e", {"metadata"}))
    cases.append(failing_unit_case(work, out))
    cases.append(interrupted_unit_case(work, out))
    cases += gates_part_cases(work, out)
    cases += mirror_cases(work, out)
    cases += variable_cases(work, out)
    cases += model_metadata_cases(work, out)
    return cases


BODY_SELECTION = {
    "python_lint": ["python_lint", "repository_code_checks"],
    "lakehouse_bronze": [*LAYERS, "repository_code_checks"],
    "lakehouse_silver": [*LAYERS[1:], "repository_code_checks"],
    "lakehouse_gold": [*LAYERS[2:], "repository_code_checks"],
    "lakehouse_features": ["lakehouse_features", "repository_code_checks"],
    "orchestration_checks": ["orchestration_checks", "repository_code_checks"],
    "repository_code_checks": ["repository_code_checks"],
    "configuration_checks": ["configuration_checks", "repository_code_checks"],
    "sql_style": ["repository_code_checks", "sql_style"],
    "terraform_checks": ["repository_code_checks", "terraform_checks"],
    "launcher": ["repository_code_checks", "launcher"],
    "reuse_gate_checks": ["repository_code_checks", "launcher", "reuse_gate_checks"],
}


def staged_edit_case(work: Path, name: str, path: str, want_run: list[str], part: str = "core+layers", want_reason: str = "") -> dict[str, Any]:
    """Stage a one-line edit of ``path`` and check which groups the staged scope selects."""
    target = work / path
    try:
        target.write_text(target.read_text(encoding="utf-8") + "\n# staged edit\n", encoding="utf-8")
        git(work, "add", path)
        return selection_case(work, name, ["--scope", "staged", "--part", part], want_run, want_reason)
    finally:
        restore(work)


def body_file_cases(work: Path) -> list[dict[str, Any]]:
    """An edit to a body file selects its own group (and the layers downstream of a lakehouse layer) and the repository group, which runs the guard, and no
    other group in the core and layer parts (the launcher part selects every path, see ``launcher_selection_cases``). The dispatcher, the workflow and the
    setup action select every group."""
    cases = [
        {
            "case": "every_body_file_has_an_expected_selection",
            "passed": sorted(BODY_SELECTION) == sorted(Path(path).stem for path in BODY_FILES),
            "body_files": [Path(path).stem for path in BODY_FILES],
        }
    ]
    for group, wanted in BODY_SELECTION.items():
        part = "all" if group in {"launcher", "reuse_gate_checks"} else "core+layers"
        cases.append(staged_edit_case(work, f"body_file_edit_selects_{group}", f"{GROUP_FILES}/{group}.sh", wanted, part))
    cases.append(staged_edit_case(work, "pull_request_template_selects_no_group", ".github/pull_request_template.md", []))
    cases.append(staged_edit_case(work, "pull_request_template_selects_only_the_launcher_in_part_all", ".github/pull_request_template.md", ["launcher"], "all"))
    cases.append(staged_edit_case(work, "dispatcher_edit_selects_every_group", "scripts/run_ci.sh", WITH_LAYERS, want_reason="selects every group"))
    cases.append(
        staged_edit_case(
            work,
            "dispatcher_edit_selects_every_group_in_part_all",
            "scripts/run_ci.sh",
            [*WITH_LAYERS, "launcher", "reuse_gate_checks"],
            "all",
            "selects every group",
        )
    )
    cases.append(
        selection_case(
            work,
            "timing_wrapper_selects_every_group",
            ["--scope", "paths", "--path", "scripts/quality/timing.sh", "--part", "core+layers"],
            WITH_LAYERS,
            "selects every group",
        )
    )
    cases += [
        selection_case(
            work,
            f"{Path(path).parent.name}_file_selects_every_group",
            ["--scope", "paths", "--path", path, "--part", "core+layers"],
            WITH_LAYERS,
            "selects every group",
        )
        for path in (".github/workflows/ci.yml", ".github/actions/setup/action.yml")
    ]
    return cases


GUARD_FILES = ("config/quality/ci_groups.json", "scripts/run_ci.sh", ".github/workflows/ci.yml")
# Text that runs a command (or a compound statement that runs one) when the file is sourced: the guard stops it before any side effect.
COMMAND_FORMS = {
    "echo": "echo SIDE >> side_effect_marker",
    "after_a_separator": "true; sql_style() { :; }",
    "assignment_with_command_substitution": "x=$(touch side_effect_marker)",
    "trap": "trap 'touch side_effect_marker' EXIT",
    "touch": "touch side_effect_marker",
    "exit": "exit 0",
    "if_block_around_a_declaration": "if true; then function sql_style { :; }; fi",
    "top_level_printf_with_ansi_quote": "printf '%s\\n' $'it\\'s'",
    "top_level_heredoc": "cat <<EOF-TAG\nsafe\nEOF-TAG",
    "arithmetic_assignment": "mask=$((1 << bits))",
    "source_of_another_file": "source ./other_group.sh",
    "trap_that_removes_the_guard": "trap - DEBUG",
}
# Declarations of a group function in the dispatcher text, in the spellings Bash accepts as the plain name.
DISPATCHER_FORMS = {
    "plain": "sql_style() { :; }",
    "escaped_identifier": "sql_sty\\le() { :; }",
    "escaped_keyword_identifier": "function sql_sty\\le { :; }",
    "ansi_encoded_identifier": "function $'sql_\\x73tyle' { :; }",
    "comment_after_the_keyword": "function # harmless comment\nsql_style { :; }",
    "split_name_continuation": "sql_\\\nstyle() { :; }",
    "quoted_name": 'function "sql_style" { :; }',
    "name_made_of_quote_parts": "sql_'style'() { :; }",
}
# Quoted function names: Bash 3.2 and 5.2 refuse them with a message, and a body file must cause none.
QUOTED_FORMS = {
    "quoted_keyword_name": 'function "sql_style" { :; }',
    "ansi_quoted_keyword_name": "function $'sql_style' { :; }",
    "locale_quoted_keyword_name": 'function $"sql_style" { :; }',
    "quoted_name_with_parentheses": '"sql_style"() { :; }',
}
# What a hostile group file can write. The guard accepts these (it checks the shape of a file, not its effect) and the dispatcher runs each group in its own
# process, so none of them changes the shell state of another group: the isolation cases below run two or more groups with each one in python_lint.sh.
HOSTILE_FORMS = {
    "foreign_function_keyword": "function sql_style { :; }",
    "foreign_function_spaced": "function sql_style ( ) { :; }",
    "foreign_function_continued_name": "sql_\\\nstyle() { :; }",
    "foreign_function_after_a_heredoc": "helper_h() {\ncat <<EOF-TAG\nsafe\nEOF-TAG\n}\nfunction sql_style { :; }",
    "foreign_function_after_an_ansi_quote": "helper_q() { printf '%s\\n' $'it\\'s'; }\nfunction sql_style { :; }",
    "nested_after_and": "helper_outer() { : && sql_style() { :; }; }",
    "nested_after_not": "helper_outer() { ! sql_style() { :; }; }",
    "loader_override_that_replaces_sql_style": 'source() {\n  builtin source "$@" && sql_style() { printf "WRONG_SQL\\n"; };\n}',
    "shadowed_trap_then_foreign_function": (
        "printf(){ :; }; exit(){ :; }\ntrap - DEBUG\nunset -f printf exit\nif [[ $- != *r* ]]; then\n  sql_style(){ printf 'WRONG_SQL\\n'; }\nfi"
    ),
    "timed_override": "timed() { :; }",
    "dispatcher_helper_overrides": (
        "record() { :; }\nis_selected() { return 0; }\nrun_group() { :; }\nrun_item() { :; }\nfinish() { :; }\nrequire_group_file() { :; }\n"
        "run_items() { :; }\nacquisition_shard() { :; }\ntiming_table() { :; }"
    ),
    "builtin_overrides": (
        "source() { :; }\nbuiltin() { :; }\neval() { :; }\nexec() { :; }\ncommand() { :; }\nset() { :; }\ndeclare() { :; }\nprintf() { :; }\necho() { :; }"
    ),
    "top_level_exit": "exit 0",
    "environment_poisoned": "export python_bin=/nonexistent PATH=/nonexistent scratch=/nonexistent repo_root=/nonexistent CI_TIMING_FILE=/nonexistent",
    "exit_trap_that_prints": "trap 'echo WRONG_SQL' EXIT",
}


def guard_case(name: str, mutate: Callable[[Path], None], named: list[str]) -> dict[str, Any]:
    """Copy the files the guard reads into a scratch folder, plant drift, run the guard and require a failure that names every string of ``named``.
    No planted drift (``named`` empty) must pass."""
    with tempfile.TemporaryDirectory(prefix="ci_group_files.") as folder:
        tree = Path(folder)
        for path in (*GUARD_FILES, *BODY_FILES):
            (tree / path).parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(ROOT / path, tree / path)
        mutate(tree)
        result = run_command(str(PYTHON), ["-m", "scripts.quality.ci_group_files", "--root", str(tree)], cwd=ROOT, timeout=120)
        leaked = [path.name for path in tree.rglob("side_effect_marker")]
    missing = [text for text in named if text not in result.stderr]
    passed = (result.returncode == 0 if not named else result.returncode == 1 and not missing) and not leaked
    return {"case": name, "passed": passed, "exit": result.returncode, "not_named": missing, "side_effect_leaked": leaked, "stderr": result.stderr[-400:]}


def append(path: str, text: str) -> Callable[[Path], None]:
    def mutate(tree: Path) -> None:
        with (tree / path).open("a", encoding="utf-8") as stream:
            stream.write(text)

    return mutate


def replace_file(path: str, text: str) -> Callable[[Path], None]:
    def mutate(tree: Path) -> None:
        (tree / path).write_text(text, encoding="utf-8")

    return mutate


def change_config(change: Callable[[dict[str, Any]], None]) -> Callable[[Path], None]:
    def mutate(tree: Path) -> None:
        path = tree / "config/quality/ci_groups.json"
        config = json.loads(path.read_text(encoding="utf-8"))
        change(config)
        path.write_text(json.dumps(config, indent=2), encoding="utf-8")

    return mutate


def guard_cases() -> list[dict[str, Any]]:
    """Planted drift for each rule of scripts/quality/ci_group_files.py."""
    sql, python_lint, terraform = (f"{GROUP_FILES}/{name}.sh" for name in ("sql_style", "python_lint", "terraform_checks"))

    def second_list(config: dict[str, Any]) -> None:
        config["groups"]["sql_style"].append(f"^{python_lint.replace('.', chr(92) + '.')}$")

    def drop_own_pattern(config: dict[str, Any]) -> None:
        config["groups"]["terraform_checks"] = [pattern for pattern in config["groups"]["terraform_checks"] if "ci/groups" not in pattern]

    def global_pattern(config: dict[str, Any]) -> None:
        config["global"].append("^scripts/ci/")

    def broad_launcher(config: dict[str, Any]) -> None:
        config["groups"]["launcher"].append("^scripts/")

    def remove_sql_file(tree: Path) -> None:
        (tree / sql).unlink()

    def stray(tree: Path) -> None:
        (tree / GROUP_FILES / "stray.sh").write_text("#!/usr/bin/env bash\nstray() { :; }\n", encoding="utf-8")

    def empty_body(tree: Path) -> None:
        (tree / python_lint).write_text("#!/usr/bin/env bash\n# no function\n", encoding="utf-8")

    def prefix_only(tree: Path) -> None:
        (tree / python_lint).write_text("#!/usr/bin/env bash\npython_lint_extra() { :; }\n", encoding="utf-8")

    def drop_gold_unit(tree: Path) -> None:
        path = tree / GROUP_FILES / "lakehouse_gold.sh"
        path.write_text(path.read_text(encoding="utf-8").replace("lakehouse_gold_writer_setup()", "lakehouse_gold_renamed()"), encoding="utf-8")

    def drop_feature_job(tree: Path) -> None:
        path = tree / ".github/workflows/ci.yml"
        source = path.read_text(encoding="utf-8")
        source = re.sub(r"^  feature_(?:sources|columns):\n.*?(?=^  [a-z_]+:\n|\Z)", "", source, flags=re.MULTILINE | re.DOTALL)
        source = re.sub(r"^          - layer: Features\n(?:            .*\n)+", "", source, flags=re.MULTILINE)
        path.write_text(source, encoding="utf-8")

    def rename_launcher(tree: Path) -> None:
        path = tree / GROUP_FILES / "launcher.sh"
        path.write_text(path.read_text(encoding="utf-8").replace("launcher_e2e()", "launcher_unit()"), encoding="utf-8")

    def duplicate(tree: Path) -> None:
        for path in (sql, terraform):
            append(path, "\nshared_helper() { :; }\n")(tree)

    return [
        guard_case("guard_passes_without_drift", lambda tree: None, []),
        guard_case("guard_fails_a_feature_group_without_a_hosted_job", drop_feature_job, [".github/workflows/ci.yml", "lakehouse_features", "no hosted job"]),
        guard_case(
            "guard_fails_a_group_function_left_in_the_dispatcher", append("scripts/run_ci.sh", "\nsql_style() { :; }\n"), ["scripts/run_ci.sh", "sql_style"]
        ),
        guard_case("guard_fails_a_body_file_listed_in_two_groups", change_config(second_list), [python_lint, "group sql_style lists it too"]),
        guard_case("guard_fails_a_body_file_missing_from_its_group", change_config(drop_own_pattern), [terraform, "terraform_checks"]),
        guard_case("guard_fails_a_global_pattern_that_matches_a_body_file", change_config(global_pattern), [sql, "global list"]),
        guard_case("guard_fails_a_launcher_pattern_that_matches_a_body_file", change_config(broad_launcher), [python_lint, "group launcher lists it too"]),
        guard_case("guard_fails_a_group_without_a_body_file", remove_sql_file, [sql, "group sql_style has no body file"]),
        guard_case("guard_fails_a_body_file_of_no_group", stray, [f"{GROUP_FILES}/stray.sh", "belongs to no group"]),
        guard_case(
            "guard_fails_a_body_file_without_its_entry_function", empty_body, [python_lint, "does not define python_lint, the function the dispatcher calls"]
        ),
        guard_case("guard_fails_a_prefix_only_function_instead_of_the_entry_function", prefix_only, [python_lint, "does not define python_lint"]),
        guard_case(
            "guard_fails_a_gold_body_file_without_a_unit_function", drop_gold_unit, ["lakehouse_gold.sh", "does not define lakehouse_gold_writer_setup"]
        ),
        guard_case("guard_fails_a_launcher_body_file_without_its_entry_function", rename_launcher, ["launcher.sh", "does not define launcher_e2e"]),
        guard_case("guard_passes_a_helper_defined_in_two_body_files", duplicate, []),
        guard_case("guard_passes_another_groups_function_name_in_a_body_file", append(python_lint, "\nsql_style() { :; }\n"), []),
        *(
            guard_case(f"guard_stops_a_command_at_the_top_level_{label}", append(terraform, "\n" + text + "\n"), [terraform, "runs a command when sourced"])
            for label, text in COMMAND_FORMS.items()
        ),
        *(guard_case(f"guard_fails_{label}", append(terraform, "\n" + text + "\n"), [terraform, "sql_style"]) for label, text in QUOTED_FORMS.items()),
        *(
            guard_case(f"guard_fails_a_dispatcher_declaration_{label}", append("scripts/run_ci.sh", "\n" + text + "\n"), ["scripts/run_ci.sh", "sql_style"])
            for label, text in DISPATCHER_FORMS.items()
        ),
        *(
            guard_case(f"guard_passes_own_entry_{label}", replace_file(python_lint, f"#!/usr/bin/env bash\n{text} {{ :; }}\n"), [])
            for label, text in {
                "keyword_and_parentheses": "function python_lint()",
                "keyword_and_spaced_parentheses": "function python_lint ( )",
                "keyword_only": "function python_lint",
                "plain_spaced_parentheses": "python_lint ( )",
            }.items()
        ),
        *(
            guard_case(f"guard_passes_the_hostile_form_{label}", append(python_lint, "\n" + text + "\n"), [])
            for label, text in HOSTILE_FORMS.items()
            if not any(word in text for word in ("exit 0", "trap", "export", "unset", "if [[", "side_effect", "declare() "))
        ),
    ]


DRY_TIMING = """#!/usr/bin/env bash
timed() { printf 'TIMED %s\\n' "$*"; }
timing_table() { :; }
timing_entry() { :; }
"""
DRY_ACQUISITION = """#!/usr/bin/env bash
set -euo pipefail
shard="${ACQUISITION_SHARD:-}"
index="${shard%%/*}"
if [[ "${ACQUISITION_PART:-}" == "shard" ]]; then
  if [[ "$index" == "@FAIL@" ]]; then
    printf 'stand-in shard %s fails\\n' "$shard" >&2
    exit 1
  fi
  if [[ "$index" != "@SKIP@" ]]; then
    mkdir -p "$ACQUISITION_SHARD_DIR"
    touch "$ACQUISITION_SHARD_DIR/shard_${index}.marker"
  fi
fi
printf 'ACQ part=%s shard=%s count=%s dir=%s\\n' "${ACQUISITION_PART:-}" "$shard" "${ACQUISITION_SHARD_COUNT:-}" "${ACQUISITION_SHARD_DIR:-}"
"""


def dry_run(
    work: Path,
    out: Path,
    name: str,
    env: dict[str, str],
    fail_shard: str = "",
    skip_shard: str = "",
    prepare: Callable[[Path], None] | None = None,
    timing: str = "",
) -> tuple[int, str]:
    """Run the dispatcher with a ``timed`` that only prints the command and a stand-in acquisition entry: the order, grouping and arguments of a full run
    without running any check. ``fail_shard`` makes that shard fail and ``skip_shard`` makes it leave no folder."""
    (work / TIMING).write_text(timing or DRY_TIMING, encoding="utf-8")
    (work / "scripts/acquisition/run_checks.sh").write_text(DRY_ACQUISITION.replace("@FAIL@", fail_shard).replace("@SKIP@", skip_shard), encoding="utf-8")
    try:
        if prepare:
            prepare(work)
        code, log, _ = run_entry(work, out, name, env)
    finally:
        for path in (work / GROUP_FILES).glob("*.sh"):
            path.chmod(0o644)
        restore(work)
    return code, log


def local_plan_of(log: str) -> dict[str, Any]:
    """The groups, gold units, acquisition shards and combining steps that a dry run of the dispatcher started."""
    items = re.findall(r"^== (\w+) ==$", log, re.MULTILINE)
    groups: set[str] = {"launcher"} if "TIMED launcher E2E" in log else set()
    units: set[str] = set()
    for item in items:
        if item.startswith("lakehouse_gold_"):
            groups.add("lakehouse_gold")
            units.add(item.removeprefix("lakehouse_gold_"))
        elif item.startswith("lakehouse_features_"):
            groups.add("lakehouse_features")
        elif item.startswith("hook_"):
            groups.add("repository_hooks")
        elif not item.startswith(("acquisition_", "combine_feature_")):
            groups.add(item)
    shards = re.findall(r"^ACQ part=shard shard=(\d+/\d+) ", log, re.MULTILINE)
    combines = re.findall(r"^ACQ part=combine shard= count=(\d+) ", log, re.MULTILINE)
    if shards or combines:
        groups.add("acquisition")
    return {
        "items": items,
        "feature_shards": set(
            f"{suite}:{shard}"
            for suite, shard in re.findall(r"^TIMED lakehouse_features: feature (sources|columns) E2E .* --shard (\d+/\d+)", log, re.MULTILINE)
        ),
        "feature_combines": set(
            f"{suite}:{count}" for suite, count in re.findall(r"^TIMED lakehouse_features: combine (sources|columns)/(\d+) ", log, re.MULTILINE)
        ),
        "hook_groups": set(re.findall(r"^TIMED repository hooks: (\w+) ", log, re.MULTILINE)),
        "launcher_shards": set(re.findall(r"^TIMED launcher E2E shard (\d+/\d+) ", log, re.MULTILINE)),
        "groups": groups,
        "units": units,
        "shards": set(shards),
        "shard_runs": len(shards),
        "combine_counts": set(combines),
        "combine_runs": len(combines),
        "folders": re.findall(r"^ACQ part=shard .* dir=(\S+)$", log, re.MULTILINE),
    }


def github_plan_of(workflow: dict[str, Any]) -> dict[str, Any]:
    """Expand the real jobs into groups, units, feature and acquisition shards and hook partitions."""
    config = ci_groups.load()
    groups: set[str] = set()
    units: set[str] = set()
    shards: set[str] = set()
    combine_counts: set[str] = set()
    feature_shards: set[str] = set()
    feature_combines: set[str] = set()
    hook_groups: set[str] = set()
    launcher_shards: set[str] = set()
    for definition in workflow["jobs"].values():
        for entry in matrix_entries(definition):
            for step in definition.get("steps", []):
                env = {key: fill_matrix(str(value), entry) for key, value in step.get("env", {}).items()}
                command = fill_matrix(str(step.get("run", "")), entry)
                if command == "bash scripts/run_ci.sh" and env.get("CI_PART") not in {None, "gates"}:
                    part = env["CI_PART"]
                    groups |= set(ci_groups.groups_of(part, config)) - {"acquisition"}
                    if env.get("LAUNCHER_SHARD"):
                        launcher_shards.add(env["LAUNCHER_SHARD"])
                    if part == "gold":
                        units |= set((env.get("CI_UNITS") or " ".join(GOLD_UNITS)).split())
                    if part in {"feature_sources", "feature_columns"}:
                        feature_shards.add(f"{part.removeprefix('feature_')}:{env.get('FEATURE_SHARD', '')}")
                elif command == "bash scripts/acquisition/run_checks_ci.sh" and env.get("ACQUISITION_PART") == "shard":
                    groups.add("acquisition")
                    shards.add(env["ACQUISITION_SHARD"])
                elif command == "bash scripts/acquisition/run_checks_ci.sh" and env.get("ACQUISITION_PART") == "combine":
                    combine_counts.add(env["ACQUISITION_SHARD_COUNT"])
                if command.startswith(".venv/bin/python -m scripts.quality.ci_hooks "):
                    found = re.search(r"--group '([^']+)'", command)
                    if found:
                        groups.add("repository_hooks")
                        hook_groups.add(found[1])
                feature_combines |= {
                    f"{suite}:{count}" for suite, count in re.findall(r"feature_shards combine --suite (sources|columns) --count (\d+)", command)
                }
    return {
        "groups": groups,
        "units": units,
        "shards": shards,
        "combine_counts": combine_counts,
        "feature_shards": feature_shards,
        "feature_combines": feature_combines,
        "hook_groups": hook_groups,
        "launcher_shards": launcher_shards,
    }


def mirror_differences(github: dict[str, Any], local: dict[str, Any]) -> dict[str, Any]:
    """What differs between the GitHub jobs and the full local run; empty when the local run mirrors GitHub."""
    differences = {
        name: {"github": sorted(github[name]), "local": sorted(local[name])}
        for name in ("groups", "units", "shards", "combine_counts", "feature_shards", "feature_combines", "hook_groups", "launcher_shards")
        if github[name] != local[name]
    }
    if local["shard_runs"] != len(local["shards"]) or local["combine_runs"] != 1:
        differences["runs"] = {"shard_runs": local["shard_runs"], "combine_runs": local["combine_runs"]}
    return differences


def planted_workflows() -> dict[str, dict[str, Any]]:
    """Copies of the workflow with one missing shard, a different combining count, a missing gold unit or a missing feature job."""
    drifts = {}
    shards = copy.deepcopy(workflow_document())
    shards["jobs"]["acquisition_shard"]["strategy"]["matrix"]["shard"] = [1, 2, 3]
    drifts["three_shards"] = shards
    count = copy.deepcopy(workflow_document())
    count["jobs"]["acquisition"]["steps"][-1]["env"]["ACQUISITION_SHARD_COUNT"] = "3"
    drifts["combine_count"] = count
    unit = copy.deepcopy(workflow_document())
    unit["jobs"]["lakehouse"]["strategy"]["matrix"]["include"][2]["units"] = "checks"
    drifts["gold_unit"] = unit
    feature = copy.deepcopy(workflow_document())
    del feature["jobs"]["feature_sources"]
    drifts["feature_job"] = feature
    shard = copy.deepcopy(workflow_document())
    shard["jobs"]["feature_sources"]["strategy"]["matrix"]["shard"].pop()
    drifts["feature_shard"] = shard
    hooks = copy.deepcopy(workflow_document())
    hooks["jobs"]["hooks"]["strategy"]["matrix"]["include"].pop()
    drifts["hook_group"] = hooks
    launcher = copy.deepcopy(workflow_document())
    launcher["jobs"]["launcher"]["strategy"]["matrix"]["shard"].pop()
    drifts["launcher_shard"] = launcher
    return drifts


def mirror_cases(work: Path, out: Path) -> list[dict[str, Any]]:
    code, log = dry_run(work, out, "local_full_dry_run", {"CI_SCOPE": "full", "CI_PART": "all"})
    local = local_plan_of(log)
    github = github_plan_of(workflow_document())
    differences = mirror_differences(github, local)
    items = local["items"]
    first_item = re.search(r"^== \w+ ==$", log, re.MULTILINE)
    shard_items = [item for item in items if item.startswith("acquisition_shard_")]
    checks = {
        "exit_zero": code == 0 and PASS_LINES["all"] in log,
        "same_groups_units_shards_and_combine_as_the_github_jobs": not differences,
        # The sharded launcher block prints under "== launcher shard K/N ==" headers, which are not group items: compare with the first "== item ==" header.
        "launcher_ran_before_every_item": 0 <= log.find("TIMED launcher E2E") < (first_item.start() if first_item else -1),
        "writer_set_up_ran_alone_first": items[:1] == ["lakehouse_gold_writer_setup"],
        "shards_ran_with_the_groups": len(shard_items) == 4 and all(item in items[1:-1] for item in shard_items),
        "combine_ran_last_after_every_shard": items[-1:] == ["acquisition_combine"] and local["combine_counts"] == {"4"},
        "every_shard_has_its_own_folder": len(set(local["folders"])) == 4,
        "combine_used_the_merged_folder": re.search(r"^ACQ part=combine .* dir=\S+/acquisition_shards/merged$", log, re.MULTILINE) is not None,
    }
    cases = [{"case": "local_full_plan_matches_the_github_jobs", "passed": all(checks.values()), "checks": checks, "differences": differences, "exit": code}]
    planted = {name: mirror_differences(github_plan_of(workflow), local) for name, workflow in planted_workflows().items()}
    cases.append({"case": "local_plan_drift_from_the_github_jobs_is_reported", "passed": all(planted.values()), "differences": planted})
    return [*cases, *dry_run_cases(work, out), *isolation_cases(work, out)]


def dry_run_cases(work: Path, out: Path) -> list[dict[str, Any]]:
    cases = []
    code, log = dry_run(work, out, "local_shard_fails", {"CI_SCOPE": "full", "CI_PART": "all"}, fail_shard="2")
    checks = {
        "exit_nonzero": code != 0,
        "names_the_shard": "Local CI failed in: acquisition_shard_2" in log,
        "no_combine_step": "acquisition_combine" not in log and "part=combine" not in log,
        "no_pass_line": PASS_LINES["all"] not in log,
    }
    cases.append({"case": "local_shard_failure_stops_the_run_before_the_combine_step", "passed": all(checks.values()), "checks": checks, "exit": code})
    code, log = dry_run(work, out, "local_shard_missing", {"CI_SCOPE": "full", "CI_PART": "all"}, skip_shard="3")
    checks = {
        "exit_nonzero": code != 0,
        "names_the_combine_step": "Local CI failed in: acquisition_combine" in log,
        "names_the_missing_folder": "shard_3" in log,
        "no_pass_line": PASS_LINES["all"] not in log,
    }
    cases.append({"case": "local_missing_shard_fails_the_combine_step", "passed": all(checks.values()), "checks": checks, "exit": code})

    def remove_silver(tree: Path) -> None:
        (tree / GROUP_FILES / "lakehouse_silver.sh").unlink()

    def lock_silver(tree: Path) -> None:
        (tree / GROUP_FILES / "lakehouse_silver.sh").chmod(0)

    for label, prepare in (("missing", remove_silver), ("unreadable", lock_silver)):
        code, log = dry_run(work, out, f"local_body_file_{label}", {"CI_SCOPE": "full", "CI_PART": "silver"}, prepare=prepare)
        checks = {
            "exit_nonzero": code != 0,
            "names_the_file_and_group": f"{GROUP_FILES}/lakehouse_silver.sh of the check group lakehouse_silver" in log,
            "the_group_never_ran": "TIMED lakehouse_silver" not in log and PASS_LINES["silver"] not in log,
        }
        cases.append({"case": f"body_file_{label}_at_run_time_fails_with_its_name", "passed": all(checks.values()), "checks": checks, "exit": code})

    def stage_sql_body(tree: Path) -> None:
        path = tree / GROUP_FILES / "sql_style.sh"
        path.write_text(path.read_text(encoding="utf-8") + "\n# staged edit\n", encoding="utf-8")
        git(tree, "add", f"{GROUP_FILES}/sql_style.sh")

    code, log = dry_run(work, out, "local_staged_body_edit", {"CI_SCOPE": "staged", "CI_PART": "all"}, prepare=stage_sql_body)
    local = local_plan_of(log)
    checks = {
        "exit_zero": code == 0 and PASS_LINES["all"] in log,
        "only_its_group_and_the_repository_group_ran": sorted(local["items"]) == ["repository_code_checks", "sql_style"],
        # Every path selects the launcher while a followed module carries the inventory_reads pattern ".", so the dry run starts it too.
        "launcher_selected_by_every_path": "TIMED launcher E2E" in log and local["groups"] == {"launcher", "repository_code_checks", "sql_style"},
        "no_acquisition_shard_ran": not local["shards"] and not local["combine_runs"] and "ACQ " not in log,
    }
    cases.append({"case": "staged_body_edit_runs_only_its_group_items", "passed": all(checks.values()), "checks": checks, "exit": code})
    return cases


# Hostile forms whose effect is visible in the output of python_lint itself: they prove that the hostile text really ran in that group's process.
OWN_GROUP_CHANGES = {"timed_override", "top_level_exit", "environment_poisoned", "exit_trap_that_prints"}


def item_blocks(log: str) -> tuple[str, dict[str, str]]:
    """The text before the first item of a dry run and the text of each item (its log and its seconds row), with seconds, stamps and scratch names replaced."""
    log = re.sub(r"\d{8}T\d{6}Z_\d+", "<stamp>", re.sub(r"hai_ci\.\w+", "hai_ci.X", re.sub(r"^(.*): \d+ s$", r"\1: N s", log, flags=re.MULTILINE)))
    head, *parts = re.split(r"^== (\w+) ==$", log, flags=re.MULTILINE)
    blocks = {name: re.split(r"\n(?:All local CI checks passed|Local CI failed)", body)[0] for name, body in zip(parts[::2], parts[1::2], strict=True)}
    return head, blocks


def isolation_cases(work: Path, out: Path) -> list[dict[str, Any]]:
    """Each group runs in its own shell process, so a hostile group file leaves the shell state of the other groups and of the dispatcher unchanged: with
    each form of HOSTILE_FORMS appended to python_lint.sh, every other group of a full core run prints exactly what it printed without it, the part
    before the groups is the same and only python_lint may fail."""
    env = {"CI_SCOPE": "full", "CI_PART": "core"}
    base_head, base = item_blocks(dry_run(work, out, "hostile_baseline", env)[1])
    cases = []
    for label, text in HOSTILE_FORMS.items():

        def hostile(tree: Path, text: str = text) -> None:
            path = tree / GROUP_FILES / "python_lint.sh"
            path.write_text(path.read_text(encoding="utf-8") + "\n" + text + "\n", encoding="utf-8")

        code, log = dry_run(work, out, f"hostile_{label}", env, prepare=hostile)
        head, blocks = item_blocks(log)
        failed = " ".join(re.findall(r"^Local CI failed in: (.*)$", log, flags=re.MULTILINE)).split()
        checks = {
            "the_other_groups_print_what_they_printed_without_it": {k: v for k, v in blocks.items() if k != "python_lint"}
            == {k: v for k, v in base.items() if k != "python_lint"},
            "every_group_still_ran": sorted(blocks) == sorted(base),
            "the_part_before_the_groups_is_unchanged": head == base_head,
            "only_python_lint_may_fail": set(failed) <= {"python_lint"},
            "nothing_wrong_outside_python_lint": "WRONG_SQL" not in "".join(v for k, v in blocks.items() if k != "python_lint") + head,
        }
        if label in OWN_GROUP_CHANGES:
            checks["the_hostile_file_did_change_its_own_group"] = blocks.get("python_lint") != base["python_lint"]
        cases.append({"case": f"hostile_group_file_{label}_changes_only_its_own_group", "passed": all(checks.values()), "checks": checks, "exit": code})
    return cases


STAND_IN_ENTRIES = {
    "lakehouse_gold": ["lakehouse_gold_checks", "lakehouse_gold_writer_setup", "lakehouse_gold_model_metadata_e2e"],
    "launcher": ["launcher_e2e"],
    "lakehouse_features": ["lakehouse_features_sources", "lakehouse_features_columns", "lakehouse_features_combine", "lakehouse_features_store"],
}
# A stand-in group file reads every variable that run_group passes to a group, so an unbound variable stops the run, and runs one real timed command.
STAND_IN_BODY = """#!/usr/bin/env bash
{name}() {{
  : "$python_bin" "$repo_root" "$scratch" "$quality_output" "$e2e_output" "$CI_TIMING_FILE"
  timed "stand-in: {name}" true
}}
"""
PYTHON_FILTER = """#!/usr/bin/env bash
# Stand-in for the interpreter of the gates: only the group selection runs for real.
if [[ "$*" == *scripts.quality.ci_groups* || "$*" == *config/quality/ci_groups.json* ]]; then
  exec {python} "$@"
fi
exit 0
"""
HOSTED_FOLDERS = ("quality_output", "e2e_output", "QUALITY_ARTIFACT_DIR", "E2E_ARTIFACT_DIR")


def stand_in_run(work: Path, out: Path, name: str, extra: dict[str, str], expected: list[str], staged: bool = False) -> dict[str, Any]:
    """Run the real dispatcher with the real timing wrapper and stand-in group files, in the environment of a hosted job: neither artifact folder is set
    (the gates run through a filter that only lets the group selection run). An unbound variable in the dispatcher or in a group process fails the run."""
    for group in BODY_SELECTION:
        entries = STAND_IN_ENTRIES.get(group, [group])
        (work / GROUP_FILES / f"{group}.sh").write_text(
            "#!/usr/bin/env bash\n" + "".join(STAND_IN_BODY.format(name=entry).split("\n", 1)[1] for entry in entries), encoding="utf-8"
        )
    (work / "scripts/acquisition/run_checks.sh").write_text(DRY_ACQUISITION.replace("@FAIL@", "").replace("@SKIP@", ""), encoding="utf-8")
    filter_path = work / "python_filter.sh"
    filter_path.write_text(PYTHON_FILTER.format(python=PYTHON), encoding="utf-8")
    filter_path.chmod(0o755)
    copy = stage_launcher_path(work) if staged else None
    environment = {key: value for key, value in os.environ.items() if key not in HOSTED_FOLDERS}
    try:
        result = run_command(
            "bash",
            ["scripts/run_ci.sh"],
            cwd=work,
            env={**environment, "GITHUB_STEP_SUMMARY": str(out / f"{name}_summary.md"), "PYTHON_BIN": str(filter_path), "HAI_CI_REUSE": "off", **extra},
            timeout=600,
        )
    finally:
        if copy:
            copy.unlink(missing_ok=True)
        restore(work)
        filter_path.unlink(missing_ok=True)
    log = result.stdout + result.stderr
    (out / f"{name}.log").write_text(log, encoding="utf-8")
    checks = {
        "exit_zero": result.returncode == 0,
        "no_unbound_variable": "unbound variable" not in log,
        "reached_the_end": PASS_LINES[extra["CI_PART"]] in log,
        "every_expected_stand_in_ran": all(f"[timing] end stand-in: {entry} at" in log for entry in expected),
    }
    return {"case": f"real_run_{name}_sets_every_variable_a_group_receives", "passed": all(checks.values()), "checks": checks, "exit": result.returncode}


def variable_cases(work: Path, out: Path) -> list[dict[str, Any]]:
    everything = [entry for group in BODY_SELECTION for entry in STAND_IN_ENTRIES.get(group, [group])]
    core = [
        entry
        for entry in everything
        if entry not in {"launcher_e2e", "reuse_gate_checks", *STAND_IN_ENTRIES["lakehouse_gold"], *STAND_IN_ENTRIES["lakehouse_features"], *LAYERS}
    ]
    layers = {
        "bronze": ["lakehouse_bronze"],
        "silver": ["lakehouse_silver"],
        "gold": ["lakehouse_gold_checks", "lakehouse_gold_writer_setup", "lakehouse_gold_model_metadata_e2e"],
        "features": STAND_IN_ENTRIES["lakehouse_features"],
        "feature_sources": ["lakehouse_features_sources", "lakehouse_features_combine"],
        "feature_columns": ["lakehouse_features_columns", "lakehouse_features_combine"],
    }
    cases = [
        stand_in_run(work, out, "full_all", {"CI_SCOPE": "full", "CI_PART": "all"}, everything),
        stand_in_run(work, out, "full_core", {"CI_SCOPE": "full", "CI_PART": "core"}, core),
        stand_in_run(work, out, "full_launcher", {"CI_SCOPE": "full", "CI_PART": "launcher"}, ["launcher_e2e"]),
        *(stand_in_run(work, out, f"full_{part}", {"CI_SCOPE": "full", "CI_PART": part}, entries) for part, entries in layers.items()),
        stand_in_run(
            work,
            out,
            "range_unusable_all",
            {"CI_SCOPE": "range", "CI_PART": "all", "RANGE_START": ZERO, "RANGE_END": INFRA_ONLY},
            everything,
        ),
        stand_in_run(
            work, out, "range_unusable_launcher", {"CI_SCOPE": "range", "CI_PART": "launcher", "RANGE_START": ZERO, "RANGE_END": INFRA_ONLY}, ["launcher_e2e"]
        ),
        stand_in_run(work, out, "staged_launcher_launcher", {"CI_SCOPE": "staged", "CI_PART": "launcher"}, ["launcher_e2e"], staged=True),
        stand_in_run(work, out, "staged_launcher_all", {"CI_SCOPE": "staged", "CI_PART": "all"}, ["launcher_e2e", "python_lint"], staged=True),
    ]
    return cases


FAST_COMMIT_HOOKS = frozenset(
    {
        "gitleaks",
        "credential-files",
        "data-files",
        "removed-names",
        "privacy-scan",
        "writing-check",
        "front-matter",
        "markdownlint",
        "ruff",
        "shell-syntax",
        "shellcheck",
        "actionlint",
        "yamllint",
        "hadolint",
        "gofmt",
        "go-vet",
        "documentation-review",
        "lint-settings",
        "suppressions",
        "subprocess-imports",
        "docker-calls",
        "container-mounts",
        "check-jsonschema",
        "import-linter",
        "tflint",
        "trivy-config",
        "typos",
        "zizmor",
    }
)
PUSH_HOOKS = frozenset({"local_ci_pushed", "acquisition-checks"})
INSTALL_TYPES = ("pre-commit", "commit-msg", "pre-push")
FINGERPRINT_KEYS = ("entry", "language", "files", "exclude", "types", "types_or", "always_run", "pass_filenames", "verbose", "args")
# What the hosted hooks job runs: every hook of the manual stage, as it was before the commit stage was reduced, and the seven linters of lint_tools.
MANUAL_FINGERPRINTS = {
    "gitleaks-outgoing": "f1c92f2c178a",
    "credential-files": "9d1f2d4ee86b",
    "data-files": "1681a06bc29d",
    "env-example": "30d8bdad529c",
    "removed-names": "acffa6ea5eac",
    "unused-code": "b94f09bd9190",
    "unused-dependencies": "66cd9ab1759e",
    "orphan-files": "517c80728999",
    "terraform-unused": "a2bc8e9b59b5",
    "privacy-scan": "ce896f9743dd",
    "writing-check": "107b4e7bb1d7",
    "front-matter": "82a03c2777b3",
    "markdownlint": "28a824e49edc",
    "sqlfluff": "7e868c7c452f",
    "ruff": "5f2147654150",
    "shell-syntax": "f63508f2a1fc",
    "shellcheck": "f0e1e106ec2c",
    "actionlint": "f19f2e73cdb9",
    "yamllint": "455872e07694",
    "hadolint": "9234e13781be",
    "build-contexts": "8ab075ce9c74",
    "container-mounts": "8be103fa11d5",
    "gofmt": "de0cf2f5169c",
    "go-vet": "5bcd441b4c87",
    "model-metadata": "50f5ae35844c",
    "documentation-review-untracked": "f6fa8c87e0d3",
    "lint-settings": "60a35b84518b",
    "suppressions": "eda6794008b9",
    "subprocess-imports": "7bae5ddf818b",
    "docker-calls": "2f3e46d94779",
    "tflint": "1cea729b9600",
    "trivy-config": "7303359ce5bc",
    "zizmor": "a7d8a0e15bb0",
    "import-linter": "5409790957ac",
    "check-jsonschema": "3b0e5a36602c",
    "typos": "61801f4f88b0",
    "osv-scanner": "a3e0e7a4f358",
    "test-checks": "0328ad4bb1bd",
    "test-documentation-review": "db426284e54a",
}


def hook_table(document: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {hook["id"]: hook for repo in document["repos"] for hook in repo["hooks"]}


def stage_set(document: dict[str, Any], hook: dict[str, Any]) -> set[str]:
    return set(hook.get("stages", document.get("default_stages", [])))


def hook_config_problems(document: dict[str, Any], push_plan_groups: list[str], partitions: dict[str, list[str]]) -> dict[str, Any]:
    """Every way the hook configuration can leave the fast commit stage, the pre-push run or the hosted manual stage wrong."""
    hooks = hook_table(document)
    commit = {name for name, hook in hooks.items() if "pre-commit" in stage_set(document, hook)}
    manual = {name for name, hook in hooks.items() if "manual" in stage_set(document, hook) and name != "local_ci"}
    fingerprints = {
        name: hashlib.sha256(json.dumps({key: hooks[name].get(key) for key in FINGERPRINT_KEYS}, sort_keys=True).encode()).hexdigest()[:12] for name in manual
    }
    moved = sorted(set(MANUAL_FINGERPRINTS) - commit)
    partitioned = {name for names in partitions.values() for name in names}
    return {
        "heavy_in_commit_stage": sorted(commit - FAST_COMMIT_HOOKS),
        "fast_missing_from_commit_stage": sorted(FAST_COMMIT_HOOKS - commit),
        "install_types": [kind for kind in INSTALL_TYPES if kind not in document.get("default_install_hook_types", [])],
        "push_hooks_not_in_push_stage": sorted(name for name in PUSH_HOOKS if stage_set(document, hooks.get(name, {})) != {"pre-push"}),
        "manual_changed": sorted(name for name in MANUAL_FINGERPRINTS if fingerprints.get(name) != MANUAL_FINGERPRINTS[name])
        + sorted(manual - set(MANUAL_FINGERPRINTS)),
        "moved_not_run_before_push": sorted(name for name in moved if name not in partitioned) + sorted(set(partitions) - set(push_plan_groups)),
        "local_ci_affected_present": ["local_ci_affected"] if "local_ci_affected" in hooks else [],
    }


def fast_hook_cases(scratch: Path) -> list[dict[str, Any]]:
    """The commit stage holds only the fast hooks, the push stage holds the pre-push run, the manual stage is what hosted CI ran before."""
    document = load_yaml((ROOT / ".pre-commit-config.yaml").read_text())
    config = json.loads((ROOT / "config/quality/ci_groups.json").read_text())
    if not isinstance(document, dict):
        raise ValueError(".pre-commit-config.yaml must contain a mapping")
    partitions = config["hook_groups"]
    plan = pre_push_plan(pre_push_repo(scratch, "fast_hook_plan"))
    groups = [step["name"].removeprefix("hooks_") for step in plan["steps"] if step["name"].startswith("hooks_")]
    real = hook_config_problems(document, groups, partitions)
    names = {
        "commit_stage_holds_only_fast_hooks": ("heavy_in_commit_stage", "fast_missing_from_commit_stage", "local_ci_affected_present"),
        "pre_push_hooks_declared": ("install_types", "push_hooks_not_in_push_stage"),
        "manual_stage_unchanged": ("manual_changed",),
        "moved_hooks_run_before_push": ("moved_not_run_before_push",),
    }
    results = [{"case": case, "passed": not any(real[key] for key in keys), **{key: real[key] for key in keys}} for case, keys in names.items()]

    def planted(label: str, mutate: Callable[[dict[str, Any], dict[str, list[str]]], None], key: str, case: str) -> dict[str, Any]:
        damaged = copy.deepcopy(document)
        damaged_partitions = copy.deepcopy(partitions)
        mutate(damaged, damaged_partitions)
        found = hook_config_problems(damaged, groups, damaged_partitions)
        return {"case": f"planted_{label}_caught_by_{case}", "passed": bool(found[key]), "caught": found[key]}

    def add_stage(name: str, stage: str) -> Callable[[dict[str, Any], dict[str, list[str]]], None]:
        def mutate(document: dict[str, Any], _: dict[str, list[str]]) -> None:
            hook = hook_table(document)[name]
            hook["stages"] = [*hook.get("stages", document["default_stages"]), stage]

        return mutate

    def drop_stage(name: str, stage: str) -> Callable[[dict[str, Any], dict[str, list[str]]], None]:
        def mutate(document: dict[str, Any], _: dict[str, list[str]]) -> None:
            hook = hook_table(document)[name]
            hook["stages"] = [kind for kind in hook.get("stages", document["default_stages"]) if kind != stage]

        return mutate

    def drop_install_type(document: dict[str, Any], _: dict[str, list[str]]) -> None:
        document["default_install_hook_types"] = [kind for kind in document["default_install_hook_types"] if kind != "pre-push"]

    def edit_entry(document: dict[str, Any], _: dict[str, list[str]]) -> None:
        hook_table(document)["unused-code"]["entry"] += " --changed"

    def unpartition(_: dict[str, Any], partitions: dict[str, list[str]]) -> None:
        partitions["code"].remove("orphan-files")

    results += [
        planted("heavy_hook_in_commit_stage", add_stage("unused-code", "pre-commit"), "heavy_in_commit_stage", "commit_stage_holds_only_fast_hooks"),
        planted(
            "privacy_gate_dropped_from_commit_stage",
            drop_stage("privacy-scan", "pre-commit"),
            "fast_missing_from_commit_stage",
            "commit_stage_holds_only_fast_hooks",
        ),
        planted("pre_push_install_type_missing", drop_install_type, "install_types", "pre_push_hooks_declared"),
        planted(
            "acquisition_hook_back_in_commit_stage", add_stage("acquisition-checks", "pre-commit"), "push_hooks_not_in_push_stage", "pre_push_hooks_declared"
        ),
        planted("moved_hook_loses_manual_stage", drop_stage("orphan-files", "manual"), "manual_changed", "manual_stage_unchanged"),
        planted("manual_hook_entry_edited", edit_entry, "manual_changed", "manual_stage_unchanged"),
        planted("moved_hook_in_no_partition", unpartition, "moved_not_run_before_push", "moved_hooks_run_before_push"),
    ]
    return results


# The paths of a pre-push range that select the full scope, and ordinary paths that stay in the range scope.
ESCALATING_PATHS = (
    "scripts/run_ci.sh",
    "scripts/quality/ci_groups.py",
    "scripts/quality/ci_hooks.py",
    "scripts/quality/run_ci_groups_e2e.py",
    "scripts/quality/timing.sh",
    "config/quality/ci_groups.json",
    "scripts/ci/groups/launcher.sh",
    ".github/workflows/ci.yml",
    ".github/actions/setup/action.yml",
    ".pre-commit-config.yaml",
    "scripts/acquisition/run_checks_ci.sh",
    "scripts/acquisition/run_checks_ci_e2e.sh",
    "scripts/acquisition/run_checks_pushed.sh",
    "scripts/acquisition/run_checks_combine.sh",
    "scripts/acquisition/run_checks_shard.sh",
    "scripts/acquisition/shards.py",
)
ORDINARY_PATHS = (
    "README.md",
    "docs/quality_checks.md",
    "scripts/lakehouse/silver.py",
    "dbt/models/gold/fact_example.sql",
    "scripts/acquisition/s3_store.py",
    "scripts/acquisition/run_checks.sh",
)
INHERITED_PATTERN = (
    "^(CI_|RANGE_|LAUNCHER_|FEATURE_|ACQUISITION_|_?PRE_COMMIT_|SKIP$|GITHUB_ACTIONS$|HAI_DOCKER_RELEASE$|HAI_CI_REUSE$|"
    "GIT_DIR$|GIT_WORK_TREE$|GIT_INDEX_FILE$)"
)
NOT_CONTROLS = frozenset(
    {
        "BASH",
        "BASH_SOURCE",
        "SECONDS",
        "TMPDIR",
        "PYTHON_BIN",
        "CI_TIMING_FILE",
        "GITHUB_STEP_SUMMARY",
        "TF_PLUGIN_CACHE_DIR",
        "COVERAGE_FILE",
        "QUALITY_ARTIFACT_DIR",
        "POLICY_ARTIFACT_DIR",
        "E2E_ARTIFACT_DIR",
        "MODEL_METADATA_WORKERS",
        "AWS_CONFIG_FILE",
        "AWS_SHARED_CREDENTIALS_FILE",
        "STUB_MARKER",
        "STUB_EXIT",
    }
)
CONTROL_SCRIPTS = ("scripts/run_ci.sh", "scripts/quality/timing.sh", "scripts/acquisition/shards.py")
CONTROL_PATTERNS = ("scripts/ci/groups/*.sh", "scripts/acquisition/run_checks*.sh")
STUB_LOG_PROGRAM = (
    "import json, os, re, sys\n"
    "keys = ('PRE_COMMIT_FROM_REF', 'PRE_COMMIT_TO_REF')\n"
    "seen = ','.join(sorted(name for name in os.environ if re.match(%r, name)))\n"
    "with open(os.environ['STUB_LOG'], 'a') as log:\n"
    "    log.write(json.dumps({'stub': %r, 'argv': sys.argv[1:], 'env': {k: os.environ.get(k) for k in keys}, 'seen': seen}) + '\\n')\n"
    "sys.exit(int(os.environ.get('STUB_EXIT_%s', '0')))\n"
)
STUB_HOOK_PROGRAM = (
    "import json, os, re, sys, time\n"
    "keys = ('PRE_COMMIT_FROM_REF', 'PRE_COMMIT_TO_REF')\n"
    "seen = ','.join(sorted(name for name in os.environ if re.match(%r, name)))\n"
    "lock, collided = os.environ.get('STUB_INDEX_LOCK', ''), False\n"
    "if lock:\n"
    "    try:\n"
    "        os.close(os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY))\n"
    "    except FileExistsError:\n"
    "        collided = True\n"
    "        sys.stderr.write('fatal: Unable to create index.lock: File exists.\\n')\n"
    "if not collided:\n"
    "    time.sleep(float(os.environ.get('STUB_HOLD', '0')))\n"
    "    if lock:\n"
    "        os.remove(lock)\n"
    "row = {'stub': 'ci_hooks', 'argv': sys.argv[1:], 'env': {k: os.environ.get(k) for k in keys}, 'seen': seen, 'collided': collided}\n"
    "with open(os.environ['STUB_LOG'], 'a') as log:\n"
    "    log.write(json.dumps(row) + '\\n')\n"
    "sys.exit(128 if collided else int(os.environ.get('STUB_EXIT_CI_HOOKS', '0')))\n"
)
STUB_RUN_CI = """#!/usr/bin/env bash
seen="$(env | cut -d= -f1 | grep -E '@PATTERN@' | sort | paste -sd, - || true)"
overlap=unchecked
if [[ -n "${STUB_WAIT_FOR_LOCK:-}" ]]; then
  overlap=false
  for _ in $(seq 100); do
    if [[ -e "$STUB_WAIT_FOR_LOCK" ]]; then overlap=true; break; fi
    sleep 0.1
  done
fi
printf '{"stub": "run_ci", "env": {"CI_SCOPE": "%s", "RANGE_START": "%s", "RANGE_END": "%s", "HAI_DOCKER_RELEASE": "%s", ' \\
  "${CI_SCOPE-}" "${RANGE_START-unset}" "${RANGE_END-unset}" "${HAI_DOCKER_RELEASE-unset}" >>"$STUB_LOG"
printf '"HAI_CI_REUSE": "%s"}, "seen": "%s", "overlap": "%s"}\\n' "${HAI_CI_REUSE-unset}" "$seen" "$overlap" >>"$STUB_LOG"
if [[ -n "${STUB_SLEEP:-}" ]]; then exec sleep "$STUB_SLEEP"; fi
exit "${STUB_EXIT_RUN_CI:-0}"
""".replace("@PATTERN@", INHERITED_PATTERN)
# The stand-in launcher of the release: it logs its arguments and answers as STUB_RELEASE says (quit, skipped, refused, error, garbage or sleep).
STUB_LAUNCHER = (
    "import json, os, re, sys, time\n"
    "seen = ','.join(sorted(name for name in os.environ if re.match(%r, name)))\n"
    "with open(os.environ['STUB_LOG'], 'a') as log:\n"
    "    log.write(json.dumps({'stub': 'launcher', 'argv': sys.argv[1:], 'seen': seen}) + '\\n')\n"
    "mode = os.environ.get('STUB_RELEASE', 'quit')\n"
    "if mode == 'quit':\n"
    "    print(json.dumps({'outcome': 'quit', 'catalog': 'down', 'seconds': 1.5}))\n"
    "elif mode == 'skipped':\n"
    "    print(json.dumps({'outcome': 'skipped', 'reason': 'no container ran since T'}))\n"
    "elif mode == 'refused':\n"
    "    sys.stderr.write('launcher: refused: docker_release is refused: running containers not created by this launcher: other_db, other_cache\\n')\n"
    "    sys.exit(3)\n"
    "elif mode == 'error':\n"
    "    sys.stderr.write('launcher: docker quit failed: boom [command_failed]\\n')\n"
    "    sys.exit(4)\n"
    "elif mode == 'garbage':\n"
    "    print('not json')\n"
    "elif mode == 'sleep':\n"
    "    time.sleep(60)\n"
)
STUB_ACQUISITION = """#!/usr/bin/env bash
collided=false
if [[ -n "${STUB_INDEX_LOCK:-}" ]]; then
  if ( set -o noclobber; : >"$STUB_INDEX_LOCK" ) 2>/dev/null; then :; else
    collided=true
    echo 'fatal: Unable to create index.lock: File exists.' >&2
  fi
fi
if [[ "$collided" == false ]]; then
  sleep "${STUB_HOLD:-0}"
  if [[ -n "${STUB_INDEX_LOCK:-}" ]]; then rm -f "$STUB_INDEX_LOCK"; fi
fi
printf '{"stub": "acquisition", "argv": [], "collided": %s}\\n' "$collided" >>"$STUB_LOG"
if [[ "$collided" == true ]]; then exit 128; fi
"""
GIT_IDENTITY = {"GIT_AUTHOR_NAME": "Probe", "GIT_COMMITTER_NAME": "Probe", "GIT_AUTHOR_EMAIL": "", "GIT_COMMITTER_EMAIL": ""}
SHIM_NAME = "hai-push-input"
SHIM_COMMAND_LINE_VALUE = ".venv/bin/python -m scripts.quality.pre_push run --hook"
SHIM_COMMAND_LINE = f'SHIM_COMMAND = "{SHIM_COMMAND_LINE_VALUE}"'


def probe_git(root: Path, *args: str) -> str:
    result = run_command("git", ["-C", str(root), *args], env={**os.environ, **GIT_IDENTITY}, timeout=120)
    if result.returncode != 0:
        raise RuntimeError(f"git {' '.join(args)}: {result.stderr.strip()}")
    return result.stdout.strip()


def pre_push_repo(
    scratch: Path,
    name: str,
    changed: str | tuple[str, ...] = "README.md",
    remote: bool = True,
    timeout: int | None = None,
    mutate: Callable[[str], str] | None = None,
    config_edit: Callable[[str], str] | None = None,
    small: dict[str, Any] | None = None,
) -> Path:
    """A small repository with the real selector configuration, a copy of the pre-push run and stand-ins for the commands a push runs.

    One topic commit per path in ``changed`` follows the base commit; ``mutate`` rewrites the copy of ``scripts/quality/pre_push.py`` to plant a defect.
    No range is a small change unless ``small`` overrides the thresholds of ``pre_push.small_change``.
    """
    root = scratch / name
    suffix = 1
    while root.exists():
        suffix += 1
        root = scratch / f"{name}_{suffix}"
    for folder in ("config/quality", "scripts/quality", "scripts/acquisition", ".github/workflows", "orchestration"):
        (root / folder).mkdir(parents=True, exist_ok=True)
    (root / ".venv").symlink_to(ROOT / ".venv")
    config = json.loads((ROOT / "config/quality/ci_groups.json").read_text())
    if timeout is not None:
        config["pre_push"]["timeout_seconds"] = {"range": timeout, "full": timeout}
    config["pre_push"]["small_change"] = {**config["pre_push"]["small_change"], **({"max_files": 0} if small is None else small)}
    (root / "config/quality/ci_groups.json").write_text(json.dumps(config))
    (root / "scripts/run_ci.sh").write_text(STUB_RUN_CI)
    (root / "scripts/quality/ci_changed_files.py").write_text(STUB_LOG_PROGRAM % (INHERITED_PATTERN, "changed_files", "CHANGED_FILES"))
    (root / "scripts/quality/ci_hooks.py").write_text(STUB_HOOK_PROGRAM % INHERITED_PATTERN)
    (root / "scripts/quality/repo_checks.py").write_text(STUB_LOG_PROGRAM % (INHERITED_PATTERN, "repo_checks", "REPO_CHECKS"))
    (root / "scripts/quality/lint_tools.py").write_text(STUB_LOG_PROGRAM % (INHERITED_PATTERN, "osv_scanner", "OSV_SCANNER"))
    for package in ("scripts", "scripts/quality", "orchestration"):
        (root / package / "__init__.py").write_text("")
    (root / "orchestration/launcher.py").write_text(STUB_LAUNCHER % INHERITED_PATTERN)
    shutil.copy2(ROOT / "scripts/process.py", root / "scripts/process.py")
    source = (ROOT / "scripts/quality/pre_push.py").read_text()
    (root / "scripts/quality/pre_push.py").write_text(mutate(source) if mutate else source)
    shutil.copy2(ROOT / "scripts/acquisition/run_checks_pushed.sh", root / "scripts/acquisition/run_checks_pushed.sh")
    hook_config = (ROOT / ".pre-commit-config.yaml").read_text()
    (root / ".pre-commit-config.yaml").write_text(config_edit(hook_config) if config_edit else hook_config)
    (root / "scripts/acquisition/run_checks.sh").write_text(STUB_ACQUISITION)
    (root / "README.md").write_text("base\n")
    probe_git(root, "init", "--quiet", "--initial-branch", "main")
    probe_git(root, "add", "--all")
    probe_git(root, "-c", "commit.gpgsign=false", "commit", "--quiet", "--message", "base")
    if remote:
        probe_git(root, "update-ref", "refs/remotes/origin/main", "HEAD")
    probe_git(root, "checkout", "--quiet", "-b", "topic")
    for changed_path in (changed,) if isinstance(changed, str) else changed:
        target = root / changed_path
        target.parent.mkdir(parents=True, exist_ok=True)
        previous = target.read_text() if target.exists() else ""
        target.write_text(previous + ("\n" if changed_path.endswith(".json") else "\n# topic change\n"))
        probe_git(root, "add", "--all")
        probe_git(root, "-c", "commit.gpgsign=false", "commit", "--quiet", "--message", "topic")
    return root


def pre_push(
    root: Path, command: str, log: Path | None = None, extra: dict[str, str] | None = None, timeout: int = 120, arguments: tuple[str, ...] = ()
) -> tuple[int, str, str]:
    environment = {key: value for key, value in os.environ.items() if not key.startswith("PRE_COMMIT_")}
    environment.update({"STUB_LOG": str(log or root / "stub.log"), **(extra or {})})
    result = run_command(str(PYTHON), ["-m", "scripts.quality.pre_push", command, "--root", str(root), *arguments], cwd=root, env=environment, timeout=timeout)
    return result.returncode, result.stdout, result.stderr


def pre_push_plan(root: Path, arguments: tuple[str, ...] = ()) -> dict[str, Any]:
    """The plan of ``pre_push plan``; a plan that cannot be made has the scope ``<no plan>`` and no steps."""
    code, out, error = pre_push(root, "plan", arguments=arguments)
    try:
        return json.loads(out) if code == 0 else {"scope": "<no plan>", "reason": error[-300:], "start": "", "end": "", "steps": []}
    except json.JSONDecodeError:
        return {"scope": "<no plan>", "reason": out[-300:], "start": "", "end": "", "steps": []}


def pre_push_calls(root: Path) -> list[dict[str, Any]]:
    log = root / "stub.log"
    return [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []


def escalation_case(scratch: Path, name: str, path: str, want_scope: str, timeout: int | None = None) -> dict[str, Any]:
    root = pre_push_repo(scratch, name, path)
    plan = pre_push_plan(root)
    return {"case": name, "passed": plan["scope"] == want_scope, "scope": plan["scope"], "want": want_scope, "path": path}


def hook_partition_case(scratch: Path, mutate: Callable[[str], str] | None = None) -> dict[str, Any]:
    """The acquisition hook and the hook partitions share one worktree and its index lock, so they run one after the other beside the other steps.

    Both pre-commit paths (the acquisition hook, which ``pre-commit run`` starts after its ``git write-tree``, and the partitions) take a lock file like
    ``index.lock`` in their stand-ins and exit 128 when it exists; the ``local_ci`` stand-in waits for that lock to be held.
    The range reaches the acquisition hook.
    """
    root = pre_push_repo(scratch, "pre_push_hook_partitions", ("scripts/acquisition/example.py", "README.md"), mutate=mutate)
    lock = root / "index.lock"
    code, out, error = pre_push(root, "run", extra={"STUB_INDEX_LOCK": str(lock), "STUB_HOLD": "5", "STUB_WAIT_FOR_LOCK": str(lock)}, timeout=300)
    calls = pre_push_calls(root)
    hooks = [call for call in calls if call["stub"] == "ci_hooks"]
    acquisition = [call for call in calls if call["stub"] == "acquisition"]
    ci = [call["overlap"] for call in calls if call["stub"] == "run_ci"]
    groups = list(json.loads((ROOT / "config/quality/ci_groups.json").read_text())["hook_groups"])
    collided = [call["argv"][call["argv"].index("--group") + 1] for call in hooks if call["collided"]]
    acquisition_collided = [call["collided"] for call in acquisition]
    return {
        "case": "pre_push_hook_partitions_run_one_after_the_other",
        "passed": code == 0 and len(hooks) == len(groups) and not collided and len(acquisition) == 1 and acquisition_collided == [False] and ci == ["true"],
        "exit": code,
        "partitions": len(hooks),
        "collided": collided,
        "acquisition_runs": len(acquisition),
        "acquisition_collided": acquisition_collided,
        "local_ci_saw_the_lock_held": ci,
        "output": (out + error)[-300:],
    }


def hook_failure_case(scratch: Path) -> dict[str, Any]:
    """A failed hook partition does not stop the later ones, and the failure report names every failed partition."""
    root = pre_push_repo(scratch, "pre_push_hook_failures")
    code, out, error = pre_push(root, "run", extra={"STUB_EXIT_CI_HOOKS": "3"})
    groups = list(json.loads((ROOT / "config/quality/ci_groups.json").read_text())["hook_groups"])
    hook_calls = [call for call in pre_push_calls(root) if call["stub"] == "ci_hooks"]
    names = [f"hooks_{group}" for group in groups]
    failed_line = next((line for line in out.splitlines() if line.startswith("pre-push: failed steps:")), "")
    return {
        "case": "pre_push_hook_failures_are_all_reported",
        "passed": code != 0 and len(hook_calls) == len(groups) and all(f"FAIL {name}" in out and name in failed_line for name in names),
        "exit": code,
        "partitions": len(hook_calls),
        "failed_line": failed_line,
    }


def hook_deadline_case(scratch: Path) -> dict[str, Any]:
    """Every hook partition keeps its own deadline when they run one after the other."""
    root = pre_push_repo(scratch, "pre_push_hook_deadlines", timeout=2)
    started = time.monotonic()
    code, out, error = pre_push(root, "run", extra={"STUB_HOLD": "60"}, timeout=120)
    seconds = time.monotonic() - started
    groups = list(json.loads((ROOT / "config/quality/ci_groups.json").read_text())["hook_groups"])
    return {
        "case": "pre_push_hook_deadlines_fail_visibly",
        "passed": code != 0 and all(f"step hooks_{group} exceeded its deadline of 2 seconds" in out for group in groups) and seconds < 60,
        "exit": code,
        "seconds": round(seconds, 1),
        "output": (out + error)[-300:],
    }


def latest_report(root: Path) -> dict[str, Any]:
    reports = sorted((root / "data/e2e/pre_push").glob("*/report.json"))
    return json.loads(reports[-1].read_text()) if reports else {}


def gate_release_case(
    scratch: Path,
    case: str,
    mode: str,
    want_exit: int,
    want_outcome: str,
    want_text: str,
    step_exit: str = "0",
    mutate: Callable[[str], str] | None = None,
    seconds_limit: float = 60.0,
) -> dict[str, Any]:
    """The gate asks a stand-in launcher (it answers as ``mode`` says) to quit Docker after its steps; the gate's exit stays the steps' result."""
    root = pre_push_repo(scratch, case, "README.md", mutate=mutate)
    before = time.time()
    began = time.monotonic()
    code, out, error = pre_push(root, "run", extra={"STUB_RELEASE": mode, "STUB_EXIT_RUN_CI": step_exit}, timeout=180)
    seconds = time.monotonic() - began
    after = time.time()
    calls = pre_push_calls(root)
    launcher_calls = [call for call in calls if call["stub"] == "launcher"]
    ci_calls = [call for call in calls if call["stub"] == "run_ci"]
    argv = launcher_calls[0]["argv"] if launcher_calls else []
    since = float(argv[argv.index("--since") + 1]) if "--since" in argv else -1.0
    released = latest_report(root).get("docker_release") or {}
    checks = {
        "exit": code == want_exit,
        "launcher_called_once": len(launcher_calls) == 1,
        "release_arguments": argv[:3] == ["docker-release", "--owner", "--since"],
        "since_is_the_run_start": before - 1 <= since <= after,
        "release_environment_clean": launcher_calls[0]["seen"] == "" if launcher_calls else False,
        "run_ci_defers_to_the_gate": bool(ci_calls) and all(call["env"]["HAI_DOCKER_RELEASE"] == "outer" for call in ci_calls),
        "run_ci_never_reuses_under_the_gate": bool(ci_calls) and all(call["env"]["HAI_CI_REUSE"] == "off" for call in ci_calls),
        "line_printed": f"pre-push: Docker release: {want_text}" in out,
        "report_entry": released.get("outcome") == want_outcome,
        "within_limit": seconds < seconds_limit,
    }
    return {"case": case, "passed": all(checks.values()), "checks": checks, "exit": code, "output": (out + error)[-500:]}


def failing_step_release_case(scratch: Path, mutate: Callable[[str], str] | None = None) -> dict[str, Any]:
    return gate_release_case(scratch, "pre_push_failing_step_still_releases", "quit", 1, "quit", "Docker Desktop quit in", step_exit="3", mutate=mutate)


def refused_release_case(scratch: Path, mutate: Callable[[str], str] | None = None) -> dict[str, Any]:
    return gate_release_case(
        scratch, "pre_push_refused_release_keeps_the_exit_zero", "refused", 0, "refused", "refused (launcher: refused: docker_release is refused", mutate=mutate
    )


def exception_release_case(scratch: Path) -> dict[str, Any]:
    """A step runner that raises (the Ctrl-C path) still releases, and the error reaches the caller."""
    raising = source_edit('    serial = [step for step in steps if step["kind"] in SERIAL_KINDS]', '    raise RuntimeError("planted step failure")')
    root = pre_push_repo(scratch, "pre_push_exception_still_releases", "README.md", mutate=raising)
    code, out, error = pre_push(root, "run", extra={"STUB_RELEASE": "quit"}, timeout=120)
    launcher_calls = [call for call in pre_push_calls(root) if call["stub"] == "launcher"]
    checks = {
        "error_reaches_the_caller": code != 0 and "planted step failure" in error,
        "launcher_called_once": len(launcher_calls) == 1,
        "line_printed": "pre-push: Docker release: Docker Desktop quit in" in out,
    }
    return {
        "case": "pre_push_exception_in_the_steps_still_releases",
        "passed": all(checks.values()),
        "checks": checks,
        "exit": code,
        "output": (out + error)[-400:],
    }


def direct_release_cases(scratch: Path) -> list[dict[str, Any]]:
    """``pre_push.release`` on its own: a program that cannot start and a launcher that never answers are reported, never raised."""
    missing = pre_push_module.release(scratch / "no_such_root", time.time())
    root = pre_push_repo(scratch, "pre_push_release_direct", "README.md")
    os.environ["STUB_LOG"] = str(root / "stub.log")
    try:
        registry = pre_push_module.release(root, time.time(), str(scratch / "registry"))
    finally:
        del os.environ["STUB_LOG"]
    registry_argv: list[str] = next((call["argv"] for call in pre_push_calls(root) if call["stub"] == "launcher"), [])
    return [
        {
            "case": "pre_push_release_reports_a_program_that_cannot_start",
            "passed": missing["outcome"] == "failed" and "could not start" in missing["detail"],
            "result": missing,
        },
        {
            "case": "pre_push_release_passes_the_test_registry_before_the_command",
            "passed": registry["outcome"] == "quit"
            and registry_argv[:2] == ["--test-registry", str(scratch / "registry")]
            and registry_argv[2] == "docker-release",
            "argv": registry_argv,
        },
    ]


def gate_release_cases(scratch: Path) -> list[dict[str, Any]]:
    """Gate cases 6 to 8 of the Docker release design, and the cases for every other answer of the launcher."""
    deadline = source_edit("RELEASE_SECONDS = 600", "RELEASE_SECONDS = 2")
    results = [
        failing_step_release_case(scratch),
        refused_release_case(scratch),
        gate_release_case(
            scratch,
            "pre_push_release_deadline_is_killed_and_reported",
            "sleep",
            0,
            "failed",
            "failed (exceeded 2 s and was stopped)",
            mutate=deadline,
            seconds_limit=45,
        ),
        gate_release_case(scratch, "pre_push_passing_gate_quits_docker", "quit", 0, "quit", "Docker Desktop quit in"),
        gate_release_case(scratch, "pre_push_skipped_release_is_reported", "skipped", 0, "skipped", "skipped (no container ran since T)"),
        gate_release_case(
            scratch,
            "pre_push_failed_release_keeps_the_exit_zero",
            "error",
            0,
            "failed",
            "failed (exit 4: launcher: docker quit failed: boom [command_failed]); the gate result stands",
        ),
        gate_release_case(scratch, "pre_push_unreadable_release_answer_keeps_the_exit_zero", "garbage", 0, "failed", "failed (unreadable answer: not json)"),
        exception_release_case(scratch),
        *direct_release_cases(scratch),
    ]
    dropped = source_edit(
        "        released = release(root, started, release_registry)", '        released = {"outcome": "skipped", "detail": "planted", "seconds": 0.0}'
    )
    blocking = source_edit(
        '    sys.stdout.write(f"pre-push: all steps passed; logs in {output}\\n")\n    return 0',
        '    sys.stdout.write(f"pre-push: all steps passed; logs in {output}\\n")\n    return 0 if released["outcome"] != "refused" else 1',
    )
    results += [
        planted_case("dropped_release", failing_step_release_case, scratch, mutate=dropped),
        planted_case("refused_release_blocks_the_push", refused_release_case, scratch, mutate=blocking),
    ]
    return results


def release_docker_run(
    scratch: Path,
    name: str,
    extra: dict[str, str],
    system: str = "Darwin",
    part: str = "all",
    launcher_exit: str = "0",
    transform: Callable[[str], str] | None = None,
) -> dict[str, Any]:
    """Run the ``release_docker`` function of ``scripts/run_ci.sh`` alone, with a stand-in interpreter that logs its arguments and a ``uname`` shim."""
    body = function_text((ROOT / "scripts/run_ci.sh").read_text(), "release_docker")
    if body is None:
        return {"code": -1, "calls": [], "output": "release_docker is not defined in scripts/run_ci.sh"}
    text = f"release_docker() {{\n{body}}}\n"
    folder = scratch / name
    (folder / "bin").mkdir(parents=True)
    log = folder / "calls.log"
    (folder / "bin/uname").write_text(f"#!/bin/sh\necho {system}\n")
    (folder / "bin/uname").chmod(0o755)
    (folder / "python_bin").write_text('#!/bin/sh\nprintf \'%s\\n\' "$*" >>"$STUB_LOG"\nexit "${STUB_EXIT:-0}"\n')
    (folder / "python_bin").chmod(0o755)
    (folder / "release_function.sh").write_text(transform(text) if transform else text)
    environment = {key: value for key, value in os.environ.items() if key not in ("HAI_DOCKER_RELEASE", "GITHUB_ACTIONS", "CI_LIST_ONLY")}
    result = run_command(
        "/bin/bash",
        [
            "-c",
            'set -euo pipefail; source "$RELEASE_FUNCTION"; python_bin="$RELEASE_PYTHON"; part="$RELEASE_PART"; '
            'ci_started=1700000000; release_docker; echo "status=$?"',
        ],
        env={
            **environment,
            "PATH": f"{folder / 'bin'}:{environment['PATH']}",
            "RELEASE_FUNCTION": str(folder / "release_function.sh"),
            "RELEASE_PYTHON": str(folder / "python_bin"),
            "RELEASE_PART": part,
            "STUB_LOG": str(log),
            "STUB_EXIT": launcher_exit,
            **extra,
        },
        timeout=60,
    )
    calls = log.read_text().splitlines() if log.exists() else []
    return {"code": result.returncode, "calls": calls, "output": result.stdout + result.stderr}


def run_ci_release_cases(scratch: Path) -> list[dict[str, Any]]:
    """Gate case 9: ``run_ci.sh`` releases Docker once after a run on macOS, never changes its exit status, and skips under the gate, on hosted CI, off macOS,
    in a list-only run and in the gates part."""
    ci = (ROOT / "scripts/run_ci.sh").read_text()
    finish = function_text(ci, "finish") or ""
    wanted = "-m orchestration.launcher docker-release --owner --since 1700000000 --deadline 600"
    results: list[dict[str, Any]] = []
    run = release_docker_run(scratch, "release_runs", {})
    results.append(
        {
            "case": "run_ci_releases_docker_after_a_run_on_macos",
            "passed": run["code"] == 0 and run["calls"] == [wanted] and "status=0" in run["output"],
            "run": run,
        }
    )
    skipped: dict[str, tuple[dict[str, str], dict[str, Any], str]] = {
        "outer": ({"HAI_DOCKER_RELEASE": "outer"}, {}, "the pre-push gate releases Docker"),
        "github": ({"GITHUB_ACTIONS": "true"}, {}, "hosted CI has no Docker Desktop"),
        "linux": ({}, {"system": "Linux"}, "this is not macOS"),
        "list_only": ({"CI_LIST_ONLY": "1"}, {}, "no group ran Docker"),
        "gates": ({}, {"part": "gates"}, "no group ran Docker"),
    }
    for label, (extra, options, reason) in skipped.items():
        run = release_docker_run(scratch, f"release_skipped_{label}", extra, **options)
        results.append(
            {
                "case": f"run_ci_skips_its_docker_release_{label}",
                "passed": run["code"] == 0 and run["calls"] == [] and f"Docker release skipped: {reason}" in run["output"] and "status=0" in run["output"],
                "run": run,
            }
        )
    run = release_docker_run(scratch, "release_refused", {}, launcher_exit="3")
    results.append(
        {
            "case": "run_ci_docker_release_failure_keeps_the_exit_status",
            "passed": run["code"] == 0
            and run["calls"] == [wanted]
            and "status=0" in run["output"]
            and "Docker release did not quit Docker (exit 3)" in run["output"],
            "run": run,
        }
    )
    results.append(
        {
            "case": "run_ci_finish_releases_after_the_scratch_folder_and_the_trap_keeps_its_rule",
            "passed": "release_docker" in finish
            and ci.index("ci_started=") < ci.index("trap 'finish;")
            and "trap 'finish; [[ \"${ci_finished:-0}\" == 1 ]] || exit 1' EXIT" in ci,
        }
    )
    planted = release_docker_run(
        scratch,
        "release_planted",
        {"HAI_DOCKER_RELEASE": "outer"},
        transform=lambda text: text.replace('"${HAI_DOCKER_RELEASE:-}" == "outer"', '"${HAI_DOCKER_RELEASE:-}" == "never"'),
    )
    results.append(
        {"case": "planted_release_under_the_gate_caught_by_run_ci_skips_its_docker_release_outer", "passed": planted["calls"] == [wanted], "run": planted}
    )
    return results


def pre_push_cases(work: Path, scratch: Path, gate_stage: bool = True) -> list[dict[str, Any]]:
    """The pre-push run selects the groups of the pushed range as hosted CI does, escalates to the full scope for selector and CI script changes, and
    every step has a deadline that fails visibly."""
    results = [escalation_case(scratch, f"pre_push_escalates_{index}", path, "full") for index, path in enumerate(ESCALATING_PATHS)]
    results += [escalation_case(scratch, f"pre_push_no_escalation_for_ordinary_path_{index}", path, "range") for index, path in enumerate(ORDINARY_PATHS)]
    config = json.loads((ROOT / "config/quality/ci_groups.json").read_text())
    root = pre_push_repo(scratch, "planted_escalation_gap", "scripts/ci/groups/launcher.sh")
    damaged = copy.deepcopy(config)
    damaged["pre_push"]["full_paths"] = [pattern for pattern in damaged["pre_push"]["full_paths"] if "scripts/ci" not in pattern]
    (root / "config/quality/ci_groups.json").write_text(json.dumps(damaged))
    plan = pre_push_plan(root)
    results.append(
        {
            "case": f"planted_escalation_gap_caught_by_pre_push_escalates_{ESCALATING_PATHS.index('scripts/ci/groups/launcher.sh')}",
            "passed": plan["scope"] == "range",
            "scope": plan["scope"],
        }
    )
    root = pre_push_repo(scratch, "pre_push_unusable_base_runs_full", "README.md", remote=False)
    plan = pre_push_plan(root)
    results.append(
        {
            "case": "pre_push_unusable_base_runs_full",
            "passed": plan["scope"] == "full" and "origin/main" in plan["reason"],
            "reason": plan["reason"],
        }
    )
    root = pre_push_repo(scratch, "pre_push_range_matches_hosted", "README.md")
    plan = pre_push_plan(root)
    hosted = workflow_document()["jobs"]["local_ci"]["steps"]
    hosted_env = next(step["env"] for step in hosted if isinstance(step.get("env"), dict) and "CI_SCOPE" in step["env"])
    base = probe_git(root, "rev-parse", "origin/main")
    head = probe_git(root, "rev-parse", "HEAD")
    steps = {step["name"]: step for step in plan["steps"]}
    local = steps.get("local_ci", {})
    hooks = [steps.get(f"hooks_{group}", {}).get("argv", []) for group in config["hook_groups"]]
    matches = {
        "scope": plan["scope"] == "range" and plan["start"] == base and plan["end"] == head,
        "env_names": set(local.get("env", {})) == {"CI_SCOPE", "RANGE_START", "RANGE_END", "HAI_DOCKER_RELEASE", "HAI_CI_REUSE"},
        "gate_never_reuses_a_local_pass": local.get("env", {}).get("HAI_CI_REUSE") == "off",
        "gate_releases_docker_itself": local.get("env", {}).get("HAI_DOCKER_RELEASE") == "outer",
        "env_values": local.get("env", {}).get("CI_SCOPE") == hosted_env["CI_SCOPE"]
        and local.get("env", {}).get("RANGE_START") == base
        and local.get("env", {}).get("RANGE_END") == head,
        "no_part": "CI_PART" not in local.get("env", {}),
        "hook_groups": all(argv[argv.index("--from") + 1] == base and argv[argv.index("--to") + 1] == head for argv in hooks if argv) and all(hooks),
        "commit_messages": steps.get("commit_messages", {}).get("argv", [])[-3:] == ["commit-range", base, head],
        "acquisition_range": steps.get("acquisition", {}).get("argv", [])[-4:] == ["--from-ref", base, "--to-ref", head],
        "removed_names": steps.get("removed_names", {}).get("env", {}) == {"PRE_COMMIT_FROM_REF": base, "PRE_COMMIT_TO_REF": head},
        "every_step_has_a_deadline": bool(plan)
        and all(isinstance(step["timeout_seconds"], int) and 0 < step["timeout_seconds"] <= 7200 for step in plan["steps"]),
    }
    results.append({"case": "pre_push_range_matches_hosted", "passed": all(matches.values()), "checks": matches})
    full_root = pre_push_repo(scratch, "pre_push_full_plan", "scripts/run_ci.sh")
    full = pre_push_plan(full_root)
    full_steps = {step["name"]: step for step in full["steps"]}
    results.append(
        {
            "case": "pre_push_full_plan_runs_the_full_scope_once",
            "passed": bool(full)
            and full["scope"] == "full"
            and full_steps.get("local_ci", {}).get("env") == {"CI_SCOPE": "full", "HAI_DOCKER_RELEASE": "outer", "HAI_CI_REUSE": "off"}
            and not any(name.startswith("hooks_") or name == "acquisition" for name in full_steps),
            "steps": list(full_steps),
        }
    )
    root = pre_push_repo(scratch, "pre_push_run", "README.md")
    code, out, error = pre_push(root, "run")
    calls = pre_push_calls(root)
    ci = [call for call in calls if call["stub"] == "run_ci"]
    hook_calls = sorted(call["argv"][call["argv"].index("--group") + 1] for call in calls if call["stub"] == "ci_hooks")
    results.append(
        {
            "case": "pre_push_run_calls_hosted_commands",
            "passed": code == 0 and len(ci) == 1 and ci[0]["env"]["CI_SCOPE"] == "range" and hook_calls == sorted(config["hook_groups"]),
            "exit": code,
            "calls": [call["stub"] for call in calls],
            "error": error[-300:],
        }
    )
    root = pre_push_repo(scratch, "pre_push_failure")
    code, out, error = pre_push(root, "run", extra={"STUB_EXIT_RUN_CI": "3"})
    hook_calls = [call for call in pre_push_calls(root) if call["stub"] == "ci_hooks"]
    results.append(
        {
            "case": "pre_push_failure_is_reported",
            "passed": code != 0 and "local_ci" in out + error and len(hook_calls) == len(config["hook_groups"]),
            "exit": code,
            "output": (out + error)[-400:],
        }
    )
    root = pre_push_repo(scratch, "pre_push_step_deadline_fails_visibly", "README.md", timeout=2)
    started = time.monotonic()
    code, out, error = pre_push(root, "run", extra={"STUB_SLEEP": "60"}, timeout=90)
    seconds = time.monotonic() - started
    results.append(
        {
            "case": "pre_push_step_deadline_fails_visibly",
            "passed": code != 0 and "local_ci" in out + error and "exceeded" in out + error and seconds < 30,
            "exit": code,
            "seconds": round(seconds, 1),
            "output": (out + error)[-300:],
        }
    )
    results += [hook_partition_case(scratch), hook_failure_case(scratch), hook_deadline_case(scratch)]
    results += gate_release_cases(scratch) + run_ci_release_cases(scratch)
    serial_jobs = 'jobs = [[step] for step in steps if step["kind"] not in SERIAL_KINDS] + ([serial] if serial else [])'
    serial_kinds = 'SERIAL_KINDS = frozenset({"pre_commit", "ci_hooks"})'
    results += [
        planted_case("hook_partitions_in_parallel", hook_partition_case, scratch, mutate=source_edit(serial_jobs, "jobs = [[step] for step in steps]")),
        planted_case("acquisition_in_parallel", hook_partition_case, scratch, mutate=source_edit(serial_kinds, 'SERIAL_KINDS = frozenset({"ci_hooks"})')),
        planted_case("every_step_in_sequence", hook_partition_case, scratch, mutate=source_edit(serial_jobs, "jobs = [steps]")),
    ]
    if gate_stage:
        results += gate_stage_cases(scratch)
    return (
        results + hook_and_tree_cases(scratch) + acquisition_cases(scratch) + control_scan_cases() + pre_commit_control_cases() + entry_cases() + docs_cases()
    )


def source_edit(old: str, new: str) -> Callable[[str], str]:
    def edit(source: str) -> str:
        if old not in source:
            raise ValueError(f"planted edit does not apply: {old!r}")
        return source.replace(old, new, 1)

    return edit


def planted_case(label: str, case: Callable[..., dict[str, Any]], scratch: Path, **edits: Any) -> dict[str, Any]:
    """Run ``case`` with a defect planted through ``edits``; the defect is caught when the case fails."""
    try:
        result = case(scratch, **edits)
    except ValueError as error:
        return {"case": f"planted_{label}_caught_by_{case.__name__}", "passed": False, "error": str(error)}
    return {"case": f"planted_{label}_caught_by_{result['case']}", "passed": not result["passed"], "caught": not result["passed"]}


def first_stub(calls: list[dict[str, Any]]) -> str:
    return calls[0]["stub"] if calls else ""


def changed_files_first_case(scratch: Path, mutate: Callable[[str], str] | None = None) -> dict[str, Any]:
    """The changed-file checks run first, over the pushed range, and the long steps start after they passed."""
    root = pre_push_repo(scratch, "gate_changed_files_first", "README.md", mutate=mutate)
    code, out, error = pre_push(root, "run", timeout=300)
    calls = pre_push_calls(root)
    base, head = probe_git(root, "rev-parse", "origin/main"), probe_git(root, "rev-parse", "HEAD")
    argv = calls[0].get("argv", []) if calls else []
    order = [call["stub"] for call in calls]
    return {
        "case": "pre_push_changed_file_checks_run_first_over_the_range",
        "passed": code == 0
        and first_stub(calls) == "changed_files"
        and order.count("changed_files") == 1
        and argv[argv.index("--from") + 1 : argv.index("--from") + 2] == [base]
        and argv[argv.index("--to") + 1 : argv.index("--to") + 2] == [head]
        and "run_ci" in order,
        "exit": code,
        "order": order,
        "output": (out + error)[-300:],
    }


def changed_files_failure_case(scratch: Path, mutate: Callable[[str], str] | None = None) -> dict[str, Any]:
    """A failed changed-file check stops the gate before the long groups, the hook partitions and the acquisition hook, and names itself."""
    root = pre_push_repo(scratch, "gate_changed_files_failure", ("README.md",), mutate=mutate)
    code, out, error = pre_push(root, "run", extra={"STUB_EXIT_CHANGED_FILES": "1"}, timeout=300)
    order = [call["stub"] for call in pre_push_calls(root)]
    return {
        "case": "pre_push_changed_file_failure_stops_before_the_long_groups",
        "passed": code != 0 and order == ["changed_files"] and "changed_file_checks" in out + error,
        "exit": code,
        "order": order,
        "output": (out + error)[-300:],
    }


def changed_files_without_a_range_case(scratch: Path) -> dict[str, Any]:
    """With no merge base there is no range to read, so the checks are left out and the full scope runs."""
    root = pre_push_repo(scratch, "gate_changed_files_no_range", "README.md", remote=False)
    code, out, error = pre_push(root, "run", timeout=300)
    order = [call["stub"] for call in pre_push_calls(root)]
    return {"case": "pre_push_changed_file_checks_need_a_range", "passed": code == 0 and "changed_files" not in order and "run_ci" in order, "order": order}


def unreadable_status_case(scratch: Path) -> dict[str, Any]:
    """A tree whose state Git cannot read is refused: the checks could not describe the pushed commit."""
    root = pre_push_repo(scratch, "gate_unreadable_status")
    (root / ".git/index").write_bytes(b"not an index")
    code, out, error = pre_push(root, "run", timeout=120)
    return {
        "case": "pre_push_refuses_a_tree_whose_state_cannot_be_read",
        "passed": code != 0 and not [call for call in pre_push_calls(root) if call["stub"] == "run_ci"] and "no check ran" in out and "cannot be read" in out,
        "exit": code,
        "output": (out + error)[-300:],
    }


def small_case(
    scratch: Path, name: str, changed: tuple[str, ...], small: dict[str, Any] | None, want_small: bool, mutate: Callable[[str], str] | None = None
) -> dict[str, Any]:
    """A small change runs the commit checks and the affected groups; the hosted partitions and the acquisition hook stay with hosted CI."""
    root = pre_push_repo(scratch, f"gate_small_{name}", changed, small=small, mutate=mutate)
    code, out, error = pre_push(root, "run", timeout=300)
    calls = pre_push_calls(root)
    stubs = [call["stub"] for call in calls]
    hosted = "ci_hooks" in stubs or "acquisition" in stubs
    ran_ci = [call["env"]["CI_SCOPE"] for call in calls if call["stub"] == "run_ci"]
    passed = (
        code == 0
        and ran_ci == ["range"]
        and "changed_files" in stubs
        and (not hosted if want_small else "ci_hooks" in stubs)
        and (("small change" in out) == want_small)
    )
    return {"case": f"pre_push_small_change_{name}", "passed": passed, "exit": code, "calls": stubs, "output": (out + error)[-300:]}


SMALL = {"max_files": 6, "max_changed_lines": 250}


def changed_file_check_cases(scratch: Path) -> list[dict[str, Any]]:
    """The changed-file checks choose their steps from the changed paths and fail with the file named."""
    changed_files = importlib.import_module("scripts.quality.ci_changed_files")

    def names(files: list[str]) -> list[str]:
        return [step["name"] for step in changed_files.steps(ROOT, files, lambda listed: listed)]

    results = [
        {
            "case": "changed_file_checks_pick_steps_from_the_changed_paths",
            "passed": names(["a.py", "dbt/models/gold/x.sql", "README.md"])
            == ["ruff_format", "ruff_check", "mypy", "sqlfluff", "docker_calls", "ci_group_files"]
            and names(["README.md"]) == ["docker_calls", "ci_group_files"]
            and names(["scripts/x.py"]) == ["ruff_format", "ruff_check", "mypy", "docker_calls", "ci_group_files"]
            and names(["scripts/x.sql"]) == ["docker_calls", "ci_group_files"],
        }
    ]
    root = scratch / "changed_file_ruff"
    (root / "config/quality").mkdir(parents=True)
    (root / "config/quality/ci_groups.json").write_text(json.dumps({"pre_push": {"changed_file_seconds": 120}}))
    (root / "good.py").write_text("print(1)\n")
    (root / "bad.py").write_text("import os\nprint( 1 )\n")
    outcomes = {}
    for name, only in (("good", "ruff_check"), ("bad", "ruff_check"), ("bad_format", "ruff_format")):
        file = "good.py" if name == "good" else "bad.py"
        result = run_command(
            str(PYTHON), ["-m", "scripts.quality.ci_changed_files", "--root", str(root), "--file", file, "--only", only], cwd=ROOT, timeout=120
        )
        outcomes[name] = (result.returncode, result.stdout)
    results.append(
        {
            "case": "changed_file_checks_fail_with_the_file_named",
            "passed": outcomes["good"][0] == 0
            and outcomes["bad"][0] == 1
            and "bad.py" in outcomes["bad"][1]
            and "FAIL ruff_check" in outcomes["bad"][1]
            and outcomes["bad_format"][0] == 1
            and "FAIL ruff_format" in outcomes["bad_format"][1],
            "outcomes": {key: value[0] for key, value in outcomes.items()},
        }
    )
    return results


def osv_scanner_case(
    scratch: Path, name: str, changed: tuple[str, ...], small: dict[str, Any] | None, want_run: bool, mutate: Callable[[str], str] | None = None
) -> dict[str, Any]:
    """The OSV-Scanner hook runs over the pushed range in its own step, also for a small change, and skips a range without a requirements file."""
    root = pre_push_repo(scratch, f"gate_osv_{name}", changed, small=small, mutate=mutate)
    plan = pre_push_plan(root)
    step: dict[str, Any] = next((item for item in plan["steps"] if item["name"] == "osv_scanner"), {})
    base, head = probe_git(root, "rev-parse", "origin/main"), probe_git(root, "rev-parse", "HEAD")
    code, out, error = pre_push(root, "run", timeout=300)
    calls = [call for call in pre_push_calls(root) if call["stub"] == "osv_scanner"]
    passed = (
        code == 0
        and step.get("argv", [])[-6:] == ["--hook-stage", "pre-push", "--from-ref", base, "--to-ref", head]
        and len(calls) == (1 if want_run else 0)
        and (not want_run or calls[0]["argv"] == ["osv-scanner"])
    )
    return {
        "case": f"pre_push_osv_scanner_{name}",
        "passed": passed,
        "exit": code,
        "calls": len(calls),
        "step": step.get("argv", []),
        "output": (out + error)[-300:],
    }


def gate_stage_cases(scratch: Path) -> list[dict[str, Any]]:
    """The changed-file checks, the OSV-Scanner step and the small-change rule of the pre-push run."""
    results = [changed_files_first_case(scratch), changed_files_failure_case(scratch), changed_files_without_a_range_case(scratch)]
    results.append(unreadable_status_case(scratch))
    results += changed_file_check_cases(scratch)
    results += [
        osv_scanner_case(scratch, "runs_for_a_requirements_change", ("requirements.txt",), None, True),
        osv_scanner_case(scratch, "runs_for_a_small_change", ("requirements.txt",), SMALL, True),
        osv_scanner_case(scratch, "skips_a_range_without_requirements", ("README.md",), None, False),
    ]
    results += [
        small_case(scratch, "runs_only_checks_and_groups", ("features/example.py", "README.md"), SMALL, True),
        small_case(scratch, "too_many_files_runs_everything", tuple(f"features/example_{index}.py" for index in range(7)), SMALL, False),
        small_case(scratch, "too_many_lines_runs_everything", ("features/example.py",), {**SMALL, "max_changed_lines": 1}, False),
        small_case(scratch, "infrastructure_runs_everything", ("infra/example.tf",), SMALL, False),
        small_case(scratch, "shell_script_runs_everything", ("scripts/lakehouse/example.sh",), SMALL, False),
        small_case(scratch, "disabled_by_the_threshold", ("features/example.py",), None, False),
    ]
    results += [
        planted_case("changed_file_checks_last", changed_files_first_case, scratch, mutate=source_edit('    first = chosen["first"]\n', "    first = None\n")),
        planted_case(
            "osv_scanner_step_left_out",
            osv_scanner_case,
            scratch,
            name="runs_for_a_requirements_change",
            changed=("requirements.txt",),
            small=None,
            want_run=True,
            mutate=source_edit('steps.append({"name": "osv_scanner"', '[].append({"name": "osv_scanner"'),
        ),
        planted_case("changed_file_failure_ignored", changed_files_failure_case, scratch, mutate=source_edit('if first_result["exit"] != 0:', "if False:")),
        planted_case(
            "every_change_small",
            small_case,
            scratch,
            name="infrastructure_runs_everything",
            changed=("infra/example.tf",),
            small=SMALL,
            want_small=False,
            mutate=source_edit('if matching([*small["not_paths"], *config["pre_push"]["full_paths"]], paths):', "if False:"),
        ),
    ]
    return results


MAIN_PASSES = "scripts.quality.ci_main_passes"
PASS_FILES = (
    "config/quality/ci_groups.json",
    "config/quality/launcher_inputs.json",
    "scripts/process.py",
    "scripts/quality/ci_groups.py",
    "scripts/quality/ci_main_passes.py",
)


def passes(root: Path, folder: Path, command: str, groups: list[str], main_ref: str = "HEAD", extra: tuple[str, ...] = ()) -> tuple[str, dict[str, str]]:
    """Run the pass-record module in ``root`` and return its output and the group lines of ``filter`` (reuse or run)."""
    environment = {**os.environ, "HAI_CI_PASSES_DIR": str(folder), "HAI_CI_MAIN_REF": main_ref, "PYTHONDONTWRITEBYTECODE": "1"}
    result = run_command(str(PYTHON), ["-B", "-m", MAIN_PASSES, command, "--groups", *groups, *extra], cwd=root, env=environment, timeout=300)
    lines = [line.split() for line in result.stdout.splitlines()]
    return result.stdout, {line[1]: line[0] for line in lines if line and line[0] in {"reuse", "run"}}


def passes_repo(scratch: Path, name: str) -> Path:
    root = scratch / name
    for path in PASS_FILES:
        (root / path).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ROOT / path, root / path)
    for package in ("scripts", "scripts/quality"):
        (root / package / "__init__.py").write_text("")
    for path in ("README.md", "features/example.py", "dbt/models/silver/example.sql", "scripts/lakehouse/example.py"):
        (root / path).parent.mkdir(parents=True, exist_ok=True)
        (root / path).write_text("base\n")
    probe_git(root, "init", "--quiet", "--initial-branch", "main")
    probe_git(root, "add", "--all")
    probe_git(root, "-c", "commit.gpgsign=false", "commit", "--quiet", "--message", "base")
    return root


def affected(path: str) -> set[str]:
    return set(ci_groups.plan("paths", "all", paths=[path])["run"])


def reuse_hit(path: str) -> set[str]:
    """The groups whose main-reuse input hash a change to ``path`` moves: those it selects, and each full-only group, whose input is every file."""
    return affected(path) | set(json.loads((ROOT / "config/quality/ci_groups.json").read_text())["full_only"])


def pass_record_cases(scratch: Path) -> list[dict[str, Any]]:
    """A group's pass is recorded only on a clean commit of main and reused only for identical inputs."""
    config = json.loads((ROOT / "config/quality/ci_groups.json").read_text())
    groups = [*config["groups"], *config["full_only"]]
    recordable = [group for group in groups if group not in config["reuse"]["never"]]
    root = passes_repo(scratch, "passes_clean")
    folder = scratch / "passes_clean_records"
    written, _ = passes(root, folder, "record", groups)
    _, clean = passes(root, folder, "filter", groups)
    results = [
        {
            "case": "pass_records_are_written_on_a_clean_commit_of_main_and_reused",
            "passed": "recorded" in written
            and sorted(path.stem for path in folder.glob("*.json")) == sorted(recordable)
            and all(clean[group] == "reuse" for group in recordable)
            and clean["repository_hooks"] == "run",
            "written": written.strip()[:200],
            "filter": clean,
        }
    ]
    probe = "dbt/models/silver/example.sql"
    (root / probe).write_text("changed\n")
    _, changed = passes(root, folder, "filter", groups)
    hit = reuse_hit(probe)
    results.append(
        {
            "case": "pass_reuse_stops_for_the_groups_a_changed_file_selects",
            "passed": all(changed[group] == ("run" if group in hit else "reuse") for group in recordable) and hit and hit != set(recordable),
            "run": sorted(group for group in groups if changed[group] == "run"),
            "selected_by_the_file": sorted(hit),
        }
    )
    written_dirty, _ = passes(root, scratch / "passes_dirty_records", "record", groups)
    results.append(
        {
            "case": "pass_record_refused_for_a_changed_tree",
            "passed": "not recorded" in written_dirty and "changes" in written_dirty and not (scratch / "passes_dirty_records").exists(),
            "output": written_dirty.strip()[:200],
        }
    )
    (root / probe).write_text("base\n")
    (root / "features/new_file.py").write_text("new\n")
    _, untracked = passes(root, folder, "filter", groups)
    results.append(
        {
            "case": "pass_reuse_stops_for_a_new_untracked_file",
            "passed": all(untracked[group] == ("run" if group in reuse_hit("features/new_file.py") else "reuse") for group in recordable)
            and bool(reuse_hit("features/new_file.py")),
            "run": sorted(group for group in groups if untracked[group] == "run"),
        }
    )
    (root / "features/new_file.py").unlink()
    (root / "README.md").write_text("docs change\n")
    _, documentation = passes(root, folder, "filter", groups)
    expected = reuse_hit("README.md")
    results.append(
        {
            "case": "pass_reuse_follows_the_selector_for_a_documentation_change",
            "passed": all(documentation[group] == ("run" if group in expected else "reuse") for group in recordable) and expected != set(recordable),
            "run": sorted(group for group in groups if documentation[group] == "run"),
            "selected_by_the_file": sorted(expected),
        }
    )
    (root / "README.md").write_text("base\n")
    unrelated = probe_git(root, "commit-tree", "HEAD^{tree}", "-m", "unrelated")
    probe_git(root, "branch", "elsewhere", unrelated)
    written_topic, _ = passes(root, scratch / "passes_topic_records", "record", groups, main_ref="elsewhere")
    _, away = passes(root, folder, "filter", groups, main_ref="elsewhere")
    results.append(
        {
            "case": "pass_record_and_reuse_need_the_commit_to_be_in_main",
            "passed": "not recorded" in written_topic and "not in" in written_topic and all(away[group] == "run" for group in groups),
            "output": written_topic.strip()[:200],
            "filter": away,
        }
    )
    return results


LAUNCHER_STUB_TIMING = """#!/usr/bin/env bash
timed() {
  local label="$1"
  shift
  if [[ "$label" == "launcher E2E shard "* ]]; then printf 'TIMED %s %s\\n' "$label" "$*"; "$@"; else printf 'TIMED %s %s\\n' "$label" "$*"; fi
}
timing_table() { :; }
timing_entry() { :; }
"""
LAUNCHER_STUB_BODY = """#!/usr/bin/env bash
launcher_e2e() {
  printf 'stub launcher shard %s\\n' "${LAUNCHER_SHARD:-all}"
  if [[ "${LAUNCHER_SHARD:-}" == "${STUB_LAUNCHER_FAIL:-none}" ]]; then return 1; fi
  sleep "${STUB_LAUNCHER_SLEEP:-1}"
}
"""
LAUNCHER_SLEEP = "40.7"


def launcher_shard_run(work: Path, out: Path, label: str, env: dict[str, str]) -> tuple[int, str, float]:
    def stub_body(tree: Path) -> None:
        (tree / GROUP_FILES / "launcher.sh").write_text(LAUNCHER_STUB_BODY, encoding="utf-8")

    started = time.monotonic()
    code, log = dry_run(
        work, out, label, {"CI_SCOPE": "full", "CI_PART": "launcher", "HAI_CI_REUSE": "off", **env}, prepare=stub_body, timing=LAUNCHER_STUB_TIMING
    )
    return code, log, time.monotonic() - started


def leftover_sleepers() -> list[str]:
    result = run_command("pgrep", ["-f", f"sleep {LAUNCHER_SLEEP}"], timeout=30)
    return result.stdout.split()


def launcher_shard_cases(work: Path, out: Path) -> list[dict[str, Any]]:
    """The full local run splits the launcher E2E like the hosted matrix, runs the shards at once, bounds them and stops what they left."""
    shards = json.loads((ROOT / "config/quality/ci_groups.json").read_text())["launcher_shards"]
    results = []
    code, log, seconds = launcher_shard_run(work, out, "launcher_shards_parallel", {"STUB_LAUNCHER_SLEEP": "6"})
    checks = {
        "exit_zero": code == 0 and PASS_LINES["launcher"] in log,
        "every_shard_ran": all(f"stub launcher shard {index}/{shards}" in log for index in range(1, shards + 1)),
        "shards_ran_at_once": seconds < 6 * shards + 4,
        "logs_print_in_shard_order": log.index(f"launcher shard 1/{shards} ==") < log.index(f"launcher shard {shards}/{shards} =="),
    }
    results.append({"case": "local_launcher_runs_its_shards_at_once", "passed": all(checks.values()), "checks": checks, "seconds": round(seconds, 1)})
    code, log, seconds = launcher_shard_run(work, out, "launcher_shard_fails", {"STUB_LAUNCHER_FAIL": f"{shards}/{shards}", "STUB_LAUNCHER_SLEEP": "1"})
    checks = {
        "exit_nonzero": code != 0,
        "names_the_shard": f"Local CI failed in: launcher E2E shard {shards}/{shards}" in log,
        "other_shards_ran_and_print": "stub launcher shard 1/" in log,
        "no_pass_line": PASS_LINES["launcher"] not in log,
    }
    results.append({"case": "local_launcher_shard_failure_fails_the_run_and_names_the_shard", "passed": all(checks.values()), "checks": checks, "exit": code})
    code, log, seconds = launcher_shard_run(work, out, "launcher_shard_deadline", {"STUB_LAUNCHER_SLEEP": LAUNCHER_SLEEP, "CI_LAUNCHER_LIMIT_SECONDS": "2"})
    left = leftover_sleepers()
    checks = {
        "exit_nonzero": code != 0,
        "names_the_deadline": "ran past its deadline of 2 s: stopping it" in log and "(deadline 2 s)" in log,
        "ended_long_before_the_stub_would_have": seconds < 35,
        "stopped_every_process": not left,
    }
    for pid in left:
        run_command("kill", ["-KILL", pid], timeout=30)
    results.append(
        {"case": "local_launcher_shards_are_bounded_and_stopped", "passed": all(checks.values()), "checks": checks, "seconds": round(seconds, 1), "left": left}
    )
    code, log, seconds = launcher_shard_run(work, out, "launcher_one_shard", {"LAUNCHER_SHARD": f"2/{shards}", "STUB_LAUNCHER_SLEEP": "1"})
    checks = {
        "exit_zero": code == 0 and PASS_LINES["launcher"] in log,
        "only_the_given_shard": f"stub launcher shard 2/{shards}" in log and "stub launcher shard 1/" not in log and "launcher shard 1/" not in log,
    }
    results.append({"case": "local_launcher_run_with_a_given_shard_runs_only_it", "passed": all(checks.values()), "checks": checks, "exit": code})
    return results


def reuse_dry_run(
    work: Path, out: Path, label: str, env: dict[str, str], folder: Path, record: bool = True, edit: Callable[[Path], None] | None = None
) -> tuple[int, str]:
    """A dry run of the dispatcher after a record of every group made inside the run's own state (the stand-ins change the inputs)."""

    def prepare(tree: Path) -> None:
        if record:
            passes(tree, folder, "record", GROUP_NAMES, extra=("--test-any-state",))
        if edit:
            edit(tree)

    return dry_run(
        work,
        out,
        label,
        # The case's own gate marker counts, not the one of a gate that runs this suite.
        {"CI_SCOPE": "full", "CI_PART": "all", "HAI_CI_PASSES_DIR": str(folder), "HAI_CI_MAIN_REF": "HEAD", "HAI_DOCKER_RELEASE": "", **env},
        prepare=prepare,
    )


GROUP_NAMES = list(json.loads((ROOT / "config/quality/ci_groups.json").read_text())["groups"]) + list(
    json.loads((ROOT / "config/quality/ci_groups.json").read_text())["full_only"]
)


def reuse_cases(work: Path, out: Path, scratch: Path) -> list[dict[str, Any]]:
    """The dispatcher skips the groups whose pass was recorded on main, only in a full local run, and never under the gate."""
    results = pass_record_cases(scratch)
    results += launcher_shard_cases(work, out)
    results.append(
        planted_case(
            "gate_without_the_reuse_switch",
            failing_step_release_case,
            scratch,
            mutate=source_edit("**OUTER_RELEASE, **NO_REUSE}", "**OUTER_RELEASE}"),
        )
    )
    recordable = [group for group in GROUP_NAMES if group != "repository_hooks"]
    code, log = reuse_dry_run(work, out, "reuse_on", {"HAI_CI_REUSE": "main"}, scratch / "reuse_on_records")
    items = local_plan_of(log)["items"]
    checks = {
        "exit_zero": code == 0 and PASS_LINES["all"] in log,
        "every_recorded_group_is_reused": all(f"reused {group}:" in log for group in recordable),
        "no_reused_group_ran": "TIMED launcher E2E" not in log and not any(item in items for item in ("python_lint", "lakehouse_bronze", "sql_style")),
        "the_hook_partitions_still_ran": any(item.startswith("hook_") for item in items),
        "no_acquisition_shard_ran": not any(item.startswith("acquisition_") for item in items),
        "nothing_recorded_from_a_changed_tree": "not recorded" in log,
    }
    results.append({"case": "local_run_reuses_the_groups_recorded_on_main", "passed": all(checks.values()), "checks": checks, "items": items})
    probe = "dbt/models/silver/example_probe.sql"

    def add_probe(tree: Path) -> None:
        (tree / probe).parent.mkdir(parents=True, exist_ok=True)
        (tree / probe).write_text("select 1\n")

    code, log = reuse_dry_run(work, out, "reuse_changed_input", {"HAI_CI_REUSE": "main"}, scratch / "reuse_changed_records", edit=add_probe)
    hit = reuse_hit(probe)
    checks = {
        "exit_zero": code == 0 and PASS_LINES["all"] in log,
        "selected_groups_ran": all(f"reused {group}:" not in log for group in hit),
        "other_groups_reused": all(f"reused {group}:" in log for group in recordable if group not in hit),
    }
    results.append(
        {"case": "local_run_reuse_stops_for_a_changed_input", "passed": all(checks.values()) and bool(hit), "checks": checks, "selected": sorted(hit)}
    )
    acquisition_probe = "scripts/acquisition/example_probe.py"

    def add_acquisition_probe(tree: Path) -> None:
        (tree / acquisition_probe).write_text("EXAMPLE = 1\n")

    code, log = reuse_dry_run(
        work, out, "reuse_changed_full_only_input", {"HAI_CI_REUSE": "main"}, scratch / "reuse_full_only_records", edit=add_acquisition_probe
    )
    results.append({"case": "local_run_reuse_stops_for_a_changed_full_only_input", "passed": code == 0 and "reused acquisition:" not in log, "exit": code})

    def break_the_lookup(tree: Path) -> None:
        (tree / "scripts/quality/ci_main_passes.py").write_text("raise SystemExit(7)\n")

    code, log = reuse_dry_run(work, out, "reuse_lookup_fails", {"HAI_CI_REUSE": "main"}, scratch / "reuse_lookup_records", edit=break_the_lookup)
    items = local_plan_of(log)["items"]
    checks = {
        "exit_zero": code == 0 and PASS_LINES["all"] in log,
        "nothing_reused": "reused " not in log,
        "the_groups_still_ran": all(item in items for item in ("python_lint", "lakehouse_bronze", "sql_style")),
    }
    results.append({"case": "local_run_runs_every_group_when_the_reuse_lookup_fails", "passed": all(checks.values()), "checks": checks, "items": items})
    for label, env, why in (
        ("off", {"HAI_CI_REUSE": "off"}, "off by request"),
        ("under_the_gate", {"HAI_CI_REUSE": "main", "HAI_DOCKER_RELEASE": "outer"}, "the gate marker"),
        ("hosted_default", {}, "hosted default"),
        ("part_core", {"HAI_CI_REUSE": "main", "CI_PART": "core"}, "a partial run"),
        ("unit_subset", {"HAI_CI_REUSE": "main", "CI_UNITS": "checks"}, "a unit subset"),
    ):
        code, log = reuse_dry_run(work, out, f"reuse_{label}", env, scratch / f"reuse_{label}_records")
        results.append({"case": f"local_run_does_not_reuse_{label}", "passed": "reused " not in log and code == 0, "why": why, "exit": code})
    code, log = reuse_dry_run(work, out, "reuse_bad_value", {"HAI_CI_REUSE": "sometimes"}, scratch / "reuse_bad_records", record=False)
    results.append(
        {"case": "local_run_refuses_an_unknown_reuse_mode", "passed": code != 0 and "HAI_CI_REUSE must be main or off" in log and "TIMED" not in log}
    )
    return results


REUSE_GATE_CASES = ("--reuse-only", "--gate-stage-only")


def reuse_group_cases(work: Path) -> list[dict[str, Any]]:
    """The reuse, shard and gate stage cases are the check group reuse_gate_checks: their own part and hosted job, not a command of the Core part."""
    path = "scripts/quality/ci_reuse.py"
    results = [
        selection_case(
            work, "reuse_gate_part_selects_only_the_reuse_gate_group", ["--scope", "paths", "--path", path, "--part", "reuse_gate"], ["reuse_gate_checks"]
        ),
        selection_case(
            work,
            "core_part_leaves_the_reuse_gate_group_out",
            ["--scope", "paths", "--path", path, "--part", "core"],
            ["python_lint", "repository_code_checks", "configuration_checks"],
        ),
    ]
    body = (ROOT / GROUP_FILES / "reuse_gate_checks.sh").read_text(encoding="utf-8")
    core = (ROOT / GROUP_FILES / "repository_code_checks.sh").read_text(encoding="utf-8")
    jobs = workflow_document()["jobs"]
    steps = [step.get("env", {}) for step in jobs["reuse_gate"]["steps"] if step.get("env", {}).get("CI_PART") == "reuse_gate"]
    required = json.loads((ROOT / "config/quality/ci_jobs.json").read_text(encoding="utf-8"))["required_pull_request_jobs"]
    results.append(
        {
            "case": "reuse_and_gate_stage_cases_run_in_their_own_group_and_hosted_job",
            "passed": all(f"run_ci_groups_e2e {flag}" in body and f"run_ci_groups_e2e {flag}" not in core for flag in REUSE_GATE_CASES)
            and "run_ci_groups_e2e --selection-only" in core
            and len(steps) == 1
            and jobs["reuse_gate"]["name"] == "CI Reuse and Gate E2E"
            and "CI Reuse and Gate E2E" in required,
        }
    )
    return results


def head_of(root: Path) -> str:
    return probe_git(root, "rev-parse", "HEAD")


def runs_of(root: Path, stub: str = "run_ci") -> int:
    return sum(1 for call in pre_push_calls(root) if call["stub"] == stub)


def remote_of(root: Path) -> Path:
    return root.parent / f"{root.name}_remote.git"


def remote_refs(root: Path) -> set[str]:
    return set(probe_git(remote_of(root), "for-each-ref", "--format=%(refname)").split())


def push_decisions(root: Path) -> dict[str, str]:
    reports = [json.loads(path.read_text()) for path in sorted((root / "data/e2e/pre_push").glob("*/report.json"))]
    return {row["ref"]: row["decision"] for report in reports for row in report.get("refs", [])}


def push_environment(extra: dict[str, str] | None = None) -> dict[str, str]:
    environment = {key: value for key, value in os.environ.items() if not key.startswith("PRE_COMMIT_")}
    return {**environment, **GIT_IDENTITY, **(extra or {})}


def gate_repo(
    scratch: Path,
    name: str,
    changed: str | tuple[str, ...] = ("README.md", "notes.md"),
    mutate: Callable[[str], str] | None = None,
    dispatch: bool = True,
    shim: bool = True,
) -> Path:
    """A pre-push repository with a bare remote that holds ``main`` (the base commit), the shim and, with ``dispatch``, pre-commit's pre-push hook."""
    root = pre_push_repo(scratch, name, changed, mutate=mutate)
    remote = remote_of(root)
    probe_git(root, "init", "--bare", "--quiet", str(remote))
    probe_git(root, "remote", "add", "origin", str(remote))
    probe_git(root, "push", "--quiet", "origin", "main")
    if shim:
        pre_push(root, "install-shim")
    if dispatch:
        run_command(str(PYTHON), ["-m", "pre_commit", "install", "--hook-type", "pre-push"], cwd=root, env=push_environment(), timeout=120)
    return root


def git_push(root: Path, arguments: tuple[str, ...], config: tuple[str, ...] = (), extra: dict[str, str] | None = None) -> tuple[int, str]:
    prefix = [part for setting in config for part in ("-c", setting)]
    environment = push_environment({"STUB_LOG": str(root / "stub.log"), **(extra or {})})
    result = run_command("git", ["-C", str(root), *prefix, "push", *arguments], env=environment, timeout=300)
    return result.returncode, result.stdout + result.stderr


def push_case(
    scratch: Path,
    case: str,
    name: str,
    setup: Callable[[Path], None],
    arguments: tuple[str, ...],
    check: Callable[[Path, int, str], dict[str, bool]],
    mutate: Callable[[str], str] | None = None,
    config: tuple[str, ...] = (),
    extra: dict[str, str] | None = None,
    **repo: Any,
) -> dict[str, Any]:
    """Push from a scratch repository through Git, the shim and pre-commit; ``check`` names what must hold afterwards."""
    root = gate_repo(scratch, name, mutate=mutate, **repo)
    setup(root)
    code, text = git_push(root, arguments, config, extra)
    checks = check(root, code, text)
    return {"case": case, "passed": all(checks.values()), "checks": checks, "exit": code, "output": text[-300:]}


def known_branch(root: Path) -> None:
    probe_git(root, "branch", "known", "main")


def known_light_tag(root: Path) -> None:
    probe_git(root, "tag", "light", "main")


def known_annotated_tag(root: Path) -> None:
    probe_git(root, "tag", "--annotate", "--message", "known", "ann", "main")


def head_annotated_tag(root: Path) -> None:
    probe_git(root, "tag", "--annotate", "--message", "head", "ann", "HEAD")


def side_branch_at_parent(root: Path) -> None:
    probe_git(root, "push", "--quiet", "--no-verify", "origin", "HEAD~1:refs/heads/feature")
    probe_git(root, "branch", "side", "HEAD~1")


def known_and_side(root: Path) -> None:
    known_branch(root)
    side_branch_at_parent(root)


def known_refs_and_head(root: Path) -> None:
    known_branch(root)
    known_light_tag(root)


def recorded_known(ref: str) -> Callable[[Path, int, str], dict[str, bool]]:
    def check(root: Path, code: int, text: str) -> dict[str, bool]:
        return {
            "pushed": code == 0 and ref in remote_refs(root),
            "no_check_ran": runs_of(root) == 0,
            "recorded_as_known": push_decisions(root).get(ref) == "known",
        }

    return check


def new_branch_known_case(scratch: Path, mutate: Callable[[str], str] | None = None) -> dict[str, Any]:
    return push_case(
        scratch,
        "pre_push_new_branch_at_origin_main_is_recorded_without_checks",
        "gate_branch_known",
        known_branch,
        ("origin", "known"),
        recorded_known("refs/heads/known"),
        mutate,
    )


def light_tag_known_case(scratch: Path, mutate: Callable[[str], str] | None = None) -> dict[str, Any]:
    return push_case(
        scratch,
        "pre_push_lightweight_tag_at_origin_main_is_recorded_without_checks",
        "gate_light_tag",
        known_light_tag,
        ("origin", "refs/tags/light"),
        recorded_known("refs/tags/light"),
        mutate,
    )


def annotated_tag_known_case(scratch: Path, mutate: Callable[[str], str] | None = None) -> dict[str, Any]:
    return push_case(
        scratch,
        "pre_push_annotated_tag_at_origin_main_is_recorded_without_checks",
        "gate_annotated_known",
        known_annotated_tag,
        ("origin", "refs/tags/ann"),
        recorded_known("refs/tags/ann"),
        mutate,
    )


def annotated_tag_head_case(scratch: Path, mutate: Callable[[str], str] | None = None) -> dict[str, Any]:
    def check(root: Path, code: int, text: str) -> dict[str, bool]:
        return {
            "pushed": code == 0 and "refs/tags/ann" in remote_refs(root),
            "checked_once": runs_of(root) == 1,
            "recorded_as_checked": push_decisions(root).get("refs/tags/ann") == "checked",
        }

    return push_case(
        scratch, "pre_push_annotated_tag_at_head_is_checked_once", "gate_annotated_head", head_annotated_tag, ("origin", "refs/tags/ann"), check, mutate
    )


def force_push_case(scratch: Path, mutate: Callable[[str], str] | None = None) -> dict[str, Any]:
    def diverge(root: Path) -> None:
        probe_git(root, "checkout", "--quiet", "-b", "diverged", "main")
        (root / "other.txt").write_text("diverged\n")
        probe_git(root, "add", "other.txt")
        probe_git(root, "-c", "commit.gpgsign=false", "commit", "--quiet", "--message", "diverged")
        probe_git(root, "push", "--quiet", "--no-verify", "origin", "diverged:topic")
        probe_git(root, "checkout", "--quiet", "topic")

    def check(root: Path, code: int, text: str) -> dict[str, bool]:
        remote_topic = probe_git(remote_of(root), "rev-parse", "refs/heads/topic")
        return {"pushed": code == 0 and remote_topic == head_of(root), "checked_once": runs_of(root) == 1}

    return push_case(scratch, "pre_push_force_push_is_checked_once", "gate_force", diverge, ("--force", "origin", "topic"), check, mutate)


def unknown_ref_case(scratch: Path, mutate: Callable[[str], str] | None = None, dispatch: bool = True) -> dict[str, Any]:
    def check(root: Path, code: int, text: str) -> dict[str, bool]:
        return {
            "refused": code != 0 and "refs/heads/side" in text,
            "nothing_pushed": "refs/heads/side" not in remote_refs(root),
            "no_check_ran": runs_of(root) == 0,
        }

    return push_case(
        scratch,
        "pre_push_ref_neither_head_nor_known_is_refused" if dispatch else "pre_push_shim_alone_refuses_ref_neither_head_nor_known",
        "gate_unknown" if dispatch else "gate_unknown_shim_alone",
        side_branch_at_parent,
        ("origin", "side"),
        check,
        mutate,
        dispatch=dispatch,
    )


def shim_alone_unknown_ref_case(scratch: Path, mutate: Callable[[str], str] | None = None) -> dict[str, Any]:
    return unknown_ref_case(scratch, mutate, dispatch=False)


def known_then_unknown_case(scratch: Path, mutate: Callable[[str], str] | None = None) -> dict[str, Any]:
    def check(root: Path, code: int, text: str) -> dict[str, bool]:
        return {
            "refused": code != 0 and "refs/heads/side" in text,
            "nothing_pushed": not ({"refs/heads/known", "refs/heads/side"} & remote_refs(root)),
            "no_check_ran": runs_of(root) == 0,
        }

    return push_case(
        scratch,
        "pre_push_known_first_ref_does_not_hide_an_unknown_second_ref",
        "gate_known_then_unknown",
        known_and_side,
        ("origin", "known", "side"),
        check,
        mutate,
    )


def several_refs_case(scratch: Path, mutate: Callable[[str], str] | None = None) -> dict[str, Any]:
    def check(root: Path, code: int, text: str) -> dict[str, bool]:
        decided = push_decisions(root)
        return {
            "pushed": code == 0 and {"refs/heads/known", "refs/tags/light", "refs/heads/topic"} <= remote_refs(root),
            "checked_once": runs_of(root) == 1,
            "decisions": decided == {"refs/heads/known": "known", "refs/tags/light": "known", "refs/heads/topic": "checked"},
        }

    return push_case(
        scratch,
        "pre_push_several_refs_are_judged_each_and_checked_once",
        "gate_several",
        known_refs_and_head,
        ("origin", "known", "refs/tags/light", "topic"),
        check,
        mutate,
    )


def deletions_case(scratch: Path, mutate: Callable[[str], str] | None = None) -> dict[str, Any]:
    def seed(root: Path) -> None:
        probe_git(root, "push", "--quiet", "origin", "main:refs/heads/gone", "main:refs/heads/gone2")

    root = gate_repo(scratch, "gate_deletions", mutate=mutate)
    seed(root)
    only_code, only_text = git_push(root, ("origin", ":gone"))
    only_runs = runs_of(root)
    mixed_code, mixed_text = git_push(root, ("origin", "topic", ":gone2"))
    checks = {
        "deletion_only_allowed_without_check": only_code == 0 and only_runs == 0,
        "deletion_with_head_ref_checked_once": mixed_code == 0 and runs_of(root) == 1,
        "remote_branches_removed": not ({"refs/heads/gone", "refs/heads/gone2"} & remote_refs(root)),
    }
    return {
        "case": "pre_push_deletions_are_allowed_and_not_checked",
        "passed": all(checks.values()),
        "checks": checks,
        "output": (only_text + mixed_text)[-300:],
    }


def tree_tag_case(scratch: Path, mutate: Callable[[str], str] | None = None) -> dict[str, Any]:
    def tag_a_tree(root: Path) -> None:
        probe_git(root, "tag", "--annotate", "--message", "tree", "treetag", "main^{tree}")

    def check(root: Path, code: int, text: str) -> dict[str, bool]:
        return {
            "refused": code != 0 and "refs/tags/treetag" in text,
            "no_check_ran": runs_of(root) == 0,
            "nothing_pushed": "refs/tags/treetag" not in remote_refs(root),
        }

    return push_case(scratch, "pre_push_tag_of_a_tree_is_refused", "gate_tree_tag", tag_a_tree, ("origin", "refs/tags/treetag"), check, mutate)


def up_to_date_case(scratch: Path, mutate: Callable[[str], str] | None = None) -> dict[str, Any]:
    def check(root: Path, code: int, text: str) -> dict[str, bool]:
        return {"allowed": code == 0 and "up-to-date" in text, "no_check_ran": runs_of(root) == 0}

    return push_case(scratch, "pre_push_up_to_date_push_runs_nothing", "gate_up_to_date", lambda root: None, ("origin", "main"), check, mutate)


def tracked_change_ref_case(scratch: Path, mutate: Callable[[str], str] | None = None) -> dict[str, Any]:
    root = gate_repo(scratch, "gate_dirty_tree", mutate=mutate)
    known_branch(root)
    (root / "README.md").write_text("changed after the commit\n")
    head_code, head_text = git_push(root, ("origin", "topic"))
    head_runs = runs_of(root)
    known_code, known_text = git_push(root, ("origin", "known"))
    checks = {
        "head_ref_refused": head_code != 0 and "README.md" in head_text and head_runs == 0,
        "known_ref_allowed": known_code == 0 and "refs/heads/known" in remote_refs(root),
        "no_check_ran": runs_of(root) == 0,
    }
    return {
        "case": "pre_push_tracked_change_refuses_head_ref_only",
        "passed": all(checks.values()),
        "checks": checks,
        "output": (head_text + known_text)[-300:],
    }


def shim_presence_case(scratch: Path, mutate: Callable[[str], str] | None = None) -> dict[str, Any]:
    def turn_off(root: Path) -> None:
        probe_git(root, "config", "--local", f"hook.{SHIM_NAME}.enabled", "false")

    def other_command(root: Path) -> None:
        probe_git(root, "config", "--local", "--replace-all", f"hook.{SHIM_NAME}.command", "true")

    def remove(root: Path) -> None:
        probe_git(root, "config", "--local", "--remove-section", f"hook.{SHIM_NAME}")

    checks: dict[str, bool] = {}
    for label, setup in (("disabled", turn_off), ("other_command", other_command), ("removed", remove)):
        root = gate_repo(scratch, f"gate_shim_{label}", mutate=mutate)
        setup(root)
        code, text = git_push(root, ("origin", "topic"))
        checks[f"{label}_refused_with_hint"] = code != 0 and "install-shim" in text and runs_of(root) == 0 and "refs/heads/topic" not in remote_refs(root)
    return {"case": "pre_push_dispatched_push_without_the_shim_is_refused", "passed": all(checks.values()), "checks": checks}


def skip_in_push_case(scratch: Path, mutate: Callable[[str], str] | None = None) -> dict[str, Any]:
    def check(root: Path, code: int, text: str) -> dict[str, bool]:
        return {"pushed": code == 0, "gate_ran": runs_of(root) == 1, "acquisition_ran_once": runs_of(root, "acquisition") == 1}

    return push_case(
        scratch,
        "pre_push_skip_in_the_push_environment_does_not_disable_the_gate",
        "gate_skip_push",
        lambda root: None,
        ("origin", "topic"),
        check,
        mutate,
        extra={"SKIP": "local_ci_pushed,acquisition-checks"},
        changed=("scripts/acquisition/example.py", "README.md"),
    )


def skip_acquisition_case(scratch: Path, mutate: Callable[[str], str] | None = None) -> dict[str, Any]:
    root = pre_push_repo(scratch, "gate_skip_acquisition", ("scripts/acquisition/example.py", "README.md"), mutate=mutate)
    code, out, error = pre_push(root, "run", extra={"SKIP": "acquisition-checks,model-metadata"}, timeout=300)
    return {
        "case": "pre_push_skip_does_not_suppress_the_acquisition_hook_in_pre_commit",
        "passed": code == 0 and runs_of(root, "acquisition") == 1,
        "exit": code,
        "acquisition_runs": runs_of(root, "acquisition"),
        "output": (out + error)[-300:],
    }


def shim_case(scratch: Path, mutate: Callable[[str], str] | None = None) -> dict[str, Any]:
    root = pre_push_repo(scratch, "shim", mutate=mutate)
    installs = [pre_push(root, "install-shim")[0]]
    probe_git(root, "config", "--local", f"hook.{SHIM_NAME}.enabled", "false")
    installs.append(pre_push(root, "install-shim")[0])
    configured = run_command("git", ["-C", str(root), "config", "--local", "--get-all", f"hook.{SHIM_NAME}.command"], timeout=60)
    entries = configured.stdout.splitlines()
    active = pre_push(root, "check-shim")[0] == 0
    return {
        "case": "pre_push_install_shim_registers_and_enables_the_gate_once",
        "passed": installs == [0, 0] and entries == [SHIM_COMMAND_LINE_VALUE] and active,
        "installs": installs,
        "entries": entries,
        "active": active,
    }


def head_not_checked_out_case(scratch: Path, mutate: Callable[[str], str] | None = None) -> dict[str, Any]:
    root = pre_push_repo(scratch, "tree_head", mutate=mutate)
    probe_git(root, "branch", "side", "HEAD~1")
    code, out, error = pre_push(root, "run", arguments=("--head", "side"))
    return {
        "case": "pre_push_refuses_head_not_checked_out",
        "passed": code != 0 and "checked-out commit" in out + error and not pre_push_calls(root),
        "exit": code,
        "output": (out + error)[-300:],
    }


def tracked_change_case(scratch: Path, staged: bool, mutate: Callable[[str], str] | None = None) -> dict[str, Any]:
    root = pre_push_repo(scratch, "tree_staged" if staged else "tree_unstaged", mutate=mutate)
    (root / "README.md").write_text("changed after the commit\n")
    if staged:
        probe_git(root, "add", "README.md")
    code, out, error = pre_push(root, "run")
    return {
        "case": f"pre_push_refuses_{'staged' if staged else 'unstaged'}_change",
        "passed": code != 0 and "README.md" in out + error and not pre_push_calls(root),
        "exit": code,
        "output": (out + error)[-300:],
    }


def staged_change_case(scratch: Path, mutate: Callable[[str], str] | None = None) -> dict[str, Any]:
    return tracked_change_case(scratch, True, mutate)


def unstaged_change_case(scratch: Path, mutate: Callable[[str], str] | None = None) -> dict[str, Any]:
    return tracked_change_case(scratch, False, mutate)


def untracked_file_case(scratch: Path, mutate: Callable[[str], str] | None = None) -> dict[str, Any]:
    root = pre_push_repo(scratch, "tree_untracked", mutate=mutate)
    (root / "notes.txt").write_text("not tracked\n")
    code, out, error = pre_push(root, "run")
    return {"case": "pre_push_allows_untracked_file", "passed": code == 0 and bool(pre_push_calls(root)), "exit": code, "output": (out + error)[-300:]}


EXPECTED_CONTROLS = (
    "CI_SCOPE",
    "CI_PART",
    "CI_UNITS",
    "CI_LIST_ONLY",
    "RANGE_START",
    "RANGE_END",
    "LAUNCHER_SHARD",
    "FEATURE_SHARD",
    "ACQUISITION_PART",
    "ACQUISITION_SHARD",
    "ACQUISITION_SHARD_DIR",
    "ACQUISITION_SHARD_COUNT",
    "ACQUISITION_EXPECT",
    "ACQUISITION_RAN_MARKER",
    "GITHUB_ACTIONS",
    "HAI_DOCKER_RELEASE",
    "HAI_CI_REUSE",
    "PRE_COMMIT_FROM_REF",
    "PRE_COMMIT_ORIGIN",
    "PRE_COMMIT_SOURCE",
    "PRE_COMMIT_REMOTE_BRANCH",
    "PRE_COMMIT_LOCAL_BRANCH",
    "PRE_COMMIT_REMOTE_NAME",
    "PRE_COMMIT_REMOTE_URL",
    "SKIP",
    "PRE_COMMIT_ALLOW_NO_CONFIG",
    "PRE_COMMIT_RUNNING_LEGACY",
    "_PRE_COMMIT_SKIP_POST_CHECKOUT",
)
STEP_SETS = {
    "run_ci": {"CI_SCOPE", "RANGE_START", "RANGE_END", "HAI_DOCKER_RELEASE", "HAI_CI_REUSE"},
    "removed-names": {"PRE_COMMIT_FROM_REF", "PRE_COMMIT_TO_REF"},
}


def inherited_controls_case(scratch: Path, mutate: Callable[[str], str] | None = None) -> dict[str, Any]:
    root = pre_push_repo(scratch, "controls", mutate=mutate)
    poison = {name: "poison" for name in EXPECTED_CONTROLS}
    poison.update({"PRE_COMMIT_TO_REF": head_of(root), "GIT_DIR": str(root / ".git"), "GIT_WORK_TREE": str(root)})
    code, out, error = pre_push(root, "run", extra=poison)
    leaks: dict[str, list[str]] = {}
    for call in pre_push_calls(root):
        key = "run_ci" if call["stub"] == "run_ci" else "removed-names" if call["argv"][:1] == ["removed-names"] else call["stub"]
        extra_names = set(filter(None, call["seen"].split(","))) - STEP_SETS.get(key, set()) - NOT_CONTROLS
        if extra_names:
            leaks[key] = sorted(extra_names)
    return {"case": "pre_push_clears_inherited_controls", "passed": code == 0 and bool(pre_push_calls(root)) and not leaks, "exit": code, "leaks": leaks}


def unlisted_controls(texts: dict[str, str], listed: set[str]) -> list[str]:
    """Environment variables the CI scripts read that the pre-push run neither clears nor classes as harmless."""
    names: set[str] = set()
    for text in texts.values():
        names.update(re.findall(r"\$\{?([A-Z][A-Z0-9_]*)", text))
        names.update(re.findall(r"os\.environ(?:\.get)?[\[(]\s*\"([A-Z][A-Z0-9_]*)\"", text))
    return sorted(names - listed - NOT_CONTROLS)


def ci_script_texts() -> dict[str, str]:
    paths = [ROOT / path for path in CONTROL_SCRIPTS] + [path for pattern in CONTROL_PATTERNS for path in sorted(ROOT.glob(pattern))]
    return {path.relative_to(ROOT).as_posix(): path.read_text() for path in paths}


def control_scan_cases() -> list[dict[str, Any]]:
    listed = set(getattr(pre_push_module, "EXECUTION_CONTROLS", ()))
    texts = ci_script_texts()
    planted = {**texts, "scripts/run_ci.sh": texts["scripts/run_ci.sh"] + '\nif [[ "${CI_SKIP_GATES:-0}" == 1 ]]; then exit 0; fi\n'}
    found = unlisted_controls(planted, listed)
    return [
        {"case": "execution_controls_cover_the_ci_scripts", "passed": not unlisted_controls(texts, listed), "unlisted": unlisted_controls(texts, listed)},
        {"case": "planted_unlisted_control_caught_by_execution_controls_cover_the_ci_scripts", "passed": found == ["CI_SKIP_GATES"], "caught": found},
    ]


HARMLESS_PRE_COMMIT_VARIABLES = frozenset(
    {
        "PRE_COMMIT",
        "PRE_COMMIT_HOME",
        "PRE_COMMIT_COLOR",
        "PRE_COMMIT_NO_CONCURRENCY",
        "XDG_CACHE_HOME",
        "TRAVIS",
        "TERM",
        "PATH",
        "PATHEXT",
        "__PYVENV_LAUNCHER__",
        "PYTHONEXECUTABLE",
    }
)
PRE_COMMIT_READ = re.compile(
    r"""(?:environ(?:\.get)?[\[(]|getenv\()\s*['"]([A-Za-z_][A-Za-z0-9_]*)['"]|['"]([A-Za-z_][A-Za-z0-9_]*)['"]\s+in\s+(?:os\.)?environ"""
)


def pre_commit_sources() -> dict[str, str]:
    """The installed pre-commit modules that select and run hooks (not the language installers, which only prepare environments)."""
    package = Path(importlib.import_module("pre_commit").__file__ or "").parent
    return {path.relative_to(package).as_posix(): path.read_text() for path in sorted(package.rglob("*.py")) if "languages" not in path.parts}


def unlisted_pre_commit_variables(texts: dict[str, str], listed: set[str]) -> list[str]:
    """Environment variables pre-commit reads or sets that the pre-push run neither clears nor classes as harmless."""
    names = {name for text in texts.values() for match in PRE_COMMIT_READ.findall(text) for name in match if name}
    return sorted(names - listed - HARMLESS_PRE_COMMIT_VARIABLES)


def pre_commit_control_cases() -> list[dict[str, Any]]:
    listed = set(getattr(pre_push_module, "EXECUTION_CONTROLS", ()))
    texts = pre_commit_sources()
    planted = {**texts, "commands/run.py": texts["commands/run.py"] + f"\nonly = os.environ.get({'PRE_COMMIT_ONLY_HOOKS'!r})\n"}
    return [
        {
            "case": "execution_controls_cover_pre_commit",
            "passed": bool(texts) and not unlisted_pre_commit_variables(texts, listed),
            "unlisted": unlisted_pre_commit_variables(texts, listed),
        },
        {
            "case": "planted_new_pre_commit_variable_caught_by_execution_controls_cover_pre_commit",
            "passed": unlisted_pre_commit_variables(planted, listed) == ["PRE_COMMIT_ONLY_HOOKS"],
            "caught": unlisted_pre_commit_variables(planted, listed),
        },
        {
            "case": "planted_uncleared_skip_caught_by_execution_controls_cover_pre_commit",
            "passed": unlisted_pre_commit_variables(texts, listed - {"SKIP"}) == ["SKIP"],
            "caught": unlisted_pre_commit_variables(texts, listed - {"SKIP"}),
        },
    ]


def entry_scripts() -> list[str]:
    text = (ROOT / ".github/workflows/ci.yml").read_text()
    modules = sorted(set(re.findall(r"-m scripts\.quality\.(ci_\w+)", text)))
    return sorted(set(re.findall(r"bash (scripts/[\w./]+\.sh)", text))) + [f"scripts/quality/{module}.py" for module in modules]


def uncovered_entries(patterns: list[str], entries: list[str]) -> list[str]:
    return [entry for entry in entries if not any(re.search(pattern, entry) for pattern in patterns)]


def entry_cases() -> list[dict[str, Any]]:
    patterns = json.loads((ROOT / "config/quality/ci_groups.json").read_text())["pre_push"]["full_paths"]
    entries = entry_scripts()
    damaged = [pattern for pattern in patterns if "run_checks_" not in pattern]
    return [
        {
            "case": "full_paths_cover_ci_entry_scripts",
            "passed": bool(entries) and not uncovered_entries(patterns, entries),
            "uncovered": uncovered_entries(patterns, entries),
        },
        {
            "case": "planted_entry_gap_caught_by_full_paths_cover_ci_entry_scripts",
            "passed": uncovered_entries(damaged, entries) == ["scripts/acquisition/run_checks_ci.sh"],
            "caught": uncovered_entries(damaged, entries),
        },
    ]


def acquisition_plan_case(scratch: Path, mutate: Callable[[str], str] | None = None) -> dict[str, Any]:
    root = pre_push_repo(scratch, "acq_plan", mutate=mutate)
    plan = pre_push_plan(root)
    argv = {step["name"]: step["argv"] for step in plan["steps"]}.get("acquisition", [])
    want = ["run", "acquisition-checks", "--hook-stage", "pre-push", "--from-ref", plan["start"], "--to-ref", plan["end"]]
    return {"case": "pre_push_acquisition_step_uses_the_computed_range", "passed": bool(plan["start"]) and argv[-len(want) :] == want, "argv": argv}


def acquisition_run_case(
    scratch: Path, reaches: bool, mutate: Callable[[str], str] | None = None, config_edit: Callable[[str], str] | None = None
) -> dict[str, Any]:
    changed = ("scripts/acquisition/example.py", "README.md") if reaches else ("README.md",)
    root = pre_push_repo(scratch, "acq_reaches" if reaches else "acq_misses", changed, mutate=mutate, config_edit=config_edit)
    code, out, error = pre_push(root, "run", timeout=300)
    runs = sum(1 for call in pre_push_calls(root) if call["stub"] == "acquisition")
    name = "pre_push_acquisition_runs_once_for_the_computed_range" if reaches else "pre_push_acquisition_skipped_when_range_misses_it"
    return {"case": name, "passed": code == 0 and runs == (1 if reaches else 0), "exit": code, "runs": runs, "output": (out + error)[-300:]}


def acquisition_reaches_case(scratch: Path, mutate: Callable[[str], str] | None = None) -> dict[str, Any]:
    return acquisition_run_case(scratch, True, mutate)


def acquisition_misses_case(scratch: Path, config_edit: Callable[[str], str] | None = None) -> dict[str, Any]:
    return acquisition_run_case(scratch, False, config_edit=config_edit)


def drop_files_filter(text: str) -> str:
    edited, count = re.subn(r"(id: acquisition-checks.*?)\n\s+files: [^\n]*", r"\1", text, count=1, flags=re.S)
    if not count:
        raise ValueError("planted edit does not apply: acquisition-checks files filter")
    return edited


def wrapper_case(scratch: Path, mutate: Callable[[str], str] | None = None) -> dict[str, Any]:
    root = pre_push_repo(scratch, "acq_wrapper")
    wrapper = root / "scripts/acquisition/run_checks_pushed.sh"
    wrapper.write_text(mutate(wrapper.read_text()) if mutate else wrapper.read_text())
    environment = {key: value for key, value in os.environ.items() if not key.startswith("PRE_COMMIT_")}
    ran: list[bool] = []
    for remote in ("origin", ""):
        log = root / f"acquisition_{remote or 'direct'}.log"
        extra = {"PRE_COMMIT_REMOTE_NAME": remote} if remote else {}
        result = run_command("bash", ["scripts/acquisition/run_checks_pushed.sh"], cwd=root, env={**environment, **extra, "STUB_LOG": str(log)}, timeout=60)
        ran.append(log.exists() and result.returncode == 0)
    return {"case": "acquisition_wrapper_skips_the_native_push", "passed": ran == [False, True], "ran": ran}


def stale_docs_problems(text: str) -> dict[str, bool]:
    flat = " ".join(text.split())
    return {
        "no_staged_commit_hook": "commit hook (`CI_SCOPE=staged`)" not in flat and "local_ci_affected" not in flat,
        "release_requirement": "before every release" in flat,
        "shim_documented": "install-shim" in flat,
        "no_first_ref_only": "Only the first pushed reference" not in flat,
        "gate_is_the_shim": "pre_push run --hook" in flat and "check-shim" in flat,
        "no_captured_input": "keeps the whole push input" not in flat,
        "skip_cleared": "`SKIP`" in flat,
    }


def runner_doc_describes_range(text: str) -> bool:
    flat = " ".join(text.split())
    return "skips once after a passing full run" not in flat and "runs once over the computed range" in flat


def docs_cases() -> list[dict[str, Any]]:
    text = (ROOT / "docs/quality_checks.md").read_text()
    found = stale_docs_problems(text)
    stale = stale_docs_problems(text + "\nThe commit hook\n(`CI_SCOPE=staged`) always runs the groups the staged paths select.\n")
    captured = stale_docs_problems(text + "\nA Git config hook keeps the whole push input for the pre-push hook.\n")
    return [
        {"case": "quality_docs_match_the_hooks", "passed": all(found.values()), "checks": found},
        {"case": "planted_stale_paragraph_caught_by_quality_docs_match_the_hooks", "passed": not stale["no_staged_commit_hook"], "caught": stale},
        {"case": "runner_describes_the_computed_range", "passed": runner_doc_describes_range(__doc__ or "")},
        {
            "case": "planted_marker_description_caught_by_runner_describes_the_computed_range",
            "passed": not runner_doc_describes_range((__doc__ or "") + " The acquisition hook skips once after a passing full run."),
        },
        {"case": "planted_capture_paragraph_caught_by_quality_docs_match_the_hooks", "passed": not captured["no_captured_input"], "caught": captured},
    ]


def hook_and_tree_cases(scratch: Path) -> list[dict[str, Any]]:
    """The hook run judges every pushed ref against the checked-out commit and tree; an inherited control never reaches a step."""
    real = [
        new_branch_known_case,
        light_tag_known_case,
        annotated_tag_known_case,
        annotated_tag_head_case,
        force_push_case,
        unknown_ref_case,
        shim_alone_unknown_ref_case,
        known_then_unknown_case,
        several_refs_case,
        deletions_case,
        tree_tag_case,
        up_to_date_case,
        tracked_change_ref_case,
        shim_presence_case,
        skip_in_push_case,
        skip_acquisition_case,
        shim_case,
        head_not_checked_out_case,
        staged_change_case,
        unstaged_change_case,
        untracked_file_case,
        inherited_controls_case,
    ]
    results = [case(scratch) for case in real]
    planted = [
        ("shim_without_the_gate", unknown_ref_case, SHIM_COMMAND_LINE, SHIM_COMMAND_LINE.replace("run --hook", "check-shim")),
        ("first_ref_only", known_then_unknown_case, "for ref, sha in updates:", "for ref, sha in updates[:1]:"),
        ("known_rule_dropped", new_branch_known_case, 'if git(root, "merge-base", "--is-ancestor", commit, "origin/main")[0] == 0:', "if False:"),
        ("known_rule_for_tags_dropped", annotated_tag_known_case, 'if git(root, "merge-base", "--is-ancestor", commit, "origin/main")[0] == 0:', "if False:"),
        ("any_commit_checked", unknown_ref_case, 'return "refused", f"is neither in origin/main nor', 'return "checked", f"is neither in origin/main nor'),
        ("deletion_refused", deletions_case, 'if not sha.strip("0"):', "if False:"),
        ("tag_not_peeled", annotated_tag_head_case, 'f"{name}^{{commit}}"', "name"),
        ("nothing_to_check_still_checks", up_to_date_case, 'if not any(row["decision"] == "checked" for row in decisions):', "if False:"),
        ("known_ref_not_recorded", new_branch_known_case, "record(output, None, [], decisions)", "pass"),
        ("tree_not_judged", tracked_change_ref_case, 'problems = refusals(root, chosen["end"])', "problems = []"),
        ("shim_disabled_accepted", shim_presence_case, "active = listed_code == 0 and SHIM_NAME in", "active = True or SHIM_NAME in"),
        ("shim_command_unchecked", shim_presence_case, "commands.splitlines() == [SHIM_COMMAND]", "True"),
        ("shim_stacks_entries", shim_case, '"--replace-all"', '"--add"'),
        ("shim_left_disabled", shim_case, ', (f"hook.{SHIM_NAME}.enabled", "true")', ""),
        ("other_head_accepted", head_not_checked_out_case, "if end != current:", "if False:"),
        ("staged_change_accepted", staged_change_case, "if dirty:", "if False:"),
        ("unstaged_change_accepted", unstaged_change_case, "if dirty:", "if False:"),
        ("untracked_file_blocks", untracked_file_case, '"--untracked-files=no"', '"--untracked-files=normal"'),
        ("skip_kept_for_the_acquisition_hook", skip_acquisition_case, '    "SKIP",\n', ""),
        ("skip_kept_in_the_push", skip_in_push_case, '    "SKIP",\n', ""),
        ("skip_kept_in_a_step", inherited_controls_case, '    "SKIP",\n', ""),
        ("allow_no_config_kept", inherited_controls_case, '    "PRE_COMMIT_ALLOW_NO_CONFIG",\n', ""),
        ("running_legacy_kept", inherited_controls_case, '    "PRE_COMMIT_RUNNING_LEGACY",\n', ""),
        ("post_checkout_skip_kept", inherited_controls_case, '    "_PRE_COMMIT_SKIP_POST_CHECKOUT",\n', ""),
        ("list_only_control_kept", inherited_controls_case, '"CI_LIST_ONLY",', ""),
        ("native_ref_variable_kept", inherited_controls_case, '"PRE_COMMIT_FROM_REF",', ""),
        ("hosted_marker_variable_kept", inherited_controls_case, '"GITHUB_ACTIONS",', ""),
    ]
    results += [planted_case(label, case, scratch, mutate=source_edit(old, new)) for label, case, old, new in planted]
    return results


def acquisition_cases(scratch: Path) -> list[dict[str, Any]]:
    """The acquisition gate runs once, from the computed range, with the path filter of the hook; the native push hook leaves it to that run."""
    native = '"--from-ref", start'
    results = [
        acquisition_plan_case(scratch),
        acquisition_reaches_case(scratch),
        acquisition_misses_case(scratch),
        wrapper_case(scratch),
        planted_case("acquisition_native_range", acquisition_plan_case, scratch, mutate=source_edit(native, '"--from-ref", end')),
        planted_case("acquisition_from_previous_push", acquisition_reaches_case, scratch, mutate=source_edit(native, '"--from-ref", end')),
        planted_case("acquisition_path_filter_dropped", acquisition_misses_case, scratch, config_edit=drop_files_filter),
        planted_case(
            "native_push_runs_acquisition_again",
            wrapper_case,
            scratch,
            mutate=source_edit('if [[ -n "${PRE_COMMIT_REMOTE_NAME:-}" ]]; then', "if false; then"),
        ),
    ]
    script = (ROOT / "scripts/acquisition/run_checks_ci.sh").read_text()
    results.append(
        {"case": "hosted_acquisition_job_uses_the_push_stage", "passed": "--hook-stage pre-push" in script and "--hook-stage pre-commit" not in script}
    )
    return results


STAGE_RUNNERS = ("scripts/quality/run_model_metadata_e2e.py", "scripts/quality/run_checks_e2e.py", "scripts/quality/run_lint_coverage_e2e.py")
MOVED_HOOKS = ("model-metadata", "build-contexts", "sqlfluff", "orphan-files", "unused-code")
FAST_HOOKS_IN_RUNNERS = ("ruff", "privacy-scan", "docker-calls")


def runner_stage_problems(sources: dict[str, str], stages: dict[str, str]) -> list[str]:
    """Runners that start a moved hook without naming its stage, and hooks that resolve to the wrong stage."""
    problems = [
        f"{name} does not pass --hook-stage from hook_stage()" for name, text in sources.items() if "hook_stage(" not in text or "--hook-stage" not in text
    ]
    problems += [f"{hook} resolves to {stages.get(hook)!r}, not 'manual'" for hook in MOVED_HOOKS if stages.get(hook) != "manual"]
    problems += [f"{hook} resolves to {stages.get(hook)!r}, not 'pre-commit'" for hook in FAST_HOOKS_IN_RUNNERS if stages.get(hook) != "pre-commit"]
    return problems


def resolved_stages(config: Path) -> dict[str, str]:
    try:
        resolver = importlib.import_module("scripts.quality.hook_stage").hook_stage
    except ImportError:
        return {}
    return {hook: resolver(config, hook) for hook in (*MOVED_HOOKS, *FAST_HOOKS_IN_RUNNERS)}


def runner_stage_cases(scratch: Path) -> list[dict[str, Any]]:
    """Every runner that starts a hook through pre-commit names the stage the hook now lives in."""
    sources = {name: (ROOT / name).read_text() for name in STAGE_RUNNERS}
    config = ROOT / ".pre-commit-config.yaml"
    stages = resolved_stages(config)
    problems = runner_stage_problems(sources, stages)
    unstaged = {name: text.replace("hook_stage(", "hook_name(") for name, text in sources.items()}
    damaged = scratch / "hook_config_damaged.yaml"
    damaged.write_text(re.sub(r"(id: orphan-files.*?\n\s+stages: )\[manual\]", r"\1[pre-commit, manual]", config.read_text(), count=1, flags=re.S))
    return [
        {"case": "runners_name_the_stage_of_moved_hooks", "passed": not problems, "problems": problems},
        {
            "case": "planted_runner_without_stage_caught_by_runners_name_the_stage_of_moved_hooks",
            "passed": any("does not pass" in problem for problem in runner_stage_problems(unstaged, stages)),
        },
        {
            "case": "planted_moved_hook_back_in_commit_stage_caught_by_runners_name_the_stage_of_moved_hooks",
            "passed": bool(stages) and any("orphan-files" in problem for problem in runner_stage_problems(sources, resolved_stages(damaged))),
        },
    ]


def main() -> int:
    clear_git_environment()
    if "--gate-stage-only" in sys.argv[1:]:
        with tempfile.TemporaryDirectory(prefix="ci_gate_stage_e2e.") as folder:
            gate_results = gate_stage_cases(Path(folder))
        gate_failed = [row["case"] for row in gate_results if not row["passed"]]
        for row in gate_results:
            sys.stdout.write(("PASS " if row["passed"] else "FAIL ") + row["case"] + "\n")
        sys.stdout.write(f"{len(gate_results) - len(gate_failed)}/{len(gate_results)} passed (iteration mode, not commit evidence)\n")
        return int(bool(gate_failed))
    if "--sharding-only" in sys.argv[1:]:
        with tempfile.TemporaryDirectory(prefix="ci_sharding_e2e.") as folder:
            shard_results = parallel_ci_cases(Path(folder)) + parallel_plan_cases(Path(folder))
        out = ROOT / "data/e2e/ci_groups" / f"{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}_{os.getpid()}"
        out.mkdir(parents=True)
        shard_report = {
            "mode": "iteration",
            "note": "not commit evidence",
            "code": code_identity(ROOT),
            "cases": shard_results,
            "failed": [row["case"] for row in shard_results if not row["passed"]],
        }
        (out / "report.json").write_text(json.dumps(shard_report, indent=2) + "\n")
        sys.stdout.write(json.dumps(shard_report, indent=2) + "\n")
        return int(bool(shard_report["failed"]))
    selection_only = "--selection-only" in sys.argv[1:]
    speed_only = "--speed-only" in sys.argv[1:]
    reuse_only = "--reuse-only" in sys.argv[1:]
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
        if speed_only:
            results += speed_static_cases(scratch) + speed_real_cases(work, out)
        elif reuse_only:
            results += reuse_cases(work, out, scratch)
        else:
            results.append(scratch_cleanup_interrupt_case(scratch))
            results += selection_cases(work)
            results += fast_hook_cases(scratch)
            results += pre_push_cases(work, scratch, gate_stage=not selection_only)
            results += reuse_group_cases(work)
            if not selection_only:
                results += reuse_cases(work, out, scratch)
            results += runner_stage_cases(scratch)
            results += speed_static_cases(scratch)
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
        "gold_units_run_alone_then_together",
        "gold_unit_selection_*",
        "timing_*_in_the_run",
        "timing_interrupted_run_fails",
        "gates_part_*",
        "model_metadata_workers_*",
        "unknown_unit_is_refused",
        "local_*",
        "body_file_missing_*",
        "body_file_unreadable_*",
        "staged_body_edit_runs_only_its_group_items",
        "real_run_*",
        "reuse_*",
        "gate_stage_*",
    ]
    mode = {"mode": "iteration", "skipped": skipped, "note": "not commit evidence"} if selection_only or speed_only or reuse_only else {"mode": "full"}
    report = {"started": out.name, **mode, "code": identity, "cases": results, "passed": len(results) - len(failed), "total": len(results), "failed": failed}
    (out / "report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    sys.stdout.write(f"Bash used by the group-file guard cases: {identity.get('tools', {}).get('bash (the group-file guard)', 'unknown')}\n")
    for result in results:
        sys.stdout.write(("PASS " if result["passed"] else "FAIL ") + result["case"] + "\n")
    sys.stdout.write(f"{report['passed']}/{report['total']} passed; report {out / 'report.json'}\n")
    return 0 if not failed else 1


if __name__ == "__main__":
    raise SystemExit(main())
