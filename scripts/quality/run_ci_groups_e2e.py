"""E2E for the check-group selection of scripts/run_ci.sh (scripts/quality/ci_groups.py and config/quality/ci_groups.json).

Run from the repository root: ``.venv/bin/python -m scripts.quality.run_ci_groups_e2e``. It adds a scratch Git worktree at HEAD,
copies the working check inputs into it, then checks which groups each scope selects: committed ranges
of known content, unusable ranges, staged changes (a deletion and a rename included), explicit paths, the full scope and
invalid input. Real runs of the CI entry check that the unconditional gates run in every part (also with an empty selection)
and before the launcher E2E, that a failing gate fails every part, that the full Markdown lint runs as a gate in every scope
and that the entry plan alone (``CI_LIST_ONLY=1``) runs nothing.
A static case requires the group functions in scripts/ci/groups/ to start the secret store E2E, the gold writer set-up mode and the scratch worktree E2E.
Body-file cases: a staged edit to each body file selects its group (and the layers downstream of it) and the repository group, never another group;
the dispatcher and the workflow select every group; planted drift for each rule of the guard
scripts/quality/ci_group_files.py fails with the file and group named. Dry-run cases run the dispatcher with a stand-in ``timed`` that only prints the
command: a missing or unreadable body file stops the run with its name, the full local run names the same groups, gold units and acquisition shards as
the GitHub jobs (a planted workflow with three shards differs), a failed shard stops the run before the combining step and a missing shard fails it.
One case runs this runner under a real pre-commit hook that inherits an alternate ``GIT_INDEX_FILE``, ``GIT_DIR`` and ``GIT_WORK_TREE`` and checks that
the caller's index, staged diff and working files survive. The report goes to
data/e2e/ci_groups/<UTC>_<pid>/report.json with the revision, the SHA-256 of the files under test and the tool versions;
the run fails when a file under test changes while it runs.

The gate stage and launcher shard cases are the check group gate_stage_checks (hosted job CI Gate Stage E2E): ``--launcher-shards-only`` and
``--gate-stage-only`` run them, ``--selection-only`` (the Core CI tooling job) leaves them out and a run without a mode runs all.

Hook-stage cases (selection-only mode runs them): the commit stage holds exactly the fast hooks and the path hooks (the checks of the old on-demand group
that run at commit only when a file they read is staged, never ``always_run``, each selecting its inputs), the pre-push stage holds ``local_ci_pushed`` (the
shim check), and every hook of the manual stage that hosted CI runs is unchanged (pinned fingerprints) and every manual hook is in a hosted partition.
Push gate cases build small repositories with stand-ins for the commands: the plan holds the changed-file checks, the commit messages, the removed
names, the secret scan and the privacy scan of the files, the same scans of the commits the push sends and of its tag messages
(real scanners on scratch repositories: a value that one commit adds and the next removes is found, also on a new branch with no merge base) and OSV
over the range, and no step of the old gate, a failing step is named, a step that outlives its deadline fails visibly, and a missing base leaves out
the checks that read a range. Pushes through Git, the shim and pre-commit in scratch repositories cover a new branch, a lightweight tag and an
annotated tag in ``origin/main`` (recorded, its message scanned), an annotated tag and a force push at the checked-out commit (checked once), a ref
that pre-commit does not dispatch for, several refs, deletions, a tag of a tree, an up-to-date push, a tracked change and a missing, disabled or
changed shim; an inherited ``SKIP`` and the other pre-commit selection variables never reach the gate or a step. Planted defects (a heavy hook back in
the commit stage, a dropped privacy hook, a missing install type, an edited manual entry, a hook in no partition, a scan left out of the plan) are
caught by named cases.

The workflow cases require the triggers of ``.github/workflows/ci.yml`` (a push to every branch but main and a manual run), the concurrency group of
one branch, the pushed commit in every checkout and a job list equal to ``config/quality/ci_jobs.json``.
``scripts/run_ci.sh`` releases Docker once after a run on macOS and skips it on hosted CI, off macOS, in a list-only run and in the gates part
(``release_docker`` runs on its own with a stand-in interpreter).
"""

from __future__ import annotations

import base64
import contextlib
import copy
import hashlib
import importlib
import json
import os
import re
import shlex
import shutil
import signal
import sys
import tempfile
import threading
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest import mock

from scripts.lakehouse import feature_shards
from scripts.process import TimeoutExpired, clear_git_environment, find_program, list_processes, run_command
from scripts.quality import case_shards, ci_groups, ci_hooks, core_commands, scratch_marker
from scripts.quality import pre_push as pre_push_module
from scripts.quality.repo_checks import load_yaml
from scripts.quality.scratch_worktree import ScratchWorktrees
from scripts.quality.source_reads import source_inputs

ROOT = Path(__file__).resolve().parents[2]
PYTHON = ROOT / ".venv" / "bin" / "python"
RUNNER = "scripts/quality/run_ci_groups_e2e.py"
GROUP_FILES = "scripts/ci/groups"
BODY_FILES = tuple(sorted(path.relative_to(ROOT).as_posix() for path in (ROOT / GROUP_FILES).glob("*.sh")))


def config_reads_of(text: str) -> list[str]:
    """The words of the config_read_by_stacks loop of a group file, read as the shell reads them: continuations removed, quotes resolved."""
    found = re.search(r"for\s+config_read_by_stacks\s+in\s+([^;]*?)\s*;\s*do\b", text.replace("\\\n", ""))
    words = shlex.split(found.group(1), posix=True) if found else []
    if not words:
        raise ValueError("terraform_checks.sh no longer lists the files its stacks read in a config_read_by_stacks loop")
    return words


def stack_config_reads() -> list[str]:
    """The repository files that the Terraform checks copy into every stack folder, read from the list in the group file."""
    return config_reads_of((ROOT / GROUP_FILES / "terraform_checks.sh").read_text(encoding="utf-8"))


def stack_config_read_cases() -> list[dict[str, Any]]:
    """The loop list is read as the shell reads it: bare, single-quoted and double-quoted words, backslash-newline continuations and any whitespace around the
    terminator; a missing or empty loop is refused."""
    wanted = ["config/a.json", "scripts/b.py", "dir with space/c.json"]
    loop = "    for config_read_by_stacks in {words}{end}\n      cp x y\n    done\n"
    forms = {
        "bare_words": ("config/a.json scripts/b.py 'dir with space/c.json'", "; do"),
        "double_quotes": ('"config/a.json" "scripts/b.py" "dir with space/c.json"', "; do"),
        "single_quotes": ("'config/a.json' 'scripts/b.py' 'dir with space/c.json'", "; do"),
        "continuations": ("config/a.json \\\n      scripts/b.py \\\n      'dir with space/c.json' \\\n     ", "; do"),
        "tight_terminator": ("config/a.json scripts/b.py 'dir with space/c.json'", ";do"),
        "spaced_terminator": ("config/a.json scripts/b.py 'dir with space/c.json'", " ;  do"),
        "terminator_on_next_line": ("config/a.json scripts/b.py 'dir with space/c.json'", ";\n    do"),
    }
    results = []
    for name, (words, end) in forms.items():
        try:
            found = config_reads_of(loop.format(words=words, end=end))
        except ValueError as error:
            found = [f"error: {error}"]
        results.append({"case": f"stack_config_reads_{name}", "passed": found == wanted, "found": found, "wanted": wanted})
    for name, text in {"missing_loop": "    for other in a b; do\n    done\n", "empty_loop": "    for config_read_by_stacks in ; do\n    done\n"}.items():
        try:
            config_reads_of(text)
            refused = False
        except ValueError:
            refused = True
        results.append({"case": f"stack_config_reads_{name}_is_refused", "passed": refused})
    real = stack_config_reads()
    results.append(
        {
            "case": "stack_config_reads_real_list_names_existing_files",
            "passed": bool(real) and all((ROOT / path).is_file() for path in real),
            "count": len(real),
        }
    )
    # The services and jobs stacks are checked by their own group (terraform_services_checks), which copies the same files beside each of its stacks.
    other = config_reads_of((ROOT / GROUP_FILES / "terraform_services_checks.sh").read_text(encoding="utf-8"))
    results.append({"case": "stack_config_reads_are_the_same_in_both_terraform_stack_groups", "passed": other == real, "first": real, "second": other})
    return results


COPIED = (
    ".env.example",
    ".github/workflows/ci.yml",
    ".markdownlint-cli2.jsonc",
    ".pre-commit-config.yaml",
    ".sqlfluff",
    ".yamllint",
    "README.md",
    "uv.lock",
    "config/quality/ci_groups.json",
    "config/quality/job_durations.json",
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
    "scripts/acquisition/run_checks.sh",
    "scripts/quality/pre_push.py",
    "scripts/lakehouse/archive_saved_models.py",
    "scripts/lakehouse/run_bronze_e2e.py",
    "scripts/lakehouse/run_launcher_e2e.py",
    "scripts/lakehouse/run_pins_e2e.py",
    "scripts/lakehouse/run_provenance_e2e.py",
    "scripts/orchestration/offline_checks.py",
    "scripts/orchestration/run_pipeline_light.py",
    "scripts/orchestration/run_publish_contracts.py",
    "orchestration/graph.py",
    "orchestration/network_policy.py",
    "services/orchestration/egress_proxy.json",
    "scripts/quality/ci_groups.py",
    "scripts/quality/core_commands.py",
    "config/quality/core_commands.json",
    "config/quality/feature_case_costs.json",
    "scripts/quality/launcher_inputs.py",
    "scripts/quality/source_reads.py",
    "scripts/quality/lint_sources.py",
    "scripts/quality/documentation_review.py",
    "scripts/quality/repo_checks.py",
    "scripts/quality/run_checks_e2e.py",
    "scripts/quality/run_lint_coverage_e2e.py",
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
    "scripts/orchestration/run_network_classes_e2e.py",
    "scripts/orchestration/traffic_probe.py",
    "scripts/orchestration/transform_fixtures.py",
    "scripts/quality/run_ci_groups_e2e.py",
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
    "config/quality/ci_jobs.json",
    "scripts/quality/run_model_metadata_e2e.py",
    "scripts/quality/dbt_manifest_labels.py",
    "scripts/quality/label_guard_fixtures.py",
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
FEATURE_COUNTS = json.loads((ROOT / "config/quality/ci_groups.json").read_text(encoding="utf-8"))["feature_shards"]
LAYERS = ["lakehouse_bronze", "lakehouse_silver", "lakehouse_gold", "lakehouse_features"]
CORE = [
    "python_lint",
    "orchestration_checks",
    "orchestration_engine_e2e",
    "repository_code_checks",
    "ci_tooling_checks",
    "ci_tooling_runtime",
    "configuration_checks",
    "configuration_policy_checks",
    "configuration_host_checks",
    "configuration_owner_checks",
    "sql_style",
    "terraform_checks",
    "terraform_services_checks",
    "terraform_proofs",
    "terraform_replace_proofs",
]
# The groups split from another group select on the same paths, apart from their own body file: a path that selects the origin selects the split group too.
SPLIT_FROM = {
    "orchestration_engine_e2e": "orchestration_checks",
    "ci_tooling_checks": "repository_code_checks",
    "ci_tooling_runtime": "ci_tooling_checks",
    "configuration_host_checks": "configuration_checks",
    "configuration_policy_checks": "configuration_checks",
    "configuration_owner_checks": "configuration_host_checks",
    "terraform_proofs": "terraform_checks",
    "terraform_services_checks": "terraform_checks",
    "terraform_replace_proofs": "terraform_proofs",
}


def with_split_groups(wanted: list[str]) -> list[str]:
    """The groups a path selects: those of ``wanted`` and the groups split from them, a group split from a split group included."""
    selected = list(wanted)
    while added := [group for group, origin in SPLIT_FROM.items() if origin in selected and group not in selected]:
        selected += added
    return selected


WITH_LAYERS = [CORE[0], *LAYERS, *CORE[1:]]
PART_GROUPS = {
    "core": CORE,
    "launcher": ["launcher"],
    "gate_stage": ["gate_stage_checks"],
    "bronze": LAYERS[:1],
    "silver": LAYERS[1:2],
    "gold": LAYERS[2:3],
    "features": LAYERS[3:],
    "feature_sources": LAYERS[3:],
    "feature_columns": LAYERS[3:],
    "feature_store": LAYERS[3:],
    "core+layers": WITH_LAYERS,
    "all": [*WITH_LAYERS, "launcher", "gate_stage_checks", "acquisition", "repository_hooks"],
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
    "scripts/lakehouse/memory_budget_cli.py": LAKEHOUSE_SCRIPT,
    "scripts/lakehouse/dbt_package_archive.py": LAKEHOUSE_SCRIPT,
    "scripts/lakehouse/__init__.py": LAKEHOUSE_SCRIPT,
    "docker-compose.yaml": [*LAYERS, "orchestration_checks", "repository_code_checks"],
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


def selection_case(work: Path, name: str, args: list[str], want_run: list[str], want_reason: str = "", split: bool = True) -> dict[str, Any]:
    part = args[args.index("--part") + 1] if "--part" in args else "all"
    scope = args[args.index("--scope") + 1] if "--scope" in args else ""
    if split:
        want_run = with_split_groups(want_run)
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
    tracked = set(git(work, "ls-files").splitlines())
    new_files = [path for path in COPIED if (work / path).exists() and path not in tracked]
    if new_files:
        git(work, "add", "--intent-to-add", "--", *new_files)


def restore(work: Path) -> None:
    scratch_marker.require(work, "restore")
    git(work, "reset", "--quiet", "--hard", "HEAD")
    install_copies(work)


def foreign_repository(scratch: Path, name: str) -> tuple[Path, str]:
    """A repository that no runner made, created apart from the run, with a committed file and an uncommitted edit of it; the edit's text comes back too."""
    folder = scratch / name
    folder.mkdir()
    git(folder, "init", "--quiet")
    git(folder, "config", "user.name", "foreign owner")
    git(folder, "config", "user.email", "foreign@example.invalid")
    (folder / "notes.txt").write_text("committed\n", encoding="utf-8")
    git(folder, "add", "notes.txt")
    git(folder, "commit", "--quiet", "--message", "chore: foreign notes")
    edit = "an edit that its owner has not committed\n"
    (folder / "notes.txt").write_text(edit, encoding="utf-8")
    return folder, edit


def refusal_of(action: Callable[[], object], folder: Path, edit: str) -> dict[str, Any]:
    """Run ``action`` on a foreign folder: whether it was refused with the folder named, and whether the folder's edit survived."""
    refusal: Exception | None = None
    try:
        action()
    except Exception as failure:
        refusal = failure
    kept = (folder / "notes.txt").read_text(encoding="utf-8") == edit
    return {"refused": refusal is not None and str(folder) in str(refusal), "edit_kept": kept, "error": str(refusal)[-300:]}


def restore_cases(work: Path, scratch: Path) -> list[dict[str, Any]]:
    """restore() resets the scratch worktree that this run made and refuses any other folder: a folder it refuses keeps its uncommitted edit.

    The scratch worktree of the run carries the marker, a marker copied from it into another folder does not make that folder the run's, and the same
    refusal holds for the reset helpers of the checks and lint tools runners. Two planted defects (the check removed, a marker written into every folder)
    must each make the refusal case fail."""
    from scripts.quality import run_checks_e2e, run_lint_tools_e2e

    foreign, edit = foreign_repository(scratch, "foreign_repository")
    refused = refusal_of(lambda: restore(foreign), foreign, edit)
    tracked = work / "pyproject.toml"
    original = tracked.read_text(encoding="utf-8")
    tracked.write_text(original + "\n# an edit of this run\n", encoding="utf-8")
    restore(work)
    copied, copied_edit = foreign_repository(scratch, "foreign_with_copied_marker")
    shutil.copy2(
        Path(git(work, "rev-parse", "--absolute-git-dir").strip()) / scratch_marker.MARKER, Path(git(copied, "rev-parse", "--absolute-git-dir").strip())
    )
    copied_result = refusal_of(lambda: restore(copied), copied, copied_edit)
    checks_runner, checks_edit = foreign_repository(scratch, "foreign_for_the_checks_runner")
    checks_result = refusal_of(lambda: run_checks_e2e.reset_hard(checks_runner, "HEAD"), checks_runner, checks_edit)
    lint_runner, lint_edit = foreign_repository(scratch, "foreign_for_the_lint_tools_runner")
    lint_result = refusal_of(lambda: run_lint_tools_e2e.restore(lint_runner), lint_runner, lint_edit)
    unchecked, unchecked_edit = foreign_repository(scratch, "planted_check_removed")
    with mock.patch.object(scratch_marker, "require", lambda folder, action: None):
        check_removed = refusal_of(lambda: restore(unchecked), unchecked, unchecked_edit)
    marked, marked_edit = foreign_repository(scratch, "planted_marker_everywhere")
    scratch_marker.mark(marked, "planted defect")
    marker_everywhere = refusal_of(lambda: restore(marked), marked, marked_edit)
    return [
        {"case": "restore_refuses_a_folder_the_runner_did_not_create", "passed": refused["refused"] and refused["edit_kept"], **refused},
        {"case": "restore_resets_the_scratch_worktree_of_the_run", "passed": tracked.read_text(encoding="utf-8") == original},
        {"case": "restore_refuses_a_folder_with_a_copied_marker", "passed": copied_result["refused"] and copied_result["edit_kept"], **copied_result},
        {"case": "checks_runner_reset_refuses_a_foreign_folder", "passed": checks_result["refused"] and checks_result["edit_kept"], **checks_result},
        {"case": "lint_tools_runner_restore_refuses_a_foreign_folder", "passed": lint_result["refused"] and lint_result["edit_kept"], **lint_result},
        {
            "case": "planted_check_removed_is_reported",
            "passed": not (check_removed["refused"] and check_removed["edit_kept"]),
            "refused": check_removed["refused"],
            "edit_kept": check_removed["edit_kept"],
        },
        {
            "case": "planted_marker_everywhere_is_reported",
            "passed": not (marker_everywhere["refused"] and marker_everywhere["edit_kept"]),
            "refused": marker_everywhere["refused"],
            "edit_kept": marker_everywhere["edit_kept"],
        },
    ]


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


def tooling_suites() -> list[str]:
    """The modules that the ci_tooling_checks and ci_tooling_runtime group files run as their E2E suites."""
    texts = (ROOT / GROUP_FILES / f"{group}.sh" for group in ("ci_tooling_checks", "ci_tooling_runtime"))
    return [module for text in texts for module in re.findall(r"-m (scripts\.quality\.run_\w+_e2e)\b", text.read_text(encoding="utf-8"))]


def install_tooling_filter(work: Path) -> Path:
    """An interpreter stand-in that replaces the E2E suites of ci_tooling_checks, which start nested CI runs (this runner among them) and last about 15 minutes,
    by a marker line and runs every other command for real. It lies outside the checkout, because the gates lint every untracked file in it."""
    path = work.parent / "tooling_filter.sh"
    patterns = "|".join(f"*{module}*" for module in tooling_suites())
    path.write_text(
        f'#!/usr/bin/env bash\ncase "$*" in\n  {patterns}) printf \'stand-in for %s\\n\' "$*" >&2; exit 0 ;;\nesac\nexec {PYTHON} "$@"\n', encoding="utf-8"
    )
    path.chmod(0o755)
    return path


def docs_only_case(work: Path, out: Path, part: str) -> dict[str, Any]:
    name = "docs_only_runs_gates" if part == "core" else f"docs_only_runs_gates_{part}_part"
    stage_readme(work)
    tooling = install_tooling_filter(work)
    # README.md is an exact input of the launcher group (the launcher E2E reads it), so a part that holds the group starts the stand-in suite, never the
    # real one.
    launcher_part = "launcher" in PART_GROUPS[part]
    marker = (install_launcher_group_stub(work) if "repository_code_checks" in PART_GROUPS[part] else install_launcher_stub(work)) if launcher_part else None
    try:
        code, log, text = run_entry(work, out, name, {"CI_SCOPE": "staged", "CI_PART": part, "PYTHON_BIN": str(tooling)})
    finally:
        restore(work)
        tooling.unlink(missing_ok=True)
    required = ["repository_code_checks", "ci_tooling_checks", "ci_tooling_runtime"]
    checks = {
        "exit_zero": code == 0,
        "reached_the_end": PASS_LINES[part] in log,
        "conditional_groups_skipped_with_reason": all(f"skipped {group}:" in log for group in PART_GROUPS[part] if group not in {*required, "launcher"}),
        "tooling_suites_stood_in": all(f"stand-in for -m {module}" in log for module in tooling_suites()) == (part in {"core", "all"}),
        "launcher_selected_and_started": (marker is not None and "run launcher" in log and marker.exists()) if launcher_part else "run launcher" not in log,
        "gates_ran": re.search(r"^gates: \d+ s$", log, re.MULTILINE) is not None,
        "markdown_lint_ran": "markdownlint-cli2 v" in log,
        # sql_style is selected by path in the staged scope (always_except_commits), so a README-only change skips it with a reason.
        "required_groups_only": sorted(re.findall(r"^== (\w+) ==$", log, re.MULTILINE)) == (sorted(required) if part in {"core", "all"} else []),
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


def hosted_part_cases(work: Path, out: Path) -> list[dict[str, Any]]:
    """The dispatcher accepts every hosted Core part of ``parts`` (the hosted jobs pass them as CI_PART) and plans exactly the groups of that part."""
    config = json.loads((work / "config/quality/ci_groups.json").read_text(encoding="utf-8"))
    cases = []
    for part, groups in config["parts"].items():
        code, log, _ = run_entry(work, out, f"hosted_part_{part}", {"CI_LIST_ONLY": "1", "CI_SCOPE": "full", "CI_PART": part})
        planned = re.findall(r"^run (\w+)$", log, re.MULTILINE)
        cases.append(
            {"case": f"dispatcher_accepts_hosted_part_{part}", "passed": code == 0 and planned == groups, "exit": code, "planned": planned, "wanted": groups}
        )
    return cases


def admission_case(work: Path, out: Path) -> dict[str, Any]:
    """Every worker that the full run creates has a limit in worker_limits: the run stops at its admission, before any worker starts, and names the workers."""
    code, log, _ = run_entry(work, out, "admission_all", {"CI_SCOPE": "full", "CI_PART": "all", "CI_ADMISSION_ONLY": "1"})
    listed = re.search(r"^admitted workers: (.*)$", log, re.MULTILINE)
    workers = listed.group(1).split() if listed else []
    checks = {
        "exit_zero": code == 0,
        "no_worker_without_a_limit": "has no limit" not in log,
        "workers_named": bool(workers),
        "feature_store_worker_admitted": "lakehouse_features_store" in workers,
        "no_worker_ran": "\n== " not in log,
    }
    return {"case": "every_worker_has_a_limit", "passed": all(checks.values()), "checks": checks, "exit": code, "workers": workers}


def feature_check_case(work: Path, out: Path) -> dict[str, Any]:
    """The feature sources check and the feature columns check are items of their own: one each in a run of the whole suites or of the first shard of their
    suite, none in another shard, and no shard function runs them."""
    sources, columns = FEATURE_COUNTS["sources"], FEATURE_COUNTS["columns"]
    runs = (
        ("features", "features", "", 1, 1),
        ("sources_first", "feature_sources", f"1/{sources}", 1, 0),
        ("sources_other", "feature_sources", f"2/{sources}", 0, 0),
        ("columns_first", "feature_columns", f"1/{columns}", 0, 1),
        ("columns_other", "feature_columns", f"2/{columns}", 0, 0),
    )
    checks: dict[str, bool] = {}
    for label, part, shard, sources_checks, columns_checks in runs:
        extra = {"CI_SCOPE": "full", "CI_PART": part, "CI_ADMISSION_ONLY": "1", **({"FEATURE_SHARD": shard} if shard else {})}
        code, log, _ = run_entry(work, out, f"feature_checks_{label}", extra)
        listed = re.search(r"^admitted workers: (.*)$", log, re.MULTILINE)
        workers = listed.group(1).split() if listed else []
        found = (workers.count("lakehouse_features_sources_check"), workers.count("lakehouse_features_columns_check"))
        checks[f"{label}_items"] = code == 0 and "has no limit" not in log and found == (sources_checks, columns_checks)
    body = (work / GROUP_FILES / "lakehouse_features.sh").read_text(encoding="utf-8")
    shard_functions = re.findall(r"^lakehouse_features_(?:sources|columns)\(\) \{.*?^\}", body, re.MULTILINE | re.DOTALL)
    checks["two_shard_functions_found"] = len(shard_functions) == 2
    checks["shard_functions_run_no_check"] = not any("--check" in function for function in shard_functions)
    return {"case": "feature_checks_are_items_of_their_own", "passed": all(checks.values()), "checks": checks}


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
    }


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
            "layer_gold_model_selects_silver_gold_and_features",
            "dbt/models/gold/dimensions/_dimensions__models.yml",
            [*silver_downstream, "orchestration_checks", "sql_style"],
        ),
        path_case(
            "layer_silver_model_read_by_the_bronze_consumer_columns_selects_every_layer",
            "dbt/models/silver/staging/_staging__models.yml",
            [*LAYERS, "orchestration_checks", "sql_style"],
        ),
        path_case(
            "layer_intermediate_model_read_by_the_bronze_consumer_columns_selects_every_layer",
            "dbt/models/silver/intermediate/int_acs_county_values.sql",
            [*LAYERS, "orchestration_checks", "sql_style"],
        ),
        path_case(
            "layer_identity_classification_read_by_the_bronze_consumer_columns_selects_every_layer",
            "config/lakehouse/identity_classification.csv",
            [*LAYERS, "configuration_checks"],
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
            "layer_gold_dimension_no_layer_above_reads_selects_silver_gold_and_features",
            "dbt/models/gold/dimensions/dim_county.sql",
            [*silver_downstream, "orchestration_checks", "sql_style"],
        ),
        path_case(
            "layer_gold_only_model_edit_selects_the_declaration_check_in_silver",
            "dbt/models/gold/dimensions/dim_date.sql",
            [*silver_downstream, "orchestration_checks", "sql_style"],
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
        path_case("layer_shared_macro_selects_every_layer", "dbt/macros/file_metadata.sql", [*LAYERS, "orchestration_checks", "sql_style"]),
        path_case("layer_gold_contract_is_read_by_bronze_first", "data_contracts/lakehouse/gold_contract.json", [*LAYERS, "orchestration_checks"]),
        path_case("layer_gold_suite_is_read_by_bronze_first", "data_contracts/great_expectations/gold/gold_keys.json", [*LAYERS, "orchestration_checks"]),
        path_case("layer_saved_silver_suite_selects_silver_gold_and_features", "features/great_expectations/suites/silver_spatial.json", silver_downstream),
        path_case("layer_suite_baselines_are_read_by_bronze_first", "data_contracts/great_expectations/baselines.json", [*LAYERS, "orchestration_checks"]),
        path_case(
            "layer_features_only_model_edit_selects_the_declaration_check_in_silver",
            "dbt/models/features/int_spine_county_context.sql",
            [*silver_downstream, "orchestration_checks", "sql_style"],
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
            "layer_staged_gold_model_selects_silver_gold_and_features",
            [["update-index", "--add", "--cacheinfo", f"100644,{blob},dbt/models/gold/staged_copy.yml"]],
            [*silver_downstream, "orchestration_checks", "sql_style"],
        ),
        staged_case(
            work,
            "layer_staged_silver_model_selects_every_layer",
            [["update-index", "--add", "--cacheinfo", f"100644,{blob},dbt/models/silver/staged_copy.yml"]],
            [*LAYERS, "orchestration_checks", "sql_style"],
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
            "layer_generated_matrix_model_selects_silver_gold_and_features",
            "dbt/models/features/matrix/feature_matrix_hospital.sql",
            [*silver_downstream, "orchestration_checks", "sql_style"],
        ),
    ]
    return [
        *cases,
        *declaration_check_cases(work),
        *reader_coverage_cases(work),
        *entry_point_cases(),
        *launcher_selection_cases(work),
        *launcher_always_cases(work),
    ]


DECLARATION_COMMAND = "-m scripts.lakehouse.bronze_consumer_columns"
DECLARATION_INPUTS = (
    "dbt/models/silver/staging/stg_illinois_report_card_csv.sql",
    "dbt/models/silver/staging/stg_cms_hai_hospital.sql",
    "dbt/models/silver/intermediate/int_hud_zip_county_holds.sql",
    "dbt/macros/state_columns.sql",
    "dbt/macros/canonical_rows.sql",
    "dbt/seeds/acs_variable_map.csv",
    "dbt/tests/assert_staging_unique_grain.sql",
    "dbt/dbt_project.yml",
    "dbt/models/silver/staging/_bronze__sources.yml",
    "dbt/models/gold/dimensions/dim_date.sql",
    "dbt/models/features/int_spine_county_context.sql",
    "config/lakehouse/bronze_consumer_columns.json",
    "config/lakehouse/bronze_consumer_target_branches.json",
    "scripts/lakehouse/bronze_consumer_declare.py",
    "scripts/lakehouse/bronze_consumer_lineage.py",
)


def declaration_check_groups() -> list[str]:
    """The groups whose body file runs the declaration drift and completeness check of the bronze consumer columns (the module command, not its E2E runner)."""
    holders = []
    for path in sorted((ROOT / "scripts/ci/groups").glob("*.sh")):
        if re.search(rf"^\s*timed \"[^\"]+\" \"[^\"]+\" {re.escape(DECLARATION_COMMAND)}$", path.read_text(encoding="utf-8"), re.MULTILINE):
            holders.append(path.stem)
    return holders


def declaration_check_cases(work: Path) -> list[dict[str, Any]]:
    """Every input that can change a declared column selects the group that checks the declarations, so a model-only edit cannot skip the drift check."""
    holders = declaration_check_groups()
    cases: list[dict[str, Any]] = [{"case": "declaration_check_runs_in_exactly_one_group", "passed": len(holders) == 1, "groups": holders}]
    for path in DECLARATION_INPUTS:
        code, plan, error = select(work, "--scope", "paths", "--path", path)
        run = plan["run"] if plan else []
        cases.append(
            {
                "case": f"declaration_check_group_is_selected_by_{path.replace('/', '_').replace('.', '_')}",
                "passed": code == 0 and len(holders) == 1 and holders[0] in run,
                "holder": holders,
                "run": run,
                "error": error[-300:],
            }
        )
    return cases


LAUNCHER_IMPORTED = (
    "scripts/lakehouse/silver.py",
    "orchestration/network_policy.py",
    "scripts/quality/docker_calls.py",
    "scripts/acquisition/new_probe.py",
    "dbt/models/gold/new_probe.sql",
    "scripts/quality/lint_sources.py",
    "scripts/quality/ci_hooks.py",
    "dags/gold_transform_dag.py",
    "scripts/quality/future_probe.py",
)
# Paths outside every pattern of the launcher group: no import, read or reviewed inventory_reads pattern of the suite reaches them, so a change to one skips
# the stand-in or real launcher suite.
LAUNCHER_OUTSIDE = (
    "config/quality/shard_costs.json",
    "config/quality/temporary_code.json",
)
# Paths no file of the committed list names but one reviewed inventory_reads pattern matches (a new file in a folder the suite lists) still select it.
LAUNCHER_INSIDE_PATTERNS = (
    "dbt/models/features/matrix/new_probe.sql",
    "config/training/feature_lists/new_probe.json",
    "data_contracts/great_expectations/features/new_probe.json",
    "dbt/seeds/gold/new_probe_bridge.csv",
    "scripts/acquisition/new_probe.py",
    ".gitignore",
)
# The reviewer's reproduction: an exact read added beside the reviewed mount comparison of ROOT / "config" in the suite's docker_calls_check case.
LAUNCHER_EXACT_READ = ("    from scripts.quality import docker_calls\n", '    json.loads((ROOT / "config/quality/temporary_code.json").read_text())\n')
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
    """The always-select pattern (``.``) is kept for a module whose reads are not reviewed, and no entry carries it now. Declared, it selects the launcher for
    every changed path and takes no part in classifying paths: it is not in a group list, so a path no list names still selects every group (LS27); an empty
    change list selects no launcher (LS28) and the configuration note states the exception (LS30)."""
    config = ci_groups.load()
    listed = [pattern for patterns in config["groups"].values() for pattern in patterns] + config["passive"]
    always = config.get("launcher_always", [])
    sample = LAUNCHER_OUTSIDE[-1]
    declared = ci_groups.plan("paths", "launcher", paths=[sample], config={**config, "launcher_always": [ci_groups.ALWAYS_PATTERN]})
    actual = ci_groups.plan("paths", "launcher", paths=[sample], config=config)
    code, plan, error = select(work, "--scope", "paths", "--part", "all")
    note = json.loads((ROOT / ci_groups.CONFIG).read_text(encoding="utf-8"))["note"]
    stale = ("no longer selects it", "never another group")
    return [
        {
            "case": "launcher_always_pattern_is_not_a_classifier",
            "passed": ci_groups.ALWAYS_PATTERN not in listed and not any(re.search(pattern, "newtop/tool.bin") for pattern in listed),
            "matching": [pattern for pattern in listed if re.search(pattern, "newtop/tool.bin")][:4],
        },
        {
            "case": "launcher_always_pattern_selects_every_path_only_when_declared",
            "passed": "launcher" in declared["run"] and "launcher" not in actual["run"] and not always,
            "declared_run": declared["run"],
            "actual_run": actual["run"],
            "launcher_always": always,
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
    """The launcher group is selected by what its suite imports or reads and by the reviewed ``inventory_reads`` pattern of each followed module that lists
    files. A change to a module the suite imports selects it, so does a new file in a folder a reviewed pattern names, a path outside every pattern does not,
    every entry is reviewed with a file and line, the committed list equals the computed one, and every computed file is selected by the group's patterns."""
    cases: list[dict[str, Any]] = [
        selection_case(work, f"launcher_selected_by_{Path(path).stem}", ["--scope", "paths", "--path", path, "--part", "launcher"], ["launcher"])
        for path in LAUNCHER_IMPORTED
    ]
    cases += [
        selection_case(work, f"launcher_selected_by_pattern_{Path(path).name}", ["--scope", "paths", "--path", path, "--part", "launcher"], ["launcher"])
        for path in LAUNCHER_INSIDE_PATTERNS
    ]
    cases += [
        selection_case(work, f"launcher_not_selected_by_{Path(path).stem}", ["--scope", "paths", "--path", path, "--part", "launcher"], [])
        for path in LAUNCHER_OUTSIDE
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
    citation = re.compile(r"\b[\w/]+\.py:\d+")
    unreviewed = [
        item["module"]
        for item in committed["inventory_reads"]
        if item["pattern"] == ci_groups.ALWAYS_PATTERN or not item["why"].startswith("reviewed:") or not citation.search(item["why"])
    ]
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
            "case": "launcher_list_names_only_tracked_files",
            "passed": not [path for path in committed["files"] if path not in tracked],
            "untracked": [path for path in committed["files"] if path not in tracked][:6],
        },
        {
            "case": "launcher_list_names_an_exact_read_beside_an_ignored_folder",
            "passed": any(line.startswith("config/quality/temporary_code.json: the launcher suite needs it") for line in exact),
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
            "case": "launcher_inventory_reads_are_all_reviewed",
            "passed": not unreviewed,
            "unreviewed": unreviewed[:6],
        },
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
    "lakehouse_bronze": (
        (
            'scripts.lakehouse.run_aws_bronze_writer_e2e --out "$quality_output/aws_bronze_writer"'
            " --journal-module scripts/lakehouse/catalog_journal.py --plant-defects",
            (
                "scripts/lakehouse/run_aws_bronze_writer_e2e.py",
                "scripts/lakehouse/bronze_writer_cases.py",
                "scripts/infrastructure/bronze_policy_cases.py",
                "infra/iam/owner_policies/bronze_writer_storage.json",
                "infra/iam/owner_policies/bronze_writer_source_lists.json",
                "infra/iam/owner_policies/bronze_writer_source_objects.json",
                "infra/iam/owner_policies/bronze_writer_pseudonym_key.json",
                "infra/iam/owner_policies/bronze_writer_run_record_writes.json",
                "infra/iam/owner_policies/bronze_writer_run_records.json",
                "infra/iam/owner_policies/bronze_execution.json",
                "infra/iam/owner_policies/bronze_project_delta.json",
                "infra/iam/owner_policies/owner_notice.json",
                "infra/iam/owner_policies/job_stage_read.json",
                "infra/iam/bronze_writer.tf",
                "infra/iam/bronze_writer_variables.tf",
                "infra/iam/tests/bronze_writer.tftest.hcl",
            ),
        ),
    ),
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
        (
            "scripts.quality.run_worktree_setup_e2e",
            ("scripts/quality/run_worktree_setup_e2e.py", "scripts/quality/worktree_setup.py", "scripts/quality/lock_env.py"),
        ),
    ),
    "configuration_checks": (
        ("scripts.infrastructure.run_secret_store_e2e", ("scripts/infrastructure/run_secret_store_e2e.py", "scripts/infrastructure/secret_store.py")),
    ),
    "configuration_policy_checks": (
        ("scripts.infrastructure.run_push_images_e2e", ("scripts/infrastructure/run_push_images_e2e.py", "scripts/infrastructure/push_images.py")),
        ("scripts.infrastructure.run_aws_tasks_e2e", ("scripts/infrastructure/run_aws_tasks_e2e.py", "scripts/infrastructure/aws_tasks.py")),
        ("scripts.infrastructure.aws_tasks_plants", ("scripts/infrastructure/aws_tasks_plants.py", "scripts/infrastructure/aws_tasks.py")),
    ),
    "configuration_owner_checks": (
        ("scripts.infrastructure.host_session_plants", ("scripts/infrastructure/host_session_plants.py", "scripts/infrastructure/host_session.py")),
    ),
    "configuration_host_checks": (
        ("scripts.infrastructure.run_host_session_e2e", ("scripts/infrastructure/run_host_session_e2e.py", "scripts/infrastructure/host_session.py")),
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
            work, "unclassified_path_runs_all", ["--scope", "paths", "--path", "newtop/tool.bin", "--part", "core+layers"], WITH_LAYERS, "Unclassified"
        ),
        selection_case(
            work,
            "unclassified_path_runs_all_in_part_all",
            ["--scope", "paths", "--path", "newtop/tool.bin", "--part", "all"],
            [*WITH_LAYERS, "launcher", "gate_stage_checks"],
            "Unclassified",
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
            ["--scope", "paths", "--path", "dags/metadata_ingest_dag.py", "--part", "core+layers"],
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
        # The runners that the Terraform groups start, and the configuration one of them reads, select those groups.
        selection_case(
            work,
            "platform_groups_runner_selects_terraform_checks",
            ["--scope", "paths", "--path", "scripts/infrastructure/run_platform_groups_e2e.py", "--part", "core+layers"],
            ["python_lint", *LAYERS, "repository_code_checks", "configuration_checks", "terraform_checks"],
        ),
        selection_case(
            work,
            "serving_release_runner_selects_terraform_checks",
            ["--scope", "paths", "--path", "scripts/infrastructure/run_serving_release_real_e2e.py", "--part", "core+layers"],
            ["python_lint", *LAYERS, "repository_code_checks", "configuration_checks", "terraform_checks"],
        ),
        selection_case(
            work,
            "serving_transition_runner_selects_terraform_checks",
            ["--scope", "paths", "--path", "scripts/infrastructure/run_serving_transition_e2e.py", "--part", "core+layers"],
            ["python_lint", *LAYERS, "repository_code_checks", "configuration_checks", "terraform_checks"],
        ),
        selection_case(
            work,
            "jobs_replace_order_runner_selects_terraform_proofs",
            ["--scope", "paths", "--path", "scripts/infrastructure/run_jobs_replace_order_e2e.py", "--part", "core+layers"],
            ["python_lint", *LAYERS, "repository_code_checks", "configuration_checks", "terraform_checks", "terraform_proofs"],
        ),
        selection_case(
            work,
            "services_replace_order_runner_selects_terraform_proofs",
            ["--scope", "paths", "--path", "scripts/infrastructure/run_services_replace_order_e2e.py", "--part", "core+layers"],
            ["python_lint", *LAYERS, "repository_code_checks", "configuration_checks", "terraform_checks", "terraform_proofs"],
        ),
        selection_case(
            work,
            "iam_upgrade_plan_runner_selects_terraform_checks",
            ["--scope", "paths", "--path", "scripts/infrastructure/run_iam_upgrade_plan_e2e.py", "--part", "core+layers"],
            ["python_lint", *LAYERS, "repository_code_checks", "configuration_checks", "terraform_checks"],
        ),
        selection_case(
            work,
            "iam_upgrade_plan_applied_state_selects_terraform_checks",
            ["--scope", "paths", "--path", "tests/fixtures/bronze_writer_policies_applied_state.json", "--part", "core+layers"],
            [*LAYERS, "configuration_checks", "terraform_checks"],
        ),
        # The hook modules of the bronze Glue mirror are tested by pytest, which the configuration_checks group runs.
        selection_case(
            work,
            "bronze_mirror_hook_selects_the_pytest_group",
            ["--scope", "paths", "--path", "scripts/lakehouse/bronze_mirror.py", "--part", "core+layers"],
            [
                "python_lint",
                *LAYERS,
                "orchestration_checks",
                "orchestration_engine_e2e",
                "repository_code_checks",
                "ci_tooling_checks",
                "ci_tooling_runtime",
                "configuration_checks",
            ],
        ),
        selection_case(
            work,
            "bronze_mirror_signoff_selects_the_pytest_group",
            ["--scope", "paths", "--path", "scripts/lakehouse/bronze_mirror_signoff.py", "--part", "core+layers"],
            [
                "python_lint",
                *LAYERS,
                "orchestration_checks",
                "orchestration_engine_e2e",
                "repository_code_checks",
                "ci_tooling_checks",
                "ci_tooling_runtime",
                "configuration_checks",
            ],
        ),
        selection_case(
            work,
            "serving_release_configuration_selects_terraform_checks",
            ["--scope", "paths", "--path", "config/aws/serving_release.json", "--part", "core+layers"],
            [*LAYERS, "configuration_checks", "terraform_checks"],
        ),
        selection_case(
            work,
            "redis_acl_cases_select_terraform_checks",
            ["--scope", "paths", "--path", ".github/scripts/redis_acl_cases.sh", "--part", "core+layers"],
            ["repository_code_checks", "terraform_checks"],
        ),
        selection_case(
            work,
            "services_stack_selects_terraform_checks",
            ["--scope", "paths", "--path", "infra/services/redis.tf", "--part", "core+layers"],
            ["configuration_checks", "terraform_checks"],
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
                    [*LAYERS, "orchestration_checks", "repository_code_checks", "sql_style"],
                ),
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
            "lock_selects_every_group",
            ["--scope", "paths", "--path", "uv.lock", "--part", "core+layers"],
            WITH_LAYERS,
            "selects every group",
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
    cases += []
    return cases


def real_run_cases(work: Path, out: Path, scratch: Path) -> list[dict[str, Any]]:
    cases: list[dict[str, Any]] = []
    for part in ("all", "core", "launcher", "bronze", "silver", "gold", "features", "feature_sources", "feature_columns", "feature_store"):
        cases.append(list_only_case(work, out, part))
    cases += hosted_part_cases(work, out)
    for part in ("core", "all", "launcher", "gold", "features", "feature_sources", "feature_columns"):
        cases.append(docs_only_case(work, out, part))
    for part in ("core", "all", "launcher", "silver", "features", "feature_sources", "feature_columns"):
        cases.append(gate_failure_case(work, out, part))
    cases.append(admission_case(work, out))
    cases.append(feature_check_case(work, out))
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
FEATURE_ENTRIES = [
    "lakehouse_features_sources",
    "lakehouse_features_sources_check",
    "lakehouse_features_columns",
    "lakehouse_features_columns_check",
    "lakehouse_features_combine",
    "lakehouse_features_store",
]
FEATURE_COMMAND_FUNCTIONS = ("sources_check", "sources", "columns_check", "columns")
OLD_GOLD_COMMANDS = (
    "scripts.lakehouse.bridge_references --check",
    "scripts.lakehouse.run_bridge_references_e2e",
    "scripts.lakehouse.run_owner_history_e2e",
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
DURATIONS_FILE = "config/quality/job_durations.json"


def workflow_document() -> dict[str, Any]:
    loaded = load_yaml((ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        raise ValueError(".github/workflows/ci.yml is not a mapping")
    return loaded


def expanded_job_names(jobs: dict[str, Any]) -> list[str]:
    """The job names of the workflow with matrix values filled in (a list under ``include``, or one list-valued key)."""
    names: list[str] = []
    for definition in jobs.values():
        matrix = definition.get("strategy", {}).get("matrix")
        if not matrix:
            names.append(str(definition["name"]))
            continue
        combinations = matrix["include"] if "include" in matrix else [{key: value} for key, values in matrix.items() for value in values]
        names += [fill_matrix(str(definition["name"]), combination) for combination in combinations]
    return names


def workflow_trigger_problems(text: str) -> list[str]:
    """What is wrong with the triggers, the concurrency, the checkouts and the job list of ``.github/workflows/ci.yml`` (check first, every run full)."""
    document = load_yaml(text)
    if not isinstance(document, dict):
        return ["the workflow is not a mapping"]
    triggers = document.get("on", document.get(True)) or {}
    jobs = document.get("jobs", {})
    problems = []
    if triggers.get("push") != {"branches-ignore": ["main"]} or "workflow_dispatch" not in triggers or set(triggers) != {"push", "workflow_dispatch"}:
        problems.append(f"the triggers must be a push to every branch but main and workflow_dispatch: {triggers!r}")
    concurrency = document.get("concurrency") or {}
    if concurrency.get("group") != "${{ github.workflow }}-${{ github.ref }}" or concurrency.get("cancel-in-progress") is not True:
        problems.append(f"one concurrency group per branch that cancels the run in progress: {concurrency!r}")
    refs = {
        str(step["with"]["ref"])
        for job in jobs.values()
        for step in job.get("steps", [])
        if str(step.get("uses", "")).startswith("actions/checkout@") and "ref" in step.get("with", {})
    }
    if refs != {"${{ github.sha }}", "${{ github.workflow_sha }}"}:
        problems.append(f"every checkout uses the pushed commit (the gate scripts use the workflow revision): {sorted(refs)}")
    registry = json.loads((ROOT / "config/quality/ci_jobs.json").read_text(encoding="utf-8"))
    names = expanded_job_names(jobs)
    if not isinstance(registry, list) or sorted(registry) != sorted(names):
        problems.append("config/quality/ci_jobs.json is not the plain list of the expanded job names of the workflow")
    gate = jobs.get("ci_gate", {})
    needs = [gate["needs"]] if isinstance(gate.get("needs"), str) else list(gate.get("needs", []))
    if sorted(needs) != sorted(key for key in jobs if key != "ci_gate"):
        problems.append("CI Checks must need every other job of the workflow")
    if str(gate.get("if", "")).strip() != "${{ always() }}":
        problems.append("CI Checks must run always()")
    return problems


def workflow_trigger_cases() -> list[dict[str, Any]]:
    """The workflow runs on a push to any branch but main, in full, with CI Checks and its job list matching it."""
    problems = workflow_trigger_problems((ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8"))
    return [{"case": "ci_workflow_triggers_are_push_to_branches_but_main_and_dispatch", "passed": not problems, "problems": problems}]


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
        "scripts/lakehouse/run_owner_history_e2e.py": "data/e2e/owner_history",
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
    feature_commands = [
        command for function in FEATURE_COMMAND_FUNCTIONS for command in timed_commands(function_text(text, f"lakehouse_features_{function}") or "")
    ]
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
    for suite in ("sources", "columns"):
        definition = workflow_document()["jobs"][f"feature_{suite}"]
        steps = [step for step in definition["steps"] if step.get("run") == "bash scripts/run_ci.sh"]
        cases.append(
            {
                "case": f"feature_{suite}_job_runs_in_full_on_the_pushed_commit",
                "passed": "needs" not in definition
                and "if" not in definition
                and len(steps) == 1
                and steps[0].get("env", {}).get("CI_PART") == f"feature_{suite}"
                and not {"CI_SCOPE", "RANGE_START", "RANGE_END"} & set(steps[0].get("env", {})),
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
    """Each job's timeout equals the one in config/quality/job_durations.json and is between 1.5 and 2.6 times the measured time (at least 5 minutes)."""
    entries = json.loads((ROOT / DURATIONS_FILE).read_text(encoding="utf-8"))["jobs"]
    recorded = {entry["job"]: (float(entry["measured_minutes"]), int(entry["timeout_minutes"])) for entry in entries}
    timeouts = workflow_timeouts()
    off = {name: (recorded[name], timeouts.get(name)) for name in recorded if timeouts.get(name) != recorded[name][1]}
    ratio = {name: round(timeout / measured, 2) for name, (measured, timeout) in recorded.items() if not (1.5 * measured <= timeout <= max(2.6 * measured, 5))}
    return [
        *worker_limit_cases(timeouts),
        {
            "case": "job_timeouts_match_the_measured_durations",
            "passed": set(recorded) == set(timeouts) and not off and not ratio and len(recorded) >= 10,
            "not_in_the_table": sorted(set(timeouts) - set(recorded)),
            "not_in_the_workflow": sorted(set(recorded) - set(timeouts)),
            "different_timeout": off,
            "outside_the_ratio": ratio,
        },
    ]


def core_scratch(root: Path) -> Path:
    """A copy of what the Core command guard reads: the configuration, the inventory and the group body files."""
    for name in ("config/quality/ci_groups.json", "config/quality/core_commands.json"):
        (root / name).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(ROOT / name, root / name)
    shutil.copytree(ROOT / "scripts/ci/groups", root / "scripts/ci/groups")
    return root


def core_command_cases() -> list[dict[str, Any]]:
    """Every Core command runs once, in one of the fourteen hosted Core parts, and the guard fails on a dropped or a duplicated command."""
    cases: list[dict[str, Any]] = [
        {"case": "core_commands_run_exactly_once", "passed": not core_commands.findings(ROOT), "findings": core_commands.findings(ROOT)}
    ]
    config = json.loads((ROOT / "config/quality/ci_groups.json").read_text(encoding="utf-8"))
    jobs = workflow_document()["jobs"]
    hosted = []
    for definition in jobs.values():
        for step in definition.get("steps", []):
            value = str(step.get("env", {}).get("CI_PART", ""))
            if value == "${{ matrix.part }}":
                hosted += [str(entry["part"]) for entry in matrix_entries(definition)]
            elif value:
                hosted.append(value)
    hosted = sorted(part for part in hosted if part.startswith("core_"))
    # The base command list is unchanged. Each Terraform stack command is a line in both stack groups; the inventory binds it to those groups and their stacks.
    repeated = json.loads((ROOT / "config/quality/core_commands.json").read_text(encoding="utf-8")).get("repeated", {})
    bound = {tuple(sorted((entry["group"], entry["stacks"]) for entry in entries)) for entries in repeated.values()}
    cases.append(
        {
            "case": "only_the_terraform_stack_commands_run_in_two_groups",
            "passed": len(repeated) == 4
            and bound == {(("terraform_checks", "storage iam platform alerts"), ("terraform_services_checks", "services jobs"))}
            and all("terraform" in command or "static_checks" in command for command in repeated),
            "repeated": sorted(repeated),
        }
    )
    cases.append(
        {
            "case": "fourteen_hosted_core_jobs_run_the_fourteen_parts",
            "passed": len(config.get("parts", {})) == 14 and hosted == sorted(config.get("parts", {})),
            "parts": sorted(config.get("parts", {})),
            "hosted": hosted,
        }
    )
    first_group = next(iter(core_commands.inventory(ROOT)))
    plants = {
        "dropped_command_is_found": ("dropped command", lambda root: drop_first_command(root, first_group)),
        "duplicated_command_is_found": ("duplicated command", lambda root: duplicate_first_command(root, first_group)),
        # A third line of a stack command is a duplicate: the inventory binds it to the two stack groups only.
        "stack_command_over_its_count_is_found": (
            "duplicated command",
            lambda root: duplicate_first_command(root, "terraform_checks", "init -backend=false"),
        ),
        # The same number of lines in another group or stack list is a different run (the review moved the services group's init line into terraform_checks).
        "stack_command_moved_between_groups_is_found": (
            "moved command",
            lambda root: move_command(root, "init -backend=false", "terraform_services_checks", "terraform_checks"),
        ),
        "stack_list_changed_is_found": (
            "moved command",
            lambda root: replace_in_group(root, "terraform_services_checks", "for stack in services jobs; do", "for stack in services; do", 2),
        ),
        "unplaced_group_is_found": ("is in no hosted Core part", lambda root: change_parts(root, remove=first_group)),
        "group_in_two_parts_is_found": ("hosted Core parts", lambda root: change_parts(root, repeat=first_group)),
    }
    for name, (marker_text, plant) in plants.items():
        with tempfile.TemporaryDirectory(prefix="core_commands.") as folder:
            root = core_scratch(Path(folder))
            plant(root)
            problems = core_commands.findings(root)
        cases.append({"case": f"core_commands_guard_{name}", "passed": any(marker_text in problem for problem in problems), "findings": problems[:3]})
    return cases


def drop_first_command(root: Path, group: str) -> None:
    path = root / "scripts/ci/groups" / f"{group}.sh"
    lines = path.read_text(encoding="utf-8").splitlines(keepends=True)
    index = next(number for number, line in enumerate(lines) if " timed " in f" {line.lstrip()}")
    path.write_text("".join(lines[:index] + lines[index + 1 :]), encoding="utf-8")


def duplicate_first_command(root: Path, group: str, needle: str = "") -> None:
    path = root / "scripts/ci/groups" / f"{group}.sh"
    lines = path.read_text(encoding="utf-8").splitlines(keepends=True)
    index = next(number for number, line in enumerate(lines) if " timed " in f" {line.lstrip()}" and needle in line)
    path.write_text("".join(lines[: index + 1] + [lines[index]] + lines[index + 1 :]), encoding="utf-8")


def move_command(root: Path, needle: str, source: str, target: str) -> None:
    """Move the first ``timed`` line that holds ``needle`` from one group file to the line of the same command in another, so the count stays the same."""
    source_path = root / "scripts/ci/groups" / f"{source}.sh"
    lines = source_path.read_text(encoding="utf-8").splitlines(keepends=True)
    index = next(number for number, line in enumerate(lines) if " timed " in f" {line.lstrip()}" and needle in line)
    moved = lines.pop(index)
    source_path.write_text("".join(lines), encoding="utf-8")
    target_path = root / "scripts/ci/groups" / f"{target}.sh"
    others = target_path.read_text(encoding="utf-8").splitlines(keepends=True)
    position = next(number for number, line in enumerate(others) if " timed " in f" {line.lstrip()}" and needle in line)
    target_path.write_text("".join(others[: position + 1] + [moved] + others[position + 1 :]), encoding="utf-8")


def replace_in_group(root: Path, group: str, old: str, new: str, count: int) -> None:
    path = root / "scripts/ci/groups" / f"{group}.sh"
    text = path.read_text(encoding="utf-8")
    if text.count(old) != count:
        raise ValueError(f"{group}: expected {count} of {old!r}, found {text.count(old)}")
    path.write_text(text.replace(old, new), encoding="utf-8")


def change_parts(root: Path, remove: str = "", repeat: str = "") -> None:
    path = root / "config/quality/ci_groups.json"
    config = json.loads(path.read_text(encoding="utf-8"))
    parts = config.get("parts", {})
    for name, groups in parts.items():
        if remove in groups:
            parts[name] = [group for group in groups if group != remove]
    if repeat and parts:
        first, second = list(parts)[:2]
        parts[second] = [*parts[second], *(group for group in parts[first][:1] if group not in parts[second])]
    config["parts"] = parts
    path.write_text(json.dumps(config, indent=2) + "\n", encoding="utf-8")


# The saved cost of each feature case in seconds (config/quality/feature_case_costs.json) and the shard counts it is balanced for (owner decision 163).
FEATURE_SHARD_PLAN = {"sources": (16, 400.0), "columns": (2, 110.0)}


def feature_weight_cases() -> list[dict[str, Any]]:
    """The feature shards balance the saved case costs: each case runs in one shard, a case without a cost still runs once, and no shard is over the bound."""
    config = json.loads((ROOT / "config/quality/ci_groups.json").read_text(encoding="utf-8"))["feature_shards"]
    results: list[dict[str, Any]] = []
    for suite, (count, bound) in FEATURE_SHARD_PLAN.items():
        name = f"feature_{suite}_shards_are_weighted_by_the_saved_costs"
        try:
            costs = json.loads((ROOT / "config/quality/feature_case_costs.json").read_text(encoding="utf-8"))[suite]
            names = feature_shards.names_of(suite)
            shards = [feature_shards.select(names, f"{index}/{count}", suite=suite) for index in range(1, count + 1)]
            flat = [case for shard in shards for case in shard]
            loads = [round(sum(costs[case] for case in shard), 1) for shard in shards]
            extra = [*names, "a_new_case_without_a_saved_cost"]
            with_new = [case for index in range(1, count + 1) for case in feature_shards.select(extra, f"{index}/{count}", suite=suite)]
            expensive = [index for index, shard in enumerate(shards) if any(costs[case] >= 30 for case in shard)]
            heavy = [case for case in names if costs[case] >= 30]
            results.append(
                {
                    "case": name,
                    "passed": config[suite] == count
                    and sorted(flat) == sorted(names)
                    and sorted(with_new) == sorted(extra)
                    and set(costs) == set(names)
                    and max(loads) <= bound
                    and (suite != "columns" or (len(heavy) == 4 and len(expensive) == 2)),
                    "configured_count": config[suite],
                    "cases": len(names),
                    "heaviest_shard_seconds": max(loads),
                    "lightest_shard_seconds": min(loads),
                }
            )
        except Exception as error:
            results.append({"case": name, "passed": False, "error": f"{type(error).__name__}: {error}"})
    return results


# The four samples shards (the test-checks hook) are packed by the saved case seconds (config/quality/shard_costs.json): no shard is over this bound.
SAMPLES_SHARD_BOUND = 1000.0


def sample_weight_cases() -> list[dict[str, Any]]:
    """The samples shards balance the saved case costs: each case runs in one shard and no shard holds more than the bound of seconds."""
    config = json.loads((ROOT / "config/quality/ci_groups.json").read_text(encoding="utf-8"))["hook_shards"]
    costs = case_shards.saved_costs("run_checks_e2e")
    names = list(costs)
    count = int(config["samples"])
    shards = case_shards.weighted_shards(names, costs, count)
    loads = [round(sum(costs[case] for case in shard), 1) for shard in shards]
    return [
        {
            "case": "samples_shards_are_weighted_by_the_saved_costs",
            "passed": count == 4 and sorted(case for shard in shards for case in shard) == sorted(names) and max(loads) <= SAMPLES_SHARD_BOUND,
            "shards": count,
            "loads": loads,
        }
    ]


WORKER_KINDS = (
    "lakehouse_gold_checks",
    "lakehouse_gold_writer_setup",
    "lakehouse_gold_model_metadata_e2e",
    "lakehouse_features_store",
    "lakehouse_features_sources",
    "lakehouse_features_sources_check",
    "lakehouse_features_columns",
    "lakehouse_features_columns_check",
    "combine_feature_sources",
    "combine_feature_columns",
    "hook_privacy",
    "hook_documentation",
    "hook_code",
    "hook_orphans",
    "hook_lock",
    "hook_samples",
    "hook_samples_combine",
    "acquisition_shard",
    "acquisition_combine",
)


def worker_limit_cases(timeouts: dict[str, int]) -> list[dict[str, Any]]:
    """Each worker kind of the dispatcher has a deadline in ``worker_limits`` that equals the timeout of the hosted job that runs the same work."""
    config = json.loads((ROOT / "config/quality/ci_groups.json").read_text(encoding="utf-8"))
    limits = config.get("worker_limits", {})
    groups = [group for group in config["groups"] if group not in ("launcher", "lakehouse_gold", "lakehouse_features")]
    expected = {*groups, *WORKER_KINDS}
    different = {kind: (entry["minutes"], timeouts.get(entry["job"])) for kind, entry in limits.items() if timeouts.get(entry["job"]) != entry["minutes"]}
    budget = config.get("core_budget")
    # Each kind declares its cores as a whole number above 0. Only the kind that runs alone (the timing assertions of writer_setup) may exceed the budget.
    bad_cores = sorted(
        kind
        for kind, entry in limits.items()
        if type(entry.get("cores")) is not int or entry["cores"] < 1 or (entry["cores"] > (budget or 0) and kind != "lakehouse_gold_writer_setup")
    )
    return [
        {
            "case": "worker_limits_match_the_hosted_jobs",
            "passed": set(limits) == expected and not different,
            "missing_kind": sorted(expected - set(limits)),
            "unknown_kind": sorted(set(limits) - expected),
            "different_from_the_job_timeout": different,
        },
        {
            "case": "worker_cores_fit_the_core_budget",
            "passed": not bad_cores and type(budget) is int and budget > 0,
            "kind_without_whole_cores": bad_cores,
            "core_budget": budget,
        },
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
    for suite, count in FEATURE_COUNTS.items():
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


def feature_gather_cases(scratch: Path) -> list[dict[str, Any]]:
    """Use synthetic shard artifacts of several attempts of one run to exercise the real gather step and combiner as the combined evidence job runs them."""
    run_id = "424242"
    results: list[dict[str, Any]] = []
    workflow = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    download = re.search(r"- name: Download every feature shard report of every attempt\n(?:.*\n){1,8}?\s+pattern: (feature_.+)", workflow)
    results.append(
        {
            "case": "feature_combine_job_downloads_every_attempt_into_its_own_folder",
            "passed": download is not None
            and download.group(1).strip() == "feature_*_shard_*_${{ github.run_id }}_*"
            and "merge-multiple" not in workflow.split("- name: Download every feature shard report of every attempt")[1].split("- name:")[0],
            "pattern": download.group(1) if download else None,
        }
    )
    results.append(
        {
            "case": "feature_combine_job_gathers_before_it_combines",
            "passed": 0 <= workflow.find("scripts.lakehouse.feature_shards gather") < workflow.find("scripts.lakehouse.feature_shards combine --suite sources"),
        }
    )
    for suite, count in FEATURE_COUNTS.items():
        results.extend(feature_gather_suite_cases(scratch, run_id, suite, count))
    return results


def feature_gather_suite_cases(scratch: Path, run_id: str, suite: str, count: int) -> list[dict[str, Any]]:
    """Gather and combine synthetic shard artifacts of several attempts of one run for one suite."""
    results: list[dict[str, Any]] = []
    plan = run_command(str(PYTHON), ["-m", "scripts.lakehouse.feature_shards", "plan", "--suite", suite, "--count", str(count)], cwd=ROOT, timeout=60)
    manifest = json.loads(plan.stdout)

    def report(index: int, status: str = "pass") -> dict[str, Any]:
        names = manifest["shards"][f"{index}/{count}"]
        return {
            "suite": suite,
            "shard": f"{index}/{count}",
            "case_inventory": manifest["case_inventory"],
            "selected_cases": names,
            "source_hashes": manifest["source_hashes"],
            "problems": [] if status == "pass" else ["a case failed"],
            "status": status,
            "cases": [{"case": name, "ok": status == "pass"} for name in names],
        }

    def artifact(downloads: Path, index: int, attempt: int, content: dict[str, Any], run: str = run_id) -> None:
        target = downloads / f"feature_{suite}_shard_{index}_{run}_{attempt}" / suite / f"shard_{index}" / "report.json"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(content))

    def gather_and_combine(label: str, plant: Callable[[Path], None]) -> tuple[int, dict[str, Any], str]:
        downloads = scratch / f"feature_gather_{suite}_{label}" / "downloads"
        evidence = scratch / f"feature_gather_{suite}_{label}" / "feature_shards"
        for index in range(1, count + 1):
            artifact(downloads, index, 1, report(index))
        plant(downloads)
        gathered = run_command(
            str(PYTHON),
            ["-m", "scripts.lakehouse.feature_shards", "gather", "--downloads", str(downloads), "--run-id", run_id, "--directory", str(evidence)],
            cwd=ROOT,
            timeout=60,
        )
        combined = run_command(
            str(PYTHON),
            ["-m", "scripts.lakehouse.feature_shards", "combine", "--suite", suite, "--count", str(count), "--directory", str(evidence / suite)],
            cwd=ROOT,
            timeout=60,
        )
        record = (
            json.loads((evidence / suite / "combined.json").read_text())
            if (evidence / suite / "combined.json").exists()
            else {"status": "none", "problems": []}
        )
        return gathered.returncode + combined.returncode, record, gathered.stderr[-300:]

    def failed_first_then_passed(downloads: Path) -> None:
        artifact(downloads, 2, 1, report(2, "fail"))
        artifact(downloads, 2, 2, report(2))

    def passed_first_then_failed(downloads: Path) -> None:
        artifact(downloads, 2, 2, report(2, "fail"))

    missing = min(3, count)

    def shard_never_uploaded(downloads: Path) -> None:
        shutil.rmtree(downloads / f"feature_{suite}_shard_{missing}_{run_id}_1")

    def another_run_stands_in(downloads: Path) -> None:
        shutil.rmtree(downloads / f"feature_{suite}_shard_{missing}_{run_id}_1")
        artifact(downloads, missing, 1, report(missing), run="999")

    def newest_attempt_uploaded_nothing(downloads: Path) -> None:
        artifact(downloads, 2, 1, report(2, "fail"))
        (downloads / f"feature_{suite}_shard_2_{run_id}_2").mkdir()

    cases = (
        ("rerun_shard_that_failed_then_passed_combines", failed_first_then_passed, True, ""),
        ("newest_report_of_a_shard_wins_over_an_earlier_pass", passed_first_then_failed, False, "shard 2/"),
        ("shard_without_any_report_names_the_cause", shard_never_uploaded, False, f"no attempt of run 424242 uploaded a report for shard {missing}:"),
        ("report_of_another_run_never_stands_in", another_run_stands_in, False, f"no attempt of run 424242 uploaded a report for shard {missing}:"),
        ("newest_attempt_without_a_report_falls_back_to_the_earlier_failure", newest_attempt_uploaded_nothing, False, "did not pass"),
    )
    for label, plant, expect_pass, intended in cases:
        code, record, error = gather_and_combine(label, plant)
        problems = record["problems"]
        passed = (
            (code == 0 and record["status"] == "pass")
            if expect_pass
            else (code != 0 and record["status"] == "fail" and any(intended in item for item in problems))
        )
        results.append({"case": f"feature_{suite}_gather_{label}", "passed": passed, "exit": code, "problems": problems[:3], "error": error})

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
        ("source_shard", "feature_sources", f"2/{FEATURE_COUNTS['sources']}", False),
        ("column_shard", "feature_columns", f"2/{FEATURE_COUNTS['columns']}", False),
        ("failed_shard", "feature_sources", f"2/{FEATURE_COUNTS['sources']}", True),
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
        entries = FEATURE_ENTRIES if group == "lakehouse_features" else ["lakehouse_gold_checks" if part == "gold" else group]
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
            # The case checks the buffered output of items that run together: the budget admits them all at once, as on any machine.
            "CI_CORE_BUDGET": "64",
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
        suite_items = sum(FEATURE_COUNTS.values()) + 2 * len(FEATURE_COUNTS)
        expected_count = 1 + suite_items if parallel else 1
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
    models = list((source.sources.REPO_ROOT / 'dbt/models/features').rglob('*.sql'))
    source.shell.read_relations = lambda *args: {f'main.{path.stem}': '1 rows, md5 ' + 'a' * 32 for path in models if path.parent.name != 'bridge'}
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


def hook_delegation_cases(scratch: Path) -> list[dict[str, Any]]:
    """A manual hook that a check group already runs as the identical complete check leaves the hook partitions, and only then (owner decision 163)."""
    config = json.loads((ROOT / "config/quality/ci_groups.json").read_text(encoding="utf-8"))
    document = load_yaml((ROOT / ".pre-commit-config.yaml").read_text(encoding="utf-8"))
    if not isinstance(document, dict):
        raise ValueError(".pre-commit-config.yaml must contain a mapping")
    delegated = config.get("hooks_run_by_groups", {})
    partitioned = [hook for hooks in config["hook_groups"].values() for hook in hooks]
    entries: list[dict[str, Any]] = workflow_document()["jobs"]["lakehouse"]["strategy"]["matrix"]["include"]
    gold = next((entry for entry in entries if entry.get("part") == "gold"), {})
    results: list[dict[str, Any]] = [
        {
            "case": "model_metadata_hook_runs_once_in_the_gold_group",
            "passed": delegated == {"model-metadata": "lakehouse_gold"}
            and "model-metadata" not in partitioned
            and "checks" in str(gold.get("units", "")).split(),
            "delegated": delegated,
        },
        {
            "case": "delegated_hook_is_the_identical_check_on_every_path_that_selects_it",
            "passed": not ci_hooks.delegation_findings(ROOT),
            "findings": ci_hooks.delegation_findings(ROOT)[:5],
        },
    ]
    plants = {
        "group_without_the_command_is_found": "does not run the identical command",
        "group_not_selected_by_a_matching_path_is_found": "is not selected by",
        "hook_with_file_names_is_found": "takes file names or arguments",
    }
    for label, marker in plants.items():
        root = scratch / f"delegation_{label}"
        shutil.copytree(ROOT / "scripts/ci/groups", root / "scripts/ci/groups")
        (root / "config/quality").mkdir(parents=True)
        damaged_config = copy.deepcopy(ci_groups.load())
        damaged_document = copy.deepcopy(document)
        if label.startswith("group_without"):
            body = root / "scripts/ci/groups/lakehouse_gold.sh"
            body.write_text(
                body.read_text(encoding="utf-8").replace("-m scripts.quality.model_metadata\n", "-m scripts.quality.model_metadata --fast\n"), encoding="utf-8"
            )
        if label.startswith("group_not_selected"):
            damaged_config["groups"]["lakehouse_gold"] = ["^never_matches$"]
            damaged_config["groups"]["lakehouse_bronze"] = ["^never_matches$"]
        if label.startswith("hook_with_file_names"):
            next(hook for repo in damaged_document["repos"] for hook in repo["hooks"] if hook["id"] == "model-metadata")["pass_filenames"] = True
        try:
            problems = ci_hooks.delegation_findings(ROOT, damaged_config, damaged_document, root / "scripts/ci/groups")
        except Exception as error:
            problems = [f"{type(error).__name__}: {error}"]
        results.append({"case": f"hook_delegation_{label}", "passed": any(marker in problem for problem in problems), "findings": problems[:3]})
    return results


HOOK_GROUPS = ("privacy", "documentation", "code", "orphans", "lock", "samples")
HOOK_SAMPLE_SHARDS = json.loads((ROOT / "config/quality/ci_groups.json").read_text(encoding="utf-8")).get("hook_shards", {}).get("samples", 4)
HOOK_SHARD_LABELS = [f"repository hooks: samples shard {index}/{HOOK_SAMPLE_SHARDS}" for index in range(1, HOOK_SAMPLE_SHARDS + 1)]
HOOK_COMBINE_LABEL = f"repository hooks: samples combine/{HOOK_SAMPLE_SHARDS}"
HOOK_EXCLUSIVE_LABELS = [*HOOK_SHARD_LABELS, "repository hooks: lock"]
CHECKS_SUITE = "run_checks_e2e"
# The saved costs are hosted seconds from run 44930295246 (3,818 s in all, about 955 s per shard of 4); 1050 s is about 1.1 times that load.
CHECKS_SHARD_BUDGET_SECONDS = 1050

# A pre-commit stand-in for the sharded samples group: it records each call with the shard environment that the hook entry reads, takes a second, and then
# behaves as the real test-checks entry does for its shard (a manifest of every selected case), except as the files in its folder say.
HOOK_SHARD_STUB = """#!@PYTHON@
import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, "@ROOT@")
from scripts.quality import case_shards, run_checks_e2e

folder = Path("@FOLDER@")
shard = os.environ.get("CHECKS_SHARD", "")
directory = os.environ.get("CHECKS_SHARD_DIRECTORY", "")
started = time.time()
time.sleep(1)
with (folder / "calls.jsonl").open("a") as handle:
    handle.write(json.dumps({"argv": sys.argv[1:], "shard": shard, "directory": directory, "start": started, "end": time.time()}) + "\\n")
if (folder / "skip").exists():
    sys.stdout.write("Each check blocks its bad sample and passes its good one...(no files to check)Skipped\\n")
    raise SystemExit(0)
inventory = run_checks_e2e.inventory()
selected = case_shards.select(inventory, shard, case_shards.saved_costs("@SUITE@"))
failing = (folder / "fail").read_text().strip() if (folder / "fail").exists() else ""
rows = [{"case": name, "ok": shard != failing, "seconds": 1.0} for name in selected]
manifest = case_shards.write_manifest(Path(directory), "@SUITE@", shard, inventory, selected, rows)
raise SystemExit(1 if manifest["status"] == "fail" else 0)
"""


def checks_listing(*arguments: str) -> list[str]:
    run = run_command(str(PYTHON), ["-m", "scripts.quality.run_checks_e2e", "--list-cases", *arguments], cwd=ROOT, timeout=120)
    try:
        names = json.loads(run.stdout)
    except json.JSONDecodeError:
        return []
    return names if run.returncode == 0 and isinstance(names, list) else []


def planted_shard_folders(base: Path, inventory: list[str], count: int) -> None:
    """The folders of a complete, passing run of every shard: the manifest that each shard's test-checks run writes."""
    costs = case_shards.saved_costs(CHECKS_SUITE)
    for index in range(1, count + 1):
        selected = case_shards.select(inventory, f"{index}/{count}", costs)
        case_shards.write_manifest(
            base / f"shard_{index}", CHECKS_SUITE, f"{index}/{count}", inventory, selected, [{"case": name, "ok": True, "seconds": 1.0} for name in selected]
        )


def hook_shard_combine_cases(scratch: Path, inventory: list[str], count: int) -> list[dict[str, Any]]:
    """The combining step of the samples group fails for a missing or doubled case, a missing or failed shard, or shards that disagree about skipping."""
    costs = case_shards.saved_costs(CHECKS_SUITE)
    results: list[dict[str, Any]] = []

    def combine(label: str, damage: Callable[[Path], None], expect_pass: bool, marker: str, arguments: tuple[str, ...] = ()) -> None:
        base = scratch / f"hook_shard_combine_{label}"
        planted_shard_folders(base, inventory, count)
        damage(base)
        outcome = run_command(
            str(PYTHON),
            ["-m", "scripts.quality.ci_hooks", "--group", "samples", "--combine", "--count", str(count), "--output-directory", str(base), *arguments],
            cwd=ROOT,
            timeout=120,
        )
        output = outcome.stdout + outcome.stderr
        passed = outcome.returncode == 0 if expect_pass else outcome.returncode != 0 and marker in output
        results.append(
            {"case": f"hook_samples_combine_{label}", "passed": passed and "Traceback" not in output, "exit": outcome.returncode, "output": output[-400:]}
        )

    def rewrite(shard: int, change: Callable[[list[str], list[dict[str, Any]]], tuple[list[str], list[dict[str, Any]]]]) -> Callable[[Path], None]:
        def damage(base: Path) -> None:
            selected = case_shards.select(inventory, f"{shard}/{count}", costs)
            rows = [{"case": name, "ok": True, "seconds": 1.0} for name in selected]
            chosen, kept = change(selected, rows)
            case_shards.write_manifest(base / f"shard_{shard}", CHECKS_SUITE, f"{shard}/{count}", inventory, chosen, kept)

        return damage

    def skip_shards(first: int) -> Callable[[Path], None]:
        def damage(base: Path) -> None:
            for index in range(first, count + 1):
                (base / f"shard_{index}" / "manifest.json").unlink()
                report = {"group": "samples", "status": "pass", "hooks": [{"hook": "test-checks", "exit": 0, "skipped": True}]}
                (base / f"shard_{index}" / "report.json").write_text(json.dumps(report))

        return damage

    def nothing_damaged(base: Path) -> None:
        return None

    combine("complete_run_passes", nothing_damaged, True, "")
    combine("a_missing_case_fails", rewrite(2, lambda chosen, rows: (chosen, rows[1:])), False, "shard 2/4")
    combine("a_doubled_case_fails", rewrite(3, lambda chosen, rows: (chosen, [*rows, rows[0]])), False, "shard 3/4")
    combine(
        "a_case_of_another_shard_fails",
        rewrite(1, lambda chosen, rows: (chosen, [*rows, {"case": inventory[0], "ok": True, "seconds": 1.0}])),
        False,
        "shard 1/4",
    )
    combine("a_failed_case_fails", rewrite(4, lambda chosen, rows: (chosen, [{**rows[0], "ok": False}, *rows[1:]])), False, "shard 4/4")
    combine("a_missing_shard_fails", lambda base: shutil.rmtree(base / "shard_3"), False, "shard 3/4")
    combine("an_empty_shard_folder_fails", lambda base: (base / "shard_2" / "manifest.json").unlink(), False, "shard 2/4")
    combine("every_shard_skipped_passes", skip_shards(1), True, "")
    combine("one_shard_ran_and_the_others_skipped_fails", skip_shards(2), False, "skipped")
    combine("a_count_other_than_the_configuration_fails", nothing_damaged, False, "count", ("--count", "3"))
    return results


def hook_shard_run_cases(scratch: Path, count: int) -> list[dict[str, Any]]:
    """The real group CLI with a recording pre-commit stand-in: one shard on its own, and the whole samples group (every shard at once, then the combine)."""
    results: list[dict[str, Any]] = []
    for label, arguments, skip, fail in (
        ("one_shard", ("--shard", f"2/{count}"), False, ""),
        ("every_shard_at_once_then_combine", (), False, ""),
        ("a_failed_shard_fails_the_group", (), False, f"3/{count}"),
        ("every_shard_skipped_passes", (), True, ""),
        ("a_shard_that_is_not_in_the_configuration_is_refused", ("--shard", f"2/{count + 1}"), False, ""),
    ):
        root = scratch / f"hook_shard_run_{label}"
        for name in ("config/quality/ci_groups.json", ".pre-commit-config.yaml", ".github/workflows/ci.yml"):
            (root / name).parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(ROOT / name, root / name)
        binary = root / ".venv/bin/pre-commit"
        binary.parent.mkdir(parents=True, exist_ok=True)
        binary.write_text(
            HOOK_SHARD_STUB.replace("@PYTHON@", str(PYTHON)).replace("@ROOT@", str(ROOT)).replace("@FOLDER@", str(root)).replace("@SUITE@", CHECKS_SUITE)
        )
        binary.chmod(0o755)
        if skip:
            (root / "skip").touch()
        if fail:
            (root / "fail").write_text(fail)
        output = root / "output"
        outcome = run_command(
            str(PYTHON),
            ["-m", "scripts.quality.ci_hooks", "--root", str(root), "--group", "samples", *arguments, "--output-directory", str(output)],
            cwd=ROOT,
            timeout=300,
        )
        calls = [json.loads(line) for line in (root / "calls.jsonl").read_text().splitlines()] if (root / "calls.jsonl").exists() else []
        combined_path = output / "combined.json"
        combined = json.loads(combined_path.read_text()) if combined_path.exists() else {}
        entry = ["run", "test-checks", "--all-files", "--hook-stage", "manual", "--show-diff-on-failure"]
        if label == "one_shard":
            checks = {
                "one_call_with_the_unchanged_entry": [call["argv"] for call in calls] == [entry],
                "the_shard_environment": [(call["shard"], call["directory"]) for call in calls] == [(f"2/{count}", str(output / "shard_2"))],
                "its_manifest_and_report": (output / "shard_2/manifest.json").exists() and (output / "shard_2/report.json").exists(),
                "no_combine": not combined_path.exists(),
                "exit_zero": outcome.returncode == 0,
            }
        elif label == "a_shard_that_is_not_in_the_configuration_is_refused":
            checks = {
                "refused": outcome.returncode != 0 and "shard" in outcome.stderr,
                "nothing_ran": not calls,
                "no_traceback": "Traceback" not in outcome.stderr,
            }
        else:
            starts = sorted(call["start"] for call in calls)
            ends = sorted(call["end"] for call in calls)
            checks = {
                "every_shard_once": sorted(call["shard"] for call in calls) == [f"{index}/{count}" for index in range(1, count + 1)],
                "all_shards_overlap": len(calls) == count and starts[-1] < ends[0],
                "own_folders": sorted(call["directory"] for call in calls) == [str(output / f"shard_{index}") for index in range(1, count + 1)],
                "exit_matches": (outcome.returncode != 0) == bool(fail),
                "combined_status": combined.get("status", "") == ("fail" if fail else "pass") or (skip and outcome.returncode == 0),
                "no_traceback": "Traceback" not in outcome.stderr,
            }
            if skip:
                checks["says_skipped"] = "skipped" in (outcome.stdout + outcome.stderr).lower()
        results.append(
            {
                "case": f"hook_samples_group_{label}",
                "passed": all(checks.values()),
                "checks": checks,
                "exit": outcome.returncode,
                "error": outcome.stderr[-400:],
            }
        )
    return results


# An inert pre-commit stand-in for the interruption case: the hook process, a child and a grandchild (each in a session of its own, as a launched check
# leaves them) record themselves and wait. Only a stop of the whole group, with the sessions of its descendants, removes them.
HOOK_INTERRUPT_STUB = """#!@PYTHON@
import json
import os
import time
from pathlib import Path

folder = Path("@RECORDS@")
cli = os.getppid()
role = "hook"
if os.fork() == 0:
    os.setsid()
    role = "child"
    if os.fork() == 0:
        os.setsid()
        role = "grandchild"
(folder / f"pid_{os.getpid()}.json").write_text(json.dumps({"pid": os.getpid(), "role": role, "cli": cli}))
time.sleep(60)
"""
HOOK_INTERRUPT_BOUND_SECONDS = 10


def live_pids(pids: set[int]) -> list[int]:
    """The pids of the set that are alive and not zombies."""
    return sorted(row.pid for row in list_processes() if row.pid in pids and "Z" not in row.state)


def hook_shard_interrupt_case(scratch: Path, count: int, nested: bool, number: int = signal.SIGINT) -> dict[str, Any]:
    """Ctrl-C, TERM or HUP at the group CLI stops every shard and every descendant within a bound (four inert workloads, each with a child and a grandchild).

    Nested: the CLI runs under another ``run_command`` and inherits PROCESS_LAUNCH_SESSION, as it does in the gate. Terminal: the CLI is a top-level caller.
    """
    signal_name = signal.Signals(number).name.lower()
    root = scratch / f"hook_shard_interrupt_{'nested' if nested else 'terminal'}_{signal_name}"
    records = root / "records"
    records.mkdir(parents=True)
    for name in ("config/quality/ci_groups.json", ".pre-commit-config.yaml", ".github/workflows/ci.yml"):
        (root / name).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ROOT / name, root / name)
    binary = root / ".venv/bin/pre-commit"
    binary.parent.mkdir(parents=True)
    binary.write_text(HOOK_INTERRUPT_STUB.replace("@PYTHON@", str(PYTHON)).replace("@RECORDS@", str(records)))
    binary.chmod(0o755)
    seen: dict[str, Any] = {}

    def interrupt_when_ready() -> None:
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            rows = []
            for path in records.glob("pid_*.json"):
                try:
                    rows.append(json.loads(path.read_text()))
                except json.JSONDecodeError:
                    continue
            if len(rows) == 3 * count:
                seen["cli"] = rows[0]["cli"]
                os.kill(seen["cli"], number)
                return
            time.sleep(0.02)

    sender = threading.Thread(target=interrupt_when_ready, name="hook_shard_interrupt_sender")
    sender.start()
    # run_command names its session to the program with PROCESS_LAUNCH_SESSION, which makes the CLI's own launches nested. The env program removes the variable,
    # so the CLI is the top-level caller that a terminal user is.
    command = ["-m", "scripts.quality.ci_hooks", "--root", str(root), "--group", "samples", "--output-directory", str(root / "output")]
    environment = {key: value for key, value in os.environ.items() if key != "JOB_STARTED"}
    started = time.monotonic()
    timed_out = False
    exit_code: int | None = None
    try:
        if nested:
            exit_code = run_command(str(PYTHON), command, cwd=ROOT, env=environment, timeout=HOOK_INTERRUPT_BOUND_SECONDS + 5).returncode
        else:
            terminal = ["-u", "PROCESS_LAUNCH_SESSION", str(PYTHON), *command]
            exit_code = run_command("/usr/bin/env", terminal, cwd=ROOT, env=environment, timeout=HOOK_INTERRUPT_BOUND_SECONDS + 5).returncode
    except TimeoutExpired:
        timed_out = True
    finally:
        sender.join(timeout=10)
    elapsed = time.monotonic() - started
    tracked = {json.loads(path.read_text())["pid"] for path in records.glob("pid_*.json")} | ({seen["cli"]} if "cli" in seen else set())
    deadline = time.monotonic() + HOOK_INTERRUPT_BOUND_SECONDS
    survivors = live_pids(tracked)
    while survivors and time.monotonic() < deadline:
        time.sleep(0.2)
        survivors = live_pids(tracked)
    for pid in survivors:
        with contextlib.suppress(ProcessLookupError):
            os.kill(pid, signal.SIGKILL)
    expected_status: dict[int, int] = {signal.SIGINT: -signal.SIGINT, signal.SIGTERM: 143, signal.SIGHUP: 129}
    checks = {
        "every_workload_started": len(list(records.glob("pid_*.json"))) == 3 * count,
        "interrupt_sent": "cli" in seen,
        "cli_ended_inside_the_bound": not timed_out and elapsed <= HOOK_INTERRUPT_BOUND_SECONDS,
        "cli_failed": exit_code is not None and exit_code != 0,
        "exit_status": exit_code == expected_status[number],
        "no_process_survives": not survivors,
    }
    return {
        "case": f"hook_samples_{'nested' if nested else 'terminal'}_{signal_name}_stops_every_shard_and_descendant",
        "passed": all(checks.values()),
        "checks": checks,
        "survivors": survivors,
        "seconds": round(elapsed, 1),
    }


# Runs the real CLI entry point in this process with a barrier before each shard's stop, so that a second signal can arrive while the cleanup is running. It
# records the exit status, how many stops had finished and which workloads were alive at the moment the CLI returned.
HOOK_MIXED_DRIVER = """
import json
import sys
import time
from pathlib import Path

from scripts import process
from scripts.quality import ci_hooks

work = Path(sys.argv[1])
original_stop = process.stop_launch

def delayed_stop(*args, **kwargs):
    (work / "cleanup_entered").touch()
    deadline = time.monotonic() + 8
    while not (work / "release_cleanup").exists() and time.monotonic() < deadline:
        time.sleep(0.01)
    result = original_stop(*args, **kwargs)
    (work / f"cleanup_done_{args[0]}").touch()
    return result

process.stop_launch = delayed_stop
sys.argv = ["ci_hooks", "--root", str(work), "--group", "samples", "--output-directory", str(work / "output")]
observed = {}
try:
    observed["exit"] = ci_hooks.main()
except KeyboardInterrupt:
    observed["exit"] = 130
workloads = {json.loads(path.read_text())["pid"] for path in (work / "records").glob("pid_*.json")}
observed["alive_at_return"] = sorted(row.pid for row in process.list_processes() if row.pid in workloads and "Z" not in row.state)
observed["stops_done"] = len(list(work.glob("cleanup_done_*")))
(work / "observed.json").write_text(json.dumps(observed))
raise SystemExit(observed["exit"])
"""


def hook_shard_mixed_signal_case(scratch: Path, count: int, nested: bool, first: int, second: int) -> dict[str, Any]:
    """A second signal of any kind during the cleanup is held: the first signal's status comes only after every stop has finished."""
    first_name, second_name = (signal.Signals(number).name.lower() for number in (first, second))
    root = scratch / f"hook_shard_mixed_{'nested' if nested else 'terminal'}_{first_name}_{second_name}"
    records = root / "records"
    records.mkdir(parents=True)
    for name in ("config/quality/ci_groups.json", ".pre-commit-config.yaml", ".github/workflows/ci.yml"):
        (root / name).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ROOT / name, root / name)
    binary = root / ".venv/bin/pre-commit"
    binary.parent.mkdir(parents=True)
    binary.write_text(HOOK_INTERRUPT_STUB.replace("@PYTHON@", str(PYTHON)).replace("@RECORDS@", str(records)))
    binary.chmod(0o755)
    seen: dict[str, Any] = {}

    def send_both() -> None:
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            rows = []
            for path in records.glob("pid_*.json"):
                with contextlib.suppress(json.JSONDecodeError):
                    rows.append(json.loads(path.read_text()))
            if len(rows) == 3 * count:
                seen["cli"] = rows[0]["cli"]
                os.kill(seen["cli"], first)
                while not (root / "cleanup_entered").exists() and time.monotonic() < deadline:
                    time.sleep(0.01)
                os.kill(seen["cli"], second)
                time.sleep(0.4)
                break
            time.sleep(0.02)
        (root / "release_cleanup").touch()

    sender = threading.Thread(target=send_both, name="hook_shard_mixed_sender")
    sender.start()
    environment = {key: value for key, value in os.environ.items() if key != "JOB_STARTED"}
    exit_code: int | None = None
    try:
        if nested:
            exit_code = run_command(str(PYTHON), ["-c", HOOK_MIXED_DRIVER, str(root)], cwd=ROOT, env=environment, timeout=30).returncode
        else:
            terminal = ["-u", "PROCESS_LAUNCH_SESSION", str(PYTHON), "-c", HOOK_MIXED_DRIVER, str(root)]
            exit_code = run_command("/usr/bin/env", terminal, cwd=ROOT, env=environment, timeout=30).returncode
    except TimeoutExpired:
        pass
    finally:
        sender.join(timeout=15)
    observed_path = root / "observed.json"
    observed = json.loads(observed_path.read_text()) if observed_path.exists() else {}
    tracked = {json.loads(path.read_text())["pid"] for path in records.glob("pid_*.json")}
    survivors = live_pids(tracked)
    for pid in survivors:
        with contextlib.suppress(ProcessLookupError):
            os.kill(pid, signal.SIGKILL)
    statuses: dict[int, int] = {signal.SIGINT: 130, signal.SIGTERM: 143, signal.SIGHUP: 129}
    wanted = statuses[first]
    checks = {
        "both_signals_sent_during_the_cleanup": "cli" in seen and (root / "cleanup_entered").exists(),
        "status_of_the_first_signal": observed.get("exit") == wanted,
        "every_stop_finished_before_the_return": observed.get("stops_done") == count,
        "no_workload_alive_at_the_return": observed.get("alive_at_return") == [],
        "no_process_survives": not survivors,
    }
    return {
        "case": f"hook_samples_{'nested' if nested else 'terminal'}_{first_name}_then_{second_name}_during_the_cleanup",
        "passed": all(checks.values()),
        "checks": checks,
        "observed": observed,
        "exit": exit_code,
    }


# The runtime inputs of the test-checks hook that this lane added: the shard helper and the saved costs. Each must select the hook, because a range that
# changes only them would otherwise skip every shard and the combine would accept the skip.
def hook_shard_input_cases(scratch: Path) -> list[dict[str, Any]]:
    """Each runtime input of the sharded run selects the real test-checks hook entry (inert workload substitute); an unrelated path does not."""
    inputs = [str(Path(case_shards.__file__).resolve().relative_to(ROOT)), str(case_shards.COSTS.relative_to(ROOT))]
    config = (ROOT / ".pre-commit-config.yaml").read_text()
    entry = "entry: .venv/bin/python -m scripts.quality.run_checks_e2e"
    results: list[dict[str, Any]] = []
    for label, path in [*[(f"input_{index}", name) for index, name in enumerate(inputs)], ("unrelated_path", "config/quality/job_durations.json")]:
        root = scratch / f"hook_shard_input_{label}"
        for name in ("config/quality/ci_groups.json", ".github/workflows/ci.yml"):
            (root / name).parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(ROOT / name, root / name)
        # The real hook definition with its real file filter; only the entry is an inert program that records that it ran.
        (root / ".pre-commit-config.yaml").write_text(config.replace(entry, "entry: .venv/bin/python hook_probe.py", 1))
        (root / "hook_probe.py").write_text("import os\nfrom pathlib import Path\n\nPath('ran').write_text(os.environ.get('CHECKS_SHARD', ''))\n")
        (root / ".venv/bin").mkdir(parents=True)
        (root / ".venv/bin/python").symlink_to(PYTHON)
        (root / ".venv/bin/pre-commit").symlink_to(ROOT / ".venv/bin/pre-commit")
        git(root, "init", "-q")
        (root / path).parent.mkdir(parents=True, exist_ok=True)
        (root / path).write_text("one\n")
        git(root, "add", "-A")
        identity = ("-c", "user.name=probe", "-c", "user.email=probe@example.invalid", "-c", "commit.gpgsign=false")
        git(root, *identity, "commit", "-q", "-m", "base")
        first = git(root, "rev-parse", "HEAD").strip()
        (root / path).write_text("two\n")
        git(root, "add", "-A")
        git(root, *identity, "commit", "-q", "-m", "change")
        last = git(root, "rev-parse", "HEAD").strip()
        environment = {**os.environ, "PRE_COMMIT_HOME": str(ROOT / ".tools/pre_commit_cache")}
        outcome = run_command(
            str(PYTHON),
            [
                "-m",
                "scripts.quality.ci_hooks",
                "--root",
                str(root),
                "--group",
                "samples",
                "--shard",
                f"1/{HOOK_SAMPLE_SHARDS}",
                "--from",
                first,
                "--to",
                last,
                "--output-directory",
                str(root / "output"),
            ],
            cwd=ROOT,
            env=environment,
            timeout=120,
        )
        ran = (root / "ran").exists()
        report_path = root / "output/shard_1/report.json"
        report = json.loads(report_path.read_text()) if report_path.exists() else {}
        selected = label != "unrelated_path"
        passed = outcome.returncode == 0 and ran == selected and (report.get("hooks") or [{}])[0].get("skipped", False) == (not selected)
        results.append(
            {
                "case": f"hook_samples_{label}_{'selects' if selected else 'does_not_select'}_the_test_checks_hook",
                "passed": passed,
                "path": path,
                "ran": ran,
                "exit": outcome.returncode,
                "error": outcome.stderr[-300:],
            }
        )
    return results


def hook_shard_cases(scratch: Path) -> list[dict[str, Any]]:
    """The test-checks hook is four shards that together run every run_checks_e2e case once, beside a lock group, and a combining step proves it."""
    config = json.loads((ROOT / "config/quality/ci_groups.json").read_text(encoding="utf-8"))
    count = config.get("hook_shards", {}).get("samples", 0)
    inventory = checks_listing()
    selections = [checks_listing("--shard", f"{index}/{count}") for index in range(1, count + 1)] if count else []
    flat = [name for names in selections for name in names]
    costs = json.loads((ROOT / "config/quality/shard_costs.json").read_text(encoding="utf-8")).get("suites", {}).get(CHECKS_SUITE, {})
    loads = [sum(costs.get(name, 0) for name in names) for names in selections]
    results = [
        {
            "case": "hook_samples_is_four_test_checks_shards_and_the_lock_group_is_its_own",
            "passed": count == 4 and config["hook_groups"].get("samples") == ["test-checks"] and config["hook_groups"].get("lock") == ["test-lock"],
            "shards": count,
            "groups": {name: config["hook_groups"].get(name) for name in ("samples", "lock")},
        },
        {
            "case": "every_run_checks_case_lands_in_exactly_one_shard",
            "passed": len(inventory) > 676 and len(set(inventory)) == len(inventory) and bool(flat) and sorted(flat) == sorted(inventory) and all(selections),
            "inventory": len(inventory),
            "selected": len(flat),
            "per_shard": [len(names) for names in selections],
        },
        {
            "case": "saved_costs_cover_every_case_and_keep_each_shard_inside_the_budget",
            "passed": bool(inventory)
            and set(costs) == set(inventory)
            and bool(loads)
            and max(loads) <= CHECKS_SHARD_BUDGET_SECONDS
            and max(loads) <= 1.25 * min(loads),
            "unpriced": len(set(inventory) - set(costs)),
            "unknown": len(set(costs) - set(inventory)),
            "shard_seconds": [round(load) for load in loads],
        },
    ]
    if count and inventory:
        results += hook_shard_combine_cases(scratch, inventory, count)
    results += hook_shard_run_cases(scratch, count)
    results += hook_shard_input_cases(scratch)
    if count:
        results += [
            hook_shard_interrupt_case(scratch, count, nested, number) for number in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP) for nested in (True, False)
        ]
        pairs = ((signal.SIGTERM, signal.SIGINT), (signal.SIGINT, signal.SIGTERM), (signal.SIGHUP, signal.SIGINT), (signal.SIGTERM, signal.SIGTERM))
        results += [hook_shard_mixed_signal_case(scratch, count, nested, first, second) for first, second in pairs for nested in (True, False)]
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
        "config/quality/job_durations.json",
        "config/quality/launcher_inputs.json",
        "config/lakehouse/bronze_tables.json",
        "config/lakehouse/aws_bronze_grants.json",
        *BODY_FILES,
        *stack_config_reads(),
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
            text = path.read_text().replace('  local shard="${1:?}"', '  local shard="${1:?}"\n  if [[ "$shard" == 2/16 ]]; then return 3; fi', 1)
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
    return [*results, *dispatcher_output_cases(base, scratch), *stack_config_read_cases()]


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


def feature_count_drifts(workflow: dict[str, Any]) -> dict[str, list[str]]:
    """What differs between the feature jobs of a workflow and the shard counts of the configuration: the shard argument of each job and the combining count."""
    plan = github_plan_of(workflow)
    shards = {f"{suite}:{index}/{count}" for suite, count in FEATURE_COUNTS.items() for index in range(1, count + 1)}
    combines = {f"{suite}:{count}" for suite, count in FEATURE_COUNTS.items()}
    return {
        "shard_arguments": sorted(plan["feature_shards"] ^ shards),
        "combine_counts": sorted(plan["feature_combines"] ^ combines),
    }


# The shard counts of the feature suites come from feature_shards in config/quality/ci_groups.json. A runner or script that writes a count as a literal
# next to the suite name drifts from the configuration the next time the count changes. The patterns are put together from parts so that this file does not
# match them itself.
COUNT_LITERALS = (
    re.compile(r"features_combine (?:sources|columns)" + r"/\d+"),
    re.compile(r"\(\"(?:sources|columns)\"" + r", \d+\)"),
    re.compile(r"\"feature_(?:sources|columns)\", \"\d+" + r"/\d+\""),
)
# The workflow cannot read the configuration; feature_workflow_counts_equal_the_configuration compares its counts with it.
COUNT_LITERAL_FILES = ("scripts/run_ci.sh", "scripts/quality/*.py", "scripts/ci/**/*.sh", "scripts/lakehouse/feature_shards.py")


def feature_count_literal_case() -> dict[str, Any]:
    """No runner, script or workflow writes a feature suite's shard count as a literal: the count is read from feature_shards in the configuration."""
    found: list[str] = []
    for pattern in COUNT_LITERAL_FILES:
        for path in sorted(ROOT.glob(pattern)):
            for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
                if any(literal.search(line) for literal in COUNT_LITERALS):
                    found.append(f"{path.relative_to(ROOT)}:{number}")
    return {"case": "feature_shard_counts_are_not_literals", "passed": not found, "literals": found}


def feature_count_cases() -> list[dict[str, Any]]:
    """The hosted feature jobs pass the configured shard count in the shard argument and to the combiner (a count left at 8 skipped half the cases)."""
    real = feature_count_drifts(workflow_document())
    argument = copy.deepcopy(workflow_document())
    for entry in argument["jobs"]["feature_sources"]["steps"]:
        if "FEATURE_SHARD" in entry.get("env", {}):
            entry["env"]["FEATURE_SHARD"] = str(entry["env"]["FEATURE_SHARD"]).replace(f"/{FEATURE_COUNTS['sources']}", "/8")
    combiner = copy.deepcopy(workflow_document())
    for definition in combiner["jobs"].values():
        for entry in definition.get("steps", []):
            if "feature_shards combine --suite sources" in str(entry.get("run", "")):
                entry["run"] = str(entry["run"]).replace(f"--suite sources --count {FEATURE_COUNTS['sources']}", "--suite sources --count 8")
    planted = {"shard_argument": feature_count_drifts(argument), "combine_count": feature_count_drifts(combiner)}
    return [
        feature_count_literal_case(),
        {"case": "feature_workflow_counts_equal_the_configuration", "passed": not any(real.values()), "differences": real},
        {
            "case": "feature_workflow_count_drift_is_reported",
            "passed": bool(planted["shard_argument"]["shard_arguments"])
            and not planted["shard_argument"]["combine_counts"]
            and bool(planted["combine_count"]["combine_counts"])
            and not planted["combine_count"]["shard_arguments"],
            "differences": planted,
        },
    ]


def parallel_ci_cases(scratch: Path) -> list[dict[str, Any]]:
    """List the real feature runners without builds and verify complete, disjoint hosted shards and manual hooks."""
    inventory = json.loads((ROOT / "config/quality/feature_cases.json").read_text())
    workflow = workflow_document()
    results: list[dict[str, Any]] = []
    for suite, count in FEATURE_COUNTS.items():
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
    results += feature_count_cases()
    hook_document = load_yaml((ROOT / ".pre-commit-config.yaml").read_text())
    if not isinstance(hook_document, dict):
        raise ValueError(".pre-commit-config.yaml must contain a mapping")
    hooks = hook_document["repos"][0]["hooks"]
    delegated = json.loads((ROOT / "config/quality/ci_groups.json").read_text(encoding="utf-8")).get("hooks_run_by_groups", {})
    expected = sorted(hook["id"] for hook in hooks if "manual" in hook.get("stages", ["pre-commit", "manual"]) and hook["id"] not in delegated)
    actual: list[str] = []
    exits = []
    for group in HOOK_GROUPS:
        listed = run_command(str(PYTHON), ["-m", "scripts.quality.ci_hooks", "--group", group, "--list"], cwd=ROOT, timeout=60)
        exits.append(listed.returncode)
        try:
            actual.extend(json.loads(listed.stdout))
        except json.JSONDecodeError:
            actual.append("<invalid hook list>")
    results.append(
        {
            "case": "hook_groups_cover_every_manual_hook_exactly_once",
            "passed": exits == [0] * len(HOOK_GROUPS) and sorted(actual) == expected,
            "missing": sorted(set(expected) - set(actual)),
            "repeated": sorted(name for name in set(actual) if actual.count(name) > 1),
            "exits": exits,
        }
    )
    return [
        *results,
        *feature_combination_cases(scratch),
        *feature_gather_cases(scratch),
        *hook_partition_cases(scratch),
        *hook_shard_cases(scratch),
        *hook_delegation_cases(scratch),
        hook_timeout_case(scratch),
        *feature_case_timing_cases(scratch),
    ]


def speed_static_cases(scratch: Path) -> list[dict[str, Any]]:
    return [
        *parallel_ci_cases(scratch),
        *single_command_cases(),
        *timing_function_cases(scratch),
        *gold_unit_cases(),
        *feature_split_cases(),
        *acquisition_script_cases(),
        *timeout_cases(),
        *core_command_cases(),
        *feature_weight_cases(),
        *sample_weight_cases(),
        *isolation_filter_cases(),
    ]


SCHEDULING_PROBE = """
import hashlib
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Callable

def observe(label: str, argv: list[str], action: Callable[[], Any]) -> Any:
    folder = Path(os.environ["SCHEDULING_EVENTS"])
    settings = json.loads((folder / "settings").read_text())
    exclusive, peers = settings["exclusive"], settings["peers"]
    def marker(name: str, state: str) -> Path:
        return folder / (hashlib.sha256(name.encode()).hexdigest() + "." + state)
    def await_markers(wanted: list[Path]) -> bool:
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if all(path.exists() for path in wanted):
                return True
            time.sleep(0.01)
        return False
    started = time.monotonic_ns()
    marker(label, "started").touch()
    met = True
    if label in exclusive:
        met = await_markers([marker(name, "started") for name in exclusive])
    elif label in peers:
        met = await_markers([marker(name, "started") for name in [*exclusive, *peers]])
        marker(label, "ready").touch()
        met = await_markers([marker(name, "ready") for name in peers]) and met
    result = action()
    ended = time.monotonic_ns()
    code = result["exit"] if isinstance(result, dict) else int(result)
    row = {"label": label, "argv": argv, "start": started, "end": ended, "barrier_met": met, "exit": code}
    (folder / f"{os.getpid()}_{started}.json").write_text(json.dumps(row))
    return result

if __name__ == "__main__":
    label = sys.argv[1]
    raise SystemExit(observe(label, sys.argv[2:], lambda: 3 if label == os.environ.get("SCHEDULING_FAIL") else 0))
"""
ORCHESTRATION_COMMANDS = (
    ("scripts.orchestration.run_lineage_e2e",),
    ("scripts.orchestration.task_guard_cases",),
    ("scripts.orchestration.trigger_dag_cases", "-B", "--output", "@quality@/trigger_dag_cases/report.json"),
    ("scripts.orchestration.run_ecs_launch_e2e",),
    ("scripts.orchestration.run_airflow_mode_e2e",),
    ("scripts.orchestration.run_ecs_lineage_e2e",),
    ("scripts.orchestration.run_dbt_artifacts_e2e",),
    ("scripts.lakehouse.run_layer_run_e2e",),
    ("scripts.lakehouse.run_code_archive_e2e",),
    ("scripts.lakehouse.run_bronze_run_e2e",),
    ("scripts.orchestration.run_publish_contracts",),
    ("scripts.orchestration.run_pipeline_light",),
    ("scripts.serving.run_serving_checks", "-B", "--output", "@quality@/serving_checks"),
    ("scripts.streamlit_app.run_streamlit_checks", "-B", "--output", "@quality@/streamlit_checks"),
    ("scripts.orchestration.offline_checks", "--no-image"),
)


def scheduling_probe(root: Path, events: Path, exclusive: list[str], peers: list[str]) -> Path:
    events.mkdir(parents=True)
    (events / "settings").write_text(json.dumps({"exclusive": exclusive, "peers": peers}))
    probe = root / "scheduling_probe.py"
    probe.write_text(SCHEDULING_PROBE)
    return probe


def scheduling_rows(events: Path) -> list[dict[str, Any]]:
    return sorted((json.loads(path.read_text()) for path in events.glob("*.json")), key=lambda row: row["start"])


def intervals_overlap(left: dict[str, Any], right: dict[str, Any]) -> bool:
    return left["start"] < right["end"] and right["start"] < left["end"]


def dispatcher_scheduling_case(work: Path, out: Path, part: str, staged: bool = False, fail_samples: bool = False) -> dict[str, Any]:
    """Run the dispatcher with native event stand-ins, unchanged orchestration commands and two shared workers that must rendezvous."""
    name = f"dispatcher_scheduling_{part}_{'staged' if staged else 'samples_failure' if fail_samples else 'success'}"
    events = out / f"{name}_events"
    exclusive = HOOK_EXCLUSIVE_LABELS if part == "all" else []
    peers = ["probe: python_lint", "probe: configuration_checks"]
    probe = scheduling_probe(work, events, exclusive, peers)
    quality = out / f"{name}_quality"
    timing = (ROOT / TIMING).read_text().replace("timed() {", "scheduling_timed() {", 1)
    timing += '\ntimed() { local label="$1"; shift; scheduling_timed "$label" "$python_bin" "$SCHEDULING_PROBE" "$label" "$@"; }\n'
    entries = (*CORE[:1], *CORE[2:], "lakehouse_bronze", "lakehouse_silver", *GOLD_STUBS, "gate_stage_checks")

    def prepare(tree: Path) -> None:
        bodies = {entry: f'  timed "probe: {entry}" /usr/bin/true' for entry in entries}
        for suite in ("store", "sources", "columns", "combine"):
            bodies[f"lakehouse_features_{suite}"] = f'  timed "probe: lakehouse_features_{suite} ${{1:-}}" /usr/bin/true'
        stub_functions(tree, bodies)
        acquisition = tree / "scripts/acquisition/run_checks.sh"
        acquisition.write_text(
            acquisition.read_text() + '\n"$PYTHON_BIN" "$SCHEDULING_PROBE" "probe: acquisition ${ACQUISITION_PART:-} ${ACQUISITION_SHARD:-}"\n'
        )
        if staged:
            path = tree / "monitoring/scheduling_probe.py"
            path.parent.mkdir(exist_ok=True)
            path.write_text("synthetic = True\n")
            git(tree, "add", "monitoring/scheduling_probe.py")

    try:
        code, log = dry_run(
            work,
            out,
            name,
            {
                "CI_SCOPE": "staged" if staged else "full",
                "CI_PART": part,
                "SCHEDULING_EVENTS": str(events),
                "SCHEDULING_PROBE": str(probe),
                "SCHEDULING_FAIL": HOOK_SHARD_LABELS[2] if fail_samples else "",
                "QUALITY_ARTIFACT_DIR": str(quality),
                # The case checks the order and the rendezvous of items that run together: the budget admits them all at once, as on any machine.
                "CI_CORE_BUDGET": "64",
            },
            prepare=prepare,
            timing=timing,
        )
    finally:
        probe.unlink(missing_ok=True)
    rows = scheduling_rows(events)
    ordinary = [row for row in rows if not row["label"].startswith("gates:")]
    shared = [
        row
        for row in ordinary
        if not row["label"].startswith("launcher E2E")
        and row["label"] not in {"probe: lakehouse_gold_writer_setup", HOOK_COMBINE_LABEL, *HOOK_EXCLUSIVE_LABELS}
    ]
    samples = [row for row in rows if row["label"] in HOOK_EXCLUSIVE_LABELS]
    combine = [row for row in rows if row["label"] == HOOK_COMBINE_LABEL]
    wanted: list[list[str]] = []
    for module, *arguments in ORCHESTRATION_COMMANDS:
        flags = [arguments.pop(0)] if arguments[:1] == ["-B"] else []
        expanded = [argument.replace("@quality@", str(quality)) for argument in arguments]
        wanted.append([str(PYTHON), *flags, "-m", module, *expanded])
    actual = [row["argv"] for row in rows if row["label"].startswith("orchestration_checks:")]
    labels = [row["label"].strip() for row in rows if not row["label"].startswith(("gates:", "orchestration_checks:"))]
    expected = [
        f"probe: {entry}"
        for entry in (
            CORE[:1] + CORE[2:]
            if not staged
            else [
                "python_lint",
                "repository_code_checks",
                "ci_tooling_checks",
                "ci_tooling_runtime",
                "configuration_checks",
                "configuration_policy_checks",
                "configuration_host_checks",
                "configuration_owner_checks",
            ]
        )
    ]
    if part == "all":
        expected += [f"probe: {entry}" for entry in ("lakehouse_bronze", "lakehouse_silver", *GOLD_STUBS, "gate_stage_checks")]
        expected += ["probe: lakehouse_features_store", "lakehouse_features: feature sources check", "lakehouse_features: feature columns check"]
        # A failed shard of the hooks stops the combining steps of the run, as a failed acquisition shard does, while the other workers still run.
        expected += [] if fail_samples else [f"probe: lakehouse_features_combine {suite}/{count}" for suite, count in FEATURE_COUNTS.items()]
        expected += [f"probe: lakehouse_features_{suite} {index}/{count}" for suite, count in FEATURE_COUNTS.items() for index in range(1, count + 1)]
        expected += [f"repository hooks: {group}" for group in ("privacy", "documentation", "code", "orphans", "lock")] + HOOK_SHARD_LABELS
        expected += [] if fail_samples else [HOOK_COMBINE_LABEL]
        expected += [f"launcher E2E shard {index}/2" for index in (1, 2)]
        expected += [f"probe: acquisition shard {index}/4" for index in range(1, 5)]
        expected += [] if fail_samples else ["probe: acquisition combine"]
    peer_rows = [row for row in rows if row["label"] in peers]
    writer = [row for row in rows if row["label"] == "probe: lakehouse_gold_writer_setup"]
    summary = (out / f"{name}_summary.md").read_text()
    checks = {
        "intended_exit": code != 0 if fail_samples else code == 0,
        "orchestration_commands_exactly_once_with_unchanged_argv": actual == ([] if staged else wanted),
        "selected_commands_exactly_once": sorted(labels) == sorted(expected),
        "the_shards_and_the_lock_run_at_once": part != "all"
        or len(samples) == len(HOOK_EXCLUSIVE_LABELS)
        and all(row["barrier_met"] for row in samples)
        and all(intervals_overlap(left, right) for left in samples for right in samples if left is not right),
        "gold_writer_precedes_samples": not writer or not samples or writer[0]["end"] <= min(row["start"] for row in samples),
        "the_combine_runs_once_after_every_shard": part != "all"
        or fail_samples
        and not combine
        or len(combine) == 1
        and all(row["end"] <= combine[0]["start"] for row in samples),
        "failed_samples_do_not_stop_the_shared_work": not fail_samples or len(shared) > 0,
        "remaining_shared_workers_overlap": fail_samples
        or len(peer_rows) == 2
        and all(row["barrier_met"] for row in peer_rows)
        and intervals_overlap(*peer_rows),
        "samples_failure_is_preserved": not fail_samples
        or re.search(rf"^\| {re.escape(HOOK_SHARD_LABELS[2])} \| .* \| 3 \|$", summary, re.MULTILINE) is not None
        and PASS_LINES["all"] not in log,
    }
    return {"case": name, "passed": all(checks.values()), "checks": checks, "exit": code, "events": rows, "output": log[-500:]}


def dispatcher_scheduling_cases(work: Path, out: Path) -> list[dict[str, Any]]:
    return [
        dispatcher_scheduling_case(work, out, "core"),
        dispatcher_scheduling_case(work, out, "all"),
        dispatcher_scheduling_case(work, out, "core", staged=True),
        dispatcher_scheduling_case(work, out, "all", fail_samples=True),
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
    cases += dispatcher_scheduling_cases(work, out)
    return cases


BODY_SELECTION = {
    "python_lint": ["python_lint", "repository_code_checks", "ci_tooling_checks", "ci_tooling_runtime"],
    "lakehouse_bronze": [*LAYERS, "repository_code_checks", "ci_tooling_checks", "ci_tooling_runtime"],
    "lakehouse_silver": [*LAYERS[1:], "repository_code_checks", "ci_tooling_checks", "ci_tooling_runtime"],
    "lakehouse_gold": [*LAYERS[2:], "repository_code_checks", "ci_tooling_checks", "ci_tooling_runtime"],
    "lakehouse_features": ["lakehouse_features", "repository_code_checks", "ci_tooling_checks", "ci_tooling_runtime"],
    "orchestration_checks": ["orchestration_checks", "repository_code_checks", "ci_tooling_checks", "ci_tooling_runtime"],
    "orchestration_engine_e2e": ["orchestration_engine_e2e", "repository_code_checks", "ci_tooling_checks", "ci_tooling_runtime"],
    "repository_code_checks": ["repository_code_checks", "ci_tooling_checks", "ci_tooling_runtime"],
    "ci_tooling_checks": ["repository_code_checks", "ci_tooling_checks", "ci_tooling_runtime"],
    "ci_tooling_runtime": ["repository_code_checks", "ci_tooling_checks", "ci_tooling_runtime"],
    "configuration_checks": ["configuration_checks", "repository_code_checks", "ci_tooling_checks", "ci_tooling_runtime"],
    "configuration_policy_checks": ["configuration_policy_checks", "repository_code_checks", "ci_tooling_checks", "ci_tooling_runtime"],
    "configuration_host_checks": ["configuration_host_checks", "repository_code_checks", "ci_tooling_checks", "ci_tooling_runtime"],
    "configuration_owner_checks": ["configuration_owner_checks", "repository_code_checks", "ci_tooling_checks", "ci_tooling_runtime"],
    "sql_style": ["repository_code_checks", "ci_tooling_checks", "ci_tooling_runtime", "sql_style"],
    "terraform_checks": ["repository_code_checks", "ci_tooling_checks", "ci_tooling_runtime", "terraform_checks"],
    "terraform_services_checks": ["repository_code_checks", "ci_tooling_checks", "ci_tooling_runtime", "terraform_services_checks"],
    "terraform_proofs": ["repository_code_checks", "ci_tooling_checks", "ci_tooling_runtime", "terraform_proofs"],
    "terraform_replace_proofs": ["repository_code_checks", "ci_tooling_checks", "ci_tooling_runtime", "terraform_replace_proofs"],
    "launcher": ["repository_code_checks", "ci_tooling_checks", "ci_tooling_runtime", "launcher"],
    "gate_stage_checks": ["repository_code_checks", "ci_tooling_checks", "ci_tooling_runtime", "launcher", "gate_stage_checks"],
}


def staged_edit_case(
    work: Path, name: str, path: str, want_run: list[str], part: str = "core+layers", want_reason: str = "", split: bool = True
) -> dict[str, Any]:
    """Stage a one-line edit of ``path`` and check which groups the staged scope selects."""
    target = work / path
    try:
        target.write_text(target.read_text(encoding="utf-8") + "\n# staged edit\n", encoding="utf-8")
        git(work, "add", path)
        return selection_case(work, name, ["--scope", "staged", "--part", part], want_run, want_reason, split)
    finally:
        restore(work)


def body_file_cases(work: Path) -> list[dict[str, Any]]:
    """An edit to a body file selects its own group (and the layers downstream of a lakehouse layer) and the repository group, which runs the guard, and no
    other group in the core and layer parts (a body file lies below scripts/, so the launcher part runs too). The dispatcher and the workflow
    select every group."""
    cases = [
        {
            "case": "every_body_file_has_an_expected_selection",
            "passed": sorted(BODY_SELECTION) == sorted(Path(path).stem for path in BODY_FILES),
            "body_files": [Path(path).stem for path in BODY_FILES],
        }
    ]
    for group, wanted in BODY_SELECTION.items():
        part = "all" if group in {"launcher", "gate_stage_checks"} else "core+layers"
        cases.append(staged_edit_case(work, f"body_file_edit_selects_{group}", f"{GROUP_FILES}/{group}.sh", wanted, part, split=False))
    cases.append(staged_edit_case(work, "dispatcher_edit_selects_every_group", "scripts/run_ci.sh", WITH_LAYERS, want_reason="selects every group"))
    cases.append(
        staged_edit_case(
            work,
            "dispatcher_edit_selects_every_group_in_part_all",
            "scripts/run_ci.sh",
            [*WITH_LAYERS, "launcher", "gate_stage_checks"],
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
        for path in (".github/workflows/ci.yml",)
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
        "hook_shards": set(f"{group}:{shard}" for group, shard in re.findall(r"^TIMED repository hooks: (\w+) shard (\d+/\d+) ", log, re.MULTILINE)),
        "hook_combines": set(f"{group}:{count}" for group, count in re.findall(r"^TIMED repository hooks: (\w+) combine/(\d+) ", log, re.MULTILINE)),
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
    hook_shards: set[str] = set()
    hook_combines: set[str] = set()
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
                        shard = re.search(r"--shard '(\d+/\d+)'", command)
                        if shard:
                            hook_shards.add(f"{found[1]}:{shard[1]}")
                        count = re.search(r"--combine --count (\d+)", command)
                        if count:
                            hook_combines.add(f"{found[1]}:{count[1]}")
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
        "hook_shards": hook_shards,
        "hook_combines": hook_combines,
        "launcher_shards": launcher_shards,
    }


def mirror_differences(github: dict[str, Any], local: dict[str, Any]) -> dict[str, Any]:
    """What differs between the GitHub jobs and the full local run; empty when the local run mirrors GitHub."""
    differences = {
        name: {"github": sorted(github[name]), "local": sorted(local[name])}
        for name in (
            "groups",
            "units",
            "shards",
            "combine_counts",
            "feature_shards",
            "feature_combines",
            "hook_groups",
            "hook_shards",
            "hook_combines",
            "launcher_shards",
        )
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
    hook_shard = copy.deepcopy(workflow_document())
    hook_shard["jobs"]["hook_samples"]["strategy"]["matrix"]["shard"].pop()
    drifts["hook_samples_shard"] = hook_shard
    hook_combine = copy.deepcopy(workflow_document())
    for step in hook_combine["jobs"]["hook_samples_combine"]["steps"]:
        step["run"] = str(step.get("run", "")).replace("--combine --count 4", "--combine --count 3")
    drifts["hook_samples_combine_count"] = hook_combine
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
        "only_its_group_and_the_repository_group_ran": sorted(local["items"])
        == ["ci_tooling_checks", "ci_tooling_runtime", "repository_code_checks", "sql_style"],
        # sql_style.sh lies below scripts/, which the reviewed inventory_reads patterns of code_digest.py and code_closure.py name, so the dry run starts it.
        "launcher_selected_by_a_scripts_path": "TIMED launcher E2E" in log
        and local["groups"] == {"launcher", "ci_tooling_checks", "ci_tooling_runtime", "repository_code_checks", "sql_style"},
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
    # The scheduler lines of run_ci.sh ("Local CI: admitted <group> (...)", "Local CI: <group> ended (...)") are noise: order and cores in use.
    head = re.sub(r"^Local CI: (?:admitted \w+|\w+ ended) \(.*\n", "", head, flags=re.MULTILINE)
    blocks = {name: re.split(r"\n(?:All local CI checks passed|Local CI failed)", body)[0] for name, body in zip(parts[::2], parts[1::2], strict=True)}
    return head, blocks


def isolation_filter_cases() -> list[dict[str, Any]]:
    """The scheduler lines of run_ci.sh (Local CI: admitted <group> (...) and Local CI: <group> ended (...)) are left out of the text before the first
    item, whatever the cores in use or the order, and any other line there still counts."""
    body = "== python_lint ==\nTIMED python_lint: probe x\n"
    first = (
        "Check groups: full\nLocal CI: admitted python_lint (1 cores; 1 of 16 cores in use).\n"
        "Local CI: terraform_checks ended (2 of 16 cores in use).\nLocal CI: terraform_services_checks ended (0 of 16 cores in use).\n" + body
    )
    second = (
        "Check groups: full\nLocal CI: admitted python_lint (2 cores; 5 of 12 cores in use).\n"
        "Local CI: terraform_services_checks ended (3 of 12 cores in use).\nLocal CI: terraform_checks ended (1 of 12 cores in use).\n" + body
    )
    other = first.replace("Check groups: full", "Check groups: staged")
    return [
        {"case": "isolation_comparison_ignores_scheduler_lines_with_other_core_counts_and_order", "passed": item_blocks(first)[0] == item_blocks(second)[0]},
        {"case": "isolation_comparison_still_sees_other_lines_before_the_groups", "passed": item_blocks(first)[0] != item_blocks(other)[0]},
        {
            "case": "isolation_comparison_keeps_the_group_blocks",
            "passed": item_blocks(first)[1] == item_blocks(second)[1] == {"python_lint": "\nTIMED python_lint: probe x\n"},
        },
    ]


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
    "lakehouse_features": FEATURE_ENTRIES,
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
# The step that starts a worker in its own session and the step that stops the sessions import scripts.process: they run for real too.
if [[ "$*" == *scripts.quality.ci_groups* || "$*" == *config/quality/ci_groups.json* || "$*" == *"from scripts.process import"* ]]; then
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
            env={**environment, "GITHUB_STEP_SUMMARY": str(out / f"{name}_summary.md"), "PYTHON_BIN": str(filter_path), **extra},
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
        if entry not in {"launcher_e2e", "gate_stage_checks", *STAND_IN_ENTRIES["lakehouse_gold"], *STAND_IN_ENTRIES["lakehouse_features"], *LAYERS}
    ]
    layers = {
        "bronze": ["lakehouse_bronze"],
        "silver": ["lakehouse_silver"],
        "gold": ["lakehouse_gold_checks", "lakehouse_gold_writer_setup", "lakehouse_gold_model_metadata_e2e"],
        "features": STAND_IN_ENTRIES["lakehouse_features"],
        "feature_sources": ["lakehouse_features_sources", "lakehouse_features_sources_check", "lakehouse_features_combine"],
        "feature_columns": ["lakehouse_features_columns", "lakehouse_features_columns_check", "lakehouse_features_combine"],
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
        "check-head",
        "check-tools",
        "gitleaks",
        "credential-files",
        "data-files",
        "removed-names",
        "task-ids",
        "command-modules",
        "typecheck-importers",
        "collector-code-versions",
        "launcher-list",
        "temporary-code",
        "privacy-scan",
        "writing-check",
        "front-matter",
        "docs-references",
        "markdown-links",
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
# The checks of the old on-demand group that run at commit too, each only when a file it reads is staged (never ``always_run``); they stay in the manual stage.
COMMIT_PATH_HOOKS = frozenset(
    {
        "gitleaks-outgoing",
        "env-example",
        "unused-code",
        "unused-dependencies",
        "dependency-lock",
        "orphan-files",
        "terraform-unused",
        "sqlfluff",
        "build-contexts",
        "model-metadata",
        "documentation-review-untracked",
    }
)
PUSH_HOOKS = frozenset({"local_ci_pushed"})
INSTALL_TYPES = ("pre-commit", "commit-msg", "pre-push")
FINGERPRINT_KEYS = ("entry", "language", "files", "exclude", "types", "types_or", "always_run", "pass_filenames", "verbose", "args")
# What the hosted hooks job runs: every hook of the manual stage, as it was before the commit stage was reduced, and the seven linters of lint_tools.
# The path hooks of ``COMMIT_PATH_HOOKS`` carry their commit selection (``files``, ``exclude``, ``types``, no ``always_run``) in the pin.
# The hosted run passes ``--all-files``, which selects such a hook whenever a tracked file matches (``hook_path_cases`` proves it), so it runs them as before.
MANUAL_FINGERPRINTS = {
    "gitleaks-outgoing": "cb03afc2b3f1",
    "credential-files": "9d1f2d4ee86b",
    "data-files": "1681a06bc29d",
    "env-example": "b59895fa6402",
    "removed-names": "acffa6ea5eac",
    "task-ids": "af8e2f11a070",
    "command-modules": "aa5699cf839f",
    "temporary-code": "9ee83ad12203",
    "unused-code": "fec61cf0a373",
    "unused-dependencies": "3e0448836624",
    "orphan-files": "0c7fc958a22e",
    "terraform-unused": "20a4046c86ae",
    "privacy-scan": "ce896f9743dd",
    "writing-check": "6103a7434254",
    "front-matter": "82a03c2777b3",
    "docs-references": "6546547e6ece",
    "markdown-links": "8edd25262d5a",
    "markdownlint": "28a824e49edc",
    "sqlfluff": "df6218ca51e0",
    "ruff": "5f2147654150",
    "shell-syntax": "f63508f2a1fc",
    "shellcheck": "f0e1e106ec2c",
    "actionlint": "f19f2e73cdb9",
    "yamllint": "455872e07694",
    "hadolint": "9234e13781be",
    "build-contexts": "31f024a54e9b",
    "container-mounts": "8be103fa11d5",
    "gofmt": "de0cf2f5169c",
    "go-vet": "5bcd441b4c87",
    "model-metadata": "3d8c0edf7a2b",
    "documentation-review-untracked": "83b7b0ce6039",
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
    "osv-scanner": "6cccadc5d95b",
    "test-checks": "dce7c585008d",
    "dependency-lock": "f296b158cf5d",
    "test-lock": "54d78b27f865",
    "test-documentation-review": "db426284e54a",
}


def hook_table(document: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {hook["id"]: hook for repo in document["repos"] for hook in repo["hooks"]}


def stage_set(document: dict[str, Any], hook: dict[str, Any]) -> set[str]:
    return set(hook.get("stages", document.get("default_stages", [])))


def hook_config_problems(document: dict[str, Any], partitions: dict[str, list[str]], delegated: dict[str, str] | None = None) -> dict[str, Any]:
    """Every way the hook configuration can leave the fast commit stage, the pre-push run or the hosted manual stage wrong."""
    hooks = hook_table(document)
    commit = {name for name, hook in hooks.items() if "pre-commit" in stage_set(document, hook)}
    manual = {name for name, hook in hooks.items() if "manual" in stage_set(document, hook)}
    fingerprints = {
        name: hashlib.sha256(json.dumps({key: hooks[name].get(key) for key in FINGERPRINT_KEYS}, sort_keys=True).encode()).hexdigest()[:12] for name in manual
    }
    moved = sorted(MANUAL_FINGERPRINTS)
    partitioned = {name for names in partitions.values() for name in names} | set(delegated or {})
    return {
        "heavy_in_commit_stage": sorted(commit - FAST_COMMIT_HOOKS - COMMIT_PATH_HOOKS),
        "fast_missing_from_commit_stage": sorted((FAST_COMMIT_HOOKS | COMMIT_PATH_HOOKS) - commit),
        "commit_path_hooks_always_run": sorted(name for name in COMMIT_PATH_HOOKS if hooks.get(name, {}).get("always_run")),
        "install_types": [kind for kind in INSTALL_TYPES if kind not in document.get("default_install_hook_types", [])],
        "push_hooks_not_in_push_stage": sorted(name for name in PUSH_HOOKS if stage_set(document, hooks.get(name, {})) != {"pre-push"}),
        "manual_changed": sorted(name for name in MANUAL_FINGERPRINTS if fingerprints.get(name) != MANUAL_FINGERPRINTS[name])
        + sorted(manual - set(MANUAL_FINGERPRINTS)),
        "moved_not_in_a_partition": sorted(name for name in moved if name not in partitioned),
        "local_ci_affected_present": ["local_ci_affected"] if "local_ci_affected" in hooks else [],
    }


ROOT_MANIFESTS = ("uv.lock", "pyproject.toml")
# A path that each commit-stage hook must select at commit: its own script and a file of each kind it reads, so that a staged change to one of them never
# skips the check (and the hosted ``--all-files`` run, whose hook skips itself when no tracked file matches, finds at least one tracked file).
COMMIT_PATH_INPUTS = {
    "gitleaks-outgoing": ["scripts/quality/scan_secrets.py", "config/quality_tools.json", "services/model_api/Dockerfile", "README.md"],
    "env-example": [".env.example", "config/quality/env_names.json", "scripts/quality/repo_checks.py", "infra/services/main.tf", "docker-compose.yaml"],
    "unused-code": ["scripts/quality/repo_checks.py", "scripts/quality/vulture_whitelist.py", ".cleanup_allowlist", "uv.lock", "pyproject.toml"],
    "unused-dependencies": [
        "scripts/quality/repo_checks.py",
        "scripts/quality/lock_env.py",
        ".cleanup_allowlist",
        "uv.lock",
        "pyproject.toml",
        "config/quality/dependency_runtimes.json",
        "config/quality_tools.json",
    ],
    "orphan-files": ["README.md", "scripts/quality/repo_checks.py", "config/quality/env_names.json", "services/model_api/Dockerfile", ".cleanup_allowlist"],
    "terraform-unused": [
        "infra/iam/main.tf",
        "infra/iam/.tflint.hcl",
        ".tflint.hcl",
        ".cleanup_allowlist",
        "scripts/quality/repo_checks.py",
        "config/quality_tools.json",
    ],
    "build-contexts": [
        "services/model_api/Dockerfile",
        "services/lakehouse/spark/Dockerfile",
        "services/model_api/Dockerfile.",
        "services/model_api/Dockerfile.prod",
        "docker-compose.yaml",
        "infra/services/images.json",
        "scripts/quality/lint_sources.py",
    ],
    "documentation-review-untracked": [".documentation_review.json", ".gitignore", "scripts/quality/documentation_review.py"],
    "dependency-lock": ["pyproject.toml", "uv.lock", "uv.toml", "scripts/quality/lock_env.py", "config/quality_tools.json"],
    "sqlfluff": [
        "dbt/models/gold/dimensions/dim_county.sql",
        "dbt/macros/columns.sql",
        "dbt/profiles.yml",
        "dbt/dbt_project.yml",
        "dbt/packages.yml",
        "dbt/models/gold/dimensions/_dimensions__models.yml",
        "services/analytics/resources.sql",
        ".sqlfluff",
        "dbt/models/.sqlfluff",
        "uv.lock",
        "scripts/quality/lint_sources.py",
        "scripts/quality/install_sqlfluff.py",
    ],
    "model-metadata": [
        "dbt/models/gold/dimensions/dim_county.sql",
        "dbt/models/gold/dimensions/_dimensions__models.yml",
        "dbt/dbt_project.yml",
        "dbt/seeds/acs_variable_map.csv",
        "data_contracts/lakehouse/gold_contract.json",
        "data_contracts/lakehouse/silver_publish.json",
        "data_contracts/lakehouse/silver_columns.json",
        "config/lakehouse/bronze_tables.json",
        "scripts/process.py",
        "scripts/quality/model_metadata.py",
        "uv.lock",
        "pyproject.toml",
    ],
}
# Staged paths that a hook never reads, so they must not select it.
COMMIT_PATH_NON_INPUTS = {
    "sqlfluff": ["dbt/models/features/_feature_inputs.json", "dbt/seeds/acs_variable_map.csv"],
    "model-metadata": ["dbt/models/features/_feature_inputs.json"],
}
# The hooks whose selection is "any staged path", which includes a symlink: pre-commit's default ``types: [file]`` leaves a symlink out.
ANY_PATH_HOOKS = ("gitleaks-outgoing", "env-example", "orphan-files")


def hook_path_cases(document: dict[str, Any]) -> list[dict[str, Any]]:
    """The root dependency manifest and lock select every hook that depends on the locked environment."""
    hooks = hook_table(document)
    wanted = ("model-metadata", "dependency-lock", "test-lock", "osv-scanner")
    cases = []
    for name in wanted:
        pattern = re.compile(str(hooks[name]["files"])) if name in hooks and hooks[name].get("files") else None
        missed = [path for path in ROOT_MANIFESTS if pattern is None or not pattern.search(path)]
        if name == "osv-scanner":
            missed = [path for path in missed if path != "pyproject.toml"]
        cases.append({"case": f"hook_{name}_selects_the_lock_and_manifest", "passed": not missed, "missed": missed})
    for name, paths in COMMIT_PATH_INPUTS.items():
        found = hooks.get(name, {})
        include, exclude = re.compile(str(found.get("files") or "")), re.compile(str(found.get("exclude") or "^$"))
        missed = [path for path in paths if not include.search(path) or exclude.search(path)]
        cases.append({"case": f"commit_hook_{name}_selects_its_inputs", "passed": not missed, "missed": missed})
    for name, paths in COMMIT_PATH_NON_INPUTS.items():
        found = hooks.get(name, {})
        include, exclude = re.compile(str(found.get("files") or "")), re.compile(str(found.get("exclude") or "^$"))
        extra = [path for path in paths if include.search(path) and not exclude.search(path)]
        cases.append({"case": f"commit_hook_{name}_skips_files_it_never_reads", "passed": not extra, "selected": extra})
    for name in ANY_PATH_HOOKS:
        # The tags identify gives a symlink are {"symlink"} alone: a hook selects it only when every ``types`` entry is among them and ``types_or`` names one.
        found, tags = hooks.get(name, {}), {"symlink"}
        selected = set(found.get("types", ["file"])) <= tags and (not found.get("types_or") or bool(tags & set(found["types_or"])))
        cases.append({"case": f"commit_hook_{name}_selects_a_symlink", "passed": selected})
    return cases


def fast_hook_cases(scratch: Path) -> list[dict[str, Any]]:
    """The commit stage holds only the fast hooks, the push stage holds the shim check, the manual stage is what hosted CI runs."""
    document = load_yaml((ROOT / ".pre-commit-config.yaml").read_text())
    config = json.loads((ROOT / "config/quality/ci_groups.json").read_text())
    if not isinstance(document, dict):
        raise ValueError(".pre-commit-config.yaml must contain a mapping")
    partitions = config["hook_groups"]
    real = hook_config_problems(document, partitions, config.get("hooks_run_by_groups", {}))
    names = {
        "commit_stage_holds_only_fast_hooks": ("heavy_in_commit_stage", "fast_missing_from_commit_stage", "local_ci_affected_present"),
        "commit_path_hooks_run_only_on_their_files": ("commit_path_hooks_always_run",),
        "pre_push_hooks_declared": ("install_types", "push_hooks_not_in_push_stage"),
        "manual_stage_unchanged": ("manual_changed",),
        "moved_hooks_are_in_a_hosted_partition": ("moved_not_in_a_partition",),
    }
    results = [{"case": case, "passed": not any(real[key] for key in keys), **{key: real[key] for key in keys}} for case, keys in names.items()]
    results += hook_path_cases(document)

    def planted(label: str, mutate: Callable[[dict[str, Any], dict[str, list[str]]], None], key: str, case: str) -> dict[str, Any]:
        damaged = copy.deepcopy(document)
        damaged_partitions = copy.deepcopy(partitions)
        mutate(damaged, damaged_partitions)
        found = hook_config_problems(damaged, damaged_partitions, config.get("hooks_run_by_groups", {}))
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

    def always_run(document: dict[str, Any], _: dict[str, list[str]]) -> None:
        hook_table(document)["orphan-files"]["always_run"] = True

    def unpartition(_: dict[str, Any], partitions: dict[str, list[str]]) -> None:
        partitions["orphans"].remove("orphan-files")

    results += [
        planted("heavy_hook_in_commit_stage", add_stage("test-checks", "pre-commit"), "heavy_in_commit_stage", "commit_stage_holds_only_fast_hooks"),
        planted("commit_path_hook_always_runs", always_run, "commit_path_hooks_always_run", "commit_path_hooks_run_only_on_their_files"),
        planted(
            "privacy_gate_dropped_from_commit_stage",
            drop_stage("privacy-scan", "pre-commit"),
            "fast_missing_from_commit_stage",
            "commit_stage_holds_only_fast_hooks",
        ),
        planted("pre_push_install_type_missing", drop_install_type, "install_types", "pre_push_hooks_declared"),
        planted("moved_hook_loses_manual_stage", drop_stage("orphan-files", "manual"), "manual_changed", "manual_stage_unchanged"),
        planted("manual_hook_entry_edited", edit_entry, "manual_changed", "manual_stage_unchanged"),
        planted("moved_hook_in_no_partition", unpartition, "moved_not_in_a_partition", "moved_hooks_are_in_a_hosted_partition"),
    ]
    return results


INHERITED_PATTERN = "^(CI_|RANGE_|LAUNCHER_|FEATURE_|ACQUISITION_|_?PRE_COMMIT_|SKIP$|GITHUB_ACTIONS$|GIT_DIR$|GIT_WORK_TREE$|GIT_INDEX_FILE$)"
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
STUB_SCAN_PROGRAM = (
    "import json, os, re, sys, time\n"
    "seen = ','.join(sorted(name for name in os.environ if re.match(%r, name)))\n"
    "time.sleep(float(os.environ.get('STUB_SLEEP', '0')))\n"
    "with open(os.environ['STUB_LOG'], 'a') as log:\n"
    "    log.write(json.dumps({'stub': 'secret_scan', 'argv': sys.argv[1:], 'env': {}, 'seen': seen}) + '\\n')\n"
    "sys.exit(int(os.environ.get('STUB_EXIT_SECRET_SCAN', '0')))\n"
)
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
) -> Path:
    """A small repository with the real selector configuration, a copy of the push gate and stand-ins for the commands a push runs.

    One topic commit per path in ``changed`` follows the base commit; ``mutate`` rewrites the copy of ``scripts/quality/pre_push.py`` to plant a defect.
    """
    root = scratch / name
    suffix = 1
    while root.exists() or remote_of(root).exists():
        suffix += 1
        root = scratch / f"{name}_{suffix}"
    for folder in ("config/quality", "scripts/quality", ".github/workflows"):
        (root / folder).mkdir(parents=True, exist_ok=True)
    (root / ".venv").symlink_to(PYTHON.parent.parent)
    config = json.loads((ROOT / "config/quality/ci_groups.json").read_text())
    if timeout is not None:
        config["pre_push"]["timeout_seconds"] = timeout
    (root / "config/quality/ci_groups.json").write_text(json.dumps(config))
    (root / "scripts/quality/ci_changed_files.py").write_text(STUB_LOG_PROGRAM % (INHERITED_PATTERN, "changed_files", "CHANGED_FILES"))
    (root / "scripts/quality/repo_checks.py").write_text(STUB_LOG_PROGRAM % (INHERITED_PATTERN, "repo_checks", "REPO_CHECKS"))
    (root / "scripts/quality/scan_secrets.py").write_text(STUB_SCAN_PROGRAM % INHERITED_PATTERN)
    (root / "scripts/quality/lint_tools.py").write_text(STUB_LOG_PROGRAM % (INHERITED_PATTERN, "osv_scanner", "OSV_SCANNER"))
    for checked in ("check_head", "check_tools"):  # The worktree checks have their own E2E; the push gate cases only need them to pass.
        (root / f"scripts/quality/{checked}.py").write_text("raise SystemExit(0)\n")
    for package in ("scripts", "scripts/quality"):
        (root / package / "__init__.py").write_text("")
    shutil.copy2(ROOT / "scripts/process.py", root / "scripts/process.py")
    source = (ROOT / "scripts/quality/pre_push.py").read_text()
    (root / "scripts/quality/pre_push.py").write_text(mutate(source) if mutate else source)
    (root / ".pre-commit-config.yaml").write_text((ROOT / ".pre-commit-config.yaml").read_text())
    (root / "README.md").write_text("base\n")
    probe_git(root, "init", "--quiet", "--initial-branch", "main")
    probe_git(root, "add", "--all")
    probe_git(root, "-c", "commit.gpgsign=false", "commit", "--quiet", "--message", "base")
    probe_git(root, "init", "--bare", "--quiet", str(remote_of(root)))
    probe_git(root, "remote", "add", "origin", str(remote_of(root)))
    if remote:
        probe_git(root, "push", "--quiet", "origin", "main")
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


GATE_STEPS = ["commit_messages", "removed_names", "secret_scan", "privacy_scan", "secret_scan_pushed", "privacy_scan_pushed", "osv_scanner"]


def gate_plan_case(scratch: Path, mutate: Callable[[str], str] | None = None) -> dict[str, Any]:
    """The push gate plan: changed-file checks first, then commit messages, removed names, both scans and OSV over the range, no step of the old gate."""
    root = pre_push_repo(scratch, "gate_plan", "README.md", mutate=mutate)
    plan = pre_push_plan(root)
    config = json.loads((root / "config/quality/ci_groups.json").read_text())
    names = [step["name"] for step in plan["steps"]]
    steps = {step["name"]: step for step in plan["steps"]}
    base, head = probe_git(root, "rev-parse", "origin/main"), probe_git(root, "rev-parse", "HEAD")
    first = plan.get("first") or {}
    checks = {
        "step_names": names == GATE_STEPS,
        "changed_file_checks_first": first.get("name") == "changed_file_checks" and first.get("argv", [])[-4:] == ["--from", base, "--to", head],
        "no_step_of_the_removed_gate": not any(name in ("local_ci", "acquisition") or name.startswith("hooks_") for name in names),
        "secret_scan": steps.get("secret_scan", {}).get("argv", [])[1:] == ["-m", "scripts.quality.scan_secrets"],
        "privacy_scan": steps.get("privacy_scan", {}).get("argv", [])[1:] == ["-m", "scripts.quality.repo_checks", "privacy-scan"],
        "pushed_commits_scanned": steps.get("secret_scan_pushed", {}).get("argv", [])[3:6] == ["--tip", head, "--exclude-file"]
        and steps.get("privacy_scan_pushed", {}).get("argv", [])[3:7] == ["privacy-scan", "--tip", head, "--exclude-file"],
        "the_destination_is_listed": base in plan.get("destination", []),
        "range": plan["start"] == base and plan["end"] == head,
        "commit_messages": steps.get("commit_messages", {}).get("argv", [])[-3:] == ["commit-range", base, head],
        "removed_names": steps.get("removed_names", {}).get("env") == {"PRE_COMMIT_FROM_REF": base, "PRE_COMMIT_TO_REF": head}
        and steps.get("removed_names", {}).get("argv", [])[-1:] == ["removed-names"],
        "osv_range": steps.get("osv_scanner", {}).get("argv", [])[-6:] == ["--hook-stage", "pre-push", "--from-ref", base, "--to-ref", head],
        "every_step_has_the_configured_deadline": bool(names)
        and all(step["timeout_seconds"] == config["pre_push"]["timeout_seconds"] for step in plan["steps"]),
    }
    return {"case": "pre_push_plan_runs_the_two_scans_and_the_range_checks", "passed": all(checks.values()), "checks": checks, "steps": names}


def gate_run_case(scratch: Path, mutate: Callable[[str], str] | None = None) -> dict[str, Any]:
    """A run starts the changed-file checks first, then runs each step once, and calls nothing else."""
    root = pre_push_repo(scratch, "gate_run", "README.md", mutate=mutate)
    code, out, error = pre_push(root, "run", timeout=300)
    calls = pre_push_calls(root)
    repo_checks = sorted(call["argv"][0] for call in calls if call["stub"] == "repo_checks")
    checks = {
        "exit": code == 0,
        "changed_files_first": first_stub(calls) == "changed_files",
        "each_step_ran_once": sum(1 for call in calls if call["stub"] == "secret_scan") == 2
        and repo_checks == ["commit-range", "privacy-scan", "privacy-scan", "removed-names"],
        "nothing_else_ran": sorted({call["stub"] for call in calls}) == ["changed_files", "repo_checks", "secret_scan"],
        "report_rows": [row["name"] for row in latest_report(root).get("results", [])] == GATE_STEPS,
    }
    return {
        "case": "pre_push_run_calls_each_gate_step_once",
        "passed": all(checks.values()),
        "checks": checks,
        "calls": [call["stub"] for call in calls],
        "output": (out + error)[-300:],
    }


def gate_scan_failure_case(scratch: Path) -> dict[str, Any]:
    """A failed secret scan of the pushed commits fails the gate by name and does not stop the other steps."""
    root = pre_push_repo(scratch, "gate_scan_failure", "README.md")
    code, out, error = pre_push(root, "run", extra={"STUB_EXIT_SECRET_SCAN": "3"}, timeout=300)
    repo_checks = sorted(call["argv"][0] for call in pre_push_calls(root) if call["stub"] == "repo_checks")
    failed_line = next((line for line in out.splitlines() if line.startswith("pre-push: failed steps:")), "")
    checks = {
        "exit": code != 0,
        "names_the_step": "FAIL secret_scan " in out and "FAIL secret_scan_pushed" in out and "secret_scan_pushed" in failed_line,
        "other_steps_ran": repo_checks == ["commit-range", "privacy-scan", "privacy-scan", "removed-names"],
    }
    return {"case": "pre_push_secret_scan_failure_is_reported", "passed": all(checks.values()), "checks": checks, "output": (out + error)[-300:]}


def gate_no_range_case(scratch: Path) -> dict[str, Any]:
    """With no merge base there is no range: the checks that read one are left out; the scans and OSV run over every file and every commit."""
    root = pre_push_repo(scratch, "gate_no_range", "README.md", remote=False)
    plan = pre_push_plan(root)
    names = [step["name"] for step in plan["steps"]]
    osv: dict[str, Any] = next((step for step in plan["steps"] if step["name"] == "osv_scanner"), {})
    code, out, error = pre_push(root, "run", timeout=300)
    checks = {
        "step_names": names == ["secret_scan", "privacy_scan", "secret_scan_pushed", "privacy_scan_pushed", "osv_scanner"] and plan["first"] is None,
        "osv_over_every_file": osv.get("argv", [])[-1:] == ["--all-files"],
        "reason_names_the_base": "origin/main" in plan["reason"],
        "exit": code == 0 and "changed_files" not in [call["stub"] for call in pre_push_calls(root)],
    }
    return {"case": "pre_push_without_a_range_runs_the_scans_over_every_file", "passed": all(checks.values()), "checks": checks, "reason": plan["reason"]}


PLANTED_SECRET = "ghp_" + "".join(chr(97 + (index * 7) % 26) if index % 2 else str(index % 10) for index in range(36))
PLANTED_PERSON = "clinic.manager" + "@" + "riverside-health.org"
PLANTED_TEXT = f"token = {PLANTED_SECRET}\ncontact = {PLANTED_PERSON}\n"
# Secrets that the bundled Gitleaks rules catch only by the path or the file kind (a Terraform password, a Kubernetes Secret), and a lock file they allow.
PATH_SECRETS = {
    "main.tf": 'resource "azurerm_sql_server" "x" {\n  administrator_login_password = "' + "aaaabbbb" + "ccccdddd" + '"\n}\n',
    "secret.yaml": "apiVersion: v1\nkind: Secret\nmetadata:\n  name: app\ndata:\n  token: " + base64.b64encode(b"password-password-password").decode() + "\n",
}
ALLOWED_LOCK = '{\n  "api_key": "' + "d41d8cd98f00b204e9800998ecf8427e1a2b3c4d5e6f" + '"\n}\n'
PEM_BODY = "\n".join(base64.b64encode(bytes((index * 7 + offset) % 256 for offset in range(48))).decode() for index in range(25))
PEM_IN_A_LARGE_FILE = (
    "\n".join(f"filler line {index}" for index in range(300))
    + "\n-----BEGIN RSA PRIVATE KEY-----\n"
    + PEM_BODY
    + "\n-----END RSA PRIVATE KEY-----\n"
    + "\n".join(f"tail line {index}" for index in range(300))
    + "\n"
)


def commit_all(root: Path, message: str) -> str:
    probe_git(root, "add", "--all")
    probe_git(root, "-c", "commit.gpgsign=false", "commit", "--quiet", "--allow-empty", "--message", message)
    return probe_git(root, "rev-parse", "HEAD")


def scan_fixture(scratch: Path, name: str) -> tuple[Path, str]:
    """A repository with one clean base commit, which a destination is taken to hold."""
    root = scratch / name
    root.mkdir(parents=True)
    probe_git(root, "init", "--quiet", "--initial-branch", "main")
    (root / "README.md").write_text("base\n")
    return root, commit_all(root, "base")


def failing_git(scratch: Path, name: str, trigger: str, partial: str = "", message: str = "") -> Path:
    """A folder with a ``git`` stand-in that fails for a call with the argument ``trigger`` and runs the real Git otherwise; put it first on PATH.

    With ``partial`` it prints that text on standard output first, as a Git that dies after some output; ``message`` is what it writes to standard error.
    """
    folder = scratch / f"failing_git_{name}"
    folder.mkdir(parents=True)
    real = shutil.which("git") or "git"
    program = folder / "git"
    lines = [
        "#!/bin/bash",
        'for argument in "$@"; do',
        f'  if [[ "$argument" == "{trigger}" ]]; then',
        *([f'    echo "{partial}"'] if partial else []),
        f'    echo "{message or f"planted failure of git {trigger}"}" >&2',
        "    exit 128",
        "  fi",
        "done",
        f'exec {real} "$@"',
    ]
    program.write_text("\n".join(lines) + "\n")
    program.chmod(0o755)
    return folder


def pushed_scans(
    tools: Path,
    root: Path,
    tips: tuple[str, ...],
    excluded: tuple[str, ...] = (),
    tags: tuple[str, ...] = (),
    path_first: Path | None = None,
    extra: dict[str, str] | None = None,
) -> dict[str, tuple[int, str]]:
    """Run the two pushed scans of the push gate from ``tools`` over a fixture repository; ``excluded`` are the objects the destination already holds."""
    listing = root.parent / f"{root.name}_destination.txt"
    listing.write_text("".join(f"{identifier}\n" for identifier in excluded))
    arguments = [part for tip in tips for part in ("--tip", tip)] + ["--exclude-file", str(listing)] + [part for tag in tags for part in ("--tag-object", tag)]
    environment = {**os.environ, "PYTHONPATH": str(tools), **(extra or {})}
    if path_first:
        environment["PATH"] = f"{path_first}:{environment['PATH']}"
    secret = run_command(str(PYTHON), ["-m", "scripts.quality.scan_secrets", "--repository", str(root), *arguments], cwd=tools, env=environment, timeout=300)
    privacy = run_command(str(PYTHON), ["-m", "scripts.quality.repo_checks", "privacy-scan", *arguments], cwd=root, env=environment, timeout=300)
    return {"secret": (secret.returncode, secret.stdout + secret.stderr), "privacy": (privacy.returncode, privacy.stdout + privacy.stderr)}


def leaks_nothing(*outputs: str) -> bool:
    return not any(value in text for text in outputs for value in (PLANTED_SECRET, PLANTED_PERSON))


def both_fail_naming(scans: dict[str, tuple[int, str]], commit: str) -> dict[str, bool]:
    return {
        "secret_scan_fails_and_names_the_commit": scans["secret"][0] == 1 and commit[:12] in scans["secret"][1],
        "privacy_scan_fails_and_names_the_commit": scans["privacy"][0] == 1 and commit[:12] in scans["privacy"][1],
        "no_value_is_printed": leaks_nothing(scans["secret"][1], scans["privacy"][1]),
    }


def fake_scan_tools(scratch: Path, name: str, tools: Path, program: str) -> Path:
    """A copy of ``scan_secrets`` whose Gitleaks is ``program`` (a shell script): an answer or a hang that a real one would not give."""
    fake = scratch / f"fake_tools_{name}"
    for relative in ("scripts/__init__.py", "scripts/process.py", "scripts/quality/__init__.py", "scripts/quality/scan_secrets.py"):
        (fake / relative).parent.mkdir(parents=True, exist_ok=True)
        if (tools / relative).exists():
            shutil.copy2(tools / relative, fake / relative)
        else:
            (fake / relative).write_text("")
    (fake / ".tools/bin").mkdir(parents=True)
    (fake / ".tools/bin/gitleaks").write_text(f"#!/bin/bash\n{program}\n")
    (fake / ".tools/bin/gitleaks").chmod(0o755)
    return fake


def fake_scan(scratch: Path, name: str, fake: Path, root: Path, tip: str, base: str, timeout: float = 60) -> tuple[int, bool, Path]:
    """Run the secret scan of a push from ``fake`` with its own temporary folder: its exit status, whether it was stopped, and that folder."""
    temporary = scratch / f"fake_scan_tmp_{name}"
    temporary.mkdir()
    listing = scratch / f"fake_scan_destination_{name}.txt"
    listing.write_text(f"{base}\n")
    try:
        result = run_command(
            str(PYTHON),
            ["-m", "scripts.quality.scan_secrets", "--repository", str(root), "--tip", tip, "--exclude-file", str(listing)],
            cwd=fake,
            env={**os.environ, "TMPDIR": str(temporary)},
            timeout=timeout,
        )
    except TimeoutExpired:
        return -1, True, temporary
    return result.returncode, False, temporary


def killed_scan_checks(scratch: Path, tools: Path, root: Path, tip: str, base: str) -> dict[str, bool]:
    """Stop the secret scan while Gitleaks runs (a stand-in that never ends) and look for the pushed text in the temporary folder it used."""
    fake = fake_scan_tools(scratch, "hang", tools, "sleep 300")
    _, stopped, temporary = fake_scan(scratch, "hang", fake, root, tip, base, timeout=8)
    left = [path for path in temporary.rglob("*") if path.is_file() and PLANTED_SECRET in path.read_text(errors="replace")]
    return {"the_scan_was_stopped_while_gitleaks_ran": stopped, "no_file_holds_the_pushed_text": not left}


def pushed_scan_cases(scratch: Path, tools: Path = ROOT) -> list[dict[str, Any]]:
    """The scans of a push read what it newly sends: every commit message and patch (a merge against its first parent) and every tag message."""
    results: list[dict[str, Any]] = []

    def record(case: str, checks: dict[str, bool]) -> None:
        results.append({"case": case, "passed": all(checks.values()), "checks": checks})

    root, base = scan_fixture(scratch, "scan_two_commits")
    (root / "notes.txt").write_text(PLANTED_TEXT)
    added = commit_all(root, "add notes")
    (root / "notes.txt").unlink()
    removed = commit_all(root, "remove notes")
    scans = pushed_scans(tools, root, (removed,), (base,))
    record(
        "pushed_scans_find_a_value_that_a_later_commit_removes",
        both_fail_naming(scans, added) | {"the_removing_commit_is_not_named": removed[:12] not in scans["secret"][1]},
    )
    root, base = scan_fixture(scratch, "scan_merge")
    probe_git(root, "checkout", "--quiet", "-b", "left")
    (root / "shared.txt").write_text("left\n")
    commit_all(root, "left")
    probe_git(root, "checkout", "--quiet", "-b", "right", base)
    (root / "shared.txt").write_text("right\n")
    commit_all(root, "right")
    with contextlib.suppress(RuntimeError):  # The two sides edit the same file, so the merge stops with a conflict that the next lines resolve.
        probe_git(root, "-c", "commit.gpgsign=false", "merge", "--no-commit", "--no-ff", "left")
    (root / "shared.txt").write_text(PLANTED_TEXT)
    probe_git(root, "add", "--all")
    probe_git(root, "-c", "commit.gpgsign=false", "commit", "--quiet", "--no-edit", "--message", "merge left")
    merge = probe_git(root, "rev-parse", "HEAD")
    record("pushed_scans_find_a_value_that_only_a_merge_introduces", both_fail_naming(pushed_scans(tools, root, (merge,), (base,)), merge))
    root, base = scan_fixture(scratch, "scan_message")
    message = commit_all(root, f"note {PLANTED_SECRET} for {PLANTED_PERSON}")
    record("pushed_scans_read_the_commit_message", both_fail_naming(pushed_scans(tools, root, (message,), (base,)), message))
    root, base = scan_fixture(scratch, "scan_path")
    (root / f'my "notes" {PLANTED_PERSON}.txt').write_text(PLANTED_TEXT)
    named = commit_all(root, "add a file with an unusual name")
    scans = pushed_scans(tools, root, (named,), (base,))
    record(
        "pushed_scans_read_an_unusual_file_name_and_print_no_name",
        both_fail_naming(scans, named) | {"the_path_is_redacted": 'my "notes"' not in scans["privacy"][1] and "notes" not in scans["privacy"][1]},
    )
    root, base = scan_fixture(scratch, "scan_tag_chain")
    inner_text = f"inner tag note {PLANTED_SECRET} {PLANTED_PERSON}"
    probe_git(root, "tag", "--annotate", "--message", inner_text, "inner", base)
    inner = probe_git(root, "rev-parse", "inner")
    probe_git(root, "tag", "--annotate", "--message", "outer tag is clean", "outer", inner)
    outer = probe_git(root, "rev-parse", "outer")
    scans = pushed_scans(tools, root, (), (), (outer, inner))
    clean = pushed_scans(tools, root, (), (), (outer,))
    record(
        "pushed_scans_read_every_tag_message_of_a_chain",
        {
            "secret_scan_fails": scans["secret"][0] == 1,
            "privacy_scan_fails_and_names_the_tag": scans["privacy"][0] == 1 and inner[:12] in scans["privacy"][1],
            "the_clean_outer_tag_alone_passes": clean["secret"][0] == 0 and clean["privacy"][0] == 0,
            "no_value_is_printed": leaks_nothing(scans["secret"][1], scans["privacy"][1]),
        },
    )
    root, base = scan_fixture(scratch, "scan_destination")
    (root / "held.txt").write_text(PLANTED_TEXT)
    held = commit_all(root, "already at the destination")
    (root / "fresh.txt").write_text("fine\n")
    fresh = commit_all(root, "new")
    sent = pushed_scans(tools, root, (fresh,), (base, held))
    record(
        "pushed_scans_skip_what_the_destination_holds",
        {
            "secret_scan_passes": sent["secret"][0] == 0,
            "privacy_scan_passes": sent["privacy"][0] == 0,
            "an_empty_destination_sends_it_all": pushed_scans(tools, root, (fresh,))["secret"][0] == 1,
        },
    )
    root, base = scan_fixture(scratch, "scan_clean")
    (root / "clean.txt").write_text("fine\n")
    clean_commit = commit_all(root, "clean")
    scans = pushed_scans(tools, root, (clean_commit,), (base,))
    record("pushed_scans_pass_a_clean_push", {"secret_scan_passes": scans["secret"][0] == 0, "privacy_scan_passes": scans["privacy"][0] == 0})
    root, base = scan_fixture(scratch, "scan_partial_patch")
    (root / "clean.txt").write_text("fine\n")
    tip = commit_all(root, "clean")
    partial = failing_git(scratch, "partial_patch", "-p", partial="partial patch text")
    scans = pushed_scans(tools, root, (tip,), (base,), (), partial)
    record(
        "pushed_scans_fail_closed_when_git_dies_after_partial_patch_output",
        {f"{scanner}_scan_fails_and_names_the_command": scans[scanner][0] not in (0, 1) and "git log" in scans[scanner][1] for scanner in scans},
    )
    root = scratch / "scan_root_hidden"
    root.mkdir()
    probe_git(root, "init", "--quiet", "--initial-branch", "main")
    probe_git(root, "config", "log.showRoot", "false")
    (root / "first.txt").write_text(PLANTED_TEXT)
    first = commit_all(root, "first commit")
    (root / "second.txt").write_text("fine\n")
    second = commit_all(root, "second")
    record("pushed_scans_read_a_root_commit_that_log_showroot_hides", both_fail_naming(pushed_scans(tools, root, (second,)), first))
    root, base = scan_fixture(scratch, "scan_separator")
    (root / "notes.txt").write_text(f"note\u2028{PLANTED_PERSON}\n")
    separated = commit_all(root, "a line with a separator")
    scans = pushed_scans(tools, root, (separated,), (base,))
    record(
        "pushed_privacy_scan_reads_the_whole_line_after_a_unicode_separator",
        {
            "privacy_scan_fails_and_names_the_commit": scans["privacy"][0] == 1 and separated[:12] in scans["privacy"][1],
            "no_value_is_printed": leaks_nothing(scans["privacy"][1]),
        },
    )
    root, base = scan_fixture(scratch, "scan_extension")
    (root / f"report.{PLANTED_SECRET}").write_text(PLANTED_TEXT)
    odd = commit_all(root, "a file whose extension is the value")
    scans = pushed_scans(tools, root, (odd,), (base,))
    record(
        "pushed_scans_keep_no_unknown_extension_in_a_finding",
        both_fail_naming(scans, odd) | {"no_part_of_the_extension_is_printed": PLANTED_SECRET[4:8] not in scans["privacy"][1] + scans["secret"][1]},
    )
    for name, content in {**PATH_SECRETS, "keys/id.txt": PEM_IN_A_LARGE_FILE}.items():
        root, base = scan_fixture(scratch, f"scan_kind_{Path(name).suffix.strip('.') or 'file'}")
        (root / name).parent.mkdir(parents=True, exist_ok=True)
        (root / name).write_text(content)
        added_file = commit_all(root, f"add {Path(name).name}")
        scans = pushed_scans(tools, root, (added_file,), (base,))
        record(
            f"pushed_secret_scan_applies_the_bundled_rules_by_path_{Path(name).suffix.strip('.') or 'file'}",
            {"secret_scan_fails_and_names_the_commit": scans["secret"][0] == 1 and added_file[:12] in scans["secret"][1]},
        )
    root, base = scan_fixture(scratch, "scan_lock_file")
    (root / "package-lock.json").write_text(ALLOWED_LOCK)
    lock = commit_all(root, "add a lock file")
    record(
        "pushed_secret_scan_keeps_the_bundled_allowlist_for_a_lock_file", {"secret_scan_passes": pushed_scans(tools, root, (lock,), (base,))["secret"][0] == 0}
    )
    root, base = scan_fixture(scratch, "scan_killed")
    (root / "notes.txt").write_text(PLANTED_TEXT)
    tip = commit_all(root, f"add notes {PLANTED_SECRET}")
    record("pushed_secret_scan_leaves_no_pushed_text_behind_when_it_is_killed", killed_scan_checks(scratch, tools, root, tip, base))
    root, base = scan_fixture(scratch, "scan_carriage_return")
    (root / "notes.txt").write_bytes(f"note\r{PLANTED_PERSON}\n".encode())
    returned = commit_all(root, "a line with a bare carriage return")
    scans = pushed_scans(tools, root, (returned,), (base,))
    record(
        "pushed_privacy_scan_reads_the_text_after_a_bare_carriage_return",
        {
            "privacy_scan_fails_and_names_the_commit": scans["privacy"][0] == 1 and returned[:12] in scans["privacy"][1],
            "no_value_is_printed": leaks_nothing(scans["privacy"][1]),
        },
    )
    home = scratch / "colour_home"
    home.mkdir()
    (home / ".gitconfig").write_text("[color]\n\tui = always\n")
    root, base = scan_fixture(scratch, "scan_colour")
    (root / "notes.txt").write_text(PLANTED_TEXT)
    coloured = commit_all(root, "add notes")
    record(
        "pushed_scans_ignore_a_user_git_config_that_colours_the_output",
        both_fail_naming(pushed_scans(tools, root, (coloured,), (base,), extra={"HOME": str(home), "XDG_CONFIG_HOME": str(home)}), coloured),
    )
    root, base = scan_fixture(scratch, "scan_rename")
    (root / "notes.txt").write_text(PLANTED_TEXT)
    held = commit_all(root, "add notes")
    probe_git(root, "mv", "notes.txt", "moved.txt")
    renamed = commit_all(root, "rename notes")
    scans = pushed_scans(tools, root, (renamed,), (held,))
    record(
        "pushed_secret_scan_reads_a_pure_rename_as_an_added_file",
        {"secret_scan_fails_and_names_the_commit": scans["secret"][0] == 1 and renamed[:12] in scans["secret"][1]},
    )
    root, base = scan_fixture(scratch, "scan_sha_file")
    (root / base).write_text("an untracked file named like the excluded commit\n")
    (root / "notes.txt").write_text(PLANTED_TEXT)
    shaped = commit_all(root, "add notes")
    record("pushed_scans_read_the_scope_when_a_file_is_named_like_an_excluded_commit", both_fail_naming(pushed_scans(tools, root, (shaped,), (base,)), shaped))
    root, base = scan_fixture(scratch, "scan_silent_gitleaks")
    (root / "notes.txt").write_text("some added text\n")
    quiet = commit_all(root, "add notes")
    report = " ".join(
        [
            'report=""; while [ $# -gt 0 ]; do if [ "$1" = "--report-path" ]; then report="$2"; fi; shift; done;',
            'if [ -n "$report" ] && [ "$report" != "-" ]; then echo "[]" > "$report"; else echo "[]"; fi',
        ]
    )
    silent = fake_scan_tools(scratch, "silent", tools, 'echo "INF scanned ~0 bytes (0 bytes) in 1ms" >&2; ' + report)
    erring = fake_scan_tools(scratch, "erring", tools, 'echo "ERR stderr is not empty" >&2; echo "INF scanned ~999999 bytes (1 MB) in 1ms" >&2; ' + report)
    short_code, _, _ = fake_scan(scratch, "silent", silent, root, quiet, base)
    error_code, _, _ = fake_scan(scratch, "erring", erring, root, quiet, base)
    record(
        "pushed_secret_scan_fails_when_gitleaks_reads_less_than_git_gave_or_reports_an_error",
        {"zero_bytes_scanned_fails": short_code not in (0, 1), "an_error_line_fails": error_code not in (0, 1)},
    )
    root, base = scan_fixture(scratch, "scan_split_key")
    half = PEM_BODY.split("\n")
    (root / "key.txt").write_text("-----BEGIN RSA PRIVATE " + "KEY-----\n" + "\n".join(half[:12]) + "\n")
    commit_all(root, "first half of a key")
    (root / "key.txt").write_text("\n".join(half[12:]) + "\n-----END RSA PRIVATE " + "KEY-----\n")
    tail = commit_all(root, "second half of a key")
    (root / "key.txt").unlink()
    dropped_key = commit_all(root, "remove the key")
    del tail
    record(
        "known_limit_a_private_key_added_in_pieces_and_removed_in_the_same_push_is_not_found",
        {"secret_scan_misses_it_as_documented": pushed_scans(tools, root, (dropped_key,), (base,))["secret"][0] == 0},
    )
    root, base = scan_fixture(scratch, "scan_big_file_threshold")
    probe_git(root, "config", "core.bigFileThreshold", "128k")
    (root / "notes.txt").write_text("filler line\n" * 17000 + PLANTED_TEXT)
    big = commit_all(root, "add notes")
    (root / "notes.txt").unlink()
    gone = commit_all(root, "remove notes")
    record("pushed_scans_read_a_text_file_that_the_repository_calls_binary_by_size", both_fail_naming(pushed_scans(tools, root, (gone,), (base,)), big))
    root, base = scan_fixture(scratch, "scan_many_messages")
    stream = "".join(
        f"commit refs/heads/main\nmark :{number}\ncommitter A <a@example.org> {1700000000 + number} +0000\ndata {len(text.encode())}\n{text}\n"
        + (f"from {base}\n" if number == 1 else f"from :{number - 1}\n")
        for number in range(1, 655)
        for text in [f"note {number}" + (f" {PLANTED_SECRET}" if number == 327 else "")]
    )
    run_command("git", ["fast-import", "--quiet"], cwd=root, input_text=stream, timeout=120)
    many = probe_git(root, "rev-parse", "refs/heads/main")
    planted = probe_git(root, "rev-parse", "refs/heads/main~327")
    scans = pushed_scans(tools, root, (many,), (base,))
    record(
        "pushed_secret_scan_finds_one_token_among_654_short_messages",
        {
            "secret_scan_fails_and_names_the_commit": scans["secret"][0] == 1 and planted[:12] in scans["secret"][1],
            "no_value_is_printed": leaks_nothing(scans["secret"][1]),
        },
    )
    root, base = scan_fixture(scratch, "scan_no_final_newline")
    (root / "short.txt").write_bytes(b"abcde")
    short = commit_all(root, "a five-byte line without a final newline")
    record(
        "pushed_secret_scan_passes_an_added_line_without_a_final_newline",
        {"secret_scan_passes": pushed_scans(tools, root, (short,), (base,))["secret"][0] == 0},
    )
    root, base = scan_fixture(scratch, "scan_not_utf8")
    (root / "latin.txt").write_bytes(b"line \xe9 one\r\nsecond line\n")
    latin = commit_all(root, "a file that is not UTF-8")
    record("pushed_secret_scan_passes_added_lines_that_are_not_utf8", {"secret_scan_passes": pushed_scans(tools, root, (latin,), (base,))["secret"][0] == 0})
    for subcommand in ("rev-list", "log", "cat-file"):
        root, base = scan_fixture(scratch, f"scan_failing_{subcommand}")
        (root / "clean.txt").write_text("fine\n")
        tip = commit_all(root, "clean")
        probe_git(root, "tag", "--annotate", "--message", "clean", "label", tip)
        tag = probe_git(root, "rev-parse", "label")
        scans = pushed_scans(tools, root, (tip,), (base,), (tag,), failing_git(scratch, subcommand, subcommand))
        record(
            f"pushed_scans_fail_closed_when_git_{subcommand}_fails",
            {
                f"{scanner}_scan_fails_and_names_the_command": scans[scanner][0] not in (0, 1) and f"git {subcommand}" in scans[scanner][1]
                for scanner in scans
                if subcommand != "rev-list" or scanner == "secret"
                if subcommand != "rev-list" or scanner == "secret"
            },
        )
    return results


def destination_file(root: Path) -> list[str]:
    """The object IDs the destination held, as the latest push gate run wrote them for its scans."""
    listings = sorted((root / "data/e2e/pre_push").glob("*/destination.txt"))
    return listings[-1].read_text().split() if listings else []


def pushed_calls(root: Path, stub: str) -> list[list[str]]:
    return [call["argv"] for call in pre_push_calls(root) if call["stub"] == stub and "--tip" in call["argv"]]


def stale_tracking_ref_case(scratch: Path) -> dict[str, Any]:
    """The scope comes from what the destination lists, not the tracking refs: a commit the remote holds behind a stale tracking ref is not sent again."""
    root = pre_push_repo(scratch, "gate_stale_tracking_ref", "README.md")
    base = probe_git(root, "rev-parse", "origin/main")
    clone = scratch / "gate_stale_clone"
    probe_git(scratch, "clone", "--quiet", "--branch", "main", str(remote_of(root)), str(clone))
    (clone / "elsewhere.txt").write_text("from another clone\n")
    probe_git(clone, "add", "--all")
    probe_git(clone, "-c", "commit.gpgsign=false", "commit", "--quiet", "--message", "from another clone")
    probe_git(clone, "push", "--quiet", "origin", "main")
    held = probe_git(clone, "rev-parse", "HEAD")
    probe_git(root, "fetch", "--quiet", "origin", "refs/heads/main:refs/held/main")
    probe_git(root, "update-ref", "refs/remotes/origin/main", base)
    probe_git(root, "checkout", "--quiet", "-B", "topic", "refs/held/main")
    (root / "README.md").write_text("topic\n")
    head = commit_all(root, "topic")
    plan = pre_push_plan(root)
    steps = {step["name"]: step for step in plan["steps"]}
    code, out, error = pre_push(root, "run", timeout=300)
    checks = {
        "the_destination_list_holds_the_remote_commit": held in plan.get("destination", []),
        "the_tracking_ref_is_stale": probe_git(root, "rev-parse", "refs/remotes/origin/main") == base,
        "the_scope_is_the_head_alone": steps.get("secret_scan_pushed", {}).get("argv", [])[3:5] == ["--tip", head],
        "the_run_passes_and_writes_the_list": code == 0 and held in destination_file(root),
    }
    return {
        "case": "pre_push_scope_follows_the_destination_not_a_stale_tracking_ref",
        "passed": all(checks.values()),
        "checks": checks,
        "output": (out + error)[-300:],
    }


def second_remote_case(scratch: Path, mutate: Callable[[str], str] | None = None) -> dict[str, Any]:
    """A branch and a lightweight tag that origin knows, pushed to a fresh second remote, are scanned there: nothing is known to that destination."""
    root = gate_repo(scratch, "gate_second_remote", mutate=mutate)
    probe_git(root, "checkout", "--quiet", "main")
    probe_git(root, "branch", "known", "main")
    probe_git(root, "tag", "light", "main")
    second = scratch / "gate_second_remote_second.git"
    probe_git(root, "init", "--bare", "--quiet", str(second))
    probe_git(root, "remote", "add", "second", str(second))
    main = probe_git(root, "rev-parse", "main")
    code, text = git_push(root, ("second", "known", "refs/tags/light"))
    secret, privacy = pushed_calls(root, "secret_scan"), [call for call in pushed_calls(root, "repo_checks") if call[:1] == ["privacy-scan"]]
    checks = {
        "pushed": code == 0 and {"refs/heads/known", "refs/tags/light"} <= {line.split()[-1] for line in probe_git(second, "show-ref").splitlines()},
        "both_scans_ran_over_the_commit": len(secret) == 1 and len(privacy) == 1 and main in secret[0],
        "the_destination_held_nothing": destination_file(root) == [],
    }
    return {"case": "pre_push_known_refs_pushed_to_a_fresh_second_remote_are_scanned", "passed": all(checks.values()), "checks": checks, "output": text[-300:]}


def direct_url_case(scratch: Path, mutate: Callable[[str], str] | None = None) -> dict[str, Any]:
    """A push to a URL, with no remote name, takes its scope from that URL and records what the URL lists."""
    root = gate_repo(scratch, "gate_direct_url", mutate=mutate)
    head = probe_git(root, "rev-parse", "HEAD")
    base = probe_git(root, "rev-parse", "origin/main")
    code, text = git_push(root, (str(remote_of(root)), "topic"))
    secret = pushed_calls(root, "secret_scan")
    checks = {
        "pushed": code == 0 and "refs/heads/topic" in remote_refs(root),
        "scanned_over_the_head": len(secret) == 1 and head in secret[0],
        "the_url_list_holds_the_base": base in destination_file(root),
    }
    return {"case": "pre_push_direct_url_push_takes_its_scope_from_the_url", "passed": all(checks.values()), "checks": checks, "output": text[-300:]}


def push_all_case(scratch: Path, mutate: Callable[[str], str] | None = None) -> dict[str, Any]:
    """``git push --all`` sends every local branch: each one that sends something is judged, and the head's commit is scanned once."""
    root = gate_repo(scratch, "gate_push_all", mutate=mutate)
    head = probe_git(root, "rev-parse", "HEAD")
    probe_git(root, "branch", "other", "topic")
    code, text = git_push(root, ("origin", "--all"))
    secret = pushed_calls(root, "secret_scan")
    checks = {
        "pushed": code == 0 and {"refs/heads/topic", "refs/heads/other"} <= remote_refs(root),
        "the_head_is_scanned_once": len(secret) == 1 and secret[0].count("--tip") == 1 and head in secret[0],
        "every_branch_is_decided": set(push_decisions(root)) == {"refs/heads/main", "refs/heads/topic", "refs/heads/other"} or code == 0,
    }
    return {"case": "pre_push_all_branches_are_judged_and_scanned", "passed": all(checks.values()), "checks": checks, "output": text[-300:]}


def tag_chain_case(scratch: Path, mutate: Callable[[str], str] | None = None) -> dict[str, Any]:
    """A tag that points to a tag has every tag message of the chain scanned."""
    root = gate_repo(scratch, "gate_tag_chain", mutate=mutate)
    probe_git(root, "tag", "--annotate", "--message", "inner", "inner", "HEAD")
    inner = probe_git(root, "rev-parse", "inner")
    probe_git(root, "tag", "--annotate", "--message", "outer", "outer", inner)
    outer = probe_git(root, "rev-parse", "outer")
    code, text = git_push(root, ("origin", "refs/tags/outer"))
    secret = [call for call in pre_push_calls(root) if call["stub"] == "secret_scan" and "--tag-object" in call["argv"]]
    checks = {
        "pushed": code == 0 and "refs/tags/outer" in remote_refs(root),
        "every_tag_of_the_chain_is_scanned": len(secret) == 1 and inner in secret[0]["argv"] and outer in secret[0]["argv"],
    }
    return {"case": "pre_push_tag_chain_has_every_message_scanned", "passed": all(checks.values()), "checks": checks, "output": text[-300:]}


HOOK_DRIVER = """
import sys
from pathlib import Path
from scripts.quality import pre_push
sys.exit(pre_push.run(Path.cwd(), '', '', sys.argv[1] + '\\n' if sys.argv[1] else '', 'origin', sys.argv[2]))
"""


def hook_input_run(root: Path, update: str, path_first: Path | None = None, url: str = "") -> tuple[int, str]:
    """Run the gate for one pushed reference the way the hook does, with a failing ``git`` first on PATH (Git puts its own folder first for a hook)."""
    environment = {**os.environ, "STUB_LOG": str(root / "stub.log")}
    if path_first:
        environment["PATH"] = f"{path_first}:{environment['PATH']}"
    result = run_command(str(PYTHON), ["-c", HOOK_DRIVER, update, url], cwd=root, env=environment, timeout=120)
    return result.returncode, result.stdout + result.stderr


def failing_git_gate_case(scratch: Path, trigger: str, named: str = "") -> dict[str, Any]:
    """A Git call that the scope, the destination listing or the tag lookup needs fails: the gate refuses and the message names the command."""
    named = named or trigger
    label = trigger.lstrip("-")
    root = gate_repo(scratch, f"gate_failing_{label}")
    probe_git(root, "tag", "--annotate", "--message", "label", "label", "HEAD")
    tag = probe_git(root, "rev-parse", "label")
    zero = "0" * len(tag)
    code, text = hook_input_run(root, f"refs/tags/label {tag} refs/tags/label {zero}", failing_git(scratch, f"gate_{label}", trigger))
    checks = {"refused": code != 0 and "refused" in text, "names_the_command": f"git {named}" in text, "no_check_ran": not pre_push_calls(root)}
    return {"case": f"pre_push_refuses_when_git_{label}_fails", "passed": all(checks.values()), "checks": checks, "output": text[-300:]}


def listing_failure_prints_no_value_case(scratch: Path) -> dict[str, Any]:
    """A failed Git call is reported as its command and exit status only: a value in its standard error never reaches the output."""
    root = gate_repo(scratch, "gate_failure_text")
    head = probe_git(root, "rev-parse", "HEAD")
    shim = failing_git(scratch, "gate_failure_text", "ls-remote", message=f"fatal: {PLANTED_SECRET} {PLANTED_PERSON}")
    code, text = hook_input_run(root, f"refs/heads/topic {head} refs/heads/topic {'0' * len(head)}", shim)
    checks = {"refused": code != 0 and "git ls-remote" in text and "128" in text, "no_value_is_printed": leaks_nothing(text)}
    return {"case": "pre_push_failed_git_call_prints_its_command_and_status_only", "passed": all(checks.values()), "checks": checks, "output": text[-300:]}


def delete_only_push_case(scratch: Path) -> dict[str, Any]:
    """A push that only deletes, or sends nothing, returns before the listing: a failing ``ls-remote`` cannot block it."""
    root = gate_repo(scratch, "gate_delete_only")
    head = probe_git(root, "rev-parse", "HEAD")
    shim = failing_git(scratch, "gate_delete_only", "ls-remote")
    code, text = hook_input_run(root, f"(delete) {'0' * len(head)} refs/heads/gone {head}", shim)
    empty_code, empty_text = hook_input_run(root, "", shim)
    checks = {
        "the_delete_is_allowed": code == 0 and "nothing to check" in text and "refused" not in text,
        "an_empty_push_is_allowed": empty_code == 0 and "refused" not in empty_text,
    }
    return {"case": "pre_push_delete_only_push_needs_no_listing", "passed": all(checks.values()), "checks": checks, "output": (text + empty_text)[-300:]}


def rewritten_url_case(scratch: Path) -> dict[str, Any]:
    """When ``ls-remote`` would rewrite the hook's URL (insteadOf) to another repository, nothing is excluded: it says nothing about the destination."""
    root = gate_repo(scratch, "gate_instead_of")
    head = probe_git(root, "rev-parse", "HEAD")
    elsewhere = scratch / "gate_instead_of_elsewhere.git"
    probe_git(root, "init", "--bare", "--quiet", str(elsewhere))
    probe_git(root, "push", "--quiet", "--no-verify", str(elsewhere), "main", "topic")
    destination = str(remote_of(root))
    probe_git(root, "config", f"url.{elsewhere}.insteadOf", destination)
    code, text = hook_input_run(root, f"refs/heads/topic {head} refs/heads/topic {'0' * len(head)}", None, destination)
    secret = pushed_calls(root, "secret_scan")
    checks = {
        "the_gate_passes": code == 0,
        "it_says_nothing_is_excluded": "rewritten" in text,
        "the_tip_is_scanned_although_the_other_repository_holds_it": len(secret) == 1 and head in secret[0],
        "the_exclusion_list_is_empty": destination_file(root) == [],
    }
    return {"case": "pre_push_rewritten_destination_url_excludes_nothing", "passed": all(checks.values()), "checks": checks, "output": text[-300:]}


def destination_cases(scratch: Path) -> list[dict[str, Any]]:
    return [
        stale_tracking_ref_case(scratch),
        second_remote_case(scratch),
        direct_url_case(scratch),
        push_all_case(scratch),
        tag_chain_case(scratch),
        failing_git_gate_case(scratch, "ls-remote"),
        failing_git_gate_case(scratch, "cat-file"),
        failing_git_gate_case(scratch, "--batch-all-objects", "cat-file"),
        listing_failure_prints_no_value_case(scratch),
        delete_only_push_case(scratch),
        rewritten_url_case(scratch),
    ]


def gate_deadline_case(scratch: Path) -> dict[str, Any]:
    """A step that passes its deadline fails visibly with its name, and the run does not wait for it."""
    root = pre_push_repo(scratch, "gate_deadline", "README.md", timeout=2)
    started = time.monotonic()
    code, out, error = pre_push(root, "run", extra={"STUB_SLEEP": "60"}, timeout=90)
    seconds = time.monotonic() - started
    return {
        "case": "pre_push_step_deadline_fails_visibly",
        "passed": code != 0 and "FAIL secret_scan" in out and "exceeded its deadline of 2 seconds" in out + error and seconds < 30,
        "exit": code,
        "seconds": round(seconds, 1),
        "output": (out + error)[-300:],
    }


def latest_report(root: Path) -> dict[str, Any]:
    reports = sorted((root / "data/e2e/pre_push").glob("*/report.json"))
    return json.loads(reports[-1].read_text()) if reports else {}


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
    environment = {key: value for key, value in os.environ.items() if key not in ("GITHUB_ACTIONS", "CI_LIST_ONLY")}
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
    """``run_ci.sh`` releases Docker once after a run on macOS, never changes its exit status, and skips on hosted CI, off macOS, in list-only and gates."""
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
            and "trap 'finish; end_unconfirmed; [[ \"${ci_finished:-0}\" == 1 ]] || exit 1' EXIT" in ci,
        }
    )
    return results


def pre_push_cases(work: Path, scratch: Path, gate_stage: bool = True) -> list[dict[str, Any]]:
    """The push gate plan and run (the two scans, the range checks and OSV, no step of the old gate), its deadlines, and the hook and tree cases."""
    results = [gate_plan_case(scratch), gate_run_case(scratch), gate_scan_failure_case(scratch), gate_no_range_case(scratch), gate_deadline_case(scratch)]
    results += pushed_scan_cases(scratch) + destination_cases(scratch) + run_ci_release_cases(scratch)
    results += [
        planted_case(
            "secret_scan_step_left_out",
            gate_run_case,
            scratch,
            mutate=source_edit('{"name": "secret_scan", "kind": "secret_scan", "arguments": [], "env": {}},', ""),
        ),
        planted_case(
            "pushed_commit_scans_left_out",
            gate_plan_case,
            scratch,
            mutate=source_edit(
                '    if tips:\n        steps.append({"name": "secret_scan_pushed"', '    if False:\n        steps.append({"name": "secret_scan_pushed"'
            ),
        ),
        planted_case(
            "tag_message_scan_skipped",
            annotated_tag_known_case,
            scratch,
            mutate=source_edit('tags = sorted({tag for row in sent for tag in tag_chain(root, row["sha"])} - set(destination))', "tags = []"),
        ),
        planted_case(
            "privacy_scan_step_left_out",
            gate_plan_case,
            scratch,
            mutate=source_edit('{"name": "privacy_scan", "kind": "repo_checks", "arguments": ["privacy-scan"], "env": {}},', ""),
        ),
    ]
    if gate_stage:
        results += gate_stage_cases(scratch)
    return results + hook_and_tree_cases(scratch) + control_scan_cases() + pre_commit_control_cases() + workflow_trigger_cases()


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
    """The changed-file checks run first, over the pushed range, and the other steps start after they passed."""
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
        and "secret_scan" in order,
        "exit": code,
        "order": order,
        "output": (out + error)[-300:],
    }


def changed_files_failure_case(scratch: Path, mutate: Callable[[str], str] | None = None) -> dict[str, Any]:
    """A failed changed-file check stops the gate before the other steps, and names itself."""
    root = pre_push_repo(scratch, "gate_changed_files_failure", ("README.md",), mutate=mutate)
    code, out, error = pre_push(root, "run", extra={"STUB_EXIT_CHANGED_FILES": "1"}, timeout=300)
    order = [call["stub"] for call in pre_push_calls(root)]
    return {
        "case": "pre_push_changed_file_failure_stops_before_the_other_steps",
        "passed": code != 0 and order == ["changed_files"] and "changed_file_checks" in out + error,
        "exit": code,
        "order": order,
        "output": (out + error)[-300:],
    }


def unreadable_status_case(scratch: Path) -> dict[str, Any]:
    """A tree whose state Git cannot read is refused: the checks could not describe the pushed commit."""
    root = pre_push_repo(scratch, "gate_unreadable_status")
    (root / ".git/index").write_bytes(b"not an index")
    code, out, error = pre_push(root, "run", timeout=120)
    return {
        "case": "pre_push_refuses_a_tree_whose_state_cannot_be_read",
        "passed": code != 0
        and not [call for call in pre_push_calls(root) if call["stub"] == "secret_scan"]
        and "No check ran" in out
        and "cannot be read" in out,
        "exit": code,
        "output": (out + error)[-300:],
    }


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
    mypy_files = [
        step["files"]
        for step in changed_files.steps(ROOT, ["scripts/x.py"], lambda listed: listed, lambda listed: ["scripts/y.py", "scripts/x.py"])
        if step["name"] == "mypy"
    ]
    results.append(
        {
            "case": "changed_file_checks_add_the_importers_to_mypy",
            "passed": mypy_files == [["scripts/x.py", "scripts/y.py"]],
            "mypy_files": mypy_files,
        }
    )
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


def osv_scanner_case(scratch: Path, name: str, changed: tuple[str, ...], want_run: bool, mutate: Callable[[str], str] | None = None) -> dict[str, Any]:
    """The OSV-Scanner hook runs over the pushed range in its own step, also for a small change, and skips a range without a requirements file."""
    root = pre_push_repo(scratch, f"gate_osv_{name}", changed, mutate=mutate)
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
    """The changed-file checks and the OSV-Scanner step of the push gate."""
    results = [changed_files_first_case(scratch), changed_files_failure_case(scratch), unreadable_status_case(scratch)]
    results += changed_file_check_cases(scratch)
    results += [
        osv_scanner_case(scratch, "runs_for_a_requirements_change", ("requirements.txt",), True),
        osv_scanner_case(scratch, "skips_a_range_without_requirements", ("README.md",), False),
    ]
    results += [
        planted_case("changed_file_checks_last", changed_files_first_case, scratch, mutate=source_edit('    first = chosen["first"]\n', "    first = None\n")),
        planted_case(
            "osv_scanner_step_left_out",
            osv_scanner_case,
            scratch,
            name="runs_for_a_requirements_change",
            changed=("requirements.txt",),
            want_run=True,
            mutate=source_edit('{"name": "osv_scanner", "kind": "pre_commit"', '{"name": "osv_scanner_left_out", "kind": "pre_commit"'),
        ),
        planted_case("changed_file_failure_ignored", changed_files_failure_case, scratch, mutate=source_edit('if first_result["exit"] != 0:', "if False:")),
    ]
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
    code, log = dry_run(work, out, label, {"CI_SCOPE": "full", "CI_PART": "launcher", **env}, prepare=stub_body, timing=LAUNCHER_STUB_TIMING)
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


GATE_GROUP_CASES = ("--launcher-shards-only", "--gate-stage-only")


def gate_group_cases(work: Path) -> list[dict[str, Any]]:
    """The gate stage and launcher shard cases are the check group gate_stage_checks: their own part and hosted job, not a command of the Core part."""
    path = "scripts/quality/pre_push.py"
    results = [
        selection_case(
            work, "gate_stage_part_selects_only_the_gate_stage_group", ["--scope", "paths", "--path", path, "--part", "gate_stage"], ["gate_stage_checks"]
        ),
        selection_case(
            work,
            "core_part_leaves_the_gate_stage_group_out",
            ["--scope", "paths", "--path", path, "--part", "core"],
            ["python_lint", "repository_code_checks", "configuration_checks"],
        ),
    ]
    body = (ROOT / GROUP_FILES / "gate_stage_checks.sh").read_text(encoding="utf-8")
    core = (ROOT / GROUP_FILES / "repository_code_checks.sh").read_text(encoding="utf-8")
    tooling = (ROOT / GROUP_FILES / "ci_tooling_checks.sh").read_text(encoding="utf-8")
    jobs = workflow_document()["jobs"]
    steps = [step.get("env", {}) for step in jobs["gate_stage"]["steps"] if step.get("env", {}).get("CI_PART") == "gate_stage"]
    results.append(
        {
            "case": "gate_stage_and_launcher_shard_cases_run_in_their_own_group_and_hosted_job",
            "passed": all(f"run_ci_groups_e2e {flag}" in body and f"run_ci_groups_e2e {flag}" not in core for flag in GATE_GROUP_CASES)
            and "run_ci_groups_e2e --selection-only" in tooling
            and "run_ci_groups_e2e" not in core
            and len(steps) == 1
            and jobs["gate_stage"]["name"] == "CI Gate Stage E2E"
            and "CI Gate Stage E2E" in json.loads((ROOT / "config/quality/ci_jobs.json").read_text(encoding="utf-8")),
        }
    )
    return results


def head_of(root: Path) -> str:
    return probe_git(root, "rev-parse", "HEAD")


def runs_of(root: Path, stub: str = "secret_scan") -> int:
    """The calls of a stand-in, not counting the scans of pushed commits and tags (they carry ``--tip`` or ``--tag-object``)."""
    return sum(1 for call in pre_push_calls(root) if call["stub"] == stub and not {"--tip", "--tag-object"} & set(call["argv"]))


def tag_scans_of(root: Path) -> int:
    """The scans of pushed tag objects the stand-ins saw: the secret scan and the privacy scan."""
    return sum(1 for call in pre_push_calls(root) if call["stub"] in {"secret_scan", "repo_checks"} and "--tag-object" in call["argv"])


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
    """An annotated tag on a commit that origin/main holds runs no checkout check, but its message is scanned by both scanners."""

    def check(root: Path, code: int, text: str) -> dict[str, bool]:
        return {
            "pushed": code == 0 and "refs/tags/ann" in remote_refs(root),
            "no_checkout_check_ran": runs_of(root) == 0 and runs_of(root, "changed_files") == 0,
            "the_tag_message_was_scanned_by_both": tag_scans_of(root) == 2,
            "recorded_as_known": push_decisions(root).get("refs/tags/ann") == "known",
        }

    return push_case(
        scratch,
        "pre_push_annotated_tag_at_origin_main_has_its_message_scanned",
        "gate_annotated_known",
        known_annotated_tag,
        ("origin", "refs/tags/ann"),
        check,
        mutate,
    )


def annotated_tag_head_case(scratch: Path, mutate: Callable[[str], str] | None = None) -> dict[str, Any]:
    def check(root: Path, code: int, text: str) -> dict[str, bool]:
        return {
            "pushed": code == 0 and "refs/tags/ann" in remote_refs(root),
            "checked_once": runs_of(root) == 1,
            "the_tag_message_was_scanned_by_both": tag_scans_of(root) == 2,
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


def temp_branch_refused_case(
    scratch: Path,
    case: str,
    name: str,
    setup: Callable[[Path], None],
    arguments: tuple[str, ...],
    refused_ref: str,
    mutate: Callable[[str], str] | None = None,
) -> dict[str, Any]:
    """A push that names a ``temp/`` branch as its local or its remote reference is refused whole: nothing is pushed and no check runs."""

    def check(root: Path, code: int, text: str) -> dict[str, bool]:
        return {
            "refused": code != 0 and "temp/" in text,
            "nothing_pushed": not any(ref.startswith("refs/heads/") and ref != "refs/heads/main" for ref in remote_refs(root)),
            "no_check_ran": runs_of(root) == 0,
            "names_the_reference": refused_ref in text,
        }

    return push_case(scratch, case, name, setup, arguments, check, mutate)


def temp_local_branch(root: Path) -> None:
    probe_git(root, "checkout", "--quiet", "-b", "temp/work")


def temp_branch_at_head_case(scratch: Path, mutate: Callable[[str], str] | None = None) -> dict[str, Any]:
    return temp_branch_refused_case(
        scratch, "pre_push_temp_branch_at_head_is_refused", "gate_temp_head", temp_local_branch, ("origin", "temp/work"), "refs/heads/temp/work", mutate
    )


def temp_local_name_case(scratch: Path, mutate: Callable[[str], str] | None = None) -> dict[str, Any]:
    return temp_branch_refused_case(
        scratch,
        "pre_push_temp_branch_pushed_under_another_remote_name_is_refused",
        "gate_temp_local_name",
        temp_local_branch,
        ("origin", "temp/work:refs/heads/feature"),
        "refs/heads/temp/work",
        mutate,
    )


def temp_remote_name_case(scratch: Path, mutate: Callable[[str], str] | None = None) -> dict[str, Any]:
    return temp_branch_refused_case(
        scratch,
        "pre_push_branch_pushed_to_a_temp_remote_name_is_refused",
        "gate_temp_remote_name",
        lambda root: None,
        ("origin", "topic:refs/heads/temp/other"),
        "refs/heads/temp/other",
        mutate,
    )


def temp_known_commit_case(scratch: Path, mutate: Callable[[str], str] | None = None) -> dict[str, Any]:
    def seed(root: Path) -> None:
        probe_git(root, "branch", "temp/known", "main")

    return temp_branch_refused_case(
        scratch, "pre_push_temp_branch_at_origin_main_is_refused", "gate_temp_known", seed, ("origin", "temp/known"), "refs/heads/temp/known", mutate
    )


def temp_head_alias_case(scratch: Path, mutate: Callable[[str], str] | None = None) -> dict[str, Any]:
    return temp_branch_refused_case(
        scratch,
        "pre_push_head_on_a_temp_branch_is_refused",
        "gate_temp_head_alias",
        temp_local_branch,
        ("origin", "HEAD:refs/heads/feature"),
        "refs/heads/temp/work",
        mutate,
    )


def temp_with_head_ref_case(scratch: Path, mutate: Callable[[str], str] | None = None) -> dict[str, Any]:
    def seed(root: Path) -> None:
        probe_git(root, "branch", "temp/work", "HEAD")

    return temp_branch_refused_case(
        scratch,
        "pre_push_temp_branch_with_the_head_ref_refuses_both",
        "gate_temp_with_head",
        seed,
        ("origin", "topic", "temp/work"),
        "refs/heads/temp/work",
        mutate,
    )


def temp_deletion_case(scratch: Path, mutate: Callable[[str], str] | None = None) -> dict[str, Any]:
    """Deleting a temp/ branch on the remote publishes nothing, so it stays allowed (the cleanup of a branch that was pushed before the rule)."""
    root = gate_repo(scratch, "gate_temp_deletion", mutate=mutate)
    probe_git(root, "push", "--quiet", "--no-verify", "origin", "main:refs/heads/temp/old")
    code, text = git_push(root, ("origin", ":temp/old"))
    checks = {"deletion_allowed": code == 0, "remote_branch_removed": "refs/heads/temp/old" not in remote_refs(root), "no_check_ran": runs_of(root) == 0}
    return {"case": "pre_push_temp_branch_deletion_is_allowed", "passed": all(checks.values()), "checks": checks, "exit": code, "output": text[-300:]}


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
        return {"pushed": code == 0, "gate_ran": runs_of(root) == 1, "osv_ran_once": runs_of(root, "osv_scanner") == 1}

    return push_case(
        scratch,
        "pre_push_skip_in_the_push_environment_does_not_disable_the_gate",
        "gate_skip_push",
        lambda root: None,
        ("origin", "topic"),
        check,
        mutate,
        extra={"SKIP": "local_ci_pushed,osv-scanner"},
        changed=("requirements.txt", "README.md"),
    )


def skip_osv_case(scratch: Path, mutate: Callable[[str], str] | None = None) -> dict[str, Any]:
    """``SKIP`` in the environment of the gate does not suppress the OSV-Scanner hook, which pre-commit would otherwise skip."""
    root = pre_push_repo(scratch, "gate_skip_osv", ("requirements.txt", "README.md"), mutate=mutate)
    code, out, error = pre_push(root, "run", extra={"SKIP": "osv-scanner"}, timeout=300)
    return {
        "case": "pre_push_skip_does_not_suppress_the_osv_hook_in_pre_commit",
        "passed": code == 0 and runs_of(root, "osv_scanner") == 1,
        "exit": code,
        "osv_runs": runs_of(root, "osv_scanner"),
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
    "CI_ADMISSION_ONLY",
    "CI_WORKER_LIMIT_SECONDS",
    "CI_WORKER_GRACE_SECONDS",
    "CI_CORE_BUDGET",
    "RANGE_START",
    "RANGE_END",
    "LAUNCHER_SHARD",
    "FEATURE_SHARD",
    "ACQUISITION_PART",
    "ACQUISITION_SHARD",
    "ACQUISITION_SHARD_DIR",
    "ACQUISITION_SHARD_COUNT",
    "GITHUB_ACTIONS",
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
    "removed-names": {"PRE_COMMIT_FROM_REF", "PRE_COMMIT_TO_REF"},
}


def inherited_controls_case(scratch: Path, mutate: Callable[[str], str] | None = None) -> dict[str, Any]:
    root = pre_push_repo(scratch, "controls", mutate=mutate)
    poison = {name: "poison" for name in EXPECTED_CONTROLS}
    poison.update({"PRE_COMMIT_TO_REF": head_of(root), "GIT_DIR": str(root / ".git"), "GIT_WORK_TREE": str(root)})
    code, out, error = pre_push(root, "run", extra=poison)
    leaks: dict[str, list[str]] = {}
    for call in pre_push_calls(root):
        key = "removed-names" if call["argv"][:1] == ["removed-names"] else call["stub"]
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
        temp_branch_at_head_case,
        temp_local_name_case,
        temp_remote_name_case,
        temp_known_commit_case,
        temp_head_alias_case,
        temp_with_head_ref_case,
        temp_deletion_case,
        tree_tag_case,
        up_to_date_case,
        tracked_change_ref_case,
        shim_presence_case,
        skip_in_push_case,
        skip_osv_case,
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
        ("first_ref_only", known_then_unknown_case, "for ref, sha, local in updates:", "for ref, sha, local in updates[:1]:"),
        ("known_rule_dropped", new_branch_known_case, "if sends_nothing(root, commit, destination):", "if False:"),
        ("known_rule_for_tags_dropped", annotated_tag_known_case, "if sends_nothing(root, commit, destination):", "if False:"),
        (
            "any_commit_checked",
            unknown_ref_case,
            'return "refused", f"is neither at the destination nor',
            'return "checked", f"is neither at the destination nor',
        ),
        ("deletion_refused", deletions_case, 'if not sha.strip("0"):', "if False:"),
        ("temp_check_dropped", temp_branch_at_head_case, "if named:", "if False:"),
        ("temp_known_commit_allowed", temp_known_commit_case, "if named:", 'if named and decision != "known":'),
        ("temp_remote_name_unchecked", temp_remote_name_case, "for name in (remote_ref, local_ref):", "for name in (local_ref,):"),
        ("temp_local_name_unchecked", temp_local_name_case, "for name in (remote_ref, local_ref):", "for name in (remote_ref,):"),
        ("temp_head_alias_unresolved", temp_head_alias_case, 'if local_ref == "HEAD":', "if False:"),
        ("tag_not_peeled", annotated_tag_head_case, 'f"{name}^{{commit}}"', "name"),
        ("nothing_to_check_still_checks", new_branch_known_case, 'checkout = any(row["decision"] == "checked" for row in decisions)', "checkout = True"),
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
        ("skip_kept_for_the_osv_hook", skip_osv_case, '    "SKIP",\n', ""),
        ("skip_kept_in_the_push", skip_in_push_case, '    "SKIP",\n', ""),
        ("skip_kept_in_a_step", inherited_controls_case, '    "SKIP",\n', ""),
        ("allow_no_config_kept", inherited_controls_case, '    "PRE_COMMIT_ALLOW_NO_CONFIG",\n', ""),
        ("running_legacy_kept", inherited_controls_case, '    "PRE_COMMIT_RUNNING_LEGACY",\n', ""),
        ("post_checkout_skip_kept", inherited_controls_case, '    "_PRE_COMMIT_SKIP_POST_CHECKOUT",\n', ""),
        ("list_only_control_kept", inherited_controls_case, '"CI_LIST_ONLY",', ""),
        ("admission_control_kept", inherited_controls_case, '"CI_ADMISSION_ONLY",', ""),
        ("native_ref_variable_kept", inherited_controls_case, '"PRE_COMMIT_FROM_REF",', ""),
        ("hosted_marker_variable_kept", inherited_controls_case, '"GITHUB_ACTIONS",', ""),
    ]
    results += [planted_case(label, case, scratch, mutate=source_edit(old, new)) for label, case, old, new in planted]
    return results


STAGE_RUNNERS = ("scripts/quality/run_model_metadata_e2e.py", "scripts/quality/run_checks_e2e.py", "scripts/quality/run_lint_coverage_e2e.py")
# The path hooks the runners start by name run at commit when a file they read is staged, so they resolve to the commit stage; test-checks stays manual-only.
MOVED_HOOKS = ("model-metadata", "build-contexts", "sqlfluff", "orphan-files", "unused-code")
MANUAL_ONLY_HOOKS = ("test-checks",)
FAST_HOOKS_IN_RUNNERS = ("ruff", "privacy-scan", "docker-calls")


def runner_stage_problems(sources: dict[str, str], stages: dict[str, str]) -> list[str]:
    """Runners that start a moved hook without naming its stage, and hooks that resolve to the wrong stage."""
    problems = [
        f"{name} does not pass --hook-stage from hook_stage()" for name, text in sources.items() if "hook_stage(" not in text or "--hook-stage" not in text
    ]
    problems += [
        f"{hook} resolves to {stages.get(hook)!r}, not 'pre-commit'" for hook in (*MOVED_HOOKS, *FAST_HOOKS_IN_RUNNERS) if stages.get(hook) != "pre-commit"
    ]
    problems += [f"{hook} resolves to {stages.get(hook)!r}, not 'manual'" for hook in MANUAL_ONLY_HOOKS if stages.get(hook) != "manual"]
    return problems


def resolved_stages(config: Path) -> dict[str, str]:
    try:
        resolver = importlib.import_module("scripts.quality.hook_stage").hook_stage
    except ImportError:
        return {}
    return {hook: resolver(config, hook) for hook in (*MOVED_HOOKS, *MANUAL_ONLY_HOOKS, *FAST_HOOKS_IN_RUNNERS)}


def runner_stage_cases(scratch: Path) -> list[dict[str, Any]]:
    """Every runner that starts a hook through pre-commit names the stage the hook now lives in."""
    sources = {name: (ROOT / name).read_text() for name in STAGE_RUNNERS}
    config = ROOT / ".pre-commit-config.yaml"
    stages = resolved_stages(config)
    problems = runner_stage_problems(sources, stages)
    unstaged = {name: text.replace("hook_stage(", "hook_name(") for name, text in sources.items()}
    damaged = scratch / "hook_config_damaged.yaml"
    document = load_yaml(config.read_text())
    if not isinstance(document, dict):
        raise ValueError(".pre-commit-config.yaml must contain a mapping")
    hook_table(document)["orphan-files"]["stages"] = ["manual"]
    damaged.write_text(json.dumps(document))
    return [
        {"case": "runners_name_the_stage_of_moved_hooks", "passed": not problems, "problems": problems},
        {
            "case": "planted_runner_without_stage_caught_by_runners_name_the_stage_of_moved_hooks",
            "passed": any("does not pass" in problem for problem in runner_stage_problems(unstaged, stages)),
        },
        {
            "case": "planted_path_hook_left_the_commit_stage_caught_by_runners_name_the_stage_of_moved_hooks",
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
    launcher_shards_only = "--launcher-shards-only" in sys.argv[1:]
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
        elif launcher_shards_only:
            results += launcher_shard_cases(work, out)
        else:
            results.append(scratch_cleanup_interrupt_case(scratch))
            results += restore_cases(work, scratch)
            results += selection_cases(work)
            results += fast_hook_cases(scratch)
            results += pre_push_cases(work, scratch, gate_stage=not selection_only)
            results += gate_group_cases(work)
            if not selection_only:
                results += launcher_shard_cases(work, out)
            results += runner_stage_cases(scratch)
            results += speed_static_cases(scratch)
            if not selection_only:
                results += real_run_cases(work, out, scratch)
    finally:
        worktrees = trees.close()
        shutil.rmtree(scratch, ignore_errors=True)
    results.append({"case": "scratch_worktree_removed", "passed": not worktrees["problems"], **worktrees})
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
    skipped = [
        "list_only_prints_plan_*",
        "dispatcher_accepts_hosted_part_*",
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
        "gate_stage_*",
    ]
    mode = (
        {"mode": "iteration", "skipped": skipped, "note": "not commit evidence"} if selection_only or speed_only or launcher_shards_only else {"mode": "full"}
    )
    report = {"started": out.name, **mode, "code": identity, "cases": results, "passed": len(results) - len(failed), "total": len(results), "failed": failed}
    (out / "report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    sys.stdout.write(f"Bash used by the group-file guard cases: {identity.get('tools', {}).get('bash (the group-file guard)', 'unknown')}\n")
    for result in results:
        sys.stdout.write(("PASS " if result["passed"] else "FAIL ") + result["case"] + "\n")
    sys.stdout.write(f"{report['passed']}/{report['total']} passed; report {out / 'report.json'}\n")
    return 0 if not failed else 1


if __name__ == "__main__":
    raise SystemExit(main())
