"""E2E for the sharded acquisition gate (scripts/acquisition/shards.py).

Run from the repository root: ``.venv/bin/python -m scripts.acquisition.run_shards_e2e``. Two kinds of case:

- The real collection: every test that the acquisition run collects belongs to exactly one shard (for one, two and three shards), the committed
  weights name only tests that exist, and the committed weights leave the CI shards (shards.CI_SHARD_COUNT) within 25% of the total apart.
- A synthetic project built in a scratch folder runs the real pytest plugin, the real ``seal`` and ``skip`` commands and the real ``gate`` command
  (the same entry points that scripts/acquisition/run_checks_shard.sh and run_checks_combine.sh start). Its shards cover different code, so neither
  covers the target alone and the merge does, and one module runs only in a child process. Each negative case changes one thing (a missing shard, a
  failed test, a dropped test, a shard of another run, a swapped coverage file, a mixed skip) and passes only when the gate fails for that cause.

Failure modes S1 to S14 (plans, local). The report goes to data/e2e/acquisition_shards_e2e/<UTC>_<pid>/report.json with the revision, the SHA-256 of
the code under test and the tool versions.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import sys
import tempfile
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.acquisition import shards
from scripts.process import clear_git_environment, run_command
from scripts.quality.repo_checks import load_yaml

ROOT = Path(__file__).resolve().parents[2]
TARGET = 85.0
CODE = (
    "scripts/acquisition/shards.py",
    "scripts/acquisition/run_shards_e2e.py",
    "config/acquisition/shard_weights.json",
    "config/acquisition/coverage.ini",
    ".github/workflows/ci.yml",
    "scripts/acquisition/run_checks_combine.sh",
    "config/quality/ci_jobs.json",
)
TEST_PATHS = ["scripts/acquisition/tests", "tests/test_source_registry.py"]

CORE = """def first(n):
    total = 0
    for value in range(n):
        total += value
    return total


def second(n):
    if n > 3:
        return "large"
    if n > 1:
        return "small"
    return "none"


def never_run():
    return "unused"
"""
CHILD = """import sys


def main():
    names = sys.argv[1:]
    for name in names:
        sys.stdout.write(name.upper() + "\\n")
    return len(names)


if __name__ == "__main__":
    main()
"""
TESTS = """import subprocess
import sys

from pkg import core


def test_first():
    assert core.first(4) == 6


def test_second():
    assert core.second(5) == "large" and core.second(2) == "small" and core.second(0) == "none"


def test_child_process():
    result = subprocess.run([sys.executable, "-m", "pkg.child", "a", "b"], capture_output=True, text=True, check=True)
    assert result.stdout == "A\\nB\\n"
"""
FAILING = """

def test_fails():
    assert False, "planted failure"
"""
WEIGHTS = {
    "default_seconds": 0.1,
    "seconds": {"tests/test_pkg.py::test_first": 10.0, "tests/test_pkg.py::test_second": 10.0, "tests/test_pkg.py::test_child_process": 1.0},
}
COVERAGE_CONFIG = "[run]\npatch = subprocess\n\n[report]\nshow_missing = True\nfail_under = 90\n"


def sha256_of(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def environment(**extra: str) -> dict[str, str]:
    return {**os.environ, "PYTHONPATH": str(ROOT), "GITHUB_SHA": "a" * 40, "GITHUB_RUN_ID": "100", "GITHUB_RUN_ATTEMPT": "1", **extra}


def build_project(folder: Path, failing: bool = False) -> Path:
    project = folder / "project"
    (project / "pkg").mkdir(parents=True)
    (project / "tests").mkdir()
    (project / "pkg" / "__init__.py").write_text("")
    (project / "pkg" / "core.py").write_text(CORE)
    (project / "pkg" / "child.py").write_text(CHILD)
    (project / "tests" / "test_pkg.py").write_text(TESTS + (FAILING if failing else ""))
    (project / "weights.json").write_text(json.dumps(WEIGHTS))
    (project / "coverage.ini").write_text(COVERAGE_CONFIG)
    (project / "pytest.ini").write_text("[pytest]\n")
    return project


def run_shard(project: Path, directory: Path, shard: str, extra: list[str] | None = None, **env: str) -> int:
    """The steps of scripts/acquisition/run_checks_shard.sh: pytest with the plugin and coverage, then ``seal``."""
    index = shard.split("/")[0]
    data = directory / "coverage" / f"shard{index}.coverage"
    data.parent.mkdir(parents=True, exist_ok=True)
    shell = environment(COVERAGE_FILE=str(data), **env)
    pytest = run_command(
        sys.executable,
        [
            "-m",
            "pytest",
            "tests",
            "-p",
            "scripts.acquisition.shards",
            "--shard",
            shard,
            "--shard-output",
            str(directory),
            "--shard-weights",
            str(project / "weights.json"),
            "--cov=pkg",
            f"--cov-config={project / 'coverage.ini'}",
            "--cov-report=",
            "--cov-fail-under=0",
            "-q",
            "-p",
            "no:cacheprovider",
            *(extra or []),
        ],
        cwd=project,
        env=shell,
        timeout=300,
    )
    sealed = run_command(
        sys.executable,
        [
            "-m",
            "scripts.acquisition.shards",
            "seal",
            "--directory",
            str(directory),
            "--shard",
            shard,
            "--pytest-exit",
            str(pytest.returncode),
            "--coverage-config",
            str(project / "coverage.ini"),
        ],
        cwd=project,
        env=shell,
        timeout=300,
    )
    return pytest.returncode or sealed.returncode


def run_gate(project: Path, directory: Path, expect: str = "run", fail_under: float = TARGET, **env: str) -> tuple[int, str]:
    result = run_command(
        sys.executable,
        [
            "-m",
            "scripts.acquisition.shards",
            "gate",
            "--directory",
            str(directory),
            "--count",
            "2",
            "--expect",
            expect,
            "--fail-under",
            str(fail_under),
            "--coverage-config",
            str(project / "coverage.ini"),
            "--root",
            str(project),
            "--weights",
            str(project / "weights.json"),
            "--",
            "tests",
        ],
        cwd=project,
        env=environment(**env),
        timeout=300,
    )
    return result.returncode, result.stdout + result.stderr


def manifest(directory: Path, index: int) -> dict[str, Any]:
    loaded: dict[str, Any] = json.loads((directory / f"shard_{index}_of_2.json").read_text())
    return loaded


def save(directory: Path, index: int, document: dict[str, Any]) -> None:
    (directory / f"shard_{index}_of_2.json").write_text(json.dumps(document))


def copy_run(source: Path, scratch: Path, name: str) -> Path:
    target = scratch / name
    shutil.copytree(source, target)
    return target


def negative(name: str, code: int, output: str, cause: str, **extra: Any) -> dict[str, Any]:
    """A negative case passes only when the gate fails and its message cites the intended cause."""
    return {"case": name, "passed": code != 0 and cause in output, "exit": code, "cause": cause, "output_tail": output.strip()[-300:], **extra}


WORKFLOW = ".github/workflows/ci.yml"
COMBINE = "scripts/acquisition/run_checks_combine.sh"
JOBS = "config/quality/ci_jobs.json"


def step_environment(job: dict[str, Any], key: str, value: str) -> dict[str, Any] | None:
    """The environment of the step of ``job`` that sets ``key`` to ``value``, or None."""
    for step in job.get("steps", []):
        environment = step.get("env") if isinstance(step, dict) else None
        if isinstance(environment, dict) and str(environment.get(key)) == value:
            return environment
    return None


def shard_count_problems(count: int, workflow: dict[str, Any], combine: str, jobs: dict[str, Any], texts: dict[str, str]) -> list[str]:
    """Every place that states the number of acquisition shards must state ``count`` (shards.CI_SHARD_COUNT); the problems found, empty when they agree."""
    problems: list[str] = []
    definitions = workflow.get("jobs", {})
    shard_job = next((job for job in definitions.values() if step_environment(job, "ACQUISITION_PART", "shard")), None)
    combine_job = next((job for job in definitions.values() if step_environment(job, "ACQUISITION_PART", "combine")), None)
    if shard_job is None or combine_job is None:
        return [f"{WORKFLOW} has no job with ACQUISITION_PART shard or no job with ACQUISITION_PART combine"]
    matrix = shard_job.get("strategy", {}).get("matrix", {}).get("shard")
    if matrix != list(range(1, count + 1)):
        problems.append(f"the matrix of the shard job is {matrix}, not 1 to {count}")
    environment = step_environment(shard_job, "ACQUISITION_PART", "shard") or {}
    argument = re.fullmatch(r"\$\{\{ matrix\.shard \}\}/(\d+)", str(environment.get("ACQUISITION_SHARD", "")))
    if not argument or int(argument.group(1)) != count:
        problems.append(f"ACQUISITION_SHARD of the shard job is {environment.get('ACQUISITION_SHARD')!r}, not a matrix shard over {count}")
    named = re.fullmatch(r".*\(Shard \$\{\{ matrix\.shard \}\} of (\d+)\)", str(shard_job.get("name", "")))
    if not named or int(named.group(1)) != count:
        problems.append(f"the name of the shard job is {shard_job.get('name')!r}, not 'of {count}'")
    environment = step_environment(combine_job, "ACQUISITION_PART", "combine") or {}
    if str(environment.get("ACQUISITION_SHARD_COUNT")) != str(count):
        problems.append(f"ACQUISITION_SHARD_COUNT of the combining job is {environment.get('ACQUISITION_SHARD_COUNT')!r}, not {count!r}")
    default = re.search(r"--count \"\$\{ACQUISITION_SHARD_COUNT:-(\d+)\}\"", combine)
    if not default or int(default.group(1)) != count:
        problems.append(f"the default of ACQUISITION_SHARD_COUNT in {COMBINE} is {default.group(1) if default else None}, not {count}")
    wanted = {f"Acquisition Checks (Shard {index} of {count})" for index in range(1, count + 1)}
    listed = {name for name in jobs.get("required_pull_request_jobs", []) if str(name).startswith("Acquisition Checks (Shard ")}
    if listed != wanted:
        problems.append(f"{JOBS} lists the shard jobs {sorted(listed)}, not {sorted(wanted)}")
    if "ACQUISITION_E2E_MODE" in texts[WORKFLOW]:
        problems.append(f"{WORKFLOW} sets ACQUISITION_E2E_MODE, which makes the acquisition E2E skip tests")
    return problems


def launcher_shard_problems(workflow: dict[str, Any]) -> list[str]:
    """The launcher E2E shard job states one count in its matrix, its shard argument and its name (the launcher has no count constant)."""
    job = next((job for job in workflow.get("jobs", {}).values() if any("LAUNCHER_SHARD" in (step.get("env") or {}) for step in job.get("steps", []))), None)
    if job is None:
        return [f"{WORKFLOW} has no launcher shard job"]
    matrix = job.get("strategy", {}).get("matrix", {}).get("shard")
    if not isinstance(matrix, list) or not matrix or matrix != list(range(1, len(matrix) + 1)):
        return [f"the matrix of the launcher shard job is {matrix}, not 1 to N"]
    shard = next(step["env"]["LAUNCHER_SHARD"] for step in job["steps"] if "LAUNCHER_SHARD" in (step.get("env") or {}))
    named = re.fullmatch(r".*\(Shard \$\{\{ matrix\.shard \}\} of (\d+)\)", str(job.get("name", "")))
    problems = []
    if shard != f"${{{{ matrix.shard }}}}/{len(matrix)}":
        problems.append(f"LAUNCHER_SHARD of the launcher shard job is {shard!r}, not a matrix shard over {len(matrix)}")
    if not named or int(named.group(1)) != len(matrix):
        problems.append(f"the name of the launcher shard job is {job.get('name')!r}, not 'of {len(matrix)}'")
    return problems


def workflow_binding_cases() -> list[dict[str, Any]]:
    """The workflow, the combine script, the job list and shards.CI_SHARD_COUNT agree on the shard count; a planted disagreement in each is caught."""
    texts = {name: (ROOT / name).read_text(encoding="utf-8") for name in (WORKFLOW, COMBINE, JOBS)}
    count = shards.CI_SHARD_COUNT

    def problems_for(count: int = count, **changes: tuple[str, str]) -> list[str]:
        changed = dict(texts)
        for name, (old, new) in changes.items():
            path = {"workflow": WORKFLOW, "combine": COMBINE, "jobs": JOBS}[name]
            if old not in changed[path]:
                return [f"planted text not found in {path}: {old}"]
            changed[path] = changed[path].replace(old, new)
        loaded = load_yaml(changed[WORKFLOW])
        if not isinstance(loaded, dict):
            return [f"{WORKFLOW} is not a mapping"]
        return shard_count_problems(count, loaded, changed[COMBINE], json.loads(changed[JOBS]), changed) + launcher_shard_problems(loaded)

    last = f"[{', '.join(str(index) for index in range(1, count + 1))}]"
    shorter = f"[{', '.join(str(index) for index in range(1, count))}]"
    cases = [{"case": "shard_count_agrees_across_workflow_script_jobs_and_constant", "passed": not problems_for(), "problems": problems_for()}]
    planted = {
        "matrix_with_one_shard_fewer": (problems_for(workflow=(f"shard: {last}", f"shard: {shorter}")), "the matrix of the shard job"),
        "denominator_of_the_shard_argument": (
            problems_for(workflow=(f"ACQUISITION_SHARD: ${{{{ matrix.shard }}}}/{count}", f"ACQUISITION_SHARD: ${{{{ matrix.shard }}}}/{count - 1}")),
            "ACQUISITION_SHARD of the shard job",
        ),
        "name_of_the_shard_job": (problems_for(workflow=("}}" + f" of {count})", "}}" + f" of {count - 1})")), "the name of the shard job"),
        "count_of_the_combining_job": (
            problems_for(workflow=(f'ACQUISITION_SHARD_COUNT: "{count}"', f'ACQUISITION_SHARD_COUNT: "{count - 1}"')),
            "ACQUISITION_SHARD_COUNT of the combining job",
        ),
        "default_of_the_combine_script": (problems_for(combine=(f":-{count}}}", f":-{count - 1}}}")), "the default of ACQUISITION_SHARD_COUNT"),
        "job_list_names": (problems_for(jobs=(f"(Shard {count} of {count})", f"(Shard {count} of {count - 1})")), "lists the shard jobs"),
        "launcher_matrix_with_one_shard_more": (problems_for(workflow=("shard: [1, 2]", "shard: [1, 2, 3]")), "LAUNCHER_SHARD of the launcher shard job"),
        "launcher_shard_argument": (
            problems_for(workflow=("LAUNCHER_SHARD: ${{ matrix.shard }}/2", "LAUNCHER_SHARD: ${{ matrix.shard }}/3")),
            "LAUNCHER_SHARD of",
        ),
        "launcher_job_name": (
            problems_for(workflow=("Launcher E2E (Shard ${{ matrix.shard }} of 2)", "Launcher E2E (Shard ${{ matrix.shard }} of 3)")),
            "the name of the launcher",
        ),
        "constant_in_the_code": (problems_for(count=count - 1), "the matrix of the shard job"),
        "iteration_mode_in_the_workflow": (
            problems_for(workflow=("ACQUISITION_PART: combine\n", "ACQUISITION_PART: combine\n          ACQUISITION_E2E_MODE: iteration\n")),
            "ACQUISITION_E2E_MODE",
        ),
    }
    for name, (found, cause) in planted.items():
        cases.append({"case": f"planted_shard_count_disagreement_is_caught:{name}", "passed": any(cause in problem for problem in found), "problems": found})
    return cases


def real_collection_cases() -> list[dict[str, Any]]:
    ids = shards.collect_ids(TEST_PATHS)
    weights, default = shards.load_weights(shards.WEIGHTS)
    cases: list[dict[str, Any]] = []
    partition: dict[str, Any] = {}
    for count in (1, 2, 3, shards.CI_SHARD_COUNT):
        owner = shards.assign(ids, count, weights, default)
        loads = shards.shard_loads(ids, count, weights, default)
        partition[str(count)] = {"tests": len(owner), "per_shard": [sum(1 for shard in owner.values() if shard == number) for number in range(1, count + 1)]}
        partition[str(count)]["exact"] = set(owner) == set(ids) and len(owner) == len(ids) == len(set(ids)) and set(owner.values()) == set(range(1, count + 1))
        partition[str(count)]["loads"] = [round(load, 1) for load in loads]
    cases.append(
        {
            "case": "real_collection_is_partitioned_exactly_once",
            "passed": all(item["exact"] for item in partition.values()) and len(ids) > 400,
            "partition": partition,
        }
    )
    loads = shards.shard_loads(ids, shards.CI_SHARD_COUNT, weights, default)
    cases.append(
        {
            "case": "committed_weights_balance_the_shards",
            "passed": (max(loads) - min(loads)) <= shards.IMBALANCE_LIMIT * sum(loads) and not set(weights) - set(ids),
            "loads": [round(load, 1) for load in loads],
            "weights_without_a_test": sorted(set(weights) - set(ids))[:3],
        }
    )
    return cases


def corrupted(good: Path, scratch: Path, name: str, index: int, change: Callable[[dict[str, Any]], None]) -> Path:
    """A copy of the good run in which ``change`` has altered the manifest of shard ``index``."""
    target = copy_run(good, scratch, name)
    document = manifest(target, index)
    change(document)
    save(target, index, document)
    return target


def corruption_cases(project: Path, good: Path, scratch: Path) -> list[dict[str, Any]]:
    """Every field of a manifest that the gate trusts is checked against the record or recomputed: one corruption each, with the cause in the message."""
    cases: list[dict[str, Any]] = []

    def first_outcome(document: dict[str, Any]) -> str:
        return str(document["assigned"][0])

    def to_failed(document: dict[str, Any]) -> None:
        document["outcomes"][first_outcome(document)][0] = "failed"

    def to_error(document: dict[str, Any]) -> None:
        document["outcomes"][first_outcome(document)][0] = "error"

    def to_unknown(document: dict[str, Any]) -> None:
        document["outcomes"][first_outcome(document)][0] = "bogus"
        document["counts"]["passed"] -= 1

    def to_text(document: dict[str, Any]) -> None:
        document["outcomes"][first_outcome(document)] = "passed"

    def to_negative_time(document: dict[str, Any]) -> None:
        document["outcomes"][first_outcome(document)][1] = -1

    def to_infinite_time(document: dict[str, Any]) -> None:
        document["outcomes"][first_outcome(document)][1] = float("inf")

    def to_negative_infinite_time(document: dict[str, Any]) -> None:
        document["outcomes"][first_outcome(document)][1] = float("-inf")

    def to_not_a_number_time(document: dict[str, Any]) -> None:
        document["outcomes"][first_outcome(document)][1] = float("nan")

    def to_skipped_consistently(document: dict[str, Any]) -> None:
        document["outcomes"][first_outcome(document)][0] = "skipped"
        document["counts"]["passed"] -= 1
        document["counts"]["skipped"] += 1

    def failed_consistently(document: dict[str, Any]) -> None:
        to_failed(document)
        document["counts"]["passed"] -= 1
        document["counts"]["failed"] += 1

    def inflated(document: dict[str, Any]) -> None:
        document["counts"]["passed"] += 1

    def extra_counter(document: dict[str, Any]) -> None:
        document["counts"]["xfailed"] = 0

    def extra_field(document: dict[str, Any]) -> None:
        document["note"] = "unreviewed"

    def lost_field(document: dict[str, Any]) -> None:
        del document["coverage_sha256"]

    def boolean_exit(document: dict[str, Any]) -> None:
        document["pytest_exit_status"] = False

    def doubled(document: dict[str, Any]) -> None:
        document["assigned"].append(document["assigned"][0])

    def coverage_claim(document: dict[str, Any]) -> None:
        document["coverage_files"] = 1

    def coverage_text(document: dict[str, Any]) -> None:
        document["coverage_files"] = str(document["coverage_files"])

    table: list[tuple[str, Callable[[dict[str, Any]], None], str]] = [
        ("passed_outcome_changed_to_failed_with_unchanged_counters_fails_the_gate", to_failed, "has outcome failed"),
        ("passed_outcome_changed_to_error_with_unchanged_counters_fails_the_gate", to_error, "has outcome error"),
        ("failed_outcome_with_matching_counters_fails_the_gate", failed_consistently, "has outcome failed"),
        ("unknown_outcome_value_fails_the_gate", to_unknown, "has an invalid outcome"),
        ("outcome_that_is_not_a_record_fails_the_gate", to_text, "has an invalid outcome"),
        ("negative_outcome_seconds_fail_the_gate", to_negative_time, "has an invalid outcome"),
        ("infinite_outcome_seconds_fail_the_gate", to_infinite_time, "has an invalid outcome"),
        ("negative_infinite_outcome_seconds_fail_the_gate", to_negative_infinite_time, "has an invalid outcome"),
        ("not_a_number_outcome_seconds_fail_the_gate", to_not_a_number_time, "has an invalid outcome"),
        ("counters_above_the_outcomes_fail_the_gate", inflated, "differ from the counts"),
        ("counter_that_no_outcome_has_fails_the_gate", extra_counter, "differ from the counts"),
        ("manifest_with_an_extra_field_fails_the_gate", extra_field, "has other fields than a manifest"),
        ("manifest_that_lost_a_field_fails_the_gate", lost_field, "has other fields than a manifest"),
        ("boolean_exit_status_fails_the_gate", boolean_exit, "failed: status passed, exit False"),
        ("assigned_test_listed_twice_fails_the_gate", doubled, "lists its assigned tests wrongly"),
        ("coverage_file_count_claim_fails_the_gate", coverage_claim, "its manifest says 1"),
        ("coverage_file_count_as_text_fails_the_gate", coverage_text, "its manifest says"),
    ]
    for name, change, cause in table:
        target = corrupted(good, scratch, name, 1, change)
        code, out = run_gate(project, target)
        cases.append(negative(name, code, out, cause))

    allowed = corrupted(good, scratch, "skipped_outcome", 1, to_skipped_consistently)
    code, out = run_gate(project, allowed)
    cases.append({"case": "skipped_outcome_with_matching_counters_passes_the_gate", "passed": code == 0, "exit": code, "output_tail": out.strip()[-200:]})

    moved = copy_run(good, scratch, "moved_test")
    first, second = manifest(moved, 1), manifest(moved, 2)
    test = first["assigned"].pop()
    second["assigned"].append(test)
    second["outcomes"][test] = first["outcomes"].pop(test)
    first["counts"]["passed"] -= 1
    second["counts"]["passed"] += 1
    save(moved, 1, first)
    save(moved, 2, second)
    code, out = run_gate(project, moved)
    cases.append(negative("test_run_in_another_shard_than_assigned_fails_the_gate", code, out, "other ones"))

    skipped = scratch / "skipped_fields_run"
    for index in (1, 2):
        run_command(
            sys.executable,
            ["-m", "scripts.acquisition.shards", "skip", "--directory", str(skipped), "--shard", f"{index}/2", "--reason", "no path reached"],
            cwd=project,
            env=environment(),
            timeout=120,
        )
    extra = copy_run(skipped, scratch, "skipped_extra_field")
    document = manifest(extra, 2)
    document["assigned"] = ["tests/test_pkg.py::test_first"]
    save(extra, 2, document)
    code, out = run_gate(project, extra, expect="skip")
    cases.append(negative("skipped_manifest_with_an_extra_field_fails_the_gate", code, out, "has other fields than a manifest"))
    reasonless = copy_run(skipped, scratch, "skipped_no_reason")
    document = manifest(reasonless, 1)
    document["reason"] = " "
    save(reasonless, 1, document)
    code, out = run_gate(project, reasonless, expect="skip")
    cases.append(negative("skipped_manifest_without_a_reason_fails_the_gate", code, out, "is skipped without a reason"))
    return cases


def synthetic_cases(scratch: Path) -> list[dict[str, Any]]:
    cases: list[dict[str, Any]] = []
    project = build_project(scratch / "good")
    good = scratch / "good_run"
    codes = [run_shard(project, good, "1/2"), run_shard(project, good, "2/2")]
    gate_code, gate_out = run_gate(project, good)
    cases.append(
        {
            "case": "all_shards_pass_the_gate",
            "passed": codes == [0, 0] and gate_code == 0 and "TOTAL" in gate_out,
            "shard_exits": codes,
            "gate_exit": gate_code,
            "output_tail": gate_out[-400:],
        }
    )

    alone = []
    for index in (1, 2):
        data = good / "coverage" / f"shard{index}.coverage"
        report = run_command(
            sys.executable,
            ["-m", "coverage", "report", "--rcfile", str(project / "coverage.ini"), "--data-file", str(data), f"--fail-under={TARGET}"],
            cwd=project,
            timeout=120,
        )
        alone.append(report.returncode)
    cases.append(
        {
            "case": "gate_never_passes_on_one_shards_coverage",
            "passed": alone == [2, 2] and gate_code == 0,
            "alone_exit_codes": alone,
            "note": "each shard's coverage is under the target alone (coverage exit 2) while the merge passes the same target",
        }
    )
    cases.append(
        {
            "case": "shard_runs_do_not_enforce_the_gate_alone",
            "passed": codes == [0, 0] and "fail_under = 90" in (project / "coverage.ini").read_text(),
            "note": "the shard runs exit 0 although the configuration file asks 90% and each shard covers less",
        }
    )
    merged = run_command(
        sys.executable,
        [
            "-m",
            "coverage",
            "json",
            "--rcfile",
            str(project / "coverage.ini"),
            "--data-file",
            str(good / "coverage" / "shard1.coverage"),
            "--fail-under=0",
            "-o",
            str(scratch / "shard1.json"),
        ],
        cwd=project,
        timeout=120,
    )
    child_lines = json.loads((scratch / "shard1.json").read_text())["files"].get("pkg/child.py", {}).get("executed_lines", []) if merged.returncode == 0 else []
    cases.append({"case": "subprocess_lines_survive_the_merge", "passed": len(child_lines) >= 4, "child_lines_executed_in_the_shard": child_lines})

    below_code, below_out = run_gate(project, good, fail_under=100.0)
    cases.append(negative("merged_coverage_below_target_fails", below_code, below_out, "is below 100.0%"))

    missing = copy_run(good, scratch, "missing")
    (missing / "shard_2_of_2.json").unlink()
    code, out = run_gate(project, missing)
    cases.append(negative("missing_shard_fails_the_gate", code, out, "shard 2 of 2 is missing"))

    other = copy_run(good, scratch, "other_run")
    code, out = run_gate(project, other, GITHUB_RUN_ID="999")
    cases.append(negative("shard_from_another_run_fails_the_gate", code, out, "belongs to another CI run"))

    tampered = copy_run(good, scratch, "mismatch")
    document = manifest(tampered, 2)
    document["collected_sha256"] = "0" * 64
    save(tampered, 2, document)
    code, out = run_gate(project, tampered)
    cases.append(negative("mismatched_manifests_fail_the_gate", code, out, "collected other tests than the gate"))

    counts = copy_run(good, scratch, "counts")
    document = manifest(counts, 1)
    document["counts"]["failed"] = 1
    save(counts, 1, document)
    code, out = run_gate(project, counts)
    cases.append(negative("failed_test_in_a_shard_fails_the_gate_even_with_exit_zero", code, out, "failed and 0 errored"))

    status = copy_run(good, scratch, "status")
    document = manifest(status, 1)
    document["status"] = "failed"
    save(status, 1, document)
    code, out = run_gate(project, status)
    cases.append(negative("failed_status_in_a_manifest_fails_the_gate", code, out, "shard 1 failed: status failed"))

    exit_status = copy_run(good, scratch, "exit_status")
    document = manifest(exit_status, 2)
    document["pytest_exit_status"] = 1
    save(exit_status, 2, document)
    code, out = run_gate(project, exit_status)
    cases.append(negative("pytest_exit_status_in_a_manifest_fails_the_gate", code, out, "shard 2 failed: status passed, exit 1"))

    unreported = copy_run(good, scratch, "unreported")
    document = manifest(unreported, 1)
    document["outcomes"].pop(document["assigned"][0])
    save(unreported, 1, document)
    code, out = run_gate(project, unreported)
    cases.append(negative("assigned_test_without_an_outcome_fails_the_gate", code, out, "did not report every assigned test"))

    in_no_shard = copy_run(good, scratch, "in_no_shard")
    document = manifest(in_no_shard, 1)
    gone = document["assigned"].pop()
    document["outcomes"].pop(gone)
    document["counts"]["passed"] -= 1
    save(in_no_shard, 1, document)
    code, out = run_gate(project, in_no_shard)
    cases.append(negative("test_in_no_shard_fails_the_gate", code, out, "not run: "))

    in_two_shards = copy_run(good, scratch, "in_two_shards")
    first, second = manifest(in_two_shards, 1), manifest(in_two_shards, 2)
    twice = second["assigned"][0]
    first["assigned"].append(twice)
    first["outcomes"][twice] = second["outcomes"][twice]
    first["counts"]["passed"] += 1
    save(in_two_shards, 1, first)
    code, out = run_gate(project, in_two_shards)
    cases.append(negative("test_in_two_shards_fails_the_gate", code, out, "is assigned to shards"))

    swapped = copy_run(good, scratch, "swapped")
    shutil.copy2(swapped / "coverage" / "shard1.coverage", swapped / "coverage" / "shard2.coverage")
    code, out = run_gate(project, swapped)
    cases.append(negative("swapped_coverage_data_fails_the_gate", code, out, "differs from its manifest"))

    files = copy_run(good, scratch, "files")
    document = manifest(files, 1)
    document["coverage_files"] = 99
    save(files, 1, document)
    code, out = run_gate(project, files)
    cases.append(negative("coverage_file_count_matches_the_manifests", code, out, "files and its manifest says 99"))

    cases.extend(corruption_cases(project, good, scratch))

    dropped = scratch / "dropped_run"
    run_shard(project, dropped, "1/2", extra=["-k", "not test_child_process"])
    run_shard(project, dropped, "2/2")
    code, out = run_gate(project, dropped)
    cases.append(
        {
            "case": "dropped_test_fails_the_gate",
            "passed": code != 0 and ("collected other tests than the gate" in out or "did not report every assigned test" in out or "failed" in out),
            "exit": code,
            "output_tail": out.strip()[-300:],
        }
    )

    failing = build_project(scratch / "failing", failing=True)
    failed_run = scratch / "failed_run"
    failed_codes = [run_shard(failing, failed_run, "1/2"), run_shard(failing, failed_run, "2/2")]
    code, out = run_gate(failing, failed_run)
    cases.append(
        negative("failed_shard_fails_the_gate", code, out, "failed", shard_exits=failed_codes) | {"passed": code != 0 and any(failed_codes) and "failed" in out}
    )

    skipped = scratch / "skipped_run"
    for index in (1, 2):
        run_command(
            sys.executable,
            ["-m", "scripts.acquisition.shards", "skip", "--directory", str(skipped), "--shard", f"{index}/2", "--reason", "no path reached"],
            cwd=project,
            env=environment(),
            timeout=120,
        )
    skip_code, skip_out = run_gate(project, skipped, expect="skip")
    run_code, run_out = run_gate(project, skipped, expect="run")
    mixed = copy_run(good, scratch, "mixed")
    run_command(
        sys.executable,
        ["-m", "scripts.acquisition.shards", "skip", "--directory", str(mixed), "--shard", "1/2", "--reason", "no path reached"],
        cwd=project,
        env=environment(),
        timeout=120,
    )
    mixed_run_code, mixed_run_out = run_gate(project, mixed, expect="run")
    mixed_skip_code, mixed_skip_out = run_gate(project, mixed, expect="skip")
    cases.append(
        {
            "case": "mixed_skip_and_run_fails_the_gate",
            "passed": skip_code == 0
            and "All 2 shards were skipped" in skip_out
            and run_code != 0
            and "expects the checks to run" in run_out
            and mixed_run_code != 0
            and "expects the checks to run" in mixed_run_out
            and mixed_skip_code != 0
            and "expects every shard to be skipped" in mixed_skip_out,
            "exits": [skip_code, run_code, mixed_run_code, mixed_skip_code],
        }
    )
    return cases


def main() -> int:
    clear_git_environment()
    started = datetime.now(UTC)
    before = {name: sha256_of(ROOT / name) for name in CODE}
    scratch = Path(tempfile.mkdtemp(prefix="acquisition_shards_e2e."))
    try:
        results = real_collection_cases() + workflow_binding_cases() + synthetic_cases(scratch)
    finally:
        shutil.rmtree(scratch, ignore_errors=True)
    after = {name: sha256_of(ROOT / name) for name in CODE}
    if before != after:
        results.append({"case": "code_unchanged_during_run", "passed": False, "changed": sorted(name for name in before if before[name] != after[name])})
    failed = [result["case"] for result in results if not result["passed"]]
    out = ROOT / "data" / "e2e" / "acquisition_shards_e2e" / f"{started:%Y%m%dT%H%M%SZ}_{os.getpid()}"
    out.mkdir(parents=True)
    revision = run_command("git", ["rev-parse", "HEAD"], cwd=ROOT, check=True).stdout.strip()
    versions = {
        "python": sys.version.split()[0],
        "pytest": run_command(sys.executable, ["-m", "pytest", "--version"], timeout=60).stdout.strip() or "see stderr",
    }
    report = {
        "started": started.isoformat(),
        "revision": revision,
        "code_sha256": before,
        "tools": versions,
        "cases": results,
        "passed": len(results) - len(failed),
        "total": len(results),
        "failed": failed,
    }
    (out / "report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    for result in results:
        sys.stdout.write(("PASS " if result["passed"] else "FAIL ") + result["case"] + "\n")
    sys.stdout.write(f"{report['passed']}/{report['total']} passed; report {out / 'report.json'}\n")
    return 0 if not failed else 1


if __name__ == "__main__":
    raise SystemExit(main())
