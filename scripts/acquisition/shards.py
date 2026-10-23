"""Shard the acquisition pytest run and gate the merged result.

GitHub CI runs the acquisition tests as N shard jobs (scripts/acquisition/run_checks_shard.sh) and one combining job
(scripts/acquisition/run_checks_combine.sh). The local hook and ``scripts/acquisition/run_checks.sh`` stay one unsharded run with the same gate.

Plugin (``pytest -p scripts.acquisition.shards --shard K/N --shard-output DIR``): every collected test goes to exactly one shard. The assignment is a
pure function of the collected node ids and the seconds of config/acquisition/shard_weights.json: tests in order of falling weight, each onto the
shard with the least weight so far. The plugin writes ``shard_K_of_N.json`` (the assigned ids, the hash of all collected ids, the outcome and
seconds of each test, the pytest exit status and the CI run it belongs to).

``seal`` completes the manifest after pytest and pytest-cov have ended: the pytest exit status and the hash and measured-file count of the shard's
coverage data. It writes a failed manifest when pytest wrote none. ``skip`` writes a ``skipped`` manifest for a job whose range does not reach
the acquisition checks.

``gate`` is the only place that enforces the coverage target. It trusts no field of a manifest that it can recompute. It passes only when every
shard 1..N has a manifest of this CI run with exactly the fields of a manifest (no other, none missing), the matching count, collected-id hash and
run; every manifest passed (exit 0, every outcome ``passed`` or ``skipped``, the counters equal to the counters recomputed from the outcomes, no
failed or errored test, every assigned test reported once); the union of the assigned ids equals the ids that the gate collects itself, each once,
and each shard was assigned the tests that the gate's own assignment gives it; each shard's coverage data matches its manifest hash and measures the
number of files that its manifest says; and the merged coverage reaches ``--fail-under``. One shard's coverage is never judged alone.
``--expect skip`` accepts only manifests that are all skipped, with exactly the fields of a skipped manifest.

    .venv/bin/python -m scripts.acquisition.shards gate --directory D --count 4 --expect run --fail-under 90 -- <pytest paths>

E2E: ``.venv/bin/python -m scripts.acquisition.run_shards_e2e``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Any

import pytest

from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]
WEIGHTS = ROOT / "config" / "acquisition" / "shard_weights.json"
COVERAGE_CONFIG = ROOT / "config" / "acquisition" / "coverage.ini"
SCHEMA = 1
PYTEST_SECONDS = 300
COVERAGE_SECONDS = 300
IMBALANCE_LIMIT = 0.25
# The number of acquisition shard jobs in GitHub CI (.github/workflows/ci.yml); the committed weights are checked for this count.
CI_SHARD_COUNT = 4


RUN_FIELDS = frozenset(
    {
        "schema",
        "kind",
        "status",
        "shard",
        "count",
        "context",
        "collected_count",
        "collected_sha256",
        "assigned",
        "outcomes",
        "counts",
        "plugin_exit_status",
        "pytest_exit_status",
        "coverage_sha256",
        "coverage_files",
    }
)
SKIPPED_FIELDS = frozenset({"schema", "kind", "status", "shard", "count", "context", "reason"})
PASSING_OUTCOMES = ("passed", "skipped")
OUTCOMES = ("passed", "failed", "error", "skipped")


class ShardError(Exception):
    """A shard run or the merged gate is not acceptable; the message names why."""


def ci_context() -> dict[str, str]:
    """The CI run the manifests of one gate must share (GitHub's run id and attempt and the commit); ``local`` outside GitHub."""
    return {
        name: os.environ.get(variable, "local")
        for name, variable in (("sha", "GITHUB_SHA"), ("run_id", "GITHUB_RUN_ID"), ("run_attempt", "GITHUB_RUN_ATTEMPT"))
    }


def is_int(value: Any) -> bool:
    """True for a JSON integer; ``True`` and ``False`` are not integers here."""
    return type(value) is int


def parse_shard(value: str) -> tuple[int, int]:
    found = re.fullmatch(r"(\d+)/(\d+)", value)
    if not found or not 1 <= int(found.group(1)) <= int(found.group(2)):
        raise ShardError(f"a shard is written K/N with 1 <= K <= N, not {value!r}")
    return int(found.group(1)), int(found.group(2))


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def sha256_ids(ids: list[str]) -> str:
    return hashlib.sha256("\n".join(sorted(ids)).encode("utf-8")).hexdigest()


def load_weights(path: Path) -> tuple[dict[str, float], float]:
    loaded = json.loads(path.read_text(encoding="utf-8"))
    return {str(name): float(seconds) for name, seconds in loaded["seconds"].items()}, float(loaded["default_seconds"])


def assign(ids: list[str], count: int, weights: dict[str, float], default: float) -> dict[str, int]:
    """Give every id one shard number 1..count: heaviest first, each onto the shard with the least weight so far (the lowest number on a tie)."""
    loads = [0.0] * count
    shards: dict[str, int] = {}
    for test in sorted(set(ids), key=lambda name: (-weights.get(name, default), name)):
        index = min(range(count), key=lambda number: (loads[number], number))
        shards[test] = index + 1
        loads[index] += weights.get(test, default)
    return shards


def shard_loads(ids: list[str], count: int, weights: dict[str, float], default: float) -> list[float]:
    loads = [0.0] * count
    for test, shard in assign(ids, count, weights, default).items():
        loads[shard - 1] += weights.get(test, default)
    return loads


class ShardPlugin:
    """Select the tests of one shard and record what ran."""

    def __init__(self, shard: tuple[int, int], output: Path, weights: Path) -> None:
        self.shard, self.output, self.weights = shard, output, weights
        self.collected: list[str] = []
        self.assigned: list[str] = []
        self.outcomes: dict[str, list[Any]] = {}

    def pytest_collection_modifyitems(self, config: pytest.Config, items: list[pytest.Item]) -> None:
        index, count = self.shard
        self.collected = [item.nodeid for item in items]
        table, default = load_weights(self.weights)
        owner = assign(self.collected, count, table, default)
        kept = [item for item in items if owner[item.nodeid] == index]
        dropped = [item for item in items if owner[item.nodeid] != index]
        self.assigned = [item.nodeid for item in kept]
        config.hook.pytest_deselected(items=dropped)
        items[:] = kept

    def pytest_runtest_logreport(self, report: pytest.TestReport) -> None:
        if report.when == "call":
            outcome = "passed" if report.passed else "failed" if report.failed else "skipped"
        elif report.failed:
            outcome = "error"
        elif report.skipped and report.when == "setup":
            outcome = "skipped"
        else:
            return
        previous = self.outcomes.get(report.nodeid)
        if previous is None or outcome in {"failed", "error"}:
            self.outcomes[report.nodeid] = [outcome, round(report.duration, 2)]

    def pytest_sessionfinish(self, session: pytest.Session, exitstatus: int) -> None:
        index, count = self.shard
        counts = {name: sum(1 for value in self.outcomes.values() if value[0] == name) for name in ("passed", "failed", "error", "skipped")}
        write_json(
            self.output / f"shard_{index}_of_{count}.json",
            {
                "schema": SCHEMA,
                "kind": "shard",
                "status": "passed" if exitstatus == 0 else "failed",
                "shard": index,
                "count": count,
                "context": ci_context(),
                "collected_count": len(self.collected),
                "collected_sha256": sha256_ids(self.collected),
                "assigned": self.assigned,
                "outcomes": self.outcomes,
                "counts": counts,
                "plugin_exit_status": int(exitstatus),
            },
        )


def pytest_addoption(parser: pytest.Parser) -> None:
    group = parser.getgroup("shard")
    group.addoption("--shard", default=None, help="run shard K of N (K/N) of the collected tests")
    group.addoption("--shard-output", default=None, help="folder of the shard manifest")
    group.addoption("--shard-weights", default=str(WEIGHTS), help="seconds of each test, for the balance of the shards")


def pytest_configure(config: pytest.Config) -> None:
    value = config.getoption("--shard")
    if value is None:
        return
    output = config.getoption("--shard-output")
    if not output:
        raise pytest.UsageError("--shard needs --shard-output")
    try:
        shard = parse_shard(str(value))
    except ShardError as error:
        raise pytest.UsageError(str(error)) from error
    config.pluginmanager.register(ShardPlugin(shard, Path(str(output)), Path(str(config.getoption("--shard-weights")))), "acquisition_shard")


def write_json(path: Path, document: dict[str, Any]) -> None:
    """Write ``document`` to ``path`` through a temporary file in the same folder, so a reader never sees half a file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    handle, temporary = tempfile.mkstemp(dir=path.parent, prefix=".manifest_", suffix=".tmp")
    with os.fdopen(handle, "w", encoding="utf-8") as stream:
        stream.write(json.dumps(document, indent=2) + "\n")
    os.replace(temporary, path)


def coverage_files(data: Path, config: Path) -> int:
    """The number of files that the coverage data measured (``coverage json``)."""
    with tempfile.TemporaryDirectory(prefix="coverage_json_") as folder:
        target = Path(folder) / "coverage.json"
        result = run_command(
            sys.executable,
            ["-m", "coverage", "json", "--rcfile", str(config), "--data-file", str(data), "--fail-under=0", "-o", str(target)],
            timeout=COVERAGE_SECONDS,
        )
        if result.returncode != 0:
            raise ShardError(f"coverage cannot read {data.name}: {(result.stdout + result.stderr).strip()[-300:]}")
        return len(json.loads(target.read_text(encoding="utf-8"))["files"])


def coverage_path(directory: Path, index: int) -> Path:
    return directory / "coverage" / f"shard{index}.coverage"


def seal(directory: Path, shard: tuple[int, int], pytest_exit: int, config: Path) -> None:
    """Complete the manifest of a shard after pytest ended; a missing manifest or coverage file makes it a failed one.

    A failed shard raises ShardError after its manifest is written, so the shard job fails itself instead of only the combining gate."""
    index, count = shard
    path = directory / f"shard_{index}_of_{count}.json"
    try:
        manifest = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        manifest = {"schema": SCHEMA, "kind": "shard", "shard": index, "count": count, "context": ci_context(), "assigned": [], "outcomes": {}, "counts": {}}
        pytest_exit = pytest_exit or 1
    manifest["pytest_exit_status"] = pytest_exit
    data = coverage_path(directory, index)
    try:
        manifest["coverage_sha256"], manifest["coverage_files"] = sha256_file(data), coverage_files(data, config)
    except (OSError, ShardError) as error:
        manifest["coverage_error"] = str(error)
        pytest_exit = pytest_exit or 1
    manifest["status"] = "passed" if pytest_exit == 0 and manifest.get("plugin_exit_status") == 0 else "failed"
    write_json(path, manifest)
    if manifest["status"] == "failed":
        cause = manifest.get("coverage_error") or f"pytest exit {pytest_exit}, plugin exit {manifest.get('plugin_exit_status')}"
        raise ShardError(f"shard {index} of {count} sealed as failed: {cause}")


def skip(directory: Path, shard: tuple[int, int], reason: str) -> None:
    index, count = shard
    write_json(
        directory / f"shard_{index}_of_{count}.json",
        {"schema": SCHEMA, "kind": "shard", "status": "skipped", "shard": index, "count": count, "context": ci_context(), "reason": reason},
    )


def read_manifests(directory: Path, count: int) -> list[dict[str, Any]]:
    manifests: list[dict[str, Any]] = []
    for index in range(1, count + 1):
        path = directory / f"shard_{index}_of_{count}.json"
        try:
            loaded = json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError as error:
            raise ShardError(f"shard {index} of {count} is missing: no {path.name}") from error
        except (OSError, ValueError) as error:
            raise ShardError(f"the manifest of shard {index} cannot be read: {error}") from error
        if (
            not isinstance(loaded, dict)
            or loaded.get("schema") != SCHEMA
            or loaded.get("kind") != "shard"
            or not is_int(loaded.get("shard"))
            or loaded.get("shard") != index
            or not is_int(loaded.get("count"))
            or loaded.get("count") != count
        ):
            raise ShardError(f"the manifest of shard {index} is not shard {index} of {count}")
        manifests.append(loaded)
    return manifests


def check_context(manifests: list[dict[str, Any]]) -> None:
    here = ci_context()
    for manifest in manifests:
        if manifest.get("context") != here:
            raise ShardError(f"shard {manifest['shard']} belongs to another CI run: {manifest.get('context')} instead of {here}")


def collect_ids(paths: list[str], root: Path = ROOT) -> list[str]:
    result = run_command(sys.executable, ["-m", "pytest", *paths, "--collect-only", "-q", "-p", "no:cacheprovider"], cwd=root, timeout=PYTEST_SECONDS)
    ids = [line for line in result.stdout.splitlines() if "::" in line]
    if result.returncode != 0 or not ids:
        raise ShardError(f"the gate cannot collect the tests (exit {result.returncode}): {(result.stdout + result.stderr).strip()[-300:]}")
    return ids


def check_fields(manifest: dict[str, Any], expected: frozenset[str]) -> None:
    fields = set(manifest)
    if fields != expected:
        raise ShardError(
            f"shard {manifest['shard']} has other fields than a manifest: unexpected {sorted(fields - expected)}, missing {sorted(expected - fields)}"
        )


def check_outcomes(index: int, outcomes: Any, assigned: list[str], counts: Any) -> None:
    """Every outcome is ``passed`` or ``skipped`` with finite, non-negative seconds and the counters equal the counters recomputed from the outcomes."""
    if not isinstance(outcomes, dict) or not isinstance(counts, dict):
        raise ShardError(f"shard {index} has outcomes or counts that are not records")
    for test, value in outcomes.items():
        valid = (
            isinstance(value, list)
            and len(value) == 2
            and value[0] in OUTCOMES
            and type(value[1]) in (int, float)
            and math.isfinite(value[1])
            and value[1] >= 0
        )
        if not valid:
            raise ShardError(f"shard {index} has an invalid outcome for {test}: {value!r}")
        if value[0] not in PASSING_OUTCOMES:
            raise ShardError(f"shard {index} test {test} has outcome {value[0]}")
    if counts.get("failed") or counts.get("error"):
        raise ShardError(f"shard {index} has {counts.get('failed', 0)} failed and {counts.get('error', 0)} errored tests")
    if set(outcomes) != set(assigned):
        raise ShardError(f"shard {index} did not report every assigned test: {len(outcomes)} reported, {len(assigned)} assigned")
    recomputed = {name: sum(1 for value in outcomes.values() if value[0] == name) for name in OUTCOMES}
    if counts != recomputed or not all(is_int(value) for value in counts.values()):
        raise ShardError(f"shard {index} counts {counts} differ from the counts {recomputed} of its outcomes")


def check_passed_shards(manifests: list[dict[str, Any]], paths: list[str], root: Path, weights: Path) -> None:
    collected = collect_ids(paths, root)
    expected_hash = sha256_ids(collected)
    seen: dict[str, int] = {}
    for manifest in manifests:
        index = manifest["shard"]
        if (
            manifest.get("status") != "passed"
            or not is_int(manifest.get("pytest_exit_status"))
            or manifest.get("pytest_exit_status") != 0
            or not is_int(manifest.get("plugin_exit_status"))
            or manifest.get("plugin_exit_status") != 0
        ):
            raise ShardError(f"shard {index} failed: status {manifest.get('status')}, exit {manifest.get('pytest_exit_status')}")
        check_fields(manifest, RUN_FIELDS)
        if (
            manifest.get("collected_sha256") != expected_hash
            or manifest.get("collected_count") != len(collected)
            or not is_int(manifest.get("collected_count"))
        ):
            raise ShardError(f"shard {index} collected other tests than the gate: {manifest.get('collected_count')} against {len(collected)}")
        assigned = manifest["assigned"]
        if not isinstance(assigned, list) or not all(isinstance(test, str) for test in assigned) or len(set(assigned)) != len(assigned):
            raise ShardError(f"shard {index} lists its assigned tests wrongly: not a list of distinct ids")
        check_outcomes(index, manifest["outcomes"], assigned, manifest["counts"])
        for test in assigned:
            if test in seen:
                raise ShardError(f"{test} is assigned to shards {seen[test]} and {index}")
            seen[test] = index
    missing = sorted(set(collected) - set(seen))
    extra = sorted(set(seen) - set(collected))
    if missing or extra:
        raise ShardError(f"the shards ran {len(seen)} tests and the gate collects {len(collected)}; not run: {missing[:3]}; not collected: {extra[:3]}")
    table, default = load_weights(weights)
    owner = assign(collected, len(manifests), table, default)
    for manifest in manifests:
        wanted = sorted(test for test, number in owner.items() if number == manifest["shard"])
        if sorted(manifest["assigned"]) != wanted:
            raise ShardError(f"shard {manifest['shard']} ran {len(manifest['assigned'])} tests and the gate assigns it {len(wanted)} other ones")


def merge_coverage(directory: Path, manifests: list[dict[str, Any]], config: Path, fail_under: float) -> str:
    with tempfile.TemporaryDirectory(prefix="coverage_merge_") as folder:
        inputs: list[str] = []
        for manifest in manifests:
            source = coverage_path(directory, manifest["shard"])
            if not source.is_file() or sha256_file(source) != manifest.get("coverage_sha256"):
                raise ShardError(f"the coverage data of shard {manifest['shard']} is missing or differs from its manifest")
            target = Path(folder) / f"shard{manifest['shard']}.coverage"
            shutil.copy2(source, target)
            measured = coverage_files(target, config)
            if not is_int(manifest.get("coverage_files")) or manifest["coverage_files"] != measured:
                raise ShardError(
                    f"the coverage data of shard {manifest['shard']} measures {measured} files and its manifest says {manifest.get('coverage_files')}"
                )
            inputs.append(str(target))
        merged = Path(folder) / "merged.coverage"
        combined = run_command(
            sys.executable, ["-m", "coverage", "combine", "--rcfile", str(config), "--data-file", str(merged), *inputs], timeout=COVERAGE_SECONDS
        )
        if combined.returncode != 0:
            raise ShardError(f"coverage cannot merge the shards: {(combined.stdout + combined.stderr).strip()[-300:]}")
        if coverage_files(merged, config) < max(int(manifest["coverage_files"]) for manifest in manifests):
            raise ShardError("the merged coverage measures fewer files than one shard did")
        report = run_command(
            sys.executable,
            ["-m", "coverage", "report", "--rcfile", str(config), "--data-file", str(merged), "--show-missing", f"--fail-under={fail_under}"],
            timeout=COVERAGE_SECONDS,
        )
        text = report.stdout + report.stderr
        if report.returncode != 0:
            raise ShardError(f"the merged coverage is below {fail_under}% (or the report failed):\n{text.strip()[-1500:]}")
        return text


def check_skipped(manifest: dict[str, Any]) -> None:
    check_fields(manifest, SKIPPED_FIELDS)
    if not isinstance(manifest["reason"], str) or not manifest["reason"].strip():
        raise ShardError(f"shard {manifest['shard']} is skipped without a reason")


def gate(directory: Path, count: int, expect: str, fail_under: float, paths: list[str], config: Path, root: Path = ROOT, weights: Path = WEIGHTS) -> str:
    """Return the report of a passing gate; raise ShardError naming the first reason otherwise."""
    manifests = read_manifests(directory, count)
    check_context(manifests)
    statuses = {manifest.get("status") for manifest in manifests}
    if expect == "skip":
        if statuses != {"skipped"}:
            raise ShardError(f"this gate expects every shard to be skipped, and the statuses are {sorted(map(str, statuses))}")
        for manifest in manifests:
            check_skipped(manifest)
        return f"All {count} shards were skipped: {manifests[0].get('reason')}"
    if "skipped" in statuses:
        raise ShardError(f"this gate expects the checks to run, and the statuses are {sorted(map(str, statuses))}")
    check_passed_shards(manifests, paths, root, weights)
    return merge_coverage(directory, manifests, config, fail_under)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("seal", "skip"):
        sub = commands.add_parser(name)
        sub.add_argument("--directory", type=Path, required=True)
        sub.add_argument("--shard", required=True)
        sub.add_argument("--coverage-config", type=Path, default=COVERAGE_CONFIG)
        sub.add_argument("--pytest-exit", type=int, default=1)
        sub.add_argument("--reason", default="")
    sub = commands.add_parser("gate")
    sub.add_argument("--directory", type=Path, required=True)
    sub.add_argument("--count", type=int, required=True)
    sub.add_argument("--expect", choices=("run", "skip"), required=True)
    sub.add_argument("--fail-under", type=float, default=90.0)
    sub.add_argument("--coverage-config", type=Path, default=COVERAGE_CONFIG)
    sub.add_argument("--root", type=Path, default=ROOT, help="folder the test paths are relative to")
    sub.add_argument("--weights", type=Path, default=WEIGHTS, help="seconds of each test, as the shards were given")
    sub.add_argument("paths", nargs="*")
    args = parser.parse_args(argv)
    try:
        if args.command == "gate":
            sys.stdout.write(gate(args.directory, args.count, args.expect, args.fail_under, args.paths, args.coverage_config, args.root, args.weights) + "\n")
        elif args.command == "seal":
            seal(args.directory, parse_shard(args.shard), args.pytest_exit, args.coverage_config)
        else:
            skip(args.directory, parse_shard(args.shard), args.reason)
    except (ShardError, OSError, ValueError, KeyError) as error:
        sys.stderr.write(f"acquisition shards: {error}\n")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
