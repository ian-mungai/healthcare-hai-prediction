"""E2E of the owner's layer acceptance command: a good silver and gold record passes, and each planted defect is refused by name.

    .venv/bin/python -m scripts.lakehouse.run_layer_acceptance_e2e --out <NEW_DIRECTORY>

The records are synthetic. Each defect case copies the good record, breaks one thing, and requires the report to fail the check that names it."""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import io
import json
import os
import re
import shutil
import sys
import tempfile
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any
from unittest import mock

from scripts.lakehouse import layer_acceptance as acceptance
from scripts.lakehouse import pins

ROOT = Path(__file__).resolve().parents[2]
RUN = "20261031T120000Z-abcdef12"
INPUT_RUN = "20261031T080000Z-12345678"
CODE = "a" * 64
TABLES = ("hospital_measures", "state_measures")


def write(path: Path, document: Any) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = (json.dumps(document, indent=2, sort_keys=True) + "\n").encode()
    path.write_bytes(raw)
    return hashlib.sha256(raw).hexdigest()


def evidence(folder: Path, layer: str) -> None:
    """A run folder that follows every rule: the digests chain from the candidates through the reconcile and the backup to the commit."""
    prefix = acceptance.INPUT_PREFIX[layer]
    snapshots = {name: 1000 + index for index, name in enumerate(TABLES)}
    write(folder / "layer_run.json", {"run_id": RUN, "layer": layer, "mode": "real", "input_listing": {"prefix": prefix, "versions": 3, "sha256": "b" * 64}})
    write(folder / "publish/export/manifest.json", {"tables": {name: {"rows": 10 + index} for index, name in enumerate(TABLES)}})
    candidates = write(
        folder / "publish/candidates.json",
        {"complete": True, "tables": {name: {"candidate_snapshot_id": snapshot, "base_snapshot_id": 1} for name, snapshot in snapshots.items()}},
    )
    reconcile_tables = {
        name: {
            "candidate_snapshot_id": snapshot,
            "rows": 10 + index,
            "export_rows": 10 + index,
            "manifest_rows": 10 + index,
            "fingerprint": str(777 + index),
            "export_fingerprint": str(777 + index),
        }
        for index, (name, snapshot) in enumerate(snapshots.items())
    }
    reconcile = write(folder / "publish/reconcile.json", {"pass": True, "run_id": RUN, "candidates_sha256": candidates, "tables": reconcile_tables})
    backup = write(folder / "publish/backup.json", {"read_back": True, "reconcile_sha256": reconcile, "backup_sha256": "c" * 64, "name": "backup"})
    write(folder / "publish/commit.json", {"state": "committed", "run_id": RUN, "backup_sha256": backup, "snapshot_set": snapshots})
    if layer == "silver":
        write(folder / "pins.json", {name: 5 for name in pins.mapped_tables(ROOT / pins.TABLE_MAP)})
        write(folder / "silver_snapshot.json", {"schema": 1, "run_id": RUN, "mode": "real", "namespace": "silver", "snapshot_set": snapshots})
    else:
        write(folder / "silver_pins.json", {"source_run_id": INPUT_RUN, "snapshot_set": {"hospital_measures": 7}})


def listings(folder: Path, layer: str) -> None:
    prefix = acceptance.INPUT_PREFIX[layer]
    inputs = [{"key": f"{prefix}file_{index}", "version": f"v{index}", "etag": '"e"', "bytes": 5} for index in range(3)]
    old = [{"key": f"{acceptance.WRITE_PREFIXES[layer][0]}old", "version": "o1", "etag": '"o"', "bytes": 9}]
    write(folder / "before.json", {"versions": inputs + old, "deleted": None})
    added = [{"key": f"{acceptance.WRITE_PREFIXES[layer][0]}table/data.parquet", "version": "n1", "etag": '"n"', "bytes": 9}]
    added += [{"key": f"lakehouse/layer_runs/{layer}/{RUN}/layer_run.json", "version": "n2", "etag": '"n"', "bytes": 9}]
    write(folder / "after.json", {"versions": inputs + old + added, "deleted": []})


def environment(folder: Path, layer: str) -> None:
    evidence(folder / "evidence", layer)
    listings(folder, layer)
    containers = [{"name": name, "lastStatus": "STOPPED", "exitCode": 0} for name in acceptance.expected_containers(layer)]
    write(
        folder / "describe.json",
        {
            "failures": [],
            "tasks": [{"taskArn": "arn:task", "lastStatus": "STOPPED", "createdAt": "2026-10-31T10:00:00.000000-07:00", "containers": containers}],
        },
    )
    write(folder / "approval.json", {"schema": 1, "layer": layer, "run_id": RUN, "code_sha256": CODE, "approved_at": "2026-10-31T16:50:00Z"})
    write(folder / "running.json", {"taskArns": []})
    write(folder / "host.json", {"desired_capacity": 0, "group_instances": 0, "active_container_instances": 0, "services": {}})
    if layer == "gold":
        write(folder / "silver/silver_snapshot.json", {"schema": 1, "run_id": INPUT_RUN, "snapshot_set": {"hospital_measures": 7}})


def run_cli(arguments: list[str], variables: Mapping[str, str] | None = None) -> tuple[int, str, str]:
    out, err = io.StringIO(), io.StringIO()
    old = {key: os.environ.pop(key, None) for key in ("CONFIRM_APPROVE", "CONFIRM_SIGN")}
    os.environ.update(variables or {})
    try:
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            try:
                code = acceptance.main(arguments)
            except SystemExit as stop:
                code = int(stop.code or 0)
    finally:
        for key in old:
            os.environ.pop(key, None)
        os.environ.update({key: value for key, value in old.items() if value is not None})
    return code, out.getvalue(), err.getvalue()


def report_arguments(folder: Path, layer: str, out: Path) -> list[str]:
    files = {
        "approval": "approval.json",
        "describe": "describe.json",
        "evidence": "evidence",
        "before": "before.json",
        "after": "after.json",
        "running-tasks": "running.json",
        "host-status": "host.json",
    }
    arguments = ["report", "--layer", layer, "--run", RUN, "--out", str(out)]
    for flag, name in files.items():
        arguments += [f"--{flag}", str(folder / name)]
    return arguments + (["--input-evidence", str(folder / "silver")] if layer == "gold" else [])


def failed_checks(folder: Path, layer: str, out: Path) -> tuple[int, list[str]]:
    code, _, _ = run_cli(report_arguments(folder, layer, out))
    document = json.loads(out.read_text()) if out.is_file() else {}
    return code, sorted(name for name, ok in document.get("checks", {}).items() if not ok)


def edit(path: Path, change: Callable[[Any], None]) -> None:
    document = json.loads(path.read_text())
    change(document)
    path.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n")


def rechain(folder: Path) -> None:
    """Rewrite the digests after a change to a record, so a case breaks one rule and not the chain."""
    evidence_folder = folder / "evidence/publish"
    candidates = hashlib.sha256((evidence_folder / "candidates.json").read_bytes()).hexdigest()
    edit(evidence_folder / "reconcile.json", lambda d: d.update(candidates_sha256=candidates))
    reconcile = hashlib.sha256((evidence_folder / "reconcile.json").read_bytes()).hexdigest()
    edit(evidence_folder / "backup.json", lambda d: d.update(reconcile_sha256=reconcile))
    backup = hashlib.sha256((evidence_folder / "backup.json").read_bytes()).hexdigest()
    edit(evidence_folder / "commit.json", lambda d: d.update(backup_sha256=backup))


def rewrite(folder: Path, name: str, change: Callable[[Any], None]) -> None:
    """Change one record of the run and rewrite the digest chain behind it."""
    edit(folder / "evidence" / name, change)
    rechain(folder)


def defects(layer: str) -> dict[str, tuple[Any, ...]]:
    """Each case: the change to a good environment and the check that must fail."""
    cases: dict[str, tuple[Any, ...]] = {
        "a table's row count differs from its export": (
            lambda f: rewrite(f, "publish/reconcile.json", lambda d: d["tables"]["state_measures"].update(rows=99)),
            "the row count of every table equals its candidate and its export",
        ),
        "a content fingerprint differs from its export": (
            lambda f: rewrite(f, "publish/reconcile.json", lambda d: d["tables"]["state_measures"].update(fingerprint="5")),
            "the content fingerprint of every table equals its export",
        ),
        "the reconcile record is not a pass": (
            lambda f: rewrite(f, "publish/reconcile.json", lambda d: d.update({"pass": False})),
            "the candidates are complete and the reconcile record is a pass for them",
        ),
        "the backup was not read back": (
            lambda f: rewrite(f, "publish/backup.json", lambda d: d.update(read_back=False)),
            "the backup was read back after the reconcile",
        ),
        "the commit is not committed": (
            lambda f: edit(f / "evidence/publish/commit.json", lambda d: d.update(state="pending")),
            "publish_runs was written last: the commit record follows the backup and is committed",
        ),
        "the commit misses a snapshot ID": (
            lambda f: edit(f / "evidence/publish/commit.json", lambda d: d["snapshot_set"].pop("state_measures")),
            "the commit names every candidate snapshot ID",
            "the silver record equals the commit and names the silver namespace",
        ),
        "a container exited nonzero": (
            lambda f: edit(f / "describe.json", lambda d: d["tasks"][0]["containers"][3].update(exitCode=1)),
            "every container of the task exited 0 and the task stopped",
        ),
        "a container has no result": (
            lambda f: edit(f / "describe.json", lambda d: d["tasks"][0]["containers"].pop()),
            "every container of the task exited 0 and the task stopped",
        ),
        "the task is still running": (
            lambda f: edit(f / "describe.json", lambda d: d["tasks"][0].update(lastStatus="RUNNING")),
            "every container of the task exited 0 and the task stopped",
        ),
        "the run was approved after the task was created": (
            lambda f: edit(f / "approval.json", lambda d: d.update(approved_at="2026-10-31T18:00:00Z")),
            "the owner approved this run ID before the task was created",
        ),
        "an input object version changed": (
            lambda f: edit(f / "after.json", lambda d: d["versions"][0].update(version="changed")),
            "H2: the input object versions are the same before and after the task",
            "the task added versions only under the layer's prefixes and its own run prefix",
            "the task removed no version",
        ),
        "the pin recorded another version count": (
            lambda f: edit(f / "evidence/layer_run.json", lambda d: d["input_listing"].update(versions=4)),
            "H2: the pin recorded the version count of the listing taken before the task",
        ),
        "the task wrote outside the layer's prefixes": (
            lambda f: edit(f / "after.json", lambda d: d["versions"].append({"key": "lakehouse/other/x", "version": "z", "etag": '"z"', "bytes": 1})),
            "the task added versions only under the layer's prefixes and its own run prefix",
        ),
        "a version disappeared": (
            lambda f: edit(f / "after.json", lambda d: d["versions"].pop(3)),
            "the task removed no version",
        ),
        "a task of the job is still running": (
            lambda f: write(f / "running.json", {"taskArns": ["arn:other"]}),
            "no task of the job is left running",
        ),
        "the host is still on": (
            lambda f: edit(f / "host.json", lambda d: d.update(group_instances=1)),
            "the host is stopped and verified: no capacity, group instance or container instance",
        ),
    }
    if layer == "silver":
        cases["the pin misses a bronze table"] = (
            lambda f: edit(f / "evidence/pins.json", lambda d: d.pop(sorted(d)[0])),
            "H3: the pinned table set is the committed bronze table map",
        )
        cases["the silver record names another namespace"] = (
            lambda f: edit(f / "evidence/silver_snapshot.json", lambda d: d.update(namespace="silver_e2e")),
            "the silver record equals the commit and names the silver namespace",
        )
    else:
        cases["the gold pin names another silver run"] = (
            lambda f: edit(f / "evidence/silver_pins.json", lambda d: d.update(source_run_id="20261030T000000Z-00000000")),
            "H3: the pin names the accepted silver run and its complete snapshot set",
        )
    return cases


def good_case(work: Path, layer: str) -> dict[str, bool]:
    folder = work / f"good_{layer}"
    environment(folder, layer)
    out = folder / "report.json"
    code, failed = failed_checks(folder, layer, out)
    checks = {f"{layer}: the good record passes": code == 0 and not failed}
    document = json.loads(out.read_text()) if out.is_file() else {}
    digest = hashlib.sha256(out.read_bytes()).hexdigest() if out.is_file() else ""
    wanted = f"sign-{layer}-{RUN}-{digest[:12]}"
    wrong, _, _ = run_cli(["sign", "--report", str(out), "--owner", "Owner"], {"CONFIRM_SIGN": "sign-other"})
    checks[f"{layer}: a wrong confirmation does not sign"] = wrong == 1 and not out.with_suffix(".signed.json").exists()
    signed, _, _ = run_cli(["sign", "--report", str(out), "--owner", "Owner"], {"CONFIRM_SIGN": wanted})
    signature = json.loads(out.with_suffix(".signed.json").read_text()) if out.with_suffix(".signed.json").exists() else {}
    checks[f"{layer}: the typed confirmation signs the report digest"] = (
        signed == 0 and signature.get("report_sha256") == digest and bool(document.get("passed"))
    )
    checks[f"{layer}: a second signature is refused"] = run_cli(["sign", "--report", str(out), "--owner", "Owner"], {"CONFIRM_SIGN": wanted})[0] == 1
    return checks


def defect_cases(work: Path, layer: str) -> dict[str, bool]:
    checks: dict[str, bool] = {}
    for label, (change, *wanted) in defects(layer).items():
        folder = work / f"{layer}_{re.sub('[^a-z]+', '_', label)}"
        environment(folder, layer)
        change(folder)
        out = folder / "report.json"
        code, failed = failed_checks(folder, layer, out)
        expected = sorted(name for name in wanted if layer == "silver" or not name.startswith("the silver record"))
        checks[f"{layer}: {label}"] = code == 1 and failed == expected
        if code == 1:
            signed, _, _ = run_cli(
                ["sign", "--report", str(out), "--owner", "Owner"], {"CONFIRM_SIGN": f"sign-{layer}-{RUN}-{hashlib.sha256(out.read_bytes()).hexdigest()[:12]}"}
            )
            checks[f"{layer}: {label}: the failed report is not signed"] = signed == 1
    return checks


def command_cases(work: Path) -> dict[str, bool]:
    folder = work / "commands"
    environment(folder, "silver")
    describe = ["task", "--layer", "silver", "--describe", f"{folder}/describe.json"]
    checks = {"task: a good result passes": run_cli(describe)[0] == 0}
    edit(folder / "describe.json", lambda d: d.update(failures=[{"reason": "MISSING"}]))
    checks["task: a failures list is refused"] = run_cli(describe)[0] == 1
    edit(folder / "describe.json", lambda d: d.update(failures=[], tasks=[]))
    checks["task: no task is refused"] = run_cli(describe)[0] == 1
    approval = folder / "new_approval.json"
    approve = ["approve", "--layer", "silver", "--run", RUN, "--code-sha256", CODE, "--out", str(approval)]
    checks["approve: without the typed text nothing is written"] = run_cli(approve)[0] == 1 and not approval.exists()
    checks["approve: the typed text writes the approval"] = (
        run_cli(approve, {"CONFIRM_APPROVE": f"approve-silver-{RUN}-{CODE[:12]}"})[0] == 0 and approval.exists()
    )
    checks["approve: a second approval does not overwrite"] = run_cli(approve, {"CONFIRM_APPROVE": f"approve-silver-{RUN}-{CODE[:12]}"})[0] == 1
    tracked, outside = ROOT / "docs/layer_acceptance_report_must_not_exist.json", Path(tempfile.gettempdir()) / "layer_acceptance_outside_the_repository.json"
    environment(folder / "second", "silver")
    checks["report: a path that Git tracks is refused"] = run_cli(report_arguments(folder / "second", "silver", tracked))[0] == 1 and not tracked.exists()
    checks["report: a path outside the repository is refused"] = (
        run_cli(report_arguments(folder / "second", "silver", outside))[0] == 1 and not outside.exists()
    )
    checks["report: Git ignores the data and plans folders"] = acceptance.ignored(ROOT / "data/e2e/x.json") and acceptance.ignored(ROOT / "plans/x/signed.json")
    return checks


def plan_events(layer: str, **changes: Any) -> list[dict[str, str]]:
    """The log events of a dry run publish stream: the lines the task prints, then the whole plan as one JSON line."""
    plan: dict[str, Any] = {
        "dry_run": True,
        "layer": layer,
        "run": RUN,
        "mode": "real",
        "namespace": layer,
        "summary": {"tables": 2},
        "tables": [],
        "blocked": [],
        "unknown": [],
        "catalog_error": "",
    } | changes
    lines = [f"dry run plan for {layer} run {RUN} (mode real, namespace {layer}): nothing was written", "CREATE    silver.hospital_measures: write 10 rows"]
    lines += [f"UNKNOWN   {item}" for item in plan["unknown"]] + [f"BLOCKED   {item}" for item in plan["blocked"]]
    lines += [f"CATALOG   the catalog could not be read: {plan['catalog_error']}"] if plan["catalog_error"] else []
    return [{"timestamp": "1", "message": line} for line in [*lines, "DRY_RUN_PLAN " + json.dumps(plan, sort_keys=True, separators=(",", ":"))]]


def plan_cases(work: Path) -> dict[str, bool]:
    """The plan check reads this run's plan from the saved log events and refuses an unknown, a catalog error, a blocked plan and another run."""
    folder = work / "plans"
    folder.mkdir()
    checks: dict[str, bool] = {}
    wrong_run = {"run": "20261030T000000Z-00000000"}
    cases = {
        "a complete plan": (plan_events("silver"), 0),
        "an unknown table": (plan_events("silver", unknown=["silver.state_measures"]), 1),
        "a catalog error": (plan_events("silver", catalog_error="timeout"), 1),
        "a blocked publish": (plan_events("silver", blocked=["this run is already committed"]), 1),
        "another run's plan": (plan_events("silver", **wrong_run), 1),
        "a plan for another layer": (plan_events("gold"), 1),
        "no plan line": (plan_events("silver")[:-1], 1),
    }
    for label, (events, expected) in cases.items():
        path = folder / (re.sub("[^a-z]+", "_", label) + ".json")
        path.write_text(json.dumps({"events": events}))
        code, _, _ = run_cli(["plan", "--layer", "silver", "--run", RUN, "--log", str(path)])
        checks[f"plan: {label}"] = code == expected
    return checks


def static_cases() -> dict[str, bool]:
    """The command's constants equal the files they restate."""
    writer = (ROOT / "infra/iam/job_writer.tf").read_text()
    prefixes = {
        layer: {f"lakehouse/{name}/" for name in re.findall(r'"([a-z_]+)"', body)}
        for layer, body in re.findall(r"(silver|gold) = \{\s*prefixes\s*=\s*\[([^\]]*)\]", writer)
    }
    broker = json.loads(acceptance.JOBS_FILE.read_text())["jobs"]
    return {
        "the write prefixes equal the layer writer roles": all(
            set(acceptance.WRITE_PREFIXES[layer]) - {"metadata/dbt/"} == prefixes[layer] for layer in acceptance.LAYERS
        ),
        "the gold role may also write the dbt metadata": '"${local.bronze_bucket_arn}/metadata/dbt/*"' in writer,
        "the expected containers are init and the broker table's containers": all(
            acceptance.expected_containers(layer) == ["init", *broker[layer]["containers"]] for layer in acceptance.LAYERS
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--out", type=Path, required=True, help="a new directory for the report")
    args = parser.parse_args()
    out = args.out.expanduser().absolute()
    out.mkdir(mode=0o700, parents=True, exist_ok=False)
    work = Path(tempfile.mkdtemp(prefix="layer_acceptance_", dir=out))
    results: dict[str, dict[str, bool]] = {"static": static_cases()}
    try:
        with mock.patch.object(acceptance, "ignored", lambda path: str(path).startswith(str(work))):
            for layer in acceptance.LAYERS:
                results[f"good {layer}"] = good_case(work, layer)
                results[f"defects {layer}"] = defect_cases(work, layer)
        results["commands"] = command_cases(work)
        results["plan"] = plan_cases(work)
    finally:
        shutil.rmtree(work, ignore_errors=True)
    passed = all(all(checks.values()) for checks in results.values())
    (out / "report.json").write_text(json.dumps({"passed": passed, "results": results}, indent=2, sort_keys=True) + "\n")
    for name, checks in results.items():
        for check, ok in checks.items():
            sys.stdout.write(f"{'PASS' if ok else 'FAIL'} {name}: {check}\n")
    total = sum(len(checks) for checks in results.values())
    sys.stdout.write(f"layer acceptance E2E: {sum(ok for checks in results.values() for ok in checks.values())} of {total} checks passed\n")
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
