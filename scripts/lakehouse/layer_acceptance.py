"""The owner's acceptance of a first layer publish: the run approval, the task result, the evidence checks and the signed local report.

Run from the repository root with synthetic or real files that the owner saved from AWS read calls. Nothing here calls AWS.

    python -m scripts.lakehouse.layer_acceptance approve --layer silver --run <RUN_ID> --code-sha256 <SHA256> --out <FILE>
    python -m scripts.lakehouse.layer_acceptance task --layer silver --describe <FILE>
    python -m scripts.lakehouse.layer_acceptance plan --layer silver --run <RUN_ID> --log <FILE>
    python -m scripts.lakehouse.layer_acceptance report --layer silver --run <RUN_ID> --approval <FILE> --describe <FILE> --evidence <DIR>
        --before <FILE> --after <FILE> --running-tasks <FILE> --host-status <FILE> [--input-evidence <DIR>] --out <FILE>
    CONFIRM_SIGN=<TEXT THAT REPORT PRINTS> python -m scripts.lakehouse.layer_acceptance sign --report <FILE> --owner <NAME>

``approve`` records the owner's approval of the run ID and the code digest before the task starts, after the owner types the confirmation text it
prints. ``task`` requires one stopped task whose every container ran to a zero exit. ``plan`` reads the saved log events of the dry run publish stream
and requires the one plan line of this run, with no unknown state, catalog error or blocked publish. ``report`` checks the evidence of the run and
writes the report only to a Git-ignored path: it names the checks, the digests of every file it read and the result. The checks repeat the acceptance
rules of the layer driver from the uploaded files, so a driver defect cannot vouch for itself: the rows and content fingerprints of every candidate
against its export, the commit record that follows the backup and the reconcile and names every snapshot ID, the pinned table set (H3) and the input
object versions before and after (H2). They also check that the pin's version count equals the listing taken before the task, that the record names
the layer's own namespace and that every object version the task added lies under a prefix the layer writer role may write. A report that passed is
signed with ``sign``: the owner sets ``CONFIRM_SIGN`` to the text that ``report`` printed, which names the layer, the run and the digest of the
report. The signature is that typed confirmation beside the report digest and the owner's name. It uses no key. Signed reports stay local (owner
decision 92)."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.lakehouse import pins
from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]
JOBS_FILE = ROOT / "config/orchestration/ecs_broker_jobs.json"
LAYERS = ("silver", "gold")
INPUT_PREFIX = {"silver": "lakehouse/bronze/", "gold": "lakehouse/silver/"}
# The prefixes of the layer writer role (infra/iam/job_writer.tf) beside the run's own upload prefix.
WRITE_PREFIXES = {"silver": ("lakehouse/silver/",), "gold": ("lakehouse/gold/", "lakehouse/catalog_backups/", "metadata/dbt/")}
PIN_FILE = {"silver": "pins.json", "gold": "silver_pins.json"}
NAMESPACE = {"silver": "silver", "gold": "gold"}
RUN_ID = re.compile(r"[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}")
SHA256 = re.compile(r"[a-f0-9]{64}")
FINGERPRINT = re.compile(r"-?[0-9]{1,38}")
EVIDENCE_FILES = (
    "layer_run.json",
    "publish/candidates.json",
    "publish/reconcile.json",
    "publish/backup.json",
    "publish/commit.json",
    "publish/export/manifest.json",
)


class AcceptanceError(RuntimeError):
    """A file is missing or malformed, or the owner's confirmation is wrong."""


def read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        raise AcceptanceError(f"{path}: cannot be read as JSON ({type(error).__name__})") from None


def file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def expected_containers(layer: str) -> list[str]:
    """The init container and every container of the layer's broker table: each must exit 0."""
    return ["init", *read_json(JOBS_FILE)["jobs"][layer]["containers"]]


def task_problems(layer: str, described: Any) -> list[str]:
    """What is wrong with the output of ``aws ecs describe-tasks`` for the run's one task."""
    if not isinstance(described, dict):
        return ["the describe output is not an object"]
    problems = [f"the describe call reported failures: {described['failures']}"] if described.get("failures") else []
    tasks = described.get("tasks")
    if not isinstance(tasks, list) or len(tasks) != 1:
        return [*problems, "the describe output must hold exactly one task"]
    task = tasks[0]
    if task.get("lastStatus") != "STOPPED":
        problems.append(f"the task is {task.get('lastStatus')}, not STOPPED")
    containers = {item.get("name"): item for item in task.get("containers", []) if isinstance(item, dict)}
    for name in expected_containers(layer):
        code = containers.get(name, {}).get("exitCode")
        if name not in containers:
            problems.append(f"container {name} has no result")
        elif type(code) is not int or code != 0:
            problems.append(f"container {name} did not exit 0 (exit {code}, reason {containers[name].get('reason')})")
    return problems


def listing_entries(path: Path) -> tuple[dict[tuple[str, str], tuple[Any, ...]], dict[tuple[str, str], tuple[Any, ...]]]:
    """The versions and delete markers of an ``aws s3api list-object-versions`` result, keyed by object key and version ID."""
    document = read_json(path)
    if not isinstance(document, dict):
        raise AcceptanceError(f"{path}: the listing is not an object")
    versions = {(item["key"], item["version"]): (item.get("etag"), item.get("bytes")) for item in document.get("versions") or []}
    deleted = {(item["key"], item["version"]): () for item in document.get("deleted") or []}
    return versions, deleted


def listing_checks(layer: str, run: str, before: Path, after: Path, recorded: Mapping[str, Any]) -> dict[str, bool]:
    """H2 and the write scope from two listings of ``lakehouse/`` (and ``metadata/dbt/`` for gold): one before the task and one after."""
    versions_before, deleted_before = listing_entries(before)
    versions_after, deleted_after = listing_entries(after)
    prefix = INPUT_PREFIX[layer]

    def held(entries: Mapping[tuple[str, str], tuple[Any, ...]]) -> dict[tuple[str, str], tuple[Any, ...]]:
        return {key: value for key, value in entries.items() if key[0].startswith(prefix)}

    h2 = held(versions_before) == held(versions_after) and held(deleted_before) == held(deleted_after)
    count = len(held(versions_before)) + len(held(deleted_before))
    allowed = (*WRITE_PREFIXES[layer], f"lakehouse/layer_runs/{layer}/{run}/")
    added = [key[0] for key in (*versions_after.keys() - versions_before.keys(), *deleted_after.keys() - deleted_before.keys())]
    removed = (versions_before.keys() - versions_after.keys()) | (deleted_before.keys() - deleted_after.keys())
    return {
        "H2: the input object versions are the same before and after the task": h2,
        "H2: the pin recorded the version count of the listing taken before the task": recorded.get("prefix") == prefix and recorded.get("versions") == count,
        "the task added versions only under the layer's prefixes and its own run prefix": bool(added) and all(key.startswith(allowed) for key in added),
        "the task removed no version": not removed,
    }


def evidence_checks(layer: str, run: str, folder: Path, input_folder: Path | None) -> tuple[dict[str, bool], dict[str, dict[str, Any]]]:
    """The acceptance rules of the layer driver, applied again to the uploaded run records."""
    missing = [name for name in (*EVIDENCE_FILES, PIN_FILE[layer], *(("silver_snapshot.json",) if layer == "silver" else ())) if not (folder / name).is_file()]
    if missing:
        raise AcceptanceError(f"the evidence folder lacks {missing}")
    identity, candidates, reconcile, backup, commit, manifest = (read_json(folder / name) for name in EVIDENCE_FILES)
    pinned = read_json(folder / PIN_FILE[layer])
    tables = set(candidates.get("tables", {}))
    summary: dict[str, dict[str, Any]] = {}
    rows_ok = fingerprints_ok = True
    for name in sorted(tables):
        entry, candidate = reconcile.get("tables", {}).get(name, {}), candidates["tables"][name]
        rows = entry.get("rows")
        rows_ok = (
            rows_ok
            and type(rows) is int
            and rows == entry.get("export_rows") == entry.get("manifest_rows") == manifest.get("tables", {}).get(name, {}).get("rows")
        )
        fingerprints_ok = (
            fingerprints_ok and entry.get("fingerprint") == entry.get("export_fingerprint") and bool(FINGERPRINT.fullmatch(str(entry.get("fingerprint"))))
        )
        rows_ok = rows_ok and entry.get("candidate_snapshot_id") == candidate.get("candidate_snapshot_id")
        summary[name] = {"rows": rows, "fingerprint": entry.get("fingerprint"), "snapshot_id": commit.get("snapshot_set", {}).get(name)}
    wanted = {name: item["candidate_snapshot_id"] for name, item in candidates.get("tables", {}).items()}
    checks = {
        "the run records belong to this run and layer in real mode": (identity.get("run_id"), identity.get("layer"), identity.get("mode"))
        == (run, layer, "real"),
        "the candidates are complete and the reconcile record is a pass for them": candidates.get("complete") is True
        and reconcile.get("pass") is True
        and reconcile.get("run_id") == run
        and reconcile.get("candidates_sha256") == file_sha256(folder / "publish/candidates.json")
        and set(reconcile.get("tables", {})) == tables == set(manifest.get("tables", {})),
        "the row count of every table equals its candidate and its export": bool(tables) and rows_ok,
        "the content fingerprint of every table equals its export": bool(tables) and fingerprints_ok,
        "the backup was read back after the reconcile": backup.get("read_back") is True
        and backup.get("reconcile_sha256") == file_sha256(folder / "publish/reconcile.json"),
        "publish_runs was written last: the commit record follows the backup and is committed": commit.get("state") == "committed"
        and commit.get("run_id") == run
        and commit.get("backup_sha256") == file_sha256(folder / "publish/backup.json"),
        "the commit names every candidate snapshot ID": commit.get("snapshot_set") == wanted and bool(wanted),
    }
    if layer == "silver":
        record = read_json(folder / "silver_snapshot.json")
        checks["the silver record equals the commit and names the silver namespace"] = (
            record.get("run_id") == run
            and record.get("mode") == "real"
            and record.get("namespace") == NAMESPACE[layer]
            and record.get("snapshot_set") == commit.get("snapshot_set")
        )
        checks["H3: the pinned table set is the committed bronze table map"] = set(pinned) == set(pins.mapped_tables(ROOT / pins.TABLE_MAP))
    else:
        producer = read_json(input_folder / "silver_snapshot.json") if input_folder and (input_folder / "silver_snapshot.json").is_file() else {}
        checks["H3: the pin names the accepted silver run and its complete snapshot set"] = (
            bool(producer) and pinned.get("source_run_id") == producer.get("run_id") and pinned.get("snapshot_set") == producer.get("snapshot_set")
        )
    return checks, summary


def moment(value: Any) -> datetime | None:
    """A time that the AWS command line prints: an ISO 8601 text with or without an offset, or a number of seconds."""
    try:
        parsed = datetime.fromtimestamp(float(value), UTC) if isinstance(value, int | float) else datetime.fromisoformat(str(value))
    except (ValueError, OverflowError, OSError):
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def plan_problems(layer: str, run: str, document: Any) -> list[str]:
    """What is wrong with the saved log events of the dry run publish stream: the plan must be this run's, complete and not blocked."""
    events = document.get("events") if isinstance(document, dict) else document
    messages = [str(item.get("message", "")) if isinstance(item, dict) else str(item) for item in events or []]
    lines = [message for message in messages if message.startswith("DRY_RUN_PLAN ")]
    if len(lines) != 1:
        return [f"the stream holds {len(lines)} plan lines, not one"]
    try:
        plan = json.loads(lines[0].removeprefix("DRY_RUN_PLAN "))
    except ValueError:
        return ["the plan line is not JSON"]
    problems = (
        []
        if (plan.get("layer"), plan.get("run"), plan.get("mode"), plan.get("namespace"), plan.get("dry_run")) == (layer, run, "real", NAMESPACE[layer], True)
        else ["the plan is not the dry run of this layer and run in real mode and its own namespace"]
    )
    problems += [f"the plan lists unknown state: {plan.get('unknown')}"] if plan.get("unknown") else []
    problems += [f"the catalog could not be read: {plan.get('catalog_error')}"] if plan.get("catalog_error") else []
    problems += [f"the real publish would be refused: {plan.get('blocked')}"] if plan.get("blocked") else []
    problems += [f"the log holds a {word} line" for word in ("UNKNOWN", "CATALOG", "BLOCKED") if any(message.startswith(word) for message in messages)]
    return problems


def host_problems(status: Any) -> list[str]:
    """What is wrong with the output of ``host_session status`` for a stopped host."""
    keys = ("desired_capacity", "group_instances", "active_container_instances")
    if not isinstance(status, dict) or any(key not in status for key in keys):
        return ["the host status lacks its capacity counts"]
    return [f"{key} is {status[key]}, not 0" for key in keys if status[key] != 0]


def ignored(path: Path) -> bool:
    """Whether Git ignores the path: signed reports stay out of Git (owner decision 92)."""
    return run_command("git", ["check-ignore", "-q", str(path.resolve())], cwd=ROOT, timeout=30).returncode == 0


def write_json(path: Path, document: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("x", encoding="utf-8") as handle:
            handle.write(json.dumps(document, indent=2, sort_keys=True) + "\n")
    except FileExistsError:
        raise AcceptanceError(f"{path} exists: a record is written once") from None


def now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def approve(layer: str, run: str, code_sha256: str, out: Path, confirmation: str) -> dict[str, Any]:
    if not RUN_ID.fullmatch(run) or not SHA256.fullmatch(code_sha256):
        raise AcceptanceError("the run ID or the code digest has the wrong form")
    wanted = f"approve-{layer}-{run}-{code_sha256[:12]}"
    if confirmation != wanted:
        raise AcceptanceError(f"set CONFIRM_APPROVE to {wanted} to approve this run ID and code digest")
    record = {"schema": 1, "layer": layer, "run_id": run, "code_sha256": code_sha256, "approved_at": now()}
    write_json(out, record)
    return record


def report(arguments: argparse.Namespace) -> dict[str, Any]:
    layer, run = arguments.layer, arguments.run
    approval, described = read_json(arguments.approval), read_json(arguments.describe)
    task = (described.get("tasks") or [{}])[0] if isinstance(described, dict) else {}
    created, approved = moment(task.get("createdAt")), moment(approval.get("approved_at"))
    checks = {
        "the owner approved this run ID before the task was created": (approval.get("layer"), approval.get("run_id")) == (layer, run)
        and created is not None
        and approved is not None
        and approved < created
    }
    checks["every container of the task exited 0 and the task stopped"] = not task_problems(layer, described)
    evidence, summary = evidence_checks(layer, run, arguments.evidence, arguments.input_evidence)
    checks |= evidence
    identity = read_json(arguments.evidence / "layer_run.json")
    checks |= listing_checks(layer, run, arguments.before, arguments.after, identity.get("input_listing") or {})
    running = read_json(arguments.running_tasks)
    checks["no task of the job is left running"] = isinstance(running, dict) and running.get("taskArns") == []
    problems = host_problems(read_json(arguments.host_status))
    checks["the host is stopped and verified: no capacity, group instance or container instance"] = not problems
    files = {
        name: path
        for name, path in (("approval", arguments.approval), ("describe", arguments.describe), ("before", arguments.before), ("after", arguments.after))
    }
    files |= {"running_tasks": arguments.running_tasks, "host_status": arguments.host_status}
    digests = {name: file_sha256(path) for name, path in files.items()}
    digests |= {name: file_sha256(arguments.evidence / name) for name in EVIDENCE_FILES}
    digests |= {PIN_FILE[layer]: file_sha256(arguments.evidence / PIN_FILE[layer])}
    return {
        "schema": 1,
        "layer": layer,
        "run_id": run,
        "created_at": now(),
        "passed": all(checks.values()),
        "checks": checks,
        "tables": summary,
        "file_sha256": digests,
        "limits": "Owner-saved read results only. The report does not prove the catalog state it did not read.",
    }


def sign(report_path: Path, owner: str, confirmation: str) -> dict[str, Any]:
    document = read_json(report_path)
    digest = file_sha256(report_path)
    wanted = f"sign-{document.get('layer')}-{document.get('run_id')}-{digest[:12]}"
    if document.get("passed") is not True:
        raise AcceptanceError("a report with a failed check is not signed")
    if confirmation != wanted or not owner.strip():
        raise AcceptanceError(f"set CONFIRM_SIGN to {wanted} and name the owner")
    signature = {"schema": 1, "report_sha256": digest, "owner": owner.strip(), "signed_at": now(), "confirmation": wanted}
    write_json(report_path.with_suffix(".signed.json"), signature)
    return signature


def parser() -> argparse.ArgumentParser:
    cli = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else "")
    steps = cli.add_subparsers(dest="step", required=True)
    for name in ("approve", "task", "report"):
        step = steps.add_parser(name)
        step.add_argument("--layer", required=True, choices=LAYERS)
        if name != "task":
            step.add_argument("--run", required=True)
    steps.choices["approve"].add_argument("--code-sha256", required=True)
    steps.choices["approve"].add_argument("--out", required=True, type=Path)
    steps.choices["task"].add_argument("--describe", required=True, type=Path)
    planned = steps.add_parser("plan")
    planned.add_argument("--layer", required=True, choices=LAYERS)
    planned.add_argument("--run", required=True)
    planned.add_argument("--log", required=True, type=Path)
    for flag in ("approval", "describe", "evidence", "before", "after", "running-tasks", "host-status", "out"):
        steps.choices["report"].add_argument(f"--{flag}", required=True, type=Path)
    steps.choices["report"].add_argument("--input-evidence", type=Path)
    signing = steps.add_parser("sign")
    signing.add_argument("--report", required=True, type=Path)
    signing.add_argument("--owner", required=True)
    return cli


def main(argv: Sequence[str] | None = None) -> int:
    arguments = parser().parse_args(argv)
    try:
        if arguments.step == "approve":
            result = approve(arguments.layer, arguments.run, arguments.code_sha256, arguments.out, os.environ.get("CONFIRM_APPROVE", ""))
        elif arguments.step == "task":
            result = {"problems": task_problems(arguments.layer, read_json(arguments.describe))}
            result["passed"] = not result["problems"]
        elif arguments.step == "plan":
            result = {"problems": plan_problems(arguments.layer, arguments.run, read_json(arguments.log))}
            result["passed"] = not result["problems"]
        elif arguments.step == "report":
            if not ignored(arguments.out):
                raise AcceptanceError("the report path must be ignored by Git: signed reports stay local")
            result = report(arguments)
            write_json(arguments.out, result)
            digest = file_sha256(arguments.out)
            result["sign_with"] = f"sign-{arguments.layer}-{arguments.run}-{digest[:12]}"
        else:
            result = sign(arguments.report, arguments.owner, os.environ.get("CONFIRM_SIGN", ""))
    except AcceptanceError as error:
        sys.stderr.write(f"layer acceptance {arguments.step}: {error}\n")
        return 1
    sys.stdout.write(json.dumps(result, indent=2, sort_keys=True) + "\n")
    return 0 if result.get("passed", True) else 1


if __name__ == "__main__":
    sys.exit(main())
