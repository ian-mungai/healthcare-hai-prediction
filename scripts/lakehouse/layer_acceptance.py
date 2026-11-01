"""The owner's acceptance of a first layer publish: the run approval, the task result, the evidence checks and the signed local report.

Run from the repository root with files that the owner saved from AWS read calls. Nothing here calls AWS.

    python -m scripts.lakehouse.layer_acceptance approve --layer silver --run <RUN_ID> --code-sha256 <SHA256> --out <FILE>
    python -m scripts.lakehouse.layer_acceptance task --layer silver --describe <FILE>
    python -m scripts.lakehouse.layer_acceptance plan --layer silver --run <RUN_ID> --log <FILE>
    python -m scripts.lakehouse.layer_acceptance report --layer silver --run <RUN_ID> --approval <FILE> --describe <FILE> --evidence <DIR>
        --before <FILE> --after <FILE> --running-tasks <FILE> --host-status <FILE> [--input-evidence <DIR> --input-report <FILE>] --out <FILE>
    CONFIRM_SIGN=<TEXT THAT REPORT PRINTS> python -m scripts.lakehouse.layer_acceptance sign --report <FILE> --owner <NAME>

``approve`` records the owner's approval of the run ID and the code digest before the task starts, after the owner types the confirmation text it prints.
``task`` requires one stopped task whose every container ran to a zero exit. ``plan`` reads the saved log events of the dry run publish stream and requires
the one plan line of this run, with no unknown state, catalog error or blocked publish.

``report`` checks the evidence of the run against the frozen publish contract of the code revision the run unpacked (read with ``git show``, so a later edit
changes nothing). It repeats the acceptance rules of the layer driver on the uploaded files, so a driver defect cannot vouch for itself. Every table set and
namespace equals the contract. The task is this run's real task: its definition, its overrides and the approved code digest. The export is this run's: the run
ID, the sealed identity and the consumer result, manifest and candidate hashes agree. The rows and content fingerprints, the commit record that follows the
backup and the reconcile and names every snapshot ID, the pinned table set (H3) and the input listing before and after (H2) are checked. The listing digest
is computed again in the way the driver computes it and compared with the pin. Every object version the task added lies under a prefix the layer writer role
may write. A gold report also needs the signed silver report and its records: the producer is accepted only with a matching signature.

Inputs are regular files that are not links, within a size limit, and every record is a JSON object. Errors are fixed texts that echo nothing from an input.
The report names the SHA-256 of every file it read, computed from the bytes it parsed. It is written only to a Git-ignored path. A report that passed is
signed with ``sign``: the owner sets ``CONFIRM_SIGN`` to the text that ``report`` printed, which names the layer, the run and the digest of the report. The
signature is that typed confirmation beside the report digest and the owner's name. It uses no key. Signed reports stay local."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import stat
import sys
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.lakehouse import version_listing
from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]
JOBS_FILE = ROOT / "config/orchestration/ecs_broker_jobs.json"
LAYERS = ("silver", "gold")
CONTRACT = {"silver": "data_contracts/lakehouse/silver_publish.json", "gold": "data_contracts/lakehouse/gold_contract.json"}
BRONZE_MAP = "config/lakehouse/bronze_tables.json"
INPUT_PREFIX = {"silver": "lakehouse/bronze/", "gold": "lakehouse/silver/"}
# The prefixes of the layer writer role (infra/iam/job_writer.tf) beside the run's own upload prefix.
WRITE_PREFIXES = {"silver": ("lakehouse/silver/",), "gold": ("lakehouse/gold/", "lakehouse/catalog_backups/", "metadata/dbt/")}
PIN_FILE = {"silver": "pins.json", "gold": "silver_pins.json"}
RUN_ID = re.compile(r"[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}")
SHA256 = re.compile(r"[a-f0-9]{64}")
REVISION = re.compile(r"[0-9a-f]{40}|[0-9a-f]{64}")
FINGERPRINT = re.compile(r"-?[0-9]{1,38}")
TASK_ARN = re.compile(r"arn:aws:ecs:[a-z0-9-]+:[0-9]{12}:task/[A-Za-z0-9_-]+/[a-f0-9]{32}")
GUARD_NAME = "guard"  # ECS stops the guard of a layer task when the last phase ends, so its exit code is never a result of the job
LIMIT = 1 << 28  # the largest input: a version listing of the lakehouse prefix
EXPORT = "publish/export/manifest.json"
EVIDENCE_FILES = (
    "layer_run.json",
    "publish/candidates.json",
    "publish/reconcile.json",
    "publish/backup.json",
    "publish/commit.json",
    EXPORT,
    "publish/consumer_result.json",
)


class AcceptanceError(RuntimeError):
    """An input is missing, linked, too large or malformed, or the owner's confirmation is wrong. The text is fixed and echoes no input."""


def read_bytes(path: Path) -> bytes:
    """A regular file that is not a link, read within the size limit."""
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    except OSError:
        raise AcceptanceError("an input file is missing or is a link") from None
    with os.fdopen(descriptor, "rb") as handle:
        if not stat.S_ISREG(os.fstat(handle.fileno()).st_mode):
            raise AcceptanceError("an input is not a regular file")
        data = handle.read(LIMIT + 1)
    if len(data) > LIMIT:
        raise AcceptanceError("an input file is larger than the limit")
    return data


def folder_file(folder: Path, relative: str) -> Path:
    """A file below an evidence folder when no part of the path is a link."""
    current = folder
    for part in ("", *Path(relative).parts[:-1]):
        current = current / part
        if current.is_symlink() or not current.is_dir():
            raise AcceptanceError("an evidence folder is missing or is a link")
    return folder / relative


class Records:
    """The JSON objects a report reads, with the SHA-256 of the bytes that were parsed."""

    def __init__(self) -> None:
        self.sha256: dict[str, str] = {}

    def load(self, path: Path, name: str) -> dict[str, Any]:
        raw = read_bytes(path)
        self.sha256[name] = hashlib.sha256(raw).hexdigest()
        try:
            value = json.loads(raw)
        except ValueError:
            raise AcceptanceError(f"{name} is not JSON") from None
        if not isinstance(value, dict):
            raise AcceptanceError(f"{name} is not a JSON object")
        return value

    def load_in(self, folder: Path, relative: str, prefix: str = "") -> dict[str, Any]:
        return self.load(folder_file(folder, relative), prefix + relative)


def read_json(path: Path) -> dict[str, Any]:
    return Records().load(path, "input")


def mapping(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def count(value: Any) -> bool:
    return type(value) is int and value >= 0


def expected_containers(layer: str) -> list[str]:
    """The init container and every container of the layer's broker table: each must exit 0."""
    return ["init", *read_json(JOBS_FILE)["jobs"][layer]["containers"]]


def task_problems(layer: str, described: Any) -> list[str]:
    """What is wrong with the output of ``aws ecs describe-tasks`` for the run's one task. The texts are fixed.

    Every container of the job must exit 0. The guard may be absent (a task definition from before it) and may have any exit code, because ECS stops it with
    SIGTERM or SIGKILL when the last phase ends."""
    if not isinstance(described, dict):
        return ["the describe output is not an object"]
    problems = ["the describe call reported failures"] if described.get("failures") else []
    tasks = described.get("tasks")
    if not isinstance(tasks, list) or len(tasks) != 1 or not isinstance(tasks[0], dict):
        return [*problems, "the describe output must hold exactly one task"]
    task = tasks[0]
    if task.get("lastStatus") != "STOPPED":
        problems.append("the task is not STOPPED")
    containers = {item.get("name"): item for item in task.get("containers") or [] if isinstance(item, dict)}
    for name in expected_containers(layer):
        if name not in containers:
            problems.append(f"container {name} has no result")
        elif type(containers[name].get("exitCode")) is not int or containers[name]["exitCode"] != 0:
            problems.append(f"container {name} did not exit 0")
    if set(containers) - {*expected_containers(layer), GUARD_NAME}:
        problems.append("the task holds a container that is not part of the job")
    return problems


def definition_pattern(layer: str) -> re.Pattern[str]:
    return re.compile(rf"arn:aws:ecs:[a-z0-9-]+:[0-9]{{12}}:task-definition/[a-z][a-z0-9_]*_job_{layer}:[0-9]+")


def task_is_this_runs(layer: str, run: str, task: Mapping[str, Any], approved_sha: str, code_run: str, input_run: str) -> bool:
    """The task ARN, its definition, its overrides and the approved code digest belong to this real run. A dry run has an extra variable and fails."""
    if not TASK_ARN.fullmatch(str(task.get("taskArn"))) or not definition_pattern(layer).fullmatch(str(task.get("taskDefinitionArn"))):
        return False
    wanted = read_json(JOBS_FILE)["jobs"][layer]["containers"]
    given = {}
    for item in mapping(task.get("overrides")).get("containerOverrides") or []:
        if item in ({"name": "init"}, {"name": GUARD_NAME}):  # describe-tasks lists a container that has no override by its name alone
            continue
        environment = item.get("environment") if isinstance(item, dict) else None
        if not isinstance(environment, list) or not all(isinstance(entry, dict) for entry in environment):
            return False
        given[item.get("name")] = {entry.get("name"): entry.get("value") for entry in environment}
    values = {"RUN_ID": run, "CODE_RUN_ID": code_run, "CODE_SHA256": approved_sha, "INPUT_RUN_ID": input_run}
    return set(given) == set(wanted) and all(
        set(given[name]) == set(variables) and all(given[name][key] == values[key] for key in variables if key in values) and all(given[name].values())
        for name, variables in wanted.items()
    )


def read_listing(records: Records, path: Path, name: str) -> tuple[list[tuple[str, str, str, int]], list[tuple[str, str]]]:
    """The versions in listing order and the delete markers of an ``aws s3api list-object-versions`` result."""
    document = records.load(path, name)
    try:
        versions = [(str(i["key"]), str(i["version"]), str(i.get("etag") or ""), int(i.get("bytes") or 0)) for i in document.get("versions") or []]
        deleted = [(str(i["key"]), str(i["version"])) for i in document.get("deleted") or []]
    except (KeyError, TypeError, ValueError):
        raise AcceptanceError(f"{name} is not a version listing") from None
    return versions, deleted


def listing_checks(layer: str, run: str, records: Records, arguments: argparse.Namespace, recorded: Any) -> dict[str, bool]:
    """H2 and the write scope from two listings of ``lakehouse/`` (and ``metadata/dbt/`` for gold): one before the task and one after."""
    before, before_deleted = read_listing(records, arguments.before, "before")
    after, after_deleted = read_listing(records, arguments.after, "after")
    prefix = INPUT_PREFIX[layer]
    held_before = [item for item in before if item[0].startswith(prefix)]
    held_after = [item for item in after if item[0].startswith(prefix)]
    markers = [item for item in before_deleted if item[0].startswith(prefix)]
    same = held_before == held_after and markers == [item for item in after_deleted if item[0].startswith(prefix)]
    entries = sorted([("V", *item) for item in held_before] + [("D", key, version, "", 0) for key, version in markers], key=lambda entry: entry[1])
    allowed = (*WRITE_PREFIXES[layer], f"lakehouse/layer_runs/{layer}/{run}/")
    seen_before = {(key, version) for key, version, _, _ in before} | set(before_deleted)
    seen_after = {(key, version) for key, version, _, _ in after} | set(after_deleted)
    added = [key for key, _ in seen_after - seen_before]
    return {
        "H2: the input object versions are the same before and after the task": same,
        "H2: the pin recorded the digest and count of the listing taken before the task": recorded == version_listing.canonical_digest(prefix, entries),
        "the task added versions only under the layer's prefixes and its own run prefix": bool(added) and all(key.startswith(allowed) for key in added),
        "the task removed no version": not seen_before - seen_after,
    }


def git_file(revision: str, path: str) -> Any:
    """A JSON file as it is at the code revision the run unpacked."""
    result = run_command("git", ["show", f"{revision}:{path}"], cwd=ROOT, timeout=30)
    if result.returncode != 0:
        raise AcceptanceError("a frozen file of the run's revision is not in this checkout")
    try:
        return json.loads(result.stdout)
    except ValueError:
        raise AcceptanceError("a frozen file of the run's revision is not JSON") from None


def contract(layer: str, revision: str) -> tuple[set[str], str]:
    """The tables and the real namespace of the layer's publish contract at the run's revision."""
    try:
        publish = git_file(revision, CONTRACT[layer])["publish"]
        return set(publish["tables"]), str(publish["namespaces"]["real"])
    except (KeyError, TypeError):
        raise AcceptanceError("the publish contract of the run's revision has an unexpected shape") from None


def bronze_tables(revision: str) -> set[str]:
    """The pinned bronze tables of the table map at the run's revision, as ``pins.mapped_tables`` reads them (and the file metadata table)."""
    try:
        return {entry["table"] for entry in git_file(revision, BRONZE_MAP)["tables"]} | {"file_metadata"}
    except (KeyError, TypeError):
        raise AcceptanceError("the bronze table map of the run's revision has an unexpected shape") from None


def producer_checks(records: Records, run: str, revision: str, pinned: Mapping[str, Any], folder: Path | None, report_path: Path | None) -> dict[str, bool]:
    """The gold pin is complete and names the silver run, and that silver run is signed: its report passed, covers its record and matches its signature."""
    if folder is None or report_path is None:
        raise AcceptanceError("a gold report needs the silver records and the signed silver report")
    silver_tables, silver_namespace = contract("silver", revision)
    producer = records.load_in(folder, "silver_snapshot.json", "input/")
    report = records.load(report_path, "input/report")
    signature = records.load(report_path.with_suffix(".signed.json"), "input/signature")
    source = pinned.get("source_run_id")
    snapshots = mapping(pinned.get("snapshot_set"))
    digest = records.sha256["input/report"]
    owner = signature.get("owner")
    return {
        "H3: the gold pin is complete: its run, mode, namespace and the silver contract's tables": (
            pinned.get("run_id"),
            pinned.get("mode"),
            pinned.get("namespace"),
        )
        == (run, "real", silver_namespace)
        and bool(RUN_ID.fullmatch(str(source)))
        and set(snapshots) == silver_tables
        and all(count(value) and value > 0 for value in snapshots.values()),
        "H3: the pin names the silver run and its complete snapshot set": (producer.get("run_id"), producer.get("mode"), producer.get("namespace"))
        == (source, "real", silver_namespace)
        and producer.get("snapshot_set") == snapshots,
        "the silver producer is signed: its report passed, covers its record and matches its signature": report.get("passed") is True
        and (report.get("layer"), report.get("run_id")) == ("silver", source)
        and mapping(report.get("file_sha256")).get("silver_snapshot.json") == records.sha256["input/silver_snapshot.json"]
        and signature.get("report_sha256") == digest
        and signature.get("confirmation") == f"sign-silver-{source}-{digest[:12]}"
        and isinstance(owner, str)
        and bool(owner.strip()),
    }


def evidence_checks(
    records: Records, layer: str, run: str, folder: Path, approval: Mapping[str, Any], arguments: argparse.Namespace
) -> tuple[dict[str, bool], dict[str, dict[str, Any]], dict[str, Any]]:
    """The acceptance rules of the layer driver on the uploaded run records, with the frozen publish contract as the reference for every set."""
    identity = records.load_in(folder, "layer_run.json")
    revision = str(identity.get("git_revision"))
    if not REVISION.fullmatch(revision):
        raise AcceptanceError("the run records no code revision")
    tables, namespace = contract(layer, revision)
    candidates, reconcile, backup, commit, manifest, consumer = (records.load_in(folder, name) for name in EVIDENCE_FILES[1:])
    pinned = records.load_in(folder, PIN_FILE[layer])
    sha = records.sha256
    cand, rec, man = (mapping(candidates.get("tables")), mapping(reconcile.get("tables")), mapping(manifest.get("tables")))
    snapshot_set = mapping(commit.get("snapshot_set"))
    wanted = {name: mapping(cand.get(name)).get("candidate_snapshot_id") for name in tables}
    sealed = consumer.get("expected_sha256")
    summary: dict[str, dict[str, Any]] = {}
    rows_ok = fingerprints_ok = True
    for name in sorted(tables):
        entry, rows = mapping(rec.get(name)), mapping(rec.get(name)).get("rows")
        fingerprint = entry.get("fingerprint")
        valid = isinstance(fingerprint, str) and bool(FINGERPRINT.fullmatch(fingerprint))
        rows_ok = rows_ok and count(rows) and rows == entry.get("export_rows") == entry.get("manifest_rows") == mapping(man.get(name)).get("rows")
        rows_ok = rows_ok and rows == mapping(cand.get(name)).get("rows") and entry.get("candidate_snapshot_id") == wanted[name] and entry.get("equal") is True
        fingerprints_ok = fingerprints_ok and valid and fingerprint == entry.get("export_fingerprint")
        summary[name] = {
            "rows": rows if count(rows) else None,
            "fingerprint": fingerprint if valid else None,
            "snapshot_id": snapshot_set.get(name) if count(snapshot_set.get(name)) else None,
        }
    sets = [set(cand), set(rec), set(man), set(snapshot_set)]
    checks = {
        "the run records belong to this run and layer in real mode, with the approved code": (
            identity.get("run_id"),
            identity.get("layer"),
            identity.get("mode"),
        )
        == (run, layer, "real")
        and bool(SHA256.fullmatch(str(approval.get("code_sha256"))))
        and identity.get("code_sha256") == approval.get("code_sha256"),
        "every table set equals the frozen publish contract": all(item == tables for item in sets) and bool(tables),
        "the records name the layer's own namespace in real mode": (candidates.get("mode"), candidates.get("namespace")) == ("real", namespace),
        "the export is this run's: run, sealed identity, consumer result and manifest hashes agree": bool(SHA256.fullmatch(str(sealed)))
        and (manifest.get("run_id"), consumer.get("run_id"), candidates.get("run_id")) == (run, run, run)
        and sealed
        == consumer.get("in_container_sha256")
        == manifest.get("sealed_sha256")
        == manifest.get("in_container_sha256")
        == candidates.get("sealed_sha256")
        and consumer.get("exported") is True
        and consumer.get("manifest_sha256") == sha[EXPORT] == candidates.get("export_manifest_sha256"),
        "the candidates are complete and the reconcile record is a pass for them": candidates.get("complete") is True
        and reconcile.get("pass") is True
        and reconcile.get("run_id") == run
        and reconcile.get("candidates_sha256") == sha["publish/candidates.json"],
        "the row count of every table equals its candidate and its export": rows_ok,
        "the content fingerprint of every table is valid and equals its export": fingerprints_ok,
        "the backup was read back after the reconcile": backup.get("read_back") is True
        and backup.get("run_id") == run
        and backup.get("name") == f"catalog_backup_{run}.json"
        and bool(SHA256.fullmatch(str(backup.get("backup_sha256"))))
        and backup.get("reconcile_sha256") == sha["publish/reconcile.json"],
        "publish_runs was written last: the commit record follows the backup and is committed": commit.get("state") == "committed"
        and commit.get("run_id") == run
        and commit.get("backup_sha256") == sha["publish/backup.json"],
        "the commit names every candidate snapshot ID": snapshot_set == wanted and all(count(value) and value > 0 for value in wanted.values()),
    }
    input_run = ""
    if layer == "silver":
        record = records.load_in(folder, "silver_snapshot.json")
        checks["the silver record equals the commit"] = (record.get("run_id"), record.get("mode"), record.get("namespace")) == (
            run,
            "real",
            namespace,
        ) and record.get("snapshot_set") == snapshot_set
        checks["the records name the layer's own namespace in real mode"] &= record.get("mode") == "real" and record.get("namespace") == namespace
        checks["H3: the pinned table set is the bronze table map of the run's revision"] = set(pinned) == bronze_tables(revision) and all(pinned.values())
    else:
        checks |= producer_checks(records, run, revision, pinned, arguments.input_evidence, arguments.input_report)
        input_run = str(pinned.get("source_run_id"))
    return checks, summary, {"identity": identity, "input_run": input_run}


def moment(value: Any) -> datetime | None:
    """A time that the AWS command line prints: an ISO 8601 text with or without an offset, or a number of seconds."""
    try:
        parsed = datetime.fromtimestamp(float(value), UTC) if isinstance(value, int | float) else datetime.fromisoformat(str(value))
    except (ValueError, OverflowError, OSError):
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def plan_problems(layer: str, run: str, document: Any) -> list[str]:
    """What is wrong with the saved log events of the dry run publish stream: the plan must be this run's, complete and not blocked. The texts are fixed."""
    events = document.get("events") if isinstance(document, dict) else document
    messages = [str(item.get("message", "")) if isinstance(item, dict) else str(item) for item in events or []]
    lines = [message for message in messages if message.startswith("DRY_RUN_PLAN ")]
    if len(lines) != 1:
        return ["the stream does not hold exactly one plan line"]
    try:
        plan = mapping(json.loads(lines[0].removeprefix("DRY_RUN_PLAN ")))
    except ValueError:
        return ["the plan line is not JSON"]
    problems = []
    if (plan.get("layer"), plan.get("run"), plan.get("mode"), plan.get("namespace"), plan.get("dry_run")) != (layer, run, "real", layer, True):
        problems.append("the plan is not the dry run of this layer and run in real mode and its own namespace")
    problems += ["the plan lists unknown state"] if plan.get("unknown") else []
    problems += ["the catalog could not be read"] if plan.get("catalog_error") else []
    problems += ["the real publish would be refused"] if plan.get("blocked") else []
    problems += [f"the log holds a {word} line" for word in ("UNKNOWN", "CATALOG", "BLOCKED") if any(message.startswith(word) for message in messages)]
    return problems


def host_problems(status: Any) -> list[str]:
    """What is wrong with the output of ``host_session status`` for a stopped host. The texts are fixed."""
    keys = ("desired_capacity", "group_instances", "active_container_instances")
    if not isinstance(status, dict) or any(key not in status for key in keys):
        return ["the host status lacks its capacity counts"]
    return [f"{key} is not 0" for key in keys if status[key] != 0]


def ignored(path: Path) -> bool:
    """Whether Git ignores the path: signed reports stay out of Git."""
    return run_command("git", ["check-ignore", "-q", str(path.resolve())], cwd=ROOT, timeout=30).returncode == 0


def write_json(path: Path, document: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("x", encoding="utf-8") as handle:
            handle.write(json.dumps(document, indent=2, sort_keys=True) + "\n")
    except FileExistsError:
        raise AcceptanceError("an output record exists: a record is written once") from None


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
    records = Records()
    approval, described = records.load(arguments.approval, "approval"), records.load(arguments.describe, "describe")
    evidence, summary, found = evidence_checks(records, layer, run, arguments.evidence, approval, arguments)
    tasks = described.get("tasks")
    task = tasks[0] if isinstance(tasks, list) and len(tasks) == 1 and isinstance(tasks[0], dict) else {}
    created, approved = moment(task.get("createdAt")), moment(approval.get("approved_at"))
    checks = {
        "the owner approved this run ID before the task was created": (approval.get("layer"), approval.get("run_id")) == (layer, run)
        and created is not None
        and approved is not None
        and approved < created,
        "the task is this run's: its definition, its overrides and the approved code digest": task_is_this_runs(
            layer, run, task, str(approval.get("code_sha256")), str(found["identity"].get("code_run")), found["input_run"]
        ),
        "every container of the task exited 0 and the task stopped": not task_problems(layer, described),
    }
    checks |= evidence
    checks |= listing_checks(layer, run, records, arguments, found["identity"].get("input_listing"))
    running = records.load(arguments.running_tasks, "running_tasks")
    checks["no task of the job is left running"] = running.get("taskArns") == []
    checks["the host is stopped and verified: no capacity, group instance or container instance"] = not host_problems(
        records.load(arguments.host_status, "host_status")
    )
    return {
        "schema": 1,
        "layer": layer,
        "run_id": run,
        "created_at": now(),
        "passed": all(checks.values()),
        "checks": checks,
        "tables": summary,
        "file_sha256": dict(sorted(records.sha256.items())),
        "limits": "Owner-saved read results only. The report does not prove the catalog state it did not read.",
    }


def sign(report_path: Path, owner: str, confirmation: str) -> dict[str, Any]:
    raw = read_bytes(report_path)
    digest = hashlib.sha256(raw).hexdigest()
    try:
        document = mapping(json.loads(raw))
    except ValueError:
        raise AcceptanceError("the report is not JSON") from None
    if document.get("passed") is not True or not RUN_ID.fullmatch(str(document.get("run_id"))) or document.get("layer") not in LAYERS:
        raise AcceptanceError("only a report that passed is signed")
    wanted = f"sign-{document['layer']}-{document['run_id']}-{digest[:12]}"
    if confirmation != wanted or not owner.strip() or len(owner) > 100 or not owner.isprintable():
        raise AcceptanceError(f"set CONFIRM_SIGN to {wanted} and name the owner")
    signature = {"schema": 1, "report_sha256": digest, "owner": owner.strip(), "signed_at": now(), "confirmation": wanted}
    write_json(report_path.with_suffix(".signed.json"), signature)
    return signature


def parser() -> argparse.ArgumentParser:
    cli = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else "")
    steps = cli.add_subparsers(dest="step", required=True)
    for name in ("approve", "task", "report", "plan"):
        step = steps.add_parser(name)
        step.add_argument("--layer", required=True, choices=LAYERS)
        if name != "task":
            step.add_argument("--run", required=True)
    steps.choices["approve"].add_argument("--code-sha256", required=True)
    steps.choices["approve"].add_argument("--out", required=True, type=Path)
    steps.choices["task"].add_argument("--describe", required=True, type=Path)
    steps.choices["plan"].add_argument("--log", required=True, type=Path)
    for flag in ("approval", "describe", "evidence", "before", "after", "running-tasks", "host-status", "out"):
        steps.choices["report"].add_argument(f"--{flag}", required=True, type=Path)
    steps.choices["report"].add_argument("--input-evidence", type=Path)
    steps.choices["report"].add_argument("--input-report", type=Path)
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
            result["sign_with"] = f"sign-{arguments.layer}-{arguments.run}-{hashlib.sha256(arguments.out.read_bytes()).hexdigest()[:12]}"
        else:
            result = sign(arguments.report, arguments.owner, os.environ.get("CONFIRM_SIGN", ""))
    except AcceptanceError as error:
        sys.stderr.write(f"layer acceptance {arguments.step}: {error}\n")
        return 1
    except (AttributeError, KeyError, TypeError, ValueError):
        sys.stderr.write(f"layer acceptance {arguments.step}: an input has an unexpected shape\n")
        return 1
    sys.stdout.write(json.dumps(result, indent=2, sort_keys=True) + "\n")
    return 0 if result.get("passed", True) else 1


if __name__ == "__main__":
    sys.exit(main())
