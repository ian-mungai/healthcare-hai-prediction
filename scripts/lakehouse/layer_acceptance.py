"""The automatic acceptance report of a layer publish: the task result, the evidence checks and the local report. No approval record and no signature.

The DAG evidence step writes the report itself (``orchestration.airflow_steps.evidence_step``) from AWS reads. The same checks run from the command line on
files that were saved from AWS read calls. Nothing here calls AWS.

    python -m scripts.lakehouse.layer_acceptance task --layer silver --describe <FILE>
    python -m scripts.lakehouse.layer_acceptance report --layer silver --run <RUN_ID> --code-digest <FILE> --describe <FILE> --evidence <DIR>
        --launch-records <DIR> --before <FILE> --after <FILE> --running-tasks <FILE> --host-status <FILE>
        [--code-archive <FILE>] [--input-evidence <DIR> --input-report <FILE>] --out <FILE>

``task`` requires one stopped task whose every container ran to a zero exit.

``report`` binds the run to the staged code digest (the ``code.sha256`` of ``lakehouse/run_stage/<run>/inputs/``) and to the launch records of the run
(``lakehouse/launch_records/<run>/<job>/``: the claim, then the started and end records of its attempt). It checks the evidence of the run against the frozen
publish contract of the code revision the run unpacked (read from the run's staged ``code.tar.gz`` after it matched ``code.sha256``, so a later edit and the
checkout change nothing). It repeats the acceptance rules of the layer driver on the uploaded files, so a driver defect cannot vouch for itself. Every table
set and namespace equals the contract. The task is this run's real task: its definition, its overrides and the staged code digest. The launch claim binds the
same digest and the input run, the started record names the described task and the end record is ``succeeded``. The export is this run's: the run ID, the
sealed identity and the consumer result, manifest and candidate hashes agree. The rows and content fingerprints, the commit record that follows the backup and
the reconcile and names every snapshot ID, the pinned table set (H3) and the input listing before and after (H2) are checked. The listing digest is computed
again in the way the driver computes it and compared with the pin. Every object version the task added lies under a prefix the layer writer role may write, or
under the run-owned prefixes of another run that holds a claim record (listed as ``other_runs_writes``, not as ``out_of_scope``). A gold report also needs the
passed silver report and its records, and a features report the passed gold report and its records: a producer is accepted when its report passed and covers
the records read here. The features pin must be complete and equal the commit of the gold run (H3). The input check of features cannot read the gold
``publish_runs`` row, because this command reads saved files, so it reads the commit record that the gold report covers. A features
report also requires the two Feast records to name this run's publication: the snapshot set that Feast applied and the one it loaded into the online store.

The report runs on the host, so the host check holds what is true while the host runs: the group desired capacity is at most 1, no task of the job family is
left running and the group's one instance is the only active container instance. The host stop is proven by ``host_session stop`` (``host_off_verified``).

Inputs are regular files that are not links, within a size limit, and every record is a JSON object. Errors are fixed texts that echo nothing from an input.
The report names the SHA-256 of every file it read, computed from the bytes it parsed. The command writes it only to a Git-ignored path."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import os
import re
import stat
import sys
import tarfile
import time
import zlib
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.lakehouse import pins, version_listing
from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]
JOBS_FILE = ROOT / "config/orchestration/ecs_broker_jobs.json"
LAYERS = ("silver", "gold", "features")
CONTRACT = {
    "silver": "data_contracts/lakehouse/silver_publish.json",
    "gold": "data_contracts/lakehouse/gold_contract.json",
    "features": "data_contracts/lakehouse/features_publish.json",
}
BRONZE_MAP = "config/lakehouse/bronze_tables.json"
OUT_OF_SCOPE_KEYS = 100
INPUT_PREFIX = {"silver": "lakehouse/bronze/", "gold": "lakehouse/silver/", "features": "lakehouse/gold/"}
PRODUCER = {"gold": "silver", "features": "gold"}  # the layer whose passed run this layer reads
# The prefixes of the layer writer role (infra/iam/job_writer.tf) beside the run's own upload prefix.
CONTRACT_FILES = (*CONTRACT.values(), BRONZE_MAP, str(pins.SOURCES))
WRITE_PREFIXES = {
    "silver": ("lakehouse/silver/",),
    "gold": ("lakehouse/gold/", "lakehouse/catalog_backups/", "metadata/dbt/"),
    "features": ("lakehouse/features/", "lakehouse/feast/registry.pb"),
}
PIN_FILE = {"silver": "pins.json", "gold": "silver_pins.json", "features": "gold_pins.json"}
FEATURES_RECORD, FEAST_APPLY, FEAST_LOAD = "features_snapshot.json", "feast_apply.json", "feast_materialize.json"
RUN_ID = re.compile(r"[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}")
JOB_NAME = re.compile(r"[a-z][a-z0-9_]{1,40}")  # the job grammar of orchestration/ecs_launch.py
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
    """The init container and every container of the layer's job catalog entry: each must exit 0."""
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
    """The job's task definition: the Fargate family ``<project>_<env>_job_<layer>`` or its host twin ``..._job_<layer>_host`` that the direct ECS launch
    starts from a DAG step. No other suffix is accepted."""
    return re.compile(rf"arn:aws:ecs:[a-z0-9-]+:[0-9]{{12}}:task-definition/[a-z][a-z0-9_]*_job_{layer}(?:_host)?:[0-9]+")


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


def other_runs_claims(listings: Sequence[Sequence[tuple[str, str, str, int]]]) -> dict[str, set[str]]:
    """The runs whose claim record (``lakehouse/launch_records/<run>/<job>/claim.json``) one of the listings holds, with the jobs of those claims. The run ID
    and the job must match their grammars: any other key names no run."""
    claimed: dict[str, set[str]] = {}
    for versions in listings:
        for key, _, _, _ in versions:
            parts = key.split("/")
            named = len(parts) == 5 and parts[:2] == ["lakehouse", "launch_records"] and parts[4] == "claim.json"
            if named and RUN_ID.fullmatch(parts[2]) and JOB_NAME.fullmatch(parts[3]):
                claimed.setdefault(parts[2], set()).add(parts[3])
    return claimed


def other_runs_key(key: str, claimed: Mapping[str, set[str]]) -> bool:
    """The key lies in the run-owned part of a run that holds a claim record: ``lakehouse/run_stage/<run>/<file>``, ``lakehouse/launch_records/<run>/<file>`` or
    its family index key ``lakehouse/launch_records/families/<job>/<run>.json`` of a job that the claim names. No other key is one run's own write."""
    parts = key.split("/")
    if len(parts) < 4 or parts[0] != "lakehouse" or not all(parts[1:]):
        return False
    if parts[1] in ("run_stage", "launch_records") and parts[2] != "families":
        return parts[2] in claimed
    if len(parts) == 5 and parts[1:3] == ["launch_records", "families"] and parts[4].endswith(".json"):
        return parts[4].removesuffix(".json") in claimed and parts[3] in claimed[parts[4].removesuffix(".json")]
    return False


def listing_checks(
    layer: str, run: str, records: Records, arguments: argparse.Namespace, recorded: Any
) -> tuple[dict[str, bool], dict[str, Any], dict[str, Any]]:
    """H2 and the write scope from two listings of ``lakehouse/`` (and ``metadata/dbt/`` for gold): one before the task and one after. Returns the checks, the
    keys added outside the run's scope and the keys that another run wrote to its own prefixes (each ``OUT_OF_SCOPE_KEYS`` at most, with their count).

    Another run's write is a key under the run's own ``lakehouse/run_stage/<run>/`` or ``lakehouse/launch_records/<run>/`` prefix or its family index key, and
    only while one of the two listings holds that run's claim record. It is named and does not fail the check. Every other key outside the scope fails it."""
    before, before_deleted = read_listing(records, arguments.before, "before")
    after, after_deleted = read_listing(records, arguments.after, "after")
    prefix = INPUT_PREFIX[layer]
    held_before = [item for item in before if item[0].startswith(prefix)]
    held_after = [item for item in after if item[0].startswith(prefix)]
    markers = [item for item in before_deleted if item[0].startswith(prefix)]
    same = held_before == held_after and markers == [item for item in after_deleted if item[0].startswith(prefix)]
    entries = sorted([("V", *item) for item in held_before] + [("D", key, version, "", 0) for key, version in markers], key=lambda entry: entry[1])
    # The run's own uploads, and the launch records and family index key that the direct ECS launch writes for it (config/orchestration/ecs_launch.json).
    allowed = (*WRITE_PREFIXES[layer], f"lakehouse/layer_runs/{layer}/{run}/", f"lakehouse/launch_records/{run}/")
    family = f"lakehouse/launch_records/families/{layer}/{run}.json"  # the write-once family index key of the run's claim
    seen_before = {(key, version) for key, version, _, _ in before} | set(before_deleted)
    seen_after = {(key, version) for key, version, _, _ in after} | set(after_deleted)
    added = [key for key, _ in seen_after - seen_before]
    claimed = other_runs_claims((before, after))
    claimed.pop(run, None)
    unscoped = sorted({key for key in added if not (key.startswith(allowed) or key == family)})
    others = [key for key in unscoped if other_runs_key(key, claimed)]
    outside = [key for key in unscoped if not other_runs_key(key, claimed)]
    checks = {
        "H2: the input object versions are the same before and after the task": same,
        "H2: the pin recorded the digest and count of the listing taken before the task": recorded == version_listing.canonical_digest(prefix, entries),
        "the task added versions only under the layer's prefixes and its own run prefix": bool(added) and not outside,
        "the task removed no version": not seen_before - seen_after,
    }
    return checks, {"count": len(outside), "keys": outside[:OUT_OF_SCOPE_KEYS]}, {"count": len(others), "keys": others[:OUT_OF_SCOPE_KEYS]}


Reader = Callable[[str, str], str]  # (code revision, repository path) -> the file's text at that revision
ARCHIVE_LIMIT = 512 * 1024 * 1024
UNPACKED_LIMIT = 8 * 1024 * 1024 * 1024
MEMBER_LIMIT = 8 * 1024 * 1024
CHUNK = 1 << 20
ARCHIVE_DEADLINE_SECONDS = 720.0


def git_file(revision: str, path: str, read: Reader) -> Any:
    """A JSON file as it is at the code revision the run unpacked."""
    try:
        return json.loads(read(revision, path))
    except ValueError:
        raise AcceptanceError("a frozen file of the run's revision is not JSON") from None


class StagedContract:
    """The files of the publish contract as the staged ``code.tar.gz`` holds them: read in one pass from regular members, never unpacked."""

    def __init__(self, commit: str, files: Mapping[str, str]) -> None:
        self.commit = commit
        self.files = dict(files)

    def read(self, revision: str, path: str) -> str:
        """The reader that the checks take: a file of the run's revision, from the archive of that revision."""
        if revision != self.commit:
            raise AcceptanceError("the staged code archive is not the code revision of the run")
        if path not in self.files:
            raise AcceptanceError("a frozen file of the run's revision is not in the staged code archive")
        return self.files[path]


class BoundedStream(io.RawIOBase):
    """The decompressed bytes of an archive, read and skipped only up to ``limit``: a read or a seek past it raises before it decompresses anything more."""

    def __init__(self, stream: Any, limit: int) -> None:
        self.stream = stream
        self.limit = limit

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def tell(self) -> int:
        return int(self.stream.tell())

    def seek(self, offset: int, whence: int = os.SEEK_SET) -> int:
        target = offset + (self.tell() if whence == os.SEEK_CUR else 0)
        if whence == os.SEEK_END or target > self.limit:
            raise AcceptanceError("the staged code archive is too large to read or took too long")
        return int(self.stream.seek(target))

    def readinto(self, buffer: Any) -> int:
        room = self.limit - self.tell()
        if room < 0:
            raise AcceptanceError("the staged code archive is too large to read or took too long")
        view = memoryview(buffer)[: room + 1]
        count = int(self.stream.readinto(view))
        if self.tell() > self.limit:
            raise AcceptanceError("the staged code archive is too large to read or took too long")
        return count


def contract_from_archive(archive: Path, deadline: float) -> StagedContract:
    """The contract files of a verified archive. A wanted member must be a regular file within the member limit and appear once. No member, read or skip
    reaches past ``UNPACKED_LIMIT`` of decompressed bytes, the last member and the end of the archive included."""
    wanted = set(CONTRACT_FILES)
    files: dict[str, str] = {}
    try:
        with gzip.open(archive, "rb") as stream, tarfile.open(fileobj=BoundedStream(stream, UNPACKED_LIMIT), mode="r:") as tar:
            commit = str(tar.pax_headers.get("comment", ""))
            if not REVISION.fullmatch(commit):
                raise AcceptanceError("the staged code archive carries no commit ID")
            for member in tar:
                if member.offset_data + member.size > UNPACKED_LIMIT or time.monotonic() > deadline:
                    raise AcceptanceError("the staged code archive is too large to read or took too long")
                name = member.name.removeprefix("./")
                if name not in wanted:
                    continue
                if not member.isreg() or member.size > MEMBER_LIMIT or name in files:
                    raise AcceptanceError("a contract file of the staged code archive is not a single regular file within the limit")
                handle = tar.extractfile(member)
                if handle is None:
                    raise AcceptanceError("a contract file of the staged code archive cannot be read")
                try:
                    files[name] = handle.read(MEMBER_LIMIT + 1).decode("utf-8")
                except UnicodeDecodeError:
                    raise AcceptanceError("a contract file of the staged code archive is not text") from None
    except (tarfile.TarError, EOFError, OSError, zlib.error):
        raise AcceptanceError("the staged code archive cannot be read") from None
    return StagedContract(commit, files)


def staged_contract(archive: Path, staged: str, records: Records) -> StagedContract:
    """The contract of the run's staged ``code.tar.gz``, which must exist, stay within its size limit and hash to the staged ``code.sha256``."""
    if not SHA256.fullmatch(staged):
        raise AcceptanceError("the staged code.sha256 is not a SHA-256")
    try:
        descriptor = os.open(archive, os.O_RDONLY | os.O_NOFOLLOW)
    except FileNotFoundError:
        raise AcceptanceError("the staged code archive is missing") from None
    except OSError:
        raise AcceptanceError("the staged code archive cannot be opened (a link or an unreadable file)") from None
    hasher, total = hashlib.sha256(), 0
    with os.fdopen(descriptor, "rb") as handle:
        if not stat.S_ISREG(os.fstat(handle.fileno()).st_mode):
            raise AcceptanceError("the staged code archive is not a regular file")
        while chunk := handle.read(CHUNK):
            total += len(chunk)
            if total > ARCHIVE_LIMIT:
                raise AcceptanceError("the staged code archive is larger than its limit")
            hasher.update(chunk)
    if hasher.hexdigest() != staged:
        raise AcceptanceError("the staged code archive does not match its code.sha256")
    records.sha256["code_archive"] = hasher.hexdigest()
    return contract_from_archive(archive, time.monotonic() + ARCHIVE_DEADLINE_SECONDS)


def contract(layer: str, revision: str, read: Reader) -> tuple[set[str], str]:
    """The tables and the real namespace of the layer's publish contract at the run's revision."""
    try:
        publish = git_file(revision, CONTRACT[layer], read)["publish"]
        return set(publish["tables"]), str(publish["namespaces"]["real"])
    except (KeyError, TypeError):
        raise AcceptanceError("the publish contract of the run's revision has an unexpected shape") from None


def bronze_tables(revision: str, read: Reader) -> set[str]:
    """The pinned bronze tables at the run's revision, as ``pins.mapped_tables`` reads them: the mapped tables a silver source declares, and file_metadata."""
    try:
        mapped = {entry["table"] for entry in git_file(revision, BRONZE_MAP, read)["tables"]}
    except (KeyError, TypeError):
        raise AcceptanceError("the bronze table map of the run's revision has an unexpected shape") from None
    return set(pins.pinned_tables(mapped, pins.declared_sources(read(revision, str(pins.SOURCES)))))


def producer_records(records: Records, layer: str, folder: Path, source: Any, namespace: str, snapshots: Mapping[str, Any]) -> tuple[bool, list[str]]:
    """Whether the passed producer's records name the pinned run and snapshot set, and the records whose digests its report must cover. Silver writes a snapshot
    record. Gold writes none: its set is the commit record, and its candidates record names the mode and namespace of that commit."""
    if layer == "gold":
        producer = records.load_in(folder, "silver_snapshot.json", "input/")
        named = (producer.get("run_id"), producer.get("mode"), producer.get("namespace")) == (source, "real", namespace)
        return named and producer.get("snapshot_set") == snapshots, ["silver_snapshot.json"]
    commit = records.load_in(folder, "publish/commit.json", "input/")
    candidates = records.load_in(folder, "publish/candidates.json", "input/")
    named = (commit.get("run_id"), commit.get("state")) == (source, "committed")
    named = named and (candidates.get("run_id"), candidates.get("mode"), candidates.get("namespace")) == (source, "real", namespace)
    return named and commit.get("snapshot_set") == snapshots, ["publish/commit.json", "publish/candidates.json"]


def report_passed(document: Mapping[str, Any]) -> bool:
    checks = mapping(document.get("checks"))
    return document.get("passed") is True and bool(checks) and all(value is True for value in checks.values())


def producer_checks(
    records: Records, layer: str, run: str, revision: str, pinned: Mapping[str, Any], folder: Path | None, report_path: Path | None, read: Reader
) -> dict[str, bool]:
    """The pin is complete and names the run of the layer below, and that run passed: its report passed and covers its record."""
    below = PRODUCER[layer]
    if folder is None or report_path is None:
        raise AcceptanceError(f"a {layer} report needs the {below} records and the {below} report")
    tables, namespace = contract(below, revision, read)
    source = pinned.get("source_run_id")
    snapshots = mapping(pinned.get("snapshot_set"))
    named, covered = producer_records(records, layer, folder, source, namespace, snapshots)
    report = records.load(report_path, "input/report")
    return {
        f"H3: the {layer} pin is complete: its run, mode, namespace and the {below} contract's tables": (
            pinned.get("run_id"),
            pinned.get("mode"),
            pinned.get("namespace"),
        )
        == (run, "real", namespace)
        and bool(RUN_ID.fullmatch(str(source)))
        and set(snapshots) == tables
        and all(count(value) and value > 0 for value in snapshots.values()),
        f"H3: the pin names the {below} run and its complete snapshot set": named,
        f"the {below} producer passed: its report passed and covers its record": report_passed(report)
        and (report.get("layer"), report.get("run_id")) == (below, source)
        and all(mapping(report.get("file_sha256")).get(name) == records.sha256[f"input/{name}"] for name in covered),
    }


def snapshot_set_digest(snapshot_set: Mapping[str, Any]) -> str:
    """The digest that the Feast jobs record for a publication: ``publication_reads.Publication.snapshot_set_sha256``."""
    return hashlib.sha256(json.dumps(dict(sorted(snapshot_set.items())), separators=(",", ":")).encode()).hexdigest()


def features_checks(records: Records, run: str, namespace: str, folder: Path, snapshot_set: Mapping[str, Any]) -> dict[str, bool]:
    """The features record equals the commit, and Feast applied and loaded exactly this run's publication."""
    record, applied, loaded = (records.load_in(folder, name) for name in (FEATURES_RECORD, FEAST_APPLY, FEAST_LOAD))
    digest = snapshot_set_digest(snapshot_set)
    publication, load = mapping(applied.get("publication")), mapping(loaded.get("publication"))
    objects = mapping(applied.get("objects"))
    return {
        "the features record equals the commit": (record.get("run_id"), record.get("mode"), record.get("namespace")) == (run, "real", namespace)
        and record.get("snapshot_set") == snapshot_set,
        "Feast applied this run's publication: its run, namespace, table count and snapshot set": (
            applied.get("step"),
            applied.get("run_id"),
            applied.get("mode"),
            applied.get("namespace"),
        )
        == ("apply", run, "real", namespace)
        and publication.get("run_id") == run
        and publication.get("tables") == len(snapshot_set)
        and publication.get("snapshot_set_sha256") == digest
        and all(count(objects.get(kind)) and objects[kind] > 0 for kind in ("entity", "feature_view", "data_source")),
        "Feast loaded this run's publication into the online store: its run, snapshot set and a positive entity count": (
            loaded.get("step"),
            loaded.get("run_id"),
            loaded.get("mode"),
        )
        == ("materialize", run, "real")
        and load.get("run_id") == run
        and load.get("snapshot_set_sha256") == digest
        and count(loaded.get("entities"))
        and loaded["entities"] > 0,
    }


def evidence_checks(
    records: Records, layer: str, run: str, folder: Path, staged: str, arguments: argparse.Namespace
) -> tuple[dict[str, bool], dict[str, dict[str, Any]], dict[str, Any]]:
    """The acceptance rules of the layer driver on the uploaded run records, with the frozen publish contract as the reference for every set."""
    identity = records.load_in(folder, "layer_run.json")
    revision = str(identity.get("git_revision"))
    if not REVISION.fullmatch(revision):
        raise AcceptanceError("the run records no code revision")
    read: Reader = arguments.frozen
    tables, namespace = contract(layer, revision, read)
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
        "the run records belong to this run and layer in real mode, with the staged code": (
            identity.get("run_id"),
            identity.get("layer"),
            identity.get("mode"),
        )
        == (run, layer, "real")
        and bool(SHA256.fullmatch(staged))
        and identity.get("code_sha256") == staged,
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
        checks["H3: the pinned table set is the silver-declared bronze tables of the run's revision"] = set(pinned) == bronze_tables(revision, read) and all(
            pinned.values()
        )
    else:
        checks |= producer_checks(records, layer, run, revision, pinned, arguments.input_evidence, arguments.input_report, read)
        input_run = str(pinned.get("source_run_id"))
    if layer == "features":
        checks |= features_checks(records, run, namespace, folder, snapshot_set)
    return checks, summary, {"identity": identity, "input_run": input_run}


def host_problems(status: Any) -> list[str]:
    """What is wrong with the host facts of the running host. The texts are fixed.

    The report runs on the host, so the host is on: the group's desired capacity is at most 1, the group holds its one instance and that instance is the only
    active container instance of the cluster. The host stop is proven by ``host_session stop`` (``host_off_verified``), not by this report."""
    keys = ("desired_capacity", "group_instances", "container_instances")
    if not isinstance(status, dict) or any(key not in status for key in keys):
        return ["the host status lacks its capacity facts"]
    desired, group, containers = (status[key] for key in keys)
    lists = (group, containers)
    if not all(isinstance(item, list) and all(isinstance(entry, str) and entry for entry in item) for item in lists):
        return ["the host status lists its instances in the wrong form"]
    problems = [] if type(desired) is int and 0 <= desired <= 1 else ["the group desired capacity is not at most 1"]
    problems += [] if len(group) == 1 else ["the group does not hold exactly its one host instance"]
    return [*problems, *([] if sorted(containers) == sorted(group) else ["the only active container instance is not the host itself"])]


def ignored(path: Path) -> bool:
    """Whether Git ignores the path: reports stay out of Git."""
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


def staged_digest(records: Records, path: Path) -> str:
    """The text of the staged ``code.sha256``: one SHA-256 and an optional line end. Another text is no digest and fails the checks that name it."""
    raw = read_bytes(path)
    records.sha256["code_digest"] = hashlib.sha256(raw).hexdigest()
    return raw.decode("utf-8", "replace").strip()


def launch_checks(records: Records, layer: str, run: str, folder: Path, staged: str, task: Mapping[str, Any], input_run: str) -> dict[str, bool]:
    """The launch records of the run and job bind the run to the staged code digest, the input run and the described task."""
    claim = records.load(folder_file(folder, "claim.json"), "launch/claim.json")
    attempt = claim.get("attempt")
    attempt_folder = f"{attempt:02d}" if type(attempt) is int and 0 < attempt < 100 else "00"
    started, ended = (
        records.load(folder_file(folder, f"{attempt_folder}/{event}.json"), f"launch/{attempt_folder}/{event}.json") for event in ("started", "end")
    )
    bound = mapping(claim.get("bound"))
    named = all((item.get("run_id"), item.get("job"), item.get("attempt")) == (run, layer, attempt) for item in (claim, started, ended))
    return {
        "the staged code digest is the run's: its run records, its launch claim and its task carry it": bool(SHA256.fullmatch(staged))
        and bound.get("code_digest") == staged,
        "the launch records are this run's one launch: the claim binds the input run, the started record names the described task and the end succeeded": named
        and claim.get("event") == "claim"
        and bound.get("input_run") == (input_run or "latest")
        and started.get("event") == "started"
        and started.get("task_arn") == task.get("taskArn")
        and ended.get("event") == "end"
        and ended.get("outcome") == "succeeded",
    }


def report(arguments: argparse.Namespace) -> dict[str, Any]:
    layer, run = arguments.layer, arguments.run
    records = Records()
    staged = staged_digest(records, arguments.code_digest)
    if getattr(arguments, "frozen", None) is None:
        archive = getattr(arguments, "code_archive", None) or arguments.code_digest.parent / "code.tar.gz"
        arguments = argparse.Namespace(**{**vars(arguments), "frozen": staged_contract(archive, staged, records).read})
    described = records.load(arguments.describe, "describe")
    evidence, summary, found = evidence_checks(records, layer, run, arguments.evidence, staged, arguments)
    tasks = described.get("tasks")
    task = tasks[0] if isinstance(tasks, list) and len(tasks) == 1 and isinstance(tasks[0], dict) else {}
    checks = launch_checks(records, layer, run, arguments.launch_records, staged, task, found["input_run"])
    checks |= {
        "the task is this run's: its definition, its overrides and the staged code digest": task_is_this_runs(
            layer, run, task, staged, str(found["identity"].get("code_run")), found["input_run"]
        ),
        "every container of the task exited 0 and the task stopped": not task_problems(layer, described),
    }
    checks |= evidence
    listed, out_of_scope, other_runs = listing_checks(layer, run, records, arguments, found["identity"].get("input_listing"))
    checks |= listed
    running = records.load(arguments.running_tasks, "running_tasks")
    checks["no task of the job is left running"] = running.get("taskArns") == []
    checks["the host runs alone: its group desired capacity is at most 1 and its one instance is the only active container instance"] = not host_problems(
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
        "out_of_scope": out_of_scope,
        "other_runs_writes": other_runs,
        "file_sha256": dict(sorted(records.sha256.items())),
        "limits": "Staged records and read-only AWS results only. The report does not prove the catalog state it did not read.",
    }


def parser() -> argparse.ArgumentParser:
    cli = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else "")
    steps = cli.add_subparsers(dest="step", required=True)
    for name in ("task", "report"):
        step = steps.add_parser(name)
        step.add_argument("--layer", required=True, choices=LAYERS)
        if name != "task":
            step.add_argument("--run", required=True)
    steps.choices["task"].add_argument("--describe", required=True, type=Path)
    for flag in ("code-digest", "describe", "evidence", "launch-records", "before", "after", "running-tasks", "host-status", "out"):
        steps.choices["report"].add_argument(f"--{flag}", required=True, type=Path)
    steps.choices["report"].add_argument("--code-archive", type=Path, help="the staged code.tar.gz of the run; default: code.tar.gz beside --code-digest")
    steps.choices["report"].add_argument("--input-evidence", type=Path)
    steps.choices["report"].add_argument("--input-report", type=Path)
    return cli


def main(argv: Sequence[str] | None = None) -> int:
    arguments = parser().parse_args(argv)
    result: dict[str, Any]
    try:
        if arguments.step == "task":
            result = {"problems": task_problems(arguments.layer, read_json(arguments.describe))}
            result["passed"] = not result["problems"]
        else:
            if not ignored(arguments.out):
                raise AcceptanceError("the report path must be ignored by Git: reports stay local")
            result = report(arguments)
            write_json(arguments.out, result)
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
