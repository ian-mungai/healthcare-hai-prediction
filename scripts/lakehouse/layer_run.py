"""Phase driver of the silver, gold and features ECS jobs that the direct launch starts.

    python -m scripts.lakehouse.layer_run <phase> --layer silver|gold|features --run <run ID> [--input-run <run ID>] [--work /workspace] [--dry-run]

Each ECS container of a job runs one phase on the shared ``/workspace`` (the container names of ``config/orchestration/ecs_broker_jobs.json``):

- silver: ``pin``, ``build``, ``validate``, ``verify``, ``export``, ``publish``, ``upload``
- gold: ``pin``, ``build``, ``validate``, ``export``, ``publish``, ``upload``, ``maintain``
- features: ``pin``, ``build``, ``validate``, ``export``, ``publish``, ``upload``, ``feast``

The release of a held online load is a phase of the features job and no container of its own (owner decision A, Nov 15 2026; ECS allows 10 containers in a
task definition): the last container, ``feast``, runs it. It releases the held online load of a model set after its model was promoted
(``scripts.features.online_release``, which reads the hold record of the features run itself). The launch names the phase in ``LAYER_PHASE``. A features
build leaves it empty: every build phase runs and ``feast`` applies Feast. A release launch sets it to ``release``: every build phase before ``feast`` ends at
once, because they have nothing to do for a run that is already published, and ``feast`` releases, for the model in ``RELEASE_MODEL`` and the features run
in ``FEATURES_RUN``. The release has no pin and no run folder of its own: the run of the launch only names its launch record.

The same container runs the reload of the online store when the launch names ``reload`` in ``LAYER_PHASE`` (owner decision A, Nov 16 2026): ``host_session
start`` launches it once per online model set after the services are stable. It needs only ``RELEASE_MODEL`` (the set) and runs
``scripts.features.online_reload``, which picks the run of the set itself, so ``FEATURES_RUN`` stays empty. Every build phase ends at once in a reload launch.
Its result line (loaded, already loaded or skipped, a skip naming a held newer run and the reason) is what ``host_session start`` reads from the task's log.

A features job takes ``FEATURE_SET`` (``--feature-set``): empty is the single path that builds every table, ``base`` the same tables as the base of the model
sets, ``m1`` to ``m6`` one model table each. A model set pins the newest committed base run (``INPUT_RUN_ID`` names it; an older base run is refused) and
builds ``feature_matrix_<set>`` from the domain matrices of that run. Every phase after the pin checks that it names the set the pin recorded.

The first container of the task (``fetch``) has already downloaded and unpacked ``code.tar.gz``. ``pin`` checks its SHA-256 again, records the code
identity and pins the input layer: bronze for silver, the silver run ``--input-run`` for gold, the gold run for features (only the newest committed gold
run: the chain reads what its gold job just published, ``features_input --newest-only``). Every later digest comes
from run folder files, so a phase runs only on the exact output of the phase before. The code is the unpacked archive in ``<work>/code``, the working
folder of every module, and the run folder is ``<work>/code/data/analytics/dbt/runs/<run ID>``.

``publish`` runs the publish steps of the layer and the acceptance checks of the first publish (rows and content fingerprints of every table against
its candidate snapshot before the commit, the commit record against the candidates, the backup and the reconcile record after it). Before any catalog
write it also checks the input. The version listing of the layer's input prefix must be the one taken at the pin (a model set lists only the folder of each
table of its base run, never the whole features prefix, so sibling sets that publish beside it change nothing it checks), and the pin must name exactly the
configured bronze tables (silver) or, as a contract check, the tables of the silver publish contract (gold) or the gold publish contract (features). Two extra
checks run beside them: the staged code objects keep their recorded versions and the backup stored in the bucket equals its record. ``upload``, ``maintain``
and ``feast`` run only for a run that passed them. Any failed check exits 1 and names the phase. Features uploads its run records before ``feast``, because the
commit is already done and a Feast failure must not lose its evidence. ``feast`` then adds the records of its own two steps. ``upload`` also copies the run's
dbt files once to ``metadata/dbt/<layer>/<run>/`` for the catalog (best effort).

``maintain`` is the last gold phase, on every gold run: Iceberg snapshot expiry and orphan-file removal on gold (``scripts.lakehouse.gold_maintenance``)
under the chain rule, so it reads no reader record. It keeps the newest ``retain_sets`` committed sets, branch and tag heads, snapshots newer than the
cutoff and orphans younger than a day.

A run ID is used once. The first real pin writes ``lakehouse/layer_runs/<layer>/<run>/state/pinned.json`` only if it does not exist. A second pin of the
same run ID fails with a named error before it writes anything shared. An interrupted run is not resumed: it restarts under a new run ID after the owner has
checked the abandoned catalog branch. A dry run takes no run ID and leaves no state, so it never blocks the real run. A later phase may be repeated in
the workspace of the same task.

The whole task has a deadline, the one the job catalog gives the job, counted from the start time the init container writes to
``<work>/task_started``. A hand-started task has no launch library to stop it, so every phase runs under what is left of that deadline and fails by name when
it passes. The driver then stops the phase module and every process that module started, so nothing goes on writing while the task shuts down.

With ``--dry-run`` or ``LAYER_DRY_RUN=1`` the phases up to ``export`` run as usual, because they write only the task's own working folder. ``publish``
then reads the catalog without writing, prints and saves the plan of the real publish, ``upload`` uploads nothing and ``maintain`` and ``feast`` do nothing. The
first dry phase marks the run folder, so a later phase without the flag stays dry. A new ``pin`` clears the mark."""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import os
import re
import shutil
import signal
import sys
import tarfile
import tempfile
import threading
import time
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from orchestration.pipeline_protocol import PipelineError, digest_file, read_record, seal_copy
from orchestration.training_contract import CODE_RUN
from scripts import resource_peaks
from scripts.features import feature_metrics, feature_sets
from scripts.infrastructure import startup_check
from scripts.lakehouse import code_archive, duckdb_bounds, layer_metrics, pins, provenance, task_capacity, version_listing
from scripts.lakehouse.gold_publish_record import RUN_ID, SHA256, PublishError, accept_consumer
from scripts.process import TimeoutExpired, run_command

PHASES = {
    "silver": ("pin", "build", "validate", "verify", "export", "publish", "upload"),
    "gold": ("pin", "build", "validate", "export", "publish", "upload", "maintain"),
    "features": ("pin", "build", "validate", "export", "publish", "upload", "feast"),
}
RELEASE_PHASE = "release"  # the launch name that makes the last container (feast) release a held online load instead of applying Feast
RELOAD_PHASE = "reload"  # the launch name that makes the last container (feast) reload the online store of a model set instead
RELEASE_RUNNER = "feast"  # the container of the features task that runs a release or reload launch
PHASE_VARIABLE, MODEL_VARIABLE, FEATURES_RUN_VARIABLE = "LAYER_PHASE", "RELEASE_MODEL", "FEATURES_RUN"
LINEAGE_VARIABLE = "LINEAGE_INVOCATION"  # filled by the launch in the pin and upload containers only (orchestration/job_lineage.py)
LINEAGE_PHASES = (
    "pin",
    "upload",
)  # the phases that send lineage events: a START after the pin, the per-table events after the upload and a FAIL after either failed
LINEAGE_MARGIN_SECONDS = 30.0  # the task time that is kept for the phase's own end after the lineage send
STEP_SECONDS = 4 * 3600
STEP_GRACE_SECONDS = 60  # the process runner stops a module this long after the timer of run_module should have
BUCKET_FILE = Path("/workspace/infra/deployment.auto.tfvars.json")  # the path gold_publish.build_store reads for the backup bucket
UPLOAD_PREFIX = "lakehouse/layer_runs"
JOBS_FILE = Path(__file__).resolve().parents[2] / "config/orchestration/ecs_broker_jobs.json"  # the deadline of each job
STARTED_FILE = "task_started"  # in the work folder: the init container writes the start time of the whole task
PHASE_DEADLINE = "HAI_PHASE_DEADLINE_EPOCH"  # read by scripts.features.features_build: the time by which the phase must end
FINGERPRINT = re.compile(r"-?[0-9]{1,38}")  # what the publisher emits: the decimal string of a sum cast to DECIMAL(38, 0)
BRONZE_TABLES = Path(__file__).resolve().parents[2] / "config/lakehouse/bronze_tables.json"
INPUT_LAYER = {"silver": "bronze", "gold": "silver", "features": "gold"}  # the layer whose data each layer reads
LIST_PAGES = 2000  # pages of the input version listing, 1000 versions each: a larger prefix is refused instead of listed in part
STAGE_PREFIX = "lakehouse/run_stage"
STAGED_INPUTS = ("code.tar.gz", "code.sha256")  # the objects the fetch container read; the pin phase records their versions
DBT_ARTIFACTS = ("manifest.json", "catalog.json", "run_results.json")
DBT_ARTIFACT_PREFIX = "metadata/dbt/"
INPUTS_RECORD = "table_inputs.json"  # beside the dbt copy of a run: each published table with the tables its model reads
DBT_RUN_PREFIX = "metadata/dbt/{layer}/{run}/"  # each layer run's own write-once copy for the catalog, one dbt workflow per layer run
# The code copy, the bulk input extracts, the build file the sealed copy repeats and the working databases of the features build, which hold a copy of gold.
NOT_UPLOADED = ("snapshot", "silver_input", "gold_input", "staging.duckdb", "features_work")
FEAST_RECORDS = ("feast_apply.json", "feast_materialize.json")
APPLY_RECORD = FEAST_RECORDS[0]  # put once by the feast phase, never by the run folder upload: a later upload must not replace it
HOLD_RECORD = "online_hold.json"  # a model set whose online load is held leaves it in the run folder; the release reads it from the run prefix
DBT_TARGET = {"features": Path("features_work/build/target")}  # where each layer build writes its dbt artifacts, beside the default <run>/target
DRY_ENV = "LAYER_DRY_RUN"
DRY_MARKER = "dry_run"  # in the run folder: once a task has a dry phase, every later phase of it is dry
PLAN_FILE = "publish/dry_run_plan.json"
PLAN_PREFIX = "DRY_RUN_PLAN "  # the printed plan line, so the owner can find it in the task log
SUITES = "data_contracts/great_expectations"
FEATURE_FILES = [
    "--schema",
    "data_contracts/features/feature_schema.json",
    "--contract",
    "data_contracts/lakehouse/features_publish.json",
]  # in the code folder
QUALITY = {"silver": ("silver",), "gold": ("gold", "cross"), "features": ("features",)}
PIN_FILE = {"silver": "pins.json", "gold": "silver_pins.json", "features": "gold_pins.json"}
INPUT_FOLDER = {"gold": "silver_input", "features": "gold_input"}
RECORD_FILE = {"silver": "silver_snapshot.json", "features": "features_snapshot.json"}  # gold records nothing: its snapshot set is the commit record


class StepError(RuntimeError):
    """A phase cannot prove its input or a module it ran failed."""


@dataclass(frozen=True)
class Job:
    layer: str
    run: str
    mode: str
    code: Path
    local: Path | None = None
    dry_run: bool = False
    input_run: str = ""
    feature_set: str = ""  # features only: empty is the single path, else base or a model set
    code_run: str = ""  # the run whose staged code the task fetched: a chain layer fetches the bronze run's code, not code staged under its own run ID

    @property
    def staged_run(self) -> str:
        return self.code_run

    @property
    def folder(self) -> Path:
        return self.code / "data/analytics/dbt/runs" / self.run

    @property
    def publish_folder(self) -> Path:
        return self.folder / "publish"

    @property
    def model_set(self) -> bool:
        return self.layer == "features" and self.feature_set in feature_sets.MODEL_SETS

    @property
    def set_flags(self) -> list[str]:
        """The set argument of the modules that scope their work to a set."""
        return ["--feature-set", self.feature_set] if self.feature_set else []

    @property
    def pin_name(self) -> str:
        """The pin record of the layer's input: a model set pins a base run, which sits in the features layer."""
        return "base_pins.json" if self.model_set else PIN_FILE[self.layer]

    @property
    def input_name(self) -> str:
        return "base_input" if self.model_set else INPUT_FOLDER[self.layer]

    @property
    def local_flags(self) -> list[str]:
        return ["--local-warehouse", str(self.local)] if self.local else []


def descendants(root: int) -> list[int]:
    """Every process started, directly or not, by ``root``. A killed module leaves its own children behind, such as the Spark JVM of a publish step."""
    parents: dict[int, int] = {}
    proc = Path("/proc")
    if proc.is_dir():
        for entry in proc.iterdir():
            if entry.name.isdigit():
                try:
                    parents[int(entry.name)] = int((entry / "stat").read_text().rsplit(")", 1)[1].split()[1])
                except (OSError, ValueError, IndexError):
                    continue
    else:
        listing = run_command("ps", ["-A", "-o", "pid=,ppid="], timeout=10).stdout
        parents = {int(pid): int(parent) for pid, parent in (line.split() for line in listing.splitlines() if line.strip())}
    found, frontier = [], [root]
    while frontier:
        parent = frontier.pop()
        for pid, owner in parents.items():
            if owner == parent and pid not in found:
                found.append(pid)
                frontier.append(pid)
    return found


def kill_descendants() -> None:
    """Stop everything the driver started: after a deadline nothing of the phase may go on writing while the container shuts down."""
    for pid in descendants(os.getpid()):
        with contextlib.suppress(ProcessLookupError, PermissionError):
            os.kill(pid, signal.SIGKILL)


def run_module(job: Job, module: str, arguments: Sequence[str | Path], environment: dict[str, str] | None = None) -> str:
    """Run ``python -m module`` in the code folder and return its standard output; a nonzero exit or a step limit names the module.

    The limit is enforced here and not by the process runner, because the runner kills only the module: its children would then belong to no process the
    driver can find. A timer stops the whole tree at the limit, while the module still links it. The runner's own limit is a later backstop.
    """
    expired = threading.Event()

    def stop_tree() -> None:
        expired.set()
        kill_descendants()

    child = {**os.environ, **(environment or {})}
    if job.layer == "features":
        child[feature_sets.VARIABLE] = job.feature_set  # the set main checked: the task's all becomes the single path's empty value
    watchdog = threading.Timer(STEP_SECONDS, stop_tree)
    watchdog.daemon = True
    watchdog.start()
    try:
        done = run_command(
            sys.executable,
            ["-m", module, *map(str, arguments)],
            cwd=job.code,
            env=child,
            timeout=STEP_SECONDS + STEP_GRACE_SECONDS,
        )
    except TimeoutExpired:
        expired.set()
        raise StepError(f"{module} ran longer than {STEP_SECONDS} seconds") from None
    finally:
        watchdog.cancel()
        kill_descendants()
    if expired.is_set():
        raise StepError(f"{module} ran longer than {STEP_SECONDS} seconds")
    sys.stdout.write(done.stdout)
    sys.stderr.write(done.stderr)
    if done.returncode and duckdb_bounds.cap_reached(done.stdout + done.stderr):
        raise StepError(
            f"{module} stopped at the temporary storage cap of DuckDB: it needed more than HAI_DUCKDB_MAX_TEMP_BYTES = {child['HAI_DUCKDB_MAX_TEMP_BYTES']} "
            f"bytes in HAI_DUCKDB_TEMP_DIRECTORY = {child['HAI_DUCKDB_TEMP_DIRECTORY']}, the share of the scratch space that was free when the phase started. "
            "Raise the scratch size of the layer task only after a measured need, or reduce what the phase holds on disk."
        )
    if done.returncode:
        raise StepError(f"{module} exited {done.returncode}")
    return done.stdout


def capacity_profile(layer: str, phase: str) -> str:
    """The thread profile of a phase for ``task_capacity``: the features build holds more memory for each thread than any other phase."""
    return "features_build" if (layer, phase) == ("features", "build") else "default"


def phase_deadline_environment(job: Job) -> dict[str, str]:
    """The absolute time (epoch seconds) by which the phase module must end: what is left of the task deadline, and no more than one step of this driver."""
    left = min(task_seconds_left(job), STEP_SECONDS)
    if left <= 0:
        raise StepError("the task deadline has passed")
    return {PHASE_DEADLINE: f"{time.time() + left:.0f}"}


def task_seconds_left(job: Job) -> float:
    """Seconds left of the whole task: the job's deadline in the job catalog, counted from the start time that the init container wrote. A hand-started
    task has no launch library to stop it, so the driver enforces the deadline itself."""
    limit = float(json.loads(JOBS_FILE.read_text())["jobs"][job.layer]["deadline_seconds"])
    started = job.code.parent / STARTED_FILE
    if not started.is_file():
        started.parent.mkdir(parents=True, exist_ok=True)
        started.write_text(f"{time.time():.0f}\n")
    return limit - (time.time() - float(started.read_text()))


@contextmanager
def task_deadline(job: Job) -> Iterator[None]:
    """Run one phase under what is left of the task deadline: a phase that starts late or runs too long fails by name."""
    left = task_seconds_left(job)
    if left <= 0:
        raise StepError("the task deadline has passed")

    def expire(signum: int, frame: Any) -> None:
        kill_descendants()
        raise StepError("the task deadline passed during the phase")

    previous = signal.signal(signal.SIGALRM, expire)
    signal.setitimer(signal.ITIMER_REAL, left)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


def s3_client() -> Any:
    import boto3

    return boto3.client("s3")


def identity(job: Job) -> dict[str, Any]:
    return read_record(job.folder, "layer_run.json")[0]


def source_digest(job: Job) -> str:
    """The digest of the code the pin phase unpacked, computed again; it must still be the one the pin recorded."""
    digest = provenance.tree_digest(job.folder / "snapshot")
    if digest != identity(job)["source_sha256"]:
        raise StepError("the code snapshot changed after the pin phase")
    return digest


def input_digests(job: Job) -> tuple[str, str | None]:
    """The digest of the layer's input pin and of its input extract (silver has none)."""
    pin = digest_file(job.folder / job.pin_name)
    return (pin, None) if job.layer == "silver" else (pin, digest_file(job.folder / job.input_name / "manifest.json"))


def staged_objects(client: Any, bucket: str, run: str) -> dict[str, dict[str, Any]]:
    """The version, entity tag and size of each staged input object, as the bucket reports them now."""
    found = {}
    for name in STAGED_INPUTS:
        head = client.head_object(Bucket=bucket, Key=f"{STAGE_PREFIX}/{run}/inputs/{name}")
        found[name] = {"version_id": head.get("VersionId", ""), "etag": head["ETag"], "bytes": head["ContentLength"]}
    return found


def input_prefix(job: Job) -> str:
    """The prefix of the layer's input. A model set reads the domain matrices of its base run, which share the features prefix with the tables that its sibling
    sets write, so ``input_prefixes`` narrows it."""
    return f"lakehouse/{'features' if job.model_set else INPUT_LAYER[job.layer]}/"


def input_prefixes(job: Job) -> list[str]:
    """The prefixes whose versions the run read: the input layer's prefix, or for a model set the folder of each table of its base run.

    The build reads the domain matrices, and the Feast step verifies the head of every table that the base run published (the spine, the support and label
    tables, the build inputs) and reads the label schema. The base run publishes every table of the features contract. The tables of the model sets are not in
    it, so the tables that sibling sets publish stay outside, as do the publish ledger and the catalog backups."""
    if not job.model_set:
        return [input_prefix(job)]
    from scripts.features import features_publish

    return [f"{input_prefix(job)}{table}/" for table in sorted(features_publish.publish_plan(job.mode).tables)]


def input_listing(client: Any, bucket: str, prefix: str) -> dict[str, Any]:
    """The version listing of the layer's input prefix as one digest and a count: every version and delete marker with its ID, entity tag and size. The rule
    is ``version_listing.canonical_digest``, which the owner's acceptance report uses too. The listing is read page by page and never held whole. It is bounded
    by ``LIST_PAGES`` and by the task deadline, and a prefix that does not fit is refused."""

    def entries() -> Iterator[version_listing.Entry]:
        markers: dict[str, str] = {}
        for _ in range(LIST_PAGES):
            page = client.list_object_versions(Bucket=bucket, Prefix=prefix, **markers)
            found = [("V", item) for item in page.get("Versions", [])] + [("D", item) for item in page.get("DeleteMarkers", [])]
            for kind, item in sorted(found, key=lambda entry: entry[1]["Key"]):
                yield kind, item["Key"], item["VersionId"], item.get("ETag", ""), item.get("Size", 0)
            if not page.get("IsTruncated"):
                return
            markers = {"KeyMarker": page["NextKeyMarker"], "VersionIdMarker": page["NextVersionIdMarker"]}
        raise StepError(f"the version listing of {prefix} is larger than {LIST_PAGES} pages: refusing to compare a partial listing")

    return version_listing.canonical_digest(prefix, entries())


def input_record(job: Job, client: Any, bucket: str) -> dict[str, Any]:
    """The listing the pin records and publish compares. A layer has one listing of its input prefix. A model set has one listing per table folder of its
    base run, and a folder without an object is refused: the pin would then guard nothing there."""
    prefixes = input_prefixes(job)
    if not job.model_set:
        return input_listing(client, bucket, prefixes[0])
    listings = {prefix: input_listing(client, bucket, prefix) for prefix in prefixes}
    empty = [prefix for prefix, listing in listings.items() if not listing["versions"]]
    if empty:
        raise StepError(f"the input folders {empty} hold no object: the input listing would guard nothing there")
    return {"prefixes": listings}


def claim_run(job: Job, bucket: str, client: Any) -> None:
    """Take the run ID, once. The first pin of a real run writes ``state/pinned.json`` under the run's prefix only if no such object exists, so a second
    task for the same run ID is refused here, before it unpacks, pins or writes anything shared. An interrupted run is not resumed: it restarts under a new
    run ID after the owner has checked the abandoned catalog branch."""
    key = f"{UPLOAD_PREFIX}/{job.layer}/{job.run}/state/pinned.json"
    body = json.dumps({"run_id": job.run, "layer": job.layer, "mode": job.mode}, sort_keys=True) + "\n"
    try:
        client.put_object(Bucket=bucket, Key=key, Body=body, IfNoneMatch="*")
    except client.exceptions.ClientError as error:
        if error.response["Error"]["Code"] not in ("PreconditionFailed", "ConditionalRequestConflict"):
            raise
        raise StepError(
            f"run {job.run} already has state in the bucket ({key}). A used run ID is never resumed: "
            "check the abandoned catalog branch, then start a new run ID"
        ) from None


def check_input_versions(job: Job, bucket: str, client: Any) -> None:
    """The input prefixes hold the same versions as when the pin phase listed them: nothing added, replaced or deleted during the run."""
    recorded = identity(job).get("input_listing")
    if not recorded:
        raise StepError("the pin phase recorded no listing of the input versions")
    current = input_record(job, client, bucket)
    if current == recorded:
        return
    if job.model_set:
        before = recorded.get("prefixes", {})
        moved = sorted(prefix for prefix, listing in current["prefixes"].items() if before.get(prefix) != listing)
        raise StepError(f"an input version changed during the run: the listing of {', '.join(moved)} differs from the one taken at the pin")
    raise StepError(f"an input version changed during the run: the listing of {recorded['prefix']} differs from the one taken at the pin")


def check_pin_tables(job: Job) -> None:
    """Silver: the pin names exactly the configured bronze tables. Gold and features: the pin step has already read the publish_runs row of the input run and
    pinned its snapshot set, so this compares the pin's table list with the tables of the input layer's publish contract. The publish container holds the
    writer pair, which has no grant on the input namespace, so it cannot read the row again. scripts.lakehouse.layer_acceptance compares the pin with the
    saved records of the input run."""
    if job.layer == "silver":
        pinned = set(json.loads((job.folder / PIN_FILE["silver"]).read_text()))
        wanted, source = set(pins.mapped_tables(BRONZE_TABLES)), BRONZE_TABLES.name
    elif job.layer == "gold":
        pinned = set(read_record(job.folder, PIN_FILE["gold"])[0].get("snapshot_set", {}))
        wanted, source = set(publish_contract(replace(job, layer="silver")).tables), "the silver publish contract"
    elif job.model_set:
        from scripts.features import features_input

        pinned = set(read_record(job.folder, job.pin_name)[0].get("snapshot_set", {}))
        wanted, source = set(features_input.base_tables()), "the domain matrices of the features contract"
    else:
        pinned = set(read_record(job.folder, PIN_FILE["features"])[0].get("snapshot_set", {}))
        wanted, source = set(publish_contract(replace(job, layer="gold")).tables), "the gold publish contract"
    if pinned != wanted:
        kind = " (a contract check, not a publish_runs row comparison)" if job.layer != "silver" else ""
        raise StepError(f"the {job.layer} pin's table list differs from {source}{kind}: missing {sorted(wanted - pinned)}, extra {sorted(pinned - wanted)}")


def check_input_objects(job: Job, bucket: str, client: Any) -> None:
    """The staged input objects are the ones the pin phase recorded, version by version."""
    recorded = identity(job).get("input_objects")
    if not recorded:
        raise StepError("the pin phase recorded no versions of the staged input objects")
    if staged_objects(client, bucket, job.staged_run) != recorded:
        raise StepError("a staged input object changed after the pin phase")


def check_stored_backup(job: Job, bucket: str, client: Any) -> None:
    """The catalog backup in the bucket is the one the backup record names, byte for byte, and belongs to this run."""
    prefix = publish_contract(job).backup_prefix
    backup = read_record(job.publish_folder, "backup.json")[0]
    if not prefix:
        raise StepError("the layer's publish contract names no backup prefix")
    stored = client.get_object(Bucket=bucket, Key=f"{prefix}{backup['name']}")["Body"].read()
    if hashlib.sha256(stored).hexdigest() != backup.get("backup_sha256") or json.loads(stored).get("run_id") != job.run:
        raise StepError("the stored backup differs from the backup record or belongs to another run")


def bundle(job: Job, archive: Path, code_sha256: str, evidence: dict[str, Any] | None = None) -> None:
    """Check the archive digest, unpack it, copy the dbt project and packages into the snapshot layout and record the code identity."""
    if not SHA256.fullmatch(code_sha256) or digest_file(archive) != code_sha256:
        raise StepError("code.tar.gz does not match the expected SHA-256")
    job.code.mkdir(parents=True, exist_ok=True)
    with tarfile.open(archive) as tar:
        revision = str(tar.pax_headers.get("comment", ""))  # git archive records the commit in the pax global header
        tar.extractall(job.code, filter="data")
    if not provenance.REVISION.fullmatch(revision):
        raise StepError("code.tar.gz carries no commit ID (it must come from git archive)")
    snapshot = job.folder / "snapshot"
    shutil.rmtree(snapshot, ignore_errors=True)
    for name, source in (("project", "dbt"), ("packages", "dbt_packages")):
        shutil.copytree(job.code / source, snapshot / name)
    record: dict[str, Any] = {
        "run_id": job.run,
        "layer": job.layer,
        "mode": job.mode,
        "code_run": job.staged_run,
        "code_sha256": code_sha256,
        "git_revision": revision,
    }
    record["source_sha256"] = provenance.tree_digest(snapshot)
    if job.feature_set:
        record["feature_set"] = job.feature_set
    record |= evidence or {}
    (job.folder / "layer_run.json").write_text(json.dumps(record, indent=2, sort_keys=True) + "\n")


def check_input_run(job: Job) -> None:
    """Gold and features pin a committed run of the layer below, which the caller must name. Checked before the run ID is claimed."""
    if job.layer != "silver" and not RUN_ID.fullmatch(job.input_run):
        raise StepError(f"the {job.layer} pin needs the input run ID (INPUT_RUN_ID or --input-run)")


def check_code_run(job: Job) -> None:
    """Every phase after the pin must name the code run that the pin recorded: a container that dropped the variable or got another one is refused."""
    recorded = identity(job).get("code_run")
    if recorded != job.code_run:
        raise StepError(f"CODE_RUN_ID {job.code_run!r} differs from the code run {recorded!r} that the pin recorded")


def check_feature_set(job: Job) -> None:
    """Every phase after the pin must name the feature set that the pin recorded: a container that dropped the variable or got another one is refused."""
    recorded = identity(job).get("feature_set", "")
    if recorded != job.feature_set:
        raise StepError(f"FEATURE_SET {job.feature_set!r} differs from the feature set {recorded!r} that the pin recorded")


def check_archive(archive: Path, code_sha256: str) -> None:
    """Refuse an archive the pin cannot use before the run ID is claimed, so a bad artifact does not consume the approved ID."""
    try:
        code_archive.check(archive, expected=code_sha256)
    except (code_archive.ArchiveError, OSError, tarfile.TarError) as error:
        raise StepError(f"code_archive: {error}") from None


def pin(job: Job) -> None:
    if job.layer == "silver":
        run_module(job, "scripts.lakehouse.pins", ["pin", "--run", job.run])
        return
    record = job.folder / job.pin_name
    common = ["--run", job.run, "--mode", job.mode, "--source-run", job.input_run]
    if job.layer == "gold":
        run_module(job, "scripts.lakehouse.silver_publish", ["pin", *common, "--record", record, *job.local_flags])
        arguments: list[str | Path] = [
            "--run",
            job.run,
            "--mode",
            job.mode,
            "--expected-pin-sha256",
            digest_file(record),
            "--folder",
            job.folder,
            *job.local_flags,
        ]
        run_module(job, "scripts.lakehouse.silver_input", arguments)
        return
    run_module(
        job, "scripts.features.features_input", ["pin", *common, "--newest-only", "--folder", job.folder, "--record", record, *job.set_flags, *job.local_flags]
    )
    run_module(
        job,
        "scripts.features.features_input",
        ["read", "--run", job.run, "--mode", job.mode, "--expected-pin-sha256", digest_file(record), "--folder", job.folder, *job.set_flags, *job.local_flags],
    )
    if read_record(job.folder, job.pin_name)[0].get("source_run_id") != job.input_run:
        raise StepError(f"the features pin names another {'base' if job.model_set else 'gold'} run than INPUT_RUN_ID")


def verify(job: Job) -> None:
    """Silver only: no bronze table moved since the pin."""
    run_module(job, "scripts.lakehouse.pins", ["verify", "--run", job.run])


def link_profile_folder(job: Job) -> None:
    """The tracked dbt profiles keep their database at /workspace/out/staging.duckdb. Make that folder a link to the run folder, so the build writes the
    file that validation and the sealed copy read."""
    out = job.code.parent / "out"
    if out.is_symlink():
        out.unlink()
    elif out.exists():
        raise StepError(f"{out} exists and is not a link to a run folder")
    out.symlink_to(job.folder)


def build(job: Job) -> None:
    link_profile_folder(job)
    source = source_digest(job)
    input_sha, extract = input_digests(job)
    snapshot = job.folder / "snapshot"
    environment = {
        "DBT_PACKAGES_INSTALL_PATH": str(snapshot / "packages"),
        "DBT_TARGET_PATH": str(job.folder / "target"),
        "DBT_LOG_PATH": str(job.folder / "logs"),
        "STAGING_E2E_CASE": str(job.folder),
        "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
    }
    if job.layer == "silver" and job.mode == "real":
        environment |= {
            provenance.VARIABLE_RUN_ID: job.run,
            provenance.VARIABLE_TREE: source,
            provenance.VARIABLE_PINS_SHA: input_sha,
            provenance.VARIABLE_PINS_JSON: (job.folder / "pins.json").read_text(),
            provenance.VARIABLE_REVISION: identity(job)["git_revision"],
            provenance.VARIABLE_REQUIRED: "1",
        }
    # The features builder takes the parent of --project as the repository root and reads the contracts and seeds there, so it gets the code folder.
    project = job.code / "dbt" if job.layer == "features" else snapshot / "project"
    arguments: list[str | Path] = ["--mode", job.mode, "--run", job.run, "--folder", job.folder, "--project", project]
    arguments += ["--source-sha256", source, "--input-sha256", input_sha, *(["--extract-sha256", extract] if extract else [])]
    try:
        if job.layer == "features":
            run_module(job, "scripts.features.features_build", [*arguments, *job.set_flags], environment | phase_deadline_environment(job))
        else:
            run_module(job, "scripts.lakehouse.pipeline_build", ["--layer", job.layer, *arguments], environment)
    finally:
        push_dbt_results(job)


def push_dbt_results(job: Job) -> None:
    """Hand the dbt run of the build to the exit push; a missing file adds nothing and nothing here can fail the job."""
    layer_metrics.dbt_file(job.folder / "target" / "run_results.json")


def push_quality_results(job: Job) -> None:
    """Hand the quality gates of the layer to the exit push, with the same rule."""
    for kind in QUALITY.get(job.layer, ()):
        layer_metrics.quality_file(kind, job.folder / f"quality_{kind}" / "result.json")


def validate(job: Job) -> None:
    """Run the layer's quality gates and require every expectation to pass on the same build bytes."""
    baselines = "fixture_baselines.json" if job.mode == "publish_fixture" else "baselines.json"
    try:
        for kind in QUALITY[job.layer]:
            output = job.folder / f"quality_{kind}"
            output.mkdir(exist_ok=True)
            suites = {"silver": ["suites"], "gold": ["gold"], "cross": ["suites", "gold"], "features": ["features"]}[kind]
            arguments: list[str | Path] = [
                "--database",
                job.folder / "staging.duckdb",
                "--suites",
                *(Path(SUITES) / name for name in suites),
                "--output",
                output / "result.json",
            ]
            if kind == "features":
                run_module(job, "scripts.features.feature_validation", [*arguments, *FEATURE_FILES, *job.set_flags])
            elif kind == "cross":
                run_module(job, "scripts.lakehouse.gold_reconcile", [*arguments, "--baselines", Path(SUITES) / baselines])
            else:
                run_module(job, "scripts.lakehouse.silver_quality", [*arguments, "--baselines", Path(SUITES) / baselines])
    finally:
        push_quality_results(job)
    validated_sha(job)


def validated_sha(job: Job) -> str:
    """The digest of the build file that every quality result of the layer passed; they must agree."""
    results = {kind: read_record(job.folder, f"quality_{kind}/result.json")[0] for kind in QUALITY[job.layer]}
    for kind, result in results.items():
        counts = result.get("counts", {})
        if counts.get("fail") != 0 or counts.get("warn") != 0:
            raise StepError(f"the {kind} quality result does not pass every expectation")
    shas = {str(result.get("build_file_sha256")) for result in results.values()}
    if len(shas) != 1 or not SHA256.fullmatch(next(iter(shas))):
        raise StepError("the quality results name different build bytes")
    if "cross" in results:
        found = results["cross"].get("reconciliation", {})
        if not found.get("facts") or found.get("facts") != found.get("facts_in_contract") or any(found.get("problems", {}).values()):
            raise StepError("the cross-layer reconciliation is incomplete or has problems")
    return shas.pop()


def sealed(job: Job) -> tuple[str, Path]:
    sha = validated_sha(job)
    path = job.folder / "layer_sealed" / job.layer / f"{sha}.duckdb"
    if digest_file(path) != sha:
        raise StepError("the sealed copy differs from the validated build")
    return sha, path


def seal(job: Job) -> None:
    """Accept the validated bytes and make the write-once sealed copy; the build receipt must belong to this run, code and input."""
    sha = validated_sha(job)
    receipt = read_record(job.folder, "build_receipt.json")[0]
    if (receipt.get("run_id"), receipt.get("layer"), receipt.get("mode")) != (job.run, job.layer, job.mode):
        raise StepError("the build receipt belongs to another run, layer or mode")
    if receipt.get("source_sha256") != source_digest(job) or receipt.get("input_sha256") != input_digests(job)[0]:
        raise StepError("the build receipt differs from the code and input digests")
    seal_copy(job.folder, job.layer, sha)


def publish_contract(job: Job) -> Any:
    """The publish contract of the layer, read the way the publish modules read it."""
    from scripts.features import features_publish
    from scripts.lakehouse import gold_publish, silver_publish

    if job.layer == "silver":
        return silver_publish.publish_plan(job.mode)
    return gold_publish.load_plan() if job.layer == "gold" else features_publish.publish_plan(job.mode, feature_set=job.feature_set)


def accepted_result(job: Job, sha: str) -> str:
    """The digest of the consumer result after the one acceptance rule: the run, the sealed digest and a completed export."""
    result, digest = read_record(job.publish_folder, "consumer_result.json")
    accept_consumer(result, {"sha256": sha}, job.run)
    return digest


def export(job: Job) -> None:
    seal(job)
    sha, path = sealed(job)
    job.publish_folder.mkdir(exist_ok=True)
    arguments: list[str | Path] = ["--run", job.run, "--sealed-sha256", sha, "--publish-dir", job.publish_folder, "--sealed", path]
    if job.layer == "silver":
        run_module(job, "scripts.lakehouse.silver_export", arguments)
    elif job.layer == "gold":
        run_module(job, "scripts.lakehouse.gold_export", arguments)
    else:
        run_module(job, "scripts.features.features_export", [*arguments, *job.set_flags])
    accepted_result(job, sha)
    manifest = read_record(job.publish_folder, "export/manifest.json")[0]
    if set(manifest.get("tables", {})) != set(publish_contract(job).tables):
        raise StepError("the export does not hold exactly the contract's tables: a table outside the layer's namespace is refused")


def write_bucket_file(bucket: str) -> None:
    """The one-key file the backup reads for the bucket name."""
    BUCKET_FILE.parent.mkdir(parents=True, exist_ok=True)
    temporary = BUCKET_FILE.with_name(f".{BUCKET_FILE.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps({"data_bucket_name": bucket}) + "\n")
    os.replace(temporary, BUCKET_FILE)


def accept_reconcile(job: Job) -> None:
    """Before any commit: rows and content fingerprints of exactly the candidate snapshots equal the export, table by table."""
    folder = job.publish_folder
    manifest = read_record(folder, "export/manifest.json")[0]
    candidates, candidates_sha = read_record(folder, "candidates.json")
    reconcile = read_record(folder, "reconcile.json")[0]
    tables = set(publish_contract(job).tables)
    problems = []
    if reconcile.get("pass") is not True or reconcile.get("run_id") != job.run or reconcile.get("candidates_sha256") != candidates_sha:
        problems.append("the reconcile record is not a pass for these candidates")
    if candidates.get("complete") is not True or set(candidates.get("tables", {})) != tables or set(reconcile.get("tables", {})) != tables:
        problems.append("the candidates or the reconcile record do not name exactly the contract's tables")
    for name in sorted(tables & set(reconcile.get("tables", {})) & set(candidates.get("tables", {}))):
        entry = reconcile["tables"][name]
        if entry.get("candidate_snapshot_id") != candidates["tables"][name]["candidate_snapshot_id"]:
            problems.append(f"{name}: reconciled another snapshot than the candidate")
        if not entry.get("rows") == entry.get("export_rows") == entry.get("manifest_rows") == manifest["tables"][name]["rows"]:
            problems.append(f"{name}: the candidate's rows differ from the export")
        if entry.get("fingerprint") != entry.get("export_fingerprint") or not FINGERPRINT.fullmatch(str(entry.get("fingerprint"))):
            problems.append(f"{name}: the candidate's fingerprint differs from the export")
    if problems:
        raise StepError("publish acceptance failed: " + "; ".join(problems[:5]))


def accept_commit(job: Job) -> None:
    """After the commit: the commit record is committed, follows the backup that followed the reconcile, and names every candidate snapshot ID."""
    folder = job.publish_folder
    candidates = read_record(folder, "candidates.json")[0]
    reconcile, reconcile_sha = read_record(folder, "reconcile.json")
    backup, backup_sha = read_record(folder, "backup.json")
    commit = read_record(folder, "commit.json")[0]
    wanted = {name: item["candidate_snapshot_id"] for name, item in candidates["tables"].items()}
    problems = []
    if commit.get("state") != "committed" or commit.get("run_id") != job.run:
        problems.append("the commit record does not show this run committed")
    if backup.get("read_back") is not True or backup.get("reconcile_sha256") != reconcile_sha or commit.get("backup_sha256") != backup_sha:
        problems.append("the backup was not read back after this reconcile, or the commit does not follow it")
    if commit.get("snapshot_set") != wanted:
        problems.append("the commit's snapshot set differs from the candidate snapshot IDs")
    if problems:
        raise StepError("publish acceptance failed: " + "; ".join(problems))


def run_publish(job: Job, arguments: Sequence[str | Path]) -> None:
    if job.layer == "silver":
        run_module(job, "scripts.lakehouse.silver_publish", arguments)
    elif job.layer == "gold":
        run_module(job, "scripts.lakehouse.gold_publish", arguments)
    else:
        run_module(job, "scripts.features.features_publish", [*arguments, *job.set_flags])


def publish(job: Job, bucket: str, client: Any = None) -> None:
    sha, _ = sealed(job)
    accepted = accepted_result(job, sha)
    if job.mode == "real":
        write_bucket_file(bucket)
    common = ["--run", job.run, "--mode", job.mode]
    check_pin_tables(job)
    if client is not None:
        check_input_versions(job, bucket, client)
        check_input_objects(job, bucket, client)
    pin_flags: list[str | Path] = ["--base-pin", job.folder / job.pin_name] if job.model_set else []
    for step in ("candidates", "reconcile", "backup", "commit"):
        step_flags = pin_flags if step in ("candidates", "commit") else []
        run_publish(
            job,
            [step, *common, "--sealed-sha256", sha, "--accepted-result-sha256", accepted, "--publish-dir", job.publish_folder, *step_flags, *job.local_flags],
        )
        if step == "reconcile":
            accept_reconcile(job)
        if step == "backup" and client is not None:
            check_stored_backup(job, bucket, client)
        if step == "commit":
            accept_commit(job)
            layer_metrics.publish_folder(job.publish_folder)
    if job.layer in RECORD_FILE:
        run_publish(
            job, ["record", *common, "--publish-dir", job.publish_folder, "--record", job.folder / RECORD_FILE[job.layer], *pin_flags, *job.local_flags]
        )
    sys.stdout.write(f"publish acceptance passed for {job.layer} run {job.run}\n")


def accepted_publish(job: Job) -> None:
    """The run's records must still show an accepted publish before anything leaves the task."""
    accept_reconcile(job)
    accept_commit(job)


def uploadable(job: Job) -> list[Path]:
    files = (path for path in sorted(job.folder.rglob("*")) if path.is_file() and not path.is_symlink())
    return [path for path in files if path.relative_to(job.folder).parts[0] not in NOT_UPLOADED and path.relative_to(job.folder).as_posix() != APPLY_RECORD]


def upload(job: Job, bucket: str, client: Any) -> int:
    """Copy the run folder to ``lakehouse/layer_runs/<layer>/<run>/``; a repeat writes the same keys."""
    accepted_publish(job)
    prefix = f"{UPLOAD_PREFIX}/{job.layer}/{job.run}/"
    files = uploadable(job)
    for path in files:
        client.upload_file(str(path), bucket, prefix + path.relative_to(job.folder).as_posix())
    return len(files)


def catalog_copy(name: str, path: Path, tables: Mapping[str, str] | None) -> bytes | None:
    """The dbt file as the catalog reads it: each model at its published table (``scripts/lakehouse/dbt_catalog_artifacts.py``), or None with the cause
    on standard error when the build names a model or relation that has no published table. The file as built is never sent."""
    from scripts.lakehouse import dbt_catalog_artifacts as artifacts

    try:
        found = artifacts.layer_tables() if tables is None else tables
        document = json.loads(path.read_bytes())
        left = artifacts.unpublished(document, found) if name == "manifest.json" else []
        if left:
            sys.stderr.write(f"layer_run: {len(left)} models of the manifest have no published table and stay as built (for example {left[0]})\n")
        return artifacts.encoded(artifacts.REWRITERS[name](document, found))
    except (artifacts.ArtifactError, ValueError, KeyError) as error:
        sys.stderr.write(f"layer_run: the dbt file {name} was not copied for the catalog: {error}\n")
        return None


def upload_dbt_artifacts(job: Job, bucket: str, client: Any, tables: Mapping[str, str] | None = None) -> list[str]:
    """Copy the gold run's dbt manifest, catalog and run results, rewritten to the published tables, to ``metadata/dbt/`` for the catalog.

    This is best effort: a refused or failed copy is reported on standard error and never fails the upload, because the publish is already committed.
    Only the gold layer uploads them, and a file the run did not write is skipped.
    """
    if job.layer != "gold":
        return []
    sent = []
    for name in DBT_ARTIFACTS:
        path = job.folder / "target" / name
        if not path.is_file():
            continue
        body = catalog_copy(name, path, tables)
        if body is None:
            continue
        try:
            with tempfile.TemporaryDirectory() as folder:
                rewritten = Path(folder) / name
                rewritten.write_bytes(body)
                client.upload_file(str(rewritten), bucket, DBT_ARTIFACT_PREFIX + name)
        except Exception as error:
            sys.stderr.write(f"layer_run: the dbt file {name} was not uploaded for the catalog: {type(error).__name__}\n")
        else:
            sent.append(name)
    return sent


def upload_dbt_run_artifacts(job: Job, bucket: str, client: Any, tables: Mapping[str, str] | None = None) -> list[str]:
    """Copy the run's dbt manifest, catalog and run results, rewritten to the published tables, once to ``metadata/dbt/<layer>/<run>/`` for the catalog,
    and return the names that are there.

    Each copy is a write-once put. A repeat of the phase finds the same bytes and counts them; a stored copy that differs is reported on standard error and
    never replaced. This is best effort like the fixed-key copy of gold: a refused or failed copy never fails the phase, because the publish is committed.
    """
    sent = []
    prefix = DBT_RUN_PREFIX.format(layer=job.layer, run=job.run)
    for name in DBT_ARTIFACTS:
        path = job.folder / DBT_TARGET.get(job.layer, Path("target")) / name
        if not path.is_file():
            continue
        body = catalog_copy(name, path, tables)
        if body is not None and put_once(client, bucket, prefix + name, body, name):
            sent.append(name)
    manifest = job.folder / DBT_TARGET.get(job.layer, Path("target")) / "manifest.json"
    if manifest.is_file():
        record = inputs_record(job, manifest, tables)
        if record is not None and put_once(client, bucket, prefix + INPUTS_RECORD, record, INPUTS_RECORD):
            sent.append(INPUTS_RECORD)
    return sent


def inputs_record(job: Job, manifest: Path, tables: Mapping[str, str] | None) -> bytes | None:
    """The per-table inputs of the run from its manifest as built (``dbt_catalog_artifacts.table_inputs``), or None with the cause on standard error."""
    from scripts.lakehouse import dbt_catalog_artifacts as artifacts

    try:
        found = artifacts.layer_tables() if tables is None else tables
        return artifacts.encoded(artifacts.table_inputs(json.loads(manifest.read_bytes()), found, job.layer, job.run))
    except (artifacts.ArtifactError, ValueError, KeyError) as error:
        sys.stderr.write(f"layer_run: the per-table inputs of the run were not copied for the catalog: {error}\n")
        return None


def put_once(client: Any, bucket: str, key: str, body: bytes, name: str) -> bool:
    """A write-once put of one catalog copy. True when the object holds exactly these bytes after the call; a failure or a different stored copy is reported on
    standard error, never replaced and never raised."""
    from botocore.exceptions import BotoCoreError, ClientError

    try:
        client.put_object(Bucket=bucket, Key=key, Body=body, IfNoneMatch="*")
    except ClientError as error:
        if error.response.get("Error", {}).get("Code") not in ("PreconditionFailed", "ConditionalRequestConflict"):
            sys.stderr.write(f"layer_run: the dbt file {name} was not copied to {key}: {error.response.get('Error', {}).get('Code', 'ClientError')}\n")
            return False
        try:
            stored = client.get_object(Bucket=bucket, Key=key)["Body"].read()
        except (ClientError, BotoCoreError) as read_error:
            sys.stderr.write(f"layer_run: {key} exists and cannot be read back: {type(read_error).__name__}\n")
            return False
        if stored != body:
            sys.stderr.write(f"layer_run: {key} already holds a copy that differs from this run's {name}; it is kept\n")
            return False
    except BotoCoreError as error:
        sys.stderr.write(f"layer_run: the dbt file {name} was not copied to {key}: {type(error).__name__}\n")
        return False
    return True


def maintain(job: Job) -> None:
    """Gold only, after the upload: expire and remove orphans under the chain rule, bound to this gold run."""
    accepted_publish(job)
    arguments: list[str | Path] = ["--run", job.run, "--mode", job.mode, "--protection", "chain", "--publish-dir", job.publish_folder, *job.local_flags]
    run_module(job, "scripts.lakehouse.gold_maintenance", arguments)


def feast(job: Job, bucket: str, client: Any) -> None:
    """Apply and materialize the committed publication, then add the records of the two steps to the run's upload, which ran before this phase. The apply
    record is the evidence of the publication the run committed (the lineage of a model set reads its commit time), so it is put once and never replaced."""
    accepted_publish(job)
    for step in ("apply", "materialize"):
        feature_metrics.materialized(run_module(job, "scripts.features.feast_job", [step, "--run", job.run, "--mode", job.mode, *job.set_flags]))
    for name in [*FEAST_RECORDS, *([HOLD_RECORD] if (job.folder / HOLD_RECORD).is_file() else [])]:
        key = f"{UPLOAD_PREFIX}/{job.layer}/{job.run}/{name}"
        if name == APPLY_RECORD:
            put_apply_record(client, bucket, key, job.folder / name, job.run)
        else:
            client.upload_file(str(job.folder / name), bucket, key)


def reload(job: Job) -> None:
    """Reload the online store of the model set that the launch names (``scripts.features.online_reload``): the newest run of the set with load evidence is
    loaded again, or the set is skipped when it has none or when a held newer run sits over runs loaded on an older schema.
    The module ends loaded, already loaded or skipped (exit 0), or refuses by name (exit 1)."""
    model = os.environ.get(MODEL_VARIABLE, "")
    if not model:
        raise StepError(f"{MODEL_VARIABLE} is not set: a reload names the model set whose online store it loads")
    started = time.monotonic()
    try:
        text = run_module(job, "scripts.features.online_reload", ["--model", model, "--run", job.run, "--mode", job.mode])
    except BaseException:
        feature_metrics.push_reload(model, None, time.monotonic() - started)
        raise
    feature_metrics.reloaded(text)
    feature_metrics.push_reload(model, text, time.monotonic() - started)


def release(job: Job) -> None:
    """Release the held online load of the model set that the launch names, for the features run it names (``scripts.features.online_release``). The module
    reads the hold itself and ends released, already released or not held (exit 0), or refuses by name (exit 1)."""
    model, run = os.environ.get(MODEL_VARIABLE, ""), os.environ.get(FEATURES_RUN_VARIABLE, "")
    if not model:
        raise StepError(f"{MODEL_VARIABLE} is not set: a release names the model set whose held online load it releases")
    if not RUN_ID.fullmatch(run):
        raise StepError(f"{FEATURES_RUN_VARIABLE} {run!r} is not a features run ID: a release names the run it releases")
    feature_metrics.released(run_module(job, "scripts.features.online_release", ["--model", model, "--features-run", run, "--mode", job.mode]))


def apply_identity(record: Mapping[str, Any]) -> dict[str, Any]:
    """What a Feast apply record says of the publication it applied: the run, its publication and, for a model set, the set and the base publication it read.
    The Feast version and the object counts are not part of it, so a repeat of the same apply has the same identity."""
    return {name: record.get(name) for name in ("run_id", "mode", "feature_set", "publication", "base_publication")}


def put_apply_record(client: Any, bucket: str, key: str, path: Path, run: str) -> None:
    """Put the apply record of the run once (``If-None-Match: *``). A record that does not name the run's own publication is refused before anything is
    written. When the key already holds a record, the same identity succeeds without a change, and another identity or a record that cannot be read back is
    refused: the stored record is kept in every case."""
    from botocore.exceptions import BotoCoreError

    body = path.read_bytes()
    record = json.loads(body)
    publication = record.get("publication") if isinstance(record, dict) else None
    if record.get("run_id") != run or not isinstance(publication, dict) or publication.get("run_id") != run:
        raise StepError(f"{path.name} does not name the publication of run {run}, so it is not uploaded: {apply_identity(record)}")
    try:
        client.put_object(Bucket=bucket, Key=key, Body=body, IfNoneMatch="*")
        return
    except client.exceptions.ClientError as error:
        if error.response["Error"]["Code"] not in ("PreconditionFailed", "ConditionalRequestConflict"):
            raise
    try:
        stored = json.loads(client.get_object(Bucket=bucket, Key=key)["Body"].read())
    except (client.exceptions.ClientError, BotoCoreError, ValueError) as error:
        raise StepError(
            f"{key} exists and cannot be read back ({type(error).__name__}), so a repeat cannot be told from another publication; it is kept"
        ) from None
    if not isinstance(stored, dict) or apply_identity(stored) != apply_identity(record):
        raise StepError(f"{key} already names another publication than {path.name} ({apply_identity(record)}); the stored record is kept")


def count(value: object) -> int | None:
    return value if type(value) is int and value >= 0 else None


def table_entry(namespace: str, name: str, rows: int, state: object) -> dict[str, Any]:
    """One table of the plan from the export row count and the read-only catalog state; whatever the state does not prove is unknown."""
    entry: dict[str, Any] = {"table": f"{namespace}.{name}", "rows": rows}
    state = state if isinstance(state, dict) else {}
    unknown = {"exists": "unknown", "action": "unknown", "rows_now": None, "rows_added": None, "rows_removed": None}
    if "error" in state or state.get("exists") not in (True, False):
        return entry | unknown | {"reason": str(state.get("error") or "the catalog state does not name this table")}
    if state["exists"] is False:
        return entry | {"exists": False, "action": "create", "rows_now": 0, "rows_added": rows, "rows_removed": 0}
    now, same = count(state.get("rows_now")), state.get("same")
    if same is True:
        return entry | {"exists": True, "action": "unchanged", "rows_now": now, "rows_added": 0, "rows_removed": 0}
    if same is False:
        return entry | {
            "exists": True,
            "action": "change",
            "rows_now": now,
            "rows_added": count(state.get("added")),
            "rows_removed": count(state.get("removed")),
        }
    return entry | unknown | {"exists": True, "rows_now": now, "reason": "the catalog did not say whether the rows differ"}


def build_plan(job: Job, contract: Any, manifest: dict[str, Any], state: dict[str, Any], catalog_error: str) -> dict[str, Any]:
    """The plan of the real publish: what ``candidates`` and ``commit`` would write, from the export and the catalog state read before it."""
    from scripts.lakehouse.gold_publish import backup_name, branch_name

    namespace = contract.namespaces[job.mode]
    tables = [table_entry(namespace, name, int(manifest["tables"][name]["rows"]), state.get("tables", {}).get(name)) for name in sorted(contract.tables)]
    runs_state = state.get("runs_table")
    runs: dict[str, Any] = runs_state if isinstance(runs_state, dict) else {}
    committed = runs.get("run_committed")
    runs_entry = {"table": f"{namespace}.{contract.runs_table}", "rows_now": count(runs.get("rows_now")), "rows_added": 0 if committed is True else 1}
    runs_entry["action"] = "refused" if committed is True else "append_row"
    blocked = ["this run is already committed: the real publish refuses it"] if committed is True else []
    if state and state.get("namespace_exists") is False and job.mode == "real":
        blocked.append(f"namespace {namespace} does not exist: the real publish refuses it")
    deletes = [{"table": entry["table"], "rows_removed": entry["rows_removed"]} for entry in tables if entry["rows_removed"]]
    unknown = [entry["table"] for entry in tables if entry["action"] == "unknown" or None in (entry["rows_now"], entry["rows_added"], entry["rows_removed"])]
    if runs_entry["rows_now"] is None:
        unknown.append(runs_entry["table"])
    prefix = contract.backup_prefix or ""
    where = f"{prefix}{backup_name(job.run)}" if job.local is None else f"{job.local.parent}/catalog_backup/{backup_name(job.run)}"
    others = [
        {"kind": "catalog_backup", "where": where, "overwrites": "only on a repeat of this run ID, with the same name"},
        {"kind": "run_branch", "branch": branch_name(contract.branch_prefix, job.run), "tables": len(tables), "dropped_after_commit": True},
        {
            "kind": "layer_run_upload",
            "prefix": f"{UPLOAD_PREFIX}/{job.layer}/{job.run}/",
            "files": "unknown until the upload phase",
            "skipped_in_dry_run": True,
        },
    ]
    summary = {action: sum(entry["action"] == action for entry in tables) for action in ("create", "change", "unchanged", "unknown")}
    summary |= {"tables": len(tables), "rows_to_write": sum(entry["rows"] for entry in tables), "rows_added": sum(entry["rows_added"] or 0 for entry in tables)}
    summary |= {"rows_removed": sum(item["rows_removed"] for item in deletes), "unknown_counts": len(unknown)}
    plan: dict[str, Any] = {"dry_run": True, "layer": job.layer, "run": job.run, "mode": job.mode, "namespace": namespace, "summary": summary}
    return plan | {
        "tables": tables,
        "runs_table": runs_entry,
        "deletes": deletes,
        "other_writes": others,
        "blocked": blocked,
        "unknown": unknown,
        "catalog_error": catalog_error,
    }


def render_plan(plan: dict[str, Any]) -> list[str]:
    """The plan as lines for the task log: a summary, one line per table, the extra writes, then the whole plan as one JSON line."""
    summary = plan["summary"]
    lines = [f"dry run plan for {plan['layer']} run {plan['run']} (mode {plan['mode']}, namespace {plan['namespace']}): nothing was written"]
    lines.append(
        f"tables {summary['tables']}: create {summary['create']}, change {summary['change']}, unchanged {summary['unchanged']}, unknown {summary['unknown']}"
    )
    totals = f"rows to write {summary['rows_to_write']}, rows added {summary['rows_added']}, rows deleted {summary['rows_removed']}"
    lines.append(f"{totals}, unknown counts {summary['unknown_counts']}")
    for entry in plan["tables"]:
        detail = f"exists {entry['exists']}, rows now {entry['rows_now']}, rows added {entry['rows_added']}, rows deleted {entry['rows_removed']}"
        lines.append(
            f"{entry['action'].upper():9} {entry['table']}: write {entry['rows']} rows; {detail}" + (f"; {entry['reason']}" if "reason" in entry else "")
        )
    runs = plan["runs_table"]
    lines.append(f"{runs['action'].upper():9} {runs['table']}: add {runs['rows_added']} row; rows now {runs['rows_now']}")
    lines += [f"DELETE    {item['table']}: {item['rows_removed']} rows" for item in plan["deletes"]]
    lines += [f"BLOCKED   {item}" for item in plan["blocked"]]
    lines += [f"UNKNOWN   {item}" for item in plan["unknown"]]
    lines += [f"OTHER     {item['kind']}: " + ", ".join(f"{key} {value}" for key, value in item.items() if key != "kind") for item in plan["other_writes"]]
    if plan["catalog_error"]:
        lines.append(f"CATALOG   the catalog could not be read: {plan['catalog_error']}")
    return [*lines, PLAN_PREFIX + json.dumps(plan, sort_keys=True, separators=(",", ":"))]


def plan_publish(job: Job) -> dict[str, Any]:
    """Dry-run ``publish``: check the export as the real steps do, read the catalog without writing, then print and save the plan."""
    sha, _ = sealed(job)
    accepted = accepted_result(job, sha)
    contract = publish_contract(job)
    manifest, manifest_sha = read_record(job.publish_folder, "export/manifest.json")
    result = read_record(job.publish_folder, "consumer_result.json")[0]
    if manifest.get("run_id") != job.run or manifest.get("sealed_sha256") != sha or result.get("manifest_sha256") != manifest_sha:
        raise StepError("the export is not the export of this run's sealed copy")
    if set(manifest.get("tables", {})) != set(contract.tables):
        raise StepError("the export does not hold exactly the contract's tables: the real publish would refuse it")
    state_file = job.folder / "publish/catalog_state.json"
    state_file.unlink(missing_ok=True)
    arguments: list[str | Path] = ["--layer", job.layer, "--run", job.run, "--mode", job.mode, "--sealed-sha256", sha, "--accepted-result-sha256", accepted]
    arguments += ["--publish-dir", job.publish_folder, "--output", state_file, *job.local_flags]
    state: dict[str, Any] = {}
    error = ""
    try:
        run_module(job, "scripts.lakehouse.publish_plan", arguments)
        state = read_record(job.publish_folder, "catalog_state.json")[0]
    except (StepError, PipelineError, OSError, ValueError) as failure:
        error = f"{type(failure).__name__}: {failure}"
    plan = build_plan(job, contract, manifest, state, error)
    (job.folder / PLAN_FILE).write_text(json.dumps(plan, indent=2, sort_keys=True) + "\n")
    sys.stdout.write("\n".join(render_plan(plan)) + "\n")
    return plan


def mark_dry(job: Job) -> None:
    job.folder.mkdir(parents=True, exist_ok=True)
    (job.folder / DRY_MARKER).write_text("this task is a dry run: no phase may write to the catalog or the bucket\n")


def launch_phase() -> str:
    """``release`` or ``reload`` when the launch names one of them in ``LAYER_PHASE`` (the feast container then runs it), else empty."""
    selected = os.environ.get(PHASE_VARIABLE, "")
    return selected if selected in (RELEASE_PHASE, RELOAD_PHASE) else ""


def lineage_time(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, UTC).isoformat()


def send_lineage(job: Job, phase: str, *, failed: bool, dry: bool) -> None:
    """Send the lineage events of a finished ``pin`` or ``upload`` phase (``orchestration/job_lineage.py``): the task's START after a pin, a START and a
    COMPLETE per published table after an upload, the task's FAIL after either failed. Nothing happens unless the launch gave the phase its lineage
    settings, and nothing here can fail the phase or change its exit code: the send is bounded by ``job_lineage.emit`` and by what is left of the task, and
    any error of this function is one line on standard error. The module is imported only when the settings are there, because the other images do not
    hold what it imports."""
    try:
        if phase not in LINEAGE_PHASES or dry or job.mode != "real" or job.local is not None or launch_phase() or not os.environ.get(LINEAGE_VARIABLE):
            return
        from orchestration import job_lineage

        left = task_seconds_left(job) - LINEAGE_MARGIN_SECONDS
        started = (job.code.parent / STARTED_FILE).read_text().strip()
        run = job_lineage.job_run(os.environ[LINEAGE_VARIABLE], job.layer, job.run, job.feature_set, job.folder, lineage_time(float(started)))
        ended = lineage_time(time.time())
        if failed:
            job_lineage.emit("failure", lambda: job_lineage.failure_lines(run, phase, ended), os.environ, min(job_lineage.START_SECONDS, left))
        elif phase == "pin":
            job_lineage.emit("start", lambda: job_lineage.start_lines(run), os.environ, min(job_lineage.START_SECONDS, left))
        else:
            manifest = job.folder / DBT_TARGET.get(job.layer, Path("target")) / "manifest.json"
            inputs = inputs_record(job, manifest, None) if manifest.is_file() else None
            job_lineage.emit("table events", lambda: job_lineage.table_lines(run, ended, inputs), os.environ, min(job_lineage.TABLE_SECONDS, left))
    except Exception as error:  # the job's work is done or has already failed: no lineage may change its exit code
        sys.stderr.write(f"layer_run: lineage of the {phase} phase was not sent: {type(error).__name__}\n")


def dispatch(job: Job, phase: str, args: argparse.Namespace) -> None:
    if phase == "pin":
        check_input_run(job)
        check_archive(args.archive or args.work / "code.tar.gz", args.code_sha256)
        evidence = None
        if args.bucket and job.mode == "real" and job.local is None:
            client = s3_client()
            if not job.dry_run:
                claim_run(job, args.bucket, client)
            evidence = {
                "input_objects": staged_objects(client, args.bucket, job.staged_run),
                "input_listing": input_record(job, client, args.bucket),
            }
        bundle(job, args.archive or args.work / "code.tar.gz", args.code_sha256, evidence)
        pin(job)
    elif phase == "upload" and job.dry_run:
        sys.stdout.write(json.dumps({"dry_run": True, "uploaded": 0, "would_upload": len(uploadable(job))}) + "\n")
    elif phase == "maintain" and job.dry_run:
        sys.stdout.write(json.dumps({"dry_run": True, "maintenance": "not run"}) + "\n")
    elif phase == RELEASE_RUNNER and job.dry_run:
        sys.stdout.write(json.dumps({"dry_run": True, "release": "not run"} if launch_phase() else {"dry_run": True, "feast": "not applied"}) + "\n")
    elif phase == "publish" and job.dry_run:
        plan = plan_publish(job)
        if plan["unknown"] or plan["catalog_error"]:
            # The plan is printed and saved first, so the owner sees which state is unknown. A dry run that could not read it proves nothing.
            error = plan["catalog_error"] or "none"
            raise StepError(f"the dry run has unknown catalog state ({len(plan['unknown'])} tables, catalog error: {error}): the plan above is incomplete")
    elif phase == "upload":
        client = s3_client()
        uploaded = upload(job, args.bucket, client)
        dbt_files = upload_dbt_artifacts(job, args.bucket, client)  # the fixed-key copy the catalog reads today; the per-run copy below replaces it later
        sys.stdout.write(json.dumps({"uploaded": uploaded, "dbt_files": dbt_files, "dbt_run_files": upload_dbt_run_artifacts(job, args.bucket, client)}) + "\n")
    elif phase == RELEASE_RUNNER and launch_phase() == RELEASE_PHASE:
        release(job)
    elif phase == RELEASE_RUNNER and launch_phase() == RELOAD_PHASE:
        reload(job)
    elif phase == "feast":
        feast(job, args.bucket, s3_client())
    elif phase == "publish":
        publish(job, args.bucket, s3_client() if job.mode == "real" and job.local is None else None)
    else:
        {"build": build, "validate": validate, "verify": verify, "export": export, "maintain": maintain}[phase](job)


def startup(job: Job, phase: str) -> None:
    """The host job startup check at the first code step (pin), at the start of the build (whose image holds the DuckDB extensions) and before Feast."""
    if phase in ("pin", "feast"):
        startup_check.check_phase(job.layer, phase)
    elif phase == "build":
        startup_check.check_build(job.layer, job.mode, job.folder / "snapshot/project/profiles.yml")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("phase", choices=sorted({phase for phases in PHASES.values() for phase in phases}))
    parser.add_argument("--layer", required=True, choices=sorted(PHASES))
    parser.add_argument("--run", required=True)
    parser.add_argument("--input-run", default=os.environ.get("INPUT_RUN_ID", ""), help="gold and features: the committed silver or gold run to pin")
    parser.add_argument(
        "--feature-set",
        default=os.environ.get(feature_sets.VARIABLE, ""),
        help="features only: empty is the single path, base or m1 to m6 is a set (FEATURE_SET)",
    )
    parser.add_argument("--mode", default="real", choices=("real", "publish_fixture"))
    parser.add_argument("--work", type=Path, default=Path("/workspace"))
    parser.add_argument("--local-warehouse", type=Path)
    parser.add_argument("--archive", type=Path)
    parser.add_argument("--code-sha256", default=os.environ.get("CODE_SHA256", ""))
    parser.add_argument(
        "--code-run-id",
        default=os.environ.get("CODE_RUN_ID", ""),
        help="the run whose staged code the task fetched; required, the run itself for a standalone run",
    )
    parser.add_argument("--bucket", default=os.environ.get("DATA_BUCKET_NAME", ""))
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=f"publish: print and save the plan and write nothing; upload, maintain and feast do nothing (also set by {DRY_ENV}=1)",
    )
    args = parser.parse_args(argv)
    if args.phase not in PHASES[args.layer]:
        parser.error(f"{args.phase} is not a phase of {args.layer}")
    if not RUN_ID.fullmatch(args.run):
        parser.error("the run ID must look like silver_0a1b2c3d")
    selected = os.environ.get(PHASE_VARIABLE, "")
    if selected and (args.layer != "features" or selected not in (*PHASES["features"], RELEASE_PHASE, RELOAD_PHASE)):
        parser.error(f"{PHASE_VARIABLE} {selected!r} is not a phase of the features task: it applies to the features layer only")
    runs = args.phase == (RELEASE_RUNNER if selected in (RELEASE_PHASE, RELOAD_PHASE) else selected)
    if selected and not runs:
        # A launch that names one phase leaves every other idle (a release or reload launch runs the feast container); a build runs every phase.
        sys.stdout.write(json.dumps({"phase": args.phase, "skipped": True, "launch_phase": selected or "build"}) + "\n")
        return 0
    try:
        chosen = feature_sets.checked(args.feature_set)
    except PublishError as error:
        parser.error(str(error))
    if chosen and args.layer != "features":
        parser.error(f"{feature_sets.VARIABLE} applies to the features layer only")
    job = Job(
        args.layer, args.run, args.mode, args.work / "code", args.local_warehouse, input_run=args.input_run, feature_set=chosen, code_run=args.code_run_id
    )
    dry = args.dry_run or os.environ.get(DRY_ENV, "") in ("1", "true", "yes")
    try:
        if args.phase == "pin":  # a new pin starts a new task
            (job.folder / DRY_MARKER).unlink(missing_ok=True)
        elif (job.folder / DRY_MARKER).exists():
            dry = True
        if dry:
            mark_dry(job)
            job = replace(job, dry_run=True)
        if args.phase in ("publish", "upload", "feast") and not args.bucket and args.mode == "real" and not dry:
            raise StepError("DATA_BUCKET_NAME (or --bucket) is not set")
        if not args.code_run_id or not re.fullmatch(CODE_RUN, args.code_run_id):
            raise StepError(
                "CODE_RUN_ID is not set to a code run ID: it names the run whose staged code the task fetches (the run itself for a standalone run)"
            )
        startup(job, args.phase)
        task_capacity.apply(capacity_profile(args.layer, args.phase))
        with task_deadline(job):
            if args.phase != "pin" and not launch_phase():
                check_code_run(job)
                check_feature_set(job)
            dispatch(job, args.phase, args)
    except (
        StepError,
        PipelineError,
        PublishError,
        provenance.ProvenanceError,
        task_capacity.CapacityError,
        startup_check.StartupCheckError,
        OSError,
        ValueError,
        KeyError,
        tarfile.TarError,
    ) as error:
        sys.stderr.write(f"layer_run: step {args.phase} ({args.layer}, run {args.run}) failed: {error}\n")
        send_lineage(job, args.phase, failed=True, dry=dry)
        return 1
    send_lineage(job, args.phase, failed=False, dry=dry)
    return 0


if __name__ == "__main__":
    resource_peaks.recorded(main)
