"""Lineage of a job that the direct ECS launch library ran: OpenLineage datasets and facets rebuilt from declared lineage and durable S3 evidence.

The launch operator (``orchestration/ecs_launch_operator.py``) returns the facets of ``LineageSource.facets`` to Airflow's OpenLineage provider, which sends
them over HTTP to OpenMetadata's lineage route with the lineage bot credential and a 5 second limit. Nothing is spooled and the job itself sends nothing.
A missed event is rebuilt by ``replay`` from the same evidence by the same builder, so a replay gives the same bytes every time:

    python -m orchestration.ecs_lineage replay --bucket <bucket> --job gold --run-id <run> --attempt 1 --template gold_build \
        --dag-id gold_transform_dag --task-id gold_build --dag-run-id <Airflow run ID> [--send http://127.0.0.1:8585 --credential-file <path>]

Where the parts come from:

- The datasets are the job's declared lineage (``declared_lineage``), derived from the files that already name each job's tables: the publish contracts of
  silver, gold and features (``data_contracts/lakehouse``), the bronze table map (``config/lakehouse/bronze_tables.json``) and the silver dbt sources, whose
  tables silver pins (``scripts/lakehouse/pins.py``). Only the jobs of ``config/orchestration/ecs_broker_jobs.json`` that read or write those tables have a
  declaration; the other jobs declare none, and the training and drift jobs are a recorded limit (below).
  A dataset is named for the Glue mirror table that OpenMetadata's Glue connector creates (``dataset``): namespace ``iceberg://<service>`` (the tables are
  Iceberg tables, and the snapshot record schemas allow that form) and name ``<OpenMetadata database>.<Glue database>.<table>``, from
  ``config/metadata/ingestion/glue.json`` and ``config/lakehouse/athena_mirror.json``. The names come from those files, never from the live mirror, so
  they are the same while the mirror's ``enabled`` is false; OpenMetadata resolves them once the mirror is on and ingested (it creates no unresolved
  entity). Bronze is a mirror layer like the others (``hai_mirror_bronze``), so a bronze table is named the same way and resolves once the mirror holds it.
- The launch identity, the times, the outcome and the input run bound at launch come from the launch records (``orchestration/ecs_launch.py``).
- The versions come from the job's evidence under ``lakehouse/layer_runs/<layer>/<run>/``: the pin file (the input snapshots) and the output record.
  The input snapshot record is checked by ``snapshots.validate_input_record``: it names exactly the declared inputs, and its pins equal the frozen pins.
  For gold and features the frozen pins are the snapshot set that the input run bound at launch published; the pin must also name that run. Silver pins
  bronze inside the job, so its frozen pins are the pin file that the run's one claim (``state/pinned.json``) covers. The output record is checked by
  ``snapshots.validate_snapshot_record``. Any record that breaks a rule is refused with its key; an unreadable object is never read as missing, except
  the one optional read: a job's output record that the role is denied (AccessDenied) gives the declared outputs without versions and the state ``unreadable``.
- A layer job's datasets travel in one event per published table, never in the task's own event. OpenMetadata pairs every input of an event with every
  output of the same event, so one event with the layer's 137 inputs and 71 outputs would claim 9,727 edges. The run's record
  ``metadata/dbt/<layer>/<run>/table_inputs.json`` (written beside its dbt copy by the job's upload phase from the manifest's dependencies) gives each table
  the tables its own model reads (``layer_table_events``), and the versions come from the evidence above. The task's own event and the facets the provider
  gets carry the run facet and its state only. The evidence step sends the table events like the bronze ones (below), and the replay adds them.
- The bronze job sends no table event. Its tables' only inputs are the raw S3 files its run record lists, and OpenMetadata 2.0.7 resolves an S3 dataset only
  to a container entity that an S3 storage connector made, which this project does not run; the receiver would discard every such event (a dataset it cannot
  resolve is skipped, and an event with no resolvable input writes nothing). The bronze task's own event names no dataset either, and the bronze to silver
  edge comes from the silver job's table events. The raw files of each bronze table are written onto the table by the metadata job (owner decision
  "Raw-file lineage route", Nov 13 2026).

Recorded limits:

- The training and drift jobs declare no datasets: their DAGs run a task per step with no lineage hooks, so no live event of theirs carries datasets.
- The provider's listener asks the operator of the Airflow task for lineage. A job DAG's registered task runs the launch operator inside it, so the task
  forwards the two hooks to that operator (``dag_runtime.JobLineage``). The features and training DAGs still run a ``@task`` callable per step with no hooks,
  so their live events carry no job datasets.
- A table event that is not sent in time or at all (a stalled or refused receiver, a credential the HTTP client refuses, the deadline) is logged, without the
  credential, and never fails the evidence step or the task; ``replay`` rebuilds it. The table events name the job task and the launch attempt of the claim.
- Silver's frozen pins are its own pin file under the run's one claim, not a value known at launch: a weaker check than gold and features get.
- A replayed event has its own stable run ID (``run_uuid``), not the run ID the provider gave the live event. OpenMetadata keys lineage edges on the
  datasets, so a replay adds the same edges.

This module imports nothing from Airflow or from ``scripts``, so it runs in the Airflow image and offline.
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import http.client
import json
import os
import re
import sys
import threading
import time
import urllib.parse
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from functools import cache
from pathlib import Path
from typing import Any

from botocore.exceptions import BotoCoreError, ClientError
from jsonschema import Draft202012Validator
from referencing import Registry, Resource

from orchestration.ecs_launch import LaunchError, LaunchStore, S3LaunchStore, launch_record_prefix
from orchestration.snapshots import SnapshotError, validate_input_record, validate_snapshot_record

ROOT = Path(__file__).resolve().parents[1]
ROUTE = "/api/v1/openlineage/lineage"
DELIVERY_SECONDS = 5.0
TABLE_EVENTS_SECONDS = 300.0  # one deadline for all the per-table events of a bronze run: the 664 declared tables are 1,328 events, which fit at 0.22 s each
TRANSPORT_TYPE, TRANSPORT_URL, TRANSPORT_KEY = "OPENLINEAGE__TRANSPORT__TYPE", "OPENLINEAGE__TRANSPORT__URL", "OPENLINEAGE__TRANSPORT__AUTH__API_KEY"
JOB_NAMESPACE = "hai-prediction-aws"  # the provider's namespace (AIRFLOW__OPENLINEAGE__NAMESPACE in infra/services/airflow.tf)
PRODUCER = "https://github.com/ian-mungai/healthcare-hai-prediction"
EVENT_SCHEMA = "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent"
PARENT_SCHEMA = "https://openlineage.io/spec/facets/1-1-0/ParentRunFacet.json#/$defs/ParentRunFacet"
VERSION_SCHEMA = "https://openlineage.io/spec/facets/1-0-0/DatasetVersionDatasetFacet.json#/$defs/DatasetVersionDatasetFacet"
FACET_FILE = Path("data_contracts/orchestration/launch_lineage_facet.schema.json")
FACET_SCHEMA = f"{PRODUCER}/blob/main/{FACET_FILE.as_posix()}"
SPEC_FOLDER = Path("data_contracts/openlineage")
SPEC_FILES = {  # copies of https://openlineage.io/spec/... taken on Nov 5 2026; a changed copy is refused
    "DatasetVersionDatasetFacet-1-0-0.json": "de983b6eef3b095d25599b2a5a39627b3181425a9fdf665ae59547e0eeb323ce",
    "OpenLineage-1-0-2.json": "262bda97312d2271f40b0ddd30c01251239c9b8292603039960f307b1a4d94fd",
    "OpenLineage-2-0-2.json": "69f68bee00b9beac88a87059c0102410e7bb05f3f43c46d02a0409831eceb0d2",
}
JOBS_FILE = Path("config/orchestration/ecs_broker_jobs.json")
HOST_FILE = Path("config/orchestration/host_jobs.json")
FEATURE_SCHEMA_FILE = Path("data_contracts/features/feature_schema.json")
GLUE_FILE = Path("config/metadata/ingestion/glue.json")
MIRROR_FILE = Path("config/lakehouse/athena_mirror.json")
BRONZE_TABLES_FILE = Path("config/lakehouse/bronze_tables.json")
SILVER_SOURCES_FILE = Path("dbt/models/silver/staging/_bronze__sources.yml")
CONTRACT_FILES = {
    "silver": Path("data_contracts/lakehouse/silver_publish.json"),
    "gold": Path("data_contracts/lakehouse/gold_contract.json"),
    "features": Path("data_contracts/lakehouse/features_publish.json"),
}
BRONZE_NAMESPACE = "bronze"  # the lakehouse namespace of the bronze tables, which have no Glue database
PINNED_ALWAYS = ("file_metadata",)  # the table every silver pin holds beside the dbt sources it declares (scripts/lakehouse/pins.py, ``pinned_tables``)
SOURCE_TABLE = re.compile(r"\s*- name:\s*(\S+)\s*")
EVIDENCE_PREFIX = "lakehouse/layer_runs"
INPUTS_KEY = "metadata/dbt/{layer}/{run}/table_inputs.json"  # written by the layer job's upload phase beside its dbt copy (scripts/lakehouse/layer_run.py)
INPUTS_KEYS = {"schema", "layer", "run_id", "tables"}
MODE = "real"
EVIDENCE_BYTES = 2 * 1024**2
IDENTIFIER = re.compile(r"[a-z][a-z0-9_]{0,127}")
RUN_ID = re.compile(r"[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}")
BRONZE_JOB = "bronze"  # the one job whose lineage comes from its run record (raw_lineage.bronze_run_events)
PIN_TEMPLATES = ("pin_silver", "pin_gold")  # the only templates whose invocation names an input run (submission_request.schema.json)
TERMINAL = {"succeeded": "COMPLETE", "cancelled": "ABORT"}  # every other outcome is FAIL


class LineageRefused(ValueError):
    """Evidence or declared lineage breaks a rule. The message names the object and the rule."""


class EvidenceUnavailable(RuntimeError):
    """An evidence object or launch record cannot be read. It is never read as missing."""


class EvidenceDenied(EvidenceUnavailable):
    """The role may not read an evidence object (S3 answers AccessDenied, which is also what an absent key answers when the role cannot list). Only the one
    optional record, a succeeded job's output record, reads it as unreadable; every other read refuses."""


class DeliveryFailed(RuntimeError):
    """The receiver did not accept an event within the limit. The message never holds the credential."""


@dataclass(frozen=True)
class Layer:
    """Where a layer job keeps its evidence: the pin file, the schema of its inputs and outputs, the output record, and the record of the input run."""

    pin: str
    input_schema: str
    output: str
    output_schema: str
    frozen: tuple[str, str] | None  # (layer, output record) of the input run, or None when the input is pinned inside the job (silver pins bronze)


LAYERS = {
    "silver": Layer("pins.json", "bronze", "silver_snapshot.json", "silver", None),
    "gold": Layer("silver_pins.json", "silver", "publish/commit.json", "gold", ("silver", "silver_snapshot.json")),
    "features": Layer("gold_pins.json", "gold", "features_snapshot.json", "features", ("gold", "publish/commit.json")),
}


# A model set's features run pins the domain matrices of a base run (base_pins.json, with the digest of the base run's ledger row) and publishes its table
MODEL_LAYER = Layer("base_pins.json", "features", "features_snapshot.json", "features", ("features", "features_snapshot.json"))

TABLE_EVENT_JOBS = tuple(LAYERS)  # the jobs whose datasets travel in one event per table, never in the task's own event


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


@dataclass(frozen=True)
class Connector:
    """What the Glue connector creates: its service, its OpenMetadata database and the Glue database of each mirrored lakehouse namespace."""

    service: str
    database: str
    schemas: Mapping[str, str]


@cache
def connector() -> Connector:
    """The service and database names of the Glue connector and the Glue database of each layer, so the datasets name the tables the connector creates."""
    source = json.loads((ROOT / GLUE_FILE).read_text())["source"]
    layers = json.loads((ROOT / MIRROR_FILE).read_text())["layers"]
    schemas = {str(spec["namespace"]): str(spec["database"]) for spec in layers.values()}
    return Connector(str(source["serviceName"]), str(source["serviceConnection"]["config"]["databaseName"]), schemas)


def dataset(schema: str, table: str) -> dict[str, str]:
    """The OpenLineage dataset of a lakehouse table: namespace ``iceberg://<service>`` and name ``<database>.<Glue database>.<table>``. A table is in the Glue
    database of its layer in ``config/lakehouse/athena_mirror.json``, bronze included."""
    if not IDENTIFIER.fullmatch(schema) or not IDENTIFIER.fullmatch(table):
        raise LineageRefused(f"{schema}.{table} is not a plain catalog identifier")
    found = connector()
    glue_database = found.schemas.get(schema)
    if glue_database is None:
        raise LineageRefused(f"{schema} is not a lakehouse namespace of the Glue mirror")
    return {"namespace": f"iceberg://{found.service}", "name": f"{found.database}.{glue_database}.{table}"}


def connector_fqn(item: Mapping[str, str]) -> str:
    """The FQN of the table the Glue connector creates for a dataset. A dataset it would name differently or not create at all is refused."""
    found = connector()
    parts = str(item.get("name", "")).split(".")
    mirrored = {database: schema for schema, database in found.schemas.items()}
    if item.get("namespace") != f"iceberg://{found.service}" or len(parts) != 3 or parts[0] != found.database or parts[1] not in mirrored:
        raise LineageRefused(f"{item.get('namespace')} {item.get('name')} is not a table the {found.service} connector creates")
    if dataset(mirrored[parts[1]], parts[2]) != dict(item):
        raise LineageRefused(f"{item.get('namespace')} {item.get('name')} is not named as the {found.service} connector names it")
    return f"{found.service}.{item['name']}"


def source_tables(text: str) -> set[str]:
    """The table names under every ``tables:`` key of a dbt sources file, read with the standard library as ``scripts.lakehouse.pins.declared_sources`` does
    (this module cannot import ``scripts``; a case compares the two)."""
    names: set[str] = set()
    block_indent: int | None = None
    item_indent: int | None = None
    for line in text.splitlines():
        content = line.strip()
        if not content or content.startswith("#"):
            continue
        indent = len(line) - len(line.lstrip())
        if block_indent is not None and indent <= block_indent:
            block_indent = item_indent = None
        if block_indent is None:
            block_indent = indent if content == "tables:" else None
            continue
        item_indent = indent if item_indent is None else item_indent
        match = SOURCE_TABLE.fullmatch(line) if indent == item_indent else None
        if match:
            names.add(match.group(1).strip("'\""))
    return names


def lineage_layer(job: str, *, root: Path = ROOT) -> str:
    """The job whose lineage a launch family carries: a model set's family (``features_m1``) is a host family that is like the features job and has the
    features layer's lineage, so its launch records stay under its own family and its datasets are the layer's. Any other family is its own layer."""
    like = json.loads((root / HOST_FILE).read_text())["jobs"].get(job, {}).get("like")
    return str(like) if like else job


def model_set_of(job: str, *, root: Path = ROOT) -> str | None:
    """The model set (``m1`` to ``m6``) whose host family ``job`` is, or None for any other family."""
    layer = lineage_layer(job, root=root)
    return job.removeprefix(f"{layer}_") if layer != job and layer == "features" else None


def declared_lineage(job: str, *, root: Path = ROOT) -> dict[str, list[dict[str, str]]]:
    """The tables a job reads and writes, from the files that already name them: bronze writes the tables of the bronze table map, silver reads the bronze
    tables its dbt sources declare (and the file metadata table) and writes its publish contract, gold reads the silver contract and writes its own, features
    read the gold contract and write theirs. A model set's family reads the domain matrices of the feature schema and writes the one table of its set. The
    other jobs declare nothing. Each list is sorted
    by table name."""
    model = model_set_of(job, root=root)
    job = lineage_layer(job, root=root)
    if job not in set(json.loads((root / JOBS_FILE).read_text())["steps"].values()):
        raise LineageRefused(f"{job} is not a job of {JOBS_FILE.name}")

    def contract(layer: str) -> list[str]:
        return sorted(json.loads((root / CONTRACT_FILES[layer]).read_text())["publish"]["tables"])

    def items(schema: str, tables: Sequence[str]) -> list[dict[str, str]]:
        return [dataset(schema, table) for table in tables]

    if model is not None:
        schema = json.loads((root / FEATURE_SCHEMA_FILE).read_text())
        if model not in schema["model_sets"]:
            raise LineageRefused(f"{FEATURE_SCHEMA_FILE.name} names no model set {model}")
        domain = sorted(name for name, table in schema["tables"].items() if table["role"] == "predictor")
        return {"inputs": items("features", domain), "outputs": items("features", [schema["model_sets"][model]["table"]])}
    bronze = sorted(entry["table"] for entry in json.loads((root / BRONZE_TABLES_FILE).read_text())["tables"])
    if job == BRONZE_JOB:
        return {"inputs": [], "outputs": items(BRONZE_NAMESPACE, bronze)}
    if job == "silver":
        declared = source_tables((root / SILVER_SOURCES_FILE).read_text())
        pinned = sorted((set(bronze) & declared) | set(PINNED_ALWAYS))
        return {"inputs": items(BRONZE_NAMESPACE, pinned), "outputs": items("silver", contract("silver"))}
    if job in ("gold", "features"):
        below = "silver" if job == "gold" else "gold"
        return {"inputs": items(below, contract(below)), "outputs": items(job, contract(job))}
    return {"inputs": [], "outputs": []}


def step_invocation(request: Mapping[str, Any]) -> dict[str, Any]:
    """The invocation a snapshot record is bound to: the step's submission request. The input run stays only on a pin template, as its schema says."""
    body = dict(request)
    if body.get("template") not in PIN_TEMPLATES:
        body.pop("input_run_id", None)
    return body


def timestamp_text(value: Any, where: str) -> str:
    """A bounded ISO 8601 UTC time as given; anything else is refused."""
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00")) if isinstance(value, str) and len(value) <= 40 else None
    except ValueError:
        parsed = None
    offset = None if parsed is None else parsed.utcoffset()
    if offset is None or offset.total_seconds() != 0:
        raise LineageRefused(f"{where}: {value!r} is not a UTC time")
    return str(value)


def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise LineageRefused(f"duplicate key {key}")
        result[key] = value
    return result


class S3Evidence:
    """Bounded reads of evidence objects of the project bucket. A missing object is None; any other failure raises ``EvidenceUnavailable``."""

    def __init__(self, client: Any, bucket: str) -> None:
        self.client = client
        self.bucket = bucket

    def read(self, key: str) -> Any:
        try:
            body = self.client.get_object(Bucket=self.bucket, Key=key)["Body"].read(EVIDENCE_BYTES + 1)
        except ClientError as error:
            code = str(error.response.get("Error", {}).get("Code", ""))
            if code == "NoSuchKey":
                return None
            raise (EvidenceDenied if code == "AccessDenied" else EvidenceUnavailable)(f"{key}: {code or 'ClientError'}") from None
        except BotoCoreError as error:
            raise EvidenceUnavailable(f"{key}: {type(error).__name__}") from None
        if len(body) > EVIDENCE_BYTES:
            raise LineageRefused(f"{key}: larger than {EVIDENCE_BYTES} bytes")
        try:
            return json.loads(body, object_pairs_hook=unique_object)
        except LineageRefused as error:
            raise LineageRefused(f"{key}: {error}") from None
        except ValueError:
            raise LineageRefused(f"{key}: is not JSON") from None


@dataclass(frozen=True)
class LaunchFacts:
    """What the launch records say of one attempt: its times, its outcome and the input run bound at launch."""

    run_id: str
    job: str
    attempt: int
    started_at: str
    ended_at: str
    outcome: str
    input_run: str
    model: str = ""  # the model set of a model set family's launch, else empty


def launch_facts(store: LaunchStore, run_id: str, job: str, attempt: int) -> LaunchFacts:
    """Read the claim and the records of the attempt and of the claim's owning attempt. A retry that adopted an earlier launch takes that launch's times
    and outcome, and keeps its own attempt as its identity. An attempt whose launch has no end record has no lineage yet."""
    try:
        claim = store.run_record(run_id, job, "claim")
        owner = int(claim["attempt"]) if claim is not None else attempt
        attempts = sorted({attempt, owner})
        records = store.records(run_id, job, attempts)
    except LaunchError as error:
        raise EvidenceUnavailable(f"launch records of {run_id}:{job}:{attempt}: {error}") from None
    by_attempt = {(record["attempt"], record["event"]): record for record in records}
    end = by_attempt.get((owner, "end")) or by_attempt.get((attempt, "end"))
    if claim is None or end is None:
        raise LineageRefused(f"{run_id}:{job}:{attempt} has no {'claim' if claim is None else 'end record'}: there is nothing to rebuild yet")
    intent = by_attempt.get((owner, "intent")) or by_attempt.get((attempt, "intent")) or claim
    return LaunchFacts(
        run_id, lineage_layer(job), attempt, intent["at"], end["at"], end["outcome"], str(claim["bound"].get("input_run", "latest")), model_set_of(job) or ""
    )


@dataclass(frozen=True)
class JobLineage:
    """The datasets of one attempt with their version facets, and the run facet ``haiLaunch``."""

    inputs: list[dict[str, Any]]
    outputs: list[dict[str, Any]]
    launch: dict[str, Any] = field(default_factory=dict)


def version_facet(identity: Mapping[str, str]) -> dict[str, Any]:
    return {"version": {"_producer": PRODUCER, "_schemaURL": VERSION_SCHEMA, "datasetVersion": identity["id"]}}


def snapshot_items(schema: str, snapshots: Any, where: str) -> list[dict[str, Any]]:
    if not isinstance(snapshots, dict):
        raise LineageRefused(f"{where}: the snapshot set is not an object")
    items = []
    for table, snapshot in snapshots.items():
        if not IDENTIFIER.fullmatch(str(table)):
            raise LineageRefused(f"{where}: table {table!r} is not a plain catalog identifier")
        items.append({**dataset(schema, table), "snapshot": {"kind": "iceberg_snapshot", "id": str(snapshot)}})
    return sorted(items, key=lambda item: (item["namespace"], item["name"]))


def require_record(record: Any, where: str, run_id: str, schema: str | None, keys: set[str], optional: frozenset[str] = frozenset()) -> dict[str, Any]:
    """A layer record of this run in real mode, of the given namespace, with exactly the given keys and any of the optional ones."""
    if record is None:
        raise LineageRefused(f"{where}: missing from the evidence of a succeeded job")
    if not isinstance(record, dict) or not keys <= set(record) <= keys | optional:
        raise LineageRefused(f"{where}: does not have exactly the keys {sorted(keys)}")
    if record["run_id"] != run_id:
        raise LineageRefused(f"{where}: belongs to another run ({record['run_id']})")
    if "mode" in keys and record["mode"] != MODE:
        raise LineageRefused(f"{where}: is not a real mode record ({record['mode']})")
    if schema is not None and "namespace" in keys and record["namespace"] != schema:
        raise LineageRefused(f"{where}: names namespace {record['namespace']}, not {schema}")
    return record


SNAPSHOT_KEYS = {"schema", "run_id", "mode", "namespace", "snapshot_set"}
PIN_KEYS = SNAPSHOT_KEYS | {"source_run_id"}
COMMIT_KEYS = {"schema", "run_id", "backup_sha256", "snapshot_set", "state"}
SET_KEYS = frozenset({"feature_set", "base_run"})  # a features run's record names its feature set, and a model set's names the base run it read
MODEL_PIN_KEYS = PIN_KEYS | {"base_row_sha256"}
DIGEST = re.compile(r"[0-9a-f]{64}")
ROW_FIELDS = (
    "run_id",
    "committed_at",
    "snapshot_set",
    "base_run",
)  # what the model set publish reads of a base run's publish_runs row (feature_sets.ROW_FIELDS)
APPLY_RECORD = "feast_apply.json"  # the base run's own record of the publication it committed, uploaded by its Feast phase


def output_set(
    evidence: S3Evidence, layer_name: str, run_id: str, *, required: bool, expect: Mapping[str, str] | None = None
) -> tuple[list[dict[str, Any]] | None, str]:
    """The published snapshots of a layer run from its output record, or None when it is absent and not required. ``expect`` names fields of the record
    that must have the given values (a model set's record names its set and the base run it read)."""
    layer = LAYERS[layer_name]
    key = f"{EVIDENCE_PREFIX}/{layer_name}/{run_id}/{layer.output}"
    record = evidence.read(key)
    if record is None and not required:
        return None, key
    if layer.output.endswith("commit.json"):
        record = require_record(record, key, run_id, None, COMMIT_KEYS)
        if record["state"] != "committed":
            raise LineageRefused(f"{key}: the publish is not committed ({record['state']})")
    else:
        record = require_record(record, key, run_id, layer.output_schema, SNAPSHOT_KEYS, SET_KEYS if layer_name == "features" else frozenset())
        for field, value in (expect or {}).items():
            if record.get(field) != value:
                raise LineageRefused(f"{key}: names {field} {record.get(field)!r}, not {value!r}")
    return snapshot_items(layer.output_schema, record["snapshot_set"], key), key


def base_row_digest(row: Mapping[str, Any]) -> str:
    """The SHA-256 of a base run's ``publish_runs`` row in the writer's canonical form (``feature_sets.base_row_digest``): the ``ROW_FIELDS``, sorted keys and
    compact separators."""
    document = {name: row.get(name) for name in ROW_FIELDS}
    return hashlib.sha256(json.dumps(document, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def base_row(evidence: S3Evidence, source: str) -> dict[str, Any]:
    """The ``publish_runs`` row of base run ``source`` as its own evidence gives it, never the ledger's head. Its full snapshot set comes from its features
    record and its commit time from the apply record of its own Feast phase, which names the publication it committed (run, commit time, table count and the
    digest of the snapshot set) and is uploaded once, after the commit. A missing or inconsistent record is refused."""
    folder = f"{EVIDENCE_PREFIX}/features/{source}"
    snapshot_key, apply_key = f"{folder}/{LAYERS['features'].output}", f"{folder}/{APPLY_RECORD}"
    snapshot = require_record(evidence.read(snapshot_key), snapshot_key, source, "features", SNAPSHOT_KEYS, SET_KEYS)
    if snapshot.get("feature_set") not in (None, "base"):
        raise LineageRefused(f"{snapshot_key}: is the record of set {snapshot['feature_set']}, not of a base run")
    snapshots = snapshot["snapshot_set"]
    applied = evidence.read(apply_key)
    if applied is None:
        raise LineageRefused(
            f"{apply_key}: missing: the commit time of base run {source}, which its row digest covers, is recorded nowhere else in the evidence"
        )
    publication = applied.get("publication") if isinstance(applied, dict) else None
    if not isinstance(publication, dict) or applied.get("step") != "apply" or applied.get("run_id") != source or applied.get("mode") != MODE:
        raise LineageRefused(f"{apply_key}: is not the apply record of real base run {source}")
    if publication.get("run_id") != source or publication.get("tables") != len(snapshots):
        raise LineageRefused(f"{apply_key}: its publication is not the {len(snapshots)} tables of base run {source}")
    sealed = hashlib.sha256(json.dumps(dict(sorted(snapshots.items())), separators=(",", ":")).encode()).hexdigest()
    if publication.get("snapshot_set_sha256") != sealed:
        raise LineageRefused(f"{snapshot_key}: its snapshot set is not the one that base run {source} committed ({apply_key})")
    return {"run_id": source, "committed_at": timestamp_text(publication.get("committed_at"), apply_key), "snapshot_set": snapshots, "base_run": None}


def input_pins(evidence: S3Evidence, facts: LaunchFacts, declared: Mapping[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], str, str | None]:
    """The pins of the job's pin file and the frozen pins they must equal, with the key of the pin file and the input run the file names. A model set's
    pin file pins the domain matrices of its base run, so the pins it must equal are those tables of the base run's record."""
    layer = MODEL_LAYER if facts.model else LAYERS[facts.job]
    folder = f"{EVIDENCE_PREFIX}/{facts.job}/{facts.run_id}"
    key = f"{folder}/{layer.pin}"
    record = evidence.read(key)
    if layer.frozen is None:
        claim_key = f"{folder}/state/pinned.json"
        claim = evidence.read(claim_key)
        if claim != {"layer": facts.job, "mode": MODE, "run_id": facts.run_id}:
            raise LineageRefused(f"{claim_key}: the run's one pin claim is missing or names another run, so its pins are not frozen")
        if record is None:
            raise LineageRefused(f"{key}: missing from the evidence of a succeeded job")
        pins = snapshot_items(layer.input_schema, record, key)
        return pins, pins, key, None
    record = require_record(record, key, facts.run_id, layer.input_schema, MODEL_PIN_KEYS if facts.model else PIN_KEYS)
    if facts.model and not (isinstance(record["base_row_sha256"], str) and DIGEST.fullmatch(record["base_row_sha256"])):
        raise LineageRefused(f"{key}: the digest of the base run's ledger row is not a SHA-256")
    source = record["source_run_id"]
    if not isinstance(source, str) or not RUN_ID.fullmatch(source):
        raise LineageRefused(f"{key}: the input run {source!r} is not a run ID")
    if facts.input_run != "latest" and source != facts.input_run:
        raise LineageRefused(f"{key}: pins run {source}, not the input run {facts.input_run} bound at launch")
    frozen, _ = output_set(evidence, layer.frozen[0], source, required=True)
    if facts.model:
        if base_row_digest(base_row(evidence, source)) != record["base_row_sha256"]:
            raise LineageRefused(f"{key}: base_row_sha256 is not the digest of the row of base run {source} that its own evidence gives")
        pinned = {(item["namespace"], item["name"]) for item in declared["inputs"]}
        frozen = [item for item in frozen or [] if (item["namespace"], item["name"]) in pinned]
    return snapshot_items(layer.input_schema, record["snapshot_set"], key), frozen or [], key, source


def job_lineage(job: str, invocation: Mapping[str, Any], declared: Mapping[str, Any], evidence: S3Evidence, facts: LaunchFacts) -> JobLineage:
    """The datasets of one ended attempt with the versions its evidence proves. Raises ``LineageRefused`` or ``EvidenceUnavailable``."""
    succeeded = facts.outcome == "succeeded"
    inputs = [dict(item) for item in declared["inputs"]]
    outputs = [dict(item) for item in declared["outputs"]]
    versions: dict[tuple[str, str], dict[str, str]] = {}
    unreadable = ""
    source = None
    if succeeded and job in LAYERS and inputs:
        pins, frozen, key, source = input_pins(evidence, facts, declared)
        record = {"version": 1, "invocation": dict(invocation), "inputs": pins}
        try:
            checked = validate_input_record(record, dict(invocation), inputs, succeeded=True, pinned=frozen)
        except SnapshotError as error:
            raise LineageRefused(f"{key}: {error}") from None
        versions |= {(item["namespace"], item["name"]): item["snapshot"] for item in checked["inputs"]}
    if succeeded and job in LAYERS and outputs:
        try:
            expect = {"feature_set": facts.model, "base_run": source} if facts.model and source else None
            found, key = output_set(evidence, job, facts.run_id, required=False, expect=expect)
        except EvidenceDenied as error:  # the one optional read: unreadable is not missing, and says so
            found, unreadable = None, str(error)
        if found is not None:
            record = {"version": 1, "invocation": dict(invocation), "outputs": found}
            try:
                checked = validate_snapshot_record(record, dict(invocation), outputs, succeeded=True)
            except SnapshotError as error:
                raise LineageRefused(f"{key}: {error}") from None
            versions |= {(item["namespace"], item["name"]): item["snapshot"] for item in checked["outputs"]}

    def versioned(item: dict[str, Any]) -> dict[str, Any]:
        identity = versions.get((item["namespace"], item["name"]))
        return item if identity is None else {**item, "facets": version_facet(identity)}

    missing = [item for item in outputs if (item["namespace"], item["name"]) not in versions]
    state = "not_applicable" if not outputs else "complete" if not missing else "missing" if len(missing) == len(outputs) else "partial"
    if unreadable:
        state = "unreadable"
    return JobLineage(
        inputs=[versioned(item) for item in inputs],
        outputs=[versioned(item) for item in outputs],
        launch=launch_facet(invocation, facts, state, missing, f"{EvidenceDenied.__name__}: {unreadable}" if unreadable else None),
    )


def table_inputs(evidence: S3Evidence, layer: str, run_id: str, declared: Mapping[str, Any]) -> dict[str, list[dict[str, str]]]:
    """The tables a layer run published, each with the tables its own model reads, from the run's record beside its dbt copy. A record that is missing, names
    another run or layer, names a table the job did not publish or an input it neither pinned nor published, or has a table read itself, is refused by key."""
    key = INPUTS_KEY.format(layer=layer, run=run_id)
    record = evidence.read(key)
    if record is None:
        raise LineageRefused(f"{key}: missing from the evidence of a succeeded job")
    if not isinstance(record, dict) or set(record) != INPUTS_KEYS or record["schema"] != 1:
        raise LineageRefused(f"{key}: does not have exactly the keys {sorted(INPUTS_KEYS)} of schema version 1")
    if record["run_id"] != run_id:
        raise LineageRefused(f"{key}: belongs to another run ({record['run_id']})")
    if record["layer"] != layer:
        raise LineageRefused(f"{key}: names layer {record['layer']}, not {layer}")
    tables = record["tables"]
    if not isinstance(tables, dict):
        raise LineageRefused(f"{key}: the tables are not an object")
    published = {item["name"] for item in declared["outputs"]}
    allowed = {(item["namespace"], item["name"]) for item in [*declared["inputs"], *declared["outputs"]]}
    found: dict[str, list[dict[str, str]]] = {}
    for table, reads in tables.items():
        if not IDENTIFIER.fullmatch(str(table)) or dataset(layer, table)["name"] not in published:
            raise LineageRefused(f"{key}: {table!r} is not a published table of the {layer} job")
        if not isinstance(reads, list) or not all(isinstance(item, dict) and set(item) == {"layer", "table"} for item in reads):
            raise LineageRefused(f"{key}: the inputs of {table} are not a list of layer and table")
        items = []
        for item in reads:
            if (item["layer"], item["table"]) == (layer, table):
                raise LineageRefused(f"{key}: {table} reads itself")
            read = dataset(str(item["layer"]), str(item["table"]))
            if (read["namespace"], read["name"]) not in allowed:
                raise LineageRefused(f"{key}: {table} reads {read['name']}, which the job neither pinned nor published (not declared)")
            items.append(read)
        found[table] = items
    return found


def layer_table_events(evidence: S3Evidence, declared: Mapping[str, Any], invocation: Mapping[str, Any], facts: LaunchFacts) -> list[dict[str, Any]]:
    """The events of a layer job's published tables. Each table is its own run, a child of the step's run: the tables its own model reads in (as the run's
    record beside its dbt copy lists them) and itself out, each with the version the layer's evidence proves, so the receiver's pairing of the inputs of an
    event with its outputs gives the tables' real edges, not every input with every output of the layer. A job that did not succeed claims no table."""
    if facts.outcome != "succeeded":
        return []
    layer = facts.job
    full = job_lineage(layer, invocation, declared, evidence, facts)
    versioned = {(item["namespace"], item["name"]): item for item in [*full.inputs, *full.outputs]}
    reads = table_inputs(evidence, layer, facts.run_id, declared)
    parent = {
        "_producer": PRODUCER,
        "_schemaURL": PARENT_SCHEMA,
        "run": {"runId": run_uuid(invocation)},
        "job": {"namespace": JOB_NAMESPACE, "name": f"{invocation['dag_id']}.{invocation['task_id']}"},
    }
    found = []
    for table in sorted(reads):
        output = versioned[(dataset(layer, table)["namespace"], dataset(layer, table)["name"])]
        inputs = [versioned[(item["namespace"], item["name"])] for item in reads[table]]
        state = "complete" if "facets" in output else "unreadable" if full.launch["snapshotState"] == "unreadable" else "missing"
        missing = [] if state == "complete" else [{"namespace": output["namespace"], "name": output["name"]}]
        launch = launch_facet(invocation, facts, state, missing, full.launch.get("refusal"))
        table_invocation = {**invocation, "task_id": f"{invocation['task_id']}.{table}"}
        for event in events(JobLineage(inputs, [output], launch), table_invocation, facts):
            event["run"]["facets"]["parent"] = parent
            found.append(event)
    return found


def launch_facet(invocation: Mapping[str, Any], facts: LaunchFacts, state: str, missing: list[dict[str, str]], refusal: str | None = None) -> dict[str, Any]:
    facet: dict[str, Any] = {
        "_producer": PRODUCER,
        "_schemaURL": FACET_SCHEMA,
        "runId": facts.run_id,
        "job": facts.job,
        "attempt": facts.attempt,
        "template": invocation["template"],
        "boundInputRun": facts.input_run,
        "snapshotState": state,
        "missingOutputs": missing,
    }
    if refusal is not None:
        facet["refusal"] = refusal[:600]
    return facet


def run_uuid(invocation: Mapping[str, Any]) -> str:
    """A stable run ID of the step's task attempt, from the same fields as the invocation key."""
    key = [JOB_NAMESPACE, invocation["dag_id"], invocation["task_id"], invocation["dag_run_id"], invocation["run_id"], invocation["attempt"]]
    return str(uuid.uuid5(uuid.NAMESPACE_URL, canonical(key)))


def events(lineage: JobLineage, invocation: Mapping[str, Any], facts: LaunchFacts) -> list[dict[str, Any]]:
    """The START and terminal events of the step's task attempt. START names the declared inputs without versions; a terminal event other than COMPLETE
    names no output."""
    terminal = TERMINAL.get(facts.outcome, "FAIL")
    plain_inputs = [{"namespace": item["namespace"], "name": item["name"]} for item in lineage.inputs]
    found = []
    for event_type, instant in (("START", facts.started_at), (terminal, facts.ended_at)):
        found.append(
            {
                "eventType": event_type,
                "eventTime": instant,
                "run": {"runId": run_uuid(invocation), "facets": {"haiLaunch": lineage.launch}},
                "job": {"namespace": JOB_NAMESPACE, "name": f"{invocation['dag_id']}.{invocation['task_id']}"},
                "inputs": lineage.inputs if event_type == "COMPLETE" else plain_inputs,
                "outputs": lineage.outputs if event_type == "COMPLETE" else [],
                "producer": PRODUCER,
                "schemaURL": EVENT_SCHEMA,
            }
        )
    return found


@cache
def validators() -> tuple[Draft202012Validator, Draft202012Validator]:
    resources = []
    for name, digest in sorted(SPEC_FILES.items()):
        raw = (ROOT / SPEC_FOLDER / name).read_bytes()
        if hashlib.sha256(raw).hexdigest() != digest:
            raise LineageRefused(f"{SPEC_FOLDER / name} is not the pinned copy of the OpenLineage spec")
        document = json.loads(raw)
        resources.append((document["$id"], Resource.from_contents(document)))
    registry = Registry().with_resources(resources)
    return Draft202012Validator({"$ref": EVENT_SCHEMA}, registry=registry), Draft202012Validator(json.loads((ROOT / FACET_FILE).read_text()))


def check_event(event: Mapping[str, Any]) -> None:
    """Refuse an event that is not an OpenLineage 2-0-2 RunEvent or whose ``haiLaunch`` facet breaks its closed schema."""
    event_check, facet_check = validators()
    problems = [error.message for error in event_check.iter_errors(dict(event))]
    problems += [f"haiLaunch: {error.message}" for error in facet_check.iter_errors(event.get("run", {}).get("facets", {}).get("haiLaunch"))]
    if problems:
        raise LineageRefused(f"event {event.get('eventType')} is not valid: {problems[:3]}")


@dataclass
class LineageSource:
    """The lineage of one step's launch, as the launch operator hands it to Airflow's OpenLineage provider and as the replay rebuilds it."""

    job: str
    run_id: str
    attempt: int
    invocation: Mapping[str, Any]
    declared: Mapping[str, Any]
    evidence: S3Evidence
    store: LaunchStore

    @property
    def layer(self) -> str:
        """The job whose lineage this launch family carries (``job`` is the family its launch records sit under)."""
        return lineage_layer(self.job)

    def lineage(self) -> tuple[JobLineage, LaunchFacts]:
        """The task's own lineage: the run facet and no dataset. A layer job's versions are checked here and stay in the facet's state, but its datasets travel
        in the per-table events, because one event holding every input and every output makes the receiver pair each input with each output."""
        facts = launch_facts(self.store, self.run_id, self.job, self.attempt)
        if self.layer == BRONZE_JOB:
            return JobLineage([], [], launch_facet(self.invocation, facts, "not_applicable", [])), facts
        full = job_lineage(self.layer, self.invocation, self.declared, self.evidence, facts)
        return (JobLineage([], [], full.launch) if self.layer in TABLE_EVENT_JOBS else full), facts

    def table_events(self, facts: LaunchFacts) -> list[dict[str, Any]]:
        """The per-table events of a succeeded layer job: its published tables and their own inputs."""
        return layer_table_events(self.evidence, self.declared, self.invocation, facts)

    def table_lines(self) -> list[str]:
        """The per-table events of a succeeded job as canonical JSON lines, the bytes the replay gives; nothing for a run that did not succeed."""
        facts = launch_facts(self.store, self.run_id, self.job, self.attempt)
        if self.layer not in TABLE_EVENT_JOBS or facts.outcome != "succeeded":
            return []
        found = self.table_events(facts)
        for event in found:
            check_event(event)
        return [canonical(event) for event in found]

    def send_table_events(self, environ: Mapping[str, str] | None = None) -> int:
        """Send the per-table events of a succeeded bronze or layer run to the receiver of the HTTP lineage settings, within ``TABLE_EVENTS_SECONDS``.
        Nothing is read or sent without those settings. Returns how many events were sent; ``DeliveryFailed`` when the receiver fails or the deadline passes."""
        target = http_lineage(os.environ if environ is None else environ)
        if self.layer not in TABLE_EVENT_JOBS or target is None:
            return 0
        lines = self.table_lines()
        send_within(lines, target[0], target[1], TABLE_EVENTS_SECONDS)
        return len(lines)

    def events(self) -> list[dict[str, Any]]:
        lineage, facts = self.lineage()
        found = events(lineage, self.invocation, facts)
        if self.layer in TABLE_EVENT_JOBS:
            found += self.table_events(facts)
        for event in found:
            check_event(event)
        return found

    def facets(self, complete: bool = True) -> dict[str, Any]:
        """Inputs, outputs and run facets for the provider. A refused or unreadable record never fails the task: the datasets go without versions and
        the facet names the refusal."""
        try:
            lineage, _ = self.lineage()
        except (LineageRefused, EvidenceUnavailable) as error:
            facts = LaunchFacts(self.run_id, self.layer, self.attempt, "", "", "unknown", "latest", model_set_of(self.job) or "")
            with contextlib.suppress(LineageRefused, EvidenceUnavailable):  # the facet still names the run, job and attempt; the refusal says why
                facts = launch_facts(self.store, self.run_id, self.job, self.attempt)
            grouped = self.layer in TABLE_EVENT_JOBS or self.layer == BRONZE_JOB  # a declaration names every table of the job, so a refusal claims none of them
            outputs = [] if self.layer == BRONZE_JOB else [dict(item) for item in self.declared["outputs"]]
            lineage = JobLineage(
                [] if grouped else [dict(item) for item in self.declared["inputs"]],
                [] if grouped else outputs,
                launch_facet(self.invocation, facts, "refused", outputs, f"{type(error).__name__}: {error}"),
            )
        inputs = lineage.inputs if complete else [{"namespace": item["namespace"], "name": item["name"]} for item in lineage.inputs]
        return {"inputs": inputs, "outputs": lineage.outputs if complete else [], "run_facets": {"haiLaunch": lineage.launch}}


def post_event(url: str, payload: bytes, credential: str, timeout: float = DELIVERY_SECONDS) -> int:
    """POST one event to ``<url>/api/v1/openlineage/lineage`` with the bearer credential. Each connect, send and read waits at most ``timeout`` seconds.
    Every failure is a ``DeliveryFailed`` that names the kind of failure and never the credential: the HTTP client puts a header value it refuses (a key with
    a trailing newline, a character outside Latin-1) into its own error text, so that text is never shown."""
    parts = urllib.parse.urlsplit(url)
    if parts.scheme not in ("http", "https") or not parts.hostname or parts.path not in ("", "/") or parts.query or parts.username:
        raise DeliveryFailed("the receiver must be an http or https address with no path, query or user")
    kind = http.client.HTTPSConnection if parts.scheme == "https" else http.client.HTTPConnection
    try:
        connection = kind(parts.hostname, parts.port, timeout=timeout)
    except ValueError:
        raise DeliveryFailed("the receiver address has an invalid port") from None
    try:
        connection.request("POST", ROUTE, body=payload, headers={"Content-Type": "application/json", "Authorization": f"Bearer {credential}"})
        response = connection.getresponse()
        response.read(65536)
    except ValueError as error:  # includes UnicodeError: the request was refused before it was sent
        raise DeliveryFailed(f"the request was refused before it was sent: {type(error).__name__}; check the credential's characters") from None
    except (OSError, http.client.HTTPException) as error:
        raise DeliveryFailed(f"the receiver did not answer within {timeout:g} s: {type(error).__name__}") from None
    finally:
        connection.close()
    if not 200 <= response.status < 300:
        raise DeliveryFailed(f"the receiver answered HTTP {response.status}")
    return int(response.status)


def http_lineage(environ: Mapping[str, str]) -> tuple[str, str] | None:
    """The receiver address and credential of the HTTP lineage settings (``infra/services/airflow.tf``), or None when HTTP lineage is not on."""
    url, key = environ.get(TRANSPORT_URL, ""), environ.get(TRANSPORT_KEY, "")
    return (url, key) if environ.get(TRANSPORT_TYPE) == "http" and url and key else None


def send_within(lines: Sequence[str], url: str, credential: str, seconds: float) -> None:
    """Send each line in order with ``post_event`` inside one total deadline. No send starts after it and each waits at most the time left. Raises
    ``DeliveryFailed`` for the first failure, for an exception of the sender that is not one (named by its type only, because its text can hold a header)
    and when the deadline passes with events unsent. The sender writes nothing to stderr and ends only by finishing or by failing."""
    end = time.monotonic() + seconds
    failure: list[DeliveryFailed] = []
    finished: list[bool] = []

    def run() -> None:
        try:
            for line in lines:
                left = end - time.monotonic()
                if left <= 0:
                    failure.append(DeliveryFailed(f"the deadline of {seconds:g} s passed with events unsent"))
                    return
                post_event(url, line.encode(), credential, timeout=min(DELIVERY_SECONDS, left))
            finished.append(True)
        except DeliveryFailed as error:
            failure.append(error)
        except Exception as error:  # nothing of the thread may reach stderr or end it as a success
            failure.append(DeliveryFailed(f"the sender stopped with {type(error).__name__}"))

    sender = threading.Thread(target=run, daemon=True)
    sender.start()
    sender.join(seconds)
    if sender.is_alive():
        raise DeliveryFailed(f"the deadline of {seconds:g} s passed with events unsent")
    if failure:
        raise failure[0]
    if not finished:
        raise DeliveryFailed("the sender ended without sending every event")


def parser() -> argparse.ArgumentParser:
    command = argparse.ArgumentParser(
        prog="python -m orchestration.ecs_lineage", description="Rebuild the lineage events of a direct ECS job from S3 evidence."
    )
    sub = command.add_subparsers(dest="command", required=True)
    replay_command = sub.add_parser("replay", help="print the events of one step's attempt, one JSON line each, and optionally send them")
    for name in ("--bucket", "--job", "--run-id", "--template", "--dag-id", "--task-id", "--dag-run-id"):
        replay_command.add_argument(name, required=True)
    replay_command.add_argument("--attempt", type=int, required=True)
    replay_command.add_argument("--input-run", help="the input run of a pin template's invocation")
    replay_command.add_argument("--training", help="the training parameters of the invocation, as JSON")
    replay_command.add_argument("--send", metavar="URL", help="the receiver, for example http://127.0.0.1:8585")
    replay_command.add_argument("--credential-file", type=Path, help="file that holds the lineage bot credential; required with --send")
    return command


def replay(client: Any, argv: Sequence[str], *, declared: Mapping[str, Any] | None = None, send: tuple[str, str] | None = None) -> list[str]:
    """The events of one step's attempt as canonical JSON lines, rebuilt from the launch records and the evidence; nothing is written to the bucket.
    ``send`` is the receiver address and credential; each event is sent in order, so a repeat sends the same bytes again."""
    args = parser().parse_args(["replay", *argv])
    request: dict[str, Any] = {
        "request_version": 2,
        "template": args.template,
        "run_id": args.run_id,
        "mode": MODE,
        "attempt": args.attempt,
        "dag_id": args.dag_id,
        "task_id": args.task_id,
        "dag_run_id": args.dag_run_id,
    }
    if args.input_run:
        request["input_run_id"] = args.input_run
    if args.training:
        request["training"] = json.loads(args.training)
    invocation = step_invocation(request)
    source = LineageSource(
        job=args.job,
        run_id=args.run_id,
        attempt=args.attempt,
        invocation=invocation,
        declared=declared if declared is not None else declared_lineage(args.job),
        evidence=S3Evidence(client, args.bucket),
        store=S3LaunchStore(client, args.bucket, launch_record_prefix()),
    )
    lines = [canonical(event) for event in source.events()]
    if send is not None:
        for line in lines:
            post_event(send[0], line.encode(), send[1])
    return lines


def s3_client() -> Any:
    import boto3

    return boto3.client("s3")


def main(argv: Sequence[str] | None = None, client_factory: Callable[[], Any] = s3_client) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    args = parser().parse_args(arguments)
    if args.send and args.credential_file is None:
        sys.stderr.write("--send needs --credential-file\n")
        return 2
    send = (args.send, args.credential_file.read_text().strip()) if args.send else None
    try:
        lines = replay(client_factory(), arguments[1:], send=send)
    except (LineageRefused, EvidenceUnavailable, DeliveryFailed) as error:
        sys.stderr.write(f"replay refused: {type(error).__name__}: {error}\n")
        return 1
    sys.stdout.write("".join(line + "\n" for line in lines))
    return 0


if __name__ == "__main__":
    sys.exit(main())
