"""Phase driver of the silver, gold and features ECS jobs that the job broker starts.

    python -m scripts.lakehouse.layer_run <phase> --layer silver|gold|features --run <run ID> [--input-run <run ID>] [--work /workspace] [--dry-run]

Each ECS container of a job runs one phase on the shared ``/workspace`` (the container names of ``config/orchestration/ecs_broker_jobs.json``):

- silver: ``pin``, ``build``, ``validate``, ``verify``, ``export``, ``publish``, ``upload``
- gold: ``pin``, ``build``, ``validate``, ``export``, ``publish``, ``upload``
- features: ``pin``, ``build``, ``validate``, ``export``, ``publish``, ``feast``

The first container of the task (``fetch``) has already downloaded and unpacked ``code.tar.gz``. ``pin`` checks its SHA-256 again, records the code
identity and pins the input layer: bronze for silver, the silver run ``--input-run`` for gold, the gold run for features. Every later digest comes
from run folder files, so a phase runs only on the exact output of the phase before. The code is the unpacked archive in ``<work>/code``, the working
folder of every module, and the run folder is ``<work>/code/data/analytics/dbt/runs/<run ID>``.

``publish`` runs the publish steps of the layer and the acceptance checks of the first publish (rows and content fingerprints of every table against
its candidate snapshot before the commit, the commit record against the candidates, the backup and the reconcile record after it). ``upload`` and
``feast`` run only for a run that passed them. Any failed check exits 1 and names the phase. A retry of one run repeats every step on the same keys
and snapshot IDs and writes no second publication.

With ``--dry-run`` or ``LAYER_DRY_RUN=1`` the phases up to ``export`` run as usual, because they write only the task's own working folder. ``publish``
then reads the catalog without writing, prints and saves the plan of the real publish, ``upload`` uploads nothing and ``feast`` does nothing. The
first dry phase marks the run folder, so a later phase without the flag stays dry. A new ``pin`` clears the mark."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import tarfile
from collections.abc import Sequence
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from orchestration.pipeline_protocol import PipelineError, digest_file, read_record, seal_copy
from scripts.lakehouse import provenance
from scripts.lakehouse.gold_publish_record import RUN_ID, SHA256, PublishError, accept_consumer
from scripts.process import TimeoutExpired, run_command

PHASES = {
    "silver": ("pin", "build", "validate", "verify", "export", "publish", "upload"),
    "gold": ("pin", "build", "validate", "export", "publish", "upload"),
    "features": ("pin", "build", "validate", "export", "publish", "feast"),
}
STEP_SECONDS = 4 * 3600
BUCKET_FILE = Path("/workspace/infra/deployment.auto.tfvars.json")  # the path gold_publish.build_store reads for the backup bucket
UPLOAD_PREFIX = "lakehouse/layer_runs"
NOT_UPLOADED = ("snapshot", "silver_input", "gold_input", "staging.duckdb")  # the code copy, the bulk input extracts and the build file the sealed copy repeats
DRY_ENV = "LAYER_DRY_RUN"
DRY_MARKER = "dry_run"  # in the run folder: once a task has a dry phase, every later phase of it is dry
PLAN_FILE = "publish/dry_run_plan.json"
PLAN_PREFIX = "DRY_RUN_PLAN "  # the printed plan line, so the owner can find it in the task log
SUITES = "data_contracts/great_expectations"
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

    @property
    def folder(self) -> Path:
        return self.code / "data/analytics/dbt/runs" / self.run

    @property
    def publish_folder(self) -> Path:
        return self.folder / "publish"

    @property
    def local_flags(self) -> list[str]:
        return ["--local-warehouse", str(self.local)] if self.local else []


def run_module(job: Job, module: str, arguments: Sequence[str | Path], environment: dict[str, str] | None = None) -> None:
    """Run ``python -m module`` in the code folder; a nonzero exit or a deadline names the module."""
    try:
        done = run_command(sys.executable, ["-m", module, *map(str, arguments)], cwd=job.code, env={**os.environ, **(environment or {})}, timeout=STEP_SECONDS)
    except TimeoutExpired:
        raise StepError(f"{module} ran longer than {STEP_SECONDS} seconds") from None
    sys.stdout.write(done.stdout)
    sys.stderr.write(done.stderr)
    if done.returncode:
        raise StepError(f"{module} exited {done.returncode}")


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
    pin = digest_file(job.folder / PIN_FILE[job.layer])
    return (pin, None) if job.layer == "silver" else (pin, digest_file(job.folder / INPUT_FOLDER[job.layer] / "manifest.json"))


def bundle(job: Job, archive: Path, code_sha256: str) -> None:
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
    record = {"run_id": job.run, "layer": job.layer, "mode": job.mode, "code_sha256": code_sha256, "git_revision": revision}
    record["source_sha256"] = provenance.tree_digest(snapshot)
    (job.folder / "layer_run.json").write_text(json.dumps(record, indent=2, sort_keys=True) + "\n")


def pin(job: Job) -> None:
    if job.layer == "silver":
        run_module(job, "scripts.lakehouse.pins", ["pin", "--run", job.run])
        return
    if not RUN_ID.fullmatch(job.input_run):
        raise StepError(f"the {job.layer} pin needs the input run ID (INPUT_RUN_ID or --input-run)")
    record = job.folder / PIN_FILE[job.layer]
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
    run_module(job, "scripts.features.features_input", ["pin", *common, "--folder", job.folder, "--record", record, *job.local_flags])
    run_module(
        job,
        "scripts.features.features_input",
        ["read", "--run", job.run, "--mode", job.mode, "--expected-pin-sha256", digest_file(record), "--folder", job.folder, *job.local_flags],
    )


def verify(job: Job) -> None:
    """Silver only: no bronze table moved since the pin."""
    run_module(job, "scripts.lakehouse.pins", ["verify", "--run", job.run])


def build(job: Job) -> None:
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
    arguments: list[str | Path] = ["--mode", job.mode, "--run", job.run, "--folder", job.folder, "--project", snapshot / "project"]
    arguments += ["--source-sha256", source, "--input-sha256", input_sha, *(["--extract-sha256", extract] if extract else [])]
    if job.layer == "features":
        run_module(job, "scripts.features.features_build", arguments, environment)
    else:
        run_module(job, "scripts.lakehouse.pipeline_build", ["--layer", job.layer, *arguments], environment)


def validate(job: Job) -> None:
    """Run the layer's quality gates and require every expectation to pass on the same build bytes."""
    baselines = "fixture_baselines.json" if job.mode == "publish_fixture" else "baselines.json"
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
            run_module(job, "scripts.features.feature_validation", arguments)
        elif kind == "cross":
            run_module(job, "scripts.lakehouse.gold_reconcile", [*arguments, "--baselines", Path(SUITES) / baselines])
        else:
            run_module(job, "scripts.lakehouse.silver_quality", [*arguments, "--baselines", Path(SUITES) / baselines])
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
    return gold_publish.load_plan() if job.layer == "gold" else features_publish.publish_plan(job.mode)


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
        run_module(job, "scripts.features.features_export", arguments)
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
    """Decision 31 item 2 before any commit: rows and content fingerprints of exactly the candidate snapshots equal the export, table by table."""
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
        if entry.get("fingerprint") != entry.get("export_fingerprint") or not SHA256.fullmatch(str(entry.get("fingerprint"))):
            problems.append(f"{name}: the candidate's fingerprint differs from the export")
    if problems:
        raise StepError("publish acceptance failed: " + "; ".join(problems[:5]))


def accept_commit(job: Job) -> None:
    """Decision 31 item 3: the commit record is committed, follows the backup that followed the reconcile, and names every candidate snapshot ID."""
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
        run_module(job, "scripts.features.features_publish", arguments)


def publish(job: Job, bucket: str) -> None:
    sha, _ = sealed(job)
    accepted = accepted_result(job, sha)
    if job.mode == "real":
        write_bucket_file(bucket)
    common = ["--run", job.run, "--mode", job.mode]
    for step in ("candidates", "reconcile", "backup", "commit"):
        run_publish(job, [step, *common, "--sealed-sha256", sha, "--accepted-result-sha256", accepted, "--publish-dir", job.publish_folder, *job.local_flags])
        if step == "reconcile":
            accept_reconcile(job)
        if step == "commit":
            accept_commit(job)
    if job.layer in RECORD_FILE:
        run_publish(job, ["record", *common, "--publish-dir", job.publish_folder, "--record", job.folder / RECORD_FILE[job.layer], *job.local_flags])
    sys.stdout.write(f"publish acceptance passed for {job.layer} run {job.run}\n")


def accepted_publish(job: Job) -> None:
    """The run's records must still show an accepted publish before anything leaves the task."""
    accept_reconcile(job)
    accept_commit(job)


def uploadable(job: Job) -> list[Path]:
    files = (path for path in sorted(job.folder.rglob("*")) if path.is_file() and not path.is_symlink())
    return [path for path in files if path.relative_to(job.folder).parts[0] not in NOT_UPLOADED]


def upload(job: Job, bucket: str, client: Any) -> int:
    """Copy the run folder to ``lakehouse/layer_runs/<layer>/<run>/``; a repeat writes the same keys."""
    accepted_publish(job)
    prefix = f"{UPLOAD_PREFIX}/{job.layer}/{job.run}/"
    files = uploadable(job)
    for path in files:
        client.upload_file(str(path), bucket, prefix + path.relative_to(job.folder).as_posix())
    return len(files)


def feast(job: Job) -> None:
    accepted_publish(job)
    for step in ("apply", "materialize"):
        run_module(job, "scripts.features.feast_job", [step, "--run", job.run, "--mode", job.mode])


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


def dispatch(job: Job, phase: str, args: argparse.Namespace) -> None:
    if phase == "pin":
        bundle(job, args.archive or args.work / "code.tar.gz", args.code_sha256)
        pin(job)
    elif phase == "upload" and job.dry_run:
        sys.stdout.write(json.dumps({"dry_run": True, "uploaded": 0, "would_upload": len(uploadable(job))}) + "\n")
    elif phase == "feast" and job.dry_run:
        sys.stdout.write(json.dumps({"dry_run": True, "feast": "not applied"}) + "\n")
    elif phase == "publish" and job.dry_run:
        plan_publish(job)
    elif phase == "upload":
        sys.stdout.write(json.dumps({"uploaded": upload(job, args.bucket, s3_client())}) + "\n")
    elif phase == "publish":
        publish(job, args.bucket)
    else:
        {"build": build, "validate": validate, "verify": verify, "export": export, "feast": feast}[phase](job)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("phase", choices=sorted({phase for phases in PHASES.values() for phase in phases}))
    parser.add_argument("--layer", required=True, choices=sorted(PHASES))
    parser.add_argument("--run", required=True)
    parser.add_argument("--input-run", default=os.environ.get("INPUT_RUN_ID", ""), help="gold and features: the committed silver or gold run to pin")
    parser.add_argument("--mode", default="real", choices=("real", "publish_fixture"))
    parser.add_argument("--work", type=Path, default=Path("/workspace"))
    parser.add_argument("--local-warehouse", type=Path)
    parser.add_argument("--archive", type=Path)
    parser.add_argument("--code-sha256", default=os.environ.get("CODE_SHA256", ""))
    parser.add_argument("--bucket", default=os.environ.get("DATA_BUCKET_NAME", ""))
    parser.add_argument(
        "--dry-run", action="store_true", help=f"publish: print and save the plan and write nothing; upload and feast do nothing (also set by {DRY_ENV}=1)"
    )
    args = parser.parse_args(argv)
    if args.phase not in PHASES[args.layer]:
        parser.error(f"{args.phase} is not a phase of {args.layer}")
    if not RUN_ID.fullmatch(args.run):
        parser.error("the run ID must look like 20261030T010203Z-0a1b2c3d")
    job = Job(args.layer, args.run, args.mode, args.work / "code", args.local_warehouse, input_run=args.input_run)
    dry = args.dry_run or os.environ.get(DRY_ENV, "") in ("1", "true", "yes")
    try:
        if args.phase == "pin":  # a new pin starts a new task
            (job.folder / DRY_MARKER).unlink(missing_ok=True)
        elif (job.folder / DRY_MARKER).exists():
            dry = True
        if dry:
            mark_dry(job)
            job = replace(job, dry_run=True)
        if args.phase in ("publish", "upload") and not args.bucket and args.mode == "real" and not dry:
            raise StepError("DATA_BUCKET_NAME (or --bucket) is not set")
        dispatch(job, args.phase, args)
    except (StepError, PipelineError, PublishError, provenance.ProvenanceError, OSError, ValueError, KeyError, tarfile.TarError) as error:
        sys.stderr.write(f"layer_run: step {args.phase} ({args.layer}, run {args.run}) failed: {error}\n")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
