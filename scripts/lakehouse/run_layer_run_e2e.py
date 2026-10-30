"""E2E of the layer driver on a throwaway catalog model, with stand-ins for the modules that need the Spark, analytics and quality images (no Docker, no AWS).

    .venv/bin/python -m scripts.lakehouse.run_layer_run_e2e [--report data/e2e/layer_run/<stamp>/report.json]

The driver (``scripts/lakehouse/layer_run.py``) runs for real through ``main``: the archive and tree digests, the seal, the consumer acceptance rule, the
publish acceptance checks, the dry run and the upload keys. The stand-ins replace only the modules of the images (pins, inputs, build, quality, exports and
publish steps, Feast). Each stand-in checks the digest arguments the driver gave it, so a wrong digest stops the chain. Every write to the shared catalog or
bucket goes through ``shared_write``, so a case can prove what a run wrote, and in what order.
"""

from __future__ import annotations

import argparse
import ast
import contextlib
import hashlib
import io
import json
import os
import re
import sys
import tarfile
import tempfile
import traceback
from collections.abc import Callable, Iterator, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest import mock

import duckdb

from scripts.lakehouse import layer_run
from scripts.lakehouse.layer_run import Job, StepError

REVISION = "0123456789abcdef0123456789abcdef01234567"
SILVER_RUN = "20261030T010203Z-0a1b2c3d"
GOLD_RUN = "20261030T020304Z-1b2c3d4e"
FEATURES_RUN = "20261030T030405Z-2c3d4e5f"
BUCKET = "hai-test-bucket"
CHAIN = {"silver": SILVER_RUN, "gold": GOLD_RUN, "features": FEATURES_RUN}
INPUT_OF = {"silver": None, "gold": SILVER_RUN, "features": GOLD_RUN}
CATALOG: list[Path] = []  # the throwaway catalog folder of the case being run
DEFECTS: set[str] = set()  # the stand-in defects of the case being run
UPLOADS: list[tuple[str, str]] = []


def require(condition: bool, evidence: object = "") -> None:
    if not condition:
        raise AssertionError(evidence)


def option(arguments: Sequence[str | Path], name: str) -> str:
    values = [str(item) for item in arguments]
    return values[values.index(name) + 1]


def sha_of(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def fail(module: str) -> None:
    raise StepError(f"{module} exited 1")


def contract(layer: str, mode: str = "real") -> tuple[str, Any]:
    """The namespace of the mode and the publish contract of the layer, as the real publish modules read them."""
    from scripts.features import features_publish
    from scripts.lakehouse import gold_publish, silver_publish

    plan = {"silver": lambda: silver_publish.publish_plan(mode), "gold": gold_publish.load_plan, "features": lambda: features_publish.publish_plan(mode)}[
        layer
    ]()
    return plan.namespaces[mode], plan


def shared_write(kind: str, **detail: Any) -> None:
    with (CATALOG[0] / "writes.log").open("a") as handle:
        handle.write(json.dumps({"kind": kind, **detail}, sort_keys=True) + "\n")


def writes() -> list[dict[str, Any]]:
    file = CATALOG[0] / "writes.log"
    return [json.loads(line) for line in file.read_text().splitlines()] if file.exists() else []


def model(name: str, default: Any) -> Any:
    file = CATALOG[0] / name
    return json.loads(file.read_text()) if file.exists() else default


def export_rows(index: int) -> dict[str, str]:
    return {f"k{number}": f"v{index}" for number in range(2 + index % 3)}


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


# Options a step never reads although the parser gives them a /workspace default: the pin step pins the input layer and touches no publish folder, and
# the four publish steps write their records into the publish folder, not to --record.
UNUSED_DEFAULTS = {("silver_publish", "pin"): {"publish_dir"}} | {
    (module, step): {"record"} for module in ("silver_publish", "gold_publish", "features_publish") for step in ("candidates", "reconcile", "backup", "commit")
}


def parser_options(module: str) -> dict[str, tuple[bool, str | None]]:
    """Each ``--option`` the module's parser declares: whether it is required and its default when that is a literal or ``Path`` of a literal. The source is
    read, not imported, because the quality and Spark modules need packages that only their images carry."""
    tree = ast.parse((Path(__file__).resolve().parents[2] / (module.replace(".", "/") + ".py")).read_text())
    options: dict[str, tuple[bool, str | None]] = {}
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "add_argument" and node.args):
            continue
        names = [arg.value for arg in node.args if isinstance(arg, ast.Constant) and isinstance(arg.value, str) and arg.value.startswith("--")]
        keywords = {keyword.arg: keyword.value for keyword in node.keywords}
        default = keywords.get("default")
        if isinstance(default, ast.Call) and default.args:
            default = default.args[0]
        value = default.value if isinstance(default, ast.Constant) else None
        flag = keywords.get("required")
        required = isinstance(flag, ast.Constant) and flag.value is True
        for name in names:
            options[name] = (required, value if isinstance(value, str) else None)
    return options


def argument_contract(module: str, arguments: Sequence[str | Path]) -> None:
    """Check the driver's arguments against the module's own parser: every option must exist, every required one must be given, and no option that has a
    default under /workspace may be left to it, because the task's code folder is elsewhere and a default would read or write the wrong run."""
    options = parser_options(module)
    require(bool(options), f"{module} declares no parser options")
    given = [str(item) for item in arguments if str(item).startswith("--")]
    step = str(arguments[0]) if arguments else ""
    unused = UNUSED_DEFAULTS.get((module.rsplit(".", 1)[1], step), set())
    problems = [f"{name} is not an option of {module}" for name in given if name not in options]
    problems += [f"{name} is required by {module}" for name, (required, _) in options.items() if required and name not in given]
    problems += [
        f"{module} {step}: {name} is left at the default {default}"
        for name, (_, default) in options.items()
        if default and default.startswith("/workspace") and name not in given and name[2:].replace("-", "_") not in unused
    ]
    require(not problems, "; ".join(problems))


def standin(job: Job, module: str, arguments: Sequence[str | Path], environment: dict[str, str] | None = None) -> None:
    """One stand-in for every module the driver runs; the digest checks are the ones the real modules make."""
    argument_contract(module, arguments)
    folder = job.folder
    name = module.rsplit(".", 1)[1]
    step = str(arguments[0]) if arguments else ""
    if name == "pins":
        if step == "pin":
            (folder / "pins.json").write_text(json.dumps({"bronze_a": "11"}, sort_keys=True) + "\n")
        elif not (folder / "pins.json").is_file():
            fail(module)
    elif name in ("silver_publish", "features_input", "features_publish", "gold_publish") and step in (
        "pin",
        "read",
        "record",
        "candidates",
        "reconcile",
        "backup",
        "commit",
    ):
        layer_step(job, module, name, step, arguments)
    elif name in ("silver_input",):
        if option(arguments, "--expected-pin-sha256") != sha_of(folder / "silver_pins.json"):
            fail(module)
        (folder / "silver_input").mkdir(exist_ok=True)
        (folder / "silver_input/manifest.json").write_text(json.dumps({"run_id": job.run}) + "\n")
    elif name in ("pipeline_build", "features_build"):
        build(job, module, arguments, environment or {})
    elif name in ("silver_quality", "gold_reconcile", "feature_validation"):
        database = Path(option(arguments, "--database"))
        counts = {"pass": 3, "warn": 1 if "quality_warn" in DEFECTS else 0, "fail": 0}
        result: dict[str, Any] = {"counts": counts, "build_file_sha256": sha_of(database)}
        if name == "gold_reconcile":
            result["reconciliation"] = {"facts": 2, "facts_in_contract": 2, "problems": {}}
        Path(option(arguments, "--output")).write_text(json.dumps(result) + "\n")
    elif name.endswith("_export"):
        export(job, module, arguments)
    elif name == "publish_plan":
        plan_state(job, arguments)
    elif name == "feast_job":
        shared_write("feast", step=step, run=job.run)
    else:
        raise AssertionError(f"no stand-in for {module}")


def profile_database(job: Job) -> None:
    """Every DuckDB output of the tracked dbt profiles writes under /workspace/out. On the task that folder is the shared volume, so the database path of the
    profile must resolve to the staging file of this run, which validation and the sealed copy read. The outputs that merge another one share its path."""
    text = (Path(__file__).resolve().parents[2] / "dbt/profiles.yml").read_text()
    paths: dict[str, str] = {}
    output = ""
    for line in text.splitlines():
        if match := re.match(r"^    (\w+):", line):
            output = match.group(1)
        elif match := re.match(r"^      path: (/workspace/\S+)\s*$", line):
            paths[output] = match.group(1)
    require(bool(paths), "no profile output keeps its database under /workspace")
    for name, path in paths.items():
        here = job.code.parent / Path(path).relative_to("/workspace")
        require(here.resolve() == (job.folder / "staging.duckdb").resolve(), f"profile {name} writes {path}, which is not this run's staging database")


def features_source_root(project: Path) -> None:
    """The features builder takes the parent of ``--project`` as the repository root and reads its contracts from there."""
    from scripts.features import features_build

    for relative in (features_build.CONTRACT, features_build.GOLD_PLAN):
        require((project.parent / relative).is_file(), f"the source root {project.parent} has no {relative}")


def build(job: Job, module: str, arguments: Sequence[str | Path], environment: dict[str, str]) -> None:
    from scripts.lakehouse import provenance

    folder = job.folder
    source = provenance.tree_digest(folder / "snapshot")
    pin_name = {"silver": "pins.json", "gold": "silver_pins.json", "features": "gold_pins.json"}[job.layer]
    expected_input = sha_of(folder / pin_name)
    if option(arguments, "--source-sha256") != source or option(arguments, "--input-sha256") != expected_input:
        fail(module)
    if job.layer != "silver":
        extract = folder / ("silver_input" if job.layer == "gold" else "gold_input") / "manifest.json"
        if option(arguments, "--extract-sha256") != sha_of(extract):
            fail(module)
    if job.layer == "silver" and job.mode == "real" and (environment or {}).get(provenance.VARIABLE_REQUIRED) != "1":
        fail(module)
    profile_database(job)
    if module.endswith("features_build"):
        features_source_root(Path(option(arguments, "--project")))
    connection = duckdb.connect(str(folder / "staging.duckdb"))
    connection.execute("create or replace table t as select 1 as x")
    connection.close()
    receipt = {"run_id": job.run, "layer": job.layer, "mode": job.mode, "source_sha256": source, "input_sha256": expected_input}
    (folder / "build_receipt.json").write_text(json.dumps(receipt) + "\n")


def export(job: Job, module: str, arguments: Sequence[str | Path]) -> None:
    sealed, expected = Path(option(arguments, "--sealed")), option(arguments, "--sealed-sha256")
    actual = sha_of(sealed)
    publish = Path(option(arguments, "--publish-dir"))
    export_dir = publish / "export"
    export_dir.mkdir(exist_ok=True)
    tables = {}
    names = sorted(contract(job.layer, job.mode)[1].tables) + (["outside_table"] if "foreign_table" in DEFECTS else [])
    for index, table in enumerate(names):
        rows = export_rows(index)
        (export_dir / f"{table}.json").write_text(json.dumps(rows))
        tables[table] = {"file": f"{table}.json", "rows": len(rows), "columns": [["key", "string"], ["value", "string"]], "merge_key": ["key"]}
    manifest = {"run_id": job.run, "sealed_sha256": expected, "in_container_sha256": actual, "tables": tables}
    (export_dir / "manifest.json").write_text(json.dumps(manifest, sort_keys=True) + "\n")
    result = {"run_id": job.run, "expected_sha256": expected, "in_container_sha256": actual, "exported": actual == expected}
    result["manifest_sha256"] = sha_of(export_dir / "manifest.json")
    (publish / "consumer_result.json").write_text(json.dumps(result) + "\n")
    if actual != expected:
        fail(module)


def layer_step(job: Job, module: str, name: str, step: str, arguments: Sequence[str | Path]) -> None:
    folder = job.folder
    publish = folder / "publish"
    runs = model("publish_runs.json", [])
    if step == "pin":
        source = option(arguments, "--source-run") if "--source-run" in [str(item) for item in arguments] else ""
        if source and not any(row["run_id"] == source for row in runs):
            fail(module)
        target = Path(option(arguments, "--record")) if "--record" in [str(item) for item in arguments] else folder / "gold_pins.json"
        target.write_text(json.dumps({"source_run_id": source, "snapshot_set": {"t": 1}}) + "\n")
    elif step == "read":
        if option(arguments, "--expected-pin-sha256") != sha_of(folder / "gold_pins.json"):
            fail(module)
        (folder / "gold_input").mkdir(exist_ok=True)
        (folder / "gold_input/manifest.json").write_text(json.dumps({"run_id": job.run}) + "\n")
    elif step == "record":
        Path(option(arguments, "--record")).write_text(json.dumps({"run_id": job.run}) + "\n")
    else:
        publish_step(job, module, step, arguments, publish)


def publish_step(job: Job, module: str, step: str, arguments: Sequence[str | Path], publish: Path) -> None:
    result_file = publish / "consumer_result.json"
    if (
        option(arguments, "--accepted-result-sha256") != sha_of(result_file)
        or option(arguments, "--sealed-sha256") != read_json(result_file)["in_container_sha256"]
    ):
        fail(module)
    if step == "backup" and job.mode == "real" and read_json(layer_run.BUCKET_FILE) != {"data_bucket_name": BUCKET}:
        fail(module)
    namespace, plan = contract(job.layer, job.mode)
    manifest = read_json(publish / "export/manifest.json")
    if step == "candidates":
        if set(manifest["tables"]) != set(plan.tables):
            fail(module)
        shared_write("branch", layer=job.layer, run=job.run)
        snapshots = {name: 100 + index for index, name in enumerate(sorted(plan.tables))}
        record = {
            "run_id": job.run,
            "complete": True,
            "export_manifest_sha256": sha_of(publish / "export/manifest.json"),
            "sealed_sha256": option(arguments, "--sealed-sha256"),
        }
        record["tables"] = {name: {"candidate_snapshot_id": snapshot, "base_snapshot_id": None} for name, snapshot in snapshots.items()}
        (publish / "candidates.json").write_text(json.dumps(record, sort_keys=True) + "\n")
    elif step == "reconcile":
        candidates = read_json(publish / "candidates.json")
        tables = {}
        for name, item in manifest["tables"].items():
            rows = item["rows"] + (1 if "reconcile_rows" in DEFECTS else 0)
            fingerprint = "f" * 64
            entry = {
                "candidate_snapshot_id": candidates["tables"][name]["candidate_snapshot_id"],
                "rows": rows,
                "export_rows": item["rows"],
                "manifest_rows": item["rows"],
            }
            entry |= {"fingerprint": fingerprint if "reconcile_fingerprint" not in DEFECTS else "e" * 64, "export_fingerprint": fingerprint}
            entry["equal"] = True
            tables[name] = entry
        record = {"run_id": job.run, "candidates_sha256": sha_of(publish / "candidates.json"), "tables": tables, "pass": True}
        (publish / "reconcile.json").write_text(json.dumps(record, sort_keys=True) + "\n")
    elif step == "backup":
        shared_write("backup", layer=job.layer, run=job.run)
        data = json.dumps({"run_id": job.run, "tables": sorted(manifest["tables"])}).encode()
        name = f"backup_{job.run}"
        STORED[f"{plan.backup_prefix}{name}"] = data + (b" altered" if "backup_object" in DEFECTS else b"")
        record = {
            "run_id": job.run,
            "read_back": True,
            "reconcile_sha256": sha_of(publish / "reconcile.json"),
            "name": name,
            "backup_sha256": hashlib.sha256(data).hexdigest(),
        }
        (publish / "backup.json").write_text(json.dumps(record, sort_keys=True) + "\n")
    elif step == "commit":
        commit(job, publish, namespace)


def commit(job: Job, publish: Path, namespace: str) -> None:
    candidates = read_json(publish / "candidates.json")
    snapshot_set = {name: item["candidate_snapshot_id"] for name, item in sorted(candidates["tables"].items())}
    if "commit_snapshots" in DEFECTS:
        snapshot_set[sorted(snapshot_set)[0]] += 1
    runs = model("publish_runs.json", [])
    tables = model("tables.json", {})
    committed = any(row["run_id"] == job.run for row in runs)
    export_dir = publish / "export"
    for name, item in sorted(read_json(export_dir / "manifest.json")["tables"].items()):
        wanted = read_json(export_dir / item["file"])
        current = tables.get(f"{namespace}.{name}")
        action = "create" if current is None else ("unchanged" if current == wanted else "change")
        if not committed:
            tables[f"{namespace}.{name}"] = wanted
            shared_write("table", table=f"{namespace}.{name}", action=action)
    (CATALOG[0] / "tables.json").write_text(json.dumps(tables, sort_keys=True))
    if not committed:
        (CATALOG[0] / "publish_runs.json").write_text(json.dumps([*runs, {"layer": job.layer, "run_id": job.run}]))
        shared_write("publish_runs", layer=job.layer, run=job.run)
    receipt = {"run_id": job.run, "backup_sha256": sha_of(publish / "backup.json"), "snapshot_set": snapshot_set, "state": "committed"}
    (publish / "commit.json").write_text(json.dumps(receipt, sort_keys=True) + "\n")


def plan_state(job: Job, arguments: Sequence[str | Path]) -> None:
    """Stand-in for the read-only catalog module: it reads the model and the export and writes nothing to the catalog."""
    namespace, plan = contract(job.layer, job.mode)
    tables_now = model("tables.json", {})
    runs = model("publish_runs.json", [])
    tables: dict[str, Any] = {}
    publish = job.folder / "publish"
    manifest = read_json(publish / "export/manifest.json")
    for name, item in manifest["tables"].items():
        wanted = read_json(publish / "export" / item["file"])
        current = tables_now.get(f"{namespace}.{name}")
        if current is None:
            tables[name] = {"exists": False}
        else:
            tables[name] = {
                "exists": True,
                "rows_now": len(current),
                "same": current == wanted,
                "added": len(wanted.keys() - current.keys()),
                "removed": len(current.keys() - wanted.keys()),
            }
    state = {"namespace_exists": True, "tables": tables}
    state["runs_table"] = {"exists": True, "rows_now": len(runs), "run_committed": any(row["run_id"] == job.run for row in runs)}
    Path(option(arguments, "--output")).write_text(json.dumps(state, sort_keys=True) + "\n")


STORED: dict[str, bytes] = {}  # the bucket objects the stand-in publish steps write: the catalog backups
STAGED_VERSION = ["1"]  # the object version of the staged code files; a case changes it to model an input replaced during the run


class FakeS3:
    def upload_file(self, filename: str, bucket: str, key: str) -> None:
        require(bucket == BUCKET, bucket)
        UPLOADS.append((bucket, key))
        shared_write("upload", key=key)

    def head_object(self, Bucket: str, Key: str) -> dict[str, Any]:
        require(Bucket == BUCKET and Key.startswith("lakehouse/run_stage/"), (Bucket, Key))
        return {"VersionId": STAGED_VERSION[0], "ETag": f'"{Key.rsplit("/", 1)[1]}"', "ContentLength": 64}

    def get_object(self, Bucket: str, Key: str) -> dict[str, Any]:
        require(Bucket == BUCKET and Key in STORED, (Bucket, Key))
        return {"Body": io.BytesIO(STORED[Key])}


def archive(folder: Path) -> tuple[Path, str]:
    """A ``git archive`` style tarball: the commit ID in the pax global header, the dbt project and its packages inside."""
    source = folder / "src"
    for name in ("dbt", "dbt_packages"):
        (source / name).mkdir(parents=True)
        (source / name / "marker.txt").write_text(name)
    repository = Path(__file__).resolve().parents[2]
    for relative in ("data_contracts/lakehouse/gold_contract.json", "data_contracts/lakehouse/features_publish.json"):
        (source / relative).parent.mkdir(parents=True, exist_ok=True)
        (source / relative).write_bytes((repository / relative).read_bytes())
    path = folder / "code.tar.gz"
    with tarfile.open(path, "w:gz", format=tarfile.PAX_FORMAT, pax_headers={"comment": REVISION}) as tar:
        for name in ("dbt", "dbt_packages", "data_contracts"):
            tar.add(source / name, arcname=name)
    return path, sha_of(path)


class Case:
    """One workspace: its own catalog model, code archive and work folder."""

    def __init__(self, root: Path) -> None:
        self.root = root
        CATALOG[:] = [root / "catalog"]
        CATALOG[0].mkdir()
        UPLOADS.clear()
        DEFECTS.clear()
        STORED.clear()
        STAGED_VERSION[0] = "1"
        self.archive, self.digest = archive(root)
        self.out = ""
        self.err = ""

    def work(self, layer: str, label: str = "w") -> Path:
        path = self.root / f"{label}_{layer}"
        path.mkdir(exist_ok=True)
        if not (path / "code.tar.gz").exists():
            (path / "code.tar.gz").write_bytes(self.archive.read_bytes())
        return path

    def phase(
        self, layer: str, phase: str, *, label: str = "w", digest: str | None = None, extra: Sequence[str] = (), environment: dict[str, str] | None = None
    ) -> int:
        work = self.work(layer, label)
        arguments = [phase, "--layer", layer, "--run", CHAIN[layer], "--work", str(work), "--code-sha256", digest or self.digest, "--bucket", BUCKET, *extra]
        if INPUT_OF[layer] and "--input-run" not in extra:
            arguments += ["--input-run", INPUT_OF[layer] or ""]
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            try:
                code = layer_run.main(arguments)
            except SystemExit as stop:
                code = int(stop.code or 0)
        self.out += out.getvalue()
        self.err += err.getvalue()
        return code

    def chain(self, layer: str, *, label: str = "w", extra: Sequence[str] = ()) -> list[int]:
        codes = []
        for phase in layer_run.PHASES[layer]:
            codes.append(self.phase(layer, phase, label=label, extra=extra))
            if codes[-1]:
                break
        return codes


@contextlib.contextmanager
def patched(root: Path) -> Iterator[Case]:
    with (
        mock.patch.object(layer_run, "run_module", standin),
        mock.patch.object(layer_run, "BUCKET_FILE", root / "deployment.auto.tfvars.json"),
        mock.patch.object(layer_run, "s3_client", lambda: FakeS3()),
        mock.patch.dict(os.environ, {"LAYER_DRY_RUN": "", "INPUT_RUN_ID": ""}),
    ):
        yield Case(root)


def kinds() -> list[str]:
    return [item["kind"] for item in writes()]


def case_silver_chain(case: Case) -> None:
    codes = case.chain("silver")
    require(codes == [0] * len(layer_run.PHASES["silver"]), (codes, case.err))
    require(len(model("publish_runs.json", [])) == 1, "one publish_runs row")
    require(kinds().index("publish_runs") > max(index for index, kind in enumerate(kinds()) if kind == "table"), "publish_runs is the last catalog write")
    prefix = f"lakehouse/layer_runs/silver/{SILVER_RUN}/"
    require(bool(UPLOADS) and all(key.startswith(prefix) for _, key in UPLOADS), UPLOADS)
    require(not any("/snapshot/" in key or "staging.duckdb" in key or "silver_input" in key for _, key in UPLOADS), "bulk files are not uploaded")
    require("publish acceptance passed" in case.out, case.out)


def case_gold_and_features_chain(case: Case) -> None:
    require(case.chain("silver") == [0] * 7, case.err)
    require(case.chain("gold") == [0] * 6, case.err)
    require(case.chain("features") == [0] * 6, case.err)
    require([row["layer"] for row in model("publish_runs.json", [])] == ["silver", "gold", "features"], model("publish_runs.json", []))
    require(kinds().count("feast") == 2 and kinds()[-1] == "feast", "feast applies and materializes last")
    for layer in ("silver", "gold"):
        require(any(key.startswith(f"lakehouse/layer_runs/{layer}/") for _, key in UPLOADS), layer)
    require(not any("/features/" in key for _, key in UPLOADS), "features uploads nothing")


def case_retry_has_no_second_effect(case: Case) -> None:
    require(case.chain("silver", label="first") == [0] * 7, case.err)
    first_writes, first_uploads = len(writes()), sorted(UPLOADS)
    first_rows = model("publish_runs.json", [])
    # A retry in a new task starts from an empty workspace.
    require(case.chain("silver", label="second") == [0] * 7, case.err)
    require(model("publish_runs.json", []) == first_rows, "no second publish_runs row")
    new = writes()[first_writes:]
    require(all(item["kind"] in ("branch", "backup", "upload") for item in new), new)
    require(not [item for item in new if item["kind"] in ("table", "publish_runs")], "no table or publish_runs write on the retry")
    require(sorted(set(UPLOADS)) == sorted(set(first_uploads)), "the retry uploads the same keys")
    # A retry of the publish phase in the same workspace also writes nothing new to the catalog.
    before = len(writes())
    require(case.phase("silver", "publish", label="second") == 0, case.err)
    require(not [item for item in writes()[before:] if item["kind"] in ("table", "publish_runs")], "publish twice in one workspace")


def refuses(case: Case, layer: str, phases: Sequence[str], at: str, **options: Any) -> None:
    """Run phases until ``at``, which must exit non-zero, and name the phase in the message."""
    for phase in phases:
        code = case.phase(layer, phase, **options) if phase == at else case.phase(layer, phase)
        if phase == at:
            require(code != 0, f"{at} must fail ({case.err})")
            require(f"step {at}" in case.err, case.err)
            return
        require(code == 0, (phase, case.err))


def case_wrong_code_digest(case: Case) -> None:
    refuses(case, "silver", ["pin"], "pin", digest="0" * 64)
    require(not (case.root / "w_silver/code/data/analytics/dbt/runs" / SILVER_RUN / "pins.json").exists(), "nothing pinned")


def case_missing_input_run(case: Case) -> None:
    require(case.chain("silver") == [0] * 7, case.err)
    for layer in ("gold", "features"):
        if layer == "features":
            require(case.chain("gold") == [0] * 6, case.err)
        code = case.phase(layer, "pin", extra=["--input-run", ""])
        require(code != 0 and "input" in case.err, case.err)


def case_input_run_not_published(case: Case) -> None:
    code = case.phase("gold", "pin", extra=["--input-run", "20260101T000000Z-deadbeef"])
    require(code != 0, "the pin of a silver run that was never committed must fail")


def case_quality_warning_stops_the_chain(case: Case) -> None:
    DEFECTS.add("quality_warn")
    refuses(case, "silver", ["pin", "build", "validate"], "validate")
    require(case.phase("silver", "export") != 0, "export must refuse an unvalidated build")
    require(writes() == [], "no shared write")


def case_snapshot_tamper(case: Case) -> None:
    require(case.phase("silver", "pin") == 0, case.err)
    (case.root / "w_silver/code/data/analytics/dbt/runs" / SILVER_RUN / "snapshot/project/marker.txt").write_text("tampered")
    require(case.phase("silver", "build") != 0 and "snapshot changed" in case.err, case.err)


def case_reconcile_mismatch_stops_before_commit(case: Case) -> None:
    DEFECTS.add("reconcile_fingerprint")
    for phase in ("pin", "build", "validate", "verify", "export"):
        require(case.phase("silver", phase) == 0, case.err)
    require(case.phase("silver", "publish") != 0 and "fingerprint" in case.err, case.err)
    require("table" not in kinds() and "publish_runs" not in kinds(), kinds())


def case_reconcile_rows_mismatch(case: Case) -> None:
    DEFECTS.add("reconcile_rows")
    for phase in ("pin", "build", "validate", "verify", "export"):
        require(case.phase("silver", phase) == 0, case.err)
    require(case.phase("silver", "publish") != 0 and "rows" in case.err, case.err)
    require("publish_runs" not in kinds(), kinds())


def case_commit_names_wrong_snapshots(case: Case) -> None:
    DEFECTS.add("commit_snapshots")
    for phase in ("pin", "build", "validate", "verify", "export"):
        require(case.phase("silver", phase) == 0, case.err)
    require(case.phase("silver", "publish") != 0 and "snapshot" in case.err, case.err)
    require(case.phase("silver", "upload") != 0, "a run that failed acceptance uploads nothing")
    require(not UPLOADS, UPLOADS)


def case_staged_input_changes_after_the_pin(case: Case) -> None:
    for phase in ("pin", "build", "validate", "verify", "export"):
        require(case.phase("silver", phase) == 0, case.err)
    STAGED_VERSION[0] = "2"
    require(case.phase("silver", "publish") != 0 and "staged input" in case.err, case.err)
    require(writes() == [], "a changed input object stops publish before any catalog write")


def case_stored_backup_differs_from_its_record(case: Case) -> None:
    DEFECTS.add("backup_object")
    for phase in ("pin", "build", "validate", "verify", "export"):
        require(case.phase("silver", phase) == 0, case.err)
    require(case.phase("silver", "publish") != 0 and "stored backup" in case.err, case.err)
    require("publish_runs" not in kinds() and "table" not in kinds(), "a backup that differs from its record stops the publish before the commit")


def case_export_names_foreign_table(case: Case) -> None:
    DEFECTS.add("foreign_table")
    for phase in ("pin", "build", "validate", "verify"):
        require(case.phase("silver", phase) == 0, case.err)
    require(case.phase("silver", "export") != 0 and "tables" in case.err, case.err)
    require(writes() == [], "a table outside the layer's namespace is refused before any write")


def case_dry_run_writes_nothing(case: Case) -> None:
    require(case.chain("silver") == [0] * 7, case.err)
    before = len(writes())
    uploads = len(UPLOADS)
    case.out = ""
    require(case.chain("gold", extra=["--dry-run"]) == [0] * 6, case.err)
    require(len(writes()) == before and len(UPLOADS) == uploads, writes()[before:])
    require(case.out.count("DRY_RUN_PLAN ") == 1 and "CREATE" in case.out, case.out)
    plan = json.loads(case.out.split("DRY_RUN_PLAN ", 1)[1].splitlines()[0])
    require(plan["layer"] == "gold" and plan["summary"]["create"] == plan["summary"]["tables"] > 0, plan["summary"])
    require(plan["blocked"] == [] and plan["runs_table"]["action"] == "append_row", plan)
    require("publish_runs" not in [row["layer"] for row in model("publish_runs.json", [])][1:], "no gold run row")
    # The first dry phase marks the folder, so a later phase without the flag stays dry.
    require(case.phase("gold", "publish") == 0 and len(writes()) == before, "a later phase of a dry task stays dry")


def case_dry_run_features_skips_feast(case: Case) -> None:
    require(case.chain("silver") == [0] * 7 and case.chain("gold") == [0] * 6, case.err)
    before = len(writes())
    require(case.chain("features", extra=["--dry-run"]) == [0] * 6, case.err)
    require(len(writes()) == before and "feast" not in kinds(), kinds())


def case_dry_run_flags_a_committed_run(case: Case) -> None:
    require(case.chain("silver") == [0] * 7, case.err)
    case.out = ""
    require(case.chain("silver", label="again", extra=["--dry-run"]) == [0] * 7, case.err)
    plan = json.loads(case.out.split("DRY_RUN_PLAN ", 1)[1].splitlines()[0])
    require(plan["blocked"] and plan["runs_table"]["action"] == "refused", plan)
    require(plan["summary"]["unchanged"] == plan["summary"]["tables"], plan["summary"])


def case_dry_run_from_the_environment(case: Case) -> None:
    require(case.chain("silver") == [0] * 7, case.err)
    before = len(writes())
    with mock.patch.dict(os.environ, {"LAYER_DRY_RUN": "1"}):
        require(case.chain("gold") == [0] * 6, case.err)
    require(len(writes()) == before, "LAYER_DRY_RUN=1 behaves as --dry-run")


def case_phase_not_in_layer(case: Case) -> None:
    require(case.phase("gold", "verify") != 0 and "not a phase of gold" in case.err, case.err)
    require(case.phase("features", "upload") != 0, case.err)
    require(case.phase("silver", "feast") != 0, case.err)


CASES: dict[str, Callable[[Case], None]] = {
    "silver chain publishes once, uploads under its run prefix": case_silver_chain,
    "gold and features chains follow silver, features uploads nothing": case_gold_and_features_chain,
    "a retry of one run has no second effect": case_retry_has_no_second_effect,
    "a wrong code digest stops the first phase": case_wrong_code_digest,
    "gold and features refuse a missing input run": case_missing_input_run,
    "gold refuses a silver run that was never committed": case_input_run_not_published,
    "a quality warning stops validate and export": case_quality_warning_stops_the_chain,
    "a changed code snapshot stops build": case_snapshot_tamper,
    "a fingerprint mismatch stops before any table or run row is written": case_reconcile_mismatch_stops_before_commit,
    "a row count mismatch stops before the run row": case_reconcile_rows_mismatch,
    "a commit naming other snapshots fails and uploads nothing": case_commit_names_wrong_snapshots,
    "an input object replaced after the pin stops publish before any write": case_staged_input_changes_after_the_pin,
    "a stored backup that differs from its record stops before the commit": case_stored_backup_differs_from_its_record,
    "an export with a table outside the contract is refused": case_export_names_foreign_table,
    "dry run writes nothing and prints one plan": case_dry_run_writes_nothing,
    "dry run of features skips Feast": case_dry_run_features_skips_feast,
    "dry run marks an already committed run as blocked": case_dry_run_flags_a_committed_run,
    "LAYER_DRY_RUN=1 starts a dry run": case_dry_run_from_the_environment,
    "a phase outside the layer exits non-zero": case_phase_not_in_layer,
}


def run_all(selected: Sequence[str]) -> list[dict[str, Any]]:
    results = []
    for name, case in CASES.items():
        if selected and not any(part in name for part in selected):
            continue
        with tempfile.TemporaryDirectory() as folder, patched(Path(folder)) as workspace:
            try:
                case(workspace)
                outcome, detail = "PASS", ""
            except Exception as error:
                outcome, detail = "FAIL", f"{type(error).__name__}: {error}\n{traceback.format_exc(limit=3)}"
        sys.stdout.write(f"{outcome} {name}\n")
        if detail:
            sys.stdout.write(detail + "\n")
        results.append({"case": name, "outcome": outcome, "detail": detail})
    return results


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--report", type=Path)
    parser.add_argument("--only", action="append", default=[], help="run the cases whose name contains this text")
    args = parser.parse_args(argv)
    results = run_all(args.only)
    failed = [item for item in results if item["outcome"] != "PASS"]
    sys.stdout.write(f"{len(results) - len(failed)} of {len(results)} cases passed\n")
    if not results:
        sys.stderr.write(f"no case name contains {' or '.join(args.only)}\n")
        return 1
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps({"at": datetime.now(UTC).isoformat(), "revision": REVISION, "results": results}, indent=2) + "\n")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
