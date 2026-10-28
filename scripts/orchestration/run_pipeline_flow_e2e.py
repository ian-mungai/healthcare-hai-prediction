"""Verify Synthetic pipeline seals, immutable placement and the final export consumer."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import tempfile
import time
from collections.abc import Callable
from datetime import UTC, datetime
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any

from orchestration import ecs_target, launcher
from scripts.lakehouse import gold_export, gold_publish
from scripts.lakehouse.gold_publish_record import PublishError
from services.hai.bootstrap.integrity import StageError, canonical, run_prefix
from services.hai.bootstrap.outputs import inspect_outputs
from services.hai.bootstrap.transfer import FileRef

ROOT = Path(__file__).resolve().parents[2]
RUN = "20261024T000000Z-a1b2c3d4"
SOURCES = (
    "scripts/orchestration/pipeline_seal.py",
    "scripts/orchestration/pipeline_flow_fixture.py",
    "scripts/orchestration/run_pipeline_flow_e2e.py",
    "orchestration/remote_run.py",
    "orchestration/pipeline_protocol.py",
    "scripts/lakehouse/gold_publish.py",
    "scripts/lakehouse/gold_export.py",
    "scripts/lakehouse/pipeline_build.py",
    "scripts/lakehouse/pipeline_quality.py",
    "orchestration/ecs_target.py",
    "scripts/orchestration/broker.py",
    "orchestration/launcher.py",
    "orchestration/pipeline_render.py",
    "orchestration/dbt_lineage.py",
    "orchestration/ecs_stage.py",
    "scripts/orchestration/pipeline_control.py",
    "scripts/orchestration/run_control.py",
    "scripts/orchestration/ledger.py",
    "scripts/orchestration/ecs_fake_api.py",
    "scripts/orchestration/ecs_fake_s3.py",
    "scripts/orchestration/ecs_stage_runtime_cases.py",
    "scripts/orchestration/run_runstore_e2e.py",
    "scripts/lakehouse/gold_publish_record.py",
    "scripts/lakehouse/gold_reconcile.py",
    "scripts/lakehouse/gold_checks.py",
    "scripts/lakehouse/silver_quality.py",
    "scripts/lakehouse/provenance.py",
    "scripts/lakehouse/run_lock.py",
    "services/hai/bootstrap/integrity.py",
    "services/hai/bootstrap/outputs.py",
    "services/hai/bootstrap/transfer.py",
    "services/hai/bootstrap/redaction.py",
    "scripts/process.py",
    "pyproject.toml",
    "config/orchestration/broker_templates.json",
    "config/quality/dependency_runtimes.json",
    "config/quality/mypy_runtimes.json",
    "data_contracts/orchestration/ecs_job.schema.json",
    "data_contracts/orchestration/ecs_outputs.schema.json",
    "data_contracts/orchestration/ecs_input_map.schema.json",
    "tests/orchestration/fixtures/tiny/dbt_project.yml",
    "tests/orchestration/fixtures/tiny/profiles.yml",
    "tests/orchestration/fixtures/tiny/models/schema.yml",
    "tests/orchestration/fixtures/tiny/models/silver_one.sql",
    "tests/orchestration/fixtures/tiny/models/silver_two.sql",
    "tests/orchestration/fixtures/tiny/models/int_silver_build_marker.sql",
    "tests/orchestration/fixtures/tiny/models/gold_one.sql",
    "tests/orchestration/fixtures/tiny_ge/baselines.json",
)


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def rejected(cause: str, action: Callable[[], Any]) -> None:
    try:
        action()
    except (StageError, PublishError, RuntimeError, OSError, ecs_target.PlacementError, launcher.ClientError) as error:
        observed = error.code if isinstance(error, StageError) else str(error)
        require(cause in observed, f"wrong refusal: expected {cause}, observed {observed}")
    else:
        raise AssertionError("missing refusal: " + cause)


def plan(tables: tuple[str, ...] = ("gold_one",)) -> gold_publish.Plan:
    return gold_publish.Plan(
        {name: ("dbt_invocation_id",) if name == "int_silver_build_marker" else ("id",) for name in tables},
        {"publish_fixture": "gold_e2e", "real": "gold"},
        "run_",
        "publish_runs",
        None,
        3,
        7,
        8,
    )


def export_fixture(folder: Path) -> gold_publish.Context:
    import duckdb

    database = folder / "sealed.duckdb"
    with duckdb.connect(str(database)) as connection:
        connection.execute("create table gold_one as select 1 as id union all select 2 as id")
    expected = gold_publish.sha256_file(database)
    publish = folder / "publish"
    gold_export.export_sealed(database, expected, publish, plan(), RUN)
    accepted = gold_publish.sha256_file(publish / "consumer_result.json")
    return gold_publish.Context(None, plan(), RUN, "publish_fixture", publish, expected, accepted_result_sha256=accepted)


def final_consumer(folder: Path, defect: str) -> dict[str, Any]:
    context = export_fixture(folder)
    manifest, digest, exports = gold_publish.verified_export(context)
    require(set(manifest["tables"]) == {"gold_one"}, "baseline exported the wrong tables")
    if defect == "missing":
        (exports / "gold_one.parquet").unlink()
    elif defect == "replaced":
        file = exports / "gold_one.parquet"
        file.write_bytes(b"x" * file.stat().st_size)
    elif defect == "symlink":
        file = exports / "gold_one.parquet"
        copy = folder / "valid_gold_one.parquet"
        copy.write_bytes(file.read_bytes())
        file.unlink()
        file.symlink_to(copy)
    elif defect == "extra":
        (exports / "unexpected.parquet").write_bytes(b"extra Synthetic export")
    elif defect == "manifest":
        (exports / "manifest.json").write_bytes((exports / "manifest.json").read_bytes() + b" ")
    elif defect == "result":
        context.path("consumer_result.json").write_bytes(context.path("consumer_result.json").read_bytes() + b" ")
    elif defect != "none":
        raise AssertionError("unknown export defect")
    if defect != "none":
        cause = "consumer result does not match" if defect == "manifest" else "not the one the launcher accepted" if defect == "result" else "exported file"
        rejected(cause, lambda: gold_publish.verified_export(context))
    return {"defect": defect, "manifest_sha256": digest, "tables": sorted(manifest["tables"])}


def seal_case(folder: Path, defect: str) -> dict[str, Any]:
    from scripts.orchestration import pipeline_seal

    inputs, outputs = folder / "inputs", folder / "out"
    inputs.mkdir()
    outputs.mkdir()
    source = inputs / "staging.duckdb"
    raw = b"Synthetic validated database\n" * 100_000
    source.write_bytes(raw)
    expected = hashlib.sha256(raw).hexdigest()
    reference = FileRef("staging.duckdb", run_prefix(RUN, 1) + "tries/build/out/staging.duckdb", "v1", expected, len(raw))
    request = {"run_id": RUN, "layer": "gold", "generation": 1, "validation_attempt": "validate_gold", "sha256": expected, "source": reference.document()}
    cause = ""
    if defect == "bytes":
        source.write_bytes(b"x" * len(raw))
        cause = "seal_digest"
    elif defect == "size":
        request["source"] = {**reference.document(), "size": len(raw) + 1}
        cause = "seal_digest"
    elif defect == "hash":
        request["sha256"] = "0" * 64
        cause = "seal_binding"
    elif defect == "malformed_source":
        request["source"] = {**reference.document(), "path": None}
        cause = "seal_binding"
    elif defect == "version":
        request["source"] = {**reference.document(), "version_id": "null"}
        cause = "invalid_reference"
    elif defect == "run":
        request["run_id"] = "20261024T000000Z-11111111"
        cause = "seal_binding"
    elif defect == "generation":
        request["generation"] = 2
        cause = "seal_binding"
    elif defect == "layer":
        request["layer"] = "bronze"
        cause = "seal_binding"
    elif defect == "validation":
        request["validation_attempt"] = "../validation"
        cause = "seal_binding"
    elif defect == "symlink":
        source.rename(inputs / "actual.duckdb")
        source.symlink_to("actual.duckdb")
        cause = "unsafe_source"
    elif defect == "missing":
        source.unlink()
        cause = "unsafe_source"
    elif defect == "wal":
        source.with_name("staging.duckdb.wal").write_bytes(b"Synthetic WAL")
        cause = "wal_present"
    request_file = folder / "request.json"
    request_file.write_bytes(canonical(request))
    if defect == "deadline":
        rejected("stage_deadline", lambda: pipeline_seal.seal(inputs, outputs, request, time.monotonic() - 1))
        require(not list(outputs.iterdir()), "deadline wrote seal evidence")
        return {"refused": "stage_deadline"}
    if cause:
        rejected(cause, lambda: pipeline_seal.seal(inputs, outputs, request, time.monotonic() + 10))
        require(not list(outputs.iterdir()), "invalid input wrote seal evidence")
        return {"refused": cause}
    result = pipeline_seal.seal(inputs, outputs, request, time.monotonic() + 10)
    wanted = {
        "run_id": RUN,
        "layer": "gold",
        "generation": 1,
        "sha256": expected,
        "size": len(raw),
        "source_key": reference.key,
        "source_version_id": "v1",
        "validation_attempt": "validate_gold",
    }
    require(result == wanted, "seal evidence lost immutable binding")
    require({path.name for path in outputs.iterdir()} == {"seal.json"}, "seal wrote an undeclared file")
    before = (outputs / "seal.json").read_bytes()
    require(pipeline_seal.seal(inputs, outputs, request, time.monotonic() + 10) == result, "repeat seal changed record")
    require((outputs / "seal.json").read_bytes() == before, "repeat changed seal bytes")
    inspect_outputs(outputs, pipeline_seal.declaration(), (), time.monotonic() + 10)
    if defect == "conflict":
        (outputs / "seal.json").write_bytes(b"conflicting Synthetic seal")
        rejected("seal_conflict", lambda: pipeline_seal.seal(inputs, outputs, request, time.monotonic() + 10))
    elif defect == "undeclared":
        (outputs / "consumer_result.json").write_bytes(b"{}")
        rejected("undeclared_output", lambda: inspect_outputs(outputs, pipeline_seal.declaration(), (), time.monotonic() + 10))
    elif defect != "none":
        raise AssertionError("unknown seal defect")
    return {"sealed_sha256": expected, "source_size": len(raw), "repeat_identical": True}


def flow_case(folder: Path, name: str) -> dict[str, Any]:
    from scripts.orchestration.pipeline_flow_fixture import run_flow

    return run_flow(folder, name)


def contract_flow(folder: Path, name: str) -> dict[str, Any]:
    from scripts.orchestration.pipeline_flow_fixture import run_flow

    return run_flow(folder, name, native_quality=False)


def acceptance_case(folder: Path, defect: str) -> dict[str, Any]:
    from scripts.orchestration.pipeline_flow_fixture import acceptance_defect

    return acceptance_defect(folder, defect)


def placement_case(folder: Path, defect: str) -> dict[str, Any]:
    from scripts.orchestration.pipeline_flow_fixture import placement_defect

    return placement_defect(folder, defect)


def cases() -> dict[str, Callable[[Path], dict[str, Any]]]:
    from functools import partial

    result: dict[str, Callable[[Path], dict[str, Any]]] = {
        "F1": partial(flow_case, name="F1"),
        "F2": partial(flow_case, name="F2"),
        "F3": partial(flow_case, name="F3"),
    }
    for name in ("F1", "F2", "F3"):
        result["contract_" + name] = partial(contract_flow, name=name)
    for defect in (
        "none",
        "bytes",
        "size",
        "hash",
        "version",
        "malformed_source",
        "run",
        "generation",
        "layer",
        "validation",
        "symlink",
        "missing",
        "wal",
        "deadline",
        "conflict",
        "undeclared",
    ):
        result["seal_" + defect] = partial(seal_case, defect=defect)
    for defect in ("none", "missing", "replaced", "extra", "manifest", "result", "symlink"):
        result["consumer_" + defect] = partial(final_consumer, defect=defect)
    for defect in (
        "receipt",
        "source",
        "generation",
        "cancelled",
        "stale_try",
        "missing_seal",
        "validation",
        "build",
        "frozen",
        "cross",
        "consumer_digest",
        "running",
        "store_conflict",
        "seal_head",
        "source_head",
        "validation_hash",
        "task_arn",
        "malformed_seal",
        "coding_error",
        "malformed_validation",
    ):
        result["acceptance_" + defect] = partial(acceptance_case, defect=defect)
    for defect in ("aws_first", "duplicate", "unverified", "writer", "started", "stale_bottleneck"):
        result["case76_" + defect] = partial(placement_case, defect=defect)
    return result


def snapshot() -> dict[str, str]:
    return {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() if (ROOT / name).is_file() else "missing" for name in SOURCES}


def dependencies() -> dict[str, str]:
    result = {}
    for name in ("duckdb", "dbt-core", "great-expectations"):
        try:
            result[name] = version(name)
        except PackageNotFoundError:
            result[name] = "unavailable"
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--case", action="append")
    parser.add_argument("--mutations", action="store_true")
    parser.add_argument("--light", action="store_true", help="Run contract flows and boundary cases. Native quality and Docker equivalence remain lead gates.")
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=False)
    before = snapshot()
    selected = cases()
    names = args.case or [name for name in selected if not args.light or name not in ("F1", "F2", "F3")]
    unknown = set(names) - selected.keys()
    if unknown:
        parser.error("unknown cases: " + ", ".join(sorted(unknown)))
    results = []
    started = time.monotonic()
    for name in names:
        case_started = time.monotonic()
        try:
            with tempfile.TemporaryDirectory(prefix="pipeline_flow_") as temporary:
                details = selected[name](Path(temporary).resolve())
            results.append({"case": name, "ok": True, "status": "passed", "details": details, "seconds": time.monotonic() - case_started})
        except Exception as error:
            status = "blocked" if isinstance(error, ModuleNotFoundError) and name in ("F1", "F2", "F3") else "failed"
            results.append(
                {"case": name, "ok": False, "status": status, "error": f"{type(error).__name__}: {error}", "seconds": time.monotonic() - case_started}
            )
    mutants: list[dict[str, Any]] = []
    if args.mutations:
        from scripts.orchestration.pipeline_flow_fixture import mutations

        try:
            mutants = mutations(args.out / "mutants")
        except Exception as error:
            mutants = [{"case": "mutation_setup", "ok": False, "error": f"{type(error).__name__}: {error}"}]
    report = {
        "feature": "Synthetic seal and pipeline flow fixtures",
        "created": datetime.now(UTC).isoformat(),
        "mode": "light" if args.light else "full",
        "command": [sys.executable, "-m", "scripts.orchestration.run_pipeline_flow_e2e", *sys.argv[1:]],
        "dependencies": dependencies(),
        "sources": before,
        "sources_unchanged": before == snapshot(),
        "results": results,
        "mutations": mutants,
        "seconds": time.monotonic() - started,
        "synthetic": True,
        "seed": 0,
        "limits": [
            "FakeS3 and FakeEcs",
            "in-process launcher handler transport",
            "RunStore injection at the unintegrated launcher hook",
            "contract flows supply Synthetic quality records and silver snapshot identities",
            "fixture adapter supplies pending local-result and broker-validation integration",
            "full native quality flows are excluded by --light",
            "no Docker baseline or image execution",
            "no AWS, catalog publication or dataset upload",
        ],
        "cleanup": "done",
    }
    report["ok"] = before == snapshot() and all(item["ok"] for item in results + mutants)
    (args.out / "report.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    (args.out / "retention.json").write_bytes(
        canonical({"class": 4, "created": report["created"], "purpose": report["feature"], "end_condition": "No report or review cites this run"})
    )
    sys.stdout.write(
        json.dumps(
            {"ok": report["ok"], "cases": len(results), "failed": [item["case"] for item in results if not item["ok"]], "report": str(args.out / "report.json")}
        )
        + "\n"
    )
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
