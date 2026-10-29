"""Check the AWS bronze dispatch contract with synthetic service stand-ins and an external journal implementation."""

from __future__ import annotations

import argparse
import hashlib
import importlib
import importlib.util
import json
import sys
import time
from datetime import UTC, datetime
from importlib.machinery import SourceFileLoader
from pathlib import Path
from types import ModuleType
from typing import Any
from unittest.mock import patch

from scripts.infrastructure.bronze_policy_cases import planted_storage_defects
from scripts.lakehouse import bronze_writer_cases

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "scripts/lakehouse/bronze_writer.py"
DEFECTS = (
    (
        "qualification_uses_production",
        '"bronze_e2e", "bronze_dictionary_e2e", "smoke_check_e2e", "duckdb_view_check_e2e"',
        '"bronze", "bronze_dictionary", "smoke_check", "duckdb_view_check"',
    ),
    ("production_gate_removed", 'if binding.layer_state != "on":', "if False:"),
    ("stale_epoch_allowed", "if binding.epoch != authority_epoch:", "if False:"),
    ("recovery_hold_removed", 'if binding.mode == "real" and binding.recovery_hold:', "if False:"),
    ("dictionary_missing_from_intent", '"bronze": ("bronze", "dictionary"),', '"bronze": ("bronze",),'),
    ("removal_grant_persists", 'if phase == "removal":', 'if phase in {"active", "removal"}:'),
    ("production_removal_grant_persists", 'if phase == "removal":', 'if phase == "removal" or (phase == "active" and mode == "real"): '),
    ("fixture_action_defaults_to_create", 'raise BindingError("checksum fixture action must be create or drop")', 'fixture_action = "create"'),
    ("task_role_not_selected", '["--bucket", binding.bucket, "--task-role"]', '["--bucket", binding.bucket]'),
    ("identity_not_checked", "if actual != expected:", "if False:"),
    ("namespace_override_allowed", "if binding.namespaces != expected_namespaces:", "if False:"),
)

STAGE_DEFECTS = (
    "scripts/acquisition/single_copy.py",
    "scripts/lakehouse/__init__.py",
    "config/lakehouse/retired_objects.json",
)


def load_module(path: Path, name: str, *, logical_path: Path | None = None) -> ModuleType:
    """Load the named read-only dependency without copying it into the checkout."""
    spec = importlib.util.spec_from_file_location(name, path, loader=SourceFileLoader(name, str(path)))
    if spec is None or spec.loader is None:
        raise ValueError("journal module cannot be loaded")
    module = importlib.util.module_from_spec(spec)
    if logical_path is not None:
        module.__file__ = str(logical_path)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def snapshot() -> dict[str, str]:
    """Hash only the declared source inputs, excluding deployment files and source data."""
    names = [
        "scripts/lakehouse/bronze_writer.py",
        "scripts/lakehouse/aws_bronze.py",
        "scripts/lakehouse/bronze_writer_cases.py",
        "scripts/lakehouse/run_aws_bronze_writer_e2e.py",
        "scripts/lakehouse/bronze.py",
        "scripts/lakehouse/dictionary.py",
        "scripts/lakehouse/smoke.py",
        "scripts/lakehouse/checksums.py",
        "scripts/lakehouse/namespace_binding.py",
        "scripts/lakehouse/bronze_namespace_cases.py",
        "scripts/infrastructure/bronze_policy_cases.py",
        "config/lakehouse/aws_bronze_templates.json",
        "config/lakehouse/aws_bronze_grants.json",
        "config/lakehouse/bronze_tables.json",
        "requirements.txt",
        "pyproject.toml",
        "scripts/infrastructure/policy_evaluator.py",
        "infra/iam/bronze_writer.tf",
        "infra/iam/bronze_writer_variables.tf",
    ]
    names += [str(path.relative_to(ROOT)) for path in sorted((ROOT / "infra/modules/bronze_writer_job").rglob("*.tf"))]
    names += [str(path.relative_to(ROOT)) for path in sorted((ROOT / "infra/modules/bronze_writer_job/tests").glob("*.tftest.hcl"))]
    names += ["infra/iam/tests/bronze_writer.tftest.hcl"]
    names += [f"infra/iam/owner_policies/{name}.json" for name in ("bronze_writer_storage", "job_reader_e2e", "bronze_execution", "bronze_project_delta")]
    names += json.loads((ROOT / "config/lakehouse/aws_bronze_templates.json").read_text()).get("stage_files", [])
    return {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in sorted(set(names)) if (ROOT / name).is_file()}


def cases(subject: ModuleType, journal: ModuleType, out: Path, deadline: float) -> list[dict[str, Any]]:
    """Run the complete, independently specified case list with one deadline."""
    results = []
    for name, case in bronze_writer_cases.CASES.items():
        if time.monotonic() >= deadline:
            raise TimeoutError("bronze stand-in cases exceeded their deadline")
        started = time.monotonic()
        try:
            detail = case(subject, journal, out / name)
            results.append({"case": name, "status": "passed", "detail": detail, "seconds": time.monotonic() - started})
        except (ValueError, RuntimeError, KeyError, TypeError, OSError) as error:
            results.append({"case": name, "status": "failed", "cause": str(error), "seconds": time.monotonic() - started})
        if time.monotonic() >= deadline:
            raise TimeoutError("bronze stand-in cases exceeded their deadline")
    return results


def defects(journal: ModuleType, out: Path, deadline: float) -> list[dict[str, Any]]:
    """Plant each declared defect in memory and require a case to detect it."""
    source = SOURCE.read_text()
    results = []
    for name, before, after in DEFECTS:
        if source.count(before) != 1:
            raise ValueError(f"defect {name} does not have exactly one source location")
        payload = out / name / "bronze_writer.source"
        payload.parent.mkdir(parents=True, exist_ok=False)
        payload.write_text(source.replace(before, after))
        module = load_module(payload, f"scripts.lakehouse.bronze_writer_mutant_{name}", logical_path=SOURCE)
        try:
            outcomes = cases(module, journal, out / name, deadline)
            failures = {row["case"]: row["cause"] for row in outcomes if row["status"] == "failed"}
            intended = {
                "removal_grant_persists": ("removal_and_recovery", "qualification active grants retain"),
                "production_removal_grant_persists": ("removal_and_recovery", "production active grants retain"),
                "production_gate_removed": ("admission_gates", "accepted the forbidden binding: off"),
                "fixture_action_defaults_to_create": ("scope_denial", "accepted the forbidden binding: fixture action"),
            }.get(name)
            caught = bool(failures) and (intended is None or intended[1] in failures.get(intended[0], ""))
            results.append({"defect": name, "status": "caught" if caught else "survived", "caught_by": sorted(failures), "causes": failures})
        finally:
            del sys.modules[module.__name__]
    return results


def stage_defects(subject: ModuleType, journal: ModuleType, out: Path, deadline: float) -> list[dict[str, Any]]:
    """Require the import-closure case to detect omitted repository modules, initializers and configuration files."""
    manifest = ROOT / "config/lakehouse/aws_bronze_templates.json"
    document = json.loads(manifest.read_text())
    original_read = Path.read_text
    results = []
    for relative in STAGE_DEFECTS:
        if time.monotonic() >= deadline:
            raise TimeoutError("worker stage defects exceeded their deadline")
        changed = {**document, "stage_files": [name for name in document["stage_files"] if name != relative]}
        folder = out / Path(relative).name
        folder.mkdir(parents=True, exist_ok=False)
        payload = json.dumps(changed, indent=2) + "\n"
        (folder / "manifest.json").write_text(payload)

        def read_text(path: Path, *args: Any, replacement: str = payload, **kwargs: Any) -> str:
            return replacement if path == manifest else original_read(path, *args, **kwargs)

        cause = ""
        try:
            with patch.object(Path, "read_text", read_text):
                bronze_writer_cases.worker_import_closure(subject, journal, folder / "case")
        except ValueError as error:
            cause = str(error)
        caught = "worker snapshot omits import or configuration files" in cause and relative in cause
        results.append(
            {"defect": f"stage_omits_{relative}", "status": "caught" if caught else "survived", "caught_by": "worker_import_closure", "cause": cause}
        )
    return results


def main() -> int:
    """Write repeatable stand-in evidence without launching AWS, Docker or Spark."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--journal-module", type=Path, required=True)
    parser.add_argument("--plant-defects", action="store_true")
    parser.add_argument("--timeout", type=float, default=60)
    args = parser.parse_args()
    if not 0 < args.timeout <= 60:
        parser.error("timeout must be greater than zero and at most 60 seconds")
    deadline = time.monotonic() + args.timeout
    args.out.mkdir(parents=True, exist_ok=False)
    stamp = datetime.now(UTC).isoformat()
    report: dict[str, Any] = {
        "started_at_utc": stamp,
        "classification": "Synthetic",
        "mode": "stand_in",
        "sources": snapshot(),
        "journal_sha256": None,
        "python_version": sys.version.split()[0],
        "seed": 0,
        "cases": [],
        "defects": [],
        "limits": ["AWS, ECS, S3, native Polaris and Spark are not exercised", "stand-in data effects do not prove native load convergence"],
        "command": [sys.executable, "-B", "-m", "scripts.lakehouse.run_aws_bronze_writer_e2e", *sys.argv[1:]],
        "cleanup": "done",
    }
    try:
        report["journal_sha256"] = hashlib.sha256(args.journal_module.read_bytes()).hexdigest()
        subject = importlib.import_module("scripts.lakehouse.bronze_writer")
        journal = load_module(args.journal_module, "bronze_qualification_journal_dependency")
        report["cases"] = cases(subject, journal, args.out / "cases", deadline)
        if args.plant_defects and all(row["status"] == "passed" for row in report["cases"]):
            report["defects"] = defects(journal, args.out / "defects", deadline)
            report["defects"].extend(planted_storage_defects(args.out / "storage_defects"))
            report["defects"].extend(stage_defects(subject, journal, args.out / "stage_defects", deadline))
        report["source_unchanged"] = snapshot() == report["sources"]
        report["journal_unchanged"] = hashlib.sha256(args.journal_module.read_bytes()).hexdigest() == report["journal_sha256"]
        report["passed"] = (
            report["source_unchanged"]
            and report["journal_unchanged"]
            and len(report["cases"]) == len(bronze_writer_cases.CASES)
            and all(row["status"] == "passed" for row in report["cases"])
            and all(row["status"] == "caught" for row in report["defects"])
            and (not args.plant_defects or len(report["defects"]) == len(DEFECTS) + 3 + len(STAGE_DEFECTS))
        )
    except (ImportError, OSError, ValueError, RuntimeError, TimeoutError) as error:
        report.update(passed=False, cause=str(error))
    finally:
        (args.out / "retention.json").write_text(
            json.dumps(
                {"class": 4, "purpose": "Bronze writer author verification", "created": stamp, "end_condition": "No review or handoff cites this run"}, indent=2
            )
            + "\n"
        )
        (args.out / "report.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    sys.stdout.write(json.dumps({"passed": report["passed"], "report": str(args.out / "report.json")}) + "\n")
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
