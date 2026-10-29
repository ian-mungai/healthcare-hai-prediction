"""Plant migration defects in isolated source files and verify that the original cases detect them."""

from __future__ import annotations

import argparse
import ast
import hashlib
import importlib.util
import json
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import patch

from scripts.lakehouse import migrate_aws_catalog as migration
from scripts.lakehouse.catalog_migration_cases import MIGRATION_CASES, Scenario
from scripts.lakehouse.run_catalog_migration_e2e import fingerprints
from scripts.process import TimeoutExpired, run_command

ROOT = Path(__file__).resolve().parents[2]
SUBJECT = Path(migration.__file__).resolve()
OBJECT_READ = "metadata, identity, stat = object_metadata(objects, location, None, deadline=deadline, clock=clock)"
REBUILD_ORDER = """    for namespace, value in sorted(namespace_map(expected).items(), key=lambda item: (len(item[0]), item[0])):
        ensure_namespace(io, namespace, value, deadline=deadline)
    for row in expected["tables"]:
        register_row(io, row, None, deadline=deadline)"""
MUTATIONS: dict[str, dict[str, Any]] = {
    "stop_paging": {
        "case": "migration_inventory",
        "replace": [("counts[label] += 1\n            token = page.get", "counts[label] += 1\n            return values\n            token = page.get")],
        "error": "coverage",
    },
    "miss_nested_namespace": {
        "case": "migration_inventory",
        "replace": [("pending.append(namespace)", "if len(namespace) > 1:\n                pending.append(namespace)")],
        "error": "coverage",
    },
    "copy_source_catalog_reading": {
        "case": "migration_four_readings",
        "replace": [
            (
                'catalog_reading = reading(namespace, name, location, loaded.get("metadata"))',
                'catalog_reading = reading(namespace, name, location, metadata if not isinstance(catalog, DestinationCatalog) else loaded.get("metadata"))',
            )
        ],
        "label": "source_catalog:table-uuid: rejection",
    },
    "copy_destination_catalog_reading": {
        "case": "migration_four_readings",
        "replace": [
            (
                'catalog_reading = reading(namespace, name, location, loaded.get("metadata"))',
                'catalog_reading = reading(namespace, name, location, metadata if isinstance(catalog, DestinationCatalog) else loaded.get("metadata"))',
            )
        ],
        "label": "destination_catalog:table-uuid: rejection",
    },
    "register_before_namespace": {
        "case": "migration_rebuild",
        "replace": [
            (
                REBUILD_ORDER,
                """    for row in expected["tables"]:
        register_row(io, row, None, deadline=deadline)
    for namespace, value in sorted(namespace_map(expected).items(), key=lambda item: (len(item[0]), item[0])):
        ensure_namespace(io, namespace, value, deadline=deadline)""",
            )
        ],
        "error": "catalog_http",
    },
    "ignore_pending_intent": {
        "case": "migration_rebuild",
        "replace": [
            (
                "replayed = confirmed_replay(frozen, intents, completions, epoch=epoch, quiesced=quiesced)",
                "replayed = confirmed_replay(frozen, {name: value for name, value in intents.items() "
                'if any(record["intent_id"] == name and record["status"] == "confirmed" for record in completions)}, '
                "completions, epoch=epoch, quiesced=quiesced)",
            )
        ],
        "label": "pending intent is named",
    },
}

for role in ("source", "destination"):
    condition = "not isinstance(catalog, DestinationCatalog)" if role == "source" else "isinstance(catalog, DestinationCatalog)"
    MUTATIONS[f"copy_{role}_object_reading"] = {
        "case": "migration_four_readings",
        "replace": [
            (
                OBJECT_READ,
                f"""if {condition}:
        metadata = loaded["metadata"]
        stat = objects.stat(location, None, deadline=deadline)
        identity = {{"version_id": stat.version_id, "size": stat.size, "etag": stat.etag,
                    "sha256": hashlib.sha256(registration.canonical(metadata)).hexdigest()}}
    else:
        {OBJECT_READ}""",
            )
        ],
        "label": f"{role}_object:table-uuid: rejection",
    }

for action in ("drop_table", "drop_namespace", "namespace_properties"):
    MUTATIONS["ignore_pending_" + action] = {
        "case": "migration_reverse_retry",
        "function": "reverse_refresh",
        "replace": [(f'if entry["action"] == "{action}"', f'if entry["action"] == "skip_{action}"')],
        "label": action + ": retry confirms the original intent",
    }

MUTATIONS.update(
    {
        "ignore_changed_frozen_read": {
            "case": "migration_frozen_change",
            "function": "freeze",
            "replace": [("if first != second:", "if False:")],
            "label": "unequal frozen reads are refused",
        },
        "compare_final_schema_only": {
            "case": "migration_final_property",
            "function": "final_read",
            "replace": [("if actual != expected:", 'if actual.get("schema") != expected.get("schema"):')],
            "label": "the final comparison refuses a property change",
        },
        "compare_parity_intersection_only": {
            "case": "migration_parity_missing",
            "function": "parity",
            "replace": [
                ("membership(actual, expected, complete=True)", "membership(actual, expected, complete=False)"),
                (
                    "        target = tables[table_identifier(row)]",
                    "        if table_identifier(row) not in tables:\n            continue\n        target = tables[table_identifier(row)]",
                ),
            ],
            "label": "empty_namespace: complete parity refuses missing membership",
        },
        "allow_writable_source": {
            "case": "migration_source_type",
            "replace": [("if type(self.source) is not GetOnlyCatalog:", "if False:")],
            "label": "a writable catalog is refused as the source",
        },
        "allow_source_post": {
            "case": "migration_source_method",
            "replace": [('if method != "GET":', 'if method not in {"GET", "POST"}:')],
            "error": "assertion",
        },
        "register_unchanged_without_intent": {
            "case": "migration_unchanged_disappears",
            "function": "refresh",
            "replace": [
                (
                    """registration.check_loaded,
                    io.destination.load(identifier, deadline=deadline),
                    pin_for(row),
                    metadata_at(io, row, deadline=deadline),
                    "refresh_conflict",""",
                    """registration.register_exact_metadata,
                    io.destination.scoped(namespace), io.source_objects, pin_for(row),
                    bucket=urlsplit(io.scope.table_root).netloc, deadline=deadline, clock=io.clock,""",
                )
            ],
            "label": "an unchanged table that disappears is refused",
        },
        "ignore_configured_branch_prefixes": {
            "case": "migration_branch_prefix",
            "function": "capture_table",
            "replace": [("branch.startswith(scope.publish_branch_prefixes)", 'branch.startswith(("publish_", "silver_publish_"))')],
            "label": "configured publication prefixes are enforced",
        },
        "constant_frozen_separation": {
            "case": "migration_freeze_timing",
            "function": "freeze",
            "replace": [('"separation_seconds": separation', '"separation_seconds": 120.0')],
            "label": "the frozen separation is measured",
        },
        "constant_command_timestamp": {
            "case": "migration_command_timing",
            "replace": [("self.clock() - self.anchor_clock", "0.0")],
            "label": "a command end records elapsed clock time",
        },
        "accept_integer_quiescence": {
            "case": "migration_replay_types",
            "function": "validate_replay_inputs",
            "replace": [("type(record[field]) is not bool", "type(bool(record[field])) is not bool")],
            "label": "quiesced_type: invalid journal input is refused",
        },
        "hide_unexpected_runner_error": {
            "case": "migration_runner_error",
            "subject": "scripts/lakehouse/run_catalog_migration_e2e.py",
            "function": "main",
            "replace": [
                ("except (OSError, RuntimeError, AssertionError, TimeoutExpired)", "except (OSError, RuntimeError, AssertionError, KeyError, TimeoutExpired)")
            ],
            "label": "the runner propagates an unexpected programming error",
        },
    }
)

MUTATIONS.update(
    {
        "remove_minimum_freeze_interval": {
            "case": "migration_short_freeze",
            "function": "freeze",
            "replace": [("if separation < 120:", "if False:")],
            "label": "an early wait cannot freeze the source",
        },
        "remove_rebuild_membership_precheck": {
            "case": "migration_rebuild_membership",
            "function": "rebuild",
            "replace": [("    membership(actual, expected, complete=False)\n", "")],
            "label": "the rebuild precheck prevents every destination mutation",
        },
        "remove_complete_final_rebuild_check": {
            "case": "migration_rebuild_membership",
            "function": "rebuild",
            "replace": [("membership(actual, expected, complete=True)", "membership(actual, expected, complete=False)")],
            "label": "complete final rebuild membership is required",
        },
    }
)

for operation in ("refresh", "rebuild"):
    MUTATIONS[operation + "_child_before_parent"] = {
        "case": "migration_" + operation + "_hierarchy",
        "function": operation,
        "replace": [("key=lambda item: (len(item[0]), item[0])", "key=lambda item: (-len(item[0]), item[0])")],
        "error": "catalog_http",
    }


def mutate(source: str, mutation: dict[str, Any]) -> str:
    lines = source.splitlines(keepends=True)
    start, end = 0, len(lines)
    if "function" in mutation:
        selected = [node for node in ast.parse(source).body if isinstance(node, ast.FunctionDef) and node.name == mutation["function"]]
        if len(selected) != 1 or selected[0].end_lineno is None:
            raise ValueError("the mutation must select exactly one complete function")
        start, end = selected[0].lineno - 1, selected[0].end_lineno
    changed = "".join(lines[start:end])
    for old, new in mutation["replace"]:
        if changed.count(old) != 1:
            raise ValueError("the mutation does not match exactly one source location")
        changed = changed.replace(old, new, 1)
    return "".join(lines[:start]) + changed + "".join(lines[end:])


def exercise(path: Path, case: Scenario, name: str, subject: Path) -> str | None:
    import scripts.lakehouse as package

    module_name = "scripts.lakehouse." + subject.stem
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise ValueError("the isolated mutation cannot be loaded")
    module = importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules, {module_name: module}), patch.object(package, subject.stem, module):
        spec.loader.exec_module(module)
        if hasattr(module, "ROOT"):
            module.__dict__["ROOT"] = ROOT
        try:
            MIGRATION_CASES[name](case)
        except RuntimeError as error:
            return getattr(error, "code", None)
        except AssertionError:
            return "assertion"
    return None


def main() -> int:
    """Run passing baselines and isolated defects within one 120-second budget."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    source = SUBJECT.read_text(encoding="utf-8")
    before = hashlib.sha256(source.encode()).hexdigest()
    hashes = fingerprints()
    started = time.monotonic()
    results = []
    baselines = []
    baseline_names = list(dict.fromkeys(item["case"] for item in MUTATIONS.values()))
    try:
        for name in baseline_names:
            case = Scenario(args.output / "baseline" / name, ROOT, started + 120)
            MIGRATION_CASES[name](case)
            baselines.append({"case": name, "passed": bool(case.assertions) and all(item["passed"] for item in case.assertions)})
        for name, mutation in MUTATIONS.items():
            item: dict[str, Any] = {"defect": name, "case": mutation["case"], "caught": False, "replacements": mutation["replace"]}
            results.append(item)
            case = Scenario(args.output / name / "case", ROOT, started + 120)
            try:
                subject = ROOT / mutation.get("subject", "scripts/lakehouse/migrate_aws_catalog.py")
                changed = mutate(subject.read_text(encoding="utf-8"), mutation)
                path = args.output / name / "subject.py"
                path.write_text(changed, encoding="utf-8")
                for action in ("format", "check"):
                    remaining = started + 120 - time.monotonic()
                    if remaining <= 0:
                        raise TimeoutError("the migration mutation run exceeded 120 seconds")
                    result = run_command(sys.executable, ["-m", "ruff", action, str(path)], cwd=ROOT, timeout=min(10, remaining))
                    if result.returncode:
                        raise ValueError("the mutated source did not pass Ruff: " + result.stdout + result.stderr)
                item["subject"], item["mutant_sha256"] = str(subject.relative_to(ROOT)), hashlib.sha256(path.read_bytes()).hexdigest()
                error_code = exercise(path, case, mutation["case"], subject)
                failed = [assertion for assertion in case.assertions if not assertion["passed"]]
                item["error_code"], item["failed_assertions"] = error_code, failed
                item["caught"] = (
                    (error_code == mutation["error"]) if "error" in mutation else any(assertion["label"] == mutation["label"] for assertion in failed)
                )
            except (OSError, ValueError, AssertionError, TimeoutExpired) as error:
                item["unexpected_error"] = f"{type(error).__name__}: {error}"
            finally:
                if sys.exception() is not None:
                    item["unexpected_error"] = f"unhandled {type(sys.exception()).__name__}"
    finally:
        unchanged = hashlib.sha256(SUBJECT.read_bytes()).hexdigest() == before and hashes == fingerprints()
        report = {
            "timestamp": datetime.now(UTC).isoformat(),
            "feature": "synthetic catalog migration planted defects",
            "subject_sha256": before,
            "subject_hashes": hashes,
            "subject_unchanged": unchanged,
            "interrupted_by": type(sys.exception()).__name__ if sys.exception() is not None else None,
            "command": [sys.executable, "-m", "scripts.lakehouse.run_catalog_migration_defects", *sys.argv[1:]],
            "environment": sys.version,
            "baseline": baselines,
            "mutations": results,
            "passed": unchanged
            and len(baselines) == len(baseline_names)
            and len(results) == len(MUTATIONS)
            and all(item["passed"] for item in baselines)
            and all(item["caught"] for item in results),
            "elapsed_seconds": time.monotonic() - started,
            "limits": ["Synthetic REST and versioned objects only", "No network, AWS, Docker or native catalog proof"],
            "cleanup": "done: original module restored and synchronous Ruff children finished",
        }
        (args.output / "retention.json").write_text(
            json.dumps(
                {
                    "class": 5,
                    "purpose": "catalog migration mutation evidence",
                    "created": report["timestamp"],
                    "end_condition": "A newer accepted run replaces it",
                }
            )
            + "\n",
            encoding="utf-8",
        )
        (args.output / "report.json").write_text(json.dumps(report, sort_keys=True, indent=2) + "\n", encoding="utf-8")
        sys.stdout.write(json.dumps({"passed": report["passed"], "mutations": len(results), "report": str(args.output / "report.json")}) + "\n")
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
