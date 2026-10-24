"""Plant bounded recovery defects in isolated source copies and run the original three scenarios."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.process import TimeoutExpired, run_command

REPO = Path(__file__).resolve().parents[2]
SUBJECT = REPO / "scripts/lakehouse/catalog_journal.py"
MUTATIONS = {
    "ignore_pending_intents": [("pending = sorted(set(intents) - confirmed_intents)", "pending = []")],
    "accept_restored_source_kind": [
        (' and observation["kind"] == "live"', ""),
        ('source["kind"] != "live"', "False"),
    ],
    "accept_replacement_database_identity": [
        (' and observation["database_id"] == intent["database_id"]', ""),
        ('source["database_id"] != intent["database_id"]', "False"),
    ],
    "merge_deleted_tables": [
        (
            "states[name] = state",
            'states[name] = {**state, "tables": {**states.get(name, {}).get("tables", {}), **state["tables"]}} if state["state"] == "present" else state',
        )
    ],
    "lose_namespace_properties": [
        (
            'rebuilt[name] = {"state": "present", "properties": state["properties"], "tables": {}}',
            'rebuilt[name] = {"state": "present", "properties": {}, "tables": {}}',
        )
    ],
    "omit_empty_namespaces": [('if state["state"] == "present"}', 'if state["state"] == "present" and state["tables"]}')],
    "ignore_extra_namespace": [
        (
            "extra, missing = sorted(set(actual) - set(expected)), sorted(set(expected) - set(actual))",
            "extra, missing = [], sorted(set(expected) - set(actual))",
        )
    ],
    "ignore_table_membership": [("if actual[name] != expected[name]", 'if any(actual[name][key] != expected[name][key] for key in ("state", "properties"))')],
    "purge_old_metadata": [
        (
            'replace_document(self.root / "rebuilt.json", rebuilt)',
            '(self.root / "metadata" / "seed_heads.json").unlink(missing_ok=True)\n        replace_document(self.root / "rebuilt.json", rebuilt)',
        )
    ],
}

for name, filename in (("infer_metadata_head", "metadata/999999.metadata.json"), ("infer_backup_head", "publish_backup.json")):
    MUTATIONS[name] = [
        (
            'replace_document(self.root / "rebuilt.json", rebuilt)',
            f'if (self.root / "{filename}").exists():\n'
            '            for name in result["unresolved"]:\n'
            f'                rebuilt[name] = {{"state": "present", "properties": {{}}, "tables": {{"guessed": read_document(self.root / "{filename}")}}}}\n'
            '        replace_document(self.root / "rebuilt.json", rebuilt)',
        )
    ]
MUTATIONS["validate_fault_after_effects"] = [
    (
        '        fault = request.get("fault")\n'
        '        if fault not in {None, "metadata_before_commit", "kill_after_commit", "database_before_hook", "before_hook"}:\n'
        '            raise ValueError("unknown stand-in interruption point")\n',
        "",
    ),
    (
        '        if fault != "metadata_before_commit":',
        '        fault = request.get("fault")\n'
        '        if fault not in {None, "metadata_before_commit", "kill_after_commit", "database_before_hook", "before_hook"}:\n'
        '            raise ValueError("unknown stand-in interruption point")\n'
        '        if fault != "metadata_before_commit":',
    ),
]
MUTATIONS["register_wrong_head"] = [('rebuilt[name]["tables"][table] = head', 'rebuilt[name]["tables"][table] = {**head, "snapshot_id": -1}')]

EXPECTED_FAILURE = {
    "ignore_pending_intents": ("intent_without_completion", "kill_after_commit: unresolved despite older confirmation"),
    "accept_restored_source_kind": ("intent_without_completion", "kill_after_commit: older dump cannot confirm"),
    "accept_replacement_database_identity": ("intent_without_completion", "replacement live database cannot confirm"),
    "infer_metadata_head": ("database_loss", "no file or backup head is inferred"),
    "infer_backup_head": ("database_loss", "no file or backup head is inferred"),
    "validate_fault_after_effects": ("namespace_replay", "invalid fault leaves every persisted file unchanged"),
    "register_wrong_head": ("namespace_replay", "normal replay has complete parity"),
    "merge_deleted_tables": ("namespace_replay", "newest namespace state replaces earlier membership"),
    "lose_namespace_properties": ("namespace_replay", "newest namespace state replaces earlier membership"),
    "omit_empty_namespaces": ("namespace_replay", "empty namespace exists"),
    "ignore_extra_namespace": ("namespace_replay", "extra namespace fails parity"),
    "ignore_table_membership": ("namespace_replay", "extra table fails parity"),
    "purge_old_metadata": ("namespace_replay", "non-purging deletion preserves original metadata"),
}


def main() -> int:
    """Save each exact mutation, its failing cases and cleanup evidence within one 120-second budget."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    original = SUBJECT.read_text(encoding="utf-8")
    digest = hashlib.sha256(original.encode()).hexdigest()
    deadline = time.monotonic() + 120
    results: list[dict[str, Any]] = []
    for name, replacements in MUTATIONS.items():
        item: dict[str, Any] = {"defect": name, "caught": False, "changes": replacements}
        results.append(item)
        try:
            changed = original
            for old, new in replacements:
                if changed.count(old) != 1:
                    raise ValueError(f"mutation {name} expected one source match")
                changed = changed.replace(old, new, 1)
            root = output / name / "subject"
            target = root / "scripts/lakehouse/catalog_journal.py"
            target.parent.mkdir(parents=True)
            target.write_text(changed, encoding="utf-8")
            for action in ("format", "check"):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("recovery mutation run passed its 120-second deadline")
                check = run_command(sys.executable, ["-m", "ruff", action, str(target)], cwd=REPO, timeout=min(10, remaining))
                if check.returncode:
                    raise ValueError(f"mutant {action} failed: {check.stdout}{check.stderr}")
            identity = run_command(
                sys.executable,
                ["-c", "import sys; import scripts.lakehouse.catalog_journal as subject; sys.stdout.write(subject.__file__)"],
                cwd=root,
                timeout=min(10, max(0.001, deadline - time.monotonic())),
            )
            if identity.returncode or identity.stdout != str(target):
                raise ValueError("the mutant source was not the imported subject")
            artifact = output / name / "run"
            command = ["-m", "scripts.lakehouse.run_catalog_journal_e2e", "--subject-root", str(root), "--output", str(artifact)]
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("recovery mutation run passed its 120-second deadline")
            result = run_command(sys.executable, command, cwd=REPO, timeout=min(60, remaining))
            report = json.loads((artifact / "report.json").read_text())
            failed_cases = [
                {"case": case["case"], "failed_assertions": [value["label"] for value in case["assertions"] if not value["passed"]], "error": case["error"]}
                for case in report["cases"]
                if not case["passed"]
            ]
            setup_ok = all(case["commands"] and case["commands"][0]["exit_code"] == 0 for case in report["cases"])
            expected_case, expected_failure = EXPECTED_FAILURE[name]
            intended_failure = any(
                case["case"] == expected_case and case["error"] is None and expected_failure in case["failed_assertions"] for case in failed_cases
            )
            item.update(
                {
                    "command": [sys.executable, *command],
                    "exit_code": result.returncode,
                    "failed_cases": failed_cases,
                    "subject_reached": setup_ok,
                    "intended_failure": intended_failure,
                    "caught": setup_ok and result.returncode == 1 and intended_failure,
                    "artifact": str(artifact / "report.json"),
                }
            )
        except (OSError, ValueError, KeyError, TimeoutExpired) as error:
            item["error"] = f"{type(error).__name__}: {error}"
            break
    unchanged = hashlib.sha256(SUBJECT.read_bytes()).hexdigest() == digest
    report = {
        "timestamp": datetime.now(UTC).isoformat(),
        "command": [sys.executable, *sys.argv],
        "source_sha256": digest,
        "source_unchanged": unchanged,
        "mutations": results,
        "passed": unchanged and len(results) == len(MUTATIONS) and all(item["caught"] for item in results),
        "limits": "File-backed recovery cases only. No cloud, native Polaris or launcher integration.",
        "cleanup": "done: all synchronous child commands finished; isolated mutant sources retained as run evidence",
    }
    (output / "report.json").write_text(json.dumps(report, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    sys.stdout.write(json.dumps({"passed": report["passed"], "caught": sum(item["caught"] for item in results), "report": str(output / "report.json")}) + "\n")
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
