"""Plant authority and pending-intent defects in isolated source copies and retain their failing cases."""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.lakehouse.catalog_authority_cases import EDGES, STATES, fingerprints
from scripts.process import TimeoutExpired, run_command

REPO = Path(__file__).resolve().parents[2]
AUTHORITY = "orchestration/catalog_authority.py"
JOURNAL = "scripts/lakehouse/catalog_journal.py"
COPIED = (
    AUTHORITY,
    JOURNAL,
    "orchestration/__init__.py",
    "scripts/lakehouse/__init__.py",
    "scripts/lakehouse/catalog_authority_cases.py",
    "scripts/process.py",
)


@dataclass(frozen=True)
class Defect:
    name: str
    changes: dict[str, list[tuple[str, str]]]
    case: str
    assertion: str
    source: str | None = None


def defects() -> list[Defect]:
    guard = 'if target not in TRANSITIONS[current["state"]]:'
    result = []
    for source in STATES:
        for target in STATES:
            allowed = target in EDGES[source]
            condition = f'(current["state"] == "{source}" and target == "{target}")'
            replacement = (
                f'if {condition} or target not in TRANSITIONS[current["state"]]:'
                if allowed
                else f'if target not in TRANSITIONS[current["state"]] and not {condition}:'
            )
            if source == "rollback_pending" and target == "mac_writer":
                old = 'if current["state"] == "rollback_pending" and target == "mac_writer" and current["phase"] != "P1":'
                replacement = 'if current["state"] == "rollback_pending" and target == "mac_writer" and current["phase"] == "P5":'
            else:
                old = guard
            result.append(
                Defect(
                    f"{'block' if allowed else 'allow'}_{source}_to_{target}",
                    {AUTHORITY: [(old, replacement)]},
                    "transition_graph",
                    f"transition {source} to {target}",
                    source,
                )
            )
    result.extend(
        [
            Defect(
                "block_early_rollback",
                {
                    AUTHORITY: [
                        (guard, 'if (current["state"] == "rollback_pending" and target == "mac_writer") or target not in TRANSITIONS[current["state"]]:')
                    ]
                },
                "phase_and_rollback",
                "owner: exit",
            ),
            Defect(
                "allow_rollback_after_admission",
                {
                    AUTHORITY: [
                        (
                            'if target == "rollback_pending" and current["phase"] == "P5":',
                            'if target == "rollback_pending" and current["phase"] == "P1" and current["state"] == "aws_writer":',
                        )
                    ]
                },
                "phase_and_rollback",
                "owner: intended rejection transition cannot roll back an admitted writer",
            ),
            Defect(
                "omit_admitted_phase",
                {AUTHORITY: [('state["launcher"]["catalog_authority"]["phase"] = "P5"', 'state["launcher"]["catalog_authority"]["phase"] = "P4"')]},
                "phase_and_rollback",
                "first admitted test try persists final phase",
            ),
            Defect("keep_epoch_equal", {AUTHORITY: [("epoch=expected + 1", "epoch=max(1, expected)")]}, "epoch_and_idempotence", "initial freeze grows epoch"),
            Defect(
                "accept_stale_owner_epoch",
                {AUTHORITY: [('if expected != (current["epoch"] if current else 0):', "if expected < 0:")]},
                "epoch_and_idempotence",
                "owner: intended rejection stale catalog epoch",
            ),
            Defect(
                "accept_boolean_epoch",
                {AUTHORITY: [("type(value) is not int", "not isinstance(value, int)")]},
                "epoch_and_idempotence",
                "owner: intended rejection catalog epoch must be an integer",
            ),
            Defect(
                "accept_stale_writer_epoch",
                {AUTHORITY: [('if epoch_number(epoch) != authority["epoch"]:', "if epoch_number(epoch) < 0:")]},
                "layer_and_admission",
                "admit: intended rejection stale catalog epoch",
            ),
            Defect(
                "accept_changed_command_identity",
                {AUTHORITY: [("if receipts[command_id] != digest or current is None:", "if current is None:")]},
                "epoch_and_idempotence",
                "owner: intended rejection command identity conflict",
            ),
            Defect(
                "ignore_owner_namespace_scope",
                {AUTHORITY: [('or names.intersection(recovery["unresolved"])', "or names.intersection(())")]},
                "layer_and_admission",
                "set_layer: intended rejection unresolved catalog namespaces",
            ),
            Defect(
                "omit_owner_check",
                {AUTHORITY: [("if owner is not True:", "if owner is None:")]},
                "owner_confirmation",
                "owner: intended rejection authenticated owner",
            ),
            Defect(
                "omit_terminal_check",
                {AUTHORITY: [("if tty is not True:", "if tty is None:")]},
                "owner_confirmation",
                "owner: intended rejection terminal confirmation",
            ),
            Defect(
                "omit_typed_confirmation",
                {AUTHORITY: [("if confirmation != confirmation_text(command):", "if confirmation is None:")]},
                "owner_confirmation",
                "owner: intended rejection typed confirmation",
            ),
            Defect(
                "omit_new_intent_file_sync",
                {JOURNAL: [("            stream.flush()\n            os.fsync(stream.fileno())", "            stream.flush()")]},
                "durability_order",
                "none: both intent syncs precede authority change",
            ),
            Defect(
                "omit_retry_intent_file_sync",
                {JOURNAL: [("            os.fsync(descriptor)\n        finally:", "            os.fstat(descriptor)\n        finally:")]},
                "durability_order",
                "file: both intent syncs precede authority change",
            ),
            Defect(
                "omit_intent_directory_sync",
                {JOURNAL: [("    sync_directory(path.parent)\n\n\ndef replace_document", "\n\n\ndef replace_document")]},
                "durability_order",
                "none: both intent syncs precede authority change",
            ),
            Defect(
                "change_authority_before_intent",
                {
                    AUTHORITY: [
                        (
                            '    write_once(intent_directory / f"{command_id}.json", '
                            '{"command": command, "before_epoch": current["epoch"] if current else 0, "after": record})\n'
                            '    launcher["catalog_authority"] = record',
                            '    launcher["catalog_authority"] = record\n'
                            '    write_once(intent_directory / f"{command_id}.json", '
                            '{"command": command, "before_epoch": current["epoch"] if current else 0, "after": record})',
                        )
                    ]
                },
                "durability_order",
                "none: state unchanged during intent sync",
            ),
            Defect(
                "ignore_pending_intents",
                {JOURNAL: [("pending = sorted(set(intents) - confirmed_intents)", "pending = []")]},
                "owner_pending_recovery",
                "owner pending blocks all namespaces despite older completion",
            ),
            Defect(
                "use_inventory_for_pending_namespace",
                {JOURNAL: [('for name in intents[intent_id]["namespaces"]}', 'for name in intents[intent_id]["namespaces"] if name not in states}')]},
                "owner_pending_recovery",
                "owner pending blocks all namespaces despite older completion",
            ),
            Defect(
                "ignore_latest_unconfirmed_record",
                {JOURNAL: [('unconfirmed.update(intent["namespaces"])', 'unconfirmed.difference_update(intent["namespaces"])')]},
                "recovery_source_order",
                "latest unconfirmed observation blocks an already confirmed intent",
            ),
            Defect(
                "merge_deleted_table_membership",
                {JOURNAL: [("states[name] = state", 'states[name] = {**state, "tables": {**states.get(name, {}).get("tables", {}), **state["tables"]}}')]},
                "recovery_source_order",
                "newest complete namespace replaces deleted membership",
            ),
            Defect(
                "sort_epochs_as_text",
                {JOURNAL: [("key=lambda item: int(item.epoch) if item.epoch.isdecimal() else 0", "key=lambda item: item.epoch")]},
                "recovery_source_order",
                "recover: exit",
            ),
            Defect(
                "ignore_sequence_order",
                {
                    JOURNAL: [
                        (
                            'if type(record["sequence"]) is not int or record["sequence"] != sequences.get(epoch, 0) + 1:',
                            'if type(record["sequence"]) is not int:',
                        )
                    ]
                },
                "recovery_source_order",
                "replay_fault: intended rejection journal",
            ),
            Defect(
                "accept_partial_completion",
                {
                    JOURNAL: [
                        ('if set(states) != set(intent["namespaces"]):', 'if set(states) - set(intent["namespaces"]):'),
                        ('or set(record["namespaces"]) != set(intent["namespaces"])', 'or set(record["namespaces"]) - set(intent["namespaces"])'),
                    ]
                },
                "owner_pending_recovery",
                "complete: intended rejection completion must observe every namespace",
            ),
        ]
    )
    cutover_guard = 'if not starts_freeze and command.get("cutover_id") != current["cutover_id"]:'
    clear_guard = 'if current["state"] == "recovering" and target == "aws_writer" or current["state"] == "reverse_fenced" and target == "mac_writer":'
    result.extend(
        [
            Defect(
                "allow_layer_cutover_overwrite",
                {AUTHORITY: [(cutover_guard, cutover_guard[:-1] + ' and action != "set_layer":')]},
                "layer_and_admission",
                "set_layer: intended rejection another cutover",
            ),
            Defect(
                "allow_transition_cutover_overwrite",
                {AUTHORITY: [(cutover_guard, cutover_guard[:-1] + ' and action != "transition":')]},
                "epoch_and_idempotence",
                "owner: intended rejection another cutover",
            ),
            Defect(
                "reuse_freeze_identity",
                {AUTHORITY: [('if command.get("cutover_id") == current["cutover_id"]:', 'if command.get("cutover_id") is None:')]},
                "epoch_and_idempotence",
                "owner: intended rejection a new freeze requires a new cutover identity",
            ),
            Defect(
                "ignore_writer_journal_epoch",
                {AUTHORITY: [("if journal.epoch != str(epoch):", 'if journal.epoch == "unbound":')]},
                "layer_and_admission",
                "admit: intended rejection writer journal does not match the catalog epoch",
            ),
            Defect(
                "allow_layer_change_outside_aws",
                {AUTHORITY: [('current["state"] != "aws_writer" or ', "")]},
                "layer_and_admission",
                "set_layer: intended rejection layer activation requires AWS authority",
            ),
            Defect(
                "ignore_record_phase",
                {AUTHORITY: [('if value["phase"] not in PHASES[state]:', 'if value["phase"] is None:')]},
                "record_boundary",
                "otherwise valid record rejects incompatible phase",
            ),
            Defect(
                "ignore_global_pending_recovery_exit",
                {AUTHORITY: [('recovery["unresolved"] or recovery["pending_intents"]', 'recovery["unresolved"]')]},
                "owner_intent_lifecycle",
                "owner: intended rejection unresolved catalog namespaces",
            ),
            Defect(
                "omit_global_pending_layer",
                {JOURNAL: [(' | {intents[intent_id]["layer"] for intent_id in pending}', "")]},
                "owner_intent_lifecycle",
                "global pending intent blocks its declared layer",
            ),
            Defect(
                "allow_backward_owner_time",
                {
                    AUTHORITY: [
                        (
                            'if current is not None and datetime.fromisoformat(result["time"]) < datetime.fromisoformat(current["time"]):',
                            'if current is not None and result["time"] == "unavailable":',
                        )
                    ]
                },
                "owner_command_time",
                "owner: intended rejection owner command time precedes",
            ),
            Defect(
                "require_clear_before_reverse",
                {
                    AUTHORITY: [
                        (
                            clear_guard,
                            'if current["state"] == "recovering" and target in ("aws_writer", "reverse_fenced") '
                            'or current["state"] == "reverse_fenced" and target == "mac_writer":',
                        )
                    ]
                },
                "reverse_pending_intents",
                "owner: exit",
            ),
            Defect(
                "reopen_mac_with_pending_reverse",
                {AUTHORITY: [(clear_guard, 'if current["state"] == "recovering" and target == "aws_writer":')]},
                "reverse_pending_intents",
                "owner: intended rejection unresolved catalog namespaces",
            ),
            Defect(
                "ignore_record_version",
                {AUTHORITY: [('value["version"] != 1', 'value["version"] < 1')]},
                "record_boundary",
                "record version has its intended rejection",
            ),
            Defect(
                "ignore_record_layer_membership",
                {AUTHORITY: [("set(layers) != set(LAYERS)", "set(layers) - set(LAYERS)")]},
                "record_boundary",
                "record layer_membership has its intended rejection",
            ),
            Defect(
                "allow_invalid_record_layer_value",
                {AUTHORITY: [('any(item not in ("off", "on") for item in layers.values())', "any(item is None for item in layers.values())")]},
                "record_boundary",
                "record layer_value has its intended rejection",
            ),
            Defect(
                "allow_mac_record_activation",
                {AUTHORITY: [('if state == "mac_writer" and any(item != "off" for item in layers.values()):', 'if state == "mac_writer" and not layers:')]},
                "record_boundary",
                "record mac_activation has its intended rejection",
            ),
            Defect(
                "allow_empty_record_identity",
                {AUTHORITY: [("if not isinstance(value[key], str) or not value[key].strip():", "if not isinstance(value[key], str):")]},
                "record_boundary",
                "record reason has its intended rejection",
            ),
            Defect(
                "accept_non_utc_record_time",
                {AUTHORITY: [("if observed.utcoffset() != timedelta(0):", "if observed.utcoffset() == timedelta(hours=25):")]},
                "record_boundary",
                "record time_zone has its intended rejection",
            ),
            Defect("ignore_record_target", {AUTHORITY: [('value["target"] != target or ', "")]}, "record_boundary", "record target has its intended rejection"),
            Defect(
                "ignore_record_frozen_flag",
                {AUTHORITY: [(' or value["frozen"] != (target is None)', "")]},
                "record_boundary",
                "record frozen has its intended rejection",
            ),
        ]
    )
    for name, writer, reader, index in (
        ("terminal", "terminal and ", '                or not record["terminal"]\n', 0),
        ("quiesced", "quiesced and ", '                or not record["quiesced"]\n', 1),
        ("available", 'observation["available"] and ', '                or not source["available"]\n', 2),
        ("live_source", ' and observation["kind"] == "live"', 'source["kind"] != "live"', 3),
        (
            "original_database",
            ' and observation["database_id"] == intent["database_id"]',
            '                or source["database_id"] != intent["database_id"]\n',
            5,
        ),
    ):
        result.append(
            Defect(
                f"confirm_without_{name}",
                {JOURNAL: [(writer, ""), (reader, "False" if name == "live_source" else "")]},
                "confirmation_sources",
                f"source {index} cannot confirm pending owner command",
            )
        )
    return result


def time_left(deadline: float, limit: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("authority defect run exceeded its 900-second deadline")
    return min(remaining, limit)


def main() -> int:
    """Save exact mutated sources, assertion failures and unchanged-source evidence without changing the worktree subject."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--defect", action="append")
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    before = fingerprints(REPO)
    originals = {name: (REPO / name).read_text(encoding="utf-8") for name in (AUTHORITY, JOURNAL)}
    selected = [item for item in defects() if not args.defect or item.name in args.defect]
    if args.defect and set(args.defect) != {item.name for item in selected}:
        parser.error("unknown authority defect")
    deadline = time.monotonic() + 900
    results: list[dict[str, Any]] = []
    for defect in selected:
        item: dict[str, Any] = {
            "defect": defect.name,
            "caught": False,
            "changes": defect.changes,
            "expected_case": defect.case,
            "expected_assertion": defect.assertion,
        }
        results.append(item)
        try:
            subject = output / defect.name / "subject"
            for name in COPIED:
                target = subject / name
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(REPO / name, target)
            for name, changes in defect.changes.items():
                changed = originals[name]
                for old, new in changes:
                    if changed.count(old) != 1:
                        raise ValueError(f"defect {defect.name} requires one exact source match")
                    changed = changed.replace(old, new, 1)
                (subject / name).write_text(changed, encoding="utf-8")
            paths = [str(subject / name) for name in defect.changes]
            for action in ("format", "check"):
                checked = run_command(sys.executable, ["-m", "ruff", action, *paths], cwd=REPO, timeout=time_left(deadline, 10))
                if checked.returncode:
                    raise ValueError(f"mutated source fails Ruff {action}: {checked.stdout}{checked.stderr}")
            identity = run_command(
                sys.executable,
                [
                    "-c",
                    "import json, sys\nfrom orchestration import catalog_authority as a\n"
                    "from scripts.lakehouse import catalog_journal as j\nsys.stdout.write(json.dumps([a.__file__, j.__file__]))",
                ],
                cwd=subject,
                timeout=time_left(deadline, 10),
            )
            if identity.returncode or json.loads(identity.stdout) != [str(subject / AUTHORITY), str(subject / JOURNAL)]:
                raise ValueError("mutated authority and journal were not the imported subjects")
            artifact = output / defect.name / "run"
            argv = ["-m", "scripts.lakehouse.catalog_authority_cases", "--subject-root", str(subject), "--case", defect.case, "--output", str(artifact)]
            if defect.source:
                argv.extend(("--transition-source", defect.source))
            tested = run_command(sys.executable, argv, cwd=subject, timeout=time_left(deadline, 120))
            report = json.loads((artifact / "report.json").read_bytes())
            failed = [value for value in report["cases"][0]["assertions"] if not value["passed"]]
            intended = any(value["label"] == defect.assertion for value in failed)
            if defect.case == "transition_graph" and not intended:
                intended = any(value["label"] == defect.assertion.replace("transition ", "transition rejection ", 1) for value in failed)
            setup_ok = report["cases"][0]["commands"][0]["exit_code"] == 0
            expected_hashes = fingerprints(subject)
            subject_reached = report["source_hashes"] == expected_hashes and report["source_unchanged"] and setup_ok
            item.update(
                command=[sys.executable, *argv],
                exit_code=tested.returncode,
                artifact=str(artifact / "report.json"),
                failed_assertions=failed,
                scenario_error=report["cases"][0]["error"],
                subject_reached=subject_reached,
                intended_failure=intended,
                caught=tested.returncode == 1 and subject_reached and intended,
            )
        except (OSError, RuntimeError, ValueError, KeyError, TimeoutExpired) as error:
            item["error"] = f"{type(error).__name__}: {error}"
        if time.monotonic() >= deadline:
            break
    unchanged = before == fingerprints(REPO)
    timestamp = datetime.now(UTC).isoformat()
    report = {
        "timestamp": timestamp,
        "command": [sys.executable, "-m", "scripts.lakehouse.run_catalog_authority_defects", *sys.argv[1:]],
        "source_hashes": before,
        "source_unchanged": unchanged,
        "mutations": results,
        "passed": unchanged and len(results) == len(selected) and all(item["caught"] for item in results),
        "limits": "Synthetic registry and catalog stand-ins only. No launcher, network, AWS, Docker or native Polaris.",
        "cleanup": "done: all bounded synchronous child commands finished; isolated mutated sources retained as evidence",
    }
    (output / "report.json").write_text(json.dumps(report, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    (output / "retention.json").write_text(
        json.dumps({"class": 5, "purpose": "authority defect proof", "created": timestamp, "end_condition": "A newer accepted run replaces it"}) + "\n",
        encoding="utf-8",
    )
    sys.stdout.write(
        json.dumps(
            {"passed": report["passed"], "caught": sum(item["caught"] for item in results), "total": len(selected), "report": str(output / "report.json")}
        )
        + "\n"
    )
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
