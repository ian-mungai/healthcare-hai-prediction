"""Check the orchestration contracts: schemas, their examples, intended refusals and the fake submission interface.

Run ``.venv/bin/python -m scripts.orchestration.run_contracts_e2e``. Reports stay in data/e2e/orchestration_contracts/.
The report lists the SHA-256 of every contract file, so a package can confirm it codes against the frozen version
(parallel plan section 2, items 3 to 5). No container, Docker or AWS service is used.
"""

from __future__ import annotations

import copy
import hashlib
import json
import sys
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError

from orchestration.fake import CONTRACTS, FakeSubmissions, validator
from orchestration.interface import State, SubmissionRefused, SubmissionRequest, Submissions
from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]
EXAMPLES = CONTRACTS / "examples"
RUN = "20261011T090000Z-0a1b2c3d"
# Each launch specification change the schema must refuse, with the template it mutates.
SPEC_MUTATIONS: list[tuple[str, str, Callable[[dict[str, Any]], None]]] = [
    ("image by tag", "image", lambda spec: spec.update(image="hai-analytics:latest")),
    ("privileged", "privileged", lambda spec: spec.update(privileged=True)),
    ("root user", "user", lambda spec: spec.update(user="0:0")),
    ("added capability", "capabilities", lambda spec: spec.update(capabilities=["SYS_ADMIN"])),
    ("writable repository mount", "mounts", lambda spec: spec["mounts"].append({"source": "repo:dbt", "destination": "/w", "read_only": False})),
    ("mount escaping the repository", "mounts", lambda spec: spec["mounts"].append({"source": "repo:../other", "destination": "/w", "read_only": True})),
    ("absolute host mount", "mounts", lambda spec: spec["mounts"].append({"source": "/Users", "destination": "/w", "read_only": True})),
    ("caller-sourced memory limit", "environment", lambda spec: spec["environment"].append({"name": "JOB_MEMORY_LIMIT", "source": "caller"})),
    ("environment from the caller's shell", "environment", lambda spec: spec["environment"].append({"name": "AWS_PROFILE", "source": "inherit"})),
    ("argument with an unknown substitution", "arguments", lambda spec: spec["arguments"].append("{aws_secret}")),
    ("unknown class", "modes", lambda spec: spec["modes"]["fixture"].update({"class": "host"})),
    ("unknown mode", "modes", lambda spec: spec["modes"].update(production={"class": "offline", "no_op": False})),
    ("extra field", "network_mode", lambda spec: spec.update(network_mode="host")),
]
# Ledger transitions the broker writes for one submission (Airflow plan section 7).
NEXT_EVENTS = {None: {"intent"}, "intent": {"started", "adopted", "finished", "failed_closed"}, "started": {"finished", "adopted", "failed_closed"}}
NEXT_EVENTS["adopted"] = {"finished", "failed_closed"}


def load(name: str) -> Any:
    """One example file."""
    return json.loads((EXAMPLES / name).read_text())


def schemas() -> dict[str, bool]:
    """Every schema is a valid Draft 2020-12 schema."""
    results = {}
    for path in sorted(CONTRACTS.glob("*.schema.json")):
        try:
            Draft202012Validator.check_schema(json.loads(path.read_text()))
            results[f"{path.name} is a valid schema"] = True
        except SchemaError:
            results[f"{path.name} is a valid schema"] = False
    return results


def templates() -> dict[str, bool]:
    """The example inventory validates, keeps the plan's no-op and class rules and refuses every mutation."""
    check = validator("launch_spec.schema.json")
    document = load("job_templates.json")
    inventory = document["templates"]
    expected = {"pin_bronze", "silver_build", "silver_validate", "gold_build", "gold_validate", "cross_layer_checks", "verify_bronze"}
    expected |= {"publish_candidates", "reconcile_candidates", "catalog_backup", "commit_publish", "gold_maintenance", "sense_markers"}
    no_ops = {(name, mode) for name, spec in inventory.items() for mode, setting in spec["modes"].items() if setting["no_op"]}
    writers = {name for name, spec in inventory.items() if any(setting["class"] == "lakehouse_write" for setting in spec["modes"].values())}
    fixture_classes = {spec["modes"]["fixture"]["class"] for spec in inventory.values() if "fixture" in spec["modes"]}
    results = {
        "example inventory validates": not list(check.iter_errors(document)),
        "inventory is exactly the 13 container templates": set(inventory) == expected,
        "fixture no-ops are pin_bronze and verify_bronze only": no_ops == {("pin_bronze", "fixture"), ("verify_bronze", "fixture")},
        "fixture mode is offline everywhere": fixture_classes == {"offline"},
        "only publish, backup, commit and maintenance write": writers == {"publish_candidates", "catalog_backup", "commit_publish", "gold_maintenance"},
        "no writer supports fixture mode": all("fixture" not in inventory[name]["modes"] for name in writers),
        "every writable mount is the run directory": all(
            mount["source"] == "run_dir" for spec in inventory.values() for mount in spec["mounts"] if not mount["read_only"]
        ),
    }
    for name, field, mutate in SPEC_MUTATIONS:
        mutated = copy.deepcopy(document)
        mutate(mutated["templates"]["silver_build"])
        errors = list(check.iter_errors(mutated))
        # The refusal must come from the mutated field, not from an unrelated error.
        intended = [error for error in errors if field in list(error.absolute_path) or (error.validator == "additionalProperties" and field in error.message)]
        results[f"spec refused: {name}"] = bool(errors) and len(intended) == len(errors)
    return results


def ledger() -> dict[str, bool]:
    """Every example ledger state validates and follows the broker's transitions."""
    check = validator("ledger_line.schema.json")
    results = {}
    for case, lines in load("ledger_states.json").items():
        valid = all(not list(check.iter_errors(line)) for line in lines)
        previous: dict[str, str | None] = {}
        ordered = True
        for line in lines:
            last = previous.get(line["submission_id"])
            ordered = ordered and line["event"] in NEXT_EVENTS.get(last, set())
            previous[line["submission_id"]] = line["event"]
        results[f"ledger state valid and ordered: {case}"] = valid and ordered
    broken = dict(load("ledger_states.json")["finished"][-1])
    broken.pop("no_op")
    results["ledger line refused: finished without no_op"] = bool(list(check.iter_errors(broken)))
    renamed = {**load("ledger_states.json")["finished"][0], "container_name": "hai-other-name"}
    results["ledger line refused: container name not derived from the submission"] = bool(list(check.iter_errors(renamed)))
    return results


def requests() -> dict[str, bool]:
    """Accepted requests validate; each caller override is refused for its intended reason."""
    check = validator("submission_request.schema.json")
    examples = load("submission_requests.json")
    results = {f"request accepted: {item['template']} {item['mode']}": not list(check.iter_errors(item)) for item in examples["accepted"]}
    for item in examples["refused"]:
        errors = list(check.iter_errors(item["request"]))
        intended = {"additionalProperties", "pattern", "enum", "minimum", "required"}
        results[f"request refused: {item['case']}"] = bool(errors) and all(error.validator in intended for error in errors)
    return results


def fake() -> dict[str, bool]:
    """The fake keeps the interface's promises: one launch per request, refusals, no-ops and failures."""
    fake_subs = FakeSubmissions(load("job_templates.json"), {"gold_validate": 1}, running={"silver_validate"})
    subs: Submissions = fake_subs  # tasks see only the interface; the ledger is the fake's own
    request = SubmissionRequest("silver_build", RUN, "fixture", 1)
    first, second = subs.submit(request), subs.submit(request)
    intents = [line for line in fake_subs.ledger if line["event"] == "intent"]
    noop = subs.submit(SubmissionRequest("pin_bronze", RUN, "fixture", 1))
    failed = subs.submit(SubmissionRequest("gold_validate", RUN, "fixture", 1))
    refusals = {}
    for name, (bad, reason) in {
        "unsupported mode": (SubmissionRequest("publish_candidates", RUN, "fixture", 1), "does not support mode fixture"),
        "unknown template": (SubmissionRequest("drop_tables", RUN, "fixture", 1), "unknown template drop_tables"),
        "malformed run ID": (SubmissionRequest("silver_build", "run_a", "fixture", 1), "does not match"),
    }.items():
        try:
            subs.submit(bad)
            refusals[name] = False
        except SubmissionRefused as refusal:
            refusals[name] = reason in str(refusal)
    results = {
        "repeated request returns the same handle": first == second,
        "repeated request writes one intent": len(intents) == 1,
        "container name derived from the submission": first.container_name == f"hai-{RUN}-silver_build-1",
        "success reported": subs.status(first).state is State.SUCCEEDED and subs.record(first)["exit_code"] == 0,
        "fixture no-op starts no container": subs.status(noop).state is State.NO_OP
        and not any(line["event"] == "started" and line["template"] == "pin_bronze" for line in fake_subs.ledger),
        "scripted failure reported": subs.status(failed).state is State.FAILED and subs.record(failed)["exit_code"] == 1,
        "cancel of a finished submission changes nothing": subs.cancel(first) == subs.status(first),
        "logs name the submission": request.submission_id in subs.logs(first),
    }
    long_job = subs.submit(SubmissionRequest("silver_validate", RUN, "fixture", 1))
    before = subs.status(long_job)
    try:
        subs.record(long_job)
        unfinished_refused = False
    except SubmissionRefused:
        unfinished_refused = True
    cancelled = subs.cancel(long_job)
    results["a running submission reports running"] = before.state is State.RUNNING and before.exit_code is None
    results["record of an unfinished submission is refused"] = unfinished_refused
    results["cancel stops a running submission"] = cancelled.state is State.CANCELLED and cancelled.exit_code == 143
    results.update({f"fake refuses: {name}": ok for name, ok in refusals.items()})
    return results


def main() -> int:
    """Run every check, write the report with the contract hashes and return 1 when any check fails."""
    started = datetime.now(UTC)
    folder = ROOT / "data" / "e2e" / "orchestration_contracts" / started.strftime("%Y%m%dT%H%M%S%fZ")
    folder.mkdir(parents=True)
    contract_files = sorted(path for path in CONTRACTS.rglob("*") if path.is_file())
    code_files = [ROOT / "orchestration" / "interface.py", ROOT / "orchestration" / "fake.py", Path(__file__)]
    hashes = {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest() for path in [*contract_files, *code_files]}
    sections = {"schemas": schemas, "launch specifications": templates, "ledger": ledger, "submission requests": requests, "fake submissions": fake}
    results = {}
    for name, section in sections.items():
        checks = section()
        results[name] = checks
        sys.stdout.write(f"{'ok' if all(checks.values()) else 'FAIL':<5} {name} ({len(checks)} checks)\n")
        for check, ok in checks.items():
            if not ok:
                sys.stdout.write(f"      failed: {check}\n")
    passed = all(all(checks.values()) for checks in results.values())
    report = {
        "feature": "orchestration contracts (wave 0 items 3 to 5)",
        "started_utc": started.isoformat(),
        "status": "pass" if passed else "fail",
        "commit": run_command("git", ["rev-parse", "HEAD"], cwd=ROOT).stdout.strip(),
        "uncommitted": run_command("git", ["status", "--porcelain", "--", "data_contracts/orchestration", "orchestration"], cwd=ROOT).stdout.splitlines(),
        "python": sys.version.split()[0],
        "command": ".venv/bin/python -m scripts.orchestration.run_contracts_e2e",
        "sha256": hashes,
        "results": results,
        "limits": ["Static contract checks and an in-memory fake; the broker's launches are verified by its own E2E (Airflow O1 and O2)."],
    }
    (folder / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    sys.stdout.write(f"{report['status']}: {folder.relative_to(ROOT)}/report.json\n")
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
