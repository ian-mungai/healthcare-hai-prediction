"""Check draft remote-run schemas and in-memory stand-ins without starting services."""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import sys
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime
from functools import partial
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError

from scripts.lakehouse.run_catalog_registration_e2e import Fixture
from scripts.orchestration.ecs_preparation import ApiRefused, CatalogTable, FakeCatalog, FakeClock, FakeEcs, FakeS3, FakeTask, ObjectVersion

ROOT = Path(__file__).resolve().parents[2]
SCHEMA = ROOT / "data_contracts/orchestration/remote_run.schema.json"
EXAMPLES = ROOT / "data_contracts/orchestration/examples/remote_run.json"
TABLE = CatalogTable("silver", "fixture_table", "00000000-0000-4000-8000-000000000001", "lakehouse/silver/table/metadata/current.json", "v1", 101, (101,))


def require(value: bool, message: str) -> None:
    if not value:
        raise AssertionError(message)


def refused(operation: Callable[[], object], code: str) -> None:
    try:
        operation()
    except ApiRefused as error:
        require(error.code == code, f"expected {code}, received {error.code}")
    else:
        raise AssertionError(f"expected {code}")


def pinned_read(factory: type[FakeS3] = FakeS3) -> None:
    store = factory()
    original = store.seed("lakehouse/fixture/input", b"original")
    store.seed("lakehouse/fixture/input", b"replaced")
    require(store.get(original.key, original.version_id, deadline=100).body == b"original", "pinned_read: used the current version")
    refused(lambda: store.get(original.key, "missing", deadline=100), "NoSuchVersion")


def conditional_write() -> None:
    store = FakeS3()
    original = store.put("lakehouse/fixture/out", b"one", deadline=100)
    refused(lambda: store.put(original.key, b"two", deadline=100), "PreconditionFailed")
    refused(lambda: store.put(original.key, b"two", deadline=100, if_none_match=False), "AccessDenied")
    require(store.get(original.key, original.version_id, deadline=100).body == b"one", "conditional_write: changed existing bytes")


def exact_registration(factory: type[FakeCatalog] = FakeCatalog) -> None:
    catalog = factory()
    require(catalog.register(TABLE, deadline=100) == "registered", "registration: first exact entry was not registered")
    require(catalog.register(TABLE, deadline=100) == "unchanged", "registration: identical repeat changed the entry")
    for changed in (
        replace(TABLE, table_uuid="other"),
        replace(TABLE, metadata_key=TABLE.metadata_key + "_changed"),
        replace(TABLE, metadata_version="v2"),
        replace(TABLE, current_snapshot=102),
    ):
        refused(partial(catalog.register, changed, deadline=100), "RegistrationConflict")
    require(catalog.load("silver", "fixture_table", deadline=100) == TABLE, "registration: conflict changed the registered table")


def retained_snapshot(factory: type[FakeCatalog] = FakeCatalog) -> None:
    catalog = factory()
    catalog.register(TABLE, deadline=100)
    catalog.advance_head("silver", "fixture_table", 102)
    require(catalog.read_snapshot("silver", "fixture_table", 101, deadline=100) == 101, "retained_snapshot: used the current head")
    refused(lambda: catalog.read_snapshot("silver", "fixture_table", 999, deadline=100), "NoSuchSnapshot")
    catalog.available = False
    refused(lambda: catalog.load("silver", "fixture_table", deadline=100), "CatalogUnavailable")


def lost_reply(factory: type[FakeEcs] = FakeEcs) -> None:
    ecs = factory()
    ecs.lose_next_run_reply = True
    refused(lambda: ecs.run_task("hai_fixture_1", "a" * 48, deadline=100), "ReplyLost")
    adopted = ecs.run_task("hai_fixture_1", "a" * 48, deadline=100)
    require(len(ecs.tasks) == 1 and adopted.task_id == "task_0001", "lost_reply: created a duplicate task")
    refused(lambda: ecs.run_task("hai_fixture_2", "a" * 48, deadline=100), "ClientTokenConflict")


def query_rules(factory: type[FakeEcs] = FakeEcs) -> None:
    ecs = factory()
    refused(lambda: ecs.run_task("hai:invalid", "a" * 48, deadline=100), "InvalidParameterException")
    require(ecs.run_task("h" * 100, "b" * 48, deadline=100).try_id == "h" * 100, "try identifier: refused 100 characters")
    refused(lambda: ecs.run_task("h" * 101, "a" * 48, deadline=100), "InvalidParameterException")
    refused(lambda: ecs.list_tasks(started_by="hai_fixture_1", desired_status="STOPPED", deadline=100), "InvalidParameterException")


def paginated_tasks(factory: type[FakeEcs] = FakeEcs) -> None:
    ecs = factory()
    wanted: set[str] = set()
    for number in range(205):
        task = ecs.run_task(f"hai_fixture_{number}", f"{number:048x}", deadline=100)
        if number % 2 == 0:
            ecs.stop_task(task.task_id, deadline=100)
            wanted.add(task.task_id)
    found: set[str] = set()
    token: int | None = 0
    for _ in range(5):
        page, token = ecs.list_tasks(desired_status="STOPPED", next_token=token or 0, deadline=100)
        found.update(task.task_id for task in page)
        if token is None:
            break
    require(found == wanted and token is None, "pagination: omitted a stopped page")
    page, _ = ecs.list_tasks(started_by="hai_fixture_1", deadline=100)
    require(len(page) == 1 and page[0].try_id == "hai_fixture_1", "pagination: used a prefix match")


def fixed_deadline(factory: type[FakeClock] = FakeClock) -> None:
    clock = factory()
    store = FakeS3(clock)
    store.put("lakehouse/fixture/out", b"one", deadline=2)
    clock.advance(2)
    refused(lambda: store.get("lakehouse/fixture/out", "v0001", deadline=2), "Deadline")


def check_invalid(schema: dict[str, Any], example: dict[str, Any]) -> None:
    checker = Draft202012Validator({"$defs": schema["$defs"], "$ref": f"#/$defs/{example['definition']}"})
    errors = list(checker.iter_errors(example["value"]))
    require(bool(errors), f"schema:{example['name']}: accepted invalid example")
    require(
        all(error.validator == example["validator"] and list(error.absolute_path) == example["path"] for error in errors),
        f"schema:{example['name']}: rejection had a different cause",
    )
    require(bool(list(Draft202012Validator(schema).iter_errors(example["value"]))), f"schema:{example['name']}: root accepted invalid example")


def registration_inventory(schema: dict[str, Any], *, rename: bool = False) -> None:
    document = Fixture().prepare().document()
    if rename:
        document["tables"][0]["version_id"] = document["tables"][0].pop("metadata_version")
    checker = Draft202012Validator({"$defs": schema["$defs"], "$ref": "#/$defs/catalog_inventory"})
    require(not list(checker.iter_errors(document)), "catalog inventory: registration document violates schema")
    require(not list(Draft202012Validator(schema).iter_errors(document)), "catalog inventory: root rejects registration document")


def try_identifier_schema(schema: dict[str, Any], examples: dict[str, Any]) -> None:
    for name, field in (("object_ref", "try_id"), ("record", "try_id"), ("seal", "validation_attempt"), ("run_map", "live_try_id")):
        checker = Draft202012Validator({"$defs": schema["$defs"], "$ref": f"#/$defs/{name}"})
        value = copy.deepcopy(examples["valid"][name])
        value[field] = "h" * 100
        require(not list(checker.iter_errors(value)), "try identifier: schema refused 100 characters")
        value[field] += "h"
        errors = list(checker.iter_errors(value))
        require(bool(errors), "try identifier: schema accepted 101 characters")
        require(all(error.validator == "pattern" and list(error.absolute_path) == [field] for error in errors), "try identifier: wrong schema refusal")


class HeadReadS3(FakeS3):
    def get(self, key: str, version_id: str, *, deadline: float) -> ObjectVersion:
        return super().get(key, self.objects[key][-1].version_id, deadline=deadline)


class OverwriteCatalog(FakeCatalog):
    def register(self, table: CatalogTable, *, deadline: float) -> str:
        key = (table.namespace, table.name)
        if key in self.tables and self.tables[key] != table:
            self.tables[key] = table
            return "registered"
        return super().register(table, deadline=deadline)


class HeadCatalog(FakeCatalog):
    def read_snapshot(self, namespace: str, name: str, snapshot: int, *, deadline: float) -> int:
        return self.load(namespace, name, deadline=deadline).current_snapshot


class DuplicateEcs(FakeEcs):
    def run_task(self, try_id: str, client_token: str, *, deadline: float) -> FakeTask:
        self.tokens.pop(client_token, None)
        return super().run_task(try_id, client_token, deadline=deadline)


class LongTryIdEcs(FakeEcs):
    def run_task(self, try_id: str, client_token: str, *, deadline: float) -> FakeTask:
        return super().run_task(try_id[:100], client_token, deadline=deadline)


class CombinedFilterEcs(FakeEcs):
    def list_tasks(
        self, *, deadline: float, started_by: str | None = None, desired_status: str | None = None, next_token: int = 0
    ) -> tuple[list[FakeTask], int | None]:
        return super().list_tasks(deadline=deadline, started_by=started_by, desired_status=None if started_by else desired_status, next_token=next_token)


class OnePageEcs(FakeEcs):
    def list_tasks(
        self, *, deadline: float, started_by: str | None = None, desired_status: str | None = None, next_token: int = 0
    ) -> tuple[list[FakeTask], int | None]:
        page, _ = super().list_tasks(deadline=deadline, started_by=started_by, desired_status=desired_status, next_token=next_token)
        return page, None


class UnboundedClock(FakeClock):
    def require_before(self, deadline: float) -> None:
        return None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    schema = json.loads(SCHEMA.read_text())
    examples = json.loads(EXAMPLES.read_text())
    Draft202012Validator.check_schema(schema)
    checks: list[dict[str, Any]] = []
    mutations: list[dict[str, Any]] = []

    def record(name: str, operation: Callable[[], None]) -> None:
        try:
            operation()
        except ValidationError as error:
            checks.append({"name": name, "ok": False, "cause": f"schema keyword {error.validator} at {list(error.absolute_path)}"})
        except (AssertionError, ApiRefused) as error:
            checks.append({"name": name, "ok": False, "cause": str(error)})
        else:
            checks.append({"name": name, "ok": True})

    for name, value in examples["valid"].items():
        checker = Draft202012Validator({"$defs": schema["$defs"], "$ref": f"#/$defs/{name}"})
        record(f"schema valid {name}", partial(checker.validate, value))
        record(f"root schema valid {name}", partial(Draft202012Validator(schema).validate, value))
    for example in examples["invalid"]:
        record(f"schema refusal {example['name']}", partial(check_invalid, schema, example))
    for operation in (pinned_read, conditional_write, exact_registration, retained_snapshot, lost_reply, query_rules, paginated_tasks, fixed_deadline):
        record(operation.__name__, operation)
    record("registration inventory matches schema", partial(registration_inventory, schema))
    record("try identifier schema boundary", partial(try_identifier_schema, schema, examples))
    planted: list[tuple[str, Callable[[], None], str]] = [
        ("read current object", lambda: pinned_read(HeadReadS3), "pinned_read: used the current version"),
        ("overwrite registration", lambda: exact_registration(OverwriteCatalog), "expected RegistrationConflict"),
        ("read current snapshot", lambda: retained_snapshot(HeadCatalog), "retained_snapshot: used the current head"),
        ("retry lost reply with fresh task", lambda: lost_reply(DuplicateEcs), "lost_reply: created a duplicate task"),
        ("accept incompatible list filters", lambda: query_rules(CombinedFilterEcs), "expected InvalidParameterException"),
        ("drop pagination token", lambda: paginated_tasks(OnePageEcs), "pagination: omitted a stopped page"),
        ("ignore fixed deadline", lambda: fixed_deadline(UnboundedClock), "expected Deadline"),
        ("accept overlong try identifier", lambda: query_rules(LongTryIdEcs), "expected InvalidParameterException"),
        ("rename emitted inventory field", partial(registration_inventory, schema, rename=True), "catalog inventory: registration document violates schema"),
    ]
    for name, field in (("object_ref", "try_id"), ("record", "try_id"), ("seal", "validation_attempt"), ("run_map", "live_try_id")):
        widened = copy.deepcopy(schema)
        widened["$defs"][name]["properties"][field]["pattern"] = widened["$defs"][name]["properties"][field]["pattern"].replace("{1,100}", "{1,128}")
        planted.append(("widen try identifier " + name, partial(try_identifier_schema, widened, examples), "try identifier: schema accepted 101 characters"))
    for example in examples["invalid"]:
        weakened = copy.deepcopy(schema)
        node = weakened["$defs"]
        for part in example["guard_path"][:-1]:
            node = node[part]
        node[example["guard_path"][-1]] = example["weakened_guard"]
        planted.append(
            (
                f"weaken {example['name']}",
                partial(check_invalid, weakened, example),
                f"schema:{example['name']}: accepted invalid example",
            )
        )
    widened_root = copy.deepcopy(schema)
    del widened_root["oneOf"]
    planted.append(
        (
            "remove root schema alternatives",
            partial(check_invalid, widened_root, examples["invalid"][0]),
            f"schema:{examples['invalid'][0]['name']}: root accepted invalid example",
        )
    )
    for name, operation, expected in planted:
        try:
            operation()
        except AssertionError as error:
            mutations.append({"name": name, "caught": str(error) == expected, "expected": expected, "observed": str(error)})
        else:
            mutations.append({"name": name, "caught": False, "expected": expected})
    paths = (
        SCHEMA,
        EXAMPLES,
        Path(__file__),
        ROOT / "scripts/orchestration/ecs_preparation.py",
        ROOT / "scripts/lakehouse/register_aws_catalog.py",
        ROOT / "scripts/lakehouse/run_catalog_registration_e2e.py",
        ROOT / "scripts/lakehouse/gold_publish_record.py",
    )
    report = {
        "scope": "preparation only: draft schemas and in-memory API stand-ins, no production acceptance or S/D/T/A proof",
        "checks": checks,
        "planted_defects": mutations,
        "source_sha256": {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest() for path in paths},
        "cleanup": "done: no services, child processes, sockets or reservations started",
    }
    args.out.mkdir(parents=True, exist_ok=True)
    (args.out / "report.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    retention = {
        "class": 4,
        "purpose": "remote-run preparation check summary",
        "created": datetime.now(UTC).isoformat(),
        "end_condition": "no task or review cites this record",
    }
    (args.out / "retention.json").write_text(json.dumps(retention, indent=2) + "\n")
    failed = sum(not item["ok"] for item in checks) + sum(not item["caught"] for item in mutations)
    sys.stdout.write(json.dumps({"checks": len(checks), "planted_defects": len(mutations), "failed": failed, "report": str(args.out / "report.json")}) + "\n")
    return int(bool(failed))


if __name__ == "__main__":
    sys.exit(main())
