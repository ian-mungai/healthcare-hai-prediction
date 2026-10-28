"""Exercise synthetic catalog migration services and recovery commands with persisted evidence."""

from __future__ import annotations

import copy
import hashlib
import json
import signal
import sys
import time
from functools import partial
from pathlib import Path
from typing import Any

from scripts.process import run_command

REHEARSAL = "rehearsal_2468abcd"
PREFIX = "migration_rehearsal_2468abcd"
SILVER, GOLD, EMPTY, ABSENT = (f"{PREFIX}.{name}" for name in ("silver", "gold", "empty", "absent"))


def head(snapshot: int) -> dict[str, Any]:
    """Return an independently specified synthetic table head, with no cloud location."""
    return {
        "uuid": "synthetic_table_uuid",
        "metadata_location": f"file:///synthetic/metadata/{snapshot}.metadata.json",
        "version_id": f"version_{snapshot}",
        "snapshot_id": snapshot,
        "refs": {"main": {"snapshot_id": snapshot, "type": "branch"}, "retained": {"snapshot_id": 1, "type": "tag"}},
    }


def namespace(tables: dict[str, Any], *, properties: dict[str, str] | None = None, present: bool = True) -> dict[str, Any]:
    """Specify namespace existence, properties and complete table membership."""
    return {"state": "present" if present else "absent", "properties": properties or {}, "tables": tables}


def evidence_value(value: Any) -> Any:
    if isinstance(value, bytes):
        return {"sha256": hashlib.sha256(value).hexdigest(), "size": len(value)}
    if isinstance(value, dict):
        return {str(key): evidence_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [evidence_value(item) for item in (sorted(value, key=str) if isinstance(value, set) else value)]
    return value


class Scenario:
    """Keep each command, input, response and assertion under one case's evidence directory."""

    def __init__(self, root: Path, subject_root: Path, deadline: float) -> None:
        self.root = root
        self.subject_root = subject_root
        self.deadline = deadline
        self.commands: list[dict[str, Any]] = []
        self.assertions: list[dict[str, Any]] = []
        root.mkdir(parents=True)

    def invoke(self, action: str, *, expected_exit: int = 0, expected_error: str | None = None, **values: Any) -> dict[str, Any]:
        """Execute the real file-backed driver with a bounded command and retain its output."""
        request = self.root / f"{len(self.commands):02d}_{action}_request.json"
        response = request.with_name(request.name.replace("_request.json", "_response.json"))
        request.write_text(json.dumps({"action": action, "rehearsal_id": REHEARSAL, **values}, sort_keys=True, indent=2) + "\n", encoding="utf-8")
        args = ["-m", "scripts.lakehouse.catalog_journal", "--stand-in", str(self.root / "driver"), "--request", str(request), "--response", str(response)]
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("recovery stand-in exceeded its absolute case deadline")
        result = run_command(sys.executable, args, cwd=self.subject_root, timeout=min(10, remaining))
        self.commands.append({"argv": [sys.executable, *args], "cwd": str(self.subject_root), "exit_code": result.returncode, "stderr": result.stderr})
        self.check(f"{action} exit", result.returncode, expected_exit)
        if expected_error is not None:
            self.check(f"{action} rejection cause", expected_error in result.stderr, True)
        if expected_exit:
            return {}
        if not response.exists():
            raise RuntimeError(f"{action} produced no response: {result.stderr}")
        return json.loads(response.read_text(encoding="utf-8"))

    def check(self, label: str, actual: Any, expected: Any) -> None:
        """Compare an observable result with an expectation supplied by the scenario."""
        self.assertions.append({"label": label, "expected": evidence_value(expected), "observed": evidence_value(actual), "passed": actual == expected})

    def scoped(self, name: str) -> Scenario:
        """Retain a variant's persisted state without replacing another variant's evidence."""
        child = Scenario(self.root / name, self.subject_root, self.deadline)
        child.commands, child.assertions = self.commands, self.assertions
        return child


def intent_without_completion(case: Scenario) -> None:
    """Cover the three missing-hook orders, old records, rebuilt catalogs and older dumps."""
    parent = case
    for fault in ("kill_after_commit", "database_before_hook", "before_hook"):
        case = parent.scoped(fault)
        initial = {SILVER: namespace({"kept": head(1)}), GOLD: namespace({"other": head(1)})}
        case.invoke("seed", inventory=initial, layers={SILVER: "silver", GOLD: "gold"})
        case.invoke("write", intent_id="older", layer="silver", changes={SILVER: initial[SILVER]})
        case.invoke("save_dump")
        case.invoke(
            "write",
            expected_exit=-signal.SIGKILL if fault == "kill_after_commit" else 0,
            intent_id="pending",
            layer="silver",
            changes={SILVER: namespace({"kept": head(2)})},
            fault=fault,
        )
        persisted = case.invoke("inspect")
        case.check(f"{fault}: pointer committed before interruption", persisted["catalog"][SILVER]["tables"]["kept"], head(2))
        before = case.invoke("recover")
        case.check(f"{fault}: unresolved despite older confirmation", before["unresolved"], [SILVER])
        case.check(f"{fault}: pending intent named", before["pending_intents"], ["pending"])
        case.check(f"{fault}: unresolved namespace not rebuilt", SILVER in before["catalog"], False)
        case.check(f"{fault}: unrelated namespace rebuilt", before["catalog"].get(GOLD), initial[GOLD])
        case.check(f"{fault}: blocked layer", before["blocked_layers"], ["silver"])
        case.invoke("complete", intent_id="pending", source="rebuilt", step="rebuilt")
        rebuilt = case.invoke("recover")
        case.check(f"{fault}: rebuilt database cannot confirm", rebuilt["pending_intents"], ["pending"])
        case.invoke("restore_dump")
        case.invoke("complete", intent_id="pending", source="live", step="dump")
        restored = case.invoke("recover")
        case.check(f"{fault}: older dump cannot confirm", restored["unresolved"], [SILVER])
        case.check(f"{fault}: dump keeps intent pending", restored["pending_intents"], ["pending"])
        if fault == "before_hook":
            case = parent.scoped("live_confirmation")
            case.invoke("seed", inventory=initial, layers={SILVER: "silver", GOLD: "gold"})
            case.invoke("write", intent_id="live_pending", layer="silver", changes={SILVER: namespace({"kept": head(2)})}, fault="before_hook")
            case.invoke("complete", intent_id="live_pending", source="live", step="reconcile")
            confirmed = case.invoke("recover")
            case.check("original quiesced live source can confirm", confirmed["pending_intents"], [])
            case.check("confirmed commit is replayed", confirmed["catalog"][SILVER]["tables"]["kept"], head(2))

    replacement = parent.scoped("replacement_live")
    initial = {SILVER: namespace({"kept": head(1)})}
    replacement.invoke("seed", inventory=initial, layers={SILVER: "silver"})
    replacement.invoke("write", intent_id="pending", layer="silver", changes={SILVER: namespace({"kept": head(2)})}, fault="before_hook")
    provenance = replacement.root / "driver/provenance.json"
    provenance.write_text(json.dumps({"kind": "live", "database_id": "replacement_database", "available": True}), encoding="utf-8")
    replacement.invoke("complete", intent_id="pending", step="replacement")
    rejected = replacement.invoke("recover")
    replacement.check("replacement live database cannot confirm", rejected["pending_intents"], ["pending"])
    replacement.check("replacement live database leaves namespace unresolved", rejected["unresolved"], [SILVER])

    late = parent.scoped("same_step_confirmation")
    late.invoke("seed", inventory=initial, layers={SILVER: "silver"})
    late.invoke("write", intent_id="pending", layer="silver", changes={SILVER: namespace({"kept": head(2)})}, fault="database_before_hook")
    late.check("unavailable terminal read stays unconfirmed", late.invoke("recover")["pending_intents"], ["pending"])
    provenance = late.root / "driver/provenance.json"
    provenance.write_text(json.dumps({"kind": "live", "database_id": REHEARSAL, "available": True}), encoding="utf-8")
    late.invoke("complete", intent_id="pending")
    confirmed = late.invoke("recover")
    late.check("same terminal step later clears the intent", confirmed["pending_intents"], [])
    late.check("same terminal step replays the committed head", confirmed["catalog"][SILVER]["tables"]["kept"], head(2))
    late.invoke("complete", intent_id="pending")
    late.check("repeated later confirmation appends only once", late.invoke("inspect")["completion_count"], 2)


def namespace_replay(case: Scenario) -> None:
    """Replay confirmed deletion, properties, empty namespaces and absence, then compare both ways."""
    initial = {SILVER: namespace({"deleted": head(1), "kept": head(1)}, properties={"location": "file:///synthetic/old"}), ABSENT: namespace({"old": head(1)})}
    final = {
        SILVER: namespace({"kept": head(2)}, properties={"location": "file:///synthetic/new", "owner": "synthetic"}),
        EMPTY: namespace({}, properties={"purpose": "empty"}),
        ABSENT: namespace({}, present=False),
    }
    case.invoke("seed", inventory=initial, layers={SILVER: "silver", EMPTY: "silver", ABSENT: "silver"})
    metadata = case.root / "driver/metadata/seed_heads.json"
    original_metadata = hashlib.sha256(metadata.read_bytes()).hexdigest()
    case.invoke("write", intent_id="replacement", layer="silver", changes=final)
    first = case.invoke("recover")
    expected = {key: value for key, value in final.items() if value["state"] == "present"}
    case.check("newest namespace state replaces earlier membership", first["catalog"], expected)
    case.check(
        "non-purging deletion preserves original metadata", hashlib.sha256(metadata.read_bytes()).hexdigest() if metadata.exists() else None, original_metadata
    )
    create = {"create_namespace": SILVER, "properties": final[SILVER]["properties"]}
    register = {"register_table": f"{SILVER}.kept", "head": head(2)}
    case.check("namespace properties restored", create in first["operations"], True)
    case.check("properties precede table registration", first["operations"].index(create) < first["operations"].index(register), True)
    case.check("empty namespace exists", first["catalog"].get(EMPTY), final[EMPTY])
    case.check("confirmed absence registers nothing", ABSENT in first["catalog"], False)
    case.check("normal replay has complete parity", case.invoke("parity", actual=first["catalog"], expected=expected)["equal"], True)
    second = case.invoke("recover")
    case.check("repeated rebuild is stable", second, first)
    case.invoke("write", intent_id="replacement", layer="silver", changes=final)
    case.check("repeated intent and completion do not append", case.invoke("inspect")["completion_count"], 1)
    extra = copy.deepcopy(expected)
    extra[ABSENT] = namespace({})
    case.check("extra namespace fails parity", case.invoke("parity", actual=extra, expected=expected)["equal"], False)
    extra = copy.deepcopy(expected)
    extra[SILVER]["tables"]["deleted"] = head(1)
    case.check("extra table fails parity", case.invoke("parity", actual=extra, expected=expected)["equal"], False)
    missing = copy.deepcopy(expected)
    missing.pop(EMPTY)
    case.check("missing empty namespace fails parity", case.invoke("parity", actual=missing, expected=expected)["equal"], False)
    wrong_properties = copy.deepcopy(expected)
    wrong_properties[SILVER]["properties"] = initial[SILVER]["properties"]
    case.check("wrong namespace properties fail parity", case.invoke("parity", actual=wrong_properties, expected=expected)["equal"], False)

    invalid = case.scoped("invalid_fault")
    invalid.invoke("seed", inventory=initial, layers={SILVER: "silver", ABSENT: "silver"})
    driver = invalid.root / "driver"
    before = {str(path.relative_to(driver)): hashlib.sha256(path.read_bytes()).hexdigest() for path in driver.rglob("*") if path.is_file()}
    invalid.invoke(
        "write",
        expected_exit=1,
        expected_error="unknown stand-in interruption point",
        intent_id="invalid",
        layer="silver",
        changes={SILVER: namespace({"kept": head(999)})},
        fault="mistyped_fault",
    )
    after = {str(path.relative_to(driver)): hashlib.sha256(path.read_bytes()).hexdigest() for path in driver.rglob("*") if path.is_file()}
    invalid.check("invalid fault leaves every persisted file unchanged", after, before)


def database_loss(case: Scenario) -> None:
    """Leave the unresolved namespace uncreated while confirmed neighbors rebuild from recorded heads."""
    initial = {SILVER: namespace({"kept": head(1)}), GOLD: namespace({"other": head(1)})}
    case.invoke("seed", inventory=initial, layers={SILVER: "silver", GOLD: "gold"})
    case.invoke("write", intent_id="confirmed_gold", layer="gold", changes={GOLD: namespace({"other": head(2)})})
    case.invoke("write", intent_id="failed_pointer", layer="gold", changes={GOLD: namespace({"other": head(999)})}, fault="metadata_before_commit")
    case.invoke("write", intent_id="lost_database", layer="silver", changes={SILVER: namespace({"kept": head(2)})}, fault="before_hook")
    case.invoke("plant_uncommitted_files", guessed_head=head(999))
    case.invoke("lose_database")
    case.invoke("complete", intent_id="lost_database", source="live", step="terminal")
    result = case.invoke("recover")
    case.check("loss names the unresolved namespace", result["unresolved"], [SILVER])
    case.check("loss names the unresolved intent", result["pending_intents"], ["lost_database"])
    case.check("loss leaves unresolved namespace uncreated", SILVER in result["catalog"], False)
    case.check("other namespace uses confirmed head", result["catalog"].get(GOLD), namespace({"other": head(2)}))
    case.check("affected layer stays closed", result["blocked_layers"], ["silver"])
    case.check("no file or backup head is inferred", "999" in json.dumps(result["catalog"]), False)


CASES = {"intent_without_completion": intent_without_completion, "namespace_replay": namespace_replay, "database_loss": database_loss}


class MigrationClock:
    def __init__(self) -> None:
        self.now = 0.0
        self.waits: list[float] = []

    def __call__(self) -> float:
        return self.now

    def wait(self, duration: float) -> None:
        self.waits.append(duration)
        self.now += duration


def migration_metadata(snapshot: int = 2) -> dict[str, Any]:
    """Specify synthetic Iceberg metadata independently of the migration implementation."""
    return {
        "format-version": 2,
        "table-uuid": "2468abcd-0000-4000-8000-000000000001",
        "location": "s3://synthetic-migration/lakehouse/source/measurements",
        "current-snapshot-id": snapshot,
        "last-sequence-number": snapshot,
        "current-schema-id": 0,
        "default-spec-id": 0,
        "default-sort-order-id": 0,
        "properties": {"owner": "synthetic", "write.data.path": "s3://synthetic-migration/lakehouse/source/measurements/data"},
        "schemas": [{"schema-id": 0, "type": "struct", "fields": []}],
        "partition-specs": [{"spec-id": 0, "fields": []}],
        "sort-orders": [{"order-id": 0, "fields": []}],
        "snapshots": [
            {
                "snapshot-id": number,
                "parent-snapshot-id": number - 1 if number > 1 else None,
                "manifest-list": f"s3://synthetic-migration/lakehouse/source/measurements/metadata/snap_{number}.avro",
            }
            for number in range(1, snapshot + 1)
        ],
        "refs": {"main": {"snapshot-id": snapshot, "type": "branch"}, "retained": {"snapshot-id": 1, "type": "tag"}},
        "metadata-log": [
            {"metadata-file": f"s3://synthetic-migration/lakehouse/source/measurements/metadata/{number}.metadata.json"} for number in range(1, snapshot)
        ],
    }


class MigrationFixture:
    """Supply paged REST responses and exact versioned bytes without network or AWS access."""

    def __init__(self, case: Scenario, *, destination_empty: bool = False) -> None:
        from scripts.lakehouse import migrate_aws_catalog as migration

        self.case = case
        self.clock = MigrationClock()
        self.events: list[dict[str, Any]] = []
        self.faults: dict[str, Any] = {}
        self.loads: dict[tuple[str, str], int] = {}
        self.object_values: dict[str, dict[str, Any]] = {}
        self.grants = {
            "catalog_admin": [{"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}],
            "reader": [{"type": "catalog", "privilege": "CATALOG_READ_PROPERTIES"}],
        }
        self.catalog: dict[str, Any] = {
            "properties": {"default-base-location": "s3://synthetic-migration/lakehouse/"},
            "storageConfigInfo": {"allowedLocations": ["s3://synthetic-migration/lakehouse/"], "stsUnavailable": True},
        }
        self.namespaces: dict[str, dict[tuple[str, ...], dict[str, Any]]] = {
            "source": {
                ("source",): {"properties": {"owner": "synthetic"}, "tables": {}, "views": []},
                ("source", "nested"): {"properties": {}, "tables": {}, "views": []},
                ("empty",): {"properties": {"purpose": "empty"}, "tables": {}, "views": []},
            },
            "destination": {},
        }
        for number in (1, 2):
            self.put(migration_metadata(number), number)
        for namespace, name in ((("source",), "measurements"), (("source", "nested"), "nested_table")):
            metadata = migration_metadata()
            if len(namespace) > 1:
                metadata["table-uuid"] = "2468abcd-0000-4000-8000-000000000002"
                metadata["location"] = "s3://synthetic-migration/lakehouse/source/nested_table"
                metadata["properties"] = {}
                metadata["metadata-log"] = []
            location = self.put(metadata, 2)
            self.namespaces["source"][namespace]["tables"][name] = {"metadata-location": location, "metadata": metadata}
        if not destination_empty:
            self.namespaces["destination"] = copy.deepcopy(self.namespaces["source"])
            old = migration_metadata(1)
            self.namespaces["destination"][("source",)]["tables"]["measurements"] = {"metadata-location": self.put(old, 1), "metadata": old}
        self.scope = migration.StorageScope(
            table_root="s3://synthetic-migration/lakehouse/",
            control_prefixes=(
                "s3://synthetic-migration/lakehouse/catalog_journal/",
                "s3://synthetic-migration/lakehouse/catalog_migration/",
                "s3://synthetic-migration/lakehouse/run_stage/",
            ),
            scratch_locations=("s3://synthetic-migration/lakehouse/scratch/",),
            required_namespaces=(("source",), ("source", "nested"), ("empty",)),
            required_tables=((("source",), "measurements"), (("source", "nested"), "nested_table")),
            publish_branch_prefixes=("publish_", "silver_publish_"),
        )
        self.artifacts = self.Artifacts()
        self.io = migration.MigrationIO(
            source=migration.GetOnlyCatalog(self.request("source"), "source_catalog", clock=self.clock),
            destination=migration.DestinationCatalog(self.request("destination"), "destination_catalog", clock=self.clock),
            source_objects=self.Objects(self, "source_object"),
            destination_objects=self.Objects(self, "destination_object"),
            scope=self.scope,
            records=migration.MigrationRecords(
                case.root / "migration",
                "synthetic_cutover",
                "synthetic_epoch",
                "destination_database",
                observed_at="2026-10-25T08:00:00Z",
                remote=self.artifacts,
                clock=self.clock,
            ),
            clock=self.clock,
        )
        (case.root / "synthetic_inputs.json").write_text(
            json.dumps({"catalog": self.catalog, "grants": self.grants, "objects": self.object_values}, sort_keys=True, indent=2) + "\n", encoding="utf-8"
        )

    def put(self, metadata: dict[str, Any], number: int) -> str:
        location = f"{metadata['location']}/metadata/{number}.metadata.json"
        body = (json.dumps(metadata, sort_keys=True, separators=(",", ":")) + "\n").encode()
        self.object_values[location] = {"body": body.decode(), "version_id": f"version_{number}", "etag": f"etag_{number}", "modified": float(number)}
        return location

    class Artifacts:
        def __init__(self) -> None:
            self.objects: dict[str, bytes] = {}
            self.calls: list[dict[str, Any]] = []

        def put(self, name: str, payload: bytes, *, sha256: str, if_none_match: str, deadline: float) -> None:
            if if_none_match != "*" or hashlib.sha256(payload).hexdigest() != sha256:
                raise AssertionError("the remote migration artifact has no valid write-once condition or digest")
            if name in self.objects and self.objects[name] != payload:
                raise AssertionError("a remote migration artifact was overwritten")
            self.objects[name] = payload
            self.calls.append({"name": name, "sha256": sha256, "condition": if_none_match, "deadline": deadline})

    def request(self, role: str) -> Any:
        from urllib.parse import parse_qs, unquote, urlsplit

        def send(method: str, path: str, *, body: Any = None, deadline: float) -> tuple[int, object]:
            self.events.append({"role": role, "method": method, "path": path, "body": copy.deepcopy(body)})
            self.clock.now += self.faults.get("request_step", 0.0)
            parsed = urlsplit(path)
            query = parse_qs(parsed.query, keep_blank_values=True)
            pieces = [unquote(part) for part in parsed.path.split("/") if part]
            if role == "source" and method != "GET":
                raise AssertionError("the source received a mutation")
            if "management" in pieces:
                if pieces[-1] == "grants":
                    return 200, self.page(self.grants[pieces[-2]], query, "grants")
                if pieces[-1] == "catalog-roles":
                    return 200, self.page([{"name": name} for name in sorted(self.grants)], query, "roles")
                return 200, copy.deepcopy(self.catalog)
            position = pieces.index("namespaces")
            suffix = pieces[position + 1 :]
            namespaces = self.namespaces[role]
            if not suffix:
                if method == "POST":
                    namespace = tuple(body["namespace"])
                    if len(namespace) > 1 and namespace[:-1] not in namespaces:
                        return 404, None
                    if namespace not in namespaces:
                        namespaces[namespace] = {"properties": copy.deepcopy(body["properties"]), "tables": {}, "views": []}
                    return 200, {"namespace": list(namespace), "properties": namespaces[namespace]["properties"]}
                parent = tuple(query.get("parent", [""])[0].split("\x1f")) if query.get("parent") else ()
                listed = [list(name) for name in sorted(namespaces) if name[:-1] == parent]
                if (
                    role == "destination"
                    and not parent
                    and self.faults.get("omit_empty_after_rebuild")
                    and all(name in namespaces.get(scope, {}).get("tables", {}) for scope, name in self.scope.required_tables)
                ):
                    listed = [name for name in listed if name != ["empty"]]
                    self.faults.pop("omit_empty_after_rebuild")
                    self.faults["omitted_final_namespace"] = True
                if self.faults.get("omit_namespace"):
                    listed = [name for name in listed if name != ["source", "nested"]]
                return 200, self.page(listed, query, "namespaces")
            namespace = tuple(suffix[0].split("\x1f"))
            if namespace not in namespaces:
                return 404, None
            entry = namespaces[namespace]
            if len(suffix) == 1:
                if method == "DELETE":
                    if entry["tables"] or entry["views"]:
                        return 409, None
                    del namespaces[namespace]
                    if self.faults.pop("lost_namespace_delete_reply", False):
                        raise OSError("synthetic namespace removal reply was lost")
                    return 204, None
                return 200, {"namespace": list(namespace), "properties": copy.deepcopy(entry["properties"])}
            if suffix[1] == "properties":
                entry["properties"].update(body["updates"])
                for name in body["removals"]:
                    entry["properties"].pop(name, None)
                if self.faults.pop("lost_properties_reply", False):
                    raise OSError("synthetic namespace property reply was lost")
                return 200, {"updated": list(body["updates"]), "removed": body["removals"], "missing": []}
            if suffix[1] in {"tables", "views"} and len(suffix) == 2:
                names = sorted(entry["tables"]) if suffix[1] == "tables" else entry["views"]
                return 200, self.page([{"namespace": list(namespace), "name": name} for name in names], query, "identifiers")
            if suffix[1] == "register":
                if self.faults.pop("register_failure", False):
                    raise OSError("synthetic private transport text")
                if namespace not in namespaces:
                    return 404, None
                name = body["name"]
                if name in entry["tables"]:
                    return 409, None
                metadata = json.loads(self.object_values[body["metadata-location"]]["body"])
                entry["tables"][name] = {"metadata-location": body["metadata-location"], "metadata": metadata}
                if self.faults.pop("lost_registration_reply", False):
                    raise OSError("synthetic private transport text")
                return 200, copy.deepcopy(entry["tables"][name])
            name = suffix[2]
            if method == "DELETE":
                if query.get("purgeRequested") != ["false"]:
                    raise AssertionError("a drop requested purge")
                entry["tables"].pop(name, None)
                if self.faults.pop("lost_table_delete_reply", False):
                    raise OSError("synthetic table removal reply was lost")
                return 204, None
            key = role, name
            self.loads[key] = self.loads.get(key, 0) + 1
            if role == "destination" and name == "measurements" and self.loads[key] == 2 and self.faults.pop("remove_before_unchanged_read", False):
                entry["tables"].pop(name)
            if name not in entry["tables"]:
                return 404, None
            loaded = copy.deepcopy(entry["tables"][name])
            fault = self.faults.get(role + "_catalog")
            if fault:
                loaded["metadata"][fault] = self.faults["corrupt_value"]
            return 200, loaded

        return send

    def page(self, items: list[Any], query: dict[str, list[str]], field: str) -> dict[str, Any]:
        offset = int(query.get("pageToken", ["0"])[0] or "0")
        page: dict[str, Any] = {field: copy.deepcopy(items[offset : offset + 1])}
        if offset + 1 < len(items):
            page["next-page-token"] = str(offset if self.faults.get("repeat_token") else offset + 1)
        if self.faults.get("unknown_page_kind"):
            page["unknown_objects"] = ["synthetic"]
        return page

    class Objects:
        def __init__(self, fixture: MigrationFixture, role: str) -> None:
            self.fixture, self.role = fixture, role

        def stat(self, location: str, version_id: str | None, *, deadline: float) -> Any:
            from scripts.lakehouse import migrate_aws_catalog as migration

            item = self.fixture.object_values[location]
            version = item["version_id"]
            if version_id is not None and version_id != version:
                raise migration.MigrationError("object_version", "the synthetic metadata version is absent")
            if self.fixture.faults.get("object_version"):
                version = "replacement_version"
            return migration.ObjectStat(version, len(item["body"].encode()), item["etag"], item["modified"])

        def read(self, location: str, version_id: str | None, *, deadline: float) -> Any:
            from scripts.lakehouse import register_aws_catalog as registration

            self.fixture.events.append({"role": self.role, "operation": "read_object", "location": location, "version_id": version_id})
            item = self.fixture.object_values[location]
            metadata = json.loads(item["body"])
            fault = self.fixture.faults.get(self.role)
            if fault:
                metadata[fault] = self.fixture.faults["corrupt_value"]
            body = (json.dumps(metadata, sort_keys=True, separators=(",", ":")) + "\n").encode()
            return registration.ObjectValue(item["version_id"], body)

        def list_metadata(self, prefix: str, token: str | None, *, deadline: float) -> Any:
            items = [
                {"location": location, "modified": item["modified"]}
                for location, item in sorted(self.fixture.object_values.items())
                if location.startswith(prefix) and location.endswith(".metadata.json") and "/999.metadata.json" not in location
            ]
            if self.fixture.faults.get("newer_metadata"):
                items.append({"location": prefix + "uncommitted.metadata.json", "modified": 1000.0})
            return self.fixture.page(items, {} if token is None else {"pageToken": [token]}, "objects")


def migration_reject(case: Scenario, operation: Any, cause: str) -> None:
    from scripts.lakehouse import migrate_aws_catalog as migration

    try:
        operation()
    except migration.MigrationError as error:
        case.check("the intended migration refusal", error.code, cause)
    else:
        case.check("the migration must refuse this input", "accepted", cause)


def migration_inventory(case: Scenario) -> None:
    from scripts.lakehouse import migrate_aws_catalog as migration

    fixture = MigrationFixture(case)
    frozen = migration.freeze(fixture.io, deadline=300, wait=fixture.clock.wait)
    inventory = frozen["inventory"]
    case.check("recursive namespace membership", [row["namespace"] for row in inventory["namespaces"]], [["empty"], ["source"], ["source", "nested"]])
    case.check(
        "table membership includes a nested table",
        [(row["namespace"], row["name"]) for row in inventory["tables"]],
        [(["source"], "measurements"), (["source", "nested"], "nested_table")],
    )
    case.check("frozen reads are separated by two minutes", fixture.clock.waits, [120.0])
    case.check("inventory records every top-level page", inventory["pages"]["namespaces:root"], 2)
    case.check(
        "the first list call explicitly requests pagination",
        any(item.get("path", "").endswith("/namespaces?pageToken=") for item in fixture.events),
        True,
    )
    case.check("the source sends GET only", {item["method"] for item in fixture.events if item.get("role") == "source"}, {"GET"})
    case.check(
        "metadata reads use immutable versions", all(item["version_id"] is not None for item in fixture.events if item.get("operation") == "read_object"), True
    )
    case.check("snapshot refs survive inventory", inventory["tables"][0]["readings"]["catalog"]["refs"]["retained"], {"snapshot-id": 1, "type": "tag"})
    for fault, cause in (
        ("omit_namespace", "coverage"),
        ("repeat_token", "paging"),
        ("unknown_page_kind", "paging"),
        ("object_version", "object_version"),
        ("newer_metadata", "newer_metadata"),
    ):
        variant = MigrationFixture(case.scoped(fault))
        variant.faults[fault] = True
        migration_reject(case, partial(migration.freeze, variant.io, deadline=300, wait=variant.clock.wait), cause)


def migration_readiness(case: Scenario) -> None:
    from dataclasses import replace

    from scripts.lakehouse import migrate_aws_catalog as migration

    for label, cause in (
        ("view", "unsupported_view"),
        ("branch", "open_publish_branch"),
        ("coverage", "coverage"),
        ("property", "namespace_properties"),
        ("residue", "rehearsal_residue"),
        ("exception", "coverage"),
        ("default", "coverage"),
    ):
        fixture = MigrationFixture(case.scoped(label))
        if label == "view":
            fixture.namespaces["source"][("source",)]["views"] = ["unsupported_view"]
        elif label == "branch":
            loaded = fixture.namespaces["source"][("source",)]["tables"]["measurements"]
            loaded["metadata"]["refs"]["silver_publish_open"] = {"snapshot-id": 2, "type": "branch"}
            loaded["metadata-location"] = fixture.put(loaded["metadata"], 2)
        elif label in {"coverage", "exception"}:
            loaded = fixture.namespaces["source"][("source",)]["tables"]["measurements"]
            loaded["metadata"]["properties"]["write.data.path"] = (
                "s3://synthetic-migration/outside/" if label == "coverage" else "s3://synthetic-migration/lakehouse/catalog_journal/data"
            )
            loaded["metadata-location"] = fixture.put(loaded["metadata"], 2)
        elif label == "property":
            fixture.namespaces["destination"][("source",)]["properties"] = {"owner": "different"}
        elif label == "residue":
            fixture.io = replace(fixture.io, residues=("synthetic_rehearsal_role",))
        elif label == "default":
            fixture.catalog["properties"]["default-base-location"] = "s3://synthetic-migration/outside/"

        def run(fixture: MigrationFixture = fixture) -> None:
            frozen = migration.freeze(fixture.io, deadline=300, wait=fixture.clock.wait)
            migration.refresh(fixture.io, frozen, deadline=300)

        migration_reject(case, run, cause)
        case.check(label + " stops before a mutation", [item for item in fixture.events if item.get("method") in {"POST", "DELETE", "PUT"}], [])


def migration_refresh(case: Scenario) -> None:
    from scripts.lakehouse import migrate_aws_catalog as migration

    fixture = MigrationFixture(case)
    frozen = migration.freeze(fixture.io, deadline=300, wait=fixture.clock.wait)
    fixture.namespaces["destination"].pop(("empty",))
    result = migration.refresh(fixture.io, frozen, deadline=300)
    case.check("refresh produces exact source state", fixture.namespaces["destination"], fixture.namespaces["source"])
    case.check(
        "refresh returns the accepted receipt shape",
        set(result["registration_receipt"]),
        {"schema", "state", "inventory_sha256", "inventory", "registered", "unchanged"},
    )
    drops = [item for item in fixture.events if item.get("method") == "DELETE"]
    case.check("only the proven ancestor is replaced", len(drops), 1)
    case.check("the replacement never purges", "purgeRequested=false" in drops[0]["path"], True)
    case.check(
        "refresh journal captures old and new pointers",
        [(item["old_location"], item["new_location"]) for item in result["refresh_journal"] if item["action"] == "table" and item["old_location"]],
        [
            (
                "s3://synthetic-migration/lakehouse/source/measurements/metadata/1.metadata.json",
                "s3://synthetic-migration/lakehouse/source/measurements/metadata/2.metadata.json",
            )
        ],
    )
    case.check("all owner intents have confirmed completions", result["pending_intents"], [])
    count = len([item for item in fixture.events if item.get("method") in {"POST", "DELETE"}])
    migration.refresh(fixture.io, frozen, deadline=300)
    case.check("an exact retry adds no catalog mutation", len([item for item in fixture.events if item.get("method") in {"POST", "DELETE"}]), count)
    parity = migration.parity(fixture.io, frozen, expected_grants=fixture.grants, deadline=300)
    case.check("all four readings pass after refresh", parity["equal"], True)
    case.check("the final source read equals both frozen reads", migration.final_read(fixture.io, frozen, deadline=300)["equal"], True)
    for fault, code in (
        ("uuid", "refresh_conflict"),
        ("unknown_ancestor", "refresh_conflict"),
        ("extra_table", "membership"),
        ("extra_namespace", "membership"),
    ):
        variant = MigrationFixture(case.scoped(fault))
        baseline = migration.freeze(variant.io, deadline=300, wait=variant.clock.wait)
        tables = variant.namespaces["destination"][("source",)]["tables"]
        if fault == "uuid":
            tables["measurements"]["metadata"]["table-uuid"] = "2468abcd-0000-4000-8000-000000000099"
            tables["measurements"]["metadata-location"] = variant.put(tables["measurements"]["metadata"], 1)
        elif fault == "unknown_ancestor":
            metadata = migration_metadata(3)
            tables["measurements"] = {"metadata-location": variant.put(metadata, 3), "metadata": metadata}
        elif fault == "extra_table":
            tables["extra"] = copy.deepcopy(tables["measurements"])
        else:
            variant.namespaces["destination"][("unexpected",)] = {"properties": {}, "tables": {}, "views": []}
        migration_reject(case, partial(migration.refresh, variant.io, baseline, deadline=300), code)
        case.check("conflicts stop before the first mutation", [item for item in variant.events if item.get("method") in {"POST", "DELETE"}], [])


def migration_interruption(case: Scenario) -> None:
    from scripts.lakehouse import migrate_aws_catalog as migration

    for fault in ("register_failure", "lost_registration_reply"):
        fixture = MigrationFixture(case.scoped(fault))
        frozen = migration.freeze(fixture.io, deadline=300, wait=fixture.clock.wait)
        fixture.faults[fault] = True
        migration_reject(case, partial(migration.refresh, fixture.io, frozen, deadline=300), "transport")
        journal = fixture.io.records.journal
        case.check("interruption leaves a durable intent", len(journal.intents()) > len(journal.completions()), True)
        starts = list((fixture.io.records.root / "refresh").glob("*_start.json"))
        case.check("interruption records the replacement start", bool(starts), True)
        result = migration.refresh(fixture.io, frozen, deadline=300)
        case.check("explicit retry completes the frozen pointer", fixture.namespaces["destination"], fixture.namespaces["source"])
        case.check("retry confirms its pending owner intent", result["pending_intents"], [])
        case.check(
            "registration never includes an overwrite option",
            all(
                set(item["body"]) == {"name", "metadata-location"}
                for item in fixture.events
                if item.get("method") == "POST" and item["path"].endswith("/register")
            ),
            True,
        )

    multiple = MigrationFixture(case.scoped("multiple_tables"), destination_empty=True)
    added = migration_metadata()
    added["table-uuid"] = "2468abcd-0000-4000-8000-000000000003"
    added["location"] = "s3://synthetic-migration/lakehouse/source/additional"
    added["properties"], added["metadata-log"] = {}, []
    multiple.namespaces["source"][("source",)]["tables"]["additional"] = {"metadata-location": multiple.put(added, 2), "metadata": added}
    frozen = migration.freeze(multiple.io, deadline=300, wait=multiple.clock.wait)
    case.check("table listings consume their continuation page", frozen["inventory"]["pages"]["tables:source"], 2)
    migration.refresh(multiple.io, frozen, deadline=300)
    mutations = len([item for item in multiple.events if item.get("method") in {"POST", "DELETE"}])
    migration.refresh(multiple.io, frozen, deadline=300)
    case.check("a namespace with several table completions is safe to retry", multiple.namespaces["destination"], multiple.namespaces["source"])
    case.check("a multi-table retry has no catalog effects", len([item for item in multiple.events if item.get("method") in {"POST", "DELETE"}]), mutations)


def migration_four_readings(case: Scenario) -> None:
    from scripts.lakehouse import migrate_aws_catalog as migration

    differences = {
        "table-uuid": "2468abcd-0000-4000-8000-000000000099",
        "current-snapshot-id": 1,
        "refs": {"main": {"snapshot-id": 2, "type": "branch"}},
        "snapshots": [{"snapshot-id": 2, "parent-snapshot-id": None}],
        "last-sequence-number": 99,
        "format-version": 1,
        "current-schema-id": 99,
        "default-spec-id": 99,
        "default-sort-order-id": 99,
        "location": "s3://synthetic-migration/lakehouse/elsewhere",
        "properties": {"owner": "different"},
    }
    for reading in ("source_catalog", "source_object", "destination_catalog", "destination_object"):
        for field, value in differences.items():
            fixture = MigrationFixture(case.scoped(reading + "_" + field.replace("-", "_")))
            frozen = None
            if reading.startswith("destination"):
                frozen = migration.freeze(fixture.io, deadline=300, wait=fixture.clock.wait)
                migration.refresh(fixture.io, frozen, deadline=300)
            fixture.faults.update({reading: field, "corrupt_value": value})

            def compare(fixture: MigrationFixture = fixture, frozen: dict[str, Any] | None = frozen) -> None:
                if frozen is None:
                    migration.freeze(fixture.io, deadline=300, wait=fixture.clock.wait)
                else:
                    migration.parity(fixture.io, frozen, expected_grants=fixture.grants, deadline=300)

            migration_reject(case, compare, "four_readings")
            case.assertions[-1]["label"] = f"{reading}:{field}: rejection"


def migration_completeness(case: Scenario) -> None:
    from scripts.lakehouse import migrate_aws_catalog as migration

    for label, cause in (
        ("namespace", "membership"),
        ("table", "membership"),
        ("properties", "namespace_properties"),
        ("catalog", "catalog_properties"),
        ("grant_extra", "grant_parity"),
        ("grant_missing", "grant_parity"),
    ):
        fixture = MigrationFixture(case.scoped(label))
        expected_grants = copy.deepcopy(fixture.grants)
        frozen = migration.freeze(fixture.io, deadline=300, wait=fixture.clock.wait)
        migration.refresh(fixture.io, frozen, deadline=300)
        if label == "namespace":
            fixture.namespaces["destination"][("extra",)] = {"properties": {}, "tables": {}, "views": []}
        elif label == "table":
            tables = fixture.namespaces["destination"][("source",)]["tables"]
            tables["extra"] = copy.deepcopy(tables["measurements"])
        elif label == "properties":
            fixture.namespaces["destination"][("empty",)]["properties"] = {}
        elif label == "catalog":
            fixture.catalog["storageConfigInfo"]["stsUnavailable"] = False
        elif label == "grant_extra":
            fixture.grants["reader"].append({"type": "catalog", "privilege": "TABLE_WRITE_DATA"})
        else:
            fixture.grants.pop("reader")
        migration_reject(case, partial(migration.parity, fixture.io, frozen, expected_grants=expected_grants, deadline=300), cause)


def migration_final_source(case: Scenario) -> None:
    from scripts.lakehouse import migrate_aws_catalog as migration

    fixture = MigrationFixture(case)
    frozen = migration.freeze(fixture.io, deadline=300, wait=fixture.clock.wait)
    migration.refresh(fixture.io, frozen, deadline=300)
    fixture.namespaces["source"][("source",)]["tables"].pop("measurements")
    migration_reject(case, lambda: migration.final_read(fixture.io, frozen, deadline=300), "source_changed")


def migration_evidence(case: Scenario) -> None:
    from dataclasses import replace

    from scripts.lakehouse import migrate_aws_catalog as migration

    fixture = MigrationFixture(case)
    marker = "synthetic_secret_do_not_persist"
    fixture.namespaces["source"][("empty",)]["properties"]["owner"] = marker
    fixture.io = replace(fixture.io, secret_values=(marker,))
    migration_reject(case, lambda: migration.freeze(fixture.io, deadline=300, wait=fixture.clock.wait), "evidence_secret")
    case.check("secret rejection creates no inventory artifact", list(fixture.io.records.root.glob("inventory*")), [])
    variant = MigrationFixture(case.scoped("deadline"))
    migration_reject(case, lambda: migration.freeze(variant.io, deadline=100, wait=variant.clock.wait), "deadline")
    case.check("an impossible freeze deadline starts no wait", variant.clock.waits, [])
    remote = MigrationFixture(case.scoped("immutable_remote_artifacts"))
    frozen = migration.freeze(remote.io, deadline=300, wait=remote.clock.wait)
    payload = remote.artifacts.objects["inventory_synthetic_cutover.json"]
    case.check("the remote inventory equals the frozen local inventory", json.loads(payload), frozen)
    case.check("the remote artifact is bound to its byte digest", remote.artifacts.calls[0]["sha256"], hashlib.sha256(payload).hexdigest())
    before = copy.deepcopy(remote.artifacts.objects)
    remote.io.records.save("inventory", frozen, secret_values=(), deadline=300, clock=remote.clock)
    case.check("an identical remote artifact retry changes no object", remote.artifacts.objects, before)
    case.check("every remote artifact write is conditional", {call["condition"] for call in remote.artifacts.calls}, {"*"})


def migration_rebuild(case: Scenario) -> None:
    from scripts.lakehouse import migrate_aws_catalog as migration

    fixture = MigrationFixture(case, destination_empty=True)
    frozen = migration.freeze(fixture.io, deadline=300, wait=fixture.clock.wait)
    states = migration.journal_inventory(frozen)
    epoch, database = "synthetic_epoch", "original_database"
    intents = {"first": {"intent_id": "first", "epoch": epoch, "layer": "source", "database_id": database, "namespaces": ["source"]}}
    changed = copy.deepcopy(states["source"])
    changed["properties"] = {"restored": "before_tables"}
    changed["tables"] = {}
    records = [
        {
            "sequence": 1,
            "epoch": epoch,
            "intent_id": "first",
            "step": "terminal",
            "status": "confirmed",
            "source": {"kind": "live", "database_id": database, "available": True},
            "terminal": True,
            "quiesced": True,
            "namespaces": {"source": changed},
        }
    ]
    result = migration.rebuild(fixture.io, frozen, intents, records, epoch=epoch, quiesced=True, expected_grants=fixture.grants, deadline=300)
    case.check("confirmed deletion removes earlier membership", fixture.namespaces["destination"][("source",)]["tables"], {})
    case.check("namespace properties are restored", fixture.namespaces["destination"][("source",)]["properties"], {"restored": "before_tables"})
    case.check("the empty namespace survives rebuild", fixture.namespaces["destination"][("empty",)]["properties"], {"purpose": "empty"})
    case.check("rebuild compares complete state", result["parity"]["equal"], True)
    case.check("rebuild has no unresolved namespaces", result["unresolved"], [])
    namespace_events = [item for item in fixture.events if item.get("method") == "POST"]
    case.check(
        "every table follows creation of its namespace",
        all(
            any(prior["body"].get("namespace") == ["source", "nested"] for prior in namespace_events[:index])
            for index, item in enumerate(namespace_events)
            if item["path"].endswith("/register")
        ),
        True,
    )
    pending_fixture = MigrationFixture(case.scoped("pending"), destination_empty=True)
    pending_frozen = migration.freeze(pending_fixture.io, deadline=300, wait=pending_fixture.clock.wait)
    pending = {**intents, "pending": {**intents["first"], "intent_id": "pending"}}
    blocked = migration.rebuild(
        pending_fixture.io, pending_frozen, pending, records, epoch=epoch, quiesced=True, expected_grants=pending_fixture.grants, deadline=300
    )
    case.check("pending intent is named", blocked["pending_intents"], ["pending"])
    case.check("pending namespace is not created", ("source",) in pending_fixture.namespaces["destination"], False)
    case.check("pending parent also blocks nested registration", ("source", "nested") in pending_fixture.namespaces["destination"], False)
    case.check("pending intent keeps recovery closed", blocked["state"], "recovering")
    for label, sequence, quiesced, cause in (("gap", 2, True, "journal_sequence"), ("not_quiesced", 1, False, "not_quiesced")):
        variant = MigrationFixture(case.scoped(label), destination_empty=True)
        baseline = migration.freeze(variant.io, deadline=300, wait=variant.clock.wait)
        bad = [{**records[0], "sequence": sequence}]
        migration_reject(
            case,
            partial(migration.rebuild, variant.io, baseline, intents, bad, epoch=epoch, quiesced=quiesced, expected_grants=variant.grants, deadline=300),
            cause,
        )
        case.check("rebuild validation precedes every mutation", [item for item in variant.events if item.get("method") in {"POST", "DELETE"}], [])


def reverse_inputs(case: Scenario) -> tuple[MigrationFixture, dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    from dataclasses import replace

    from scripts.lakehouse import migrate_aws_catalog as migration

    fixture = MigrationFixture(case)
    removed = migration_metadata()
    removed["table-uuid"] = "2468abcd-0000-4000-8000-000000000004"
    removed["location"] = "s3://synthetic-migration/lakehouse/source/removed"
    removed["properties"], removed["metadata-log"] = {}, []
    removed_row: dict[str, Any] = {"metadata-location": fixture.put(removed, 2), "metadata": removed}
    for catalog in fixture.namespaces.values():
        catalog[("source",)]["tables"]["removed"] = copy.deepcopy(removed_row)
        catalog[("vanished",)] = {"properties": {}, "tables": {}, "views": []}
    cutover = migration.freeze(fixture.io, deadline=300, wait=fixture.clock.wait)
    fixture.namespaces["source"][("source",)]["tables"].pop("removed")
    fixture.namespaces["source"].pop(("vanished",))
    fixture.namespaces["source"][("empty",)]["properties"] = {"purpose": "changed"}
    added = copy.deepcopy(removed)
    added["table-uuid"] = "2468abcd-0000-4000-8000-000000000005"
    added["location"] = "s3://synthetic-migration/lakehouse/source/aws_only"
    fixture.namespaces["source"][("source",)]["tables"]["aws_only"] = {"metadata-location": fixture.put(added, 2), "metadata": added}
    records = migration.MigrationRecords(
        case.root / "reverse", "synthetic_reverse", "synthetic_epoch", "reverse_destination", observed_at="2026-10-25T08:05:00Z", clock=fixture.clock
    )
    fixture.io = replace(fixture.io, records=records)
    frozen = migration.freeze(fixture.io, deadline=400, wait=fixture.clock.wait)
    states = migration.journal_inventory(frozen)
    states["vanished"] = namespace({}, present=False)
    intent = {"intent_id": "committed", "epoch": "synthetic_epoch", "layer": "source", "database_id": "original_database", "namespaces": sorted(states)}
    completion = {
        "sequence": 1,
        "epoch": "synthetic_epoch",
        "intent_id": "committed",
        "step": "terminal",
        "status": "confirmed",
        "source": {"kind": "live", "database_id": "original_database", "available": True},
        "terminal": True,
        "quiesced": True,
        "namespaces": states,
    }
    return fixture, cutover, frozen, intent, completion, removed_row


def migration_reverse(case: Scenario) -> None:
    from scripts.lakehouse import migrate_aws_catalog as migration

    fixture, cutover, frozen, intent, completion, removed_row = reverse_inputs(case)
    result = migration.reverse_refresh(
        fixture.io, frozen, cutover, {"committed": intent}, [completion], epoch="synthetic_epoch", quiesced=True, expected_grants=fixture.grants, deadline=400
    )
    case.check("reverse refresh restores complete live source membership", fixture.namespaces["destination"], fixture.namespaces["source"])
    case.check("reverse refresh proves complete parity", result["parity"]["equal"], True)
    case.check("reverse refresh confirms its final source read", result["final_source"]["equal"], True)
    case.check(
        "reverse drops are all non-purging",
        all("purgeRequested=false" in item["path"] for item in fixture.events if item.get("method") == "DELETE" and "/tables/" in item["path"]),
        True,
    )
    case.check("reverse deletion keeps the metadata object", removed_row["metadata-location"] in fixture.object_values, True)

    unknown = MigrationFixture(case.scoped("unconfirmed_deletion"))
    baseline = migration.freeze(unknown.io, deadline=300, wait=unknown.clock.wait)
    unknown.namespaces["destination"][("source",)]["tables"]["unconfirmed"] = copy.deepcopy(removed_row)
    unknown.object_values[removed_row["metadata-location"]] = copy.deepcopy(fixture.object_values[removed_row["metadata-location"]])
    migration_reject(
        case,
        partial(
            migration.reverse_refresh,
            unknown.io,
            baseline,
            baseline,
            {},
            [],
            epoch="synthetic_epoch",
            quiesced=True,
            expected_grants=unknown.grants,
            deadline=300,
        ),
        "reverse_conflict",
    )
    case.check("unconfirmed reverse deletion starts no mutation", [item for item in unknown.events if item.get("method") in {"POST", "DELETE", "PUT"}], [])


def migration_reverse_retry(case: Scenario) -> None:
    from scripts.lakehouse import migrate_aws_catalog as migration

    for fault, action in (
        ("lost_table_delete_reply", "drop_table"),
        ("lost_namespace_delete_reply", "drop_namespace"),
        ("lost_properties_reply", "namespace_properties"),
    ):
        fixture, cutover, frozen, intent, completion, _ = reverse_inputs(case.scoped(fault))
        fixture.faults[fault] = True
        invoke = partial(
            migration.reverse_refresh,
            fixture.io,
            frozen,
            cutover,
            {"committed": intent},
            [completion],
            epoch="synthetic_epoch",
            quiesced=True,
            expected_grants=fixture.grants,
            deadline=400,
        )
        migration_reject(case, invoke, "transport")
        journal = fixture.io.records.journal
        completed = {record["intent_id"] for record in journal.completions() if record["status"] == "confirmed"}
        pending = set(journal.intents()) - completed
        case.check(action + ": the interrupted command is pending", len(pending), 1)
        result = invoke()
        case.check(
            action + ": retry confirms the original intent",
            pending.issubset({record["intent_id"] for record in journal.completions() if record["status"] == "confirmed"}),
            True,
        )
        case.check(action + ": no owner intent stays pending", result["refresh"]["pending_intents"], [])
        case.check(action + ": the destination matches the live source", fixture.namespaces["destination"], fixture.namespaces["source"])
        if action == "drop_table":
            effects = [item for item in fixture.events if item.get("method") == "DELETE" and "/tables/removed?" in item["path"]]
        elif action == "drop_namespace":
            effects = [item for item in fixture.events if item.get("method") == "DELETE" and item["path"].endswith("/namespaces/vanished")]
        else:
            effects = [item for item in fixture.events if item.get("method") == "POST" and item["path"].endswith("/namespaces/empty/properties")]
        case.check(action + ": the successful effect is not repeated", len(effects), 1)


def migration_frozen_change(case: Scenario) -> None:
    from scripts.lakehouse import migrate_aws_catalog as migration

    fixture = MigrationFixture(case)

    def change_during_wait(duration: float) -> None:
        fixture.clock.wait(duration)
        fixture.namespaces["source"][("empty",)]["properties"]["purpose"] = "changed_between_reads"

    migration_reject(case, partial(migration.freeze, fixture.io, deadline=300, wait=change_during_wait), "source_changed")
    case.assertions[-1]["label"] = "unequal frozen reads are refused"
    case.check("unequal reads publish no frozen artifact", fixture.artifacts.objects, {})


def migration_final_property(case: Scenario) -> None:
    from scripts.lakehouse import migrate_aws_catalog as migration

    fixture = MigrationFixture(case)
    frozen = migration.freeze(fixture.io, deadline=300, wait=fixture.clock.wait)
    fixture.namespaces["source"][("empty",)]["properties"]["purpose"] = "changed_after_freeze"
    migration_reject(case, partial(migration.final_read, fixture.io, frozen, deadline=300), "source_changed")
    case.assertions[-1]["label"] = "the final comparison refuses a property change"


def migration_parity_missing(case: Scenario) -> None:
    from scripts.lakehouse import migrate_aws_catalog as migration

    for missing in ("empty_namespace", "table", "nested_namespace"):
        fixture = MigrationFixture(case.scoped(missing))
        frozen = migration.freeze(fixture.io, deadline=300, wait=fixture.clock.wait)
        migration.refresh(fixture.io, frozen, deadline=300)
        if missing == "table":
            fixture.namespaces["destination"][("source", "nested")]["tables"].pop("nested_table")
        else:
            fixture.namespaces["destination"].pop(("empty",) if missing == "empty_namespace" else ("source", "nested"))
        migration_reject(case, partial(migration.parity, fixture.io, frozen, expected_grants=fixture.grants, deadline=300), "membership")
        case.assertions[-1]["label"] = missing + ": complete parity refuses missing membership"


def migration_hierarchy(case: Scenario, operation: str) -> None:
    from scripts.lakehouse import migrate_aws_catalog as migration

    fixture = MigrationFixture(case, destination_empty=True)
    status, _ = fixture.request("destination")("POST", fixture.io.destination.base, body={"namespace": ["missing", "child"], "properties": {}}, deadline=300)
    case.check("the stand-in refuses a child without its parent", status, 404)
    fixture.namespaces["source"][("source", "nested", "leaf")] = {"properties": {"level": "grandchild"}, "tables": {}, "views": []}
    frozen = migration.freeze(fixture.io, deadline=300, wait=fixture.clock.wait)
    if operation == "refresh":
        migration.refresh(fixture.io, frozen, deadline=300)
    else:
        migration.rebuild(fixture.io, frozen, {}, [], epoch="synthetic_epoch", quiesced=True, expected_grants=fixture.grants, deadline=300)
    case.check(operation + ": complete nested state is restored", fixture.namespaces["destination"], fixture.namespaces["source"])
    created = [item["body"]["namespace"] for item in fixture.events if item.get("method") == "POST" and item["path"] == fixture.io.destination.base]
    case.check(
        operation + ": the parent precedes its child",
        created.index(["source"]) < created.index(["source", "nested"]) < created.index(["source", "nested", "leaf"]),
        True,
    )


def migration_refresh_hierarchy(case: Scenario) -> None:
    migration_hierarchy(case, "refresh")


def migration_rebuild_hierarchy(case: Scenario) -> None:
    migration_hierarchy(case, "rebuild")


def migration_source_type(case: Scenario) -> None:
    from dataclasses import replace

    fixture = MigrationFixture(case)
    migration_reject(case, partial(replace, fixture.io, source=fixture.io.destination), "source_read_only")
    case.assertions[-1]["label"] = "a writable catalog is refused as the source"
    case.check("source validation makes no transport call", fixture.events, [])


def migration_source_method(case: Scenario) -> None:
    fixture = MigrationFixture(case)
    migration_reject(
        case, partial(fixture.io.source._request, "POST", fixture.io.source.base, body={"namespace": ["unwanted"]}, deadline=300), "source_read_only"
    )
    case.assertions[-1]["label"] = "the source transport refuses a mutation"
    case.check("a refused source mutation never reaches transport", fixture.events, [])


def migration_unchanged_disappears(case: Scenario) -> None:
    from scripts.lakehouse import migrate_aws_catalog as migration

    fixture = MigrationFixture(case)
    fixture.namespaces["destination"] = copy.deepcopy(fixture.namespaces["source"])
    frozen = migration.freeze(fixture.io, deadline=300, wait=fixture.clock.wait)
    fixture.faults["remove_before_unchanged_read"] = True
    migration_reject(case, partial(migration.refresh, fixture.io, frozen, deadline=300), "refresh_conflict")
    case.assertions[-1]["label"] = "an unchanged table that disappears is refused"
    case.check(
        "unchanged verification makes no registration request",
        [item for item in fixture.events if item.get("method") == "POST" and item["path"].endswith("/register")],
        [],
    )
    case.check("unchanged verification does not restore a missing table", "measurements" in fixture.namespaces["destination"][("source",)]["tables"], False)


def migration_branch_prefix(case: Scenario) -> None:
    from dataclasses import replace

    from scripts.lakehouse import migrate_aws_catalog as migration

    fixture = MigrationFixture(case)
    fixture.io = replace(fixture.io, scope=replace(fixture.scope, publish_branch_prefixes=("edition_write_",)))
    loaded = fixture.namespaces["source"][("source",)]["tables"]["measurements"]
    loaded["metadata"]["refs"]["edition_write_open"] = {"snapshot-id": 2, "type": "branch"}
    loaded["metadata-location"] = fixture.put(loaded["metadata"], 2)
    migration_reject(case, partial(migration.freeze, fixture.io, deadline=300, wait=fixture.clock.wait), "open_publish_branch")
    case.assertions[-1]["label"] = "configured publication prefixes are enforced"


def migration_freeze_timing(case: Scenario) -> None:
    from scripts.lakehouse import migrate_aws_catalog as migration

    fixture = MigrationFixture(case)
    frozen = migration.freeze(fixture.io, deadline=400, wait=lambda _duration: fixture.clock.wait(147.25))
    case.check("the frozen separation is measured", frozen["separation_seconds"], 147.25)


def migration_command_timing(case: Scenario) -> None:
    from dataclasses import replace
    from datetime import datetime

    from scripts.lakehouse import migrate_aws_catalog as migration

    fixture = MigrationFixture(case)
    records = migration.MigrationRecords(
        case.root / "timed", "timed_cutover", "synthetic_epoch", "destination_database", observed_at="2026-10-25T08:00:00Z", clock=fixture.clock
    )
    fixture.io = replace(fixture.io, records=records)
    frozen = migration.freeze(fixture.io, deadline=400, wait=fixture.clock.wait)
    fixture.faults["request_step"] = 0.25
    migration.refresh(fixture.io, frozen, deadline=400)
    entries = records.entries()
    for entry in entries:
        case.check("a command end records elapsed clock time", datetime.fromisoformat(entry["end"]) > datetime.fromisoformat(entry["start"]), True)
    migration.refresh(fixture.io, frozen, deadline=400)
    case.check("a retry preserves its recorded timestamps", records.entries(), entries)


def migration_replay_types(case: Scenario) -> None:
    from scripts.lakehouse import migrate_aws_catalog as migration

    for problem in ("intent_shape", "source_shape", "namespace_shape", "quiesced_type", "available_type", "state_type"):
        fixture, cutover, _, intent, completion, _ = reverse_inputs(case.scoped(problem))
        intents: dict[str, Any] = {"committed": copy.deepcopy(intent)}
        records = [copy.deepcopy(completion)]
        if problem == "intent_shape":
            intents["committed"] = None
        elif problem == "source_shape":
            records[0]["source"] = None
        elif problem == "namespace_shape":
            records[0]["namespaces"] = None
        elif problem == "quiesced_type":
            records[0]["quiesced"] = 1
        elif problem == "available_type":
            records[0]["source"]["available"] = 1
        else:
            records[0]["namespaces"]["source"]["state"] = []
        migration_reject(
            case,
            partial(
                migration.rebuild, fixture.io, cutover, intents, records, epoch="synthetic_epoch", quiesced=True, expected_grants=fixture.grants, deadline=400
            ),
            "journal_replay",
        )
        case.assertions[-1]["label"] = problem + ": invalid journal input is refused"
        case.check("invalid journal input starts no mutation", [item for item in fixture.events if item.get("method") in {"POST", "DELETE"}], [])


def migration_runner_error(case: Scenario) -> None:
    from unittest.mock import patch

    from scripts.lakehouse import run_catalog_migration_e2e as runner

    def broken(_case: Scenario) -> None:
        raise KeyError("synthetic_programming_error")

    output = case.root / "failed_child_run"
    propagated = False
    with (
        patch.dict(runner.MIGRATION_CASES, {"migration_evidence": broken}),
        patch.object(sys, "argv", ["migration_runner", "--output", str(output), "--case", "migration_evidence"]),
    ):
        try:
            runner.main()
        except KeyError:
            propagated = True
    case.check("the runner propagates an unexpected programming error", propagated, True)
    report = json.loads((output / "report.json").read_text())
    case.check("an unexpected error retains a failed artifact", report["passed"], False)
    case.check("the retained artifact identifies the unhandled error", report["cases"][0]["error"], "unhandled KeyError")


def migration_short_freeze(case: Scenario) -> None:
    from scripts.lakehouse import migrate_aws_catalog as migration

    fixture = MigrationFixture(case)
    migration_reject(case, partial(migration.freeze, fixture.io, deadline=300, wait=lambda _duration: fixture.clock.wait(30.0)), "freeze_interval")
    case.assertions[-1]["label"] = "an early wait cannot freeze the source"
    case.check("a short wait writes no frozen artifact", fixture.artifacts.objects, {})


def migration_rebuild_membership(case: Scenario) -> None:
    from scripts.lakehouse import migrate_aws_catalog as migration

    fixture = MigrationFixture(case.scoped("extra_before_rebuild"), destination_empty=True)
    frozen = migration.freeze(fixture.io, deadline=300, wait=fixture.clock.wait)
    metadata = migration_metadata()
    metadata["table-uuid"] = "2468abcd-0000-4000-8000-000000000006"
    metadata["location"] = "s3://synthetic-migration/lakehouse/source/unexpected"
    metadata["properties"], metadata["metadata-log"] = {}, []
    for snapshot in metadata["snapshots"]:
        snapshot["manifest-list"] = f"{metadata['location']}/metadata/snap_{snapshot['snapshot-id']}.avro"
    fixture.namespaces["destination"][("source",)] = {
        "properties": {"owner": "synthetic"},
        "tables": {"unexpected": {"metadata-location": fixture.put(metadata, 2), "metadata": metadata}},
        "views": [],
    }
    before = copy.deepcopy(fixture.namespaces["destination"])
    migration_reject(
        case,
        partial(migration.rebuild, fixture.io, frozen, {}, [], epoch="synthetic_epoch", quiesced=True, expected_grants=fixture.grants, deadline=300),
        "membership",
    )
    case.check("the rebuild precheck prevents every destination mutation", [item for item in fixture.events if item.get("method") in {"POST", "DELETE"}], [])
    case.check("the rebuild precheck preserves the existing destination", fixture.namespaces["destination"], before)
    case.check("the rebuild precheck creates no owner intent", fixture.io.records.journal.intents(), {})

    final = MigrationFixture(case.scoped("missing_final_namespace"), destination_empty=True)
    baseline = migration.freeze(final.io, deadline=300, wait=final.clock.wait)
    final.faults["omit_empty_after_rebuild"] = True
    migration_reject(
        case,
        partial(migration.rebuild, final.io, baseline, {}, [], epoch="synthetic_epoch", quiesced=True, expected_grants=final.grants, deadline=300),
        "membership",
    )
    case.assertions[-1]["label"] = "complete final rebuild membership is required"
    case.check("the omission is injected after table registration", final.faults.get("omitted_final_namespace"), True)


MIGRATION_CASES = {
    "migration_inventory": migration_inventory,
    "migration_readiness": migration_readiness,
    "migration_refresh": migration_refresh,
    "migration_interruption": migration_interruption,
    "migration_four_readings": migration_four_readings,
    "migration_completeness": migration_completeness,
    "migration_final_source": migration_final_source,
    "migration_evidence": migration_evidence,
    "migration_rebuild": migration_rebuild,
    "migration_reverse": migration_reverse,
    "migration_reverse_retry": migration_reverse_retry,
    "migration_frozen_change": migration_frozen_change,
    "migration_final_property": migration_final_property,
    "migration_parity_missing": migration_parity_missing,
    "migration_refresh_hierarchy": migration_refresh_hierarchy,
    "migration_rebuild_hierarchy": migration_rebuild_hierarchy,
    "migration_source_type": migration_source_type,
    "migration_source_method": migration_source_method,
    "migration_unchanged_disappears": migration_unchanged_disappears,
    "migration_branch_prefix": migration_branch_prefix,
    "migration_freeze_timing": migration_freeze_timing,
    "migration_command_timing": migration_command_timing,
    "migration_replay_types": migration_replay_types,
    "migration_runner_error": migration_runner_error,
    "migration_short_freeze": migration_short_freeze,
    "migration_rebuild_membership": migration_rebuild_membership,
}
