"""Check proposed task schemas and an in-memory storage model with synthetic bytes.

Run as a module with --report data/e2e/ecs_target/<run>/report.json. This checks no
launcher, HTTP transport, image, engine or AWS service.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import sys
import time
from collections.abc import Callable
from datetime import UTC, datetime
from importlib.metadata import version
from pathlib import Path
from typing import Any
from unittest.mock import patch

from jsonschema import Draft202012Validator
from referencing import Registry, Resource

from scripts.orchestration.ecs_fake_s3 import FakeS3, Principal, S3Refusal
from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]
CONTRACTS = ROOT / "data_contracts/orchestration"
SCHEMAS = ("ecs_input_map", "ecs_job", "ecs_outputs")
PREFIX = "lakehouse/run_stage/20261024T000000Z-a1b2c3d4/g1/"
TRY = PREFIX + "tries/try_one/"
PROJECT = Principal("project", "synthetic_project")
TASK = Principal("task", "AROASYNTHETIC:task_one")
WRITER = Principal("writer", "synthetic_writer", TRY, 100.0)
MARKER = "synthetic_fixture_value"


class CaseFailure(RuntimeError):
    pass


def require(condition: bool, message: str) -> None:
    if not condition:
        raise CaseFailure(message)


def refused(code: str, action: Callable[[], Any]) -> None:
    try:
        action()
    except S3Refusal as error:
        require(error.code == code, f"expected {code}, received {error.code}")
    else:
        raise CaseFailure(f"expected {code}, received success")


def validators(overrides: dict[str, Any] | None = None) -> dict[str, Draft202012Validator]:
    documents = {name: json.loads((CONTRACTS / f"{name}.schema.json").read_text()) for name in SCHEMAS}
    documents.update(overrides or {})
    registry: Registry[Any] = Registry().with_resources((document["$id"], Resource.from_contents(document)) for document in documents.values())
    result = {}
    for name, document in documents.items():
        Draft202012Validator.check_schema(document)
        result[name] = Draft202012Validator(document, registry=registry)
    return result


def schema_cases(overrides: dict[str, Any] | None = None) -> None:
    checks = validators(overrides)
    examples = {name: json.loads((CONTRACTS / "examples" / f"{name}.json").read_text()) for name in SCHEMAS}
    for name, example in examples.items():
        require(checks[name].is_valid(example), f"the {name} example must validate")

    def bad(name: str, mutate: Callable[[Any], None], field: tuple[str | int, ...], rule: str) -> None:
        example = copy.deepcopy(examples[name])
        mutate(example)
        errors = list(checks[name].iter_errors(example))
        require(any(tuple(error.absolute_path) == field and error.validator == rule for error in errors), f"{name}: expected {rule} at {field}")

    for spelling in ("../outside", "/absolute", "a/../b", "a//b", "a/%2e%2e/b", "a\\b", "a/./b", "path\n"):

        def bad_path(rows: Any, value: str = spelling) -> None:
            rows[0]["path"] = value

        bad("ecs_input_map", bad_path, (0, "path"), "pattern")
    bad("ecs_input_map", lambda rows: rows[0].pop("version_id"), (0,), "required")
    bad("ecs_input_map", lambda rows: rows[0].update(version_id="null"), (0, "version_id"), "not")
    bad("ecs_input_map", lambda rows: rows[0].update(size=-1), (0, "size"), "minimum")
    bad("ecs_input_map", lambda rows: rows[0].update(sha256="a" * 64 + "\n"), (0, "sha256"), "pattern")
    bad("ecs_input_map", lambda rows: rows[0].update(secret=MARKER), (0,), "additionalProperties")
    bad("ecs_job", lambda doc: doc["plain_environment"].update(AWS_PROFILE="synthetic"), ("plain_environment",), "additionalProperties")
    bad("ecs_job", lambda doc: doc["plain_environment"].update(AWS_SECRET_ACCESS_KEY=MARKER), ("plain_environment",), "additionalProperties")
    bad("ecs_job", lambda doc: doc["secret_names"].append("POLARIS_READER_CLIENT_SECRET"), ("secret_names",), "maxItems")
    bad("ecs_job", lambda doc: doc.update(job_class="lakehouse_write"), ("job_class",), "enum")
    bad("ecs_job", lambda doc: doc.update(max_record_bytes=8388608), ("max_record_bytes",), "const")
    bad("ecs_job", lambda doc: doc["declaration_source"].pop("schema_sha256"), ("declaration_source",), "required")
    bad("ecs_outputs", lambda doc: doc.pop("job_spec_sha256"), (), "required")
    bad("ecs_outputs", lambda doc: doc.pop("input_map_sha256"), (), "required")
    bad("ecs_outputs", lambda doc: doc["objects"][0].update(size=33554433), ("objects", 0, "size"), "maximum")
    bad("ecs_outputs", lambda doc: doc.update(exit_code=72), ("objects", 0, "name"), "enum")
    bad("ecs_outputs", lambda doc: doc["objects"][0].update(name="../result.json"), ("objects", 0, "name"), "pattern")
    for size in (7999899, 8388609, 33554431, 33554432):
        document = copy.deepcopy(examples["ecs_outputs"])
        document["objects"][0]["size"] = size
        require(checks["ecs_outputs"].is_valid(document), f"a permitted record of {size} bytes must validate")
    data = copy.deepcopy(examples["ecs_outputs"])
    data["objects"][0].update(kind="data", size=33554433)
    require(checks["ecs_outputs"].is_valid(data), "the controller bound must not cap streamed data objects")
    job_bytes = (CONTRACTS / "examples/ecs_job.json").read_bytes()
    require(hashlib.sha256(job_bytes).hexdigest() == examples["ecs_outputs"]["job_spec_sha256"], "the marker must pin the saved job bytes")
    canonical = json.dumps(examples["ecs_input_map"], sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
    require(hashlib.sha256(canonical).hexdigest() == examples["ecs_job"]["input_map_sha256"], "the example must pin canonical input-map bytes")
    fixture = (CONTRACTS / "examples/ecs_fixture.txt").read_bytes()
    require(len(fixture) == examples["ecs_input_map"][0]["size"], "the input example must name the fixture's actual size")
    require(hashlib.sha256(fixture).hexdigest() == examples["ecs_input_map"][0]["sha256"], "the input example must name the fixture's actual hash")
    required = {row["name"] for row in examples["ecs_job"]["expected_outputs"]["required"]}
    require({row["name"] for row in examples["ecs_outputs"]["objects"]} == required, "the example marker must include every required result")


def put_conditions() -> None:
    store = FakeS3(PREFIX)
    key = TRY + "out/result.json"
    first = store.put(WRITER, key, b"first", "*", {"sha256": "client_metadata"})
    refused("PreconditionFailed", lambda: store.put(WRITER, key, b"other", "*", {}))
    refused("AccessDenied", lambda: store.put(WRITER, TRY + "out/missing.json", b"body", None, {}))
    require(store.get(TASK, key, first.version_id) == b"first", "a refused put must preserve existing bytes")
    require(store.head(TASK, key).metadata["sha256"] == "client_metadata", "the model must not repair client metadata")
    require(len(store.versions(PROJECT, key)) == 1, "conditional failures must add no versions")


def principal_scopes() -> None:
    store = FakeS3(PREFIX)
    store.put(WRITER, TRY + "out/result.json", b"own", "*", {})
    refused("AccessDenied", lambda: store.put(WRITER, PREFIX + "tries/try_two/out/result.json", b"foreign", "*", {}))
    refused("AccessDenied", lambda: store.put(WRITER, TRY + "job.json", b"job", "*", {}))
    refused("AccessDenied", lambda: store.put(TASK, TRY + "out/task.json", b"task", "*", {}))
    escaped = Principal("writer", "synthetic_writer", PREFIX + "inputs/", 100.0)
    refused("AccessDenied", lambda: store.put(escaped, PREFIX + "inputs/code.tar", b"code", "*", {}))
    refused("AccessDenied", lambda: store.get(WRITER, TRY + "out/result.json"))
    refused("AccessDenied", lambda: store.list_uploads(TASK, PREFIX))
    refused("AccessDenied", lambda: store.put(PROJECT, PREFIX + "../other/result.json", b"bad", "*", {}))


def grant_reads() -> None:
    store = FakeS3(PREFIX)
    own = PREFIX + "grants/" + TASK.user_id + ".json"
    producer = PREFIX + "grants/AROASYNTHETIC:producer.json"
    sibling = PREFIX + "grants/AROASYNTHETIC:task_two.json"
    for key in (own, producer, sibling):
        store.put(PROJECT, key, b"synthetic grant object", "*", {})
    require(store.get(TASK, own) == b"synthetic grant object", "the supplied own identity must read its grant")
    refused("AccessDenied", lambda: store.get(TASK, producer))
    refused("AccessDenied", lambda: store.head(TASK, sibling))
    refused("AccessDenied", lambda: store.get(TASK, PREFIX + "grants/missing.json"))


def pinned_versions() -> None:
    store = FakeS3(PREFIX)
    key = PREFIX + "inputs/files/synthetic"
    first = store.seed(key, b"old", {"sha256": "copied_metadata"})
    second = store.seed(key, b"new", {"sha256": "copied_metadata"})
    require(first.version_id != second.version_id, "a replacement must receive a different version")
    require(store.get(TASK, key, first.version_id) == b"old", "a pinned read must not follow the current version")
    require(store.get(TASK, key, second.version_id) == b"new", "the positive replacement read must reach new bytes")
    require(store.head(TASK, key, first.version_id).metadata == store.head(TASK, key, second.version_id).metadata, "copied metadata is not identity")
    refused("NoSuchVersion", lambda: store.get(PROJECT, key, "missing-version"))
    refused("AccessDenied", lambda: store.get(TASK, "lakehouse/another_source/file", first.version_id))


def checksummed_multipart() -> None:
    store = FakeS3(PREFIX)
    key = TRY + "out/parts.bin"
    upload = store.create_upload(WRITER, key, {})
    refused("BadDigest", lambda: store.upload_part(WRITER, upload, 1, b"alpha", "incorrect"))
    require(store.list_parts(WRITER, upload) == [], "a rejected part must not appear")
    first_part = b"alpha" * (1 << 20)
    left = store.upload_part(WRITER, upload, 1, first_part, store.checksum(first_part))
    right = store.upload_part(WRITER, upload, 2, b"beta", store.checksum(b"beta"))
    refused("InvalidPartOrder", lambda: store.complete(WRITER, upload, [(2, right), (1, left)], "*"))
    refused("InvalidPart", lambda: store.complete(WRITER, upload, [(1, "wrong"), (2, right)], "*"))
    refused("AccessDenied", lambda: store.complete(WRITER, upload, [(1, left), (2, right)], None))
    head = store.complete(WRITER, upload, [(1, left), (2, right)], "*")
    require(store.get(TASK, key, head.version_id) == first_part + b"beta", "completion must concatenate the submitted verified parts")
    require(store.list_uploads(PROJECT, PREFIX)[0] == [], "a completed upload must leave no active upload")


def conflicting_completion() -> None:
    store = FakeS3(PREFIX)
    key = TRY + "out/conflict.bin"
    old = store.create_upload(WRITER, key, {})
    digest = store.upload_part(WRITER, old, 1, b"first", store.checksum(b"first"))
    store.conflict_on_complete(old)
    refused("ConditionalRequestConflict", lambda: store.complete(WRITER, old, [(1, digest)], "*"))
    refused("NoSuchUpload", lambda: store.complete(WRITER, old, [(1, digest)], "*"))
    store.abort(WRITER, old)
    fresh = store.create_upload(WRITER, key, {})
    require(fresh != old, "a restart must use a fresh upload identifier")
    refused("InvalidPart", lambda: store.complete(WRITER, fresh, [(1, digest)], "*"))
    uploaded = store.upload_part(WRITER, fresh, 1, b"first", store.checksum(b"first"))
    head = store.complete(WRITER, fresh, [(1, uploaded)], "*")
    require(store.get(TASK, key, head.version_id) == b"first", "a fresh upload must receive every part")


def competing_completions() -> None:
    store = FakeS3(PREFIX)
    key = TRY + "out/competing.bin"
    first = store.create_upload(WRITER, key, {})
    second = store.create_upload(WRITER, key, {})
    for upload in (first, second):
        store.upload_part(WRITER, upload, 1, b"same", store.checksum(b"same"))
    part = [(1, store.checksum(b"same"))]
    store.complete(WRITER, first, part, "*")
    refused("PreconditionFailed", lambda: store.complete(WRITER, second, part, "*"))
    require(len(store.versions(PROJECT, key)) == 1, "competing completions must not replace the first version")
    store.abort(WRITER, second)


def paged_aborts() -> None:
    store = FakeS3(PREFIX)
    expected = {store.create_upload(WRITER, TRY + f"out/part_{index}", {}) for index in range(3)}
    found: set[str] = set()
    token = None
    for _ in range(3):
        page, token = store.list_uploads(PROJECT, PREFIX, token, 1)
        found.update(page)
        if token is None:
            break
    require(found == expected and token is None, "paging must expose every upload exactly once")
    for upload in sorted(found):
        store.abort(WRITER, upload)
    require(store.list_uploads(PROJECT, PREFIX)[0] == [], "the complete abort sweep must leave no uploads")
    refused("AccessDenied", lambda: store.list_uploads(PROJECT, "lakehouse/another_run/"))


def retained_versions_and_expiry() -> None:
    store = FakeS3(PREFIX)
    key = TRY + "out/retained.json"
    expired = Principal("writer", "synthetic_writer", TRY, 5.0)
    refused("ExpiredToken", lambda: store.put(expired, key, b"bad", "*", {}, now=5.0))
    first = store.put(expired, key, b"old", "*", {}, now=4.0)
    store.delete_current(PROJECT, key)
    refused("NoSuchKey", lambda: store.get(PROJECT, key))
    require(store.get(TASK, key, first.version_id) == b"old", "deletion must retain an explicitly pinned old version")
    require(len(store.versions(PROJECT, key)) == 2, "current deletion must append a marker, not purge versions")


MODEL_CASES = {
    "conditional writes preserve prior bytes": put_conditions,
    "role and session scopes intersect": principal_scopes,
    "grant reads use the supplied exact identity": grant_reads,
    "pinned versions resist same-size replacement": pinned_versions,
    "multipart verifies checksums, order and conditions": checksummed_multipart,
    "conflict requires a fresh upload and all parts": conflicting_completion,
    "competing completions leave one version": competing_completions,
    "paged aborts cover the complete upload set": paged_aborts,
    "expiry refuses writes and deletion retains versions": retained_versions_and_expiry,
}


def outcomes(cases: dict[str, Callable[[], None]], deadline: float) -> dict[str, dict[str, Any]]:
    result = {}
    for name, case in cases.items():
        try:
            require(time.monotonic() < deadline, "the preparation check exceeded its 30-second deadline")
            case()
        except (CaseFailure, S3Refusal) as error:
            result[name] = {"passed": False, "reason": str(error)}
        else:
            result[name] = {"passed": True}
    return result


def planted_defects(deadline: float) -> dict[str, Any]:
    defects: dict[str, tuple[str, Callable[..., Any]]] = {
        "permit every task-role operation": ("_task_allowed", lambda *_: True),
        "ignore the writer session boundary": ("_writer_allowed", lambda *_: True),
        "ignore conditional headers and conflicts": ("_conditional", lambda *_: None),
        "trust an invalid part checksum": ("_verify_checksum", lambda *_: None),
        "return the latest version for a pinned read": ("_find_version", lambda self, key, version_id: self._objects[key][-1]),
        "accept completion without restarting after conflict": ("_completion_fault", lambda *_: None),
        "purge old versions during current deletion": ("delete_current", lambda self, principal, key: self._objects.pop(key, None)),
    }
    expected_causes = {
        "permit every task-role operation": ("grant reads use the supplied exact identity", "expected AccessDenied, received success"),
        "ignore the writer session boundary": ("role and session scopes intersect", "expected AccessDenied, received success"),
        "ignore conditional headers and conflicts": ("conditional writes preserve prior bytes", "expected PreconditionFailed, received success"),
        "trust an invalid part checksum": ("multipart verifies checksums, order and conditions", "expected BadDigest, received success"),
        "return the latest version for a pinned read": ("pinned versions resist same-size replacement", "a pinned read must not follow the current version"),
        "accept completion without restarting after conflict": (
            "conflict requires a fresh upload and all parts",
            "expected ConditionalRequestConflict, received success",
        ),
        "purge old versions during current deletion": ("expiry refuses writes and deletion retains versions", "AccessDenied"),
    }
    result: dict[str, Any] = {}
    for name, (method, implementation) in defects.items():
        with patch.object(FakeS3, method, implementation):
            observed = outcomes(MODEL_CASES, deadline)
        caught = {case: record["reason"] for case, record in observed.items() if not record["passed"]}
        expected_case, expected_reason = expected_causes[name]
        result[name] = {
            "caught": caught.get(expected_case) == expected_reason,
            "expected_case": expected_case,
            "expected_reason": expected_reason,
            "caught_by": caught,
        }
    documents = {name: json.loads((CONTRACTS / f"{name}.schema.json").read_text()) for name in SCHEMAS}
    mutations = {
        "allow an oversized controller record": (
            "ecs_outputs",
            lambda doc: doc["$defs"]["object"]["allOf"][0]["then"]["properties"]["size"].update(maximum=33554433),
        ),
        "allow an unversioned input": ("ecs_input_map", lambda doc: doc["$defs"]["file"]["required"].remove("version_id")),
        "allow a writer in the read-task schema": ("ecs_job", lambda doc: doc["properties"]["job_class"]["enum"].append("lakehouse_write")),
    }
    schema_reasons = {
        "allow an oversized controller record": "ecs_outputs: expected maximum at ('objects', 0, 'size')",
        "allow an unversioned input": "ecs_input_map: expected required at (0,)",
        "allow a writer in the read-task schema": "ecs_job: expected enum at ('job_class',)",
    }
    for name, (schema, mutate) in mutations.items():
        document = copy.deepcopy(documents[schema])
        mutate(document)

        def check_schema(schema_name: str = schema, mutated: dict[str, Any] = document) -> None:
            schema_cases({schema_name: mutated})

        observed = outcomes({"schema refusal cases": check_schema}, deadline)
        reason = observed["schema refusal cases"].get("reason")
        result[name] = {"caught": reason == schema_reasons[name], "expected_reason": schema_reasons[name], "caught_by": observed}
    return result


def fingerprints() -> dict[str, str]:
    files = [CONTRACTS / f"{name}.schema.json" for name in SCHEMAS]
    files += [CONTRACTS / "examples" / f"{name}.json" for name in SCHEMAS]
    files.append(CONTRACTS / "examples/ecs_fixture.txt")
    files += [Path(__file__), Path(__file__).with_name("ecs_fake_s3.py")]
    return {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest() for path in files}


def main() -> int:
    """Save bounded synthetic schema and storage checks, including in-memory mutation evidence."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    destination = args.report.resolve()
    if not destination.is_relative_to(ROOT / "data/e2e/ecs_target"):
        parser.error("the report must stay under data/e2e/ecs_target in this worktree")
    started = time.monotonic()
    deadline = started + 30.0
    frozen = fingerprints()
    observed = outcomes({"proposed schemas and examples": schema_cases, **MODEL_CASES}, deadline)
    defects = planted_defects(deadline)
    restored = outcomes({"proposed schemas and examples": schema_cases, **MODEL_CASES}, deadline)
    unchanged = frozen == fingerprints()
    passed = unchanged and all(row["passed"] for row in observed.values()) and all(row["caught"] for row in defects.values())
    passed = passed and all(row["passed"] for row in restored.values())
    commit = run_command("git", ["rev-parse", "HEAD"], cwd=ROOT, timeout=5, check=True).stdout.strip()
    created = datetime.now(UTC).isoformat()
    record = {
        "created_utc": created,
        "classification": "Synthetic",
        "proof": "in_memory_model_and_schema_only",
        "base_commit": commit,
        "uncommitted_candidate_sha256": frozen,
        "candidate_unchanged": unchanged,
        "reproduce": f"python -B -m scripts.orchestration.ecs_stage_contract_cases --report {destination.relative_to(ROOT)}",
        "dependencies": {"python": sys.version.split()[0], "jsonschema": version("jsonschema"), "referencing": version("referencing")},
        "cases": observed,
        "planted_defects": defects,
        "restored_cases": restored,
        "passed": passed,
        "duration_seconds": time.monotonic() - started,
        "limits": [
            "No HTTP client or request authentication",
            "No launcher, worker, bootstrap or image execution",
            "No policy interpreter or live identity premise",
            "No real source rows or credential access",
            "No complete numbered case acceptance",
        ],
        "cleanup": "done",
    }
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(record, indent=2, sort_keys=True) + "\n")
    retention = {
        "class": 4,
        "purpose": "Synthetic schema, storage model and mutation check summary",
        "created": created,
        "end_condition": "The coordinating agent no longer cites this candidate's preparation checks",
    }
    destination.with_name("retention.json").write_text(json.dumps(retention, indent=2, sort_keys=True) + "\n")
    sys.stdout.write(
        json.dumps({"passed": passed, "checks": len(observed), "planted_defects": len(defects), "report": str(destination.relative_to(ROOT))}) + "\n"
    )
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
