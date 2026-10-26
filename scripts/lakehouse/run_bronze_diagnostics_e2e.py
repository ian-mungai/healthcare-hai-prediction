"""Check bronze diagnostics with synthetic metadata and manifest-only command adapters."""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from orchestration import launcher
from scripts.lakehouse import bronze, bronze_metadata, catalog, dictionary
from scripts.quality.model_metadata import load_yaml

TABLE = {"table": "files", "collection": "cms/synthetic", "roles": ["data"], "format": "csv", "encodings": ["utf-8"], "member_pattern": r"\.csv$"}
CONTENT_SENTINEL = "SYNTHETIC_CONTENT_MUST_NOT_APPEAR"


def manifest(snapshot: str, key: str, version: str = "v1") -> dict[str, Any]:
    """Return a synthetic manifest with an exact physical reference and a content sentinel."""
    return {
        "snapshot_id": snapshot,
        "source_id": "synthetic",
        "release_id": "2026-01-01",
        "objects": [
            {
                "role": "data",
                "dataset_id": "files",
                "member_chain": ["file.csv"],
                "content_sentinel": CONTENT_SENTINEL,
                "object": {"bucket": "synthetic", "key": key, "version_id": version, "sha256": "a" * 64, "byte_count": 17},
            }
        ],
    }


def item(name: str) -> dict[str, Any]:
    document = manifest(name, "cms/synthetic/datasets/files/release_date=2026-01-01/" + "a" * 64 + "/file.csv")
    stored = document["objects"][0]
    return bronze.input_item(
        TABLE, document, f"cms/synthetic/manifests/{name}/manifest.json", "v1", json.dumps(document).encode(), stored["object"], stored, ["file.csv"]
    )


def resign(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    objects = sum(row["kind"] == "object" for row in rows)
    for row in rows:
        row["generation_row_count"], row["generation_object_count"] = len(rows), objects
    digest = bronze_metadata.generation_digest(rows)
    for row in rows:
        row["generation"] = digest
    return rows


def row_cases() -> dict[str, bool]:
    """Cover invalid generation states that discovery cannot produce as selected inputs."""
    seed = item("seed")
    alias = {**seed, "observation_id": "alias", "object_key": "alias", "manifest_key": "cms/synthetic/manifests/alias/manifest.json"}
    baseline = bronze_metadata.build(["files"], [seed], [{"copy": alias}], [], ["files"])

    def changed(selector: str, **fields: Any) -> list[dict[str, Any]]:
        rows = copy.deepcopy(baseline)
        for row in rows:
            if row["kind"] == selector or (row["kind"] == "observation" and row["observation_id"] == selector):
                row.update(fields)
        return resign(rows)

    cases = [
        ("different keys", changed("alias", s3_key="another/file.csv"), "multiple_physical_locations"),
        ("different buckets", changed("alias", bucket="another-synthetic"), "multiple_physical_locations"),
        ("different versions of one key", changed("alias", s3_version_id="v2"), "multiple_versions_at_one_location"),
        ("different byte counts", changed("alias", byte_count=18), "inconsistent_byte_counts"),
        ("object physical mismatch", changed("object", s3_key="wrong/file.csv"), "object_physical_mismatch"),
        ("no loaded observation", changed("observation", loaded=False), "no_canonical_observation"),
        ("superseded selected observation", changed(seed["observation_id"], superseded_by_manifest_key="replacement"), "no_canonical_observation"),
        ("multiple loaded observations", changed("alias", loaded=True), "multiple_canonical_observations"),
        ("canonical observation ID mismatch", changed("object", observation_id="wrong"), "canonical_identity_mismatch"),
        ("canonical logical key mismatch", changed("object", _object_key="wrong"), "canonical_identity_mismatch"),
        ("missing object", resign([row for row in copy.deepcopy(baseline) if row["kind"] != "object"]), "missing_object"),
        ("null object version", changed("object", s3_version_id="null"), "invalid_object_version"),
        ("checksum in two tables", changed("alias", table_name="other"), "checksum_spans_tables"),
        ("wrong canonical reference", changed("alias", canonical_object_key="wrong"), "canonical_reference_mismatch"),
        ("inactive observations with object", changed("observation", retired=True), "inactive_observations_reference_object"),
        (
            "superseded observations stay physically live",
            changed("alias", s3_key="another/file.csv", superseded_by_manifest_key="replacement"),
            "multiple_physical_locations",
        ),
    ]
    if not hasattr(bronze_metadata, "diagnose_rows"):
        return {"diagnostic row API exists": False}
    results: dict[str, bool] = {}
    for label, rows, reason in cases:
        report = bronze_metadata.diagnose_rows(rows)
        try:
            bronze_metadata.validate(rows)
        except bronze_metadata.MetadataError:
            rejected = True
        else:
            rejected = False
        results[label] = rejected and not report["passed"] and report["failing_checksum_count"] == 1 and report["reason_counts"].get(reason) == 1
        results[f"{label}: no contents"] = CONTENT_SENTINEL not in json.dumps(report)
    results["valid alias passes"] = bronze_metadata.diagnose_rows(baseline)["passed"]
    for flag in ("retired", "failed_attempt"):
        rows = changed("alias", s3_key="another/file.csv", **{flag: True})
        report = bronze_metadata.diagnose_rows(rows)
        results[f"{flag} alias excluded from live references"] = report["passed"] and not report["reason_counts"]
    retired = {**seed, "retired": True}
    history = bronze_metadata.build(["files"], [], [{"copy": retired}], [], ["files"])
    results["inactive history without object passes"] = bronze_metadata.diagnose_rows(history)["passed"]
    rows = [row for row in copy.deepcopy(baseline) if row["kind"] == "observation"]
    for row in rows:
        row["loaded"] = False
    report = bronze_metadata.diagnose_rows(resign(rows))
    results["missing object and zero canonical both reported"] = report["reason_counts"] == {"missing_object": 1, "no_canonical_observation": 1}
    many = []
    for number in range(25):
        for row in changed("alias", s3_version_id="v2"):
            many.append({**row, "sha256": hashlib.sha256(str(number).encode()).hexdigest(), "observation_id": f"{number}-{row['observation_id']}"})
    many = resign(many)
    report = bronze_metadata.diagnose_rows(many)
    results["all failures counted with twenty global examples"] = (
        report["failing_checksum_count"] == 25
        and report["reason_counts"].get("multiple_versions_at_one_location") == 25
        and report["tables"]["files"]["failing_checksum_count"] == 25
        and len(report["examples"]) == 20
        and report["omitted_checksum_examples"] == 5
    )
    results["repeat and row ordering preserve summary"] = report == bronze_metadata.diagnose_rows(list(reversed(many)))
    crowded = copy.deepcopy(baseline)
    for number in range(30):
        crowded.append({**crowded[-1], "kind": "observation", "observation_id": f"crowded-{number}", "s3_key": f"extra/{number}.csv", "loaded": False})
    example: dict[str, Any] = next(iter(bronze_metadata.diagnose_rows(resign(crowded))["examples"]), {})
    results["per-checksum details bounded with exact totals"] = (
        example.get("observation_count") == 32
        and example["physical_reference_count"] == 31
        and len(example["observations"]) == 20
        and len(example["physical_references"]) == 20
    )
    return results


class ManifestStorage:
    """Read synthetic manifests only and refuse every payload download."""

    def __init__(self, documents: list[dict[str, Any]]) -> None:
        self.objects = {(f"cms/synthetic/manifests/{number}/manifest.json", "v1"): json.dumps(document).encode() for number, document in enumerate(documents)}
        self.reads: list[str] = []

    def list_keys(self, bucket: str, prefix: str) -> list[tuple[str, str]]:
        return sorted(identity for identity in self.objects if identity[0].startswith(prefix))

    def list_current(self, bucket: str, prefix: str) -> list[tuple[str, str]]:
        return self.list_keys(bucket, prefix)

    def download(self, bucket: str, key: str, version_id: str, path: Path) -> None:
        raise AssertionError("diagnostics reached a payload download")

    def get(self, bucket: str, key: str, version_id: str) -> bytes:
        self.reads.append(key)
        return self.objects[(key, version_id)]


def command_case(
    root: Path,
    label: str,
    documents: list[dict[str, Any]],
    diagnose: bool,
    previous: list[dict[str, Any]] | None = None,
    tokens: list[str] | None = None,
    tables: list[dict[str, Any]] | None = None,
    ownership: Path | None = None,
) -> dict[str, Any]:
    output = root / label
    deployment = root / "synthetic_deployment.json"
    deployment.write_text('{"data_bucket_name":"synthetic"}')
    storage = ManifestStorage(documents)
    stops: list[bool] = []
    mutations: list[bool] = []

    def forbidden(*_args: Any, **_kwargs: Any) -> Any:
        mutations.append(True)
        raise AssertionError("diagnostics reached a mutation or payload path")

    session = SimpleNamespace(spark_session=lambda *_args, **_kwargs: SimpleNamespace(stop=lambda: stops.append(True)), job_memory=lambda: "synthetic")
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(sys, "argv", ["bronze", *(tokens if tokens is not None else ["--bootstrap-all", *(["--diagnose"] if diagnose else [])])])
        patch.setitem(sys.modules, "scripts.lakehouse.session", session)
        patch.setattr(bronze, "DEPLOYMENT", deployment)
        patch.setattr(bronze, "REPORT_ROOT", output)
        patch.setattr(bronze, "S3Storage", lambda: storage)
        patch.setattr(bronze, "load_table_map", lambda: {"namespace": "synthetic", "tables": tables or [TABLE]})
        patch.setattr(bronze, "load_retired", bronze.RetiredObjects)
        if ownership is not None:
            patch.setattr(bronze_metadata, "BOOTSTRAP_OWNERSHIP", ownership, raising=False)
        patch.setattr(bronze_metadata, "read", lambda *_args: previous or [])
        for module, names in (
            (bronze, ("load_objects", "prune", "write_copies", "verify")),
            (bronze_metadata, ("publish", "write_bootstrap")),
            (dictionary, ("build",)),
        ):
            for name in names:
                patch.setattr(module, name, forbidden)
        code: int | str | None
        try:
            code = bronze.main()
        except SystemExit as error:
            code = error.code
    path = output / "diagnostic.json"
    return {"code": code, "report": json.loads(path.read_text()) if path.exists() else None, "stops": stops, "mutations": mutations, "reads": storage.reads}


def command_cases(root: Path) -> dict[str, bool]:
    """Drive the real bronze command through discovery while replacing external services."""
    key = "cms/synthetic/datasets/files/release_date=2026-01-01/" + "a" * 64 + "/file.csv"
    conflicted = [manifest("first", key), manifest("second", key, "v2")]
    original = command_case(root, "ordinary_rejection", conflicted, False)
    failed = command_case(root, "diagnostic_rejection", conflicted, True)
    good = command_case(root, "diagnostic_success", [manifest("first", key), manifest("second", key)], True)
    report = failed["report"] or {}
    results = {
        "ordinary build still rejects live duplicate versions": original["code"] == 1 and original["report"] is None and not original["mutations"],
        "diagnostic command writes counted failure and exits one": failed["code"] == 1
        and report.get("reason_counts") == {"multiple_versions_at_one_location": 1},
        "diagnostic command reports valid generation and exits zero": good["code"] == 0 and (good["report"] or {}).get("passed") is True,
        "diagnostic command stops session and performs no mutations": all(case["stops"] == [True] and not case["mutations"] for case in (failed, good)),
        "diagnostic command reads only discovered manifests": len(failed["reads"]) == 2 and all(key.endswith("/manifest.json") for key in failed["reads"]),
        "diagnostic command excludes manifest contents": CONTENT_SENTINEL not in json.dumps(report),
    }
    for label, tokens in (
        ("selected tables", ["--tables", "files", "--diagnose"]),
        ("copies only", ["--bootstrap-all", "--copies-only", "--diagnose"]),
        ("removed tables", ["--drop-removed", "--diagnose"]),
    ):
        invalid = command_case(root, label.replace(" ", "_"), conflicted, True, tokens=tokens)
        results[f"invalid diagnostic mode refused before services: {label}"] = (
            invalid["code"] == 2 and invalid["report"] is None and not invalid["stops"] and not invalid["reads"] and not invalid["mutations"]
        )
    for tokens in (["--bootstrap-all", "--diagnose"], ["--bootstrap-all"]):
        try:
            accepted = launcher.validate_spark_args("bronze", tokens) == tokens
        except launcher.LauncherError:
            accepted = False
        results["closed grammar accepts " + " ".join(tokens)] = accepted
    try:
        launcher.validate_spark_args("bronze", ["--bootstrap-all", "--unknown-diagnostic"])
    except launcher.LauncherError as error:
        results["closed grammar still refuses unknown flags"] = "closed schema" in str(error)
    else:
        results["closed grammar still refuses unknown flags"] = False
    if hasattr(bronze_metadata, "diagnose"):
        seed = item("seed")
        partial = bronze_metadata.diagnose(["files"], [seed], [], [], ["files", "other"])
        results["preparation error produces an explicit unanalysed report"] = (
            not partial["passed"] and partial["phase"] == "preparation" and not partial["checksum_analysis_performed"] and "full committed" in partial["error"]
        )
    else:
        results["diagnostic preparation API exists"] = False
    return results


def ownership_fixture(root: Path, count: int) -> tuple[dict[str, Any], list[dict[str, Any]], Path, list[dict[str, Any]]]:
    """Return synthetic table conflicts and a map that explicitly chooses the named dataset."""
    names = ["named_files", "legacy_files", "generic_files"][:count]
    tables = [{**{key: value for key, value in TABLE.items() if key != "member_pattern"}, "table": name, "dataset_ids": [name]} for name in names]
    documents = []
    for name in names:
        document = manifest(name, f"cms/synthetic/datasets/{name}/release_date=2026-01-01/" + "a" * 64 + "/file.csv", "version_" + name)
        document["objects"][0]["dataset_id"] = name
        documents.append(document)
    config = {"namespace": "synthetic", "tables": tables}
    inputs, _excluded = bronze.discover(ManifestStorage(documents), "synthetic", config, names)
    canonical = next(candidate for candidate in inputs if candidate["table"] == "named_files")
    fields = ("table_name", "observation_id", "_object_key", "s3_key", "s3_version_id", "byte_count", "manifest_key", "manifest_version_id")
    mapping = {
        "schema_version": 1,
        "groups": [
            {
                "sha256": "a" * 64,
                "owner_table": "named_files",
                "canonical_observation_id": canonical["observation_id"],
                "observations": [
                    {"logical_id" if field == "_object_key" else field: row[field] for field in fields}
                    for candidate in inputs
                    for row in [bronze_metadata.observation(candidate, True)]
                ],
            }
        ],
    }
    ownership = root / f"ownership_{count}.json"
    ownership.write_text(json.dumps(mapping, indent=2) + "\n")
    return config, documents, ownership, inputs


def ownership_cases(root: Path) -> dict[str, bool]:
    """Require exact reviewed ownership through discovery, command diagnostics and generation validation."""
    results: dict[str, bool] = {}
    outcomes: dict[str, Any] = {}
    for count in (2, 3):
        config, documents, ownership, inputs = ownership_fixture(root, count)
        names = [table["table"] for table in config["tables"]]
        saved = copy.deepcopy(inputs)
        command = command_case(root, f"ownership_command_{count}", documents, True, tables=config["tables"], ownership=ownership)
        results[f"{count} tables: diagnostic resolves reviewed owner"] = command["code"] == 0 and (command["report"] or {}).get("passed") is True
        results[f"{count} tables: command reads manifests and never mutates"] = (
            not command["mutations"] and len(command["reads"]) == count and command["stops"] == [True]
        )
        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(bronze_metadata, "BOOTSTRAP_OWNERSHIP", ownership, raising=False)
            try:
                rows = bronze_metadata.build(names, inputs, [], [], names)
            except bronze_metadata.MetadataError as error:
                outcomes[str(count)] = {"error": str(error), "diagnostic": command["report"]}
                results[f"{count} tables: generation has one owner and all aliases"] = False
                continue
            observations = [row for row in rows if row["kind"] == "observation"]
            objects = [row for row in rows if row["kind"] == "object"]
            canonical = next(candidate for candidate in saved if candidate["table"] == "named_files")
            aliases = [row for row in observations if row["table_name"] != "named_files"]
            results[f"{count} tables: generation has one owner and all aliases"] = (
                len(objects) == 1
                and len(observations) == count
                and objects[0]["table_name"] == "named_files"
                and objects[0]["observation_id"] == canonical["observation_id"]
                and sum(row["loaded"] for row in observations) == 1
                and all(not row["loaded"] and row["canonical_object_key"] == canonical["object_key"] for row in aliases)
            )
            original = {candidate["observation_id"]: bronze_metadata.observation(candidate, True) for candidate in saved}
            frozen = (
                "table_name",
                "_object_key",
                "bucket",
                "s3_key",
                "s3_version_id",
                "byte_count",
                "manifest_key",
                "manifest_version_id",
                "manifest_entry_json",
            )
            results[f"{count} tables: aliases retain exact original references"] = all(
                all(row[field] == original[row["observation_id"]][field] for field in frozen) for row in observations
            )
            replay = bronze_metadata.build(names, inputs, [], rows, names)
            bronze_metadata.validate_successor(rows, replay)
            results[f"{count} tables: replay and order preserve generation"] = (
                replay == rows == bronze_metadata.build(reversed(names), list(reversed(inputs)), [], [], names)
            )
            results[f"{count} tables: discovery choices stay in their own table"] = (
                inputs == saved and len(inputs) == count and all(candidate["table"] in candidate["key"] for candidate in inputs)
            )
            partial = bronze_metadata.build(["legacy_files"], [candidate for candidate in inputs if candidate["table"] == "legacy_files"], [], rows, names)
            bronze_metadata.validate_successor(rows, partial)
            results[f"{count} tables: partial alias update retains owner"] = partial == rows
            outcomes[str(count)] = {"generation": rows[0]["generation"], "observations": observations, "objects": objects, "diagnostic": command["report"]}
            for field, value in (
                ("key", "cms/synthetic/changed.csv"),
                ("version_id", "changed"),
                ("object_key", "changed"),
                ("byte_count", 18),
                ("table", "unlisted_files"),
                ("bucket", "another_synthetic"),
                ("manifest_key", "cms/synthetic/manifests/changed/manifest.json"),
                ("manifest_version_id", "changed_manifest_version"),
            ):
                changed = copy.deepcopy(inputs)
                next(candidate for candidate in changed if candidate["table"] == "legacy_files")[field] = value
                try:
                    bronze_metadata.build(names, changed, [], [], names)
                except bronze_metadata.MetadataError:
                    rejected = True
                else:
                    rejected = False
                results[f"{count} tables: changed alias {field} rejected"] = rejected
            for label, changes in (("another canonical", {"loaded": True}), ("wrong canonical reference", {"canonical_object_key": "wrong"})):
                changed_rows = copy.deepcopy(rows)
                next(row for row in changed_rows if row["kind"] == "observation" and row["table_name"] == "legacy_files").update(changes)
                try:
                    bronze_metadata.validate(resign(changed_rows))
                except bronze_metadata.MetadataError:
                    rejected = True
                else:
                    rejected = False
                results[f"{count} tables: {label} rejected"] = rejected
    (root / "ownership_outcomes.json").write_text(json.dumps(outcomes, indent=2, sort_keys=True) + "\n")
    config, _documents, ownership, inputs = ownership_fixture(root, 3)
    names = [table["table"] for table in config["tables"]]
    valid = json.loads(ownership.read_text())
    variants = []
    duplicate = copy.deepcopy(valid)
    duplicate["groups"].append(copy.deepcopy(duplicate["groups"][0]))
    variants.append(("duplicate checksum mapping", duplicate))
    duplicate = copy.deepcopy(valid)
    duplicate["groups"][0]["observations"].append(copy.deepcopy(duplicate["groups"][0]["observations"][0]))
    variants.append(("duplicate mapped observation", duplicate))
    for label, field, value in (
        ("missing canonical observation", "canonical_observation_id", "f" * 64),
        ("owner differs from canonical table", "owner_table", "legacy_files"),
    ):
        document = copy.deepcopy(valid)
        document["groups"][0][field] = value
        variants.append((label, document))
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(bronze_metadata, "BOOTSTRAP_OWNERSHIP", ownership, raising=False)
        for label, document in variants:
            ownership.write_text(json.dumps(document))
            try:
                bronze_metadata.build(names, inputs, [], [], names)
            except bronze_metadata.MetadataError as error:
                rejected = "bootstrap" in str(error)
            else:
                rejected = False
            results[label] = rejected
        ownership.write_text(json.dumps({"schema_version": 1, "groups": []}))
        try:
            bronze_metadata.build(names, inputs, [], [], names)
        except bronze_metadata.MetadataError as error:
            results["unmapped conflict retains original rejection"] = "exactly one live physical version" in str(error)
        else:
            results["unmapped conflict retains original rejection"] = False
    return results


def all_ownership_groups(root: Path) -> dict[str, bool]:
    """Drive synthetic copies of every committed conflict group through the real command."""
    committed = json.loads(Path("config/lakehouse/bootstrap_ownership.json").read_text())
    documents = []
    owners: dict[str, str] = {}
    for group_index, group in enumerate(committed["groups"]):
        digest = hashlib.sha256(f"synthetic_group_{group_index}".encode()).hexdigest()
        owners[digest] = group["owner_table"]
        for row_index, row in enumerate(group["observations"]):
            name = row["table_name"]
            key = f"cms/synthetic/datasets/{name}/release_date=2026-01-01/{digest}/file_{row_index}.csv"
            document = manifest(f"group_{group_index}_copy_{row_index}", key, f"version_{row_index}")
            document["objects"][0].update(dataset_id=name, member_chain=[f"file_{row_index}.csv"])
            document["objects"][0]["object"]["sha256"] = digest
            documents.append(document)
    names = sorted({document["objects"][0]["dataset_id"] for document in documents})
    tables = [{**{key: value for key, value in TABLE.items() if key != "member_pattern"}, "table": name, "dataset_ids": [name]} for name in names]
    config = {"namespace": "synthetic", "tables": tables}
    inputs, excluded = bronze.discover(ManifestStorage(documents), "synthetic", config, names)
    copies = [*inputs, *(entry["copy"] for entry in excluded if "copy" in entry)]
    fields = ("table_name", "observation_id", "_object_key", "s3_key", "s3_version_id", "byte_count", "manifest_key", "manifest_version_id")
    mapped = []
    for digest, owner in owners.items():
        canonical = next(candidate for candidate in inputs if candidate["sha256"] == digest and candidate["table"] == owner)
        mapped.append(
            {
                "sha256": digest,
                "owner_table": owner,
                "canonical_observation_id": canonical["observation_id"],
                "observations": [
                    {"logical_id" if field == "_object_key" else field: row[field] for field in fields}
                    for candidate in copies
                    if candidate["sha256"] == digest
                    for row in [bronze_metadata.observation(candidate, True)]
                ],
            }
        )
    ownership = root / "all_ownership_groups.json"
    ownership.write_text(json.dumps({"schema_version": 1, "groups": mapped}))
    command = command_case(root, "all_ownership_groups", documents, True, tables=tables, ownership=ownership)
    report = command["report"] or {}
    source, _ = load_yaml("dbt/models/silver/staging/_bronze__sources.yml", Path("dbt/models/silver/staging/_bronze__sources.yml").read_text())
    silver_sources = {table["name"] for source in source["sources"] if source["name"] == "bronze" for table in source["tables"]}
    project, _ = load_yaml("dbt/dbt_project.yml", Path("dbt/dbt_project.yml").read_text())
    copy_tables = set(project["vars"]["copy_tables"])
    results = {
        "all forty-four synthetic groups pass the diagnostic command": command["code"] == 0
        and report.get("passed") is True
        and report.get("failing_checksum_count") == 0
        and report.get("checksum_count") == 44,
        "all synthetic group observations and objects remain complete": report.get("observation_count") == 153 and report.get("object_count") == 44,
        "all synthetic groups read manifests and never mutate": len(command["reads"]) == 153 and not command["mutations"] and command["stops"] == [True],
        "no silver source or analytical family table becomes an alias": all(
            row["table_name"] == group["owner_table"]
            for group in committed["groups"]
            for row in group["observations"]
            if row["table_name"] in silver_sources | copy_tables
        ),
        "all ownership tables remain declared in the full bronze map": set(names) <= {table["table"] for table in bronze.load_table_map()["tables"]},
    }
    (root / "all_ownership_outcomes.json").write_text(
        json.dumps(
            {"synthetic": True, "group_count": len(mapped), "observation_count": len(copies), "command": command, "results": results}, indent=2, sort_keys=True
        )
        + "\n"
    )
    return results


def ownership_reference_commands(root: Path) -> dict[str, bool]:
    """Require a reviewed map update before accepting another observation of a mapped checksum."""
    config, documents, ownership, _inputs = ownership_fixture(root, 2)
    duplicate = copy.deepcopy(documents[0])
    duplicate["snapshot_id"] = "another_owner_observation"
    extended = [*documents, duplicate]
    unknown = command_case(root, "unlisted_owner_observation", extended, True, tables=config["tables"], ownership=ownership)
    unknown_report = unknown["report"] or {}
    results = {
        "unlisted owner observation requires exact review": unknown["code"] == 1
        and unknown_report.get("reason_counts", {}).get("bootstrap_mapping_mismatch") == 1,
    }
    inputs, excluded = bronze.discover(ManifestStorage(extended), "synthetic", config, [table["table"] for table in config["tables"]])
    mapping = json.loads(ownership.read_text())
    group = mapping["groups"][0]
    candidates = [*inputs, *(entry["copy"] for entry in excluded if "copy" in entry)]
    fields = ("table_name", "observation_id", "_object_key", "s3_key", "s3_version_id", "byte_count", "manifest_key", "manifest_version_id")
    group["observations"] = [
        {"logical_id" if field == "_object_key" else field: row[field] for field in fields}
        for candidate in candidates
        for row in [bronze_metadata.observation(candidate, True)]
    ]
    canonical = next(candidate for candidate in inputs if candidate["table"] == "named_files")
    group["canonical_observation_id"] = canonical["observation_id"]
    ownership.write_text(json.dumps(mapping))
    reviewed = command_case(root, "reviewed_owner_observation", extended, True, tables=config["tables"], ownership=ownership)
    results["explicitly reviewed owner observation remains usable"] = reviewed["code"] == 0 and (reviewed["report"] or {}).get("passed") is True
    missing = command_case(root, "missing_reviewed_observation", documents, True, tables=config["tables"], ownership=ownership)
    missing_report = missing["report"] or {}
    results["first bootstrap requires every reviewed observation"] = (
        missing["code"] == 1 and missing_report.get("phase") == "preparation" and "bootstrap observations are missing" in missing_report.get("error", "")
    )
    unselected = next(
        candidate for candidate in candidates if candidate["table"] == "named_files" and candidate["observation_id"] != canonical["observation_id"]
    )
    group["canonical_observation_id"] = unselected["observation_id"]
    ownership.write_text(json.dumps(mapping))
    refused = command_case(root, "unselected_owner_observation", extended, True, tables=config["tables"], ownership=ownership)
    refused_report = refused["report"] or {}
    results["bootstrap owner must already be the table's selected copy"] = (
        refused["code"] == 1 and refused_report.get("phase") == "preparation" and "selected canonical observation" in refused_report.get("error", "")
    )
    return results


def ownership_identity_commands(folder: Path) -> dict[str, bool]:
    """Reject a changed alias table and a changed owning physical reference through the command."""
    config, documents, ownership, _inputs = ownership_fixture(folder, 2)
    tables = copy.deepcopy(config["tables"])
    next(table for table in tables if table["table"] == "legacy_files")["table"] = "another_legacy_files"
    changed = command_case(folder, "changed_alias_table", documents, True, tables=tables, ownership=ownership)
    results = {
        "exact alias table remains pinned": changed["code"] == 1 and (changed["report"] or {}).get("reason_counts", {}).get("bootstrap_mapping_mismatch") == 1
    }
    extra = copy.deepcopy(documents[0])
    extra["snapshot_id"] = "another_physical_owner"
    extra["objects"][0]["object"].update(key="cms/synthetic/datasets/named_files/another.csv", version_id="another_version")
    documents.append(extra)
    inputs, excluded = bronze.discover(ManifestStorage(documents), "synthetic", config, [table["table"] for table in config["tables"]])
    candidates = [*inputs, *(entry["copy"] for entry in excluded if "copy" in entry)]
    observations = [bronze_metadata.observation(candidate, candidate["key"].endswith("another.csv")) for candidate in candidates]
    selected = next(row for row in observations if row["loaded"])
    mapping = json.loads(ownership.read_text())
    mapping["groups"][0]["observations"] = [
        {"logical_id" if field == "_object_key" else field: row[field] for field in bronze_metadata.BOOTSTRAP_REFERENCE_FIELDS} for row in observations
    ]
    ownership.write_text(json.dumps(mapping))
    rows = [*observations, {**selected, "kind": "object"}]
    for row in rows:
        row["canonical_object_key"] = selected["_object_key"]
    resign(rows)
    changed = command_case(folder, "changed_owner_reference", documents, True, previous=rows, tables=config["tables"], ownership=ownership)
    results["owning object retains the reviewed physical reference"] = (
        changed["code"] == 1
        and (changed["report"] or {}).get("phase") == "preparation"
        and "reviewed bootstrap mapping differs" in (changed["report"] or {}).get("error", "")
    )
    return results


class CatalogLink:
    """Record the host job wrapper's requests without starting services."""

    def __init__(self, root: Path, phase: str = "up") -> None:
        self.config = SimpleNamespace(registry=SimpleNamespace(kind="real"), runs_root=root)
        self.phase = phase
        self.attempts: list[dict[str, Any]] = []

    def call(self, method: str, _params: Any = None) -> dict[str, Any]:
        if method == "status":
            return {"phase": self.phase}
        if method == "plan":
            return {
                "environment": {"SPARK_JOB_MEMORY": "2g", "JOB_MEMORY_LIMIT": "2147483648", "JOB_THREADS": "1"},
                "budget": {"total": 2147483648, "used_by_containers": 0, "headroom": 0, "mac_available": None},
            }
        return {}

    def principal_run(self, run_id: str) -> dict[str, str]:
        return {"run_id": run_id}

    def run_attempt(self, *_args: Any, **kwargs: Any) -> Any:
        self.attempts.append(kwargs)
        return SimpleNamespace(ok=True, stdout="", stderr="", reason="completed", exit_code=0)

    def settle(self, _principal: Any) -> None:
        return None

    def cleanup_call(self, *_args: Any, **_kwargs: Any) -> None:
        return None


def catalog_cases(root: Path) -> dict[str, bool]:
    """Run the real host wrapper with a synthetic launcher and observe setup and deadline behavior."""
    results: dict[str, bool] = {}
    for label, arguments, phase, setup_count, seconds in (
        ("diagnostic wrapper", ["--bootstrap-all", "--diagnose"], "up", 0, 1200),
        ("ordinary wrapper", ["--bootstrap-all"], "up", 1, None),
        ("diagnostic without catalog", ["--bootstrap-all", "--diagnose"], "down", 0, 1200),
    ):
        link = CatalogLink(root / label.replace(" ", "_"), phase)
        setups: list[bool] = []
        error = ""
        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(catalog, "client", lambda bound=link: bound)
            patch.setattr(catalog, "up", lambda observed=setups: observed.append(True))
            try:
                catalog.job("bronze", arguments)
            except catalog.CatalogError as caught:
                error = str(caught)
        if phase == "down":
            results[label] = "already running catalog" in error and not setups and not link.attempts
        else:
            results[label] = not error and len(setups) == setup_count and len(link.attempts) == 1 and link.attempts[0].get("seconds") == seconds
    return results


def main() -> int:
    """Write synthetic assertion results and return failure if any result fails."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    sources = (
        Path(__file__),
        Path(bronze.__file__),
        Path(bronze_metadata.__file__),
        Path(catalog.__file__),
        Path(launcher.__file__),
        root / "config/lakehouse/bootstrap_ownership.json",
    )
    source_sha256 = {str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest() for path in sources}
    with tempfile.TemporaryDirectory(prefix="bronze_diagnostics_") as temporary:
        folder = Path(temporary)
        results = {
            **row_cases(),
            **command_cases(folder),
            **ownership_cases(folder),
            **all_ownership_groups(folder),
            **ownership_reference_commands(folder),
            **ownership_identity_commands(folder),
            **catalog_cases(folder),
        }
        outcomes = json.loads((folder / "ownership_outcomes.json").read_text())
        all_groups = json.loads((folder / "all_ownership_outcomes.json").read_text())
        command_reports = {path.parent.name: json.loads(path.read_text()) for path in folder.glob("*/diagnostic.json")}
    results["sources unchanged during run"] = source_sha256 == {str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest() for path in sources}
    report = {
        "created_utc": datetime.now(UTC).isoformat(),
        "boundary": "synthetic rows and bronze.main with manifest-only storage and a substituted Spark session",
        "inputs": "synthetic only",
        "python": sys.version,
        "source_sha256": source_sha256,
        "command_reports": command_reports,
        "ownership_outcomes": outcomes,
        "all_ownership_groups": all_groups,
        "results": results,
        "passed": all(results.values()),
        "cleanup": "done",
    }
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    sys.stdout.write(
        json.dumps(
            {
                "report": str(args.report),
                "passed": sum(results.values()),
                "total": len(results),
                "failed": [name for name, passed in results.items() if not passed],
            }
        )
        + "\n"
    )
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
