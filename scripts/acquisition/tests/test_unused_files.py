from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path

import pytest

from scripts.acquisition import unused_files
from tests.support import check

COPY_HEADER = ["source_id", "snapshot_id", "s3_key", "sha256", "loaded", "retired", "tables"]
INVENTORY_HEADER = ["source_id", "snapshot_id", "s3_key", "s3_version_id", "sha256"]
BUCKET = "example-bucket"
LOADED, TWIN, DICTIONARY, NO_READER, NOT_LOADED, RETIRED, UNLISTED = "a" * 64, "b" * 64, "c" * 64, "d" * 64, "e" * 64, "f" * 64, "1" * 64


def key(snapshot: str, name: str, digest: str, root: str = "raw/", area: str = "datasets") -> str:
    return f"{root}pub/coll/{area}/table/release_date=2026/{snapshot}/{digest}/{name}"


def version_of(object_key: str) -> str:
    return "v-" + hashlib.sha256(object_key.encode()).hexdigest()[:10]


class Case:
    """Bronze copies rows and stored-object inventory rows for one synthetic collection."""

    def __init__(self, folder: Path) -> None:
        self.folder = folder
        self.copies: list[list[str]] = []
        self.inventory: list[list[str]] = []
        self.keep: list[dict[str, str]] = [{"snapshot_id": "S1__3", "reason": "stored_dictionary"}]

    def stored(self, snapshot: str, name: str, digest: str, *, in_copies: tuple[str, str, str] | None = None, **kwargs: str) -> str:
        object_key = key(snapshot, name, digest, **kwargs)
        self.inventory.append(["S1", snapshot, object_key, version_of(object_key), digest])
        if in_copies is not None:
            loaded, retired, tables = in_copies
            self.copies.append(["S1", snapshot, object_key, digest, loaded, retired, tables])
        return object_key

    def write(self) -> dict[str, Path]:
        copies, inventory, keep = self.folder / "copies.csv", self.folder / "inventory.csv", self.folder / "keep.json"
        for path, header, rows in ((copies, COPY_HEADER, self.copies), (inventory, INVENTORY_HEADER, self.inventory)):
            with path.open("w", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(header)
                writer.writerows(rows)
        keep.write_text(json.dumps({"keep": self.keep}))
        return {"copies": copies, "inventory": inventory, "keep": keep}

    def plan(self, **overrides: str) -> Path:
        paths = self.write()
        arguments = ["plan", "--copies", str(paths["copies"]), "--inventory", str(paths["inventory"]), "--keep", str(paths["keep"]), "--bucket", BUCKET]
        arguments += ["--output", str(self.folder / "out")]
        for name, value in overrides.items():
            arguments[arguments.index(f"--{name}") + 1] = value
        unused_files.main(arguments)
        return self.folder / "out"


def standard(folder: Path) -> Case:
    case = Case(folder)
    case.stored("S1__1", "used.csv", LOADED, in_copies=("true", "false", "1"))
    case.stored("S1__2", "twin.csv", LOADED, in_copies=("false", "false", "1"))
    case.stored("S1__3", "dictionary.pdf", DICTIONARY, in_copies=("false", "false", "0"))
    case.stored("S1__4", "never_read.csv", NO_READER, in_copies=("false", "false", "0"))
    case.stored("S1__5", "superseded.csv", NOT_LOADED, in_copies=("false", "false", "2"))
    case.stored("S1__6", "gone.csv", RETIRED, in_copies=("false", "true", "0"))
    case.stored("S1__7", "never_listed.csv", UNLISTED)
    return case


class Client:
    """S3 stand-in: delete-objects without a version id adds a delete marker and removes nothing."""

    def __init__(self, versions: dict[str, str], bucket: str = BUCKET) -> None:
        self.configuration = {"data_bucket_name": bucket}
        self.versions = dict(versions)
        self.markers: dict[str, str] = {}
        self.deletes: list[list[str]] = []

    def call(self, service: str, operation: str, arguments: list[str] | None = None) -> dict:
        if operation == "get-bucket-versioning":
            return {"Status": "Enabled"}
        if operation == "list-object-versions":
            return {
                "Versions": [{"Key": name, "VersionId": version, "IsLatest": name not in self.markers} for name, version in self.versions.items()],
                "DeleteMarkers": [{"Key": name, "VersionId": marker, "IsLatest": True} for name, marker in self.markers.items()],
            }
        check(operation == "delete-objects" and arguments is not None, "only delete-objects is allowed")
        if arguments is None:
            raise ValueError("Missing deletion arguments")
        request = json.loads(Path(arguments[1].removeprefix("file://")).read_text())
        check(all(set(item) == {"Key"} for item in request["Objects"]), "a version id would delete permanently")
        self.deletes.append([item["Key"] for item in request["Objects"]])
        for item in request["Objects"]:
            self.markers[item["Key"]] = f"m-{len(self.markers)}"
        return {"Deleted": [{"Key": item["Key"], "DeleteMarker": True, "DeleteMarkerVersionId": self.markers[item["Key"]]} for item in request["Objects"]]}


def approve(out: Path) -> list[str]:
    plan = json.loads((out / "removal_plan.json").read_text())
    approval = out.parent / "approval.json"
    approval.write_text(json.dumps({"plan_sha256": plan["plan_sha256"], "approved_by": "owner"}))
    return ["apply", "--plan", str(out / "removal_plan.json"), "--approval", str(approval), "--output", str(out)]


def report(out: Path) -> dict:
    return json.loads((out / "unused_report.json").read_text())


def test_report_names_each_unused_file_with_a_reason(tmp_path: Path) -> None:
    out = standard(tmp_path).plan()
    found = report(out)
    unused = {item["snapshot_id"]: item["reason"] for item in found["unused"]}
    check(unused == {"S1__4": "no_table_reads_it", "S1__7": "not_in_bronze_copies"}, "only files no table reads are unused")
    kept = {item["snapshot_id"]: item["reason"] for item in found["kept"]}
    check(kept["S1__1"] == "loaded_by_bronze" and kept["S1__2"] == "same_bytes_as_loaded_file" and kept["S1__3"] == "stored_dictionary", "kept reasons")
    check(kept["S1__5"] == "listed_as_a_table_copy", "a file a table lists as a copy is held, not removed")
    check(found["already_retired"] == ["S1__6"], "retired files are listed apart")


def test_the_plan_binds_the_bucket_and_each_immutable_version(tmp_path: Path) -> None:
    case = standard(tmp_path)
    plan = json.loads((case.plan() / "removal_plan.json").read_text())
    check(plan["bucket"] == BUCKET and plan["executed"] is False and plan["permanent_version_deletion"] is False, "bucket and dry state")
    check([item["snapshot_id"] for item in plan["items"]] == ["S1__4", "S1__7"], "planned snapshots")
    check(all(item["version_id"] == version_of(item["s3_key"]) for item in plan["items"]), "each item names its immutable version")


def test_a_loaded_files_manifest_and_other_non_payload_keys_are_never_planned(tmp_path: Path) -> None:
    case = standard(tmp_path)
    manifest = case.stored("S1__1", "manifest.json", "9" * 64, area="manifests")
    reference = case.stored("S1__1", "layout.pdf", "8" * 64, area="references")
    evidence = case.stored("S1__1", "proof.json", "7" * 64, area="audit")
    lake = "lakehouse/bronze/table/metadata/00000.metadata.json"
    case.inventory.append(["S1", "S1__8", lake, version_of(lake), "6" * 64])
    plan = json.loads((case.plan() / "removal_plan.json").read_text())
    planned = {item["s3_key"] for item in plan["items"]}
    check(not planned & {manifest, reference, evidence, lake}, "no manifest, reference, evidence or lakehouse key is planned")
    protected = {item["s3_key"]: item["reason"] for item in report(tmp_path / "out")["kept"]}
    check(protected[manifest] == "not_an_acquisition_payload", "the manifest is reported as protected")


def test_a_physical_key_shared_with_a_kept_capture_is_never_planned(tmp_path: Path) -> None:
    case = standard(tmp_path)
    shared = case.stored("S1__3", "shared.pdf", "5" * 64, in_copies=("false", "false", "0"))
    case.inventory.append(["S1", "S1__9", shared, version_of(shared), "5" * 64])
    case.copies.append(["S1", "S1__9", shared, "5" * 64, "false", "false", "0"])
    out = case.plan()
    planned = {item["s3_key"] for item in json.loads((out / "removal_plan.json").read_text())["items"]}
    found = report(out)
    check(shared not in planned and shared not in {item["s3_key"] for item in found["unused"]}, "a shared key stays out of the plan and the unused list")
    check(shared in {item["s3_key"] for item in found["kept"]}, "a shared key is reported as kept")


@pytest.mark.parametrize("damage", ["empty inventory", "no inventory rows for copies", "empty copies", "missing bucket", "missing version column"])
def test_incomplete_inputs_are_refused_before_a_plan_exists(tmp_path: Path, damage: str) -> None:
    case = standard(tmp_path)
    paths = case.write()
    arguments = ["plan", "--copies", str(paths["copies"]), "--inventory", str(paths["inventory"]), "--keep", str(paths["keep"]), "--bucket", BUCKET]
    arguments += ["--output", str(tmp_path / "out")]
    if damage == "empty inventory":
        paths["inventory"].write_text(",".join(INVENTORY_HEADER) + "\n")
    elif damage == "no inventory rows for copies":
        rows = [row for row in case.inventory if row[1] != "S1__1"]
        paths["inventory"].write_text("\n".join([",".join(INVENTORY_HEADER), *[",".join(row) for row in rows]]) + "\n")
    elif damage == "empty copies":
        paths["copies"].write_text(",".join(COPY_HEADER) + "\n")
    elif damage == "missing bucket":
        arguments = arguments[: arguments.index("--bucket")] + arguments[arguments.index("--bucket") + 2 :]
    else:
        paths["inventory"].write_text("source_id,snapshot_id,s3_key,sha256\n")
    with pytest.raises(SystemExit):
        unused_files.main(arguments)
    check(not (tmp_path / "out" / "removal_plan.json").exists(), f"{damage} writes no plan")


def test_rerun_with_same_inputs_changes_nothing_and_changed_inputs_are_refused(tmp_path: Path) -> None:
    case = standard(tmp_path)
    out = case.plan()
    before = (out / "removal_plan.json").read_bytes()
    case.plan()
    check((out / "removal_plan.json").read_bytes() == before, "same inputs give the same plan")
    case.stored("S1__8", "late.csv", "9" * 64)
    with pytest.raises(SystemExit):
        case.plan()


def versions_of(out: Path, extra: dict[str, str] | None = None) -> dict[str, str]:
    plan = json.loads((out / "removal_plan.json").read_text())
    return {item["s3_key"]: item["version_id"] for item in plan["items"]} | (extra or {})


def test_apply_needs_the_owner_approval_of_this_exact_plan(tmp_path: Path) -> None:
    out = standard(tmp_path).plan()
    client = Client(versions_of(out))
    plan_file = str(out / "removal_plan.json")
    with pytest.raises(SystemExit):
        unused_files.main(["apply", "--plan", plan_file, "--approval", str(tmp_path / "missing.json"), "--output", str(out)], client=client)
    wrong = tmp_path / "approval.json"
    wrong.write_text(json.dumps({"plan_sha256": "0" * 64, "approved_by": "owner"}))
    with pytest.raises(SystemExit):
        unused_files.main(["apply", "--plan", plan_file, "--approval", str(wrong), "--output", str(out)], client=client)
    check(client.deletes == [], "no marker without approval")


def test_apply_writes_delete_markers_once_and_a_retry_has_no_second_effect(tmp_path: Path) -> None:
    out = standard(tmp_path).plan()
    kept = "raw/pub/coll/datasets/kept.csv"
    client = Client(versions_of(out, {kept: "v-kept"}))
    arguments = approve(out)
    unused_files.main(arguments, client=client)
    check(len(client.deletes) == 1 and kept not in client.markers, "one batch, kept key untouched")
    record = json.loads((out / "removal_record.json").read_text())
    check(record["bucket"] == BUCKET and {item["s3_key"] for item in record["items"]} == set(versions_of(out)), "the record covers every planned key")
    check(all(item["delete_marker_version_id"] and item["version_id"] for item in record["items"]), "each item names its version and its marker")
    unused_files.main(arguments, client=client)
    check(len(client.deletes) == 1, "a retry adds no marker")


def test_apply_refuses_another_bucket(tmp_path: Path) -> None:
    out = standard(tmp_path).plan()
    client = Client(versions_of(out), bucket="another-bucket")
    with pytest.raises(SystemExit):
        unused_files.main(approve(out), client=client)
    check(client.deletes == [], "no marker in another bucket")


def test_apply_refuses_a_key_whose_current_version_changed(tmp_path: Path) -> None:
    out = standard(tmp_path).plan()
    versions = versions_of(out)
    changed = sorted(versions)[0]
    versions[changed] = "v-replaced"
    client = Client(versions)
    with pytest.raises(SystemExit):
        unused_files.main(approve(out), client=client)
    check(client.deletes == [], "no marker when any planned version is not the current one")


def test_apply_refuses_a_key_with_no_version(tmp_path: Path) -> None:
    out = standard(tmp_path).plan()
    versions = versions_of(out)
    del versions[sorted(versions)[0]]
    client = Client(versions)
    with pytest.raises(SystemExit):
        unused_files.main(approve(out), client=client)
    check(client.deletes == [], "no marker when a planned key is missing")
