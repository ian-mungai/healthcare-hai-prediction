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
    check(plan["bucket"] == BUCKET and plan["removal"] == "by hand by the owner", "bucket and the hand removal")
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


def test_there_is_no_automatic_removal_in_the_module_or_on_its_command_line(tmp_path: Path) -> None:
    check(not hasattr(unused_files, "apply") and not hasattr(unused_files, "approved_plan"), "the module has no apply step")
    out = standard(tmp_path).plan()
    with pytest.raises(SystemExit):
        unused_files.main(["apply", "--plan", str(out / "removal_plan.json"), "--output", str(out)])
    source = Path(unused_files.__file__).read_text(encoding="utf-8")
    check("delete-objects" not in source and "delete_marker" not in source.lower() and "AwsCli" not in source, "the module calls no S3 operation")


def test_the_plan_states_that_the_owner_removes_the_keys_by_hand(tmp_path: Path) -> None:
    plan = json.loads((standard(tmp_path).plan() / "removal_plan.json").read_text())
    check(plan["removal"] == "by hand by the owner" and "method" not in plan and "executed" not in plan, "the plan names no automatic method")


def test_the_confirmation_template_lists_every_planned_key_for_the_owner_to_sign(tmp_path: Path) -> None:
    out = standard(tmp_path).plan()
    template = tmp_path / "confirmation.json"
    unused_files.main(["confirmation-template", "--plan", str(out / "removal_plan.json"), "--output", str(template)])
    plan = json.loads((out / "removal_plan.json").read_text())
    document = json.loads(template.read_text())
    check(
        document["plan_sha256"] == plan["plan_sha256"] and document["bucket"] == BUCKET and document["confirmed_by"] is None,
        "it names the plan, the bucket and no confirmer",
    )
    check(document["keys"] == sorted(item["s3_key"] for item in plan["items"]), "it lists each planned key once, sorted")
    before = template.read_bytes()
    unused_files.main(["confirmation-template", "--plan", str(out / "removal_plan.json"), "--output", str(template)])
    check(template.read_bytes() == before, "a second run changes nothing")


def test_the_confirmation_template_refuses_a_plan_changed_after_it_was_written(tmp_path: Path) -> None:
    out = standard(tmp_path).plan()
    document = json.loads((out / "removal_plan.json").read_text())
    document["items"] = document["items"][:1]
    (out / "removal_plan.json").write_text(json.dumps(document))
    with pytest.raises(SystemExit):
        unused_files.main(["confirmation-template", "--plan", str(out / "removal_plan.json"), "--output", str(tmp_path / "confirmation.json")])
    check(not (tmp_path / "confirmation.json").exists(), "no template for an edited plan")


def test_a_copies_row_and_an_inventory_row_that_disagree_about_the_bytes_are_refused_before_a_plan_exists(tmp_path: Path) -> None:
    case = standard(tmp_path)
    for row in case.copies:
        if row[1] == "S1__4":
            row[3] = LOADED
    with pytest.raises(SystemExit):
        case.plan()
    check(not (tmp_path / "out" / "removal_plan.json").exists(), "a contradictory input produces no plan")


def test_every_planned_target_carries_the_hash_of_its_inventory_record(tmp_path: Path) -> None:
    case = standard(tmp_path)
    plan = json.loads((case.plan() / "removal_plan.json").read_text())
    stored = {row[2]: row[4] for row in case.inventory}
    check(all(item["sha256"] == stored[item["s3_key"]] for item in plan["items"]), "plan hashes come from the inventory records")
