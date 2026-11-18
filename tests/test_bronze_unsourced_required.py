"""A declared bronze table that no run loaded is not required by the steps that read "every mapped table", on evidence a forged record cannot fake.

Live finding of Nov 18 2026: the bronze load leaves ``hcai_util_workbook_sheet_rows`` out until its workbooks arrive, so no Iceberg table exists for it.
``register_aws_catalog.required_tables`` and ``glue_mirror register --layer bronze`` listed every table of ``config/lakehouse/bronze_tables.json`` and would
refuse for the absent table.

Review round 2 (Codex, High): a record the reader accepted was not bound to anything. The rule is now:

- a record is a complete bronze run record (the shape ``raw_lineage.loaded_objects`` checks) whose run ID is the one in its key and whose skipped reasons are
  the stage's reason; a key that is not a run's record key is not evidence, and any other defect refuses the read (nothing is guessed);
- a table is unsourced when some record skips it and no record loaded it, whatever the order of the records, the write times or the argument order;
- a table that has Iceberg metadata in S3, a registration in Glue or a table in the source catalog is never left out, whatever a record says.

Failure modes: a body with missing fields; a record under a wrong key or whose body names another run; a skipped reason that is not the stage's; a table that
a record skipped and another loaded, in either write order; a record that skips a table whose metadata exists; a silver reader of a skipped table; records
that cannot be read (the layer fails).
"""

from __future__ import annotations

import io
import json
from pathlib import Path
from typing import Any

import pytest
from botocore.exceptions import ClientError

from scripts.lakehouse import bronze_unsourced, glue_mirror, register_aws_catalog
from tests.support import check

ROOT = Path(__file__).resolve().parents[1]
BUCKET = "example-bucket"
TABLE = "hcai_util_workbook_sheet_rows"
RUN_A, RUN_B = "bronze_00000001", "bronze_ffffffff"


def record(run: str, loaded: list[str], skipped: list[str], **changes: Any) -> bytes:
    """A complete run record as the check phase writes it (``bronze_run.run_record``)."""
    body: dict[str, Any] = {
        "schema_version": 1,
        "run_id": run,
        "target": "real",
        "namespaces": ["bronze", "bronze_dictionary"],
        "tables": loaded,
        "report_sha256": "a" * 64,
        "metadata_generation": "7",
        "snapshots": {name: 100 + number for number, name in enumerate(loaded)},
        "objects": [{"table": name, "bucket": BUCKET, "key": f"raw/{name}/1.csv", "version_id": "v1", "sha256": "b" * 64, "byte_count": 1} for name in loaded],
        "skipped_tables": [{"table": name, "reason": "no source object"} for name in skipped],
    }
    return json.dumps(body | changes).encode()


def key_of(run: str) -> str:
    return f"lakehouse/bronze_runs/{run}/check.json"


def test_a_complete_record_names_the_tables_it_loaded_and_the_tables_it_skipped() -> None:
    loaded, skipped = bronze_unsourced.record_tables(record(RUN_A, ["a", "b"], ["c"]), key_of(RUN_A), RUN_A)
    check(loaded == {"a", "b"} and skipped == {"c"}, f"{loaded} {skipped}")
    older = json.loads(record(RUN_A, ["a"], []))
    del older["skipped_tables"]
    check(bronze_unsourced.record_tables(json.dumps(older).encode(), key_of(RUN_A), RUN_A) == ({"a"}, frozenset()), "an older record skipped something")


FORGED = json.dumps({"schema_version": 1, "target": "real", "tables": [], "skipped_tables": [{"table": TABLE, "reason": None}]}).encode()


@pytest.mark.parametrize(
    "body",
    [
        b"not json",
        b"[]",
        FORGED,
        record(RUN_A, ["a"], [], schema_version=2),
        record(RUN_A, ["a"], [], target="qualify"),
        record(RUN_A, ["a"], [], namespaces=["x"]),
        record(RUN_A, ["a"], ["a"]),
        record(RUN_A, ["a"], ["c", "c"]),
        record(RUN_A, ["a"], [], skipped_tables="c"),
        record(RUN_A, ["a"], [], skipped_tables=[{"table": "c"}]),
        record(RUN_A, ["a"], [], skipped_tables=[{"table": "c", "reason": None}]),
        record(RUN_A, ["a"], [], skipped_tables=[{"table": "c", "reason": "because"}]),
        record(RUN_B, ["a"], []),
        record(RUN_A, ["a"], [], objects=[]),
        record(RUN_A, ["a"], [], snapshots={}),
        record(RUN_A, "a", []),
    ],
    ids=[
        "not json",
        "not an object",
        "the reviewer's forged body",
        "another schema version",
        "not a real run",
        "another namespace list",
        "skipped and loaded",
        "skipped twice",
        "skipped not a list",
        "a skipped table with no reason",
        "a null reason",
        "a reason that is not the stage's",
        "the body names another run",
        "no loaded object",
        "no snapshot",
        "tables not a list",
    ],
)
def test_a_body_that_is_not_a_complete_record_of_its_own_run_is_refused(body: bytes) -> None:
    with pytest.raises(bronze_unsourced.UnsourcedError):
        bronze_unsourced.record_tables(body, key_of(RUN_A), RUN_A)


def test_a_table_is_unsourced_when_a_record_skips_it_and_no_record_loaded_it_in_any_order() -> None:
    skip = (frozenset({"a"}), frozenset({"c"}))
    load = (frozenset({"a", "c"}), frozenset())
    other = (frozenset({"a", "b"}), frozenset({"d"}))
    check(bronze_unsourced.unsourced_in([skip]) == {"c"}, "a skipped table is not unsourced")
    check(bronze_unsourced.unsourced_in([skip, other]) == {"c", "d"}, "two skipped tables")
    for records in ([skip, load], [load, skip]):
        check(bronze_unsourced.unsourced_in(records) == frozenset(), "a table that a record loaded was left out")
    check(bronze_unsourced.unsourced_in([]) == frozenset(), "no record leaves a table out")


def test_a_silver_reader_of_a_skipped_table_keeps_it_required_and_an_undeclared_name_is_ignored() -> None:
    declared = ["a", "b", "c"]
    check(bronze_unsourced.left_out(declared, {"c"}, set()) == {"c"}, "the skipped table was required")
    check(bronze_unsourced.left_out(declared, {"c"}, {"c"}) == frozenset(), "silver reads the table, so it stays required")
    check(bronze_unsourced.left_out(declared, {"ghost"}, set()) == frozenset(), "an undeclared name was left out")


def test_the_catalog_registration_does_not_require_the_unsourced_table_but_requires_every_other_bronze_table() -> None:
    every = register_aws_catalog.required_tables(ROOT)
    check(("bronze", TABLE) in every, "the table is not required when no record skips it")
    left = register_aws_catalog.required_tables(ROOT, frozenset({TABLE}))
    check(("bronze", TABLE) not in left and every - left == {("bronze", TABLE)}, f"{sorted(every - left)}")
    check(("bronze", "file_metadata") in left, "the file metadata table is no longer required")


def test_the_catalog_registration_still_requires_a_table_a_silver_model_reads_even_when_a_record_skips_it() -> None:
    mapped = {entry["table"] for entry in json.loads((ROOT / "config/lakehouse/bronze_tables.json").read_text())["tables"]}
    readers = sorted(glue_mirror.silver_bronze_sources() & mapped)
    check(bool(readers), "no mapped table is read by silver")
    kept = register_aws_catalog.required_tables(ROOT, frozenset({readers[0]}))
    check(("bronze", readers[0]) in kept, f"{readers[0]} is read by silver but was left out")


def test_the_registration_reads_the_unsourced_tables_from_records_given_with_their_run_in_any_order() -> None:
    skip, load = (RUN_A, record(RUN_A, ["a"], ["c"])), (RUN_B, record(RUN_B, ["a", "c"], []))
    check(register_aws_catalog.unsourced_from_records([skip]) == {"c"}, "the skipped table was not read")
    for records in ([skip, load], [load, skip]):
        check(register_aws_catalog.unsourced_from_records(records) == frozenset(), "a record that loaded the table did not require it, in one argument order")
    for bad in ([(RUN_A, b"not json")], [(RUN_A, FORGED)], [(RUN_B, record(RUN_A, ["a"], ["c"]))], [("not_a_run", record(RUN_A, ["a"], ["c"]))]):
        with pytest.raises(register_aws_catalog.RegistrationError) as error:
            register_aws_catalog.unsourced_from_records(bad)
        check(error.value.code == "coverage", f"{error.value.code}")


class Bucket:
    """The read calls the readers may use: a listing by prefix and a read by key. It holds run records and table metadata files."""

    def __init__(self, objects: dict[str, bytes], broken: bool = False) -> None:
        self.objects, self.broken = objects, broken

    def get_paginator(self, operation: str) -> Any:
        outer = self

        class Pages:
            def paginate(self, Bucket: str, Prefix: str) -> list[dict[str, Any]]:
                keys = sorted(key for key in outer.objects if key.startswith(Prefix))
                return [{"Contents": [{"Key": key} for key in keys]}] if keys else [{}]

        return Pages()

    def get_object(self, Bucket: str, Key: str) -> dict[str, Any]:
        if self.broken:
            raise ClientError({"Error": {"Code": "AccessDenied", "Message": "stand-in"}}, "GetObject")
        return {"Body": io.BytesIO(self.objects[Key])}


def test_the_run_records_are_read_without_regard_to_their_names_write_times_or_order() -> None:
    skipped_then_loaded = Bucket({key_of(RUN_B): record(RUN_B, ["a"], ["c"]), key_of(RUN_A): record(RUN_A, ["a", "c"], [])})
    check(bronze_unsourced.read_unsourced(skipped_then_loaded, BUCKET) == frozenset(), "a record that loaded the table did not require it")
    only_skipped = Bucket({key_of(RUN_B): record(RUN_B, ["a"], ["c"])})
    check(bronze_unsourced.read_unsourced(only_skipped, BUCKET) == {"c"}, "the skipped table was not read")


def test_a_file_under_another_key_is_not_evidence_and_a_wrong_record_under_a_run_key_refuses_the_read() -> None:
    forged = Bucket({"lakehouse/bronze_runs/not_a_run/check.json": FORGED, "lakehouse/bronze_runs/bronze_00000001/carried_over.json": b"{}"})
    check(bronze_unsourced.read_unsourced(forged, BUCKET) == frozenset(), "a key that is not a run record was read")
    wrong = Bucket({key_of(RUN_A): record(RUN_B, ["a"], ["c"])})
    with pytest.raises(bronze_unsourced.UnsourcedError, match="run"):
        bronze_unsourced.read_unsourced(wrong, BUCKET)
    with pytest.raises(bronze_unsourced.UnsourcedError):
        bronze_unsourced.read_unsourced(Bucket({key_of(RUN_A): FORGED}), BUCKET)


class Registered:
    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.tables: list[list[str]] = []
        monkeypatch.setattr(glue_mirror, "layer_tables", lambda layer: ["alpha", "beta", "gamma"])
        monkeypatch.setattr(glue_mirror, "silver_bronze_sources", lambda *arguments: {"alpha"})

        def register_from_s3(config: Any, layer: str, tables: list[str], *rest: Any, **keywords: Any) -> dict[str, Any]:
            self.tables.append(list(tables))
            return {"status": "ok", "tables": {name: "created" for name in tables}}

        monkeypatch.setattr(glue_mirror, "register_from_s3", register_from_s3)


def run_register(s3: Bucket, capsys: pytest.CaptureFixture[str]) -> tuple[int, str, str]:
    code = glue_mirror.main(["register", "--source", "s3", "--layer", "bronze", "--bucket", BUCKET], glue=object(), s3=s3, config=object())
    out = capsys.readouterr()
    return code, out.out, out.err


def test_the_bronze_register_leaves_out_the_unsourced_table_and_says_so(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    registered = Registered(monkeypatch)
    code, out, _ = run_register(Bucket({key_of(RUN_A): record(RUN_A, ["alpha", "beta"], ["gamma"])}), capsys)
    check(code == 0 and registered.tables == [["alpha", "beta"]], f"{code} {registered.tables}")
    check("bronze.gamma: left out" in out and "no source object" in out, out)


def test_the_bronze_register_never_leaves_out_a_table_that_has_metadata_whatever_a_record_says(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """The reviewer's reproduction: valid metadata for every table, then a record that skips one of them."""
    registered = Registered(monkeypatch)
    metadata = {"lakehouse/bronze/gamma/metadata/00001-0f0f0f0f-0f0f-0f0f-0f0f-0f0f0f0f0f0f.metadata.json": b"{}"}
    code, out, _ = run_register(Bucket({key_of(RUN_A): record(RUN_A, ["alpha", "beta"], ["gamma"]), **metadata}), capsys)
    check(code == 0 and registered.tables == [["alpha", "beta", "gamma"]], f"a table with metadata was left out: {registered.tables}")
    check("bronze.gamma" in out and "metadata exists" in out, out)
    forged = Registered(monkeypatch)
    code, _, _ = run_register(
        Bucket({"lakehouse/bronze_runs/not_a_run/check.json": FORGED.replace(b"hcai_util_workbook_sheet_rows", b"gamma"), **metadata}), capsys
    )
    check(code == 0 and forged.tables == [["alpha", "beta", "gamma"]], f"{forged.tables}")


def test_the_bronze_register_still_registers_a_table_that_silver_reads_even_when_a_record_skips_it(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    registered = Registered(monkeypatch)
    code, _, _ = run_register(Bucket({key_of(RUN_A): record(RUN_A, ["beta"], ["alpha"])}), capsys)
    check(code == 0 and registered.tables == [["alpha", "beta", "gamma"]], f"{registered.tables}")


def test_the_bronze_register_with_no_run_record_registers_every_table_as_before(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    registered = Registered(monkeypatch)
    code, _, _ = run_register(Bucket({}), capsys)
    check(code == 0 and registered.tables == [["alpha", "beta", "gamma"]], f"{registered.tables}")


def test_the_bronze_register_fails_the_layer_when_a_run_record_cannot_be_read_or_is_not_a_complete_record(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    registered = Registered(monkeypatch)
    code, out, err = run_register(Bucket({key_of(RUN_A): record(RUN_A, ["alpha"], ["gamma"])}, broken=True), capsys)
    check(code == 1 and registered.tables == [], f"a layer whose records cannot be read registered tables: {registered.tables}")
    check("bronze" in out + err and "run record" in out + err, out + err)
    code, out, _ = run_register(Bucket({key_of(RUN_A): FORGED}), capsys)
    check(code == 1 and registered.tables == [] and "run record" in out, out)
