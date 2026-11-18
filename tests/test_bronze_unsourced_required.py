"""A declared bronze table that the committed bronze run left out for lack of a source object is not required by the steps that read "every mapped table".

Live finding of Nov 18 2026: the bronze load now leaves ``hcai_util_workbook_sheet_rows`` out until its workbooks arrive, so no Iceberg table exists for it.
``register_aws_catalog.required_tables`` and ``glue_mirror register --layer bronze`` listed every table of ``config/lakehouse/bronze_tables.json`` and would
refuse for the absent table. A table that has sources, a table that a later run loaded and a table that a silver model reads stay required.

Failure modes: a record that is not JSON, not a real run's, or has a malformed skipped list; a skipped table that is also loaded; records read in the wrong
order; a skipped name the map does not declare; a silver reader of a skipped table; a record that cannot be read for the mirror (the layer fails, nothing is
guessed).
"""

from __future__ import annotations

import io
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from botocore.exceptions import ClientError

from scripts.lakehouse import bronze_unsourced, glue_mirror, register_aws_catalog
from tests.support import check

ROOT = Path(__file__).resolve().parents[1]
BUCKET = "example-bucket"
TABLE = "hcai_util_workbook_sheet_rows"


def record(loaded: list[str], skipped: list[str], **changes: Any) -> bytes:
    body = {
        "schema_version": 1,
        "run_id": "bronze_3f9a1c07",
        "target": "real",
        "namespaces": ["bronze", "bronze_dictionary"],
        "tables": loaded,
        "skipped_tables": [{"table": name, "reason": "no source object"} for name in skipped],
    } | changes
    return json.dumps(body).encode()


def test_a_record_names_the_tables_it_loaded_and_the_tables_it_skipped() -> None:
    loaded, skipped = bronze_unsourced.record_tables(record(["a", "b"], ["c"]))
    check(loaded == {"a", "b"} and skipped == {"c"}, f"{loaded} {skipped}")
    older = json.loads(record(["a"], []))
    del older["skipped_tables"]
    check(bronze_unsourced.record_tables(json.dumps(older).encode()) == ({"a"}, frozenset()), "an older record skipped something")


@pytest.mark.parametrize(
    "body",
    [
        b"not json",
        b"[]",
        record(["a"], [], schema_version=2),
        record(["a"], [], target="qualify"),
        record(["a"], ["a"]),
        record(["a"], ["c", "c"]),
        record(["a"], [], skipped_tables="c"),
        record(["a"], [], skipped_tables=[{"table": "c"}]),
        record("a", []),
    ],
)
def test_a_record_that_is_not_a_real_run_record_with_a_clean_skipped_list_is_refused(body: bytes) -> None:
    with pytest.raises(bronze_unsourced.UnsourcedError):
        bronze_unsourced.record_tables(body)


def test_the_newest_record_that_names_a_table_decides_whether_it_is_unsourced() -> None:
    first = (frozenset({"a"}), frozenset({"c"}))
    second = (frozenset({"a", "b"}), frozenset())
    third = (frozenset({"a"}), frozenset({"d"}))
    check(bronze_unsourced.unsourced_after([first]) == {"c"}, "a skipped table is not unsourced")
    check(bronze_unsourced.unsourced_after([first, second]) == {"c"}, "a record that does not name the table changed it")
    check(
        bronze_unsourced.unsourced_after([first, (frozenset({"c"}), frozenset())]) == frozenset(), "a later run that loaded the table did not require it again"
    )
    check(bronze_unsourced.unsourced_after([first, second, third]) == {"c", "d"}, "two skipped tables")
    check(bronze_unsourced.unsourced_after([]) == frozenset(), "no record leaves a table out")


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
    readers = sorted(
        glue_mirror.silver_bronze_sources() & {entry["table"] for entry in json.loads((ROOT / "config/lakehouse/bronze_tables.json").read_text())["tables"]}
    )
    check(bool(readers), "no mapped table is read by silver")
    kept = register_aws_catalog.required_tables(ROOT, frozenset({readers[0]}))
    check(("bronze", readers[0]) in kept, f"{readers[0]} is read by silver but was left out")


def test_the_registration_reads_the_unsourced_tables_from_the_run_records_given_oldest_first() -> None:
    check(register_aws_catalog.unsourced_from_records([record(["a"], ["c"])]) == {"c"}, "the skipped table was not read")
    check(
        register_aws_catalog.unsourced_from_records([record(["a"], ["c"]), record(["a", "c"], [])]) == frozenset(),
        "a later load did not require the table again",
    )
    with pytest.raises(register_aws_catalog.RegistrationError) as error:
        register_aws_catalog.unsourced_from_records([b"not json"])
    check(error.value.code == "coverage", f"{error.value.code}")


class Bucket:
    """The read calls the record reader may use. Every record carries its write time, as the listing of S3 gives it."""

    def __init__(self, records: dict[str, bytes], times: dict[str, datetime], broken: bool = False) -> None:
        self.records, self.times, self.broken = records, times, broken

    def get_paginator(self, operation: str) -> Any:
        outer = self

        class Pages:
            def paginate(self, Bucket: str, Prefix: str) -> list[dict[str, Any]]:
                keys = sorted(key for key in outer.records if key.startswith(Prefix))
                return [{"Contents": [{"Key": key, "LastModified": outer.times.get(key)} for key in keys]}]

        return Pages()

    def get_object(self, Bucket: str, Key: str) -> dict[str, Any]:
        if self.broken:
            raise ClientError({"Error": {"Code": "AccessDenied", "Message": "stand-in"}}, "GetObject")
        return {"Body": io.BytesIO(self.records[Key])}


def key_of(run: str) -> str:
    return f"lakehouse/bronze_runs/{run}/check.json"


def moment(hour: int) -> datetime:
    return datetime(2026, 11, 18, hour, tzinfo=UTC)


def test_the_run_records_are_read_in_the_order_they_were_written_not_the_order_of_their_names() -> None:
    older, newer = key_of("bronze_ffffffff"), key_of("bronze_00000001")
    skipped_then_loaded = Bucket({older: record(["a"], ["c"]), newer: record(["a", "c"], [])}, {older: moment(9), newer: moment(10)})
    loaded_then_skipped = Bucket({older: record(["a", "c"], []), newer: record(["a"], ["c"])}, {older: moment(9), newer: moment(10)})
    check(bronze_unsourced.read_unsourced(skipped_then_loaded, BUCKET) == frozenset(), "the later load did not win")
    check(bronze_unsourced.read_unsourced(loaded_then_skipped, BUCKET) == {"c"}, "the later skip did not win")
    other = Bucket({"lakehouse/bronze_runs/bronze_00000001/carried_over.json": b"{}", key_of("x") + "/nested/check.json": b"{}"}, {})
    check(bronze_unsourced.read_unsourced(other, BUCKET) == frozenset(), "a file that is not a run record was read")


def test_a_listing_without_write_times_is_refused_and_never_guessed() -> None:
    with pytest.raises(bronze_unsourced.UnsourcedError, match="write time"):
        bronze_unsourced.read_unsourced(Bucket({key_of("bronze_00000001"): record(["a"], [])}, {}), BUCKET)


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
    older = key_of("bronze_00000001")
    code, out, _ = run_register(Bucket({older: record(["alpha", "beta"], ["gamma"])}, {older: moment(9)}), capsys)
    check(code == 0 and registered.tables == [["alpha", "beta"]], f"{code} {registered.tables}")
    check("bronze.gamma: left out" in out and "no source object" in out, out)


def test_the_bronze_register_still_registers_a_table_that_silver_reads_even_when_a_record_skips_it(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    registered = Registered(monkeypatch)
    older = key_of("bronze_00000001")
    code, _, _ = run_register(Bucket({older: record(["beta"], ["alpha"])}, {older: moment(9)}), capsys)
    check(code == 0 and registered.tables == [["alpha", "beta", "gamma"]], f"{registered.tables}")


def test_the_bronze_register_with_no_run_record_registers_every_table_as_before(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    registered = Registered(monkeypatch)
    code, _, _ = run_register(Bucket({}, {}), capsys)
    check(code == 0 and registered.tables == [["alpha", "beta", "gamma"]], f"{registered.tables}")


def test_the_bronze_register_fails_the_layer_when_the_run_records_cannot_be_read(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    registered = Registered(monkeypatch)
    older = key_of("bronze_00000001")
    code, out, err = run_register(Bucket({older: record(["alpha"], ["gamma"])}, {older: moment(9)}, broken=True), capsys)
    check(code == 1 and registered.tables == [], f"a layer whose records cannot be read registered tables: {registered.tables}")
    check("bronze" in out + err and "run record" in out + err, out + err)
