"""Repeatable synthetic ACS table-vintage capture through the Census API, derived export-layout CSVs and S3 storage, without live requests or credentials."""

import argparse
import copy
import csv
import http.client
import io
import json
import os
import re
import shutil
import socket
import sys
import tempfile
from collections.abc import Callable
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import patch

from scripts.acquisition import api_keys, rate_limit
from scripts.acquisition import bls_api_contract as bls_contract
from scripts.acquisition import census_acs_tables_contract as contract
from scripts.acquisition import census_acs_tables_transport as transport
from scripts.acquisition import transport as common_transport
from scripts.acquisition.bls_api_transport import RetryableRequest
from scripts.acquisition.build_census_acs_tables_plan import build
from scripts.acquisition.collect_census_acs_tables import derived_name, execute, run_all, verify_local
from scripts.acquisition.run_bls_api_e2e import inventory
from scripts.acquisition.s3_store import encoded_json, write_once
from scripts.acquisition.source_registry import canonical_hash, read_json, require
from scripts.acquisition.tests.test_s3_store import OUTPUTS, FakeS3, key_reader

MINI_SCOPE = {"subject": {"S1701": [2023]}, "profile": {"DP03": [2023]}, "detailed": {"B19013": [2016]}}
LINES = ("001", "002", "003")
LABELS = {
    "001": "Estimate!!Total:",
    "002": 'Estimate!!Total!!Speak English "very well"',
    "003": "Estimate!!Total!!Income, in the past 12 months",
}
# Built at run time and low in entropy, so the secret scan has nothing to read as a key.
KEY = "synthetic" + "q" * 20
CT_LEGACY = [1, 3, 5, 7, 9, 11, 13, 15]
CT_PLANNING = list(range(110, 200, 10))
EDITS = {"csv": (b",1000,", b",1001,"), "raw": (b'"1000"', b'"1001"'), "dictionary": (b"Total", b"Totals")}
# The vintage pattern of the lakehouse periods script, which acquisition code may not import; a scenario checks the script still holds it.
ACS_FILE = re.compile(r"ACS(?:DP|ST|DT)5Y(\d{4})\.(?:DP\d{2}|[BCS]\d{4,5})-Data\.csv")
SENTINEL = "-666666666"
ANNOTATION = "*****"


def counties(year: int) -> list[str]:
    """Synthetic county FIPS codes: 55 states of 55 counties, Connecticut by vintage (8 legacy counties or 9 planning regions) and 78 in Puerto Rico."""
    out = []
    for state in range(1, 57):
        numbers = (CT_PLANNING if year >= 2022 else CT_LEGACY) if state == 9 else range(1, 56)
        out += [f"{state:02d}{number:03d}" for number in numbers]
    return out + [f"72{number:03d}" for number in range(1, 156, 2)]


def dictionary(table: str) -> bytes:
    """A group dictionary as the API serves it: estimates, margins and annotations, and the two geography variables."""
    variables: dict[str, dict] = {"GEO_ID": {"label": "Geography"}, "NAME": {"label": "Geographic Area Name"}}
    for line, label in LABELS.items():
        for suffix in ("E", "M", "EA", "MA"):
            variables[f"{table}_{line}{suffix}"] = {"label": label, "predicateType": "int", "group": table}
    return encoded_json({"name": table, "variables": variables})


def response(table: str, year: int, change: Callable[[list], None] | None = None) -> bytes:
    """A group response for every synthetic county; annotations are JSON null."""
    names = sorted([*json.loads(dictionary(table))["variables"], "state", "county"])
    rows: list[list[str | None]] = [names]
    for index, fips in enumerate(counties(year)):
        cells: dict[str, str | None] = {"GEO_ID": f"0500000US{fips}", "NAME": f"County {fips}", "state": fips[:2], "county": fips[2:]}
        for line in LINES:
            cells[f"{table}_{line}E"] = str(1000 + index)
            cells[f"{table}_{line}M"] = "10"
            cells[f"{table}_{line}EA"] = None
            cells[f"{table}_{line}MA"] = None
        rows.append([cells[name] for name in names])
    if change is not None:
        change(rows)
    return json.dumps(rows[:1] + rows[1:][::-1]).encode()


def locked(root: Path) -> tuple[dict, Path]:
    """A locked synthetic plan of three batches, one per product."""
    plan = build(MINI_SCOPE)
    path = root / "plan.json"
    write_once(path, encoded_json(plan))
    write_once(path.with_suffix(".lock.json"), encoded_json({"plan_sha256": canonical_hash(plan)}))
    return plan, path


class Requests:
    """A fake keyed request that serves synthetic responses and dictionaries and counts calls."""

    def __init__(self) -> None:
        self.calls: list[str] = []
        self.override: dict[tuple[str, str], bytes] = {}

    def __call__(self, plan: dict, batch: dict, client: Any) -> bytes:
        kind = "dictionary" if batch.get("kind") == "dictionary" else "data"
        self.calls.append(kind)
        if (batch["table"], kind) in self.override:
            return self.override[(batch["table"], kind)]
        return dictionary(batch["table"]) if kind == "dictionary" else response(batch["table"], batch["year"])


class Reply:
    """A fake HTTP response."""

    def __init__(self, status: int, body: bytes, kind: str = "application/json") -> None:
        self.status, self.body, self.kind = status, body, kind

    def getheader(self, name: str, default: str = "") -> str:
        return self.kind if name == "Content-Type" else default

    def read(self, size: int = -1) -> bytes:
        """The next SIZE bytes; the rest stays for the next read, as a real reply does."""
        count = len(self.body) if size < 0 else size
        chunk, self.body = self.body[:count], self.body[count:]
        return chunk


def connection_class(answer: Callable[[str], Reply | Exception], sent: list[str]) -> type:
    """A stand-in for the HTTPS connection: records each request path and answers by path."""

    class Sock:
        def settimeout(self, _value: float) -> None:
            return None

    class Connection:
        def __init__(self, netloc: str, timeout: float) -> None:
            self.netloc, self.sock, self.path = netloc, Sock(), ""

        def connect(self) -> None:
            return None

        def request(self, method: str, path: str, body: bytes | None = None, headers: dict | None = None) -> None:
            self.path = path
            sent.append(f"{self.netloc}{path}")

        def getresponse(self) -> Reply:
            result = answer(self.path)
            if isinstance(result, Exception):
                raise result
            return result

        def close(self) -> None:
            return None

    return Connection


def live_client() -> FakeS3:
    """A fake project client whose key reader answers with the synthetic key."""
    client = FakeS3()
    client.overrides["get-secret-value"] = {"SecretString": KEY}
    return client


def exercise(root: Path) -> list[dict]:
    """Production orchestration with synthetic inputs, a fake keyed request and versioned fake storage."""
    versions = root / "versions.json"
    write_once(versions, encoded_json({"versions": [{"code_sha256": contract.collector_code_hashes()}]}))
    scenarios: list[dict] = []

    def scenario(name: str, action: Callable[[], Any], error: str | None = None) -> None:
        try:
            action()
            scenarios.append({"name": name, "passed": error is None, "observed": "success", "expected": error or "success"})
        except (ValueError, OSError, KeyError, TypeError, StopIteration, IndexError, AssertionError, RetryableRequest) as failure:
            scenarios.append(
                {"name": name, "passed": error is not None and error in str(failure), "observed": str(failure)[:200], "expected": error or "success"}
            )

    scope_scenarios(scenario)
    base = root / "base"
    base.mkdir()
    plan, plan_path = locked(base)
    requests, client = Requests(), FakeS3()
    with patch.object(contract, "PLAN_PATH", plan_path), patch.object(contract, "VERSIONS_PATH", versions), patch.object(contract, "SCOPE", MINI_SCOPE):
        scenario("locked_scope", contract.load_plan)
        run = root / "run"
        first = plan["batches"][0]
        scenario("capture_one_without_storage", lambda: execute(plan, first, run, True, False, client, OUTPUTS, requests))
        scenario("offline_receipt_replay", lambda: verify_local(run / "batches" / first["id"] / "capture/receipt.json"))

        def all_stored() -> None:
            results, held = run_all(plan, run, True, True, client, OUTPUTS, requests)
            require(not held and len(results) == 3 and all(r["status"] == "stored" for r in results), f"Held: {held}")

        scenario("all_captures_stored", all_stored)
        before, objects, calls = inventory(run), copy.deepcopy(client.objects), len(requests.calls)

        def repeat() -> None:
            _, held = run_all(plan, run, True, True, client, OUTPUTS, requests)
            require(not held and before == inventory(run) and objects == client.objects and len(requests.calls) == calls, "Repeated effects")

        scenario("repeat_one_no_effects", repeat)
        scenario("repeat_two_no_effects", repeat)
        scenario("replay_without_network", lambda: require(not run_all(plan, run, False, True, client, OUTPUTS, None)[1], "Replay needed the network"))
        csv_scenarios(scenario, plan)
        response_scenarios(scenario, plan)
        replay_scenarios(scenario, run, plan)
        binding_scenarios(scenario, root, run, plan)
        transport_scenarios(scenario, root, plan)
        deadline_scenarios(scenario, plan)
        drip_scenario(scenario, plan)

        def unreviewed_current() -> None:
            with patch.object(contract, "collector_code_hashes", return_value={"changed": "0" * 64}):
                execute(plan, first, root / "unreviewed", True, True, client, OUTPUTS, requests)

        scenario("unreviewed_current_code_before_effects", unreviewed_current, "Unreviewed Census tables")
        scenario("unreviewed_run_wrote_nothing", lambda: require(not (root / "unreviewed").exists(), "Unreviewed code wrote files"))

        def interrupted() -> None:
            partial, s3 = root / "partial", FakeS3()
            s3.corrupt = True
            with suppress(ValueError):
                execute(plan, first, partial, True, True, s3, OUTPUTS, requests)
            require(len(s3.objects) == 1, "No interrupted upload")
            s3.corrupt = False
            execute(plan, first, partial, True, True, s3, OUTPUTS, requests)
            stored = copy.deepcopy(s3.objects)
            execute(plan, first, partial, True, True, s3, OUTPUTS, requests)
            require(stored == s3.objects, "Resume duplicated effects")

        scenario("resume_interrupted_storage", interrupted)
    return scenarios


def scope_scenarios(scenario: Callable[..., None]) -> None:
    """Test 1, 2 and 5: the committed plan binds the 173 table-vintage units, requests carry no key and every file name carries its vintage."""
    real = read_json(contract.PLAN_PATH)

    def plan_has_173_bound_batches() -> None:
        require(read_json(contract.PLAN_PATH.with_suffix(".lock.json"))["plan_sha256"] == canonical_hash(real), "Real plan lock differs")
        pairs = [(b["table"], b["year"]) for b in real["batches"]]
        expected = [(b["table"], b["year"]) for b in contract.scope_batches()]
        require(len(pairs) == 173 and sorted(pairs) == sorted(expected) and len(set(pairs)) == 173, "Real scope differs from the scope table")
        require(len({b["id"] for b in real["batches"]}) == 173, "Batch IDs are not unique")
        for batch in real["batches"]:
            require(
                batch["id"] == canonical_hash({"endpoint": contract.endpoint(batch["product"], batch["year"]), "parameters": contract.request_for(batch)}),
                "ID differs",
            )
        require(real["credential_reference"] == "census_api_key" and real["source_id"] == "ACS" and real["model_eligible"] is False, "Plan hold differs")
        require(contract.load_plan()["batches"] == real["batches"], "Real plan does not load")

    scenario("real_plan_binds_173_unique_hashed_batches", plan_has_173_bound_batches)

    def lock_rejects_edits() -> None:
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "plan.json"
            edited = copy.deepcopy(real)
            edited["batches"][0]["year"] += 1
            path.write_bytes(encoded_json(edited))
            path.with_suffix(".lock.json").write_bytes(encoded_json({"plan_sha256": canonical_hash(real)}))
            contract.load_plan(path)

    scenario("plan_lock_rejects_an_edited_batch", lock_rejects_edits, "plan lock differs")

    def relocked_edit_rejected() -> None:
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "plan.json"
            edited = copy.deepcopy(real)
            edited["batches"][0]["endpoint"] = "https://api.census.gov/data/1999/acs/acs5"
            path.write_bytes(encoded_json(edited))
            path.with_suffix(".lock.json").write_bytes(encoded_json({"plan_sha256": canonical_hash(edited)}))
            contract.load_plan(path)

    scenario("a_relocked_batch_that_breaks_its_binding_is_refused", relocked_edit_rejected, "batch binding differs")

    def requests_carry_no_key() -> None:
        shapes = set()
        for batch in real["batches"]:
            parameters = contract.request_for(batch)
            require("key" not in parameters and set(parameters) == {"get", "for", "in"}, "A request parameter set differs")
            require(parameters["get"] == f"group({batch['table']})", "Group call differs")
            url = contract.endpoint(batch["product"], batch["year"])
            suffix = {"subject": "/subject", "profile": "/profile", "detailed": ""}[batch["product"]]
            require(url == f"https://api.census.gov/data/{batch['year']}/acs/acs5{suffix}" and batch["endpoint"] == url, "Endpoint differs")
            shapes.add(batch["product"])
        require(shapes == {"subject", "profile", "detailed"}, "A product is missing")
        require("key=" not in json.dumps(real) and "key=" not in json.dumps([contract.dictionary_url(b) for b in real["batches"]]), "A URL holds a key")

    scenario("request_for_never_holds_a_key_and_each_product_and_year_has_its_endpoint", requests_carry_no_key)
    scenario(
        "a_table_outside_the_scope_has_no_request",
        lambda: contract.request_for({"table": "B01001", "year": 2020, "product": "detailed"}),
        "outside approved scope",
    )

    def file_names_carry_the_vintage() -> None:
        periods = (contract.REPO_ROOT / "scripts/lakehouse/geography_file_periods.py").read_text(encoding="utf-8")
        require(ACS_FILE.pattern in periods, "The geography periods script no longer holds the vintage pattern of this check")
        for batch in real["batches"]:
            match = ACS_FILE.fullmatch(batch["file_name"])
            require(match is not None and int(match.group(1)) == batch["year"], f"File name {batch['file_name']} does not carry its vintage")
            require(re.fullmatch(r"ACS(ST|DP|DT)5Y\d{4}\.[A-Z0-9]+-Data\.csv", batch["file_name"]) is not None, "File name layout differs")
        require(len({b["file_name"] for b in real["batches"]}) == 173, "File names repeat")

    scenario("all_173_file_names_match_the_vintage_pattern_of_the_periods_script", file_names_carry_the_vintage)


def csv_scenarios(scenario: Callable[..., None], plan: dict) -> None:
    """Test 4: the derived CSV keeps the export layout, native tokens and reproducible bytes."""
    batch = next(b for b in plan["batches"] if b["table"] == "S1701")
    table = batch["table"]

    def layout() -> None:
        body, statistics = contract.validate_response(response(table, batch["year"]), dictionary(table), batch)
        rows = list(csv.reader(io.StringIO(body.decode(), newline="")))
        require(rows[0][:2] == ["GEO_ID", "NAME"] and rows[0][-2:] == ["state", "county"], "Code row differs")
        require(rows[0][2:-2] == sorted(rows[0][2:-2]), "Variable codes not sorted")
        require(rows[1][0] == "Geography" and rows[1][1] == "Geographic Area Name" and len(rows[1]) == len(rows[0]), "Label row differs")
        labels = dict(zip(rows[0], rows[1], strict=True))
        require(labels[f"{table}_001E"] == LABELS["001"] and labels[f"{table}_002E"] == LABELS["002"], "Labels differ from the dictionary")
        require(b'""very well""' in body and b"\r" not in body and not body.startswith(b"\xef\xbb\xbf"), "Quoting, line ends or byte order mark differ")
        ids = [r[0] for r in rows[2:]]
        require(ids == sorted(ids) and len(ids) == statistics["rows"] == len(counties(batch["year"])), "Rows are not sorted or complete")
        require(rows[2][rows[0].index(f"{table}_001EA")] == "\\N", "Null annotation is not the null marker")
        require(statistics["puerto_rico_counties"] == 78 and statistics["connecticut_counties"] == 9, f"Statistics differ: {statistics}")

    scenario("derived_csv_has_the_export_layout_labels_quotes_nulls_and_sorted_rows", layout)

    def reproducible() -> None:
        first = contract.validate_response(response(table, batch["year"]), dictionary(table), batch)
        reordered = json.loads(response(table, batch["year"]))
        reordered = [reordered[0], *reversed(reordered[1:])]
        second = contract.validate_response(json.dumps(reordered).encode(), dictionary(table), batch)
        require(first == second, "The CSV depends on row order or differs between runs")

    scenario("derived_csv_is_byte_identical_for_identical_input_in_any_row_order", reproducible)

    def native_tokens() -> None:
        def special(rows: list) -> None:
            column = rows[0].index(f"{table}_001E")
            rows[1][column] = SENTINEL
            rows[2][rows[0].index(f"{table}_001M")] = ANNOTATION

        body, _ = contract.validate_response(response(table, batch["year"], special), dictionary(table), batch)
        text = body.decode()
        require(SENTINEL in text and ANNOTATION in text, "A native token was not kept as served")

    scenario("native_tokens_are_kept_as_served", native_tokens)

    def derived_names_match() -> None:
        require(all(derived_name(b) == b["file_name"] for b in plan["batches"]), "Derived name differs from the plan")

    scenario("derived_name_is_the_plan_file_name", derived_names_match)


def response_scenarios(scenario: Callable[..., None], plan: dict) -> None:
    """Test 3: each response defect stops that unit with a fixed message that names no response text."""
    batch = next(b for b in plan["batches"] if b["table"] == "DP03")
    table, year = batch["table"], batch["year"]
    planted = "PLANTEDRESPONSETEXT"

    def mutate(change: Callable[[list], None]) -> bytes:
        return response(table, year, change)

    def drop_last_column(rows: list) -> None:
        for row in rows:
            row.pop()

    def extra_column(rows: list) -> None:
        rows[0].append(f"{table}_999E")
        for row in rows[1:]:
            row.append(planted)

    def drop_annotation_column(rows: list) -> None:
        index = rows[0].index(f"{table}_001EA")
        for row in rows:
            del row[index]

    def duplicate_header(rows: list) -> None:
        rows[0][2] = rows[0][3]

    def ragged_row(rows: list) -> None:
        rows[5].append(planted)

    def null_marker_cell(rows: list) -> None:
        rows[5][rows[0].index(f"{table}_001E")] = "\\N"

    def number_cell(rows: list) -> None:
        rows[5][rows[0].index(f"{table}_001E")] = 7

    def geography_mismatch(rows: list) -> None:
        rows[1][rows[0].index("GEO_ID")] = "0500000US99999"

    def short_state(rows: list) -> None:
        rows[1][rows[0].index("state")] = "1"
        rows[1][rows[0].index("GEO_ID")] = "0500000US1" + rows[1][rows[0].index("county")]

    def nonnumeric_county(rows: list) -> None:
        rows[1][rows[0].index("county")] = "0x1"

    def duplicate_county(rows: list) -> None:
        rows.append(list(rows[1]))

    cases: list[tuple[str, bytes, str]] = [
        ("not_json", b"<html>" + planted.encode(), "invalid JSON"),
        ("json_object_not_array", json.dumps({"error": planted}).encode(), "JSON schema changed"),
        ("header_only", json.dumps([["GEO_ID"]]).encode(), "JSON schema changed"),
        ("header_not_text", json.dumps([[1, 2], ["a", "b"]]).encode(), "JSON schema changed"),
        ("header_missing_a_column", mutate(drop_last_column), "headers differ"),
        ("header_has_an_extra_column", mutate(extra_column), "headers differ"),
        ("annotation_column_dropped", mutate(drop_annotation_column), "headers differ"),
        ("header_has_a_duplicate", mutate(duplicate_header), "headers differ"),
        ("row_width_differs", mutate(ragged_row), "row width changed"),
        ("cell_holds_the_null_marker", mutate(null_marker_cell), "native cell type changed"),
        ("cell_is_not_text_or_null", mutate(number_cell), "native cell type changed"),
        ("geography_differs_from_state_and_county", mutate(geography_mismatch), "geography invalid"),
        ("state_has_one_digit", mutate(short_state), "geography invalid"),
        ("county_is_not_numeric", mutate(nonnumeric_county), "geography invalid"),
        ("duplicate_county", mutate(duplicate_county), "Duplicate county"),
        ("below_the_county_floor", mutate(lambda rows: rows.__delitem__(slice(1, 200))), "below the floor"),
    ]
    for name, raw, error in cases:

        def refused(raw: bytes = raw, error: str = error) -> None:
            try:
                contract.validate_response(raw, dictionary(table), batch)
            except ValueError as failure:
                require(planted not in str(failure) and "0500000US" not in str(failure), "The refusal names response text")
                raise
            raise ValueError("Response accepted")

        scenario(name, refused, error)

    def dictionary_defect(change: Callable[[dict], None]) -> Callable[[], Any]:
        def run() -> Any:
            document = json.loads(dictionary(table))
            change(document)
            return contract.validate_response(response(table, year), json.dumps(document).encode(), batch)

        return run

    scenario("dictionary_not_json", lambda: contract.validate_response(response(table, year), b"<html>", batch), "dictionary invalid")
    scenario("dictionary_without_variables", dictionary_defect(lambda d: d.pop("variables")), "dictionary schema changed")
    scenario("dictionary_label_not_text", dictionary_defect(lambda d: d["variables"][f"{table}_001E"].__setitem__("label", 3)), "dictionary schema changed")
    scenario(
        "dictionary_variable_missing_from_the_data", dictionary_defect(lambda d: d["variables"].__setitem__(f"{table}_777E", {"label": "x"})), "headers differ"
    )


def replay_scenarios(scenario: Callable[..., None], run: Path, plan: dict) -> None:
    """Test 8: replay rebuilds the derived CSV from the stored raw response and dictionary without a network, and refuses tampered stores."""
    first = plan["batches"][0]

    def offline_rebuild() -> None:
        def no_network(*_args: Any, **_options: Any) -> Any:
            raise AssertionError("verify_capture used the network")

        with patch.object(http.client, "HTTPSConnection", no_network), patch.object(transport, "request_dictionary", no_network):
            verify_local(run / "batches" / first["id"] / "capture/receipt.json")

    scenario("verify_capture_rebuilds_the_csv_from_stored_bytes_without_a_network", offline_rebuild)

    def corrupt(kind: str) -> None:
        target_root = run.parent / f"corrupt_{kind}"
        if not target_root.exists():
            import shutil

            shutil.copytree(run, target_root)
        target = target_root / "batches" / first["id"] / "capture"
        receipt = read_json(target / "receipt.json")
        lineage = json.loads(receipt["lineage"]["extraction_or_query"])
        path = target / {"csv": f"derived/{first['file_name']}", "dictionary": "references/group.json", "raw": "raw/response.json"}.get(kind, "receipt.json")
        if kind in {"csv", "dictionary", "raw"}:
            path.write_bytes(path.read_bytes().replace(*EDITS[kind], 1))
            from scripts.acquisition.s3_store import fingerprint

            for item in receipt["artifacts"]:
                if item["storage_path"] == path.relative_to(target).as_posix():
                    item["sha256"], item["byte_count"] = fingerprint(path)
            if kind == "raw":
                lineage["expected_sha256"] = fingerprint(path)[0]
        elif kind == "model_promotion":
            lineage["model_eligible"] = True
        elif kind == "unreviewed_code":
            lineage["code_sha256"] = {"bad": "0" * 64}
        receipt["lineage"]["extraction_or_query"] = json.dumps(lineage)
        (target / "receipt.json").write_bytes(encoded_json(receipt))
        verify_local(target / "receipt.json")

    for kind, error in [
        ("csv", "CSV differs"),
        ("dictionary", "dictionary differs"),
        ("raw", "CSV differs"),
        ("model_promotion", "hold differs"),
        ("unreviewed_code", "Unreviewed Census tables"),
    ]:
        scenario(f"resealed_{kind}_is_refused_on_replay", lambda kind=kind: corrupt(kind), error)


def binding_scenarios(scenario: Callable[..., None], root: Path, run: Path, plan: dict) -> None:
    """Round 1 review: a cached capture or completion record must belong to the selected unit and every receipt field must match it."""
    first, other = plan["batches"][:2]

    def substituted() -> None:
        folder, client = root / "substituted", FakeS3()
        execute(plan, other, folder, True, False, client, OUTPUTS, Requests())
        shutil.copytree(folder / "batches" / other["id"] / "capture", folder / "batches" / first["id"] / "capture")
        try:
            execute(plan, first, folder, False, True, client, OUTPUTS, None)
        finally:
            require(not client.objects, "A capture of another unit was stored")

    scenario("a_capture_of_another_unit_in_this_units_cache_is_refused_and_stores_nothing", substituted, "differs from the selected unit")

    def completion_rows_changed() -> None:
        folder, client = root / "completion", FakeS3()
        execute(plan, first, folder, True, True, client, OUTPUTS, Requests())
        done_path = folder / "batches" / first["id"] / "completed.json"
        done = read_json(done_path)
        done["rows"] += 100
        done_path.write_bytes(encoded_json(done))
        execute(plan, first, folder, False, True, client, OUTPUTS, None)

    scenario("a_completion_record_with_another_row_count_than_its_receipt_is_refused", completion_rows_changed, "completion row count differs")

    def completion_names_another_unit() -> None:
        folder, client = root / "completion_unit", FakeS3()
        execute(plan, first, folder, True, True, client, OUTPUTS, Requests())
        done_path = folder / "batches" / first["id"] / "completed.json"
        done = read_json(done_path)
        done["id"] = other["id"]
        done_path.write_bytes(encoded_json(done))
        execute(plan, first, folder, False, True, client, OUTPUTS, None)

    scenario("a_completion_record_of_another_unit_is_refused", completion_names_another_unit, "completion receipt differs")

    def edited(name: str, change: Callable[[dict], None]) -> Callable[[], Any]:
        def action() -> Any:
            target_root = run.parent / f"receipt_edit_{name}"
            shutil.copytree(run, target_root)
            target = target_root / "batches" / first["id"] / "capture"
            receipt = read_json(target / "receipt.json")
            change(receipt)
            (target / "receipt.json").write_bytes(encoded_json(receipt))
            return verify_local(target / "receipt.json")

        return action

    def another_window(receipt: dict) -> None:
        receipt["measurement_periods"][0].update(start_date="2006-01-01", end_date="2010-12-31")

    def another_endpoint(receipt: dict) -> None:
        receipt["acquisition"]["resolved_url"] = "https://api.census.gov/data/1999/acs/acs5"

    def another_governance(receipt: dict) -> None:
        receipt["governance"]["access_class"] = "restricted"

    def another_release(receipt: dict) -> None:
        receipt["release"]["observed_history"] = "2006 S1701 all-fields county request"

    def another_snapshot(receipt: dict) -> None:
        receipt["snapshot_id"] = receipt["snapshot_id"][:-8] + "0" * 8

    def another_headers(receipt: dict) -> None:
        receipt["schema_profile"]["native_headers"] = receipt["schema_profile"]["native_headers"][::-1]

    def another_method(receipt: dict) -> None:
        receipt["acquisition"]["request_method"] = "POST"

    def another_route(receipt: dict) -> None:
        lineage = json.loads(receipt["lineage"]["extraction_or_query"])
        lineage["route_id"] = "ACS:tables_api:subject:S1701:2012"
        receipt["lineage"]["extraction_or_query"] = json.dumps(lineage)

    for name, change, error in [
        ("measurement_period", another_window, "measurement period differs"),
        ("resolved_url", another_endpoint, "resolved URL differs"),
        ("governance", another_governance, "governance differs"),
        ("release_text", another_release, "release text differs"),
        ("snapshot_id", another_snapshot, "snapshot ID differs"),
        ("native_headers", another_headers, "native headers differ"),
        ("request_method", another_method, "request method differs"),
        ("route_id", another_route, "route differs"),
    ]:
        scenario(f"a_receipt_with_another_{name}_is_refused_on_replay", edited(name, change), error)

    def cached_dictionary_url_changed() -> None:
        folder = root / "cache_url"
        execute(plan, first, folder, True, False, FakeS3(), OUTPUTS, Requests())
        path = folder / "batches" / first["id"] / "transport/request.json"
        envelope = read_json(path)
        envelope["dictionary_url"] = envelope["dictionary_url"].replace("S1701", "S2701")
        path.write_bytes(encoded_json(envelope))
        transport.fetch(plan, first, folder, False, None)

    scenario("a_cached_envelope_that_names_another_dictionary_is_refused", cached_dictionary_url_changed, "cache differs")


def deadline_scenarios(scenario: Callable[..., None], plan: dict) -> None:
    """Round 1 review: a name lookup that outlasts the request deadline fails the attempt before anything is sent, for the dictionary and the keyed request."""
    batch = plan["batches"][0]

    def stalled(action: Callable[[FakeS3], Any]) -> None:
        clock, sent, closed = [0.0], [], []
        client = live_client()

        class Sock:
            def setsockopt(self, *_args: object) -> None:
                return None

            def settimeout(self, _seconds: float) -> None:
                return None

            def sendall(self, _data: bytes) -> None:
                sent.append(1)

            def makefile(self, _mode: str) -> io.BytesIO:
                body = dictionary(batch["table"])
                return io.BytesIO(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: " + str(len(body)).encode() + b"\r\n\r\n" + body)

            def close(self) -> None:
                closed.append(1)

        class TLS:
            def wrap_socket(self, sock: Sock, **_options: object) -> Sock:
                return sock

        def resolve(*_args: object, **_options: object) -> list:
            clock[0] += 121.0
            return [(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", ("93.184.216.34", 443))]

        environment = {k: v for k, v in os.environ.items() if k != "HAI_CAPTURE_ROOT"}
        original_close = common_transport.PublicHTTPSConnection.close
        connections: list[int] = []

        def counted_close(self: Any) -> None:
            connections.append(1)
            original_close(self)

        with (
            patch.dict(os.environ, environment, clear=True),
            patch.object(common_transport.time, "monotonic", lambda: clock[0]),
            patch.object(socket, "getaddrinfo", resolve),
            patch.object(socket, "create_connection", lambda *_args: Sock()),
            patch.object(common_transport.ssl, "create_default_context", TLS),
            patch.object(common_transport.PublicHTTPSConnection, "close", counted_close),
            patch.object(api_keys, "read", key_reader(client)),
        ):
            try:
                action(client)
            finally:
                require(not sent, "A request was sent after the deadline passed")
                require(bool(connections), "The connection was not closed")

    scenario(
        "a_dictionary_lookup_that_outlasts_the_deadline_fails_before_anything_is_sent",
        lambda: stalled(lambda _client: transport.request_dictionary(contract.dictionary_url(batch))),
        "network failure",
    )
    scenario(
        "a_keyed_lookup_that_outlasts_the_deadline_fails_before_anything_is_sent",
        lambda: stalled(lambda client: transport.live_request(plan, batch, client)),
        "network failure",
    )


def drip_scenario(scenario: Callable[..., None], plan: dict) -> None:
    """A reply that arrives a little at a time cannot outlast the request deadline: the reply is read in pieces with the time check between them."""
    batch = plan["batches"][0]

    def dripping() -> None:
        clock, sent = [0.0], list[str]()

        class Drip(Reply):
            def read(self, size: int = -1) -> bytes:
                clock[0] += 50.0
                return b"["

        environment = {k: v for k, v in os.environ.items() if k != "HAI_CAPTURE_ROOT"}
        with (
            patch.dict(os.environ, environment, clear=True),
            patch.object(transport.time, "monotonic", lambda: clock[0]),
            patch.object(http.client, "HTTPSConnection", connection_class(lambda _path: Drip(200, b""), sent)),
        ):
            transport.request_dictionary(contract.dictionary_url(batch))

    scenario("a_reply_that_drips_past_the_deadline_is_refused_before_it_is_read_to_the_end", dripping, "network failure")


def transport_scenarios(scenario: Callable[..., None], root: Path, plan: dict) -> None:
    """Tests 6 and 7: the key stays out of every file and log, transient failures retry three times, redirects and oversized replies are refused."""
    batch = plan["batches"][0]
    folder = [0]

    def no_sleep(_seconds: float) -> None:
        return None

    def run_with(answer: Callable[[str], Reply | Exception], action: Callable[[FakeS3, Path], Any], sent: list[str] | None = None) -> list[str]:
        sent = [] if sent is None else sent
        client = live_client()
        folder[0] += 1
        target = root / f"transport_{folder[0]}"
        environment = {k: v for k, v in os.environ.items() if k != "HAI_CAPTURE_ROOT"}
        with (
            patch.dict(os.environ, environment, clear=True),
            patch.object(http.client, "HTTPSConnection", connection_class(answer, sent)),
            patch.object(api_keys, "read", key_reader(client)),
        ):
            action(client, target)
        return sent

    def served(path: str) -> Reply:
        if "/groups/" in path:
            return Reply(200, dictionary(batch["table"]))
        return Reply(200, response(batch["table"], batch["year"]))

    def key_stays_out() -> None:
        target_holder: list[Path] = []

        def action(client: FakeS3, target: Path) -> None:
            target_holder.append(target)
            execute(plan, batch, target, True, True, client, OUTPUTS, transport.live_request)

        sent = run_with(served, action)
        require(len(sent) == 2 and KEY in sent[0] and KEY not in sent[1] and "/groups/" in sent[1], f"Key sent to the wrong request: {sent}")
        leaks = [str(p) for p in target_holder[0].rglob("*") if p.is_file() and KEY.encode() in p.read_bytes()]
        require(not leaks, f"The key is in stored files: {leaks}")

    scenario("the_key_goes_only_to_the_keyed_request_and_appears_in_no_stored_file", key_stays_out)

    def key_in_storage_and_logs() -> None:
        import logging

        stream = io.StringIO()
        handler = logging.StreamHandler(stream)
        logging.getLogger().addHandler(handler)
        try:
            holder: list[FakeS3] = []

            def action(client: FakeS3, target: Path) -> None:
                holder.append(client)
                execute(plan, batch, target, True, True, client, OUTPUTS, transport.live_request)

            run_with(served, action)
        finally:
            logging.getLogger().removeHandler(handler)
        require(KEY not in stream.getvalue(), "The key is in the log")
        require(KEY not in json.dumps(holder[0].objects, default=repr) and KEY not in repr(holder[0].calls), "The key is in the stored objects or calls")

    scenario("the_key_appears_in_no_object_call_or_log_line", key_in_storage_and_logs)

    def echoed() -> None:
        run_with(
            lambda path: Reply(200, json.dumps([["GEO_ID"], [KEY]]).encode()) if "/groups/" not in path else served(path),
            lambda client, target: execute(plan, batch, target, True, False, client, {}, transport.live_request),
        )

    scenario("a_reply_that_echoes_the_key_fails_with_a_fixed_message", echoed, "echoed credential")

    def echo_leaves_nothing() -> None:
        target = root / f"transport_{folder[0]}"
        require(not any(KEY.encode() in p.read_bytes() for p in target.rglob("*") if p.is_file()), "The echoed key was stored")
        require(not (target / "batches" / batch["id"] / "capture").exists(), "A capture exists after the echo")

    scenario("an_echoing_reply_stores_nothing", echo_leaves_nothing)

    def fetch_with(status: int, sent: list[str]) -> None:
        run_with(
            lambda _path: Reply(status, b"{}"),
            lambda client, target: transport.fetch(plan, batch, target, True, client, transport.live_request, no_sleep),
            sent,
        )

    def attempts(status: int, expected: int, error: str) -> None:
        sent: list[str] = []
        try:
            fetch_with(status, sent)
        except (RetryableRequest, ValueError, rate_limit.RateLimited) as failure:
            require(error in str(failure) and KEY not in str(failure), f"Unexpected refusal: {failure}")
            require(len(sent) == expected, f"{len(sent)} requests for status {status}, expected {expected}")
            return
        raise ValueError("The failing reply was accepted")

    for status in (500, 502, 503, 504):
        scenario(
            f"status_{status}_makes_exactly_three_attempts_then_fails_without_text", lambda status=status: attempts(status, 3, "transient publisher failure")
        )
    scenario("status_429_makes_one_attempt_and_is_a_typed_rate_limit", lambda: attempts(429, 1, "Publisher rate limit"))

    class Delayed(Reply):
        """A 429 reply that names its delay in the Retry-After header."""

        def __init__(self, delay: str) -> None:
            super().__init__(429, b"{}")
            self.delay = delay

        def getheader(self, name: str, default: str = "") -> str:
            return self.delay if name == "Retry-After" else super().getheader(name, default)

    def delay_of(header: str) -> str | None:
        sent: list[str] = []
        try:
            run_with(
                lambda _path: Delayed(header),
                lambda client, target: transport.fetch(plan, batch, target, True, client, transport.live_request, no_sleep),
                sent,
            )
        except rate_limit.RateLimited as limited:
            require(len(sent) == 1, f"{len(sent)} requests for a rate limit")
            return limited.retry_after
        raise ValueError("The rate limit was accepted")

    long_header = "9" * (rate_limit.HEADER_LIMIT + 1)
    for name, header, kept in (
        ("seconds", "45", "45"),
        ("a_large_value_is_not_shortened", "999999", "999999"),
        ("an_http_date", "Wed, 18 Nov 2026 19:00:00 GMT", "Wed, 18 Nov 2026 19:00:00 GMT"),
        ("text", "soon", "soon"),
        ("absent", "", None),
        ("over_the_bound", long_header, None),
        ("a_control_character", "4\n5", None),
    ):
        scenario(
            f"a_rate_limit_reports_the_header_{name}",
            lambda header=header, kept=kept: require(delay_of(header) == kept, f"Header {delay_of(header)!r} for {header!r}"),
        )
    scenario("a_redirect_is_refused_after_one_attempt", lambda: attempts(302, 1, "no redirect followed"))

    def over_cap(limit: int, dictionary_reply: bool) -> None:
        body = b"[" + b"0" * limit

        def answer(path: str) -> Reply:
            return Reply(200, body if ("/groups/" in path) == dictionary_reply else response(batch["table"], batch["year"]))

        run_with(answer, lambda client, target: transport.fetch(plan, batch, target, True, client, transport.live_request, no_sleep))

    scenario("a_data_response_over_64_mib_is_refused", lambda: over_cap(64 * 1024**2, False), "response size rejected")
    scenario("a_dictionary_over_8_mib_is_refused", lambda: over_cap(8 * 1024**2, True), "dictionary size rejected")

    def wrong_type() -> None:
        run_with(
            lambda _path: Reply(200, b"<html></html>", "text/html"),
            lambda client, target: transport.fetch(plan, batch, target, True, client, transport.live_request, no_sleep),
        )

    scenario("a_reply_that_is_not_json_is_refused", wrong_type, "HTTP status/type rejected")

    def dictionary_unkeyed() -> None:
        sent = run_with(served, lambda client, target: transport.fetch(plan, batch, target, True, client, transport.live_request, no_sleep))
        require(all(KEY not in item for item in sent[1:]) and len(sent) == 2, "The dictionary request carried the key")

    scenario("the_dictionary_request_is_unkeyed", dictionary_unkeyed)

    def foreign_host() -> None:
        transport.request_dictionary("https://example.org/data/2023/acs/acs5/groups/S1701.json")

    scenario("a_dictionary_url_off_the_census_host_is_refused", foreign_host, "scope differs")


def main() -> None:
    """Write one immutable artifact for every E2E invocation."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="census_tables_e2e_") as directory:
        scenarios = exercise(Path(directory))
    passed = all(s["passed"] for s in scenarios)
    report = {
        "status": "passed" if passed else "failed",
        "feature": "ACS table-vintage capture through the Census API: scope plan, derived export-layout CSV, key handling, retries and versioned S3 storage",
        "run_at_utc": datetime.now(UTC).isoformat(),
        "python": sys.version.split()[0],
        "code_sha256": contract.collector_code_hashes(),
        "dependency_sha256": bls_contract.digest(Path("uv.lock").read_bytes()),
        "reproduce": f".venv/bin/python -m scripts.acquisition.run_census_acs_tables_e2e --output {args.output}",
        "boundary": "Synthetic dictionaries, 3,100-county responses and a fake HTTPS connection with fake versioned S3; no live Census or AWS call.",
        "cleanup": "Temporary folder removed on exit.",
        "scenarios": scenarios,
    }
    write_once(args.output, encoded_json(report))
    sys.stdout.write(
        json.dumps({"status": report["status"], "passed": sum(s["passed"] for s in scenarios), "total": len(scenarios), "artifact": str(args.output)}) + "\n"
    )
    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
