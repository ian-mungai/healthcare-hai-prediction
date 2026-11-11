"""Bronze pseudonymizes the person fields of a version 2 capture at load and leaves a version 1 capture unchanged.

Every value is synthetic. The cases run the loader's real CSV boundary (``bronze.read_csv_object``) over files written to a temporary folder and
read the rows bronze would hand to Spark. The Spark write that follows is covered by the bronze end-to-end suites.
"""

from __future__ import annotations

import base64
import csv
import hashlib
import json
import re
from collections.abc import Callable
from pathlib import Path

import pytest

from scripts.acquisition import cms_owners_contract, onc_mu_contract
from scripts.lakehouse import bronze, person_fields
from tests.support import check

PSEUDONYM = re.compile("v1:[0-9a-f]{64}")
OWNER_TABLE = "cms_hospital_owners"
ONC_TABLE = "onc_pi_attestations_csv"
PEOPLE = ("Zelda", "Quimby", "Wally", "Obrien", "0123456789", "0999999999", "11 Nowhere Lane", "Sampletown", "Zelda Quimby Holdings", "Owner Alpha")


def key_for(label: str) -> Callable[[str], str | None]:
    text = base64.urlsafe_b64encode(hashlib.sha256(label.encode()).digest()).decode()
    return lambda _version: text


def no_key(_version: str) -> str | None:
    raise AssertionError("the key must not be read for this capture")


def write_csv(path: Path, headers: list[str], rows: list[dict[str, str]]) -> Path:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(headers)
        writer.writerows([[row.get(h, "") for h in headers] for row in rows])
    return path


def owner_row(kind: str, **cells: str) -> dict[str, str]:
    return {"ENROLLMENT ID": "E-1", "ASSOCIATE ID": "A-1", "TYPE - OWNER": kind, "PERCENTAGE OWNERSHIP": "25", "STATE - OWNER": "ZZ", **cells}


OWNER_ROWS = [
    owner_row(
        "I",
        **{
            "ASSOCIATE ID - OWNER": "0123456789",
            "FIRST NAME - OWNER": "Zelda",
            "LAST NAME - OWNER": "Quimby",
            "TITLE - OWNER": "Owner Alpha",
            "ADDRESS LINE 1 - OWNER": "11 Nowhere Lane",
            "CITY - OWNER": "Sampletown",
            "ZIP CODE - OWNER": "99999",
        },
    ),
    owner_row(
        "I",
        **{
            "ASSOCIATE ID - OWNER": "0999999999",
            "FIRST NAME - OWNER": "Wally",
            "LAST NAME - OWNER": "Obrien",
            "ADDRESS LINE 1 - OWNER": "22 Elsewhere Road",
            "CITY - OWNER": "Sampletown",
            "ZIP CODE - OWNER": "99998",
        },
    ),
    owner_row(
        "O",
        **{
            "ASSOCIATE ID - OWNER": "B-777",
            "ORGANIZATION NAME - OWNER": "Plain Industries Inc",
            "ADDRESS LINE 1 - OWNER": "1 Main Street",
            "CITY - OWNER": "Orgville",
            "ZIP CODE - OWNER": "99001",
        },
    ),
    owner_row("O", **{"ASSOCIATE ID - OWNER": "B-778", "ORGANIZATION NAME - OWNER": "Zelda Quimby Holdings LLC"}),
]


def load(tmp_path: Path, table: str, headers: list[str], rows: list[dict[str, str]], keys: Callable[[str], str | None]) -> tuple[dict, str]:
    source = write_csv(tmp_path / "source.csv", headers, rows)
    rows_path = tmp_path / "rows.jsonl"
    item = {"table": table, "encodings": ["utf-8", "cp1252"], "format": "csv"}
    found = bronze.read_csv_object(item, source, rows_path, keys)
    return (found, rows_path.read_text())


def records(text: str) -> list[list[str]]:
    return [json.loads(line)["v"] for line in text.splitlines()]


def test_version_2_owner_capture_reaches_bronze_without_person_fields(tmp_path: Path) -> None:
    headers = cms_owners_contract.HEADERS["v2"]
    found, text = load(tmp_path, OWNER_TABLE, headers, OWNER_ROWS, key_for("synthetic one"))
    for value in PEOPLE[:-1]:
        check(
            value.lower() not in text.lower() and value not in found["headers"], 'expected: value.lower() not in text.lower() and value not in found["headers"]'
        )
    for column in cms_owners_contract.DROPPED:
        check(
            column not in found["headers"] or column in ("ADDRESS LINE 1 - OWNER", "ADDRESS LINE 2 - OWNER", "CITY - OWNER", "ZIP CODE - OWNER"),
            'expected: column not in found["headers"] or column in ("ADDRESS LINE 1 - OWNER", "ADDRESS LINE 2 - O',
        )
    check(
        "FIRST NAME - OWNER" not in found["headers"] and "TITLE - OWNER" not in found["headers"],
        'expected: "FIRST NAME - OWNER" not in found["headers"] and "TITLE - OWNER" not in found["headers"]',
    )
    table = [dict(zip(found["headers"], row, strict=True)) for row in records(text)]
    check(len(table) == 4 and found["rows"] == 4, 'expected: len(table) == 4 and found["rows"] == 4')
    check(
        all(PSEUDONYM.fullmatch(r["owner_person_pseudonym"]) and PSEUDONYM.fullmatch(r["owner_name_match_pseudonym"]) for r in table[:2]),
        'expected: all(PSEUDONYM.fullmatch(r["owner_person_pseudonym"]) and PSEUDONYM.fullmatch(r["owner_name',
    )
    check(
        table[0]["owner_person_pseudonym"] != table[1]["owner_person_pseudonym"],
        'expected: table[0]["owner_person_pseudonym"] != table[1]["owner_person_pseudonym"]',
    )
    check(
        table[0]["ASSOCIATE ID - OWNER"] == "" and table[0]["ADDRESS LINE 1 - OWNER"] == "" and (table[0]["ZIP CODE - OWNER"] == ""),
        'expected: table[0]["ASSOCIATE ID - OWNER"] == "" and table[0]["ADDRESS LINE 1 - OWNER"] == "" and ta',
    )
    organisation = table[2]
    check(
        organisation["ASSOCIATE ID - OWNER"] == "B-777" and organisation["ADDRESS LINE 1 - OWNER"] == "1 Main Street",
        'expected: organisation["ASSOCIATE ID - OWNER"] == "B-777" and organisation["ADDRESS LINE 1 - OWNER"]',
    )
    check(
        organisation["owner_person_pseudonym"] == "" and organisation["owner_name_match_pseudonym"] == "",
        'expected: organisation["owner_person_pseudonym"] == "" and organisation["owner_name_match_pseudonym"',
    )
    check(table[3]["ORGANIZATION NAME - OWNER"] == "[REDACTED]", 'expected: table[3]["ORGANIZATION NAME - OWNER"] == "[REDACTED]"')
    check(
        table[0]["ENROLLMENT ID"] == "E-1" and table[0]["PERCENTAGE OWNERSHIP"] == "25" and (table[0]["STATE - OWNER"] == "ZZ"),
        'expected: table[0]["ENROLLMENT ID"] == "E-1" and table[0]["PERCENTAGE OWNERSHIP"] == "25" and table[',
    )
    check(
        found["stats"]["person_rows"] == 4 and found["stats"]["person_cells_scrubbed"] == 1,
        'expected: found["stats"]["person_rows"] == 4 and found["stats"]["person_cells_scrubbed"] == 1',
    )


def test_version_1_owner_capture_loads_unchanged_without_a_key(tmp_path: Path) -> None:
    headers = cms_owners_contract.kept_columns("v1")
    organisations = [{k: v for k, v in row.items() if k in headers} for row in OWNER_ROWS if row["TYPE - OWNER"] == "O"]
    organisations[1]["ORGANIZATION NAME - OWNER"] = "[REDACTED]"
    found, text = load(tmp_path, OWNER_TABLE, headers, organisations, no_key)
    check(
        found["headers"] == headers and found["rows"] == 2 and ("person_rows" not in found["stats"]),
        'expected: found["headers"] == headers and found["rows"] == 2 and "person_rows" not in found["stats"]',
    )
    check(
        records(text) == [[row.get(h, "") for h in headers] for row in organisations],
        'expected: records(text) == [[row.get(h, "") for h in headers] for row in organisations]',
    )


def test_same_input_and_key_give_the_same_pseudonyms_and_a_new_key_gives_new_ones(tmp_path: Path) -> None:
    headers = cms_owners_contract.HEADERS["v1"]
    first = load(tmp_path, OWNER_TABLE, headers, OWNER_ROWS, key_for("synthetic one"))[1]
    again = load(tmp_path, OWNER_TABLE, headers, OWNER_ROWS, key_for("synthetic one"))[1]
    other = load(tmp_path, OWNER_TABLE, headers, OWNER_ROWS, key_for("synthetic two"))[1]
    check(first == again and first != other, "expected: first == again and first != other")
    check(
        {r[-2] for r in records(first)[:2]}.isdisjoint({r[-2] for r in records(other)[:2]}),
        "expected: {r[-2] for r in records(first)[:2]}.isdisjoint({r[-2] for r in records(other)[:2]})",
    )


@pytest.mark.parametrize("reader", [lambda _v: None, lambda _v: "  ", lambda _v: "not base64 !!"])
def test_a_missing_or_bad_key_stops_the_load_before_any_row_is_handed_on(tmp_path: Path, reader: Callable[[str], str | None]) -> None:
    source = write_csv(tmp_path / "source.csv", cms_owners_contract.HEADERS["v2"], OWNER_ROWS)
    rows_path = tmp_path / "rows.jsonl"
    item = {"table": OWNER_TABLE, "encodings": ["utf-8"], "format": "csv"}
    with pytest.raises(bronze.BronzeError, match="key_(missing|malformed)") as error:
        bronze.read_csv_object(item, source, rows_path, reader)
    check(not rows_path.exists(), "expected: not rows_path.exists()")
    check(
        not any(value.lower() in str(error.value).lower() for value in PEOPLE),
        "expected: not any(value.lower() in str(error.value).lower() for value in PEOPLE)",
    )


def test_a_person_column_without_a_reviewed_layout_stops_the_load(tmp_path: Path) -> None:
    headers = [*cms_owners_contract.HEADERS["v2"], "SOCIAL HANDLE - OWNER"]
    source = write_csv(tmp_path / "source.csv", headers, OWNER_ROWS)
    rows_path = tmp_path / "rows.jsonl"
    with pytest.raises(bronze.BronzeError, match="reviewed layout"):
        bronze.read_csv_object({"table": OWNER_TABLE, "encodings": ["utf-8"], "format": "csv"}, source, rows_path, key_for("synthetic one"))
    check(not rows_path.exists(), "expected: not rows_path.exists()")


def test_an_unapproved_owner_type_stops_the_load(tmp_path: Path) -> None:
    rows = [owner_row("X", **{"ASSOCIATE ID - OWNER": "0123456789", "FIRST NAME - OWNER": "Zelda"})]
    source = write_csv(tmp_path / "source.csv", cms_owners_contract.HEADERS["v2"], rows)
    rows_path = tmp_path / "rows.jsonl"
    with pytest.raises(bronze.BronzeError, match="discriminator_unknown"):
        bronze.read_csv_object({"table": OWNER_TABLE, "encodings": ["utf-8"], "format": "csv"}, source, rows_path, key_for("synthetic one"))
    check(not rows_path.exists(), "expected: not rows_path.exists()")


def onc_row(kind: str, npi: str, zip_code: str, specialty: str) -> dict[str, str]:
    return {
        "NPI": npi,
        "CCN": "999999",
        "Provider_Type": kind,
        "Business_State_Territory": "ZZ",
        "ZIP": zip_code,
        "Specialty": specialty,
        "Program_Year": "2014",
    }


def test_version_2_onc_file_reaches_bronze_with_clinician_identifiers_pseudonymized(tmp_path: Path) -> None:
    rows = [onc_row("EP", "1111111111", "99997", "Synthetic Cardiology"), onc_row("Hospital", "2222222222", "99996", "")]
    found, text = load(tmp_path, ONC_TABLE, onc_mu_contract_header(), rows, key_for("synthetic one"))
    check(
        "1111111111" not in text and "99997" not in text and ("Synthetic Cardiology" not in text) and ("Specialty" not in found["headers"]),
        'expected: "1111111111" not in text and "99997" not in text and "Synthetic Cardiology" not in text an',
    )
    table = [dict(zip(found["headers"], row, strict=True)) for row in records(text)]
    check(
        PSEUDONYM.fullmatch(table[0]["provider_pseudonym"]) and table[0]["NPI"] == "" and (table[0]["ZIP"] == ""),
        'expected: PSEUDONYM.fullmatch(table[0]["provider_pseudonym"]) and table[0]["NPI"] == "" and table[0]',
    )
    check(
        table[1]["NPI"] == "2222222222" and table[1]["ZIP"] == "99996" and (table[1]["provider_pseudonym"] == ""),
        'expected: table[1]["NPI"] == "2222222222" and table[1]["ZIP"] == "99996" and table[1]["provider_pseu',
    )
    check(
        table[0]["Business_State_Territory"] == "ZZ" and table[0]["Program_Year"] == "2014",
        'expected: table[0]["Business_State_Territory"] == "ZZ" and table[0]["Program_Year"] == "2014"',
    )


def onc_mu_contract_header() -> list[str]:
    return list(onc_mu_contract.read_json(onc_mu_contract.PLAN_PATH)["header"])


def test_version_1_onc_file_loads_unchanged_and_refuses_clinician_rows(tmp_path: Path) -> None:
    headers = [h for h in onc_mu_contract_header() if h not in onc_mu_contract.DROPPED]
    hospitals = [{k: v for k, v in onc_row("Hospital", "2222222222", "99996", "").items() if k in headers}]
    found, text = load(tmp_path, ONC_TABLE, headers, hospitals, no_key)
    check(
        found["headers"] == headers and records(text) == [[hospitals[0].get(h, "") for h in headers]],
        'expected: found["headers"] == headers and records(text) == [[hospitals[0].get(h, "") for h in header',
    )
    clinicians = [{k: v for k, v in onc_row("EP", "1111111111", "99997", "").items() if k in headers}]
    with pytest.raises(bronze.BronzeError, match="guard"):
        load(tmp_path, ONC_TABLE, headers, clinicians, no_key)


def test_policy_headers_match_the_acquisition_contracts() -> None:
    policies = {p["headers"] if isinstance(p["headers"], str) else tuple(p["headers"]) for p in person_fields.load_policies()[OWNER_TABLE]["policies"]}
    check(
        policies == {tuple(cms_owners_contract.HEADERS["v1"]), tuple(cms_owners_contract.HEADERS["v2"])},
        'expected: policies == {tuple(cms_owners_contract.HEADERS["v1"]), tuple(cms_owners_contract.HEADERS["',
    )
    layouts = {tuple(layout["headers"]) for layout in person_fields.load_policies()[OWNER_TABLE]["unchanged"]}
    check(
        layouts == {tuple(cms_owners_contract.kept_columns("v1")), tuple(cms_owners_contract.kept_columns("v2"))},
        'expected: layouts == {tuple(cms_owners_contract.kept_columns("v1")), tuple(cms_owners_contract.kept_',
    )
    onc = person_fields.load_policies()[ONC_TABLE]
    check(
        [tuple(p["headers"]) for p in onc["policies"]] == [tuple(onc_mu_contract_header())],
        'expected: [tuple(p["headers"]) for p in onc["policies"]] == [tuple(onc_mu_contract_header())]',
    )


ORGANISATION_NAME_CASES = [
    ("Fixturegiven Fixturesurname Clinic", True),
    ("Fixturegiven Fixturemiddle Fixturesurname Clinic", True),
    ("FIXTUREGIVEN FIXTURESURNAME'S CLINIC", True),
    ("Clinic of fixturegiven   fixturesurname", True),
    ("Fixturegiven Clinic", False),
    ("Fixturesurname Associates", False),
    ("Unrelated Clinic", False),
]


@pytest.mark.parametrize("layout", ["v1", "v2"])
def test_an_organisation_name_holding_a_collected_person_name_is_scrubbed(tmp_path: Path, layout: str) -> None:
    owner = owner_row(
        "I",
        **{
            "ASSOCIATE ID - OWNER": "0123456789",
            "FIRST NAME - OWNER": "Fixturegiven",
            "MIDDLE NAME - OWNER": "Fixturemiddle",
            "LAST NAME - OWNER": "Fixturesurname",
        },
    )
    rows = [owner] + [owner_row("O", **{"ORGANIZATION NAME": name, "ASSOCIATE ID - OWNER": f"B-{n}"}) for n, (name, _) in enumerate(ORGANISATION_NAME_CASES)]
    found, text = load(tmp_path, OWNER_TABLE, cms_owners_contract.HEADERS[layout], rows, key_for("synthetic one"))
    table = [dict(zip(found["headers"], row, strict=True)) for row in records(text)]
    check(len(table) == len(rows), "a row was lost")
    for row, (name, scrubbed) in zip(table[1:], ORGANISATION_NAME_CASES, strict=True):
        check(row["ORGANIZATION NAME"] == ("[REDACTED]" if scrubbed else name), f"organisation name handled wrongly: {name}")
    check(found["stats"]["person_cells_scrubbed"] == sum(scrubbed for _, scrubbed in ORGANISATION_NAME_CASES), "the scrub count is wrong")
    check(not re.search("fixturegiven fixture(middle )?surname", text, re.IGNORECASE), "a collected full name survived in the rows")
