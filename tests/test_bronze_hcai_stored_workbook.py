"""The HCAI utilization workbook the collector E2E stores whole goes through the bronze person policy: synthetic workbook and synthetic keys only.

``scripts.acquisition.run_hcai_util_e2e`` builds the synthetic workbook that its capture cases store as published. Here the same bytes are read with the
collector's own workbook reader and handed to ``person_fields.protect_sheets`` under a copy of the real policy whose reviewed layout is the synthetic
header, so the person rules (columns, owner type, namespace, key version) are the reviewed ones. The acquisition package may not import the lakehouse
package, so these cases live here and not in the collector runner.
"""

from __future__ import annotations

import base64
import hashlib
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from scripts import pseudonym_core
from scripts.acquisition import hcai_util_contract as contract
from scripts.acquisition import run_hcai_util_e2e as runner
from scripts.lakehouse import person_fields
from tests.support import check

TABLE = "hcai_util_workbook_sheet_rows"
NAMESPACE = "hcai_person_name"
Keys = Callable[[str], str | None]


def synthetic_key(label: str) -> Keys:
    """A key reader over 32 bytes taken from a label, so no real key is ever used."""
    text = base64.urlsafe_b64encode(hashlib.sha256(label.encode()).digest()).decode()
    return lambda _version: text


KEY_ONE = synthetic_key("hcai stored workbook key one")
KEY_TWO = synthetic_key("hcai stored workbook key two")


def expected(name: str, keys: Keys) -> str:
    """The pseudonym of one name from the shared core, computed apart from the loader."""
    return pseudonym_core.derive(pseudonym_core.load_key("v1", keys), "v1", NAMESPACE, "name", [pseudonym_core.normalize_name(name)])


@pytest.fixture(scope="module")
def stored() -> bytes:
    """The workbook bytes as the collector stores them."""
    return runner.workbook(runner.sheet_rows())


@pytest.fixture
def policy_path(tmp_path: Path) -> Path:
    policy = json.loads(person_fields.POLICY_PATH.read_text())
    layout = {"layout_id": "synthetic_e2e", "years": [2025], "sheets": list(runner.sheet_rows()), "header": runner.HEADER}
    policy["tables"][TABLE]["sheet_cells"]["layouts"] = [layout]
    path = tmp_path / "person_policy.json"
    path.write_text(json.dumps(policy))
    return path


def protect(raw: bytes, folder: Path, policy: Path, keys: Keys, name: str = "rows") -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for index, (sheet, rows) in enumerate(contract.read_workbook(raw).items(), start=1):
        for number, cells in enumerate(rows, start=1):
            records.append({"r": len(records) + 1, "s": sheet, "i": index, "w": number, "d": "1900", "c": cells, "t": ["s" if c else None for c in cells]})
    rows_path = folder / f"{name}.jsonl"
    rows_path.write_text("".join(json.dumps(record) + "\n" for record in records))
    person_fields.protect_sheets(TABLE, rows_path, keys, policy)
    return [json.loads(line) for line in rows_path.read_text().splitlines()]


def cells_of(records: list[dict[str, Any]], number: int, sheet: str = "Page 1-6") -> dict[str, str]:
    return next(dict(zip(runner.HEADER, r["c"], strict=False)) for r in records if r["s"] == sheet and r["w"] == number)


def test_the_stored_workbook_person_cells_are_pseudonymized_and_stable_per_key(stored: bytes, policy_path: Path, tmp_path: Path) -> None:
    first = protect(stored, tmp_path, policy_path, KEY_ONE, "first")
    again = protect(stored, tmp_path, policy_path, KEY_ONE, "again")
    same_key = protect(stored, tmp_path, policy_path, synthetic_key("hcai stored workbook key one"), "same_key")
    other = protect(stored, tmp_path, policy_path, KEY_TWO, "other")
    check(first == again == same_key, "the same workbook and key gave different rows")
    hospital, owner = cells_of(first, 6), cells_of(first, 7)
    check(hospital["FAC_ADMIN_NAME"] == expected("Pat Q Example", KEY_ONE), "the administrator is not its keyed pseudonym")
    check(hospital["REPT_PREP_NAME"] == expected("Lee Sample", KEY_ONE), "the report preparer is not its keyed pseudonym")
    check(owner["FAC_PAR_CORP_NAME"] == expected("Jordan Owner", KEY_ONE), "the individual owner is not its keyed pseudonym")
    check(hospital["FAC_PAR_CORP_NAME"] == "SYNTHETIC HEALTH SYSTEM", "an organisation owner name changed")
    check(cells_of(other, 6)["FAC_ADMIN_NAME"] == expected("Pat Q Example", KEY_TWO) != hospital["FAC_ADMIN_NAME"], "another key gave the same pseudonym")
    check(
        all(len(c) == 67 and c.startswith("v1:") for c in (hospital["FAC_ADMIN_NAME"], hospital["REPT_PREP_NAME"], owner["FAC_PAR_CORP_NAME"])),
        "no pseudonym form",
    )


def test_no_raw_person_value_survives_from_the_stored_workbook(stored: bytes, policy_path: Path, tmp_path: Path) -> None:
    text = json.dumps(protect(stored, tmp_path, policy_path, KEY_ONE)).lower()
    values = [*runner.PEOPLE, "pat q. example", "pat example", runner.PHONE_ONE, "1 example way", "2 example way", "shelbyville", "jordan", "sample", "example"]
    check(stored.lower().count(b"jordan owner") > 0, "the synthetic workbook did not hold the owner name to begin with")
    check(not [value for value in values if value.lower() in text], "a raw person value form survived")


def test_a_key_that_cannot_be_read_changes_nothing_for_the_stored_workbook(stored: bytes, policy_path: Path, tmp_path: Path) -> None:
    with pytest.raises(pseudonym_core.PseudonymizationError, match="key_missing"):
        protect(stored, tmp_path, policy_path, lambda _version: None)
    check(sorted(p.name for p in tmp_path.glob("*.protected")) == [], "a failed key read left a staging file")
