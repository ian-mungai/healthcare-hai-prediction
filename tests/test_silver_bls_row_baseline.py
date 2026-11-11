"""The silver BLS row-count band follows the owner's Run 3 scope (2006 to 2025) and the rule recorded in baselines.json."""

from __future__ import annotations

import json
import math
from pathlib import Path

from tests.support import check

BASELINES = Path(__file__).resolve().parents[1] / "data_contracts/great_expectations/baselines.json"
# Rows of int_bls_county_series for data years 2006 to 2025 in the staged silver build (3,222 counties, 167,180 to 167,492 rows per year).
STAGED_ROWS_2006_TO_2025 = 3_348_072


def test_the_bls_row_band_is_95_to_105_percent_of_the_staged_2006_to_2025_rows() -> None:
    values = json.loads(BASELINES.read_text())["values"]
    check(values["silver.rows.int_bls_county_series.min"] == math.floor(0.95 * STAGED_ROWS_2006_TO_2025), "the minimum is 95% of the staged rows")
    check(values["silver.rows.int_bls_county_series.max"] == math.ceil(1.05 * STAGED_ROWS_2006_TO_2025), "the maximum is 105% of the staged rows")
