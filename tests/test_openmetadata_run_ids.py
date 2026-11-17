"""The OpenMetadata ingest and chain entry points take the run ID form and refuse the old date form."""

from __future__ import annotations

from pathlib import Path

import pytest

from scripts.infrastructure import openmetadata_chain, openmetadata_ingest
from tests.support import check

NEW_RUNS = ["silver_deadbeef", "gold_deadbeef_2", "features_deadbeef"]
OLD_RUN = "20261116T201053Z-b10a4f2d"


def ingest(tmp_path: Path, item: str) -> int:
    """The exit code of ``openmetadata_ingest dbt`` for one ``--dbt-run``. A run that passes the check reaches the config read, which finds no file."""
    arguments = ["dbt", "--config-dir", str(tmp_path / "missing"), "--work-dir", str(tmp_path), "--token-env", "TOKEN", "--dbt-run", item]
    try:
        return openmetadata_ingest.main(arguments, env={"TOKEN": "x"})
    except FileNotFoundError:
        return -1


@pytest.mark.parametrize("run", NEW_RUNS)
def test_a_dbt_run_of_the_new_form_passes_the_ingest_check(tmp_path: Path, run: str) -> None:
    layer = run.split("_")[0]
    check(ingest(tmp_path, f"{layer}={run}") == -1, f"{layer}={run} was refused by the argument check")


@pytest.mark.parametrize("item", [f"silver={OLD_RUN}", "silver=latest", "silver=silver_DEADBEEF", "silver=silver_deadbee", "other=silver_deadbeef"])
def test_a_dbt_run_of_the_old_form_or_a_wrong_name_is_refused_with_exit_two(tmp_path: Path, item: str) -> None:
    check(ingest(tmp_path, item) == 2, f"{item} was not refused")


@pytest.mark.parametrize("run", NEW_RUNS)
def test_the_chain_reads_the_run_ids_of_the_new_form(run: str) -> None:
    check(openmetadata_chain.RUN_ID.fullmatch(run) is not None, f"the chain refuses {run}")
    check(openmetadata_chain.RUN_ID.fullmatch(OLD_RUN) is None, "the chain accepts the old form")
