"""Every schema and every Python parser that checks a run ID accepts the valid forms and refuses the invalid ones.

The corpus is written by hand here, not derived from ``orchestration/training_contract.py``, so a copy that drifts from the contract (a schema that keeps retry
days 1 and 2, or has no ``reload`` run) fails even when it still equals an older copy of itself.
"""

from __future__ import annotations

import json
import re
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from jsonschema import Draft202012Validator

from orchestration import training_contract as contract
from scripts.orchestration import sync_run_id_schemas as sync
from tests.support import check

ROOT = Path(__file__).resolve().parents[1]
HEX = "deadbeef"
# The run ID of a code run keeps a commit time form; every other parser and schema refuses it.
DATE_FORM = "20261116T201053Z-b10a4f2d"
VALID = [
    f"ingestion_{HEX}",
    f"ingestion_{HEX}_2",
    f"ingestion_{HEX}_99",
    f"ingestion_retry_{HEX}_2",
    f"ingestion_retry_{HEX}_3",
    f"ingestion_retry_{HEX}_2_2",
    f"ingestion_retry_{HEX}_2_3",
    f"ingestion_retry_{HEX}_99_3",
    f"bronze_{HEX}",
    f"bronze_{HEX}_2",
    f"silver_{HEX}",
    f"gold_{HEX}_3",
    f"features_{HEX}",
    f"metadata_{HEX}",
    f"research_{HEX}",
    f"serving_{HEX}",
    f"promotion_{HEX}",
    f"features_elastic_net_{HEX}",
    f"drift_ols_{HEX}_2",
    f"training_spatial_{HEX}",
    f"research_mixed_effects_{HEX}",
    f"reload_elastic_net_{HEX}",
    f"reload_ols_{HEX}_2",
    f"canary_deploy_clabsi_{HEX}",
    f"stable_deploy_ssi_colon_{HEX}",
    f"promote_cdiff_{HEX}",
    f"release_mrsa_{HEX}_2",
    f"canary_cauti_{HEX}",
    f"notice_ssi_hysterectomy_{HEX}",
]
INVALID = [
    f"ingestion_retry_{HEX}_1",
    f"ingestion_retry_{HEX}_4",
    f"ingestion_retry_{HEX}_0",
    f"ingestion_retry_{HEX}",
    f"ingestion_retry_{HEX}_1_2",
    f"ingestion_retry_{HEX}_2_1",
    f"ingestion_retry_{HEX}_2_4",
    f"ingestion_retry_{HEX}_100_2",
    f"ingestion_{HEX}_1",
    f"ingestion_{HEX}_0",
    f"ingestion_{HEX}_100",
    "ingestion_DEADBEEF",
    "ingestion_deadbee",
    f"ingestion_{HEX}0",
    f"ingestion_{HEX}_",
    "",
    "latest",
    f"reload_{HEX}",
    f"reload_unknown_{HEX}",
    "reload_ols",
    f"drift_{HEX}",
    f"training_{HEX}",
    "features_elastic_net",
    f"canary_ols_{HEX}",
    f"promote_clabsi_{HEX}_1",
    DATE_FORM,
    f"silver_{HEX}/../x",
    f"bronze_{HEX}_retry_2",
]
# A final newline passes ``$`` of a Python search, so these go only through full-match parsers.
NEWLINE_INVALID = [f"silver_{HEX}\n", f"ingestion_retry_{HEX}_2\n"]
DRIFT_VALID = [f"drift_ols_{HEX}", f"drift_gradient_boosting_{HEX}_2"]
DRIFT_INVALID = [f"drift_ols_{HEX}_1", f"features_ols_{HEX}", f"reload_ols_{HEX}", f"drift_{HEX}", f"drift_ols_{HEX}_100", f"drift_unknown_{HEX}"]


def is_original(run: str) -> bool:
    return run.startswith("ingestion_") and not run.startswith("ingestion_retry_")


def is_retry(run: str) -> bool:
    return run.startswith("ingestion_retry_")


SCOPES: dict[str, Callable[[str], bool]] = {
    "every run ID": lambda run: True,
    "an original ingestion run": is_original,
    "an ingestion run or a retry": lambda run: is_original(run) or is_retry(run),
    "a retry run": is_retry,
    "a bronze run": lambda run: run.startswith("bronze_"),
}
# Wrappers of the pattern copies in the schemas, with the text a run ID makes inside each (the ``<RUN>`` stands for the copy).
SAMPLES: dict[str, Callable[[str], str]] = {
    "^<RUN>$": lambda run: run,
    "^<RUN>$(?![\\s\\S])": lambda run: run,
    "^(latest|<RUN>)$": lambda run: run,
    "^(manual|drift:<RUN>)$": lambda run: f"drift:{run}",
    "^<RUN>:[a-z][a-z0-9_]{1,40}:(fixture|publish_fixture|real):[0-9]{1,2}(:<RUN>)?$": lambda run: f"{run}:step:real:1",
    "^hai-<RUN>-[a-z][a-z0-9_]{1,40}-[0-9]{1,2}$": lambda run: f"hai-{run}-step-1",
}


def accepts(pattern: str, text: str) -> bool:
    """Whether the schema validator accepts ``text`` for a string that has ``pattern``."""
    return not list(Draft202012Validator({"type": "string", "pattern": pattern}).iter_errors(text))


def test_every_schema_copy_of_a_run_id_pattern_is_the_contracts() -> None:
    wrong = sync.differing()
    check(not wrong, f"{len(wrong)} copies differ from the contract; run .venv/bin/python -m scripts.orchestration.sync_run_id_schemas --write: {wrong[:2]}")
    check(len(sync.copies()) >= 35, f"only {len(sync.copies())} copies of a run ID pattern were found")


def wrapped(held: str) -> str:
    """The pattern of a copy with each run ID part replaced by ``<RUN>``."""
    return sync.substituted(held, {"run": "<RUN>", "drift": "<RUN>"})


def test_every_schema_copy_has_a_wrapper_this_test_knows() -> None:
    unknown = [f"{path.name}{where}: {wrapped(held)[:100]}" for path, where, _, held, _ in sync.copies() if wrapped(held) not in SAMPLES]
    check(not unknown, f"a copy has a wrapper that the corpus test cannot fill in; add it to SAMPLES: {unknown}")


def schema_cases() -> list[Any]:
    return [pytest.param(held, wrapped(held), kind, id=f"{path.name}{where}") for path, where, kind, held, _ in sync.copies()]


@pytest.mark.parametrize(("pattern", "wrapper", "kind"), schema_cases())
def test_every_schema_copy_accepts_the_valid_run_ids_and_refuses_the_invalid_ones(pattern: str, wrapper: str, kind: str) -> None:
    sample = SAMPLES[wrapper]
    valid, invalid = (DRIFT_VALID, DRIFT_INVALID) if kind.startswith("drift") else (VALID, INVALID)
    refused_valid = [run for run in valid if not accepts(pattern, sample(run))]
    accepted_invalid = [run for run in invalid if accepts(pattern, sample(run)) and not (run == "latest" and wrapper == "^(latest|<RUN>)$")]
    check(not refused_valid, f"the schema refuses valid run IDs: {refused_valid}")
    check(not accepted_invalid, f"the schema accepts invalid run IDs: {accepted_invalid}")
    if wrapper == "^(latest|<RUN>)$":
        check(accepts(pattern, "latest"), "the schema refuses latest")
    if wrapper == "^(manual|drift:<RUN>)$":
        check(accepts(pattern, "manual"), "the schema refuses manual")


def test_the_capture_root_of_a_test_run_takes_only_an_ingestion_run() -> None:
    schema = json.loads((ROOT / "data_contracts/orchestration/submission_request.schema.json").read_text())
    pattern = schema["properties"]["publisher"]["properties"]["capture_root"]["pattern"]
    for run in VALID + INVALID:
        wanted = is_original(run) and run in VALID
        check(accepts(pattern, f"ingestion_test/{run}/") == wanted, f"{run}: the capture root of a test run is {'refused' if wanted else 'accepted'}")
    check(accepts(pattern, "raw/"), "the schema refuses the production capture root")


def full_match(pattern: str) -> Callable[[str], bool]:
    return lambda text: re.fullmatch(pattern, text) is not None


def parsers() -> list[tuple[str, Callable[[str], bool], str]]:
    """Each Python parser of a run ID, with the scope of the names it takes."""
    from scripts.acquisition import plan_rebuild, publisher_run, raw_root
    from scripts.lakehouse import bronze_run, gold_publish_record

    def parts(run: str) -> bool:
        try:
            contract.run_id_parts(run)
        except ValueError:
            return False
        return True

    return [
        ("contract.RUN_ID", full_match(contract.RUN_ID), "every run ID"),
        ("contract.run_id_parts", parts, "every run ID"),
        ("contract.CODE_RUN", full_match(contract.CODE_RUN), "every run ID"),
        ("gold_publish_record.RUN_ID_PATTERN", full_match(gold_publish_record.RUN_ID_PATTERN), "every run ID"),
        ("run_id_pattern('ingestion')", full_match(contract.run_id_pattern("ingestion")), "an original ingestion run"),
        ("run_id_pattern('ingestion_retry')", full_match(contract.run_id_pattern("ingestion_retry")), "a retry run"),
        ("contract.RETRY_RUN_ID", full_match(contract.RETRY_RUN_ID), "a retry run"),
        ("bronze_run.RUN_ID", bronze_run.RUN_ID.fullmatch, "a bronze run"),
        ("plan_rebuild.RUN_ID", plan_rebuild.RUN_ID.fullmatch, "an ingestion run or a retry"),
        ("publisher_run.RUN_ID_FORM", publisher_run.RUN_ID_FORM.fullmatch, "an original ingestion run"),
        ("raw_root.INGESTION_RUN", full_match(raw_root.INGESTION_RUN), "an original ingestion run"),
    ]


def test_every_python_parser_accepts_the_valid_run_ids_of_its_names_and_refuses_the_rest() -> None:
    problems = []
    for label, accepts_run, scope in parsers():
        applies = SCOPES[scope]
        for run in [*VALID, *INVALID, *NEWLINE_INVALID]:
            wanted = (run in VALID and applies(run)) or (label == "contract.CODE_RUN" and run == DATE_FORM)
            if bool(accepts_run(run)) != wanted:
                problems.append(f"{label} {'refuses' if wanted else 'accepts'} {run!r}")
    check(not problems, f"{len(problems)} parser answers are wrong: {problems[:8]}")


def test_every_module_that_imports_the_contract_pattern_holds_the_contracts_own_string() -> None:
    from orchestration import ecs_launch, ecs_lineage, ecs_preflight
    from scripts.features import publication_reads, training_extract
    from scripts.infrastructure import openmetadata_chain, openmetadata_ingest, openmetadata_raw_files
    from scripts.lakehouse import layer_acceptance

    for module in (
        ecs_launch,
        ecs_lineage,
        ecs_preflight,
        openmetadata_chain,
        openmetadata_ingest,
        openmetadata_raw_files,
        publication_reads,
        training_extract,
        layer_acceptance,
    ):
        check(module.RUN_ID_PATTERN is contract.RUN_ID, f"{module.__name__} does not use the contract's run ID pattern")
