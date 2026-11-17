"""The run ID form ``<name>_<8 hex>[_<attempt>]``: one shared hex per pipeline run, the attempt suffix and the clean break from the old form."""

from __future__ import annotations

import hashlib
import re
from datetime import UTC, datetime
from typing import Any

import pytest

from orchestration import training_contract as contract
from tests.support import check

HEX = "3f9a1c07"
OLD_RUN = "20261116T201053Z-b10a4f2d"


def test_the_names_tables_hold_the_approved_short_names() -> None:
    check(
        contract.MODEL_NAMES == {"m1": "ols", "m2": "elastic_net", "m3": "mixed_effects", "m4": "gradient_boosting", "m5": "spatial", "m6": "fixed_effects"},
        "model names",
    )
    check(
        contract.INFECTION_NAMES == {"HAI_1": "clabsi", "HAI_2": "cauti", "HAI_3": "ssi_colon", "HAI_4": "ssi_hysterectomy", "HAI_5": "mrsa", "HAI_6": "cdiff"},
        "infection names",
    )


@pytest.mark.parametrize(
    "run",
    [
        f"ingestion_{HEX}",
        f"ingestion_{HEX}_2",
        f"ingestion_retry_{HEX}_1",
        f"ingestion_retry_{HEX}_2",
        f"ingestion_retry_{HEX}_3_2",
        f"bronze_{HEX}",
        f"silver_{HEX}_2",
        f"gold_{HEX}",
        f"features_{HEX}",
        f"metadata_{HEX}",
        f"features_elastic_net_{HEX}",
        f"drift_gradient_boosting_{HEX}_3",
        f"training_ols_{HEX}",
        f"research_{HEX}",
        f"research_{HEX}_2",
        f"research_spatial_{HEX}",
        f"promote_clabsi_{HEX}",
        f"canary_deploy_ssi_colon_{HEX}",
        f"stable_deploy_cdiff_{HEX}_2",
        f"serving_{HEX}",
        f"promotion_{HEX}",
    ],
)
def test_every_run_id_of_the_new_form_is_valid(run: str) -> None:
    check(re.fullmatch(contract.RUN_ID, run) is not None, f"{run} is refused")


@pytest.mark.parametrize(
    "run",
    [
        OLD_RUN,
        "latest",
        "",
        f"silver_{HEX.upper()}",
        f"silver_{HEX[:7]}",
        f"silver_{HEX}_1",
        f"silver_{HEX}_100",
        f"silver_{HEX}_0",
        f"silver_{HEX}_a",
        f"{HEX}",
        f"unknown_{HEX}",
        f"features_m2_{HEX}",
        f"promote_HAI_1_{HEX}",
        f"drift_{HEX}",
        f"ingestion_retry_{HEX}",
        f"ingestion_retry_{HEX}_4",
        f"ingestion_retry_{HEX}_0",
        f"promote_{HEX}",
        f" silver_{HEX}",
        f"silver_{HEX}\n",
    ],
)
def test_anything_else_is_refused_and_the_old_form_is_not_valid(run: str) -> None:
    check(re.fullmatch(contract.RUN_ID, run) is None, f"{run!r} is accepted")


def test_the_parts_of_a_run_id_are_read_back() -> None:
    parts = contract.run_id_parts(f"drift_elastic_net_{HEX}_3")
    check((parts.name, parts.qualifier, parts.hex, parts.attempt) == ("drift", "elastic_net", HEX, 3), f"{parts}")
    parts = contract.run_id_parts(f"ingestion_retry_{HEX}_2")
    check((parts.name, parts.hex, parts.attempt, parts.day) == ("ingestion_retry", HEX, 1, 2), f"{parts}")
    parts = contract.run_id_parts(f"ingestion_retry_{HEX}_3_2")
    check((parts.hex, parts.attempt, parts.day) == (HEX, 3, 2), f"{parts}")
    parts = contract.run_id_parts(f"promote_ssi_hysterectomy_{HEX}")
    check((parts.name, parts.qualifier, parts.attempt) == ("promote", "ssi_hysterectomy", 1), f"{parts}")
    with pytest.raises(ValueError, match="not a run ID"):
        contract.run_id_parts(OLD_RUN)


def test_the_hex_of_a_hand_start_is_random_and_of_the_right_form() -> None:
    seen = {contract.new_run_hex() for _ in range(20)}
    check(len(seen) == 20 and all(re.fullmatch("[a-f0-9]{8}", item) for item in seen), f"{seen}")


def test_the_hex_of_a_scheduled_run_is_the_first_eight_digits_of_the_digest_of_its_logical_date() -> None:
    date = datetime(2026, 12, 1, tzinfo=UTC)
    expected = hashlib.sha256(date.isoformat().encode()).hexdigest()[:8]
    check(contract.scheduled_run_hex(date) == expected, "the hex is not the digest of the logical date")
    check(contract.scheduled_run_hex(date) == contract.scheduled_run_hex(date.astimezone(UTC)), "the hex changed")
    with pytest.raises(ValueError, match="time zone"):
        contract.scheduled_run_hex(datetime(2026, 12, 1))


def test_a_run_of_a_name_takes_the_hex_and_the_attempt_given() -> None:
    check(contract.compose_run_id("ingestion", HEX) == f"ingestion_{HEX}", "plain")
    check(contract.compose_run_id("bronze", HEX, 2) == f"bronze_{HEX}_2", "attempt 2")
    check(contract.compose_run_id("training", HEX, 1, "mixed_effects") == f"training_mixed_effects_{HEX}", "model")
    check(contract.compose_run_id("canary", HEX, 3, "mrsa") == f"canary_mrsa_{HEX}_3", "chain step")
    for bad in (
        ("features", HEX, 1, "m2"),
        ("features", "XYZ", 1, None),
        ("promote", HEX, 1, None),
        ("serving", HEX, 1, "ols"),
        ("bronze", HEX, 0, None),
        ("bronze", HEX, 100, None),
    ):
        with pytest.raises(ValueError):
            contract.compose_run_id(*bad)


def test_the_runs_of_one_pipeline_run_share_one_hex() -> None:
    bronze = f"bronze_{HEX}"
    check(contract.chain_layer_run("silver", bronze) == f"silver_{HEX}", "silver")
    check(contract.chain_layer_run("gold", bronze) == f"gold_{HEX}", "gold")
    check(contract.chain_layer_run("features", bronze) == f"features_{HEX}", "features")
    check(contract.chain_layer_run("metadata", bronze) == f"metadata_{HEX}", "metadata")
    base = f"features_{HEX}"
    check(contract.model_features_run_id(base, "m2") == f"features_elastic_net_{HEX}", "model features")
    drift = contract.model_drift_run_id(contract.model_features_run_id(base, "m2"))
    check(drift == f"drift_elastic_net_{HEX}", drift)
    check(contract.model_training_run_id(drift, "m2") == f"training_elastic_net_{HEX}", "training")
    check(contract.trained_run_id(base, "m6") == f"training_fixed_effects_{HEX}", "trained")
    check(contract.research_run_id(base) == f"research_{HEX}", "research")
    check(contract.research_dag_run_id(f"training_ols_{HEX}") == f"research_ols_{HEX}", "research DAG run")
    research = f"research_{HEX}"
    check(contract.chain_run_id(research, "HAI_1", "promote") == f"promote_clabsi_{HEX}", "chain step")
    check(contract.chain_run_id(research, "HAI_3", "canary_deploy") == f"canary_deploy_ssi_colon_{HEX}", "chain step 2")


def test_the_attempt_suffix_carries_down_but_research_takes_only_its_own() -> None:
    bronze = f"bronze_{HEX}_2"
    check(contract.chain_layer_run("silver", bronze) == f"silver_{HEX}_2", "layer")
    base = contract.chain_layer_run("features", bronze)
    features = contract.model_features_run_id(base, "m4")
    check(features == f"features_gradient_boosting_{HEX}_2", features)
    drift = contract.model_drift_run_id(features)
    check(drift == f"drift_gradient_boosting_{HEX}_2", drift)
    check(contract.model_training_run_id(drift, "m4") == f"training_gradient_boosting_{HEX}_2", "training")
    check(contract.trained_run_id(base, "m4") == f"training_gradient_boosting_{HEX}_2", "trained")
    check(contract.research_run_id(base) == f"research_{HEX}", "research ignores the base run's attempt")
    check(contract.research_run_id(base, 2) == f"research_{HEX}_2", "research has its own attempt")
    check(contract.research_dag_run_id(f"training_ols_{HEX}_2") == f"research_ols_{HEX}", "research DAG run has no attempt of the training run")
    check(contract.chain_run_id(f"research_{HEX}_2", "HAI_6", "notice") == f"notice_cdiff_{HEX}_2", "the research attempt carries to the chain")
    check(contract.chain_run_id(f"research_{HEX}", "HAI_6", "notice") == f"notice_cdiff_{HEX}", "attempt 1 has no suffix")


def test_a_drift_attempt_names_its_own_suffix_and_its_training_run_follows_it() -> None:
    features = f"features_elastic_net_{HEX}"
    check(contract.model_drift_run_id(features, 1) == f"drift_elastic_net_{HEX}", "attempt 1")
    second = contract.model_drift_run_id(features, 2)
    check(second == f"drift_elastic_net_{HEX}_2", second)
    check(contract.model_training_run_id(second, "m2") == f"training_elastic_net_{HEX}_2", "the suffix carries down")
    wrong: tuple[Any, ...] = (0, 100, True, "2", 2.0)
    for bad in wrong:
        with pytest.raises(ValueError, match="attempt"):
            contract.model_drift_run_id(features, bad)


def test_a_derivation_refuses_a_parent_of_the_wrong_name_or_the_old_form() -> None:
    with pytest.raises(ValueError):
        contract.model_features_run_id(OLD_RUN, "m1")
    with pytest.raises(ValueError):
        contract.model_features_run_id(f"silver_{HEX}", "m1")
    with pytest.raises(ValueError):
        contract.model_drift_run_id(f"features_{HEX}")
    with pytest.raises(ValueError):
        contract.model_training_run_id(f"drift_ols_{HEX}", "m2")
    with pytest.raises(ValueError):
        contract.chain_run_id(f"bronze_{HEX}", "HAI_1", "promote")
    with pytest.raises(ValueError):
        contract.chain_run_id(f"research_{HEX}", "HAI_9", "promote")
    with pytest.raises(ValueError):
        contract.chain_layer_run("features", f"silver_{HEX}")


def test_the_record_key_of_a_research_run_takes_only_the_new_form() -> None:
    check(contract.record_key("research", f"research_{HEX}", "HAI_2") == f"research/research_{HEX}/research_HAI_2.json", "key")
    with pytest.raises(ValueError):
        contract.record_key("research", OLD_RUN, "HAI_2")


def test_the_whole_chain_of_a_pipeline_run_is_valid_at_every_step() -> None:
    ids = [f"ingestion_{HEX}", f"ingestion_retry_{HEX}_1", f"bronze_{HEX}"]
    for layer in ("silver", "gold", "features", "metadata"):
        ids.append(contract.chain_layer_run(layer, f"bronze_{HEX}"))
    base = f"features_{HEX}"
    for model in contract.MODELS:
        features = contract.model_features_run_id(base, model)
        drift = contract.model_drift_run_id(features)
        ids += [features, drift, contract.model_training_run_id(drift, model), contract.research_dag_run_id(contract.trained_run_id(base, model))]
    ids.append(contract.research_run_id(base))
    for hai_type in contract.HAI_TYPES:
        for step in contract.CHAIN_STEPS:
            ids.append(contract.chain_run_id(f"research_{HEX}", hai_type, step))
    bad = [item for item in ids if not re.fullmatch(contract.RUN_ID, item)]
    check(not bad and len(set(ids)) == len(ids), f"refused or repeated: {bad}")


def test_a_model_run_pattern_accepts_only_its_own_model() -> None:
    pattern = contract.model_run_pattern("features", "m2")
    check(re.fullmatch(pattern, f"features_elastic_net_{HEX}_2") is not None, "its own model is refused")
    check(
        re.fullmatch(pattern, f"features_ols_{HEX}") is None and re.fullmatch(pattern, f"features_{HEX}") is None, "another model or the base run is accepted"
    )
    with pytest.raises(ValueError):
        contract.model_run_pattern("silver", "m2")
