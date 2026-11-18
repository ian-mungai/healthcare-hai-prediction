"""Every place that checks a run ID checks the one form of ``orchestration/training_contract.py``.

The copies that cannot import it agree with it, the schemas carry it and the old date form is left only where a code run keeps it.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from orchestration import training_contract as contract
from scripts.acquisition import plan_rebuild, publisher_run, raw_root
from scripts.lakehouse import catalog_restore, gold_publish_record
from scripts.orchestration import sync_run_id_schemas as sync
from tests.support import check

ROOT = Path(__file__).resolve().parents[1]
OLD_FORM = r"[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}"
OLD_RUN = "20261116T201053Z-b10a4f2d"
HEX = "3f9a1c07"
FOLDERS = ("dags", "orchestration", "scripts", "infra", "services", "monitoring", "feature_store", "features", "data_contracts", "config", ".github", "tests")
# The files that still name the old form, with the reason: a code run (the commit time and eight digits of the commit that CI stages) keeps it.
OLD_FORM_ALLOWED = {
    "orchestration/training_contract.py": "the code run form",
    "infra/services/scripts/code_identify.py": "the code run shape of the host identity one-shot",
    "services/training/fetch_code.py": "the code run shape of the image's code fetch",
    "infra/jobs/layers.tf": "the code run shape of the layer code fetch",
    "scripts/infrastructure/code_installer_before/fetch_code.py.txt": "the fetch script before the code installer, kept as a fixture",
    "data_contracts/training/extract_manifest.schema.json": "the stored runs of a publication (the stored run pattern of the contract)",
    "tests/test_run_id_patterns.py": "this test",
}
VALID = [
    f"ingestion_{HEX}",
    f"ingestion_retry_{HEX}_2",
    f"bronze_{HEX}_2",
    f"features_elastic_net_{HEX}",
    f"drift_ols_{HEX}",
    f"training_spatial_{HEX}_3",
    f"research_{HEX}",
    f"research_ols_{HEX}",
    f"promote_clabsi_{HEX}",
    f"serving_{HEX}",
    f"promotion_{HEX}",
]


def folder_files() -> list[Path]:
    found = []
    for folder in FOLDERS:
        for path in sorted((ROOT / folder).rglob("*")):
            relative = path.relative_to(ROOT).as_posix()
            if (
                path.is_file()
                and ".terraform" not in relative
                and "__pycache__" not in relative
                and path.suffix in (".py", ".json", ".tf", ".hcl", ".sh", ".txt", ".yml")
            ):
                found.append(path)
    return found


def test_the_old_form_is_left_only_where_a_code_run_keeps_it() -> None:
    found = {}
    for path in folder_files():
        text = path.read_text(encoding="utf-8", errors="replace")
        if OLD_FORM in text:
            found[path.relative_to(ROOT).as_posix()] = text.count(OLD_FORM)
    unexpected = sorted(name for name in found if name not in OLD_FORM_ALLOWED)
    check(not unexpected, f"these files still check the old run ID form: {unexpected}")


def test_the_code_run_shape_of_the_standalone_scripts_is_the_contracts_and_takes_every_run_id() -> None:
    for name in ("infra/services/scripts/code_identify.py", "services/training/fetch_code.py", "infra/jobs/layers.tf"):
        check(contract.CODE_RUN_SHAPE in (ROOT / name).read_text(), f"{name} does not carry the contract's code run shape")
    for run in (OLD_RUN, *VALID):
        check(re.fullmatch(contract.CODE_RUN_SHAPE, run) is not None, f"the shape refuses {run}")
        check(re.fullmatch(contract.CODE_RUN, run) is not None, f"the code run pattern refuses {run}")
    for bad in ("", "../x", "a/b", "silver_3f9a1c07/..", "UPPER_3f9a1c07", "silver_3f9a1c0", "silver_3f9a1c07_1_2_3"):
        check(re.fullmatch(contract.CODE_RUN_SHAPE, bad) is None, f"the shape accepts {bad!r}")


@pytest.mark.parametrize("run", [f"ingestion_{HEX}", f"ingestion_{HEX}_2"])
def test_the_acquisition_copies_take_an_ingestion_run(run: str) -> None:
    check(raw_root.is_test_root(f"ingestion_test/{run}/"), f"the test root of {run} is refused")
    check(plan_rebuild.RUN_ID.fullmatch(run) is not None and publisher_run.RUN_ID_FORM.fullmatch(run) is not None, f"{run} is refused")


@pytest.mark.parametrize("run", [OLD_RUN, f"silver_{HEX}", f"ingestion_retry_{HEX}", f"bronze_{HEX}", "latest", ""])
def test_the_acquisition_copies_refuse_everything_but_an_ingestion_run(run: str) -> None:
    check(not raw_root.is_test_root(f"ingestion_test/{run}/"), f"the test root of {run} is accepted")
    check(publisher_run.RUN_ID_FORM.fullmatch(run) is None, f"{run} is accepted as an original run")


def test_the_retry_run_ids_of_the_job_and_the_plan_report_agree() -> None:
    for run in (f"ingestion_retry_{HEX}_2", f"ingestion_retry_{HEX}_2_3"):
        check(plan_rebuild.RUN_ID.fullmatch(run) is not None, f"the plan report refuses {run}")
    for run in (f"ingestion_retry_{HEX}_4", f"ingestion_retry_{HEX}_1", f"ingestion_retry_{HEX}"):
        check(plan_rebuild.RUN_ID.fullmatch(run) is None, f"the plan report accepts {run}")


def test_every_run_id_pattern_of_a_schema_is_the_contracts() -> None:
    """Each schema holds the contract's pattern itself, not only a pattern that is free of the old date form. ``tests/test_run_id_forms.py`` sends valid
    and invalid run IDs through each one."""
    wrong = sync.differing()
    check(not wrong, f"{len(wrong)} schema patterns differ from the contract; run scripts.orchestration.sync_run_id_schemas --write: {wrong[:2]}")
    held = sync.copies()
    check(len(held) >= 35, f"only {len(held)} run ID patterns were found in the schemas")
    for path, where, kind, pattern, _ in held:
        check(OLD_FORM not in pattern or kind == "stored", f"{path.name}{where}: a pattern still holds the old form")


# The schema fields that name a run a stored record made (the stamp form of before the run ID redesign too). Every other copy takes the run ID form alone.
STORED_FIELDS = {
    ("data_contracts/training/extract_manifest.schema.json", "/properties/publication_run_id"),
    ("data_contracts/training/extract_manifest.schema.json", "/properties/gold_run_id"),
    ("data_contracts/training/extract_manifest.schema.json", "/properties/base_run"),
}


def test_only_the_stored_fields_of_the_extract_manifest_take_the_stamp_form_and_their_pattern_is_the_contracts() -> None:
    stored = {(str(path.relative_to(ROOT)), where) for path, where, kind, _, _ in sync.copies() if kind == "stored"}
    check(stored == STORED_FIELDS, f"the schema fields that take a stored run differ: {sorted(stored ^ STORED_FIELDS)}")
    schema = json.loads((ROOT / "data_contracts/training/extract_manifest.schema.json").read_text())
    for name in ("publication_run_id", "gold_run_id", "base_run"):
        pattern = schema["properties"][name]["pattern"]
        check(pattern == f"^{contract.STORED_RUN}$", f"{name} does not hold the contract's stored run pattern")
    own = schema["properties"]["run_id"]["pattern"]
    check(re.match(own, f"silver_{HEX}") is not None and re.match(own, OLD_RUN) is None, "the manifest's own run takes a stamp form or refuses a new one")


def test_the_schemas_take_the_new_form_and_refuse_the_old_one() -> None:
    schema = json.loads((ROOT / "data_contracts/orchestration/ecs_input_map.schema.json").read_text())
    pattern = schema["$defs"]["run_id"]["pattern"]
    check(re.match(pattern, f"silver_{HEX}") is not None, "the input map schema refuses a new run ID")
    check(re.match(pattern, OLD_RUN) is None, "the input map schema accepts the old form")


def test_the_gold_publish_copy_of_the_run_id_form_is_the_contracts() -> None:
    check(gold_publish_record.RUN_ID_PATTERN == contract.RUN_ID, "the copy that the gold containers hold differs from the contract's form")


def test_the_catalog_restore_reads_a_snapshot_name_of_every_run_id_shape_and_refuses_the_old_form() -> None:
    for run in (f"silver_{HEX}", f"gold_{HEX}_2", f"research_ols_{HEX}", f"canary_deploy_clabsi_{HEX}", f"ingestion_retry_{HEX}_2"):
        check(catalog_restore.BACKUP_NAME.fullmatch(f"catalog_committed_{run}.json") is not None, f"the catalog restore refuses the snapshot of {run}")
    check(catalog_restore.BACKUP_NAME.fullmatch(f"catalog_committed_{OLD_RUN}.json") is None, "the catalog restore accepts the old run ID form")
