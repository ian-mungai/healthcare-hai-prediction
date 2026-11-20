"""The bootstrap queue, capture specification, secrets list and seed manifest after the ACS table-vintage units moved from manual exports to the Census API."""

from __future__ import annotations

import hashlib
import json
import shutil
from collections import Counter
from pathlib import Path

import pytest

from scripts.acquisition import build_bootstrap_queue as build
from scripts.acquisition import build_capture_specs as specs
from scripts.acquisition import census_acs_tables_contract as contract
from scripts.acquisition import check_baseline_run as check_run
from scripts.acquisition import completion_record, redownload, task_role
from scripts.acquisition.source_registry import REPO_ROOT
from scripts.acquisition.tests.test_build_bootstrap_queue import unit as synthetic_unit
from tests.support import check

ROOT = REPO_ROOT / "config/acquisition/baseline_run"
SEED = REPO_ROOT / "config/acquisition/bootstrap_seed"
FLAG = "browser_export_moved_to_census_api"
# SHA-256 of the 1,867 queue entries the tracked queue held, in order, before the 16 manual ACS units left it (canonical JSON, sorted keys).
UNCHANGED_UNITS_SHA256 = "c1c87002edaf489203fc847dacf8318f99f29cd5227d97bbc7f1a41a0858f487"
MINI = {"subject": {"S1701": [2012, 2013]}, "profile": {"DP03": [2012]}}


def units_digest(units: list[dict]) -> str:
    return hashlib.sha256(json.dumps(units, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def tracked() -> dict:
    return json.loads((ROOT / "queue.json").read_text(encoding="utf-8"))


def plan_units() -> list[dict]:
    return contract.load_plan()["batches"]


def expected_unit(batch: dict) -> dict:
    return {
        "bytes": 64 * 1024**2,
        "disposition": "api",
        "flags": [FLAG],
        "mode": "census_acs_tables",
        "pages": 1,
        "route": "publisher_api",
        "snapshot_id": contract.queue_snapshot_id(batch),
        "source_id": "ACS",
        "url": batch["endpoint"],
    }


def test_the_tracked_queue_swaps_the_16_manual_acs_units_for_173_api_units_after_the_acs_file_units() -> None:
    queue = tracked()
    units = [u for u in queue["queue"] if u["mode"] != "ahrq_compendium"]
    check(len(queue["queue"]) == 2050 and queue["entries"] == 2050 and len(units) == 2040, f"{len(queue['queue'])} units")
    check(not [u for u in units if u["source_id"] == "ACS" and u["disposition"] == "approved_manual"], "a manual ACS unit is left")
    api = [i for i, u in enumerate(units) if u["mode"] == "census_acs_tables"]
    check(len(api) == 173 and api == list(range(api[0], api[0] + 173)), "the 173 units are not one block")
    last_file = max(i for i, u in enumerate(units) if u["source_id"] == "ACS" and u["mode"] == "file")
    check(api[0] == last_file + 1, "the API units do not follow the last ACS file unit")
    check([units[i] for i in api] == [expected_unit(b) for b in plan_units()], "the unit entries differ from the plan")
    manual = Counter(u["mode"] for u in queue["queue"] if u["disposition"] == "approved_manual")
    check(manual == {"hud_xlsx": 44, "wonder_export": 6, "ahrq_compendium": 10}, f"the manual set is {dict(manual)}")
    budgets = queue["budgets"]
    check(budgets["acquisition_units"] == 2050 and budgets["max_primary_attempts"] == 4100 and budgets["pages_recorded"] == 2050, f"budgets: {budgets}")
    check(budgets["units_by_route"] == {"manual_browser_download": 60, "publisher_api": 1297, "publisher_url": 693}, f"routes: {budgets['units_by_route']}")
    check(queue["dispositions"] == {"api": 1297, "approved_manual": 60, "direct_download": 693}, f"dispositions: {queue['dispositions']}")
    check(check_run.queue_problems(ROOT) == [], f"queue problems: {check_run.queue_problems(ROOT)}")


def test_the_other_1867_units_are_byte_equal_to_the_entries_the_queue_held_before() -> None:
    queue = tracked()
    others = [u for u in queue["queue"] if u["mode"] not in ("census_acs_tables", "ahrq_compendium")]
    check(len(others) == 1867 and units_digest(others) == UNCHANGED_UNITS_SHA256, "an entry other than the swapped ones changed")
    swap = queue["derived_from"]["acs_api_swap"]
    check(swap["kept_units_sha256"] == UNCHANGED_UNITS_SHA256 and swap["dropped"] == 16 and swap["added"] == 173, f"swap record: {swap}")


def acs_source(extra_manual: int = 2, file_units: int = 2) -> tuple[bytes, str]:
    units = [synthetic_unit("a", "api", "api.example.invalid") | {"route": "publisher_api"}]
    units += [synthetic_unit(f"f{n}", "direct_download") | {"source_id": "ACS"} for n in range(file_units)]
    units += [synthetic_unit(f"m{n}", "approved_manual") | {"source_id": "ACS", "route": "manual_browser_download", "bytes": 7} for n in range(extra_manual)]
    units += [synthetic_unit("z", "direct_download")]
    routes = Counter(u["route"] for u in units)
    queue = {
        "budgets": {
            "acquisition_units": len(units),
            "recorded_bytes": sum(u["bytes"] for u in units),
            "pages_recorded": len(units),
            "max_primary_attempts": 2 * len(units),
            "units_by_route": dict(routes),
            "bls_calls": 0,
        },
        "dispositions": {},
        "entries": len(units),
        "execution_status": "frozen_offline_no_requests_made",
        "inputs_sha256": {"x": "0" * 64},
        "kind": "bounded_redownload_queue",
        "loaded_files_no_unit_covers": [],
        "model_eligible": False,
        "queue": units,
        "run": 3,
    }
    body = (json.dumps(queue, indent=1, sort_keys=True) + "\n").encode()
    return body, hashlib.sha256(body).hexdigest()


def mini_plan() -> dict:
    return {"batches": contract.planned_batches(MINI)}


def test_the_swap_drops_the_manual_acs_units_and_places_the_api_units_after_the_last_acs_file_unit() -> None:
    body, digest = acs_source()
    plan = mini_plan()
    trimmed = json.loads(build.trim_queue(body, digest, plan))
    ids = [u["snapshot_id"] for u in trimmed["queue"]]
    api = [contract.queue_snapshot_id(b) for b in plan["batches"]]
    check(ids == ["a", "f0", "f1", *api, "z"], f"order: {ids}")
    budgets = trimmed["budgets"]
    units = trimmed["queue"]
    check(budgets["acquisition_units"] == len(units) == trimmed["entries"], f"unit count: {budgets}")
    check(budgets["recorded_bytes"] == sum(u["bytes"] for u in units) and budgets["pages_recorded"] == len(units), f"sums: {budgets}")
    check(budgets["max_primary_attempts"] == 2 * len(units), f"attempts: {budgets}")
    check(budgets["units_by_route"] == dict(Counter(u["route"] for u in units)) and "manual_browser_download" not in budgets["units_by_route"], "routes")
    check(trimmed["dispositions"] == {"api": 1 + len(api), "direct_download": 3}, f"dispositions: {trimmed['dispositions']}")
    swap = trimmed["derived_from"]["acs_api_swap"]
    check(swap["dropped"] == 2 and swap["added"] == len(api) and swap["plan_sha256"], f"swap record: {swap}")
    check(swap["kept_units_sha256"] == units_digest([u for u in units if u["mode"] != "census_acs_tables"]), "kept-unit digest")


def test_the_swap_leaves_every_other_entry_byte_equal_and_is_deterministic() -> None:
    body, digest = acs_source()
    source_units = json.loads(body)["queue"]
    trimmed = json.loads(build.trim_queue(body, digest, mini_plan()))
    kept = [u for u in source_units if u["source_id"] != "ACS" or u["disposition"] != "approved_manual"]
    check([u for u in trimmed["queue"] if u["mode"] != "census_acs_tables"] == kept, "an entry other than the swapped ones changed")
    check(build.trim_queue(body, digest, mini_plan()) == build.trim_queue(body, digest, mini_plan()), "two builds differ")


def test_a_queue_without_a_plan_keeps_its_manual_acs_units() -> None:
    body, digest = acs_source()
    trimmed = json.loads(build.trim_queue(body, digest))
    check(len(trimmed["queue"]) == 6 and "acs_api_swap" not in trimmed["derived_from"], "the swap ran without a plan")


def test_a_source_with_no_acs_file_unit_to_follow_is_refused() -> None:
    body, digest = acs_source(file_units=0)
    with pytest.raises(ValueError, match="no ACS file unit"):
        build.trim_queue(body, digest, mini_plan())


def test_the_queue_flag_has_no_date_and_does_not_look_like_a_secret_or_a_moved_mmd_unit() -> None:
    flag = build.ACS_API_FLAG
    check(flag == FLAG and not any(ch.isdigit() for ch in flag), "the flag holds a digit")
    check(not flag.startswith("browser_export_moved_to_api"), "the flag starts like the MMD flag")


def test_the_tracked_specs_for_the_173_units_come_from_the_plan_and_hold_no_bucket_key_or_path() -> None:
    document = json.loads((ROOT / "capture_specs.json").read_text(encoding="utf-8"))
    batches = {contract.queue_snapshot_id(b): b for b in plan_units()}
    check(document["units"] == 2050 and len(document["specs"]) == 2050, "spec count")
    for snapshot, batch in batches.items():
        expected = {"mode": "census_acs_tables", "plan_item": batch["id"], "stored_data_files": [batch["file_name"]]}
        check(document["specs"][snapshot] == expected, f"spec of {snapshot}")
    check(check_run.specs_problems(ROOT) == [], f"spec problems: {check_run.specs_problems(ROOT)}")
    check(not any(marker in json.dumps(document["specs"][s]) for s in batches for marker in check_run.ENVIRONMENT_MARKERS), "an environment marker")


def test_a_unit_with_no_receipt_takes_its_spec_from_the_plan_and_a_unit_outside_the_plan_is_refused(tmp_path: Path) -> None:
    plan = mini_plan()
    batch = plan["batches"][0]
    unit = expected_unit(batch)
    check("receipt" not in unit and "receipt_sha256" not in unit, "the unit holds a receipt")
    result = json.loads(specs.derive([unit], tmp_path, "f" * 64, tables_plan=plan))["specs"][unit["snapshot_id"]]
    check(result == {"mode": "census_acs_tables", "plan_item": batch["id"], "stored_data_files": [batch["file_name"]]}, f"spec: {result}")
    with pytest.raises(ValueError, match="no batch of the Census table plan"):
        specs.derive([unit | {"snapshot_id": "Census_ACS_Tables__v1__" + "0" * 32}], tmp_path, "f" * 64, tables_plan=plan)
    with pytest.raises(ValueError, match="no stored receipt"):
        specs.derive([unit | {"mode": "file", "receipt": "data/none/receipt.json"}], tmp_path, "f" * 64, tables_plan=plan)


def test_a_previous_specification_supplies_the_specs_of_units_that_stay(tmp_path: Path) -> None:
    plan = mini_plan()
    unit = expected_unit(plan["batches"][0])
    stay = {"snapshot_id": "S", "mode": "hud_api", "receipt": "data/none/receipt.json"}
    kept = {"mode": "hud_api", "plan_item": "x", "stored_data_files": ["c.csv"]}
    rewrites = {"pointers": 3, "phone_examples": 1}
    previous = {"specs": {"S": kept}, "template_rewrites": rewrites}
    document = json.loads(specs.derive([stay, unit], tmp_path, "f" * 64, tables_plan=plan, previous=previous))
    check(document["specs"]["S"] == kept and document["template_rewrites"] == rewrites, "kept spec or header changed")
    check(document["units"] == 2 and unit["snapshot_id"] in document["specs"], "the new unit is missing")


def test_the_secrets_list_counts_the_173_keyed_units_and_the_check_fails_on_a_stale_count_or_another_key_name(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    names = check_run.secrets_read_by_code()
    document = json.loads((ROOT / "required_secrets.json").read_text(encoding="utf-8"))
    census = next(item for item in document["secrets"] if item["name"] == "census_api_key")
    check(census["queue_units"] == 173 and "census_acs_tables_transport.py" in census["read_by"], f"entry: {census}")
    check("No unit of the bootstrap queue uses" not in census["used_for"] and "173" in census["used_for"], "the use text is stale")
    check(check_run.secret_problems(ROOT, names) == [], f"secret problems: {check_run.secret_problems(ROOT, names)}")
    stale = tmp_path / "stale"
    stale.mkdir()
    shutil.copy(ROOT / "capture_specs.json", stale / "capture_specs.json")
    census["queue_units"] = 0
    (stale / "required_secrets.json").write_text(json.dumps(document), encoding="utf-8")
    problems = check_run.secret_problems(stale, names)
    check(any("census_api_key says 0 queue units, the queue has 173" in p for p in problems), f"stale count not found: {problems}")
    monkeypatch.setitem(check_run.MODE_SECRETS, "census_acs_tables", "hud_api_key")
    other = check_run.secret_problems(ROOT, names)
    check(any("census_api_key says 173 queue units, the queue has 0" in p for p in other), f"another key name not found: {other}")


def test_the_task_reads_the_census_key_for_the_new_mode_and_the_unit_is_a_keyed_task_unit() -> None:
    check(task_role.KEY_OF_MODE["census_acs_tables"] == "census_api_key", "key of the mode")
    check("census_acs_tables" in redownload.SECRET_MODES, "secret modes")
    check(check_run.MODE_SECRETS["census_acs_tables"] == "census_api_key", "check map")
    check(redownload.lane_of(expected_unit(plan_units()[0])) == redownload.TASK_KEYED, "lane")
    check(contract.load_plan()["credential_reference"] in check_run.secrets_read_by_code(), "the plan names a secret the list lacks")


def identities(units: list[dict], by_snapshot: dict) -> dict[str, str]:
    """The unit key of each unit from source, handler mode, request and reviewed selection only (no snapshot ID, time or run)."""
    return {
        u["snapshot_id"]: completion_record.unit_key(
            {
                "source_id": u["source_id"],
                "mode": u["mode"],
                "request": {"method": "GET", "url": u["url"]},
                "selection": {"plan_item": by_snapshot[u["snapshot_id"]]["plan_item"], "pages": u["pages"]},
            }
        )
        for u in units
    }


def test_the_173_unit_keys_are_distinct_stable_under_reorder_and_equal_across_two_builds() -> None:
    document = json.loads((ROOT / "capture_specs.json").read_text(encoding="utf-8"))["specs"]
    units = [u for u in tracked()["queue"] if u["mode"] == "census_acs_tables"]
    first = identities(units, document)
    check(len(units) == 173 and len(set(first.values())) == 173, "the keys are not distinct")
    check(identities(units[::-1], document) == first, "the keys depend on queue order")
    renamed = [u | {"snapshot_id": f"renamed_{i}"} for i, u in enumerate(units)]
    again = identities(renamed, {f"renamed_{i}": document[u["snapshot_id"]] for i, u in enumerate(units)})
    check(list(again.values()) == list(first.values()), "the keys depend on the snapshot ID")
    second = [expected_unit(b) for b in plan_units()]
    check(identities(second, document) == first, "two builds give other keys")


def test_the_seed_manifest_binds_the_new_queue_and_holds_no_acs_file() -> None:
    manifest = json.loads((SEED / "manifest.json").read_text(encoding="utf-8"))
    body = (ROOT / "queue.json").read_bytes()
    check(manifest["queue_sha256"] == hashlib.sha256(body).hexdigest(), "the manifest names another queue")
    check(check_run.seed_problems(ROOT, SEED, manifest) == [], f"seed problems: {check_run.seed_problems(ROOT, SEED, manifest)}")
    modes = {u["snapshot_id"]: u["mode"] for u in json.loads(body)["queue"]}
    seeded = {item["unit"] for item in manifest["files"] if "unit" in item}
    check(seeded and {modes[s] for s in seeded} == {"hud_xlsx", "wonder_export", "ahrq_compendium"}, "a seeded unit is not a HUD, WONDER or AHRQ unit")
    check(not any(Path(item["path"]).name.startswith("ACS") for item in manifest["files"]), "an ACS file is in the seed")


def test_the_whole_tracked_bundle_passes_its_offline_check() -> None:
    found = check_run.problems(ROOT, SEED)
    check(found == [], f"bundle problems: {found}")
