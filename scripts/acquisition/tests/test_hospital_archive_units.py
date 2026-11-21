"""The 34 hospital archives of main-hai-pdc as seed-only manual units: the owner's browser downloads, bound to the digest each unit's specification records."""

from __future__ import annotations

import hashlib
import json
import re
import shutil
import zipfile
from collections import Counter
from pathlib import Path

import pytest

from scripts.acquisition import build_seed_manifest, manual_content, redownload_route
from scripts.acquisition import check_baseline_run as check_run
from scripts.acquisition.s3_store import encoded_json
from tests.support import check

MODE = "hospital_archive"
BASELINE = Path("config/acquisition/baseline_run")
SEED = Path("config/acquisition/bootstrap_seed")
ANNUAL = "hospitals_annual_2026.zip"
FLAG = "original_contains_personal_details:private_owner_only:redacted_derivative_only"


def fetched(path: Path) -> bool:
    """True for a seeded file that is present with its content: a job that does not fetch Git LFS holds only a pointer, or no file."""
    if not path.is_file():
        return False
    with path.open("rb") as handle:
        return not handle.read(64).startswith(b"version https://git-lfs")


def committed() -> tuple[dict, dict, dict]:
    queue = json.loads((BASELINE / "queue.json").read_bytes())
    specs = json.loads((BASELINE / "capture_specs.json").read_text(encoding="utf-8"))
    manifest = json.loads((SEED / "manifest.json").read_text(encoding="utf-8"))
    return queue, specs, manifest


def hospital_units(queue: dict) -> list[dict]:
    return [unit for unit in queue["queue"] if unit["source_id"] == "main-hai-pdc"]


def test_the_34_main_hai_pdc_units_are_seed_only_manual_units_that_request_nothing() -> None:
    queue, specs, _ = committed()
    units = hospital_units(queue)
    check(len(units) == 34 and all(unit["mode"] == MODE for unit in units), f"{Counter(unit['mode'] for unit in units)}")
    for unit in units:
        check(unit["disposition"] == "approved_manual" and unit["route"] == "manual_browser_download", f"{unit['snapshot_id']} shape")
        check(unit["flags"] == [FLAG], f"{unit['snapshot_id']} keeps its flag until the owner rules on it")
        spec = specs["specs"][unit["snapshot_id"]]
        mode, name = manual_content.plan_unit(unit, spec)
        check(mode == MODE and name == unit["url"].rsplit("/", 1)[-1], f"{unit['snapshot_id']} stores the file its URL names")
    check(MODE in redownload_route.SEED_ONLY_MODES and MODE in manual_content.SEED_ONLY_MODES, "the mode is declared seed-only once")
    check(
        not any(unit["url"].startswith("https://data.cms.gov/provider-data/") for unit in queue["queue"] if unit["disposition"] == "direct_download"),
        "no direct download",
    )
    check(check_run.queue_problems(BASELINE) == [], "the queue, its lock and its budgets agree")


def test_each_unit_records_the_digest_that_binds_its_seeded_archive_and_when_and_from_where_the_owner_saved_it() -> None:
    queue, specs, manifest = committed()
    listed = {item["unit"]: item for item in manifest["files"] if "unit" in item}
    for unit in hospital_units(queue):
        spec = specs["specs"][unit["snapshot_id"]]
        digest = spec["receipt_template"]["expected_sha256"]
        check(re.fullmatch(r"[0-9a-f]{64}", digest) is not None, f"{unit['snapshot_id']} records a digest")
        name = spec["stored_data_files"][0]
        check(
            listed[unit["snapshot_id"]] == {"path": f"manual_units/{unit['snapshot_id']}/{name}", "sha256": digest, "unit": unit["snapshot_id"]},
            f"{name} manifest entry",
        )
        saved = spec["saved"]
        check(
            saved["origin_url"] == unit["url"] and saved["saved_by"] == "owner, browser" and saved["birth_time_utc"].startswith("2026-11-2"),
            f"{name} saved record",
        )
        path = SEED / "manual_units" / unit["snapshot_id"] / name
        if fetched(path):
            check(hashlib.sha256(path.read_bytes()).hexdigest() == digest and path.stat().st_size == unit["bytes"], f"{name} equals its record")


def test_the_seeded_archives_hold_the_members_the_specifications_list() -> None:
    queue, specs, _ = committed()
    for unit in hospital_units(queue):
        spec = specs["specs"][unit["snapshot_id"]]
        path = SEED / "manual_units" / unit["snapshot_id"] / spec["stored_data_files"][0]
        if not fetched(path):
            continue
        with zipfile.ZipFile(path) as archive:
            names = {
                info.filename
                for info in archive.infolist()
                if not info.is_dir() and "__MACOSX" not in info.filename.split("/") and not info.filename.endswith(".DS_Store")
            }
        if path.name != ANNUAL:
            check(names == {"/".join(item["member_chain"]) for item in spec["archive_members"]}, f"{path.name} members")


def test_the_republished_annual_archive_is_taken_as_published_with_every_data_member_still_data() -> None:
    queue, specs, _ = committed()
    [unit] = [u for u in hospital_units(queue) if u["url"].endswith(ANNUAL)]
    spec = specs["specs"][unit["snapshot_id"]]
    check(unit["bytes"] == 66_861_725, f"{unit['bytes']}")
    check(spec["receipt_template"]["expected_sha256"] == "66d34f59080dbf132f74960b7b4d20e4b1726cd4fe9cba72cbede1315de2de78", "the new digest")
    chains = [tuple(item["member_chain"]) for item in spec["archive_members"]]
    check(len(chains) == len(set(chains)), "no member twice")
    check(
        {chain[0] for chain in chains}
        == {
            "hospitals_2026-02-25.zip",
            "hospitals_2026-05-13.zip",
            "hospitals_2026-08-13.zip",
            "hospitals_2026-09-30.zip",
            "hospitals_2026-11-12.zip",
            "manifest.json",
        },
        "the five archives and the manifest",
    )


def test_data_cms_gov_requests_come_from_the_other_units_only_and_the_34_units_add_the_two_release_hosts_only() -> None:
    queue_body = (BASELINE / "queue.json").read_bytes()
    hosts = json.loads((BASELINE / "publisher_hosts_scope.json").read_text(encoding="utf-8"))
    others = [unit for unit in json.loads(queue_body)["queue"] if unit["source_id"] != "main-hai-pdc"]
    release = {"github.com", "release-assets.githubusercontent.com"}
    check(
        redownload_route.queue_hosts({"queue": others}) | release == redownload_route.queue_hosts(json.loads(queue_body)), "the 34 units add no publisher host"
    )
    check(
        release <= set(hosts["hosts"]) and not release & redownload_route.queue_hosts({"queue": others}),
        "the release hosts are on the hosts file and from these units",
    )
    check(
        redownload_route.host_problems(queue_body, BASELINE / "publisher_hosts_scope.json", BASELINE / "hosts_approval.json") == [],
        "the approval governs the queue",
    )
    check(encoded_json(hosts) == (BASELINE / "publisher_hosts_scope.json").read_bytes(), "the hosts file keeps its form")


def test_the_manifest_builder_takes_the_digest_of_a_hospital_archive_from_its_specification() -> None:
    queue, specs, _ = committed()
    body = build_seed_manifest.manifest_body(BASELINE)
    check(
        json.loads(body)["queue_sha256"] == hashlib.sha256((BASELINE / "queue.json").read_bytes()).hexdigest() and len(hospital_units(queue)) == 34,
        "the manifest follows the queue",
    )
    digests = [item["sha256"] for item in json.loads(body)["files"] if item.get("unit", "").startswith("main-hai-pdc__")]
    check(len(digests) == 34 and None not in digests, f"{len(digests)} digests")


def test_every_fetched_hospital_archive_passes_the_seed_check_of_the_bundle() -> None:
    pointers = [path for path in SEED.joinpath("manual_units").glob("main-hai-pdc__*/*.zip") if not fetched(path)]
    if pointers:
        pytest.skip("Git LFS files are not fetched")
    found = [line for line in check_run.seed_only_problems(BASELINE, SEED) if "main-hai-pdc" in line]
    check(found == [], f"{found}")


def test_the_release_manifest_lists_the_34_archives_with_the_size_digest_and_address_of_each_and_nothing_else() -> None:
    queue, specs, manifest = committed()
    document = json.loads((SEED / "release_assets.json").read_text(encoding="utf-8"))
    check(document["kind"] == "bootstrap_seed_release_manifest" and document["tag"] == "seed-main-hai-pdc-20261120", "the kind and the tag")
    check(document["queue_sha256"] == hashlib.sha256((BASELINE / "queue.json").read_bytes()).hexdigest(), "the manifest follows the queue")
    assets = document["assets"]
    check(len(assets) == 34 and sum(item["bytes"] for item in assets) == 578_560_347, f"{len(assets)} assets, {sum(item['bytes'] for item in assets)} bytes")
    check(all(set(item) == {"name", "path", "bytes", "sha256", "url"} for item in assets), "name, path, size, digest and address only")
    listed = {item["path"]: item["sha256"] for item in manifest["files"]}
    by_unit = {unit["snapshot_id"]: unit for unit in hospital_units(queue)}
    for item in assets:
        unit = by_unit[item["path"].split("/")[1]]
        spec = specs["specs"][unit["snapshot_id"]]
        check(
            item["bytes"] == unit["bytes"] and item["sha256"] == spec["receipt_template"]["expected_sha256"] == listed[item["path"]], f"{item['name']} agrees"
        )
        check(
            item["url"] == f"https://github.com/ian-mungai/healthcare-hai-prediction/releases/download/seed-main-hai-pdc-20261120/{item['name']}", item["name"]
        )
        check(item["name"] == spec["stored_data_files"][0] and item["bytes"] <= 2 * 1024**3, f"{item['name']} is the stored file and within the 2 GiB bound")
    check(len({item["name"] for item in assets}) == 34, "the asset names are unique, as one release needs")
    check(check_run.release_problems_of_seed(BASELINE, SEED) == [], "the check finds the manifest exact")


def test_a_release_manifest_that_differs_from_the_queue_and_the_specifications_is_a_baseline_problem(tmp_path: Path) -> None:
    shutil.copytree(BASELINE, tmp_path / "bundle")
    seed = tmp_path / "seed"
    seed.mkdir()
    document = json.loads((SEED / "release_assets.json").read_text(encoding="utf-8"))
    document["assets"][0]["sha256"] = "0" * 64
    (seed / "release_assets.json").write_text(json.dumps(document), encoding="utf-8")
    check(check_run.release_problems_of_seed(tmp_path / "bundle", seed) != [], "another digest is a problem")
    check(check_run.release_problems_of_seed(tmp_path / "bundle", tmp_path / "none") == [], "a seed without a release manifest has none to check")
