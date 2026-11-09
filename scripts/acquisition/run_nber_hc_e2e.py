"""Repeatable synthetic NBER Hospital Compare mirror capture-to-S3 verification, without live downloads or credentials."""

import argparse
import base64
import copy
import hashlib
import io
import json
import shutil
import sys
import tempfile
import zipfile
from contextlib import contextmanager, suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import patch

from scripts.acquisition import nber_hc_contract as contract
from scripts.acquisition import store_nber_hc
from scripts.acquisition.build_nber_hc_plan import build
from scripts.acquisition.code_closure import closure_hashes
from scripts.acquisition.run_bls_api_e2e import inventory
from scripts.acquisition.s3_store import encoded_json, fingerprint, write_once
from scripts.acquisition.source_registry import REPO_ROOT, canonical_hash, read_json, require
from scripts.acquisition.store_nber_hc import check_gate, execute, fetch, run_all, verify_local
from scripts.acquisition.tests.test_s3_store import OUTPUTS, SETTINGS, FakeS3
from scripts.acquisition.transport import Limits

LIMITS = Limits(attempts=1, timeout_seconds=5, max_seconds=30)
HCAHPS, TIMELY, HAI = "HCAHPS - Hospital.csv", "Timely and Effective Care - Hospital.csv", "Healthcare Associated Infections - Hospital.csv"
GENERAL, FOOTNOTES = "Hospital General Information.csv", "Footnote Crosswalk.csv"
# Newest member time per synthetic release: the release_date rule must return exactly these days.
NEWEST = {"2017/10": (2017, 9, 28), "2018/07": (2018, 6, 19), "2018/10": (2018, 9, 27), "2019/03": (2019, 3, 4)}
DATES = {folder: "-".join(f"{part:02}" for part in day) for folder, day in NEWEST.items()}
NBER = "https://data.nber.org/compare/hospital/"


class Response(io.BytesIO):
    """A minimal HTTP response for the project downloader."""

    def __init__(self, body: bytes, url: str, status: int = 200, headers: dict | None = None) -> None:
        super().__init__(body)
        self.status, self.url = status, url
        self.headers = {"Content-Length": str(len(body)), "Content-Type": "application/zip", **(headers or {})}

    def geturl(self) -> str:
        return self.url


class Web:
    """Serves synthetic zip bytes by URL and counts requests; overrides make one URL misbehave."""

    def __init__(self, bodies: dict[str, bytes]) -> None:
        self.bodies, self.requests = bodies, 0
        self.seen: list[str] = []
        self.override: dict[str, Response | Exception] = {}

    def open(self, request: Any, timeout: float) -> Response:
        self.requests += 1
        self.seen.append(request.full_url)
        special = self.override.get(request.full_url)
        if isinstance(special, Exception):
            raise special
        return special if special is not None else Response(self.bodies[request.full_url], request.full_url)


class Offline(Web):
    """Any network use is a failure."""

    def open(self, request: Any, timeout: float) -> Response:
        raise AssertionError("network used")


def make_zip(folder: str, names: tuple[str, ...], oldest: bool = False) -> bytes:
    """A synthetic CMS flat-file bundle; one member carries the newest modification time."""
    buffer = io.BytesIO()
    year, month, day = NEWEST[folder]
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for index, name in enumerate(names):
            when = (year, month, day, 12, 0, 0) if index == 0 else (year - 1, month, day, 8, 30, 0)
            archive.writestr(zipfile.ZipInfo(name, when), f"id,value\nsynthetic_{folder.replace('/', '_')}_{index},{index}\n".encode())
    return buffer.getvalue()


FULL = (HAI, TIMELY, HCAHPS, GENERAL, FOOTNOTES)
CONTENT = {"2017/10": FULL, "2018/07": FULL, "2018/10": FULL, "2019/03": FULL}


def zip_url(folder: str) -> str:
    return f"{NBER}{folder}/hospitals_compare_{folder.replace('/', '')}.zip"


def bodies() -> dict[str, bytes]:
    return {zip_url(folder): make_zip(folder, names) for folder, names in CONTENT.items()}


def synthetic_plan(root: Path, served: dict[str, bytes]) -> tuple[dict, Path, str]:
    """A locked plan for three synthetic releases and the gate, built through the production plan builder."""
    inputs = [
        {"folder": folder, "sha256": hashlib.sha256(served[zip_url(folder)]).hexdigest(), "bytes": len(served[zip_url(folder)]), "release_date": DATES[folder]}
        for folder in ("2017/10", "2018/07", "2018/10")
    ]
    gate_body = served[zip_url("2019/03")]
    gate_sha256 = hashlib.sha256(gate_body).hexdigest()
    gate = {"sha256": gate_sha256, "bytes": len(gate_body)}
    plan = build(inputs, gate, [])
    path = root / "plan.json"
    write_once(path, encoded_json(plan))
    write_once(path.with_suffix(".lock.json"), encoded_json({"plan_sha256": canonical_hash(plan)}))
    return plan, path, gate_sha256


def refusing(action: Any, folder: Path) -> Any:
    """Run an action that must be refused; a refusal also proves nothing was stored, so a missing refusal never passes."""

    def run() -> None:
        try:
            action()
        except (ValueError, OSError, KeyError, TypeError) as failure:
            if not isinstance(failure, NotImplementedError):
                require(
                    not (folder / "batches").exists() and not any(p.is_file() for p in folder.rglob("*") if "downloads" not in p.parts),
                    "A refused run wrote files",
                )
            raise

    return run


@contextmanager
def edited_plan(plan: dict, plan_path: Path, folder: str, edit: Any) -> Any:
    """Yield a re-locked copy of the plan with one release edited, with the contract pointed at it."""
    changed = copy.deepcopy(plan)
    release = next(r for r in changed["releases"] if r["folder"] == folder)
    edit(release)
    release["id"] = contract.release_id(release)
    path = Path(tempfile.mkdtemp(dir=plan_path.parent)) / "plan.json"
    write_once(path, encoded_json(changed))
    write_once(path.with_suffix(".lock.json"), encoded_json({"plan_sha256": canonical_hash(changed)}))
    with patch.object(contract, "PLAN_PATH", path):
        yield changed, release


def storage_keys(client: FakeS3) -> list[str]:
    return sorted(client.objects)


def exercise(root: Path) -> list[dict]:
    """Use production orchestration with synthetic releases and versioned fake storage."""
    served = bodies()
    plan, plan_path, gate_sha = synthetic_plan(root, served)
    versions = root / "versions.json"
    write_once(versions, encoded_json({"versions": [{"code_sha256": closure_hashes("scripts.acquisition.store_nber_hc")}]}))
    scenarios: list[dict] = []

    def scenario(name: str, action: Any, error: str | None = None) -> None:
        try:
            action()
            scenarios.append({"name": name, "passed": error is None, "observed": "success", "expected": error or "success"})
        except (ValueError, OSError, KeyError, TypeError, StopIteration, IndexError, AssertionError, NotImplementedError) as failure:
            # The temporary folder's path never counts toward a match: a path can contain the expected words.
            observed = str(failure).replace(str(root), "<root>")
            scenarios.append({"name": name, "passed": error is not None and error in observed, "observed": observed[:200], "expected": error or "success"})

    by_folder = {r["folder"]: r for r in plan["releases"]}
    july, october, hai_only = by_folder["2018/07"], by_folder["2018/10"], by_folder["2017/10"]
    web, client = Web(served), FakeS3()
    with (
        patch.object(contract, "PLAN_PATH", plan_path),
        patch.object(contract, "VERSIONS_PATH", versions),
        patch.object(contract, "gate_sha256", lambda: gate_sha),
    ):
        scenario("locked_plan_loads", contract.load_plan)

        def real_plan() -> None:
            with (
                patch.object(contract, "PLAN_PATH", REPO_ROOT / "config/acquisition/nber_hospital_compare_plan.json"),
                patch.object(contract, "gate_sha256", contract.registry_gate_sha256),
            ):
                real = contract.load_plan()
            require(real["fidelity_gate"]["expected_sha256"] == "1a0e71473719b4108796d7f2e42e68011d5fd0560e33b9a35ceb2a7674ce19ef", "Gate hash differs")
            sizes = {r["folder"]: (r["expected_sha256"], r["bytes"]) for r in real["releases"]}
            require(sizes["2018/10"] == ("a9c6cc3c6dc77851a64c7d69c9d30b76dad235ea54c706c7cdc549fded4fa6de", 14889985), "2018/10 binding differs")
            require(sizes["2018/07"] == ("0e9c9363114172e81743e74720bfe8f4d4fd2d937264890ecd5280cb52b8806f", 16203900), "2018/07 binding differs")
            require(
                "2017/10" not in sizes and real["pending_releases"] == [{"folder": "2017/10", "reason": contract.PENDING_REASON}],
                "2017/10 not marked incomplete",
            )

        scenario("committed_plan_binds_registry_gate_and_marks_2017_10_incomplete", real_plan)

        def tampered_plan() -> None:
            original = plan_path.read_bytes()
            try:
                plan_path.write_bytes(encoded_json(plan | {"model_eligible": True}))
                contract.load_plan()
            finally:
                plan_path.write_bytes(original)

        scenario("plan_hash_mismatch_refused", tampered_plan, "plan lock differs")

        def relocked(edit: Any) -> Any:
            """Load a plan that was edited and re-locked, so only the plan's own checks can refuse it."""

            def action() -> None:
                original, lock = plan_path.read_bytes(), plan_path.with_suffix(".lock.json").read_bytes()
                changed = copy.deepcopy(plan)
                edit(changed)
                try:
                    plan_path.write_bytes(encoded_json(changed))
                    plan_path.with_suffix(".lock.json").write_bytes(encoded_json({"plan_sha256": canonical_hash(changed)}))
                    contract.load_plan()
                finally:
                    plan_path.write_bytes(original)
                    plan_path.with_suffix(".lock.json").write_bytes(lock)

            return action

        def edit_release(index: int, **values: Any) -> Any:
            return lambda changed: changed["releases"][index].update(values)

        scenario("relocked_member_map_change_refused", relocked(lambda c: c["releases"][1]["members"].__setitem__(HAI, contract.HAI_ID)), "member map differs")
        scenario(
            "relocked_url_outside_nber_host_refused",
            relocked(edit_release(0, url="https://example.org/compare/hospital/2017/10/hospitals_compare_201710.zip")),
            "host",
        )
        scenario("relocked_unreviewed_folder_refused", relocked(edit_release(0, folder="2018/01")), "not reviewed")
        scenario("relocked_bytes_without_new_id_refused", relocked(edit_release(0, bytes=1)), "binding differs")
        scenario("relocked_release_date_without_new_id_refused", relocked(edit_release(0, release_date="2000-01-01")), "binding differs")
        scenario("relocked_governance_change_refused", relocked(lambda c: c["governance"].__setitem__("contains_pii", True)), "governance differs")
        scenario(
            "relocked_gate_hash_not_cms_record_refused",
            relocked(lambda c: c["fidelity_gate"].__setitem__("expected_sha256", "0" * 64)),
            "hash differs from the CMS",
        )
        scenario("relocked_model_eligible_refused", relocked(lambda c: c.__setitem__("model_eligible", True)), "hold differs")

        def pending_input() -> None:
            pending = build([], {"sha256": gate_sha, "bytes": 1}, ["2017/10"])
            require(
                pending["pending_releases"] == [{"folder": "2017/10", "reason": contract.PENDING_REASON}] and pending["releases"] == [],
                "Pending release differs",
            )

        scenario("plan_builder_marks_release_without_hash_as_pending", pending_input)

        def rule() -> None:
            for folder in ("2017/10", "2018/07", "2018/10"):
                path = root / f"rule_{folder.replace('/', '_')}.zip"
                path.write_bytes(served[zip_url(folder)])
                require(contract.release_date_from_zip(path) == DATES[folder], f"Release date rule differs for {folder}")

        scenario("release_date_is_newest_member_modification_day", rule)

        run = root / "run"
        scenario("capture_without_storage", lambda: execute(plan, october, run, True, False, None, OUTPUTS, web, LIMITS))
        receipt_path = run / "batches" / october["id"] / "capture/receipt.json"
        scenario("offline_receipt_replay", lambda: verify_local(receipt_path))

        def receipt_names_sources() -> None:
            receipt = read_json(receipt_path)
            lineage = json.loads(receipt["lineage"]["extraction_or_query"])
            require(
                receipt["acquisition"]["requested_url"] == october["url"] and october["url"].startswith("https://data.nber.org/"), "Receipt lacks the NBER URL"
            )
            require(receipt["source"]["publisher"] == "Centers for Medicare & Medicaid Services", "Receipt lacks the CMS publisher")
            require(receipt["release"]["release_date"] == DATES["2018/10"], "Receipt release date differs from the rule")
            require(lineage["mode"] == "nber_hospital_compare" and lineage["gate_sha256"] == gate_sha and lineage["model_eligible"] is False, "Lineage differs")
            require(receipt["governance"]["access_class"] == "public" and receipt["governance"]["contains_pii"] is False, "Governance differs")
            require("mirror" in " ".join(receipt["governance"]["use_restrictions"]).lower(), "Mirror caveat missing")

        scenario("receipt_names_nber_url_and_cms_publisher", receipt_names_sources)
        scenario("capture_and_version_verified_storage", lambda: execute(plan, october, run, True, True, client, OUTPUTS, web, LIMITS))

        def mapped_only() -> None:
            keys = storage_keys(client)
            data = [k for k in keys if "/datasets/" in k and "/manifests/" not in k and "/audit/" not in k]
            names = {k.split("/datasets/")[1].split("/")[0] for k in data}
            require(
                names
                == {"cms_legacy_hcahps_hospital", "cms_legacy_timely_and_effective_care_hospital", "cms_legacy_healthcare_associated_infections_hospital"},
                f"Data datasets differ: {names}",
            )
            require(len(data) == 3, f"Unmapped member stored as data: {data}")
            require(all(f"release_date={DATES['2018/10']}" in k for k in data), "Release partition differs")
            references = [k for k in keys if "/references/" in k]
            require(
                any(k.endswith(GENERAL.replace(" ", "_")) for k in references) and any(k.endswith(FOOTNOTES.replace(" ", "_")) for k in references),
                "Unmapped members missing",
            )
            require(not any(k.endswith(".zip") for k in keys), "ZIP container uploaded")

        scenario("only_mapped_members_become_data_storage_entries", mapped_only)
        before, objects, requests = inventory(run), copy.deepcopy(client.objects), web.requests

        def repeat() -> None:
            execute(plan, october, run, True, True, client, OUTPUTS, web, LIMITS)
            require(before == inventory(run) and objects == client.objects and web.requests == requests, "Repeated effects")

        scenario("second_execute_writes_nothing_new", repeat)
        scenario("third_execute_writes_nothing_new", repeat)

        def replay_without_downloads() -> None:
            copied = root / "replay_copy"
            shutil.copytree(run, copied)
            shutil.rmtree(copied / "downloads")
            execute(plan, october, copied, False, True, client, OUTPUTS, Offline({}), LIMITS)

        scenario("replay_needs_no_download_folder", replay_without_downloads)
        scenario("replay_without_network", lambda: execute(plan, october, run, False, True, client, OUTPUTS, Offline({}), LIMITS))

        def unmapped_hai() -> None:
            target, s3 = root / "july", FakeS3()
            result = execute(plan, july, target, True, True, s3, OUTPUTS, web, LIMITS)
            hai = [k for k in s3.objects if HAI.replace(" ", "_") in k]
            require(
                result["status"] == "stored" and len(hai) == 1 and "/references/" in hai[0] and "/datasets/" not in hai[0],
                f"HAI of 2018/07 is not a reference: {hai}",
            )
            data = {k.split("/datasets/")[1].split("/")[0] for k in s3.objects if "/datasets/" in k and "/manifests/" not in k and "/audit/" not in k}
            require(data == {"cms_legacy_timely_and_effective_care_hospital"}, f"2018/07 data differs: {data}")

        scenario("unmapped_hai_of_2018_07_stays_reference", unmapped_hai)

        def hai_release() -> None:
            target, s3 = root / "hai", FakeS3()
            execute(plan, hai_only, target, True, True, s3, OUTPUTS, web, LIMITS)
            data = {k.split("/datasets/")[1].split("/")[0] for k in s3.objects if "/datasets/" in k and "/manifests/" not in k and "/audit/" not in k}
            require(data == {"cms_legacy_healthcare_associated_infections_hospital"}, f"2017/10 data differs: {data}")

        scenario("release_2017_10_maps_only_hai", hai_release)
        fetch_scenarios(scenario, plan, plan_path, october, root, served)
        gate_scenarios(scenario, plan, root, served)
        member_scenarios(scenario, plan, plan_path, october, root, served)
        corrupt_scenarios(scenario, receipt_path)
        storage_scenarios(scenario, plan, july, october, root, run, client, web)
        single_copy_scenarios(scenario, plan, october, root, web)
        no_flags_scenarios(scenario, plan, october, root, web)

        def unreviewed_current() -> None:
            with patch.object(contract, "closure_hashes", return_value={"changed": "0" * 64}):
                execute(plan, july, root / "unreviewed", True, True, client, OUTPUTS, web, LIMITS)

        scenario("unreviewed_current_code_before_any_effect", refusing(unreviewed_current, root / "unreviewed"), "Unreviewed NBER")

        def independent() -> None:
            broken = Web(dict(served))
            broken.bodies[zip_url("2018/07")] = make_zip("2018/07", (GENERAL,))
            results, held = run_all(plan, root / "all", True, True, FakeS3(), OUTPUTS, broken, LIMITS)
            require([h["folder"] for h in held] == ["2018/07"] and "differs from the plan" in held[0]["reason"], f"Held set differs: {held}")
            require(len(results) == 2 and all(r["status"] == "stored" for r in results), "Other releases did not complete")

        scenario("failing_release_held_others_stored", independent)
    return scenarios


def fetch_scenarios(scenario: Any, plan: dict, plan_path: Path, release: dict, root: Path, served: dict[str, bytes]) -> None:
    """Wrong or failed downloads are refused and never reach a snapshot."""
    url, good = release["url"], served[release["url"]]

    def refused(name: str, body: bytes | Response | Exception, error: str, edit: Any = None) -> None:
        web = Web(served)
        web.override[url] = Response(body, url) if isinstance(body, bytes) else body
        target = root / f"fetch_{name}"

        def action() -> None:
            with edited_plan(plan, plan_path, release["folder"], edit or (lambda r: None)) as (changed, edited):
                execute(changed, edited, target, True, True, FakeS3(), OUTPUTS, web, LIMITS)

        scenario(name, refusing(action, target), error)

    refused("hash_mismatch_refused", good + b"x", "hash differs from the plan")
    refused("size_mismatch_refused", good, "size differs from the plan", lambda r: r.__setitem__("bytes", r["bytes"] + 1))
    refused("redirect_to_other_host_refused", Response(good, "https://example.org/hospitals_compare_201810.zip"), "download failed")
    refused("html_page_refused", b"<!doctype html><html></html>", "download failed")
    refused("server_error_refused", Response(b"busy", url, status=503), "download failed")
    refused("truncated_download_refused", Response(good[:-10], url, headers={"Content-Length": str(len(good))}), "download failed")
    refused("connection_error_refused", OSError("reset"), "download failed")

    def oversized() -> None:
        web = Web(served)
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_STORED) as archive:
            archive.writestr(HAI, b"0" * (64 * 1024**2 + 1))
        web.override[url] = Response(buffer.getvalue(), url)
        execute(plan, release, root / "oversized", True, True, FakeS3(), OUTPUTS, web, None)

    scenario("default_limits_refuse_a_zip_over_64_mib", refusing(oversized, root / "oversized"), "download failed")

    def date_differs() -> None:
        with edited_plan(plan, plan_path, release["folder"], lambda r: r.__setitem__("release_date", "2018-09-26")) as (changed, edited):
            execute(changed, edited, root / "date_differs", True, True, FakeS3(), OUTPUTS, Web(served), LIMITS)

    scenario("release_date_not_matching_the_rule_refused", refusing(date_differs, root / "date_differs"), "release date differs")
    scenario("no_fetch_without_original", lambda: execute(plan, release, root / "no_fetch", False, False, None, OUTPUTS, Offline({}), LIMITS), "not captured")


def gate_scenarios(scenario: Any, plan: dict, root: Path, served: dict[str, bytes]) -> None:
    """The fidelity gate runs first; a failed gate stops the run before any release download."""
    gate_url = plan["fidelity_gate"]["url"]

    def failed_gate() -> None:
        web = Web(served)
        web.override[gate_url] = Response(served[gate_url] + b"y", gate_url)
        results, held = run_all(plan, root / "gate_failed", True, True, FakeS3(), OUTPUTS, web, LIMITS)
        require(results == [] and len(held) == len(plan["releases"]), f"Run continued after a failed gate: {held}")
        require(all("fidelity gate" in h["reason"] for h in held), f"Held reason differs: {held}")
        require(web.seen == [gate_url], f"A release was requested after a failed gate: {web.seen}")
        require(not (root / "gate_failed" / "batches").exists(), "A failed gate produced a snapshot")

    scenario("failed_fidelity_gate_stops_before_any_release_download", failed_gate)

    def gate_first_order() -> None:
        web = Web(served)
        results, held = run_all(plan, root / "gate_ok", True, False, None, OUTPUTS, web, LIMITS)
        require(not held and len(results) == len(plan["releases"]), f"Run differs: {held}")
        require(web.seen[0] == gate_url and set(web.seen[1:]) == {r["url"] for r in plan["releases"]}, f"Request order differs: {web.seen}")

    scenario("gate_downloaded_before_releases", gate_first_order)

    def gate_each_fetch_run() -> None:
        web = Web(served)
        for _ in range(2):
            run_all(plan, root / "gate_repeat", True, False, None, OUTPUTS, web, LIMITS)
        require(web.seen.count(gate_url) == 2, f"Gate not repeated on a fetch run: {web.seen}")

    scenario("gate_downloaded_again_on_every_fetch_run", gate_each_fetch_run)

    def gate_offline() -> None:
        results, held = run_all(plan, root / "gate_offline", False, False, None, OUTPUTS, Offline({}), LIMITS)
        require(results == [] and all("not captured" in h["reason"] for h in held), f"Offline run differs: {held}")

    scenario("offline_default_downloads_nothing", gate_offline)


def member_scenarios(scenario: Any, plan: dict, plan_path: Path, release: dict, root: Path, served: dict[str, bytes]) -> None:
    """A release that lacks a mapped member is refused whole; nothing is stored."""
    body = make_zip("2018/10", (HAI, TIMELY, GENERAL))

    def lacking() -> None:

        def edit(entry: dict) -> None:
            entry.update(expected_sha256=hashlib.sha256(body).hexdigest(), bytes=len(body))

        with edited_plan(plan, plan_path, release["folder"], edit) as (changed, edited):
            execute(changed, edited, root / "lacking", True, True, FakeS3(), OUTPUTS, Web(served | {release["url"]: body}), LIMITS)

    scenario("release_lacking_a_mapped_member_refused", refusing(lacking, root / "lacking"), "mapped member")


def corrupt_scenarios(scenario: Any, receipt_path: Path) -> None:
    """Replay refuses tampered artifacts, lineage and governance, even when hashes are resealed."""
    collection = receipt_path.parents[3]

    def corrupt(kind: str) -> None:
        target_root = collection.parent / f"corrupt_{kind}"
        shutil.copytree(collection, target_root)
        target = target_root / receipt_path.relative_to(collection).parent
        receipt = read_json(target / "receipt.json")
        lineage = json.loads(receipt["lineage"]["extraction_or_query"])
        archive = next((target / "raw").glob("*.zip"))
        if kind == "zip_changed":
            archive.write_bytes(archive.read_bytes() + b"z")
            receipt["artifacts"][0]["sha256"], receipt["artifacts"][0]["byte_count"] = fingerprint(archive)
        elif kind == "model_promotion":
            lineage["model_eligible"] = True
        elif kind == "unreviewed_code":
            lineage["code_sha256"] = {"bad": "0" * 64}
        elif kind == "wrong_plan":
            lineage["plan_sha256"] = "0" * 64
        elif kind == "pii_governance":
            receipt["governance"]["contains_pii"] = True
        elif kind == "other_host_url":
            receipt["acquisition"]["requested_url"] = "https://example.org/hospitals_compare_201810.zip"
        elif kind == "other_publisher":
            receipt["source"]["publisher"] = "National Bureau of Economic Research"
        receipt["lineage"]["extraction_or_query"] = json.dumps(lineage)
        (target / "receipt.json").write_bytes(encoded_json(receipt))
        verify_local(target / "receipt.json")

    for kind, error in [
        ("zip_changed", "differs from the plan"),
        ("model_promotion", "hold differs"),
        ("unreviewed_code", "Unreviewed NBER"),
        ("wrong_plan", "plan differs"),
        ("pii_governance", "governance differs"),
        ("other_host_url", "route differs"),
        ("other_publisher", "governance differs"),
    ]:
        scenario(kind, lambda kind=kind: corrupt(kind), error)


def storage_scenarios(scenario: Any, plan: dict, july: dict, october: dict, root: Path, run: Path, client: FakeS3, web: Web) -> None:
    """Interrupted uploads resume without duplicates; resealed storage evidence is refused."""

    def interrupted() -> None:
        partial, s3 = root / "partial", FakeS3()
        s3.corrupt = True
        with suppress(ValueError):
            execute(plan, july, partial, True, True, s3, OUTPUTS, web, LIMITS)
        require(len(s3.objects) == 1, "No interrupted upload")
        s3.corrupt = False
        execute(plan, july, partial, True, True, s3, OUTPUTS, web, LIMITS)
        first = copy.deepcopy(s3.objects)
        execute(plan, july, partial, True, True, s3, OUTPUTS, web, LIMITS)
        require(first == s3.objects, "Resume duplicated effects")

    scenario("resume_interrupted_storage", interrupted)

    def invalid_version() -> None:
        target = root / "bad_storage"
        shutil.copytree(run, target)
        branch = target / "batches" / october["id"]
        path = branch / "capture/s3_collections_reconciliation.json"
        rec = read_json(path)
        rec["objects"][0]["object"]["version_id"] = ""
        path.write_bytes(encoded_json(rec))
        done = read_json(branch / "completed.json")
        done["reconciliation_sha256"] = fingerprint(path)[0]
        (branch / "completed.json").write_bytes(encoded_json(done))
        execute(plan, october, target, False, True, client, OUTPUTS, Offline({}), LIMITS)

    scenario("resealed_missing_storage_version", invalid_version, "version missing")

    def reseal(kind: str) -> None:
        target = root / f"resealed_{kind}"
        shutil.copytree(run, target)
        branch = target / "batches" / october["id"]
        path = branch / "capture/s3_collections_reconciliation.json"
        rec = read_json(path)
        if kind == "member_dropped":
            chain = next(e["member_chain"] for e in rec["objects"] if e.get("member_chain") and e["role"] == "reference")
            rec["objects"] = [e for e in rec["objects"] if e.get("member_chain") != chain]
            path.write_bytes(encoded_json(rec))
        else:
            manifest_path = next((branch / "capture/s3_collections").glob("*/manifest.json"))
            manifest = read_json(manifest_path)
            del manifest["objects"][0]
            manifest_path.write_bytes(encoded_json(manifest))
        done = read_json(branch / "completed.json")
        done["reconciliation_sha256"] = fingerprint(path)[0]
        (branch / "completed.json").write_bytes(encoded_json(done))
        execute(plan, october, target, False, True, client, OUTPUTS, Offline({}), LIMITS)

    scenario("resealed_storage_missing_a_member", lambda: reseal("member_dropped"), "stored members differ")
    scenario("manifest_objects_changed", lambda: reseal("manifest_changed"), "manifest differs")
    scenario(
        "release_outside_plan_rejected",
        lambda: execute(plan, october | {"bytes": october["bytes"] + 1}, root / "outside", True, False, None, OUTPUTS, web, LIMITS),
        "plan or release",
    )


ENABLED = {"enabled": True, "objects": {}, "evidence_objects": {}, "bootstrap_sha256": "0" * 64}


def single_copy_client(objects: dict | None = None) -> FakeS3:
    """A fake store whose writer selects the single-copy policy."""
    client = FakeS3()
    client.storage_policy = {**ENABLED, "objects": objects or {}}
    return client


def reseal_reconciliation(target: Path, release: dict, edit: Any) -> None:
    """Edit the stored reconciliation and re-seal the completion record, so only the storage check can refuse the change."""
    branch = target / "batches" / release["id"]
    path = branch / "capture/s3_collections_reconciliation.json"
    reconciliation = read_json(path)
    edit(reconciliation)
    path.write_bytes(encoded_json(reconciliation))
    done = read_json(branch / "completed.json")
    done["reconciliation_sha256"] = fingerprint(path)[0]
    (branch / "completed.json").write_bytes(encoded_json(done))


def single_copy_scenarios(scenario: Any, plan: dict, october: dict, root: Path, web: Web) -> None:
    """Storage verification follows the policy the writer selected: collection keys when off, content keys or bootstrap keys when on."""

    def content_keys() -> None:
        target, s3 = root / "single_copy", single_copy_client()
        result = execute(plan, october, target, True, True, s3, OUTPUTS, web, LIMITS)
        require(result["status"] == "stored" and (target / "batches" / october["id"] / "completed.json").is_file(), "No completion record")
        keys = sorted(s3.objects)
        require(
            any(k.startswith("bronze_objects/sha256/") for k in keys) and any(k.startswith("bronze_evidence/sha256/") for k in keys), f"Keys differ: {keys}"
        )
        before, objects = inventory(target), copy.deepcopy(s3.objects)
        execute(plan, october, target, False, True, s3, OUTPUTS, Offline({}), LIMITS)
        require(before == inventory(target) and objects == s3.objects, "Replay wrote")

    scenario("single_copy_on_stores_content_keys_and_completes", content_keys)

    def bootstrap_setup(target: Path) -> tuple[FakeS3, str, str]:
        body = next(a for a in zip_members(served_zip(web, october)) if a[0] == TIMELY)[1]
        sha = hashlib.sha256(body).hexdigest()
        key = f"cms/hospitals/datasets/{contract.TIMELY_ID}/release_date=2018-09-27/{sha}/Timely_and_Effective_Care_-_Hospital.csv"
        record = {"bucket": SETTINGS["data_bucket_name"], "key": key, "version_id": "version_boot", "sha256": sha, "byte_count": len(body)}
        s3 = single_copy_client({sha: record})
        checksum = base64.b64encode(hashlib.sha256(body).digest()).decode()
        s3.objects[key] = {"VersionId": "version_boot", "ContentLength": len(body), "ChecksumSHA256": checksum, "ServerSideEncryption": "AES256", "body": body}
        return s3, sha, key

    def bootstrap_reuse() -> None:
        target = root / "bootstrap_reuse"
        s3, sha, key = bootstrap_setup(target)
        result = execute(plan, october, target, True, True, s3, OUTPUTS, web, LIMITS)
        reconciliation = read_json(target / "batches" / october["id"] / "capture/s3_collections_reconciliation.json")
        keys = [e["object"]["key"] for e in reconciliation["objects"] if e["object"]["sha256"] == sha]
        require(result["status"] == "stored" and keys == [key], f"Bootstrap object not reused: {keys}")
        require(f"bronze_objects/sha256/{sha}" not in s3.objects, "Reused content written again")
        execute(plan, october, target, False, True, s3, OUTPUTS, Offline({}), LIMITS)

    scenario("single_copy_on_bootstrap_reuse_completes", bootstrap_reuse)

    def bootstrap_other_version() -> None:
        target = root / "bootstrap_other_version"
        s3, sha, _ = bootstrap_setup(target)
        execute(plan, october, target, True, True, s3, OUTPUTS, web, LIMITS)

        def edit(reconciliation: dict) -> None:
            next(e["object"] for e in reconciliation["objects"] if e["object"]["sha256"] == sha)["version_id"] = "version_other"

        reseal_reconciliation(target, october, edit)
        execute(plan, october, target, False, True, s3, OUTPUTS, Offline({}), LIMITS)

    scenario("resealed_bootstrap_object_with_other_version_refused", bootstrap_other_version, "storage key differs")

    def resume_after_partial_upload() -> None:
        target, s3 = root / "single_copy_partial", single_copy_client()
        s3.corrupt = True
        with suppress(ValueError):
            execute(plan, october, target, True, True, s3, OUTPUTS, web, LIMITS)
        require(len(s3.objects) == 1 and not (target / "batches" / october["id"] / "completed.json").exists(), "No interrupted upload")
        s3.corrupt = False
        execute(plan, october, target, True, True, s3, OUTPUTS, web, LIMITS)
        first = copy.deepcopy(s3.objects)
        execute(plan, october, target, True, True, s3, OUTPUTS, web, LIMITS)
        require(first == s3.objects and (target / "batches" / october["id"] / "completed.json").is_file(), "Resume duplicated effects or did not complete")

    scenario("single_copy_on_retry_after_partial_upload_completes", resume_after_partial_upload)

    def policy_changed() -> None:
        target = root / "single_copy_policy_changed"
        execute(plan, october, target, True, True, single_copy_client(), OUTPUTS, web, LIMITS)
        execute(plan, october, target, False, True, FakeS3(), OUTPUTS, Offline({}), LIMITS)

    scenario("completion_stored_under_single_copy_refused_when_policy_is_off", policy_changed, "storage key differs")

    def collection_key_under_single_copy() -> None:
        target, s3 = root / "single_copy_forged", single_copy_client()
        execute(plan, october, target, True, True, s3, OUTPUTS, web, LIMITS)

        def edit(reconciliation: dict) -> None:
            entry = next(e for e in reconciliation["objects"] if e["role"] == "data")
            record = entry["object"]
            record["key"] = f"cms/hospitals/datasets/{entry['dataset_id']}/release_date=2018-09-27/{record['sha256']}/{record['key'].rsplit('/', 1)[-1]}"

        reseal_reconciliation(target, october, edit)
        execute(plan, october, target, False, True, s3, OUTPUTS, Offline({}), LIMITS)

    scenario("resealed_collection_key_refused_under_single_copy", collection_key_under_single_copy, "storage key differs")

    def foreign_content_key() -> None:
        target, s3 = root / "single_copy_foreign", single_copy_client()
        execute(plan, october, target, True, True, s3, OUTPUTS, web, LIMITS)

        def edit(reconciliation: dict) -> None:
            record = next(e["object"] for e in reconciliation["objects"] if e["role"] == "data")
            record["key"] = f"bronze_evidence/sha256/{record['sha256']}"

        reseal_reconciliation(target, october, edit)
        execute(plan, october, target, False, True, s3, OUTPUTS, Offline({}), LIMITS)

    scenario("resealed_evidence_key_for_data_refused_under_single_copy", foreign_content_key, "storage key differs")


def zip_members(body: bytes) -> list[tuple[str, bytes]]:
    """Every member name and its bytes from a synthetic release zip."""
    with zipfile.ZipFile(io.BytesIO(body)) as archive:
        return [(name, archive.read(name)) for name in archive.namelist()]


def served_zip(web: Web, release: dict) -> bytes:
    return web.bodies[release["url"]]


def tree(folder: Path) -> dict:
    """Every path under a folder, lock files included, with a file fingerprint or a folder marker."""
    return {str(p.relative_to(folder)): fingerprint(p) if p.is_file() else "folder" for p in sorted(folder.rglob("*"))}


def cached_downloads(plan: dict, folder: Path, web: Web) -> None:
    """Leave only downloads behind, as an interrupted fetch run does: no receipt, no batch folder and no lock."""
    check_gate(plan, folder, True, web, LIMITS)
    for release in plan["releases"]:
        fetch(
            release["url"],
            folder / "downloads" / release["id"],
            contract.zip_name(release["folder"]),
            release["expected_sha256"],
            release["bytes"],
            "release",
            True,
            web,
            LIMITS,
        )


def no_flags_scenarios(scenario: Any, plan: dict, october: dict, root: Path, web: Web) -> None:
    """A run with neither --fetch nor --execute only reads: it takes no lock and creates no capture, receipt or folder."""

    def plain_run(target: Path) -> tuple[list[dict], list[dict]]:
        with (
            patch.object(store_nber_hc, "load_configuration", lambda path: (SETTINGS.copy(), None)),
            patch.object(store_nber_hc, "load_policy", lambda: FakeS3().storage_policy, create=True),
        ):
            return run_all(plan, target, False, False, None, OUTPUTS, Offline({}), LIMITS)

    def cached() -> None:
        target = root / "no_flags_cached"
        cached_downloads(plan, target, web)
        before = tree(target)
        results, held = plain_run(target)
        require(tree(target) == before, f"No-flags run wrote files: {sorted(set(tree(target)) - set(before))}")
        require(not held and len(results) == len(plan["releases"]), f"No-flags run differs: {held}")
        require(all(r["status"] == "download_verified" for r in results), f"Status differs: {[r['status'] for r in results]}")

    scenario("no_flags_run_over_cached_downloads_writes_nothing", cached)

    def empty() -> None:
        target = root / "no_flags_empty"
        results, held = plain_run(target)
        require(not target.exists(), "No-flags run created its folder")
        require(results == [] and all("not captured" in h["reason"] for h in held), f"Held set differs: {held}")

    scenario("no_flags_run_with_nothing_cached_creates_no_folder", empty)

    def captured() -> None:
        target = root / "no_flags_captured"
        built = root / "no_flags_captured_build"
        run_all(plan, built, True, False, None, OUTPUTS, web, LIMITS)
        shutil.copytree(built, target, ignore=shutil.ignore_patterns(".collection.lock"))
        before = tree(target)
        results, held = plain_run(target)
        require(tree(target) == before, f"No-flags run wrote files: {sorted(set(tree(target)) - set(before))}")
        require(not held and all(r["status"] == "capture_ready" for r in results), f"No-flags run differs: {held}")

    scenario("no_flags_run_over_captured_receipts_writes_nothing", captured)

    def completed() -> None:
        target = root / "no_flags_completed"
        built = root / "no_flags_completed_build"
        run_all(plan, built, True, True, FakeS3(), OUTPUTS, web, LIMITS)
        shutil.copytree(built, target, ignore=shutil.ignore_patterns(".collection.lock"))
        before = tree(target)
        results, held = plain_run(target)
        require(tree(target) == before, f"No-flags run wrote files: {sorted(set(tree(target)) - set(before))}")
        require(not held and all(r["status"] == "stored" for r in results), f"No-flags run differs: {held}")

    scenario("no_flags_run_over_completed_releases_writes_nothing", completed)


def main() -> None:
    """Write one immutable artifact for every E2E invocation."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="nber_hc_e2e_") as directory:
        scenarios = exercise(Path(directory))
    passed = all(s["passed"] for s in scenarios)
    report = {
        "status": "passed" if passed else "failed",
        "feature": "NBER Hospital Compare mirror capture: original CMS zips, mapped members only, versioned S3 storage",
        "run_at_utc": datetime.now(UTC).isoformat(),
        "python": sys.version.split()[0],
        "code_sha256": closure_hashes("scripts.acquisition.run_nber_hc_e2e"),
        "dependency_sha256": hashlib.sha256(Path("uv.lock").read_bytes()).hexdigest(),
        "reproduce": f".venv/bin/python -m scripts.acquisition.run_nber_hc_e2e --output {args.output}",
        "boundary": "Synthetic CMS-named zips served by a fake opener through the real downloader; fake versioned S3; no live NBER, CMS or AWS.",
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
