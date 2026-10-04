"""Repeatable synthetic verification of reference documents saved in a browser, from Downloads to versioned S3, without live downloads or credentials."""

import argparse
import copy
import json
import shutil
import sys
import tempfile
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import patch

from scripts.acquisition import bls_api_contract as bls_contract
from scripts.acquisition import reference_download_contract as contract
from scripts.acquisition.data_paths import current
from scripts.acquisition.run_bls_api_e2e import inventory
from scripts.acquisition.run_hud_xlsx_e2e import download_metadata, set_origin
from scripts.acquisition.s3_store import TERMS_ACCEPTANCE_PATH, encoded_json, fingerprint, write_once
from scripts.acquisition.source_registry import canonical_hash, read_json, require
from scripts.acquisition.store_reference_download import execute, run_all, verify_local
from scripts.acquisition.tests.test_s3_store import OUTPUTS, FakeS3

# Synthetic stand-ins for the three kinds of document: a text definitions file, a PDF and a held source's page.
REQUESTS = [
    {
        "source_id": "BLS",
        "role": "dictionary",
        "title": "LAUS series, area and measure definitions",
        "publisher": "U.S. Bureau of Labor Statistics",
        "origin_url": "https://download.bls.gov/pub/time.series/la/la.txt",
        "file_name": "la.txt",
        "media_type": "text/plain",
    },
    {
        "source_id": "SVI",
        "role": "methodology",
        "title": "CDC/ATSDR SVI 2020 documentation",
        "publisher": "Centers for Disease Control and Prevention, ATSDR",
        "origin_url": "https://www.atsdr.cdc.gov/placeandhealth/svi/documentation/pdf/SVI2020Documentation_08.05.22.pdf",
        "file_name": "SVI2020Documentation_08.05.22.pdf",
        "media_type": "application/pdf",
    },
    {
        "source_id": "HUD",
        "role": "dictionary",
        "title": "HUD USPS ZIP code crosswalk documentation",
        "publisher": "U.S. Department of Housing and Urban Development",
        "origin_url": "https://www.huduser.gov/portal/datasets/usps_crosswalk.html",
        "file_name": "usps_crosswalk.html",
        "media_type": "text/html",
        "release_binding": "terms",
    },
]
BODIES = {
    "la.txt": b"LAUS series identifiers\nla.area\tarea_code\tarea_text\nla.measure\tmeasure_code\tmeasure_text\n",
    "SVI2020Documentation_08.05.22.pdf": b"%PDF-1.4\n% synthetic SVI documentation\n%%EOF\n",
    "usps_crosswalk.html": b"<html><body><h1>USPS ZIP Code Crosswalk Files</h1><p>RES_RATIO: share of residential addresses</p></body></html>\n",
}
REFERRER = "https://www.example.org/search?q=definitions"


def downloads_for(target: Path, requests: list[dict] = REQUESTS) -> None:
    """Write each synthetic document with the origin a browser records: the file URL, then the referring page."""
    target.mkdir(parents=True)
    for request in requests:
        path = target / request["file_name"]
        path.write_bytes(BODIES[request["file_name"]])
        set_origin(path, [request["origin_url"], REFERRER])


def exercise(root: Path) -> list[dict]:
    """Use production orchestration with synthetic downloads and versioned fake storage."""
    downloads = root / "downloads"
    downloads_for(downloads)
    plan_path, versions = root / "plan.json", root / "versions.json"
    write_once(versions, encoded_json({"versions": [{"code_sha256": bls_contract.code_hashes()}]}))
    scenarios: list[dict] = []

    def scenario(name: str, action: Any, error: str | None = None) -> None:
        try:
            action()
            scenarios.append({"name": name, "passed": error is None, "observed": "success", "expected": error or "success"})
        except (ValueError, OSError, KeyError, TypeError, StopIteration, IndexError) as failure:
            scenarios.append(
                {"name": name, "passed": error is not None and error in str(failure), "observed": str(failure)[:200], "expected": error or "success"}
            )

    with patch.object(contract, "PLAN_PATH", plan_path), patch.object(contract, "VERSIONS_PATH", versions):
        plan = contract.build_plan(REQUESTS, downloads)
        write_once(plan_path, encoded_json(plan))
        write_once(plan_path.with_suffix(".lock.json"), encoded_json({"plan_sha256": canonical_hash(plan)}))
        entries = {e["file_name"]: e for e in plan["files"]}
        entry = entries["la.txt"]
        client = FakeS3()
        scenario("locked_scope", contract.load_plan)
        scenario("plan_binds_hash_and_size", lambda: require(fingerprint(downloads / "la.txt") == (entry["sha256"], entry["bytes"]), "Plan binding differs"))
        planning_scenarios(scenario, downloads)
        run = root / "run"
        scenario("capture_without_storage", lambda: execute(plan, entry, run, downloads, False, client, OUTPUTS))
        receipt_path = run / "files" / entry["id"] / "capture/receipt.json"
        scenario("offline_receipt_replay", lambda: verify_local(receipt_path))

        def provenance() -> None:
            receipt = verify_local(receipt_path)
            acquisition = receipt["acquisition"]
            require(acquisition["http_status"] is None and acquisition["request_method"] == "manual_download", "Browser download given an HTTP status")
            require(acquisition["transport_mode"] == "reference_document" and acquisition["requested_url"] == entry["origin_url"], "Route differs")
            proof = read_json(receipt_path.parent / "evidence/download_proof.json")
            require(proof["origin_url"] == entry["origin_url"] and proof["created_at_utc"] == acquisition["retrieved_at_utc"], "Proof differs")
            require(all(REFERRER not in p.read_text(errors="ignore") for p in run.rglob("*") if p.is_file()), "Referring page persisted")
            require({a["role"] for a in receipt["artifacts"]} == {"dictionary", "export_receipt"}, "Roles differ")
            require((receipt_path.parent / "raw/la.txt").read_bytes() == BODIES["la.txt"], "Original changed")

        scenario("no_http_status_planned_origin_only_original_kept", provenance)
        scenario("capture_and_version_verified_storage", lambda: execute(plan, entry, run, downloads, True, client, OUTPUTS))

        def under_references() -> None:
            keys = list(client.objects)
            require(bool(keys) and all("/references/" in k or "/manifests/" in k for k in keys), f"Stored outside references: {keys}")

        scenario("stored_under_references_only", under_references)
        before, objects = inventory(run), copy.deepcopy(client.objects)

        def repeat() -> None:
            execute(plan, entry, run, downloads, True, client, OUTPUTS)
            require(before == inventory(run) and objects == client.objects, "Repeated effects")

        scenario("repeat_one_no_effects", repeat)
        scenario("repeat_two_no_effects", repeat)

        def downloads_not_needed() -> None:
            moved = root / "moved"
            moved.mkdir()
            execute(plan, entry, run, moved, True, client, OUTPUTS)
            require(before == inventory(run) and objects == client.objects, "Replay needed the Downloads copy")

        scenario("replay_without_downloads_copy", downloads_not_needed)

        def held_with_terms() -> None:
            held = entries["usps_crosswalk.html"]
            result = execute(plan, held, root / "held", downloads, True, FakeS3(), OUTPUTS)
            receipt = read_json(Path(current(result["receipt_path"])))
            lineage = json.loads(receipt["lineage"]["extraction_or_query"])
            require(
                result["status"] == "stored" and receipt["acquisition"]["preferred_route"] == "documentation_only", "Held reference not stored as documentation"
            )
            require(lineage["terms_sha256"] == contract.digest(TERMS_ACCEPTANCE_PATH.read_bytes()), "Terms binding missing")

        scenario("held_reference_stored_with_terms_binding", held_with_terms)
        scenario(
            "unheld_receipt_has_no_binding",
            lambda: require("terms_sha256" not in json.loads(verify_local(receipt_path)["lineage"]["extraction_or_query"]), "Binding leaked"),
        )
        staging_scenarios(scenario, plan, entries, root, client)
        storage_scenarios(scenario, plan, entries, root, run, downloads)

        def tampered_plan() -> None:
            original = plan_path.read_bytes()
            try:
                plan_path.write_bytes(encoded_json(plan | {"model_eligible": True}))
                contract.load_plan()
            finally:
                plan_path.write_bytes(original)

        scenario("plan_tampering_rejected", tampered_plan, "plan lock differs")

        def unreviewed_current() -> None:
            with patch.object(contract, "code_hashes", return_value={"changed": "0" * 64}):
                execute(plan, entries["SVI2020Documentation_08.05.22.pdf"], root / "unreviewed", downloads, True, client, OUTPUTS)

        scenario("unreviewed_current_code_before_effects", unreviewed_current, "Unreviewed reference download")
        scenario("unreviewed_run_wrote_nothing", lambda: require(not (root / "unreviewed" / "files").exists(), "Unreviewed code wrote files"))

        def independent() -> None:
            partial = root / "partial_downloads"
            downloads_for(partial)
            (partial / "SVI2020Documentation_08.05.22.pdf").unlink()
            s3 = FakeS3()
            results, held = run_all(plan, root / "all", partial, True, s3, OUTPUTS)
            require([h["file_name"] for h in held] == ["SVI2020Documentation_08.05.22.pdf"] and "missing" in held[0]["reason"], f"Held set differs: {held}")
            require(len(results) == 2 and all(r["status"] == "stored" for r in results), "Other files did not complete")
            again, held_again = run_all(plan, root / "all", partial, True, s3, OUTPUTS)
            require(again == results and len(held_again) == 1, "Rerun differs")

        scenario("failing_file_held_others_stored", independent)
    return scenarios


def planning_scenarios(scenario: Any, downloads: Path) -> None:
    """Plans refuse data roles, unknown or excluded sources, wrong origins and bindings that do not fit the hold."""
    bls, hud = REQUESTS[0], REQUESTS[2]
    unbound = {k: v for k, v in hud.items() if k != "release_binding"}
    scenario("data_role_rejected", lambda: contract.build_plan([bls | {"role": "data"}], downloads), "Reference plan entry invalid")
    scenario("plain_http_origin_rejected", lambda: contract.build_plan([bls | {"origin_url": "http://download.bls.gov/x.txt"}], downloads), "entry invalid")
    scenario("unsafe_file_name_rejected", lambda: contract.build_plan([bls | {"file_name": "../la.txt"}], downloads), "entry invalid")
    scenario("unknown_source_rejected", lambda: contract.build_plan([bls | {"source_id": "NOT_A_SOURCE"}], downloads), "Reference source unknown")
    scenario("held_source_without_binding_rejected", lambda: contract.build_plan([unbound], downloads), "release binding differs")
    scenario("binding_on_unheld_source_rejected", lambda: contract.build_plan([bls | {"release_binding": "terms"}], downloads), "release binding differs")
    scenario("unknown_binding_rejected", lambda: contract.build_plan([hud | {"release_binding": "other"}], downloads), "release binding differs")
    scenario("origin_differs_at_planning", lambda: contract.build_plan([bls | {"origin_url": "https://www.bls.gov/la.txt"}], downloads), "origin differs")
    scenario("duplicate_file_rejected", lambda: contract.build_plan([bls, bls], downloads), "entry invalid")
    scenario("unknown_field_rejected", lambda: contract.build_plan([bls | {"http_status": 200}], downloads), "entry invalid")


def staging_scenarios(scenario: Any, plan: dict, entries: dict, root: Path, client: FakeS3) -> None:
    """A Downloads copy with no origin, another origin or other bytes is held before any write."""
    entry = entries["SVI2020Documentation_08.05.22.pdf"]

    def prepared(name: str, body: bytes, origins: list[str] | None) -> Path:
        folder = root / name
        folder.mkdir()
        path = folder / entry["file_name"]
        path.write_bytes(body)
        if origins is not None:
            set_origin(path, origins)
        return folder

    body = BODIES[entry["file_name"]]
    for name, folder, error in (
        ("origin_missing_refused", prepared("no_origin", body, None), "origin missing"),
        ("origin_differs_refused", prepared("other_origin", body, ["https://mirror.example.org/svi.pdf"]), "origin differs"),
        ("changed_bytes_refused", prepared("changed", body + b"% edited\n", [entry["origin_url"]]), "differs from the plan"),
    ):
        target = root / f"run_{name}"
        scenario(name, lambda f=folder, t=target: execute(plan, entry, t, f, True, client, OUTPUTS), error)
        scenario(f"{name}_wrote_nothing", lambda t=target: require(not (t / "files").exists() or not any((t / "files").rglob("*.json")), "Wrote files"))
    scenario(
        "entry_outside_plan_rejected",
        lambda: execute(plan, entry | {"role": "layout"}, root / "outside", root / "downloads", False, client, OUTPUTS),
        "plan or file",
    )


def storage_scenarios(scenario: Any, plan: dict, entries: dict, root: Path, run: Path, downloads: Path) -> None:
    """Interrupted uploads resume without duplicates; edited staged copies and resealed evidence are refused."""
    entry = entries["la.txt"]

    def interrupted() -> None:
        partial, s3 = root / "interrupted", FakeS3()
        s3.corrupt = True
        with suppress(ValueError):
            execute(plan, entry, partial, downloads, True, s3, OUTPUTS)
        require(len(s3.objects) == 1, "No interrupted upload")
        s3.corrupt = False
        execute(plan, entry, partial, downloads, True, s3, OUTPUTS)
        first = copy.deepcopy(s3.objects)
        execute(plan, entry, partial, downloads, True, s3, OUTPUTS)
        require(first == s3.objects, "Resume duplicated effects")

    scenario("resume_interrupted_storage", interrupted)

    def edited_staged_copy() -> None:
        target = root / "edited"
        shutil.copytree(run, target)
        raw = target / "files" / entry["id"] / "capture/raw/la.txt"
        raw.chmod(0o644)
        raw.write_bytes(b"edited\n")
        verify_local(target / "files" / entry["id"] / "capture/receipt.json")

    scenario("edited_staged_copy_refused", edited_staged_copy, "differ")

    def invalid_version() -> None:
        target = root / "bad_storage"
        shutil.copytree(run, target)
        branch = target / "files" / entry["id"]
        path = branch / "capture/s3_collections_reconciliation.json"
        record = read_json(path)
        record["objects"][0]["object"]["version_id"] = ""
        path.chmod(0o644)
        path.write_bytes(encoded_json(record))
        done = read_json(branch / "completed.json")
        done["reconciliation_sha256"] = fingerprint(path)[0]
        (branch / "completed.json").chmod(0o644)
        (branch / "completed.json").write_bytes(encoded_json(done))
        execute(plan, entry, target, downloads, True, FakeS3(), OUTPUTS)

    scenario("resealed_missing_storage_version", invalid_version, "version missing")


def main() -> None:
    """Write one immutable artifact for every E2E invocation."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="reference_download_e2e_") as directory, download_metadata():
        scenarios = exercise(Path(directory))
    passed = all(s["passed"] for s in scenarios)
    report = {
        "status": "passed" if passed else "failed",
        "feature": "Reference documents saved in a browser (origin, hash and plan bound) through versioned S3 storage",
        "run_at_utc": datetime.now(UTC).isoformat(),
        "python": sys.version.split()[0],
        "code_sha256": bls_contract.code_hashes(),
        "dependency_sha256": bls_contract.digest(Path("requirements.txt").read_bytes()),
        "reproduce": f".venv/bin/python -m scripts.acquisition.run_reference_download_e2e --output {args.output}",
        "boundary": "Synthetic documents with real macOS origins; the real registry and terms record; fake versioned S3; no live publisher or AWS access.",
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
