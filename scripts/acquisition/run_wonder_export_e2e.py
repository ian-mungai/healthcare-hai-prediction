"""Repeatable synthetic CDC WONDER county-export capture-to-S3 verification, without live downloads or credentials."""

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
from scripts.acquisition import wonder_export_contract as contract
from scripts.acquisition.build_wonder_plan import build
from scripts.acquisition.hud_api_contract import STATE_FIPS
from scripts.acquisition.run_bls_api_e2e import inventory
from scripts.acquisition.run_hud_xlsx_e2e import download_metadata, set_origin
from scripts.acquisition.s3_store import encoded_json, fingerprint, write_once
from scripts.acquisition.source_registry import canonical_hash, read_json, require
from scripts.acquisition.store_wonder_export import execute, run_all, verify_local
from scripts.acquisition.tests.test_s3_store import OUTPUTS, FakeS3

# Synthetic exports are small; the real plan keeps the 3,100-county floor.
FLOOR = 100
EXPORTS = [
    ("D76", 2020, 2020, "pilot_d76.xls"),
    ("D158", 2024, 2024, "pilot_d158.xls"),
    ("D76", 1999, 2020, "full_d76.xls"),
    ("D158", 2018, 2024, "full_d158.xls"),
]
SESSION = "https://wonder.cdc.gov/controller/datarequest/{db};jsessionid=SYNTHETICSESSION0000"


def rows(database: str, years: range) -> list[list[str]]:
    """Generic county-year rows with every published flag: suppressed, unreliable, not available and missing."""
    out = []
    for year in years:
        label = f"{year} " if database == "D158" else str(year)
        for state in STATE_FIPS:
            for n in (1, 3):
                code = f"{state}{n:03d}"
                out.append(["", f"Synthetic {code}", code, label, str(year), "120", "10000", "1200.0", "990.1", "1409.9", "109.5"])
        out[-102][5:] = ["Suppressed", "800", "Suppressed", "Suppressed", "Suppressed", "Suppressed"]
        out[-101][7] = "Unreliable" if database == "D76" else "200.0"
        out[-100][6:] = ["Not Available"] * 5
        out[-99][5:] = ["Missing"] * 6
    return out


def export_text(
    database: str, first: int, last: int, table: list[list[str]] | None = None, parameters: dict | None = None, dataset: str | None = None
) -> bytes:
    """Serialize like WONDER: quoted tab-delimited rows, then a notes section with the query record."""
    table = rows(database, range(first, last + 1)) if table is None else table
    lines = ["\t".join(f'"{c}"' for c in contract.HEADER), *["\t".join(f'"{c}"' for c in r) for r in table]]
    params = {"Year/Month": "; ".join(str(y) for y in range(first, last + 1))} | contract.FIXED_PARAMETERS
    params = params | (parameters or {})
    title = dataset or contract.DATABASES[database]["title"]
    notes = ['"---"', f'"Dataset: {title}"', '"Query Parameters:"', *[f'"{k}: {v}"' for k, v in params.items() if v is not None], '"---"']
    notes += [
        '"Query Date: Jan 1, 2026 12:00:00 AM"',
        '"---"',
        '"Suggested Citation: Synthetic citation line one"',
        '"continued citation."',
        '"---"',
        "Caveats:",
        '"1. Synthetic caveat."',
    ]
    return ("\r\n".join(lines + notes) + "\r\n").encode()


def downloads_for(target: Path) -> dict:
    """Write the four synthetic exports with session-bearing WONDER origins; return a manifest like the real one."""
    target.mkdir(parents=True)
    records = []
    for database, first, last, name in EXPORTS:
        path = target / name
        path.write_bytes(export_text(database, first, last))
        set_origin(path, [SESSION.format(db=database), f"https://wonder.cdc.gov/controller/datarequest/{database}"])
        sha, size = fingerprint(path)
        records.append({"database": database, "years": [first, last], "filename": name, "bytes": size, "sha256": sha})
    return {"verified_downloads": records}


def exercise(root: Path) -> list[dict]:
    """Use production orchestration with synthetic downloads and versioned fake storage."""
    downloads = root / "downloads"
    manifest = downloads_for(downloads)
    plan = build(manifest, min_counties_per_year=FLOOR)
    plan_path, versions = root / "plan.json", root / "versions.json"
    write_once(plan_path, encoded_json(plan))
    write_once(plan_path.with_suffix(".lock.json"), encoded_json({"plan_sha256": canonical_hash(plan)}))
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

    batches = {b["file_name"]: b for b in plan["batches"]}
    batch = batches["pilot_d158.xls"]
    client = FakeS3()
    with patch.object(contract, "PLAN_PATH", plan_path), patch.object(contract, "VERSIONS_PATH", versions):
        scenario("locked_scope", contract.load_plan)
        scenario("plan_has_4_exports", lambda: require(len(plan["batches"]) == 4 and plan["min_counties_per_year"] == FLOOR, "Plan scope differs"))
        run = root / "run"
        scenario("capture_without_storage", lambda: execute(plan, batch, run, downloads, False, client, OUTPUTS))
        receipt_path = run / "batches" / batch["id"] / "capture/receipt.json"
        scenario("offline_receipt_replay", lambda: verify_local(receipt_path))

        def text_kept() -> None:
            csv = (receipt_path.parent / "derived/county_year.csv").read_bytes().decode()
            require(csv.splitlines()[0] == ",".join(contract.HEADER), "Derived header differs")
            require(",01001,2024 ,2024," in csv and "Suppressed" in csv and "Not Available" in csv and "Missing" in csv, "Published text or zeros lost")
            require(
                (receipt_path.parent / "raw" / "wonder_D158_2024_2024.txt").read_bytes() == (downloads / batch["file_name"]).read_bytes(), "Original changed"
            )

        scenario("published_text_zeros_and_original_kept", text_kept)

        def provenance() -> None:
            receipt = verify_local(receipt_path)
            require(receipt["acquisition"]["http_status"] is None and receipt["acquisition"]["request_method"] == "browser_export", "Provenance differs")
            period = receipt["measurement_periods"][0]
            require((period["start_date"], period["end_date"]) == ("2024-01-01", "2024-12-31"), "Year dates differ")
            require(all("jsessionid" not in p.read_text(errors="ignore") for p in run.rglob("*") if p.is_file()), "Session identifier persisted")
            proof = read_json(receipt_path.parent / "evidence/download_proof.json")
            require(proof["origin_urls"] == ["https://wonder.cdc.gov/controller/datarequest/D158"], "Origin not sanitized")
            require(proof["query"]["dataset"] == contract.DATABASES["D158"]["title"] and proof["query"]["query_date"], "Query record not kept")

        scenario("session_removed_query_record_kept", provenance)
        scenario("capture_and_version_verified_storage", lambda: execute(plan, batch, run, downloads, True, client, OUTPUTS))
        before, objects = inventory(run), copy.deepcopy(client.objects)

        def repeat() -> None:
            execute(plan, batch, run, downloads, True, client, OUTPUTS)
            require(before == inventory(run) and objects == client.objects, "Repeated effects")

        scenario("repeat_one_no_effects", repeat)
        scenario("repeat_two_no_effects", repeat)

        def downloads_not_needed() -> None:
            moved = root / "moved"
            moved.mkdir()
            execute(plan, batch, run, moved, True, client, OUTPUTS)
            require(before == inventory(run) and objects == client.objects, "Replay needed the Downloads copy")

        scenario("replay_without_downloads_copy", downloads_not_needed)

        def full_range() -> None:
            full = batches["full_d76.xls"]
            result = execute(plan, full, root / "full", downloads, True, client, OUTPUTS)
            require(result["status"] == "stored" and result["rows"] == 22 * 2 * len(STATE_FIPS), "Full range rows differ")

        scenario("full_range_export_stored", full_range)
        malformed_scenarios(scenario, plan, batches)
        staging_scenarios(scenario, plan, batches, root, client)
        corrupt_scenarios(scenario, receipt_path)
        storage_scenarios(scenario, plan, batches, root, run, client, downloads)

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
                execute(plan, batches["pilot_d76.xls"], root / "unreviewed", downloads, True, client, OUTPUTS)

        scenario("unreviewed_current_code_before_effects", unreviewed_current, "Unreviewed WONDER")
        scenario("unreviewed_run_wrote_nothing", lambda: require(not (root / "unreviewed").exists(), "Unreviewed code wrote files"))

        def independent() -> None:
            partial = root / "partial_downloads"
            require(downloads_for(partial) == manifest, "Synthetic downloads are not deterministic")
            (partial / "full_d158.xls").unlink()
            s3 = FakeS3()
            results, held = run_all(plan, root / "all", partial, True, s3, OUTPUTS)
            require([h["file_name"] for h in held] == ["full_d158.xls"] and "missing" in held[0]["reason"], f"Held set differs: {held}")
            require(len(results) == 3 and all(r["status"] == "stored" for r in results), "Other exports did not complete")
            again, held_again = run_all(plan, root / "all", partial, True, s3, OUTPUTS)
            require(again == results and len(held_again) == 1, "Rerun differs")

        scenario("failing_export_held_others_stored", independent)
    return scenarios


def malformed_scenarios(scenario: Any, plan: dict, batches: dict) -> None:
    """Every content defect named in the failure modes stops that export."""
    batch = batches["pilot_d76.xls"]

    def check(raw: bytes) -> None:
        contract.validate_export(raw, batch, plan)

    def mutated(change: Any) -> bytes:
        table = rows("D76", range(2020, 2021))
        change(table)
        return export_text("D76", 2020, 2020, table)

    cases: list[tuple[str, bytes, str]] = [
        ("wrong_dataset", export_text("D76", 2020, 2020, dataset="Multiple Cause of Death, 1999-2020"), "dataset differs"),
        ("cause_filter", export_text("D76", 2020, 2020, parameters={"UCD - ICD-10 Codes": "A00-B99"}), "query parameters differ"),
        ("totals_enabled", export_text("D76", 2020, 2020, parameters={"Show Totals": "True"}), "query parameters differ"),
        ("wrong_year_listed", export_text("D76", 2020, 2020, parameters={"Year/Month": "2019"}), "query years differ"),
        ("missing_notes", b'"Notes"\t"County"\r\n', "query record missing"),
        ("repeated_county_year", mutated(lambda t: t.append(list(t[5]))), "Repeated county-year"),
        ("missing_state", mutated(lambda t: t.__delitem__(slice(100, 102))), "state coverage"),
        ("below_floor", mutated(lambda t: t.__delitem__(slice(2, 10))), "county count"),
        ("small_count", mutated(lambda t: t[5].__setitem__(5, "7")), "count of 1-9"),
        ("unknown_token", mutated(lambda t: t[5].__setitem__(6, "n/a")), "value invalid"),
        ("unreliable_deaths", mutated(lambda t: t[5].__setitem__(5, "Unreliable")), "value invalid"),
        ("totals_row", mutated(lambda t: t[5].__setitem__(0, "Total")), "totals or notes row"),
        ("year_code_mismatch", mutated(lambda t: t[5].__setitem__(3, "2019")), "year differs"),
        ("short_row", mutated(lambda t: t[5].pop()), "row shape"),
        ("non_numeric_code", mutated(lambda t: t[5].__setitem__(2, "0100A")), "county code invalid"),
    ]
    for name, raw, error in cases:
        scenario(name, lambda raw=raw: check(raw), error)
    extra = export_text("D76", 2020, 2020).replace(b'"Crude Rate Standard Error"', b'"Crude Rate Standard Error"\t"Age Adjusted Rate"', 1)
    scenario("extra_column", lambda: check(extra), "header differs")


def staging_scenarios(scenario: Any, plan: dict, batches: dict, root: Path, client: FakeS3) -> None:
    """The copied original must be the exact recorded WONDER export for its database."""
    batch = batches["full_d158.xls"]

    def stage(kind: str) -> None:
        folder = root / f"stage_{kind}"
        folder.mkdir()
        target = folder / batch["file_name"]
        target.write_bytes(export_text("D158", 2018, 2023) if kind == "other_file" else export_text("D158", 2018, 2024))
        if kind == "other_database":
            set_origin(target, ["https://wonder.cdc.gov/controller/datarequest/D76"])
        elif kind == "other_site":
            set_origin(target, ["https://example.org/controller/datarequest/D158"])
        elif kind != "missing_origin":
            set_origin(target, [SESSION.format(db="D158")])
        if kind == "symlink":
            real = folder / "real.xls"
            target.rename(real)
            target.symlink_to(real)
        execute(plan, batch, root / f"run_{kind}", folder, False, client, OUTPUTS)

    for kind, error in [
        ("other_database", "origin differs"),
        ("other_site", "origin differs"),
        ("missing_origin", "origin missing"),
        ("other_file", "differs from the recorded download"),
        ("symlink", "missing or not a regular file"),
    ]:
        scenario(f"staging_{kind}_rejected", lambda kind=kind: stage(kind), error)
        scenario(f"staging_{kind}_wrote_nothing", lambda kind=kind: require(not (root / f"run_{kind}").exists(), "Rejected download left files"))


def corrupt_scenarios(scenario: Any, receipt_path: Path) -> None:
    """Replay refuses tampered artifacts, lineage and governance, even when hashes are resealed."""

    def corrupt(kind: str) -> None:
        target = receipt_path.parent.parent / f"corrupt_{kind}"
        shutil.copytree(receipt_path.parent, target)
        receipt = read_json(target / "receipt.json")
        lineage = json.loads(receipt["lineage"]["extraction_or_query"])

        def reseal(relative: str) -> None:
            for item in receipt["artifacts"]:
                if item["storage_path"] == relative:
                    item["sha256"], item["byte_count"] = fingerprint(target / relative)

        if kind == "raw_corruption":
            raw = next((target / "raw").iterdir())
            raw.write_bytes(raw.read_bytes() + b"x")
        elif kind == "resealed_raw_substitution":
            raw = next((target / "raw").iterdir())
            raw.write_bytes(export_text("D158", 2024, 2024, parameters={"Show Zero Values": "False"}))
            reseal(f"raw/{raw.name}")
        elif kind == "resealed_csv_corruption":
            path = target / "derived/county_year.csv"
            path.write_bytes(path.read_bytes() + b"extra\n")
            reseal("derived/county_year.csv")
        elif kind == "resealed_session_in_proof":
            path = target / "evidence/download_proof.json"
            proof = read_json(path)
            proof["origin_urls"] = [SESSION.format(db="D158")]
            path.write_bytes(encoded_json(proof))
            reseal("evidence/download_proof.json")
        elif kind == "model_promotion":
            lineage["model_eligible"] = True
        elif kind == "unreviewed_code":
            lineage["code_sha256"] = {"bad": "0" * 64}
        elif kind == "wrong_plan":
            lineage["plan_sha256"] = "0" * 64
        elif kind == "terms_binding":
            lineage["terms_sha256"] = "0" * 64
        elif kind == "restrictions_removed":
            receipt["governance"]["use_restrictions"] = receipt["governance"]["use_restrictions"][1:]
        receipt["lineage"]["extraction_or_query"] = json.dumps(lineage)
        (target / "receipt.json").write_bytes(encoded_json(receipt))
        verify_local(target / "receipt.json")

    for kind, error in [
        ("raw_corruption", "Artifact hash"),
        ("resealed_raw_substitution", "raw hash differs"),
        ("resealed_csv_corruption", "CSV differs"),
        ("resealed_session_in_proof", "proof differs"),
        ("model_promotion", "hold differs"),
        ("unreviewed_code", "Unreviewed WONDER"),
        ("wrong_plan", "plan differs"),
        ("terms_binding", "terms binding"),
        ("restrictions_removed", "governance differs"),
    ]:
        scenario(kind, lambda kind=kind: corrupt(kind), error)


def storage_scenarios(scenario: Any, plan: dict, batches: dict, root: Path, run: Path, client: FakeS3, downloads: Path) -> None:
    """Interrupted uploads resume without duplicates; resealed storage evidence is refused."""
    batch = batches["pilot_d76.xls"]

    def interrupted() -> None:
        partial, s3 = root / "partial", FakeS3()
        s3.corrupt = True
        with suppress(ValueError):
            execute(plan, batch, partial, downloads, True, s3, OUTPUTS)
        require(len(s3.objects) == 1, "No interrupted upload")
        s3.corrupt = False
        execute(plan, batch, partial, downloads, True, s3, OUTPUTS)
        first = copy.deepcopy(s3.objects)
        execute(plan, batch, partial, downloads, True, s3, OUTPUTS)
        require(first == s3.objects, "Resume duplicated effects")

    scenario("resume_interrupted_storage", interrupted)

    def invalid_version() -> None:
        stored = batches["pilot_d158.xls"]
        target = root / "bad_storage"
        shutil.copytree(run, target)
        branch = target / "batches" / stored["id"]
        path = branch / "capture/s3_collections_reconciliation.json"
        rec = read_json(path)
        rec["objects"][0]["object"]["version_id"] = ""
        path.write_bytes(encoded_json(rec))
        done = read_json(branch / "completed.json")
        done["reconciliation_sha256"] = fingerprint(path)[0]
        (branch / "completed.json").write_bytes(encoded_json(done))
        execute(plan, stored, target, downloads, True, client, OUTPUTS)

    scenario("resealed_missing_storage_version", invalid_version, "version missing")
    scenario(
        "batch_outside_plan_rejected",
        lambda: execute(plan, batch | {"years": [1998, 2020]}, root / "outside", downloads, False, client, OUTPUTS),
        "plan or batch",
    )


def main() -> None:
    """Write one immutable artifact for every E2E invocation."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="wonder_export_e2e_") as directory, download_metadata():
        scenarios = exercise(Path(directory))
    passed = all(s["passed"] for s in scenarios)
    report = {
        "status": "passed" if passed else "failed",
        "feature": "CDC WONDER county-year export capture (browser exports) through versioned S3 storage",
        "run_at_utc": datetime.now(UTC).isoformat(),
        "python": sys.version.split()[0],
        "code_sha256": bls_contract.code_hashes(),
        "dependency_sha256": bls_contract.digest(Path("requirements.txt").read_bytes()),
        "reproduce": f".venv/bin/python -m scripts.acquisition.run_wonder_export_e2e --output {args.output}",
        "boundary": "Synthetic exports (102 counties a year, floor 100, not 3,100) with real macOS origins; fake versioned S3; no live WONDER or AWS access.",
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
