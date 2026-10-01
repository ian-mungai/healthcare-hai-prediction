"""Repeatable synthetic HUD crosswalk-workbook capture-to-S3 verification, without live downloads or credentials."""

import argparse
import copy
import io
import json
import plistlib
import shutil
import sys
import tempfile
import zipfile
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import patch

from scripts.acquisition import hud_api_contract as api_contract
from scripts.acquisition import hud_xlsx_contract as contract
from scripts.acquisition.build_hud_xlsx_plan import build, build_repeat_decision
from scripts.acquisition.process import CompletedProcess, run_command
from scripts.acquisition.run_bls_api_e2e import inventory
from scripts.acquisition.s3_store import encoded_json, fingerprint, write_once
from scripts.acquisition.source_registry import canonical_hash, read_json, require
from scripts.acquisition.store_hud_xlsx import execute, run_all, verify_local
from scripts.acquisition.tests.test_s3_store import OUTPUTS, FakeS3

MAIN = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
SMALL = "7.2E-5"
LARGE = "0.999928"
# The synthetic 2014 Q2 repeats three rows exactly, like the published quarter repeats 49,140.
REPEATS = 3
ORIGIN_ATTRIBUTE = "com.apple.metadata:kMDItemWhereFroms"
# macOS records a browser download's origin and creation time in file metadata. Elsewhere
# (Linux CI) a synthetic record answers those two operating-system reads, and the real plist
# parsing and origin checks still run. On macOS the real xattr and birth time are always used.
SYNTHETIC_METADATA = sys.platform != "darwin"
SYNTHETIC_ORIGINS: dict[Path, str] = {}


def rows(year: int, quarter: int) -> list[list[str]]:
    """Generic crosswalk-shaped rows: one ZIP per state, a split ZIP with scientific-notation text and a business-only ZIP."""
    body = [[f"{i:05d}", f"{state}001", "1", "1", "0", "1"] for i, state in enumerate(api_contract.STATE_FIPS, 1)]
    body[0][2:] = [SMALL, "0", "0", SMALL]
    body.append([body[0][0], "01003", LARGE, "1", "0", LARGE])
    body.append(["99999", "02013", "0", "1", "0", "1"])
    body.append([f"{year % 100:03d}{quarter:02d}", "72001", "1", "0", "0", "1"])
    return [list(contract.HEADER), *body]


def cell(ref: str, value: str, kind: str) -> str:
    """One worksheet cell in the publisher's observed style: inline text or a plain number."""
    if kind == "text":
        return f'<c r="{ref}" s="0" t="inlineStr"><is><t>{value}</t></is></c>'
    if kind == "shared":
        return f'<c r="{ref}" t="s"><v>0</v></c>'
    if kind == "formula":
        return f'<c r="{ref}" s="2"><f>1/1</f><v>{value}</v></c>'
    return f'<c r="{ref}" s="2"><v>{value}</v></c>'


def sheet(table: list[list[str]], kinds: dict[tuple[int, int], str] | None = None, prolog: str = "") -> bytes:
    """Serialize rows the way the published workbooks do: header and codes as inline text, ratios as numbers."""
    kinds = kinds or {}
    out = []
    for r, values in enumerate(table, 1):
        cells = []
        for c, value in enumerate(values):
            kind = kinds.get((r, c), "text" if r == 1 or c < 2 else "number")
            cells.append(cell(f"{'ABCDEFGH'[c]}{r}", value, kind))
        out.append(f'<row r="{r}" spans="1:{len(values)}">{"".join(cells)}</row>')
    return (f'<?xml version="1.0" encoding="UTF-8"?>{prolog}<worksheet xmlns="{MAIN}"><sheetData>{"".join(out)}</sheetData></worksheet>').encode()


def workbook(table: list[list[str]], *, kinds: dict | None = None, prolog: str = "", sheets: int = 1, extra: dict[str, bytes] | None = None) -> bytes:
    """A synthetic package with the same member set as the published workbooks."""
    listed = "".join(f'<sheet name="Sheet{i}" sheetId="{i}" r:id="rIdSheet{i}"/>' for i in range(1, sheets + 1))
    members = {
        "[Content_Types].xml": b'<?xml version="1.0"?><Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"/>',
        "xl/workbook.xml": f'<?xml version="1.0"?><workbook xmlns="{MAIN}" xmlns:r="r"><sheets>{listed}</sheets></workbook>'.encode(),
        "xl/styles.xml": f'<?xml version="1.0"?><styleSheet xmlns="{MAIN}"/>'.encode(),
        "docProps/app.xml": b'<?xml version="1.0"?><Properties/>',
        "docProps/core.xml": b'<?xml version="1.0"?><coreProperties/>',
        "xl/comments1.xml": f'<?xml version="1.0"?><comments xmlns="{MAIN}"/>'.encode(),
        "xl/drawings/vmlDrawing1.vml": b"<xml/>",
        "xl/sharedStrings.xml": f'<?xml version="1.0"?><sst xmlns="{MAIN}" count="0" uniqueCount="0"></sst>'.encode(),
        "xl/worksheets/sheet1.xml": sheet(table, kinds, prolog),
        "xl/worksheets/_rels/sheet1.xml.rels": b'<?xml version="1.0"?><Relationships/>',
        "xl/_rels/workbook.xml.rels": b'<?xml version="1.0"?><Relationships/>',
        "_rels/.rels": b'<?xml version="1.0"?><Relationships/>',
    } | (extra or {})
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, body in members.items():
            info = zipfile.ZipInfo(name, date_time=(2026, 1, 1, 0, 0, 0))
            archive.writestr(info, body, zipfile.ZIP_DEFLATED)
    return buffer.getvalue()


def set_origin(path: Path, origins: list[str]) -> None:
    """Record a download origin the way macOS browsers do, so the real reader is exercised."""
    value = plistlib.dumps(origins, fmt=plistlib.FMT_BINARY).hex()
    if SYNTHETIC_METADATA:
        SYNTHETIC_ORIGINS[path.resolve()] = value
        return
    result = run_command("xattr", ["-wx", ORIGIN_ATTRIBUTE, value, str(path)], timeout=30)
    require(result.returncode == 0, "Could not record the synthetic download origin")


@contextmanager
def download_metadata() -> Iterator[None]:
    """Off macOS, answer the download-metadata reads from the synthetic record; on macOS, change nothing."""
    if not SYNTHETIC_METADATA:
        yield
        return

    def read_attribute(program: str, arguments: list[str], **_options: Any) -> CompletedProcess[str]:
        require(program == "xattr" and len(arguments) == 3 and arguments[:2] == ["-px", ORIGIN_ATTRIBUTE], "Unexpected metadata command")
        value = SYNTHETIC_ORIGINS.get(Path(arguments[2]).resolve())
        return CompletedProcess([program, *arguments], 0 if value else 1, value or "", "" if value else "No such xattr")

    def created_at(path: Path) -> str:
        return datetime.fromtimestamp(path.stat().st_mtime, UTC).isoformat()

    try:
        with patch.object(contract, "run_command", read_attribute), patch.object(contract, "download_created_at", created_at):
            yield
    finally:
        SYNTHETIC_ORIGINS.clear()


def downloads_for(target: Path) -> dict:
    """Write all 44 synthetic quarters with HUD origins; return a manifest shaped like the real one."""
    target.mkdir(parents=True)
    records = []
    for year, quarter in contract.quarters():
        path = target / contract.file_name(year, quarter)
        table = rows(year, quarter)
        if (year, quarter) == (2014, 2):
            table = [table[0], *[r for row in table[1 : REPEATS + 1] for r in (row, list(row))], *table[REPEATS + 1 :]]
        path.write_bytes(workbook(table))
        set_origin(path, [contract.ORIGIN])
        sha, size = fingerprint(path)
        records.append({"quarter": f"{year}Q{quarter}", "filename": path.name, "bytes": size, "sha256": sha})
    return {"verified_downloads": records}


def exercise(root: Path) -> list[dict]:
    """Use production orchestration with synthetic downloads and versioned fake storage."""
    downloads = root / "downloads"
    manifest = downloads_for(downloads)
    plan = build(manifest)
    plan_path, versions = root / "plan.json", root / "versions.json"
    write_once(plan_path, encoded_json(plan))
    write_once(plan_path.with_suffix(".lock.json"), encoded_json({"plan_sha256": canonical_hash(plan)}))
    write_once(versions, encoded_json({"versions": [{"code_sha256": api_contract.code_hashes()}]}))
    decision_path = root / "repeat_decision.json"
    decision = build_repeat_decision(plan, 2014, 2, REPEATS)
    write_once(decision_path, encoded_json(decision))
    write_once(decision_path.with_suffix(".lock.json"), encoded_json({"decision_sha256": canonical_hash(decision)}))
    scenarios: list[dict] = []

    def scenario(name: str, action: Any, error: str | None = None) -> None:
        try:
            action()
            scenarios.append({"name": name, "passed": error is None, "observed": "success", "expected": error or "success"})
        except (ValueError, OSError, KeyError, TypeError, StopIteration, zipfile.BadZipFile) as failure:
            scenarios.append(
                {"name": name, "passed": error is not None and error in str(failure), "observed": str(failure)[:200], "expected": error or "success"}
            )

    batches = {(b["year"], b["quarter"]): b for b in plan["batches"]}
    batch = batches[(2010, 1)]
    client = FakeS3()
    with (
        patch.object(contract, "PLAN_PATH", plan_path),
        patch.object(contract, "DECISION_PATH", decision_path),
        patch.object(api_contract, "VERSIONS_PATH", versions),
    ):
        scenario("locked_scope", contract.load_plan)
        scenario(
            "plan_has_44_quarters", lambda: require(len(plan["batches"]) == 44 and min(batches) == (2010, 1) and max(batches) == (2020, 4), "Range differs")
        )
        eras = {k: b["county_geography"] for k, b in batches.items()}
        scenario("geography_eras", lambda: require(eras[(2011, 4)] == "2000_census" and eras[(2012, 1)] == "2010_census", "Era boundary differs"))
        run = root / "run"
        scenario("capture_without_storage", lambda: execute(plan, batch, run, downloads, False, client, OUTPUTS))
        receipt_path = run / "batches" / batch["id"] / "capture/receipt.json"
        scenario("offline_receipt_replay", lambda: verify_local(receipt_path))

        def text_kept() -> None:
            raw = (downloads / batch["file_name"]).read_bytes()
            csv, stats = contract.validate_workbook(raw, batch, plan)
            require(csv.startswith(b"bus_ratio,geoid,oth_ratio,res_ratio,tot_ratio,zip\n"), "Derived layout differs from the API captures")
            require(SMALL.encode() in csv and LARGE.encode() in csv and b",00001\n" in csv and b",01001," in csv, "Ratio text or leading zeros lost")
            require(stats["rows"] == len(api_contract.STATE_FIPS) + 3 and stats["zips"] == len(api_contract.STATE_FIPS) + 2, "One-to-many rows collapsed")
            require(stats["ratio_sums"]["tot_ratio"]["zips_outside_tolerance"] == 0, "Exact decimal sums differ")
            require((raw_copy := run / "batches" / batch["id"] / "capture/raw" / batch["file_name"]).read_bytes() == raw, f"Original bytes changed: {raw_copy}")

        scenario("text_zeros_split_rows_and_original_bytes_kept", text_kept)

        def provenance() -> None:
            receipt = verify_local(receipt_path)
            lineage = json.loads(receipt["lineage"]["extraction_or_query"])
            require(lineage["county_geography"] == "2000_census" and receipt["acquisition"]["request_method"] == "manual_download", "Provenance differs")
            require(
                receipt["measurement_periods"][0]["start_date"] == "2010-01-01" and receipt["measurement_periods"][0]["end_date"] == "2010-03-31",
                "Quarter dates",
            )
            require(receipt["acquisition"]["http_status"] is None, "HTTP status invented for a browser download")

        scenario("era_dates_and_manual_provenance_recorded", provenance)
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
            with patch.object(api_contract, "code_hashes", return_value={"changed": "0" * 64}):
                execute(plan, batches[(2010, 2)], root / "unreviewed", downloads, True, client, OUTPUTS)

        scenario("unreviewed_current_code_before_effects", unreviewed_current, "Unreviewed HUD")
        scenario("unreviewed_run_wrote_nothing", lambda: require(not (root / "unreviewed").exists(), "Unreviewed code wrote files"))

        def independent() -> None:
            # A fresh folder: macOS copies do not carry download-origin attributes.
            partial = root / "partial_downloads"
            require(downloads_for(partial) == manifest, "Synthetic downloads are not deterministic")
            (partial / batches[(2014, 2)]["file_name"]).unlink()
            s3 = FakeS3()
            results, held = run_all(plan, root / "all", partial, True, s3, OUTPUTS)
            require([h["quarter"] for h in held] == ["2014Q2"] and "missing" in held[0]["reason"], f"Held set differs: {held}")
            require(len(results) == 43 and all(r["status"] == "stored" for r in results), "Other quarters did not complete")
            again, held_again = run_all(plan, root / "all", partial, True, s3, OUTPUTS)
            require(again == results and [h["quarter"] for h in held_again] == ["2014Q2"], "Rerun differs")

        scenario("failing_quarter_held_others_stored", independent)
        repeat_scenarios(scenario, plan, batches, root, client, downloads, decision_path)
    return scenarios


def repeat_scenarios(scenario: Any, plan: dict, batches: dict, root: Path, client: FakeS3, downloads: Path, decision_path: Path) -> None:
    """Only the approved quarter may drop exact repeats, only exact ones, only the approved count, and never from the original."""
    batch = batches[(2014, 2)]
    run = root / "repeat_run"
    scenario("approved_quarter_stored_with_repeats_dropped", lambda: execute(plan, batch, run, downloads, True, client, OUTPUTS))
    receipt_path = run / "batches" / batch["id"] / "capture/receipt.json"

    def recorded() -> None:
        receipt = verify_local(receipt_path)
        raw = (downloads / batch["file_name"]).read_bytes()
        require((receipt_path.parent / "raw" / batch["file_name"]).read_bytes() == raw, "Original workbook changed")
        csv = (receipt_path.parent / "derived/crosswalk.csv").read_bytes().decode()
        require(len(csv.splitlines()) - 1 == len(rows(2014, 2)) - 1 == receipt["schema_profile"]["row_count"], "Repeats not dropped exactly once")
        require(any(w.startswith(f"{REPEATS} exact repeated rows") for w in receipt["quality_profile"]["warnings"]), "Dropped count not recorded")
        lineage = json.loads(receipt["lineage"]["extraction_or_query"])
        require(lineage["exact_repeat_decision_sha256"] == canonical_hash(read_json(decision_path)), "Decision not bound")

    scenario("original_kept_count_recorded_decision_bound", recorded)
    scenario("repeat_decision_replay", lambda: execute(plan, batch, run, downloads, True, client, OUTPUTS))

    def validate(table: list[list[str]], target: dict = batch) -> None:
        contract.validate_workbook(workbook(table), target, plan)

    base = rows(2014, 2)
    conflicting = [base[0], *[r for row in base[1 : REPEATS + 1] for r in (row, list(row))], *base[REPEATS + 1 :]]
    conflicting[2][5] = "0.5"
    scenario("conflicting_repeat_rejected_even_when_approved", lambda: validate(conflicting), "Duplicate ZIP-county")
    fewer = [base[0], *[r for row in base[1:REPEATS] for r in (row, list(row))], *base[REPEATS:]]
    scenario("different_repeat_count_rejected", lambda: validate(fewer), "repeat count differs")
    other = rows(2014, 3)
    other.append(list(other[1]))
    scenario("repeats_in_other_quarter_rejected", lambda: validate(other, batches[(2014, 3)]), "Duplicate ZIP-county")

    def tampered(kind: str) -> None:
        original = decision_path.read_bytes()
        body = read_json(decision_path)
        try:
            if kind == "edited":
                body["quarters"][0]["exact_repeat_rows"] += 1
                decision_path.write_bytes(encoded_json(body))
            else:
                body["quarters"][0]["sha256"] = "0" * 64
                decision_path.write_bytes(encoded_json(body))
                decision_path.with_suffix(".lock.json").write_bytes(encoded_json({"decision_sha256": canonical_hash(body)}))
            contract.load_repeat_decisions(plan)
        finally:
            decision_path.write_bytes(original)
            decision_path.with_suffix(".lock.json").write_bytes(encoded_json({"decision_sha256": canonical_hash(read_json(decision_path))}))

    scenario("decision_edit_rejected", lambda: tampered("edited"), "decision lock differs")
    scenario("decision_for_other_file_rejected", lambda: tampered("rebound"), "decision binding differs")

    def unbound() -> None:
        target = root / "repeat_unbound"
        shutil.copytree(receipt_path.parent, target)
        receipt = read_json(target / "receipt.json")
        lineage = json.loads(receipt["lineage"]["extraction_or_query"])
        lineage.pop("exact_repeat_decision_sha256")
        receipt["lineage"]["extraction_or_query"] = json.dumps(lineage)
        (target / "receipt.json").write_bytes(encoded_json(receipt))
        verify_local(target / "receipt.json")

    scenario("receipt_without_decision_binding_rejected", unbound, "repeat decision binding differs")


def malformed_scenarios(scenario: Any, plan: dict, batches: dict) -> None:
    """Every content defect named in the failure modes stops that quarter."""
    batch = batches[(2012, 1)]

    def check(**changes: Any) -> None:
        table = rows(2012, 1)
        mutate = changes.pop("mutate", None)
        if mutate:
            mutate(table)
        contract.validate_workbook(changes.pop("raw", None) or workbook(table, **changes), batch, plan)

    for name, kwargs, error in [
        ("duplicate_row", {"mutate": lambda t: t.append(list(t[1]))}, "Duplicate ZIP-county"),
        ("missing_state", {"mutate": lambda t: t.__delitem__(slice(51, 52))}, "state coverage"),
        ("extra_column", {"mutate": lambda t: [r.append("x") for r in t]}, "row shape"),
        ("short_row", {"mutate": lambda t: t[3].pop()}, "row shape"),
        ("header_order", {"mutate": lambda t: t.__setitem__(0, [t[0][1], t[0][0], *t[0][2:]])}, "header differs"),
        ("shared_string_cell", {"kinds": {(3, 0): "shared"}}, "cell type"),
        ("formula_cell", {"kinds": {(3, 2): "formula"}}, "cell type"),
        ("numeric_zip", {"kinds": {(3, 0): "number"}}, "cell type"),
        ("text_ratio", {"kinds": {(3, 2): "text"}}, "cell type"),
        ("short_county", {"mutate": lambda t: t[3].__setitem__(1, "1001")}, "identifier invalid"),
        ("territory_code_not_in_these_years", {"mutate": lambda t: t[3].__setitem__(1, "64")}, "identifier invalid"),
        ("ratio_above_one", {"mutate": lambda t: t[3].__setitem__(5, "1.5")}, "ratio invalid"),
        ("ratio_not_a_number", {"mutate": lambda t: t[3].__setitem__(5, "NaN")}, "ratio invalid"),
        ("second_sheet", {"sheets": 2}, "one worksheet"),
        ("extra_member", {"extra": {"xl/vbaProject.bin": b"x"}}, "package members differ"),
        ("entity_declaration", {"prolog": '<!DOCTYPE x [<!ENTITY a "b">]>'}, "declarations"),
        ("header_only", {"mutate": lambda t: t.__delitem__(slice(1, None))}, "no data rows"),
        ("not_a_workbook", {"raw": b"<html>Login</html>"}, "not a workbook package"),
    ]:
        scenario(name, lambda kwargs=kwargs: check(**dict(kwargs)), error)

    def oversized() -> None:
        with patch.object(contract, "MAX_SHEET_BYTES", 100):
            check()

    scenario("expanded_size_bound", oversized, "size bound")


def staging_scenarios(scenario: Any, plan: dict, batches: dict, root: Path, client: FakeS3) -> None:
    """The copied original must be the exact recorded HUD download for that quarter."""
    batch = batches[(2015, 3)]

    def stage(kind: str) -> None:
        folder = root / f"stage_{kind}"
        folder.mkdir()
        target = folder / batch["file_name"]
        body = workbook(rows(2015, 3))
        if kind == "other_quarter":
            body = workbook(rows(2015, 4))
        target.write_bytes(body)
        if kind == "wrong_origin":
            set_origin(target, ["https://example.org/"])
        elif kind != "missing_origin":
            set_origin(target, [contract.ORIGIN])
        if kind == "symlink":
            real = folder / "real.xlsx"
            target.rename(real)
            target.symlink_to(real)
        execute(plan, batch, root / f"run_{kind}", folder, False, client, OUTPUTS)

    for kind, error in [
        ("wrong_origin", "origin differs"),
        ("missing_origin", "origin missing"),
        ("other_quarter", "differs from the recorded download"),
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
            raw.write_bytes(workbook(rows(2010, 2)))
            reseal(f"raw/{raw.name}")
        elif kind == "resealed_csv_corruption":
            path = target / "derived/crosswalk.csv"
            path.write_bytes(path.read_bytes() + b"extra\n")
            reseal("derived/crosswalk.csv")
        elif kind == "resealed_origin_change":
            path = target / "evidence/download_proof.json"
            proof = read_json(path)
            proof["origin_urls"] = ["https://example.org/"]
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
        elif kind == "wrong_era":
            lineage["county_geography"] = "2010_census"
        elif kind == "restrictions_removed":
            receipt["governance"]["use_restrictions"] = receipt["governance"]["use_restrictions"][1:]
        receipt["lineage"]["extraction_or_query"] = json.dumps(lineage)
        (target / "receipt.json").write_bytes(encoded_json(receipt))
        verify_local(target / "receipt.json")

    for kind, error in [
        ("raw_corruption", "Artifact hash"),
        ("resealed_raw_substitution", "raw hash differs"),
        ("resealed_csv_corruption", "CSV differs"),
        ("resealed_origin_change", "proof differs"),
        ("model_promotion", "hold differs"),
        ("unreviewed_code", "Unreviewed HUD"),
        ("wrong_plan", "plan differs"),
        ("terms_binding", "terms binding"),
        ("wrong_era", "geography era differs"),
        ("restrictions_removed", "governance differs"),
    ]:
        scenario(kind, lambda kind=kind: corrupt(kind), error)


def storage_scenarios(scenario: Any, plan: dict, batches: dict, root: Path, run: Path, client: FakeS3, downloads: Path) -> None:
    """Interrupted uploads resume without duplicates; resealed storage evidence is refused."""
    batch = batches[(2016, 4)]

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
        first = plan["batches"][0]
        target = root / "bad_storage"
        shutil.copytree(run, target)
        branch = target / "batches" / first["id"]
        path = branch / "capture/s3_collections_reconciliation.json"
        rec = read_json(path)
        rec["objects"][0]["object"]["version_id"] = ""
        path.write_bytes(encoded_json(rec))
        done = read_json(branch / "completed.json")
        done["reconciliation_sha256"] = fingerprint(path)[0]
        (branch / "completed.json").write_bytes(encoded_json(done))
        execute(plan, first, target, downloads, True, client, OUTPUTS)

    scenario("resealed_missing_storage_version", invalid_version, "version missing")
    scenario("batch_outside_plan_rejected", lambda: execute(plan, batch | {"year": 2021}, root / "outside", downloads, False, client, OUTPUTS), "plan or batch")


def main() -> None:
    """Write one immutable artifact for every E2E invocation."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="hud_xlsx_e2e_") as directory, download_metadata():
        scenarios = exercise(Path(directory))
    passed = all(s["passed"] for s in scenarios)
    report = {
        "status": "passed" if passed else "failed",
        "feature": "HUD ZIP-COUNTY workbook capture (2010 Q1 to 2020 Q4 manual downloads) through versioned S3 storage",
        "run_at_utc": datetime.now(UTC).isoformat(),
        "python": sys.version.split()[0],
        "code_sha256": api_contract.code_hashes(),
        "dependency_sha256": api_contract.digest(Path("requirements.txt").read_bytes()),
        "reproduce": f".venv/bin/python -m scripts.acquisition.run_hud_xlsx_e2e --output {args.output}",
        "boundary": "Synthetic workbooks written to a temporary folder with real macOS origin attributes; fake versioned S3; no live HUD or AWS access.",
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
