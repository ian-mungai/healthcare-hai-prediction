"""Repeatable synthetic ONC meaningful-use capture-to-S3 verification, without live downloads or credentials."""

import argparse
import copy
import csv
import io
import json
import shutil
import stat
import sys
import tempfile
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import patch

from scripts.acquisition import bls_api_contract as bls_contract
from scripts.acquisition import onc_mu_contract as contract
from scripts.acquisition.build_onc_mu_plan import build
from scripts.acquisition.run_bls_api_e2e import inventory
from scripts.acquisition.s3_store import encoded_json, fingerprint, write_once
from scripts.acquisition.source_registry import REPO_ROOT, canonical_hash, read_json, require
from scripts.acquisition.store_onc_mu import execute, verify_local
from scripts.acquisition.tests.test_s3_store import OUTPUTS, FakeS3
from scripts.acquisition.transport import Limits

DOWNLOADS_PATH = "config/acquisition/e2e_inputs/onc_download.json"

HEADER = read_json(REPO_ROOT / DOWNLOADS_PATH)["header"]
# Synthetic clinician identifiers; they must never reach a snapshot or S3.
CLINICIAN_NPIS = ["1999999991", "1999999992"]
LIMITS = Limits(attempts=1, timeout_seconds=5, max_seconds=30)


def rows(change: Any = None) -> list[list[str]]:
    """Clinician and hospital rows for every required program year."""
    out = [list(HEADER)]

    def row(kind: str, year: str, **values: str) -> list[str]:
        cells = dict.fromkeys(HEADER, "")
        cells.update({"Provider_Type": kind, "Program_Year": year, "Business_State_Territory": "California", "Vendor_Name": "Synthetic EHR Café Co"})
        cells.update(values)
        return [cells[c] for c in HEADER]

    for i, year in enumerate(contract.REQUIRED_YEARS):
        out.append(row("Hospital", year, NPI=f"10000000{i:02d}", CCN=f"05{i:04d}", Hospital_Type="General", ZIP="90000"))
        out.append(row("EP", year, NPI=CLINICIAN_NPIS[i % 2], Specialty="Family Practice", ZIP="90001"))
    if change is not None:
        change(out)
    return out


def body(change: Any = None) -> bytes:
    """Serialize like the publisher: quoted CSV, CRLF, Windows-1252."""
    output = io.StringIO(newline="")
    csv.writer(output, quoting=csv.QUOTE_ALL, lineterminator="\r\n").writerows(rows(change))
    return output.getvalue().encode("cp1252")


class Response(io.BytesIO):
    """A minimal HTTP response for the project downloader."""

    def __init__(self, data: bytes, url: str, status: int = 200, headers: dict | None = None) -> None:
        super().__init__(data)
        self.status, self.url = status, url
        self.headers = {"Content-Length": str(len(data)), "Content-Type": "text/csv", **(headers or {})}

    def geturl(self) -> str:
        return self.url


class Web:
    """Serves one synthetic file and counts requests; an override makes it misbehave."""

    def __init__(self, data: bytes) -> None:
        self.data, self.requests = data, 0
        self.override: Response | Exception | None = None

    def open(self, request: Any, timeout: float) -> Response:
        self.requests += 1
        if isinstance(self.override, Exception):
            raise self.override
        return self.override if self.override is not None else Response(self.data, request.full_url)


class Offline(Web):
    """Any network use is a failure."""

    def open(self, request: Any, timeout: float) -> Response:
        raise AssertionError("network used")


def locked(root: Path, data: bytes) -> tuple[dict, Path]:
    """A locked plan for the real URL bound to synthetic bytes."""
    record = read_json(REPO_ROOT / DOWNLOADS_PATH) | {"sha256": bls_contract.digest(data), "bytes": len(data)}
    plan = build(record)
    path = root / "plan.json"
    write_once(path, encoded_json(plan))
    write_once(path.with_suffix(".lock.json"), encoded_json({"plan_sha256": canonical_hash(plan)}))
    return plan, path


def outside_private(run: Path) -> str:
    """Everything outside the private original folder, as text."""
    return "\n".join(p.read_bytes().decode("utf-8", "ignore") for p in run.rglob("*") if p.is_file() and "private_original" not in p.parts)


def exercise(root: Path) -> list[dict]:
    """Use production orchestration with a synthetic file and versioned fake storage."""
    data = body()
    plan, plan_path = locked(root, data)
    versions = root / "versions.json"
    write_once(versions, encoded_json({"versions": [{"code_sha256": bls_contract.code_hashes()}]}))
    scenarios: list[dict] = []

    def scenario(name: str, action: Any, error: str | None = None) -> None:
        try:
            action()
            scenarios.append({"name": name, "passed": error is None, "observed": "success", "expected": error or "success"})
        except (ValueError, OSError, KeyError, TypeError, StopIteration, IndexError, AssertionError) as failure:
            scenarios.append(
                {"name": name, "passed": error is not None and error in str(failure), "observed": str(failure)[:200], "expected": error or "success"}
            )

    web, client = Web(data), FakeS3()
    with patch.object(contract, "PLAN_PATH", plan_path), patch.object(contract, "VERSIONS_PATH", versions):
        scenario("locked_scope", contract.load_plan)
        run = root / "run"
        scenario("capture_without_storage", lambda: execute(plan, run, True, False, None, OUTPUTS, web, LIMITS))
        receipt_path = run / "batches" / plan["id"] / "capture/receipt.json"
        scenario("offline_receipt_replay", lambda: verify_local(receipt_path))

        def original_private() -> None:
            originals = list((run / "private_original").rglob("*.csv"))
            require(len(originals) == 1 and originals[0].read_bytes() == data, "Original not kept unchanged locally")
            require(stat.S_IMODE(originals[0].stat().st_mode) == 0o600, "Original is not owner-only")
            require(not (receipt_path.parent / "raw").exists() and not (receipt_path.parent / "audit").exists(), "Snapshot holds original bytes")
            text = outside_private(run)
            require(not any(n in text for n in CLINICIAN_NPIS) and "Family Practice" not in text, "Clinician data outside the private original")

        scenario("original_local_owner_only_and_outside_snapshot", original_private)

        def derived_content() -> None:
            table = list(csv.reader(io.StringIO((receipt_path.parent / "derived/hospital_attestations.csv").read_text())))
            require(table[0] == [c for c in HEADER if c != "Specialty"] and len(table) == 7, "Derived rows or columns differ")
            require(all(r[table[0].index("Provider_Type")] == "Hospital" for r in table[1:]), "Clinician rows kept")
            stats = read_json(receipt_path.parent / "evidence/download_proof.json")["statistics"]
            require((stats["rows_read"], stats["clinician_rows_dropped"], stats["hospital_rows_kept"], stats["encoding"]) == (12, 6, 6, "cp1252"), "Counts")

        scenario("derived_hospital_rows_only_without_specialty", derived_content)
        scenario("capture_and_version_verified_storage", lambda: execute(plan, run, True, True, client, OUTPUTS, web, LIMITS))

        def stored_clean() -> None:
            stored = "\n".join(o["body"].decode("utf-8", "ignore") for o in client.objects.values())
            require(not any(n in stored for n in CLINICIAN_NPIS) and "Family Practice" not in stored, "S3 received clinician data")
            require(not any("private_original" in k or k.endswith("mu_report.csv") for k in client.objects), "S3 received the original")

        scenario("s3_holds_no_clinician_data_or_original", stored_clean)
        before, objects, requests = inventory(run), copy.deepcopy(client.objects), web.requests

        def repeat() -> None:
            execute(plan, run, True, True, client, OUTPUTS, web, LIMITS)
            require(before == inventory(run) and objects == client.objects and web.requests == requests, "Repeated effects")

        scenario("repeat_one_no_effects", repeat)
        scenario("repeat_two_no_effects", repeat)
        scenario("replay_without_network", lambda: execute(plan, run, False, True, client, OUTPUTS, Offline(b""), LIMITS))
        derive_scenarios(scenario, plan, root)
        fetch_scenarios(scenario, plan, root, data)
        corrupt_scenarios(scenario, receipt_path, data)
        storage_scenarios(scenario, plan, root, run, client, web)

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
                execute(plan, root / "unreviewed", True, True, client, OUTPUTS, web, LIMITS)

        scenario("unreviewed_current_code_before_effects", unreviewed_current, "Unreviewed ONC meaningful-use")
        scenario("unreviewed_run_wrote_nothing", lambda: require(not (root / "unreviewed").exists(), "Unreviewed code wrote files"))
    return scenarios


def derive_scenarios(scenario: Any, plan: dict, root: Path) -> None:
    """Every content defect named in the failure modes stops the capture."""
    h = {c: i for i, c in enumerate(HEADER)}

    def check(data: bytes) -> None:
        path = root / f"derive_{len(list(root.glob('derive_*')))}.csv"
        path.write_bytes(data)
        contract.derive(path, plan)

    cases = [
        ("unknown_provider_type", body(lambda t: t[1].__setitem__(h["Provider_Type"], "Group")), "provider type differs"),
        ("hospital_row_with_specialty", body(lambda t: t[1].__setitem__(h["Specialty"], "Surgery")), "carries a specialty"),
        ("hospital_row_without_ccn", body(lambda t: t[1].__setitem__(h["CCN"], "")), "has no CCN"),
        ("program_year_missing", body(lambda t: t.__delitem__(slice(1, 3))), "program years missing"),
        ("header_differs", body(lambda t: t[0].__setitem__(0, "Provider_NPI")), "header differs"),
        ("short_row", body(lambda t: t[2].pop()), "row shape differs"),
    ]
    for name, data, error in cases:
        scenario(name, lambda data=data: check(data), error)

    def utf8() -> None:
        path = root / "derive_utf8.csv"
        path.write_bytes(b"\xef\xbb\xbf" + body().decode("cp1252").encode())
        _, stats = contract.derive(path, plan)
        require(stats["encoding"] == "utf-8-sig", "UTF-8 not detected")

    scenario("utf8_with_bom_read_and_recorded", utf8)


def fetch_scenarios(scenario: Any, plan: dict, root: Path, data: bytes) -> None:
    """Failed, wrong or partial downloads are held and never reach a snapshot."""
    cases: list[tuple[str, Response | Exception, str]] = [
        ("html_page_held", Response(b"<!doctype html><html></html>", plan["url"]), "download failed"),
        ("server_error_held", Response(b"busy", plan["url"], status=503), "download failed"),
        ("truncated_download_held", Response(data[:-10], plan["url"], headers={"Content-Length": str(len(data))}), "download failed"),
        ("revised_publisher_file_held", Response(body(lambda t: t.append(list(t[1]))), plan["url"]), "differs from the plan"),
    ]
    for name, response, error in cases:
        web = Web(data)
        web.override = response
        target = root / f"fetch_{name}"
        scenario(name, lambda web=web, target=target: execute(plan, target, True, False, None, OUTPUTS, web, LIMITS), error)
        scenario(f"{name}_no_snapshot", lambda target=target: require(not (target / "batches").exists(), "Failed download produced a snapshot"))
    scenario("no_fetch_without_original", lambda: execute(plan, root / "no_fetch", False, False, None, OUTPUTS, Offline(b""), LIMITS), "original not captured")


def corrupt_scenarios(scenario: Any, receipt_path: Path, data: bytes) -> None:
    """Replay refuses tampered artifacts, originals, lineage and governance, even when hashes are resealed."""
    collection = receipt_path.parents[3]

    def corrupt(kind: str) -> None:
        target_root = collection.parent / f"corrupt_{kind}"
        shutil.copytree(collection, target_root)
        target = target_root / receipt_path.relative_to(collection).parent
        receipt = read_json(target / "receipt.json")
        lineage = json.loads(receipt["lineage"]["extraction_or_query"])
        original = next((target_root / "private_original").rglob("*.csv"))
        if kind == "resealed_csv_with_clinician":
            path = target / "derived/hospital_attestations.csv"
            path.write_bytes(path.read_bytes() + CLINICIAN_NPIS[0].encode() + b"\n")
            for item in receipt["artifacts"]:
                if item["storage_path"] == "derived/hospital_attestations.csv":
                    item["sha256"], item["byte_count"] = fingerprint(path)
        elif kind == "original_changed":
            original.write_bytes(data + b"\r\n")
        elif kind == "original_deleted":
            original.unlink()
        elif kind == "original_copied_into_snapshot":
            (target / "raw").mkdir()
            (target / "raw" / original.name).write_bytes(original.read_bytes())
        elif kind == "model_promotion":
            lineage["model_eligible"] = True
        elif kind == "unreviewed_code":
            lineage["code_sha256"] = {"bad": "0" * 64}
        elif kind == "pii_governance":
            receipt["governance"]["contains_pii"] = True
        receipt["lineage"]["extraction_or_query"] = json.dumps(lineage)
        (target / "receipt.json").write_bytes(encoded_json(receipt))
        verify_local(target / "receipt.json")

    for kind, error in [
        ("resealed_csv_with_clinician", "derived CSV differs"),
        ("original_changed", "original hash differs"),
        ("original_deleted", "No such file"),
        ("original_copied_into_snapshot", "holds original bytes"),
        ("model_promotion", "hold differs"),
        ("unreviewed_code", "Unreviewed ONC meaningful-use"),
        ("pii_governance", "governance differs"),
    ]:
        scenario(kind, lambda kind=kind: corrupt(kind), error)


def storage_scenarios(scenario: Any, plan: dict, root: Path, run: Path, client: FakeS3, web: Web) -> None:
    """Interrupted uploads resume without duplicates; resealed storage evidence is refused."""

    def interrupted() -> None:
        partial, s3 = root / "partial", FakeS3()
        s3.corrupt = True
        with suppress(ValueError):
            execute(plan, partial, True, True, s3, OUTPUTS, web, LIMITS)
        require(len(s3.objects) == 1, "No interrupted upload")
        s3.corrupt = False
        execute(plan, partial, True, True, s3, OUTPUTS, web, LIMITS)
        stored = copy.deepcopy(s3.objects)
        execute(plan, partial, True, True, s3, OUTPUTS, web, LIMITS)
        require(stored == s3.objects, "Resume duplicated effects")

    scenario("resume_interrupted_storage", interrupted)

    def invalid_version() -> None:
        target = root / "bad_storage"
        shutil.copytree(run, target)
        branch = target / "batches" / plan["id"]
        path = branch / "capture/s3_collections_reconciliation.json"
        rec = read_json(path)
        rec["objects"][0]["object"]["version_id"] = ""
        path.write_bytes(encoded_json(rec))
        done = read_json(branch / "completed.json")
        done["reconciliation_sha256"] = fingerprint(path)[0]
        (branch / "completed.json").write_bytes(encoded_json(done))
        execute(plan, target, False, True, client, OUTPUTS, Offline(b""), LIMITS)

    scenario("resealed_missing_storage_version", invalid_version, "version missing")
    scenario("plan_outside_lock_rejected", lambda: execute(plan | {"bytes": 1}, root / "outside", True, False, None, OUTPUTS, web, LIMITS), "plan differs")


def main() -> None:
    """Write one immutable artifact for every E2E invocation."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="onc_mu_e2e_") as directory:
        scenarios = exercise(Path(directory))
    passed = all(s["passed"] for s in scenarios)
    report = {
        "status": "passed" if passed else "failed",
        "feature": "ONC meaningful-use attestations: original kept locally, hospital rows only through versioned S3 storage",
        "run_at_utc": datetime.now(UTC).isoformat(),
        "python": sys.version.split()[0],
        "code_sha256": bls_contract.code_hashes(),
        "dependency_sha256": bls_contract.digest(Path("requirements.txt").read_bytes()),
        "reproduce": f".venv/bin/python -m scripts.acquisition.run_onc_mu_e2e --output {args.output}",
        "boundary": "Synthetic 12-row file served by a fake opener through the real downloader; fake versioned S3; no live healthit.gov or AWS.",
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
