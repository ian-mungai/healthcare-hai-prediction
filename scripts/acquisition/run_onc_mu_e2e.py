"""Repeatable synthetic ONC meaningful-use capture-to-S3 verification, without live downloads or credentials.

All the data is public: a capture stores the file whole under raw/ as published and records that it names clinicians, whom bronze
pseudonymizes at load. Captures stored under plan version 1 (hospital rows only, original on the Mac) keep verifying.
"""

import argparse
import copy
import csv
import io
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
# Synthetic clinician identifiers; the published file holds them, so the stored file does too.
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


def exercise(root: Path) -> list[dict]:
    """Use production orchestration with a synthetic file and versioned fake storage."""
    data = body()
    plan, plan_path = locked(root, data)
    versions = root / "versions.json"
    write_once(versions, encoded_json({"versions": [{"code_sha256": contract.collector_code_hashes()}]}))
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

        def stored_whole() -> None:
            capture = receipt_path.parent
            paths = {item["storage_path"] for item in read_json(receipt_path)["artifacts"]}
            require(paths == {f"raw/{contract.FILE_NAME}", "evidence/download_proof.json", "references/scope.json"}, f"Artifact set differs: {paths}")
            require((capture / "raw" / contract.FILE_NAME).read_bytes() == data, "The file is not stored as published")
            require(not (run / "private_original").exists() and not (capture / "derived").exists(), "A private folder or a derived copy was made")
            stats = read_json(capture / "evidence/download_proof.json")["statistics"]
            require((stats["rows_read"], stats["clinician_rows"], stats["hospital_rows"], stats["encoding"]) == (12, 6, 6, "cp1252"), f"Counts: {stats}")

        scenario("file_stored_whole_as_published", stored_whole)

        def public_with_names() -> None:
            receipt = read_json(receipt_path)
            governance = receipt["governance"]
            require(governance["access_class"] == "public" and governance["contains_pii"] is True, "Governance does not say the public file names people")
            require(contract.PSEUDONYMIZE_AT_LOAD in governance["use_restrictions"], "The bronze pseudonymization restriction is missing")
            require(receipt["schema_profile"]["native_headers"] == HEADER, "The schema profile is not the published header")

        scenario("receipt_records_public_names_pseudonymized_in_bronze", public_with_names)
        scenario("capture_and_version_verified_storage", lambda: execute(plan, run, True, True, client, OUTPUTS, web, LIMITS))

        scenario(
            "s3_holds_the_file_as_published",
            lambda: require(any(o["body"] == data for o in client.objects.values()), "S3 does not hold the file as published"),
        )
        before, objects, requests = inventory(run), copy.deepcopy(client.objects), web.requests

        def repeat() -> None:
            execute(plan, run, True, True, client, OUTPUTS, web, LIMITS)
            require(before == inventory(run) and objects == client.objects and web.requests == requests, "Repeated effects")

        scenario("repeat_one_no_effects", repeat)
        scenario("repeat_two_no_effects", repeat)
        scenario("replay_without_network", lambda: execute(plan, run, False, True, client, OUTPUTS, Offline(b""), LIMITS))
        check_scenarios(scenario, plan, root)
        version_1_scenario(scenario, plan, root, data)
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
            with patch.object(contract, "collector_code_hashes", return_value={"changed": "0" * 64}):
                execute(plan, root / "unreviewed", True, True, client, OUTPUTS, web, LIMITS)

        scenario("unreviewed_current_code_before_effects", unreviewed_current, "Unreviewed ONC meaningful-use")
        scenario("unreviewed_run_wrote_nothing", lambda: require(not (root / "unreviewed").exists(), "Unreviewed code wrote files"))
    return scenarios


def check_scenarios(scenario: Any, plan: dict, root: Path) -> None:
    """A file that is not the published shape stops the capture."""
    h = {c: i for i, c in enumerate(HEADER)}

    def check(data: bytes) -> None:
        path = root / f"check_{len(list(root.glob('check_*')))}.csv"
        path.write_bytes(data)
        contract.check_raw(path, plan)

    cases = [
        ("unknown_provider_type", body(lambda t: t[1].__setitem__(h["Provider_Type"], "Group")), "provider type differs"),
        ("hospital_row_without_ccn", body(lambda t: t[1].__setitem__(h["CCN"], "")), "has no CCN"),
        ("program_year_missing", body(lambda t: t.__delitem__(slice(1, 3))), "program years missing"),
        ("header_differs", body(lambda t: t[0].__setitem__(0, "Provider_NPI")), "header differs"),
        ("short_row", body(lambda t: t[2].pop()), "row shape differs"),
    ]
    for name, data, error in cases:
        scenario(name, lambda data=data: check(data), error)

    def utf8() -> None:
        path = root / "check_utf8.csv"
        path.write_bytes(b"\xef\xbb\xbf" + body().decode("cp1252").encode())
        require(contract.check_raw(path, plan)["encoding"] == "utf-8-sig", "UTF-8 not detected")

    scenario("utf8_with_bom_read_and_recorded", utf8)


def version_1_scenario(scenario: Any, plan: dict, root: Path, data: bytes) -> None:
    """A capture stored under plan version 1 keeps verifying: its derived CSV is rebuilt from the original kept on the Mac."""

    def stored_version_1() -> None:
        from scripts.acquisition.capture import receipt_validator

        collection = root / "version_1"
        capture = collection / "batches" / plan["id"] / "capture"
        kept = collection / "private_original" / "run_01" / "attempt_01" / contract.FILE_NAME
        kept.parent.mkdir(parents=True)
        kept.write_bytes(data)
        derived, statistics = contract.derive(kept, plan)
        proof = {"requested_url": plan["url"], "sha256": plan["sha256"], "original_path": kept.relative_to(collection).as_posix(), "statistics": statistics}
        write_once(capture / "derived/hospital_attestations.csv", derived)
        write_once(capture / "evidence/download_proof.json", encoded_json(proof))
        write_once(capture / "references/scope.json", encoded_json(plan))
        receipt = {
            "snapshot_status": "acquired_unvalidated",
            "artifacts": [{"storage_path": name} for name in sorted(contract.V1_ARTIFACTS)],
            "acquisition": {"requested_url": plan["url"], "http_status": 200},
            "schema_profile": {"row_count": statistics["hospital_rows_kept"]},
            "governance": {"contains_pii": False},
        }
        lineage = {"file_id": plan["id"], "plan_sha256": canonical_hash(plan), "model_eligible": False, "registry_sha256": plan["registry_sha256"]}
        lineage |= {"code_sha256": contract.collector_code_hashes(), "schema_sha256": canonical_hash(receipt_validator().schema)}
        contract.verify_capture(receipt, {"source_id": contract.SOURCE_ID}, lineage, capture, False)

    scenario("stored_version_1_capture_still_verifies", stored_version_1)


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
    """Replay refuses tampered files, lineage and governance, even when hashes are resealed."""
    collection = receipt_path.parents[3]

    def corrupt(kind: str) -> None:
        target_root = collection.parent / f"corrupt_{kind}"
        shutil.copytree(collection, target_root)
        target = target_root / receipt_path.relative_to(collection).parent
        receipt = read_json(target / "receipt.json")
        lineage = json.loads(receipt["lineage"]["extraction_or_query"])
        stored = target / "raw" / contract.FILE_NAME
        if kind == "resealed_changed_file":
            stored.write_bytes(data + b"\r\n")
            for item in receipt["artifacts"]:
                if item["storage_path"] == f"raw/{contract.FILE_NAME}":
                    item["sha256"], item["byte_count"] = fingerprint(stored)
        elif kind == "stored_file_deleted":
            stored.unlink()
        elif kind == "derived_copy_added":
            (target / "derived").mkdir()
            (target / "derived" / "hospital_attestations.csv").write_bytes(b"x\n")
        elif kind == "model_promotion":
            lineage["model_eligible"] = True
        elif kind == "unreviewed_code":
            lineage["code_sha256"] = {"bad": "0" * 64}
        elif kind == "names_hidden_in_governance":
            receipt["governance"]["contains_pii"] = False
        receipt["lineage"]["extraction_or_query"] = json.dumps(lineage)
        (target / "receipt.json").write_bytes(encoded_json(receipt))
        verify_local(target / "receipt.json")

    for kind, error in [
        ("resealed_changed_file", "file hash differs"),
        ("stored_file_deleted", "Artifact is missing"),
        ("derived_copy_added", "holds a derived copy"),
        ("model_promotion", "hold differs"),
        ("unreviewed_code", "Unreviewed ONC meaningful-use"),
        ("names_hidden_in_governance", "governance differs"),
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
        "feature": "ONC meaningful-use attestations: the file stored whole under raw/ as published, through versioned S3 storage",
        "run_at_utc": datetime.now(UTC).isoformat(),
        "python": sys.version.split()[0],
        "code_sha256": contract.collector_code_hashes(),
        "dependency_sha256": bls_contract.digest(Path("uv.lock").read_bytes()),
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
