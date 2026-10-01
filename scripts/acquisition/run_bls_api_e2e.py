"""Run synthetic collector-to-S3 scenarios and retain repeatable sanitized evidence."""

import argparse
import copy
import json
import platform
import shutil
import sys
import tempfile
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import patch

from scripts.acquisition import bls_api_contract as contract
from scripts.acquisition import bls_api_transport as transport
from scripts.acquisition.collect_bls_api import execute, verify_local
from scripts.acquisition.s3_store import encoded_json, fingerprint, write_once
from scripts.acquisition.source_registry import canonical_hash, load_registry, read_json, require
from scripts.acquisition.tests.test_s3_store import OUTPUTS, FakeS3


def fixture(batch: dict) -> bytes:
    """Use only generic synthetic values with a missing token and footnotes."""
    series = []
    for sid in batch["series_ids"]:
        rows: list[dict] = [
            {
                "year": str(y),
                "period": f"M{m:02}",
                "periodName": "Synthetic period",
                "value": "-" if m == 10 else "5.0",
                "footnotes": [{"code": "X", "text": "Synthetic missing value"}] if m == 10 else [{}],
            }
            for y in range(batch["start_year"], batch["end_year"] + 1)
            for m in range(1, 14)
        ]
        series.append({"seriesID": sid, "data": rows})
    return encoded_json({"status": "REQUEST_SUCCEEDED", "message": [], "Results": {"series": series}})


def inventory(root: Path) -> dict:
    """Fingerprint every persisted file, excluding OS lock bookkeeping."""
    return {str(p.relative_to(root)): fingerprint(p) for p in sorted(root.rglob("*")) if p.is_file() and p.name != ".collection.lock"}


def exercise(root: Path) -> list[dict]:
    """Drive the real integrated workflow with synthetic network and S3 boundaries."""
    batch: dict = {"series_ids": ["LAUCN010010000000003", "LAUCN010010000000004"], "start_year": 2000, "end_year": 2000}
    batch["id"] = contract.batch_id(batch)
    other_batch = dict(batch, start_year=2001, end_year=2001)
    other_batch["id"] = contract.batch_id(other_batch)
    plan = {
        "source_id": "BLS",
        "vintage": "2000-01-01",
        "endpoint": "https://api.bls.gov/publicAPI/v2/timeseries/data/",
        "credential_reference": "bls_api_key",
        "request_limit_24h": 2,
        "request_spacing_seconds": 0,
        "registry_sha256": canonical_hash(load_registry()),
        "references": [],
        "batches": [batch, other_batch],
        "model_eligible": False,
    }
    plan_path, versions_path = root / "plan.json", root / "versions.json"
    write_once(plan_path, encoded_json(plan))
    write_once(plan_path.with_suffix(".lock.json"), encoded_json({"plan_sha256": canonical_hash(plan)}))
    write_once(versions_path, encoded_json({"versions": [{"version": "synthetic_pending_review", "code_sha256": contract.code_hashes()}]}))
    scenarios: list[dict] = []

    def scenario(name: str, action: Any, error: str | None = None) -> None:
        try:
            action()
            scenarios.append({"name": name, "passed": error is None, "expected": error or "success", "observed": "success"})
        except (ValueError, OSError, KeyError, TypeError, StopIteration) as failure:
            message = str(failure)
            scenarios.append({"name": name, "passed": error is not None and error in message, "expected": error or "success", "observed": message[:300]})

    payload = fixture(batch)
    calls: list[int] = []

    def request(*_args: Any) -> bytes:
        calls.append(1)
        return payload

    client = FakeS3()
    run_root = root / "run"
    with patch.object(contract, "PLAN_PATH", plan_path), patch.object(contract, "VERSIONS_PATH", versions_path):
        scenario("locked_plan", contract.load_plan)
        scenario("capture_and_version_verified_storage", lambda: execute(plan, batch, run_root, True, True, client, OUTPUTS, request))
        snapshot = inventory(run_root)
        stored = copy.deepcopy(client.objects)

        def repeated() -> None:
            result = execute(plan, batch, run_root, True, True, client, OUTPUTS, request)
            require(result["status"] == "stored" and len(calls) == 1, "Repeat made request")
            require(inventory(run_root) == snapshot and client.objects == stored, "Repeat changed files or versions")

        scenario("repeat_one_no_requests_or_writes", repeated)
        scenario("repeat_two_no_requests_or_writes", repeated)
        receipt_path = run_root / "batches" / batch["id"] / "capture/receipt.json"

        def altered(kind: str) -> None:
            target = root / kind
            shutil.copytree(receipt_path.parent, target)
            receipt = read_json(target / "receipt.json")
            if kind == "corrupt_raw":
                (target / "raw/response.json").write_bytes(b"{}")
            elif kind == "corrupt_derived":
                path = target / "derived/observations.csv"
                path.write_bytes(path.read_bytes() + b"extra\n")
                for item in receipt["artifacts"]:
                    if item["role"] == "data":
                        item["sha256"], item["byte_count"] = fingerprint(path)
            else:
                lineage = json.loads(receipt["lineage"]["extraction_or_query"])
                if kind == "model_promotion":
                    lineage["model_eligible"] = True
                elif kind == "unreviewed_code":
                    lineage["code_sha256"] = {"altered": "0" * 64}
                elif kind == "wrong_plan":
                    lineage["plan_sha256"] = "0" * 64
                receipt["lineage"]["extraction_or_query"] = json.dumps(lineage)
            (target / "receipt.json").write_bytes(encoded_json(receipt))
            verify_local(target / "receipt.json")

        for name, error in (
            ("corrupt_raw", "Artifact hash"),
            ("corrupt_derived", "CSV differs"),
            ("model_promotion", "hold differs"),
            ("unreviewed_code", "Unreviewed BLS code"),
            ("wrong_plan", "plan differs"),
        ):
            scenario(name, lambda name=name: altered(name), error)

        def partial_resume() -> None:
            partial = root / "partial"
            s3 = FakeS3()
            s3.corrupt = True
            with suppress(ValueError):
                execute(plan, batch, partial, True, True, s3, OUTPUTS, request)
            require(len(s3.objects) == 1, "Expected one interrupted upload")
            request_count = len(calls)
            s3.corrupt = False
            execute(plan, batch, partial, False, True, s3, OUTPUTS, request)
            require(len(calls) == request_count and len(s3.objects) == len(stored), "Partial resume duplicated effects")

        scenario("resume_interrupted_s3_without_refetch", partial_resume)
        scenario("offline_replay", lambda: verify_local(receipt_path))

        def swapped_capture() -> None:
            target = root / "swapped/batches" / other_batch["id"] / "capture"
            shutil.copytree(receipt_path.parent, target)
            execute(plan, other_batch, root / "swapped", False, True, client, OUTPUTS)

        scenario("valid_capture_in_wrong_batch_rejected", swapped_capture, "selected batch differs")

        def replaced_reconciliation() -> None:
            target = root / "tampered"
            shutil.copytree(run_root, target)
            branch = target / "batches" / batch["id"]
            rec = branch / "capture/s3_collections_reconciliation.json"
            evidence = read_json(rec)
            evidence["objects"][0]["object"]["version_id"] = ""
            rec.write_bytes(encoded_json(evidence))
            done = read_json(branch / "completed.json")
            done["reconciliation_sha256"] = fingerprint(rec)[0]
            (branch / "completed.json").write_bytes(encoded_json(done))
            execute(plan, batch, target, False, True, client, OUTPUTS)

        scenario("resealed_incomplete_storage_evidence_rejected", replaced_reconciliation, "version missing")

        def invalid_response(kind: str) -> None:
            data = json.loads(payload)
            rows = data["Results"]["series"][0]["data"]
            if kind == "missing_period":
                rows.pop()
            elif kind == "duplicate_period":
                rows.append(rows[0])
            elif kind == "extra_series":
                data["Results"]["series"].append(data["Results"]["series"][0])
            elif kind == "warning":
                data["message"] = ["Unexpected publisher warning"]
            elif kind == "unknown_token":
                rows[0]["value"] = "unknown"
            contract.validate_response(encoded_json(data), batch)

        for name, error in (
            ("missing_period", "missing year-period"),
            ("duplicate_period", "duplicate or unrequested"),
            ("extra_series", "extra series"),
            ("warning", "unreviewed warning"),
            ("unknown_token", "unknown value"),
        ):
            scenario(name, lambda name=name: invalid_response(name), error)

        def explicit_absence() -> None:
            data = json.loads(payload)
            sid = batch["series_ids"][0]
            data["Results"]["series"][0]["data"] = []
            data["message"] = [f"Series does not exist for Series {sid}"]
            _, stats = contract.validate_response(encoded_json(data), batch)
            require(stats["rows"] == 13 and stats["availability_holds"][0]["status"] == "availability_hold", "Absence was lost")

        scenario("explicit_absence_preserves_hold", explicit_absence)

        def year_absence(kind: str) -> None:
            scope = dict(batch, end_year=2001)
            data = json.loads(fixture(scope))
            item = data["Results"]["series"][0]
            sid = item["seriesID"]
            item["data"] = [row for row in item["data"] if row["year"] != "2000"]
            data["message"] = [f"No Data Available for Series {sid} Year: 2000"]
            if kind == "missing_evidence":
                data["message"] = []
            elif kind == "contradictory":
                item["data"] = json.loads(fixture(scope))["Results"]["series"][0]["data"]
            elif kind == "partial_year":
                item["data"].pop()
                data["message"].append(f"No Data Available for Series {sid} Year: 2001")
            elif kind == "all_years":
                item["data"] = []
                data["message"].append(f"No Data Available for Series {sid} Year: 2001")
            elif kind == "surplus":
                data["message"].append(f"No Data Available for Series {sid} Year: 1999")
            _, stats = contract.validate_response(encoded_json(data), scope)
            holds = stats["availability_holds"]
            require(all(h["status"] == "availability_hold" for h in holds), "Year absence cleared hold")
            require(stats["rows"] == (26 if kind == "all_years" else 39), "Available observations lost")
            require(stats["available_series"] == (1 if kind == "all_years" else 2), "Data-bearing series count differs")
            require([h["year"] for h in holds] == ([2000, 2001] if kind == "all_years" else [2000]), "Held years differ")

        for name, year_error in (
            ("explicit_year", None),
            ("missing_evidence", "absence evidence"),
            ("contradictory", "unreviewed warning"),
            ("partial_year", "missing year-period"),
            ("all_years", None),
            ("surplus", "unreviewed warning"),
        ):
            scenario(f"year_availability_{name}", lambda name=name: year_absence(name), year_error)

        def store_year_hold() -> None:
            data = json.loads(payload)
            sid = batch["series_ids"][0]
            data["Results"]["series"][0]["data"] = []
            data["message"] = [f"No Data Available for Series {sid} Year: 2000"]
            year_root, year_client = root / "year_hold", FakeS3()
            result = execute(plan, batch, year_root, True, True, year_client, OUTPUTS, lambda *_args: encoded_json(data))
            require(result["rows"] == 13, "Year hold storage count differs")
            before = inventory(year_root)
            execute(plan, batch, year_root, False, True, year_client, OUTPUTS)
            require(inventory(year_root) == before, "Year hold replay changed evidence")

        scenario("year_availability_storage_and_no_effect_replay", store_year_hold)

        def quota() -> None:
            now = datetime.now(UTC)
            for _ in range(3):
                transport.reserve(root / "quota", batch["id"], 2, now)

        scenario("quota_reserves_before_request", quota, "budget reached")

        def concurrency() -> None:
            with transport.collection_lock(root / "lock"), transport.collection_lock(root / "lock"):
                raise ValueError("Lock failed")

        scenario("concurrent_run_rejected", concurrency, "already running")

        def retry() -> None:
            attempts: list[int] = []

            def flaky(*_args: Any) -> bytes:
                attempts.append(1)
                if len(attempts) < 3:
                    raise transport.RetryableRequest("Synthetic network failure")
                return payload

            altered_plan = plan | {"request_limit_24h": 3}
            transport.fetch(altered_plan, batch, root / "retry", True, client, request=flaky, sleep=lambda _seconds: None)
            require(len(list((root / "retry/requests").glob("*.json"))) == 3, "Retries not counted")

        scenario("bounded_counted_network_retries", retry)
        scenario("secret_transport_and_echo_refusal", lambda: secret_scenarios(plan, batch, payload, root / "secret"))
    return scenarios


def secret_scenarios(plan: dict, batch: dict, payload: bytes, root: Path) -> None:
    """Exercise runtime credentials with a synthetic canary and HTTPS substitution only."""
    canary = "SYNTHETIC_BLS_CREDENTIAL_FOR_E2E"
    client = FakeS3()
    client.overrides["get-secret-value"] = {"SecretString": json.dumps({"api_key": canary})}

    class Connection:
        status = 200

        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            self.body = payload

        def request(self, method: str, path: str, body: bytes, headers: dict) -> None:
            require(method == "POST" and canary not in path and json.loads(body)["registrationkey"] == canary, "Credential transport invalid")

        def getresponse(self) -> Any:
            return self

        def getheader(self, _name: str, _default: str) -> str:
            return "application/json"

        def read(self, _size: int) -> bytes:
            return self.body

        def close(self) -> None:
            return

    connection = Connection()
    with patch.object(transport.http.client, "HTTPSConnection", return_value=connection):
        require(transport.request_live(plan, batch, client) == payload, "Synthetic credential request failed")
        execute(plan, batch, root, True, True, client, OUTPUTS)
        require(all(canary.encode() not in p.read_bytes() for p in root.rglob("*") if p.is_file()), "Credential persisted locally")
        require(all(canary.encode() not in value["body"] for value in client.objects.values()), "Credential persisted in S3")
        client.overrides["get-caller-identity"] = {"Account": "wrong", "Arn": "wrong"}
        count = len([c for c in client.calls if c[0] == "get-secret-value"])
        try:
            transport.request_live(plan, batch, client)
        except ValueError as error:
            require("identity differs" in str(error), "Identity rejection missing")
        else:
            raise ValueError("Wrong identity accepted")
        require(len([c for c in client.calls if c[0] == "get-secret-value"]) == count, "Secret read before identity check")
        client.overrides.pop("get-caller-identity")
        connection.body = encoded_json({"status": "REQUEST_SUCCEEDED", "echo": canary})
        try:
            transport.request_live(plan, batch, client)
        except ValueError as error:
            require("echoed credential" in str(error) and canary not in str(error), "Unsafe credential diagnostic")
        else:
            raise ValueError("Credential echo accepted")
        connection.status = 302
        try:
            transport.request_live(plan, batch, client)
        except ValueError as error:
            require("no redirect" in str(error), "Unexpected redirect outcome")
        else:
            raise ValueError("Redirect accepted")


def main() -> None:
    """Persist both passing and failing synthetic workflow evidence."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="bls_e2e_") as temporary:
        scenarios = exercise(Path(temporary))
    report = {
        "feature": "separate_bls_api_collector",
        "run_at_utc": datetime.now(UTC).isoformat(),
        "python": platform.python_version(),
        "code_sha256": contract.code_hashes(),
        "dependency_sha256": contract.digest(Path("requirements.txt").read_bytes()),
        "reproduce": f".venv/bin/python -m scripts.acquisition.run_bls_api_e2e --output {args.output}",
        "scenarios": scenarios,
        "passed": all(s["passed"] for s in scenarios),
        "boundary": "Synthetic BLS HTTPS and S3 adapters; real collector, validators, receipt and upload/version checks. No live services or credentials.",
        "cleanup": "Temporary synthetic files removed; no production data changed",
        "clean_checkout": "Deferred until end-of-acquisition commit",
    }
    write_once(args.output, encoded_json(report))
    sys.stdout.write(json.dumps({"passed": report["passed"], "scenarios": len(scenarios), "failures": [s for s in scenarios if not s["passed"]]}) + "\n")
    sys.exit(0 if report["passed"] else 1)


if __name__ == "__main__":
    main()
