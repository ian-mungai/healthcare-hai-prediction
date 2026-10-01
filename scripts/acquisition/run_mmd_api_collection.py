"""Resume approved MMD collection sequentially, preserving completed browser history."""

import argparse
import http.client
import json
import logging
import sys
import time
from collections.abc import Callable
from datetime import UTC, datetime
from functools import partial
from pathlib import Path

from scripts.acquisition.capture import receipt_validator, validate_receipt
from scripts.acquisition.collect_mmd_api import execute
from scripts.acquisition.mmd_api_contract import PLAN_PATH, PLANS, load_plan, verify_capture
from scripts.acquisition.s3_store import StorageError, encoded_json, fingerprint, write_once
from scripts.acquisition.source_registry import load_registry, read_json, require

LOGGER = logging.getLogger(__name__)
BROWSER_ROOT = Path("data/historical_acquisition/mmd_browser_history")
ATTEMPTS = 3
RETRY_WAIT_SECONDS = 30


def transient(error: Exception) -> bool:
    """Only network-level failures retry; data, scope, permission and plan errors stop at once."""
    if isinstance(error, StorageError):
        return error.code in {"request_failed", "request_unconfirmed"}
    return isinstance(error, (OSError, http.client.HTTPException))


def with_retries(action: Callable[[], object], retries: list[dict], label: str, sleep: Callable[[float], None] = time.sleep) -> None:
    """Repeat one idempotent item with growing waits; write-once storage makes each attempt safe."""
    for attempt in range(1, ATTEMPTS + 1):
        try:
            action()
            return
        except Exception as error:
            if attempt == ATTEMPTS or not transient(error):
                raise
            retries.append({"item": label, "attempt": attempt, "error": f"{type(error).__name__}: {error}"})
            LOGGER.warning("RETRY %s attempt=%s %s", label, attempt, error)
            sleep(RETRY_WAIT_SECONDS * attempt)


def approved_conditions() -> list[dict]:
    """Combine the conditions of every hash-locked plan; each measure appears in exactly one."""
    conditions = [c for sha in PLANS for c in load_plan(sha)["conditions"]]
    require(len({c["measure_id"] for c in conditions}) == len(conditions), "Condition is not in exactly one approved plan")
    return conditions


def verify_completion(path: Path, api: bool) -> dict:
    """Reconcile local originals against complete, version-specific storage evidence."""
    completion = read_json(path)
    require(completion["model_eligible"] is False, "Completed capture cleared model hold")
    reconciliation_path = Path(completion["reconciliation_path"])
    root = reconciliation_path.parent
    require(root.resolve().is_relative_to(path.parent.resolve()), "Completed capture points outside its branch")
    receipt = read_json(root / "receipt.json")
    reconciliation = read_json(reconciliation_path)
    validate_receipt(receipt, receipt_validator(), root)
    require(receipt["snapshot_id"] == reconciliation["snapshot_id"], "Stored snapshot differs")
    selections = receipt["acquisition"]["export_selections"]
    measure_id = completion.get("measure_id")
    if measure_id is None:
        require(selections["condition"] == "Acute Myocardial Infarction", "Unknown legacy completion")
        measure_id = "C258.01"
    year = int(selections["year"])
    require(completion.get("year", year) == year, "Completion year differs")
    lineage = json.loads(receipt["lineage"]["extraction_or_query"])
    if api:
        source = next(s for s in load_registry(expected_sha256=lineage["registry_sha256"])["sources"] if s["source_id"] == "MMD")
        verify_capture(receipt, source, lineage, root, False)
        require(lineage["measure_id"] == measure_id and lineage["year"] == year, "API completion lineage differs")
    objects = {item["storage_path"]: item["object"] for item in reconciliation["objects"]}
    for artifact in receipt["artifacts"]:
        expected = objects[artifact["storage_path"]]
        require(fingerprint(root / artifact["storage_path"]) == (expected["sha256"], expected["byte_count"]), "Stored object/local bytes differ")
    for item in reconciliation["objects"] + reconciliation["manifests"]:
        obj = item["object"]
        require(bool(obj["version_id"]) and obj["verification"] == "version_get_sha256_and_length_match", "Storage lacks immutable version readback")
    return {
        "measure_id": measure_id,
        "year": year,
        "rows": receipt["schema_profile"]["row_count"],
        "completion_path": str(path),
        "receipt_path": str(root / "receipt.json"),
    }


def inventory() -> tuple[dict, dict, list[dict]]:
    """Classify all approved slots without downloading a previously completed year."""
    plan, conditions = load_plan(), approved_conditions()
    browser, api = {}, {}
    for path in sorted(BROWSER_ROOT.glob("*/completed.json")):
        item = verify_completion(path, False)
        key = (item["measure_id"], item["year"])
        require(key not in browser, "Duplicate browser completion requires review")
        browser[key] = item
    for path in sorted(PLAN_PATH.parent.glob("c258_*/*/completed.json")):
        item = verify_completion(path, True)
        key = (item["measure_id"], item["year"])
        require(key not in api and key not in browser, "Duplicate completed condition-year requires review")
        api[key] = item
    held = []
    anemia = read_json(BROWSER_ROOT / "c258_03_coverage_2012_2023.json")
    require(sorted(anemia["not_offered_years"]) == list(range(2012, 2022)), "Anemia absence evidence changed")
    for condition in conditions:
        for year in range(2023, 2011, -1):
            if year not in condition["available_years"]:
                held.append(
                    {
                        "measure_id": condition["measure_id"],
                        "year": year,
                        "status": "not_offered" if condition["measure_id"] == "C258.03" else "availability_hold",
                        "reason": "Publisher menu restrictions; only Anemia absence closure is user-approved",
                    }
                )
    held.extend(
        {"measure_id": mid, "year": year, "status": "registry_hold", "reason": "Addition validation, lock and consumer integration incomplete"}
        for mid in plan["excluded_unregistered_ids"]
        if mid not in {c["measure_id"] for c in conditions}
        for year in range(2012, 2024)
    )
    return browser, api, held


def main() -> None:
    """Run until the eligible queue finishes or its first unresolved failure occurs."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--max-new", type=int)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    require(not args.output.exists(), "Choose a new output path; existing run evidence is immutable")
    require(args.max_new is None or args.max_new > 0, "--max-new must be positive")
    result: dict = {
        "started_at_utc": datetime.now(UTC).isoformat(),
        "plan_sha256": sorted(PLANS),
        "browser_completed": [],
        "api_previously_completed": [],
        "newly_stored": [],
        "held": [],
        "initial_pending_count": None,
        "remaining_pending": [],
        "active_item": None,
        "model_eligible": False,
        "status": "planned",
        "network_scope": "MMD only; existing project S3 destination",
        "reproduce": f".venv/bin/python -m scripts.acquisition.run_mmd_api_collection --output {args.output.parent}/rerun_{args.output.name}"
        + (" --execute" if args.execute else "")
        + (f" --max-new {args.max_new}" if args.max_new is not None else ""),
        "script_sha256": fingerprint(Path(__file__))[0],
        "error": None,
        "retries": [],
    }
    try:
        conditions = approved_conditions()
        browser, api, held = inventory()
        pending = [
            (c["measure_id"], year)
            for c in conditions
            for year in sorted(c["available_years"], reverse=True)
            if (c["measure_id"], year) not in browser and (c["measure_id"], year) not in api
        ]
        result.update(
            browser_completed=list(browser.values()),
            api_previously_completed=list(api.values()),
            held=held,
            initial_pending_count=len(pending),
            remaining_pending=[{"measure_id": m, "year": y} for m, y in pending],
        )
        LOGGER.info("browser_stored=%s api_stored=%s ready=%s held=%s", len(browser), len(api), len(pending), len(held))
        if args.execute:
            result["status"] = "running"
            for index, (measure_id, year) in enumerate(pending):
                if args.max_new is not None and index >= args.max_new:
                    result["status"] = "bounded_batch_complete"
                    break
                LOGGER.info("START %s %s remaining=%s", measure_id, year, len(pending) - index)
                result["active_item"] = {"measure_id": measure_id, "year": year}
                with_retries(partial(execute, measure_id, year, True, True), result["retries"], f"{measure_id} {year}")
                path = PLAN_PATH.parent / measure_id.lower().replace(".", "_") / str(year) / "completed.json"
                item = verify_completion(path, True)
                result["newly_stored"].append(item)
                result["remaining_pending"] = [{"measure_id": m, "year": y} for m, y in pending[index + 1 :]]
                result["active_item"] = None
                LOGGER.info("STORED %s %s rows=%s", measure_id, year, item["rows"])
            else:
                result["status"] = "eligible_queue_complete"
    except Exception as error:
        result.update(status="blocked", error=f"{type(error).__name__}: {error}")
        LOGGER.error("STOP %s", result["error"])
    finally:
        result["finished_at_utc"] = datetime.now(UTC).isoformat()
        write_once(args.output, encoded_json(result))
    sys.stdout.write(
        json.dumps(
            {
                "status": result["status"],
                "newly_stored": len(result["newly_stored"]),
                "remaining": len(result["remaining_pending"]),
                "artifact": str(args.output),
            }
        )
        + "\n"
    )
    require(result["status"] != "blocked", "MMD collection stopped; inspect the saved error artifact")


if __name__ == "__main__":
    main()
