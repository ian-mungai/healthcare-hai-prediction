"""Compare stored MMD browser exports with CSVs rebuilt from the MMD API; no S3 access or original changes."""

import argparse
import hashlib
import json
import logging
import sys
from datetime import UTC, datetime
from pathlib import Path

from scripts.acquisition.collect_mmd_api import fetch
from scripts.acquisition.mmd_api_contract import condition_for, current_code_hashes, digest, load_plan, reconstruct, request_parameters, url_for, validate_rows
from scripts.acquisition.run_mmd_api_collection import BROWSER_ROOT, verify_completion
from scripts.acquisition.s3_store import encoded_json, write_once
from scripts.acquisition.source_registry import read_json, require

LOGGER = logging.getLogger(__name__)
EVIDENCE_ROOT = Path("data/e2e/mmd_browser_api_parity/20260926")
# C258.01 (AMI) already has its own twelve-year parity report; this run covers the other browser exports.
COMPARED_ELSEWHERE = {"C258.01"}


def differences(browser: bytes, derived: bytes, limit: int = 5) -> dict:
    """Summarize line-level differences for review without altering either file."""
    left, right = browser.decode().split("\r\n"), derived.decode().split("\r\n")
    changed = [{"line": i + 1, "browser": a, "api": b} for i, (a, b) in enumerate(zip(left, right, strict=False)) if a != b]
    return {"browser_lines": len(left), "api_lines": len(right), "differing_lines": len(changed), "examples": changed[:limit]}


def compare(item: dict, plan: dict, allow_network: bool) -> dict:
    """Rebuild one browser-stored condition-year from the API response and compare the bytes."""
    measure_id, year = item["measure_id"], item["year"]
    condition = condition_for(plan, measure_id, year)
    parameters = request_parameters(plan, condition, year)
    receipt_path = Path(item["receipt_path"])
    data = next(a for a in read_json(receipt_path)["artifacts"] if a["role"] == "data")
    browser = (receipt_path.parent / data["storage_path"]).read_bytes()
    require(digest(browser) == data["sha256"], "Browser original changed")
    transport = EVIDENCE_ROOT / measure_id.lower().replace(".", "_") / str(year) / "transport"
    raw, meta = fetch(transport, "main", url_for(parameters), allow_network)
    rows, _ = validate_rows(raw, parameters)
    derived, _ = reconstruct(plan, condition, year, rows)
    result = {
        "measure_id": measure_id,
        "year": year,
        "browser_sha256": data["sha256"],
        "api_raw_sha256": meta["sha256"],
        "api_retrieved_at_utc": meta["retrieved_at_utc"],
        "derived_sha256": digest(derived),
        "api_rows": len(rows),
        "identical": derived == browser,
    }
    if not result["identical"]:
        result["differences"] = differences(browser, derived)
    return result


def main() -> None:
    """Compare every non-AMI browser export; --fetch permits first-time MMD API reads."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--fetch", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    require(not args.output.exists(), "Choose a new output path; existing run evidence is immutable")
    plan = load_plan()
    items = [verify_completion(path, False) for path in sorted(BROWSER_ROOT.glob("*/completed.json"))]
    items = sorted((i for i in items if i["measure_id"] not in COMPARED_ELSEWHERE), key=lambda i: (i["measure_id"], i["year"]))
    results = []
    for item in items:
        try:
            results.append(compare(item, plan, args.fetch))
        except (ValueError, OSError, KeyError, TypeError, StopIteration) as error:
            results.append({"measure_id": item["measure_id"], "year": item["year"], "identical": False, "error": f"{type(error).__name__}: {error}"})
        LOGGER.info("%s %s identical=%s", item["measure_id"], item["year"], results[-1]["identical"])
    report = {
        "created_at_utc": datetime.now(UTC).isoformat(),
        "scope": "MMD browser exports other than C258.01, compared with CSVs rebuilt from MMD API responses. No S3 access; originals unchanged.",
        "compared": len(results),
        "identical": sum(r["identical"] for r in results),
        "errors": sum("error" in r for r in results),
        "results": results,
        "collector_code_sha256": current_code_hashes(),
        "script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "python_version": sys.version,
        "model_eligible": False,
        "reproduce": f".venv/bin/python -m scripts.acquisition.compare_mmd_browser_api --output {args.output.parent}/rerun_{args.output.name}",
    }
    write_once(args.output, encoded_json(report))
    sys.stdout.write(
        json.dumps({"compared": report["compared"], "identical": report["identical"], "errors": report["errors"], "artifact": str(args.output)}) + "\n"
    )


if __name__ == "__main__":
    main()
