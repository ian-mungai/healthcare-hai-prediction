"""Write the locked plan for the HCAI annual utilization workbooks, 2018-2025, once, from the recorded downloads."""

import re
import sys
import urllib.parse

from scripts.acquisition import hcai_util_contract as contract
from scripts.acquisition.bls_api_contract import digest
from scripts.acquisition.s3_store import encoded_json, write_once
from scripts.acquisition.source_registry import REPO_ROOT, canonical_hash, load_registry, read_json, require
from scripts.acquisition.transport import SIGNED_REDIRECT_ROUTES

DOWNLOADS_PATH = "data/acquisition_planning/hcai_util_2018_2025_downloads.json"
APPROVAL = (
    "User decisions 2026-09-29: extend the 2012-2017 public-business-data exception to the 2018-2025 annual files; collect 2018-2024 "
    "and the 2025 preliminary file; originals on this Mac only; abstracted sheet CSVs only in S3; columns checked on capture."
)


def redirect_target(url: str) -> str:
    """The exact unsigned destination in the publisher bucket already reviewed for HCAI."""
    match = re.fullmatch(r"/dataset/[^/]+/resource/([^/]+)/download/([^/]+)", urllib.parse.urlsplit(url).path)
    require(match is not None, "HCAI utilization download URL differs")
    bucket = next(iter(SIGNED_REDIRECT_ROUTES.values())).split("/resources/")[0]
    return f"{bucket}/resources/{match[1] if match else ''}/{match[2] if match else ''}"


def build(downloads: dict, min_hospitals: int = contract.MIN_HOSPITALS) -> dict:
    """Bind every year to its recorded original, sheets and data header; no network access."""
    years = []
    for record in sorted(downloads["downloads"], key=lambda r: r["year"]):
        entry = {
            "year": record["year"],
            "resource_id": record["resource_id"],
            "title": record["title"],
            "url": record["url"],
            "redirect_target": redirect_target(record["url"]),
            "sha256": record["sha256"],
            "bytes": record["bytes"],
            "sheets": record["sheets"],
            "header": record["data_header"],
            "min_hospitals": min_hospitals,
            "preliminary": "Preliminary" in record["title"],
        }
        years.append(entry | {"id": contract.year_id(entry)})
    release = contract.RELEASE_PATH
    return {
        "version": 1,
        "source_id": contract.SOURCE_ID,
        "scope": APPROVAL,
        "redact": contract.REDACT,
        "not_personal": contract.NOT_PERSONAL,
        "individual_owner_redact": contract.OWNER_REDACT,
        "replacement": contract.REPLACEMENT,
        "access_release": {"path": release, "sha256": digest((REPO_ROOT / release).read_bytes())},
        "downloads": DOWNLOADS_PATH,
        "registry_sha256": canonical_hash(load_registry()),
        "model_eligible": False,
        "years": years,
    }


def main() -> None:
    """Write the plan and its lock write-once, then prove it loads."""
    plan = build(read_json(REPO_ROOT / DOWNLOADS_PATH))
    require(len(plan["years"]) == 8, "HCAI utilization plan must cover 2018-2025")
    write_once(contract.PLAN_PATH, encoded_json(plan))
    write_once(contract.PLAN_PATH.with_suffix(".lock.json"), encoded_json({"plan_sha256": canonical_hash(plan)}))
    contract.load_plan()
    sys.stdout.write(f"{contract.PLAN_PATH.relative_to(REPO_ROOT)} {canonical_hash(plan)} {len(plan['years'])} years\n")


if __name__ == "__main__":
    main()
