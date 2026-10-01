"""Write the locked plan for ONC's meaningful-use attestation file, once, from the recorded private download."""

import sys

from scripts.acquisition import onc_mu_contract as contract
from scripts.acquisition.s3_store import encoded_json, write_once
from scripts.acquisition.source_registry import REPO_ROOT, canonical_hash, load_registry, read_json

DOWNLOADS_PATH = "data/acquisition_planning/onc_mu_downloads.json"
APPROVAL = "User decisions 2026-09-29: collect hospital rows only as a separate Medicare meaningful-use 2011-2017 series; the original stays on this Mac."


def build(record: dict) -> dict:
    """Bind the file to its recorded download; no network access."""
    plan = {
        "version": 1,
        "source_id": contract.SOURCE_ID,
        "scope": APPROVAL,
        "series": "Medicare meaningful use 2011-2017 (not Promoting Interoperability; C071-C074 holds kept)",
        "url": record["url"],
        "sha256": record["sha256"],
        "bytes": record["bytes"],
        "header": record["header"],
        "hospital_value": contract.HOSPITAL,
        "dropped_columns": contract.DROPPED,
        "limits": {"max_bytes": contract.MAX_BYTES, "max_seconds": contract.MAX_SECONDS},
        "downloads": DOWNLOADS_PATH,
        "registry_sha256": canonical_hash(load_registry()),
        "model_eligible": False,
    }
    return plan | {"id": contract.file_id(plan)}


def main() -> None:
    """Write the plan and its lock write-once, then prove it loads."""
    plan = build(read_json(REPO_ROOT / DOWNLOADS_PATH))
    write_once(contract.PLAN_PATH, encoded_json(plan))
    write_once(contract.PLAN_PATH.with_suffix(".lock.json"), encoded_json({"plan_sha256": canonical_hash(plan)}))
    contract.load_plan()
    sys.stdout.write(f"{contract.PLAN_PATH.relative_to(REPO_ROOT)} {canonical_hash(plan)}\n")


if __name__ == "__main__":
    main()
