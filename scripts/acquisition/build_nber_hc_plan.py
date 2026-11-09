"""Write the locked plan for NBER-mirrored CMS Hospital Compare releases, once, from reviewed hash and size inputs.

Each release is given as FOLDER=SHA256:BYTES:RELEASE_DATE. A release whose hash and size are not known yet is given with
--pending and is recorded as incomplete: it is never captured until a later plan binds it. Nothing is downloaded here.
"""

import argparse
import sys

from scripts.acquisition import nber_hc_contract as contract
from scripts.acquisition.s3_store import encoded_json, write_once
from scripts.acquisition.source_registry import REPO_ROOT, canonical_hash, load_registry, require


def release_entry(item: dict) -> dict:
    """Bind one release folder to its URL, hash, size, release date and the reviewed member map."""
    folder = item["folder"]
    require(folder in contract.MEMBER_MAP, "NBER release folder is not reviewed")
    year, month = folder.split("/")
    entry = {
        "folder": folder,
        "route_id": contract.route_id(folder),
        "source_id": contract.RECEIPT_SOURCE,
        "url": contract.zip_url(folder),
        "expected_sha256": item["sha256"],
        "bytes": item["bytes"],
        "release_label": f"Hospital Compare {year}-{month} (NBER mirror)",
        "release_date": item["release_date"],
        "members": dict(contract.MEMBER_MAP[folder]),
    }
    contract.check_release(entry)
    return entry | {"id": contract.release_id(entry)}


def build(releases: list[dict], gate: dict, pending: list[str]) -> dict:
    """The locked scope; no network access. Releases listed as pending carry a reason and no hash."""
    registry = load_registry()
    require(all(folder in contract.MEMBER_MAP for folder in pending), "NBER pending release folder is not reviewed")
    return {
        "version": 1,
        "source_ids": contract.SOURCE_IDS,
        "scope": contract.SCOPE,
        "registry_sha256": canonical_hash(registry),
        "model_eligible": False,
        "governance": contract.GOVERNANCE,
        "fidelity_gate": {
            "url": contract.zip_url(contract.GATE_FOLDER),
            "registry_route_id": contract.GATE_ROUTE_ID,
            "expected_sha256": gate["sha256"],
            "bytes": gate["bytes"],
        },
        "releases": sorted((release_entry(item) for item in releases), key=lambda r: r["folder"]),
        "pending_releases": [{"folder": folder, "reason": contract.PENDING_REASON} for folder in sorted(pending)],
    }


def parse_release(text: str) -> dict:
    """Read FOLDER=SHA256:BYTES:RELEASE_DATE."""
    folder, _, rest = text.partition("=")
    digest, size, date = rest.split(":")
    return {"folder": folder, "sha256": digest, "bytes": int(size), "release_date": date}


def main() -> None:
    """Write the plan and its lock write-once, then prove it loads."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--release", action="append", default=[], type=parse_release)
    parser.add_argument("--pending", action="append", default=[])
    parser.add_argument("--gate-bytes", type=int, required=True)
    args = parser.parse_args()
    plan = build(args.release, {"sha256": contract.gate_sha256(), "bytes": args.gate_bytes}, args.pending)
    write_once(contract.PLAN_PATH, encoded_json(plan))
    write_once(contract.PLAN_PATH.with_suffix(".lock.json"), encoded_json({"plan_sha256": canonical_hash(plan)}))
    contract.load_plan()
    sys.stdout.write(
        f"{contract.PLAN_PATH.relative_to(REPO_ROOT)} {canonical_hash(plan)} {len(plan['releases'])} releases {len(plan['pending_releases'])} pending\n"
    )


if __name__ == "__main__":
    main()
