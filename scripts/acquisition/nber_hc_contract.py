"""Frozen scope and checks for CMS Hospital Compare release zips copied from the NBER mirror (data.nber.org).

CMS's own archive serves nothing before 2019-03-04, so earlier releases come from NBER's byte-identical copies of the CMS zips.
A plan binds each release to its URL, SHA-256 and size. A fidelity gate compares NBER's March 2019 zip with the CMS archive hash
recorded for route care-hcahps-cms:file:001, and a failed gate stops a run before any release is downloaded. Only the members
named in MEMBER_MAP get the data role and a legacy dataset ID; every other member stays an unloaded reference.
Classification: Public (hospital-level aggregates, no persons).
"""

import re
import zipfile
from pathlib import Path
from urllib.parse import urlsplit

from scripts.acquisition.bls_api_contract import digest
from scripts.acquisition.code_closure import closure_hashes
from scripts.acquisition.code_versions import read_code_versions
from scripts.acquisition.collection_layout import load_routes
from scripts.acquisition.legacy_versions import plan_matches
from scripts.acquisition.mapped_archives import expand_tree
from scripts.acquisition.source_registry import REPO_ROOT, canonical_hash, load_registry, read_json, require

PLAN_PATH = REPO_ROOT / "config/acquisition/nber_hospital_compare_plan.json"
VERSIONS_PATH = REPO_ROOT / "config/acquisition/nber_hc_code_versions.json"
GATE_SOURCE_PATH = REPO_ROOT / "config/acquisition/planning_v1/acquisition_plans_v1.json"
CODE_ENTRY = "scripts.acquisition.store_nber_hc"
HOST = "data.nber.org"
BASE_URL = f"https://{HOST}/compare/hospital/"
PUBLISHER = "Centers for Medicare & Medicaid Services"
TERMS_URL = "https://www.usa.gov/government-works"
RECEIPT_SOURCE = "care-timely-cms"
SOURCE_IDS = ["care-hcahps-cms", "care-timely-cms"]
GATE_FOLDER = "2019/03"
GATE_ROUTE_ID = "care-hcahps-cms:file:001"
MODE = "nber_hospital_compare"
SCOPE = "Original CMS flat-file zips from the NBER mirror; no NBER conversions."
PENDING_REASON = "The zip hash and size are not recorded yet; add them with the plan builder before this release is captured."
HCAHPS_MEMBER, TIMELY_MEMBER, HAI_MEMBER = (
    "HCAHPS - Hospital.csv",
    "Timely and Effective Care - Hospital.csv",
    "Healthcare Associated Infections - Hospital.csv",
)
HCAHPS_ID = "cms_legacy_hcahps_hospital"
TIMELY_ID = "cms_legacy_timely_and_effective_care_hospital"
HAI_ID = "cms_legacy_healthcare_associated_infections_hospital"
# The reviewed map. Any member not listed here, in any release, stays an unloaded reference.
MEMBER_MAP = {
    "2017/10": {HAI_MEMBER: HAI_ID},
    "2018/07": {TIMELY_MEMBER: TIMELY_ID},
    "2018/10": {HCAHPS_MEMBER: HCAHPS_ID, TIMELY_MEMBER: TIMELY_ID, HAI_MEMBER: HAI_ID},
}
GOVERNANCE = {
    "access_class": "public",
    "contains_phi": False,
    "contains_pii": False,
    "credential_reference": None,
    "credentials_required": False,
    "license_or_terms_url": TERMS_URL,
    "use_restrictions": [
        "U.S. government work (CMS flat files); the NBER mirror adds no rights and NBER's site terms are not separately reviewed.",
        "Original CMS zip bytes from the NBER mirror, checked against the CMS archive by the March 2019 fidelity gate; no NBER conversions are captured.",
        "Preserve all source and measure holds; no model approval.",
    ],
}


def require_code(hashes: dict | None = None) -> dict:
    """Accept only an explicitly reviewed implementation version of the collector and every module it can run."""
    actual = closure_hashes(CODE_ENTRY) if hashes is None else hashes
    require(bool(actual) and actual in [v["code_sha256"] for v in read_code_versions(VERSIONS_PATH)["versions"]], "Unreviewed NBER capture code version")
    return actual


def zip_name(folder: str) -> str:
    """The zip name NBER uses for a year/month folder."""
    return f"hospitals_compare_{folder.replace('/', '')}.zip"


def zip_url(folder: str) -> str:
    """The NBER URL of a release zip."""
    return f"{BASE_URL}{folder}/{zip_name(folder)}"


def route_id(folder: str) -> str:
    """The route name of one NBER release; it is not in the base registry, which stays unchanged."""
    return f"{RECEIPT_SOURCE}:nber:{folder.replace('/', '-')}"


def release_id(release: dict) -> str:
    """Identify one release by everything bound to it, including its bytes."""
    return canonical_hash({k: v for k, v in release.items() if k != "id"})


def release_date_from_zip(path: Path) -> str:
    """The owner's rule for the release date: the newest member modification day in the zip."""
    with zipfile.ZipFile(path) as archive:
        stamps = [info.date_time for info in archive.infolist() if not info.is_dir()]
    require(bool(stamps), "NBER release zip has no members")
    year, month, day = max(stamps)[:3]
    return f"{year:04}-{month:02}-{day:02}"


def registry_gate_sha256() -> str:
    """The CMS archive zip hash recorded for the gate route in the locked planning record."""
    routes = [r for r in read_json(GATE_SOURCE_PATH)["routes"] if r["route_id"] == GATE_ROUTE_ID]
    require(len(routes) == 1, "NBER gate route differs")
    return routes[0]["capture_plan"]["expected_sha256"]


def gate_sha256() -> str:
    """The hash the fidelity gate must find."""
    return registry_gate_sha256()


def check_release(release: dict) -> None:
    """Refuse a release entry that is off the NBER host, outside the reviewed member map or incompletely bound."""
    folder = release["folder"]
    require(folder in MEMBER_MAP, "NBER release folder is not reviewed")
    require(urlsplit(release["url"]).hostname == HOST and release["url"] == zip_url(folder), "NBER release URL is not on the NBER host")
    require(release["members"] == MEMBER_MAP[folder], "NBER member map differs")
    require(release["route_id"] == route_id(folder) and release["source_id"] == RECEIPT_SOURCE, "NBER release route differs")
    require(re.fullmatch(r"[0-9a-f]{64}", release["expected_sha256"]) is not None and release["bytes"] > 0, "NBER release hash or size invalid")
    require(re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", release["release_date"]) is not None, "NBER release date invalid")
    year, month = folder.split("/")
    require(release["release_label"] == f"Hospital Compare {year}-{month} (NBER mirror)", "NBER release label differs")


def load_plan(path: Path | None = None) -> dict:
    """Verify the locked scope, every release binding, the gate binding and the base registry."""
    path = PLAN_PATH if path is None else path
    plan = read_json(path)
    require(read_json(path.with_suffix(".lock.json"))["plan_sha256"] == canonical_hash(plan), "NBER plan lock differs")
    require(plan["version"] == 1 and plan["model_eligible"] is False and plan["source_ids"] == SOURCE_IDS, "NBER plan hold differs")
    require(plan["scope"] == SCOPE and plan["governance"] == GOVERNANCE, "NBER plan scope or governance differs")
    registry = load_registry(expected_sha256=plan["registry_sha256"])
    require(plan["registry_sha256"] == canonical_hash(registry), "NBER registry differs")
    sources = {s["source_id"]: s for s in registry["sources"]}
    require(set(SOURCE_IDS) <= set(sources), "NBER plan source is not in the registry")
    require(any(r["route_id"] == GATE_ROUTE_ID for r in sources["care-hcahps-cms"]["file_routes"]), "NBER gate route differs")
    gate = plan["fidelity_gate"]
    require(gate["url"] == zip_url(GATE_FOLDER) and gate["registry_route_id"] == GATE_ROUTE_ID, "NBER fidelity gate differs")
    require(gate["expected_sha256"] == gate_sha256() and gate["bytes"] > 0, "NBER fidelity gate hash differs from the CMS archive record")
    require(bool(plan["releases"]), "NBER plan has no releases")
    folders = [r["folder"] for r in plan["releases"]]
    require(len(set(folders)) == len(folders) and len({r["id"] for r in plan["releases"]}) == len(folders), "NBER plan releases repeat")
    for release in plan["releases"]:
        check_release(release)
        require(release["id"] == release_id(release), "NBER release binding differs")
    pending = plan["pending_releases"]
    require(
        all(set(p) == {"folder", "reason"} and p["folder"] in MEMBER_MAP and p["folder"] not in folders and p["reason"] == PENDING_REASON for p in pending),
        "NBER pending release invalid",
    )
    return plan


def archive_mapping(receipt_path: Path, registry: dict, release: dict) -> dict:
    """The checksum-bound member map of a captured release zip: mapped members are data, every other member is a reference."""
    receipt = read_json(receipt_path)
    parent = receipt["artifacts"][0]
    leaves = expand_tree(receipt_path.parent / parent["storage_path"], receipt_path.parent / "mapped_expanded" / parent["sha256"], parent["sha256"])
    top = {chain[0] for chain in leaves if len(chain) == 1}
    require(set(release["members"]) <= top, "NBER mapped member missing from the release zip")
    members = [
        {
            "member_chain": list(chain),
            "sha256": leaf["sha256"],
            "bytes": leaf["byte_count"],
            "role": "data" if len(chain) == 1 and chain[0] in release["members"] else "reference",
            "dataset_id": release["members"].get(chain[0]) if len(chain) == 1 else None,
            "mapping_basis": "Reviewed NBER member map; every other member stays an unloaded reference.",
        }
        for chain, leaf in sorted(leaves.items())
    ]
    route = load_routes(registry)[receipt["source"]["source_record_id"]]
    return {
        "registry_sha256": canonical_hash(registry),
        "sha256": parent["sha256"],
        "bytes": parent["byte_count"],
        "members": members,
        "source_ids": [receipt["source"]["source_record_id"]],
        "route_ids": [release["route_id"]],
        "url": receipt["acquisition"]["requested_url"],
        "release_date": receipt["release"]["release_date"],
        "collection_root": f"{route['publisher']}/{route['collection']}",
    }


def verify_capture(receipt: dict, source: dict, lineage: dict, root: Path, evidence_only: bool) -> None:
    """Check the stored zip, its route and its bindings against the locked plan and the preserved holds."""
    from scripts.acquisition.capture import receipt_validator

    require(not evidence_only and source["source_id"] == RECEIPT_SOURCE and lineage.get("mode") == MODE, "NBER storage source differs")
    plan = load_plan()
    require(plan_matches(plan, lineage["plan_sha256"]), "NBER capture plan differs")
    require(lineage["registry_sha256"] == plan["registry_sha256"], "NBER registry differs")
    require(lineage["schema_sha256"] == canonical_hash(receipt_validator().schema), "NBER schema differs")
    require(lineage["model_eligible"] is False and receipt["snapshot_status"] == "acquired_unvalidated", "NBER modeling hold differs")
    require_code(lineage["code_sha256"])
    matches = [r for r in plan["releases"] if r["id"] == lineage["release_id"]]
    require(len(matches) == 1, "NBER capture release not in its plan")
    release = matches[0]
    require(lineage["route_id"] == release["route_id"] and lineage["gate_sha256"] == plan["fidelity_gate"]["expected_sha256"], "NBER capture lineage differs")
    acq = receipt["acquisition"]
    require(acq["requested_url"] == release["url"] and acq["http_status"] == 200 and urlsplit(acq["resolved_url"]).hostname == HOST, "NBER route differs")
    require(
        receipt["source"]["publisher"] == PUBLISHER and {k: receipt["governance"][k] for k in GOVERNANCE} == GOVERNANCE,
        "NBER publisher or governance differs",
    )
    artifacts = receipt["artifacts"]
    path = f"raw/{zip_name(release['folder'])}"
    require(len(artifacts) == 1 and artifacts[0]["storage_path"] == path and artifacts[0]["role"] == "data", "NBER artifact set differs")
    raw = (root / path).read_bytes()
    require(digest(raw) == release["expected_sha256"] == lineage["expected_sha256"] and len(raw) == release["bytes"], "NBER stored zip differs from the plan")
    stated = receipt["release"]
    require(
        stated["release_date"] == release["release_date"] == release_date_from_zip(root / path)
        and stated["publisher_release_label"] == release["release_label"],
        "NBER release date differs from the member modification rule",
    )
