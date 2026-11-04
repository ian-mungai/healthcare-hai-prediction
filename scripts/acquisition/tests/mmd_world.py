"""A synthetic MMD world for the task path: the publisher's API, a locked plan with its references and the stored captures of both variants.

The code root stand-in is the folder of the case. It holds copies of the collector files the capture fingerprints, the plan, its references, the baseline of
the year and the plan template, so the capture reads them by relative path as it does in the task. Nothing here reaches a real network or a real bucket.
"""

from __future__ import annotations

import hashlib
import http.client
import json
import shutil
from collections.abc import Callable
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit

import pytest

from scripts.acquisition import collect_mmd_api as collector
from scripts.acquisition import mmd_api_contract as contract
from scripts.acquisition import redownload as harness
from scripts.acquisition.run_redownload_e2e import Fixture
from scripts.acquisition.s3_store import encoded_json
from scripts.acquisition.source_registry import REPO_ROOT, canonical_hash, load_registry, read_json
from tests.support import check

YEAR = 2023
YEAR_CODE = "23_p"
API_MEASURE, MOVED_MEASURE = "C258.07", "C258.08"
CONDITIONS = {
    API_MEASURE: {"condition_code": "3", "condition_label": "Atrial Fibrillation", "domain_label": "Primary chronic conditions"},
    MOVED_MEASURE: {"condition_code": "4", "condition_label": "Asthma", "domain_label": "Primary chronic conditions"},
}
COUNTIES = {1001: "Autauga", 1003: "Baldwin", 1005: "Barbour"}
RATES = ["10.5", "11.5", "12.5"]
PLAN_FOLDER = Path("data/mmd")
PLAN_FILE = PLAN_FOLDER / "collection_plan.json"
TEMPLATE_FILE = PLAN_FOLDER / "template_plan.json"
REPORT_FILE = PLAN_FOLDER / "report.json"
BASELINE_FILE = PLAN_FOLDER / f"baseline_{YEAR}.csv"
MOVED_FLAG = "browser_export_moved_to_api:parity_verified"
PAGE_URL = "https://data.cms.gov/mmd-population/map.html"
STORED_SOURCE = "MMD"


def digest(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


class Response:
    status = 200

    def __init__(self, body: bytes) -> None:
        self.left = body

    def getheader(self, name: str, default: str = "") -> str:
        return "application/json" if name == "Content-Type" else default

    def read(self, size: int = -1) -> bytes:
        part, self.left = self.left[:size], self.left[size:]
        return part


class Connection:
    def __init__(self, cms: Cms) -> None:
        self.cms, self.response = cms, Response(b"[]")

    def request(self, method: str, target: str, headers: dict | None = None) -> None:
        self.response = Response(self.cms.answer(method, target))

    def getresponse(self) -> Response:
        return self.response

    def close(self) -> None:
        return None


class Cms:
    """The publisher's API. It records every request and serves the rows its table holds for the requested condition."""

    def __init__(self) -> None:
        self.rates = {item["condition_code"]: list(RATES) for item in CONDITIONS.values()}
        self.requests: list[str] = []

    def connect(self, netloc: str, timeout: float = 0) -> Connection:
        check(netloc == "data.cms.gov", f"the stand-in serves one host: {netloc}")
        return Connection(self)

    def rows(self, condition: str) -> list[dict]:
        keys = {"agecat": ".", "dual": ".", "eligcat": ".", "fltr": "1", "geography": "c", "measure": "v", "racecat": ".", "sexcat": ".", "year": YEAR_CODE}
        return [
            keys | {"condition": condition, "dencat": "5", "fips": f"{fips:05}", "rate": rate}
            for fips, rate in zip(COUNTIES, self.rates[condition], strict=True)
        ]

    def answer(self, method: str, target: str) -> bytes:
        check(method == "GET", f"the stand-in answers reads only: {method}")
        self.requests.append(target)
        query = parse_qs(urlsplit(target).query, keep_blank_values=True)
        rows = self.rows(query["condition"][0])
        if query["_size"][0] != "2":
            return json.dumps(rows).encode()
        offset = int(query["_offset"][0])
        return json.dumps(rows[offset : offset + 2]).encode()


def reference_files() -> dict[str, bytes]:
    crosswalk = "population,measure,year,elig,race_code,sex_code,adjust,dual,url\nf,v," + YEAR_CODE + ",.,.,.,1,.,synthetic_source\n"
    names = "id\tname\n1\tAlabama\n" + "".join(f"{fips}\t{name}\n" for fips, name in COUNTIES.items())
    return {
        "menus.js": f"var menu = {{'disp': \"{YEAR}\", 'val': \"{YEAR_CODE}\"}};\n".encode(),
        "codebook_crosswalk.csv": crosswalk.encode(),
        "countynames.tsv": names.encode(),
        "urban.tsv": b"id\tname\n1001\turban\n",
    }


class World:
    """The code root stand-in and the plan, with the stored captures of one API unit and one moved browser unit."""

    def __init__(self, folder: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        self.folder, self.monkeypatch, self.cms = folder, monkeypatch, Cms()
        monkeypatch.chdir(folder)
        self.write_code_root(folder)
        self.write_plan(folder)
        monkeypatch.setattr(contract, "PLANS", {self.plan_sha: PLAN_FILE})
        monkeypatch.setattr(collector, "PLANS", contract.PLANS)
        monkeypatch.setattr(contract, "PLAN_PATH", PLAN_FILE)
        monkeypatch.setattr(collector, "TEMPLATE", TEMPLATE_FILE)
        approved = [contract.current_code_hashes()]
        monkeypatch.setattr(collector, "reviewed_code_versions", lambda: approved)
        monkeypatch.setattr(contract, "reviewed_code_versions", lambda: approved)
        monkeypatch.setattr(http.client, "HTTPSConnection", self.cms.connect)
        self.fixture = Fixture(folder)

    @staticmethod
    def write_code_root(root: Path) -> None:
        for name in contract.current_code_hashes():
            (root / name).parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(REPO_ROOT / name, root / name)

    def write_plan(self, root: Path) -> None:
        folder = root / PLAN_FOLDER
        (folder / "references").mkdir(parents=True)
        references = {}
        for name, body in reference_files().items():
            (folder / "references" / name).write_bytes(body)
            references[name] = {
                "path": (PLAN_FOLDER / "references" / name).as_posix(),
                "sha256": digest(body),
                "url": f"https://data.cms.gov/mmd-population/{name}",
            }
        baseline = b"fips,county\n1001,Autauga\n"
        (root / BASELINE_FILE).write_bytes(baseline)
        report = encoded_json({"years": [{"year": YEAR, "original_path": BASELINE_FILE.as_posix(), "original_sha256": digest(baseline)}]})
        (root / REPORT_FILE).write_bytes(report)
        registry = canonical_hash(load_registry())
        conditions = [
            {
                "available_years": [YEAR],
                "domain_code": "d1",
                "gate_status": "not_run",
                "geography": "c",
                "measure_id": measure,
                "review_decision": "hold_definition",
                **fields,
            }
            for measure, fields in CONDITIONS.items()
        ]
        plan = {
            "comparison_report": REPORT_FILE.as_posix(),
            "comparison_report_sha256": digest(report),
            "conditions": conditions,
            "denominator_labels": {"1": "11-499", "2": "500-999", "3": "1,000-4,999", "4": "5,000-9,999", "5": "10,000+"},
            "endpoint": contract.ENDPOINT,
            "export_selections": {
                "adjustment": "Unsmoothed actual",
                "analysis": "Base measure",
                "domain": "Primary chronic conditions",
                "measure": "Prevalence",
                "population": "Medicare Fee For Service",
                "primary_age": "All",
                "primary_dual": "Dual & non-dual",
                "primary_eligibility": "All",
                "primary_race": "All",
                "primary_sex": "All",
            },
            "filters": {"agecat": ".", "dual": ".", "eligcat": ".", "fltr": "1", "measure": "v", "population": "f", "racecat": ".", "sexcat": "."},
            "model_eligible": False,
            "references": references,
            "registry_sha256": registry,
        }
        body = encoded_json(plan)
        (root / PLAN_FILE).write_bytes(body)
        self.plan_sha = digest(body)
        scenario = read_json(REPO_ROOT / "config/acquisition/e2e_scenario.json")
        jobs = read_json(REPO_ROOT / scenario["batch_plan"])["jobs"]
        template = next(job for job in jobs if job["job_id"] == "CMS_HCRIS_PUF_file_001")["plan"]
        (root / TEMPLATE_FILE).write_bytes(encoded_json(template))
        # The task's mirror carries the template, because the run's controls bind it there.
        (root / "mirror" / TEMPLATE_FILE).parent.mkdir(parents=True)
        (root / "mirror" / TEMPLATE_FILE).write_bytes(encoded_json(template))

    def capture_api(self, measure: str, root: Path) -> Path:
        """The real collector's capture on the Mac path, answered by the stand-in API."""
        return collector.capture(measure, YEAR, True, root=root)

    def add_api_unit(self) -> dict:
        """A stored capture of the API variant, made by the collector itself."""
        receipt = self.capture_api(API_MEASURE, self.folder / "stored_api")
        record = read_json(receipt)
        entry = {
            "snapshot_id": record["snapshot_id"],
            "source_id": STORED_SOURCE,
            "receipt": str(receipt.relative_to(self.folder)),
            "receipt_sha256": harness.digest(receipt.read_bytes()),
            "url": record["acquisition"]["requested_url"],
            "mode": "mmd_api",
            "bytes": sum(item["byte_count"] for item in record["artifacts"]),
            "pages": 1,
            "disposition": "api",
            "route": "publisher_api",
            "flags": [],
        }
        self.fixture.entries.append(entry)
        return entry

    def add_moved_unit(self, body: bytes | None = None) -> dict:
        """A stored browser export of the moved variant. By default its file is the very file the API capture derives for the same condition."""
        if body is None:
            scratch = self.capture_api(MOVED_MEASURE, self.folder / "scratch_moved")
            derived = next(item for item in read_json(scratch)["artifacts"] if item["role"] == "data")
            body = (scratch.parent / derived["storage_path"]).read_bytes()
            shutil.rmtree(self.folder / "scratch_moved")
        name = f"mmd_ffs_county_{MOVED_MEASURE.lower().replace('.', '_')}_prevalence_{YEAR}.csv"
        entry = self.fixture.stored(STORED_SOURCE, name, body, disposition="api", flags=[MOVED_FLAG], mode="file")
        path = self.folder / entry["receipt"]
        record = read_json(path)
        fields = CONDITIONS[MOVED_MEASURE]
        record["acquisition"] |= {
            "export_selections": {"condition": fields["condition_label"], "domain": fields["domain_label"], "geography": "County", "year": str(YEAR)}
        }
        record["lineage"] = {"extraction_or_query": json.dumps({"mode": "browser_export"})}
        path.write_bytes(encoded_json(record))
        entry["receipt_sha256"] = harness.digest(path.read_bytes())
        entry["url"] = PAGE_URL
        return entry

    def inventory(self, path: Path) -> Path:
        """The inventory rows a mirror build needs: one stand-in object per stored payload."""
        lines = ["source_id,snapshot_id,s3_key,s3_version_id,sha256"]
        for entry in self.fixture.entries:
            for artifact in read_json(self.folder / entry["receipt"])["artifacts"]:
                lines.append(
                    f"{entry['source_id']},{entry['snapshot_id']},raw/stored/{artifact['sha256']}/{artifact['stored_file_name']},v1,{artifact['sha256']}"
                )
        path.write_text("\n".join(lines) + "\n")
        return path


def counting(adapters: dict[str, Callable[[dict, Any], Path]], seen: list[str]) -> dict[str, Callable[[dict, Any], Path]]:
    """The harness adapters with the MMD adapter wrapped to record each dispatch by unit."""
    real = adapters["mmd_api"]

    def record(unit: dict, run: Any) -> Path:
        seen.append(unit["snapshot_id"])
        return real(unit, run)

    return {**adapters, "mmd_api": record}
