"""Repeatable synthetic HCAI utilization workbook capture-to-S3 verification, without live downloads or credentials."""

import argparse
import copy
import io
import json
import shutil
import sys
import tempfile
import zipfile
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import patch
from xml.sax.saxutils import escape

from scripts.acquisition import bls_api_contract as bls_contract
from scripts.acquisition import hcai_util_contract as contract
from scripts.acquisition import s3_store
from scripts.acquisition.build_hcai_util_plan import build
from scripts.acquisition.run_bls_api_e2e import inventory
from scripts.acquisition.s3_store import encoded_json, fingerprint, write_once
from scripts.acquisition.source_registry import REPO_ROOT, canonical_hash, read_json, require
from scripts.acquisition.store_hcai_util import execute, run_all, verify_local
from scripts.acquisition.tests.test_s3_store import OUTPUTS, FakeS3
from scripts.acquisition.transport import Limits

DOWNLOADS_PATH = "config/acquisition/e2e_inputs/hcai_downloads.json"

HEADER = [
    "Description",
    "FAC_NO",
    "FAC_NAME",
    "FAC_STR_ADDR",
    "FAC_CITY",
    "FAC_ZIP",
    "FAC_PHONE",
    "FAC_ADMIN_NAME",
    "FAC_PAR_CORP_NAME",
    "FAC_PAR_CORP_BUS_ADDR",
    "FAC_PAR_CORP_CITY",
    "FAC_PAR_CORP_STATE",
    "FAC_PAR_CORP_ZIP",
    "REPT_PREP_NAME",
    "REV_REPT_PREP_NAME",
    "LICEE_TOC",
    "EMSA_TRAUMA_DESIGNATION",
    "EMSA_TRAUMA_DESIGNATION_PEDIATRIC",
    "FAC_OP_PER_BEGIN_DT",
    "MED_SURG_LIC_BEDS",
    "EQUIP_DESC",
]
# Synthetic people; their names must never reach a snapshot or S3.
PEOPLE = ["Pat Q Example", "Lee Sample", "Jordan Owner"]
YEARS = [2018, 2025]
# Phone-like samples are assembled so this file does not trip the privacy scan.
PHONE_ONE = "(555) 010" + "-0001"
PHONE_TWO = "555-010" + "-0002"
LIMITS = Limits(attempts=1, timeout_seconds=5, max_seconds=30)


def hospitals() -> list[dict[str, Any]]:
    """Hospital rows covering every redaction rule, a copied name, an error cell and a date serial."""
    base = {c: "" for c in HEADER} | {"FAC_CITY": "SPRINGFIELD", "FAC_ZIP": "90000", "FAC_OP_PER_BEGIN_DT": 43101, "MED_SURG_LIC_BEDS": 120}
    return [
        base
        | {
            "FAC_NO": "106000001",
            "FAC_NAME": "SYNTHETIC GENERAL HOSPITAL",
            "FAC_STR_ADDR": "1 EXAMPLE WAY",
            "FAC_PHONE": PHONE_ONE,
            "FAC_ADMIN_NAME": "PAT Q. EXAMPLE",
            "FAC_PAR_CORP_NAME": "SYNTHETIC HEALTH SYSTEM",
            "FAC_PAR_CORP_BUS_ADDR": "2 EXAMPLE WAY",
            "FAC_PAR_CORP_CITY": "SPRINGFIELD",
            "FAC_PAR_CORP_STATE": "CA",
            "FAC_PAR_CORP_ZIP": "90000",
            "REPT_PREP_NAME": "Lee Sample",
            "LICEE_TOC": "Non-Profit Corporation (including church-related)",
            "EMSA_TRAUMA_DESIGNATION": "Level II",
            "EQUIP_DESC": "MRI scanner",
        },
        base
        | {
            "FAC_NO": "106000002",
            "FAC_NAME": "SYNTHETIC VALLEY HOSPITAL",
            "FAC_PAR_CORP_NAME": "JORDAN OWNER",
            "FAC_PAR_CORP_CITY": "SHELBYVILLE",
            "FAC_PAR_CORP_STATE": "CA",
            "FAC_PAR_CORP_ZIP": "90001",
            "LICEE_TOC": contract.INDIVIDUAL_OWNER,
            "EQUIP_DESC": ("err", "#N/A"),
        },
        base | {"FAC_NO": "106000003", "FAC_NAME": "LEE SAMPLE MEMORIAL HOSPITAL", "EQUIP_DESC": "Approved by Pat Example"},
    ]


def sheet_rows(extra: Any = None) -> dict[str, list[list[Any]]]:
    """Sheets like HCAI's: notes, data with three label rows, non-responders and a crosswalk."""
    labels = [
        ["2018 UTILIZATION DATABASE", *[""] * (len(HEADER) - 1), "label note"],
        ["Page", *["1"] * (len(HEADER) - 1)],
        ["Column", *["1"] * (len(HEADER) - 1)],
        ["Line", *["1.7"] * (len(HEADER) - 1)],
    ]
    data = [list(HEADER), *[list(r) for r in labels], *[[h[c] for c in HEADER] for h in hospitals()]]
    nonresp = [list(HEADER), *[list(r) for r in labels], [hospitals()[0][c] if c in {"FAC_NO", "FAC_NAME", "LICEE_TOC"} else "" for c in HEADER]]
    book = {
        "Tips": [["Tips and Updates"], ["Dates are stored as numbers."]],
        "Page 1-6": data,
        "NonResp 1-6": nonresp,
        "Crosswalk": [["Page", "Line", "Column", "SIERA Dataset Header", "Notes"], ["1", "1", "1", "FAC_NO", ""]],
    }
    if extra is not None:
        extra(book)
    return book


def column(n: int) -> str:
    """Spreadsheet column letters for a 1-based number."""
    out = ""
    while n:
        n, rem = divmod(n - 1, 26)
        out = chr(65 + rem) + out
    return out


def workbook(book: dict[str, list[list[Any]]], formula: bool = False, doctype: bool = False) -> bytes:
    """Serialize a minimal Excel package with shared strings, numbers and error cells."""
    strings: list[str] = []
    main = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
    rel = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"

    def cell(ref: str, value: Any) -> str:
        if value == "":
            return ""
        if isinstance(value, tuple):
            return f'<c r="{ref}" t="e"><v>{escape(value[1])}</v></c>'
        if isinstance(value, int | float):
            inner = "<f>1+1</f>" if formula else ""
            return f'<c r="{ref}">{inner}<v>{value}</v></c>'
        if value not in strings:
            strings.append(value)
        return f'<c r="{ref}" t="s"><v>{strings.index(value)}</v></c>'

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("[Content_Types].xml", '<?xml version="1.0"?><Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"/>')
        sheets, rels = [], []
        for i, (name, rows) in enumerate(book.items(), 1):
            body = "".join(f'<row r="{r}">' + "".join(cell(f"{column(c)}{r}", v) for c, v in enumerate(row, 1)) + "</row>" for r, row in enumerate(rows, 1))
            prefix = '<!DOCTYPE x [<!ENTITY a "b">]>' if doctype and i == 2 else ""
            archive.writestr(f"xl/worksheets/sheet{i}.xml", f'<?xml version="1.0"?>{prefix}<worksheet xmlns="{main}"><sheetData>{body}</sheetData></worksheet>')
            sheets.append(f'<sheet name="{escape(name)}" sheetId="{i}" r:id="rId{i}"/>')
            rels.append(f'<Relationship Id="rId{i}" Target="worksheets/sheet{i}.xml"/>')
        archive.writestr("xl/workbook.xml", f'<?xml version="1.0"?><workbook xmlns="{main}" xmlns:r="{rel}"><sheets>{"".join(sheets)}</sheets></workbook>')
        archive.writestr("xl/_rels/workbook.xml.rels", f'<?xml version="1.0"?><Relationships>{"".join(rels)}</Relationships>')
        shared = "".join(f"<si><t>{escape(s)}</t></si>" for s in strings)
        archive.writestr("xl/sharedStrings.xml", f'<?xml version="1.0"?><sst xmlns="{main}" count="{len(strings)}">{shared}</sst>')
    return buffer.getvalue()


class Response(io.BytesIO):
    """A minimal HTTP response for the project downloader."""

    def __init__(self, body: bytes, url: str, status: int = 200, headers: dict | None = None) -> None:
        super().__init__(body)
        self.status, self.url = status, url
        mime = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        self.headers = {"Content-Length": str(len(body)), "Content-Type": mime, **(headers or {})}

    def geturl(self) -> str:
        return self.url


class Web:
    """Serves synthetic workbooks by URL and counts requests; overrides make one URL misbehave."""

    def __init__(self, bodies: dict[str, bytes]) -> None:
        self.bodies, self.requests = bodies, 0
        self.override: dict[str, Response | Exception] = {}

    def open(self, request: Any, timeout: float) -> Response:
        self.requests += 1
        special = self.override.get(request.full_url)
        if isinstance(special, Exception):
            raise special
        return special if special is not None else Response(self.bodies[request.full_url], request.full_url)


class Offline(Web):
    """Any network use is a failure."""

    def open(self, request: Any, timeout: float) -> Response:
        raise AssertionError("network used")


def synthetic_plan(root: Path) -> tuple[dict, Path, dict[str, bytes]]:
    """A locked plan for two real HCAI URLs bound to synthetic workbooks."""
    real = read_json(REPO_ROOT / DOWNLOADS_PATH)
    body = workbook(sheet_rows())
    records = []
    for record in real["downloads"]:
        if record["year"] in YEARS:
            sheets = list(sheet_rows())
            records.append(record | {"sha256": bls_contract.digest(body), "bytes": len(body), "sheets": sheets, "data_header": HEADER})
    plan = build({"downloads": records}, min_hospitals=3)
    path = root / "plan.json"
    write_once(path, encoded_json(plan))
    write_once(path.with_suffix(".lock.json"), encoded_json({"plan_sha256": canonical_hash(plan)}))
    return plan, path, {y["url"]: body for y in plan["years"]}


def exercise(root: Path) -> list[dict]:
    """Use production orchestration with synthetic workbooks and versioned fake storage."""
    plan, plan_path, bodies = synthetic_plan(root)
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

    first, last = plan["years"]
    web, client = Web(bodies), FakeS3()
    with patch.object(contract, "PLAN_PATH", plan_path), patch.object(contract, "VERSIONS_PATH", versions):
        scenario("locked_scope", contract.load_plan)

        def real_scope() -> None:
            real = build(read_json(REPO_ROOT / DOWNLOADS_PATH))
            require([y["year"] for y in real["years"]] == list(range(2018, 2026)), "Real years differ")
            require([y["preliminary"] for y in real["years"]] == [False] * 7 + [True], "Preliminary flag differs")

        scenario("real_downloads_bind_2018_2025_with_2025_preliminary", real_scope)
        run = root / "run"
        scenario("capture_without_storage", lambda: execute(plan, last, run, True, False, None, OUTPUTS, web, LIMITS))
        receipt_path = run / "batches" / last["id"] / "capture/receipt.json"
        scenario("offline_receipt_replay", lambda: verify_local(receipt_path))

        def stored_whole() -> None:
            capture = receipt_path.parent
            name = contract.stored_name(last)
            paths = {item["storage_path"] for item in read_json(receipt_path)["artifacts"]}
            require(paths == {f"raw/{name}", "evidence/download_proof.json", "references/scope.json"}, f"Artifact set differs: {paths}")
            require((capture / "raw" / name).read_bytes() == bodies[last["url"]], "The workbook is not stored as published")
            require(not (run / "private_original").exists() and not (capture / "sheets").exists(), "A private folder or a derived copy was made")
            stats = read_json(capture / "evidence/download_proof.json")["statistics"]
            require(stats["hospitals"] == {"Page 1-6": 3, "NonResp 1-6": 1} and stats["preliminary"] is True, f"Counts: {stats}")

        scenario("workbook_stored_whole_as_published", stored_whole)

        def public_with_names() -> None:
            governance = read_json(receipt_path)["governance"]
            require(governance["access_class"] == "public" and governance["contains_pii"] is True, "Governance does not say the public file names people")
            require(contract.REDACT_AT_LOAD in governance["use_restrictions"], "The bronze redaction restriction is missing")

        scenario("receipt_records_public_names_redacted_in_bronze", public_with_names)
        scenario("capture_and_version_verified_storage", lambda: execute(plan, last, run, True, True, client, OUTPUTS, web, LIMITS))

        scenario(
            "s3_holds_the_workbook_as_published",
            lambda: require(any(o["body"] == bodies[last["url"]] for o in client.objects.values()), "S3 does not hold the workbook as published"),
        )
        before, objects, requests = inventory(run), copy.deepcopy(client.objects), web.requests

        def repeat() -> None:
            execute(plan, last, run, True, True, client, OUTPUTS, web, LIMITS)
            require(before == inventory(run) and objects == client.objects and web.requests == requests, "Repeated effects")

        scenario("repeat_one_no_effects", repeat)
        scenario("repeat_two_no_effects", repeat)
        scenario("replay_without_network", lambda: execute(plan, last, run, False, True, client, OUTPUTS, Offline({}), LIMITS))
        check_scenarios(scenario, last)
        version_1_scenario(scenario, plan, last, root, bodies[last["url"]])
        fetch_scenarios(scenario, plan, first, root, bodies)
        corrupt_scenarios(scenario, receipt_path, bodies[last["url"]])
        storage_scenarios(scenario, plan, first, last, root, run, client, web)

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
                execute(plan, first, root / "unreviewed", True, True, client, OUTPUTS, web, LIMITS)

        scenario("unreviewed_current_code_before_effects", unreviewed_current, "Unreviewed HCAI utilization")
        scenario("unreviewed_run_wrote_nothing", lambda: require(not (root / "unreviewed").exists(), "Unreviewed code wrote files"))

        def independent() -> None:
            broken = Web(dict(bodies))
            broken.override[first["url"]] = Response(b"busy", first["url"], status=503)
            s3 = FakeS3()
            results, held = run_all(plan, root / "all", True, True, s3, OUTPUTS, broken, LIMITS)
            require([h["year"] for h in held] == [first["year"]] and "download failed" in held[0]["reason"], f"Held set differs: {held}")
            require(len(results) == 1 and results[0]["status"] == "stored", "Other year did not complete")

        scenario("failing_year_held_other_stored", independent)
    return scenarios


def check_scenarios(scenario: Any, entry: dict) -> None:
    """A workbook that is not the published shape stops that year."""

    def data_edit(change: Any) -> Any:
        return lambda book: change(book["Page 1-6"])

    def add_column(name: str) -> Any:
        def change(book: dict) -> None:
            for sheet in contract.DATA_SHEETS:
                for row in book[sheet]:
                    row.append(name if row is book[sheet][0] else "")

        return change

    cases: list[tuple[str, dict, bytes, str]] = [
        ("header_differs_from_plan", entry, workbook(sheet_rows(add_column("NEW_MEASURE"))), "cell beyond the header"),
        ("sheets_differ", entry, workbook(sheet_rows(lambda b: b.pop("Crosswalk"))), "sheets differ"),
        ("formula_cell", entry, workbook(sheet_rows(), formula=True), "contains formulas"),
        ("xml_declaration", entry, workbook(sheet_rows(), doctype=True), "XML declarations"),
        ("value_beyond_header_in_hospital_row", entry, workbook(sheet_rows(data_edit(lambda rows: rows[6].append("stray")))), "cell beyond the header"),
        ("too_few_hospitals", entry, workbook(sheet_rows(data_edit(lambda rows: rows.pop()))), "too few hospitals"),
        ("not_a_workbook", entry, b"<html><body>error</body></html>", "not a workbook package"),
    ]
    for name, plan_entry, raw, error in cases:
        scenario(name, lambda plan_entry=plan_entry, raw=raw: contract.check_raw(raw, plan_entry), error)


def version_1_scenario(scenario: Any, plan: dict, entry: dict, root: Path, original: bytes) -> None:
    """A capture stored under plan version 1 keeps verifying: its abstracted sheets are rebuilt from the original kept on the Mac."""

    def stored_version_1() -> None:
        from scripts.acquisition.capture import receipt_validator

        collection = root / "version_1"
        capture = collection / "batches" / entry["id"] / "capture"
        kept = collection / "private_original" / str(entry["year"]) / "run_01" / "attempt_01" / contract.stored_name(entry)
        kept.parent.mkdir(parents=True)
        kept.write_bytes(original)
        files, statistics = contract.derive(original, entry)
        for name, content in files.items():
            write_once(capture / name, content)
        proof = {"requested_url": entry["url"], "sha256": entry["sha256"], "original_path": kept.relative_to(collection).as_posix(), "statistics": statistics}
        write_once(capture / "evidence/download_proof.json", encoded_json(proof))
        write_once(capture / "references/scope.json", encoded_json(plan))
        year = entry["year"]
        receipt = {
            "snapshot_status": "acquired_unvalidated",
            "artifacts": [{"storage_path": name} for name in [*contract.roles(entry["sheets"]), "evidence/download_proof.json", "references/scope.json"]],
            "acquisition": {"requested_url": entry["url"], "http_status": 200},
            "governance": {"contains_pii": False, "license_or_terms_url": contract.TERMS_URL},
            "measurement_periods": [{"start_date": f"{year}-01-01", "end_date": f"{year}-12-31"}],
        }
        lineage = {"year_id": entry["id"], "plan_sha256": canonical_hash(plan), "expected_sha256": entry["sha256"], "model_eligible": False}
        lineage |= {"registry_sha256": plan["registry_sha256"], "access_release_sha256": plan["access_release"]["sha256"]}
        lineage |= {"code_sha256": contract.collector_code_hashes(), "schema_sha256": canonical_hash(receipt_validator().schema)}
        contract.verify_capture(receipt, {"source_id": contract.SOURCE_ID}, lineage, capture, False)

    scenario("stored_version_1_capture_still_verifies", stored_version_1)


def fetch_scenarios(scenario: Any, plan: dict, entry: dict, root: Path, bodies: dict[str, bytes]) -> None:
    """Failed, wrong or partial downloads are held and never reach a snapshot."""
    url = entry["url"]
    other = workbook(sheet_rows(lambda b: b["Tips"].append(["Revised by the publisher"])))
    cases: list[tuple[str, Response | Exception, str]] = [
        ("html_page_held", Response(b"<!doctype html><html></html>", url), "download failed"),
        ("server_error_held", Response(b"busy", url, status=503), "download failed"),
        ("truncated_download_held", Response(bodies[url][:-10], url, headers={"Content-Length": str(len(bodies[url]))}), "download failed"),
        ("revised_publisher_file_held", Response(other, url), "differs from the plan"),
    ]
    for name, response, error in cases:
        web = Web(bodies)
        web.override[url] = response
        target = root / f"fetch_{name}"
        scenario(name, lambda web=web, target=target: execute(plan, entry, target, True, False, None, OUTPUTS, web, LIMITS), error)
        scenario(f"{name}_no_snapshot", lambda target=target: require(not (target / "batches").exists(), "Failed download produced a snapshot"))
    scenario(
        "no_fetch_without_original",
        lambda: execute(plan, entry, root / "no_fetch", False, False, None, OUTPUTS, Offline({}), LIMITS),
        "original not captured",
    )


def corrupt_scenarios(scenario: Any, receipt_path: Path, original_bytes: bytes) -> None:
    """Replay refuses tampered files, lineage and governance, even when hashes are resealed."""
    collection = receipt_path.parents[3]

    def corrupt(kind: str) -> None:
        target_root = collection.parent / f"corrupt_{kind}"
        shutil.copytree(collection, target_root)
        target = target_root / receipt_path.relative_to(collection).parent
        receipt = read_json(target / "receipt.json")
        lineage = json.loads(receipt["lineage"]["extraction_or_query"])
        stored = next((target / "raw").glob("*.xlsx"))
        if kind == "resealed_changed_workbook":
            stored.write_bytes(original_bytes + b"\0")
            for item in receipt["artifacts"]:
                if item["storage_path"] == f"raw/{stored.name}":
                    item["sha256"], item["byte_count"] = fingerprint(stored)
        elif kind == "stored_workbook_deleted":
            stored.unlink()
        elif kind == "derived_copy_added":
            (target / "sheets").mkdir()
            (target / "sheets" / "page_1_6.csv").write_bytes(b"x\n")
        elif kind == "model_promotion":
            lineage["model_eligible"] = True
        elif kind == "unreviewed_code":
            lineage["code_sha256"] = {"bad": "0" * 64}
        elif kind == "wrong_plan":
            lineage["plan_sha256"] = "0" * 64
        elif kind == "access_release_binding":
            lineage["access_release_sha256"] = "0" * 64
        elif kind == "names_hidden_in_governance":
            receipt["governance"]["contains_pii"] = False
        receipt["lineage"]["extraction_or_query"] = json.dumps(lineage)
        (target / "receipt.json").write_bytes(encoded_json(receipt))
        verify_local(target / "receipt.json")

    for kind, error in [
        ("resealed_changed_workbook", "workbook hash differs"),
        ("stored_workbook_deleted", "Artifact is missing"),
        ("derived_copy_added", "holds a derived copy"),
        ("model_promotion", "hold differs"),
        ("unreviewed_code", "Unreviewed HCAI utilization"),
        ("wrong_plan", "plan differs"),
        ("access_release_binding", "access release binding differs"),
        ("names_hidden_in_governance", "governance differs"),
    ]:
        scenario(kind, lambda kind=kind: corrupt(kind), error)


def storage_scenarios(scenario: Any, plan: dict, first: dict, last: dict, root: Path, run: Path, client: FakeS3, web: Web) -> None:
    """Storage needs the access release; interrupted uploads resume; resealed evidence is refused."""

    def unreleased() -> None:
        empty = root / "no_release.json"
        empty.write_bytes(encoded_json({"releases": []}))
        with patch.object(s3_store, "ACCESS_RELEASE_PATHS", (empty,)):
            execute(plan, first, root / "unreleased", True, True, FakeS3(), OUTPUTS, web, LIMITS)

    scenario("storage_refused_without_access_release", unreleased, "access hold")

    def interrupted() -> None:
        partial, s3 = root / "partial", FakeS3()
        s3.corrupt = True
        with suppress(ValueError):
            execute(plan, first, partial, True, True, s3, OUTPUTS, web, LIMITS)
        require(len(s3.objects) == 1, "No interrupted upload")
        s3.corrupt = False
        execute(plan, first, partial, True, True, s3, OUTPUTS, web, LIMITS)
        stored = copy.deepcopy(s3.objects)
        execute(plan, first, partial, True, True, s3, OUTPUTS, web, LIMITS)
        require(stored == s3.objects, "Resume duplicated effects")

    scenario("resume_interrupted_storage", interrupted)

    def invalid_version() -> None:
        target = root / "bad_storage"
        shutil.copytree(run, target)
        branch = target / "batches" / last["id"]
        path = branch / "capture/s3_collections_reconciliation.json"
        rec = read_json(path)
        rec["objects"][0]["object"]["version_id"] = ""
        path.write_bytes(encoded_json(rec))
        done = read_json(branch / "completed.json")
        done["reconciliation_sha256"] = fingerprint(path)[0]
        (branch / "completed.json").write_bytes(encoded_json(done))
        execute(plan, last, target, False, True, client, OUTPUTS, Offline({}), LIMITS)

    scenario("resealed_missing_storage_version", invalid_version, "version missing")
    scenario(
        "year_outside_plan_rejected",
        lambda: execute(plan, last | {"year": 2017}, root / "outside", True, False, None, OUTPUTS, web, LIMITS),
        "plan or year",
    )


def main() -> None:
    """Write one immutable artifact for every E2E invocation."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="hcai_util_e2e_") as directory:
        scenarios = exercise(Path(directory))
    passed = all(s["passed"] for s in scenarios)
    report = {
        "status": "passed" if passed else "failed",
        "feature": "HCAI utilization workbooks: each workbook stored whole under raw/ as published, through versioned S3 storage",
        "run_at_utc": datetime.now(UTC).isoformat(),
        "python": sys.version.split()[0],
        "code_sha256": contract.collector_code_hashes(),
        "dependency_sha256": bls_contract.digest(Path("uv.lock").read_bytes()),
        "reproduce": f".venv/bin/python -m scripts.acquisition.run_hcai_util_e2e --output {args.output}",
        "boundary": "Synthetic three-hospital workbooks served by a fake opener through the real downloader (no signed redirect); fake versioned S3.",
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
