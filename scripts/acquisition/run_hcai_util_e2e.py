"""Repeatable synthetic HCAI utilization workbook capture-to-S3 verification, without live downloads or credentials."""

import argparse
import copy
import io
import json
import shutil
import stat
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


def leaked(text: str) -> bool:
    """True when any synthetic person's name, phone or street address appears, in any case or spacing."""
    flat = contract.normalized(text)
    return any(contract.normalized(p) in flat for p in [*PEOPLE, "Pat Example", "Pat Q. Example"]) or "010 0001" in flat or "example way" in flat


def outside_private(run: Path) -> str:
    """Everything outside the private original folder, as text."""
    return "\n".join(p.read_bytes().decode("utf-8", "ignore") for p in run.rglob("*") if p.is_file() and "private_original" not in p.parts)


def exercise(root: Path) -> list[dict]:
    """Use production orchestration with synthetic workbooks and versioned fake storage."""
    plan, plan_path, bodies = synthetic_plan(root)
    versions = root / "versions.json"
    write_once(versions, encoded_json({"versions": [{"code_sha256": bls_contract.code_hashes()}]}))
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

        def original_private() -> None:
            originals = list((run / "private_original").rglob("*.xlsx"))
            require(len(originals) == 1 and originals[0].read_bytes() == bodies[last["url"]], "Original not kept unchanged locally")
            require(stat.S_IMODE(originals[0].stat().st_mode) == 0o600, "Original is not owner-only")
            capture = receipt_path.parent
            require(not (capture / "raw").exists() and not (capture / "audit").exists(), "Snapshot holds original bytes")
            require(not leaked(outside_private(run)), "Personal detail outside the private original")

        scenario("original_local_owner_only_and_outside_snapshot", original_private)

        def redactions() -> None:
            import csv

            rows = list(csv.reader(io.StringIO((receipt_path.parent / "sheets/page_1_6.csv").read_text())))
            h = {c: i for i, c in enumerate(rows[0])}
            one, owner, copied = rows[5], rows[6], rows[7]
            for c in contract.REDACT:
                require(one[h[c]] in {contract.REPLACEMENT, ""}, f"{c} not redacted")
            require(one[h["FAC_NAME"]] == "SYNTHETIC GENERAL HOSPITAL" and one[h["FAC_PAR_CORP_NAME"]] == "SYNTHETIC HEALTH SYSTEM", "Business names lost")
            require([owner[h[c]] for c in contract.OWNER_REDACT] == [contract.REPLACEMENT] * 3 and owner[h["FAC_PAR_CORP_STATE"]] == "CA", "Owner rule")
            require(copied[h["FAC_NAME"]] == contract.REPLACEMENT and copied[h["EQUIP_DESC"]] == contract.REPLACEMENT, "Copied names kept")
            require(owner[h["EQUIP_DESC"]] == "#N/A" and one[h["FAC_OP_PER_BEGIN_DT"]] == "43101" and rows[1][-1] == "label note", "Published values changed")
            stats = read_json(receipt_path.parent / "evidence/download_proof.json")["statistics"]
            require(
                stats["hospitals"] == {"Page 1-6": 3, "NonResp 1-6": 1}
                and stats["contact_detail_cells"] == 0
                and stats["individual_owner_rows"] == 1
                and stats["preliminary"] is True,
                "Counts",
            )

        scenario("personal_owner_and_copied_names_redacted_values_kept", redactions)
        scenario("capture_and_version_verified_storage", lambda: execute(plan, last, run, True, True, client, OUTPUTS, web, LIMITS))

        def stored_clean() -> None:
            stored = "\n".join(o["body"].decode("utf-8", "ignore") for o in client.objects.values())
            require(not leaked(stored) and not any(k.endswith(".xlsx") or "private_original" in k for k in client.objects), "S3 received personal data")

        scenario("s3_holds_no_personal_data_or_original", stored_clean)
        before, objects, requests = inventory(run), copy.deepcopy(client.objects), web.requests

        def repeat() -> None:
            execute(plan, last, run, True, True, client, OUTPUTS, web, LIMITS)
            require(before == inventory(run) and objects == client.objects and web.requests == requests, "Repeated effects")

        scenario("repeat_one_no_effects", repeat)
        scenario("repeat_two_no_effects", repeat)
        scenario("replay_without_network", lambda: execute(plan, last, run, False, True, client, OUTPUTS, Offline({}), LIMITS))
        derive_scenarios(scenario, last)
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
            with patch.object(contract, "code_hashes", return_value={"changed": "0" * 64}):
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


def derive_scenarios(scenario: Any, entry: dict) -> None:
    """Every content defect named in the failure modes stops that year."""

    def data_edit(change: Any) -> Any:
        return lambda book: change(book["Page 1-6"])

    def add_column(name: str) -> Any:
        def change(book: dict) -> None:
            for sheet in contract.DATA_SHEETS:
                for row in book[sheet]:
                    row.append(name if row is book[sheet][0] else "")

        return change

    def drop_column(name: str) -> Any:
        def change(book: dict) -> None:
            for sheet in contract.DATA_SHEETS:
                i = book[sheet][0].index(name)
                for row in book[sheet]:
                    del row[i]

        return change

    widened = entry | {"header": [*HEADER, "CONTACT_EMAIL"]}
    narrowed = entry | {"header": [c for c in HEADER if c != "REV_REPT_PREP_NAME"]}
    cases: list[tuple[str, dict, bytes, str]] = [
        ("unreviewed_personal_column", widened, workbook(sheet_rows(add_column("CONTACT_EMAIL"))), "unreviewed personal-looking column"),
        ("redaction_column_missing", narrowed, workbook(sheet_rows(drop_column("REV_REPT_PREP_NAME"))), "redaction column missing"),
        ("header_differs_from_plan", entry, workbook(sheet_rows(add_column("NEW_MEASURE"))), "cell beyond the header"),
        ("sheets_differ", entry, workbook(sheet_rows(lambda b: b.pop("Crosswalk"))), "sheets differ"),
        ("formula_cell", entry, workbook(sheet_rows(), formula=True), "contains formulas"),
        ("xml_declaration", entry, workbook(sheet_rows(), doctype=True), "XML declarations"),
        ("value_beyond_header_in_hospital_row", entry, workbook(sheet_rows(data_edit(lambda rows: rows[6].append("stray")))), "cell beyond the header"),
        ("too_few_hospitals", entry, workbook(sheet_rows(data_edit(lambda rows: rows.pop()))), "too few hospitals"),
        ("not_a_workbook", entry, b"<html><body>error</body></html>", "not a workbook package"),
    ]
    for name, plan_entry, raw, error in cases:
        scenario(name, lambda plan_entry=plan_entry, raw=raw: contract.derive(raw, plan_entry), error)

    def contacts_removed() -> None:
        def edit(book: dict) -> None:
            book["Tips"].append(["Ask someone@example.org"])
            book["Page 1-6"][5][-1] = "call " + PHONE_TWO
            book["Page 1-6"][7][book["Page 1-6"][0].index("FAC_PAR_CORP_NAME")] = "billing@example.org"

        files, stats = contract.derive(workbook(sheet_rows(edit)), entry)
        text = b"".join(files.values()).decode()
        require(stats["contact_detail_cells"] == 3 and "example.org" not in text and PHONE_TWO not in text, "Contact details kept")

    scenario("contact_details_in_other_fields_redacted", contacts_removed)

    def label_numbers_not_names() -> None:
        files, stats = contract.derive(workbook(sheet_rows()), entry)
        require(stats["copied_name_cells"] == 2 and b"1.7" in files["sheets/page_1_6.csv"], "Label-row numbers treated as names")

    scenario("label_row_numbers_are_not_names", label_numbers_not_names)


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
    """Replay refuses tampered sheets, originals, lineage and governance, even when hashes are resealed."""
    collection = receipt_path.parents[3]

    def corrupt(kind: str) -> None:
        target_root = collection.parent / f"corrupt_{kind}"
        shutil.copytree(collection, target_root)
        target = target_root / receipt_path.relative_to(collection).parent
        receipt = read_json(target / "receipt.json")
        lineage = json.loads(receipt["lineage"]["extraction_or_query"])
        original = next((target_root / "private_original").rglob("*.xlsx"))

        def reseal(relative: str) -> None:
            for item in receipt["artifacts"]:
                if item["storage_path"] == relative:
                    item["sha256"], item["byte_count"] = fingerprint(target / relative)

        if kind == "resealed_sheet_with_name":
            path = target / "sheets/page_1_6.csv"
            path.write_bytes(path.read_bytes() + b"Lee Sample\n")
            reseal("sheets/page_1_6.csv")
        elif kind == "original_changed":
            original.write_bytes(original_bytes + b"\0")
        elif kind == "original_deleted":
            original.unlink()
        elif kind == "original_copied_into_snapshot":
            (target / "raw").mkdir()
            (target / "raw" / original.name).write_bytes(original.read_bytes())
        elif kind == "model_promotion":
            lineage["model_eligible"] = True
        elif kind == "unreviewed_code":
            lineage["code_sha256"] = {"bad": "0" * 64}
        elif kind == "wrong_plan":
            lineage["plan_sha256"] = "0" * 64
        elif kind == "access_release_binding":
            lineage["access_release_sha256"] = "0" * 64
        elif kind == "pii_governance":
            receipt["governance"]["contains_pii"] = True
        receipt["lineage"]["extraction_or_query"] = json.dumps(lineage)
        (target / "receipt.json").write_bytes(encoded_json(receipt))
        verify_local(target / "receipt.json")

    for kind, error in [
        ("resealed_sheet_with_name", "derived sheet differs"),
        ("original_changed", "original hash differs"),
        ("original_deleted", "No such file"),
        ("original_copied_into_snapshot", "holds original bytes"),
        ("model_promotion", "hold differs"),
        ("unreviewed_code", "Unreviewed HCAI utilization"),
        ("wrong_plan", "plan differs"),
        ("access_release_binding", "access release binding differs"),
        ("pii_governance", "governance differs"),
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
        "feature": "HCAI utilization workbooks: original kept locally, abstracted sheet CSVs through versioned S3 storage",
        "run_at_utc": datetime.now(UTC).isoformat(),
        "python": sys.version.split()[0],
        "code_sha256": bls_contract.code_hashes(),
        "dependency_sha256": bls_contract.digest(Path("requirements.txt").read_bytes()),
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
