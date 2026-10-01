"""Repeatable synthetic CMS Hospital All Owners capture-to-S3 verification, without live downloads or credentials."""

import argparse
import copy
import csv
import io
import json
import shutil
import stat
import sys
import tempfile
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import patch

from scripts.acquisition import bls_api_contract as bls_contract
from scripts.acquisition import cms_owners_contract as contract
from scripts.acquisition.build_cms_owners_plan import build
from scripts.acquisition.run_bls_api_e2e import inventory
from scripts.acquisition.s3_store import encoded_json, fingerprint, write_once
from scripts.acquisition.source_registry import REPO_ROOT, canonical_hash, read_json, require
from scripts.acquisition.store_cms_owners import execute, run_all, verify_local
from scripts.acquisition.tests.test_s3_store import OUTPUTS, FakeS3
from scripts.acquisition.transport import Limits

# Two releases of each layout, taken from the real catalogue so plan bindings use real registry routes.
CATALOGUE_PATH = "config/acquisition/e2e_inputs/cms_catalogue.json"
HEADER_LINES_PATH = "config/acquisition/e2e_inputs/cms_headers.json"

PERIODS = ["2022-11-01", "2025-03-01", "2025-04-01", "2026-08-01"]
# Synthetic individuals; their names must never reach the snapshot or S3.
PEOPLE = [("Alex", "Q", "Example"), ("Robin", "", "Sample")]
LIMITS = Limits(attempts=1, timeout_seconds=5, max_seconds=30)


class Response(io.BytesIO):
    """A minimal HTTP response for the project downloader."""

    def __init__(self, body: bytes, url: str, status: int = 200, headers: dict | None = None) -> None:
        super().__init__(body)
        self.status, self.url = status, url
        self.headers = {"Content-Length": str(len(body)), "Content-Type": "text/csv", **(headers or {})}

    def geturl(self) -> str:
        return self.url


class Web:
    """Serves synthetic release bytes by URL and counts requests; overrides make one URL misbehave."""

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


def table(layout: str, change: Any = None) -> list[list[str]]:
    """Organisation and individual owner rows with every flag value, a blank percentage and a company named after an owner."""
    header = contract.HEADERS[layout]
    flags = contract.flag_columns(layout)
    out = [list(header)]

    def row(kind: str, **values: str) -> list[str]:
        cells = dict.fromkeys(header, "")
        cells.update({"ENROLLMENT ID": "O20000000000001", "ASSOCIATE ID": "1234567890", "ORGANIZATION NAME": "SYNTHETIC GENERAL HOSPITAL"})
        cells.update(
            {
                "ASSOCIATE ID - OWNER": "0987654321",
                "TYPE - OWNER": kind,
                "ROLE CODE - OWNER": "34",
                "ROLE TEXT - OWNER": "5% OR GREATER DIRECT OWNERSHIP INTEREST",
            }
        )
        cells.update({"ASSOCIATION DATE - OWNER": "2020-01-01", "PERCENTAGE OWNERSHIP": "50"})
        cells.update(values)
        return [cells[c] for c in header]

    for first, middle, last in PEOPLE:
        out.append(row("I", **{"FIRST NAME - OWNER": first, "MIDDLE NAME - OWNER": middle, "LAST NAME - OWNER": last, "TITLE - OWNER": "DIRECTOR"}))
    org = {"ORGANIZATION NAME - OWNER": "SYNTHETIC HEALTH PARTNERS LLC", "ADDRESS LINE 1 - OWNER": "1 EXAMPLE WAY", "CITY - OWNER": "SPRINGFIELD"}
    out.append(row("O", **org, **{"STATE - OWNER": "CA", "ZIP CODE - OWNER": "90000"}, **dict.fromkeys(flags, "Y")))
    out.append(row("O", **{"ORGANIZATION NAME - OWNER": "ALEX Q. EXAMPLE HOLDINGS, LLC", "PERCENTAGE OWNERSHIP": ""}, **dict.fromkeys(flags, "N")))
    out.append(row("O", **{"ORGANIZATION NAME - OWNER": "ROBINSON SAMPLES INC", "DOING BUSINESS AS NAME - OWNER": "robin sample group"}))
    if change is not None:
        change(out)
    return out


def body(layout: str, change: Any = None, encoding: str = "utf-8") -> bytes:
    """Serialize like CMS: comma-separated with CRLF line endings."""
    output = io.StringIO(newline="")
    csv.writer(output, lineterminator="\r\n").writerows(table(layout, change))
    return output.getvalue().encode(encoding)


def synthetic_plan(root: Path) -> tuple[dict, Path, dict[str, bytes]]:
    """A locked plan for four real catalogue releases, plus their synthetic bytes."""
    plan = build(read_json(REPO_ROOT / CATALOGUE_PATH))
    plan["releases"] = [r for r in plan["releases"] if r["period_start"] in PERIODS]
    require(len(plan["releases"]) == 4, "Synthetic periods missing from the catalogue")
    path = root / "plan.json"
    write_once(path, encoded_json(plan))
    write_once(path.with_suffix(".lock.json"), encoded_json({"plan_sha256": canonical_hash(plan)}))
    return plan, path, {r["url"]: body(r["layout"]) for r in plan["releases"]}


def snapshot_text(run: Path) -> str:
    """Everything outside the private original folder, as text, for leak checks."""
    return "\n".join(p.read_bytes().decode("utf-8", "ignore") for p in run.rglob("*") if p.is_file() and "private_original" not in p.parts)


def leaked(text: str) -> bool:
    """True when any synthetic individual's whole name or title appears, in any case or spacing."""
    flat = contract.normalized(text)
    return any(
        contract.normalized(" ".join(p for p in person if p)) in flat or contract.normalized(f"{person[0]} {person[2]}") in flat for person in PEOPLE
    ) or ("director" in flat)


def exercise(root: Path) -> list[dict]:
    """Use production orchestration with synthetic releases and versioned fake storage."""
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

    by_period = {r["period_start"]: r for r in plan["releases"]}
    v1, v2 = by_period["2022-11-01"], by_period["2026-08-01"]
    web, client = Web(bodies), FakeS3()
    with patch.object(contract, "PLAN_PATH", plan_path), patch.object(contract, "VERSIONS_PATH", versions):
        scenario("locked_scope", contract.load_plan)

        def real_scope() -> None:
            real = build(read_json(REPO_ROOT / CATALOGUE_PATH), read_json(REPO_ROOT / HEADER_LINES_PATH))
            layouts = [r["layout"] for r in real["releases"]]
            require(len(real["releases"]) == 44 and layouts.count("v1") == 28 and layouts.count("v2") == 16, "Real catalogue scope differs")

        scenario("real_catalogue_binds_44_releases_two_layouts", real_scope)
        run = root / "run"
        scenario("capture_without_storage", lambda: execute(plan, v2, run, True, False, None, OUTPUTS, web, LIMITS))
        receipt_path = run / "batches" / v2["id"] / "capture/receipt.json"
        scenario("offline_receipt_replay", lambda: verify_local(receipt_path))

        def original_private() -> None:
            originals = [p for p in (run / "private_original").rglob("*.csv")]
            require(len(originals) == 1 and originals[0].read_bytes() == bodies[v2["url"]], "Original not kept unchanged locally")
            require(stat.S_IMODE(originals[0].stat().st_mode) == 0o600, "Original is not owner-only")
            require(
                all(stat.S_IMODE(p.stat().st_mode) == 0o700 for p in originals[0].parents if p.is_relative_to(run / "private_original")),
                "Folder is not owner-only",
            )
            capture = receipt_path.parent
            require(not (capture / "raw").exists() and not (capture / "audit").exists(), "Snapshot holds original bytes")
            require(not leaked(snapshot_text(run)), "Individual name outside the private original")

        scenario("original_local_owner_only_and_outside_snapshot", original_private)

        def derived_content() -> None:
            rows = list(csv.reader(io.StringIO((receipt_path.parent / "derived/organisation_owners.csv").read_text())))
            require(rows[0] == contract.kept_columns("v2") and not set(contract.DROPPED) & set(rows[0]), "Derived columns differ")
            require(len(rows) == 4 and all(r[rows[0].index("TYPE - OWNER")] == "O" for r in rows[1:]), "Individual rows kept")
            names = [r[rows[0].index("ORGANIZATION NAME - OWNER")] for r in rows[1:]]
            require(names == ["SYNTHETIC HEALTH PARTNERS LLC", contract.REPLACEMENT, "ROBINSON SAMPLES INC"], "Name redaction differs")
            require(rows[3][rows[0].index("DOING BUSINESS AS NAME - OWNER")] == contract.REPLACEMENT, "DBA name redaction differs")
            require(rows[2][rows[0].index("PERCENTAGE OWNERSHIP")] == "" and "PRIVATE EQUITY COMPANY - OWNER" in rows[0], "Published values changed")
            proof = read_json(receipt_path.parent / "evidence/download_proof.json")
            stats = proof["statistics"]
            require(
                (stats["rows_read"], stats["individual_rows_dropped"], stats["organisation_rows_kept"], stats["redacted_name_cells"]) == (5, 2, 3, 2),
                "Counts differ",
            )

        scenario("derived_organisation_rows_only_names_redacted", derived_content)
        scenario("capture_and_version_verified_storage", lambda: execute(plan, v2, run, True, True, client, OUTPUTS, web, LIMITS))

        def stored_clean() -> None:
            stored = "\n".join(o["body"].decode("utf-8", "ignore") for o in client.objects.values())
            require(
                not leaked(stored)
                and not any("private_original" in key or key.endswith(".csv") and "organisation_owners" not in key for key in client.objects),
                "S3 received individual data",
            )

        scenario("s3_holds_no_individual_data", stored_clean)
        before, objects, requests = inventory(run), copy.deepcopy(client.objects), web.requests

        def repeat() -> None:
            execute(plan, v2, run, True, True, client, OUTPUTS, web, LIMITS)
            require(before == inventory(run) and objects == client.objects and web.requests == requests, "Repeated effects")

        scenario("repeat_one_no_effects", repeat)
        scenario("repeat_two_no_effects", repeat)
        scenario("replay_without_network", lambda: execute(plan, v2, run, False, True, client, OUTPUTS, Offline({}), LIMITS))

        def older_layout() -> None:
            result = execute(plan, v1, root / "v1", True, True, client, OUTPUTS, web, LIMITS)
            header = (root / "v1" / "batches" / v1["id"] / "capture/derived/organisation_owners.csv").read_text().splitlines()[0]
            require(result["status"] == "stored" and "PRIVATE EQUITY" not in header and "REIT" not in header, "Older layout gained flags")

        scenario("older_layout_stored_without_invented_flags", older_layout)
        derive_scenarios(scenario, v1, v2)
        fetch_scenarios(scenario, plan, v2, root, bodies)
        corrupt_scenarios(scenario, receipt_path, bodies[v2["url"]])
        storage_scenarios(scenario, plan, v1, v2, root, run, client, web)

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
                execute(plan, v1, root / "unreviewed", True, True, client, OUTPUTS, web, LIMITS)

        scenario("unreviewed_current_code_before_effects", unreviewed_current, "Unreviewed CMS owners")
        scenario("unreviewed_run_wrote_nothing", lambda: require(not (root / "unreviewed").exists(), "Unreviewed code wrote files"))

        def independent() -> None:
            broken = Web(dict(bodies))
            bad = by_period["2025-03-01"]
            broken.bodies[bad["url"]] = body("v2")
            s3 = FakeS3()
            results, held = run_all(plan, root / "all", True, True, s3, OUTPUTS, broken, LIMITS)
            require([h["period_start"] for h in held] == ["2025-03-01"] and "header differs" in held[0]["reason"], f"Held set differs: {held}")
            require(len(results) == 3 and all(r["status"] == "stored" for r in results), "Other releases did not complete")
            again, held_again = run_all(plan, root / "all", True, True, s3, OUTPUTS, broken, LIMITS)
            require(again == results and len(held_again) == 1, "Rerun differs")

        scenario("failing_release_held_others_stored", independent)
    return scenarios


def derive_scenarios(scenario: Any, v1: dict, v2: dict) -> None:
    """Every content defect named in the failure modes stops that release."""

    def row_edit(index: int, column: str, value: str, layout: str = "v2") -> Any:
        header = contract.HEADERS[layout]
        return lambda t: t[index].__setitem__(header.index(column), value)

    cases: list[tuple[str, dict, bytes, str]] = [
        ("unknown_owner_type", v2, body("v2", row_edit(3, "TYPE - OWNER", "X")), "type differs"),
        ("organisation_row_with_first_name", v2, body("v2", row_edit(3, "FIRST NAME - OWNER", "Kim")), "carries a personal name"),
        ("organisation_row_with_title", v2, body("v2", row_edit(3, "TITLE - OWNER", "OFFICER")), "carries a personal name"),
        ("flag_value_changed", v2, body("v2", row_edit(3, "REIT - OWNER", "Yes")), "flag value differs"),
        ("blank_enrollment_id", v2, body("v2", row_edit(4, "ENROLLMENT ID", "")), "no enrollment ID"),
        ("no_organisation_rows", v2, body("v2", lambda t: t.__delitem__(slice(3, 6))), "no organisation rows"),
        ("short_row", v2, body("v2", lambda t: t[3].pop()), "row shape differs"),
        ("new_layout_in_old_period", v1, body("v2"), "header differs"),
        ("old_layout_in_new_period", v2, body("v1"), "header differs"),
        ("extra_column", v2, body("v2", lambda t: [r.append("") for r in t]), "header differs"),
        ("html_instead_of_csv", v2, b"<html><body>error</body></html>", "header differs"),
    ]
    for name, release, raw, error in cases:
        scenario(name, lambda release=release, raw=raw: contract.derive(raw, release), error)

    def windows_1252() -> None:
        raw = body("v2", row_edit(3, "ORGANIZATION NAME - OWNER", "CAFÉ HEALTH LLC"), encoding="cp1252")
        derived, stats = contract.derive(raw, v2)
        require(stats["encoding"] == "cp1252" and "CAFÉ HEALTH LLC" in derived.decode(), "Windows-1252 text lost")

    scenario("windows_1252_decoded_and_recorded", windows_1252)


def fetch_scenarios(scenario: Any, plan: dict, release: dict, root: Path, bodies: dict[str, bytes]) -> None:
    """Failed or wrong downloads are held and never reach a snapshot."""
    url = release["url"]
    cases: list[tuple[str, Response | Exception, str]] = [
        ("html_page_held", Response(b"<!doctype html><html></html>", url), "download failed"),
        ("server_error_held", Response(b"busy", url, status=503), "download failed"),
        ("truncated_download_held", Response(bodies[url][:-10], url, headers={"Content-Length": str(len(bodies[url]))}), "download failed"),
        ("connection_error_held", OSError("reset"), "download failed"),
    ]
    for name, response, error in cases:
        web = Web(bodies)
        web.override[url] = response
        target = root / f"fetch_{name}"
        scenario(name, lambda web=web, target=target: execute(plan, release, target, True, False, None, OUTPUTS, web, LIMITS), error)
        scenario(f"{name}_no_snapshot", lambda target=target: require(not (target / "batches").exists(), "Failed download produced a snapshot"))
    scenario(
        "no_fetch_without_original",
        lambda: execute(plan, release, root / "no_fetch", False, False, None, OUTPUTS, Offline({}), LIMITS),
        "original not captured",
    )


def corrupt_scenarios(scenario: Any, receipt_path: Path, original_bytes: bytes) -> None:
    """Replay refuses tampered artifacts, originals, lineage and governance, even when hashes are resealed."""
    collection = receipt_path.parents[3]

    def corrupt(kind: str) -> None:
        target_root = collection.parent / f"corrupt_{kind}"
        shutil.copytree(collection, target_root)
        target = target_root / receipt_path.relative_to(collection).parent
        receipt = read_json(target / "receipt.json")
        lineage = json.loads(receipt["lineage"]["extraction_or_query"])
        original = next((target_root / "private_original").rglob("*.csv"))

        def reseal(relative: str) -> None:
            for item in receipt["artifacts"]:
                if item["storage_path"] == relative:
                    item["sha256"], item["byte_count"] = fingerprint(target / relative)

        if kind == "resealed_csv_with_individual":
            path = target / "derived/organisation_owners.csv"
            path.write_bytes(path.read_bytes() + b"Alex Example\n")
            reseal("derived/organisation_owners.csv")
        elif kind == "original_changed":
            original.write_bytes(original_bytes + b"\r\n")
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
        elif kind == "pii_governance":
            receipt["governance"]["contains_pii"] = True
        receipt["lineage"]["extraction_or_query"] = json.dumps(lineage)
        (target / "receipt.json").write_bytes(encoded_json(receipt))
        verify_local(target / "receipt.json")

    for kind, error in [
        ("resealed_csv_with_individual", "derived CSV differs"),
        ("original_changed", "original hash differs"),
        ("original_deleted", "No such file"),
        ("original_copied_into_snapshot", "holds original bytes"),
        ("model_promotion", "hold differs"),
        ("unreviewed_code", "Unreviewed CMS owners"),
        ("wrong_plan", "plan differs"),
        ("pii_governance", "governance differs"),
    ]:
        scenario(kind, lambda kind=kind: corrupt(kind), error)


def storage_scenarios(scenario: Any, plan: dict, v1: dict, v2: dict, root: Path, run: Path, client: FakeS3, web: Web) -> None:
    """Interrupted uploads resume without duplicates; resealed storage evidence is refused."""

    def interrupted() -> None:
        partial, s3 = root / "partial", FakeS3()
        s3.corrupt = True
        with suppress(ValueError):
            execute(plan, v1, partial, True, True, s3, OUTPUTS, web, LIMITS)
        require(len(s3.objects) == 1, "No interrupted upload")
        s3.corrupt = False
        execute(plan, v1, partial, True, True, s3, OUTPUTS, web, LIMITS)
        first = copy.deepcopy(s3.objects)
        execute(plan, v1, partial, True, True, s3, OUTPUTS, web, LIMITS)
        require(first == s3.objects, "Resume duplicated effects")

    scenario("resume_interrupted_storage", interrupted)

    def invalid_version() -> None:
        target = root / "bad_storage"
        shutil.copytree(run, target)
        branch = target / "batches" / v2["id"]
        path = branch / "capture/s3_collections_reconciliation.json"
        rec = read_json(path)
        rec["objects"][0]["object"]["version_id"] = ""
        path.write_bytes(encoded_json(rec))
        done = read_json(branch / "completed.json")
        done["reconciliation_sha256"] = fingerprint(path)[0]
        (branch / "completed.json").write_bytes(encoded_json(done))
        execute(plan, v2, target, False, True, client, OUTPUTS, Offline({}), LIMITS)

    scenario("resealed_missing_storage_version", invalid_version, "version missing")
    scenario(
        "release_outside_plan_rejected",
        lambda: execute(plan, v2 | {"period_end": "2026-09-30"}, root / "outside", True, False, None, OUTPUTS, web, LIMITS),
        "plan or release",
    )


def main() -> None:
    """Write one immutable artifact for every E2E invocation."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="cms_owners_e2e_") as directory:
        scenarios = exercise(Path(directory))
    passed = all(s["passed"] for s in scenarios)
    report = {
        "status": "passed" if passed else "failed",
        "feature": "CMS Hospital All Owners capture: original kept locally, organisation rows only through versioned S3 storage",
        "run_at_utc": datetime.now(UTC).isoformat(),
        "python": sys.version.split()[0],
        "code_sha256": bls_contract.code_hashes(),
        "dependency_sha256": bls_contract.digest(Path("requirements.txt").read_bytes()),
        "reproduce": f".venv/bin/python -m scripts.acquisition.run_cms_owners_e2e --output {args.output}",
        "boundary": "Synthetic five-row releases in both layouts, served by a fake opener through the real downloader; fake versioned S3; no live CMS or AWS.",
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
