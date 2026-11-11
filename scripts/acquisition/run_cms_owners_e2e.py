"""Repeatable synthetic CMS Hospital All Owners capture-to-S3 verification, without live downloads or credentials.

All the data is public: a capture stores the release whole under raw/ as published and records that it names individual owners, whom bronze
pseudonymizes at load. Captures stored under plan version 1 (organisation rows only, original on the Mac) keep verifying.
"""

import argparse
import copy
import csv
import io
import json
import shutil
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
# Synthetic individuals; the published file names them, so the stored file does too.
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


def exercise(root: Path) -> list[dict]:
    """Use production orchestration with synthetic releases and versioned fake storage."""
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

        def original_stored_whole() -> None:
            capture = receipt_path.parent
            paths = {item["storage_path"] for item in read_json(receipt_path)["artifacts"]}
            require(paths == {f"raw/{contract.stored_name(v2)}", "evidence/download_proof.json", "references/scope.json"}, f"Artifact set differs: {paths}")
            require((capture / "raw" / contract.stored_name(v2)).read_bytes() == bodies[v2["url"]], "The release is not stored as published")
            require(not (run / "private_original").exists() and not (capture / "derived").exists(), "A private folder or a derived copy was made")
            stats = read_json(capture / "evidence/download_proof.json")["statistics"]
            require((stats["rows_read"], stats["individual_rows"], stats["organisation_rows"]) == (5, 2, 3), f"Counts differ: {stats}")

        scenario("original_stored_whole_as_published", original_stored_whole)

        def public_with_names() -> None:
            receipt = read_json(receipt_path)
            governance = receipt["governance"]
            require(governance["access_class"] == "public" and governance["contains_pii"] is True, "Governance does not say the public file names people")
            require(contract.PSEUDONYMIZE_AT_LOAD in governance["use_restrictions"], "The bronze pseudonymization restriction is missing")
            require(receipt["schema_profile"]["native_headers"] == contract.HEADERS["v2"], "The schema profile is not the published header")

        scenario("receipt_records_public_names_pseudonymized_in_bronze", public_with_names)
        scenario("capture_and_version_verified_storage", lambda: execute(plan, v2, run, True, True, client, OUTPUTS, web, LIMITS))

        def stored_as_published() -> None:
            require(any(o["body"] == bodies[v2["url"]] for o in client.objects.values()), "S3 does not hold the release as published")
            require(all(key.startswith("raw/") or not key.startswith("private") for key in client.objects), "An object left the storage layout")

        scenario("s3_holds_the_release_as_published", stored_as_published)
        before, objects, requests = inventory(run), copy.deepcopy(client.objects), web.requests

        def repeat() -> None:
            execute(plan, v2, run, True, True, client, OUTPUTS, web, LIMITS)
            require(before == inventory(run) and objects == client.objects and web.requests == requests, "Repeated effects")

        scenario("repeat_one_no_effects", repeat)
        scenario("repeat_two_no_effects", repeat)
        scenario("replay_without_network", lambda: execute(plan, v2, run, False, True, client, OUTPUTS, Offline({}), LIMITS))

        def older_layout() -> None:
            result = execute(plan, v1, root / "v1", True, True, client, OUTPUTS, web, LIMITS)
            stored = (root / "v1" / "batches" / v1["id"] / "capture/raw" / contract.stored_name(v1)).read_bytes()
            require(result["status"] == "stored" and stored == bodies[v1["url"]], "Older layout not stored as published")

        scenario("older_layout_stored_as_published", older_layout)
        check_scenarios(scenario, v1, v2)
        version_1_scenario(scenario, v2, root, bodies[v2["url"]])
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
            with patch.object(contract, "collector_code_hashes", return_value={"changed": "0" * 64}):
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


def check_scenarios(scenario: Any, v1: dict, v2: dict) -> None:
    """A release that is not the published shape stops that release: header, row width, owner type, flag values and enrollment ID."""

    def row_edit(index: int, column: str, value: str, layout: str = "v2") -> Any:
        header = contract.HEADERS[layout]
        return lambda t: t[index].__setitem__(header.index(column), value)

    cases: list[tuple[str, dict, bytes, str]] = [
        ("unknown_owner_type", v2, body("v2", row_edit(3, "TYPE - OWNER", "X")), "type differs"),
        ("flag_value_changed", v2, body("v2", row_edit(3, "REIT - OWNER", "Yes")), "flag value differs"),
        ("blank_enrollment_id", v2, body("v2", row_edit(4, "ENROLLMENT ID", "")), "no enrollment ID"),
        ("no_rows", v2, body("v2", lambda t: t.__delitem__(slice(1, 6))), "has no rows"),
        ("short_row", v2, body("v2", lambda t: t[3].pop()), "row shape differs"),
        ("new_layout_in_old_period", v1, body("v2"), "header differs"),
        ("old_layout_in_new_period", v2, body("v1"), "header differs"),
        ("extra_column", v2, body("v2", lambda t: [r.append("") for r in t]), "header differs"),
        ("html_instead_of_csv", v2, b"<html><body>error</body></html>", "header differs"),
    ]
    for name, release, raw, error in cases:
        scenario(name, lambda release=release, raw=raw: contract.check_raw(raw, release), error)

    def windows_1252() -> None:
        stats = contract.check_raw(body("v2", row_edit(3, "ORGANIZATION NAME - OWNER", "CAFÉ HEALTH LLC"), encoding="cp1252"), v2)
        require(stats["encoding"] == "cp1252", "Windows-1252 not recorded")

    scenario("windows_1252_decoded_and_recorded", windows_1252)


def version_1_scenario(scenario: Any, release: dict, root: Path, original: bytes) -> None:
    """A capture stored under plan version 1 keeps verifying: its derived CSV is rebuilt from the original kept on the Mac."""

    def stored_version_1() -> None:
        collection = root / "version_1"
        capture = collection / "batches" / release["id"] / "capture"
        kept = collection / "private_original" / release["id"] / "run_01" / "attempt_01" / contract.stored_name(release)
        kept.parent.mkdir(parents=True)
        kept.write_bytes(original)
        derived, statistics = contract.derive(original, release)
        proof = {"requested_url": release["url"], "retrieved_at_utc": "2026-09-29T00:00:00+00:00", "sha256": fingerprint(kept)[0], "bytes": len(original)}
        proof |= {"original_path": kept.relative_to(collection).as_posix(), "statistics": statistics}
        plan = contract.load_plan()
        for relative, content in (("derived/organisation_owners.csv", derived), ("evidence/download_proof.json", encoded_json(proof))):
            write_once(capture / relative, content)
        write_once(capture / "references/scope.json", encoded_json(plan))
        receipt = {
            "snapshot_status": "acquired_unvalidated",
            "artifacts": [{"storage_path": name} for name in ("derived/organisation_owners.csv", "evidence/download_proof.json", "references/scope.json")],
            "acquisition": {"requested_url": release["url"], "http_status": 200, "retrieved_at_utc": proof["retrieved_at_utc"]},
            "schema_profile": {"native_headers": contract.kept_columns(release["layout"]), "row_count": statistics["organisation_rows_kept"]},
            "governance": {"contains_pii": False, "license_or_terms_url": contract.TERMS_URL},
            "measurement_periods": [{"start_date": release["period_start"], "end_date": release["period_end"]}],
        }
        lineage = {"release_id": release["id"], "plan_sha256": canonical_hash(plan), "expected_sha256": proof["sha256"], "model_eligible": False}
        lineage |= {"code_sha256": contract.collector_code_hashes(), "registry_sha256": plan["registry_sha256"]}
        from scripts.acquisition.capture import receipt_validator

        lineage["schema_sha256"] = canonical_hash(receipt_validator().schema)
        source = {"source_id": contract.SOURCE_ID}
        contract.verify_capture(receipt, source, lineage, capture, False)

    scenario("stored_version_1_capture_still_verifies", stored_version_1)


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
    """Replay refuses tampered files, lineage and governance, even when hashes are resealed."""
    collection = receipt_path.parents[3]

    def corrupt(kind: str) -> None:
        target_root = collection.parent / f"corrupt_{kind}"
        shutil.copytree(collection, target_root)
        target = target_root / receipt_path.relative_to(collection).parent
        receipt = read_json(target / "receipt.json")
        lineage = json.loads(receipt["lineage"]["extraction_or_query"])
        stored = next((target / "raw").glob("*.csv"))

        def reseal(path: Path) -> None:
            for item in receipt["artifacts"]:
                if item["storage_path"] == path.relative_to(target).as_posix():
                    item["sha256"], item["byte_count"] = fingerprint(path)

        if kind == "resealed_changed_release":
            stored.write_bytes(original_bytes + b"\r\n")
            reseal(stored)
        elif kind == "stored_release_deleted":
            stored.unlink()
        elif kind == "derived_copy_added":
            (target / "derived").mkdir()
            (target / "derived" / "organisation_owners.csv").write_bytes(b"x\n")
        elif kind == "model_promotion":
            lineage["model_eligible"] = True
        elif kind == "unreviewed_code":
            lineage["code_sha256"] = {"bad": "0" * 64}
        elif kind == "wrong_plan":
            lineage["plan_sha256"] = "0" * 64
        elif kind == "names_hidden_in_governance":
            receipt["governance"]["contains_pii"] = False
        elif kind == "pseudonymization_restriction_dropped":
            receipt["governance"]["use_restrictions"] = [item for item in receipt["governance"]["use_restrictions"] if item != contract.PSEUDONYMIZE_AT_LOAD]
        receipt["lineage"]["extraction_or_query"] = json.dumps(lineage)
        (target / "receipt.json").write_bytes(encoded_json(receipt))
        verify_local(target / "receipt.json")

    for kind, error in [
        ("resealed_changed_release", "release hash differs"),
        ("stored_release_deleted", "Artifact is missing"),
        ("derived_copy_added", "holds a derived copy"),
        ("model_promotion", "hold differs"),
        ("unreviewed_code", "Unreviewed CMS owners"),
        ("wrong_plan", "plan differs"),
        ("names_hidden_in_governance", "governance differs"),
        ("pseudonymization_restriction_dropped", "governance differs"),
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
        "feature": "CMS Hospital All Owners capture: the release stored whole under raw/ as published, through versioned S3 storage",
        "run_at_utc": datetime.now(UTC).isoformat(),
        "python": sys.version.split()[0],
        "code_sha256": contract.collector_code_hashes(),
        "dependency_sha256": bls_contract.digest(Path("uv.lock").read_bytes()),
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
