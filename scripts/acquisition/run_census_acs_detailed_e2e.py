"""Repeatable synthetic ACS detailed-table capture, derivation checks and S3 storage, without live requests or credentials."""

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
from scripts.acquisition import census_acs_detailed_contract as contract
from scripts.acquisition.build_census_acs_detailed_plan import build
from scripts.acquisition.collect_census_acs_detailed import derived_id, execute, run_all, verify_local
from scripts.acquisition.run_bls_api_e2e import inventory
from scripts.acquisition.s3_store import encoded_json, fingerprint, write_once
from scripts.acquisition.source_registry import canonical_hash, read_json, require
from scripts.acquisition.tests.test_s3_store import OUTPUTS, FakeS3

TABLE_YEARS = {"B17001": [2011, 2012], "B16001": [2015], "B16004": [2015]}
DERIVED = {"poverty": [2011], "english": [2015]}
COUNTIES = [f"{s:02d}{c:03d}" for s in range(1, 63) for c in range(1, 51)]
LABELS = {
    "B17001": {"001": "Estimate!!Total", "002": "Estimate!!Total!!Income in the past 12 months below poverty level", "003": "Estimate!!Total!!Male"},
    "B16001": {
        "001": "Estimate!!Total",
        "002": "Estimate!!Total!!Speak only English",
        "003": "Estimate!!Total!!Spanish",
        "004": 'Estimate!!Total!!Spanish!!Speak English "very well"',
        "005": 'Estimate!!Total!!Spanish!!Speak English less than "very well"',
        "006": "Estimate!!Total!!Other languages",
        "007": 'Estimate!!Total!!Other languages!!Speak English "very well"',
        "008": 'Estimate!!Total!!Other languages!!Speak English less than "very well"',
    },
    "B16004": {
        "001": "Estimate!!Total",
        "002": "Estimate!!Total!!5 to 17 years",
        "003": 'Estimate!!Total!!5 to 17 years!!Speak Spanish!!Speak English "very well"',
        "004": 'Estimate!!Total!!5 to 17 years!!Speak Spanish!!Speak English "well"',
        "005": 'Estimate!!Total!!5 to 17 years!!Speak Spanish!!Speak English "not well"',
    },
}


def variables(table: str) -> dict:
    """A group dictionary with estimates, margins and annotations."""
    out = {"GEO_ID": {"label": "Geography"}, "NAME": {"label": "Geographic Area Name"}}
    for line, label in LABELS[table].items():
        for suffix in ("E", "M", "EA", "MA"):
            out[f"{table}_{line}{suffix}"] = {"label": label}
    return out


def value(table: str, line: str, index: int) -> int:
    """Deterministic synthetic estimates with consistent totals."""
    base = 1000 + index
    if table == "B16004":
        return {"001": base, "002": base // 4, "003": base // 10, "004": base // 20, "005": base // 32}[line]
    return {"001": base, "002": base // 5, "003": base // 2, "004": base // 10, "005": base // 20, "006": base // 8, "007": base // 16, "008": base // 32}[line]


def response(table: str, change: Any = None) -> bytes:
    """A Census group response for every synthetic county."""
    names = sorted([*variables(table), "state", "county"])
    rows: list[list[str | None]] = [names]
    for i, county in enumerate(COUNTIES):
        cells: dict[str, str | None] = {"GEO_ID": f"0500000US{county}", "NAME": f"County {county}", "state": county[:2], "county": county[2:]}
        for line in LABELS[table]:
            cells[f"{table}_{line}E"] = str(value(table, line, i))
            cells[f"{table}_{line}M"] = "10"
            cells[f"{table}_{line}EA"] = None
            cells[f"{table}_{line}MA"] = None
        rows.append([cells[n] for n in names])
    if change is not None:
        change(rows)
    return json.dumps(rows).encode()


def comparison(kind: str, change: Any = None) -> str:
    """A data.census.gov-style export matching the synthetic derivation exactly."""
    require(kind == "poverty", "Only S1701 exports are compared")
    header = ["GEO_ID", "NAME", "S1701_C01_001E", "S1701_C02_001E", "S1701_C03_001E"]
    data = [[f"0500000US{c}", c, str(value("B17001", "001", i)), str(value("B17001", "002", i))] for i, c in enumerate(COUNTIES)]
    data = [[*r, f"{round(int(r[3]) / int(r[2]) * 100, 1)}"] for r in data]
    rows = [header, ["label"] * len(header), *data]
    if change is not None:
        change(rows)
    output = io.StringIO(newline="")
    csv.writer(output, lineterminator="\n").writerows(rows)
    return output.getvalue()


def fixtures(root: Path, poverty_change: Any = None) -> tuple[Path, Path]:
    """Dictionaries with download receipts, and stored comparison exports."""
    refs, browser = root / "references", root / "browser"
    for table, years in TABLE_YEARS.items():
        for year in years:
            folder = refs / f"{table}_{year}"
            folder.mkdir(parents=True)
            (folder / "group.json").write_bytes(encoded_json({"variables": variables(table)}))
            url = f"{contract.endpoint(year)}/groups/{table}.json"
            sha = bls_contract.digest((folder / "group.json").read_bytes())
            write_once(folder / "receipt.json", encoded_json({"complete": True, "requested_url": url, "path": str(folder / "group.json"), "sha256": sha}))
    members = browser / "capture" / "history_members" / "members"
    members.mkdir(parents=True)
    (members / "ACSST5Y2012.S1701-Data.csv").write_text(comparison("poverty", poverty_change))
    return refs, browser


def locked(root: Path, poverty_change: Any = None) -> tuple[dict, Path]:
    """A locked synthetic plan."""
    refs, browser = fixtures(root, poverty_change)
    comparisons = [("poverty", 2012, "ACSST5Y2012.S1701-Data.csv"), ("english", 2015, None)]
    plan = build(refs, TABLE_YEARS, DERIVED, comparisons, browser)
    path = root / "plan.json"
    write_once(path, encoded_json(plan))
    write_once(path.with_suffix(".lock.json"), encoded_json({"plan_sha256": canonical_hash(plan)}))
    return plan, path


class Requests:
    """A fake keyed request: serves synthetic responses and counts calls; overrides change one table."""

    def __init__(self) -> None:
        self.calls = 0
        self.override: dict[tuple[str, int], bytes] = {}

    def __call__(self, plan: dict, batch: dict, client: Any) -> bytes:
        self.calls += 1
        return self.override.get((batch["table"], batch["year"]), response(batch["table"]))


def exercise(root: Path) -> list[dict]:
    """Production orchestration with synthetic inputs and versioned fake storage."""
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

    def real_scope() -> None:
        real = read_json(contract.PLAN_PATH)
        require(read_json(contract.PLAN_PATH.with_suffix(".lock.json"))["plan_sha256"] == canonical_hash(real), "Real plan lock differs")
        tables = [(b["table"], b["year"]) for b in real["batches"]]
        require(len(tables) == 21 and len(real["comparisons"]) == 12, "Real scope differs")
        lines = {(b["table"], len(b["less_lines"])) for b in real["batches"] if b["table"] != "B17001"}
        require(lines == {("B16001", 39), ("B16004", 36)}, "Real English line counts differ")

    scenario("real_plan_binds_21_tables_12_checks_and_label_lines", real_scope)
    base = root / "base"
    base.mkdir()
    plan, plan_path = locked(base)
    requests, client = Requests(), FakeS3()
    with patch.object(contract, "PLAN_PATH", plan_path), patch.object(contract, "VERSIONS_PATH", versions):
        scenario("locked_scope", contract.load_plan)
        run = root / "run"
        first = plan["batches"][0]
        scenario("capture_one_without_storage", lambda: execute(plan, first, run, True, False, client, OUTPUTS, requests))
        scenario("offline_receipt_replay", lambda: verify_local(run / "batches" / first["id"] / "capture/receipt.json"))

        def all_stored() -> None:
            results, held = run_all(plan, run, True, True, client, OUTPUTS, requests)
            require(not held and len(results) == 5 and all(r["status"] == "stored" for r in results), f"Held: {held}")

        scenario("all_captures_and_derived_snapshot_stored", all_stored)
        derived_capture = run / "batches" / derived_id(plan) / "capture"

        def derived_values() -> None:
            rows = list(csv.DictReader(io.StringIO((derived_capture / "derived/poverty_2011_2011.csv").read_text())))
            first_row = rows[0]
            require(first_row["county"] == COUNTIES[0] and float(first_row["numerator"]) == 200 and float(first_row["denominator"]) == 1000, "Poverty inputs")
            require(abs(float(first_row["percent"]) - 20.0) < 1e-9 and first_row["percent_moe_approx"] != "", "Poverty percent")
            english = list(csv.DictReader(io.StringIO((derived_capture / "derived/english_2015_2015.csv").read_text())))
            require(float(english[0]["numerator"]) == 50 + 31 and english[0]["source_lines"] == "B16001_005E+B16001_008E", "English sum or lines")
            report = read_json(derived_capture / "evidence/checks.json")
            require([c["mismatches"] for c in report["checks"]] == [0, 0] and report["checks"][0]["counties_compared"] == len(COUNTIES), "Checks")

        scenario("derived_values_lines_and_checks", derived_values)
        before, objects, calls = inventory(run), copy.deepcopy(client.objects), requests.calls

        def repeat() -> None:
            results, held = run_all(plan, run, True, True, client, OUTPUTS, requests)
            require(not held and before == inventory(run) and objects == client.objects and requests.calls == calls, "Repeated effects")

        scenario("repeat_one_no_effects", repeat)
        scenario("repeat_two_no_effects", repeat)
        scenario("replay_without_network", lambda: require(not run_all(plan, run, False, True, client, OUTPUTS, None)[1], "Replay needed the network"))
        response_scenarios(scenario, plan)
        corrupt_scenarios(scenario, run, plan)

        def unreviewed_current() -> None:
            with patch.object(contract, "code_hashes", return_value={"changed": "0" * 64}):
                execute(plan, first, root / "unreviewed", True, True, client, OUTPUTS, requests)

        scenario("unreviewed_current_code_before_effects", unreviewed_current, "Unreviewed Census detailed")
        scenario("unreviewed_run_wrote_nothing", lambda: require(not (root / "unreviewed").exists(), "Unreviewed code wrote files"))

        def interrupted() -> None:
            partial, s3 = root / "partial", FakeS3()
            s3.corrupt = True
            with suppress(ValueError):
                execute(plan, first, partial, True, True, s3, OUTPUTS, requests)
            require(len(s3.objects) == 1, "No interrupted upload")
            s3.corrupt = False
            execute(plan, first, partial, True, True, s3, OUTPUTS, requests)
            stored = copy.deepcopy(s3.objects)
            execute(plan, first, partial, True, True, s3, OUTPUTS, requests)
            require(stored == s3.objects, "Resume duplicated effects")

        scenario("resume_interrupted_storage", interrupted)
    mismatch_scenarios(scenario, root, versions)
    return scenarios


def response_scenarios(scenario: Any, plan: dict) -> None:
    """Response defects stop that table-year."""
    batch = next(b for b in plan["batches"] if b["table"] == "B17001")

    def drop_last_column(rows: list) -> None:
        for r in rows:
            r.pop()

    cases = [
        ("header_differs", response("B17001", drop_last_column), "headers differ"),
        ("duplicate_county", response("B17001", lambda rows: rows.append(list(rows[1]))), "Duplicate county"),
        ("below_county_floor", response("B17001", lambda rows: rows.__delitem__(slice(1, 200))), "below the floor"),
        ("geography_mismatch", response("B17001", lambda rows: rows[1].__setitem__(rows[0].index("GEO_ID"), "0500000US99999")), "geography invalid"),
        ("not_json", b"<html>", "invalid JSON"),
    ]
    for name, raw, error in cases:
        scenario(name, lambda raw=raw: contract.validate_response(raw, batch), error)

    def sentinel_skipped() -> None:
        def sentinel(rows: list) -> None:
            rows[1][rows[0].index("B17001_001E")] = "-666666666"

        derived = contract.measure(contract.table_rows(contract.validate_response(response("B17001", sentinel), batch)[0]), ["B17001_002E"], "B17001_001E")
        require(derived[COUNTIES[0]] is None and sum(v is None for v in derived.values()) == 1, "Sentinel used as a number")

    scenario("sentinel_value_never_used_as_number", sentinel_skipped)


def corrupt_scenarios(scenario: Any, run: Path, plan: dict) -> None:
    """Replay refuses tampered derived files, inputs and lineage, even when hashes are resealed."""
    capture = run / "batches" / derived_id(plan) / "capture"

    def corrupt(kind: str) -> None:
        target_root = run.parent / f"corrupt_{kind}"
        shutil.copytree(run, target_root)
        target = target_root / capture.relative_to(run)
        receipt = read_json(target / "receipt.json")
        lineage = json.loads(receipt["lineage"]["extraction_or_query"])
        if kind == "resealed_derived_csv":
            path = target / "derived/poverty_2011_2011.csv"
            path.write_bytes(path.read_bytes() + b"2011,99999,1,1,100,0,B17001,x\n")
            for item in receipt["artifacts"]:
                if item["storage_path"] == "derived/poverty_2011_2011.csv":
                    item["sha256"], item["byte_count"] = fingerprint(path)
        elif kind == "input_capture_changed":
            first = plan["batches"][0]
            path = target_root / "batches" / first["id"] / "capture/derived/observations.csv"
            path.write_bytes(path.read_bytes().replace(b",1000,", b",1001,", 1))
        elif kind == "model_promotion":
            lineage["model_eligible"] = True
        elif kind == "unreviewed_code":
            lineage["code_sha256"] = {"bad": "0" * 64}
        receipt["lineage"]["extraction_or_query"] = json.dumps(lineage)
        (target / "receipt.json").write_bytes(encoded_json(receipt))
        verify_local(target / "receipt.json")

    for kind, error in [
        ("resealed_derived_csv", "derived CSV differs"),
        ("input_capture_changed", "inputs differ"),
        ("model_promotion", "hold differs"),
        ("unreviewed_code", "Unreviewed Census detailed"),
    ]:
        scenario(kind, lambda kind=kind: corrupt(kind), error)


def mismatch_scenarios(scenario: Any, root: Path, versions: Path) -> None:
    """A derivation that does not reproduce the published table is never stored."""

    def poverty_off(rows: list) -> None:
        rows[2][3] = str(int(rows[2][3]) + 1)

    def english_off(rows: list) -> None:
        column = rows[0].index("B16004_004E")
        rows[5][column] = str(int(rows[5][column]) + 1)

    def missing_county(rows: list) -> None:
        rows.pop()

    for name, poverty, english, error in [
        ("poverty_mismatch_blocks_derived", poverty_off, None, "does not reproduce"),
        ("english_mismatch_blocks_derived", None, english_off, "does not reproduce"),
        ("county_set_mismatch_blocks_derived", missing_county, None, "county sets differ"),
    ]:

        def action(name: str = name, poverty: Any = poverty, english: Any = english, error: str = error) -> None:
            folder = root / name
            folder.mkdir()
            plan, path = locked(folder, poverty)
            requests = Requests()
            if english is not None:
                requests.override[("B16004", 2015)] = response("B16004", english)
            with patch.object(contract, "PLAN_PATH", path), patch.object(contract, "VERSIONS_PATH", versions):
                s3 = FakeS3()
                results, held = run_all(plan, folder / "run", True, True, s3, OUTPUTS, requests)
                require(len(results) == 4 and [h["table"] for h in held] == ["derived"] and error in held[0]["reason"], f"Held: {held}")
                require(not (folder / "run" / "batches" / derived_id(plan) / "capture/receipt.json").exists(), "Derived snapshot written")
                require(not any("derived/" in k and ("poverty_" in k or "english_" in k) for k in s3.objects), "Derived values stored")

        scenario(name, action)


def main() -> None:
    """Write one immutable artifact for every E2E invocation."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="census_detailed_e2e_") as directory:
        scenarios = exercise(Path(directory))
    passed = all(s["passed"] for s in scenarios)
    report = {
        "status": "passed" if passed else "failed",
        "feature": "ACS detailed tables (B17001, B16001) capture, exact overlap checks and derived history through versioned S3 storage",
        "run_at_utc": datetime.now(UTC).isoformat(),
        "python": sys.version.split()[0],
        "code_sha256": bls_contract.code_hashes(),
        "dependency_sha256": bls_contract.digest(Path("requirements.txt").read_bytes()),
        "reproduce": f".venv/bin/python -m scripts.acquisition.run_census_acs_detailed_e2e --output {args.output}",
        "boundary": "Synthetic dictionaries, 3,100-county responses and comparison exports; fake keyed request and fake versioned S3; no live Census or AWS.",
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
