"""Exercise the MMD collector's real offline CLI against disposable pilot copies.

The retained captures of the pilot years were made under the two plans that the registry rewrite retired, and the collector refuses them by name. The runner
therefore keeps them for two jobs only: the refusal of a retired plan, and the stored responses from which it rebuilds a disposable capture under the current
plan. Every success, replay and mutation case runs on that disposable capture. What the runner still needs from the local data folder: the plan inputs as the
publisher stages them (``host_inputs.prepare``) and the retained capture of C258.07 2023 with its raw page and evidence.
"""

import argparse
import hashlib
import json
import shutil
import sys
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

from scripts.acquisition import mmd_api_contract as contract
from scripts.acquisition.cli_tools import run_argv
from scripts.acquisition.data_paths import current
from scripts.acquisition.mmd_api_contract import (
    PLAN_PATH,
    PRIOR_CODE_FILES,
    RETIRED_MESSAGE,
    RETIRED_PLAN_SHA256S,
    condition_for,
    current_code_hashes,
    load_plan,
    reconstruct,
    reviewed_code_versions,
)
from scripts.acquisition.run_mmd_api_collection import ATTEMPTS, with_retries
from scripts.acquisition.s3_store import StorageError, encoded_json, fingerprint, write_once
from scripts.acquisition.source_registry import RegistryError, read_json, require

RETIRED_PLAN = "97f72f7b82b39bfbb8afec75778fe335aceb59f57c0b5fa72c773517377fbfe4"
RETIRED_ADDITIONS_PLAN = "1525659d4da7ffd41ce12e4513dc2e04b0e4122a93b21439af2653e19f8656a8"
# A capture under a current plan is expected valid; one under a retired plan is expected refused by name; any other plan is refused as unbound.
OUTCOME_BY_PLAN_STATUS = {"current": "valid", "retired": RETIRED_MESSAGE, "unknown": "MMD authorization binding differs"}
RETIRED_EXPECTED = {"retired_plan": RETIRED_MESSAGE, "retired_additions_plan": RETIRED_MESSAGE}
NEGATIVE_EXPECTED = {
    "fips_three_digit": "Native FIPS width/value changed",
    "fips_padding_collision": "Duplicate native FIPS",
    "unreviewed_code_hashes": "MMD capture implementation changed; review before reuse",
    "wrong_filter": "MMD transport differs",
    "model_promotion": "MMD review hold differs",
    "wrong_plan": "MMD authorization binding differs",
    "derived_bytes": "Derived CSV differs",
    "raw_checksum": "Artifact hash or byte count differs",
    "missing_code_hashes": "MMD code binding is incomplete",
    "empty_code_hashes": "MMD code binding is incomplete",
    # A capture made under the closure binding holds the core files, so a file added to it is an unreviewed version, not an incomplete binding.
    "extra_code_file": "MMD capture implementation changed; review before reuse",
    "core_file_missing": "MMD code binding is incomplete",
    "current_file_set_unreviewed": "MMD capture implementation changed; review before reuse",
}
# These cases edit county codes past the probe window, so they need the real rows of a retained year.
NEEDS_STORED_ROWS = frozenset({"fips_three_digit", "fips_padding_collision", "fips_four_digit"})
# The disposable capture is checked with the current collector code approved in memory only, because the list that names it follows the E2E.
APPROVED_CLI = """
import sys
from unittest.mock import patch

from scripts.acquisition import collect_mmd_api, mmd_api_contract

approved = [*mmd_api_contract.reviewed_code_versions(), mmd_api_contract.current_code_hashes()]
sys.argv = ["collect_mmd_api", *sys.argv[1:]]
with (
    patch.object(collect_mmd_api, "reviewed_code_versions", lambda: approved),
    patch.object(mmd_api_contract, "reviewed_code_versions", lambda: approved),
):
    collect_mmd_api.main()
"""


def plan_status(lineage: dict) -> str:
    """Whether the plan a capture names is current, retired or unknown, which decides what the runner expects of it."""
    plan = lineage.get("plan_sha256")
    return "retired" if plan in RETIRED_PLAN_SHA256S else "current" if plan in contract.PLANS else "unknown"


def command(arguments: list[str], approve_current: bool = False) -> tuple[int, str]:
    """Run the actual local CLI without network or cloud flags; ``approve_current`` approves the current collector code in the child process only."""
    if approve_current:
        result = run_argv([sys.executable, "-c", APPROVED_CLI, *arguments], capture_output=True, text=True, check=False)
    else:
        result = run_argv([sys.executable, "-m", "scripts.acquisition.collect_mmd_api", *arguments], capture_output=True, text=True, check=False)
    return result.returncode, (result.stdout + result.stderr).strip()


def rewrite_fips(target: Path, receipt: dict, lineage: dict, kind: str) -> None:
    """Change county codes past the probe window and re-seal every dependent hash."""
    roles = {a["role"]: a for a in receipt["artifacts"]}
    raw_path, evidence_path = target / roles["api_page"]["storage_path"], target / roles["export_receipt"]["storage_path"]
    rows = json.loads(raw_path.read_bytes())
    # Probes cover offsets 0-3, so edits start at index 4 to leave completeness evidence intact.
    zero_led = [i for i in range(4, len(rows)) if rows[i]["fips"].startswith("0")]
    first, second = zero_led[0], zero_led[1]
    if kind == "fips_four_digit":
        rows[first]["fips"] = rows[first]["fips"][1:]
    elif kind == "fips_three_digit":
        rows[first]["fips"] = rows[first]["fips"][2:]
    else:
        rows[second]["fips"] = rows[first]["fips"][1:]
    raw = json.dumps(rows).encode()
    raw_path.write_bytes(raw)
    roles["api_page"]["sha256"], roles["api_page"]["byte_count"] = fingerprint(raw_path)
    evidence = read_json(evidence_path)
    evidence["main"].update(sha256=hashlib.sha256(raw).hexdigest(), bytes=len(raw))
    evidence_path.write_bytes(encoded_json(evidence))
    roles["export_receipt"]["sha256"], roles["export_receipt"]["byte_count"] = fingerprint(evidence_path)
    lineage["expected_sha256"] = lineage["derivation"]["input_sha256"] = hashlib.sha256(raw).hexdigest()


def retained_rows(source: Path) -> list[dict]:
    """The rows of the raw API page that the retained capture stored."""
    page = next(a for a in read_json(source)["artifacts"] if a["role"] == "api_page")
    return json.loads((source.parent / page["storage_path"]).read_bytes())


def unknown_county_case(name: str, replacement: str | None, rows: list[dict]) -> dict:
    """Rebuild the CSV from real rows with one row at a state's unknown-county code (990), or at another code that no lookup knows.

    CMS uses state code + 990 for beneficiaries whose county is unknown, and the contract accepts that code only. The rule does not depend on the measure,
    so the rows are the retained pilot year's.
    """
    plan = load_plan()
    require(not any(r["fips"] == "49990" for r in rows), "Retained C258.07 2023 evidence changed")
    rows = [dict(r, fips=replacement or "49990") if index == 4 else r for index, r in enumerate(rows)]
    try:
        derived, _ = reconstruct(plan, condition_for(plan, "C258.07", 2023), 2023, rows)
    except (ValueError, OSError, KeyError, TypeError, StopIteration) as error:
        return {"name": name, "observed": f"{type(error).__name__}: {error}", "derived_sha256": None}
    unknown = next(line for line in derived.decode().split("\r\n") if ",49990," in line)
    return {"name": name, "observed": "reconstructed", "derived_sha256": hashlib.sha256(derived).hexdigest(), "row_49990": unknown}


def retry_case(name: str, failures: list[Exception], expect_success: bool, expect_attempts: int) -> dict:
    """Drive the queue retry wrapper with a scripted action; no network, AWS or waiting."""
    attempts: list[int] = []

    def action() -> None:
        attempts.append(len(attempts) + 1)
        if len(attempts) <= len(failures):
            raise failures[len(attempts) - 1]

    retries: list[dict] = []
    try:
        with_retries(action, retries, name, sleep=lambda _seconds: None)
        succeeded = True
    except (ValueError, OSError, KeyError, TypeError, StopIteration):
        succeeded = False
    passed = succeeded == expect_success and len(attempts) == expect_attempts
    return {"name": name, "attempts": len(attempts), "recorded_retries": len(retries), "succeeded": succeeded, "passed": passed}


def branch_files(base: Path, measure: str, year: int) -> dict[str, tuple[str, int]]:
    """Fingerprint every local file for one condition-year to prove a rerun writes nothing."""
    root = base / measure / str(year)
    return {str(p): fingerprint(p) for p in sorted(root.rglob("*")) if p.is_file()}


def negative_copy(source: Path, kind: str, directory: Path) -> Path:
    """Mutate only a disposable local candidate; the production snapshot is read only."""
    target = directory / kind
    shutil.copytree(source.parent, target)
    receipt = read_json(target / "receipt.json")
    lineage = json.loads(receipt["lineage"]["extraction_or_query"])
    if kind.startswith("fips_"):
        rewrite_fips(target, receipt, lineage, kind)
    elif kind == "unreviewed_code_hashes":
        lineage["code_sha256"] = dict.fromkeys(lineage["code_sha256"], "0" * 64)
    elif kind == "wrong_filter":
        receipt["acquisition"]["request_parameters"]["condition"] = "999"
    elif kind == "model_promotion":
        lineage["model_eligible"] = True
    elif kind == "wrong_plan":
        lineage["plan_sha256"] = "0" * 64
    elif kind == "retired_plan":
        lineage["plan_sha256"] = RETIRED_PLAN
    elif kind == "retired_additions_plan":
        lineage["plan_sha256"] = RETIRED_ADDITIONS_PLAN
    elif kind == "missing_code_hashes":
        lineage.pop("code_sha256")
    elif kind == "empty_code_hashes":
        lineage["code_sha256"] = {}
    elif kind == "core_file_missing":
        lineage["code_sha256"] = {name: digest for name, digest in lineage["code_sha256"].items() if name != "scripts/acquisition/code_versions.py"}
    elif kind == "extra_code_file":
        lineage["code_sha256"] = dict(lineage["code_sha256"], **{"scripts/acquisition/unlisted.py": "0" * 64})
    elif kind == "current_file_set_unreviewed":
        lineage["code_sha256"] = dict.fromkeys(PRIOR_CODE_FILES, "0" * 64)
    elif kind == "derived_bytes":
        item = next(a for a in receipt["artifacts"] if a["role"] == "data")
        path = target / item["storage_path"]
        path.write_bytes(path.read_bytes() + b"\r\n")
        item["sha256"], item["byte_count"] = fingerprint(path)
    elif kind == "raw_checksum":
        item = next(a for a in receipt["artifacts"] if a["role"] == "api_page")
        path = target / item["storage_path"]
        path.write_bytes(path.read_bytes() + b"\n")
    else:
        raise ValueError(kind)
    receipt["lineage"]["extraction_or_query"] = json.dumps(lineage)
    (target / "receipt.json").write_bytes(encoded_json(receipt))
    return target / "receipt.json"


def pilot_case(source: Path, directory: Path) -> tuple[dict, Path | None]:
    """Rebuild a disposable capture under the current plan, offline, from the responses the retained capture stored, and return its receipt path.

    The stored page and the probes sit in the retained capture itself, so no separate transport folder is read. The capture gets the current code fingerprints.
    """
    from scripts.acquisition.collect_mmd_api import capture

    receipt = read_json(source)
    lineage = json.loads(receipt["lineage"]["extraction_or_query"])
    page = next(a for a in receipt["artifacts"] if a["role"] == "api_page")
    root = directory / "pilot_root"
    transport = root / "c258_07" / "2023" / "transport"
    transport.mkdir(parents=True)
    evidence = read_json(source.parent / "evidence" / "export_evidence.json")
    (transport / "main.json").write_bytes((source.parent / page["storage_path"]).read_bytes())
    (transport / "main_metadata.json").write_bytes(encoded_json(evidence["main"]))
    for probe in evidence["probes"]:
        offset = probe["url"].rsplit("_offset=", 1)[1]
        (transport / f"probe_{offset}.json").write_bytes(probe["body_utf8"].encode())
        (transport / f"probe_{offset}_metadata.json").write_bytes(encoded_json({k: v for k, v in probe.items() if k != "body_utf8"}))
    from scripts.acquisition import collect_mmd_api, mmd_api_contract

    # Approve the current fingerprint in memory only, so the capture reaches verify_capture; the real catalog is never written.
    approved = [*reviewed_code_versions(), current_code_hashes()]
    calls: list[dict] = []

    def verify(receipt: dict, source: dict, lineage: dict, folder: Path, evidence_only: bool) -> None:
        calls.append(json.loads(receipt["lineage"]["extraction_or_query"])["code_sha256"])
        mmd_api_contract.verify_capture(receipt, source, lineage, folder, evidence_only)

    path: Path | None = None
    try:
        with (
            patch.object(collect_mmd_api, "reviewed_code_versions", lambda: approved),
            patch.object(mmd_api_contract, "reviewed_code_versions", lambda: approved),
            patch.object(collect_mmd_api, "verify_capture", verify),
        ):
            path = capture("C258.07", 2023, False, root=root)
        fresh = json.loads(read_json(path)["lineage"]["extraction_or_query"])
        observed = "valid"
        passed = (
            bool(calls)
            and all(set(call) == set(current_code_hashes()) for call in calls)
            and set(fresh["code_sha256"]) == set(current_code_hashes())
            and fresh["expected_sha256"] == lineage["expected_sha256"]
            and plan_status(fresh) == "current"
        )
    except (RegistryError, StorageError, OSError, KeyError, ValueError) as error:
        observed, passed = f"{type(error).__name__}: {error}", False
    scenario = {"name": "fresh_capture_current_code_offline", "observed": observed.replace(str(directory), "<disposable copy>"), "passed": passed}
    return scenario, (path if passed else None)


@contextmanager
def current_code_approved() -> Iterator[None]:
    """Approve the current collector code in memory only, so a disposable capture reaches the checks; the real catalog is never written."""
    from scripts.acquisition import collect_mmd_api

    approved = [*reviewed_code_versions(), current_code_hashes()]
    with patch.object(collect_mmd_api, "reviewed_code_versions", lambda: approved), patch.object(contract, "reviewed_code_versions", lambda: approved):
        yield


def rerun_scenarios(root: Path, pilot: Path) -> list[dict]:
    """A finished year of the disposable root is re-verified and returned: the rerun adds no file, requests no data and stores nothing."""
    from scripts.acquisition.collect_mmd_api import execute

    scenarios = []
    before = branch_files(root, "c258_07", 2023)
    with current_code_approved():
        result = execute("C258.07", 2023, False, False, root=root)
    scenarios.append(
        {
            "name": "finished_year_rerun_offline",
            "observed_status": result["status"],
            "files_unchanged": branch_files(root, "c258_07", 2023) == before,
            "passed": result["status"] == "capture_ready" and branch_files(root, "c258_07", 2023) == before,
        }
    )
    # A stored year carries a completion record; the replay returns it without a request or an upload.
    done = {"measure_id": "C258.07", "year": 2023, "receipt_path": str(pilot), "status": "stored", "model_eligible": False}
    write_once(pilot.parents[2] / "completed.json", encoded_json(done))
    before = branch_files(root, "c258_07", 2023)
    with current_code_approved():
        result = execute("C258.07", 2023, False, True, root=root)
    scenarios.append(
        {
            "name": "finished_year_replay_returns_the_completion_record",
            "observed_status": result["status"],
            "files_unchanged": branch_files(root, "c258_07", 2023) == before,
            "passed": result["status"] == "stored" and branch_files(root, "c258_07", 2023) == before,
        }
    )
    return scenarios


def refusal(name: str, path: Path, expected: str, temporary: str, baseline: list[str], approve_current: bool = False) -> dict:
    code, output = command([*baseline, "--validate-receipt", str(path)], approve_current)
    return {
        "name": name,
        "expected_exit": "nonzero",
        "observed_exit": code,
        "expected_error": expected,
        "passed": code != 0 and expected in output,
        "output": output.replace(temporary, "<disposable copy>") if temporary else output,
    }


def main() -> None:
    """Retain bounded evidence for actual CLI replay and refusal scenarios."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    ready = read_json(PLAN_PATH.parent / "c258_07/2023/capture_ready.json")
    # The record keeps the path it was written with; the path map gives where it lives now.
    source = Path(current(ready["receipt_path"]))
    retained = json.loads(read_json(source)["lineage"]["extraction_or_query"])
    before = {str(p): fingerprint(p) for p in source.parent.rglob("*") if p.is_file()}
    scenarios = []
    baseline = ["--measure-id", "C258.07", "--year", "2023"]
    # The retained capture was made under a retired plan: the collector refuses it by name, validated or reused, and any retired plan is refused the same way.
    expected_retained = OUTCOME_BY_PLAN_STATUS[plan_status(retained)]
    scenarios.append(refusal("retained_capture_refused_by_name", source, expected_retained, "", baseline))
    code, output = command(baseline)
    scenarios.append(
        {
            "name": "retained_branch_not_reused",
            "expected_exit": "nonzero",
            "observed_exit": code,
            "expected_error": expected_retained,
            "passed": code != 0 and expected_retained in output,
            "output": output,
        }
    )
    with tempfile.TemporaryDirectory(prefix="mmd_cli_retired_") as temporary:
        path = negative_copy(source, "retired_additions_plan", Path(temporary))
        scenarios.append(refusal("retired_additions_plan_refused_by_name", path, RETIRED_EXPECTED["retired_additions_plan"], temporary, baseline))
    with tempfile.TemporaryDirectory(prefix="mmd_cli_pilot_") as temporary:
        workspace = Path(temporary)
        scenario, pilot = pilot_case(source, workspace)
        scenarios.append(scenario)
        if pilot is not None:
            # Every success, replay and mutation case runs on the disposable capture under the current plan.
            for name in ("current_capture_valid_one", "current_capture_valid_two"):
                code, output = command([*baseline, "--validate-receipt", str(pilot)], True)
                scenarios.append(
                    {"name": name, "expected_exit": 0, "observed_exit": code, "passed": code == 0 and '"status": "valid"' in output, "output": output}
                )
            for name, error in NEGATIVE_EXPECTED.items():
                path = negative_copy(pilot, name, workspace / "negative")
                scenarios.append(refusal(name, path, error, temporary, baseline, True))
            path = negative_copy(pilot, "fips_four_digit", workspace / "negative")
            code, output = command([*baseline, "--validate-receipt", str(path)], True)
            scenarios.append(
                {
                    "name": "fips_four_digit_accepted",
                    "expected_exit": 0,
                    "observed_exit": code,
                    "passed": code == 0 and '"status": "valid"' in output,
                    "output": output.replace(temporary, "<disposable copy>"),
                }
            )
            scenarios += rerun_scenarios(workspace / "pilot_root", pilot)
    pilot_rows = retained_rows(source)
    accepted = unknown_county_case("unknown_county_990_accepted", None, pilot_rows)
    accepted["passed"] = accepted["observed"] == "reconstructed" and ",49990," in accepted["row_49990"]
    rejected = unknown_county_case("unknown_non_990_rejected", "49991", pilot_rows)
    rejected["passed"] = "New missing geographic lookup requires review" in rejected["observed"]
    scenarios += [accepted, rejected]
    transient_error = StorageError("head-object", "request_failed")
    scenarios += [
        retry_case("retry_recovers_after_two_transient_failures", [transient_error, OSError("connection reset")], True, 3),
        retry_case("retry_gives_up_after_limit", [transient_error] * ATTEMPTS, False, ATTEMPTS),
        retry_case("no_retry_for_data_errors", [RegistryError("New missing geographic lookup requires review")], False, 1),
    ]
    for name, mid, year, error in (
        ("unregistered_measure", "C258.99", "2023", "not in the approved locked registry"),
        ("unavailable_year", "C258.06", "2021", "availability_hold"),
        # C258.80 resolves through the additions plan; 2020 predates CMS's behavioral-health menu for code 136.
        ("addition_unavailable_year", "C258.80", "2020", "availability_hold"),
    ):
        code, output = command(["--measure-id", mid, "--year", year])
        scenarios.append({"name": name, "expected_error": error, "observed_exit": code, "passed": code != 0 and error in output, "output": output})
    after = {str(p): fingerprint(p) for p in source.parent.rglob("*") if p.is_file()}
    code, output = command(["--measure-id", "C258.07", "--year", "2022", "--validate-receipt", str(source)])
    scenarios.append({"name": "validation_year_mismatch", "observed_exit": code, "passed": code != 0 and "Validation year differs" in output, "output": output})
    scenarios.append({"name": "original_snapshot_unchanged", "passed": before == after})
    result = {
        "status": "passed" if all(s["passed"] for s in scenarios) else "failed",
        "created_at_utc": datetime.now(UTC).isoformat(),
        "scope": "Real offline collector CLI, public pilot copies, no API requests or AWS calls",
        "scenarios": scenarios,
        "input_receipt": str(source),
        "input_receipt_sha256": fingerprint(source)[0],
        "input_receipt_plan_status": plan_status(retained),
        "python_version": sys.version,
        "runner_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "collector_code_sha256": current_code_hashes(),
        "collector_code_listed": current_code_hashes() in reviewed_code_versions(),
        "temporary_copies_removed": True,
        "reproduce": f".venv/bin/python -m scripts.acquisition.run_mmd_api_e2e --output {args.output.parent}/rerun_{args.output.name}",
        "untested": [
            "Live S3 path is verified separately",
            "Live 2012-2018 county-code capture is verified by the resumed queue run",
            "No suppressed/non-numeric input accepted; those stop for review",
            "No other-source API route change",
            "The --execute replay of a stored current-plan year needs S3 evidence; the replay case reads a disposable completion record",
        ],
    }
    write_once(args.output, encoded_json(result))
    sys.stdout.write(json.dumps({"status": result["status"], "scenarios": len(scenarios), "artifact": str(args.output)}) + "\n")
    require(result["status"] == "passed", "MMD CLI E2E scenario failed")


if __name__ == "__main__":
    main()
