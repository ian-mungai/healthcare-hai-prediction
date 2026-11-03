"""Synthetic dbt artifact regressions through the real readers and package CLI, with inert launcher transport and fixture production."""

from __future__ import annotations

import hashlib
import json
import sys
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from orchestration import launcher
from scripts.lakehouse import package_fixture, run_gold_e2e
from tests.support import check

PACKAGE = "REPORT_FIXTURE"
MODEL = "int_report_input"
MODEL_ID = f"model.report_fixture.{MODEL}"
SEED_ID = "seed.report_fixture.report_seed"
TEST_NAME = "assert_report_input"
TEST_ID = f"test.report_fixture.{TEST_NAME}"
GOOD_ROWS = ((MODEL_ID, "success"), (SEED_ID, "success"), (TEST_ID, "pass"))
RELEASE_ID = "test.report_fixture.release_copy_consistency_dim_release_.0123456789"


@dataclass(frozen=True)
class DbtReply:
    rows: tuple[tuple[str, str], ...]
    exit_code: int = 0


class DbtTransport:
    """Emit synthetic dbt artifacts and fingerprint CSV where a launcher attempt would produce them."""

    def __init__(self) -> None:
        self.selected = sorted(unique_id for unique_id, _status in GOOD_ROWS)
        self.replies = [DbtReply(GOOD_ROWS)]
        self.calls: list[tuple[str, list[str]]] = []
        self.emitted: list[DbtReply] = []
        self.allocations: list[str] = []
        self.released: list[str] = []
        self.fingerprints = {"staging.duckdb": "2,17,23\n", "baseline.duckdb": "2,17,23\n"}

    def allocate(self) -> str:
        allocation = f"synthetic-allocation-{len(self.allocations)}"
        self.allocations.append(allocation)
        return allocation

    def principal_alloc(self, allocation: str) -> dict[str, str]:
        check(allocation in self.allocations, "only this test's synthetic allocation is used")
        return {"kind": "alloc", "alloc_id": allocation}

    def release_allocation(self, allocation: str) -> None:
        self.released.append(allocation)

    def run_attempt(
        self, principal: dict[str, Any], template: str, launch: str, args: list[str], params: dict[str, Any], **_arguments: Any
    ) -> launcher.AttemptResult:
        check(principal["alloc_id"] in self.allocations, "the real Session supplies the synthetic allocation principal")
        self.calls.append((template, list(args)))
        if template == "duckdb_helper":
            stdout = self.fingerprints[Path(params["database"]).name]
            return launcher.AttemptResult(launch, "exited", 0, stdout, "", {})
        check(template == "fixture_dbt", "no other launcher template is permitted by this test transport")
        if args[0] == "ls":
            stdout = "".join(json.dumps({"unique_id": unique_id}) + "\n" for unique_id in self.selected)
            return launcher.AttemptResult(launch, "exited", 0, stdout, "", {})
        check(args[0] in {"build", "test"}, "only the requested dbt control or mutant command reaches the transport")
        check(bool(self.replies), "each dbt command must consume an explicitly supplied synthetic reply")
        reply = self.replies.pop(0)
        path = Path(params["case_host"]) / "target/run_results.json"
        check(not path.exists(), "the real dbt reader removes stale results before invoking transport")
        path.write_text(json.dumps({"results": [{"unique_id": unique_id, "status": status} for unique_id, status in reply.rows]}), encoding="utf-8")
        self.emitted.append(reply)
        return launcher.AttemptResult(launch, "exited", reply.exit_code, "synthetic dbt output", "", {})


@dataclass
class Rig:
    folder: Path
    selector: Path
    transport: DbtTransport
    monkeypatch: pytest.MonkeyPatch

    def make_case(self, name: str, _session: package_fixture.Session, _patches: list[Path]) -> package_fixture.Case:
        folder = self.folder / "cases" / name
        target = folder / "target"
        target.mkdir(parents=True, exist_ok=True)
        identifiers = {*self.transport.selected, "model.report_fixture.unselected_not_run", "model.report_fixture.dim_release"}
        nodes = {
            unique_id: {
                "resource_type": unique_id.split(".")[0],
                "name": unique_id.split(".")[2],
                "checksum": {"checksum": hashlib.sha256(unique_id.encode()).hexdigest()},
            }
            for unique_id in identifiers
        }
        nodes[RELEASE_ID] = {
            "resource_type": "test",
            "name": "release_copy_consistency_dim_release_",
            "attached_node": "model.report_fixture.dim_release",
            "config": {"enabled": True, "severity": "error"},
        }
        (target / "manifest.json").write_text(json.dumps({"nodes": nodes, "sources": {}, "disabled": {}}), encoding="utf-8")
        return package_fixture.Case(name, folder)

    def invoke(self, capsys: pytest.CaptureFixture[str], arguments: list[str] | None = None) -> tuple[int, str]:
        argv = ["package_fixture", "check", "--package", PACKAGE, "--selector", str(self.selector), *(arguments if arguments is not None else ["--record"])]
        self.monkeypatch.setattr(sys, "argv", argv)
        code = package_fixture.main()
        captured = capsys.readouterr()
        return code, captured.err + captured.out

    @property
    def records(self) -> Path:
        return self.folder / "out/records" / PACKAGE.lower()


@pytest.fixture
def rig(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Rig]:
    monkeypatch.delenv("HAI_ALLOCATION", raising=False)
    for name, value in {
        "AWS_PROFILE": "dbt_report_no_cloud",
        "AWS_CONFIG_FILE": "/dev/null",
        "AWS_SHARED_CREDENTIALS_FILE": "/dev/null",
        "AWS_EC2_METADATA_DISABLED": "true",
    }.items():
        monkeypatch.setenv(name, value)
    transport = DbtTransport()
    selector = tmp_path / "selector.yml"
    baseline = tmp_path / "baseline.json"
    baseline.write_text(json.dumps({"relations": {MODEL: {"columns": [["id", "INTEGER"]]}}}), encoding="utf-8")
    selector.write_text(
        json.dumps(
            {
                "package": PACKAGE,
                "owned_models": [MODEL],
                "owned_seeds": [],
                "required_tests": [TEST_NAME],
                "required_test_count": 1,
                "retired_tests": [],
                "exclude_tests": [],
                "documented_changes": [],
                "compare_unchanged_with_baseline": [MODEL],
                "baseline": str(baseline),
                "note": "synthetic",
            }
        ),
        encoding="utf-8",
    )
    harness = Rig(tmp_path, selector, transport, monkeypatch)
    active_before = tuple(package_fixture.ACTIVE)
    monkeypatch.setattr(package_fixture.launcher, "load_client", lambda *_args, **_kwargs: transport)
    monkeypatch.setattr(package_fixture, "make_case", harness.make_case)
    monkeypatch.setattr(package_fixture, "OUT", tmp_path / "out")
    monkeypatch.setattr(package_fixture, "BASE_BUILD", tmp_path / "baseline.duckdb")
    monkeypatch.setattr(package_fixture, "TEST_REGISTRY", None)
    yield harness
    check(tuple(package_fixture.ACTIVE) == active_before, "every real reservation removes its active Session")
    check(transport.released == transport.allocations, "every synthetic allocation is released, including refused controls")


@pytest.mark.parametrize("failed_status", ["fail", "error"])
def test_raw_results_do_not_replace_an_earlier_failure_with_pass(rig: Rig, failed_status: str) -> None:
    rig.transport.replies = [DbtReply(((TEST_ID, failed_status), (TEST_ID, "pass")))]
    with package_fixture.reservation() as session:
        case = rig.make_case("raw_duplicate", session, [])
        try:
            _code, _tail, statuses = package_fixture.dbt(case, ["test", "--select", TEST_NAME], session)
        except package_fixture.FixtureError as refusal:
            message = str(refusal).lower()
            check(TEST_ID in str(refusal) and ("duplicate" in message or "ambig" in message), "refusal must identify the duplicate full unique ID")
        else:
            check(statuses.get(TEST_ID) == failed_status, "a later pass must not erase the earlier failure for the identical full unique ID")
    check(len(rig.transport.emitted) == 1, "the real reader consumed the duplicate raw result list")


def test_package_cli_rejects_duplicate_failed_then_passing_model(rig: Rig, capsys: pytest.CaptureFixture[str]) -> None:
    rig.transport.replies = [DbtReply(((MODEL_ID, "fail"), *GOOD_ROWS))]
    code, diagnostics = rig.invoke(capsys)
    check(code == 1, "a duplicate failed model must prevent package success at the public CLI")
    check(MODEL_ID in diagnostics, "the refusal must identify the failed or ambiguous full model ID")
    check(any(word in diagnostics.lower() for word in ("fail", "duplicate", "ambig")), "the refusal must concern failed or duplicate result evidence")


@pytest.mark.parametrize("replacement", [None, "model.other_package.int_report_input"], ids=["absent_model", "same_name_other_package"])
def test_package_cli_requires_the_selected_non_test_id(rig: Rig, capsys: pytest.CaptureFixture[str], replacement: str | None) -> None:
    rows = GOOD_ROWS[1:] if replacement is None else ((replacement, "success"), *GOOD_ROWS[1:])
    rig.transport.replies = [DbtReply(rows)]
    code, diagnostics = rig.invoke(capsys)
    check(code == 1, "required tests passing cannot conceal an absent exact selected model ID")
    check(MODEL_ID in diagnostics, "the refusal must identify the missing selected model, including its resource type and package")
    check(any(word in diagnostics.lower() for word in ("missing", "not run", "not reported", "absent", "unreported")), "the refusal must name missing evidence")


def test_package_cli_rejects_nonzero_control_exit_with_passing_rows(rig: Rig, capsys: pytest.CaptureFixture[str]) -> None:
    rig.transport.replies = [DbtReply(GOOD_ROWS, exit_code=7)]
    code, diagnostics = rig.invoke(capsys)
    check(code == 1, "a nonzero control build exit must prevent package success even when every selected result passes")
    check("dbt" in diagnostics.lower() and "7" in diagnostics, "the refusal must preserve the dbt transport failure and its exit code")


def test_gold_test_nodes_rejects_nonzero_control_exit_with_passing_rows(rig: Rig) -> None:
    rig.transport.replies = [DbtReply(((TEST_ID, "pass"),), exit_code=7)]
    with package_fixture.reservation() as session:
        case = rig.make_case("gold_nonzero", session, [])
        problems, evidence = run_gold_e2e.test_nodes(case, [TEST_NAME], require_release_test=True)
    check(not run_gold_e2e.not_passing(evidence["statuses"]), "the rows pass and the required release declaration is valid")
    check(any("dbt" in problem.lower() and "7" in problem for problem in problems), "gold test evidence must preserve the control command's exit 7")


@pytest.mark.parametrize("other_id", [f"test.other_package.{TEST_NAME}", f"test.report_fixture.{TEST_NAME}.fedcba9876"])
def test_gold_short_names_do_not_erase_a_distinct_ids_failure(rig: Rig, other_id: str) -> None:
    rig.transport.replies = [DbtReply(((TEST_ID, "fail"), (other_id, "pass")))]
    with package_fixture.reservation() as session:
        case = rig.make_case("gold_names", session, [])
        try:
            problems, _evidence = run_gold_e2e.test_nodes(case, [TEST_NAME], require_release_test=True)
        except package_fixture.FixtureError as refusal:
            message = str(refusal).lower()
            check(
                TEST_NAME in str(refusal) and any(word in message for word in ("ambig", "collis", "duplicate")), "refusal must identify ambiguous test evidence"
            )
        else:
            check(any(TEST_NAME in problem for problem in problems), "the failure must survive reduction of distinct full IDs to one short name")


@pytest.mark.parametrize("extra_rows", [(), (("model.other_package.int_report_input", "success"), ("seed.other_package.int_report_input", "success"))])
def test_package_cli_accepts_the_full_selected_set_with_legitimate_distinct_ids(
    rig: Rig, capsys: pytest.CaptureFixture[str], extra_rows: tuple[tuple[str, str], ...]
) -> None:
    rows = (*GOOD_ROWS, *extra_rows)
    rig.transport.selected = sorted(unique_id for unique_id, _status in rows)
    rig.transport.replies = [DbtReply(rows)]
    code, diagnostics = rig.invoke(capsys)
    check(code == 0, f"the complete selected set must pass, including legitimate package and resource identities: {diagnostics}")
    report = json.loads((rig.records / "check.json").read_text())
    effective = json.loads((rig.records / "effective_selection.json").read_text())
    check(report["status"] == "pass" and report["problems"] == [], "the real package check writes a passing report")
    check(effective["nodes"] == rig.transport.selected and report["effective_nodes"] == len(rows), "the real ls parser records every requested full ID")
    check(report["comparisons"] == {MODEL: "equal"}, "the real comparison evaluates the two synthetic fingerprints")
    check("model.report_fixture.unselected_not_run" not in effective["nodes"], "manifest membership must not replace the command's selected-ID authority")
    dbt_args = [args for template, args in rig.transport.calls if template == "fixture_dbt"]
    check([args[0] for args in dbt_args] == ["ls", "build"], "selection and build both execute through real package code")
    check(all(args[args.index("--indirect-selection") + 1] == "cautious" for args in dbt_args), "the package retains cautious indirect selection")
    check(len(rig.transport.emitted) == 1 and not rig.transport.replies, "the public CLI consumes the supplied build evidence")


@pytest.mark.parametrize("status", ["pass", "success"])
def test_gold_test_nodes_accepts_passing_distinct_test_ids(rig: Rig, status: str) -> None:
    rows = ((TEST_ID, status), (f"test.other_package.{TEST_NAME}", status), (f"test.report_fixture.{TEST_NAME}.fedcba9876", status))
    rig.transport.replies = [DbtReply(rows)]
    with package_fixture.reservation() as session:
        case = rig.make_case("gold_positive", session, [])
        problems, evidence = run_gold_e2e.test_nodes(case, [TEST_NAME], require_release_test=True)
    check(problems == [], "legitimate passing test IDs must not receive a blanket package or short-name collision refusal")
    check(bool(evidence["statuses"]), "the real gold reader retains passing status evidence")
    check(rig.transport.emitted == [DbtReply(rows)], "the real raw reader consumes the supplied distinct IDs")


def test_gold_name_helper_accepts_passing_distinct_resource_ids() -> None:
    raw = {"model.package_one.shared_name": "success", "seed.package_two.shared_name": "success", "test.package_one.shared_name.0123456789": "pass"}
    statuses = run_gold_e2e.statuses_by_name(raw)
    check(bool(statuses) and run_gold_e2e.not_passing(statuses) == [], "distinct passing resource IDs may share a short name without a blanket refusal")


def test_package_expected_mutant_failure_keeps_its_nonzero_exit(rig: Rig, capsys: pytest.CaptureFixture[str]) -> None:
    rig.transport.replies = [DbtReply(GOOD_ROWS), DbtReply(((MODEL_ID, "success"), (TEST_ID, "fail")), exit_code=1)]
    mutant = rig.folder / "synthetic_mutant.patch"
    code, diagnostics = rig.invoke(capsys, ["--record", "--expected-failure", f"expected:{mutant}:{TEST_NAME}"])
    check(code == 0, f"an expected mutant's exact named test failure remains valid evidence: {diagnostics}")
    report = json.loads((rig.records / "check.json").read_text())
    check(
        report["expected_failures"] == {"expected": {"test": TEST_NAME, "failed": [TEST_NAME], "ok": True}}, "the real check validates the exact mutant failure"
    )
    check([reply.exit_code for reply in rig.transport.emitted] == [0, 1], "only the control must exit zero, while the deliberate mutant exits nonzero")


@pytest.mark.parametrize("key", ["selector_sha256", "manifest_sha256", "nodes"])
def test_package_approved_selection_bindings_remain_required(rig: Rig, capsys: pytest.CaptureFixture[str], key: str) -> None:
    code, diagnostics = rig.invoke(capsys)
    check(code == 0, f"the untampered control must pass before changing an approval binding: {diagnostics}")
    accepted = json.loads((rig.records / "effective_selection.json").read_text())
    accepted[key] = [] if key == "nodes" else "0" * 64
    approved = rig.folder / "tampered_approval.json"
    approved.write_text(json.dumps(accepted), encoding="utf-8")
    rig.transport.replies.append(DbtReply(GOOD_ROWS))
    code, diagnostics = rig.invoke(capsys, ["--approved", str(approved)])
    check(code == 1 and f"{key} differs from the approved selection" in diagnostics, "the original selector, manifest and selected-ID checks remain active")


def test_package_comparison_still_rejects_changed_fingerprints(rig: Rig, capsys: pytest.CaptureFixture[str]) -> None:
    rig.transport.fingerprints["staging.duckdb"] = "3,18,24\n"
    code, diagnostics = rig.invoke(capsys)
    check(code == 1 and f"{MODEL} differs" in diagnostics, "the real comparison must reject different independently supplied fingerprints")
