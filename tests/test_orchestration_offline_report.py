"""Exercise offline image report acceptance through the real consumer and CLI with synthetic launcher replies."""

from __future__ import annotations

import ast
import copy
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from scripts.orchestration import offline_checks as offline
from tests.support import check

SOURCE_ROOT = Path(__file__).resolve().parents[1]
GRAPH_CASES = {
    "reference": None,
    "extra leaf": "leaf",
    "setup task": "setup_or_teardown",
    "teardown task": "setup_or_teardown",
    "final_gate all_success": "trigger_rule",
    "release_run all_success": "trigger_rule",
    "work task all_done": "trigger_rule",
    "missing edge into final_gate": "upstream",
    "missing edge into catalog_release": "upstream",
    "cycle": "cycle",
}
PIPELINE_DAGS = (
    "training_dag",
    "model_promotion_dag",
    "serving_deploy_dag",
    "silver_transform_dag",
    "gold_transform_dag",
    "features_transform_dag",
    "feature_store_dag",
)
IMAGE_CHECK = "image labels, user and top layers"
DAG_CHECK = "every DAG file imports in the image with no error or cycle"
PINS_CHECK = "image packages equal the constraints file and the release pins"
TRANSFORM_CHECK = "transform_dag passes the graph validator (Airflow object)"
VIOLATION = {"rule": "upstream", "task": "final_gate", "detail": "synthetic missing upstream edge"}
ALLOCATION = "synthetic-offline-report-allocation"
TRANSPORT_ERROR = "synthetic image job transport failure"
CLEANUP_ERROR = "synthetic allocation cleanup refusal"


def _platform() -> dict[str, Any]:
    return {
        "airflow": {
            "labels": {"org.example.image": "synthetic-offline-image", "org.example.constraints": "synthetic-constraints"},
            "user": "50000:0",
            "release_packages": {"synthetic-release-package": "1.0"},
        }
    }


def _document(transform_present: bool = False) -> dict[str, Any]:
    return {
        "dags": {"files": ["synthetic_dag.py"], "dag_ids": ["transform_dag"] if transform_present else list(PIPELINE_DAGS), "import_errors": {}},
        "graph_cases": {name: {"expected": rule, "rules": [] if rule is None else [rule], "ok": True} for name, rule in GRAPH_CASES.items()},
        "transform_graph": {"present": transform_present, "violations": [], "tasks": ["synthetic_task"] if transform_present else []},
        "pipeline_graphs": {name: {"present": True, "violations": []} for name in PIPELINE_DAGS},
        "constraints": {
            "pinned": 1,
            "installed": 4,
            "differing": {},
            "unpinned": [],
            "build_tools": {"pip": {"ok": True}, "uv": {"ok": True}},
            "release": {"synthetic-release-package": "1.0"},
            "release_problems": [],
        },
    }


def _stdout(document: dict[str, Any]) -> str:
    return "ordinary image startup output\nIN_IMAGE " + json.dumps(document) + "\nordinary image shutdown output\n"


class ImageJobTransport:
    """Supply image metadata and one finite job reply without a launcher, process fixture or Airflow import."""

    def __init__(self, root: Path, platform: dict[str, Any]) -> None:
        self.root = root
        self.description: dict[str, Any] = {
            "inspect": {"Config": {"Labels": copy.deepcopy(platform["airflow"]["labels"]), "User": platform["airflow"]["user"]}},
            "history": [{"CreatedBy": "COPY deadline_run /opt/hai/deadline_run"}, {"CreatedBy": "LABEL synthetic-image"}, {"CreatedBy": "base layer"}],
        }
        self.attempt = offline.launcher.AttemptResult("synthetic-image-attempt", "exited", 0, _stdout(_document()), "", {})
        self.events: list[str] = []
        self.refuse_attempt = False
        self.refuse_cleanup = False

    def call(self, operation: str, payload: dict[str, Any], *, timeout: float) -> dict[str, Any]:
        """Return only the image description requested by the real consumer."""
        self.events.append("image")
        check(operation == "image", "only the image description transport is substituted")
        check(
            payload == {"service": "airflow-check", "compose": "orchestration", "history": True, "checkout": str(self.root)},
            "the real image request is retained",
        )
        check(timeout == 300, "the image request retains its deadline")
        return copy.deepcopy(self.description)

    def allocate(self) -> str:
        """Record allocation admission without acquiring a real allocation."""
        self.events.append("allocate")
        return ALLOCATION

    def principal_alloc(self, allocation: str) -> dict[str, str]:
        """Record the allocation principal requested by the real consumer."""
        self.events.append("principal")
        check(allocation == ALLOCATION, "the admitted allocation is used")
        return {"kind": "alloc", "alloc_id": allocation}

    def run_attempt(
        self, principal: dict[str, Any], template: str, launch_id: str, arguments: list[str], parameters: dict[str, Any], *, checkout: Path, seconds: float
    ) -> offline.launcher.AttemptResult:
        """Return saved job output or an explicit synthetic transport refusal."""
        self.events.append("attempt")
        check(principal == {"kind": "alloc", "alloc_id": ALLOCATION}, "the image job uses its allocation principal")
        check(
            (template, launch_id, arguments, parameters, checkout, seconds) == ("orchestration_check", "offline-checks", [], {}, self.root, 600),
            "the real image job is requested",
        )
        if self.refuse_attempt:
            raise offline.launcher.ClientError(TRANSPORT_ERROR, "unavailable")
        return self.attempt

    def release_allocation(self, allocation: str) -> offline.launcher.ReleaseOutcome:
        """Record cleanup and preserve a refused release as an exception."""
        self.events.append("release")
        check(allocation == ALLOCATION, "cleanup releases the admitted allocation")
        if self.refuse_cleanup:
            raise offline.launcher.ClientError(CLEANUP_ERROR, "refused")
        return offline.launcher.ReleaseOutcome.RELEASED


@dataclass
class ImageBoundary:
    """A disposable CLI output root and its synthetic image-job transport."""

    root: Path
    platform: dict[str, Any]
    transport: ImageJobTransport


@pytest.fixture
def boundary(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> ImageBoundary:
    """Redirect filesystem inputs and the launcher boundary while retaining the real parser, checks and CLI."""
    platform = _platform()
    platform_path = tmp_path / "synthetic_platform.json"
    platform_path.write_text(json.dumps(platform), encoding="utf-8")
    pending = tmp_path / "config/quality/mypy_runtimes.json"
    pending.parent.mkdir(parents=True)
    pending.write_text(json.dumps({"pending": {}}), encoding="utf-8")
    for relative in ("scripts/orchestration/in_image_checks.py", "orchestration/graph.py"):
        source = tmp_path / relative
        source.parent.mkdir(parents=True, exist_ok=True)
        source.symlink_to(SOURCE_ROOT / relative)
    transport = ImageJobTransport(tmp_path, platform)
    monkeypatch.setattr(offline, "ROOT", tmp_path)
    monkeypatch.setattr(offline, "PLATFORM_FILE", platform_path)
    monkeypatch.setattr(offline.launcher, "load_client", lambda registry: transport)
    monkeypatch.setattr(sys, "argv", [str(offline.__file__), "--only", "image"])
    return ImageBoundary(tmp_path, platform, transport)


def _released(boundary: ImageBoundary, calls: int = 1) -> None:
    check(
        boundary.transport.events == ["image", "allocate", "principal", "attempt", "release"] * calls,
        "every image attempt releases its allocation exactly once",
    )


def _run_cli(boundary: ImageBoundary) -> tuple[int, dict[str, Any]]:
    code = offline.main()
    reports = list((boundary.root / "data/e2e/orchestration_offline").glob("*/report.json"))
    check(len(reports) == 1, "the real outer CLI persists exactly one report")
    report: dict[str, Any] = json.loads(reports[0].read_text(encoding="utf-8"))
    check(report["sections"] == ["image"], "only the actual image-report path ran")
    return code, report


def _failures(code: int, report: dict[str, Any]) -> dict[str, Any]:
    check(code == 1, "the real outer CLI must refuse this image evidence")
    check(report["status"] == "fail", "the persisted outer verdict must fail")
    failures = {name: row for name, row in report["results"].items() if row["ok"] is False}
    check(bool(failures), "a named false check must explain the refusal")
    return failures


def _complete(code: int, report: dict[str, Any], transform_present: bool = False) -> None:
    required = {IMAGE_CHECK, DAG_CHECK, PINS_CHECK, *(f"graph (Airflow objects): {name}" for name in GRAPH_CASES)}
    required |= {TRANSFORM_CHECK} if transform_present else {f"{name} passes the graph validator (Airflow object)" for name in PIPELINE_DAGS}
    check(code == 0 and report["status"] == "pass", "complete consistent image evidence passes the real outer CLI")
    check(required <= report["results"].keys(), "all independently pinned required checks survive result projection")
    check(all(row["ok"] is True for row in report["results"].values()), "every computed control succeeds")


def test_literal_inventory_matches_the_declared_producers() -> None:
    producer = ast.parse((SOURCE_ROOT / "scripts/orchestration/in_image_checks.py").read_text(encoding="utf-8"))
    function = next(node for node in producer.body if isinstance(node, ast.FunctionDef) and node.name == "graph_cases")
    declaration = next(
        node.value for node in function.body if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name) and node.target.id == "cases"
    )
    if not isinstance(declaration, ast.Dict):
        raise AssertionError("the producer declares its graph inventory as a dictionary")
    declared: dict[str, str | None] = {}
    for key, value in zip(declaration.keys, declaration.values, strict=True):
        if key is None or not isinstance(value, ast.Tuple) or len(value.elts) != 2:
            raise AssertionError("each graph case declares its options and expected rule")
        declared[ast.literal_eval(key)] = ast.literal_eval(value.elts[1])
    check(declared == GRAPH_CASES, "the independent ten-case inventory and expected rules match the actual producer declaration")
    graph = ast.parse((SOURCE_ROOT / "orchestration/graph.py").read_text(encoding="utf-8"))
    chains = next(
        node.value
        for node in graph.body
        if isinstance(node, ast.Assign) and any(isinstance(target, ast.Name) and target.id == "PIPELINE_CHAINS" for target in node.targets)
    )
    collapsed = next(
        node.value
        for node in graph.body
        if isinstance(node, ast.Assign) and any(isinstance(target, ast.Name) and target.id == "COLLAPSED_DAGS" for target in node.targets)
    )
    if not isinstance(chains, ast.Dict) or any(key is None for key in chains.keys) or not isinstance(collapsed, ast.Call) or len(collapsed.args) != 1:
        raise AssertionError("the producer declares split DAG names without importing Airflow")
    declared = {ast.literal_eval(key) for key in chains.keys if key is not None} | set(ast.literal_eval(collapsed.args[0]))
    check(declared == set(PIPELINE_DAGS), "the independent seven-DAG inventory matches PIPELINE_CHAINS and COLLAPSED_DAGS")


@pytest.mark.parametrize("transform_present", [False, True], ids=["split", "transform"])
def test_complete_branches_allow_ordinary_stdout(boundary: ImageBoundary, transform_present: bool) -> None:
    document = _document(transform_present)
    document["graph_cases"] = dict(reversed(list(document["graph_cases"].items())))
    document["pipeline_graphs"] = dict(reversed(list(document["pipeline_graphs"].items())))
    if transform_present:
        document.pop("pipeline_graphs")
    boundary.transport.attempt.stdout = _stdout(document)
    _complete(*_run_cli(boundary), transform_present)
    _released(boundary)


@pytest.mark.parametrize("transform_present", [False, True], ids=["split", "transform"])
@pytest.mark.parametrize("damage", ["missing", "empty", "partial", "replacement", "unexpected"])
def test_exact_graph_case_inventory_is_required(boundary: ImageBoundary, transform_present: bool, damage: str) -> None:
    document = _document(transform_present)
    if damage == "missing":
        document.pop("graph_cases")
    elif damage == "empty":
        document["graph_cases"] = {}
    else:
        if damage in {"partial", "replacement"}:
            document["graph_cases"].pop("cycle")
        if damage in {"replacement", "unexpected"}:
            document["graph_cases"]["synthetic unexpected case"] = {"ok": True, "expected": None, "rules": []}
    boundary.transport.attempt.stdout = _stdout(document)
    failures = _failures(*_run_cli(boundary))
    detail = json.dumps(failures).lower()
    check("graph" in detail, "the refusal identifies graph-case inventory evidence")
    if damage in {"partial", "replacement"}:
        check("cycle" in detail, "the refusal identifies the omitted declared graph case")
    _released(boundary)


@pytest.mark.parametrize("damage", ["missing", "empty", "partial", "replacement", "unexpected"])
def test_exact_split_pipeline_inventory_is_required(boundary: ImageBoundary, damage: str) -> None:
    document = _document()
    if damage == "missing":
        document.pop("pipeline_graphs")
    elif damage == "empty":
        document["pipeline_graphs"] = {}
    else:
        if damage in {"partial", "replacement"}:
            document["pipeline_graphs"].pop("feature_store_dag")
        if damage in {"replacement", "unexpected"}:
            document["pipeline_graphs"]["synthetic_unexpected_dag"] = {"present": True, "violations": []}
    boundary.transport.attempt.stdout = _stdout(document)
    failures = _failures(*_run_cli(boundary))
    detail = json.dumps(failures).lower()
    check("pipeline" in detail or "dag" in detail, "the refusal identifies the split pipeline inventory")
    if damage in {"partial", "replacement"}:
        check("feature_store_dag" in detail, "the refusal identifies the omitted declared split DAG")
    _released(boundary)


def test_import_errors_cannot_be_overwritten_by_incoming_ok(boundary: ImageBoundary) -> None:
    document = _document()
    document["dags"].update(import_errors={"synthetic_dag.py": "synthetic import error"}, ok=True)
    boundary.transport.attempt.stdout = _stdout(document)
    code, report = _run_cli(boundary)
    failures = _failures(code, report)
    check(set(failures) == {DAG_CHECK}, "the computed DAG import verdict is the intended and only failure")
    check(failures[DAG_CHECK]["import_errors"] == document["dags"]["import_errors"], "the import-error evidence survives projection")
    _released(boundary)


@pytest.mark.parametrize(
    "field,value",
    [
        ("differing", {"synthetic-pinned-package": {"expected": "1.0", "installed": "2.0"}}),
        ("unpinned", ["synthetic-unpinned-package"]),
        ("build_tools", {"pip": {"ok": False}, "uv": {"ok": True}}),
        ("release_problems", ["synthetic release version mismatch"]),
        ("release", {}),
    ],
)
def test_constraint_failures_cannot_be_overwritten_by_incoming_ok(boundary: ImageBoundary, field: str, value: Any) -> None:
    document = _document()
    document["constraints"].update({field: value, "ok": True})
    boundary.transport.attempt.stdout = _stdout(document)
    failures = _failures(*_run_cli(boundary))
    check(set(failures) == {PINS_CHECK}, "the computed constraint verdict is the intended and only failure")
    check(failures[PINS_CHECK][field] == value, "the failed constraint evidence survives projection")
    _released(boundary)


@pytest.mark.parametrize(
    "tools",
    [
        pytest.param({"pip": {"ok": True}}, id="uv_record_removed"),
        pytest.param({"uv": {"ok": True}}, id="pip_record_removed"),
        pytest.param({}, id="both_records_removed"),
        pytest.param({"pip": {"ok": True}, "uv": {"ok": True}, "setuptools": {"ok": True}}, id="unexpected_tool_record"),
        pytest.param({"pip": {"ok": True}, "uv": {"ok": "true"}}, id="string_verdict"),
        pytest.param({"pip": {"ok": True}, "uv": {}}, id="verdict_removed"),
    ],
)
def test_incomplete_build_tool_inventory_cannot_pass(boundary: ImageBoundary, tools: dict[str, Any]) -> None:
    document = _document()
    document["constraints"]["build_tools"] = tools
    boundary.transport.attempt.stdout = _stdout(document)
    code, report = _run_cli(boundary)
    check(code == 1 and report["status"] == "fail", "a build tool inventory other than pip and uv with Boolean verdicts is a failed run")
    check(report["results"]["in-image checks ran"]["ok"] is False, "the report boundary refuses the incomplete build tool evidence")
    check(PINS_CHECK not in report["results"] or report["results"][PINS_CHECK]["ok"] is False, "no aggregate pass remains for the package check")
    _released(boundary)


@pytest.mark.parametrize("graph_kind", ["transform", "pipeline_violations", "pipeline_absent"])
def test_graph_failures_cannot_be_overwritten_by_incoming_ok(boundary: ImageBoundary, graph_kind: str) -> None:
    document = _document(graph_kind == "transform")
    if graph_kind == "transform":
        graph = document["transform_graph"]
        name = TRANSFORM_CHECK
    else:
        graph = document["pipeline_graphs"]["gold_transform_dag"]
        name = "gold_transform_dag passes the graph validator (Airflow object)"
    graph.update(ok=True)
    if graph_kind == "pipeline_absent":
        graph.update(present=False)
    else:
        graph.update(violations=[VIOLATION])
    boundary.transport.attempt.stdout = _stdout(document)
    failures = _failures(*_run_cli(boundary))
    check(set(failures) == {name}, "the computed graph verdict is the intended and only failure")
    check(failures[name]["present"] == graph["present"] and failures[name]["violations"] == graph["violations"], "the graph evidence survives projection")
    _released(boundary)


@pytest.mark.parametrize("records", [0, 2, 3])
def test_exactly_one_in_image_document_is_required(boundary: ImageBoundary, records: int) -> None:
    line = "IN_IMAGE " + json.dumps(_document()) + "\n"
    boundary.transport.attempt.stdout = "ordinary image startup output\n" + line * records + "ordinary image shutdown output\n"
    failures = _failures(*_run_cli(boundary))
    check(boundary.transport.attempt.ok, "the refusal cannot be attributed to failed job transport")
    check(failures.get("in-image checks ran", {}).get("ok") is False, "the report boundary rejects missing or ambiguous image documents")
    _released(boundary)


@pytest.mark.parametrize("shape", ["empty", "partial"])
def test_empty_or_partial_document_is_a_recorded_refusal(boundary: ImageBoundary, shape: str) -> None:
    document = {} if shape == "empty" else _document()
    if shape == "partial":
        document.pop("constraints")
    boundary.transport.attempt.stdout = _stdout(document)
    failures = _failures(*_run_cli(boundary))
    check(failures.get("in-image checks ran", {}).get("ok") is False, "incomplete report evidence fails at its owning boundary")
    _released(boundary)


@pytest.mark.parametrize("broken", ["labels", "user", "layers", "dag_ids", "graph_case", "constraints"])
def test_existing_image_checks_still_refuse_their_intended_failure(boundary: ImageBoundary, broken: str) -> None:
    document = _document()
    expected = IMAGE_CHECK
    if broken == "labels":
        boundary.transport.description["inspect"]["Config"]["Labels"].pop("org.example.constraints")
    elif broken == "user":
        boundary.transport.description["inspect"]["Config"]["User"] = "0:0"
    elif broken == "layers":
        boundary.transport.description["history"][0]["CreatedBy"] = "RUN pip install synthetic-package"
    elif broken == "dag_ids":
        document["dags"]["dag_ids"] = []
        expected = DAG_CHECK
    elif broken == "graph_case":
        document["graph_cases"]["cycle"]["ok"] = False
        expected = "graph (Airflow objects): cycle"
    else:
        document["constraints"]["unpinned"] = ["synthetic-unpinned-package"]
        expected = PINS_CHECK
    boundary.transport.attempt.stdout = _stdout(document)
    check(set(_failures(*_run_cli(boundary))) == {expected}, "only the intended preexisting control fails")
    _released(boundary)


@pytest.mark.parametrize("failure", ["job_exit", "transport_refusal"])
def test_failed_transport_cannot_be_rescued_by_passing_stdout(boundary: ImageBoundary, failure: str) -> None:
    if failure == "transport_refusal":
        boundary.transport.refuse_attempt = True
    else:
        boundary.transport.attempt.exit_code = 7
        boundary.transport.attempt.stderr = TRANSPORT_ERROR
    failures = _failures(*_run_cli(boundary))
    check(set(failures) == {"in-image checks ran"}, "transport failure prevents image evidence acceptance")
    check(TRANSPORT_ERROR in failures["in-image checks ran"]["error"], "the refusal preserves the actual transport cause")
    _released(boundary)


def test_cleanup_refusal_is_not_a_successful_outer_result(boundary: ImageBoundary) -> None:
    boundary.transport.refuse_cleanup = True
    try:
        code = offline.main()
    except offline.launcher.ClientError as refusal:
        check(refusal.code == "refused" and str(refusal) == CLEANUP_ERROR, "the specific allocation-release refusal remains visible")
        reports = list((boundary.root / "data/e2e/orchestration_offline").glob("*/report.json"))
        check(all(json.loads(path.read_text())["status"] == "fail" for path in reports), "cleanup refusal cannot leave a passing report")
    else:
        reports = list((boundary.root / "data/e2e/orchestration_offline").glob("*/report.json"))
        check(len(reports) == 1, "a handled cleanup refusal persists a failed report")
        failures = _failures(code, json.loads(reports[0].read_text()))
        check(CLEANUP_ERROR in json.dumps(failures), "a handled cleanup refusal preserves its specific cause")
    _released(boundary)


def test_parse_error_releases_allocation_and_does_not_skip_the_next_control(boundary: ImageBoundary) -> None:
    boundary.transport.attempt.stdout = "ordinary output\nIN_IMAGE {\n"
    try:
        answer = offline.in_image(boundary.platform, None)
    except json.JSONDecodeError as error:
        check(error.doc == "{", "the parse failure comes from the intended malformed image document")
    else:
        check("error" in answer, "a handled malformed image document must remain an explicit refusal")
    _released(boundary)
    boundary.transport.attempt.stdout = _stdout(_document())
    _complete(*_run_cli(boundary))
    _released(boundary, 2)


@pytest.mark.parametrize(
    "field,value", [("release", None), ("release", ["synthetic-release-package"]), ("release", "synthetic-release-package"), ("release_problems", None)]
)
def test_malformed_release_fields_preserve_failed_report(boundary: ImageBoundary, field: str, value: Any) -> None:
    document = _document()
    document["constraints"][field] = value
    boundary.transport.attempt.stdout = _stdout(document)
    failures = _failures(*_run_cli(boundary))
    check(failures.get("in-image checks ran", {}).get("ok") is False, "malformed release evidence is a persisted report refusal")
    _released(boundary)
