"""E2E check of the gold star schema: facts, dimensions and bridges against their silver models (failure modes 838 to 843).

Run from the repository root with Docker running:

    .venv/bin/python -m scripts.lakehouse.run_gold_e2e                 # fixture stage only: silver and gold built on the fixture bronze
    .venv/bin/python -m scripts.lakehouse.run_gold_e2e --real          # then two real builds with a rebuild comparison (full)
    .venv/bin/python -m scripts.lakehouse.run_gold_e2e --real-once     # fixture stage, then one real build: an iteration run, not commit evidence
    .venv/bin/python -m scripts.lakehouse.run_gold_e2e --real-only     # one real build, no fixture stage: an iteration run, not commit evidence

The fixture stage uses the package fixture kit's credential-free launcher attempts (templates with no network), admitted through one
allocation; it never starts the catalog or reads a secret. The real stage builds from the catalog as the staging runner
does, with the bronze pin around each build (``pinned_run``), and reads the finished build file offline.

Every check reads the fact list, grains, columns, surrogate keys, source dates, text columns and expression allowlist from the
gold contract (``scripts/lakehouse/gold_checks.py``, ``gold_sql.py``). Verifying the runner before the gold models exist uses a smaller
contract and stand-in models: ``--contract FILE --overlay DIR --suites DIR`` and ``--self-test DIR`` (see ``plans/wp/g1_synthetic/``).
``--inject FAULT`` raises a fault in the fixture stage only, to show that the report is still written (failure mode 842).

The report goes to ``data/e2e/gold/report_<UTC>.json`` with the run ID, mode, phase, every check by name, per-fact row counts and the dbt
tree hash; it holds counts and model names only, never data values or credentials. An interrupt, an exit or an error in any phase still
writes the report with the run ID, the phase and the verdict so far, and the run fails.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import secrets
import shutil
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.lakehouse import catalog, gold_checks, package_fixture, provenance, silver
from scripts.lakehouse import run_staging_e2e as staging

REPO_ROOT = catalog.REPO_ROOT
REPORTS = REPO_ROOT / "data/e2e/gold"
BASELINES = REPO_ROOT / "data_contracts/great_expectations/baselines.json"
GOLD_MODELS = "path:models/gold"
BUILD_OK = ("success", "pass")
RELEASE_COPY_TEST = "release_copy_consistency_dim_release"
# dbt names a model-level generic test <test>_<model>_ (an empty column name leaves the trailing underscore); the lead names it without it.
RELEASE_COPY_NAMES = frozenset({RELEASE_COPY_TEST, RELEASE_COPY_TEST + "_"})
RELEASE_MODEL = "dim_release"
# Amendment A39 item 2: facts whose silver model holds no rows in the fixture build, each with the reason. A fact named here may be empty in the
# fixture stage only; the real stage has no exceptions. The gate adds an entry only after its own fixture run lists the empty fact.
FIXTURE_EMPTY_FACTS: dict[str, str] = {}
# Amendment A48: in the real stage a hold fact may be empty, because a source with nothing held has no hold rows. Row reconciliation still
# requires gold to equal silver, and at least one non-empty hold fact must still prove row preservation.
REAL_EMPTY_HOLD_REASON = "A48: nothing is held for this source in the real silver build"


def real_empty_allowed(contract: gold_checks.Contract) -> dict[str, str]:
    """The hold facts, each with the reason it may be empty in the real stage (amendment A48)."""
    return {fact.name: REAL_EMPTY_HOLD_REASON for fact in contract.facts if fact.is_hold}


FAULTS = ("interrupt", "exit", "error", "cleanup", "rebuild-empty", "real-setup-exit")
# The real stage's outcome as it runs, so a stage that stops still reaches the report [842].
REAL_PARTIAL: dict[str, Any] = {}
# GX replaces the historical A45 report-only split: all expression findings count in the verdict.
# Lineage and analysis completeness remain independent counted checks.
EXPRESSION_CHECKS = frozenset({"expression_arithmetic", "expression_case", "expression_functions"})
# Analysis that may have skipped a relation read is still counted, as analysis_complete: an unsupported query node or expression kind, a
# model with no compiled SQL or SQL that does not parse, and an unanalyzed expression of a class that can hold a subquery. Leaf classes cannot
# hold a read. Expression-policy violations also count, independently of analysis completeness.
LEAF_EXPRESSION_CLASSES = frozenset({"COLUMN_REF", "CONSTANT", "PARAMETER", "DEFAULT"})


def incomplete_analysis(problem: str) -> bool:
    """The analysis may not have visited every relation the model reads."""
    marker = " was not analyzed (unsupported shape)"
    if marker in problem:
        return problem.split(marker)[0].rsplit(" ", 1)[-1] not in LEAF_EXPRESSION_CLASSES
    return any(text in problem for text in (" is not supported", "no compiled SQL to check", "compiled SQL does not parse"))


# Every contract-driven check, so a check with no problem still appears in the report as a pass.
CHECK_NAMES = (
    "contract_matches_silver",
    "gold_relations_present",
    "column_map",
    "source_dates_typed",
    "published_text_kept",
    "reconcile_rows",
    "reconcile_content",
    "grain_unique_not_null",
    "hold_rows_exact",
    "fact_keys_not_null",
    "fact_keys_resolve",
    "fact_keys_follow_rule",
    "dimension_keys",
    "bridge_grain",
    "bridge_keys_resolve",
    "lineage",
    "gold_models_match_contract",
    "expression_arithmetic",
    "expression_case",
    "expression_functions",
    "analysis_complete",
    "coverage_complete",
    "suites_match_contract",
)


@dataclass
class Backend:
    """Offline queries on a finished build file, read-only in a container with no network and no secrets [843]."""

    database: Path
    attach: Path | None
    plan: package_fixture.Session

    def query(self, sql: str) -> list[list[str]]:
        return package_fixture.duckdb_query(self.database, sql, self.plan, self.attach)

    def run(self, statements: list[str]) -> list[list[str]]:
        """Run statements in chunks and return every row."""
        return [row for chunk in gold_checks.chunks(statements) for row in self.query(chunk)]


@dataclass
class SealedBackend(Backend):
    """Offline queries on the sealed copy of a run. Each query runs in a container whose entrypoint checks the copy that container attached before it runs
    ``duckdb``; the rows come back only for an accepted check, and a rejected one raises (D1 section 10 item 6). No path of the copy is ever handed out."""

    run_id: str = ""
    completion: dict[str, Any] = field(default_factory=dict)

    def query(self, sql: str) -> list[list[str]]:
        return provenance.sealed_duckdb(self.plan, self.run_id, self.completion, sql)


@dataclass
class Results:
    """Check verdicts by name: an empty problem list is a pass; ``details`` holds counts only."""

    problems: dict[str, list[str]]
    details: dict[str, Any]

    def add(self, name: str, problems: list[str]) -> None:
        self.problems.setdefault(name, []).extend(problems)

    def merge(self, found: dict[str, list[str]]) -> None:
        for name, problems in found.items():
            self.add(name, problems)

    @property
    def verdicts(self) -> dict[str, bool]:
        """Every computed check counts in the verdict."""
        return {name: not problems for name, problems in sorted(self.problems.items())}

    @property
    def report_only(self) -> dict[str, int]:
        """No expression check is report-only; retain the report field for existing consumers."""
        return {}

    @property
    def failed(self) -> list[str]:
        """Every check with findings, so a mutation case still names the check it targets."""
        return [name for name, problems in sorted(self.problems.items()) if problems]


@dataclass
class Run:
    """One run's state, kept so that any stop still produces a report with the run ID, the phase and the verdict so far (failure mode 842)."""

    mode: str
    run_id: str = field(default_factory=lambda: f"{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}-{secrets.token_hex(4)}")
    report: dict[str, Any] = field(default_factory=dict)
    checks: dict[str, bool] = field(default_factory=dict)
    cleanups: list[tuple[str, Callable[[], object]]] = field(default_factory=list)
    phase: str = "start"
    stopped: str | None = None

    def enter(self, phase: str) -> None:
        self.phase = phase
        self.report["phase"] = phase

    def stop(self, kind: str, detail: str) -> None:
        """Record why the run stopped and in which phase; the run fails."""
        self.stopped = f"{kind}: {detail}"
        self.report["stopped"] = {"kind": kind, "detail": detail, "phase": self.phase}

    def clean_up(self) -> None:
        """Run every registered cleanup; a failing cleanup, even an interrupted one, is recorded and never blocks the report."""
        failures: dict[str, str] = {}
        for name, action in self.cleanups:
            try:
                action()
            except BaseException as error:
                failures[name] = f"{type(error).__name__}: {error}"
        self.checks["cleanup_completed"] = not failures
        if failures:
            self.report["cleanup_failures"] = failures

    def finalize(self, dbt_at_start: str) -> Path | None:
        """Write the report; each step that can fail is protected so the report is still written."""
        self.clean_up()
        try:
            self.checks["dbt_unchanged_during_run"] = staging.dbt_tree_sha256() == dbt_at_start
        except BaseException as error:
            self.checks["dbt_unchanged_during_run"] = False
            self.report["tree_hash_error"] = f"{type(error).__name__}: {error}"
        self.checks["run_completed"] = self.stopped is None
        self.report |= {"run_id": self.run_id, "mode": self.mode, "dbt_tree_sha256": dbt_at_start, "checks": dict(sorted(self.checks.items()))}
        self.report["passed"], self.report["total"] = sum(self.checks.values()), len(self.checks)
        try:
            REPORTS.mkdir(parents=True, exist_ok=True)
            path = REPORTS / f"report_{self.run_id}.json"
            path.write_text(json.dumps(self.report, indent=2, default=str) + "\n")
        except BaseException as error:
            sys.stderr.write(f"gold e2e: the report could not be written ({type(error).__name__}: {error}); run {self.run_id}, phase {self.phase}\n")
            return None
        return path


def parse_compiled(target: Path, manifest: dict[str, Any], models: list[str], plan: package_fixture.Session) -> tuple[dict[str, str | None], frozenset[str]]:
    """Each gold model's compiled SQL as DuckDB's parse (JSON), None where no compiled file exists, and DuckDB's aggregate function names [840]."""
    nodes = gold_checks.gold_nodes(manifest)
    paths = {}
    for name in models:
        node = nodes.get(name)
        if node and (target / "compiled" / node["package_name"] / node["original_file_path"]).is_file():
            paths[name] = f"/workspace/compiled/{node['package_name']}/{node['original_file_path']}"
    query = gold_checks.parse_query(paths)
    params = {"mounts": [{"host": str((target / "compiled").resolve()), "container": "/workspace/compiled"}], "sql": query, "flags": ["-csv", "-noheader"]}
    result = plan.run("duckdb_helper", f"parse-{secrets.token_hex(4)}", [], params)
    if not result.ok:
        raise package_fixture.FixtureError(f"compiled SQL parse failed: {result.stderr.strip()[-300:]}")
    csv.field_size_limit(sys.maxsize)
    return gold_checks.split_parse_rows([row for row in csv.reader(io.StringIO(result.stdout))], models)


def run_checks(
    contract: gold_checks.Contract,
    backend: Backend,
    target: Path,
    suites: Path | None,
    statuses: dict[str, str] | None,
    empty_allowed: dict[str, str],
) -> Results:
    """Every contract-driven check on one build file and its dbt target folder; ``empty_allowed`` names facts that may hold no rows (A39)."""
    results = Results({}, {})
    for name in CHECK_NAMES:
        results.add(name, [])
    results.add("contract_matches_silver", gold_checks.contract_problems(contract))
    relations = gold_checks.parse_relations(backend.query(gold_checks.COLUMNS_SQL))
    results.merge(gold_checks.structure_problems(contract, relations))
    covered: dict[str, tuple[int, int]] = {}

    ready, unready = gold_checks.reconcilable(contract, relations)
    results.add("reconcile_rows", unready)
    rows = backend.run(gold_checks.reconcile_statements(ready))
    results.merge(gold_checks.reconcile_problems(ready, rows))
    covered["reconciled_facts"] = (len(ready), len(contract.facts))
    gold_rows = {name: int(values[0]) for name, side, *values in rows if side == "gold" and values}
    silver_rows = {name: int(count) for name, count in backend.run(gold_checks.silver_row_statements(contract, relations))}
    problems, detail = gold_checks.coverage_problems(contract, silver_rows, gold_rows, empty_allowed)
    results.add("coverage_complete", problems)
    results.details["rows_by_fact"] = detail

    grained = [fact for fact in contract.facts if fact.name in relations and fact.grain in dict(relations[fact.name])]
    results.add("grain_unique_not_null", gold_checks.grain_problems(backend.run(gold_checks.grain_statements(grained)), [fact.name for fact in grained]))
    results.add("grain_unique_not_null", [f"{fact.name}: grain column {fact.grain} is absent" for fact in contract.facts if fact not in grained])

    holds = [fact for fact in ready if fact.is_hold]
    results.add("hold_rows_exact", gold_checks.hold_problems(backend.run(gold_checks.hold_statements(holds)), [fact.name for fact in holds]))
    covered["hold_facts"] = (len(holds), sum(fact.is_hold for fact in contract.facts))

    keyed, skipped = keyed_facts(contract, relations)
    results.add("fact_keys_follow_rule", skipped)
    verdicts, unknown_rows = gold_checks.key_problems(keyed, backend.run(gold_checks.key_statements(keyed)))
    results.merge(verdicts)
    results.details["unknown_member_rows"] = unknown_rows
    covered["facts_with_keys"] = (len(keyed), sum(bool(fact.keys) for fact in contract.facts))

    dimensions = [table for table in contract.dimensions if table.key in dict(relations.get(table.name, []))]
    results.add(
        "dimension_keys", gold_checks.dimension_problems(backend.run(gold_checks.dimension_statements(dimensions)), [table.name for table in dimensions])
    )
    results.add("dimension_keys", [f"{table.name}: key column {table.key} is absent" for table in contract.dimensions if table not in dimensions])
    covered["dimensions"] = (len(dimensions), len(contract.dimensions))

    bridges = [table for table in contract.bridges if bridge_ready(table, relations)]
    results.merge(gold_checks.bridge_problems(bridges, backend.run(gold_checks.bridge_statements(bridges))))
    results.add(
        "bridge_grain", [f"{table.name}: relation, grain columns or referenced dimensions are absent" for table in contract.bridges if table not in bridges]
    )
    covered["bridges"] = (len(bridges), len(contract.bridges))

    manifest = json.loads((target / "manifest.json").read_text())
    lineage = gold_checks.lineage_problems(contract, manifest, gold_checks.published_silver())
    results.add("lineage", lineage)
    results.details["lineage_problems"] = lineage
    results.add("gold_models_match_contract", gold_checks.contract_models_problems(contract, manifest))
    models = sorted(set(contract.model_names) | set(gold_checks.gold_nodes(manifest)))
    parsed, aggregates = parse_compiled(target, manifest, models, backend.plan)
    compiled = {}
    for name, node in gold_checks.gold_nodes(manifest).items():
        path = target / "compiled" / node["package_name"] / node["original_file_path"]
        if path.is_file():
            compiled[name] = path.read_text()
    expression, reads = gold_checks.expression_problems(
        contract, parsed, models, aggregates, gold_checks.declared_relations(manifest), compiled, gold_checks.rendered_facts(contract, manifest)
    )
    results.merge(expression)
    incomplete = {problem for name in EXPRESSION_CHECKS for problem in expression.get(name, []) if incomplete_analysis(problem)}
    results.add("analysis_complete", sorted(incomplete))
    results.details["relation_reads"] = reads
    covered["parsed_models"] = (sum(text is not None for text in parsed.values()), len(models))

    if statuses is not None:
        results.add("gold_nodes_pass", gold_node_problems(manifest, statuses, RELEASE_MODEL in contract.model_names))
    if suites is not None:
        results.add("suites_match_contract", gold_checks.suites_problems(contract, suites))
    # A check that examined fewer models than the contract lists must not pass [843].
    results.add("coverage_complete", [f"{name}: {found} of {wanted}" for name, (found, wanted) in covered.items() if found < wanted])
    results.add("coverage_complete", ["the contract lists no facts"] if not contract.facts else [])
    results.details["coverage"] = {name: {"checked": found, "listed": wanted} for name, (found, wanted) in covered.items()}
    return results


def bridge_ready(table: gold_checks.Table, relations: gold_checks.Relations) -> bool:
    """A bridge whose relation holds its grain columns and whose referenced dimensions hold their keys."""
    columns = dict(relations.get(table.name, []))
    return (
        all(name in columns for name in table.grain_columns)
        and all(column in columns for column, _ in table.references)
        and all(gold_checks.dimension_key(dimension) in dict(relations.get(dimension, [])) for _, dimension in table.references)
    )


def keyed_facts(contract: gold_checks.Contract, relations: gold_checks.Relations) -> tuple[list[gold_checks.Fact], list[str]]:
    """Facts with surrogate keys whose columns and dimensions are in the build, and a problem for each one that is not."""
    keyed, problems = [], []
    for fact in contract.facts:
        if not fact.keys:
            continue
        gold = dict(relations.get(fact.name, []))
        lacking = [key.name for key in fact.keys if key.name not in gold] + [key.natural_key for key in fact.keys if key.natural_key not in gold]
        lacking += [key.dimension for key in fact.keys if gold_checks.dimension_key(key.dimension) not in dict(relations.get(key.dimension, []))]
        if lacking:
            problems.append(f"{fact.name}: cannot check keys, absent: {sorted(set(lacking))}")
        else:
            keyed.append(fact)
    return keyed, problems


def accepted(name: str, status: str) -> bool:
    """Return whether a required build node completed successfully."""
    return status in BUILD_OK


def not_passing(statuses: dict[str, str]) -> list[str]:
    """The nodes (by name) whose status is not accepted."""
    return sorted(name for name, status in statuses.items() if not accepted(name, status))


def disabled_problems(manifest: dict[str, Any]) -> list[str]:
    """Report any disabled declaration of the required physical-copy check."""
    declared = [
        node for nodes in manifest.get("disabled", {}).values() for node in nodes if node["resource_type"] == "test" and node["name"] in RELEASE_COPY_NAMES
    ]
    return [f"{RELEASE_COPY_TEST}: the required check is disabled"] if declared else []


def release_test_problems(manifest: dict[str, Any]) -> list[str]:
    """Require an enabled physical-copy check attached to dim_release with error severity."""
    found = [
        node
        for node in manifest["nodes"].values()
        if node["resource_type"] == "test"
        and node["name"] in RELEASE_COPY_NAMES
        and str(node.get("attached_node", "")).rsplit(".", 1)[-1] == RELEASE_MODEL
        and str(node.get("config", {}).get("severity", "")).lower() == "error"
        and node.get("config", {}).get("enabled", True) is True
    ]
    return [] if found else [f"{RELEASE_COPY_TEST}: no active test of that name is attached to {RELEASE_MODEL} with error severity"]


def gold_node_problems(manifest: dict[str, Any], statuses: dict[str, str], require_release_test: bool) -> list[str]:
    """Require every gold model and attached test to complete successfully."""
    gold = set(gold_checks.gold_nodes(manifest))
    wanted = sorted(gold) + sorted(
        unique_id.split(".")[2]
        for unique_id, node in manifest["nodes"].items()
        if node["resource_type"] == "test" and str(node.get("attached_node", "")).rsplit(".", 1)[-1] in gold
    )
    return (
        [f"{name}: {statuses.get(name, 'not run')}" for name in wanted if not accepted(name, statuses.get(name, "not run"))]
        + disabled_problems(manifest)
        + (release_test_problems(manifest) if require_release_test else [])
    )


def run_suites_offline(case: package_fixture.Case, suites: Path, plan: package_fixture.Session) -> dict[str, Any]:
    """Great Expectations on the fixture build in the quality image: no network, build file and code read-only [843]."""
    out = case.folder / "ge_out"
    shutil.rmtree(out, ignore_errors=True)
    out.mkdir()
    # The launcher mounts only folders inside the data folder (or the contract folder), so the suites are copied next to the case.
    staged = case.folder / "ge_suites"
    shutil.rmtree(staged, ignore_errors=True)
    shutil.copytree(suites.resolve(), staged)
    baselines = case.folder / "ge_baselines.json"
    shutil.copy2(BASELINES, baselines)
    params = {
        "database": str(case.folder / "staging.duckdb"),
        "build_root": str(case.folder),
        "suites_host": str(staged),
        "baselines_host": str(baselines),
        "out_host": str(out),
    }
    result = plan.run("quality_validate", f"suites-{secrets.token_hex(4)}", [], params)
    return suite_summary(out / "result.json", result.exit_code if result.exit_code is not None else 1)


def suite_summary(path: Path, exit_code: int) -> dict[str, Any]:
    """Counts and failed expectation IDs from a quality result; zero expectations is a failure [841]."""
    if not path.is_file():
        return {"exit_code": exit_code, "problems": ["the quality container wrote no result"], "expectations": 0}
    record = json.loads(path.read_text())
    if "error" in record:
        return {"exit_code": exit_code, "problems": [f"quality run stopped: {record['error']}"], "expectations": 0}
    failed = [item["id"] for item in record["expectations"] if item["level"] != "pass"]
    problems = [f"expectation failed: {name}" for name in failed[:20]]
    if not record["expectations"]:
        problems.append("no expectation ran")
    return {"exit_code": exit_code, "problems": problems, "expectations": len(record["expectations"]), "counts": record["counts"], "failed": len(failed)}


def copy_overlay(overlay: Path, project: Path) -> list[Path]:
    """Copy a folder of project-relative files (models, macros) into a case project; return the paths written."""
    written = []
    for source in sorted(item for item in overlay.rglob("*") if item.is_file()):
        destination = project / source.relative_to(overlay)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, destination)
        written.append(destination)
    return written


def overlay_sha256(overlays: list[Path]) -> str | None:
    if not overlays:
        return None
    digest = hashlib.sha256()
    for overlay in overlays:
        for path in sorted(item for item in overlay.rglob("*") if item.is_file()):
            digest.update(str(path.relative_to(overlay)).encode() + b"\0" + hashlib.sha256(path.read_bytes()).digest())
    return digest.hexdigest()


def statuses_by_name(statuses: dict[str, str]) -> dict[str, str]:
    """Node statuses keyed by node name (a unique ID is ``type.package.name``; a generic test's name ends with a hash)."""
    return {unique.split(".")[2]: status for unique, status in statuses.items()}


def dbt_run(case: package_fixture.Case, select: str) -> tuple[int, dict[str, str]]:
    """Run models of a case after a project edit and return dbt's exit code and each node's status by name; stale compiled files go first."""
    shutil.rmtree(case.folder / "target/compiled", ignore_errors=True)
    code, _tail, statuses = package_fixture.dbt(case, ["run", "--select", *select.split()])
    return code, statuses_by_name(statuses)


def rebuild_problems(code: int, statuses: dict[str, str], models: list[str]) -> list[str]:
    """A rebuild counts only when dbt exited 0 and every contract model reports success; the fingerprints are not compared otherwise [750]."""
    problems = [f"dbt exited {code}"] if code else []
    problems += [f"{model}: {statuses.get(model, 'not run')} in the rebuild" for model in models if not accepted(model, statuses.get(model, "not run"))]
    return problems


def gold_prints(contract: gold_checks.Contract, backend: Backend) -> list[list[str]]:
    """Fingerprint every gold relation over all its columns, to compare two builds [750]."""
    relations = gold_checks.parse_relations(backend.query(gold_checks.COLUMNS_SQL))
    return backend.run(gold_checks.rebuild_statements(contract, relations))


def rebuild_identical(
    case: package_fixture.Case, contract: gold_checks.Contract, backend: Backend, before: list[list[str]], select: str = GOLD_MODELS
) -> list[str]:
    """Rebuild the gold models and compare their fingerprints with the earlier ones; the problems, empty when the builds are identical [750]."""
    code, statuses = dbt_run(case, select)
    failed = rebuild_problems(code, statuses, contract.model_names)
    if failed:
        return failed
    return gold_checks.rebuild_problems(before, gold_prints(contract, backend), contract.model_names)


def restore(overlays: list[Path], added: list[Path], project: Path) -> None:
    """Put the base files back and delete files only the mutation added; the next case rebuilds."""
    for overlay in overlays:
        copy_overlay(overlay, project)
    for path in added:
        if not any((overlay / path.relative_to(project)).is_file() for overlay in overlays):
            path.unlink()


def case_sql(case: package_fixture.Case, sql: str) -> None:
    """Run SQL on the fixture build file itself (a data mutation of the self-test); the build file is the case's own copy."""
    (case.folder / "mutation.sql").write_text(sql)
    base = f"{package_fixture.CONTAINER_OUT}/{case.name}"
    params = {"case_host": str(case.folder.resolve()), "case_container": base, "database": f"{base}/staging.duckdb", "sql_file": f"{base}/mutation.sql"}
    result = package_fixture.ACTIVE[-1].run("fixture_init", f"mutation-{secrets.token_hex(4)}", [], params)
    if not result.ok:
        raise package_fixture.FixtureError(f"mutation SQL failed: {result.stderr.strip()[-300:]}")


def test_nodes(case: package_fixture.Case, tests: list[str], require_release_test: bool) -> tuple[list[str], dict[str, Any]]:
    """Run the named tests of a case and reject every status outside pass or success."""
    _code, _tail, raw = package_fixture.dbt(case, ["test", "--select", *tests])
    statuses = statuses_by_name(raw)
    manifest = json.loads((case.folder / "target/manifest.json").read_text())
    problems = not_passing(statuses) + disabled_problems(manifest) + ([] if statuses else ["no selected test ran"])
    problems += release_test_problems(manifest) if require_release_test else []
    return problems, {"statuses": statuses}


def contract_error_case(path: Path, fragment: str) -> dict[str, Any]:
    """A contract with an incomplete table definition must be refused when it is read, never run as an empty schema (A39)."""
    try:
        gold_checks.load_contract(path)
    except gold_checks.ContractError as error:
        return {"expected": [fragment], "failed": [str(error)], "ok": fragment in str(error)}
    return {"expected": [fragment], "failed": [], "ok": False}


def self_test(
    case: package_fixture.Case, folder: Path, overlays: list[Path], contract: gold_checks.Contract, backend: Backend, suites: Path | None
) -> dict[str, Any]:
    """Each mutation must fail exactly the checks ``expected.json`` names; the base must pass again at the end [843].

    An entry may name an overlay folder of the same name, a replacement ``contract``, ``sql`` run on the build file with the silver models in
    ``restore`` rebuilt afterwards, ``allow_empty`` facts (a test of A39's named exception), ``suites`` and ``rebuild`` runs.
    """
    expected = json.loads((folder / "expected.json").read_text())["mutations"]
    project = case.folder / "project"
    outcome: dict[str, Any] = {}
    for name, entry in expected.items():
        wanted = sorted(entry["fails"])
        if entry.get("contract_error"):
            outcome[name] = contract_error_case(folder / entry["contract"], entry["contract_error"])
            continue
        added = copy_overlay(folder / name, project) if (folder / name).is_dir() else []
        for statement in entry.get("sql", []):
            case_sql(case, statement)
        code, statuses = dbt_run(case, GOLD_MODELS)
        errors = not_passing(statuses) + ([f"dbt exited {code}"] if code else [])
        if errors:
            outcome[name] = {"expected": wanted, "failed": [], "build_errors": errors, "ok": False}
        else:
            mutated = gold_checks.load_contract(folder / entry["contract"]) if entry.get("contract") else contract
            results = run_checks(mutated, backend, case.folder / "target", suites, None, entry.get("allow_empty", {}))
            failed = results.failed
            if entry.get("rebuild") and rebuild_identical(case, mutated, backend, gold_prints(mutated, backend)):
                failed.append("gold_rebuild_identical")
            if entry.get("suites") and suites is not None and run_suites_offline(case, suites, backend.plan)["problems"]:
                failed.append("gold_suites_pass")
            extra: dict[str, Any] = {}
            if entry.get("tests"):
                node_problems, extra = test_nodes(case, entry["tests"], RELEASE_MODEL in mutated.model_names)
                if node_problems:
                    failed.append("dbt_nodes_pass")
            outcome[name] = {"expected": wanted, "failed": sorted(failed), "ok": sorted(failed) == wanted, **extra}
        restore(overlays, added, project)
        if entry.get("restore"):
            code, restored = dbt_run(case, " ".join(entry["restore"]))
            if code or not_passing(restored):
                outcome[name] = {**outcome[name], "ok": False, "restore_failed": True}
    code, statuses = dbt_run(case, GOLD_MODELS)
    again = run_checks(contract, backend, case.folder / "target", suites, None, {})
    failed_again = again.failed + ([f"dbt exited {code}"] if code else [])
    outcome["base_again"] = {"expected": [], "failed": failed_again, "ok": not failed_again}
    return outcome


def inject(run: Run, args: argparse.Namespace, point: str) -> None:
    """Raise the fault chosen with ``--inject`` at its point (fixture stage only), to show that the report is still written [842]."""
    if args.inject == "interrupt" and point == "checks":
        raise KeyboardInterrupt
    if args.inject == "exit" and point == "checks":
        raise SystemExit(3)
    if args.inject == "error" and point == "checks":
        raise RuntimeError("injected fault")
    if args.inject == "real-setup-exit" and point == "checks":

        def failing_install() -> None:
            raise SystemExit(4)

        real_setup(run, lambda: None, failing_install, lambda: run.report.setdefault("injected_release", []).append("called"))
    if args.inject == "cleanup" and point == "start":

        def failing_cleanup() -> None:
            raise OSError("injected cleanup failure")

        run.cleanups.append(("injected_cleanup", failing_cleanup))


def fixture_copy_conflict(case: package_fixture.Case) -> dict[str, bool]:
    """Require the real fixture dbt test to reject a second physical copy and pass after its removal."""
    case_sql(
        case,
        """INSERT INTO stg_bronze__file_copies
        SELECT copies.* REPLACE ('__synthetic_storage_duplicate__' AS s3_key, false AS is_canonical)
        FROM stg_bronze__file_copies AS copies
        INNER JOIN dim_release AS releases USING (member_sha256)
        WHERE releases.member_sha256 != '<unknown>' LIMIT 1;""",
    )
    try:
        code, _tail, statuses = package_fixture.dbt(case, ["test", "--select", "test_name:release_copy_consistency"])
        named = statuses_by_name(statuses)
        report = case.folder / "target/run_results.json"
        if report.exists():
            shutil.copyfile(report, case.folder / "storage_copy_conflict.run_results.json")
        rejected = code != 0 and {status for name, status in named.items() if name in RELEASE_COPY_NAMES} == {"fail"}
    finally:
        case_sql(case, "DELETE FROM stg_bronze__file_copies WHERE s3_key = '__synthetic_storage_duplicate__';")
    code, _tail, restored = package_fixture.dbt(case, ["test", "--select", "test_name:release_copy_consistency"])
    restored_release = [status for name, status in statuses_by_name(restored).items() if name in RELEASE_COPY_NAMES]
    return {
        "fixture.physical_copy_conflict_fails": rejected,
        "fixture.restored_physical_copy_test_passes": code == 0 and bool(restored_release) and all(status in BUILD_OK for status in restored_release),
    }


def fixture_stage(run: Run, args: argparse.Namespace, contract: gold_checks.Contract) -> None:
    """Build silver and gold on the fixture bronze and run every check; the whole stage holds one memory admission [843]."""
    overlays = [Path(item) for item in args.overlay]
    suites = args.suites
    stage: dict[str, Any] = {"details": {}}
    run.report["fixture"] = stage
    run.enter("fixture:admission")
    inject(run, args, "start")
    with package_fixture.reservation() as plan:
        stage["reservation"] = plan.allocation
        started = time.monotonic()
        run.enter("fixture:build")
        case = package_fixture.make_case("gold_fixture", plan, [])
        run.cleanups.append(("remove_ge_output", lambda: shutil.rmtree(case.folder / "ge_out", ignore_errors=True)))
        project = case.folder / "project"
        for overlay in overlays:
            copy_overlay(overlay, project)
        code, tail, statuses = package_fixture.dbt(case, ["build"])
        failed = not_passing(statuses_by_name(statuses))
        run.checks["fixture_build_passes"] = code == 0 and bool(statuses) and not failed
        stage["details"]["not_passing"] = failed[:30]
        stage["details"]["build_nodes"] = len(statuses)
        if not statuses:
            stage["details"]["build_tail"] = tail[-400:]
            return
        run.enter("fixture:checks")
        backend = Backend(case.folder / "staging.duckdb", case.folder / "fixture_lakehouse.duckdb", plan)
        results = run_checks(contract, backend, case.folder / "target", suites, statuses_by_name(statuses), FIXTURE_EMPTY_FACTS)
        run.checks.update({f"fixture.{name}": ok for name, ok in results.verdicts.items()})
        stage["report_only"] = results.report_only
        stage["details"].update(results.details)
        stage["problems"] = {name: problems[:10] for name, problems in results.problems.items() if problems}
        inject(run, args, "checks")
        run.enter("fixture:rebuild")
        select = "path:models/none" if args.inject == "rebuild-empty" else GOLD_MODELS
        rebuild = rebuild_identical(case, contract, backend, gold_prints(contract, backend), select)
        run.checks["fixture.gold_rebuild_identical"] = not rebuild
        stage["details"]["rebuild_problems"] = rebuild[:10]
        if suites is not None:
            run.enter("fixture:suites")
            summary = run_suites_offline(case, suites, plan)
            run.checks["fixture.gold_suites_pass"] = not summary["problems"]
            stage["details"]["suites"] = summary
        if args.self_test:
            run.enter("fixture:self_test")
            test = self_test(case, args.self_test, overlays, contract, backend, suites)
            stage["self_test"] = test
            run.checks.update({f"self_test.{name}": item["ok"] for name, item in test.items()})
        run.enter("fixture:physical_copy_conflict")
        run.checks.update(fixture_copy_conflict(case))
        stage["seconds"] = round(time.monotonic() - started, 1)


def real_setup(run: Run, start: Callable[[], object], install: Callable[[], object], release: Callable[[], object]) -> None:
    """Register the real cleanup before the first launch, then start the catalog and install the packages; a stop in either still releases [842]."""
    run.enter("real:setup")
    run.cleanups.append(("docker_release", release))
    run.enter("real:catalog")
    start()
    run.enter("real:packages")
    install()


def seal_check(build: str, run_id: str, outcome: dict[str, Any]) -> dict[str, Any]:
    """The completion record of the run's sealed copy, after the launcher's consumer container accepted it; an unsealed run or a rejected copy stops the stage
    (the readers still check the copy they attach: this call is the early stop, not the check)."""
    client = staging.launcher_client()
    principal = client.principal_run(run_id)
    completion = provenance.sealed_completion(client, principal, run_id)
    accepted = provenance.run_consumer(client, principal, run_id, f"verify-{build}", REPO_ROOT)
    outcome["checks"][f"{build}_sealed_consumer_accepts"] = accepted
    if not accepted:
        raise RuntimeError(f"the consumer container rejected the sealed copy of the {build} build; no reader opens it")
    outcome.setdefault("sealed", {})[build] = {key: completion[key] for key in ("sha256", "inode", "device", "invocation_id")}
    return completion


def real_stage(run: Run, contract: gold_checks.Contract, suites: Path, once: bool) -> None:
    """Two real builds (one in an iteration run), each inside a bronze pin; checks on the last build file, read offline [842]."""
    outcome: dict[str, Any] = {"checks": {}, "details": {}, "rebuild_checks_skipped": [], "docker_release": []}
    REAL_PARTIAL.clear()
    REAL_PARTIAL["outcome"] = outcome
    run.report["real"] = outcome
    real_setup(run, catalog.up, staging.install_packages, lambda: outcome["docker_release"].append(catalog.release()))
    prints: list[list[list[str]]] = []
    runs = ("real",) if once else ("real", "real_again")
    statuses: dict[str, str] = {}
    sealed: dict[str, tuple[str, dict[str, Any]]] = {}
    for build in runs:
        (staging.CASES / build).mkdir(parents=True, exist_ok=True)
        run.enter(f"real:{build}:pin")
        with staging.pinned_run(build, outcome) as pinned:
            run.enter(f"real:{build}:build")
            code, statuses = staging.dbt_build(build, "lakehouse")
            failed = not_passing(statuses)
            outcome["checks"][f"{build}_build_passes"] = pinned["build_passed"] = code == 0 and bool(statuses) and not failed
            if pinned["build_passed"]:
                run.enter(f"real:{build}:sealed")
                sealed[build] = (pinned["run_id"], seal_check(build, pinned["run_id"], outcome))
            run.enter(f"real:{build}:verify")
        outcome["checks"][f"{build}_pins_verified"] = pinned["pins_verified"]
        if build not in sealed:
            outcome["checks"][f"{build}_sealed"] = False
            return
        outcome[f"{build}_not_passing"] = failed[:30]
        run.enter(f"real:{build}:fingerprint")
        with package_fixture.reservation() as plan:
            prints.append(gold_prints(contract, SealedBackend(Path(sealed[build][1]["file"]), None, plan, *sealed[build])))
        if build == "real" and not once:
            # Free the memory the first build left in Docker's VM before the second build; the catalog starts again.
            outcome["docker_release"].append(catalog.release())
            catalog.up()
    if once:
        outcome["rebuild_checks_skipped"].append("gold_rebuild_identical")
    else:
        outcome["checks"]["gold_rebuild_identical"] = not gold_checks.rebuild_problems(prints[0], prints[1], contract.model_names)
    run.enter("real:checks")
    with package_fixture.reservation() as plan:
        backend = SealedBackend(Path(sealed[runs[-1]][1]["file"]), None, plan, *sealed[runs[-1]])
        results = run_checks(contract, backend, staging.CASES / runs[-1] / "target", suites, statuses, real_empty_allowed(contract))
        outcome["checks"].update({f"real.{name}": ok for name, ok in results.verdicts.items()})
        outcome["report_only"] = results.report_only
        outcome["details"].update(results.details)
        outcome["problems"] = {name: problems[:10] for name, problems in results.problems.items() if problems}
    # Great Expectations on the final build file, through the silver launcher with the gold suites folder.
    run.enter("real:suites")
    last_run, last_completion = sealed[runs[-1]]
    code, record = silver.validate(None, suites, BASELINES, staging.CASES / runs[-1] / "target/manifest.json", sealed_run=last_run, completion=last_completion)
    summary = json.loads(record.read_text())
    outcome["checks"]["real.gold_suites_pass"] = code == 0 and bool(summary.get("expectations")) and summary["counts"]["fail"] == 0
    outcome["suites"] = {"record": str(record.relative_to(REPO_ROOT)), "counts": summary["counts"], "repeated_dbt_tests": summary["repeated_dbt_tests"]}


def execute(run: Run, args: argparse.Namespace, contract: gold_checks.Contract, real: bool) -> None:
    """The stages in order; an exception here is caught by ``main`` and recorded with the phase."""
    if not args.real_only:
        fixture_stage(run, args, contract)
    if real:
        real_stage(run, contract, args.suites, once=args.real_once or args.real_only)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--real", action="store_true", help="after the fixture stage, build from the real bronze tables twice (full)")
    parser.add_argument(
        "--real-once", action="store_true", help="after the fixture stage, one real build, no rebuild comparison: an iteration run; implies --real"
    )
    parser.add_argument("--real-only", action="store_true", help="one real build and no fixture stage: an iteration run")
    parser.add_argument("--contract", type=Path, default=gold_checks.CONTRACT, help="the gold contract (a smaller one verifies the runner on stand-in models)")
    parser.add_argument("--suites", type=Path, default=gold_checks.SUITES, help="the Great Expectations gold suites folder")
    parser.add_argument("--overlay", action="append", default=[], metavar="DIR", help="project-relative files copied into the fixture project before the build")
    parser.add_argument(
        "--self-test", type=Path, metavar="DIR", help="a folder of mutation overlays with expected.json: each must fail exactly the named checks"
    )
    parser.add_argument("--inject", choices=FAULTS, help="raise a fault in the fixture stage to show that the report is still written; never with a real stage")
    args = parser.parse_args()
    real = args.real or args.real_once or args.real_only
    if args.inject and real:
        parser.error("--inject runs in the fixture stage only")
    mode = "iteration" if args.real_once or args.real_only else ("full" if args.real else "fixture_only")
    dbt_at_start = staging.dbt_tree_sha256()
    run = Run(mode)
    run.report |= {"started_at": datetime.now(UTC).isoformat(timespec="seconds"), "image": package_fixture.IMAGE, "inject": args.inject}
    code = 1
    try:
        contract = gold_checks.load_contract(args.contract)
        run.report["contract"] = {
            "path": str(args.contract),
            "sha256": hashlib.sha256(args.contract.read_bytes()).hexdigest(),
            "facts": len(contract.facts),
            "dimensions": len(contract.dimensions),
            "bridges": len(contract.bridges),
        }
        run.report["overlay_sha256"] = overlay_sha256([Path(item) for item in args.overlay])
        execute(run, args, contract, real)
    except KeyboardInterrupt:
        run.stop("KeyboardInterrupt", "the run was interrupted")
        code = 130
    except SystemExit as stop:
        run.stop("SystemExit", str(stop.code))
        code = 2
    except Exception as error:
        run.stop(type(error).__name__, str(error))
    finally:
        # A real stage that stopped keeps what ran and the run ID of any pin [842].
        if real and run.report.get("real") is None:
            run.report["real"] = REAL_PARTIAL.get("outcome") or {"checks": {}}
        real_checks = (run.report.get("real") or {}).get("checks", {})
        run.checks.update({name: ok for name, ok in real_checks.items()})
        path = run.finalize(dbt_at_start)
    for name, ok in sorted(run.checks.items()):
        sys.stdout.write(f"{'PASS' if ok else 'FAIL'} {name}\n")
    note = "; iteration run: not commit evidence" if mode == "iteration" else ""
    where = f"report {path.relative_to(REPO_ROOT)}" if path else "no report written"
    sys.stdout.write(f"gold e2e ({mode}): {run.report['passed']} of {run.report['total']} passed; phase {run.phase}; run {run.run_id}; {where}{note}\n")
    ok = run.report["passed"] == run.report["total"] and run.report["total"] > 0 and path is not None
    return 0 if ok else code


if __name__ == "__main__":
    sys.exit(main())
