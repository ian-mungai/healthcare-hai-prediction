"""Synthetic caller cases for training snapshots, broker admission and extraction."""

from __future__ import annotations

import copy
import hashlib
import json
import os
import shutil
import tempfile
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import date
from functools import partial
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest import mock

from orchestration import code_digest, graph, launcher, mlflow_service, training_approvals, training_contract, training_render
from orchestration.interface import SubmissionRefused, SubmissionRequest
from scripts.features import training_extract as extract
from scripts.lakehouse import memory_budget, run_lock
from scripts.orchestration.broker import Broker, BrokerPaths, load_templates
from scripts.process import CompletedProcess
from scripts.training.step_records import verify_done, write_done

ROOT = Path(__file__).resolve().parents[2]
RUN = "20261024T000000Z-1234abcd"
PUBLICATION = "20261023T000000Z-1234abcd"
GOLD = "20261022T000000Z-1234abcd"
PARAMETERS = {"features_run": "latest", "population": "primary", "hai_types": list(training_contract.HAI_TYPES), "trigger": "manual", "mlflow": "local"}
REGISTRY_ID = "12345678-1234-4234-8234-123456789abc"


def require(condition: Any, message: str) -> None:
    """Fail a case with a bounded contract message."""
    if not condition:
        raise ValueError(message)


@dataclass
class Cases:
    """A named synthetic case ledger, without starting a launcher or service."""

    results: list[dict[str, Any]]

    def check(self, name: str, operation: Callable[[], Any]) -> None:
        """Record a successful assertion or its visible failure."""
        try:
            operation()
        except Exception as error:
            self.results.append({"name": name, "passed": False, "detail": f"{type(error).__name__}: {error}"})
        else:
            self.results.append({"name": name, "passed": True, "detail": ""})

    def refuses(self, name: str, operation: Callable[[], Any], reason: str = "") -> None:
        """Require the real boundary to reject a named synthetic invalid input."""

        def run() -> None:
            try:
                operation()
            except (ValueError, OSError, TimeoutError, SubmissionRefused, memory_budget.BudgetError) as error:
                require(not reason or reason in str(error), "refusal has another cause")
            else:
                raise ValueError("invalid input was accepted")

        self.check(name, run)


def fixture(root: Path) -> dict[str, Any]:
    """Create public-shape synthetic metadata and the declared snapshot roots."""
    for name in (
        "scripts",
        "orchestration",
        "feature_store",
        "config/training/feature_lists",
        "data_contracts/features",
        "data_contracts/lakehouse",
        "data_contracts/training",
        "docs",
        "dbt/seeds/gold",
        "dbt/seeds/features",
    ):
        (root / name).mkdir(parents=True, exist_ok=True)
    for name in (
        "scripts/sample.py",
        "orchestration/sample.py",
        "feature_store/feature_store.yaml",
        "data_contracts/lakehouse/sample.json",
        "docs/analysis_plan.md",
    ):
        (root / name).write_text("synthetic snapshot input\n")
    for name in ("hai_outcome_measures_bridge.csv", "cost_report_measures_bridge.csv"):
        (root / "dbt/seeds/gold" / name).write_text("synthetic control metadata\n")
    for name in ("targets.json", "excluded_controls.json"):
        shutil.copyfile(ROOT / "config/training" / name, root / "config/training" / name)
    shutil.copyfile(ROOT / "data_contracts/training/extract_manifest.schema.json", root / "data_contracts/training/extract_manifest.schema.json")
    groups: list[dict[str, Any]] = []
    for name in extract.GROUP_COLUMNS:
        groups.append(
            {
                "name": name,
                "type": "bigint" if name == "bed_count" else "varchar",
                "role": "predictor",
                "view": "feature_matrix_hospital_window",
                "pivot": {"copy": name},
            }
        )
    predictor: dict[str, Any] = {
        "name": "c001__score__number",
        "type": "double",
        "role": "predictor",
        "view": "feature_matrix_hospital",
        "pivot": {"match": {"measure_control": "C001"}},
    }
    labels = [
        {"name": f"hai_{number}__{field}", "type": "double" if field == "sir__number" else "varchar", "role": "label", "view": f"label_hai_{number}"}
        for number in range(1, 7)
        for field in ("sir__number", "alignment_status__category")
    ]
    schema: dict[str, Any] = {
        "schema": 1,
        "tables": {
            "feature_matrix_hospital_window": {"role": "predictor", "columns": groups},
            "feature_matrix_hospital": {"role": "predictor", "columns": [predictor]},
            "label_matrix_hai_outcomes": {"role": "label", "columns": labels},
        },
        "servable": sorted(f"{column['view']}:{column['name']}" for column in [*groups, predictor]),
    }
    (root / "data_contracts/features/feature_schema.json").write_bytes(extract.canonical(schema) + b"\n")
    refs = [f"{predictor['view']}:{predictor['name']}"]
    (root / "config/training/feature_lists/m1_ols.json").write_bytes(extract.canonical({"references": refs}) + b"\n")
    seed = "measure_control,hai_type,field\n" + "".join(f"C{268 + number},HAI_{number},sir\n" for number in range(1, 7))
    (root / "dbt/seeds/features/frozen_hai_outcome_controls.csv").write_text(seed)
    return schema


def fixture_git(root: Path, files: Sequence[str], deadline: float) -> dict[str, Any]:
    """Name an explicit synthetic commit and copied untracked module."""
    return {"git_commit": "a" * 40, "git_dirty": True, "untracked_files": ["scripts/sample.py"]}


@dataclass(frozen=True)
class Publication:
    """The accepted publication shape, with synthetic snapshots."""

    namespace: str
    run_id: str
    snapshot_set: dict[str, int]

    @property
    def snapshot_set_sha256(self) -> str:
        """Match the producer's canonical snapshot-set identity."""
        return hashlib.sha256(json.dumps(dict(sorted(self.snapshot_set.items())), separators=(",", ":")).encode()).hexdigest()


class SyntheticReader:
    """Stand in for the accepted helper and pinned scans using no Feast or catalog service."""

    def __init__(self, root: Path) -> None:
        self.schema = json.loads((root / "data_contracts/features/feature_schema.json").read_bytes())
        self.schema_sha256 = hashlib.sha256((root / "data_contracts/features/feature_schema.json").read_bytes()).hexdigest()
        self.selected = Publication(
            "features_e2e", PUBLICATION, {name: index + 1 for index, name in enumerate((*extract.SUPPORT_TABLES, "feature_matrix_hospital"))}
        )
        self.heads = dict(self.selected.snapshot_set)
        self.references: tuple[str, ...] = ()
        self.historical_calls = 0
        self.scans: list[dict[str, Any]] = []
        self.move_support_head = False
        self.rows: list[dict[str, Any]] = []
        spine = []
        validation = []
        for hospital in range(1, 4):
            for year in (2024, 2025, 2026):
                key = f"{hospital:06d}:{year}"
                row = {
                    "spine_key": key,
                    "c001__score__number": float(hospital),
                    "control_type_code": "1",
                    "provider_subtype_code": "01",
                    "state_code": "WA",
                    "county_fips": "53001",
                    "bed_count": 100,
                }
                for number in range(1, 7):
                    row[f"hai_{number}__sir__number"] = hospital / 10 + number
                    row[f"hai_{number}__alignment_status__category"] = "aligned"
                self.rows.append(row)
                spine.append({"spine_key": key, "window_start": date(year, 1, 1), "is_primary_population": hospital < 3, "is_sensitivity_population": True})
                for measure, control in (("PSI_90", "E038"), ("PSI_13", "E039")):
                    validation.append(
                        {
                            "alignment_key": f"{key}:{control}:{measure}",
                            "spine_key": key,
                            "measure_control": control,
                            "measure": measure,
                            "fact": "fct_complications_deaths_window",
                            "value_text": None,
                            "value_number": float(hospital),
                            "period_start": date(year - 1, 1, 1),
                            "period_end": date(year - 1, 12, 31),
                            "fiscal_year": None,
                            "alignment_status": "aligned",
                        }
                    )
                validation.append(
                    {
                        "alignment_key": f"{key}:C285:payment_reduction",
                        "spine_key": key,
                        "measure_control": "C285",
                        "measure": "payment_reduction",
                        "fact": "fct_hac_program_year",
                        "value_text": "Yes",
                        "value_number": None,
                        "period_start": date(year, 1, 1),
                        "period_end": date(year, 12, 31),
                        "fiscal_year": year + 1,
                        "alignment_status": "aligned",
                    }
                )
        self.rows[0]["hai_1__sir__number"] = None
        self.rows[1]["hai_1__alignment_status__category"] = "held_in_staging"
        self.rows[2]["hai_1__sir__number"] = float("nan")
        self.tables: dict[str, list[dict[str, Any]]] = {
            "int_hospital_spine": spine,
            "int_spine_validation_measures": validation,
            "int_county_adjacency": [{"edge_key": "53001:53001", "county_fips": "53001", "neighbor_fips": "53001", "is_self_link": True, "is_isolated": False}],
            "feature_build_inputs": [
                {
                    "build_key": "current",
                    "features_run_id": PUBLICATION,
                    "gold_run_id": GOLD,
                    "gold_snapshot_set_sha256": "e" * 64,
                    "feature_schema_sha256": self.schema_sha256,
                    "hai_outcome_controls_sha256": hashlib.sha256((root / "dbt/seeds/features/frozen_hai_outcome_controls.csv").read_bytes()).hexdigest(),
                }
            ],
        }

    def publication(self, run: str | None) -> Publication:
        """Return the named synthetic publication."""
        require(run in (None, PUBLICATION), "another publication was requested")
        return self.selected

    def historical(self, references: Sequence[str], run: str) -> Any:
        """Materialize the historical result through the helper-shaped stand-in."""
        require(run == PUBLICATION, "historical helper did not receive the selected publication")
        self.historical_calls += 1
        self.references = tuple(references)
        return SimpleNamespace(
            frame=SimpleNamespace(to_pylist=lambda: copy.deepcopy(self.rows)),
            publication=self.selected,
            feature_schema_sha256=self.schema_sha256,
            entity_count=len(self.rows),
        )

    def consistent(self, publication: Any, operation: Callable[[], Any]) -> Any:
        """Reject support-head movement around the consumed scans."""
        require(self.heads == publication.snapshot_set, "support head differs before reading")
        result = operation()
        if self.move_support_head:
            self.heads["int_county_adjacency"] += 1
        require(self.heads == publication.snapshot_set, "support head differs after reading")
        return result

    def scan(self, publication: Any, table: str, fields: tuple[str, ...]) -> list[dict[str, Any]]:
        """Return explicit fields and record the exact snapshot requested."""
        self.scans.append({"table": table, "snapshot_id": publication.snapshot_set[table], "fields": list(fields)})
        return [{name: row.get(name) for name in fields} for row in copy.deepcopy(self.tables[table])]


class JsonTableStandIn:
    """Write typed synthetic JSON to exercise publication without claiming Parquet verification."""

    def __init__(self) -> None:
        self.rows: dict[str, list[dict[str, Any]]] = {}
        self.calls = 0
        self.fail_at: int | None = None

    def write(self, path: Path, rows: Sequence[Mapping[str, Any]], columns: Mapping[str, str]) -> None:
        """Write the explicit JSON stand-in and retain its synthetic rows for assertions."""
        self.calls += 1
        if self.calls == self.fail_at:
            raise OSError("synthetic writer interrupted")
        self.rows[path.name] = copy.deepcopy([dict(row) for row in rows])
        payload = {
            "classification": "Synthetic",
            "encoding": "JSON stand-in, not Parquet",
            "columns": dict(columns),
            "rows": [[extract._value(row.get(name)) for name in columns] for row in rows],
        }
        path.write_bytes(extract.canonical(payload) + b"\n")


def extract_command_case(source: Path, folder: Path) -> None:
    """Run the real extraction command and reader adapter with explicit external-library stand-ins."""
    reader, writer = SyntheticReader(source), JsonTableStandIn()
    scratch, output = folder / "feast_scratch", folder / "command_extract"
    parameters, snapshot, lock = (folder / name for name in ("parameters.json", "snapshot.json", "writer_lock.txt"))
    parameters.write_text(json.dumps(PARAMETERS))
    snapshot.write_text(json.dumps({"run_id": RUN, "code_digest": "a" * 64}))
    lock.write_text("synthetic writer lock\n")

    def settings(environ: Mapping[str, str], mode: str) -> Any:
        require(mode == "publish_fixture", "the command selected another catalog mode")
        return SimpleNamespace(sql_uri=f"sqlite:///{environ['FEAST_RUN_FOLDER']}/feast/catalog.db", sql_warehouse="file:///workspace/local_catalog/warehouse")

    def catalog(environ: Mapping[str, str], selected: str | None) -> SyntheticReader:
        require(environ is os.environ, "Feast cannot see a detached command environment")
        expected = settings(environ, "publish_fixture")
        require(environ.get("FEAST_SQL_CATALOG_URI") == expected.sql_uri, "the SQL catalog URI was not prepared")
        require(environ.get("FEAST_SQL_WAREHOUSE") == expected.sql_warehouse, "the SQL warehouse was not prepared")
        require((scratch / "feast").is_dir(), "the writable Feast cache directory is absent")
        require(selected is None, "the latest publication parameter changed")
        (scratch / "feast/catalog.db").write_text("synthetic SQLite cache\n")
        return reader

    def load_table(selected: SyntheticReader, namespace: str, name: str) -> Any:
        require(selected is reader and namespace == "features_e2e", "the support read selected another catalog")

        def scan(*, selected_fields: tuple[str, ...], snapshot_id: int) -> Any:
            require(snapshot_id == reader.selected.snapshot_set[name], "the support scan selected another snapshot")
            rows = reader.scan(reader.selected, name, selected_fields)
            return SimpleNamespace(to_arrow=lambda: SimpleNamespace(to_pylist=lambda: rows))

        return SimpleNamespace(scan=scan)

    def historical(references: Sequence[str], **kwargs: Any) -> Any:
        require(kwargs["environ"] is os.environ and kwargs["catalog"] is reader and not kwargs["register"], "the helper lost its catalog environment")
        return reader.historical(references, kwargs["publication_run"])

    modules = {
        "scripts.features.publication_reads": SimpleNamespace(
            catalog_settings=settings,
            read_schema=lambda path: (reader.schema, reader.schema_sha256),
            read_publication=lambda selected, namespace, run: selected.publication(run),
            consistent_read=lambda selected, publication, operation: selected.consistent(publication, operation),
            load_table=load_table,
        ),
        "scripts.features.historical": SimpleNamespace(default_catalog=catalog, historical_features=historical),
    }
    arguments = [
        "extract",
        "--run",
        RUN,
        "--mode",
        "fixture",
        "--run-dir",
        str(output),
        "--parameters",
        str(parameters),
        "--snapshot-record",
        str(snapshot),
        "--root",
        str(source),
        "--writer-lock",
        str(lock),
    ]
    with (
        mock.patch.dict(os.environ, {"FEAST_NAMESPACE": "features_e2e", "FEAST_RUN_FOLDER": str(scratch)}),
        mock.patch.object(extract.importlib, "import_module", side_effect=modules.__getitem__),
        mock.patch.object(extract, "ParquetWriter", return_value=writer),
    ):
        require(extract.main(arguments) == 0, "the extraction command did not prepare the accepted Feast environment")
    require(len(verify_done(output, "extract", "a" * 64)["outputs"]) == 15, "the command mixed Feast cache files with its extract outputs")


def extraction_cases(cases: Cases, folder: Path) -> None:
    """Exercise the extraction entry point with explicit stand-ins and adversarial publications."""
    source = folder / "source"
    fixture(source)
    cases.check("extract_command_prepares_feast_scratch", lambda: extract_command_case(source, folder))
    reader, writer = SyntheticReader(source), JsonTableStandIn()
    output = folder / "extract"

    def run(
        destination: Path, selected: SyntheticReader = reader, table_writer: JsonTableStandIn = writer, params: dict[str, Any] = PARAMETERS, **extra: Any
    ) -> dict[str, Any]:
        return extract.extract(source, destination, RUN, params, "a" * 64, selected, writer_lock_sha256="b" * 64, writer=table_writer, **extra)

    cases.check(
        "extract_publication_complete",
        lambda: require(len(run(output)["outputs"]) == 15, "six extracts, six manifests and three auxiliary files were not written"),
    )
    if not (output / "extract.done.json").exists():
        return
    manifest = json.loads((output / "manifest_HAI_1.json").read_bytes())
    cases.check(
        "extract_population_and_finite_sir",
        lambda: require(
            manifest["counts"] == {"spine": 9, "population_excluded": 3, "label_excluded": 3, "included": 3},
            "population, status or finite published SIR filtering changed",
        ),
    )
    cases.check("extract_one_historical_call", lambda: require(reader.historical_calls == 1, "historical helper was not called exactly once"))
    cases.check(
        "extract_pinned_support_scans",
        lambda: require(
            {row["table"]: row["snapshot_id"] for row in reader.scans} == {name: reader.selected.snapshot_set[name] for name in extract.SUPPORT_TABLES},
            "support scans did not use the selected snapshots",
        ),
    )
    cases.check(
        "extract_no_mediator_or_hac_predictors",
        lambda: require(
            all(
                not any("psi" in key.lower() or "hac" in key.lower() or key.startswith(("e038", "e039", "c285")) for key in row)
                for row in writer.rows["extract_HAI_1.parquet"]
            ),
            "a mediator or HAC column reached a predictor extract",
        ),
    )
    cases.check(
        "extract_auxiliary_separation",
        lambda: require(
            len(writer.rows["mediator.parquet"]) == 12 and len(writer.rows["hac.parquet"]) == 6, "auxiliary files did not retain the selected population"
        ),
    )
    cases.check(
        "extract_manifest_identity",
        lambda: require(
            manifest["publication_run_id"] == PUBLICATION and manifest["gold_run_id"] == GOLD and manifest["writer_lock_sha256"] == "b" * 64,
            "manifest input identities changed",
        ),
    )
    cases.check(
        "extract_retry_skips_reader", lambda: require(run(output)["resumed"] and reader.historical_calls == 1, "a completed extract read the publication again")
    )
    shuffled = SyntheticReader(source)
    shuffled.rows.reverse()
    for rows in shuffled.tables.values():
        rows.reverse()
    repeat_writer = JsonTableStandIn()
    cases.check("extract_shuffled_input_is_equal", lambda: run(folder / "repeat", shuffled, repeat_writer))
    cases.check(
        "extract_value_hash_is_repeatable",
        lambda: require(
            json.loads((folder / "repeat/manifest_HAI_1.json").read_bytes())["value_content_sha256"] == manifest["value_content_sha256"],
            "row order changed the content identity",
        ),
    )
    shuffled_aux = SyntheticReader(source)
    for row in shuffled_aux.tables["int_spine_validation_measures"]:
        row["value_number"] = 999.0
        row["value_text"] = "No"
    cases.check(
        "extract_auxiliary_shuffle_does_not_change_predictors",
        lambda: require(
            run(folder / "aux_changed", shuffled_aux, JsonTableStandIn())
            and json.loads((folder / "aux_changed/manifest_HAI_1.json").read_bytes())["value_content_sha256"] == manifest["value_content_sha256"],
            "auxiliary values changed predictor content",
        ),
    )
    sensitivity = {**PARAMETERS, "population": "sensitivity"}
    cases.check(
        "extract_sensitivity_population",
        lambda: require(
            run(folder / "sensitivity", SyntheticReader(source), JsonTableStandIn(), sensitivity)
            and json.loads((folder / "sensitivity/manifest_HAI_2.json").read_bytes())["row_count"] == 9,
            "sensitivity flags were not used",
        ),
    )
    for name, mutate, reason in (
        ("extract_moving_support_head_refused", lambda item: setattr(item, "move_support_head", True), "support head"),
        ("extract_duplicate_historical_key_refused", lambda item: item.rows.append(copy.deepcopy(item.rows[0])), "repeated"),
        (
            "extract_duplicate_population_key_refused",
            lambda item: item.tables["int_hospital_spine"].append(copy.deepcopy(item.tables["int_hospital_spine"][0])),
            "repeated",
        ),
        (
            "extract_control_identity_refused",
            lambda item: item.tables["feature_build_inputs"][0].update(hai_outcome_controls_sha256="0" * 64),
            "control identity",
        ),
        ("extract_schema_identity_refused", lambda item: item.tables["feature_build_inputs"][0].update(feature_schema_sha256="0" * 64), "schema"),
        ("extract_missing_support_refused", lambda item: item.selected.snapshot_set.pop("int_county_adjacency"), "support snapshot"),
        (
            "extract_psi_period_boundary_refused",
            lambda item: item.tables["int_spine_validation_measures"][0].update(period_end=date(2024, 1, 1)),
            "mediator period",
        ),
        ("extract_unexpected_real_column_refused", lambda item: item.rows[0].update(psi_90=1.0), "unexpected columns"),
    ):
        selected = SyntheticReader(source)
        mutate(selected)
        cases.refuses(name, partial(run, folder / name, selected, JsonTableStandIn()), reason)
    interrupted = JsonTableStandIn()
    interrupted.fail_at = 2
    cases.refuses("extract_interruption_has_no_done", lambda: run(folder / "interrupted", SyntheticReader(source), interrupted), "interrupted")
    cases.check(
        "extract_interrupted_output_is_incomplete",
        lambda: require(not (folder / "interrupted/extract.done.json").exists(), "an interrupted extract has a completion record"),
    )
    cases.check(
        "extract_interruption_retry",
        lambda: require(
            not run(folder / "interrupted", SyntheticReader(source), JsonTableStandIn())["resumed"], "the interrupted extract was incorrectly skipped"
        ),
    )
    clock_values = iter((0.0, 0.0, 10.0))
    cases.refuses(
        "extract_absolute_deadline",
        lambda: run(folder / "expired", SyntheticReader(source), JsonTableStandIn(), seconds=1.0, clock=lambda: next(clock_values, 10.0)),
        "deadline",
    )
    poisoned = copy.deepcopy(reader.schema)
    column = poisoned["tables"]["feature_matrix_hospital"]["columns"][0]
    column["pivot"]["match"]["measure_control"] = "E038"
    cases.refuses("extract_mediator_reference_refused", lambda: extract.reference_union(source, poisoned), "excluded")
    (output / "extract_HAI_1.parquet").write_text("changed synthetic bytes")
    cases.refuses("extract_changed_done_output_refused", lambda: run(output), "differs")


def orchestration_cases(cases: Cases, folder: Path) -> None:
    """Exercise snapshots, broker rows, approvals and fixed rendered mount sets."""
    source = folder / "source"
    fixture(source)
    run_folder = folder / RUN
    record = code_digest.snapshot_training(source, run_folder, RUN, git_reader=fixture_git)
    cases.check(
        "snapshot_all_control_bridges",
        lambda: require((run_folder / "code/dbt/seeds/gold/cost_report_measures_bridge.csv").is_file(), "a non-HAI bridge was omitted"),
    )
    cases.check(
        "snapshot_feature_control_identity",
        lambda: require((run_folder / "code/dbt/seeds/features/frozen_hai_outcome_controls.csv").is_file(), "feature control identity was omitted"),
    )
    cases.check(
        "snapshot_untracked_provenance",
        lambda: require(record["git_dirty"] and record["untracked_files"] == ["scripts/sample.py"], "copied untracked code was not recorded"),
    )
    before = (run_folder / code_digest.RECORD).read_bytes()
    (source / "docs/analysis_plan.md").write_text("changed live plan\n")
    cases.check(
        "snapshot_retry_ignores_live_edits",
        lambda: require(
            code_digest.snapshot_training(source, run_folder, RUN, git_reader=fixture_git)["code_digest"] == record["code_digest"]
            and (run_folder / code_digest.RECORD).read_bytes() == before,
            "retry refreshed the code snapshot",
        ),
    )
    with mock.patch.object(
        code_digest, "run_command", side_effect=[CompletedProcess([], 0, "a" * 40 + "\n", ""), CompletedProcess([], 0, "", ""), CompletedProcess([], 0, "", "")]
    ):
        cases.check(
            "untracked_files_are_dirty",
            lambda: require(
                code_digest.git_state(source, ["scripts/sample.py"], time.monotonic() + 10)["git_dirty"], "copied untracked files did not mark the run dirty"
            ),
        )
    linked = folder / "linked"
    fixture(linked)
    (linked / "docs/analysis_plan.md").rename(linked / "docs/plan_original.md")
    (linked / "docs/analysis_plan.md").symlink_to("plan_original.md")
    cases.refuses("snapshot_linked_plan_refused", lambda: code_digest.snapshot_training(linked, folder / "linked_run", RUN, git_reader=fixture_git), "link")
    extras = run_folder / "code/unexpected.py"
    extras.write_text("unexpected staged code\n")
    cases.refuses("snapshot_added_code_refused", lambda: code_digest.verify_snapshot(run_folder, RUN), "changed")
    extras.unlink()
    parameters_path = run_folder / "training_parameters.json"
    parameters_path.write_text(json.dumps(PARAMETERS))
    snapshot_state = {"phase": "complete", "code_digest": record["code_digest"], "record_sha256": hashlib.sha256(before).hexdigest()}
    state = {
        "registry_kind": "test",
        "training": {"dag_id": "training_dag", "parameters": PARAMETERS},
        "training_snapshot": snapshot_state,
        "training_mlflow": {"phase": "ready", "name": "isolated-mlflow"},
    }
    for step in ("guard", "extract", "version_extract", "fit", "mediation", "hac_validate", "log_run"):
        output = run_folder / "outputs" / step
        output.mkdir(parents=True)
        names = [f"{step}.json"]
        if step == "extract":
            names += [f"{prefix}_{hai}.{suffix}" for hai in training_contract.HAI_TYPES for prefix, suffix in (("extract", "parquet"), ("manifest", "json"))]
            names += ["mediator.parquet", "hac.parquet", "adjacency.parquet"]
        for name in names:
            (output / name).write_text("synthetic step output\n")
        write_done(output, step, record["code_digest"], names)
    inp = launcher.RenderInput(
        "training_job",
        RUN,
        "contract",
        1,
        1,
        "op",
        {"JOB_MEMORY_LIMIT": str(1024**3), "JOB_THREADS": "1"},
        2**31,
        [],
        {"action": "fit", "parameters": PARAMETERS},
        {},
        run_folder=run_folder,
        run_mode="fixture",
        pipeline=state,
    )
    spec = training_render.build(inp, {}, None)
    extract_input = replace(inp, template="training_extract", params={"action": "extract", "parameters": PARAMETERS})
    publication = {"local_catalog": str(folder / "publication/local_catalog"), "registry": str(folder / "publication/registry.pb")}
    extract_spec = training_render.build(replace(extract_input, pipeline={**state, "training_publication": publication}), {}, None)
    cases.check(
        "fixture_feast_scratch_is_separate_from_outputs",
        lambda: require(
            extract_spec.environment["FEAST_RUN_FOLDER"] == "/workspace/tmp"
            and extract_spec.environment["FEAST_REGISTRY_PATH"] == "/workspace/tmp/feast/registry.pb"
            and (publication["registry"], "/workspace/tmp/feast/registry.pb", True) in extract_spec.mounts,
            "the Feast registry or SQLite cache can enter the checked output set",
        ),
    )
    cases.check(
        "fit_has_no_mediator_or_hac_mount",
        lambda: require(
            not any(Path(host).name in ("mediator.parquet", "hac.parquet") for host, _, _ in spec.mounts), "fit received an auxiliary outcome file"
        ),
    )
    cases.check(
        "fit_has_only_snapshot_code",
        lambda: require(
            all(
                (str(run_folder / "code" / name), f"/workspace/{name}", True) in spec.mounts
                for name in ("scripts", "orchestration", "feature_store", "config", "data_contracts", "docs", "dbt")
            )
            and not any(host == str(source) for host, _, _ in spec.mounts),
            "fit mounts live code",
        ),
    )
    cases.check(
        "fit_has_no_writable_code_alias",
        lambda: require(
            not any(not readonly and (run_folder / "code").is_relative_to(Path(host)) for host, _, readonly in spec.mounts),
            "fit can mutate code through an output alias",
        ),
    )
    cases.check(
        "fit_has_no_network_or_credentials",
        lambda: require(
            spec.network == "none" and not any(key.startswith(("AWS_", "POLARIS_")) or key == "MLFLOW_TRACKING_URI" for key in spec.environment),
            "offline fit received network access or a credential",
        ),
    )
    cases.refuses("training_extra_parameter_refused", lambda: training_render.build(replace(inp, params={**inp.params, "command": "other"}), {}, None), "only")
    cases.refuses(
        "training_parameters_rebind_refused",
        lambda: training_render.build(replace(inp, params={"action": "fit", "parameters": {**PARAMETERS, "population": "sensitivity"}}), {}, None),
        "identity",
    )
    mlflow_inp = replace(inp, template="training_mlflow", params={"action": "promotion_check", "parameters": PARAMETERS})
    cases.check(
        "mlflow_client_only_on_its_network",
        lambda: require(training_render.build(mlflow_inp, {}, None).network == "isolated-mlflow", "writer did not use the isolated MLflow network"),
    )
    cases.refuses(
        "offline_action_in_mlflow_template_refused",
        lambda: training_render.build(replace(mlflow_inp, params={"action": "fit", "parameters": PARAMETERS}), {}, None),
        "not allowed",
    )
    config = launcher.Config(run_lock.Registry(folder / "registry", "test"), source, "", folder / "tokens", folder / "runs", allowed_roots=(folder,))
    client = launcher.LauncherClient(config)
    broker = Broker(BrokerPaths(config.registry, folder / "broker_state"), load_templates(), client, repo_root=source, runs_root=folder / "runs")
    request = SubmissionRequest("promotion_check", RUN, "fixture", 1, "training_dag", "promotion_check", "manual", training=PARAMETERS)
    cases.check("broker_primary_promotion_check_runs", lambda: require(not broker.row_for(request)["no_op"], "primary promotion check was skipped"))
    cases.check(
        "broker_sensitivity_never_promotes",
        lambda: require(
            broker.row_for(replace(request, training={**PARAMETERS, "population": "sensitivity"}))["no_op"], "sensitivity run can mutate a model alias"
        ),
    )
    cases.check(
        "broker_forwards_training_parameters",
        lambda: require(broker.payload_of(request, broker.row_for(request))["params"]["parameters"] == PARAMETERS, "broker dropped the parameters"),
    )
    cases.refuses("broker_aws_route_refused", lambda: broker.row_for(replace(request, training={**PARAMETERS, "mlflow": "aws"})), "not built")
    cases.refuses("broker_wrong_training_task_refused", lambda: broker.row_for(replace(request, template="gold_build")), "outside")
    guard = replace(request, template="guard", task_id="guard")
    broker.write_intent(guard, broker.row_for(guard), broker.payload_of(guard, broker.row_for(guard)))
    with mock.patch.object(broker, "advance_locked", side_effect=RuntimeError("a light case attempted external dispatch")):
        cases.refuses(
            "broker_retry_parameter_change_refused",
            lambda: broker.submit(replace(guard, training={**PARAMETERS, "population": "sensitivity"})),
            "parameters differ",
        )
    output = run_folder / "outputs/fit"
    cases.refuses("step_code_digest_refused", lambda: verify_done(output, "fit", "c" * 64), "code digest")
    (output / "unrecorded.json").write_text("unrecorded synthetic output")
    cases.refuses("step_unlisted_output_refused", lambda: verify_done(output, "fit", record["code_digest"]), "exact output set")
    for dag_id in training_contract.DAGS:
        original = graph.reference_graph(dag_id)
        broken = dict(original)
        broken.pop("snapshot_training")

        def graph_refuses(broken: dict[str, graph.TaskNode] = broken, dag_id: str = dag_id) -> None:
            require(graph.validate_transform_graph(broken, dag_id), "missing snapshot was accepted")

        cases.check(f"graph_{dag_id}_missing_snapshot_refused", graph_refuses)
    promotion = {"hai_type": "HAI_1", "model_version": "2", "promotion_mode": "promote", "mlflow": "local"}
    approved_request = SubmissionRequest("acquire_run", RUN, "real", 1, "model_promotion_dag", "acquire_run", "manual", training=promotion)
    state_folder = folder / "approval_state"
    path = state_folder / "approvals/model_promotion_dag" / f"{RUN}.json"
    path.parent.mkdir(parents=True)
    approval = {"run_id": RUN, "dag_id": "model_promotion_dag", "scope": "promote", "approved_on": "2026-10-23", "hai_type": "HAI_1", "model_version": "2"}

    def read(body: dict[str, Any], selected: SubmissionRequest = approved_request) -> dict[str, Any]:
        path.write_text(json.dumps(body))
        path.chmod(0o600)
        return training_approvals.read_approval(state_folder, selected, today=date(2026, 10, 23))

    cases.check("promotion_approval_exact", lambda: read(approval))
    for field, value in (
        ("run_id", GOLD),
        ("dag_id", "training_dag"),
        ("scope", "rollback"),
        ("hai_type", "HAI_2"),
        ("model_version", "3"),
        ("approved_on", "2027-01-01"),
    ):
        reason = (
            "date is invalid"
            if field == "approved_on"
            else "another HAI type or model version"
            if field in ("hai_type", "model_version")
            else "another run, DAG or scope"
        )
        cases.refuses(f"approval_wrong_{field}_refused", partial(read, {**approval, field: value}), reason)
    cases.refuses("approval_extra_key_refused", lambda: read({**approval, "extra": True}), "fields")
    rollback_request = replace(approved_request, training={**promotion, "promotion_mode": "rollback"})
    cases.check("rollback_uses_its_own_scope", lambda: read({**approval, "scope": "rollback"}, rollback_request))
    cases.refuses("rollback_cannot_use_promote_scope", lambda: read(approval, rollback_request), "scope")
    service_config = replace(config, root=ROOT)
    renderer = launcher.Renderer(service_config, launcher.ComposeModel(ROOT / mlflow_service.COMPOSE), ROOT, ROOT, b"synthetic-key" * 3)
    service_input = replace(
        inp,
        template=mlflow_service.TEMPLATE,
        values={"HAI_MLFLOW_STATE": str(folder / "mlflow_state"), "HAI_MLFLOW_MEMORY": str(1024**3), "HAI_MLFLOW_DB_MEMORY": str(256 * 1024**2)},
        params={"service": "mlflow"},
    )
    body = renderer.render_mlflow_service(service_input, "mlflow", REGISTRY_ID).body
    cases.check("service_closed_startup", lambda: mlflow_service.validate_bindings("mlflow", body))
    public = copy.deepcopy(body)
    public["HostConfig"]["PortBindings"]["5100/tcp"][0]["HostIp"] = "192.0.2.1"
    cases.refuses("service_public_binding_refused", lambda: mlflow_service.validate_bindings("mlflow", public), "127.0.0.1")
    database = {"HostConfig": {"PortBindings": {"5432/tcp": [{"HostIp": "127.0.0.1", "HostPort": "5432"}]}}}
    cases.refuses("service_database_binding_refused", lambda: mlflow_service.validate_bindings("mlflow_db", database), "only")
    weakened = copy.deepcopy(body)
    weakened["HostConfig"]["ReadonlyRootfs"] = False
    cases.refuses("service_writable_root_refused", lambda: mlflow_service.validate_bindings("mlflow", weakened), "hardening")


def run() -> list[dict[str, Any]]:
    """Run all bounded synthetic cases and remove their temporary working copies."""
    cases = Cases([])
    with tempfile.TemporaryDirectory(prefix="hai_training_lane_b_") as temporary:
        folder = Path(temporary)
        cases.check("orchestration_case_setup", lambda: orchestration_cases(cases, folder / "orchestration"))
        cases.check("extraction_case_setup", lambda: extraction_cases(cases, folder / "extraction"))
    return cases.results
