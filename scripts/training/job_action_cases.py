"""Synthetic E2E cases for the training job's data actions, driven through the job entry point with explicit library stand-ins."""

from __future__ import annotations

import ast
import contextlib
import copy
import hashlib
import importlib
import io
import itertools
import json
import os
import re
import shutil
import tempfile
import time
from collections.abc import Callable, Iterator, Sequence
from datetime import date
from functools import partial
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest import mock

from orchestration import training_contract
from scripts import model_code
from scripts.features import feature_schema, feature_sets
from scripts.features.training_extract import canonical, content_sha256
from scripts.training import (
    bundle,
    dvc_baseline,
    guards,
    hac_validation,
    job,
    job_actions,
    job_inputs,
    mediation,
    production_record,
    promotion,
    promotion_cases,
    promotion_check,
    registry,
    research_tests,
    run_tracking,
)
from scripts.training.dvc_baseline import HASH_TAG, PATH_TAG, SNAPSHOT_TAG
from scripts.training.job_cases import DIGEST, arguments_for, call, failure_of, listing, prepare
from scripts.training.job_failure import JobFailure
from scripts.training.lane_b_cases import RUN, Cases, require
from scripts.training.model_cases import panel_standin, request
from scripts.training.model_test_support import libraries
from scripts.training.models import panel, suite
from scripts.training.models.common import REGISTRABLE, SLOTS, Feature, NotEstimable
from scripts.training.step_records import file_sha256, verify_done, write_done

REAL = Path(__file__).resolve().parents[2]
GUARD = {"analysis_plan_sha256": "7" * 64, "targets_sha256": "8" * 64, "training_config_sha256": "9" * 64}
SAVED: dict[str, Any] = {}
CHILDREN = {"m2_elastic_net", "rq1_joint", "rq3_mediation", "hac_validation"}
HAI_TYPE = "HAI_1"
BASE_RUN = "features_5678abcd"
CHAMPION_PARENT_RUN = "training_elastic_net_aaaaaaaa"
BUNDLE = f"bundles/bundle_{HAI_TYPE}"
LABEL = "hai_1__sir__number"
SQL_TYPES = {"varchar": "VARCHAR", "double": "DOUBLE", "bigint": "BIGINT", "boolean": "BOOLEAN", "date": "DATE"}
LATE_REFERENCE = "feature_matrix_hospital:late"
FIXTURE_COLUMNS = {
    "late": "double",
    "spine_key": "varchar",
    "ccn": "varchar",
    "window_year": "bigint",
    "control_type_code": "varchar",
    "county_fips": "varchar",
    "process": "double",
    "cmi": "double",
    "extra": "double",
    LABEL: "double",
}
SCHEMA_COLUMNS = (
    (
        "feature_matrix_care_compare",
        "care_compare",
        "process",
        "double",
        {"kind": "number", "match": {"measure_control": "C119"}, "value_column": "value_number"},
    ),
    ("feature_matrix_hospital_window", "hospital_window", "cmi", "double", {"copy": "cmi"}),
    ("feature_matrix_hospital_window", "hospital_window", "control_type_code", "varchar", {"copy": "control_type_code"}),
    ("feature_matrix_hospital_window", "hospital_window", "county_fips", "varchar", {"copy": "county_fips"}),
    ("feature_matrix_hospital", "hospital", "extra", "double", {"kind": "number", "match": {"measure_control": "C001"}, "value_column": "value_number"}),
)
LATE_COLUMN = (
    "feature_matrix_hospital",
    "hospital",
    "late",
    "double",
    {"kind": "number", "match": {"measure_control": "C002"}, "value_column": "value_number"},
)
REFERENCES = tuple(f"{view}:{name}" for view, _, name, _, _ in SCHEMA_COLUMNS)
SLOT_LISTS = {
    "m1_ols": REFERENCES[:1],
    "m2_elastic_net": REFERENCES[:2] + REFERENCES[4:],
    "m3_mixed": REFERENCES[:2] + REFERENCES[4:],
    "m4_boosting": REFERENCES[:2] + REFERENCES[4:],
    "m5_spatial": REFERENCES[:2] + REFERENCES[4:],
    "m6_fixed_effects": REFERENCES[:2] + REFERENCES[4:],
    "rq4_reduced": REFERENCES[:1],
}
ADJACENCY_COLUMNS = {"edge_key": "varchar", "county_fips": "varchar", "neighbor_fips": "varchar", "is_self_link": "boolean", "is_isolated": "boolean"}
MEDIATOR_COLUMNS = {
    "spine_key": "varchar",
    "measure_control": "varchar",
    "measure": "varchar",
    "value_number": "double",
    "period_start": "date",
    "period_end": "date",
    "alignment_status": "varchar",
}
HAC_COLUMNS = {
    "spine_key": "varchar",
    "measure_control": "varchar",
    "value_number": "double",
    "value_text": "varchar",
    "fiscal_year": "bigint",
    "alignment_status": "varchar",
}


def write_parquet(path: Path, rows: list[dict[str, Any]], columns: dict[str, str]) -> None:
    """Write a typed Parquet file with DuckDB, which stands in for the training image's pyarrow in the host checks."""
    import duckdb

    names = sorted(columns)
    connection = duckdb.connect()
    connection.execute("create table t (" + ", ".join(f'"{name}" {SQL_TYPES[columns[name]]}' for name in names) + ")")
    relation = connection.table("t")
    for row in rows:
        relation.insert([row.get(name) for name in names])
    connection.execute(f"copy t to '{path}' (format parquet)")
    connection.close()


def read_parquet(path: Path) -> list[dict[str, Any]]:
    """Read a Parquet file back with DuckDB, the same stand-in as the writer."""
    import duckdb

    connection = duckdb.connect()
    cursor = connection.execute("select * from read_parquet(?)", [str(path)])
    names = [column[0] for column in cursor.description]
    rows = [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]
    connection.close()
    return rows


def table_entry(folder: Path, name: str, rows: list[dict[str, Any]], columns: dict[str, str], kind: str | None = None) -> dict[str, Any]:
    """Write one extract table and describe it the way the manifest does."""
    write_parquet(folder / name, rows, columns)
    entry = {
        "file": name,
        "sha256": file_sha256(folder / name),
        "row_count": len(rows),
        "columns": dict(sorted(columns.items())),
        "value_content_sha256": content_sha256(rows, columns),
    }
    return entry | ({"kind": kind} if kind else {})


def single_window(rows: list[dict[str, Any]]) -> None:
    """Keep each hospital in one training window, so no hospital repeats, and every hospital in the newest window."""
    rows[:] = [row for row in rows if row["window_year"] == 2023 or row["window_year"] == 2019 + int(row["ccn"]) % 4]


class Fixture:
    """One run folder with the mounted extract inputs, a small code root and the job's output folder."""

    def __init__(
        self,
        folder: Path,
        *,
        hai_types: tuple[str, ...] = (HAI_TYPE,),
        model: str = "m2",
        run: str = RUN,
        base_run: str = BASE_RUN,
        tracking: FileTracking | None = None,
        mediator: bool = True,
        hac: bool = True,
        late_mediator: bool = False,
        years: Sequence[int] = range(2019, 2024),
        hospitals: int = 48,
        period_years: int = 2,
        adjust: Callable[[list[dict[str, Any]]], None] | None = None,
        flags: dict[str, float] | None = None,
        population: str = "primary",
        late: Callable[[dict[str, Any]], Any] | None = None,
        adjust_type: dict[str, Callable[[list[dict[str, Any]]], None]] | None = None,
    ) -> None:
        self.run = run
        # Changes the rows of one type's extract only, so one type of a run can be left unestimable while another fits.
        self.adjust_type = adjust_type or {}
        self.folder = prepare(folder, run_id=run)
        self.extract = folder / "in/extract"
        self.extract.mkdir(parents=True)
        self.out = folder / "out"
        self.hai_types = hai_types
        self.model = model
        self.root = folder / "code"
        self.rows = [dict(row) for row in request(years, hospitals).data.rows]
        # A fourth predictor of the m6 list whose values come from ``late(row)``; None marks a missing value.
        self.late, self.population = late is not None, population
        self.schema_columns = SCHEMA_COLUMNS + (LATE_COLUMN,) * self.late
        self.slot_lists = {**SLOT_LISTS, "m6_fixed_effects": SLOT_LISTS["m6_fixed_effects"] + (LATE_REFERENCE,) * self.late}
        if late is not None:
            for row in self.rows:
                row["late"] = late(row)
        if adjust is not None:
            adjust(self.rows)
        self.newest, self.period_years, self.flag_values = max(row["window_year"] for row in self.rows), period_years, flags
        self.parameters = {
            "features_run": "latest",
            "population": population,
            "hai_types": list(hai_types),
            "trigger": "manual",
            "mlflow": "local",
            "model": model,
        }
        (folder / "training_parameters.json").write_text(json.dumps(self.parameters) + "\n")
        self.code_root()
        self.late_mediator = late_mediator
        self.base_run = base_run
        self.tracking = tracking if tracking is not None else FileTracking(folder / "tracking")
        self.champion: Predictor | None = None
        self.champion_loads: list[str] = []
        self.candidate: Any = None
        # What the promotion check reads: the path and cap of each fetch, the path and trusted list of each model read, and the champion's run.
        self.fetched: list[str] = []
        self.fetch_limits: dict[str, int] = {}
        self.fetch_faults: dict[str, Callable[[str, str, str, int], str]] = {}
        self.loads: list[tuple[str, tuple[str, ...]]] = []
        self.champion_run = ""
        self.write_extract(mediator=mediator, hac=hac)

    def code_root(self) -> None:
        """Copy the real target lists and bridge, and write a small model configuration, feature lists and feature schema."""
        for name in (
            "config/training/targets.json",
            "config/training/excluded_controls.json",
            "dbt/seeds/gold/hai_outcome_measures_bridge.csv",
            "data_contracts/training/extract_manifest.schema.json",
            "data_contracts/training/model_contract.json",
            "config/training/census_regions.json",
            model_code.RECORD,
            *(item["path"] for item in json.loads((REAL / model_code.RECORD).read_text())["files"]),
        ):
            (self.root / name).parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(REAL / name, self.root / name)
        rules = json.loads((REAL / "config/training/promotion.json").read_text())
        rules["bootstrap"]["draws"] = 300
        (self.root / "config/training/promotion.json").write_text(json.dumps(rules) + "\n")
        config = copy.deepcopy(dict(request().config))
        config["slots"]["m2_elastic_net"]["forced_references"] = [REFERENCES[1]]
        (self.root / "config/training/models.json").write_text(json.dumps(config) + "\n")
        (self.root / "config/training/feature_lists").mkdir(parents=True)
        for slot, references in self.slot_lists.items():
            (self.root / f"config/training/feature_lists/{slot}.json").write_text(
                json.dumps({"schema": 1, "scope": "cms_only", "slot": slot, "references": list(references)}) + "\n"
            )
        tables: dict[str, Any] = {}
        for view, family, name, kind, pivot in self.schema_columns:
            table = tables.setdefault(view, {"family": family, "role": "predictor", "columns": []})
            table["columns"].append({"name": name, "view": view, "type": kind, "role": "predictor", "pivot": pivot})
        (self.root / "data_contracts/features").mkdir(parents=True)
        (self.root / "data_contracts/features/feature_schema.json").write_text(json.dumps({"schema": 1, "tables": tables, "servable": list(REFERENCES)}) + "\n")
        hac_years = self.root / "config/training/hac_year_mapping.json"
        period = {"start": f"{self.newest - self.period_years + 1}-01-01", "end": f"{self.newest}-12-31", "psi_90": "unused"}
        mapping = {"schema": 1, "mapping": {str(self.newest): self.newest + 2}, "nhsn_periods": {str(self.newest + 2): period}}
        hac_years.write_text(json.dumps(mapping) + "\n")

    def write_extract(self, *, mediator: bool, hac: bool) -> None:
        """Write the per-type extract and manifest, and the three shared tables, the way the extract step lays them out."""
        newest = self.newest
        counties = sorted({row["county_fips"] for row in self.rows})
        adjacency = [
            {"edge_key": f"{left}:{right}", "county_fips": left, "neighbor_fips": right, "is_self_link": False, "is_isolated": False}
            for left, right in zip(counties, counties[1:], strict=False)
        ]
        previous = [row for row in self.rows if row["window_year"] == newest]
        mediators = [
            {
                "spine_key": row["spine_key"],
                "measure_control": "C300",
                "measure": measure,
                "value_number": float(index % 7) + 0.5 * (row["extra"] + (1 if measure == "PSI_90" else 2)),
                "period_start": date(row["window_year"] - 1, 1, 1),
                "period_end": date(row["window_year"], 3, 1) if self.late_mediator else date(row["window_year"] - 1, 12, 31),
                "alignment_status": "aligned",
            }
            for index, row in enumerate(self.rows)
            for measure in ("PSI_90", "PSI_13")
        ] * mediator
        values = self.flag_values if self.flag_values is not None else {row["ccn"]: float(index % 2) for index, row in enumerate(previous)}
        flags = [
            {
                "spine_key": row["spine_key"],
                "measure_control": "C400",
                "value_number": values[row["ccn"]],
                "value_text": None,
                "fiscal_year": newest + 2,
                "alignment_status": "aligned",
            }
            for row in previous
            if row["ccn"] in values
        ] * hac
        shared = {
            "mediator.parquet": table_entry(self.extract, "mediator.parquet", mediators, MEDIATOR_COLUMNS, "mediator"),
            "hac.parquet": table_entry(self.extract, "hac.parquet", flags, HAC_COLUMNS, "hac"),
            "adjacency.parquet": table_entry(self.extract, "adjacency.parquet", adjacency, ADJACENCY_COLUMNS, "adjacency"),
        }
        if self.model == "m1":
            # The m1 list leaves CMI out, so the extract supplies it in a table of its own for the intercept-plus-CMI reference.
            cmi_rows = [{"spine_key": row["spine_key"], "cmi": float(row["cmi"])} for row in self.rows]
            shared["cmi.parquet"] = table_entry(self.extract, "cmi.parquet", cmi_rows, {"spine_key": "varchar", "cmi": "double"}, "cmi")
        for hai_type in self.hai_types:
            name = f"extract_{hai_type}.parquet"
            label = f"hai_{hai_type.split('_')[1]}__sir__number"
            weight = f"hai_{hai_type.split('_')[1]}__predicted__number"
            listed = {reference.split(":", 1)[1] for slot in job_inputs.REQUEST_LISTS[self.model] for reference in self.slot_lists[slot]}
            kept = listed | {"spine_key", "ccn", "window_year", "control_type_code", "county_fips"}
            rows = [
                {key: value for key, value in row.items() if key in kept} | {label: row[LABEL], weight: 1.0 + index % 5} for index, row in enumerate(self.rows)
            ]
            if hai_type in self.adjust_type:
                self.adjust_type[hai_type](rows)
            columns = {key: kind for key, kind in FIXTURE_COLUMNS.items() if key in kept} | {label: "double", weight: "double"}
            years = [row["window_year"] for row in self.rows]
            manifest = {
                "schema_version": 1,
                "kind": "training_extract",
                "run_id": self.run,
                "hai_type": hai_type,
                "population": self.population,
                "parameters_sha256": hashlib.sha256(canonical(self.parameters)).hexdigest(),
                "code_digest": DIGEST,
                "publication_run_id": "features_elastic_net_1234abcd",
                "features_namespace": "features_e2e",
                "snapshot_set": {"feature_matrix_care_compare": 101, "feature_matrix_hospital_window": 102, "feature_matrix_hospital": 103},
                "features_snapshot_set_sha256": "1" * 64,
                "gold_run_id": "gold_1234abcd",
                "gold_snapshot_set_sha256": "2" * 64,
                "feature_schema_sha256": "3" * 64,
                "hai_outcome_controls_sha256": "4" * 64,
                "feature_list_sha256": "5" * 64,
                "model": self.model,
                "feature_service": f"model_{self.model}",
                "feature_set": self.model,
                "feature_set_schema_sha256": feature_schema.set_schema_sha256(self.model),
                "base_run": self.base_run,
                "base_snapshot_set": {"int_hospital_spine": 201, "label_matrix_hai_outcomes": 202},
                "base_snapshot_set_sha256": "7" * 64,
                "writer_lock_sha256": "6" * 64,
                **table_entry(self.extract, name, rows, columns),
                "label_column": label,
                "weight_column": weight,
                "oldest_window_year": min(years),
                "newest_window_year": max(years),
                "counts": {"spine": len(self.rows), "population_excluded": 0, "label_excluded": 0, "weight_excluded": 0, "included": len(self.rows)},
                "auxiliary_files": shared,
            }
            (self.extract / f"manifest_{hai_type}.json").write_bytes(canonical(manifest) + b"\n")

    def arguments(self, action: str, out: Path | None = None) -> list[str]:
        """Return the rendered option set for one action of this run, writing to the action folder or to a mounted input folder."""
        arguments = arguments_for(action, self.folder)
        arguments[arguments.index("--run") + 1] = self.run
        if out is not None:
            arguments[arguments.index("--run-dir") + 1] = str(out)
        return arguments

    def manifest(self, hai_type: str = HAI_TYPE) -> dict[str, Any]:
        """Return a type's manifest."""
        return dict(json.loads((self.extract / f"manifest_{hai_type}.json").read_bytes()))

    def rewrite_manifest(self, change: Callable[[dict[str, Any]], None], hai_type: str = HAI_TYPE) -> None:
        """Rewrite a type's manifest after a change."""
        manifest = self.manifest(hai_type)
        change(manifest)
        (self.extract / f"manifest_{hai_type}.json").write_bytes(canonical(manifest) + b"\n")


def tracking_fetch(fixture: Fixture, run_id: str, path: str, local_path: str, limit: int) -> str:
    """Stand in for ``model_code.mlflow_fetch``: copy one file of the run from the tracking stand-in to ``local_path`` and record the path and the cap. A file
    the run lacks raises FileNotFoundError, as the transport does."""
    fixture.fetched.append(path)
    fixture.fetch_limits[path] = limit
    if path in fixture.fetch_faults:
        return fixture.fetch_faults[path](run_id, path, local_path, limit)
    source = fixture.tracking.root / run_id / path
    if not source.is_file():
        raise FileNotFoundError(path)
    shutil.copyfile(source, local_path)
    return local_path


@contextlib.contextmanager
def standins(fixture: Fixture) -> Iterator[None]:
    """Run the job with the fixture's code root, the host Parquet reader, the tracking client stand-in and the numerical library stand-ins."""
    with (
        mock.patch.object(job_inputs, "ROOT", fixture.root),
        mock.patch.object(job_inputs, "read_rows", read_parquet),
        mock.patch.object(guards, "check", return_value=GUARD),
        mock.patch.object(bundle, "write_table", write_parquet),
        mock.patch.object(bundle, "save_model", save_model_standin),
        mock.patch.object(bundle, "SAFE_MODULES", (*bundle.SAFE_MODULES, "scripts.training.model_test_support")),
        mock.patch.object(run_tracking, "open_store", side_effect=lambda uri: fixture.tracking),
        mock.patch.object(promotion_check, "fetch_file", create=True, side_effect=partial(tracking_fetch, fixture)),
        mock.patch.dict(
            os.environ, {"MLFLOW_TRACKING_URI": "http://mlflow:5100", "DVC_REMOTE_URL": str(fixture.folder / "dvc_remote"), "DVC_NO_ANALYTICS": "1"}
        ),
        libraries(),
        mock.patch.object(panel, "native_fits", side_effect=panel_standin),
    ):
        yield


REAL_OPEN_STORE = run_tracking.open_store


class NotFound(Exception):
    """The error MLflow raises for a missing entity, with its error code."""

    error_code = "RESOURCE_DOES_NOT_EXIST"


class InvalidParameter(Exception):
    """The error the MLflow 3.17.2 client raises (``RestException``) for an HTTP 400: the code is a prefix of the message, as the client formats it."""

    error_code = "INVALID_PARAMETER_VALUE"

    def __init__(self, message: str) -> None:
        super().__init__(f"{self.error_code}: {message}")
        self.message = f"{self.error_code}: {message}"


class FileTracking:
    """A stand-in for the MLflow client: the calls the log_run action makes, with MLflow's rules for parameters, tags and run lookup."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.experiments: dict[str, str] = {}
        self.experiment_tags: dict[str, dict[str, str]] = {}
        self.runs: dict[str, dict[str, Any]] = {}
        self.calls: list[str] = []
        self.fail_on: str | None = None
        self.creates = 0
        self.lose_create: int | None = None
        self.models: dict[str, dict[str, Any]] = {}
        self.location = "mlflow-artifacts:/0"
        self.hold_alias = False
        self.pending: list[tuple[str, str, str]] = []

    def enter(self, call: str) -> None:
        """Record a call and fail it when the case asks for that failure."""
        self.calls.append(call)
        if self.fail_on == call:
            raise ConnectionError(f"synthetic tracking failure in {call}")

    def get_experiment_by_name(self, name: str) -> SimpleNamespace | None:
        self.enter("get_experiment_by_name")
        found = SimpleNamespace(experiment_id=self.experiments[name], artifact_location=self.location) if name in self.experiments else None
        return found

    def create_experiment(self, name: str) -> str:
        self.enter("create_experiment")
        self.experiments[name] = str(len(self.experiments))
        self.experiment_tags[self.experiments[name]] = {}
        return self.experiments[name]

    def get_experiment(self, experiment_id: str) -> SimpleNamespace:
        self.enter("get_experiment")
        return SimpleNamespace(experiment_id=experiment_id, tags=dict(self.experiment_tags[experiment_id]))

    def set_experiment_tag(self, experiment_id: str, key: str, value: str) -> None:
        self.enter("set_experiment_tag")
        self.experiment_tags[experiment_id][key] = value

    def delete_experiment_tag(self, experiment_id: str, key: str) -> None:
        self.enter("delete_experiment_tag")
        self.experiment_tags[experiment_id].pop(key, None)

    def create_run(self, experiment_id: str, tags: dict[str, str] | None = None, run_name: str | None = None) -> SimpleNamespace:
        self.enter("create_run")
        run_id = f"{len(self.runs):032x}"
        self.runs[run_id] = {
            "experiment": experiment_id,
            "name": run_name,
            "status": "RUNNING",
            "start_time": len(self.runs),
            "deleted": False,
            "params": {},
            "metrics": {},
            "tags": dict(tags or {}),
        }
        self.creates += 1
        if self.lose_create == self.creates:
            raise ConnectionError("synthetic lost response after the run was created")
        return SimpleNamespace(info=SimpleNamespace(run_id=run_id))

    def search_runs(self, experiment_ids: list[str], filter_string: str = "", run_view_type: int = 1, max_results: int = 1000) -> list[SimpleNamespace]:
        self.enter("search_runs")
        wanted = dict(re.findall(r"tags\.(\w+) = '([^']*)'", filter_string))
        found = [
            run_id
            for run_id, run in self.runs.items()
            if run["experiment"] in experiment_ids
            and (run_view_type == 3 or not run["deleted"])
            and all(run["tags"].get(key) == value for key, value in wanted.items())
        ]
        return [self.get_run(run_id) for run_id in found[:max_results]]

    def log_param(self, run_id: str, key: str, value: Any) -> None:
        self.enter("log_param")
        params = self.runs[run_id]["params"]
        if key in params and params[key] != str(value):
            raise ValueError(f"param {key} already holds another value")
        params[key] = str(value)

    def log_metric(self, run_id: str, key: str, value: float, timestamp: int | None = None, step: int | None = None) -> None:
        self.enter("log_metric")
        self.runs[run_id]["metrics"].setdefault(key, []).append(float(value))

    def set_tag(self, run_id: str, key: str, value: str) -> None:
        self.enter("set_tag")
        self.runs[run_id]["tags"][key] = str(value)

    def log_artifact(self, run_id: str, local_path: str, artifact_path: str | None = None) -> None:
        self.enter("log_artifact")
        target = self.root / run_id / (artifact_path or "")
        target.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(local_path, target / Path(local_path).name)

    def log_artifacts(self, run_id: str, local_dir: str, artifact_path: str | None = None) -> None:
        self.enter("log_artifacts")
        shutil.copytree(local_dir, self.root / run_id / (artifact_path or ""), dirs_exist_ok=True)

    def get_registered_model(self, name: str) -> SimpleNamespace:
        self.enter("get_registered_model")
        if name not in self.models:
            raise NotFound(name)
        return SimpleNamespace(name=name, tags=dict(self.models[name]["tags"]))

    def create_registered_model(self, name: str) -> SimpleNamespace:
        self.enter("create_registered_model")
        self.models[name] = {"tags": {}, "versions": [], "aliases": {}}
        return self.get_registered_model(name)

    def search_model_versions(self, filter_string: str = "") -> list[SimpleNamespace]:
        self.enter("search_model_versions")
        name = re.findall(r"name='([^']*)'", filter_string)[0]
        return [self.version_of(name, item) for item in self.models.get(name, {"versions": []})["versions"]]

    def version_of(self, name: str, item: dict[str, Any]) -> SimpleNamespace:
        return SimpleNamespace(name=name, version=item["version"], run_id=item["run_id"], tags=dict(item["tags"]))

    def create_model_version(self, name: str, source: str, run_id: str | None = None, tags: dict[str, str] | None = None) -> SimpleNamespace:
        self.enter("create_model_version")
        versions = self.models[name]["versions"]
        versions.append({"version": str(len(versions) + 1), "run_id": run_id, "source": source, "tags": dict(tags or {})})
        return self.version_of(name, versions[-1])

    def find_version(self, name: str, version: str) -> dict[str, Any]:
        return next(item for item in self.models[name]["versions"] if item["version"] == version)

    def set_model_version_tag(self, name: str, version: str, key: str, value: str) -> None:
        self.enter("set_model_version_tag")
        self.find_version(name, version)["tags"][key] = value

    def set_registered_model_alias(self, name: str, alias: str, version: str) -> None:
        self.enter("set_registered_model_alias")
        if self.hold_alias:
            self.hold_alias = False
            self.pending.append((name, alias, version))
            raise ConnectionError("synthetic read timeout; the server accepted the alias request and has not committed it")
        self.models[name]["aliases"][alias] = version

    def deliver_pending(self) -> None:
        """Commit the alias requests the server accepted earlier, the way a request handler that outlived its caller does."""
        for name, alias, version in self.pending:
            self.models[name]["aliases"][alias] = version
        self.pending.clear()

    def drop_pending(self) -> None:
        """End the server process: a request it accepted and did not commit never commits."""
        self.pending.clear()

    def get_model_version_by_alias(self, name: str, alias: str) -> SimpleNamespace:
        self.enter("get_model_version_by_alias")
        if name not in self.models:
            raise NotFound(f"Registered Model with name={name} not found")
        if alias not in self.models[name]["aliases"]:
            raise InvalidParameter(f"Registered model alias {alias} not found.")
        return self.version_of(name, self.find_version(name, self.models[name]["aliases"][alias]))

    def set_registered_model_tag(self, name: str, key: str, value: str) -> None:
        self.enter("set_registered_model_tag")
        self.models[name]["tags"][key] = value

    def delete_registered_model_tag(self, name: str, key: str) -> None:
        self.enter("delete_registered_model_tag")
        self.models[name]["tags"].pop(key, None)

    def active(self) -> dict[str, dict[str, Any]]:
        """Return the runs that are not deleted."""
        return {run_id: run for run_id, run in self.runs.items() if not run["deleted"]}

    def parents(self) -> dict[str, dict[str, Any]]:
        """Return the active parent runs."""
        return {run_id: run for run_id, run in self.active().items() if run["tags"].get("step_key") == "parent"}

    def set_terminated(self, run_id: str, status: str = "FINISHED") -> None:
        self.enter("set_terminated")
        self.runs[run_id]["status"] = status

    def get_run(self, run_id: str) -> SimpleNamespace:
        run = self.runs[run_id]
        data = SimpleNamespace(params=dict(run["params"]), metrics={key: values[-1] for key, values in run["metrics"].items()}, tags=dict(run["tags"]))
        return SimpleNamespace(info=SimpleNamespace(run_id=run_id, status=run["status"], start_time=run["start_time"]), data=data)

    def download_artifacts(self, run_id: str, path: str, dst_path: str | None = None) -> str:
        source = self.root / run_id / path
        if not source.is_file():
            raise FileNotFoundError(path)
        target = Path(dst_path or tempfile.mkdtemp()) / source.name
        shutil.copyfile(source, target)
        return str(target)

    def artifacts(self, run_id: str) -> list[str]:
        base = self.root / run_id
        return sorted(path.relative_to(base).as_posix() for path in base.rglob("*") if path.is_file()) if base.is_dir() else []


def stage_baseline(fixture: Fixture) -> dict[str, Any]:
    """Write the mounted outputs of version_extract the way that action records them: a metafile, a baseline record and the completion record."""
    folder = fixture.folder / "in/version_extract"
    folder.mkdir(parents=True)
    records = {}
    for hai_type in fixture.hai_types:
        matrix = f"extract_{hai_type}.parquet"
        metafile = f"{matrix}.dvc"
        (folder / metafile).write_text(f"outs:\n- md5: {'a' * 32}\n  size: 1\n  path: {matrix}\n")
        record = {"hai_type": hai_type, "matrix_path": f"data/training/{RUN}/{matrix}", "metafile": metafile, "metafile_sha256": file_sha256(folder / metafile)}
        (folder / f"baseline_{hai_type}.json").write_text(json.dumps(record, sort_keys=True) + "\n")
        records[hai_type] = record
    write_done(folder, "version_extract", DIGEST, sorted(path.name for path in folder.iterdir()))
    return records


def save_model_standin(pipeline: Any, folder: Path, features: Any, contract: Any, code_root: Path) -> None:
    """Stand in for mlflow.sklearn.save_model: write a token for the pipeline and the bare feature columns the signature would list."""
    folder.mkdir(parents=True)
    SAVED[str(id(pipeline))] = pipeline
    (folder / "model.skops").write_text(str(id(pipeline)))
    (folder / "MLmodel").write_text(json.dumps({"columns": [feature.name for feature in features], "format": contract["serialization"]["format"]}) + "\n")


def run_action(fixture: Fixture, action: str, out: Path | None = None) -> tuple[int, str, str]:
    """Run one action of the fixture's run through the job entry point."""
    with standins(fixture):
        return call(fixture.arguments(action, out))


def stage(fixture: Fixture, *actions: str) -> None:
    """Run earlier actions into the input folders that a later action mounts."""
    for action in actions:
        folder = fixture.folder / "in" / action
        folder.mkdir(parents=True, exist_ok=True)
        code, _, error = run_action(fixture, action, folder)
        require(code == 0, f"staging {action} failed: {error}")


def output_of(fixture: Fixture, name: str) -> Any:
    """Read one JSON output of the action folder."""
    return json.loads((fixture.out / name).read_text())


def fit_cases(cases: Any, folder: Path) -> None:
    """Fit the six-model suite on a fixture extract, then check the retry, the refusals and the deadline."""

    def fitted() -> None:
        fixture = Fixture(folder / "fitted")
        code, out, error = run_action(fixture, "fit")
        require(code == 0, f"fit failed: {error}")
        names = listing(fixture.out)
        required = [f"{BUNDLE}/{name}" for name in ("result.json", "provenance.json", "holdout_predictions.parquet", "feature_list.json", "shap.json")]
        require(all(name in names for name in [*required, "fit.done.json"]), f"missing outputs: {names}")
        require(any(name.startswith(f"{BUNDLE}/model/") for name in names), "the registry candidate was not saved")
        require(
            all(name.startswith(f"{BUNDLE}/") or name in ("fit.done.json", f"hac_predictions_{HAI_TYPE}.json") for name in names),
            f"files outside the bundle: {names}",
        )
        result = output_of(fixture, f"{BUNDLE}/result.json")
        contract = json.loads((REAL / "data_contracts/training/bundle.json").read_text())
        require(set(result) == set(contract["result_fields"]), f"result.json fields differ from the bundle contract: {sorted(result)}")
        require(set(result["statuses"]) == set(SLOTS) and result["statuses"]["m2_elastic_net"]["status"] == "fitted", f"statuses: {result['statuses']}")
        manifest = fixture.manifest()
        require(result["input_hashes"]["extract_sha256"] == manifest["sha256"], "result.json does not record the extract it read")
        provenance = output_of(fixture, f"{BUNDLE}/provenance.json")
        require(set(contract["provenance_fields"]) <= set(provenance) and provenance["run_id"] == RUN, f"provenance: {sorted(provenance)}")
        require("model_code_closure_sha256" in contract["provenance_fields"], f"bundle.json provenance_fields: {contract['provenance_fields']}")
        require("self_check.json" in contract["optional_files"], f"bundle.json optional_files: {contract['optional_files']}")
        require(
            provenance.get("model_code_closure_sha256") == model_code.closure_digest(fixture.root) == model_code.closure_digest(REAL),
            f"provenance closure value: {provenance.get('model_code_closure_sha256')}",
        )
        require("run_id" not in json.dumps(result), "result.json carries a run-specific fact")
        require(
            provenance["inputs"]["auxiliary_files"]["hac.parquet"]["sha256"] == manifest["auxiliary_files"]["hac.parquet"]["sha256"],
            "provenance lacks the HAC table hash",
        )
        table = read_parquet(fixture.out / f"{BUNDLE}/holdout_predictions.parquet")
        require(set(table[0]) == {"spine_key", "observed_sir", *(f"predicted_{slot}" for slot in SLOTS)}, f"holdout columns: {sorted(table[0])}")
        require([row["spine_key"] for row in table] == sorted(row["spine_key"] for row in table), "holdout rows are not sorted by key")
        feature_list = output_of(fixture, f"{BUNDLE}/feature_list.json")
        require(feature_list["slot"] == result["selection"]["registry_candidate"] and feature_list["columns"], f"feature list: {feature_list}")
        verify_done(fixture.out, "fit", DIGEST)
        require(json.loads(out)["action"] == "fit" and json.loads(out)["resumed"] is False, "fit printed no summary")

    def reproducible() -> None:
        first, second = Fixture(folder / "repeat_a"), Fixture(folder / "repeat_b")
        for fixture in (first, second):
            require(run_action(fixture, "fit")[0] == 0, "a fit failed")
        name = f"{BUNDLE}/result.json"
        require((first.out / name).read_bytes() == (second.out / name).read_bytes(), "two fits of one input gave different result.json bytes")

    def holdout_row_of(fixture: Fixture, references: Sequence[str], cells: Sequence[Any]) -> dict[str, Any]:
        """Return the one newest-window fixture row whose predictors are these cells, so a saved row can be traced back to its key."""
        names = [reference.split(":", 1)[-1] for reference in references]
        found = [row for row in fixture.rows if row["window_year"] == fixture.newest and [row[name] for name in names] == list(cells)]
        require(len(found) == 1, f"{len(found)} holdout rows match the saved cells {cells}")
        return found[0]

    def self_check_written() -> None:
        """The saved candidate has a self-check file: the design's keys, 8 rows of feature values in feature-list order, the candidate's own holdout
        predictions, no identifier, year or label value, and the same bytes from the upload list, the done record and the contract."""
        fixture = Fixture(folder / "self_check")
        require(run_action(fixture, "fit")[0] == 0, "fit failed")
        path = fixture.out / f"{BUNDLE}/self_check.json"
        require(path.is_file(), f"no self_check.json: {listing(fixture.out)}")
        raw = path.read_bytes()
        document = json.loads(raw)
        feature_list = output_of(fixture, f"{BUNDLE}/feature_list.json")
        require(set(document) == {"schema", "slot", "references", "rows", "predictions"}, f"self_check keys: {sorted(document)}")
        require(
            document["schema"] == 1 and document["slot"] == feature_list["slot"] == "m2_elastic_net", f"schema or slot: {document['schema']} {document['slot']}"
        )
        require(document["references"] == feature_list["references"], "references differ from feature_list.json")
        reader = importlib.import_module("scripts.serving.mlflow_loader")
        references, _ = reader.check_feature_list(feature_list)
        reader.check_provenance_shape(output_of(fixture, f"{BUNDLE}/provenance.json"))
        saved_rows, saved_predictions = reader.check_self_check(document, references)
        require(len(saved_rows) == len(saved_predictions) == 8, "the serving reader reads the three files this writer wrote, unchanged")
        require(len(document["rows"]) == len(document["predictions"]) == 8, f"rows {len(document['rows'])}, predictions {len(document['predictions'])}")
        width = len(feature_list["references"])
        require(all(len(row) == width for row in document["rows"]), f"a row is not {width} cells wide")
        require(raw == bundle.canonical(document), "self_check.json is not canonical JSON")
        table = {row["spine_key"]: row["predicted_m2_elastic_net"] for row in read_parquet(fixture.out / f"{BUNDLE}/holdout_predictions.parquet")}
        keys = [holdout_row_of(fixture, document["references"], row)["spine_key"] for row in document["rows"]]
        require(len(set(keys)) == 8 and keys == sorted(keys), f"the saved rows are not 8 distinct holdout rows in key order: {keys}")
        require(document["predictions"] == [table[key] for key in keys], "the saved predictions are not the candidate's holdout predictions")
        text = raw.decode()
        leaked = [word for word in ("spine_key", "ccn", "window_year", "observed_sir", LABEL) if word in text]
        leaked += [value for row in fixture.rows for value in (row["spine_key"], row["ccn"]) if value in text]
        require(not leaked, f"identifiers in self_check.json: {leaked[:3]}")
        require(f"{BUNDLE}/self_check.json" in verify_done(fixture.out, "fit", DIGEST)["outputs"], "fit.done.json does not list self_check.json")

    def self_check_keeps_bundle_hash() -> None:
        """The new file is not an identity file: the bundle hash is the hash of the other files' digests, and a refit gives the same file bytes."""
        first, second = Fixture(folder / "hash_a"), Fixture(folder / "hash_b")
        for fixture in (first, second):
            require(run_action(fixture, "fit")[0] == 0, "a fit failed")
        name = f"{BUNDLE}/self_check.json"
        require((first.out / name).read_bytes() == (second.out / name).read_bytes(), "two fits of one input gave different self_check.json bytes")
        done = verify_done(first.out, "fit", DIGEST)["outputs"]
        require(name in done and "self_check.json" not in bundle.IDENTITY_FILES, "self_check.json is an identity file or not in the done record")
        without = {key: value for key, value in done.items() if key != name}
        require(bundle.sha256_of(done, HAI_TYPE) == bundle.sha256_of(without, HAI_TYPE), "self_check.json changed the bundle hash")

    def self_check_encodes_missing_values() -> None:
        """A held-out row with missing predictors is saved with null in those cells, and the two rows with the most missing values join the first six."""

        def blank(rows: list[dict[str, Any]]) -> None:
            newest = [row for row in rows if row["window_year"] == max(item["window_year"] for item in rows)]
            newest.sort(key=lambda row: row["spine_key"])
            newest[-1]["process"] = newest[-1]["cmi"] = None
            newest[-2]["process"] = None

        fixture = Fixture(folder / "self_check_missing", adjust=blank)
        code, _, error = run_action(fixture, "fit")
        require(code == 0, f"fit failed: {error}")
        document = output_of(fixture, f"{BUNDLE}/self_check.json")
        names = [reference.split(":", 1)[-1] for reference in document["references"]]
        rows = [holdout_row_of(fixture, document["references"], row) for row in document["rows"]]
        newest = sorted(row["spine_key"] for row in fixture.rows if row["window_year"] == fixture.newest)
        require(
            [row["spine_key"] for row in rows][-2:] == [newest[-2], newest[-1]],
            f"the rows with most missing values were not chosen: {[row['spine_key'] for row in rows]}",
        )
        require([row["spine_key"] for row in rows][:6] == newest[:6], "the first six rows by key were not chosen")
        for saved, row in zip(document["rows"], rows, strict=True):
            require(saved == [row[name] for name in names], f"a saved row differs from its fixture row: {saved}")
        cells = {key: [saved[names.index(key)] for saved in document["rows"]] for key in ("process", "cmi")}
        require(cells["process"][-2:] == [None, None] and cells["cmi"][-1] is None, f"missing values are not null: {cells}")
        require("NaN" not in (fixture.out / f"{BUNDLE}/self_check.json").read_text(), "a NaN token in self_check.json")

    def self_check_for_a_small_holdout() -> None:
        """A holdout with fewer than 8 predicted rows saves every row it has, and one with a single row saves that row. The fit refuses a holdout under
        the configured minimum, so this reaches the writer's own rule with a suite stand-in."""
        features = (Feature("view:process", "number"), Feature("view:kind", "category"))
        for count in (3, 1):
            rows = tuple({"spine_key": f"k{index}", "process": float(index) if index else None, "kind": index} for index in range(count))
            holdout = SimpleNamespace(rows=rows, keys=tuple(row["spine_key"] for row in rows))
            result = SimpleNamespace(holdout_positions=tuple(range(count)), predictions=tuple(0.5 + index for index in range(count)))
            standin = SimpleNamespace(
                candidate="m2_elastic_net",
                slots={"m2_elastic_net": result},
                request=SimpleNamespace(data=SimpleNamespace(split=lambda holdout=holdout: (None, holdout))),
            )
            document = bundle.self_check_document(standin, features)
            require(document["rows"] == [[None if index == 0 else float(index), str(index)] for index in range(count)], f"{count} rows: {document['rows']}")
            require(document["predictions"] == [0.5 + index for index in range(count)], f"{count} rows: predictions {document['predictions']}")

    def self_check_refuses_a_category_the_serving_reader_refuses() -> None:
        """The serving reader refuses a text cell of more than 256 characters, so the writer refuses it first, by name, and never truncates: a category of 256
        characters is written and read back, one of 257 stops the fit, and so does a category whose text is longer (a number saved in a category column)."""
        reader = importlib.import_module("scripts.serving.mlflow_loader")
        features = (Feature("view:kind", "category"),)

        def document_for(category: Any) -> dict[str, Any]:
            rows = ({"spine_key": "k0", "kind": category},)
            standin = SimpleNamespace(
                candidate="m2_elastic_net",
                slots={"m2_elastic_net": SimpleNamespace(holdout_positions=(0,), predictions=(0.5,))},
                request=SimpleNamespace(data=SimpleNamespace(split=lambda: (None, SimpleNamespace(rows=rows, keys=("k0",))))),
            )
            return bundle.self_check_document(standin, features)

        saved = document_for("x" * 256)
        require(saved["rows"] == [["x" * 256]], "a category of 256 characters was changed")
        require(reader.check_self_check(saved, ("view:kind",)) == ([["x" * 256]], [0.5]), "the serving reader refused a category of 256 characters")
        for label, category in (("257 characters", "x" * 257), ("a number of 301 digits", 10**300)):
            try:
                document_for(category)
            except JobFailure as failure:
                require(failure.name == "self_check_category_too_long", f"{label}: refused as {failure.name}")
                require("x" * 20 not in failure.detail, f"{label}: the refusal repeats the value: {failure.detail}")
            else:
                raise AssertionError(f"a category of {label} was written to the self-check")
        require(
            bundle.MAX_CATEGORY_TEXT == reader.MAX_TEXT == 256, f"the writer bound {bundle.MAX_CATEGORY_TEXT} and the reader bound {reader.MAX_TEXT} differ"
        )

    def self_check_only_for_a_candidate() -> None:
        """A model that cannot be registered saves no model folder and no self-check file; a registrable one saves both."""
        for model in ("m1", "m3"):
            fixture = Fixture(folder / f"self_check_{model}", model=model)
            require(run_action(fixture, "fit")[0] == 0, f"fit of {model} failed")
            saved = (fixture.out / f"{BUNDLE}/self_check.json").is_file()
            require(
                saved == (model in training_contract.PROMOTION_MODELS) == (fixture.out / f"{BUNDLE}/model").is_dir(), f"{model}: self_check saved is {saved}"
            )

    def retried() -> None:
        fixture = Fixture(folder / "retried")
        require(run_action(fixture, "fit")[0] == 0, "the first fit failed")
        before = {name: (fixture.out / name).read_bytes() for name in listing(fixture.out)}
        with mock.patch.object(suite, "fit_model", side_effect=AssertionError("a completed fit must not run again")):
            code, out, error = run_action(fixture, "fit")
        require(code == 0 and json.loads(out)["resumed"] is True, f"the retry did not resume: {code} {error}")
        require(before == {name: (fixture.out / name).read_bytes() for name in listing(fixture.out)}, "a retry changed a completed output")

    def interrupted() -> None:
        fixture = Fixture(folder / "interrupted")
        require(run_action(fixture, "fit")[0] == 0, "the first fit failed")
        first = {name: (fixture.out / name).read_bytes() for name in listing(fixture.out) if name.endswith("result.json")}
        (fixture.out / "fit.done.json").unlink()
        code, _, error = run_action(fixture, "fit")
        require(code == 0 and verify_done(fixture.out, "fit", DIGEST)["step"] == "fit", f"the retry after an interrupt failed: {error}")
        require(first == {name: (fixture.out / name).read_bytes() for name in first}, "a retry after an interrupt changed a result")

    def conflict() -> None:
        fixture = Fixture(folder / "conflict")
        require(run_action(fixture, "fit")[0] == 0, "the first fit failed")
        (fixture.out / f"{BUNDLE}/result.json").write_text("{}\n")
        code, _, error = run_action(fixture, "fit")
        require(code == job.EXIT_FAILED and failure_of(error) == "done_conflict", f"{code} {error}")

    def stray() -> None:
        fixture = Fixture(folder / "stray")
        (fixture.out / "other.txt").write_text("left by another step\n")
        code, _, error = run_action(fixture, "fit")
        require(code == job.EXIT_FAILED and failure_of(error) == "run_dir_not_clean", f"{code} {error}")
        require(listing(fixture.out) == ["other.txt"], "a refused fit changed the output folder")

    def changed_extract() -> None:
        fixture = Fixture(folder / "changed_extract")
        with (fixture.extract / f"extract_{HAI_TYPE}.parquet").open("ab") as handle:
            handle.write(b"x")
        code, _, error = run_action(fixture, "fit")
        require(code == job.EXIT_FAILED and failure_of(error) == "extract_changed", f"{code} {error}")
        require(listing(fixture.out) == [], "a refused fit wrote output")

    def wrong_run() -> None:
        fixture = Fixture(folder / "wrong_run")
        fixture.rewrite_manifest(lambda manifest: manifest.update(run_id="training_elastic_net_ffffffff"))
        code, _, error = run_action(fixture, "fit")
        require(code == job.EXIT_FAILED and failure_of(error) == "extract_manifest", f"{code} {error}")

    def wrong_type() -> None:
        fixture = Fixture(folder / "wrong_type")
        fixture.rewrite_manifest(lambda manifest: manifest.update(hai_type="HAI_2"))
        code, _, error = run_action(fixture, "fit")
        require(code == job.EXIT_FAILED and failure_of(error) == "extract_manifest", f"{code} {error}")

    def missing_input() -> None:
        fixture = Fixture(folder / "missing_input")
        (fixture.extract / f"manifest_{HAI_TYPE}.json").unlink()
        code, _, error = run_action(fixture, "fit")
        require(code == job.EXIT_FAILED and failure_of(error) == "input_missing", f"{code} {error}")

    def aws_lane_accepted() -> None:
        fixture = Fixture(folder / "aws_lane")
        (fixture.folder / "training_parameters.json").write_text(json.dumps(fixture.parameters | {"mlflow": "aws"}) + "\n")
        code, _, error = run_action(fixture, "fit")
        require(code == 0, f"a job of the AWS lane was refused: {error}")
        require(f"{BUNDLE}/result.json" in listing(fixture.out), "an AWS lane fit wrote no result")

    def parameters_refused() -> None:
        fixture = Fixture(folder / "parameters")
        (fixture.folder / "training_parameters.json").write_text(json.dumps(fixture.parameters | {"mlflow": "gcp"}) + "\n")
        code, _, error = run_action(fixture, "fit")
        require(code == job.EXIT_FAILED and failure_of(error) == "parameters" and "mlflow must be" in error, f"{code} {error}")

    def targets_refused() -> None:
        fixture = Fixture(folder / "targets")
        path = fixture.root / "config/training/targets.json"
        path.write_text(path.read_text().replace('"C269"', '"C999"', 1))
        code, _, error = run_action(fixture, "fit")
        require(code == job.EXIT_FAILED and failure_of(error) == "targets_refused", f"{code} {error}")
        require(listing(fixture.out) == [], "a fit with unchecked targets wrote output")

    def too_few_rows() -> None:
        fixture = Fixture(folder / "few_rows")
        fixture.rows = fixture.rows[:40]
        fixture.write_extract(mediator=True, hac=True)
        code, _, error = run_action(fixture, "fit")
        require(code == job.EXIT_FAILED and failure_of(error) == "fit_refused" and "insufficient split rows" in error, f"{code} {error}")
        require(listing(fixture.out) == [], "a refused fit left output")

    def deadline() -> None:
        fixture = Fixture(folder / "deadline")
        with mock.patch.object(job, "budget", return_value=0.3), mock.patch.object(suite, "fit_model", side_effect=lambda *_: time.sleep(5)):
            code, _, error = run_action(fixture, "fit")
        require(code == job.EXIT_DEADLINE and failure_of(error) == "deadline", f"{code} {error}")
        require(listing(fixture.out) == [], "a deadline left output")

    def model_deadline() -> None:
        fixture = Fixture(folder / "model_deadline")
        config = json.loads((fixture.root / "config/training/models.json").read_text())
        config["budgets"]["fit_seconds"] = 1e-9
        (fixture.root / "config/training/models.json").write_text(json.dumps(config) + "\n")
        code, _, error = run_action(fixture, "fit")
        require(code == job.EXIT_DEADLINE and failure_of(error) == "deadline", f"{code} {error}")

    def two_types() -> None:
        fixture = Fixture(folder / "types", hai_types=("HAI_1", "HAI_2"))
        code, _, error = run_action(fixture, "fit")
        require(code == 0, f"fit failed: {error}")
        names = listing(fixture.out)
        require(all(f"bundles/bundle_{kind}/result.json" in names for kind in fixture.hai_types), f"missing outputs: {names}")

    def second_type_missing() -> None:
        fixture = Fixture(folder / "second_missing", hai_types=("HAI_1", "HAI_2"))
        (fixture.extract / "manifest_HAI_2.json").unlink()
        code, _, error = run_action(fixture, "fit")
        require(code == job.EXIT_FAILED and failure_of(error) == "input_missing", f"{code} {error}")
        require(listing(fixture.out) == [], "a failed type left partial output")

    def trust_list_covers_candidates() -> None:
        held: set[str] = set()
        for model in training_contract.PROMOTION_MODELS:
            fixture = Fixture(folder / f"trust_list_{model}", model=model)
            seen: list[Any] = []
            build = bundle.build

            def spy(model_suite: Any, *rest: Any, build: Any = build, seen: list[Any] = seen) -> Any:
                seen.append(model_suite)
                return build(model_suite, *rest)

            with mock.patch.object(bundle, "build", side_effect=spy):
                code, _, error = run_action(fixture, "fit")
            require(code == 0 and seen, f"fit of {model} failed: {error}")
            held |= {
                name
                for slot in REGISTRABLE
                if seen[0].slots[slot].status == "fitted"
                for name in bundle.pipeline_types(seen[0].slots[slot].pipeline)
                if name.startswith("scripts.training.models.")
            }
        contract = json.loads((REAL / "data_contracts/training/model_contract.json").read_text())
        listed = {name for name in contract["serialization"]["trusted_types"] if name.startswith("scripts.")}
        require(
            listed == held, f"trust list differs from the project types the candidates hold: missing {sorted(held - listed)}, extra {sorted(listed - held)}"
        )
        for name in listed:
            module, _, attribute = name.rpartition(".")
            tree = ast.parse((REAL / f"{module.replace('.', '/')}.py").read_text())
            require(any(isinstance(node, ast.ClassDef) and node.name == attribute for node in tree.body), f"trusted type {name} is not defined")

    def untrusted_type_refused() -> None:
        fixture = Fixture(folder / "untrusted")
        path = fixture.root / "data_contracts/training/model_contract.json"
        contract = json.loads(path.read_text())
        contract["serialization"]["trusted_types"] = [name for name in contract["serialization"]["trusted_types"] if not name.endswith(".FoldPreprocessor")]
        path.write_text(json.dumps(contract) + "\n")
        code, _, error = run_action(fixture, "fit")
        require(code == job.EXIT_FAILED and failure_of(error) == "untrusted_model_type" and "FoldPreprocessor" in error, f"{code} {error}")
        require(listing(fixture.out) == [], "a fit with an untrusted candidate wrote output")

    def serialized_state_trusted() -> None:
        """XGBoost's Booster keeps a ctypes handle in __dict__ and replaces it with raw bytes in __getstate__, which skops saves; only the saved state
        counts. Training run 6 stopped in fit when the check walked __dict__ instead."""

        class NativeHandle:
            """Stands in for the ctypes.c_void_p handle of a live Booster."""

        class HandleBooster:
            def __init__(self) -> None:
                self.handle = NativeHandle()

            def __getstate__(self) -> dict[str, Any]:
                return {**self.__dict__, "handle": bytearray(b"raw model")}

        class HandleRegressor:
            def __init__(self) -> None:
                self.booster = HandleBooster()

        regressor = HandleRegressor()
        require(isinstance(regressor.booster.handle, NativeHandle), "the stand-in does not hold its handle as an attribute")
        held = bundle.pipeline_types(regressor)
        require(not any(name.endswith("NativeHandle") for name in held), f"the trust check read an attribute the saved state replaces: {sorted(held)}")
        require(any(name.endswith("HandleBooster") for name in held), f"the trust check did not reach the object the saved state holds: {sorted(held)}")

    OWN_TESTS = {"m1": {"joint_process"}, "m2": {"process_vs_intercept_cmi"}, "m3": {"ownership"}, "m4": set(), "m5": {"spatial"}, "m6": {"hospital_effects"}}
    OWN_SIDES = {"m1": {"rq4_reduced"}, "m2": {"rq4_full"}}

    def one_model_per_run() -> None:
        for model in training_contract.MODELS:
            fixture = Fixture(folder / f"one_{model}", model=model)
            code, _, error = run_action(fixture, "fit")
            require(code == 0, f"fit of {model} failed: {error}")
            result = output_of(fixture, f"{BUNDLE}/result.json")
            own = training_contract.MODEL_LISTS[model][0]
            others = {slot: status for slot, status in result["statuses"].items() if slot != own}
            require(
                all(status == {"reason": suite.NOT_IN_RUN, "status": "not_run"} for status in others.values()),
                f"{model} ran or reported another model: {others}",
            )
            require(result["statuses"][own]["reason"] != suite.NOT_IN_RUN, f"{model} did not run its own model: {result['statuses'][own]}")
            require(set(result["test_statistics"]) == OWN_TESTS[model], f"{model} ran tests it does not own: {sorted(result['test_statistics'])}")
            sides = fixture.out / f"{BUNDLE}/rq4_side.json"
            require(sides.is_file() == (model in OWN_SIDES), f"{model} wrote the RQ4 side file wrongly")
            if model in OWN_SIDES:
                require(set(json.loads(sides.read_text())) == OWN_SIDES[model], f"{model} wrote the wrong RQ4 side")

    def candidate_only_for_promotion_models() -> None:
        for model in ("m1", "m3"):
            fixture = Fixture(folder / f"candidate_{model}", model=model)
            require(run_action(fixture, "fit")[0] == 0, f"fit of {model} failed")
            candidate = output_of(fixture, f"{BUNDLE}/result.json")["selection"]["registry_candidate"]
            require((candidate is not None) == (model in training_contract.PROMOTION_MODELS), f"{model} registry candidate is {candidate}")

    def feature_list_of_every_model_and_population() -> None:
        """The artifact ``job fit`` emits, for each model under both populations: the registry keys keep their meaning (only a promotion model has a slot,
        references and columns) and the kept list is the fitted model's, registrable or not. Spatial significance is a declared stand-in."""
        significant = {"status": "fitted", "p_value": 0.01, "statistic": 0.0, "islands": 0}
        for model in training_contract.MODELS:
            for population in ("primary", "sensitivity"):
                fixture = Fixture(folder / f"kept_{model}_{population}", model=model)
                fixture.rewrite_manifest(lambda manifest, population=population: manifest.update(population=population))
                with mock.patch.object(suite, "moran_test", return_value=significant):
                    code, _, error = run_action(fixture, "fit")
                label = f"{model} {population}"
                require(code == 0, f"fit of {label} failed: {error}")
                result, listing = output_of(fixture, f"{BUNDLE}/result.json"), output_of(fixture, f"{BUNDLE}/feature_list.json")
                slot = training_contract.MODEL_LISTS[model][0]
                require(result["statuses"][slot]["status"] == "fitted", f"{label} did not fit: {result['statuses'][slot]}")
                used = result["fitted_parameters"][slot]["used_references"]
                expected = [reference for reference in SLOT_LISTS[slot] if reference in used]
                require(
                    expected and listing["kept_references"] == expected,
                    f"{label}: kept references {listing['kept_references']}, the fitted design's are {expected}",
                )
                require(listing["kept_columns"] == [reference.split(":", 1)[-1] for reference in expected], f"{label}: kept columns {listing['kept_columns']}")
                require(listing["fitted_slot"] == slot, f"{label}: fitted_slot is {listing['fitted_slot']}")
                promotion = model in training_contract.PROMOTION_MODELS
                keys = (listing["slot"], listing["references"], listing["columns"])
                wanted = (slot, list(SLOT_LISTS[slot]), [reference.split(":", 1)[-1] for reference in SLOT_LISTS[slot]]) if promotion else (None, [], [])
                require(keys == wanted, f"{label}: the registry keys are {keys}, expected {wanted}")

    def feature_list_of_m6_follows_the_panel() -> None:
        """The kept list of m6 names the sources of the fitted panel's columns. A predictor the selection drops for its missing share and a predictor that the
        hospital effects absorb both leave the fitted design, the used references and the feature list, and the first stays out of the used list while the
        second still appears in the preprocessing's kept set."""
        for name, late in (("missing", lambda row: None), ("absorbed", lambda row: float(int(row["ccn"]) % 7))):
            fixture = Fixture(folder / f"kept_m6_{name}", model="m6", hospitals=60, late=late)
            code, _, error = run_action(fixture, "fit")
            require(code == 0, f"fit of m6 with a {name} predictor failed: {error}")
            result, listing = output_of(fixture, f"{BUNDLE}/result.json"), output_of(fixture, f"{BUNDLE}/feature_list.json")
            slot = "m6_fixed_effects"
            require(result["statuses"][slot]["status"] == "fitted", f"m6 with a {name} predictor is {result['statuses'][slot]}")
            details = result["fitted_parameters"][slot]
            require(
                LATE_REFERENCE not in details["used_references"], f"{name}: the used references still name the removed predictor: {details['used_references']}"
            )
            require(
                LATE_REFERENCE not in listing["kept_references"] and "late" not in listing["kept_columns"], f"{name}: the drift metadata keeps it: {listing}"
            )
            require("late" not in details["mundlak"]["tested_columns"], f"{name}: the fitted panel tests it: {details['mundlak']['tested_columns']}")
            expected = [reference for reference in SLOT_LISTS[slot] if reference in details["used_references"]]
            require(
                expected and listing["kept_references"] == expected, f"{name}: kept references {listing['kept_references']}, the fitted design's are {expected}"
            )
            require(
                listing["fitted_slot"] == slot and listing["slot"] is None, f"{name}: fitted_slot {listing['fitted_slot']}, registry slot {listing['slot']}"
            )
            selection = details["panel_selection"]
            if name == "absorbed":
                require("late" in details["absorbed_columns"], f"the fixture's predictor was not absorbed: {details['absorbed_columns']}")
                require(LATE_REFERENCE in selection["kept_references"] and LATE_REFERENCE in details["selected_references"], "the selection stage dropped it")
            else:
                require([entry["reference"] for entry in selection["dropped"]] == [LATE_REFERENCE], f"the selection stage kept it: {selection}")

    def feature_list_of_an_unfitted_run() -> None:
        """A fixed-effects run with no repeated hospital ends not_estimable, and its fit action still exits 0 (the training run fails later, at its last
        action). The bundle says the run is unfitted: no fitted slot and no kept list."""
        fixture = Fixture(folder / "kept_unfitted", model="m6", hospitals=120, adjust=single_window)
        code, _, error = run_action(fixture, "fit")
        require(code == 0, f"fit failed: {error}")
        require(
            output_of(fixture, f"{BUNDLE}/result.json")["statuses"]["m6_fixed_effects"]["status"] == "not_estimable",
            "the m6 run fitted with no repeated hospital",
        )
        listing = output_of(fixture, f"{BUNDLE}/feature_list.json")
        require(
            listing == {"slot": None, "references": [], "columns": [], "fitted_slot": None, "kept_references": [], "kept_columns": []},
            f"feature list: {listing}",
        )

    def m6_fits_with_a_missing_predictor_in_both_populations() -> None:
        """The reviewer's reproduction through the real fit action: a predictor with no value used to leave m6 no complete row, yet the job exited 0."""
        for population in ("primary", "sensitivity"):
            fixture = Fixture(folder / f"m6_late_{population}", model="m6", hospitals=60, population=population, late=lambda row: None)
            code, out, error = run_action(fixture, "fit")
            require(code == 0, f"fit of m6 failed for {population}: {error}")
            require(fixture.manifest()["population"] == population, f"the fixture did not write a {population} extract")
            result = output_of(fixture, f"{BUNDLE}/result.json")
            require(result["statuses"]["m6_fixed_effects"]["status"] == "fitted", f"m6 on {population} is {result['statuses']['m6_fixed_effects']}")
            selection = result["fitted_parameters"]["m6_fixed_effects"]["panel_selection"]
            require([entry["reference"] for entry in selection["dropped"]] == [LATE_REFERENCE], f"m6 on {population} dropped {selection['dropped']}")
            require(LATE_REFERENCE not in selection["kept_references"], f"m6 on {population} kept the missing predictor")
            record = result["test_statistics"]["hospital_effects"]
            require(record["status"] == "fitted" and "late" not in record["tested_columns"], f"the Mundlak record on {population}: {record}")
            require(record["numerator_degrees_of_freedom"] == len(record["tested_columns"]), f"the Mundlak record on {population} does not report q: {record}")
            validation = output_of(fixture, f"hac_predictions_{HAI_TYPE}.json")
            require(validation["status"] == "fitted", f"the validation refit on {population} is {validation['status']}: {validation['reason']}")
            require(json.loads(out.strip().splitlines()[-1])["model_status"] == {HAI_TYPE: {"status": "fitted", "reason": ""}}, f"summary: {out}")

    def an_unfitted_model_is_said_plainly() -> None:
        """A panel that is truly not estimable still ends the fit with exit 0, and the summary and the error stream say that no model was fitted."""
        fixture = Fixture(folder / "m6_unfitted", model="m6", hospitals=120, adjust=single_window)
        code, out, error = run_action(fixture, "fit")
        require(code == 0, f"fit of an unfitted m6 failed: {error}")
        result = output_of(fixture, f"{BUNDLE}/result.json")
        status = result["statuses"]["m6_fixed_effects"]
        require(status["status"] == "not_estimable" and "no complete repeated training hospitals" in status["reason"], f"m6 status: {status}")
        reported = json.loads(out.strip().splitlines()[-1])
        require(reported["model_status"] == {HAI_TYPE: {"status": "not_estimable", "reason": status["reason"]}}, f"the summary does not say it: {reported}")
        require("not fitted" in error and "m6" in error and "not_estimable" in error and HAI_TYPE in error, f"the error stream does not say it: {error!r}")

    cases.check("job_fit_writes_results_and_done", fitted)
    cases.check("job_fit_m6_fits_with_a_missing_predictor_in_both_populations", m6_fits_with_a_missing_predictor_in_both_populations)
    cases.check("job_fit_an_unfitted_model_is_said_plainly", an_unfitted_model_is_said_plainly)
    cases.check("job_fit_each_model_fits_only_its_own_model_and_tests", one_model_per_run)
    cases.check("job_fit_only_promotion_models_have_a_registry_candidate", candidate_only_for_promotion_models)
    cases.check("job_fit_feature_list_of_every_model_and_population", feature_list_of_every_model_and_population)
    cases.check("job_fit_feature_list_of_m6_follows_the_panel", feature_list_of_m6_follows_the_panel)
    cases.check("job_fit_feature_list_of_an_unfitted_run", feature_list_of_an_unfitted_run)
    cases.check("job_fit_trust_check_reads_saved_state", serialized_state_trusted)
    cases.check("job_fit_trust_list_covers_candidates", trust_list_covers_candidates)
    cases.check("job_fit_refuses_untrusted_candidate_type", untrusted_type_refused)
    cases.check("job_fit_result_is_reproducible", reproducible)
    cases.check("job_fit_self_check_is_written_for_the_candidate", self_check_written)
    cases.check("job_fit_self_check_keeps_the_bundle_hash", self_check_keeps_bundle_hash)
    cases.check("job_fit_self_check_encodes_missing_values_as_null", self_check_encodes_missing_values)
    cases.check("job_fit_self_check_for_a_small_holdout", self_check_for_a_small_holdout)
    cases.check("job_fit_self_check_refuses_a_category_the_serving_reader_refuses", self_check_refuses_a_category_the_serving_reader_refuses)
    cases.check("job_fit_self_check_only_for_a_registry_candidate", self_check_only_for_a_candidate)
    cases.check("job_fit_retry_resumes_completed", retried)
    cases.check("job_fit_retry_after_interrupt", interrupted)
    cases.check("job_fit_done_conflict", conflict)
    cases.check("job_fit_refuses_stray_outputs", stray)
    cases.check("job_fit_refuses_changed_extract", changed_extract)
    cases.check("job_fit_refuses_manifest_of_another_run", wrong_run)
    cases.check("job_fit_refuses_manifest_of_another_type", wrong_type)
    cases.check("job_fit_missing_input_named", missing_input)
    cases.check("job_fit_accepts_aws_lane", aws_lane_accepted)
    cases.check("job_fit_refuses_unknown_lane", parameters_refused)
    cases.check("job_fit_checks_targets_against_bridge", targets_refused)
    cases.check("job_fit_refuses_too_few_rows", too_few_rows)
    cases.check("job_fit_deadline_named", deadline)
    cases.check("job_fit_model_deadline_named", model_deadline)
    cases.check("job_fit_two_types", two_types)
    cases.check("job_fit_second_type_missing_writes_nothing", second_type_missing)


def mediation_cases(cases: Any, folder: Path) -> None:
    """Run the separate PSI-90 and PSI-13 mediation analyses on the fixture extract and check the refusals."""

    def written() -> None:
        fixture = Fixture(folder / "written")
        code, _, error = run_action(fixture, "mediation")
        require(code == 0, f"mediation failed: {error}")
        result = output_of(fixture, f"mediation_{HAI_TYPE}.json")
        require(set(result["mediators"]) == {"PSI_90", "PSI_13"} and result["hai_type"] == HAI_TYPE, f"another result shape: {sorted(result)}")
        require(all(item["status"] == "fitted" for item in result["mediators"].values()), f"a mediator did not fit: {result['mediators']}")
        require(listing(fixture.out) == ["mediation.done.json", f"mediation_{HAI_TYPE}.json"], "mediation wrote other files")
        verify_done(fixture.out, "mediation", DIGEST)

    def retried() -> None:
        fixture = Fixture(folder / "retried")
        require(run_action(fixture, "mediation")[0] == 0, "the first run failed")
        before = (fixture.out / f"mediation_{HAI_TYPE}.json").read_bytes()
        with mock.patch.object(mediation, "analyze_mediation", side_effect=AssertionError("a completed step must not run again")):
            code, out, error = run_action(fixture, "mediation")
        require(code == 0 and json.loads(out)["resumed"] is True, f"the retry did not resume: {error}")
        require((fixture.out / f"mediation_{HAI_TYPE}.json").read_bytes() == before, "a retry changed the result")

    def changed_table() -> None:
        fixture = Fixture(folder / "changed")
        with (fixture.extract / "mediator.parquet").open("ab") as handle:
            handle.write(b"x")
        code, _, error = run_action(fixture, "mediation")
        require(code == job.EXIT_FAILED and failure_of(error) == "extract_changed", f"{code} {error}")
        require(listing(fixture.out) == [], "a refused step wrote output")

    def late_period() -> None:
        fixture = Fixture(folder / "late", late_mediator=True)
        code, _, error = run_action(fixture, "mediation")
        require(code == job.EXIT_FAILED and failure_of(error) == "mediation_refused" and "before the outcome window" in error, f"{code} {error}")
        require(listing(fixture.out) == [], "a refused step wrote output")

    def empty_table() -> None:
        fixture = Fixture(folder / "empty", mediator=False)
        code, _, error = run_action(fixture, "mediation")
        require(code == 0, f"an empty mediator table must be reported, not refused: {error}")
        require(
            all(item["status"] == "not_run" for item in output_of(fixture, f"mediation_{HAI_TYPE}.json")["mediators"].values()), "an empty table was fitted"
        )

    def deadline() -> None:
        fixture = Fixture(folder / "deadline")
        with mock.patch.object(job, "budget", return_value=0.3), mock.patch.object(mediation, "analyze_mediation", side_effect=lambda *_: time.sleep(5)):
            code, _, error = run_action(fixture, "mediation")
        require(code == job.EXIT_DEADLINE and failure_of(error) == "deadline" and listing(fixture.out) == [], f"{code} {error}")

    def other_model_refused() -> None:
        fixture = Fixture(folder / "mediation_m1", model="m1")
        code, _, error = run_action(fixture, "mediation")
        require(code == job.EXIT_FAILED and failure_of(error) == "mediation_not_applicable", f"{code} {error}")
        require(listing(fixture.out) == [], "a refused mediation left output")

    cases.check("job_mediation_writes_result_and_done", written)
    cases.check("job_mediation_belongs_to_m2", other_model_refused)
    cases.check("job_mediation_retry_resumes_completed", retried)
    cases.check("job_mediation_refuses_changed_table", changed_table)
    cases.check("job_mediation_refuses_mediator_inside_window", late_period)
    cases.check("job_mediation_reports_empty_table", empty_table)
    cases.check("job_mediation_deadline_named", deadline)


def pairwise_auc(scores: dict[str, float], flags: dict[str, float]) -> float:
    """Return the AUC by comparing every flagged hospital with every unflagged one, so no rank formula is shared with the code under test."""
    high, low = [scores[ccn] for ccn in scores if flags[ccn] == 1], [scores[ccn] for ccn in scores if flags[ccn] == 0]
    return sum((a > b) + 0.5 * (a == b) for a in high for b in low) / (len(high) * len(low))


def averaging_rows(rows: list[dict[str, Any]]) -> None:
    """Make the label an exact linear function of one predictor, and make each flagged hospital score high in only one of the last two windows."""
    newest = max(row["window_year"] for row in rows)
    for row in rows:
        number = int(row["ccn"]) - 990000
        if row["window_year"] in (newest - 1, newest) and number % 3 == 0:
            earlier = row["window_year"] == newest - 1
            row["process"] = (6.0 if (number % 6 == 0) == earlier else -2.0) + 0.01 * number
        elif row["window_year"] in (newest - 1, newest):
            row["process"] = 0.01 * number
        row[LABEL] = 2 + 0.5 * row["process"]


def drop_windows(*years: int, hospital: str | None = None) -> Callable[[list[dict[str, Any]]], None]:
    """Return a fixture change that removes whole window years, or one hospital's rows of those years."""

    def change(rows: list[dict[str, Any]]) -> None:
        rows[:] = [row for row in rows if row["window_year"] not in years or (hospital is not None and row["ccn"] != hospital)]

    return change


def both(*changes: Callable[[list[dict[str, Any]]], None]) -> Callable[[list[dict[str, Any]]], None]:
    """Apply several fixture changes in order."""

    def change(rows: list[dict[str, Any]]) -> None:
        for each in changes:
            each(rows)

    return change


@contextlib.contextmanager
def watch_validation_training() -> Iterator[list[set[int]]]:
    """Record the window years of every training set that the validation-only fit hands to the model library, seen from outside the code under test."""
    seen: list[set[int]] = []
    inside = [False]
    refit, tune, pipeline = suite.fit_validation, suite._tune, suite._fit_pipeline

    def watched_refit(*arguments: Any, **options: Any) -> Any:
        inside[0] = True
        try:
            return refit(*arguments, **options)
        finally:
            inside[0] = False

    def recording(real: Callable[..., Any]) -> Callable[..., Any]:
        def wrapper(*arguments: Any) -> Any:
            if inside[0]:
                seen.append({row["window_year"] for row in arguments[-1].rows})
            return real(*arguments)

        return wrapper

    with (
        mock.patch.object(suite, "fit_validation", watched_refit),
        mock.patch.object(suite, "_tune", recording(tune)),
        mock.patch.object(suite, "_fit_pipeline", recording(pipeline)),
    ):
        yield seen


def hac_cases(cases: Any, folder: Path) -> None:
    """Validate the selected specification, refit through window Y-2, against the HAC flags of the program year that covers windows Y-1 and Y."""
    flagged = {f"{990000 + number}": float(number % 3 == 0) for number in range(48)}

    def written() -> None:
        fixture = Fixture(folder / "written")
        with watch_validation_training() as seen:
            stage(fixture, "fit")
        code, _, error = run_action(fixture, "hac_validate")
        require(code == 0, f"hac_validate failed: {error}")
        result = output_of(fixture, f"hac_{HAI_TYPE}.json")
        require(result["status"] == "fitted" and result["program_year"] == 2025 and result["matched_hospitals"] > 0, f"another result: {result}")
        require(result["validation_only"] is True and "selected specification" in result["scope"], f"the result does not say what it validates: {result}")
        require(result["outcome_year"] == 2023 and result["training_last_year"] == 2021 and result["windows_compared"] == [2022, 2023], f"plan: {result}")
        require(not result["one_year_period"], "a two-year period was reported as one year")
        require(listing(fixture.out) == ["hac_HAI_1.json", "hac_validate.done.json"], f"outputs: {listing(fixture.out)}")
        verify_done(fixture.out, "hac_validate", DIGEST)
        years = set().union(*seen)
        require(seen and years == {2019, 2020, 2021}, f"the validation-only fit trained on {sorted(years)}, not on the windows through 2021")

    def averaged() -> None:
        fixture = Fixture(folder / "averaged", adjust=averaging_rows, flags=flagged)
        stage(fixture, "fit")
        require(run_action(fixture, "hac_validate")[0] == 0, "hac_validate failed")
        result = output_of(fixture, f"hac_{HAI_TYPE}.json")
        by_window = {year: {row["ccn"]: row[LABEL] for row in fixture.rows if row["window_year"] == year} for year in (2022, 2023)}
        average = {ccn: (by_window[2022][ccn] + by_window[2023][ccn]) / 2 for ccn in flagged}
        expected = pairwise_auc(average, flagged)
        singles = [pairwise_auc(by_window[year], flagged) for year in (2022, 2023)]
        require(all(single < 0.95 for single in singles) and expected > 0.99, f"the fixture does not tell the average from one window: {singles} {expected}")
        require(abs(result["auc"] - expected) < 1e-6, f"auc {result['auc']} is not the AUC {expected} of the two-window average")
        require(result["matched_hospitals"] == 48 and result["flagged_hospitals"] == 16 and result["hospitals_one_window"] == 0, f"counts: {result}")
        require(result["prediction_rows"] == 96, f"prediction rows: {result['prediction_rows']}")

    def one_window_hospital() -> None:
        change = both(averaging_rows, drop_windows(2022, hospital="990001"))
        fixture = Fixture(folder / "one_window", adjust=change, flags=flagged)
        stage(fixture, "fit")
        require(run_action(fixture, "hac_validate")[0] == 0, "hac_validate failed")
        result = output_of(fixture, f"hac_{HAI_TYPE}.json")
        require(result["hospitals_one_window"] == 1 and result["matched_hospitals"] == 47 and result["prediction_rows"] == 95, f"counts: {result}")

    def one_year_period() -> None:
        fixture = Fixture(folder / "one_year", years=range(2018, 2023), period_years=1, adjust=averaging_rows, flags=flagged)
        with watch_validation_training() as seen:
            stage(fixture, "fit")
        require(run_action(fixture, "hac_validate")[0] == 0, "hac_validate failed")
        result = output_of(fixture, f"hac_{HAI_TYPE}.json")
        require(result["program_year"] == 2024 and result["windows_compared"] == [2022] and result["one_year_period"] is True, f"plan: {result}")
        require("one year" in result["period_note"] and "2022" in result["period_note"], f"the one-year period is not recorded: {result['period_note']}")
        require(result["prediction_rows"] == 48 and result["training_last_year"] == 2020, f"counts: {result}")
        newest = {row["ccn"]: row[LABEL] for row in fixture.rows if row["window_year"] == 2022}
        require(abs(result["auc"] - pairwise_auc(newest, flagged)) < 1e-6, "the one-year period was not compared on its own window")
        require(set().union(*seen) == {2018, 2019, 2020}, f"trained on {sorted(set().union(*seen))}")

    def missing_flags() -> None:
        kept = {ccn: flag for ccn, flag in flagged.items() if int(ccn) % 6 != 5}
        fixture = Fixture(folder / "missing_flags", flags=kept)
        stage(fixture, "fit")
        require(run_action(fixture, "hac_validate")[0] == 0, "hac_validate failed")
        result = output_of(fixture, f"hac_{HAI_TYPE}.json")
        require(result["matched_hospitals"] == len(kept) and result["missing_flags"] == 48 - len(kept), f"missing flags were not reported: {result}")

    def retried() -> None:
        fixture = Fixture(folder / "retried")
        stage(fixture, "fit")
        require(run_action(fixture, "hac_validate")[0] == 0, "the first run failed")
        with mock.patch.object(hac_validation, "validate_hac", side_effect=AssertionError("a completed step must not run again")):
            code, out, error = run_action(fixture, "hac_validate")
        require(code == 0 and json.loads(out)["resumed"] is True, f"the retry did not resume: {error}")

    def no_mapping() -> None:
        fixture = Fixture(folder / "no_mapping")
        (fixture.root / "config/training/hac_year_mapping.json").unlink()
        code, _, error = run_action(fixture, "fit")
        require(code == job.EXIT_FAILED and failure_of(error) == "hac_year_mapping_missing", f"{code} {error}")
        require(listing(fixture.out) == [], "a refused step wrote output")

    def unmapped_year() -> None:
        fixture = Fixture(folder / "unmapped")
        (fixture.root / "config/training/hac_year_mapping.json").write_text(json.dumps({"schema": 1, "mapping": {"2019": 2021}, "nhsn_periods": {}}) + "\n")
        code, _, error = run_action(fixture, "fit")
        require(code == job.EXIT_FAILED and failure_of(error) == "fit_refused" and "year mapping" in error, f"{code} {error}")
        require(listing(fixture.out) == [], "a refused step wrote output")

    def window_without_rows() -> None:
        fixture = Fixture(folder / "no_previous", adjust=drop_windows(2022))
        code, _, error = run_action(fixture, "fit")
        require(code == job.EXIT_FAILED and failure_of(error) == "fit_refused", f"{code} {error}")
        require("window 2022" in error and "no rows" in error, f"the refusal does not name the window: {error}")
        require(listing(fixture.out) == [], "a refused step wrote output")

    def no_training_windows() -> None:
        fixture = Fixture(folder / "no_training", years=(2022, 2023), hospitals=110)
        code, _, error = run_action(fixture, "fit")
        require(code == job.EXIT_FAILED and failure_of(error) == "fit_refused", f"{code} {error}")
        require("through 2021" in error and "no windows" in error, f"the refusal does not name the cut: {error}")
        require(listing(fixture.out) == [], "a refused step wrote output")

    def dropped_predictions() -> None:
        for dropped in (2022, 2023):
            fixture = Fixture(folder / f"no_predictions_{dropped}")
            refit = suite.fit_validation

            def without(request: Any, slot: str, last: int, years: Sequence[int], *, window: int = dropped, real: Any = refit) -> Any:
                result = real(request, slot, last, years)
                held = [row for row in request.data.rows if row["window_year"] in years]
                keep = [i for i, position in enumerate(result.holdout_positions) if held[position]["window_year"] != window]
                result.holdout_positions = tuple(result.holdout_positions[i] for i in keep)
                result.predictions = tuple(result.predictions[i] for i in keep)
                return result

            with mock.patch.object(suite, "fit_validation", without):
                stage(fixture, "fit")
            code, _, error = run_action(fixture, "hac_validate")
            require(code == job.EXIT_FAILED and failure_of(error) == "hac_validate_refused", f"{code} {error}")
            require(f"window {dropped}" in error and "no predictions" in error, f"the refusal does not name the window: {error}")
            require(listing(fixture.out) == [], "a refused step wrote output")

    def refit_deadline_keeps_the_fit() -> None:
        from scripts.training.models.common import DeadlineExceeded

        fixture = Fixture(folder / "refit_deadline")
        real = suite._fit_slot

        def expired(request: Any, slot: str, *args: Any, **kwargs: Any) -> Any:
            if len(args) >= 2 and args[1] is not None:
                raise DeadlineExceeded("the validation refit ran out of its slot budget")
            return real(request, slot, *args, **kwargs)

        with mock.patch.object(suite, "_fit_slot", expired):
            stage(fixture, "fit")
        stored = [path for path in (fixture.folder / "in" / "fit").rglob("*") if path.is_file()]
        documents = [json.loads(path.read_text()) for path in stored if path.name.startswith("hac_predictions_")]
        require(documents and all(document["status"] == "not_run" for document in documents), f"no not_run validation file: {documents}")
        require(len(stored) > len(documents), "the fit's own bundle was lost")

    def overlap_refused() -> None:
        fixture = Fixture(folder / "overlap")
        with mock.patch.object(hac_validation, "training_cutoff", side_effect=lambda year: year - 1):
            code, _, error = run_action(fixture, "fit")
        require(code == job.EXIT_FAILED and failure_of(error) == "fit_refused" and "overlap" in error, f"{code} {error}")

    def planted_overlap_is_caught() -> None:
        fixture = Fixture(folder / "planted")
        with (
            mock.patch.object(hac_validation, "training_cutoff", side_effect=lambda year: year - 1),
            mock.patch.object(hac_validation, "check_no_overlap"),
            watch_validation_training() as seen,
        ):
            stage(fixture, "fit")
        leaked = sorted(set().union(*seen) & {2022, 2023})
        require(leaked == [2022], f"the watcher must see window 2022 in the planted defect, saw {leaked}")

    def stays_out_of_promotion() -> None:
        fixture = Fixture(folder / "outside")
        saved: list[str] = []
        real = save_model_standin

        def counted(*arguments: Any) -> None:
            saved.append("model")
            real(*arguments)

        with mock.patch(f"{__name__}.save_model_standin", side_effect=counted):
            stage(fixture, "fit")
            fitted_models = len(saved)
            require(run_action(fixture, "hac_validate")[0] == 0, "hac_validate failed")
        require(fitted_models == 1 and len(saved) == 1, f"models saved: {fitted_models} by the fit, {len(saved) - fitted_models} by the validation")
        require(not fixture.tracking.calls, f"the validation called MLflow: {fixture.tracking.calls}")
        names = listing(fixture.folder / "in/fit")
        require(all(name.startswith((BUNDLE, "hac_predictions_", "fit.done")) for name in names), f"fit outputs: {names}")
        outside = [name for name in names if not name.startswith(BUNDLE) and name.startswith("hac_predictions_")]
        require(outside == [f"hac_predictions_{HAI_TYPE}.json"], f"the validation predictions must stay outside the bundle: {names}")
        result = json.loads((fixture.folder / "in/fit" / BUNDLE / "result.json").read_text())
        require("hac" not in json.dumps(result), "the fit result carries the validation")

    def changed_fit() -> None:
        fixture = Fixture(folder / "changed_fit")
        stage(fixture, "fit")
        (fixture.folder / "in/fit" / f"hac_predictions_{HAI_TYPE}.json").write_text("{}\n")
        code, _, error = run_action(fixture, "hac_validate")
        require(code == job.EXIT_FAILED and failure_of(error) == "fit_changed", f"{code} {error}")

    def changed_table() -> None:
        fixture = Fixture(folder / "changed_table")
        stage(fixture, "fit")
        with (fixture.extract / "hac.parquet").open("ab") as handle:
            handle.write(b"x")
        code, _, error = run_action(fixture, "hac_validate")
        require(code == job.EXIT_FAILED and failure_of(error) == "extract_changed", f"{code} {error}")

    def no_flags() -> None:
        fixture = Fixture(folder / "no_flags", hac=False)
        stage(fixture, "fit")
        code, _, error = run_action(fixture, "hac_validate")
        require(code == 0 and output_of(fixture, f"hac_{HAI_TYPE}.json")["status"] == "not_run", f"an empty flag table must be reported: {error}")

    def deadline() -> None:
        fixture = Fixture(folder / "deadline")
        stage(fixture, "fit")
        with mock.patch.object(job, "budget", return_value=0.3), mock.patch.object(hac_validation, "validate_hac", side_effect=lambda *_: time.sleep(5)):
            code, _, error = run_action(fixture, "hac_validate")
        require(code == job.EXIT_DEADLINE and failure_of(error) == "deadline" and listing(fixture.out) == [], f"{code} {error}")

    cases.check("job_hac_writes_result_and_done", written)
    cases.check("job_hac_averages_the_windows_of_the_period", averaged)
    cases.check("job_hac_drops_a_one_window_hospital_and_reports_it", one_window_hospital)
    cases.check("job_hac_one_year_period_compares_only_its_year", one_year_period)
    cases.check("job_hac_reports_missing_flags", missing_flags)
    cases.check("job_hac_retry_resumes_completed", retried)
    cases.check("job_hac_year_mapping_missing_named", no_mapping)
    cases.check("job_hac_refuses_unmapped_year", unmapped_year)
    cases.check("job_hac_refuses_window_without_rows", window_without_rows)
    cases.check("job_hac_refuses_training_without_windows_through_y_minus_2", no_training_windows)
    cases.check("job_hac_refuses_window_without_predictions", dropped_predictions)
    cases.check("job_hac_refit_deadline_keeps_the_fit", refit_deadline_keeps_the_fit)
    cases.check("job_hac_refuses_training_that_overlaps_the_period", overlap_refused)
    cases.check("job_hac_watcher_catches_planted_overlap", planted_overlap_is_caught)
    cases.check("job_hac_stays_out_of_promotion_registry_and_rq", stays_out_of_promotion)
    cases.check("job_hac_refuses_changed_fit_output", changed_fit)
    cases.check("job_hac_refuses_changed_table", changed_table)
    cases.check("job_hac_reports_empty_flag_table", no_flags)
    cases.check("job_hac_deadline_named", deadline)


def version_cases(cases: Any, folder: Path) -> None:
    """Version each exported matrix with the pinned DVC against a local remote, then check that the run can load it back."""

    def versioned() -> None:
        fixture = Fixture(folder / "versioned")
        code, out, error = run_action(fixture, "version_extract")
        require(code == 0, f"version_extract failed: {error}")
        names = listing(fixture.out)
        metafile = f"extract_{HAI_TYPE}.parquet.dvc"
        require(names == [f"baseline_{HAI_TYPE}.json", metafile, "version_extract.done.json"], f"outputs: {names}")
        baseline = output_of(fixture, f"baseline_{HAI_TYPE}.json")
        require(baseline["metafile_sha256"] == file_sha256(fixture.out / metafile), "the baseline record carries another metafile hash")
        require(baseline["matrix_path"] == f"data/training/{RUN}/extract_{HAI_TYPE}.parquet", f"matrix path: {baseline['matrix_path']}")
        require(
            f"md5: {dvc_baseline.file_digest(fixture.extract / f'extract_{HAI_TYPE}.parquet', 'md5')}" in (fixture.out / metafile).read_text(),
            "the metafile does not hold the matrix MD5",
        )
        require(any(path.is_file() for path in (fixture.folder / "dvc_remote").rglob("*")), "nothing reached the remote")
        verify_done(fixture.out, "version_extract", DIGEST)
        require(json.loads(out)["resumed"] is False, "the first run reported a resume")

    def retried() -> None:
        fixture = Fixture(folder / "retried")
        require(run_action(fixture, "version_extract")[0] == 0, "the first run failed")
        with mock.patch.object(dvc_baseline, "run_dvc", side_effect=AssertionError("a completed step must not call DVC again")):
            code, out, error = run_action(fixture, "version_extract")
        require(code == 0 and json.loads(out)["resumed"] is True, f"the retry did not resume: {error}")

    def interrupted() -> None:
        fixture = Fixture(folder / "interrupted")
        require(run_action(fixture, "version_extract")[0] == 0, "the first run failed")
        before = {name: (fixture.out / name).read_bytes() for name in listing(fixture.out) if not name.endswith(".done.json")}
        (fixture.out / "version_extract.done.json").unlink()
        code, _, error = run_action(fixture, "version_extract")
        require(code == 0, f"the retry failed: {error}")
        require(before == {name: (fixture.out / name).read_bytes() for name in before}, "a retry changed the recorded baseline")

    def no_remote() -> None:
        fixture = Fixture(folder / "no_remote")
        with standins(fixture), mock.patch.dict(os.environ, {"DVC_REMOTE_URL": ""}):
            code, _, error = call(fixture.arguments("version_extract"))
        require(code == job.EXIT_FAILED and failure_of(error) == "dvc_remote_missing" and listing(fixture.out) == [], f"{code} {error}")

    def push_failure() -> None:
        fixture = Fixture(folder / "push_failure")
        (fixture.folder / "blocked").write_text("a file, not a folder\n")
        with standins(fixture), mock.patch.dict(os.environ, {"DVC_REMOTE_URL": str(fixture.folder / "blocked/remote")}):
            code, _, error = call(fixture.arguments("version_extract"))
        require(code == job.EXIT_FAILED and failure_of(error) == "dvc_failed" and "dvc push" in error, f"{code} {error}")
        require(listing(fixture.out) == [], "a failed push left output")

    def changed_extract() -> None:
        fixture = Fixture(folder / "changed")
        with (fixture.extract / f"extract_{HAI_TYPE}.parquet").open("ab") as handle:
            handle.write(b"x")
        code, _, error = run_action(fixture, "version_extract")
        require(code == job.EXIT_FAILED and failure_of(error) == "extract_changed" and listing(fixture.out) == [], f"{code} {error}")

    def deadline() -> None:
        fixture = Fixture(folder / "deadline")
        with mock.patch.object(job, "budget", return_value=0.5), mock.patch.object(dvc_baseline, "run_dvc", side_effect=lambda *_: time.sleep(5)):
            code, _, error = run_action(fixture, "version_extract")
        require(code == job.EXIT_DEADLINE and failure_of(error) == "deadline" and listing(fixture.out) == [], f"{code} {error}")

    def round_trip() -> None:
        fixture = staged(folder / "round_trip", baseline=False)
        stage(fixture, "version_extract")
        code, _, error = run_action(fixture, "log_run")
        require(code == 0, f"log_run failed: {error}")
        run_id = next(iter(fixture.tracking.runs))
        project = folder / "round_trip/loader"
        (project / ".dvc").mkdir(parents=True)
        (project / ".dvc/config").write_text("[core]\n    analytics = false\n    autostage = false\n    no_scm = true\n")
        dvc_baseline.run_dvc(["remote", "add", "--local", "--default", "store", str(fixture.folder / "dvc_remote")], project, 120)
        matrix = dvc_baseline.load(dvc_baseline.Run(fixture.tracking, run_id), project=project)
        require(matrix.read_bytes() == (fixture.extract / f"extract_{HAI_TYPE}.parquet").read_bytes(), "the loaded baseline differs from the exported matrix")

    cases.check("job_version_extract_pushes_matrix_and_records_metafile", versioned)
    cases.check("job_version_extract_retry_resumes_completed", retried)
    cases.check("job_version_extract_retry_after_interrupt_keeps_baseline", interrupted)
    cases.check("job_version_extract_needs_remote", no_remote)
    cases.check("job_version_extract_push_failure_named", push_failure)
    cases.check("job_version_extract_refuses_changed_extract", changed_extract)
    cases.check("job_version_extract_deadline_named", deadline)
    cases.check("job_version_extract_baseline_loads_from_logged_run", round_trip)


class Predictor:
    """A stand-in champion: it predicts the label plus a fixed offset, or a constant when no offset is given."""

    def __init__(self, label: str, offset: float | None = None, constant: float | None = None) -> None:
        self.label, self.offset, self.constant = label, offset, constant

    def predict(self, rows: Any) -> Any:
        return [self.constant if self.constant is not None else row[self.label] + (self.offset or 0.0) for row in rows]


class Mapped:
    """A stand-in candidate that predicts a fixed value for each hospital-window key."""

    def __init__(self, values: dict[str, float]) -> None:
        self.values = values

    def predict(self, rows: Any) -> Any:
        return [self.values[row["spine_key"]] for row in rows]


def baseline_of(fixture: Fixture) -> tuple[dict[str, float], dict[str, float]]:
    """The intercept + CMI baseline's holdout predictions and the observed SIRs, keyed by hospital-window, fitted the way the fit step fits it."""
    config = job_inputs.config_of(fixture.root)
    built = job_inputs.model_request(HAI_TYPE, fixture.rows, (), config, 60.0, fixture.root, "m2")
    holdout = built.data.split()[1]
    with libraries():
        predictions = research_tests.intercept_cmi_predictions(built)
    return dict(zip(holdout.keys, (float(value) for value in predictions), strict=True)), dict(
        zip(holdout.keys, (float(value) for value in holdout.y), strict=True)
    )


def champion_lists(columns: Sequence[str]) -> tuple[list[str], dict[str, Any], dict[str, Any]]:
    """The references, the feature list and the self-check of a champion with these columns (one row of ones that the self-check saves)."""
    references = [f"facility.{name}" for name in columns]
    feature_list = {"slot": "m1_ols", "references": references, "columns": list(columns)}
    saved = {"schema": 1, "slot": "m1_ols", "references": references, "rows": [[1.0] * len(columns)], "predictions": [1.0]}
    return references, feature_list, saved


def seed_champion(fixture: Fixture, *, newest_window: str = "2023", columns: tuple[str, ...] = ("process", "cmi", "extra")) -> str:
    """Put a production version of the type in the registry stand-in, with the source run and the four files the check reads: a provenance that holds the
    closure digest of this fixture's code root, the feature list, the self-check and the model file."""
    store = fixture.tracking
    name = f"hai_model_{HAI_TYPE}"
    store.models[name] = {"tags": {}, "versions": [], "aliases": {}}
    run = store.create_run("0", tags={"newest_window": newest_window, "run_id": CHAMPION_PARENT_RUN, "step_key": "parent"}).info.run_id
    target = store.root / run / "bundle"
    target.mkdir(parents=True)
    _, feature_list, saved = champion_lists(columns)
    (target / "feature_list.json").write_text(json.dumps(feature_list) + "\n")
    (target / "self_check.json").write_text(json.dumps(saved) + "\n")
    (target / "provenance.json").write_text(json.dumps({"run_id": "champion_train", model_code.CLOSURE_KEY: model_code.closure_digest(fixture.root)}) + "\n")
    (store.root / run / "model").mkdir()
    (store.root / run / "model/model.skops").write_text("champion")
    store.create_model_version(name, f"runs:/{run}/model", run_id=run, tags={"bundle_sha256": "e" * 64})
    store.set_registered_model_alias(name, "production", "1")
    store.calls.clear()
    fixture.champion_run = run
    return run


def promoted(folder: Path, champion: Predictor | None | str = None, **options: Any) -> Fixture:
    """Return a fixture with every earlier step complete, log_run included, and the champion the case names registered."""
    fixture = staged(folder, **options)
    (fixture.folder / "in/log_run").mkdir(exist_ok=True)
    code, _, error = run_action(fixture, "log_run", fixture.folder / "in/log_run")
    require(code == 0, f"log_run failed while staging: {error}")
    if champion is not None:
        seed_champion(fixture)
    fixture.champion = champion if isinstance(champion, Predictor) else None
    fixture.tracking.calls.clear()
    return fixture


def run_promotion(fixture: Fixture, **patches: Any) -> tuple[int, str, str]:
    """Run promotion_check with the model loader replaced by stand-ins: the saved candidate and, when seeded, the champion."""
    label = LABEL

    def loader(path: str, trusted: Any) -> Any:
        fixture.loads.append((path, tuple(trusted)))
        content = Path(path).read_text()
        if content == "champion":
            fixture.champion_loads.append(path)
            if fixture.champion is None:
                raise OSError("synthetic champion cannot be loaded")
            return fixture.champion
        return fixture.candidate if fixture.candidate is not None else SAVED[content]

    del label
    with mock.patch.object(promotion_check, "load_pipeline", side_effect=loader):
        return run_action(fixture, "promotion_check")


def promotion_check_cases(cases: Any, folder: Path) -> None:
    """Register the candidate, compare it with the champion and record the verdict, without ever moving production."""
    name = f"hai_model_{HAI_TYPE}"

    def registers_the_feature_set_and_its_schema_hash() -> None:
        """The registered version carries the set the run trained on and that set's schema hash, so the Production record names them and the features
        job counts the champion in its schema comparison (a version without them made a changed schema load online)."""
        fixture = promoted(folder / "set_identity")
        fixture.candidate = Predictor(LABEL, offset=0.0)
        code, _, error = run_promotion(fixture)
        require(code == 0, f"promotion_check failed: {error}")
        tags = fixture.tracking.models[name]["versions"][0]["tags"]
        expected = {"feature_set": fixture.model, "feature_set_schema_sha256": feature_schema.set_schema_sha256(fixture.model)}
        require({key: tags.get(key) for key in expected} == expected, f"the version does not carry its set and schema hash: {tags}")
        record = production_record.build(
            HAI_TYPE, {"model": name, "version": "1", "run_id": fixture.run, "mode": "promote"}, 1, tags, "2026-11-10T00:00:00+00:00"
        )
        found = feature_sets.production_models({HAI_TYPE: record}, fixture.model)
        require(found == {HAI_TYPE: expected["feature_set_schema_sha256"]}, f"the features job does not count the champion: {found}")

    def first_version() -> None:
        fixture = promoted(folder / "first")
        fixture.candidate = Predictor(LABEL, offset=0.0)
        code, out, error = run_promotion(fixture)
        require(code == 0, f"promotion_check failed: {error}")
        store = fixture.tracking
        version = store.models[name]["versions"][0]
        require(version["version"] == "1" and version["tags"]["promotion_verdict"] == "pass_baseline", f"version: {version}")
        require(
            store.models[name]["aliases"] == {},
            f"the promotion check set an alias; the research action chooses the challenger: {store.models[name]['aliases']}",
        )
        require(
            set(version["tags"])
            >= {
                "hai_type",
                "target_control",
                "feature_schema_sha256",
                "feature_list_sha256",
                "source_run_id",
                "bundle_sha256",
                "create_identity",
                "comparison_sha256",
            },
            f"tags: {sorted(version['tags'])}",
        )
        require(store.models[name]["tags"] == {}, "the pending marker was left behind")
        document = output_of(fixture, f"comparison_{HAI_TYPE}.json")
        require(
            document["champion_version"] is None
            and document["challenger_version"] == "1"
            and document["reference"] == "baseline"
            and document["verdict"]["verdict"] == "pass_baseline",
            f"comparison: {document}",
        )
        require(
            version["tags"]["comparison_sha256"] == hashlib.sha256((fixture.out / f"comparison_{HAI_TYPE}.json").read_bytes()).hexdigest(),
            "the comparison hash tag differs",
        )
        table = read_parquet(fixture.out / f"comparison_{HAI_TYPE}.parquet")
        expected, _ = baseline_of(fixture)
        require(set(table[0]) == set(promotion.COLUMNS), f"comparison rows: {sorted(table[0])}")
        require(
            all(abs(row["champion_prediction"] - expected[row["spine_key"]]) < 1e-9 for row in table),
            "the reference column does not hold the intercept + CMI baseline of the fit's training years",
        )
        logged = json.dumps(json.loads((fixture.folder / "in/fit" / BUNDLE / "result.json").read_text()))
        research = re.search(r'"intercept_cmi_rmse": ([0-9.eE+-]+)', logged)
        require(
            research is None or abs(float(research[1]) - document["verdict"]["baseline_rmse"]) < 1e-9,
            f"the baseline RMSE differs from the fit's RQ1 intercept + CMI RMSE: {research and research[1]} {document['verdict'].get('baseline_rmse')}",
        )
        promotion_run = [run for run in store.active().values() if run["tags"].get("step_key") == "promotion"]
        require(
            len(promotion_run) == 1 and promotion_run[0]["status"] == "FINISHED" and "bundle_sha256" in promotion_run[0]["tags"],
            "the promotion child run is missing or incomplete",
        )
        require(promotion_run[0]["tags"]["mlflow.parentRunId"] == next(iter(store.parents())), "the promotion run is not a child of the parent run")
        verify_done(fixture.out, "promotion_check", DIGEST)

    def first_version_of_m1() -> None:
        fixture = promoted(folder / "first_m1", model="m1")
        fixture.candidate = Predictor(LABEL, offset=0.0)
        code, _, error = run_promotion(fixture)
        require(code == 0, f"promotion_check of m1 failed: {error}")
        document = json.loads((fixture.out / f"comparison_{HAI_TYPE}.json").read_text())
        require(document["verdict"]["verdict"] == "pass_baseline", f"m1 could not become the first champion: {document['baseline']} {document['verdict']}")
        require(document["baseline"]["status"] == "fitted", f"the baseline was not fitted for m1: {document['baseline']}")

    def better_challenger() -> None:
        fixture = promoted(folder / "better", Predictor(LABEL, constant=3.0))
        code, _, error = run_promotion(fixture)
        require(code == 0, f"promotion_check failed: {error}")
        store = fixture.tracking
        require(store.models[name]["aliases"] == {"production": "1"}, f"aliases: {store.models[name]['aliases']}")
        require(store.models[name]["versions"][1]["tags"]["promotion_verdict"] == "pass", f"tags: {store.models[name]['versions'][1]['tags']}")
        document = output_of(fixture, f"comparison_{HAI_TYPE}.json")
        require(document["champion_version"] == "1" and document["challenger_version"] == "2", f"comparison: {document}")

    def worse_challenger() -> None:
        fixture = promoted(folder / "worse", Predictor(LABEL, offset=0.01))
        code, _, error = run_promotion(fixture)
        require(code == 0, f"promotion_check failed: {error}")
        store = fixture.tracking
        verdict = store.models[name]["versions"][1]["tags"]["promotion_verdict"]
        require(verdict.startswith("fail:") and "rmse_improvement" in verdict, f"verdict tag: {verdict}")
        require(store.models[name]["aliases"] == {"production": "1"}, f"a failed challenger got an alias: {store.models[name]['aliases']}")

    def production_untouched() -> None:
        fixture = promoted(folder / "production", Predictor(LABEL, constant=3.0))
        require(run_promotion(fixture)[0] == 0, "promotion_check failed")
        require("set_model_version_tag" in fixture.tracking.calls, "the case did not reach the verdict step")
        require(fixture.tracking.models[name]["aliases"] == {"production": "1"}, f"an alias moved or was set: {fixture.tracking.models[name]['aliases']}")

    def holdout_leak() -> None:
        fixture = promoted(folder / "leak", Predictor(LABEL, constant=3.0))
        for run in fixture.tracking.runs.values():
            if run["tags"].get("newest_window") == "2023" and run["tags"].get("step_key") == "parent" and run["tags"]["run_id"] == CHAMPION_PARENT_RUN:
                run["tags"]["newest_window"] = "2024"
        code, _, error = run_promotion(fixture)
        require(code == job.EXIT_FAILED and failure_of(error) == "holdout_leak" and listing(fixture.out) == [], f"{code} {error}")

    def champion_run_file(fixture: Fixture, path: str) -> Path:
        return fixture.tracking.root / fixture.champion_run / path

    def rewrite_champion(fixture: Fixture, columns: tuple[str, ...]) -> None:
        """Give the champion a feature list and a self-check of these columns, so that the two still agree."""
        _, feature_list, saved = champion_lists(columns)
        champion_run_file(fixture, "bundle/feature_list.json").write_text(json.dumps(feature_list) + "\n")
        champion_run_file(fixture, "bundle/self_check.json").write_text(json.dumps(saved) + "\n")

    def champion_is_read_as_data_only() -> None:
        """The check reads a champion through the bounded fetch of ``model_code`` and nothing else: its provenance, feature list and self-check (1 MiB cap) and
        then its model file (64 MiB cap), never ``MLmodel``, never the code folder, never ``download_artifacts``, and the model through the trusted types of the
        code root's own contract. A planted code folder and an ``MLmodel`` that trusts one more type change nothing. The challenger is read from its own
        ``model.skops`` under the same list."""
        fixture = promoted(folder / "data_only", Predictor(LABEL, constant=3.0))
        marker = fixture.folder / "written_by_artifact_code"
        champion = fixture.tracking.root / fixture.champion_run / "model"
        (champion / "MLmodel").write_text("trusted_types: [evil.Thing]\n")
        (champion / "code").mkdir()
        (champion / "code/evil.py").write_text(f"open({str(marker)!r}, 'w')\n")
        trusted = json.loads((fixture.root / "data_contracts/training/model_contract.json").read_text())["serialization"]["trusted_types"]
        with mock.patch.object(fixture.tracking, "download_artifacts", side_effect=AssertionError("download_artifacts was called")):
            code, _, error = run_promotion(fixture)
        require(code == 0, f"a champion with a planted code folder failed the check: {code} {error}")
        wanted = ["bundle/provenance.json", "bundle/feature_list.json", "bundle/self_check.json", "model/model.skops"]
        require(fixture.fetched == wanted, f"the check fetched {fixture.fetched}, not exactly the four fixed paths in order")
        require(
            fixture.fetch_limits == {name: model_code.MODEL_CAP if name.endswith(".skops") else model_code.JSON_CAP for name in wanted},
            f"caps: {fixture.fetch_limits}",
        )
        require(not marker.exists(), "the planted artifact code ran")
        require(
            len(fixture.loads) == 2 and all(found == tuple(trusted) and found for _, found in fixture.loads),
            f"the model reads did not use the contract's own list: {fixture.loads}",
        )
        require([Path(path).name for path, _ in fixture.loads] == ["model.skops", "model.skops"], f"model reads: {fixture.loads}")

    def no_champion_reads_no_champion_file() -> None:
        """With no champion (the first champion) the check reads no champion file and scores the candidate against the baseline, as before."""
        fixture = promoted(folder / "no_champion_files")
        code, _, error = run_promotion(fixture)
        require(code == 0 and fixture.fetched == [] and fixture.champion_loads == [], f"{code} {error} {fixture.fetched}")
        require(len(fixture.loads) == 1 and Path(fixture.loads[0][0]).name == "model.skops", f"only the candidate is read: {fixture.loads}")

    def champion_fetch_deadline() -> None:
        """A fetch of any of the champion's four files that reaches its deadline, or that is refused because the one worker slot of the shared fetch stays
        taken, is refused by name, as ``champion_fetch_deadline``, and the check writes and tags nothing: the shared fetch (``model_code.mlflow_fetch``) raises
        the rule ``deadline`` or ``busy`` and the check never waits longer."""

        def late(rule: str) -> Callable[[str, str, str, int], str]:
            def fetch(run_id: str, path: str, local_path: str, limit: int) -> str:
                raise model_code.DataRuleError(rule, f"the fetch of {path} did not finish within its deadline")

            return fetch

        for rule, path in itertools.product(
            ("deadline", "busy"), ("bundle/provenance.json", "bundle/feature_list.json", "bundle/self_check.json", "model/model.skops")
        ):
            fixture = promoted(folder / f"fetch_{rule}_{Path(path).stem}", Predictor(LABEL, constant=3.0))
            fixture.fetch_faults[path] = late(rule)
            code, _, error = run_promotion(fixture)
            require(code == job.EXIT_FAILED and failure_of(error) == "champion_fetch_deadline", f"{rule} {path}: {code} {error}")
            require(fixture.champion_loads == [] and listing(fixture.out) == [], f"{path}: the champion was loaded or output was written")
            require("promotion_verdict" not in fixture.tracking.models[name]["versions"][1]["tags"], f"{path}: the version got a verdict")

    def champion_admission() -> None:
        """A champion is admitted before its model file is read: its provenance must hold the closure digest of this job's own code root and its self-check must
        pass the data rules of the model API. A champion that predates the closure record is refused by name (the owner deletes such models at rollout). A
        refused champion is never fetched past its self-check, never loaded and never scored, and the check writes and tags nothing."""
        refusals: dict[str, tuple[Callable[[Fixture], None], str]] = {}

        def provenance(**changes: Any) -> Callable[[Fixture], None]:
            def plant(fixture: Fixture) -> None:
                path = champion_run_file(fixture, "bundle/provenance.json")
                document = {**json.loads(path.read_text()), **changes}
                path.write_text(json.dumps({key: value for key, value in document.items() if value is not None}) + "\n")

            return plant

        def write(path: str, data: bytes) -> Callable[[Fixture], None]:
            return lambda fixture: champion_run_file(fixture, path).write_bytes(data)

        def remove(path: str) -> Callable[[Fixture], None]:
            return lambda fixture: champion_run_file(fixture, path).unlink()

        def self_check(**changes: Any) -> Callable[[Fixture], None]:
            def plant(fixture: Fixture) -> None:
                path = champion_run_file(fixture, "bundle/self_check.json")
                path.write_text(json.dumps({**json.loads(path.read_text()), **changes}) + "\n")

            return plant

        refusals["predates_the_closure_record"] = (provenance(**{model_code.CLOSURE_KEY: None}), "model_code_closure_missing")
        refusals["trained_with_other_model_code"] = (provenance(**{model_code.CLOSURE_KEY: "0" * 64}), "model_code_closure_mismatch")
        refusals["provenance_with_a_repeated_key"] = (write("bundle/provenance.json", b'{"run_id": "a", "run_id": "b"}'), "model_provenance_unreadable")
        refusals["provenance_in_utf16"] = (write("bundle/provenance.json", json.dumps({"run_id": "a"}).encode("utf-16")), "model_provenance_unreadable")
        refusals["provenance_with_an_unknown_key"] = (provenance(note="x"), "model_provenance_unreadable")
        refusals["no_provenance_file"] = (remove("bundle/provenance.json"), "model_provenance_unreadable")
        refusals["no_self_check_file"] = (remove("bundle/self_check.json"), "self_check_missing")
        refusals["self_check_with_a_repeated_key"] = (write("bundle/self_check.json", b'{"schema": 2, "schema": 1}'), "self_check_not_json")
        refusals["self_check_in_utf16"] = (write("bundle/self_check.json", json.dumps({"schema": 1}).encode("utf-16")), "self_check_not_json")
        refusals["self_check_with_no_rows"] = (self_check(rows=[], predictions=[]), "self_check_row_count")
        refusals["self_check_with_a_slot_of_257_characters"] = (self_check(slot="x" * 257), "self_check_keys")
        refusals["self_check_with_other_references"] = (self_check(references=["a", "b", "c"]), "self_check_references")
        refusals["feature_list_over_the_bounds"] = (
            lambda fixture: champion_run_file(fixture, "bundle/feature_list.json").write_text(
                json.dumps({"slot": "m1_ols", "references": ["r"] * 2001, "columns": ["process"] * 2001}) + "\n"
            ),
            "champion_feature_list_invalid",
        )
        for label, (plant, reason) in refusals.items():
            fixture = promoted(folder / f"admission_{label}", Predictor(LABEL, constant=3.0))
            plant(fixture)
            code, _, error = run_promotion(fixture)
            require(code == job.EXIT_FAILED and failure_of(error) == reason, f"{label}: {code} {error}")
            require("model/model.skops" not in fixture.fetched and fixture.champion_loads == [], f"{label}: the champion's model was read: {fixture.fetched}")
            require(listing(fixture.out) == [], f"{label}: a refused check wrote output")
            tags = fixture.tracking.models[name]["versions"][1]["tags"]
            require("promotion_verdict" not in tags, f"{label}: the version got a verdict: {tags}")
            require(fixture.tracking.models[name]["aliases"] == {"production": "1"}, f"{label}: an alias changed")

    def champion_columns() -> None:
        fixture = promoted(folder / "columns", Predictor(LABEL, constant=3.0))
        rewrite_champion(fixture, ("process", "unknown_column"))
        code, _, error = run_promotion(fixture)
        require(code == 0, f"a champion this extract cannot score failed the run: {error}")
        require(
            fixture.tracking.models[name]["versions"][1]["tags"]["promotion_verdict"] == "not_comparable", "the version did not get the verdict not_comparable"
        )
        document = output_of(fixture, f"comparison_{HAI_TYPE}.json")
        require(
            document["verdict"] == {"verdict": "not_comparable", "failed": ["champion_features"]} and "unknown_column" in document["reason"],
            f"comparison: {document}",
        )
        require(fixture.tracking.models[name]["aliases"] == {"production": "1"}, "an alias changed")

    def champion_feature_list_damaged() -> None:
        """A champion whose list has no usable ``columns``, or whose list file is missing, unreadable or not JSON, is never loaded or scored: the check fails
        by name with what was found, writes nothing, tags no verdict and moves no alias. A list that is well formed but needs a column the extract lacks
        stays the verdict ``not_comparable``."""

        class Counted(Predictor):
            scored = 0

            def predict(self, rows: Any) -> Any:
                Counted.scored += 1
                return super().predict(rows)

        Plant = Callable[[Fixture, contextlib.ExitStack], None]

        def faulty(fixture: Fixture, fault: Callable[[str, str, str, int], str]) -> None:
            fixture.fetch_faults["bundle/feature_list.json"] = fault

        def refusing_download(error: Exception) -> Plant:
            def raises(run_id: str, path: str, local_path: str, limit: int) -> str:
                raise error

            return lambda fixture, stack: faulty(fixture, raises)

        def gone_after_download(fixture: Fixture, stack: contextlib.ExitStack) -> None:
            faulty(fixture, lambda run_id, path, local_path, limit: local_path)

        def listed(fixture: Fixture) -> Path:
            return champion_run_file(fixture, "bundle/feature_list.json")

        def document(value: Any) -> Plant:
            return lambda fixture, stack: listed(fixture).write_text(json.dumps(value) + "\n")

        def raw(value: bytes) -> Plant:
            return lambda fixture, stack: listed(fixture).write_bytes(value)

        marker = "secret-marker"
        shapes: dict[str, tuple[Plant, str]] = {
            "absent": (document({"slot": "m1_ols", "references": []}), "absent"),
            "empty": (document({"columns": []}), "an empty list"),
            "text": (document({"columns": "process"}), "text, not a list"),
            "null": (document({"columns": None}), "null, not a list"),
            "number": (document({"columns": 3}), "a number, not a list"),
            "object": (document({"columns": {"process": 1}}), "an object, not a list"),
            "not_text": (document({"columns": ["process", 2]}), "element 1 is a number, not text"),
            "empty_name": (document({"columns": ["", "cmi"]}), "element 0 is an empty name"),
            "repeat": (document({"columns": ["process", "process"]}), "element 1 repeats an earlier name"),
            "not_an_object": (document(["process", "cmi"]), "a list, not an object"),
            "truncated_json": (raw(b'{"columns": ["process",'), "is not JSON: JSONDecodeError"),
            "invalid_encoding": (raw(b'{"columns": ["\xff\xfe"]}'), "is not JSON: UnicodeDecodeError"),
            "empty_file": (raw(b""), "is not JSON: JSONDecodeError"),
            "missing_after_download": (gone_after_download, "was not fetched as one regular file"),
            "missing_artifact": (lambda fixture, stack: listed(fixture).unlink(), "could not be downloaded: FileNotFoundError"),
            "download_not_found": (refusing_download(FileNotFoundError(marker)), "could not be downloaded: FileNotFoundError"),
            "download_oserror": (refusing_download(OSError(marker)), "could not be downloaded: OSError"),
            "download_error": (refusing_download(RuntimeError(marker)), "could not be downloaded: RuntimeError"),
        }
        columns_shapes = ("absent", "empty", "text", "null", "number", "object", "not_text", "empty_name", "repeat")
        for label, (plant, found) in shapes.items():
            fixture = promoted(folder / f"damaged_{label}", Counted(LABEL, constant=3.0))
            Counted.scored = 0
            with contextlib.ExitStack() as stack:
                plant(fixture, stack)
                code, _, error = run_promotion(fixture)
            require(code == job.EXIT_FAILED and failure_of(error) == "champion_feature_list_invalid", f"{label}: {code} {error}")
            require(found in error, f"{label}: the failure does not say what was found ({found}): {error}")
            require(marker not in error, f"{label}: the failure repeats the text of the exception: {error}")
            require("feature list" in error, f"{label}: the failure does not name the feature list: {error}")
            require(label not in columns_shapes or "columns" in error, f"{label}: the failure does not name the key: {error}")
            require(fixture.champion_loads == [], f"{label}: the champion was loaded: {fixture.champion_loads}")
            require(Counted.scored == 0, f"{label}: the champion was scored")
            require(listing(fixture.out) == [], f"{label}: a refused check wrote output")
            tags = fixture.tracking.models[name]["versions"][1]["tags"]
            require("promotion_verdict" not in tags, f"{label}: the version got a verdict: {tags}")
            require(fixture.tracking.models[name]["aliases"] == {"production": "1"}, f"{label}: an alias changed")

    def champion_unloadable() -> None:
        fixture = promoted(folder / "unloadable")
        seed_champion(fixture)
        fixture.champion = None
        code, _, error = run_promotion(fixture)
        require(
            code == job.EXIT_FAILED
            and failure_of(error) == "champion_unloadable"
            and "production" not in fixture.tracking.models[name]["aliases"].get("challenger", ""),
            f"{code} {error}",
        )
        require(listing(fixture.out) == [], "a refused check wrote output")

    def sensitivity_population() -> None:
        fixture = promoted(folder / "sensitivity", Predictor(LABEL, constant=3.0))
        next(iter(fixture.tracking.parents().values()))["tags"]["population"] = "sensitivity"
        code, _, error = run_promotion(fixture)
        require(code == job.EXIT_FAILED and failure_of(error) == "population_not_primary" and "sensitivity" in error, f"{code} {error}")
        store = fixture.tracking
        require(len(store.models[name]["versions"]) == 1, "a sensitivity run was registered as a model version")
        require(store.models[name]["aliases"] == {"production": "1"}, f"a sensitivity run set an alias: {store.models[name]['aliases']}")
        require(listing(fixture.out) == [], "a refused check wrote output")

    def lost_child_response() -> None:
        fixture = promoted(folder / "lost_child")
        store = fixture.tracking
        before = len(store.runs)
        store.lose_create = store.creates + 1
        code, _, error = run_promotion(fixture)
        require(code == job.EXIT_FAILED and failure_of(error) == "mlflow_failed" and len(store.runs) == before + 1, f"{code} {error}")
        store.lose_create = None
        code, _, error = run_promotion(fixture)
        require(code == 0, f"the retry failed: {error}")
        promotion_runs = [run for run in store.runs.values() if run["tags"].get("step_key") == "promotion"]
        require(len(store.runs) == before + 1 and len(promotion_runs) == 1 and not promotion_runs[0]["deleted"], f"runs: {len(store.runs)} of {before + 1}")
        require(promotion_runs[0]["status"] == "FINISHED" and "bundle_sha256" in promotion_runs[0]["tags"], "the adopted promotion run was not completed")
        require(len(store.models[name]["versions"]) == 1, "the retry registered another model version")

    def dirty_code() -> None:
        fixture = promoted(folder / "dirty")
        fixture.candidate = Predictor(LABEL, offset=0.0)
        next(iter(fixture.tracking.parents().values()))["tags"]["git_dirty"] = "true"
        code, _, error = run_promotion(fixture)
        require(code == 0, f"promotion_check failed: {error}")
        verdict = fixture.tracking.models[name]["versions"][0]["tags"]["promotion_verdict"]
        require(verdict == "fail:provenance", f"a dirty tree that beat the baseline was not failed on provenance alone: {verdict}")
        require(fixture.tracking.models[name]["aliases"] == {}, "a dirty first version got an alias")

    def first_one_percent() -> None:
        fixture = promoted(folder / "first_one_percent")
        baseline, observed = baseline_of(fixture)
        fixture.candidate = Mapped({key: value + 0.01 * (observed[key] - value) for key, value in baseline.items()})
        code, _, error = run_promotion(fixture)
        require(code == 0, f"promotion_check failed: {error}")
        verdict = fixture.tracking.models[name]["versions"][0]["tags"]["promotion_verdict"]
        require(verdict.startswith("fail:") and "rmse_improvement" in verdict, f"a candidate 1 percent better than the baseline was not refused: {verdict}")
        improvement = output_of(fixture, f"comparison_{HAI_TYPE}.json")["verdict"]["conditions"]["rmse_improvement"]["value"]
        require(abs(improvement - 0.01) < 1e-9, f"the case did not build a 1 percent gain: {improvement}")
        require(fixture.tracking.models[name]["aliases"] == {}, "a refused first version got an alias")

    def first_interval() -> None:
        fixture = promoted(folder / "first_interval")
        baseline, observed = baseline_of(fixture)
        worst = max(baseline, key=lambda key: abs(observed[key] - baseline[key]))
        fixture.candidate = Mapped({**baseline, worst: observed[worst]})
        code, _, error = run_promotion(fixture)
        require(code == 0, f"promotion_check failed: {error}")
        verdict = fixture.tracking.models[name]["versions"][0]["tags"]["promotion_verdict"]
        conditions = output_of(fixture, f"comparison_{HAI_TYPE}.json")["verdict"]["conditions"]
        require(conditions["rmse_improvement"]["passed"], f"the case did not build a gain of at least 2 percent: {conditions['rmse_improvement']}")
        require(verdict == "fail:interval", f"a gain from one hospital passed although its interval touches zero: {verdict} {conditions['interval']}")

    def baseline_not_estimable() -> None:
        fixture = promoted(folder / "baseline_not_estimable")
        fixture.candidate = Predictor(LABEL, offset=0.0)
        with mock.patch.object(research_tests, "intercept_cmi_predictions", side_effect=NotEstimable("forced covariate exceeds the half-missing threshold")):
            code, _, error = run_promotion(fixture)
        require(code == 0, f"promotion_check failed: {error}")
        document = output_of(fixture, f"comparison_{HAI_TYPE}.json")
        verdict = fixture.tracking.models[name]["versions"][0]["tags"]["promotion_verdict"]
        require(
            verdict == "no_champion" and document["baseline"]["status"] == "not_estimable",
            f"an unfitted baseline did not leave the type without a reference: {verdict}",
        )
        require(fixture.tracking.models[name]["aliases"] == {}, "a version got an alias without a reference")

    def champion_rule_unchanged() -> None:
        fixture = promoted(folder / "champion_rule", Predictor(LABEL, constant=3.0))
        fixture.candidate = Predictor(LABEL, offset=0.0)
        require(run_promotion(fixture)[0] == 0, "promotion_check failed")
        document = output_of(fixture, f"comparison_{HAI_TYPE}.json")
        require(document["reference"] == "champion" and "groups" in document["verdict"]["conditions"], f"the champion rule changed: {document['verdict']}")
        require(fixture.tracking.models[name]["versions"][1]["tags"]["promotion_verdict"] == "pass", "a candidate better than the champion did not pass")

    def retried() -> None:
        fixture = promoted(folder / "retried")
        require(run_promotion(fixture)[0] == 0, "the first run failed")
        fixture.tracking.calls.clear()
        code, out, error = run_promotion(fixture)
        require(
            code == 0 and json.loads(out)["resumed"] is True and not fixture.tracking.calls, f"the retry called the store: {error} {fixture.tracking.calls}"
        )

    def interrupted() -> None:
        fixture = promoted(folder / "interrupted")
        require(run_promotion(fixture)[0] == 0, "the first run failed")
        before = (fixture.out / f"comparison_{HAI_TYPE}.json").read_bytes()
        (fixture.out / "promotion_check.done.json").unlink()
        code, _, error = run_promotion(fixture)
        store = fixture.tracking
        require(code == 0 and len(store.models[name]["versions"]) == 1, f"the retry created a second version: {error}")
        require(len([run for run in store.active().values() if run["tags"].get("step_key") == "promotion"]) == 1, "the retry left two promotion runs")
        require((fixture.out / f"comparison_{HAI_TYPE}.json").read_bytes() == before, "the retry changed the comparison")

    def pending_create() -> None:
        fixture = promoted(folder / "pending")
        store = fixture.tracking
        store.models[name] = {"tags": {"pending_create." + "a" * 32: f"{RUN}:{HAI_TYPE}:promotion"}, "versions": [], "aliases": {}}
        code, _, error = run_promotion(fixture)
        require(code == job.EXIT_FAILED and failure_of(error) == "pending_create" and "create_model_version" not in store.calls, f"{code} {error}")

    def version_conflict() -> None:
        fixture = promoted(folder / "conflict")
        require(run_promotion(fixture)[0] == 0, "the first run failed")
        (fixture.out / "promotion_check.done.json").unlink()
        fixture.tracking.models[name]["versions"][0]["tags"]["bundle_sha256"] = "0" * 64
        code, _, error = run_promotion(fixture)
        require(code == job.EXIT_FAILED and failure_of(error) == "model_version_conflict", f"{code} {error}")

    def other_location() -> None:
        fixture = promoted(folder / "location")
        fixture.tracking.location = "s3://elsewhere/0"
        code, _, error = run_promotion(fixture)
        require(code == job.EXIT_FAILED and failure_of(error) == "artifact_location" and not fixture.tracking.models, f"{code} {error}")

    def incomplete_run() -> None:
        fixture = promoted(folder / "incomplete")
        next(iter(fixture.tracking.parents().values()))["tags"].pop("bundle_sha256")
        code, _, error = run_promotion(fixture)
        require(code == job.EXIT_FAILED and failure_of(error) == "run_incomplete" and not fixture.tracking.models, f"{code} {error}")

    def changed_log_run() -> None:
        fixture = promoted(folder / "changed_log")
        (fixture.folder / "in/log_run/log_run.json").write_text("{}\n")
        code, _, error = run_promotion(fixture)
        require(code == job.EXIT_FAILED and failure_of(error) == "log_run_changed", f"{code} {error}")

    def mismatch() -> None:
        fixture = promoted(folder / "mismatch")
        real = promotion.evaluate
        calls = []

        def drifting(*args: Any, **kwargs: Any) -> Any:
            calls.append(1)
            result = real(*args, **kwargs)
            return result if len(calls) == 1 else {**result, "verdict": "pass"}

        with mock.patch.object(promotion, "evaluate", side_effect=drifting):
            code, _, error = run_promotion(fixture)
        require(
            code == job.EXIT_FAILED
            and failure_of(error) == "comparison_mismatch"
            and not fixture.tracking.models[name]["versions"][0]["tags"].get("promotion_verdict"),
            f"{code} {error}",
        )

    def deadline() -> None:
        fixture = promoted(folder / "deadline")
        with mock.patch.object(job, "budget", return_value=0.5), mock.patch.object(promotion_check, "predict", side_effect=lambda *_: time.sleep(5)):
            code, _, error = run_promotion(fixture)
        require(code == job.EXIT_DEADLINE and failure_of(error) == "deadline" and listing(fixture.out) == [], f"{code} {error}")

    def champion_reads_only_the_missing_alias_answer_as_none() -> None:
        store = FileTracking(folder / "alias_answers")
        store.models[name] = {"tags": {}, "versions": [], "aliases": {}}
        require(registry.champion(store, "hai_model_missing") is None, "a registered model that does not exist (404) was not read as no champion")
        require(
            registry.champion(store, name) is None,
            "a registered model without the production alias (400, the message of MLflow 3.17.2) was not read as no champion",
        )
        other = InvalidParameter(f"Invalid alias name: '{registry.CHAMPION_ALIAS}'. Aliases may not contain spaces.")
        wrong_code = InvalidParameter(f"Registered model alias {registry.CHAMPION_ALIAS} not found.")
        wrong_code.error_code = "BAD_REQUEST"
        challenger = InvalidParameter("Registered model alias challenger not found.")
        for label, error in (
            ("another 400 message", other),
            ("another code", wrong_code),
            ("another alias", challenger),
            ("a server error", RuntimeError("boom")),
        ):
            with mock.patch.object(store, "get_model_version_by_alias", side_effect=error):
                try:
                    registry.champion(store, name)
                except type(error):
                    continue
            raise ValueError(f"{label} was read as no champion")

    def champion_deadline() -> None:
        fixture = promoted(folder / "champion_deadline", Predictor(LABEL, constant=3.0))

        def slow_champion(path: str, trusted: Any) -> Any:
            content = Path(path).read_text()
            if content == "champion":
                time.sleep(5)
            return fixture.champion if content == "champion" else SAVED[content]

        with mock.patch.object(job, "budget", return_value=2.0), mock.patch.object(promotion_check, "load_pipeline", side_effect=slow_champion):
            code, _, error = run_action(fixture, "promotion_check")
        require(
            code == job.EXIT_DEADLINE and failure_of(error) == "deadline",
            f"a deadline during the champion load was reported as another failure: {code} {error}",
        )

    def no_uri() -> None:
        fixture = promoted(folder / "no_uri")
        with standins(fixture), mock.patch.dict(os.environ, {"MLFLOW_TRACKING_URI": ""}):
            code, _, error = call(fixture.arguments("promotion_check"))
        require(code == job.EXIT_FAILED and failure_of(error) == "mlflow_uri_missing", f"{code} {error}")

    cases.check("job_promotion_check_registers_the_feature_set_and_its_schema_hash", registers_the_feature_set_and_its_schema_hash)
    cases.check("job_promotion_check_first_version_passes_against_the_baseline", first_version)
    cases.check("job_promotion_check_m1_becomes_the_first_champion_against_the_baseline", first_version_of_m1)
    cases.check("job_promotion_check_first_version_one_percent_better_than_baseline_refused", first_one_percent)
    cases.check("job_promotion_check_first_version_interval_touching_zero_refused", first_interval)
    cases.check("job_promotion_check_champion_rule_unchanged", champion_rule_unchanged)
    cases.check("job_promotion_check_unfitted_baseline_keeps_no_champion", baseline_not_estimable)
    cases.check("job_promotion_check_tags_a_passing_version_and_sets_no_alias", better_challenger)
    cases.check("job_promotion_check_fails_worse_challenger_without_alias", worse_challenger)
    cases.check("job_promotion_check_never_moves_production", production_untouched)
    cases.check("job_promotion_check_refuses_sensitivity_population", sensitivity_population)
    cases.check("job_promotion_check_retry_adopts_run_whose_response_was_lost", lost_child_response)
    cases.check("job_promotion_check_refuses_holdout_leak", holdout_leak)
    cases.check("job_promotion_check_records_not_comparable_for_champion_columns_outside_extract", champion_columns)
    cases.check("job_promotion_check_fails_a_damaged_champion_feature_list", champion_feature_list_damaged)
    cases.check("job_promotion_check_refuses_unloadable_champion", champion_unloadable)
    cases.check("job_promotion_check_fails_dirty_code", dirty_code)
    cases.check("job_promotion_check_retry_resumes_completed", retried)
    cases.check("job_promotion_check_retry_after_interrupt_keeps_one_version", interrupted)
    cases.check("job_promotion_check_refuses_unresolved_pending_create", pending_create)
    cases.check("job_promotion_check_refuses_version_with_other_bundle", version_conflict)
    cases.check("job_promotion_check_refuses_other_artifact_location", other_location)
    cases.check("job_promotion_check_refuses_incomplete_run", incomplete_run)
    cases.check("job_promotion_check_refuses_changed_log_run", changed_log_run)
    cases.check("job_promotion_check_refuses_verdict_mismatch_on_written_file", mismatch)
    cases.check("job_promotion_check_deadline_named", deadline)
    cases.check("job_promotion_check_champion_reads_only_the_missing_alias_answer_as_none", champion_reads_only_the_missing_alias_answer_as_none)
    cases.check("job_promotion_check_champion_load_deadline_named", champion_deadline)
    cases.check("job_promotion_check_needs_tracking_uri", no_uri)
    cases.check("job_promotion_check_reads_a_champion_as_data_only_through_the_bounded_fetch", champion_is_read_as_data_only)
    cases.check("job_promotion_check_reads_no_champion_file_without_a_champion", no_champion_reads_no_champion_file)
    cases.check("job_promotion_check_admits_a_champion_by_its_closure_and_self_check_before_reading_its_model", champion_admission)
    cases.check("job_promotion_check_names_a_champion_fetch_that_reaches_its_deadline", champion_fetch_deadline)


def staged(folder: Path, *, baseline: bool = True, **options: Any) -> Fixture:
    """Return a fixture whose earlier steps (version_extract, fit, mediation, hac_validate) are complete and mounted."""
    fixture = Fixture(folder, **options)
    stage(fixture, *(step for step in ("fit", "mediation", "hac_validate") if step in training_contract.train_steps(fixture.model)))
    if baseline:
        stage_baseline(fixture)
    return fixture


def log_run_cases(cases: Any, folder: Path) -> None:
    """Log one parent run and its child runs per type, with the required tags and the completion mark last, once, even after a retry."""

    def logged() -> None:
        fixture = staged(folder / "logged")
        code, out, error = run_action(fixture, "log_run")
        require(code == 0, f"log_run failed: {error}")
        store = fixture.tracking
        require(list(store.experiments) == [f"hai_training_{HAI_TYPE}"], f"experiments: {list(store.experiments)}")
        require(len(store.active()) == 1 + len(CHILDREN) and len(store.parents()) == 1, f"runs: {len(store.active())}")
        run_id, run = next(iter(store.parents().items()))
        manifest = fixture.manifest()
        contract = json.loads((REAL / "data_contracts/training/model_contract.json").read_text())
        missing = [tag for tag in contract["required_run_tags"] if tag not in run["tags"]]
        require(not missing, f"required tags are missing: {missing}")
        base = fixture.folder / f"in/fit/{BUNDLE}"
        done = json.loads((fixture.folder / "in/fit/fit.done.json").read_text())["outputs"]
        require(run["tags"]["bundle_sha256"] == bundle.sha256_of(done, HAI_TYPE), "the completion mark is not the bundle hash")
        require(run["tags"]["reproducibility_sha256"] == file_sha256(base / "result.json"), "the reproducibility hash is not the result.json hash")
        require(
            run["tags"]["extract_sha256"] == manifest["sha256"] and run["tags"]["newest_window"] == str(manifest["newest_window_year"]), "extract tags differ"
        )
        require(run["tags"]["target_control"] == "C269" and run["tags"]["git_dirty"] == "false" and run["tags"]["master_seed"] == "42", f"tags: {run['tags']}")
        require(run["status"] == "FINISHED" and run["name"] == f"{RUN}_primary", f"parent: {run['status']} {run['name']}")
        metafile = fixture.folder / f"in/version_extract/extract_{HAI_TYPE}.parquet.dvc"
        require(run["tags"][HASH_TAG] == file_sha256(metafile) and run["tags"]["dvc_md5"] == "a" * 32, "the metafile tags differ from the metafile")
        require(run["tags"][PATH_TAG] == f"data/training/{RUN}/extract_{HAI_TYPE}.parquet", "the matrix path tag is wrong")
        snapshots = {key.removeprefix(SNAPSHOT_TAG): value for key, value in run["tags"].items() if key.startswith(SNAPSHOT_TAG)}
        require(snapshots == {table: str(value) for table, value in manifest["snapshot_set"].items()}, f"snapshot tags differ: {snapshots}")
        require("counts.holdout" in run["metrics"] and run["params"]["selected_model"], f"parent: {sorted(run['metrics'])[:5]} {run['params']}")
        children = {item["tags"]["step_key"]: item for item in store.active().values() if item["tags"].get("mlflow.parentRunId") == run_id}
        contract_children = set(contract["child_runs"])
        # An m2 run logs its own children only; the contract names the children of every model and job.
        require(set(children) == CHILDREN, f"the m2 run's children differ from its expected children: {sorted(set(children) ^ CHILDREN)}")
        require(CHILDREN.issubset(contract_children), f"the contract lacks a child of the m2 run: {sorted(CHILDREN - contract_children)}")
        require(
            "rq6_mundlak" in contract_children and "rq6_hausman" not in contract_children,
            f"the contract does not name the m6 child rq6_mundlak: {sorted(contract_children)}",
        )
        require(
            all(item["status"] == "FINISHED" and item["tags"]["run_id"] == RUN for item in children.values()), "a child was not finished or lacks its run ID"
        )
        require("m2_elastic_net.metrics.rmse" in children["m2_elastic_net"]["metrics"], f"m2: {sorted(children['m2_elastic_net']['metrics'])}")
        require(any(name.startswith("rq3_mediation.PSI_90.") for name in children["rq3_mediation"]["metrics"]), "mediation metrics are missing")
        require("hac_validation.matched_hospitals" in children["hac_validation"]["metrics"], "HAC metrics are missing")
        require(all(len(values) == 1 for item in store.active().values() for values in item["metrics"].values()), "a metric was logged twice")
        artifacts = store.artifacts(run_id)
        expected = [
            f"bundle/{name}" for name in ("result.json", "provenance.json", "holdout_predictions.parquet", "feature_list.json", "shap.json", "self_check.json")
        ]
        require(all(name in artifacts for name in expected), f"bundle artifacts: {artifacts}")
        require(
            "model/model.skops" in artifacts and "model/MLmodel" in artifacts and f"extract/manifest_{HAI_TYPE}.json" in artifacts, f"artifacts: {artifacts}"
        )
        require(f"dvc/{metafile.name}" in artifacts, f"the DVC metafile was not logged: {artifacts}")
        mediation_id = next(key for key, item in store.active().items() if item["tags"].get("step_key") == "rq3_mediation")
        require(f"result/mediation_{HAI_TYPE}.json" in store.artifacts(mediation_id), "the mediation result was not logged on its child run")
        require(output_of(fixture, "log_run.json") == {"runs": {HAI_TYPE: run_id}} and json.loads(out)["resumed"] is False, "log_run.json differs")
        verify_done(fixture.out, "log_run", DIGEST)

    def retried() -> None:
        fixture = staged(folder / "retried")
        require(run_action(fixture, "log_run")[0] == 0, "the first log_run failed")
        calls = len(fixture.tracking.calls)
        code, out, error = run_action(fixture, "log_run")
        require(code == 0 and json.loads(out)["resumed"] is True, f"the retry did not resume: {error}")
        require(len(fixture.tracking.calls) == calls, "a completed step called the tracking store again")

    def interrupted() -> None:
        fixture = staged(folder / "interrupted")
        require(run_action(fixture, "log_run")[0] == 0, "the first log_run failed")
        before = len(fixture.tracking.runs)
        (fixture.out / "log_run.done.json").unlink()
        (fixture.out / "log_run.json").unlink()
        code, _, error = run_action(fixture, "log_run")
        require(code == 0, f"the retry failed: {error}")
        require(len(fixture.tracking.runs) == before and len(fixture.tracking.parents()) == 1, "a retry of a complete run created or deleted runs")

    def unfinished() -> None:
        fixture = staged(folder / "unfinished")
        fixture.tracking.fail_on = "set_terminated"
        code, _, error = run_action(fixture, "log_run")
        require(code == job.EXIT_FAILED and failure_of(error) == "mlflow_failed", f"{code} {error}")
        require(listing(fixture.out) == [] and len(fixture.tracking.active()) == 2, "a failed log_run left output or an unexpected set of runs")
        fixture.tracking.fail_on = None
        code, _, error = run_action(fixture, "log_run")
        store = fixture.tracking
        require(code == 0 and len(store.active()) == 1 + len(CHILDREN), f"the retry did not leave one complete set of runs: {error}")
        require(len(store.runs) == len(store.active()), "the retry deleted an interrupted run instead of resuming it")
        require(all(len(values) == 1 for run in store.active().values() for values in run["metrics"].values()), "a retry duplicated a metric")

    def lost_parent_response() -> None:
        fixture = staged(folder / "lost_parent")
        store = fixture.tracking
        store.lose_create = 1
        code, _, error = run_action(fixture, "log_run")
        require(code == job.EXIT_FAILED and failure_of(error) == "mlflow_failed" and len(store.runs) == 1, f"{code} {error}")
        first = next(iter(store.runs))
        store.lose_create = None
        code, _, error = run_action(fixture, "log_run")
        require(code == 0, f"the retry failed: {error}")
        require(
            len(store.runs) == 1 + len(CHILDREN) and len(store.active()) == len(store.runs),
            f"runs after the retry: {len(store.runs)} of which {len(store.active())} active",
        )
        require(list(store.parents()) == [first] and store.experiment_tags == {"0": {}}, "the retry did not adopt the run whose response was lost")
        require(
            all(run["tags"].get("mlflow.parentRunId") == first for run_id, run in store.runs.items() if run_id != first),
            "a child is not under the adopted parent",
        )

    def lost_child_response() -> None:
        fixture = staged(folder / "lost_child")
        store = fixture.tracking
        store.lose_create = 2
        code, _, error = run_action(fixture, "log_run")
        require(code == job.EXIT_FAILED and failure_of(error) == "mlflow_failed" and len(store.runs) == 2, f"{code} {error}")
        lost = list(store.runs)[1]
        store.lose_create = None
        code, _, error = run_action(fixture, "log_run")
        require(code == 0, f"the retry failed: {error}")
        require(
            len(store.runs) == 1 + len(CHILDREN) and len(store.active()) == len(store.runs),
            f"runs after the retry: {len(store.runs)} of which {len(store.active())} active",
        )
        keys = sorted(run["tags"]["step_key"] for run in store.active().values())
        require(
            keys == sorted(set(keys)) and lost in store.runs and not store.runs[lost]["deleted"], f"a step key repeats or the lost child was replaced: {keys}"
        )
        require(all(run["status"] == "FINISHED" for run in store.active().values()), "a run was left unfinished")

    def duplicate_parents() -> None:
        fixture = staged(folder / "duplicates")
        require(run_action(fixture, "log_run")[0] == 0, "the first log_run failed")
        run = next(iter(fixture.tracking.parents().values()))
        fixture.tracking.runs["f" * 32] = {**run, "tags": dict(run["tags"])}
        (fixture.out / "log_run.done.json").unlink()
        (fixture.out / "log_run.json").unlink()
        code, _, error = run_action(fixture, "log_run")
        require(code == job.EXIT_FAILED and failure_of(error) == "mlflow_duplicate_runs", f"{code} {error}")

    def bundle_conflict() -> None:
        fixture = staged(folder / "conflict")
        require(run_action(fixture, "log_run")[0] == 0, "the first log_run failed")
        next(iter(fixture.tracking.parents().values()))["tags"]["bundle_sha256"] = "0" * 64
        (fixture.out / "log_run.done.json").unlink()
        (fixture.out / "log_run.json").unlink()
        code, _, error = run_action(fixture, "log_run")
        require(code == job.EXIT_FAILED and failure_of(error) == "mlflow_bundle_conflict", f"{code} {error}")
        require(len(fixture.tracking.active()) == 1 + len(CHILDREN), "a conflicting run was deleted")

    def markers_cleaned() -> None:
        fixture = staged(folder / "markers")
        require(run_action(fixture, "log_run")[0] == 0, "log_run failed")
        require(fixture.tracking.experiment_tags == {"0": {}}, f"a pending marker was left behind: {fixture.tracking.experiment_tags}")
        require(all("create_identity" in run["tags"] for run in fixture.tracking.active().values()), "a run was created without its create identity")

    def unresolved_marker() -> None:
        fixture = staged(folder / "unresolved")
        fixture.tracking.create_experiment(f"hai_training_{HAI_TYPE}")
        fixture.tracking.experiment_tags["0"]["pending_create." + "a" * 32] = f"{RUN}:parent"
        fixture.tracking.calls.clear()
        code, _, error = run_action(fixture, "log_run")
        require(code == job.EXIT_FAILED and failure_of(error) == "pending_create" and "create_run" not in fixture.tracking.calls, f"{code} {error}")
        require(listing(fixture.out) == [], "a refused log_run wrote output")

    def adopted_marker() -> None:
        fixture = staged(folder / "adopted")
        store = fixture.tracking
        store.create_experiment(f"hai_training_{HAI_TYPE}")
        late = store.create_run("0", tags={"run_id": RUN, "step_key": "parent", "create_identity": "b" * 32}).info.run_id
        store.experiment_tags["0"]["pending_create." + "b" * 32] = f"{RUN}:parent"
        code, _, error = run_action(fixture, "log_run")
        require(code == 0, f"the late run was not resolved: {error}")
        require(
            list(store.parents()) == [late] and len(store.runs) == len(store.active()) and store.experiment_tags == {"0": {}},
            "the late run was not adopted as the parent or the marker remained",
        )

    def no_uri() -> None:
        fixture = staged(folder / "no_uri")
        with standins(fixture), mock.patch.dict(os.environ, {"MLFLOW_TRACKING_URI": ""}):
            code, _, error = call(fixture.arguments("log_run"))
        require(code == job.EXIT_FAILED and failure_of(error) == "mlflow_uri_missing" and not fixture.tracking.calls, f"{code} {error}")

    def tracking_down() -> None:
        fixture = staged(folder / "down")
        fixture.tracking.fail_on = "create_run"
        code, _, error = run_action(fixture, "log_run")
        require(code == job.EXIT_FAILED and failure_of(error) == "mlflow_failed" and "ConnectionError" in error, f"{code} {error}")
        require(listing(fixture.out) == [], "a failed log_run wrote output")

    def changed_fit() -> None:
        fixture = staged(folder / "changed_fit")
        (fixture.folder / "in/fit" / f"{BUNDLE}/result.json").write_text("{}\n")
        code, _, error = run_action(fixture, "log_run")
        require(code == job.EXIT_FAILED and failure_of(error) == "fit_changed" and not fixture.tracking.calls, f"{code} {error}")

    def changed_hac() -> None:
        fixture = staged(folder / "changed_hac")
        (fixture.folder / "in/hac_validate" / f"hac_{HAI_TYPE}.json").write_text("{}\n")
        code, _, error = run_action(fixture, "log_run")
        require(code == job.EXIT_FAILED and failure_of(error) == "hac_validate_changed" and not fixture.tracking.calls, f"{code} {error}")

    def changed_metafile() -> None:
        fixture = staged(folder / "changed_metafile")
        path = fixture.folder / f"in/version_extract/extract_{HAI_TYPE}.parquet.dvc"
        path.write_text(path.read_text().replace("a" * 32, "b" * 32))
        code, _, error = run_action(fixture, "log_run")
        require(code == job.EXIT_FAILED and failure_of(error) == "version_extract_changed" and not fixture.tracking.calls, f"{code} {error}")

    def other_extract() -> None:
        fixture = staged(folder / "other_extract")
        fixture.rows = fixture.rows[:-1]
        fixture.write_extract(mediator=True, hac=True)
        code, _, error = run_action(fixture, "log_run")
        require(code == job.EXIT_FAILED and failure_of(error) == "fit_inputs_differ" and not fixture.tracking.calls, f"{code} {error}")

    def deadline() -> None:
        fixture = staged(folder / "deadline")
        with mock.patch.object(job, "budget", return_value=0.3), mock.patch.object(fixture.tracking, "create_run", side_effect=lambda *_, **__: time.sleep(5)):
            code, _, error = run_action(fixture, "log_run")
        require(code == job.EXIT_DEADLINE and failure_of(error) == "deadline" and listing(fixture.out) == [], f"{code} {error}")

    def metric_names() -> None:
        require(
            run_tracking.metric_name("effects.feature_matrix_x:c1__value__number") == "effects.feature_matrix_x_c1__value__number", "a metric name kept a colon"
        )
        require(len(run_tracking.metric_name("a" * 400)) == 250, "a metric name was not shortened")

    def tags_model_and_base_run() -> None:
        for model in ("m1", "m4"):
            fixture = staged(folder / f"tags_{model}", model=model)
            code, _, error = run_action(fixture, "log_run")
            require(code == 0, f"log_run of {model} failed: {error}")
            run = next(iter(fixture.tracking.parents().values()))
            require(run["tags"]["model"] == model and run["tags"]["base_run"] == BASE_RUN, f"tags: {run['tags'].get('model')} {run['tags'].get('base_run')}")
            require(run["params"]["model"] == model, f"parameters: {run['params']}")

    def logs_only_the_tests_of_its_model() -> None:
        expected = {"m1": {"m1_ols", "rq1_joint", "hac_validation"}, "m3": {"m3_mixed", "hac_validation"}}
        for model, names in expected.items():
            fixture = staged(folder / f"children_{model}", model=model)
            require(run_action(fixture, "log_run")[0] == 0, f"log_run of {model} failed")
            run_id = next(iter(fixture.tracking.parents()))
            children = {item["tags"]["step_key"] for item in fixture.tracking.active().values() if item["tags"].get("mlflow.parentRunId") == run_id}
            require(children == names, f"{model} children: {sorted(children)}")

    def training_key_differs_per_model() -> None:
        keys = set()
        for model in ("m1", "m4"):
            fixture = staged(folder / f"key_{model}", model=model)
            require(run_action(fixture, "log_run")[0] == 0, "log_run failed")
            keys.add(next(iter(fixture.tracking.parents().values()))["tags"]["training_key"])
        require(len(keys) == 2, "two models of one publication share a training key")

    def nothing_fitted_fails_after_the_records_are_saved() -> None:
        """The last action of a run that fitted none of its types fails by name, after the MLflow runs and the completion record are saved: the parent run
        and the slot's child run carry the status, and a rerun resumes and fails the same way."""
        fixture = staged(folder / "unfitted_record", model="m6", hospitals=120, adjust=single_window)
        code, out, error = run_action(fixture, "log_run")
        failure = json.loads(error.strip().splitlines()[-1])
        require(
            code == 1 and failure["failure"] == "nothing_fitted" and failure["action"] == "log_run", f"log_run of a run that fitted nothing: {code} {error}"
        )
        require(
            HAI_TYPE in failure["detail"] and "not_estimable" in failure["detail"] and "no complete repeated training hospitals" in failure["detail"], failure
        )
        verify_done(fixture.out, "log_run", DIGEST)
        require((fixture.out / "log_run.json").is_file() and json.loads(out.strip().splitlines()[-1])["action"] == "log_run", f"the step did not finish: {out}")
        run_id, run = next(iter(fixture.tracking.parents().items()))
        require(run["params"]["model_status"] == "not_estimable", f"the parent run does not name the status: {run['params']}")
        require("no complete repeated training hospitals" in run["params"]["model_reason"], f"the parent run does not name the reason: {run['params']}")
        children = {item["tags"]["step_key"]: item for item in fixture.tracking.active().values() if item["tags"].get("mlflow.parentRunId") == run_id}
        require(children["m6_fixed_effects"]["params"]["status"] == "not_estimable", f"the slot child run: {children['m6_fixed_effects']['params']}")
        calls = len(fixture.tracking.calls)
        again, _, error = run_action(fixture, "log_run")
        require(again == 1 and failure_of(error) == "nothing_fitted", f"a rerun of the run that fitted nothing: {again} {error}")
        require(len(fixture.tracking.calls) == calls and len(fixture.tracking.parents()) == 1, "the rerun logged again")

    def fitted_run_passes_with_its_status() -> None:
        """A fitted run passes and its parent run says fitted, with no reason."""
        fitted_fixture = staged(folder / "fitted_record", model="m6", hospitals=60, late=lambda row: None)
        code, _, error = run_action(fitted_fixture, "log_run")
        require(code == 0 and "not fitted" not in error, f"log_run of a fitted m6: {code} {error}")
        fitted_run = next(iter(fitted_fixture.tracking.parents().values()))
        require(
            fitted_run["params"]["model_status"] == "fitted" and "model_reason" not in fitted_run["params"], f"a fitted model's parent: {fitted_run['params']}"
        )

    def partly_fitted_passes_and_names_the_unfitted_type() -> None:
        """One type fitted and one not: the last action passes, logs both types and names the unfitted one with its reason on standard error."""
        fixture = staged(folder / "partly_record", model="m6", hai_types=("HAI_1", "HAI_2"), hospitals=120, adjust_type={"HAI_2": single_window})
        code, out, error = run_action(fixture, "log_run")
        require(code == 0, f"a partly fitted run failed: {error}")
        require(
            "fitted for HAI_1" in error and "HAI_2: not_estimable:" in error and "no complete repeated training hospitals" in error,
            f"the unfitted type is not named: {error!r}",
        )
        require(len(fixture.tracking.parents()) == 2, "a type was not logged")
        require(json.loads(out.strip().splitlines()[-1])["action"] == "log_run", f"summary: {out}")

    def only_the_last_action_judges_the_outcome() -> None:
        """For m1 to m6 the check belongs to the model's last data action (log_run, or promotion_check for m1, m2 and m4) and to no earlier one, and it reads
        the run's own slot."""
        types = ["HAI_1", "HAI_2"]
        for model in training_contract.MODELS:
            slot = training_contract.MODEL_LISTS[model][0]
            work = folder / f"outcome_{model}"
            for hai_type in types:
                target = work / "in/fit" / bundle.prefix(hai_type) / "result.json"
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text(
                    json.dumps({"statuses": {slot: {"status": "not_estimable", "reason": "nothing to estimate"}, "other": {"status": "fitted", "reason": ""}}})
                )
            write_done(work / "in/fit", "fit", DIGEST, [f"{bundle.prefix(hai_type)}/result.json" for hai_type in types])
            (work / "training_parameters.json").write_text(
                json.dumps({**{"features_run": "latest", "population": "primary", "trigger": "manual", "mlflow": "local"}, "hai_types": types, "model": model})
            )
            steps = training_contract.train_steps(model)
            for action in steps:
                context = SimpleNamespace(action=action, parameters=work / "training_parameters.json")
                try:
                    job_actions.accept_training(context, {"code_digest": DIGEST})
                except JobFailure as failure:
                    require(action == steps[-1] and failure.name == "nothing_fitted", f"{model}: {action} judged the outcome: {failure.name}")
                else:
                    require(action != steps[-1], f"{model}: the last action {action} accepted a run that fitted nothing")
            require(steps[-1] == ("promotion_check" if model in training_contract.PROMOTION_MODELS else "log_run"), f"{model}: the last action is {steps[-1]}")

    cases.check("job_log_run_logs_runs_tags_metrics_artifacts_and_metafile", logged)
    cases.check("job_log_run_nothing_fitted_fails_after_the_records_are_saved", nothing_fitted_fails_after_the_records_are_saved)
    cases.check("job_log_run_a_fitted_run_passes_with_its_status", fitted_run_passes_with_its_status)
    cases.check("job_log_run_a_partly_fitted_run_passes_and_names_the_unfitted_type", partly_fitted_passes_and_names_the_unfitted_type)
    cases.check("job_log_run_only_the_last_action_judges_the_outcome", only_the_last_action_judges_the_outcome)
    cases.check("job_log_run_tags_the_model_and_the_base_run", tags_model_and_base_run)
    cases.check("job_log_run_logs_only_the_tests_of_its_model", logs_only_the_tests_of_its_model)
    cases.check("job_log_run_training_key_differs_per_model", training_key_differs_per_model)
    cases.check("job_log_run_retry_resumes_completed", retried)
    cases.check("job_log_run_retry_after_interrupt_keeps_complete_runs", interrupted)
    cases.check("job_log_run_resumes_unfinished_runs", unfinished)
    cases.check("job_log_run_retry_adopts_parent_whose_response_was_lost", lost_parent_response)
    cases.check("job_log_run_retry_adopts_child_whose_response_was_lost", lost_child_response)
    cases.check("job_log_run_refuses_duplicate_parents", duplicate_parents)
    cases.check("job_log_run_refuses_other_bundle_hash", bundle_conflict)
    cases.check("job_log_run_leaves_no_pending_marker", markers_cleaned)
    cases.check("job_log_run_refuses_unresolved_pending_create", unresolved_marker)
    cases.check("job_log_run_adopts_late_run_of_pending_create", adopted_marker)
    cases.check("job_log_run_needs_tracking_uri", no_uri)
    cases.check("job_log_run_tracking_failure_named", tracking_down)
    cases.check("job_log_run_refuses_changed_fit_output", changed_fit)
    cases.check("job_log_run_refuses_changed_hac_output", changed_hac)
    cases.check("job_log_run_refuses_changed_metafile", changed_metafile)
    cases.check("job_log_run_refuses_fit_of_another_extract", other_extract)
    cases.check("job_log_run_deadline_named", deadline)
    cases.check("job_log_run_metric_names", metric_names)


ENTRY_MODULES = (
    "scripts/training/aws_run.py",
    "scripts/lakehouse/layer_run.py",
    "scripts/lakehouse/bronze_run.py",
    "monitoring/aws_run.py",
    "scripts/serving/deploy_job.py",
    "scripts/infrastructure/openmetadata_host_ingest.py",
)
PEAK_FIELDS = {"peak_memory_bytes", "peak_memory_source", "peak_memory_reason", "peak_scratch_bytes", "scratch_measure", "scratch_samples", "exit_code"}


def _peak_line(stream: str) -> dict[str, Any]:
    lines = [line for line in stream.splitlines() if line.startswith("event=resource_peaks ")]
    require(len(lines) == 1, f"expected one resource_peaks line, saw {len(lines)}")
    record = json.loads(lines[0].removeprefix("event=resource_peaks "))
    require(set(record) == PEAK_FIELDS, f"fields {sorted(record)}")
    return dict(record)


def _recorded(job: Callable[[], int], scratch: Path, **patches: Any) -> tuple[int | None, dict[str, Any]]:
    """Run a job through the shared recorder and return its exit code and the one log record it wrote."""
    from scripts import resource_peaks

    stream = io.StringIO()
    code: int | None = None
    with contextlib.ExitStack() as stack:
        for name, value in patches.items():
            stack.enter_context(mock.patch.object(resource_peaks, name, value))
        stack.enter_context(contextlib.redirect_stderr(stream))
        try:
            resource_peaks.recorded(job, scratch=(scratch,), interval=0.02)
        except SystemExit as stop:
            code = stop.code if isinstance(stop.code, int) else None
    return code, _peak_line(stream.getvalue())


def resource_peak_cases(cases: Any, folder: Path) -> None:
    """Every job container logs its peak memory and peak scratch use at exit, and the recording never changes the job's result."""
    scratch = folder / "scratch"

    def fields() -> None:
        scratch.mkdir(parents=True, exist_ok=True)
        code, record = _recorded(lambda: 0, scratch)
        require(code == 0 and record["exit_code"] == 0, f"exit {code} {record['exit_code']}")
        require(isinstance(record["peak_memory_bytes"], int) and record["peak_memory_bytes"] > 0, f"memory {record['peak_memory_bytes']}")
        require(record["peak_memory_source"] in {"cgroup_v2_memory_peak", "cgroup_v1_max_usage", "rusage_max_rss"}, record["peak_memory_source"])
        require(record["scratch_measure"] == "sampled_folder_allocation", record["scratch_measure"])
        require(isinstance(record["peak_scratch_bytes"], int) and record["peak_scratch_bytes"] >= 0, f"scratch {record['peak_scratch_bytes']}")

    def scratch_peak() -> None:
        scratch.mkdir(parents=True, exist_ok=True)
        size = 8 * 1024 * 1024

        def job() -> int:
            path = scratch / "spill.bin"
            with path.open("wb") as handle:
                handle.write(os.urandom(size))
                handle.flush()
                os.fsync(handle.fileno())
            time.sleep(0.3)
            path.unlink()
            return 0

        _, record = _recorded(job, scratch)
        require(record["peak_scratch_bytes"] >= size, f"peak {record['peak_scratch_bytes']} under {size}")
        require(record["scratch_samples"] >= 2, f"samples {record['scratch_samples']}")

    def unreadable_counters() -> None:
        scratch.mkdir(parents=True, exist_ok=True)

        def broken(_who: int) -> Any:
            raise OSError("no rusage")

        missing = folder / "no_cgroup"
        code, record = _recorded(lambda: 0, scratch, CGROUP_V2_PEAK=missing / "memory.peak", CGROUP_V1_PEAK=missing / "max", getrusage=broken)
        require(code == 0, f"exit {code}")
        require(record["peak_memory_bytes"] is None and record["peak_memory_source"] == "unavailable", f"{record}")
        require(bool(record["peak_memory_reason"]), "an unreadable counter names its reason")

    def cgroup_preferred() -> None:
        scratch.mkdir(parents=True, exist_ok=True)
        counter = folder / "cgroup" / "memory.peak"
        counter.parent.mkdir(parents=True, exist_ok=True)
        counter.write_text("123456789\n", encoding="utf-8")
        _, record = _recorded(lambda: 0, scratch, CGROUP_V2_PEAK=counter)
        require(record["peak_memory_bytes"] == 123456789 and record["peak_memory_source"] == "cgroup_v2_memory_peak", f"{record}")

    def failures_kept() -> None:
        scratch.mkdir(parents=True, exist_ok=True)
        code, record = _recorded(lambda: 3, scratch)
        require(code == 3 and record["exit_code"] == 3, f"exit {code} {record['exit_code']}")

        def raises() -> int:
            raise RuntimeError("job failed")

        from scripts import resource_peaks

        stream = io.StringIO()
        try:
            with contextlib.redirect_stderr(stream):
                resource_peaks.recorded(raises, scratch=(scratch,), interval=0.02)
        except RuntimeError as error:
            require(str(error) == "job failed", str(error))
        else:
            raise AssertionError("the job's exception was swallowed")
        require(_peak_line(stream.getvalue())["exit_code"] == 1, "an exception logs exit code 1")

    def broken_sampler_kept() -> None:
        scratch.mkdir(parents=True, exist_ok=True)

        def explode(_folders: Any) -> int:
            raise OSError("walk failed")

        code, record = _recorded(lambda: 0, scratch, folder_allocation=explode)
        require(code == 0, f"a failing scratch sampler changed the job's exit code to {code}")
        require(record["peak_scratch_bytes"] is None and record["scratch_measure"].startswith("unavailable"), f"{record}")

    def sampler_start_failure_kept() -> None:
        scratch.mkdir(parents=True, exist_ok=True)
        from scripts import resource_peaks

        ran: list[bool] = []

        def job() -> int:
            ran.append(True)
            return 3

        try:
            with mock.patch.object(resource_peaks._Sampler, "start", side_effect=RuntimeError("can't start new thread")):
                code, record = _recorded(job, scratch)
        except RuntimeError as error:
            raise AssertionError(f"a sampler thread that could not start stopped the job: {error}") from error
        require(ran == [True], "the job did not run when the sampler thread could not start")
        require(code == 3 and record["exit_code"] == 3, f"exit {code} {record['exit_code']}")
        require(record["peak_scratch_bytes"] is None and "did not start" in record["scratch_measure"], f"{record}")

    def sampler_construction_failure_kept() -> None:
        scratch.mkdir(parents=True, exist_ok=True)
        ran: list[bool] = []

        def job() -> int:
            ran.append(True)
            return 3

        def unbuildable(*_args: Any, **_kwargs: Any) -> Any:
            raise RuntimeError("can't create a thread lock")

        try:
            code, record = _recorded(job, scratch, _Sampler=unbuildable)
        except RuntimeError as error:
            raise AssertionError(f"a sampler that could not be built stopped the job: {error}") from error
        require(ran == [True], "the job did not run when the sampler could not be built")
        require(code == 3 and record["exit_code"] == 3, f"exit {code} {record['exit_code']}")
        require(record["peak_scratch_bytes"] is None and record["scratch_measure"].startswith("unavailable"), f"{record}")
        require(record["scratch_samples"] == 0, f"samples {record['scratch_samples']}")

    def recorder_interrupt_kept() -> None:
        scratch.mkdir(parents=True, exist_ok=True)
        from scripts import resource_peaks

        def raises() -> int:
            raise RuntimeError("job failed")

        interrupted = mock.patch.object(resource_peaks._Sampler, "join", side_effect=KeyboardInterrupt)
        written = io.StringIO()
        try:
            with interrupted, contextlib.redirect_stderr(written):
                resource_peaks.recorded(raises, scratch=(scratch,), interval=0.02)
        except RuntimeError as error:
            require(str(error) == "job failed", f"the recorder replaced the job's exception: {error}")
            # After an interrupt the recorder abandons its record: a further stderr write could block on a full pipe whose reader stopped.
            require(written.getvalue() == "", f"the recorder wrote to stderr after an interrupt: {written.getvalue()!r}")
        except BaseException as error:
            raise AssertionError(f"an interrupt inside the recorder replaced the job's exception with {type(error).__name__}") from error
        else:
            raise AssertionError("the job's exception was swallowed")

        try:
            with interrupted, contextlib.redirect_stderr(io.StringIO()):
                resource_peaks.recorded(lambda: 3, scratch=(scratch,), interval=0.02)
        except SystemExit as stop:
            require(stop.code == 3, f"an interrupt inside the recorder changed the exit code to {stop.code}")
        except BaseException as error:
            raise AssertionError(f"an interrupt inside the recorder replaced the job's exit with {type(error).__name__}") from error
        else:
            raise AssertionError("recorded returned without the job's exit")

    def startup_interrupt_silent() -> None:
        scratch.mkdir(parents=True, exist_ok=True)
        from scripts import resource_peaks

        def raises() -> int:
            raise RuntimeError("job failed")

        def unbuildable(*_args: Any, **_kwargs: Any) -> Any:
            raise KeyboardInterrupt

        for label, patch in (
            ("construction", mock.patch.object(resource_peaks, "_Sampler", side_effect=unbuildable)),
            ("start", mock.patch.object(resource_peaks._Sampler, "start", side_effect=KeyboardInterrupt)),
        ):
            # After an interrupt while the sampler is built or started, the recorder abandons its record: no stderr write that could block.
            written = io.StringIO()
            try:
                with patch, contextlib.redirect_stderr(written):
                    resource_peaks.recorded(raises, scratch=(scratch,), interval=0.02)
            except RuntimeError as error:
                require(str(error) == "job failed", f"{label}: the recorder replaced the job's exception: {error}")
            except BaseException as error:
                raise AssertionError(f"{label}: an interrupt inside the recorder replaced the job's exception with {type(error).__name__}") from error
            else:
                raise AssertionError(f"{label}: the job's exception was swallowed")
            require(written.getvalue() == "", f"{label}: the recorder wrote to stderr after an interrupt: {written.getvalue()!r}")

            written = io.StringIO()
            try:
                with patch, contextlib.redirect_stderr(written):
                    resource_peaks.recorded(lambda: 3, scratch=(scratch,), interval=0.02)
            except SystemExit as stop:
                require(stop.code == 3, f"{label}: an interrupt inside the recorder changed the exit code to {stop.code}")
            except BaseException as error:
                raise AssertionError(f"{label}: an interrupt inside the recorder replaced the job's exit with {type(error).__name__}") from error
            else:
                raise AssertionError(f"{label}: recorded returned without the job's exit")
            require(written.getvalue() == "", f"{label}: the recorder wrote to stderr after an interrupt: {written.getvalue()!r}")

    def job_interrupt_propagates() -> None:
        scratch.mkdir(parents=True, exist_ok=True)
        from scripts import resource_peaks

        def interrupted() -> int:
            raise KeyboardInterrupt

        stream = io.StringIO()
        try:
            with contextlib.redirect_stderr(stream):
                resource_peaks.recorded(interrupted, scratch=(scratch,), interval=0.02)
        except KeyboardInterrupt:
            pass
        except BaseException as error:
            raise AssertionError(f"the job's own interrupt became {type(error).__name__}") from error
        else:
            raise AssertionError("the job's own interrupt was swallowed")
        require(_peak_line(stream.getvalue())["exit_code"] == 1, "an interrupted job logs exit code 1")

    def failure_line_interrupt_kept() -> None:
        """The failure line is part of the recorder: a fault while it is written, an interrupt included, never replaces the job's result."""
        scratch.mkdir(parents=True, exist_ok=True)
        from scripts import phase_failure, resource_peaks

        class Odd(BaseException):
            pass

        def faulty(fault: type[BaseException]) -> type[io.StringIO]:
            class Faulty(io.StringIO):
                def write(self, text: str) -> int:
                    if text.startswith(phase_failure.MARKER):
                        raise fault
                    return super().write(text)

            return Faulty

        for fault in (KeyboardInterrupt, Odd, OSError, ValueError):
            Faulty = faulty(fault)

            try:
                with contextlib.redirect_stderr(Faulty()):
                    resource_peaks.recorded(lambda: 7, scratch=(scratch,), interval=0.02)
            except SystemExit as stop:
                require(stop.code == 7, f"{fault.__name__}: a fault in the failure line changed the exit code to {stop.code}")
            except BaseException as error:
                raise AssertionError(f"{fault.__name__}: a fault in the failure line replaced the job's exit with {type(error).__name__}") from error
            else:
                raise AssertionError(f"{fault.__name__}: recorded returned without the job's exit")

            def raises() -> int:
                raise RuntimeError("job failed")

            try:
                with contextlib.redirect_stderr(Faulty()):
                    resource_peaks.recorded(raises, scratch=(scratch,), interval=0.02)
            except RuntimeError as error:
                require(str(error) == "job failed", f"{fault.__name__}: the job's exception changed: {error}")
            except BaseException as error:
                raise AssertionError(f"{fault.__name__}: a fault in the failure line replaced the job's exception with {type(error).__name__}") from error
            else:
                raise AssertionError(f"{fault.__name__}: the job's exception was swallowed")

    def failure_line_silent_after_abandon() -> None:
        """After the recorder abandoned its record (an interrupt at the sampler or the join), the failure line is not written either, for any job result."""
        scratch.mkdir(parents=True, exist_ok=True)
        from scripts import phase_failure, resource_peaks

        def unbuildable(*_args: Any, **_kwargs: Any) -> Any:
            raise KeyboardInterrupt

        patches = (
            ("join", mock.patch.object(resource_peaks._Sampler, "join", side_effect=KeyboardInterrupt)),
            ("construction", mock.patch.object(resource_peaks, "_Sampler", side_effect=unbuildable)),
            ("start", mock.patch.object(resource_peaks._Sampler, "start", side_effect=KeyboardInterrupt)),
        )
        for label, patch in patches:
            written = io.StringIO()
            try:
                with patch, contextlib.redirect_stderr(written):
                    resource_peaks.recorded(lambda: 3, scratch=(scratch,), interval=0.02)
            except SystemExit as stop:
                require(stop.code == 3, f"{label}: the exit code changed to {stop.code}")
            require(
                phase_failure.MARKER not in written.getvalue(),
                f"{label}: a failure line was written after the recorder abandoned its record: {written.getvalue()!r}",
            )

    def broken_stderr_kept() -> None:
        scratch.mkdir(parents=True, exist_ok=True)
        from scripts import resource_peaks

        def closed() -> io.StringIO:
            stream = io.StringIO()
            stream.close()
            return stream

        try:
            with contextlib.redirect_stderr(closed()):
                resource_peaks.recorded(lambda: 0, scratch=(scratch,), interval=0.02)
        except SystemExit as stop:
            require(stop.code == 0, f"a closed stderr changed the exit code to {stop.code}")
        except Exception as error:
            raise AssertionError(f"a closed stderr replaced a successful result with {type(error).__name__}: {error}") from error
        else:
            raise AssertionError("recorded returned without the job's exit")

        def raises() -> int:
            raise RuntimeError("job failed")

        try:
            with contextlib.redirect_stderr(closed()):
                resource_peaks.recorded(raises, scratch=(scratch,), interval=0.02)
        except RuntimeError as error:
            require(str(error) == "job failed", f"a closed stderr replaced the job's exception: {error}")
        except Exception as error:
            raise AssertionError(f"a closed stderr masked the job's exception with {type(error).__name__}: {error}") from error
        else:
            raise AssertionError("the job's exception was swallowed")

    def unreadable_subfolder() -> None:
        root = folder / "locked_scratch"
        locked = root / "other_owner"
        locked.mkdir(parents=True, exist_ok=True)
        (root / "kept.bin").write_bytes(os.urandom(1024 * 1024))
        locked.chmod(0)
        try:
            _, record = _recorded(lambda: 0, root)
        finally:
            locked.chmod(0o700)
        require(record["scratch_measure"] == "sampled_folder_allocation", f"an unreadable folder ended the measure: {record['scratch_measure']}")
        require(record["peak_scratch_bytes"] >= 1024 * 1024, f"peak {record['peak_scratch_bytes']}")

    def entry_points() -> None:
        for relative in ENTRY_MODULES:
            tree = ast.parse((REAL / relative).read_text(encoding="utf-8"))
            guards = [node for node in tree.body if isinstance(node, ast.If) and "__main__" in ast.unparse(node.test)]
            require(len(guards) == 1, f"{relative}: one __main__ block")
            require(
                any(call in ast.unparse(guards[0]) for call in ("resource_peaks.recorded(main)", "resource_peaks.recorded(partial(main,")),
                f"{relative}: the __main__ block does not run main through resource_peaks.recorded",
            )

    cases.check("resource_peaks_record_fields", fields)
    cases.check("resource_peaks_scratch_peak_sampled", scratch_peak)
    cases.check("resource_peaks_unreadable_counters_record_null", unreadable_counters)
    cases.check("resource_peaks_cgroup_counter_preferred", cgroup_preferred)
    cases.check("resource_peaks_job_result_kept", failures_kept)
    cases.check("resource_peaks_broken_sampler_kept", broken_sampler_kept)
    cases.check("resource_peaks_sampler_start_failure_kept", sampler_start_failure_kept)
    cases.check("resource_peaks_sampler_construction_failure_kept", sampler_construction_failure_kept)
    cases.check("resource_peaks_recorder_interrupt_kept", recorder_interrupt_kept)
    cases.check("resource_peaks_startup_interrupt_silent", startup_interrupt_silent)
    cases.check("resource_peaks_failure_line_interrupt_kept", failure_line_interrupt_kept)
    cases.check("resource_peaks_failure_line_silent_after_abandon", failure_line_silent_after_abandon)
    cases.check("resource_peaks_job_interrupt_propagates", job_interrupt_propagates)
    cases.check("resource_peaks_broken_stderr_kept", broken_stderr_kept)
    cases.check("resource_peaks_unreadable_subfolder_skipped", unreadable_subfolder)
    cases.check("resource_peaks_every_job_entry_point", entry_points)


def scenarios(cases: Any, folder: Path) -> None:
    """Register every data-action case."""
    fit_cases(cases, folder / "fit")
    mediation_cases(cases, folder / "mediation")
    hac_cases(cases, folder / "hac")
    version_cases(cases, folder / "version")
    log_run_cases(cases, folder / "log_run")
    promotion_cases.gate_cases(cases)
    promotion_check_cases(cases, folder / "promotion")
    from scripts.training import auto_promotion_cases, aws_run_cases, promotion_chain_cases, research_cases

    research_cases.research_cases(cases, folder / "research")
    auto_promotion_cases.auto_promotion_cases(cases, folder / "auto_promotion")
    promotion_chain_cases.promotion_chain_cases(cases, folder / "promotion_chain")
    aws_run_cases.scenarios(cases, folder / "aws_run")
    resource_peak_cases(cases, folder / "resource_peaks")


class Selected(Cases):
    """A ledger that runs only the named case, so a single case builds only its own fixture."""

    def __init__(self, only: str | None) -> None:
        super().__init__([])
        self.only = only

    def check(self, name: str, operation: Callable[[], Any]) -> None:
        """Run the case when it is selected."""
        if self.only is None or name == self.only:
            super().check(name, operation)


def run(only: str | None = None) -> list[dict[str, Any]]:
    """Run the data-action cases, or one named case, in a temporary folder."""
    cases = Selected(only)
    with tempfile.TemporaryDirectory(prefix="hai_training_actions_") as temporary:
        scenarios(cases, Path(temporary))
    if only is not None and not cases.results:
        return [{"name": only, "passed": False, "detail": "named case is unavailable"}]
    return cases.results
