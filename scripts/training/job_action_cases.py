"""Synthetic E2E cases for the training job's data actions, driven through the job entry point with explicit library stand-ins."""

from __future__ import annotations

import ast
import contextlib
import copy
import hashlib
import json
import os
import re
import shutil
import tempfile
import time
from collections.abc import Callable, Iterator, Sequence
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest import mock

from scripts.features.training_extract import canonical, content_sha256
from scripts.training import bundle, dvc_baseline, guards, hac_validation, job, job_inputs, mediation, promotion, promotion_cases, promotion_check, run_tracking
from scripts.training.dvc_baseline import HASH_TAG, PATH_TAG, SNAPSHOT_TAG
from scripts.training.job_cases import DIGEST, arguments_for, call, failure_of, listing, prepare
from scripts.training.lane_b_cases import RUN, Cases, require
from scripts.training.model_cases import panel_standin, request
from scripts.training.model_test_support import libraries
from scripts.training.models import panel, suite
from scripts.training.models.common import REGISTRABLE, SLOTS
from scripts.training.step_records import file_sha256, verify_done, write_done

REAL = Path(__file__).resolve().parents[2]
GUARD = {"analysis_plan_sha256": "7" * 64, "targets_sha256": "8" * 64, "training_config_sha256": "9" * 64}
SAVED: dict[str, Any] = {}
CHILDREN = {*SLOTS, "rq1_joint", "rq3_mediation", "rq4_nested", "rq5_moran", "rq6_hausman", "hac_validation"}
HAI_TYPE = "HAI_1"
BUNDLE = f"bundles/bundle_{HAI_TYPE}"
LABEL = "hai_1__sir__number"
SQL_TYPES = {"varchar": "VARCHAR", "double": "DOUBLE", "bigint": "BIGINT", "boolean": "BOOLEAN", "date": "DATE"}
FIXTURE_COLUMNS = {
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


class Fixture:
    """One run folder with the mounted extract inputs, a small code root and the job's output folder."""

    def __init__(
        self,
        folder: Path,
        *,
        hai_types: tuple[str, ...] = (HAI_TYPE,),
        mediator: bool = True,
        hac: bool = True,
        late_mediator: bool = False,
        years: Sequence[int] = range(2019, 2024),
        hospitals: int = 48,
        period_years: int = 2,
        adjust: Callable[[list[dict[str, Any]]], None] | None = None,
        flags: dict[str, float] | None = None,
    ) -> None:
        self.folder = prepare(folder)
        self.extract = folder / "in/extract"
        self.extract.mkdir(parents=True)
        self.out = folder / "out"
        self.hai_types = hai_types
        self.root = folder / "code"
        self.rows = [dict(row) for row in request(years, hospitals).data.rows]
        if adjust is not None:
            adjust(self.rows)
        self.newest, self.period_years, self.flag_values = max(row["window_year"] for row in self.rows), period_years, flags
        self.parameters = {"features_run": "latest", "population": "primary", "hai_types": list(hai_types), "trigger": "manual", "mlflow": "local"}
        (folder / "training_parameters.json").write_text(json.dumps(self.parameters) + "\n")
        self.code_root()
        self.late_mediator = late_mediator
        self.tracking = FileTracking(folder / "tracking")
        self.champion: Predictor | None = None
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
        for slot, references in SLOT_LISTS.items():
            (self.root / f"config/training/feature_lists/{slot}.json").write_text(
                json.dumps({"schema": 1, "scope": "cms_only", "slot": slot, "references": list(references)}) + "\n"
            )
        tables: dict[str, Any] = {}
        for view, family, name, kind, pivot in SCHEMA_COLUMNS:
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
        for hai_type in self.hai_types:
            name = f"extract_{hai_type}.parquet"
            label = f"hai_{hai_type.split('_')[1]}__sir__number"
            rows = [{key: value for key, value in row.items() if key != LABEL} | {label: row[LABEL]} for row in self.rows]
            columns = {key: kind for key, kind in FIXTURE_COLUMNS.items() if key != LABEL} | {label: "double"}
            years = [row["window_year"] for row in self.rows]
            manifest = {
                "schema_version": 1,
                "kind": "training_extract",
                "run_id": RUN,
                "hai_type": hai_type,
                "population": "primary",
                "parameters_sha256": hashlib.sha256(canonical(self.parameters)).hexdigest(),
                "code_digest": DIGEST,
                "publication_run_id": "20261023T000000Z-1234abcd",
                "features_namespace": "features_e2e",
                "snapshot_set": {"feature_matrix_care_compare": 101, "feature_matrix_hospital_window": 102, "feature_matrix_hospital": 103},
                "features_snapshot_set_sha256": "1" * 64,
                "gold_run_id": "20261022T000000Z-1234abcd",
                "gold_snapshot_set_sha256": "2" * 64,
                "feature_schema_sha256": "3" * 64,
                "hai_outcome_controls_sha256": "4" * 64,
                "feature_list_sha256": "5" * 64,
                "writer_lock_sha256": "6" * 64,
                **table_entry(self.extract, name, rows, columns),
                "label_column": label,
                "oldest_window_year": min(years),
                "newest_window_year": max(years),
                "counts": {"spine": len(self.rows), "population_excluded": 0, "label_excluded": 0, "included": len(self.rows)},
                "auxiliary_files": shared,
            }
            (self.extract / f"manifest_{hai_type}.json").write_bytes(canonical(manifest) + b"\n")

    def arguments(self, action: str, out: Path | None = None) -> list[str]:
        """Return the rendered option set for one action of this run, writing to the action folder or to a mounted input folder."""
        arguments = arguments_for(action, self.folder)
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
        mock.patch.dict(
            os.environ, {"MLFLOW_TRACKING_URI": "http://mlflow:5100", "DVC_REMOTE_URL": str(fixture.folder / "dvc_remote"), "DVC_NO_ANALYTICS": "1"}
        ),
        libraries(),
        mock.patch.object(panel, "native_fits", side_effect=panel_standin),
    ):
        yield


class NotFound(Exception):
    """The error MLflow raises for a missing entity, with its error code."""

    error_code = "RESOURCE_DOES_NOT_EXIST"


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
        self.models[name]["aliases"][alias] = version

    def get_model_version_by_alias(self, name: str, alias: str) -> SimpleNamespace:
        self.enter("get_model_version_by_alias")
        if alias not in self.models.get(name, {"aliases": {}})["aliases"]:
            raise NotFound(alias)
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
        return SimpleNamespace(info=SimpleNamespace(run_id=run_id, status=run["status"]), data=data)

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
    (folder / "model.bin").write_text(str(id(pipeline)))
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
        require(set(result["statuses"]) == set(SLOTS) and result["statuses"]["m1_ols"]["status"] == "fitted", f"statuses: {result['statuses']}")
        manifest = fixture.manifest()
        require(result["input_hashes"]["extract_sha256"] == manifest["sha256"], "result.json does not record the extract it read")
        provenance = output_of(fixture, f"{BUNDLE}/provenance.json")
        require(set(contract["provenance_fields"]) <= set(provenance) and provenance["run_id"] == RUN, f"provenance: {sorted(provenance)}")
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

    def retried() -> None:
        fixture = Fixture(folder / "retried")
        require(run_action(fixture, "fit")[0] == 0, "the first fit failed")
        before = {name: (fixture.out / name).read_bytes() for name in listing(fixture.out)}
        with mock.patch.object(suite, "fit_models", side_effect=AssertionError("a completed fit must not run again")):
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
        fixture.rewrite_manifest(lambda manifest: manifest.update(run_id="20261024T000000Z-ffffffff"))
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
        with mock.patch.object(job, "budget", return_value=0.3), mock.patch.object(suite, "fit_models", side_effect=lambda *_: time.sleep(5)):
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
        fixture = Fixture(folder / "trust_list")
        seen: list[Any] = []
        build = bundle.build

        def spy(model_suite: Any, *rest: Any) -> Any:
            seen.append(model_suite)
            return build(model_suite, *rest)

        with mock.patch.object(bundle, "build", side_effect=spy):
            code, _, error = run_action(fixture, "fit")
        require(code == 0 and seen, f"fit failed: {error}")
        held = {
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

    cases.check("job_fit_writes_results_and_done", fitted)
    cases.check("job_fit_trust_list_covers_candidates", trust_list_covers_candidates)
    cases.check("job_fit_refuses_untrusted_candidate_type", untrusted_type_refused)
    cases.check("job_fit_result_is_reproducible", reproducible)
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

    cases.check("job_mediation_writes_result_and_done", written)
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
        require(result["hospitals_one_window"] == 1 and result["matched_hospitals"] == 48 and result["prediction_rows"] == 95, f"counts: {result}")

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
    cases.check("job_hac_averages_a_one_window_hospital_and_reports_it", one_window_hospital)
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


def seed_champion(fixture: Fixture, *, newest_window: str = "2023", columns: tuple[str, ...] = ("process", "cmi", "extra")) -> str:
    """Put a production version of the type in the registry stand-in, with the source run and feature list the check reads."""
    store = fixture.tracking
    name = f"hai_model_{HAI_TYPE}"
    store.models[name] = {"tags": {}, "versions": [], "aliases": {}}
    run = store.create_run("0", tags={"newest_window": newest_window, "run_id": "20250101T000000Z-aaaaaaaa", "step_key": "parent"}).info.run_id
    target = store.root / run / "bundle"
    target.mkdir(parents=True)
    (target / "feature_list.json").write_text(json.dumps({"slot": "m1_ols", "references": [], "columns": list(columns)}) + "\n")
    store.create_model_version(name, f"runs:/{run}/model", run_id=run, tags={"bundle_sha256": "e" * 64})
    store.set_registered_model_alias(name, "production", "1")
    store.calls.clear()
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

    def loader(uri: str) -> Any:
        if uri.startswith("models:/"):
            if fixture.champion is None:
                raise OSError("synthetic champion cannot be loaded")
            return fixture.champion
        return SAVED[(Path(uri) / "model.bin").read_text()]

    del label
    with mock.patch.object(promotion_check, "load_pipeline", side_effect=loader):
        return run_action(fixture, "promotion_check")


def promotion_check_cases(cases: Any, folder: Path) -> None:
    """Register the candidate, compare it with the champion and record the verdict, without ever moving production."""
    name = f"hai_model_{HAI_TYPE}"

    def first_version() -> None:
        fixture = promoted(folder / "first")
        code, out, error = run_promotion(fixture)
        require(code == 0, f"promotion_check failed: {error}")
        store = fixture.tracking
        version = store.models[name]["versions"][0]
        require(version["version"] == "1" and version["tags"]["promotion_verdict"] == "no_champion", f"version: {version}")
        require(store.models[name]["aliases"] == {}, f"a first version must not set an alias: {store.models[name]['aliases']}")
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
            document["champion_version"] is None and document["challenger_version"] == "1" and document["verdict"]["verdict"] == "no_champion",
            f"comparison: {document}",
        )
        require(
            version["tags"]["comparison_sha256"] == hashlib.sha256((fixture.out / f"comparison_{HAI_TYPE}.json").read_bytes()).hexdigest(),
            "the comparison hash tag differs",
        )
        table = read_parquet(fixture.out / f"comparison_{HAI_TYPE}.parquet")
        require(set(table[0]) == set(promotion.COLUMNS) and all(row["champion_prediction"] is None for row in table), f"comparison rows: {sorted(table[0])}")
        promotion_run = [run for run in store.active().values() if run["tags"].get("step_key") == "promotion"]
        require(
            len(promotion_run) == 1 and promotion_run[0]["status"] == "FINISHED" and "bundle_sha256" in promotion_run[0]["tags"],
            "the promotion child run is missing or incomplete",
        )
        require(promotion_run[0]["tags"]["mlflow.parentRunId"] == next(iter(store.parents())), "the promotion run is not a child of the parent run")
        verify_done(fixture.out, "promotion_check", DIGEST)

    def better_challenger() -> None:
        fixture = promoted(folder / "better", Predictor(LABEL, constant=3.0))
        code, _, error = run_promotion(fixture)
        require(code == 0, f"promotion_check failed: {error}")
        store = fixture.tracking
        require(store.models[name]["aliases"] == {"production": "1", "challenger": "2"}, f"aliases: {store.models[name]['aliases']}")
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
        require("set_registered_model_alias" in fixture.tracking.calls, "the case did not reach the alias step")
        require(fixture.tracking.models[name]["aliases"]["production"] == "1", "the production alias moved")

    def holdout_leak() -> None:
        fixture = promoted(folder / "leak", Predictor(LABEL, constant=3.0))
        for run in fixture.tracking.runs.values():
            if run["tags"].get("newest_window") == "2023" and run["tags"].get("step_key") == "parent" and run["tags"]["run_id"].startswith("2025"):
                run["tags"]["newest_window"] = "2024"
        code, _, error = run_promotion(fixture)
        require(code == job.EXIT_FAILED and failure_of(error) == "holdout_leak" and listing(fixture.out) == [], f"{code} {error}")

    def champion_columns() -> None:
        fixture = promoted(folder / "columns", Predictor(LABEL, constant=3.0))
        feature_list = next(fixture.tracking.root.glob("*/bundle/feature_list.json"))
        feature_list.write_text(json.dumps({"slot": "m1_ols", "references": [], "columns": ["process", "unknown_column"]}) + "\n")
        code, _, error = run_promotion(fixture)
        require(code == job.EXIT_FAILED and failure_of(error) == "champion_features", f"{code} {error}")

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
        next(iter(fixture.tracking.parents().values()))["tags"]["git_dirty"] = "true"
        code, _, error = run_promotion(fixture)
        require(code == 0, f"promotion_check failed: {error}")
        verdict = fixture.tracking.models[name]["versions"][0]["tags"]["promotion_verdict"]
        require(verdict.startswith("fail:") and "provenance" in verdict, f"a dirty tree was not failed: {verdict}")

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

    def champion_deadline() -> None:
        fixture = promoted(folder / "champion_deadline", Predictor(LABEL, constant=3.0))

        def slow_champion(uri: str) -> Any:
            if uri.startswith("models:/"):
                time.sleep(5)
            return SAVED[(Path(uri) / "model.bin").read_text()]

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

    cases.check("job_promotion_check_registers_first_version_without_alias", first_version)
    cases.check("job_promotion_check_sets_challenger_on_pass", better_challenger)
    cases.check("job_promotion_check_fails_worse_challenger_without_alias", worse_challenger)
    cases.check("job_promotion_check_never_moves_production", production_untouched)
    cases.check("job_promotion_check_refuses_sensitivity_population", sensitivity_population)
    cases.check("job_promotion_check_retry_adopts_run_whose_response_was_lost", lost_child_response)
    cases.check("job_promotion_check_refuses_holdout_leak", holdout_leak)
    cases.check("job_promotion_check_refuses_champion_columns_outside_extract", champion_columns)
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
    cases.check("job_promotion_check_champion_load_deadline_named", champion_deadline)
    cases.check("job_promotion_check_needs_tracking_uri", no_uri)


def staged(folder: Path, *, baseline: bool = True, **options: Any) -> Fixture:
    """Return a fixture whose earlier steps (version_extract, fit, mediation, hac_validate) are complete and mounted."""
    fixture = Fixture(folder, **options)
    stage(fixture, "fit", "mediation", "hac_validate")
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
        require(set(children) == CHILDREN, f"children: {sorted(children)}")
        require(
            all(item["status"] == "FINISHED" and item["tags"]["run_id"] == RUN for item in children.values()), "a child was not finished or lacks its run ID"
        )
        require("m1_ols.metrics.rmse" in children["m1_ols"]["metrics"], f"m1: {sorted(children['m1_ols']['metrics'])}")
        require(any(name.startswith("rq3_mediation.PSI_90.") for name in children["rq3_mediation"]["metrics"]), "mediation metrics are missing")
        require("hac_validation.matched_hospitals" in children["hac_validation"]["metrics"], "HAC metrics are missing")
        require(all(len(values) == 1 for item in store.active().values() for values in item["metrics"].values()), "a metric was logged twice")
        artifacts = store.artifacts(run_id)
        expected = [f"bundle/{name}" for name in ("result.json", "provenance.json", "holdout_predictions.parquet", "feature_list.json", "shap.json")]
        require(all(name in artifacts for name in expected), f"bundle artifacts: {artifacts}")
        require("model/model.bin" in artifacts and "model/MLmodel" in artifacts and f"extract/manifest_{HAI_TYPE}.json" in artifacts, f"artifacts: {artifacts}")
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

    cases.check("job_log_run_logs_runs_tags_metrics_artifacts_and_metafile", logged)
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


def scenarios(cases: Any, folder: Path) -> None:
    """Register every data-action case."""
    fit_cases(cases, folder / "fit")
    mediation_cases(cases, folder / "mediation")
    hac_cases(cases, folder / "hac")
    version_cases(cases, folder / "version")
    log_run_cases(cases, folder / "log_run")
    promotion_cases.gate_cases(cases)
    promotion_check_cases(cases, folder / "promotion")
    from scripts.training import aws_run_cases

    aws_run_cases.scenarios(cases, folder / "aws_run")


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
