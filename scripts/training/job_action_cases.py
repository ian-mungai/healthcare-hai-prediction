"""Synthetic E2E cases for the training job's data actions, driven through the job entry point with explicit library stand-ins."""

from __future__ import annotations

import contextlib
import copy
import hashlib
import json
import os
import re
import shutil
import tempfile
import time
from collections.abc import Callable, Iterator
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest import mock

from scripts.features.training_extract import canonical, content_sha256
from scripts.training import hac_validation, job, job_inputs, mediation, run_tracking
from scripts.training.dvc_baseline import HASH_TAG, PATH_TAG, SNAPSHOT_TAG
from scripts.training.job_cases import DIGEST, arguments_for, call, failure_of, listing, prepare
from scripts.training.lane_b_cases import RUN, require
from scripts.training.model_cases import panel_standin, request
from scripts.training.model_test_support import libraries
from scripts.training.models import panel, suite
from scripts.training.models.common import SLOTS
from scripts.training.step_records import file_sha256, verify_done, write_done

REAL = Path(__file__).resolve().parents[2]
HAI_TYPE = "HAI_1"
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

    def __init__(self, folder: Path, *, hai_types: tuple[str, ...] = (HAI_TYPE,), mediator: bool = True, hac: bool = True, late_mediator: bool = False) -> None:
        self.folder = prepare(folder)
        self.extract = folder / "in/extract"
        self.extract.mkdir(parents=True)
        self.out = folder / "out"
        self.hai_types = hai_types
        self.root = folder / "code"
        self.rows = [dict(row) for row in request().data.rows]
        self.parameters = {"features_run": "latest", "population": "primary", "hai_types": list(hai_types), "trigger": "manual", "mlflow": "local"}
        (folder / "training_parameters.json").write_text(json.dumps(self.parameters) + "\n")
        self.code_root()
        self.late_mediator = late_mediator
        self.tracking = FileTracking(folder / "tracking")
        self.write_extract(mediator=mediator, hac=hac)

    def code_root(self) -> None:
        """Copy the real target lists and bridge, and write a small model configuration, feature lists and feature schema."""
        for name in (
            "config/training/targets.json",
            "config/training/excluded_controls.json",
            "dbt/seeds/gold/hai_outcome_measures_bridge.csv",
            "data_contracts/training/extract_manifest.schema.json",
        ):
            (self.root / name).parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(REAL / name, self.root / name)
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
        hac_years.write_text(json.dumps({"schema": 1, "mapping": {"2023": 2025}}) + "\n")

    def write_extract(self, *, mediator: bool, hac: bool) -> None:
        """Write the per-type extract and manifest, and the three shared tables, the way the extract step lays them out."""
        newest = max(row["window_year"] for row in self.rows)
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
        flags = [
            {
                "spine_key": row["spine_key"],
                "measure_control": "C400",
                "value_number": float(index % 2),
                "value_text": None,
                "fiscal_year": 2025,
                "alignment_status": "aligned",
            }
            for index, row in enumerate(previous)
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
        mock.patch.object(run_tracking, "open_store", side_effect=lambda uri: fixture.tracking),
        mock.patch.dict(os.environ, {"MLFLOW_TRACKING_URI": "http://mlflow:5100"}),
        libraries(),
        mock.patch.object(panel, "native_fits", side_effect=panel_standin),
    ):
        yield


class FileTracking:
    """A stand-in for the MLflow client: the calls the log_run action makes, with MLflow's rules for parameters, tags and run lookup."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.experiments: dict[str, str] = {}
        self.runs: dict[str, dict[str, Any]] = {}
        self.calls: list[str] = []
        self.fail_on: str | None = None

    def enter(self, call: str) -> None:
        """Record a call and fail it when the case asks for that failure."""
        self.calls.append(call)
        if self.fail_on == call:
            raise ConnectionError(f"synthetic tracking failure in {call}")

    def get_experiment_by_name(self, name: str) -> SimpleNamespace | None:
        self.enter("get_experiment_by_name")
        return SimpleNamespace(experiment_id=self.experiments[name]) if name in self.experiments else None

    def create_experiment(self, name: str) -> str:
        self.enter("create_experiment")
        self.experiments[name] = str(len(self.experiments))
        return self.experiments[name]

    def create_run(self, experiment_id: str, tags: dict[str, str] | None = None, run_name: str | None = None) -> SimpleNamespace:
        self.enter("create_run")
        run_id = f"{len(self.runs):032x}"
        self.runs[run_id] = {"experiment": experiment_id, "name": run_name, "status": "RUNNING", "params": {}, "metrics": {}, "tags": dict(tags or {})}
        return SimpleNamespace(info=SimpleNamespace(run_id=run_id))

    def search_runs(self, experiment_ids: list[str], filter_string: str = "", max_results: int = 1000) -> list[SimpleNamespace]:
        self.enter("search_runs")
        wanted = dict(re.findall(r"tags\.(\w+) = '([^']*)'", filter_string))
        found = [
            run_id
            for run_id, run in self.runs.items()
            if run["experiment"] in experiment_ids and all(run["tags"].get(key) == value for key, value in wanted.items())
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
        expected = [f"fit_{HAI_TYPE}.json", f"holdout_{HAI_TYPE}.json", "fit.done.json"]
        require(all(name in names for name in expected), f"missing outputs: {names}")
        result = output_of(fixture, f"fit_{HAI_TYPE}.json")
        require(set(result["statuses"]) == set(SLOTS), "fit_ result lacks a slot status")
        require(result["statuses"]["m1_ols"]["status"] == "fitted", f"OLS did not fit: {result['statuses']['m1_ols']}")
        candidate = result["selection"]["registry_candidate"]
        require(candidate is not None and f"model_{HAI_TYPE}.pkl" in names, "the registry candidate was not saved")
        holdout = output_of(fixture, f"holdout_{HAI_TYPE}.json")
        require(holdout["model"] == result["selection"]["selected"] and holdout["predictions"], "holdout predictions are missing")
        require({"spine_key", "predicted"} == set(holdout["predictions"][0]), "a holdout prediction has another shape")
        manifest = fixture.manifest()
        require(result["inputs"]["extract_sha256"] == manifest["sha256"], "fit does not record the extract it read")
        require(
            result["inputs"]["auxiliary_sha256"]["hac.parquet"] == manifest["auxiliary_files"]["hac.parquet"]["sha256"],
            "fit does not record the HAC table hash",
        )
        verify_done(fixture.out, "fit", DIGEST)
        require(json.loads(out)["action"] == "fit" and json.loads(out)["resumed"] is False, "fit printed no summary")

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
        first = {name: (fixture.out / name).read_bytes() for name in listing(fixture.out) if name.endswith(".json") and name != "fit.done.json"}
        (fixture.out / "fit.done.json").unlink()
        code, _, error = run_action(fixture, "fit")
        require(code == 0 and verify_done(fixture.out, "fit", DIGEST)["step"] == "fit", f"the retry after an interrupt failed: {error}")
        require(first == {name: (fixture.out / name).read_bytes() for name in first}, "a retry after an interrupt changed a result")

    def conflict() -> None:
        fixture = Fixture(folder / "conflict")
        require(run_action(fixture, "fit")[0] == 0, "the first fit failed")
        (fixture.out / f"fit_{HAI_TYPE}.json").write_text("{}\n")
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

    def parameters_refused() -> None:
        fixture = Fixture(folder / "parameters")
        (fixture.folder / "training_parameters.json").write_text(json.dumps(fixture.parameters | {"mlflow": "aws"}) + "\n")
        code, _, error = run_action(fixture, "fit")
        require(code == job.EXIT_FAILED and failure_of(error) == "parameters" and "not built" in error, f"{code} {error}")

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
        require(all(f"fit_{kind}.json" in names and f"holdout_{kind}.json" in names for kind in fixture.hai_types), f"missing outputs: {names}")

    def second_type_missing() -> None:
        fixture = Fixture(folder / "second_missing", hai_types=("HAI_1", "HAI_2"))
        (fixture.extract / "manifest_HAI_2.json").unlink()
        code, _, error = run_action(fixture, "fit")
        require(code == job.EXIT_FAILED and failure_of(error) == "input_missing", f"{code} {error}")
        require(listing(fixture.out) == [], "a failed type left partial output")

    cases.check("job_fit_writes_results_and_done", fitted)
    cases.check("job_fit_retry_resumes_completed", retried)
    cases.check("job_fit_retry_after_interrupt", interrupted)
    cases.check("job_fit_done_conflict", conflict)
    cases.check("job_fit_refuses_stray_outputs", stray)
    cases.check("job_fit_refuses_changed_extract", changed_extract)
    cases.check("job_fit_refuses_manifest_of_another_run", wrong_run)
    cases.check("job_fit_refuses_manifest_of_another_type", wrong_type)
    cases.check("job_fit_missing_input_named", missing_input)
    cases.check("job_fit_refuses_aws_parameters", parameters_refused)
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


def hac_cases(cases: Any, folder: Path) -> None:
    """Rank the fitted holdout against the HAC flags with the committed year map and check the refusals."""

    def written() -> None:
        fixture = Fixture(folder / "written")
        stage(fixture, "fit")
        code, _, error = run_action(fixture, "hac_validate")
        require(code == 0, f"hac_validate failed: {error}")
        result = output_of(fixture, f"hac_{HAI_TYPE}.json")
        require(result["status"] == "fitted" and result["program_year"] == 2025 and result["matched_hospitals"] > 0, f"another result: {result}")
        verify_done(fixture.out, "hac_validate", DIGEST)

    def retried() -> None:
        fixture = Fixture(folder / "retried")
        stage(fixture, "fit")
        require(run_action(fixture, "hac_validate")[0] == 0, "the first run failed")
        with mock.patch.object(hac_validation, "validate_hac", side_effect=AssertionError("a completed step must not run again")):
            code, out, error = run_action(fixture, "hac_validate")
        require(code == 0 and json.loads(out)["resumed"] is True, f"the retry did not resume: {error}")

    def no_mapping() -> None:
        fixture = Fixture(folder / "no_mapping")
        stage(fixture, "fit")
        (fixture.root / "config/training/hac_year_mapping.json").unlink()
        code, _, error = run_action(fixture, "hac_validate")
        require(code == job.EXIT_FAILED and failure_of(error) == "hac_year_mapping_missing", f"{code} {error}")
        require(listing(fixture.out) == [], "a refused step wrote output")

    def unmapped_year() -> None:
        fixture = Fixture(folder / "unmapped")
        stage(fixture, "fit")
        (fixture.root / "config/training/hac_year_mapping.json").write_text(json.dumps({"schema": 1, "mapping": {"2019": 2021}}) + "\n")
        code, _, error = run_action(fixture, "hac_validate")
        require(code == job.EXIT_FAILED and failure_of(error) == "hac_validate_refused" and "year mapping" in error, f"{code} {error}")

    def changed_fit() -> None:
        fixture = Fixture(folder / "changed_fit")
        stage(fixture, "fit")
        (fixture.folder / "in/fit" / f"holdout_{HAI_TYPE}.json").write_text("{}\n")
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
    cases.check("job_hac_retry_resumes_completed", retried)
    cases.check("job_hac_year_mapping_missing_named", no_mapping)
    cases.check("job_hac_refuses_unmapped_year", unmapped_year)
    cases.check("job_hac_refuses_changed_fit_output", changed_fit)
    cases.check("job_hac_refuses_changed_table", changed_table)
    cases.check("job_hac_reports_empty_flag_table", no_flags)
    cases.check("job_hac_deadline_named", deadline)


def staged(folder: Path, **options: Any) -> Fixture:
    """Return a fixture whose earlier steps (version_extract, fit, mediation, hac_validate) are complete and mounted."""
    fixture = Fixture(folder, **options)
    stage(fixture, "fit", "mediation", "hac_validate")
    stage_baseline(fixture)
    return fixture


def log_run_cases(cases: Any, folder: Path) -> None:
    """Log one MLflow run per type with params, metrics, artifacts, the DVC metafile and the snapshot tags, once, even after a retry."""

    def logged() -> None:
        fixture = staged(folder / "logged")
        code, out, error = run_action(fixture, "log_run")
        require(code == 0, f"log_run failed: {error}")
        store = fixture.tracking
        require(len(store.runs) == 1, f"expected one run, found {len(store.runs)}")
        run_id, run = next(iter(store.runs.items()))
        manifest = fixture.manifest()
        metafile = fixture.folder / f"in/version_extract/extract_{HAI_TYPE}.parquet.dvc"
        require(run["status"] == "FINISHED" and run["tags"][run_tracking.COMPLETE_TAG] == "true", "the run was not finished and marked complete")
        require(run["tags"][HASH_TAG] == file_sha256(metafile), "the metafile hash tag differs from the metafile")
        require(run["tags"][PATH_TAG] == f"data/training/{RUN}/extract_{HAI_TYPE}.parquet", "the matrix path tag is wrong")
        snapshots = {key.removeprefix(SNAPSHOT_TAG): value for key, value in run["tags"].items() if key.startswith(SNAPSHOT_TAG)}
        require(snapshots == {table: str(value) for table, value in manifest["snapshot_set"].items()}, f"snapshot tags differ: {snapshots}")
        require(run["tags"][run_tracking.RUN_TAG] == RUN and run["tags"]["hai_type"] == HAI_TYPE, "the run does not name its training run and type")
        require(
            run["params"]["master_seed"] == "42" and run["params"]["selected_model"] and run["params"]["extract_sha256"] == manifest["sha256"],
            f"params: {run['params']}",
        )
        require("holdout_rmse.m1_ols" in run["metrics"] and "counts.holdout" in run["metrics"], f"metrics: {sorted(run['metrics'])[:8]}")
        require(
            any(name.startswith("mediation.PSI_90.") for name in run["metrics"]) and "hac.matched_hospitals" in run["metrics"],
            "mediation or HAC metrics are missing",
        )
        require(all(len(values) == 1 for values in run["metrics"].values()), "a metric was logged twice")
        artifacts = store.artifacts(run_id)
        expected = [f"fit/fit_{HAI_TYPE}.json", f"fit/holdout_{HAI_TYPE}.json", f"mediation/mediation_{HAI_TYPE}.json", f"hac/hac_{HAI_TYPE}.json"]
        require(all(name in artifacts for name in expected) and f"model/model_{HAI_TYPE}.pkl" in artifacts, f"artifacts: {artifacts}")
        require(f"dvc/{metafile.name}" in artifacts, f"the DVC metafile was not logged: {artifacts}")
        record = output_of(fixture, "log_run.json")
        require(record["runs"] == {HAI_TYPE: run_id} and json.loads(out)["resumed"] is False, f"log_run.json: {record}")
        verify_done(fixture.out, "log_run", DIGEST)

    def retried() -> None:
        fixture = staged(folder / "retried")
        require(run_action(fixture, "log_run")[0] == 0, "the first log_run failed")
        calls = len(fixture.tracking.calls)
        code, out, error = run_action(fixture, "log_run")
        require(code == 0 and json.loads(out)["resumed"] is True, f"the retry did not resume: {error}")
        require(len(fixture.tracking.calls) == calls and len(fixture.tracking.runs) == 1, "a completed step called the tracking store again")

    def interrupted() -> None:
        fixture = staged(folder / "interrupted")
        require(run_action(fixture, "log_run")[0] == 0, "the first log_run failed")
        (fixture.out / "log_run.done.json").unlink()
        (fixture.out / "log_run.json").unlink()
        code, _, error = run_action(fixture, "log_run")
        require(code == 0, f"the retry failed: {error}")
        runs = fixture.tracking.runs
        require(len(runs) == 1 and all(len(values) == 1 for values in next(iter(runs.values()))["metrics"].values()), "a retry duplicated a run or a metric")

    def partial_run() -> None:
        fixture = staged(folder / "partial")
        fixture.tracking.fail_on = "set_terminated"
        code, _, error = run_action(fixture, "log_run")
        require(code == job.EXIT_FAILED and failure_of(error) == "mlflow_failed", f"{code} {error}")
        require(listing(fixture.out) == [] and len(fixture.tracking.runs) == 1, "a failed log_run left output or no run to resume")
        fixture.tracking.fail_on = None
        code, _, error = run_action(fixture, "log_run")
        run = next(iter(fixture.tracking.runs.values()))
        require(code == 0 and len(fixture.tracking.runs) == 1 and run["status"] == "FINISHED", f"the retry did not finish the same run: {error}")
        require(all(len(values) == 1 for values in run["metrics"].values()), "a retry duplicated a metric")

    def duplicate_runs() -> None:
        fixture = staged(folder / "duplicates")
        require(run_action(fixture, "log_run")[0] == 0, "the first log_run failed")
        run = next(iter(fixture.tracking.runs.values()))
        fixture.tracking.runs["f" * 32] = {**run, "tags": dict(run["tags"])}
        (fixture.out / "log_run.done.json").unlink()
        (fixture.out / "log_run.json").unlink()
        code, _, error = run_action(fixture, "log_run")
        require(code == job.EXIT_FAILED and failure_of(error) == "mlflow_duplicate_runs", f"{code} {error}")

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
        (fixture.folder / "in/fit" / f"fit_{HAI_TYPE}.json").write_text("{}\n")
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

    cases.check("job_log_run_logs_params_metrics_artifacts_and_tags", logged)
    cases.check("job_log_run_retry_resumes_completed", retried)
    cases.check("job_log_run_retry_after_interrupt_keeps_one_run", interrupted)
    cases.check("job_log_run_resumes_unfinished_run", partial_run)
    cases.check("job_log_run_refuses_duplicate_runs", duplicate_runs)
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
    log_run_cases(cases, folder / "log_run")


def run(only: str | None = None) -> list[dict[str, Any]]:
    """Run the data-action cases, or one named case, in a temporary folder."""
    from scripts.training.lane_b_cases import Cases

    cases = Cases([])
    with tempfile.TemporaryDirectory(prefix="hai_training_actions_") as temporary:
        scenarios(cases, Path(temporary))
    if only is None:
        return cases.results
    selected = [result for result in cases.results if result["name"] == only]
    return selected or [{"name": only, "passed": False, "detail": "named case is unavailable"}]
