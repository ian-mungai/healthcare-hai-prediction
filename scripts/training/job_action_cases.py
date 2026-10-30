"""Synthetic E2E cases for the training job's data actions, driven through the job entry point with explicit library stand-ins."""

from __future__ import annotations

import contextlib
import copy
import hashlib
import json
import shutil
import tempfile
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any
from unittest import mock

from scripts.features.training_extract import canonical, content_sha256
from scripts.training import job, job_inputs
from scripts.training.job_cases import DIGEST, arguments_for, call, failure_of, listing, prepare
from scripts.training.lane_b_cases import RUN, require
from scripts.training.model_cases import panel_standin, request
from scripts.training.model_test_support import libraries
from scripts.training.models import panel, suite
from scripts.training.models.common import SLOTS
from scripts.training.step_records import file_sha256, verify_done

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

    def __init__(self, folder: Path, *, hai_types: tuple[str, ...] = (HAI_TYPE,), mediator: bool = True, hac: bool = True) -> None:
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
                "period_start": __import__("datetime").date(row["window_year"] - 1, 1, 1),
                "period_end": __import__("datetime").date(row["window_year"] - 1, 12, 31),
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

    def arguments(self, action: str) -> list[str]:
        """Return the rendered option set for one action of this run."""
        return arguments_for(action, self.folder)

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
    """Run the job with the fixture's code root, the host Parquet reader and the explicit numerical library stand-ins."""
    with (
        mock.patch.object(job_inputs, "ROOT", fixture.root),
        mock.patch.object(job_inputs, "read_rows", read_parquet),
        libraries(),
        mock.patch.object(panel, "native_fits", side_effect=panel_standin),
    ):
        yield


def run_action(fixture: Fixture, action: str) -> tuple[int, str, str]:
    """Run one action of the fixture's run through the job entry point."""
    with standins(fixture):
        return call(fixture.arguments(action))


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


def scenarios(cases: Any, folder: Path) -> None:
    """Register every data-action case."""
    fit_cases(cases, folder / "fit")


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
