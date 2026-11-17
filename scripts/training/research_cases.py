"""Synthetic E2E cases for the research action: the tests that need two models and the one challenger of a type, driven through the job entry point."""

from __future__ import annotations

import contextlib
import json
import shutil
from collections.abc import Callable
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest import mock

from orchestration import training_contract
from scripts.training import aws_run_cases, job, promote_receipt, run_tracking
from scripts.training.job_action_cases import BASE_RUN, HAI_TYPE, LABEL, FileTracking, Fixture, Predictor, promoted, require, run_promotion, standins
from scripts.training.job_cases import arguments_for, call, failure_of, listing, prepare
from scripts.training.presigned_probe import ProbingClient

RESEARCH_RUN = "research_1234abcd"
RUNS = {"m1": "training_ols_1234abce", "m2": "training_elastic_net_1234abcf", "m4": "training_gradient_boosting_1234abc0"}
NAME = f"hai_model_{HAI_TYPE}"
PARAMETERS = {"base_run": BASE_RUN, "population": "primary", "hai_types": [HAI_TYPE], "mlflow": "local"}


def finished_models(
    folder: Path,
    models: tuple[str, ...] = ("m1", "m2", "m4"),
    tracking: FileTracking | None = None,
    runs: dict[str, str] = RUNS,
    **options: dict[str, Any],
) -> tuple[FileTracking, dict[str, Fixture]]:
    """Run log_run and the promotion check of each model into one shared tracking store, the way separate model runs leave it."""
    tracking = tracking if tracking is not None else FileTracking(folder / "tracking")
    fixtures: dict[str, Fixture] = {}
    for position, model in enumerate(models):
        fixture = promoted(folder / model, run=runs[model], model=model, tracking=tracking, **options.get(model, {}))
        fixture.candidate = Predictor(LABEL, offset=0.01 * position)
        code, _, error = run_promotion(fixture)
        require(code == 0, f"the promotion check of {model} failed: {error}")
        fixtures[model] = fixture
    return tracking, fixtures


def research(folder: Path, anchor: Fixture, opening: Callable[[str], Any] | None = None, run: str = RESEARCH_RUN, **changes: Any) -> tuple[int, str, str, Path]:
    """Run the research action on the shared store through the job entry point and return its result and output folder.

    ``opening`` replaces the store stand-in's ``open_store`` inside the run, so a case can drive the real ``run_tracking.open_store``.
    """
    work = prepare(folder, run_id=run)
    (work / "training_parameters.json").write_text(json.dumps(PARAMETERS | changes) + "\n")
    arguments = arguments_for("research", work)
    arguments[arguments.index("--run") + 1] = run
    with standins(anchor), contextlib.ExitStack() as stack:
        if opening is not None:
            stack.enter_context(mock.patch.object(run_tracking, "open_store", opening))
        code, out, error = call(arguments)
    return code, out, error, work / "out"


def parent_of(tracking: FileTracking, model: str) -> dict[str, Any]:
    """Return the parent run a model logged."""
    return next(run for run in tracking.parents().values() if run["tags"]["model"] == model)


def research_cases(cases: Any, folder: Path) -> None:
    """The research action compares two models, chooses one challenger per type and refuses by name when the models are not all there."""

    def compares_and_chooses() -> None:
        tracking, fixtures = finished_models(folder / "all")
        code, out, error, output = research(folder / "all_research", fixtures["m2"])
        require(code == 0, f"research failed: {error}")
        document = json.loads((output / f"research_{HAI_TYPE}.json").read_text())
        require(document["base_run"] == BASE_RUN and document["runs"] == RUNS, f"runs: {document['runs']}")
        require(document["rq1"]["joint_process"]["status"] == "fitted" and "p_value" in document["rq1"]["joint_process"], f"rq1: {document['rq1']}")
        require(
            document["rq1"]["process_vs_intercept_cmi"]["status"] == "fitted" and "rmse_improvement" in document["rq1"]["process_vs_intercept_cmi"],
            f"rq1 baseline: {document['rq1']}",
        )
        rq4 = document["rq4"]
        require(rq4["status"] == "fitted" and rq4["extra_degrees_of_freedom"] > 0 and 0 <= rq4["f_p_value"] <= 1 and len(rq4["interval"]) == 2, f"rq4: {rq4}")
        by_model = {item["model"]: item for item in document["promotion"]["candidates"]}
        require(set(by_model) == {"m1", "m2", "m4"}, f"candidates: {sorted(by_model)}")
        require(by_model["m1"]["verdict"] == "pass_baseline" and by_model["m1"]["eligible"], f"m1 has no baseline: {by_model['m1']}")
        eligible = [item for item in by_model.values() if item["eligible"]]
        require(len(eligible) == 3, f"m1, m2 and m4 pass the baseline: {by_model}")
        best = min(eligible, key=lambda item: item["rmse"])
        require(document["promotion"]["challenger"]["model"] == best["model"], f"challenger: {document['promotion']['challenger']}, best {best}")
        offline = document["promotion"]["challenger_predictions"]
        require(offline and all(key.count(":") == 1 for key in offline), f"the record holds no offline predictions of the challenger: {offline}")
        aliases = tracking.models[NAME]["aliases"]
        require(aliases == {"challenger": best["version"]}, f"aliases: {aliases}")
        logged = [run for run in tracking.active().values() if run["tags"].get("step_key") == "research"]
        require(
            len(logged) == 1 and logged[0]["tags"]["base_run"] == BASE_RUN and "rq4.f_statistic" in logged[0]["metrics"],
            "the research run is missing or incomplete",
        )

    def never_moves_production() -> None:
        tracking, fixtures = finished_models(folder / "production")
        tracking.models[NAME]["aliases"]["production"] = "1"
        code, _, error, output = research(folder / "production_research", fixtures["m2"])
        require(code == 0, f"research failed: {error}")
        require(tracking.models[NAME]["aliases"]["production"] == "1", "research moved the production alias")
        document = json.loads((output / f"research_{HAI_TYPE}.json").read_text())
        require(document["promotion"]["production_version"] == "1", f"the record does not name the Production version: {document['promotion']}")

    def records_the_features_runs_the_models_trained_on_and_no_production() -> None:
        """The record names, for each model, the features run its training run read (the tag of its parent run, not a run derived from the base run), and
        records that no Production model exists (``production_version`` is None, present in the record) so the promotion chain can tell a first champion."""
        tracking, fixtures = finished_models(folder / "identity_runs")
        real = {model: f"features_{training_contract.MODEL_NAMES[model]}_1234abc{position}" for position, model in enumerate(("m1", "m2", "m4"), start=1)}
        for model, run in real.items():
            parent_of(tracking, model)["tags"]["features_run_id"] = run
        code, _, error, output = research(folder / "identity_runs_research", fixtures["m2"])
        require(code == 0, f"research failed: {error}")
        document = json.loads((output / f"research_{HAI_TYPE}.json").read_text())
        require(document["features_runs"] == real, f"the record's features runs: {document['features_runs']}, not {real}")
        require("production_version" in document["promotion"] and document["promotion"]["production_version"] is None, f"{document['promotion']}")

    def refuses_a_missing_model() -> None:
        tracking, fixtures = finished_models(folder / "missing", models=("m1", "m2"))
        code, _, error, output = research(folder / "missing_research", fixtures["m2"])
        require(code == job.EXIT_FAILED and failure_of(error) == "research_models_missing" and "m4" in error, f"{code} {error}")
        require(listing(output) == [] and not tracking.models[NAME]["aliases"], "a refused research wrote output or an alias")

    def refuses_runs_of_another_base() -> None:
        tracking, fixtures = finished_models(folder / "other_base", m4={"base_run": "features_9999abcd"})
        code, _, error, _ = research(folder / "other_base_research", fixtures["m2"])
        require(code == job.EXIT_FAILED and failure_of(error) == "research_models_missing" and "m4" in error, f"{code} {error}")
        require(not tracking.models[NAME]["aliases"], "a challenger was chosen across two bases")

    def refuses_other_windows() -> None:
        tracking, fixtures = finished_models(folder / "windows", m1={"years": range(2018, 2023)})
        code, _, error, _ = research(folder / "windows_research", fixtures["m2"])
        require(code == job.EXIT_FAILED and failure_of(error) == "research_rows_differ", f"{code} {error}")
        require(not tracking.models[NAME]["aliases"], "a challenger was chosen from runs on other windows")

    def refuses_an_unfinished_run() -> None:
        tracking, fixtures = finished_models(folder / "unfinished")
        run = parent_of(tracking, "m4")
        del run["tags"]["bundle_sha256"]
        code, _, error, _ = research(folder / "unfinished_research", fixtures["m2"])
        require(code == job.EXIT_FAILED and failure_of(error) == "research_models_missing" and "m4" in error, f"{code} {error}")

    def retry_changes_nothing() -> None:
        tracking, fixtures = finished_models(folder / "retry")
        first = research(folder / "retry_research", fixtures["m2"])
        require(first[0] == 0, f"research failed: {first[2]}")
        runs, aliases = len(tracking.active()), dict(tracking.models[NAME]["aliases"])
        require(research(folder / "retry_again", fixtures["m2"])[0] == 0, "the retry in a new task failed")
        require(len(tracking.active()) == runs and tracking.models[NAME]["aliases"] == aliases, "a retry added a run or moved an alias")

    def sets_no_challenger_when_none_passes() -> None:
        tracking, fixtures = finished_models(folder / "none")
        for version in tracking.models[NAME]["versions"]:
            version["tags"]["promotion_verdict"] = "fail:minimum_improvement"
        code, _, error, output = research(folder / "none_research", fixtures["m2"])
        require(code == 0, f"research failed: {error}")
        document = json.loads((output / f"research_{HAI_TYPE}.json").read_text())
        require(document["promotion"]["challenger"] is None and not tracking.models[NAME]["aliases"], f"challenger: {document['promotion']['challenger']}")

    def later_run_of_a_model_counts() -> None:
        tracking, fixtures = finished_models(folder / "later")
        later = "training_elastic_net_1234abc1"
        finished_models(folder / "later_m2", models=("m2",), tracking=tracking, runs={"m2": later})
        code, _, error, output = research(folder / "later_research", fixtures["m2"])
        require(code == 0, f"research failed: {error}")
        document = json.loads((output / f"research_{HAI_TYPE}.json").read_text())
        require(document["runs"]["m2"] == later, f"runs: {document['runs']}")

    def refuses_other_holdout_rows_at_the_same_count() -> None:
        tracking, fixtures = finished_models(folder / "identity", m4={"years": range(2018, 2023)})
        code, _, error, output = research(folder / "identity_research", fixtures["m2"])
        require(code == job.EXIT_FAILED and failure_of(error) == "research_holdouts_differ", f"m4 scored other hospital-windows: {code} {error}")
        require(listing(output) == [] and not tracking.models[NAME]["aliases"], "a challenger was chosen from runs on other holdout rows")

    def refuses_other_holdout_outcomes() -> None:
        def shifted(rows: list[dict[str, Any]]) -> None:
            newest = max(row["window_year"] for row in rows)
            for row in rows:
                if row["window_year"] == newest:
                    row[LABEL] = row[LABEL] + 0.5

        tracking, fixtures = finished_models(folder / "outcomes", m4={"adjust": shifted})
        code, _, error, _ = research(folder / "outcomes_research", fixtures["m2"])
        require(code == job.EXIT_FAILED and failure_of(error) == "research_holdouts_differ", f"m4 scored other outcomes on the same keys: {code} {error}")
        require(not tracking.models[NAME]["aliases"], "a challenger was chosen from runs with other holdout outcomes")

    def later_model_run_cannot_change_a_recorded_research() -> None:
        tracking, fixtures = finished_models(folder / "freeze")
        first = research(folder / "freeze_first", fixtures["m2"])
        require(first[0] == 0, f"research failed: {first[2]}")
        before = dict(tracking.models[NAME]["aliases"])
        later = "training_elastic_net_1234abc1"
        finished_models(folder / "freeze_later", models=("m2",), tracking=tracking, runs={"m2": later})
        for parent in tracking.parents().values():
            if parent["tags"].get("run_id") == later:
                parent["metrics"]["comparison_rmse.m2_elastic_net"] = [0.0]
        second = research(folder / "freeze_second", fixtures["m2"])
        require(second[0] == 0, f"the retry of a recorded research failed: {second[2]}")
        document = json.loads((second[3] / f"research_{HAI_TYPE}.json").read_text())
        require(document["runs"] == RUNS, f"the retry did not keep the runs of the record: {document['runs']}")
        require(tracking.models[NAME]["aliases"] == before, f"the retry moved the alias: {before} to {tracking.models[NAME]['aliases']}")

    def changed_record_is_refused_before_any_alias_write() -> None:
        tracking, fixtures = finished_models(folder / "changed")
        require(research(folder / "changed_first", fixtures["m2"])[0] == 0, "research failed")
        before = dict(tracking.models[NAME]["aliases"])
        challenger = next(item for item in tracking.models[NAME]["versions"] if item["version"] == before["challenger"])
        challenger["tags"]["promotion_verdict"] = "fail:minimum_improvement"
        code, _, error, _ = research(folder / "changed_second", fixtures["m2"])
        require(code == job.EXIT_FAILED and failure_of(error) == "mlflow_bundle_conflict", f"a changed record was accepted: {code} {error}")
        require(tracking.models[NAME]["aliases"] == before, f"the refused retry moved the alias: {before} to {tracking.models[NAME]['aliases']}")

    def reads_run_artifacts_through_the_tracking_server() -> None:
        tracking, fixtures = finished_models(folder / "proxied")
        client = ProbingClient(tracking)
        library = SimpleNamespace(import_module=lambda name: SimpleNamespace(MlflowClient=lambda tracking_uri: client))
        with mock.patch.object(run_tracking, "importlib", library):
            code, _, error, output = research(folder / "proxied_research", fixtures["m2"], opening=run_tracking.open_store)
        require(code == 0 and client.direct_reads == 0, f"research read an artifact over the presigned path ({client.direct_reads} reads): {code} {error}")
        require((output / f"research_{HAI_TYPE}.json").is_file(), "the research wrote no record")

    def records_the_research_attempt() -> None:
        tracking, fixtures = finished_models(folder / "attempt")
        first = research(folder / "attempt_first", fixtures["m2"])
        require(first[0] == 0, f"research failed: {first[2]}")
        require("research_attempt" not in json.loads((first[3] / f"research_{HAI_TYPE}.json").read_text()), "the first attempt names an attempt in its record")
        later = training_contract.research_run_id(BASE_RUN, 2)
        second = research(folder / "attempt_second", fixtures["m2"], run=later, research_attempt=2)
        require(second[0] == 0, f"the second attempt failed: {second[2]}")
        records = [item for item in tracking.active().values() if item["tags"].get("step_key") == "research"]
        require({item["tags"]["run_id"] for item in records} == {RESEARCH_RUN, later}, f"the research runs: {[item['tags']['run_id'] for item in records]}")
        require(
            json.loads((second[3] / f"research_{HAI_TYPE}.json").read_text()).get("research_attempt") == 2, "the record of attempt 2 does not name its attempt"
        )
        refused = research(folder / "attempt_spelled_out", fixtures["m2"], research_attempt=1)
        require(refused[0] == job.EXIT_FAILED and failure_of(refused[2]) == "parameters", f"attempt 1 spelled out was accepted: {refused[0]} {refused[2]}")

    def phase_publishes_each_record_once() -> None:
        tracking, fixtures = finished_models(folder / "phase")
        bucket = aws_run_cases.ListingBucket(aws_run_cases.BUCKET, "research/")
        work = folder / "phase_work"
        work.mkdir(parents=True)
        _, sha = aws_run_cases.code_archive(work)

        def phase() -> tuple[int, str, str]:
            with (
                standins(fixtures["m2"]),
                mock.patch.object(promote_receipt, "s3_client", return_value=bucket),
                aws_run_cases.environment(CODE_SHA256=sha, TRAINING_PARAMETERS=json.dumps(PARAMETERS | {"mlflow": "aws"})),
            ):
                return aws_run_cases.call("research", "--run", RESEARCH_RUN, "--mode", "real", "--work", str(work))

        code, _, error = phase()
        require(code == 0, f"the research phase failed: {error}")
        key = f"research/{RESEARCH_RUN}/research_{HAI_TYPE}.json"
        stored = bucket.objects.get(key)
        require(stored is not None and json.loads(stored)["promotion"]["challenger"] is not None, f"stored records: {sorted(bucket.objects)}")
        again = phase()
        require(again[0] == 0 and bucket.objects[key] == stored, f"a rerun changed the record: {again[2]}")
        bucket.objects[key] = b"another record\n"
        shutil.rmtree(work / "in/research")
        (work / "in").mkdir(exist_ok=True)
        conflict = phase()
        require(conflict[0] != 0 and "record_conflict" in conflict[2], f"another stored record was accepted: {conflict}")
        require(len(tracking.parents()) == 3, "the phase changed the model runs")

    cases.check("research_compares_two_models_and_chooses_one_challenger_per_type", compares_and_chooses)
    cases.check("research_reads_run_artifacts_through_the_tracking_server", reads_run_artifacts_through_the_tracking_server)
    cases.check("research_records_a_later_attempt_and_refuses_attempt_one_spelled_out", records_the_research_attempt)
    cases.check("research_phase_stores_each_record_once_and_refuses_another_record", phase_publishes_each_record_once)
    cases.check("research_never_moves_the_production_alias_and_records_its_version", never_moves_production)
    cases.check("research_records_the_features_runs_and_no_production", records_the_features_runs_the_models_trained_on_and_no_production)
    cases.check("research_refuses_when_a_model_has_no_finished_run", refuses_a_missing_model)
    cases.check("research_refuses_runs_of_another_base_run", refuses_runs_of_another_base)
    cases.check("research_refuses_runs_on_other_holdout_windows", refuses_other_windows)
    cases.check("research_refuses_a_run_without_its_completion_mark", refuses_an_unfinished_run)
    cases.check("research_refuses_holdouts_with_other_rows_at_the_same_count", refuses_other_holdout_rows_at_the_same_count)
    cases.check("research_refuses_holdouts_with_other_outcomes", refuses_other_holdout_outcomes)
    cases.check("research_retry_keeps_the_runs_of_its_record_when_a_later_run_exists", later_model_run_cannot_change_a_recorded_research)
    cases.check("research_refuses_a_changed_record_before_any_alias_write", changed_record_is_refused_before_any_alias_write)
    cases.check("research_retry_adds_no_run_and_moves_no_alias", retry_changes_nothing)
    cases.check("research_sets_no_challenger_when_no_version_passed", sets_no_challenger_when_none_passes)
    cases.check("research_takes_the_later_run_of_a_model", later_run_of_a_model_counts)
