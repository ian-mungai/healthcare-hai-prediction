"""Synthetic caller scenarios for model bodies, research statistics and separated analyses."""

from __future__ import annotations

import json
import math
import sys
from collections.abc import Callable
from dataclasses import replace
from datetime import date
from functools import partial
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

import numpy as np

from scripts.training.model_test_support import SpatialOLS, libraries


def require(condition: Any, reason: str) -> None:
    """Fail a scenario at its independently specified observable contract."""
    if not condition:
        raise AssertionError(reason)


def rejects(call: Callable[[], Any], reason: str) -> None:
    """Require the intended domain error, rather than any failing dependency or assertion."""
    from scripts.training.models.common import ModelError

    try:
        call()
    except ModelError as error:
        require(reason in str(error), f"wrong rejection: expected {reason}, got {error}")
    else:
        raise AssertionError(f"missing rejection: {reason}")


def request() -> Any:
    """Build synthetic hospital panels and small, explicitly non-native analysis settings."""
    from scripts.training.models.common import SLOTS, Dataset, Deadline, Feature, ModelRequest

    rng = np.random.default_rng(726)
    features = (
        Feature("feature_matrix_care_compare:process", "number", "C119", "care_compare"),
        Feature("feature_matrix_hospital_window:cmi", "number", "", "hospital_window"),
        Feature("feature_matrix_hospital:extra", "number", "C001", "hospital"),
    )
    rows = []
    for hospital in range(48):
        effect = (hospital % 6 - 2.5) * 0.2
        for year in range(2020, 2024):
            process, cmi, extra = rng.normal(size=3)
            rows.append(
                {
                    "spine_key": f"{990000 + hospital}:{year}",
                    "ccn": str(990000 + hospital),
                    "window_year": year,
                    "control_type_code": str(hospital % 6),
                    "county_fips": str(99001 + hospital // 4),
                    "process": float(process),
                    "cmi": float(cmi),
                    "extra": float(extra),
                    "hai_1__sir__number": float(2 + 0.4 * process + 0.25 * cmi + 0.6 * extra + effect + rng.normal(scale=0.03)),
                }
            )
    config: dict[str, Any] = {
        "master_seed": 42,
        "minimum_rows": {"training": 100, "holdout": 30},
        "thresholds": {"max_missing_fraction": 0.5, "significance_level": 0.05},
        "cross_validation": {"folds": 3, "shuffle": True},
        "moran": {"permutations": 99, "p_value": "p_sim"},
        "bootstrap": {"nested_comparison": {"draws": 99, "confidence_level": 0.95}, "mediation": {"draws": 99, "confidence_level": 0.95}},
        "budgets": {"fit_seconds": 60, "bootstrap_seconds": 20, "shap_seconds": 10},
        "shap": {"sample_cap": 128},
        "slots": {slot: {"grid": {}, "budget_seconds": 10} for slot in SLOTS},
    }
    config["slots"]["m2_elastic_net"]["grid"] = {"alpha": [0.01, 0.1], "l1_ratio": [0.1, 0.5]}
    config["slots"]["m4_boosting"]["grid"] = {"learning_rate": [0.1], "max_depth": [3], "n_estimators": [5]}
    config["slots"]["m4_boosting"]["check"] = {"grid": {"max_depth": [3], "n_estimators": [5]}}
    references = tuple(feature.reference for feature in features)
    lists = {slot: references for slot in SLOTS}
    lists["m1_ols"] = references[:1]
    lists["rq4_reduced"] = references[:1]
    return ModelRequest(
        Dataset(tuple(rows), features, "HAI_1"),
        lists,
        config,
        (),
        (references[0],),
        references[1],
        1,
        Deadline.after(60),
    )


def panel_standin(y: Any, x: Any, entities: Any, years: Any, names: Any) -> Any:
    """Provide independently computed within slopes at the native panel-library boundary."""
    from scripts.training.models.panel import PanelFits

    unique = sorted(set(entities))
    centered_x = x.copy()
    centered_y = np.asarray(y).copy()
    for entity in unique:
        mask = np.asarray(entities) == entity
        centered_x[mask] -= np.mean(x[mask], axis=0)
        centered_y[mask] -= np.mean(np.asarray(y)[mask])
    slopes = np.linalg.lstsq(centered_x[:, 1:], centered_y, rcond=None)[0]
    beta = np.r_[np.mean(y) - np.mean(x[:, 1:], axis=0) @ slopes, slopes]
    beta[0] -= 0.4
    effects = {entity: float(np.mean(np.asarray(y)[np.asarray(entities) == entity] - x[np.asarray(entities) == entity] @ beta)) for entity in unique}
    covariance = np.eye(len(names)) * 0.2
    return PanelFits(dict(zip(names, beta, strict=True)), covariance, dict(zip(names, beta + 0.01, strict=True)), covariance / 2, effects)


def scenarios() -> list[tuple[str, Callable[[], None]]]:
    """Return the predeclared model and analysis scenarios for the light caller runner."""
    from scripts.training import hac_validation, mediation, research_tests
    from scripts.training.models import panel, spatial, suite
    from scripts.training.models.common import SLOTS, Dataset, Deadline, Feature, seed_for
    from scripts.training.models.preprocessing import FeatureBatch, FoldPreprocessor
    from scripts.training.models.trees import shap_values

    req = request()
    prepared = FoldPreprocessor(req.data.features, forced=(req.cmi_reference,))

    def preprocessing() -> None:
        rows = [{"a": 1.0, "b": None}, {"a": None, "b": None}, {"a": 3.0, "b": 2.0}, {"a": None, "b": None}]
        features = (Feature("v:a", "number"), Feature("v:b", "number"))
        transform = FoldPreprocessor(features, native_missing=True).set_params(native_missing=False)
        fitting = transform.fit_transform(rows)
        require(transform.get_feature_names_out().tolist() == list(fitting.names), "preprocessing feature names differ from the fitted design")
        require(transform.kept_references_ == ("v:a",), "exactly half missing must be retained")
        output = transform.transform([{"a": None, "b": 500.0}])
        require(np.isfinite(output.matrix).all(), "linear missing values must be imputed")
        require(output.names == ("a", "a__missing"), "missing indicator is required")
        before = transform.medians_.copy()
        transform.transform([{"a": 50000.0, "b": 0.0}])
        require(transform.medians_ == before, "holdout changed the training median")
        rejects(lambda: FoldPreprocessor(features, forced=("v:b",)).fit(rows), "forced covariate")

    def unknown_category() -> None:
        transform = FoldPreprocessor((Feature("v:kind", "category"),)).fit([{"kind": "a"}, {"kind": "b"}])
        result = transform.transform([{"kind": "unseen"}])
        require(result.unknown_categories == 1 and np.isfinite(result.matrix).all(), "unseen category must be counted and encoded")

    def input_contract() -> None:
        rejects(lambda: replace(req.data, rows=(*req.data.rows, req.data.rows[0])), "duplicate")
        bad = dict(req.data.rows[0], hai_1__sir__number=float("nan"))
        rejects(lambda: replace(req.data, rows=(bad, *req.data.rows[1:])), "finite label")
        bad_feature = Feature("v:psi_90", "number", "C281")
        rejects(lambda: Dataset(tuple(dict(row, psi_90=0.2) for row in req.data.rows), (*req.data.features, bad_feature), "HAI_1"), "excluded")
        extra = tuple(dict(row, hac_flag=1) for row in req.data.rows)
        rejects(lambda: replace(req.data, rows=extra), "unexpected extract column")

    def folds() -> None:
        training, _ = req.data.split()
        with libraries():
            folds = suite.folds_for(training, req.config, seed_for(42, "HAI_1", "folds"))
            repeated = suite.folds_for(training, req.config, seed_for(42, "HAI_1", "folds"))
        require(len(folds) == 3, "wrong hospital fold count")
        for left, right in folds:
            require(not ({training.rows[i]["ccn"] for i in left} & {training.rows[i]["ccn"] for i in right}), "hospital crosses a fold")
        require([(a.tolist(), b.tolist()) for a, b in folds] == [(a.tolist(), b.tolist()) for a, b in repeated], "folds changed for the same seed")
        for left, right in suite.time_folds(training):
            require(max(training.rows[i]["window_year"] for i in left) < min(training.rows[i]["window_year"] for i in right), "panel folds leak time")

    def complete_suite() -> None:
        with libraries(), patch.object(panel, "native_fits", side_effect=panel_standin):
            output = suite.fit_models(req)
            again = suite.fit_models(replace(req, deadline=Deadline.after(60)))
        require(set(output.slots) == set(SLOTS), "model result must contain six slots")
        for slot, result in output.slots.items():
            require(result.status in ("fitted", "not_converged", "not_estimable", "not_run"), "unrecognized model status")
            if result.status == "fitted":
                require(len(result.predictions) == 48, f"{slot} changed the holdout row set")
                require(set(result.metrics) >= {"rmse", "mae", "r2"}, "holdout metrics missing")
            else:
                require(bool(result.reason), "failed or omitted model lacks a reason")
        require(output.selected in SLOTS and output.candidate in ("m1_ols", "m2_elastic_net", "m4_boosting"), "selection or registration contract failed")
        require(output.to_result() == again.to_result(), "same synthetic input changed the reproducibility payload")
        require(output.shap["status"] == "fitted", "tree attribution did not run")
        require(
            bool(output.to_result()["p_values"]) and all(isinstance(value, float) and 0 <= value <= 1 for value in output.to_result()["p_values"].values()),
            "p-value bundle field contains nonnumeric results",
        )
        require(
            output.research["joint_process"]["status"] == "fitted" and output.research["joint_process"]["f_statistic"] > 5,
            "joint process test missed the planted effect",
        )
        require(output.research["ownership"]["icc"] > 0 and output.research["ownership"]["reml"] is False, "ownership inference contract failed")
        tree = output.slots["m4_boosting"]
        require(
            tree.pipeline.named_steps["model"].native_.parameters["random_state"] == seed_for(42, "HAI_1", "m4_boosting"),
            "tree passed another seed to its native estimator",
        )
        require(tree.pipeline.named_steps["model"].native_.parameters["n_jobs"] == 1, "tree ignored its thread allocation")
        require(tree.details["forest_check"]["registrable"] is False, "forest check became registrable")
        require(output.slots["m2_elastic_net"].details["grid_edge"] is True, "Elastic Net grid edge was hidden")
        with libraries() as replacements, patch.object(panel, "native_fits", side_effect=panel_standin):
            original = replacements["esda"].Moran

            def significant(*args: Any, **kwargs: Any) -> Any:
                statistic = original(*args, **kwargs)
                statistic.p_sim = 0.001
                return statistic

            replacements["esda"].Moran = significant
            for number in range(1, 7):
                label = f"hai_{number}__sir__number"
                rows = tuple({(label if name == req.data.label else name): value for name, value in row.items()} for row in req.data.rows)
                expanded = suite.fit_models(replace(req, data=Dataset(rows, req.data.features, f"HAI_{number}"), deadline=Deadline.after(60)))
                require(
                    all(result.status == "fitted" for result in expanded.slots.values()), "a six-type spatial-gated caller did not exercise all model bodies"
                )
                spatial_result = expanded.slots["m5_spatial"]
                _, holdout = expanded.request.data.split()
                design = spatial_result.pipeline.named_steps["prepare"].transform(holdout.rows)
                estimator = spatial_result.pipeline.named_steps["model"]
                weights = spatial.build_weights(holdout.rows, ()).native().full()[0]
                fixed = np.column_stack((np.ones(len(design.matrix)), design.matrix)) @ estimator.coefficients_
                expected = np.linalg.solve(np.eye(len(fixed)) - estimator.spatial_parameter_ * weights, fixed)
                require(np.allclose(spatial_result.predictions, expected), "spatial prediction used another graph or outcome values")

            class ErrorDiagnostics(SpatialOLS):
                def __init__(self, *args: Any, **kwargs: Any) -> None:
                    super().__init__(*args, **kwargs)
                    self.rlm_lag, self.rlm_error = (0.2, 0.7), (10.0, 0.002)

            replacements["spreg"].OLS = ErrorDiagnostics
            error_suite = suite.fit_models(replace(req, deadline=Deadline.after(60)))
            spatial_result = error_suite.slots["m5_spatial"]
            require(spatial_result.status == "fitted" and spatial_result.details["kind"] == "error", "LM diagnostics did not choose the error model")
            _, holdout = req.data.split()
            design = spatial_result.pipeline.named_steps["prepare"].transform(holdout.rows)
            expected = np.column_stack((np.ones(len(design.matrix)), design.matrix)) @ spatial_result.pipeline.named_steps["model"].coefficients_
            require(np.allclose(spatial_result.predictions, expected), "spatial error prediction used holdout residuals")

            class BothDiagnostics(SpatialOLS):
                def __init__(self, *args: Any, **kwargs: Any) -> None:
                    super().__init__(*args, **kwargs)
                    self.rlm_lag, self.rlm_error = (10.0, 0.01), (9.0, 0.02)

            replacements["spreg"].OLS = BothDiagnostics
            ambiguous = suite.fit_models(replace(req, deadline=Deadline.after(60))).slots["m5_spatial"]
            require(ambiguous.status == "fitted" and ambiguous.details["kind"] == "lag", "both significant LM tests did not choose the larger statistic")
        group = Feature("feature_matrix_hospital_window:provider_subtype_code", "category", "", "hospital_window")
        lists = {**req.feature_lists, "m3_mixed": (*req.feature_lists["m3_mixed"], group.reference)}
        with libraries():
            overlap = suite.fit_slot(
                replace(
                    req,
                    data=replace(
                        req.data,
                        rows=tuple(dict(row, provider_subtype_code=row["control_type_code"]) for row in req.data.rows),
                        features=(*req.data.features, group),
                    ),
                    feature_lists=lists,
                ),
                "m3_mixed",
            )
        require(overlap.status == "not_converged" and "fixed" in overlap.reason, "ownership fixed effects hid an unidentified random intercept")
        with libraries() as replacements:
            elastic = replacements["sklearn.linear_model"].ElasticNet
            original_elastic = elastic.fit

            def no_elastic_convergence(self: Any, *args: Any, **kwargs: Any) -> Any:
                fitted = original_elastic(self, *args, **kwargs)
                fitted.n_iter_ = 10000
                return fitted

            with patch.object(elastic, "fit", no_elastic_convergence):
                failed_elastic = suite.fit_slot(req, "m2_elastic_net")
            require(failed_elastic.status == "not_converged", "tuning hid Elastic Net nonconvergence")
            mixed = replacements["statsmodels.api"].MixedLM
            original_mixed = mixed.fit

            def no_mixed_convergence(self: Any, **kwargs: Any) -> Any:
                fitted = original_mixed(self, **kwargs)
                fitted.converged = False
                return fitted

            with patch.object(mixed, "fit", no_mixed_convergence):
                failed_mixed = suite.fit_slot(req, "m3_mixed")
            require(failed_mixed.status == "not_converged", "mixed-model nonconvergence was ignored")
            constant = replace(
                req.data,
                rows=tuple(
                    dict(row, process=float(int(row["ccn"]) % 7), cmi=float(int(row["ccn"]) % 9), extra=float(int(row["ccn"]) % 11)) for row in req.data.rows
                ),
            )
            failed_panel = suite.fit_slot(replace(req, data=constant), "m6_fixed_effects")
            require(failed_panel.status == "not_estimable" and "absorbed" in failed_panel.reason, "absorbed panel columns were hidden")

    def collinear_ols() -> None:
        data = replace(req.data, rows=tuple(dict(row, extra=row["process"]) for row in req.data.rows))
        changed = replace(req, data=data, feature_lists={**req.feature_lists, "m1_ols": tuple(feature.reference for feature in data.features)})
        with libraries():
            result = suite.fit_slot(changed, "m1_ols")
        require(result.status == "not_estimable" and "rank" in result.reason, "collinear OLS silently used a pseudoinverse")

    def forced_elastic() -> None:
        from scripts.training.models.elastic import ElasticRegressor

        training, _ = req.data.split()
        design = prepared.fit_transform(training.rows)
        forced = design.references.index(req.cmi_reference)
        y = 7 + 2 * design.matrix[:, forced]
        with libraries():
            model = ElasticRegressor(100000.0, 0.5, (req.cmi_reference,), 73).fit(design, y)
        require(np.allclose(model.predict(design), y, atol=1e-9), "forced CMI was penalized")
        rows = tuple(dict(row, cmi=None, process=None) if int(row["ccn"]) % 6 == 0 else dict(row) for row in training.rows)
        median = float(np.median([row["cmi"] for row in rows if row["cmi"] is not None]))
        observed = np.asarray([7 + 2 * (median if row["cmi"] is None else row["cmi"]) + 3 * (row["cmi"] is None) for row in rows])
        design = FoldPreprocessor(training.features, forced=(req.cmi_reference,)).fit_transform(rows)
        with libraries():
            model = ElasticRegressor(100000.0, 0.5, (req.cmi_reference,), 73).fit(design, observed)
        require(np.allclose(model.predict(design), observed, atol=1e-9), "aliased forced CMI indicator was penalized")

    def ownership_singleton() -> None:
        changed = replace(req.data, rows=tuple(dict(row, control_type_code=row["ccn"]) for row in req.data.rows))
        with libraries():
            result = suite.fit_slot(replace(req, data=changed), "m3_mixed")
        require(result.status == "not_converged" and "hospital" in result.reason, "singleton ownership group was accepted")

    def tied_selection() -> None:
        from scripts.training.models.common import SlotResult

        rows = {slot: SlotResult(slot, "fitted", predictions=(1.0,), metrics={"rmse": 1.0}) for slot in SLOTS}
        require(suite.select(rows) == "m1_ols", "slot order must break equal RMSE")
        rows["m1_ols"] = SlotResult("m1_ols", "not_estimable", "rank", metrics={"rmse": 0.0})
        require(suite.select(rows) == "m2_elastic_net", "failed slot won selection")
        rows["m3_mixed"].metrics["rmse"] = 0.1
        require(suite.select(rows, registrable=True) == "m2_elastic_net", "research model became registrable")

    def expired() -> None:
        rejects(lambda: suite.fit_models(replace(req, deadline=Deadline(0.0))), "deadline")

    def nested_gain() -> None:
        with libraries():
            result = research_tests.nested_comparison(req)
        require(result["status"] == "fitted" and result["f_statistic"] > 50, "nested test missed the planted gain")
        require(result["rmse_improvement"] > 0.1 and result["interval"][0] > 0, "paired bootstrap missed the planted gain")
        require(result["bootstrap"]["unit"] == "hospital" and result["bootstrap"]["draws"] == 99, "bootstrap contract missing")

    def nested_invalid() -> None:
        changed = replace(req, feature_lists={**req.feature_lists, "rq4_reduced": ("v:missing",)})
        with libraries():
            rejects(lambda: research_tests.nested_comparison(changed), "nested")
        changed = replace(req, feature_lists={**req.feature_lists, "rq4_reduced": (req.cmi_reference,)})
        with libraries():
            rejects(lambda: research_tests.nested_comparison(changed), "CMS-only")

    def nested_no_gain() -> None:
        rng = np.random.default_rng(199)
        data = replace(
            req.data,
            rows=tuple(dict(row, hai_1__sir__number=3 + 0.4 * row["process"] + float(rng.normal(scale=0.1))) for row in req.data.rows),
        )
        with libraries():
            result = research_tests.nested_comparison(replace(req, data=data))
        require(result["interval"][0] <= 0 <= result["interval"][1], "nested no-gain interval excludes zero")

    def spatial_order_and_islands() -> None:
        _, holdout = req.data.split()
        graph = spatial.build_weights(holdout.rows, ())
        require(graph.keys == tuple(row["spine_key"] for row in holdout.rows), "weights reordered hospital keys")
        require(not graph.islands, "same-county neighbors were missed")
        isolated = (*holdout.rows[:-1], dict(holdout.rows[-1], county_fips="99999"))
        graph = spatial.build_weights(isolated, ())
        require(len(graph.islands) == 1, "island count is wrong")
        with libraries():
            rejects(lambda: spatial.moran_test(np.arange(48, dtype=float), tuple(reversed(graph.keys)), graph, 42, 99, Deadline.after(10)), "order")

    def moran_signal() -> None:
        _, holdout = req.data.split()
        graph = spatial.build_weights(holdout.rows, ())
        signal = np.asarray([int(row["county_fips"]) % 2 for row in holdout.rows], dtype=float)
        with libraries() as replacements:
            original = replacements["esda"].Moran

            def seeded(*args: Any, **kwargs: Any) -> Any:
                require(
                    np.array_equal(np.random.get_state()[1], np.random.RandomState(731).get_state()[1]),
                    "global NumPy seed was not set immediately before Moran",
                )
                return original(*args, **kwargs)

            replacements["esda"].Moran = seeded
            first = spatial.moran_test(signal, graph.keys, graph, 731, 99, Deadline.after(10))
            second = spatial.moran_test(signal, graph.keys, graph, 731, 99, Deadline.after(10))
        require(first == second and first["p_value_name"] == "p_sim", "Moran permutations were not seeded or the gate p-value changed")
        require(first["statistic"] > 0.8 and first["p_value"] < 0.05, "spatial pattern was missed")

    def hausman_contract() -> None:
        with libraries() as replacements:
            fitted = research_tests.hausman(np.asarray([1.0, 2.0]), np.eye(2), np.asarray([0.0, 0.0]), np.eye(2) / 2, ("a", "b"))
            singular = research_tests.hausman(np.asarray([1.0]), np.eye(1), np.asarray([0.0]), np.eye(1), ("a",))
        require(math.isclose(fitted["statistic"], 10.0), "Hausman coefficient or covariance order is wrong")
        require(singular["status"] == "not_estimable", "singular Hausman covariance used a pseudoinverse")
        require(replacements["scipy.stats"].chi2.calls == [(fitted["statistic"], 2)], "Hausman tail received wrong degrees of freedom")

    def panel_unbalanced_and_unseen() -> None:
        rows = tuple(row for row in req.data.rows if row["ccn"] != "990000" or row["window_year"] in (2020, 2023))
        changed = replace(req, data=replace(req.data, rows=rows))
        with libraries(), patch.object(panel, "native_fits", side_effect=panel_standin):
            result = suite.fit_slot(changed, "m6_fixed_effects")
        require(result.status == "fitted", "unbalanced panel failed")
        require(
            result.details["unseen_holdout_hospitals"] == 1 and result.details["dropped_singleton_hospital_count"] == 1,
            "panel singleton or unseen-effect count is wrong",
        )
        _, holdout = changed.data.split()
        design = result.pipeline.named_steps["prepare"].transform(holdout.rows)
        fitted = result.pipeline.named_steps["model"]
        expected = np.r_[1.0, design.matrix[0, fitted.kept_indices_]] @ fitted.coefficients_ + sum(fitted.effects_.values()) / len(fitted.effects_)
        require(math.isclose(result.predictions[0], expected), "unseen hospital did not receive the mean fitted effect")

    def shap_failure() -> None:
        from scripts.training.model_test_support import TreeExplainer

        with libraries():
            fitted = suite.fit_slot(req, "m4_boosting")
            _, holdout = req.data.split()
            original = TreeExplainer.shap_values

            def wrong(self: Any, x: Any, *, check_additivity: bool) -> Any:
                return original(self, x, check_additivity=check_additivity) + 1.0

            with patch.object(TreeExplainer, "shap_values", wrong):
                rejects(lambda: shap_values(fitted.pipeline, holdout.rows, 12, 8, Deadline.after(10)), "additivity")

    def mediation_separation() -> None:
        mediator_rows = tuple(
            {
                "spine_key": row["spine_key"],
                "measure": measure,
                "value_number": row["process"] * 0.8 + row["extra"] * 0.2,
                "alignment_status": "aligned",
                "period_end": date(row["window_year"] - 1, 12, 31),
            }
            for row in req.data.rows
            for measure in ("PSI_90", "PSI_13")
        )
        resample = mediation.hospital_resamples

        def hospital_blocks(entities: Any, draws: int, seed: int) -> Any:
            for positions in resample(entities, draws, seed):
                counts = np.bincount(positions, minlength=len(entities))
                for entity in set(entities):
                    require(len(set(counts[[index for index, value in enumerate(entities) if value == entity]])) == 1, "bootstrap split a hospital's windows")
                yield positions

        with libraries(), patch.object(mediation, "hospital_resamples", side_effect=hospital_blocks):
            output = mediation.analyze_mediation(req.data, mediator_rows, req.process_references, (req.cmi_reference,), req.config, Deadline.after(30))
        require(set(output["mediators"]) == {"PSI_90", "PSI_13"}, "both owner-authorized mediators must be analyzed")
        require(all(value["status"] == "fitted" for value in output["mediators"].values()), "mediation did not estimate its association")
        require(all(value["interpretation"] == "associational" for value in output["mediators"].values()), "mediation claims causation")
        require(
            all(effect["interval"][0] < effect["interval"][1] for value in output["mediators"].values() for effect in value["effects"].values()),
            "mediation bootstrap did not refit resampled hospitals",
        )
        future = (dict(mediator_rows[0], period_end=date(2020, 1, 1)), *mediator_rows[1:])
        with libraries():
            rejects(
                lambda: mediation.analyze_mediation(req.data, future, req.process_references, (req.cmi_reference,), req.config, Deadline.after(30)),
                "before the outcome",
            )

    def hac_join() -> None:
        predictions = tuple({"spine_key": f"{990000 + i}:2023", "predicted": float(i), "observed": 0.0} for i in range(8))
        flags = tuple({"spine_key": f"{990000 + i}:2023", "fiscal_year": 2025, "value_number": int(i >= 6), "alignment_status": "aligned"} for i in range(7))
        output = hac_validation.validate_hac(predictions, flags, {2023: 2025}, Deadline.after(10))
        require(output["status"] == "fitted", "explicit HAC program-year mapping was not used")
        require(output["missing_flags"] == 1 and output["matched_hospitals"] == 7, "missing HAC flags became negative flags")
        require(output["auc"] == 1.0 and output["top_quartile_flagged_share"] == 0.5, "HAC ranking or quartile is wrong")
        require(
            hac_validation.validate_hac(predictions, (), {2023: 2025}, Deadline.after(10))["status"] == "not_run", "missing HAC source must not fail training"
        )
        require(hac_validation.validate_hac(predictions, flags, {2023: 2024}, Deadline.after(10))["status"] == "not_run", "wrong program year joined")
        rejects(lambda: hac_validation.validate_hac(predictions, (*flags, flags[0]), {2023: 2025}, Deadline.after(10)), "duplicate HAC")
        rejects(lambda: hac_validation.validate_hac(predictions, flags, {}, Deadline.after(10)), "year mapping")

    def shared_missing_indicators() -> None:
        changed_rows = tuple(dict(row, process=None, extra=None) if int(row["ccn"]) % 6 == 0 else dict(row) for row in req.data.rows)
        data = replace(req.data, rows=changed_rows)
        lists = {**req.feature_lists, "m1_ols": tuple(feature.reference for feature in data.features)}
        changed = replace(req, data=data, feature_lists=lists)
        with libraries(), patch.object(panel, "native_fits", side_effect=panel_standin):
            for slot in SLOTS:
                result = suite.fit_slot(changed, slot)
                require(result.status == "fitted", f"shared missing indicators made {slot} non-estimable: {result.reason}")
            nested = research_tests.nested_comparison(changed)
        require(nested["status"] == "fitted", "shared missing indicators prevented the nested test")
        training, holdout = data.split()
        transform = FoldPreprocessor(data.features).fit(training.rows)
        design = transform.transform(holdout.rows)
        require(design.names.count("process__missing") == 1 and "extra__missing" not in design.names, "duplicate indicator was retained")
        require(transform.indicator_aliases_ == {"extra__missing": "process__missing"}, "indicator aliases were not recorded")
        require(result.details["indicator_aliases"] == transform.indicator_aliases_, "result lost the indicator mapping")
        masked = np.asarray([row["extra"] is None for row in training.rows], dtype=float)
        learned = transform.transform(training.rows)
        require(np.array_equal(masked, learned.matrix[:, learned.names.index("process__missing")]), "indicator deduplication lost fitting-row information")
        constant_rows = tuple(dict(row, extra=3.0) for row in training.rows)
        constant = FoldPreprocessor(data.features).fit_transform(constant_rows)
        require("extra" not in constant.names, "constant numeric predictor was retained")
        transform = FoldPreprocessor(data.features).fit(constant_rows)
        require(transform.constant_columns_ == {"extra": 3.0}, "constant predictor value was not recorded")
        rejects(
            lambda: FoldPreprocessor(data.features, forced=(req.cmi_reference,)).fit(tuple(dict(row, cmi=1.0) for row in training.rows)), "forced covariate"
        )

        rng = np.random.default_rng(1593)
        categories = (
            Feature("feature_matrix_care_compare:kind_a", "category", family="care_compare"),
            Feature("feature_matrix_care_compare:kind_b", "category", family="care_compare"),
        )
        category_rows = tuple(
            dict(
                row, kind_a=None if int(row["ccn"]) % 6 == 0 else str(rng.integers(0, 3)), kind_b=None if int(row["ccn"]) % 6 == 0 else str(rng.integers(0, 4))
            )
            for row in req.data.rows
        )
        for category_count, numeric_missing in ((2, False), (1, True), (2, True)):
            features = (
                *categories[:category_count],
                *req.data.features[:2],
                replace(req.data.features[2], reference="feature_matrix_care_compare:extra", family="care_compare", control="C120"),
            )
            rows = tuple(dict(row, extra=None) if numeric_missing and int(row["ccn"]) % 6 == 0 else dict(row) for row in category_rows)
            if category_count == 1:
                rows = tuple({key: value for key, value in row.items() if key != "kind_b"} for row in rows)
            data = replace(req.data, rows=rows, features=features)
            references = tuple(feature.reference for feature in features)
            changed = replace(req, data=data, feature_lists={**req.feature_lists, **dict.fromkeys(SLOTS, references)})
            with libraries(), patch.object(panel, "native_fits", side_effect=panel_standin):
                for slot in SLOTS:
                    fitted = suite.fit_slot(changed, slot)
                    require(fitted.status == "fitted", f"categorical shared missing indicators made {slot} non-estimable: {fitted.reason}")
                require(research_tests.nested_comparison(changed)["status"] == "fitted", "categorical shared missingness prevented nesting")
                if numeric_missing:
                    extra_reference = features[-1].reference
                    fitted = suite.fit_slot(changed, "m1_ols")
                    joint = research_tests.joint_significance(fitted.pipeline, (extra_reference,))
                    nested_alias = research_tests.nested_comparison(
                        replace(changed, feature_lists={**changed.feature_lists, "rq4_reduced": (extra_reference,)})
                    )
                    require(joint["numerator_degrees_of_freedom"] == 2, "joint test lost a categorical canonical missing indicator")
                    require(
                        nested_alias["extra_degrees_of_freedom"] == (4 if category_count == 1 else 7),
                        "nested test lost a categorical canonical missing indicator",
                    )
            training, holdout = data.split()
            transform = FoldPreprocessor(features).fit(training.rows)
            learned, scored = transform.transform(training.rows), transform.transform(holdout.rows)
            aliases = {"kind_b__missing": "kind_a__missing"} if category_count == 2 else {}
            if numeric_missing:
                aliases["extra__missing"] = "kind_a__missing"
            require(transform.indicator_aliases_ == aliases, "categorical indicator aliases were not recorded")
            require(all(reference in transform.kept_references_ for reference in references), "categorical repair silently dropped a predictor")
            null_index = learned.names.index("kind_a__missing")
            require(np.array_equal(learned.matrix[:, null_index], [row["kind_a"] is None for row in training.rows]), "categorical null indicator changed")
            require(np.array_equal(scored.matrix[:, null_index], [row["kind_a"] is None for row in holdout.rows]), "scoring changed categorical null coding")
            for feature in features[:category_count]:
                counts = {value: sum(row[feature.name] == value for row in training.rows) for value in ("0", "1", "2", "3")}
                expected = min(counts, key=lambda value: (-counts[value], value))
                require(transform.categories_[feature.reference][0] == expected, "categorical reference is not the fitting-row non-null mode")
                require(null_index in learned.columns_for((feature.reference,)), "categorical indicator alias lost source ownership")
            if numeric_missing:
                require(null_index in learned.columns_for((features[-1].reference,)), "numeric indicator alias lost categorical canonical ownership")
            reversed_design = FoldPreprocessor(tuple(reversed(features))).fit_transform(training.rows)
            require(
                np.linalg.matrix_rank(np.column_stack((np.ones(len(training.rows)), reversed_design.matrix))) == reversed_design.matrix.shape[1] + 1,
                "shared indicator repair depends on predictor order",
            )
        from scripts.training.models.elastic import ElasticRegressor

        features = (categories[0], *req.data.features)
        rows = tuple(
            {key: (None if key == "cmi" and row["kind_a"] is None else value) for key, value in row.items() if key != "kind_b"} for row in category_rows
        )
        median = float(np.median([row["cmi"] for row in rows if row["cmi"] is not None]))
        observed = np.asarray([7 + 2 * (median if row["cmi"] is None else row["cmi"]) + 3 * (row["cmi"] is None) for row in rows])
        design = FoldPreprocessor(features, forced=(req.cmi_reference,)).fit_transform(rows)
        with libraries():
            elastic = ElasticRegressor(100000.0, 0.5, (req.cmi_reference,), 73).fit(design, observed)
        require(np.allclose(elastic.predict(design), observed, atol=1e-9), "categorical canonical indicator penalized forced CMI")

    def nested_indicator_alias_ownership() -> None:
        features = (req.data.features[2], req.data.features[0], req.data.features[1])
        rows = tuple(dict(row, process=None, extra=None) if int(row["ccn"]) % 6 == 0 else dict(row) for row in req.data.rows)
        references = tuple(feature.reference for feature in features)
        changed = replace(
            req, data=replace(req.data, rows=rows, features=features), feature_lists={**req.feature_lists, "m1_ols": references, "m2_elastic_net": references}
        )
        with libraries():
            result = research_tests.nested_comparison(changed)
            fitted = suite.fit_slot(changed, "m1_ols")
            joint = research_tests.joint_significance(fitted.pipeline, req.process_references)
        require(result["extra_degrees_of_freedom"] == 2, "nested reduced design lost an aliased missing indicator")
        require(joint["numerator_degrees_of_freedom"] == 2, "joint process test lost an aliased missing indicator")

    def tuning_prepares_each_fold_once() -> None:
        calls: list[frozenset[str]] = []
        original = FoldPreprocessor.fit

        def tracked(self: Any, values: Any, y: Any = None) -> Any:
            calls.append(frozenset(row["spine_key"] for row in getattr(values, "rows", values)))
            return original(self, values, y)

        with libraries(), patch.object(FoldPreprocessor, "fit", tracked):
            result = suite.fit_slot(req, "m2_elastic_net")
        require(result.status == "fitted", "cached-fold tuning did not fit")
        require(len(calls) == req.config["cross_validation"]["folds"] + 1, "tuning refitted preprocessing for each grid option")
        training, holdout = req.data.split()
        require(calls[-1] == frozenset(training.keys), "final preprocessing did not refit all training rows")
        require(all(not (keys & set(holdout.keys)) for keys in calls), "holdout entered cached preprocessing")

    def excluded_controls_from_column_names() -> None:
        for column in ("c281__value__number", "c283__lower_limits__number", "e038__value__number"):
            for declared in ("", "C119"):
                feature = Feature("feature_matrix_hac:" + column, "number", declared)
                rows = tuple(dict(row, **{column: 0.1}) for row in req.data.rows)
                rejects(partial(Dataset, rows, (*req.data.features, feature), "HAI_1"), "excluded")

    def panel_effect_normalization() -> None:
        from types import SimpleNamespace

        training, _ = req.data.split()
        prepared_panel = FoldPreprocessor(training.features).fit_transform(training.rows)
        entities = tuple(row["ccn"] for row in training.rows)
        years = tuple(row["window_year"] for row in training.rows)
        names = ("intercept", *prepared_panel.names)
        matrix = np.column_stack((np.ones(len(training.rows)), prepared_panel.matrix))
        reference = panel_standin(training.y, matrix, entities, years, names)

        class Series:
            def __init__(self, values: Any, **kwargs: Any) -> None:
                self.values = values
                self.index = tuple(values) if isinstance(values, dict) else ()

            def to_dict(self) -> Any:
                return dict(self.values)

        class Covariance:
            @property
            def loc(self) -> Any:
                return self

            def __getitem__(self, keys: Any) -> Any:
                return self

            def to_numpy(self) -> Any:
                return np.eye(len(names)) * 0.2

        class Effects:
            @property
            def iloc(self) -> Any:
                return self

            def __getitem__(self, key: Any) -> Any:
                return self

            def groupby(self, **kwargs: Any) -> Any:
                return self

            def mean(self) -> Any:
                return self

            def to_dict(self) -> Any:
                return {entity: reference.effects[entity] + reference.fixed["intercept"] for entity in set(entities)}

        class Native:
            def __init__(self, *args: Any, **kwargs: Any) -> None:
                pass

            def fit(self, **kwargs: Any) -> Any:
                return SimpleNamespace(nobs=len(training.rows), params=Series(reference.fixed), cov=Covariance(), estimated_effects=Effects(), loglik=-10.0)

        fixture_effects = Effects().iloc[:, 0].groupby(level="ccn").mean().to_dict()
        require(set(fixture_effects) == set(entities), "panel effect fixture lost its native indexing interface")
        fake_pandas = SimpleNamespace(MultiIndex=SimpleNamespace(from_arrays=lambda *args, **kwargs: None), DataFrame=Series, Series=Series)
        fake_panel = SimpleNamespace(PanelOLS=Native, RandomEffects=Native)
        original_import = panel.importlib.import_module

        def load(name: str, package: Any = None) -> Any:
            return {"pandas": fake_pandas, "linearmodels.panel": fake_panel}.get(name) or original_import(name, package)

        with patch.object(panel.importlib, "import_module", side_effect=load):
            result = panel.native_fits(training.y, matrix, entities, years, names)
        beta = np.asarray([result.fixed[name] for name in names])
        predicted = matrix @ beta + np.asarray([result.effects[entity] for entity in entities])
        require(abs(float(np.mean(training.y - predicted))) < 1e-10, "panel hospital effects double-counted the intercept")
        with libraries(), patch.object(panel, "native_fits", return_value=result):
            fitted = panel.PanelRegressor().fit(prepared_panel, training.y)
        require(abs(float(np.mean(training.y - fitted.predict(prepared_panel)))) < 1e-10, "panel fitted residual mean is not zero")

    def result_has_aggregate_panel_counts() -> None:
        rows = tuple(row for row in req.data.rows if row["ccn"] != "990000" or row["window_year"] in (2020, 2023))
        with libraries(), patch.object(panel, "native_fits", side_effect=panel_standin):
            output = suite.fit_models(replace(req, data=replace(req.data, rows=rows)))
        payload = output.to_result()
        text = json.dumps(payload, sort_keys=True)
        require(all(row["ccn"] not in text for row in req.data.rows), "canonical result contains hospital identifiers")
        details = payload["fitted_parameters"]["m6_fixed_effects"]
        require(details["training_hospitals_by_window_count"] == {"3": 47}, "panel window histogram is wrong")
        require(details["dropped_singleton_hospital_count"] == 1, "panel singleton aggregate is wrong")

    def nested_and_joint_values() -> None:
        references = tuple(feature.reference for feature in req.data.features)
        changed = replace(
            req,
            feature_lists={**req.feature_lists, "m1_ols": references, "rq4_reduced": (req.process_references[0],)},
            process_references=(references[0], references[2]),
        )
        training, _ = changed.data.split()
        y = training.y
        columns = np.asarray([[1.0, row["process"], row["cmi"], row["extra"]] for row in training.rows])
        reduced = columns[:, [0, 1]]
        joint_reduced = columns[:, [0, 2]]
        full_error = y - columns @ np.linalg.lstsq(columns, y, rcond=None)[0]
        reduced_error = y - reduced @ np.linalg.lstsq(reduced, y, rcond=None)[0]
        full_ssr, reduced_ssr = float(full_error @ full_error), float(reduced_error @ reduced_error)
        degrees = len(y) - columns.shape[1]
        expected_f = ((reduced_ssr - full_ssr) / 2) / (full_ssr / degrees)
        expected_lr = len(y) * math.log(reduced_ssr / full_ssr)
        joint_error = y - joint_reduced @ np.linalg.lstsq(joint_reduced, y, rcond=None)[0]
        expected_joint_f = ((float(joint_error @ joint_error) - full_ssr) / 2) / (full_ssr / degrees)
        with libraries() as replacements:
            result = research_tests.nested_comparison(changed)
            fitted = suite.fit_slot(changed, "m1_ols")
            joint = research_tests.joint_significance(fitted.pipeline, changed.process_references)
        require(math.isclose(result["f_statistic"], expected_f, rel_tol=1e-10), "nested F differs from the independent sums of squares")
        require(math.isclose(result["likelihood_ratio"], expected_lr, rel_tol=1e-10), "nested likelihood ratio differs from the Gaussian likelihood")
        require(result["extra_degrees_of_freedom"] == 2, "nested numerator degrees of freedom changed")
        require(math.isclose(joint["f_statistic"], expected_joint_f, rel_tol=1e-10), "joint F differs from the independent sums of squares")
        require(joint["numerator_degrees_of_freedom"] == 2 and joint["denominator_degrees_of_freedom"] == degrees, "joint F degrees of freedom changed")
        require(
            replacements["scipy.stats"].f.calls == [(result["f_statistic"], 2, degrees), (joint["f_statistic"], 2, degrees)],
            "F tails received wrong numerator or residual degrees of freedom",
        )
        require(replacements["scipy.stats"].chi2.calls == [(result["likelihood_ratio"], 2)], "nested likelihood tail received wrong degrees of freedom")

    def mixed_values_and_unresolved_ownership() -> None:
        training, _ = req.data.split()
        matrix = np.asarray([[1.0, row["process"], row["cmi"], row["extra"]] for row in training.rows])
        residual = training.y - matrix @ np.linalg.lstsq(matrix, training.y, rcond=None)[0]
        ownership = np.asarray([row["control_type_code"] for row in training.rows])
        variance = max(float(np.var([np.mean(residual[ownership == group]) for group in sorted(set(ownership))])), 0.01)
        residual_variance = float(residual @ residual) / (len(residual) - matrix.shape[1])
        with libraries() as replacements:
            replacements["scipy.stats"].chi2 = type("Tail", (), {"sf": staticmethod(lambda statistic, df: 0.2 if statistic == 6 and df == 1 else -1)})
            result = suite.fit_slot(req, "m3_mixed")
        require(result.status == "fitted" and result.details["likelihood_ratio"] == 6.0, "mixed likelihood ratio differs from the supplied log likelihoods")
        require(math.isclose(result.details["icc"], variance / (variance + residual_variance), rel_tol=1e-10), "mixed ICC differs from the variance ratio")
        require(result.details["p_value"] == 0.1, "mixed likelihood test lost its half chi-squared boundary weight")
        with libraries():
            absent = replace(req.data, rows=(dict(req.data.rows[0], control_type_code=None), *req.data.rows[1:]))
            failed = suite.fit_slot(replace(req, data=absent), "m3_mixed")
        require(
            failed.status == "fitted" and failed.details["excluded_missing_ownership"] == {"training": 1, "holdout": 0, "total": 1},
            "missing ownership exclusion or count is wrong",
        )
        with libraries() as replacements:
            mixed = replacements["statsmodels.api"].MixedLM
            original_fit = mixed.fit

            def zero_variance(self: Any, **kwargs: Any) -> Any:
                fitted = original_fit(self, **kwargs)
                fitted.cov_re = np.asarray([[0.0]])
                return fitted

            with patch.object(mixed, "fit", zero_variance):
                boundary = suite.fit_slot(req, "m3_mixed")
        require(boundary.status == "not_converged" and "singular variance" in boundary.reason, "zero ownership variance changed an unresolved rule")

    def mediation_known_values() -> None:
        x = np.asarray([row["process"] for row in req.data.rows])
        cmi = np.asarray([row["cmi"] for row in req.data.rows])
        base = np.column_stack((np.ones(len(x)), x, cmi))
        rng = np.random.default_rng(538)
        noise = rng.normal(size=len(x))
        noise -= base @ np.linalg.lstsq(base, noise, rcond=None)[0]
        mediator_values = 0.8 * x + 0.2 * cmi + noise * 0.3
        augmented = np.column_stack((base, mediator_values))
        noise = rng.normal(size=len(x))
        noise -= augmented @ np.linalg.lstsq(augmented, noise, rcond=None)[0]
        outcome = 1.7 + 0.3 * x + 0.4 * cmi + 1.5 * mediator_values + noise * 0.05
        data = replace(req.data, rows=tuple(dict(row, **{req.data.label: float(value)}) for row, value in zip(req.data.rows, outcome, strict=True)))
        mediator_rows = tuple(
            {
                "spine_key": row["spine_key"],
                "measure": name,
                "value_number": float(value),
                "alignment_status": "aligned",
                "period_end": date(row["window_year"] - 1, 12, 31),
            }
            for row, value in zip(data.rows, mediator_values, strict=True)
            for name in ("PSI_90", "PSI_13")
        )
        with libraries():
            result = mediation.analyze_mediation(data, mediator_rows, req.process_references, (req.cmi_reference,), req.config, Deadline.after(30))
        for fitted in result["mediators"].values():
            effect = fitted["effects"]["process"]
            for name, expected in (("indirect_effect", 1.2), ("direct_effect", 0.3), ("total_effect", 1.5)):
                require(math.isclose(effect[name], expected, abs_tol=1e-9), f"mediation {name} differs from its planted coefficient product")
            require(effect["interval"][0] < 1.2 < effect["interval"][1], "mediation interval missed its planted indirect effect")

    def moran_island_values_and_work() -> None:
        _, holdout = req.data.split()
        rows = (*holdout.rows[:-1], dict(holdout.rows[-1], county_fips="99999"))
        graph = spatial.build_weights(rows, ())
        values = np.asarray([int(row["county_fips"]) % 2 for row in rows], dtype=float)
        active_rows = rows[:-1]
        weights = np.asarray(
            [[float(i != j and left["county_fips"] == right["county_fips"]) for j, right in enumerate(active_rows)] for i, left in enumerate(active_rows)]
        )
        weights /= weights.sum(axis=1, keepdims=True)
        centered = values[:-1] - np.mean(values[:-1])
        expected = float(centered @ weights @ centered / (centered @ centered))
        calls = 0
        original = vars(spatial.Weights)["islands"].fget

        def counted(self: Any) -> Any:
            nonlocal calls
            calls += 1
            return original(self)

        with libraries(), patch.object(spatial.Weights, "islands", property(counted)):
            result = spatial.moran_test(values, graph.keys, graph, 731, 99, Deadline.after(10))
        require(result["included_hospitals"] == 47 and result["islands"] == 1, "Moran included an island")
        require(math.isclose(result["statistic"], expected, abs_tol=1e-12), "Moran with islands differs from the dense reference")
        require(result["p_value"] == 0.01, "Moran permutation tail differs from the seeded island pattern")
        require(calls <= 2, "Moran recomputed the island set per hospital")
        random = np.random.default_rng(224).normal(size=len(rows))
        with libraries():
            null = spatial.moran_test(random, graph.keys, graph, 19, 999, Deadline.after(10))
            no_effect = research_tests.hausman(np.asarray([0.2, 0.3]), np.eye(2), np.asarray([0.2, 0.3]), np.eye(2) / 2, ("a", "b"))
        require(null["p_value"] > 0.05, "random spatial residuals falsely passed the gate")
        require(no_effect["statistic"] == 0.0 and no_effect["p_value"] == 1.0, "no hospital effect did not retain the null")

    def optional_analysis_failures() -> None:
        from scripts.training.models.common import DeadlineExceeded, NotEstimable

        for owner, name, key in (
            (research_tests, "joint_significance", "joint_process"),
            (research_tests, "intercept_cmi_comparison", "process_vs_intercept_cmi"),
            (suite, "shap_values", "shap"),
        ):
            with (
                libraries(),
                patch.object(panel, "native_fits", side_effect=panel_standin),
                patch.object(owner, name, side_effect=NotEstimable("synthetic optional failure")),
            ):
                result = suite.fit_models(req)
            report = result.shap if key == "shap" else result.research[key]
            require(
                report["status"] == "not_estimable" and "synthetic optional failure" in report["reason"], "optional analysis failure discarded the fitted slots"
            )
            require(result.slots["m1_ols"].status == "fitted" and result.candidate is not None, "optional failure lost the fitted candidate")
            with (
                libraries(),
                patch.object(panel, "native_fits", side_effect=panel_standin),
                patch.object(owner, name, side_effect=DeadlineExceeded("synthetic deadline")),
            ):
                rejects(lambda: suite.fit_models(req), "synthetic deadline")

    def mediation_prepares_source_once() -> None:
        from scripts.training.models import preprocessing as preparation

        rows = tuple(
            {
                "spine_key": row["spine_key"],
                "measure": name,
                "value_number": row["process"] * 0.8 + row["extra"] * 0.2,
                "alignment_status": "aligned",
                "period_end": date(row["window_year"] - 1, 12, 31),
            }
            for row in req.data.rows
            for name in ("PSI_90", "PSI_13")
        )
        calls = 0
        original = preparation.records

        def counted(value: Any) -> Any:
            nonlocal calls
            calls += 1
            return original(value)

        sizes = []
        tolerances = []
        original_solve = np.linalg.lstsq

        def solve(matrix: Any, responses: Any, **kwargs: Any) -> Any:
            sizes.append(len(matrix))
            tolerances.append(kwargs["rcond"])
            return original_solve(matrix, responses, **kwargs)

        with libraries(), patch.object(preparation, "records", counted), patch.object(np.linalg, "lstsq", solve):
            output = mediation.analyze_mediation(req.data, rows, req.process_references, (req.cmi_reference,), req.config, Deadline.after(30))
        require(all(result["status"] == "fitted" for result in output["mediators"].values()), "cached mediation did not fit")
        require(1 <= calls <= 2, "mediation repeated source conversion in bootstrap draws")
        require(len(sizes) == 2 * (req.config["bootstrap"]["mediation"]["draws"] + 1), "mediation changed its bootstrap draw count")
        require(sum(size == len(req.data.rows) for size in sizes) == 2, "mediation repeated duplicate bootstrap rows in its solver")
        require(all(value == np.finfo(np.float64).eps * len(req.data.rows) for value in tolerances), "mediation compression changed the rank cutoff")
        original_products = mediation._products

        def repeated_rows(*args: Any) -> Any:
            return original_products(args[0], args[1], args[2], args[3], args[4], args[5], None, args[7], *args[8:])

        with patch.object(mediation, "_products", repeated_rows):
            reference = mediation.analyze_mediation(req.data, rows, req.process_references, (req.cmi_reference,), req.config, Deadline.after(30))
        for name in ("PSI_90", "PSI_13"):
            actual, expected = output["mediators"][name], reference["mediators"][name]
            require(actual["bootstrap"] == expected["bootstrap"], "mediation compression changed resampling metadata")
            require(actual["status"] == expected["status"], "mediation compression changed estimability")
            for key in actual["effects"]:
                for field in ("indirect_effect", "direct_effect", "total_effect", "interval"):
                    require(
                        np.allclose(actual["effects"][key][field], expected["effects"][key][field], rtol=1e-11, atol=1e-12),
                        "mediation compression changed an estimate or interval",
                    )

    def mediation_frozen_category_contrasts() -> None:
        rng = np.random.default_rng(61827)
        categories = np.asarray(["A"] * 480 + ["B"] * 480 + ["C"] * 480 + [None] * 160, dtype=object)
        rng.shuffle(categories)
        numeric = rng.normal(size=(1600, 3))
        features = (Feature("synthetic:exposure", "category"), Feature("synthetic:adjustment", "number"))
        rows = tuple(
            {
                "spine_key": f"{990000 + index // 4}:{2020 + index % 4}",
                "ccn": str(990000 + index // 4),
                "window_year": 2020 + index % 4,
                "exposure": value,
                "adjustment": None if index % 9 == 0 else float(numeric[index, 0]),
                "hai_1__sir__number": float(0.6 * (value == "B") - 0.4 * (value == "C") + numeric[index, 0] + numeric[index, 2]),
            }
            for index, value in enumerate(categories)
        )
        mediator_rows = tuple(
            {
                "spine_key": row["spine_key"],
                "measure": name,
                "value_number": float(0.8 * (row["exposure"] == "B") - 0.5 * (row["exposure"] == "C") + numeric[index, 1]),
                "period_end": date(row["window_year"] - 1, 12, 31),
                "alignment_status": "aligned",
            }
            for index, row in enumerate(rows)
            for name in ("PSI_90", "PSI_13")
        )
        encodings = []
        medians = []
        original_fit = FoldPreprocessor.fit

        def recorded_fit(self: Any, values: Any, y: Any = None) -> Any:
            result = original_fit(self, values, y)
            encodings.append(dict(self.categories_))
            medians.append(self.medians_[features[1].reference])
            return result

        with patch.object(FoldPreprocessor, "fit", recorded_fit):
            output = mediation.analyze_mediation(
                Dataset(rows, features, "HAI_1"), mediator_rows, (features[0].reference,), (features[1].reference,), req.config, Deadline.after(30)
            )
        require(all(result["status"] == "fitted" for result in output["mediators"].values()), "categorical mediation did not fit")
        require(len(encodings) >= 2 * (req.config["bootstrap"]["mediation"]["draws"] + 1), "categorical bootstrap did not refit numeric preprocessing")
        require(all(value == encodings[0] for value in encodings), "mediation categorical reference or level set changed across draws")
        require(len(set(medians)) > 2, "mediation froze numeric imputation across draws")

        def uncached(self: FeatureBatch, levels: Any) -> FeatureBatch:
            return FeatureBatch(self.rows, self.columns)

        with patch.object(FeatureBatch, "encode_categories", uncached):
            uncached_output = mediation.analyze_mediation(
                Dataset(rows, features, "HAI_1"), mediator_rows, (features[0].reference,), (features[1].reference,), req.config, Deadline.after(30)
            )
        require(output == uncached_output, "frozen category codes changed a mediation estimate or interval")
        reference = FoldPreprocessor(features, forced=(features[1].reference,)).fit(rows)
        names = reference.transform(rows).names
        for omitted in ("A", "B", "C", None):
            sample = tuple(row for row in rows if row["exposure"] != omitted)
            fitted = FoldPreprocessor(features, forced=(features[1].reference,), category_levels=reference.categories_)
            design = fitted.fit_transform(sample)
            require(fitted.categories_ == reference.categories_, "absent categorical level changed the frozen encoding")
            require(design.names == names, "absent categorical level removed a frozen column")
            for index, category in enumerate(reference.categories_[features[0].reference][1:], 1):
                column = "exposure__missing" if category is None else f"exposure__category_{index}"
                expected = np.asarray([row["exposure"] == category for row in sample], dtype=float)
                require(np.array_equal(design.matrix[:, design.names.index(column)], expected), "frozen category column changed its level identity")

        for kinds in (("category", "category"), ("category", "number"), ("number", "category")):
            shared_features = tuple(
                Feature(f"synthetic:shared_{index}", "category") if kind == "category" else Feature(f"synthetic:shared_{index}", "number")
                for index, kind in enumerate(kinds)
            )
            shared_rows = tuple(
                {
                    feature.name: None if index % 10 == 0 else str((index // (column + 1)) % 3) if feature.kind == "category" else float(index % 17)
                    for column, feature in enumerate(shared_features)
                }
                for index in range(120)
            )
            full = FoldPreprocessor(shared_features).fit(shared_rows)
            expected_names = full.transform(shared_rows).names
            no_nulls = tuple(row for row in shared_rows if row[shared_features[0].name] is not None)
            shared_design = FoldPreprocessor(shared_features, category_levels=full.categories_, indicator_aliases=full.indicator_aliases_).fit_transform(
                no_nulls
            )
            require(shared_design.names == expected_names, "absent null level changed frozen shared indicator columns")

    def model_action_budgets() -> None:
        from orchestration.training_contract import ACTION_SECONDS, PROMOTION_STEPS, TRAINING_STEPS, action_environment, parameters_for

        parameters = {"features_run": "latest", "population": "primary", "hai_types": ["HAI_1"], "trigger": "manual", "mlflow": "local"}
        require(parameters_for("training_dag", parameters) == parameters, "local analysis parameters changed at the action boundary")
        for action in ("mediation", "hac_validate"):
            require(action in TRAINING_STEPS and action not in PROMOTION_STEPS, "analysis action belongs to another workflow")
            require(action_environment("training_job", action, "real") == "offline", "analysis action lost its offline execution boundary")
        require(ACTION_SECONDS["mediation"] == 3000, "mediation step budget must be 3000 seconds")
        require(ACTION_SECONDS["hac_validate"] == 1800, "HAC step budget must stay 1800 seconds")
        configuration = json.loads((Path(__file__).resolve().parents[2] / "config/training/models.json").read_bytes())
        require(configuration["bootstrap"]["mediation"]["draws"] == 5000, "mediation production draw count changed")

    def mediation_attempted_drop_counts() -> None:
        covariate = Feature("synthetic:rare_group", "category")
        exposure = Feature("synthetic:exposure", "category")
        rows = tuple(
            dict(
                row,
                rare_group="Y" if index < 4 else "Z" if index < 8 else "A" if index % 2 else "B",
                exposure="Z" if 8 <= index < 12 else "A" if (index // 4) % 2 else "B",
            )
            for index, row in enumerate(req.data.rows)
        )
        mediator_rows = tuple(
            {
                "spine_key": row["spine_key"],
                "measure": name,
                "value_number": row["process"] * 0.8 + row["extra"] * 0.2,
                "period_end": date(row["window_year"] - 1, 12, 31),
                "alignment_status": "aligned",
            }
            for row in rows
            for name in mediation.MEDIATORS
        )
        for rare_process in (False, True):
            features = (*req.data.features, covariate, exposure)
            processes = (*req.process_references, exposure.reference) if rare_process else req.process_references
            data = replace(req.data, rows=rows, features=features)
            output = mediation.analyze_mediation(data, mediator_rows, processes, (req.cmi_reference, covariate.reference), req.config, Deadline.after(30))
            for name, result in output["mediators"].items():
                draws = req.config["bootstrap"]["mediation"]["draws"]
                seed = seed_for(req.config["master_seed"], data.hai_type, "mediation_" + name)
                omitted = [
                    (not np.any(positions < 4), not np.any((positions >= 4) & (positions < 8)), not np.any((positions >= 8) & (positions < 12)))
                    for positions in research_tests.hospital_resamples(tuple(row["ccn"] for row in rows), draws, seed)
                ]
                expected_dropped = sum(left or right for left, right, _ in omitted)
                require(sum(left and right for left, right, _ in omitted) > 0, "counter fixture never dropped two covariate columns")
                expected_invalid = sum(process for _, _, process in omitted) if rare_process else 0
                require(
                    result["status"] == "fitted" and result["bootstrap"]["invalid_draws"] == expected_invalid, "counter fixture has unexpected invalid draws"
                )
                if rare_process:
                    require(
                        sum((left or right) and process for left, right, process in omitted) > 0, "counter fixture never dropped a column in an invalid draw"
                    )
                require(
                    result["bootstrap"]["draws_with_dropped_covariate_columns"] == expected_dropped,
                    "dropped-covariate counter must count attempted draws rather than columns or valid draws",
                )

    def nullable_ownership_stub() -> None:
        with patch.dict(sys.modules, {"pandas": SimpleNamespace(NA=object())}):
            missing_ownership_policy()

    def mediation_rare_covariate_levels() -> None:
        covariate = Feature("synthetic:rare_group", "category")
        for rare in ("Z", None):
            rows = tuple(dict(row, rare_group=rare if index < 4 else "A" if index % 2 else "B") for index, row in enumerate(req.data.rows))
            data = replace(req.data, rows=rows, features=(*req.data.features, covariate))
            mediator_rows = tuple(
                {
                    "spine_key": row["spine_key"],
                    "measure": name,
                    "value_number": row["process"] * 0.8 + row["extra"] * 0.2,
                    "period_end": date(row["window_year"] - 1, 12, 31),
                    "alignment_status": "aligned",
                }
                for row in rows
                for name in mediation.MEDIATORS
            )
            output = mediation.analyze_mediation(
                data, mediator_rows, req.process_references, (req.cmi_reference, covariate.reference), req.config, Deadline.after(30)
            )
            for name, result in output["mediators"].items():
                seed = seed_for(req.config["master_seed"], data.hai_type, "mediation_" + name)
                draws = req.config["bootstrap"]["mediation"]["draws"]
                absent = sum(not np.any(positions < 4) for positions in research_tests.hospital_resamples(tuple(row["ccn"] for row in rows), draws, seed))
                require(result["status"] == "fitted" and result["bootstrap"]["invalid_draws"] == 0, "absent covariate level invalidated a mediation draw")
                require(0 < absent < draws, "rare-covariate fixture did not exercise both hospital draws")
                require(result["bootstrap"]["draws_with_dropped_covariate_columns"] == absent, "dropped-covariate draw count is wrong")
                require(set(result["effects"]) == {"process"}, "covariate removal changed process coefficient identity")
                matrix = np.column_stack(
                    (
                        np.ones(len(rows)),
                        [row["process"] for row in rows],
                        [row["cmi"] for row in rows],
                        [row["rare_group"] == "B" for row in rows],
                        [index < 4 for index in range(len(rows))],
                    )
                )
                mediator = np.asarray([row["value_number"] for row in mediator_rows if row["measure"] == name])
                coefficients = np.linalg.lstsq(matrix, np.column_stack((mediator, data.y)), rcond=None)[0]
                residuals = np.column_stack((mediator, data.y)) - matrix @ coefficients
                b = residuals[:, 0] @ residuals[:, 1] / (residuals[:, 0] @ residuals[:, 0])
                expected = coefficients[1, 0] * b
                require(
                    np.isclose(result["effects"]["process"]["indirect_effect"], expected, rtol=1e-11, atol=1e-12),
                    "covariate removal shifted a process coefficient",
                )

    def mediation_missing_process_level() -> None:
        exposure = Feature("synthetic:exposure", "category")
        widths: list[int] = []
        original_solve = np.linalg.lstsq

        def recorded_solve(matrix: Any, responses: Any, **kwargs: Any) -> Any:
            widths.append(matrix.shape[1])
            return original_solve(matrix, responses, **kwargs)

        for rare in ("Z", None):
            rows = tuple(dict(row, exposure=rare if index < 4 else "A" if index % 2 else "B") for index, row in enumerate(req.data.rows))
            data = replace(req.data, rows=rows, features=(*req.data.features, exposure))
            mediator_rows = tuple(
                {
                    "spine_key": row["spine_key"],
                    "measure": name,
                    "value_number": row["process"] * 0.8 + row["extra"] * 0.2,
                    "period_end": date(row["window_year"] - 1, 12, 31),
                    "alignment_status": "aligned",
                }
                for row in rows
                for name in mediation.MEDIATORS
            )
            widths.clear()
            with patch.object(np.linalg, "lstsq", recorded_solve):
                output = mediation.analyze_mediation(data, mediator_rows, (exposure.reference,), (req.cmi_reference,), req.config, Deadline.after(30))
            require(set(widths) == {4}, "absent process level was removed from the frozen design")
            for name, result in output["mediators"].items():
                seed = seed_for(req.config["master_seed"], data.hai_type, "mediation_" + name)
                draws = req.config["bootstrap"]["mediation"]["draws"]
                absent = sum(not np.any(positions < 4) for positions in research_tests.hospital_resamples(tuple(row["ccn"] for row in rows), draws, seed))
                require(
                    result["status"] == "fitted" and result["bootstrap"]["invalid_draws"] == absent,
                    "absent process level was dropped instead of invalidating its draw",
                )
                require(result["bootstrap"]["draws_with_dropped_covariate_columns"] == 0, "process column was counted as a dropped covariate")

    def mediation_frozen_indicator_aliases() -> None:
        rows = tuple(dict(row, cmi=None, extra=None) if index < 4 else row for index, row in enumerate(req.data.rows))
        data = replace(req.data, rows=rows)
        mediator_rows = tuple(
            {
                "spine_key": row["spine_key"],
                "measure": name,
                "value_number": row["process"] * 0.8 + req.data.rows[index]["extra"] * 0.2 + 0.1 * np.sin(index),
                "period_end": date(row["window_year"] - 1, 12, 31),
                "alignment_status": "aligned",
            }
            for index, row in enumerate(rows)
            for name in mediation.MEDIATORS
        )
        aliases = []
        original = FoldPreprocessor.fit

        def recorded(self: Any, values: Any, y: Any = None) -> Any:
            result = original(self, values, y)
            aliases.append(dict(self.indicator_aliases_))
            return result

        with patch.object(FoldPreprocessor, "fit", recorded):
            output = mediation.analyze_mediation(
                data, mediator_rows, req.process_references, (req.cmi_reference, req.data.features[2].reference), req.config, Deadline.after(30)
            )
        require(
            len(aliases) == 2 * (req.config["bootstrap"]["mediation"]["draws"] + 2) and all(value == {"extra__missing": "cmi__missing"} for value in aliases),
            "mediation did not freeze shared indicator aliases across draws",
        )
        require(
            all(result["status"] == "fitted" and result["bootstrap"]["invalid_draws"] == 0 for result in output["mediators"].values()),
            "frozen shared-indicator mediation did not fit every draw",
        )

    def native_shap_failure() -> None:
        class ExplainerFailure(Exception):
            pass

        try:
            with (
                libraries() as replacements,
                patch.object(panel, "native_fits", side_effect=panel_standin),
                patch.object(replacements["shap.utils._exceptions"], "ExplainerError", ExplainerFailure),
                patch.object(replacements["shap"], "TreeExplainer", side_effect=ExplainerFailure("synthetic explainer failure")),
            ):
                output = suite.fit_models(req)
        except ExplainerFailure as error:
            raise AssertionError("native SHAP failure discarded the fitted slots") from error
        require(output.shap["status"] == "not_estimable" and output.candidate is not None, "native SHAP failure lost its explicit status or candidate")

    def configured_shap_sample() -> None:
        config = {**req.config, "shap": {"sample_cap": 7}}
        with libraries(), patch.object(panel, "native_fits", side_effect=panel_standin):
            output = suite.fit_models(replace(req, config=config))
        require(output.shap["sample_count"] == 7 and output.shap["sample_cap"] == 7, "SHAP ignored the configured sample cap")

    def approved_nested_policy() -> None:
        changed = replace(req, feature_lists={**req.feature_lists, "rq4_reduced": (req.process_references[0],)})
        with libraries():
            result = research_tests.nested_comparison(changed)
        require(result["status"] == "fitted" and result["cmi_in_reduced"] is False, "CMS-only nested comparison did not fit without CMI")
        require(req.cmi_reference in result["full_references"], "nested full model lost CMI")
        invalid = [req.data.features[1], req.data.features[2]]
        invalid.extend(
            Feature(f"feature_matrix_hospital_window:{name}", "number", family="hospital_window") for name in ("cmi_cases", "has_cmi", "is_cmi_held")
        )
        invalid.extend(
            (Feature("feature_matrix_cost_report:cost", "number", family="cost_report"), Feature("feature_matrix_county:svi", "number", family="county"))
        )
        for feature in invalid:
            extra = () if feature.reference in {item.reference for item in req.data.features} else (feature,)
            rows = tuple(dict(row, **{feature.name: 1.0}) for row in req.data.rows) if extra else req.data.rows
            data = replace(req.data, rows=rows, features=(*req.data.features, *extra))
            lists = {
                **changed.feature_lists,
                "rq4_reduced": (req.process_references[0], feature.reference),
                "m2_elastic_net": (*req.feature_lists["m2_elastic_net"], *(item.reference for item in extra)),
            }
            with libraries():
                rejects(partial(research_tests.nested_comparison, replace(changed, data=data, feature_lists=lists)), "CMS-only")
        lists = {
            **changed.feature_lists,
            "m2_elastic_net": tuple(reference for reference in req.feature_lists["m2_elastic_net"] if reference != req.cmi_reference),
        }
        with libraries():
            rejects(lambda: research_tests.nested_comparison(replace(changed, feature_lists=lists)), "full list omits CMI")

        designated = Feature("feature_matrix_hospital_window:declared_case_mix", "number", family="hospital_window")
        renamed = replace(
            changed,
            cmi_reference=designated.reference,
            data=replace(
                req.data,
                features=(req.data.features[0], designated, req.data.features[2]),
                rows=tuple({**{key: value for key, value in row.items() if key != "cmi"}, designated.name: row["cmi"]} for row in req.data.rows),
            ),
            feature_lists={
                **{
                    slot: tuple(designated.reference if reference == req.cmi_reference else reference for reference in references)
                    for slot, references in changed.feature_lists.items()
                },
                "rq4_reduced": (req.process_references[0], designated.reference),
            },
        )
        with libraries():
            rejects(lambda: research_tests.nested_comparison(renamed), "CMS-only")

    def ownership_fixed_part_policy() -> None:
        group = Feature("feature_matrix_hospital_window:control_type_code", "category", family="hospital_window")
        lists = {**req.feature_lists, **{slot: (*req.feature_lists[slot], group.reference) for slot in SLOTS}}
        changed = replace(req, data=replace(req.data, features=(*req.data.features, group)), feature_lists=lists)
        with libraries(), patch.object(panel, "native_fits", side_effect=panel_standin):
            for slot in SLOTS:
                result = suite.fit_slot(changed, slot)
                require(result.status == "fitted", f"ownership fixed-part policy prevented {slot}: {result.reason}")
                kept = result.details["selected_references"]
                require((group.reference in kept) == (slot != "m3_mixed"), f"ownership fixed-part policy is wrong for {slot}")
                if slot == "m3_mixed":
                    require(
                        not any(name.startswith("control_type_code") for name in result.details["coefficients"]), "mixed fixed coefficients contain ownership"
                    )
        require(changed.feature_lists == lists, "mixed ownership exclusion mutated the caller's feature lists")

    def missing_ownership_policy() -> None:
        nullable = getattr(sys.modules.get("pandas"), "NA", None)
        nulls = (None, float("nan"), np.float64("nan"), np.float32("nan"))
        for null in (*nulls, nullable) if nullable is not None else nulls:
            missing_ownership_value(null)

    def missing_ownership_value(null: Any) -> None:
        missing = {0, 3, 5, 11}
        rows = tuple(
            dict(row, control_type_code=null, process=10000.0 if row["window_year"] < 2023 else row["process"]) if index in missing else row
            for index, row in enumerate(req.data.rows)
        )
        changed = replace(req, data=replace(req.data, rows=rows), feature_lists={**req.feature_lists, "rq4_reduced": (req.process_references[0],)})
        with libraries(), patch.object(panel, "native_fits", side_effect=panel_standin):
            training, _ = changed.data.split()
            design = FoldPreprocessor(changed.data.features).fit_transform(training.rows)
            rejects(lambda: suite.MixedRegressor().fit(design, training.y), "ownership group is missing")
            result = suite.fit_slot(changed, "m3_mixed")
            require(result.status == "fitted" and len(result.predictions) == 46, "mixed model did not exclude only missing-ownership windows")
            require(result.details["excluded_missing_ownership"] == {"training": 2, "holdout": 2, "total": 4}, "missing-ownership counts are wrong")
            require(result.details["training_rows"] == 142 and result.details["holdout_rows"] == 46, "mixed eligible row counts are wrong")
            require(result.holdout_positions == tuple(index for index in range(48) if index not in (0, 2)), "mixed predictions lost their holdout positions")
            training, _ = changed.data.split()
            expected_median = float(np.median([row["process"] for row in training.rows if isinstance(row["control_type_code"], str)]))
            require(
                result.pipeline.named_steps["prepare"].medians_[req.process_references[0]] == expected_median,
                "excluded ownership rows changed mixed preprocessing",
            )
            original_moran = suite.moran_test
            _, full_holdout = changed.data.split()
            eligible = full_holdout.subset(result.holdout_positions)

            def checked_moran(residuals: Any, keys: Any, *args: Any, **kwargs: Any) -> Any:
                require(
                    keys == eligible.keys and np.allclose(residuals, eligible.y - np.asarray(result.predictions)),
                    "Moran residuals do not match the eligible mixed holdout rows",
                )
                return original_moran(residuals, keys, *args, **kwargs)

            with patch.object(suite, "moran_test", side_effect=checked_moran):
                output = suite.fit_models(changed)
            repeated = suite.fit_models(replace(changed, deadline=Deadline.after(60)))
            for slot in ("m1_ols", "m2_elastic_net", "m4_boosting", "m6_fixed_effects"):
                require(output.slots[slot].status == "fitted" and len(output.slots[slot].predictions) == 48, f"missing ownership removed rows from {slot}")
                require(output.slots[slot].details["training_rows"] == 144, f"missing ownership removed training rows from {slot}")
            payload = output.to_result()
            require(payload == repeated.to_result(), "missing-ownership results changed on rerun")
            mixed = output.slots["m3_mixed"]
            for changed_mixed in (
                replace(mixed, predictions=(mixed.predictions[0] + 0.1, *mixed.predictions[1:])),
                replace(mixed, holdout_positions=(0, *mixed.holdout_positions[1:])),
            ):
                changed_output = replace(output, slots={**output.slots, "m3_mixed": changed_mixed})
                require(
                    changed_output.to_result()["holdout_predictions_content_sha256"] != payload["holdout_predictions_content_sha256"],
                    "canonical prediction hash lost a mixed prediction or its row identity",
                )
            require(payload["counts"]["training"] == 144 and payload["counts"]["holdout"] == 48, "mixed filtering changed global split counts")
            require(payload["selection"]["comparison_holdout_rows"] == 46, "model selection used unmatched holdout cohorts")
            require(payload["selection"]["registry_holdout_rows"] == 48, "mixed exclusions changed the registry cohort")
            require(
                output.research["spatial"]["residual_model"] == "m3_mixed" and output.research["spatial"]["included_hospitals"] == 46,
                "Moran residuals lost the mixed model's eligible row alignment",
            )
            _, holdout = changed.data.split()
            tree_predictions = dict(zip(holdout.keys, output.slots["m4_boosting"].predictions, strict=True))
            stressed_rows = tuple(
                dict(row, **{changed.data.label: tree_predictions[row["spine_key"]] if isinstance(row["control_type_code"], str) else 10000.0})
                if row["window_year"] == 2023
                else row
                for row in changed.data.rows
            )
            stressed = suite.fit_models(replace(changed, data=replace(changed.data, rows=stressed_rows), deadline=Deadline.after(60)))
            require(stressed.selected == "m4_boosting" and stressed.candidate == "m4_boosting", "model selection ranked different holdout cohorts")
            require(stressed.to_result()["selection"]["comparison_rmse"]["m4_boosting"] == 0.0, "comparison RMSE used excluded mixed holdout windows")
            for split in ("training", "holdout"):
                empty_rows = tuple(
                    dict(row, control_type_code=null) if ((row["window_year"] < 2023) == (split == "training")) else row for row in req.data.rows
                )
                failed = suite.fit_slot(replace(changed, data=replace(req.data, rows=empty_rows)), "m3_mixed")
                require(failed.status == "not_estimable" and "ownership" in failed.reason, f"empty mixed {split} split did not produce its intended status")
                require(
                    failed.details["excluded_missing_ownership"][split] == (144 if split == "training" else 48), "failed mixed model lost its exclusion count"
                )

    def robust_spatial_choice_policy() -> None:
        choices = (
            ((8.0, 0.01), (20.0, 0.5), "lag", "one_significant_robust_alternative"),
            ((20.0, 0.5), (8.0, 0.01), "error", "one_significant_robust_alternative"),
            ((10.0, 0.02), (9.0, 0.001), "lag", "both_significant_larger_robust_statistic"),
            ((9.0, 0.001), (10.0, 0.02), "error", "both_significant_larger_robust_statistic"),
        )
        for lag, error, expected, rule in choices:

            class Diagnostics(SpatialOLS):
                def __init__(self, *args: Any, lag: tuple[float, float] = lag, error: tuple[float, float] = error, **kwargs: Any) -> None:
                    super().__init__(*args, **kwargs)
                    self.rlm_lag, self.rlm_error = lag, error

            with libraries() as replacements:
                replacements["spreg"].OLS = Diagnostics
                result = suite.fit_slot(req, "m5_spatial")
            require(result.status == "fitted" and result.details["kind"] == expected, "robust LM choice ignored significance or the larger statistic")
            require(result.details["lm_selection"] == rule, "spatial model did not record its selection rule")
        for lag, error, reason in (
            ((1.0, 0.05), (2.0, 0.5), "significant"),
            ((8.0, 0.01), (8.0, 0.01), "equal"),
            ((float("nan"), 0.01), (8.0, 0.01), "finite"),
        ):

            class RejectedDiagnostics(SpatialOLS):
                def __init__(self, *args: Any, lag: tuple[float, float] = lag, error: tuple[float, float] = error, **kwargs: Any) -> None:
                    super().__init__(*args, **kwargs)
                    self.rlm_lag, self.rlm_error = lag, error

            with libraries() as replacements:
                replacements["spreg"].OLS = RejectedDiagnostics
                result = suite.fit_slot(req, "m5_spatial")
            require(result.status == "not_estimable" and reason in result.reason, "unresolved robust LM diagnostics produced a fitted model")

    return [
        ("model_action_budgets", model_action_budgets),
        ("mediation_attempted_drop_counts", mediation_attempted_drop_counts),
        ("nullable_ownership_stub", nullable_ownership_stub),
        ("mediation_rare_covariate_levels", mediation_rare_covariate_levels),
        ("mediation_missing_process_level", mediation_missing_process_level),
        ("mediation_frozen_indicator_aliases", mediation_frozen_indicator_aliases),
        ("approved_nested_policy", approved_nested_policy),
        ("ownership_fixed_part_policy", ownership_fixed_part_policy),
        ("missing_ownership_policy", missing_ownership_policy),
        ("robust_spatial_choice_policy", robust_spatial_choice_policy),
        ("fold_local_missingness", preprocessing),
        ("unseen_category", unknown_category),
        ("extract_input_contract", input_contract),
        ("seeded_hospital_and_time_folds", folds),
        ("six_slot_caller_and_repeatability", complete_suite),
        ("rank_deficient_ols", collinear_ols),
        ("unpenalized_cmi", forced_elastic),
        ("singleton_ownership", ownership_singleton),
        ("rmse_selection_and_registration", tied_selection),
        ("operation_deadline", expired),
        ("nested_gain", nested_gain),
        ("invalid_nesting", nested_invalid),
        ("nested_no_gain", nested_no_gain),
        ("spatial_order_and_islands", spatial_order_and_islands),
        ("seeded_moran_pattern", moran_signal),
        ("hausman_covariance", hausman_contract),
        ("unbalanced_panel_and_unseen_effect", panel_unbalanced_and_unseen),
        ("shap_additivity_refusal", shap_failure),
        ("separated_mediation", mediation_separation),
        ("hac_year_join_and_missingness", hac_join),
        ("shared_missing_indicators", shared_missing_indicators),
        ("tuning_prepares_each_fold_once", tuning_prepares_each_fold_once),
        ("nested_indicator_alias_ownership", nested_indicator_alias_ownership),
        ("excluded_controls_from_column_names", excluded_controls_from_column_names),
        ("panel_effect_normalization", panel_effect_normalization),
        ("aggregate_panel_counts", result_has_aggregate_panel_counts),
        ("nested_and_joint_values", nested_and_joint_values),
        ("mixed_values_and_ownership_hold", mixed_values_and_unresolved_ownership),
        ("mediation_known_values", mediation_known_values),
        ("moran_island_values_and_work", moran_island_values_and_work),
        ("optional_analysis_failures", optional_analysis_failures),
        ("configured_shap_sample", configured_shap_sample),
        ("native_shap_failure", native_shap_failure),
        ("mediation_prepares_source_once", mediation_prepares_source_once),
        ("mediation_frozen_category_contrasts", mediation_frozen_category_contrasts),
    ]


def source_paths(root: Path) -> list[Path]:
    """Name only model implementation and caller-check sources for bounded evidence hashing."""
    return sorted(
        [
            *root.joinpath("scripts/training/models").glob("*.py"),
            root / "orchestration/training_contract.py",
            root / "config/training/models.json",
            root / "data_contracts/training/models.schema.json",
            *[
                root / f"scripts/training/{name}.py"
                for name in ("research_tests", "mediation", "hac_validation", "model_cases", "model_test_support", "run_model_checks")
            ],
        ]
    )
