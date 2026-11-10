"""Synthetic cases for automatic promotion: the plan from a research record, the canary checks, the release of held online data and the owner notice."""

from __future__ import annotations

import io
import json
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest import mock

from scripts.training import auto_promotion
from scripts.training.lane_b_cases import Cases, require

RUN = "20261024T000000Z-1234abcd"
RESEARCH = {
    "schema": 1,
    "hai_type": "HAI_1",
    "promotion": {"registered_model": "hai_model_HAI_1", "candidates": [], "challenger": {"model": "m2", "version": "4", "verdict": "pass"}},
}
VERSION = "4"
OFFLINE = {f"{index:06d}:2023": 0.5 + index / 100 for index in range(40)}
ROWS = [
    {"ccn": f"{index:06d}", "window_year": 2023, "served_by": "canary", "model_version": VERSION, "prediction": OFFLINE[f"{index:06d}:2023"], "seconds": 0.05}
    for index in range(40)
] + [{"ccn": f"9{index:05d}", "window_year": 2023, "served_by": "stable", "model_version": "3", "prediction": 0.9, "seconds": 0.05} for index in range(60)]
REPORT = {
    "requests": 100,
    "statuses": {"200": 100},
    "served_by": {"stable": 60, "canary": 40},
    "comparison": {"status": "ok", "stable": {"count": 60}, "canary": {"count": 40}},
    "rows": ROWS,
}


def judged(report: dict[str, Any], offline: dict[str, float] | None = None, version: str = VERSION) -> dict[str, Any]:
    """The canary decision for a report against the offline predictions of the candidate version."""
    return auto_promotion.canary_checks(report, OFFLINE if offline is None else offline, version)


def raises(call: Any, message: str) -> None:
    """Require the call to refuse with a message that names the cause."""
    try:
        call()
    except (auto_promotion.PlanRefused, ValueError, RuntimeError) as error:
        require(message in str(error), f"the refusal names another cause: {error}")
    else:
        raise ValueError("the invalid input was accepted")


def command(arguments: list[str]) -> tuple[int, str, str]:
    """Run the command line and return its exit code, standard output and standard error."""
    out, err = io.StringIO(), io.StringIO()
    with redirect_stdout(out), redirect_stderr(err):
        code = auto_promotion.main(arguments)
    return code, out.getvalue(), err.getvalue()


def auto_promotion_cases(cases: Cases, folder: Path) -> None:
    """Plan, judge, release and notify without a service."""
    folder.mkdir(parents=True)

    def plans_canary_and_promotion() -> None:
        result = auto_promotion.plan(RESEARCH, 10)
        require(result["canary"] == {"hai_type": "HAI_1", "candidate_version": "4", "percent": 10, "deploy_mode": "canary"}, f"canary: {result.get('canary')}")
        require(result["promotion"] == {"hai_type": "HAI_1", "model_version": "4", "promotion_mode": "promote"}, f"promotion: {result.get('promotion')}")

    def plans_nothing_without_a_challenger() -> None:
        record = {**RESEARCH, "promotion": {**RESEARCH["promotion"], "challenger": None}}
        require(auto_promotion.plan(record, 10) == {"hai_type": "HAI_1", "challenger": None}, "a type without a challenger got a plan")

    def refuses_ineligible_challengers() -> None:
        for challenger in ({"model": "m3", "version": "4", "verdict": "pass"}, {"model": "m2", "version": "4", "verdict": "fail:minimum_improvement"}):
            raises(
                lambda challenger=challenger: auto_promotion.plan({**RESEARCH, "promotion": {**RESEARCH["promotion"], "challenger": challenger}}, 10),
                "challenger",
            )

    def refuses_a_canary_share_outside_the_serving_contract() -> None:
        raises(lambda: auto_promotion.plan(RESEARCH, 51), "percent")

    def canary_passes() -> None:
        require(judged(REPORT) == {"decision": "promote", "failed": []}, f"a clean canary was held: {judged(REPORT)}")

    def canary_holds() -> None:
        failed_request = {**REPORT, "statuses": {"200": 99, "500": 1}}
        thin_rows = [row for row in ROWS if row["served_by"] == "stable" or int(row["ccn"]) < 29]
        thin = {**REPORT, "rows": thin_rows, "comparison": {"status": "insufficient", "stable": {"count": 60}, "canary": {"count": 29}}}
        slow = {**REPORT, "rows": [{**ROWS[0], "seconds": 5.5}, *ROWS[1:]]}
        other_version = {**REPORT, "rows": [{**ROWS[0], "model_version": "5"}, *ROWS[1:]]}
        off = {**REPORT, "rows": [{**ROWS[0], "prediction": ROWS[0]["prediction"] * (1 + 2e-6)}, *ROWS[1:]]}
        within = {**REPORT, "rows": [{**ROWS[1], "prediction": ROWS[1]["prediction"] * (1 + 5e-7)}, *ROWS[:1], *ROWS[2:]]}
        unscored = {**REPORT, "rows": [{**ROWS[0], "ccn": "777777"}, *ROWS[1:]]}
        require(judged(failed_request) == {"decision": "hold", "failed": ["requests_answered"]}, "a failed request did not hold the canary")
        require(judged(thin) == {"decision": "hold", "failed": ["both_sides_compared"]}, f"too few canary rows did not hold the canary: {judged(thin)}")
        few_rows = {**REPORT, "rows": thin_rows}
        require(
            judged(few_rows) == {"decision": "hold", "failed": ["both_sides_compared"]}, f"29 canary rows passed on a report that says ok: {judged(few_rows)}"
        )
        require(judged(slow) == {"decision": "hold", "failed": ["within_time_limit"]}, f"a slow answer did not hold the canary: {judged(slow)}")
        require(judged(other_version) == {"decision": "hold", "failed": ["canary_version"]}, f"another version served as the canary: {judged(other_version)}")
        require(
            judged(off) == {"decision": "hold", "failed": ["predictions_match_offline"]}, f"a prediction off by 2e-6 did not hold the canary: {judged(off)}"
        )
        require(judged(within)["decision"] == "promote", f"a difference of 5e-7 held the canary: {judged(within)}")
        require(
            judged(unscored) == {"decision": "hold", "failed": ["predictions_match_offline"]}, f"a row with no offline prediction passed: {judged(unscored)}"
        )
        require(judged({})["decision"] == "hold", "an empty report did not hold the canary")

    def record_binds_the_result_to_the_candidate() -> None:
        store = SimpleNamespace(
            versions={"4": SimpleNamespace(version="4", run_id="mlrun4", tags={})},
            tags={},
        )
        store.set_model_version_tag = lambda name, number, key, value: store.tags.setdefault(number, {}).__setitem__(key, value)
        store.search_model_versions = lambda filter_string="": list(store.versions.values())
        held = auto_promotion.record_canary(store, "hai_model_HAI_1", VERSION, {**REPORT, "statuses": {"200": 99, "500": 1}}, OFFLINE)
        require(held["decision"] == "hold" and not store.tags, f"a held canary left tags: {held} {store.tags}")
        done = auto_promotion.record_canary(store, "hai_model_HAI_1", VERSION, REPORT, OFFLINE)
        require(done["decision"] == "promote", f"a clean canary was held: {done}")
        require(
            store.tags["4"] == {"canary_verdict": "promote", "canary_version": "4", "canary_run": "mlrun4"},
            f"the result is not bound to the version and its run: {store.tags}",
        )

    def schema_comparison() -> None:
        promoted, online = {"feature_list_sha256": "a" * 64}, {"feature_list_sha256": "a" * 64}
        require(not auto_promotion.schema_changed(promoted, online), "the same feature schema was reported as changed")
        require(auto_promotion.schema_changed(promoted, {"feature_list_sha256": "b" * 64}), "another feature schema was not reported")
        require(auto_promotion.schema_changed(promoted, None), "a model with no online predecessor needs its online data")

    def release_calls_the_feast_command() -> None:
        calls: list[tuple[str, list[str]]] = []

        def stand_in(program: str, args: list[str], **_: Any) -> Any:
            calls.append((program, args))
            return SimpleNamespace(returncode=0, stderr="")

        with mock.patch.object(auto_promotion, "run_command", side_effect=stand_in):
            auto_promotion.release("m2", RUN)
        require(len(calls) == 1 and calls[0][1] == ["-m", "scripts.features.feast_materialize", "--release", "m2", RUN], f"calls: {calls}")

    def release_failure_is_visible() -> None:
        def failing(program: str, args: list[str], **_: Any) -> Any:
            return SimpleNamespace(returncode=3, stderr="no held run")

        with mock.patch.object(auto_promotion, "run_command", side_effect=failing):
            raises(lambda: auto_promotion.release("m2", RUN), "exit code 3")
            raises(lambda: auto_promotion.release("m7", RUN), "release needs")
            raises(lambda: auto_promotion.release("m2", "latest"), "release needs")

    def notice_reaches_the_topic() -> None:
        sent: list[dict[str, Any]] = []
        client = SimpleNamespace(publish=lambda **kwargs: sent.append(kwargs) or {"MessageId": "m-1"})
        require(
            auto_promotion.notify(client, "arn:aws:sns:example-region:example-account:alerts", "HAI_1", "promoted", "m2 version 4") == "m-1", "no message ID"
        )
        require(sent[0]["Subject"] == "model promoted: HAI_1" and "m2 version 4" in sent[0]["Message"], f"notice: {sent}")
        raises(lambda: auto_promotion.notify(client, "arn", "HAI_1", "approved", "x"), "outcome")
        require(len(sent) == 1, "a refused notice was sent")

    def command_line() -> None:
        research, report = folder / "research.json", folder / "report.json"
        research.write_text(json.dumps(RESEARCH))
        report.write_text(json.dumps(REPORT))
        code, out, _ = command(["plan", "--research", str(research), "--percent", "10"])
        require(code == 0 and json.loads(out)["promotion"]["model_version"] == "4", f"plan: {code} {out}")
        with_predictions = {**RESEARCH, "promotion": {**RESEARCH["promotion"], "challenger_predictions": OFFLINE}}
        research.write_text(json.dumps(with_predictions))
        report.write_text(json.dumps(REPORT))
        code, out, _ = command(["verdict", "--report", str(report), "--research", str(research)])
        require(code == 0 and json.loads(out)["decision"] == "promote", f"verdict: {code} {out}")
        research.write_text(json.dumps(RESEARCH))
        code, _, error = command(["verdict", "--report", str(report), "--research", str(research)])
        require(code == 2 and "offline predictions" in error, f"a research record without offline predictions judged a canary: {code} {error}")
        research.write_text(json.dumps(RESEARCH))
        code, _, error = command(["plan", "--research", str(research), "--percent", "99"])
        require(code == 2 and json.loads(error)["failure"] == "ValueError", f"refusal: {code} {error}")

    cases.check("auto_promotion_plans_the_canary_and_the_promotion_of_a_challenger", plans_canary_and_promotion)
    cases.check("auto_promotion_plans_nothing_without_a_challenger", plans_nothing_without_a_challenger)
    cases.check("auto_promotion_refuses_an_ineligible_challenger", refuses_ineligible_challengers)
    cases.check("auto_promotion_refuses_a_canary_share_outside_the_serving_contract", refuses_a_canary_share_outside_the_serving_contract)
    cases.check("auto_promotion_canary_checks_pass_a_clean_canary", canary_passes)
    cases.check("auto_promotion_canary_checks_hold_a_failed_or_thin_canary", canary_holds)
    cases.check("auto_promotion_canary_result_is_bound_to_the_version_and_its_run", record_binds_the_result_to_the_candidate)
    cases.check("auto_promotion_compares_the_feature_schema_with_the_online_one", schema_comparison)
    cases.check("auto_promotion_release_calls_the_feast_release_command", release_calls_the_feast_command)
    cases.check("auto_promotion_release_failure_is_visible", release_failure_is_visible)
    cases.check("auto_promotion_notice_reaches_the_alerts_topic", notice_reaches_the_topic)
    cases.check("auto_promotion_command_line_prints_one_json_line_or_refuses", command_line)
