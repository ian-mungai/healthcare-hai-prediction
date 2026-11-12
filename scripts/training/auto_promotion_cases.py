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
RESEARCH: dict[str, Any] = {
    "schema": 1,
    "hai_type": "HAI_1",
    "base_run": "20261023T000000Z-5678abcd",
    "runs": {"m2": RUN},
    "promotion": {
        "registered_model": "hai_model_HAI_1",
        "candidates": [],
        "challenger": {"model": "m2", "version": "4", "verdict": "pass", "source_run": "mlrun4"},
    },
}
VERSION = "4"
OFFLINE = {f"{index:06d}:2023": 0.5 + index / 100 for index in range(40)}
ROWS: list[dict[str, Any]] = [
    {
        "ccn": f"{index:06d}",
        "window_year": 2023,
        "served_by": "canary",
        "model_version": VERSION,
        "prediction": OFFLINE[f"{index:06d}:2023"],
        "attempts": [{"status": 200, "seconds": 0.05}],
        "elapsed_seconds": 0.05,
    }
    for index in range(40)
] + [
    {
        "ccn": f"9{index:05d}",
        "window_year": 2023,
        "served_by": "stable",
        "model_version": "3",
        "prediction": 0.9,
        "attempts": [{"status": 200, "seconds": 0.05}],
        "elapsed_seconds": 0.05,
    }
    for index in range(60)
]
REPORT: dict[str, Any] = {
    "hai_type": "HAI_1",
    "requests": 100,
    "statuses": {"200": 100},
    "attempt_statuses": {"200": 100},
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


def replayed(record: dict[str, Any], hai_type: str | None = None, limited: dict[str, float] | None = None) -> dict[str, Any]:
    """Replay a 60-row corpus through ``replay.run`` against a stand-in API: 30 of the record's holdout rows go to the canary, which answers with the
    record's offline predictions under the challenger's version, and 30 other hospital-windows go to the stable model.

    Every answer takes 0.01 s. A key in ``limited`` first answers 429 after the seconds it maps to, with ``Retry-After: 1``, and then 200."""
    from scripts.serving import replay

    offline = record["promotion"]["challenger_predictions"]
    canary_keys = sorted(offline)[:30]
    corpus = [(key.split(":")[0], int(key.split(":")[1])) for key in canary_keys] + [(f"8{index:05d}", 2023) for index in range(30)]
    now = [0.0]
    pending = dict(limited or {})

    def sleep(seconds: float) -> None:
        now[0] += seconds

    def send(payload: dict[str, Any], timeout: float) -> tuple[int, dict[str, str], dict[str, Any]]:
        key = f"{payload['ccn']}:{payload['window_year']}"
        if key in pending:
            now[0] += pending.pop(key)
            return 429, {"Retry-After": "1"}, {"error": "rate_limited"}
        now[0] += 0.01
        if key in offline:
            return 200, {}, {"served_by": "canary", "prediction": offline[key], "model_version": record["promotion"]["challenger"]["version"]}
        return 200, {}, {"served_by": "stable", "prediction": 0.9, "model_version": "1"}

    return replay.run(send, corpus, hai_type or record["hai_type"], clock=lambda: now[0], sleep=sleep, deadline_seconds=900.0)


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
        answered = {"requests": len(thin_rows), "statuses": {"200": len(thin_rows)}, "attempt_statuses": {"200": len(thin_rows)}}
        thin = {**REPORT, **answered, "rows": thin_rows, "comparison": {"status": "insufficient", "stable": {"count": 60}, "canary": {"count": 29}}}
        slow = {**REPORT, "rows": [{**ROWS[0], "elapsed_seconds": 5.5}, *ROWS[1:]]}
        other_version = {**REPORT, "rows": [{**ROWS[0], "model_version": "5"}, *ROWS[1:]]}
        off = {**REPORT, "rows": [{**ROWS[0], "prediction": ROWS[0]["prediction"] * (1 + 2e-6)}, *ROWS[1:]]}
        within = {**REPORT, "rows": [{**ROWS[1], "prediction": ROWS[1]["prediction"] * (1 + 5e-7)}, *ROWS[:1], *ROWS[2:]]}
        unscored = {**REPORT, "rows": [{**ROWS[0], "ccn": "777777"}, *ROWS[1:]]}
        require(judged(failed_request) == {"decision": "hold", "failed": ["requests_answered"]}, "a failed request did not hold the canary")
        require(judged(thin) == {"decision": "hold", "failed": ["both_sides_compared"]}, f"too few canary rows did not hold the canary: {judged(thin)}")
        few_rows = {**REPORT, **answered, "rows": thin_rows}
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
        registered = {"model": "m2", "hai_type": "HAI_1", "base_run": RESEARCH["base_run"], "source_run_id": "mlrun4"}
        store = SimpleNamespace(
            versions={"4": SimpleNamespace(version="4", run_id="mlrun4", tags=registered)},
            tags={},
        )
        store.set_model_version_tag = lambda name, number, key, value: store.tags.setdefault(number, {}).__setitem__(key, value)
        store.search_model_versions = lambda filter_string="": list(store.versions.values())
        store.get_run = lambda run_id: SimpleNamespace(data=SimpleNamespace(tags={"run_id": RUN}))
        record = {**RESEARCH, "promotion": {**RESEARCH["promotion"], "challenger_predictions": OFFLINE}}
        held = auto_promotion.record_canary(store, record, {**REPORT, "statuses": {"200": 99, "500": 1}})
        require(held["decision"] == "hold" and not store.tags, f"a held canary left tags: {held} {store.tags}")
        done = auto_promotion.record_canary(store, record, REPORT)
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
            return SimpleNamespace(returncode=0, stderr="", stdout='{"feature_set": "m2", "outcome": "not_held", "run_id": "' + RUN + '"}\n')

        with mock.patch.object(auto_promotion, "run_command", side_effect=stand_in):
            outcome = auto_promotion.release("m2", RUN)
        require(len(calls) == 1 and calls[0][1] == ["-m", "scripts.features.online_release", "--model", "m2", "--features-run", RUN], f"calls: {calls}")
        require(outcome == "not_held", f"the release outcome was not returned: {outcome}")
        with mock.patch.object(auto_promotion, "run_command", side_effect=stand_in):
            code, out, _ = command(["release", "--model", "m2", "--features-run", RUN])
        require(code == 0 and json.loads(out) == {"features_run": RUN, "outcome": "not_held", "released": "m2"}, f"release command: {code} {out}")

    def release_failure_is_visible() -> None:
        def failing(program: str, args: list[str], **_: Any) -> Any:
            return SimpleNamespace(returncode=3, stderr="no held run")

        with mock.patch.object(auto_promotion, "run_command", side_effect=failing):
            raises(lambda: auto_promotion.release("m2", RUN), "exit code 3")
            raises(lambda: auto_promotion.release("m7", RUN), "release needs")
            raises(lambda: auto_promotion.release("m2", "latest"), "release needs")

    def notice_reaches_the_topic() -> None:
        sent: list[dict[str, Any]] = []

        def publish(**kwargs: Any) -> dict[str, str]:
            sent.append(kwargs)
            return {"MessageId": "m-1"}

        client = SimpleNamespace(publish=publish)
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

    def record_refuses_a_candidate_of_another_identity() -> None:
        """The reviewer's scenario: a research record and its replay from one tracking store certify version 3 of another store, which holds another run."""
        from scripts.training import research_cases
        from scripts.training.job_action_cases import standins

        source, fixtures = research_cases.finished_models(folder / "identity_a")
        code, _, error, output = research_cases.research(folder / "identity_a_research", fixtures["m2"])
        require(code == 0, f"research failed: {error}")
        research_path = output / f"research_{research_cases.HAI_TYPE}.json"
        record = json.loads(research_path.read_text())
        number = str(record["promotion"]["challenger"]["version"])
        other_runs = {"m1": "20261025T000001Z-1234abcd", "m2": "20261025T000002Z-1234abcd", "m4": "20261025T000004Z-1234abcd"}
        other, other_fixtures = research_cases.finished_models(folder / "identity_b", models=("m1", "m4", "m2"), runs=other_runs)

        def canary_tags(store: Any) -> dict[str, dict[str, str]]:
            return {
                str(item.version): {key: value for key, value in item.tags.items() if key.startswith("canary_")}
                for item in store.search_model_versions(f"name='{research_cases.NAME}'")
                if any(key.startswith("canary_") for key in item.tags)
            }

        def recorded(store_fixture: Any, research_file: Path, report: dict[str, Any], step: str = "record") -> tuple[int, str, str]:
            report_path = folder / f"identity_report_{len(list(folder.glob('identity_report_*')))}.json"
            report_path.write_text(json.dumps(report))
            with standins(store_fixture):
                return command([step, "--report", str(report_path), "--research", str(research_file)])

        def refusal_code(error: str) -> str:
            return str(json.loads(error).get("code", "")) if error.strip() else ""

        report = replayed(record)
        require(auto_promotion.canary_checks(report, record["promotion"]["challenger_predictions"], number)["decision"] == "promote", "the replay itself fails")
        code, out, error = recorded(other_fixtures["m1"], research_path, report)
        require(
            code == 2 and refusal_code(error).startswith("candidate_") and not canary_tags(other),
            f"another store's version {number} was certified from this research record: exit {code}, {out.strip()} {error.strip()}, tags {canary_tags(other)}",
        )
        for step in ("verdict", "record"):
            code, out, error = recorded(fixtures["m2"], research_path, replayed(record, hai_type="HAI_2"), step)
            require(
                code == 2 and refusal_code(error) == "replay_type_differs" and not canary_tags(source),
                f"{step} accepted a replay of another infection type: exit {code}, {out.strip()} {error.strip()}",
            )
        unbound = folder / "identity_unbound.json"
        unbound.write_text(
            json.dumps({**record, "promotion": {**record["promotion"], "challenger": {**record["promotion"]["challenger"], "source_run": None}}})
        )
        code, out, error = recorded(fixtures["m2"], unbound, report)
        require(
            code == 2 and refusal_code(error) == "research_source_run_missing" and not canary_tags(source),
            f"a record that names no source run certified a version: exit {code}, {out.strip()} {error.strip()}",
        )
        renamed = source.find_version(research_cases.NAME, number)
        renamed["tags"]["model"], kept = "m1", renamed["tags"]["model"]
        code, out, error = recorded(fixtures["m2"], research_path, report)
        renamed["tags"]["model"] = kept
        require(
            code == 2 and refusal_code(error) == "candidate_model_differs" and not canary_tags(source),
            f"a version registered for another model was certified: exit {code}, {out.strip()} {error.strip()}",
        )
        code, out, error = recorded(fixtures["m2"], research_path, report)
        require(code == 0 and json.loads(out)["decision"] == "promote", f"the matching candidate was refused: exit {code}, {out.strip()} {error.strip()}")
        source_run = record["promotion"]["challenger"]["source_run"]
        require(
            canary_tags(source) == {number: {"canary_verdict": "promote", "canary_version": number, "canary_run": source_run}},
            f"the result is not bound to the research record's source run: {canary_tags(source)}",
        )

    cases.check("auto_promotion_plans_the_canary_and_the_promotion_of_a_challenger", plans_canary_and_promotion)
    cases.check("auto_promotion_plans_nothing_without_a_challenger", plans_nothing_without_a_challenger)
    cases.check("auto_promotion_refuses_an_ineligible_challenger", refuses_ineligible_challengers)
    cases.check("auto_promotion_refuses_a_canary_share_outside_the_serving_contract", refuses_a_canary_share_outside_the_serving_contract)
    cases.check("auto_promotion_canary_checks_pass_a_clean_canary", canary_passes)
    cases.check("auto_promotion_canary_checks_hold_a_failed_or_thin_canary", canary_holds)
    cases.check("auto_promotion_canary_result_is_bound_to_the_version_and_its_run", record_binds_the_result_to_the_candidate)

    def canary_holds_a_request_that_needed_a_retry() -> None:
        """The reviewer's scenario: a canary request answers 429 after 4 s, waits 1 s and then answers 200 in 0.01 s. The lead's reading of the pass rule
        (an assumption for the owner): a request passes only when every attempt answered 200 and its total time, retries included, is within the limit."""
        record = {
            **RESEARCH,
            "promotion": {**RESEARCH["promotion"], "challenger_predictions": {f"{index:06d}:2023": 0.5 + index / 100 for index in range(40)}},
        }
        record["promotion"]["challenger"] = {**record["promotion"]["challenger"], "version": VERSION}
        first = sorted(record["promotion"]["challenger_predictions"])[0]
        clean = replayed(record)
        require(judged(clean, record["promotion"]["challenger_predictions"])["decision"] == "promote", f"the clean replay was held: {judged(clean)}")
        retried_slowly = replayed(record, limited={first: 4.0})
        attempts = [row.get("attempts") for row in retried_slowly["rows"] if f"{row['ccn']}:{row['window_year']}" == first]
        statuses = [[attempt.get("status") for attempt in item] for item in attempts if isinstance(item, list)]
        elapsed = [row.get("elapsed_seconds") for row in retried_slowly["rows"] if f"{row['ccn']}:{row['window_year']}" == first]
        require(
            statuses == [[429, 200]] and len(elapsed) == 1 and isinstance(elapsed[0], float) and abs(elapsed[0] - 5.01) < 1e-9,
            f"the report does not keep both attempts and the total time of the retried request: attempts {attempts}, elapsed {elapsed}",
        )
        require(
            judged(retried_slowly, record["promotion"]["challenger_predictions"]) == {"decision": "hold", "failed": ["requests_answered", "within_time_limit"]},
            f"a request that answered 429 and took 5.01 s in all passed the canary: {judged(retried_slowly, record['promotion']['challenger_predictions'])}",
        )
        retried_quickly = replayed(record, limited={first: 0.01})
        require(
            judged(retried_quickly, record["promotion"]["challenger_predictions"]) == {"decision": "hold", "failed": ["requests_answered"]},
            f"a request whose first attempt answered 429 passed the canary: {judged(retried_quickly, record['promotion']['challenger_predictions'])}",
        )
        research_file, report_file = folder / "retry_research.json", folder / "retry_report.json"
        research_file.write_text(json.dumps(record))
        report_file.write_text(json.dumps(retried_slowly))
        code, out, error = command(["verdict", "--report", str(report_file), "--research", str(research_file)])
        require(
            code == 0 and json.loads(out)["decision"] == "hold", f"the verdict command passed the retried canary: exit {code}, {out.strip()} {error.strip()}"
        )

    cases.check("auto_promotion_record_refuses_a_candidate_of_another_identity", record_refuses_a_candidate_of_another_identity)
    cases.check("auto_promotion_canary_holds_a_request_that_needed_a_retry", canary_holds_a_request_that_needed_a_retry)
    cases.check("auto_promotion_compares_the_feature_schema_with_the_online_one", schema_comparison)
    cases.check("auto_promotion_release_calls_the_feast_release_command", release_calls_the_feast_command)
    cases.check("auto_promotion_release_failure_is_visible", release_failure_is_visible)
    cases.check("auto_promotion_notice_reaches_the_alerts_topic", notice_reaches_the_topic)
    cases.check("auto_promotion_command_line_prints_one_json_line_or_refuses", command_line)
