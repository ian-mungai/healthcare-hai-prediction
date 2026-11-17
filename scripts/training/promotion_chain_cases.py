"""Synthetic cases of the canary check and the owner notice, the two phases of the promote family that follow the research phase, through ``aws_run.main``.

Each case runs the phase over the bucket as the training task role sees it (``research/`` only: create-only writes, reads and lists), the tracking store
stand-in of the research cases and a notification client stand-in. Nothing reaches a cloud.
"""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest import mock

from orchestration import training_contract
from scripts.training import aws_run, aws_run_cases, promote_receipt, research_cases
from scripts.training.auto_promotion_cases import replayed
from scripts.training.job_action_cases import HAI_TYPE, require, standins

TOPIC = f"arn:aws:sns:us-west-2:{'1' * 12}:example_project_alerts_dev"  # a synthetic all-ones account, built so no real-looking ID sits in the file
CHAIN_RUN = "promote_clabsi_1234abcd"
RESEARCH_RUN = research_cases.RESEARCH_RUN


def key_of(kind: str, hai_type: str = HAI_TYPE, run: str = RESEARCH_RUN) -> str:
    """The key the phases and the DAG read: ``research/<run>/<kind>_<type>.json``."""
    return training_contract.record_key(kind, run, hai_type)


def staged(folder: Path) -> SimpleNamespace:
    """Run the research action over three finished models, store its record as the research phase does and return the stand-ins."""
    tracking, fixtures = research_cases.finished_models(folder / "models")
    code, _, error, output = research_cases.research(folder / "research", fixtures["m2"])
    require(code == 0, f"the research action failed: {error}")
    body = (output / f"research_{HAI_TYPE}.json").read_bytes()
    bucket = aws_run_cases.ListingBucket(aws_run_cases.BUCKET, "research/")
    bucket.objects[key_of("research")] = body
    return SimpleNamespace(folder=folder, fixture=fixtures["m2"], bucket=bucket, record=json.loads(body), sent=[], publish=None)


def canary_parameters(**changes: Any) -> dict[str, Any]:
    return {"research_run": RESEARCH_RUN, "hai_type": HAI_TYPE, "mlflow": "aws", **changes}


def notice_parameters(outcome: str, **changes: Any) -> dict[str, Any]:
    return {**canary_parameters(**changes), "outcome": outcome}


def phase(world: SimpleNamespace, name: str, parameters: dict[str, Any], **environment: str | None) -> tuple[int, str, str]:
    """Run one phase in a fresh workspace, as a new task of the run does: the code fetched, the container variables, the bucket and the client stand-ins."""
    work = world.folder / f"work_{name}_{len(list(world.folder.glob(f'work_{name}_*')))}"
    work.mkdir(parents=True)
    _, sha = aws_run_cases.code_archive(work)
    publish = world.publish or (lambda **arguments: world.sent.append(arguments) or {"MessageId": f"message-{len(world.sent)}"})
    sns = SimpleNamespace(publish=publish)
    values: dict[str, str | None] = {"CODE_SHA256": sha, "TRAINING_PARAMETERS": json.dumps(parameters), "ALERTS_TOPIC_ARN": TOPIC, **environment}
    with (
        standins(world.fixture),
        mock.patch.object(promote_receipt, "s3_client", return_value=world.bucket),
        mock.patch.object(aws_run, "sns_client", return_value=sns),
        aws_run_cases.environment(**values),
    ):
        return aws_run_cases.call(name, "--run", CHAIN_RUN, "--mode", "real", "--work", str(work))


def stored(world: SimpleNamespace, kind: str) -> dict[str, Any] | None:
    body = world.bucket.objects.get(key_of(kind))
    return None if body is None else dict(json.loads(body))


def canary_tags(world: SimpleNamespace) -> dict[str, dict[str, str]]:
    """The canary tags each registered version carries."""
    versions = world.fixture.tracking.search_model_versions(f"name='{research_cases.NAME}'")
    return {
        str(item.version): {k: v for k, v in item.tags.items() if k.startswith("canary_")}
        for item in versions
        if any(k.startswith("canary_") for k in item.tags)
    }


def promotion_chain_cases(cases: Any, folder: Path) -> None:
    """The canary check decides the full rollout from a stored replay, and the notice tells the owner once."""
    folder.mkdir(parents=True)
    number = ""

    def clean(name: str) -> SimpleNamespace:
        nonlocal number
        world = staged(folder / name)
        number = str(world.record["promotion"]["challenger"]["version"])
        world.bucket.objects[key_of("replay")] = json.dumps(replayed(world.record)).encode()
        return world

    def passes_a_clean_replay() -> None:
        world = clean("pass")
        code, _, error = phase(world, "canary", canary_parameters())
        require(code == 0, f"the canary check failed: {error}")
        result = stored(world, "canary")
        if result is None:
            raise AssertionError("the canary check stored no result")
        require(result["decision"] == "promote" and result["failed"] == [], f"the stored result: {result}")
        require(
            result["version"] == number and result["model"] == world.record["promotion"]["challenger"]["model"], f"the result names another candidate: {result}"
        )
        source = world.record["promotion"]["challenger"]["source_run"]
        require(canary_tags(world) == {number: {"canary_verdict": "promote", "canary_version": number, "canary_run": source}}, f"tags: {canary_tags(world)}")

    def holds_a_failed_replay() -> None:
        world = clean("hold")
        report = json.loads(world.bucket.objects[key_of("replay")])
        world.bucket.objects[key_of("replay")] = json.dumps({**report, "statuses": {"200": 99, "500": 1}}).encode()
        code, _, error = phase(world, "canary", canary_parameters())
        result = stored(world, "canary")
        if result is None:
            raise AssertionError(f"a failed canary was not held, no result was stored: {code} {error}")
        require(code == 0 and result["decision"] == "hold", f"a failed canary was not held: {code} {error} {result}")
        require(
            result["failed"] == ["requests_answered"] and not canary_tags(world), f"a held canary left tags or lost its reason: {result} {canary_tags(world)}"
        )

    def holds_when_no_replay_was_stored() -> None:
        world = staged(folder / "no_replay")
        code, _, error = phase(world, "canary", canary_parameters())
        result = stored(world, "canary")
        if result is None:
            raise AssertionError(f"a missing replay failed the phase instead of holding: {code} {error}")
        require(code == 0, f"a missing replay failed the phase instead of holding: {code} {error}")
        require(result["decision"] == "hold" and result["failed"] == ["replay_report_missing"] and not canary_tags(world), f"result: {result}")

    def rerun_keeps_the_first_result() -> None:
        world = clean("rerun")
        require(phase(world, "canary", canary_parameters())[0] == 0, "the first canary check failed")
        first = world.bucket.objects[key_of("canary")]
        again = phase(world, "canary", canary_parameters())
        require(again[0] == 0 and world.bucket.objects[key_of("canary")] == first, f"a rerun changed the result: {again}")
        world.bucket.objects[key_of("canary")] = b"another result\n"
        conflict = phase(world, "canary", canary_parameters())
        require(conflict[0] != 0 and "record_conflict" in conflict[2], f"another stored result was accepted: {conflict}")

    def refuses(name: str, parameters: dict[str, Any] | None = None, remove: str | None = None, **environment: str | None) -> None:
        def case() -> None:
            world = clean(f"refuse_{name}")
            if remove is not None:
                del world.bucket.objects[key_of(remove)]
            code, _, error = phase(world, "canary", parameters or canary_parameters(), **environment)
            require(code != 0 and "refused" in error, f"the canary check accepted {name}: {code} {error}")
            require(stored(world, "canary") is None and not canary_tags(world), f"a refused canary check stored a result or tags: {name}")

        cases.check(f"canary_check_refuses_{name}", case)

    def refuses_a_type_without_a_challenger() -> None:
        world = clean("none")
        record = {**world.record, "promotion": {**world.record["promotion"], "challenger": None}}
        world.bucket.objects[key_of("research")] = json.dumps(record).encode()
        code, _, error = phase(world, "canary", canary_parameters())
        require(code != 0 and "challenger" in error and stored(world, "canary") is None, f"a type without a challenger was checked: {code} {error}")

    def refuses_a_record_of_another_type() -> None:
        world = clean("other_type")
        world.bucket.objects[key_of("research")] = json.dumps({**world.record, "hai_type": "HAI_2"}).encode()
        code, _, error = phase(world, "canary", canary_parameters())
        require(code != 0 and "HAI_2" in error and stored(world, "canary") is None, f"a record of another type was checked: {code} {error}")

    def notice_tells_the_owner_once() -> None:
        world = clean("notice")
        require(phase(world, "canary", canary_parameters())[0] == 0, "the canary check failed")
        code, _, error = phase(world, "notify", notice_parameters("promoted"))
        require(code == 0 and len(world.sent) == 1, f"the notice was not sent once: {code} {error} {world.sent}")
        sent = world.sent[0]
        require(sent["TopicArn"] == TOPIC and sent["Subject"] == f"model promoted: {HAI_TYPE}", f"notice: {sent}")
        challenger = world.record["promotion"]["challenger"]
        require(challenger["model"] in sent["Message"] and f"version {number}" in sent["Message"], f"the notice names no candidate: {sent['Message']}")
        receipt = stored(world, "notice")
        require(receipt is not None and receipt["outcome"] == "promoted" and receipt["message_id"] == "message-1", f"receipt: {receipt}")
        again = phase(world, "notify", notice_parameters("promoted"))
        require(again[0] == 0 and len(world.sent) == 1, f"a rerun sent the notice again: {again} {world.sent}")

    def held_notice_names_the_failed_checks() -> None:
        world = clean("held_notice")
        report = json.loads(world.bucket.objects[key_of("replay")])
        world.bucket.objects[key_of("replay")] = json.dumps({**report, "statuses": {"200": 99, "500": 1}}).encode()
        require(phase(world, "canary", canary_parameters())[0] == 0, "the canary check failed")
        code, _, error = phase(world, "notify", notice_parameters("held"))
        require(code == 0 and "requests_answered" in world.sent[0]["Message"], f"the held notice names no failed check: {code} {error} {world.sent}")

    def notice_without_a_stored_canary_still_goes() -> None:
        world = clean("early_notice")
        code, _, error = phase(world, "notify", notice_parameters("held"))
        require(
            code == 0 and len(world.sent) == 1 and "no canary result" in world.sent[0]["Message"],
            f"a notice with no canary result: {code} {error} {world.sent}",
        )

    def notice_refusals() -> None:
        for name, parameters, environment in (
            ("outcome", notice_parameters("approved"), {}),
            ("topic_missing", notice_parameters("held"), {"ALERTS_TOPIC_ARN": None}),
            ("topic_form", notice_parameters("held"), {"ALERTS_TOPIC_ARN": "not an arn"}),
            ("type", notice_parameters("held", hai_type="HAI_9"), {}),
        ):
            world = clean(f"notice_{name}")
            code, _, error = phase(world, "notify", parameters, **environment)
            require(code != 0 and "refused" in error and not world.sent, f"a notice with a bad {name} was sent: {code} {error} {world.sent}")

    def failed_publish_leaves_no_receipt() -> None:
        world = clean("publish_failure")

        def refusing(**_: Any) -> dict[str, str]:
            raise RuntimeError("the topic refused")

        world.publish = refusing
        code, _, error = phase(world, "notify", notice_parameters("promoted"))
        require(code != 0 and "publish_failed" in error and stored(world, "notice") is None, f"a failed publish left a receipt or no failure: {code} {error}")
        world.publish = None
        require(phase(world, "notify", notice_parameters("promoted"))[0] == 0 and len(world.sent) == 1, "the retry of a failed notice did not send it")

    cases.check("canary_check_passes_a_clean_replay_and_tags_the_version", passes_a_clean_replay)
    cases.check("canary_check_holds_a_failed_replay_and_tags_nothing", holds_a_failed_replay)
    cases.check("canary_check_holds_when_no_replay_was_stored", holds_when_no_replay_was_stored)
    cases.check("canary_check_rerun_keeps_the_first_result_and_refuses_another", rerun_keeps_the_first_result)
    cases.check("canary_check_refuses_a_type_without_a_challenger", refuses_a_type_without_a_challenger)
    cases.check("canary_check_refuses_a_research_record_of_another_type", refuses_a_record_of_another_type)
    refuses("a_missing_research_record", remove="research")
    refuses("parameters_of_another_step", {**canary_parameters(), "outcome": "held"})
    refuses("a_local_mlflow_lane", canary_parameters(mlflow="local"))
    refuses("a_missing_tracking_login", MLFLOW_TRACKING_PASSWORD=None)
    cases.check("owner_notice_goes_to_the_alerts_topic_once_and_stores_its_receipt", notice_tells_the_owner_once)
    cases.check("owner_notice_of_a_hold_names_the_failed_checks", held_notice_names_the_failed_checks)
    cases.check("owner_notice_goes_even_when_no_canary_result_was_stored", notice_without_a_stored_canary_still_goes)
    cases.check("owner_notice_refuses_a_bad_outcome_topic_or_type", notice_refusals)
    cases.check("owner_notice_that_failed_to_publish_leaves_no_receipt", failed_publish_leaves_no_receipt)
