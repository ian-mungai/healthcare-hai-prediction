"""Deterministic admission and request cases with synthetic operational metadata."""

from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

NOW = 1_800_000_000.0


def selector_cases() -> dict[str, bool]:
    """Exercise placement rules without starting a launcher, process or network connection."""
    from orchestration import ecs_target as subject

    size = subject.Size(1024, 4096, 30)
    attempt: dict[str, Any] = {"id": "at-example", "deadline_utc": NOW + 300, "generation": 1}
    subject.initialize(attempt, "auto", NOW, None)
    ready = subject.Availability()

    def choose(
        view: subject.Availability = ready, request: str = "auto", job_class: str = "offline", at: float = NOW, *, prior_capacity: bool = False
    ) -> subject.Decision:
        candidate = copy.deepcopy(attempt)
        candidate["execution"]["requested_target"] = request
        if prior_capacity:
            previous = subject.reserve(candidate, size, subject.Decision("dispatch", "ecs", "spot_capacity_unavailable", "FARGATE"), NOW, subject.Usage())
            subject.fenced(candidate, previous["try_id"], NOW, no_start=True)
        return subject.select(candidate, job_class, size, view, at)

    result = {
        "healthy_aws_precedes_free_mac": choose().target == "ecs",
        "healthy_aws_needs_no_mac_daemon": choose(subject.Availability(local_available=False)).target == "ecs",
        "forced_ecs_disabled_is_visible": choose(subject.Availability(enabled=False), "ecs").reason == "ecs_disabled",
        "explicit_docker_needs_bottleneck": choose(request=subject.TARGET_DOCKER).reason == "fallback_not_allowed",
        "writers_refused_in_read_increment": choose(job_class="lakehouse_write").reason == "writer_not_enabled",
        "deadline_never_restarts": choose(at=NOW + 300).action == "expire",
    }
    for reason in ("application", "integrity", "credentials", "configuration", "missing_data"):
        decision = choose(subject.Availability(reason=reason, observed_at=NOW))
        result[f"{reason}_cannot_fallback"] = decision.action == "refuse" and decision.target is None
    for reason in ("cloud_cap", "capacity", "breaker", "patience"):
        view = subject.Availability(reason=reason, observed_at=NOW + 121, no_start=True, on_demand_tried=True, portable=True)
        result[f"{reason}_permits_verified_fallback"] = choose(view, at=NOW + 121, prior_capacity=True).target == "docker"
        for field in ("local_available", "local_free", "local_fits", "portable"):
            changed = {**view.__dict__, field: False}
            result[f"{reason}_waits_without_{field}"] = choose(subject.Availability(**changed), at=NOW + 121, prior_capacity=True).action == "wait"
        stale = subject.Availability(**{**view.__dict__, "observed_at": NOW - 61})
        result[f"{reason}_needs_fresh_observation"] = choose(stale, at=NOW, prior_capacity=True).action == "wait"
    early = subject.Availability(reason="patience", observed_at=NOW, no_start=True, portable=True)
    result["patience_starts_at_first_admission"] = choose(early, at=NOW + 119).action == "wait"
    capacity = subject.Availability(reason="capacity", observed_at=NOW, no_start=True, portable=True)
    result["capacity_tries_on_demand_before_mac"] = choose(capacity).provider == "FARGATE"
    for state in ("intent", "pending", "running", "unknown", "stopping"):
        live = copy.deepcopy(attempt)
        subject.reserve(live, size, subject.Decision("dispatch", "ecs", "aws_first", "FARGATE_SPOT"), NOW, subject.Usage())
        live["execution"]["placements"][-1]["state"] = state
        decision = subject.select(live, "offline", size, capacity, NOW + 121)
        result[f"live_{state}_blocks_second_target"] = decision.action in ("reconcile", "fence") and decision.target is None
        totals = subject.active_usage([live])
        result[f"live_{state}_counts_against_cap"] = totals == subject.Usage(1, 1024, 4096)
    result["task_count_cap"] = choose(subject.Availability(usage=subject.Usage(3, 3072, 12288))).reason == "cloud_cap"
    result["cpu_cap"] = choose(subject.Availability(usage=subject.Usage(1, 16384, 8192))).reason == "cloud_cap"
    result["memory_cap"] = choose(subject.Availability(usage=subject.Usage(1, 1024, 122880))).reason == "cloud_cap"
    result["exact_cap_fits"] = choose(subject.Availability(usage=subject.Usage(2, 15360, 118784))).target == "ecs"
    history = copy.deepcopy(attempt)
    aws = subject.Decision("dispatch", "ecs", "aws_first", "FARGATE_SPOT")
    entry = subject.reserve(history, size, aws, NOW, subject.Usage())
    result["intent_has_fixed_deadline"] = entry["deadline_utc"] == attempt["deadline_utc"]
    result["intent_precedes_effect"] = entry["state"] == "intent" and history["execution"]["live_try"] == entry["try_id"]
    stable_history = copy.deepcopy(history)
    try:
        subject.reserve(history, size, aws, NOW, subject.Usage())
    except subject.PlacementError as error:
        result["duplicate_live_reservation_refused"] = str(error) == "a live placement already exists"
    else:
        result["duplicate_live_reservation_refused"] = False
    history = stable_history
    subject.fenced(history, entry["try_id"], NOW + 1, no_start=True)
    result["confirmed_stop_releases_cap"] = subject.active_usage([history]) == subject.Usage()
    result["placement_history_survives_fence"] = len(history["execution"]["placements"]) == 1
    result["aws_recovery_precedes_occupied_mac"] = choose(subject.Availability(local_free=False)).target == "ecs"
    subject.initialize(history, "auto", NOW + 122, None)
    result["repeat_initialization_keeps_queue_time"] = history["execution"]["queued_at"] == NOW
    pending = copy.deepcopy(attempt)
    pending_try = subject.reserve(pending, size, aws, NOW, subject.Usage())
    pending_try["state"] = "pending"
    for elapsed, action in ((5, "reconcile"), (119, "reconcile"), (120, "fence")):
        view = subject.Availability(reason="patience", observed_at=NOW + elapsed, no_start=True, portable=True)
        result[f"pending_patience_at_{elapsed}_seconds"] = subject.select(pending, "offline", size, view, NOW + elapsed).action == action
    pending_try["state"], pending_try["started"] = "running", True
    result["started_task_ignores_provisioning_patience"] = subject.select(pending, "offline", size, view, NOW + 121).action == "reconcile"
    result["started_task_fences_at_fixed_deadline"] = subject.select(pending, "offline", size, view, NOW + 300).action == "fence"
    ondemand = copy.deepcopy(attempt)
    prior = subject.reserve(ondemand, size, subject.Decision("dispatch", "ecs", "spot_capacity_unavailable", "FARGATE"), NOW, subject.Usage())
    subject.fenced(ondemand, prior["try_id"], NOW + 1, no_start=True)
    result["saved_on_demand_fence_overrides_missing_flags"] = subject.select(ondemand, "offline", size, capacity, NOW + 2).target == "docker"
    for started in (False, True):
        prior_run = copy.deepcopy(attempt)
        previous = subject.reserve(prior_run, size, aws, NOW, subject.Usage())
        previous["started"] = started
        subject.fenced(prior_run, previous["try_id"], NOW + 1, no_start=False)
        for reason in ("ready", "capacity", "cloud_cap", "breaker", "patience"):
            inconsistent = subject.Availability(reason=reason, observed_at=NOW + 121, no_start=True, on_demand_tried=True, portable=True)
            decision = subject.select(prior_run, "offline", size, inconsistent, NOW + 121)
            result[f"saved_{'started' if started else 'uncertain'}_try_blocks_{reason}_replay"] = (
                decision.action == "refuse" and decision.reason == "prior_try_not_replayable"
            )
    spot_only = copy.deepcopy(attempt)
    previous = subject.reserve(spot_only, size, aws, NOW, subject.Usage())
    subject.fenced(spot_only, previous["try_id"], NOW + 1, no_start=True)
    inflated = subject.Availability(reason="capacity", observed_at=NOW + 2, no_start=True, on_demand_tried=True, portable=True)
    result["saved_spot_fence_still_requires_on_demand"] = subject.select(spot_only, "offline", size, inflated, NOW + 2).provider == "FARGATE"
    omitted_usage = copy.deepcopy(attempt)
    try:
        subject.reserve(omitted_usage, size, aws, NOW)
    except subject.PlacementError as error:
        result["cloud_reservation_requires_usage"] = str(error) == "cloud placement requires current usage"
    else:
        result["cloud_reservation_requires_usage"] = False
    return result


def sizing_cases() -> dict[str, bool]:
    """Check independent memory and disk examples against the admitted Fargate sizes."""
    from orchestration import ecs_target as subject

    size = subject.ecs_plan(8, 2, inputs_gib=3, outputs_total_gib=4, scratch_gib=5, temp_cap_gib=6, image_compressed_gib=1, image_uncompressed_gib=2)
    checks = {"all_disk_terms_counted": size.storage_gib == 28, "memory_includes_headroom": size.cpu == 2048 and size.memory_mib == 10240}
    for label, values in (
        ("memory_over_cap", {"memory_need_gib": 119}),
        ("disk_over_cap", {"inputs_gib": 201}),
        ("negative_size", {"scratch_gib": -1}),
        ("nonfinite_size", {"scratch_gib": float("nan")}),
    ):
        args = {
            "memory_need_gib": 4.0,
            "vcpu_min": 1.0,
            "inputs_gib": 0.0,
            "outputs_total_gib": 0.0,
            "scratch_gib": 0.0,
            "temp_cap_gib": 1.0,
            "image_compressed_gib": 1.0,
            "image_uncompressed_gib": 2.0,
            **values,
        }
        try:
            subject.ecs_plan(**args)
        except subject.PlacementError:
            checks[label] = True
        else:
            checks[label] = False
    return checks


def contract_cases(root: Path) -> dict[str, bool]:
    """Read only the declared synthetic schemas and placement configuration."""
    import json

    from jsonschema import Draft202012Validator

    from orchestration import ecs_target as subject
    from orchestration.fake import FakeSubmissions
    from orchestration.interface import SubmissionRefused, SubmissionRequest

    jobs = subject.load_jobs(root)
    request = SubmissionRequest("gold_validate", "20261023T000000Z-01234567", "fixture", 1, "gold_dag", "validate", "manual", target="ecs")
    schema = json.loads((root / "data_contracts/orchestration/submission_request.schema.json").read_text())
    restored = SubmissionRequest.from_record(request.as_dict())
    checks = {
        "request_schema_accepts_target": Draft202012Validator(schema).is_valid(request.as_dict()),
        "target_roundtrip": restored == request,
        "target_preserves_submission_key": request.submission_id == SubmissionRequest(**{**request.__dict__, "target": None}).submission_id,
        "rollout_disabled_without_runtime": jobs["enabled"] is False,
        "writer_has_no_enabled_row": jobs["enabled"] is False
        and all(
            row["class"] != "lakehouse_write" or row.get("mode") == "aws_qualify" and row.get("layer") in ("silver", "gold", "features") for row in jobs["jobs"]
        ),
    }
    candidate: dict[str, Any] = {"id": "at-example", "deadline_utc": NOW + 300, "generation": 1}
    subject.initialize(candidate, "ecs", NOW, 7)
    subject.reserve(candidate, subject.Size(1024, 4096, 21), subject.Decision("dispatch", "ecs", "aws_first", "FARGATE_SPOT"), NOW, subject.Usage())
    execution_schema = json.loads((root / "data_contracts/orchestration/execution.schema.json").read_text())
    ledger_schema = json.loads((root / "data_contracts/orchestration/ledger_line.schema.json").read_text())
    checks["ledger_execution_copy_matches_source"] = ledger_schema["properties"]["execution"] == execution_schema
    checks["execution_schema_accepts_intent"] = Draft202012Validator(execution_schema).is_valid(candidate["execution"])
    fake = FakeSubmissions(json.loads((root / "data_contracts/orchestration/examples/job_templates.json").read_text()))
    fake_request = SubmissionRequest(**{**request.__dict__, "mode": "fixture", "target": None})
    handle = fake.submit(fake_request)
    count = len(fake.ledger)
    try:
        replay = fake.submit(SubmissionRequest(**{**fake_request.__dict__, "target": "auto"}))
    except SubmissionRefused:
        checks["fake_omitted_and_auto_replay_match"] = False
    else:
        checks["fake_omitted_and_auto_replay_match"] = replay == handle and len(fake.ledger) == count
    for bad in ("auto_local", "ECS", ""):
        checks[f"request_rejects_{bad or 'empty'}_target"] = not Draft202012Validator(schema).is_valid({**request.as_dict(), "target": bad})
    try:
        SubmissionRequest.from_record({**request.as_dict(), "target": None})
    except SubmissionRefused:
        checks["explicit_null_target_is_not_omission"] = True
    else:
        checks["explicit_null_target_is_not_omission"] = False
    return checks
