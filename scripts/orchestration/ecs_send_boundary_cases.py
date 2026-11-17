"""Cases of the guard at the SDK send boundary of the ECS adapter: the hold read again after signing and any credential refresh.

Real botocore clients with scripted HTTP answers (``SdkStub``), and for the review's race the SDK's real container metadata fetcher against a loopback server
that trickles the credentials. They are in their own module because the in-image DAG run imports ``ecs_adapter_cases`` and mounts no loopback server.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any

from botocore.credentials import ContainerMetadataFetcher, RefreshableCredentials
from botocore.exceptions import ClientError

from orchestration import ecs_adapter
from orchestration.ecs_adapter import EcsAdapter, TaskRefused, TaskUnavailable
from scripts.loopback_trickle import trickling_server
from scripts.orchestration.ecs_adapter_cases import ENVIRONMENT, KEY, SETTINGS, raises, require
from scripts.orchestration.ecs_fake import ACCOUNT, SdkStub, client_error


def sender_with_credential_refresh(refresh: Callable[[], dict[str, str]]) -> tuple[EcsAdapter, SdkStub]:
    """The adapter as production builds it over a scripted SDK, whose RunTask client has expired credentials that ``refresh`` renews while signing."""
    stub = SdkStub({"ListTasks": [{"taskArns": []}], "RunTask": [{"tasks": [{"taskArn": f"arn:aws:ecs:r:{ACCOUNT}:task/c/stand-in"}], "failures": []}]})
    adapter = EcsAdapter.from_session(stub, replace(SETTINGS, host_capacity_provider="stand-in_host"))
    expired = {"access_key": "stand-in", "secret_key": "stand-in", "token": "stand-in", "expiry_time": (datetime.now(UTC) - timedelta(seconds=1)).isoformat()}
    credentials = RefreshableCredentials.create_from_metadata(expired, refresh, "case")
    sender: Any = adapter.sender
    sender._request_signer._credentials = credentials
    return adapter, stub


def renewed() -> dict[str, str]:
    return {"access_key": "stand-in", "secret_key": "stand-in", "token": "stand-in", "expiry_time": (datetime.now(UTC) + timedelta(hours=1)).isoformat()}


def guard_runs_again_at_the_send_boundary() -> None:
    """A guard that passed before the call is run again by the SDK at the send boundary, after signing and the credential refresh it can trigger, so a
    hold that is set during the refresh still withholds the call."""
    held: list[bool] = []
    calls: list[bool] = []

    def refresh() -> dict[str, str]:
        held.append(True)
        return renewed()

    def guard() -> None:
        calls.append(bool(held))
        if held:
            raise ecs_adapter.SendWithheld

    adapter, stub = sender_with_credential_refresh(refresh)
    raises(ecs_adapter.SendWithheld, lambda: adapter.start("train", KEY, ENVIRONMENT, guard=guard))
    require(calls == [False, True], f"the guard ran before the call and again after the refresh: {calls}")
    require(stub.sent.count("RunTask") == 0, f"a RunTask went out although the guard refused at the send boundary: {stub.sent}")
    again, again_stub = sender_with_credential_refresh(renewed)
    arn = again.start("train", KEY, ENVIRONMENT, guard=lambda: None)
    require(arn.endswith("stand-in") and again_stub.sent.count("RunTask") == 1, "a guard that passes at both points lets the call through")


def guard_failure_at_the_send_boundary_is_not_a_send_failure() -> None:
    """A guard that fails at the send boundary raises its own error: the adapter does not read it as an SDK failure of a call that may have been sent."""
    calls: list[int] = []

    def guard() -> None:
        calls.append(1)
        if len(calls) == 2:
            raise client_error("AccessDeniedException", "GetParameter")

    adapter, stub = sender_with_credential_refresh(renewed)
    try:
        adapter.start("train", KEY, ENVIRONMENT, guard=guard)
    except ecs_adapter.SendUnknown:
        raise AssertionError("a guard failure before the send read as an unknown reply") from None
    except (TaskRefused, TaskUnavailable) as error:
        raise AssertionError(f"a guard failure was classified as a RunTask failure: {error}") from None
    except ClientError as error:
        require(error.response["Error"]["Code"] == "AccessDeniedException", "the guard's own error is raised")
    else:
        raise AssertionError("the guard failure was swallowed")
    require(stub.sent.count("RunTask") == 0, "nothing was sent")


def refresh_by_the_real_metadata_fetcher_is_rechecked() -> None:
    """The review's race with the SDK's real container metadata fetcher: the credential response trickles in for over two seconds after the guard passed.
    The hold that is set when the refresh starts withholds the call at the send boundary, and no RunTask goes out."""
    credentials = json.dumps(
        {"AccessKeyId": "stand-in", "SecretAccessKey": "stand-in", "Token": "stand-in", "Expiration": (datetime.now(UTC) + timedelta(hours=1)).isoformat()}
    ).encode()
    with trickling_server(credentials, pad=6, interval=0.4) as server:
        fetcher = ContainerMetadataFetcher()

        def refresh() -> dict[str, str]:
            found = fetcher.retrieve_full_uri(f"{server.url}/credentials")
            return {"access_key": found["AccessKeyId"], "secret_key": found["SecretAccessKey"], "token": found["Token"], "expiry_time": found["Expiration"]}

        seen: list[bool] = []

        def guard() -> None:
            seen.append(server.started.is_set())
            if server.started.is_set():
                raise ecs_adapter.SendWithheld

        adapter, stub = sender_with_credential_refresh(refresh)
        began = time.monotonic()
        raises(ecs_adapter.SendWithheld, lambda: adapter.start("train", KEY, ENVIRONMENT, guard=guard))
        took = time.monotonic() - began
    require(took >= 2.0, f"the refresh was slow, as in the review: {took:.2f} s")
    require(seen == [False, True], f"the hold was read again after the slow refresh: {seen}")
    require(stub.sent.count("RunTask") == 0, f"a RunTask went out while the hold was set: {stub.sent}")


CASES: dict[str, Callable[[], None]] = {
    "guard_runs_again_at_the_send_boundary": guard_runs_again_at_the_send_boundary,
    "guard_failure_at_the_send_boundary_is_not_a_send_failure": guard_failure_at_the_send_boundary_is_not_a_send_failure,
    "refresh_by_the_real_metadata_fetcher_is_rechecked": refresh_by_the_real_metadata_fetcher_is_rechecked,
}
