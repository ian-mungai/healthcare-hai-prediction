"""Offline cases for the Parameter Store module and the Terraform names, against a loopback stand-in; no AWS service is contacted.

Run from the repository root: ``.venv/bin/python -m scripts.infrastructure.secret_store_cases [--root <folder>]``. The cases are
one process, take seconds and start no container. ``scripts/infrastructure/run_secret_store_e2e.py`` runs them and keeps the report.
"""

from __future__ import annotations

import argparse
import base64
import contextlib
import functools
import importlib.util
import io
import json
import logging
import os
import re
import socket
import stat
import sys
import tempfile
import threading
import time
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any
from unittest import mock

from botocore.exceptions import ClientError

from scripts.infrastructure import render_project_config, secret_store
from scripts.infrastructure.mlflow_auth_stand_in import MlflowAuthStandIn
from scripts.infrastructure.secret_store_stand_in import StandIn

REPO_ROOT = Path(__file__).resolve().parents[2]
SECRET_RESOURCE = re.compile(r'^\s*(resource|data|ephemeral)\s+"(aws_secretsmanager_[a-z_]*|aws_ssm_parameter[a-z_]*)"', re.MULTILINE)
NAME = secret_store.POSTGRES_PASSWORD
STALL_SECONDS = 30.0
# A check that loses the socket of a closing response ends about FIRST_BYTE_SECONDS after its deadline (the next read times out that long after the first byte).
# A check that holds its deadline ends 0.0003 to 0.0021 s after it (40 runs under load). The allowance lies between the two.
FIRST_BYTE_SECONDS = 0.35
DEADLINE_ALLOWANCE = 0.2

Case = Callable[[Path], dict[str, bool]]


STREAM_CHUNK = 1024 * 1024
SCAN_SECONDS = 120.0
SKIPPED_FOLDERS = (".git", ".venv", ".tools", "node_modules", ".terraform")


def read_status(path: Path, needles: list[bytes], deadline: float) -> str:
    """Read one regular file in chunks and say what was found: ``found``, ``clean``, ``skipped`` (no longer a regular file), ``changed`` or ``unfinished``.

    The open never waits (a named pipe with no writer would block an ordinary open for ever) and never follows a link, and it checks the kind and the size again
    on the descriptor. A file that grows past the size it had at the open, or a read that passes the deadline, ends as incomplete, so no read is unbounded.
    """
    overlap = max(len(needle) for needle in needles) - 1
    with os.fdopen(os.open(path, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW), "rb") as handle:
        info = os.fstat(handle.fileno())
        if not stat.S_ISREG(info.st_mode):
            return "skipped"
        tail, total = b"", 0
        while True:
            if time.monotonic() > deadline:
                return "unfinished"
            chunk = handle.read(STREAM_CHUNK)
            if not chunk:
                return "clean"
            total += len(chunk)
            window = tail + chunk
            if any(needle in window for needle in needles):
                return "found"
            if total > info.st_size:
                return "changed"
            tail = window[-overlap:] if overlap else b""


def link_not_covered(path: Path, roots: list[Path]) -> bool:
    """Whether a link points at an existing target that no scan root holds, so the scan reads it through no root."""
    target = path.resolve()
    return target.exists() and not any(target.is_relative_to(root) for root in roots)


def files_holding(values: Iterable[str], roots: Iterable[Path], since: float, seconds: float = SCAN_SECONDS, report_links: bool = True) -> list[str]:
    """Every regular file under the roots written since ``since`` whose bytes hold one of the values, read in chunks whatever its size.

    The result also names what the scan could not cover, each with its reason, so that an empty result means a complete scan: a link made since ``since``
    to a target outside the roots (unless ``report_links`` is false, for a folder that other runs share), a file that changed while it was read and a scan
    that passed its
    ``seconds`` deadline. A pipe, a device or a link is never opened.
    """
    needles = [value.encode() for value in values if len(value) > 8]
    if not needles:
        return []
    found: list[str] = []
    scanned = [Path(root) for root in roots]
    resolved = [root.resolve() for root in scanned]
    deadline = time.monotonic() + seconds
    for root in scanned:
        for folder, names, files in os.walk(root):
            names[:] = [name for name in names if name not in SKIPPED_FOLDERS]
            for name in [*names, *files]:
                path = Path(folder) / name
                try:
                    info = path.lstat()
                    if info.st_mtime < since:
                        continue
                    if stat.S_ISLNK(info.st_mode):
                        if report_links and link_not_covered(path, resolved):
                            found.append(f"{path}: not covered: a link to a target outside the scan roots")
                        continue
                    if not stat.S_ISREG(info.st_mode):
                        continue
                    status = read_status(path, needles, deadline)
                except OSError:
                    continue
                if status == "found":
                    found.append(str(path))
                elif status in ("changed", "unfinished"):
                    found.append(f"{path}: {status if status == 'changed' else 'not finished'}: the read did not complete")
                if status == "unfinished":
                    return found
    return found


def refusal(call: Callable[[], Any]) -> str | None:
    """The text of the secret-store error a call raises, or ``None`` when it succeeds."""
    try:
        call()
    except secret_store.SecretStoreError as error:
        return str(error)
    return None


def store_behaviour(root: Path) -> dict[str, bool]:
    """First use creates, a rerun changes nothing, each write is read back, a missing permission names the secret and no value is in any message."""
    began = time.time() - 1
    sentinel = "sentinel-" + os.urandom(8).hex()
    shared_name = secret_store.SHARED[0]
    printed = io.StringIO()
    with StandIn(shared={shared_name: sentinel + "-shared"}) as stand_in, contextlib.redirect_stdout(printed), contextlib.redirect_stderr(printed):
        store = secret_store.open_store(root, stand_in.endpoint)
        value, created = store.ensure(NAME, lambda: sentinel)
        writes_first = stand_in.writes()
        again, created_again = store.ensure(NAME, lambda: "must-not-be-used")
        writes_rerun = stand_in.writes()
        plain_read = store.get(NAME)
        store.put(secret_store.AIRFLOW_JWT_SECRET, sentinel + "-one")
        store.put(secret_store.AIRFLOW_JWT_SECRET, sentinel + "-two")
        missing = refusal(lambda: store.get(secret_store.REDIS_PASSWORD))
        before_wrong = len(stand_in.log)
        wrong = refusal(lambda: store.put("other/name", sentinel))
        wrong_read = refusal(lambda: store.find("other/name"))
        dash_name = refusal(lambda: store.find("healthcare-hai-prediction/postgres-password"))
        no_slash = refusal(lambda: store.find(secret_store.PREFIX.strip("/") + "/postgres_password"))
        read_only_write = refusal(lambda: store.put(secret_store.AIRFLOW_ADMIN, sentinel))
        shared_write = refusal(lambda: store.put(shared_name, sentinel))
        empty_value = refusal(lambda: store.put(NAME, ""))
        oversize_value = refusal(lambda: store.put(NAME, "x" * (secret_store.MAX_VALUE_BYTES + 1)))
        requests_for_wrong = len(stand_in.log) - before_wrong
        stand_in.values[secret_store.AIRFLOW_ADMIN] = sentinel + "-admin"
        admin_read = store.get(secret_store.AIRFLOW_ADMIN)
        before_shared = len(stand_in.log)
        shared_value = store.get(shared_name)
        shared_requests = [action for action, _ in stand_in.log[before_shared:]]
        stand_in.denied.add("CreateParameter")
        denied_create = refusal(lambda: store.ensure(secret_store.POLARIS_ROOT_SECRET, lambda: sentinel + "-denied"))
        stand_in.denied.clear()
        stand_in.denied.add("GetParameter")
        denied_read = refusal(lambda: store.get(NAME))
        stand_in.denied.clear()
        real_put = store.parameters.put_parameter

        def racing(**arguments: Any) -> Any:
            stand_in.values[arguments["Name"]] = "winner-" + sentinel
            return real_put(**arguments)

        store.parameters.put_parameter = racing
        raced, raced_created = store.ensure(secret_store.POLARIS_READER_CLIENT_ID, lambda: "loser-" + sentinel)

        def altering(**arguments: Any) -> Any:
            return real_put(**{**arguments, "Value": arguments["Value"] + "-altered"})

        store.parameters.put_parameter = altering
        readback = refusal(lambda: store.put(secret_store.POLARIS_GOLD_CLIENT_ID, sentinel + "-readback"))
        store.parameters.put_parameter = real_put
        written_types = {(item["type"], item["tier"]) for item in stand_in.writes_seen}
        stored_names = set(stand_in.values)
        refused_endpoint = refusal(lambda: secret_store.open_store(root, "http://example.org:4566"))
        ignores_endpoint_settings = secret_store.CLIENT_OPTIONS.get("ignore_configured_endpoint_urls") is True and secret_store.client_config() is not None
        messages = [
            str(text)
            for text in (
                missing,
                wrong,
                wrong_read,
                denied_create,
                denied_read,
                refused_endpoint,
                read_only_write,
                shared_write,
                empty_value,
                oversize_value,
                readback,
            )
        ]
    return {
        "first use creates the secret with the generated value": created and value == sentinel and writes_first == 1,
        "a rerun reads the stored value and changes nothing": not created_again and again == sentinel and writes_rerun == writes_first,
        "a read asks for decryption and returns the plain value": plain_read == sentinel and not plain_read.startswith("AQECAH"),
        "a new value replaces the old one": stand_in.values[secret_store.AIRFLOW_JWT_SECRET] == sentinel + "-two",
        "a missing secret fails and names it": missing is not None and secret_store.REDIS_PASSWORD in missing,
        "a name outside the managed names, a dash name and a name without the leading slash are refused before any request": wrong is not None
        and wrong_read is not None
        and dash_name is not None
        and no_slash is not None
        and requests_for_wrong == 0,
        "the Airflow administrator password is read and never written": admin_read == sentinel + "-admin" and read_only_write is not None,
        "a shared key is read from Secrets Manager and never written": shared_value == sentinel + "-shared"
        and shared_requests == ["GetSecretValue"]
        and shared_write is not None
        and shared_name not in stand_in.values,
        "an empty or oversized value is refused before any request": empty_value is not None and oversize_value is not None and requests_for_wrong == 0,
        "a missing create permission fails visibly and names the secret and the action": denied_create is not None
        and secret_store.POLARIS_ROOT_SECRET in denied_create
        and "creating" in denied_create,
        "a missing read permission fails visibly and names the secret": denied_read is not None and NAME in denied_read,
        "a concurrent create leaves the winner's value": not raced_created and raced == "winner-" + sentinel,
        "a write that the service stores differently is caught by the readback": readback is not None
        and secret_store.POLARIS_GOLD_CLIENT_ID in readback
        and "readback" in readback,
        "every write is a SecureString on the standard tier": written_types == {("SecureString", "Standard")},
        "the client allows two requests in all, not two retries": secret_store.CLIENT_OPTIONS["retries"] == {"total_max_attempts": 2, "mode": "standard"},
        "only managed names reach the stand-in": stored_names <= set(secret_store.MANAGED) | set(secret_store.READ_ONLY),
        "an endpoint off the loopback address is refused": refused_endpoint is not None,
        "the client ignores endpoint URLs from the environment and the AWS files": ignores_endpoint_settings,
        "no message holds a secret value": not any(sentinel in message for message in messages),
        "nothing is printed": printed.getvalue() == "",
        "no file under the scratch folder or the repository holds a secret value": not files_holding(
            [sentinel, "winner-" + sentinel], [Path(tempfile.gettempdir()), root], began, report_links=False
        ),
    }


def store_races(root: Path) -> dict[str, bool]:
    """Two initializers get one value; a loser waits for the winner and never writes; a winner that never shows its value ends the wait at the deadline."""
    sentinel = "race-" + os.urandom(8).hex()
    with StandIn() as stand_in:
        store = secret_store.open_store(root, stand_in.endpoint)
        barrier = threading.Barrier(2)
        returned: dict[str, tuple[str, bool]] = {}
        real_put = store.parameters.put_parameter

        def meet(**arguments: Any) -> Any:
            with contextlib.suppress(threading.BrokenBarrierError):
                barrier.wait(timeout=5)
            return real_put(**arguments)

        store.parameters.put_parameter = meet

        def initialize(label: str) -> None:
            returned[label] = store.ensure(NAME, lambda: f"{sentinel}-{label}")

        threads = [threading.Thread(target=initialize, args=(label,)) for label in ("one", "two")]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)
        stored = stand_in.values.get(NAME)
        creates = [entry for entry in stand_in.changed() if entry[1] == NAME]
        values = {value for value, _ in returned.values()}
        created_flags = sorted(flag for _, flag in returned.values())

        slow = secret_store.POLARIS_ROOT_SECRET
        timers: list[threading.Timer] = []

        def collide_late(**arguments: Any) -> Any:
            timers.append(threading.Timer(0.6, lambda: stand_in.values.__setitem__(slow, "late-" + sentinel)))
            timers[0].start()
            raise ClientError({"Error": {"Code": "ParameterAlreadyExists", "Message": "exists"}}, "PutParameter")

        store.parameters.put_parameter = collide_late
        began = time.monotonic()
        waited, waited_created = store.ensure(slow, lambda: "must-not-be-used", wait=5)
        waited_seconds = time.monotonic() - began
        for timer in timers:
            timer.join(timeout=5)

        stuck = secret_store.POLARIS_READER_CLIENT_ID

        def collide(**arguments: Any) -> Any:
            raise ClientError({"Error": {"Code": "ParameterAlreadyExists", "Message": "exists"}}, "PutParameter")

        store.parameters.put_parameter = collide
        changes_before = len(stand_in.changed())
        began = time.monotonic()
        never = refusal(lambda: store.ensure(stuck, lambda: sentinel + "-never", wait=0.6))
        never_seconds = time.monotonic() - began
        store.parameters.put_parameter = real_put
        never_wrote = len(stand_in.changed()) == changes_before and stuck not in stand_in.values
    return {
        "two initializers return one value": len(values) == 1 and stored in values,
        "exactly one initializer created the secret and the stored value is the one returned": created_flags == [False, True] and len(creates) == 1,
        "a loser waits for the winner's value and returns it without writing": waited == "late-" + sentinel
        and not waited_created
        and 0.5 <= waited_seconds < 4,
        "a winner that never shows a value ends the wait at the deadline and nothing is written": never is not None
        and secret_store.POLARIS_READER_CLIENT_ID in never
        and 0.5 <= never_seconds < 4
        and never_wrote,
    }


def store_deadline(root: Path) -> dict[str, bool]:
    """A stalled fetch returns at the deadline naming each unread secret, and a healthy fetch returns every value."""
    sentinel = "fetch-" + os.urandom(8).hex()
    names = [secret_store.POSTGRES_PASSWORD, secret_store.POLARIS_ROOT_SECRET, secret_store.REDIS_PASSWORD]
    with StandIn({names[0]: sentinel + "-a", names[1]: sentinel + "-b"}) as stand_in:
        healthy = secret_store.fetch_all(root, stand_in.endpoint, names, 10)
        stand_in.stalled.add("GetParameter")
        began = time.monotonic()
        stalled = secret_store.fetch_all(root, stand_in.endpoint, names, 1.0)
        stalled_seconds = time.monotonic() - began
        stand_in.released.set()
    unreadable = secret_store.fetch_all(root, "http://example.org:4566", names, 5)
    return {
        "a healthy fetch returns every present value and marks the missing one": healthy[names[0]] == sentinel + "-a"
        and healthy[names[1]] == sentinel + "-b"
        and isinstance(healthy[names[2]], secret_store.SecretMissing),
        "a stalled fetch returns at the deadline": 0.9 <= stalled_seconds < 3,
        "a stalled fetch names every unread secret and holds no value": all(
            isinstance(stalled[name], secret_store.SecretStoreError) and name in str(stalled[name]) for name in names
        ),
        "a failure to set up the client is reported for every name": all(isinstance(unreadable[name], secret_store.SecretStoreError) for name in names),
    }


def regular_text(path: Path) -> str | None:
    """The text of a regular file, or ``None`` for a link, a pipe or any other kind of entry, none of which is opened for a wait or followed."""
    try:
        with os.fdopen(os.open(path, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW), "rb") as handle:
            return handle.read().decode() if stat.S_ISREG(os.fstat(handle.fileno()).st_mode) else None
    except OSError:
        return None


def source_offenders(root: Path) -> list[str]:
    """The Terraform files that declare a secret or a parameter; only a data source of a public AWS parameter (``/aws/service/...``) is no offender."""
    found = []
    for path in sorted((root / "infra").rglob("*.tf")):
        if ".terraform" in path.parts:
            continue
        text = regular_text(path)
        if text is None:
            found.append(f"{path.relative_to(root)} (not a regular file)")
            continue
        for match in SECRET_RESOURCE.finditer(text):
            body = text[match.end() : text.find("\n}", match.end())]
            if match.group(2) == "aws_ssm_parameter" and match.group(1) == "data" and re.search(r'^\s*name\s*=\s*"/aws/service/', body, re.MULTILINE):
                continue
            found.append(str(path.relative_to(root)))
    return found


def variable_defaults(root: Path, names: Iterable[str]) -> list[str | None]:
    """The default of each named variable in ``infra/iam/variables.tf``, or ``None`` when it has none."""
    text = (root / secret_store.VARIABLES).read_text()
    return [(found.group(1) if (found := re.search(rf'variable "{name}" \{{[^}}]*?default\s*=\s*"([^"]*)"', text, re.DOTALL)) else None) for name in names]


def a_parameter_resource_is_always_an_offender() -> bool:
    """The services stack manages no parameter: a plain String parameter resource there is an offender like any other."""
    block = 'resource "aws_ssm_parameter" "managed_value" {\n  name = "/healthcare_hai_prediction/managed_value"\n  type = "String"\n  value = "1"\n}\n'
    with tempfile.TemporaryDirectory() as folder:
        root = Path(folder)
        (root / "infra/services").mkdir(parents=True)
        (root / "infra/services/main.tf").write_text(block)
        return source_offenders(root) == ["infra/services/main.tf"]


def terraform_names(root: Path) -> dict[str, bool]:
    """The module's names equal the Terraform defaults, every name is a slash path with underscores, and no Terraform file declares a secret or a parameter."""
    declared = secret_store.terraform_defaults(root)
    offenders = source_offenders(root)
    family = ("aws_secretsmanager_secret", "aws_secretsmanager_secret_version", "aws_secretsmanager_random_password", "aws_secretsmanager_secrets")
    parameters = ("aws_ssm_parameter", "aws_ssm_parameters_by_path")
    shared_defaults = variable_defaults(root, ("census_secret_name", "bls_secret_name", "hud_secret_name", "jev_secret_name"))
    ordered = (*secret_store.MANAGED, *secret_store.READ_ONLY)
    return {
        "the source check matches every kind of block of the Secrets Manager and Parameter Store types": all(
            SECRET_RESOURCE.search(f'{kind} "{name}" "x" {{}}') for kind in ("resource", "data", "ephemeral") for name in (*family, *parameters)
        ),
        "the 23 variable defaults equal the module's managed names in order": declared == list(secret_store.MANAGED),
        "the 23 managed names and the read-only name are all different": len(set(ordered)) == len(ordered) == 24,
        "the policy files grant the module's names: four shared keys read only, 24 parameters read and the 23 managed ones written": secret_store.policy_names(
            root / secret_store.VARIABLES
        )
        == {
            "SHARED_SECRET_NAME": sorted(secret_store.SHARED),
            "READ_PARAMETER_NAME": sorted(ordered),
            "MANAGED_PARAMETER_NAME": sorted(secret_store.MANAGED),
        },
        "every name is a slash path under the project prefix with underscores": all(
            re.fullmatch(r"/healthcare_hai_prediction/[a-z0-9_]+", name) for name in ordered
        ),
        "the shared keys keep their Secrets Manager names and are not parameters": sorted(name or "" for name in shared_defaults) == sorted(secret_store.SHARED)
        and not any(name.startswith("/") for name in secret_store.SHARED),
        "no Terraform file declares a secret, a secret version, a parameter or a secret data source": not offenders,
        "a parameter resource in the services stack is an offender, with no exemption by name": a_parameter_resource_is_always_an_offender(),
    }


def clean_plan() -> dict[str, Any]:
    """The part of ``terraform show -json`` of a plan for the IAM stack that matters here: no secret resource, in every section the JSON has."""
    user = {"address": "aws_iam_user.project", "mode": "managed", "type": "aws_iam_user", "name": "project"}
    policy = {"address": "aws_iam_policy.secrets", "mode": "managed", "type": "aws_iam_policy", "name": "secrets"}
    return {
        "complete": True,
        "errored": False,
        "resource_changes": [{**user, "change": {"actions": ["create"]}}, {**policy, "change": {"actions": ["create"]}}],
        "planned_values": {"root_module": {"resources": [user, policy], "child_modules": [{"address": "module.one", "resources": []}]}},
        "prior_state": {"values": {"root_module": {"resources": []}}},
        "configuration": {"root_module": {"resources": [user, policy], "module_calls": {}}},
    }


def plan_refusal(plan: dict[str, Any]) -> str | None:
    """The text with which the saved-plan resource check refuses a plan, or ``None`` when it accepts it."""
    try:
        render_project_config.verify_plan_resources(plan)
    except render_project_config.ConfigurationError as error:
        return str(error)
    return None


def saved_plan(root: Path) -> dict[str, bool]:
    """The saved-plan check accepts a plan without Secrets Manager or Parameter Store objects and rejects every type of those families it meets."""
    secret = {"address": "aws_secretsmanager_secret.planted", "mode": "managed", "type": "aws_secretsmanager_secret", "name": "planted"}
    version = {"address": "aws_secretsmanager_secret_version.planted", "mode": "managed", "type": "aws_secretsmanager_secret_version", "name": "planted"}
    reading = {"address": "data.aws_secretsmanager_secret_version.planted", "mode": "data", "type": "aws_secretsmanager_secret_version", "name": "planted"}
    with_resource = clean_plan()
    with_resource["resource_changes"].append({**secret, "change": {"actions": ["create"]}})
    with_version = clean_plan()
    with_version["planned_values"]["root_module"]["child_modules"][0]["resources"].append(version)
    with_data = clean_plan()
    with_data["configuration"]["root_module"]["resources"].append(reading)
    with_data_state = clean_plan()
    with_data_state["prior_state"]["values"]["root_module"]["resources"].append(reading)
    password = {"address": "data.aws_secretsmanager_random_password.planted", "mode": "data", "type": "aws_secretsmanager_random_password", "name": "planted"}
    with_password = clean_plan()
    with_password["configuration"]["root_module"]["resources"].append(password)
    private = f"{secret_store.PREFIX}postgres_password"
    parameter = {"address": "aws_ssm_parameter.planted", "mode": "managed", "type": "aws_ssm_parameter", "name": "planted"}
    with_parameter = clean_plan()
    with_parameter["resource_changes"].append({**parameter, "change": {"actions": ["create"], "after": {"name": private}}})
    with_value = clean_plan()
    with_value["planned_values"]["root_module"]["resources"].append({**parameter, "values": {"name": private, "type": "SecureString"}})
    parameter_read = {"address": "data.aws_ssm_parameter.planted", "mode": "data", "type": "aws_ssm_parameter", "name": "planted"}
    read_private = clean_plan()
    read_private["configuration"]["root_module"]["resources"].append({**parameter_read, "expressions": {"name": {"constant_value": private}}})
    read_state = clean_plan()
    read_state["prior_state"]["values"]["root_module"]["resources"].append({**parameter_read, "values": {"name": private}})
    read_unnamed = clean_plan()
    read_unnamed["configuration"]["root_module"]["resources"].append({**parameter_read, "expressions": {"name": {"references": ["var.name"]}}})
    by_path = clean_plan()
    by_path["configuration"]["root_module"]["resources"].append(
        {"address": "data.aws_ssm_parameters_by_path.planted", "mode": "data", "type": "aws_ssm_parameters_by_path", "name": "planted"}
    )
    ephemeral = clean_plan()
    ephemeral["configuration"]["root_module"]["resources"].append(
        {"address": "ephemeral.aws_ssm_parameter.planted", "mode": "ephemeral", "type": "aws_ssm_parameter", "name": "planted"}
    )
    public = {"address": "data.aws_ssm_parameter.ecs_ami", "mode": "data", "type": "aws_ssm_parameter", "name": "ecs_ami"}
    public_name = "/aws/service/ecs/optimized-ami/amazon-linux-2023/arm64/recommended/image_id"
    with_public = clean_plan()
    with_public["configuration"]["root_module"]["resources"].append({**public, "expressions": {"name": {"constant_value": public_name}}})
    with_public["prior_state"]["values"]["root_module"]["resources"].append({**public, "values": {"name": public_name}})
    with_public["resource_changes"].append({**public, "change": {"actions": ["read"], "after": {"name": public_name}}})
    public_managed = clean_plan()
    public_managed["resource_changes"].append({**parameter, "change": {"actions": ["create"], "after": {"name": public_name}}})
    refusals = [plan_refusal(plan) for plan in (with_resource, with_version, with_data, with_data_state, with_password)]
    parameter_refusals = [
        plan_refusal(plan) for plan in (with_parameter, with_value, read_private, read_state, read_unnamed, by_path, ephemeral, public_managed)
    ]
    return {
        "a plan without Secrets Manager or Parameter Store objects is accepted": plan_refusal(clean_plan()) is None,
        "a plan that adds a secret resource is refused and names it": refusals[0] is not None and "aws_secretsmanager_secret.planted" in refusals[0],
        "a plan that adds a secret version inside a module is refused": refusals[1] is not None and "aws_secretsmanager_secret_version.planted" in refusals[1],
        "a plan that reads a secret through a data source is refused": refusals[2] is not None
        and "data.aws_secretsmanager_secret_version.planted" in refusals[2],
        "a data source only in the prior state is refused": refusals[3] is not None,
        "a plan with the random password data source is refused and names it": refusals[4] is not None
        and "data.aws_secretsmanager_random_password.planted" in refusals[4],
        "a plan that adds a parameter resource is refused and names it": parameter_refusals[0] is not None
        and "aws_ssm_parameter.planted" in parameter_refusals[0],
        "a parameter value held in the planned values is refused": parameter_refusals[1] is not None,
        "a plan that reads a project parameter through a data source is refused, in the configuration and in the prior state": parameter_refusals[2] is not None
        and parameter_refusals[3] is not None,
        "a parameter data source whose name is not a literal is refused": parameter_refusals[4] is not None,
        "a parameters-by-path data source and an ephemeral parameter are refused": parameter_refusals[5] is not None and parameter_refusals[6] is not None,
        "a public AWS parameter data source in every section is accepted": plan_refusal(with_public) is None,
        "a managed parameter under the public AWS path is refused": parameter_refusals[7] is not None,
    }


ADMIN_NAME = secret_store.PREFIX + "openmetadata_admin_password"
DB_NAME = secret_store.PREFIX + "openmetadata_db_password"
ADMIN_POLICY = (r"[A-Z]", r"[a-z]", r"[0-9]", r"[^A-Za-z0-9]")
# PasswordUtil.java of OpenMetadata 2.0.7, lines 38 to 48: 8 to 56 characters, no white space, one upper case letter, one lower case letter, one digit
# and one special character. Line 83 lists the specials its own generator uses. The setup keeps to that list, because the validator library's set is unread.
OM_SPECIALS = "!@#$%^&*()_+"


def meets_admin_policy(value: str) -> bool:
    """OpenMetadata 2.0.7's rule for a new administrator password, with the special characters limited to those its own generator uses."""
    return (
        8 <= len(value) <= 56
        and re.search(r"\s", value) is None
        and all(re.search(pattern, value) for pattern in ADMIN_POLICY)
        and all(character in OM_SPECIALS for character in value if not character.isalnum())
    )


def openmetadata_passwords(root: Path) -> dict[str, bool]:
    """The setup creates the catalog's two passwords once, straight into Parameter Store, and a rerun or a partial store changes only what is absent."""
    from scripts.infrastructure import openmetadata_secrets

    began = time.time() - 1
    printed = io.StringIO()

    def run(stand_in: StandIn) -> tuple[int, str]:
        buffer = io.StringIO()
        with contextlib.redirect_stdout(buffer), contextlib.redirect_stderr(buffer):
            code = openmetadata_secrets.main(["--root", str(root)], endpoint=stand_in.endpoint)
        printed.write(buffer.getvalue())
        return code, buffer.getvalue()

    with StandIn() as fresh:
        first, first_output = run(fresh)
        admin, database = fresh.values.get(ADMIN_NAME, ""), fresh.values.get(DB_NAME, "")
        writes_first = fresh.writes()
        second, second_output = run(fresh)
        writes_rerun = fresh.writes()
        stored_names = set(fresh.values)
        written_types = {(item["type"], item["tier"]) for item in fresh.writes_seen}
    with StandIn(values={ADMIN_NAME: "kept-" + os.urandom(6).hex()}) as partial:
        kept = partial.values[ADMIN_NAME]
        partial_exit, partial_output = run(partial)
        partial_changes = [entry for entry in partial.changed() if entry[1] in (ADMIN_NAME, DB_NAME)]
        partial_db = partial.values.get(DB_NAME, "")
        partial_admin = partial.values[ADMIN_NAME]
    with StandIn(denied={"CreateParameter"}) as denied:
        denied_exit, denied_text = run(denied)
        denied_values = dict(denied.values)
    with StandIn() as outside:
        outside_store = secret_store.open_store(root, outside.endpoint)
        default_refuses = refusal(lambda: outside_store.put(ADMIN_NAME, "x" * 12))
        outside_writes = outside.writes()
    secrets_seen = [value for value in (admin, database, kept, partial_db) if value]
    return {
        "a first run exits zero and creates both passwords": first == 0 and bool(admin) and bool(database) and writes_first == 2,
        "the administrator password meets the OpenMetadata password rule": meets_admin_policy(admin),
        "300 more generated administrator passwords all meet the OpenMetadata password rule": all(
            meets_admin_policy(openmetadata_secrets.new_admin_password()) for _ in range(300)
        ),
        "the two passwords differ, and the database password is long and holds only letters, digits, dash and underscore": admin != database
        and re.fullmatch(r"[A-Za-z0-9_-]{32,}", database) is not None,
        "a rerun exits zero and writes nothing": second == 0 and writes_rerun == writes_first,
        "only the two catalog names reach the store, as SecureString on the standard tier": stored_names == {ADMIN_NAME, DB_NAME}
        and written_types == {("SecureString", "Standard")},
        "a store that holds one password keeps it and creates only the other": partial_exit == 0
        and partial_admin == kept
        and partial_changes == [("CreateParameter", DB_NAME)]
        and bool(partial_db),
        "a missing create permission fails visibly, names the secret and creates nothing": denied_exit != 0
        and ADMIN_NAME in denied_text
        and "creating" in denied_text
        and not denied_values,
        "the default store still refuses to write a catalog name": default_refuses is not None and outside_writes == 0,
        "the output names each secret as created, then as present, and shows no value": f"{ADMIN_NAME}: created" in first_output
        and f"{DB_NAME}: created" in first_output
        and f"{ADMIN_NAME}: present" in second_output
        and f"{DB_NAME}: present" in second_output
        and f"{ADMIN_NAME}: present" in partial_output
        and f"{DB_NAME}: created" in partial_output
        and not any(value in printed.getvalue() for value in secrets_seen),
        "no file under the scratch folder or the repository holds a password": not files_holding(
            secrets_seen, [Path(tempfile.gettempdir()), root], began, report_links=False
        ),
    }


def model_api_redis_password(root: Path) -> dict[str, bool]:
    """The tool for the model API's Redis user password writes it once into Parameter Store, prints and keeps it nowhere, and never overwrites it."""
    from scripts.infrastructure import create_model_api_redis_password as tool

    began = time.time() - 1
    name = secret_store.MODEL_API_REDIS_PASSWORD
    first, second, denied_text = io.StringIO(), io.StringIO(), io.StringIO()
    with tempfile.TemporaryDirectory(prefix="redis_password_cases_") as scratch, StandIn(shared={}) as stand_in:
        with contextlib.redirect_stdout(first), contextlib.redirect_stderr(first):
            code_first = tool.run(root, stand_in.endpoint)
        stored = stand_in.values.get(name, "")
        writes_first = stand_in.writes()
        with contextlib.redirect_stdout(second), contextlib.redirect_stderr(second):
            code_second = tool.run(root, stand_in.endpoint)
        stand_in.values[name] = "kept-" + os.urandom(4).hex()
        kept = stand_in.values[name]
        with contextlib.redirect_stdout(second), contextlib.redirect_stderr(second):
            tool.run(root, stand_in.endpoint)
        unchanged_after_rerun = stand_in.values[name] == kept and stand_in.writes() == writes_first
        del stand_in.values[name]
        stand_in.denied.add("CreateParameter")
        with contextlib.redirect_stdout(denied_text), contextlib.redirect_stderr(denied_text):
            code_denied = tool.run(root, stand_in.endpoint)
        stand_in.denied.clear()
        written_types = {(item["type"], item["tier"]) for item in stand_in.writes_seen}
        leaked_files = files_holding([stored], [Path(scratch), root / "data"], began) if stored else ["unchecked"]
    printed = first.getvalue() + second.getvalue() + denied_text.getvalue()
    return {
        "the first run creates the parameter and exits 0": code_first == 0 and bool(stored) and writes_first == 1,
        "the generated password is long and random": len(stored) >= 32 and stored == stored.strip() and tool.secret_store.is_fernet_key(stored) is False,
        "it is a SecureString on the standard tier": written_types == {("SecureString", "Standard")},
        "a second run exits 0, changes nothing and says so": code_second == 0 and "already exists" in second.getvalue() and unchanged_after_rerun,
        "a denied create exits 1 and names the secret": code_denied == 1 and name in denied_text.getvalue(),
        "no output holds the value": bool(stored) and stored not in printed and kept not in printed,
        "no file written since the run holds the value": leaked_files == [],
        "the tool names exactly the model API Redis parameter": tool.NAME == name == secret_store.PREFIX + "model_api_redis_password",
    }


def password_tool(root: Path, tool: Any, name: str) -> dict[str, bool]:
    """The tool for one service password writes it once into Parameter Store, prints and keeps it nowhere, and never overwrites it."""

    began = time.time() - 1
    first, second, denied_text = io.StringIO(), io.StringIO(), io.StringIO()
    with tempfile.TemporaryDirectory(prefix="password_tool_cases_") as scratch, StandIn(shared={}) as stand_in:
        with contextlib.redirect_stdout(first), contextlib.redirect_stderr(first):
            code_first = tool.run(root, stand_in.endpoint)
        stored = stand_in.values.get(name, "")
        writes_first = stand_in.writes()
        with contextlib.redirect_stdout(second), contextlib.redirect_stderr(second):
            code_second = tool.run(root, stand_in.endpoint)
        stand_in.values[name] = "kept-" + os.urandom(4).hex()
        kept = stand_in.values[name]
        with contextlib.redirect_stdout(second), contextlib.redirect_stderr(second):
            tool.run(root, stand_in.endpoint)
        unchanged_after_rerun = stand_in.values[name] == kept and stand_in.writes() == writes_first
        del stand_in.values[name]
        stand_in.denied.add("CreateParameter")
        with contextlib.redirect_stdout(denied_text), contextlib.redirect_stderr(denied_text):
            code_denied = tool.run(root, stand_in.endpoint)
        stand_in.denied.clear()
        written_types = {(item["type"], item["tier"]) for item in stand_in.writes_seen}
        leaked_files = files_holding([stored], [Path(scratch), root / "data"], began) if stored else ["unchecked"]
    printed = first.getvalue() + second.getvalue() + denied_text.getvalue()
    return {
        "the first run creates the parameter and exits 0": code_first == 0 and bool(stored) and writes_first == 1,
        "the generated password is long and random": len(stored) >= 32 and stored == stored.strip() and tool.secret_store.is_fernet_key(stored) is False,
        "it is a SecureString on the standard tier": written_types == {("SecureString", "Standard")},
        "a second run exits 0, changes nothing and says so": code_second == 0 and "already exists" in second.getvalue() and unchanged_after_rerun,
        "a denied create exits 1 and names the secret": code_denied == 1 and name in denied_text.getvalue(),
        "no output holds the value": bool(stored) and stored not in printed and kept not in printed,
        "no file written since the run holds the value": leaked_files == [],
        "the tool names exactly its one parameter": name == tool.NAME and name.startswith(secret_store.PREFIX) and name in secret_store.MANAGED,
    }


def redis_exporter_password(root: Path) -> dict[str, bool]:
    from scripts.infrastructure import create_redis_exporter_password as tool

    return password_tool(root, tool, secret_store.REDIS_EXPORTER_PASSWORD)


def postgres_exporter_password(root: Path) -> dict[str, bool]:
    from scripts.infrastructure import create_postgres_exporter_password as tool

    return password_tool(root, tool, secret_store.POSTGRES_EXPORTER_PASSWORD)


class RecordedLogs(logging.Handler):
    """Keeps the text of every log record that reaches the root logger at any level."""

    def __init__(self) -> None:
        super().__init__(logging.DEBUG)
        self.lines: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.lines.append(record.getMessage())


def captured(call: Callable[[], int]) -> tuple[int, str]:
    """The exit code of a tool call and everything it printed or logged, standard error included."""
    handler, logger, level, output = RecordedLogs(), logging.getLogger(), logging.getLogger().level, io.StringIO()
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    try:
        with contextlib.redirect_stdout(output), contextlib.redirect_stderr(output):
            code = call()
    finally:
        logger.removeHandler(handler)
        logger.setLevel(level)
    return code, output.getvalue() + "\n".join(handler.lines)


def secret_writes_stay_out_of_debug_logs(root: Path) -> dict[str, bool]:
    """With the root logger at DEBUG, no write path of a secret puts its value in a log record; the values are synthetic markers, never a real secret."""
    from scripts.infrastructure import create_model_api_redis_password, mlflow_reader_setup, openmetadata_persistent_bootstrap, openmetadata_secrets

    def marker(label: str) -> str:
        return f"marker-{label}-" + os.urandom(8).hex()

    def logged(call: Callable[[], Any]) -> str:
        return captured(lambda: call() and 0)[1]

    one, two, three, collided, token, control = marker("put"), marker("ensure"), marker("send"), marker("collide"), marker("token"), marker("control")
    with StandIn(shared={}) as stand_in:
        store = secret_store.open_store(root, stand_in.endpoint)
        control_text = logged(lambda: store.parameters.put_parameter(Name=NAME, Value=control, Type="SecureString", Overwrite=True))
        put_text = logged(lambda: store.put(secret_store.AIRFLOW_JWT_SECRET, one))
        ensure_text = logged(lambda: store.ensure(secret_store.POLARIS_ROOT_SECRET, lambda: two))
        send_text = logged(lambda: store.send(secret_store.POSTGRES_EXPORTER_PASSWORD, three, overwrite=False))
        collision_text = logged(lambda: store.send(secret_store.POSTGRES_EXPORTER_PASSWORD, collided, overwrite=False))
        token_text = logged(
            lambda: openmetadata_persistent_bootstrap.write_parameter(store.parameters, secret_store.PREFIX + "openmetadata_bot_lineage", token)
        )
        reader_text = logged(lambda: mlflow_reader_setup.stored_reader_login(store.parameters, secret_store.PREFIX + "mlflow_reader_password"))
        reader_value = stand_in.values.get(secret_store.PREFIX + "mlflow_reader_password", "")
        token_read_text = logged(lambda: openmetadata_persistent_bootstrap.read_parameter(store.parameters, secret_store.PREFIX + "openmetadata_bot_lineage"))
    with StandIn(shared={}) as stand_in:
        redis_text = captured(lambda: create_model_api_redis_password.run(root, stand_in.endpoint))[1]
        redis_value = stand_in.values.get(create_model_api_redis_password.NAME, "")
    with StandIn(shared={}) as stand_in:
        om_text = captured(lambda: openmetadata_secrets.main(["--root", str(root)], stand_in.endpoint))[1]
        om_values = [stand_in.values.get(name, "") for name in openmetadata_secrets.NAMES]
    with StandIn(shared={}) as stand_in:
        redis_exporter_text = captured(lambda: redis_exporter_password_run(root, stand_in.endpoint))[1]
        redis_exporter_value = stand_in.values.get(secret_store.REDIS_EXPORTER_PASSWORD, "")
        postgres_exporter_text = captured(lambda: postgres_exporter_password_run(root, stand_in.endpoint))[1]
        postgres_exporter_value = stand_in.values.get(secret_store.POSTGRES_EXPORTER_PASSWORD, "")
    script = root / "scripts" / "infrastructure" / "openmetadata_persistent_bootstrap.py"
    spec = importlib.util.spec_from_file_location("bootstrap_run_by_path", script)
    if spec is None or spec.loader is None:
        raise RuntimeError("the bootstrap file cannot be loaded by path")
    plain_script = importlib.util.module_from_spec(spec)
    with (
        mock.patch.dict(sys.modules, {"scripts.sdk_logs": None, "scripts.infrastructure.openmetadata_bootstrap": None}),
        mock.patch.object(sys, "path", [str(script.parent), *sys.path]),
    ):
        spec.loader.exec_module(plain_script)
    return {
        "the unguarded control shows its marker, so the check can see a leak": control in control_text,
        "put keeps its value out of the log": one not in put_text,
        "an ensure that creates keeps its value out of the log": two not in ensure_text,
        "a create that succeeds keeps its value out of the log": three not in send_text,
        "a create that collides keeps its value out of the log": collided not in collision_text,
        "the Redis password for the model API stays out of the log": len(redis_value) >= 32 and redis_value not in redis_text,
        "the OpenMetadata passwords stay out of the log": all(len(value) >= 32 and value not in om_text for value in om_values),
        "the Redis exporter password stays out of the log": len(redis_exporter_value) >= 32 and redis_exporter_value not in redis_exporter_text,
        "the Postgres exporter password stays out of the log": len(postgres_exporter_value) >= 32 and postgres_exporter_value not in postgres_exporter_text,
        "an OpenMetadata bot token write and read stay out of the log": token not in token_text and token not in token_read_text,
        "the MLflow reader password stays out of the log": len(reader_value) >= 32 and reader_value not in reader_text,
        "the bootstrap, run as a plain script without the repository root on the import path, still loads the guard": plain_script.sdk_logs.__name__
        == "sdk_logs",
    }


def redis_exporter_password_run(root: Path, endpoint: str) -> int:
    from scripts.infrastructure import create_redis_exporter_password as tool

    return tool.run(root, endpoint)


def postgres_exporter_password_run(root: Path, endpoint: str) -> int:
    from scripts.infrastructure import create_postgres_exporter_password as tool

    return tool.run(root, endpoint)


def serving_secrets_store(root: Path) -> dict[str, bool]:
    """The serving secrets tool creates each Parameter Store value once, keeps the model API key hashes consistent with the page key and never overwrites."""
    from scripts.infrastructure import create_serving_secrets as tool
    from scripts.serving import keys

    began = time.time() - 1
    prefix = secret_store.PREFIX
    grafana, page_key, hashes, user, credential = (
        secret_store.GRAFANA_ADMIN_PASSWORD,
        secret_store.STREAMLIT_MODEL_API_KEY,
        secret_store.MODEL_API_KEY_HASHES,
        secret_store.MODEL_API_MLFLOW_USER,
        secret_store.MODEL_API_MLFLOW_CREDENTIAL,
    )
    five = {grafana, page_key, hashes, user, credential}
    other = keys.hash_key("another-caller-" + os.urandom(4).hex())
    with tempfile.TemporaryDirectory(prefix="serving_secrets_cases_") as scratch, StandIn(shared={}) as stand_in:
        first_code, first = captured(lambda: tool.run(root, stand_in.endpoint))
        stored = dict(stand_in.values)
        first_changes, first_writes = stand_in.changed(), stand_in.writes()
        second_code, second = captured(lambda: tool.run(root, stand_in.endpoint))
        rerun_quiet = stand_in.writes() == first_writes and stand_in.values == stored
        never_read_admin = all(name != secret_store.MLFLOW_ADMIN_PASSWORD for _, name in stand_in.requests())
        written_types = {(item["type"], item["tier"]) for item in stand_in.writes_seen}
        seen = [stored.get(grafana, ""), stored.get(page_key, ""), stored.get(credential, "")]
        leaked = files_holding([value for value in seen if value], [Path(scratch), root / "data"], began) if all(seen) else ["unchecked"]
        try:
            ring = keys.KeyRing.from_hashes(json.loads(stored[hashes]))
            ring_names_page = ring.identify(stored[page_key]) == "streamlit"
        except (KeyError, ValueError, keys.KeyRingInvalid):
            ring_names_page = False
    with StandIn(values={hashes: json.dumps({"other": other})}, shared={}) as stand_in:
        missing_code, missing = captured(lambda: tool.run(root, stand_in.endpoint))
        missing_kept = stand_in.values[hashes] == json.dumps({"other": other}) and page_key not in stand_in.values
        independent_created = {grafana, user, credential} <= set(stand_in.values)
    wrong_hashes = json.dumps({"streamlit": other})
    with StandIn(values={hashes: wrong_hashes, page_key: "page-key-" + os.urandom(4).hex()}, shared={}) as stand_in:
        kept_key = stand_in.values[page_key]
        wrong_code, wrong = captured(lambda: tool.run(root, stand_in.endpoint))
        wrong_kept = stand_in.values[hashes] == wrong_hashes and stand_in.values[page_key] == kept_key
    shared = json.dumps({"other": other, "streamlit": keys.hash_key("page-key-fixed")})
    with StandIn(values={hashes: shared, page_key: "page-key-fixed"}, shared={}) as stand_in:
        match_code, match = captured(lambda: tool.run(root, stand_in.endpoint))
        match_kept = stand_in.values[hashes] == shared and stand_in.values[page_key] == "page-key-fixed"
    with StandIn(values={page_key: "page-key-only"}, shared={}) as stand_in:
        late_code, _ = captured(lambda: tool.run(root, stand_in.endpoint))
        late_hashes = json.loads(stand_in.values.get(hashes, "{}"))
        late_kept = stand_in.values[page_key] == "page-key-only"
    with StandIn(values={hashes: "not json"}, shared={}) as stand_in:
        garbled_code, garbled = captured(lambda: tool.run(root, stand_in.endpoint))
        garbled_kept = stand_in.values[hashes] == "not json"
    with StandIn(denied={"CreateParameter"}, shared={}) as stand_in:
        denied_code, denied = captured(lambda: tool.run(root, stand_in.endpoint))
        denied_values = dict(stand_in.values)
    options = {option for action in tool.parser()._actions for option in action.option_strings}
    return {
        "the first run exits 0 and creates the five values with one create each": first_code == 0
        and set(stored) == five
        and first_changes == [("CreateParameter", name) for _, name in first_changes]
        and {name for _, name in first_changes} == five
        and first_writes == 5,
        "every value is a SecureString on the standard tier": written_types == {("SecureString", "Standard")},
        "the Grafana password, the page key and the credential are long random values": len(stored.get(grafana, "")) >= 32
        and len(stored.get(credential, "")) >= 32
        and len(set(seen)) == 3
        and len(stored.get(page_key, "")) >= 32,
        "the MLflow user value is model_api": stored.get(user) == "model_api" == tool.MLFLOW_USER_VALUE,
        "the key hashes hold the streamlit entry, the SHA-256 of the stored page key, and the key ring accepts them": json.loads(stored.get(hashes, "{}"))
        == {"streamlit": keys.hash_key(stored.get(page_key, "-"))}
        and ring_names_page,
        "a second run exits 0, writes nothing and says present for each value": second_code == 0
        and rerun_quiet
        and all(f"{name}: present" in second for name in five),
        "the first run says created for each value": all(f"{name}: created" in first for name in five),
        "a run without an MLflow address never reads the MLflow administrator password": never_read_admin,
        "no output or log line holds a generated value": all(seen) and not any(value in first + second for value in seen),
        "no file written since the run holds a generated value": leaked == [],
        "hashes without a streamlit entry and no page key: exit 1, named, nothing overwritten, no page key created": missing_code == 1
        and hashes in missing
        and "streamlit" in missing
        and missing_kept,
        "a refusal does not stop the independent values from being created": independent_created,
        "hashes with another streamlit hash: exit 1, names the hashes, the hashes and the key stay": wrong_code == 1
        and hashes in wrong
        and "streamlit" in wrong
        and wrong_kept,
        "hashes with other callers and the matching streamlit entry stay and the run says present": match_code == 0
        and f"{hashes}: present" in match
        and match_kept,
        "a page key without hashes gets hashes made from that key and the key stays": late_code == 0
        and late_hashes == {"streamlit": keys.hash_key("page-key-only")}
        and late_kept,
        "hashes that are not a JSON object of key identifiers: exit 1, named, unchanged": garbled_code == 1 and hashes in garbled and garbled_kept,
        "a denied create exits 1, names a secret and stores nothing": denied_code == 1 and prefix in denied and not denied_values,
        "the command line has no option that takes a secret": options <= {"-h", "--help", "--root", "--hai-types", "--preparation-inputs", "--mlflow-url"},
    }


def free_loopback_address() -> str:
    """A loopback address that nothing listens on."""
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return f"http://127.0.0.1:{probe.getsockname()[1]}"


def serving_secrets_mlflow(root: Path) -> dict[str, bool]:
    """The tool calls the shared account check after the secrets: user and READ grants made, a wrong password and DENY refused, deadline kept."""
    from scripts.infrastructure import create_serving_secrets as tool

    began = time.time() - 1
    admin = "admin-" + os.urandom(8).hex()
    types = ("HAI_1", "HAI_2")
    admin_name, credential_name, user_name = secret_store.MLFLOW_ADMIN_PASSWORD, secret_store.MODEL_API_MLFLOW_CREDENTIAL, secret_store.MODEL_API_MLFLOW_USER
    served = "model_api"
    model_1, model_2, experiment_1, experiment_2 = "hai_model_HAI_1", "hai_model_HAI_2", "hai_training_HAI_1", "hai_training_HAI_2"
    grant_model_1, grant_experiment_1 = (served, "registered_model", model_1), (served, "experiment", "7")
    routes = {
        ("GET", "/api/2.0/mlflow/experiments/search"),
        ("GET", "/api/2.0/mlflow/users/get"),
        ("POST", "/api/2.0/mlflow/users/create"),
        ("GET", "/api/2.0/mlflow/registered-models/search"),
        ("GET", "/api/2.0/mlflow/registered-models/get"),
        ("GET", "/api/2.0/mlflow/experiments/get-by-name"),
        ("GET", "/api/3.0/mlflow/users/permissions/get"),
        ("POST", "/api/3.0/mlflow/users/permissions/grant"),
    }
    with (
        tempfile.TemporaryDirectory(prefix="serving_mlflow_cases_") as scratch,
        StandIn(values={admin_name: admin}, shared={}) as store,
        MlflowAuthStandIn(admin, models={model_1}, experiments={experiment_1: ("7", "active"), experiment_2: ("8", "deleted")}) as server,
    ):
        server.default_permission = "NO_PERMISSIONS"
        first_code, first = captured(lambda: tool.run(root, store.endpoint, server.url, types))
        credential = store.values.get(credential_name, "")
        first_grants = dict(server.grants)
        first_users = list(server.created_users)
        first_routes = {(method, address.partition("?")[0]) for method, address in server.log}
        create_bodies = [(kind, names) for path, kind, names in server.bodies if path == "/api/2.0/mlflow/users/create"]
        in_address = any(admin in address or credential in address for _, address in server.log)
        password_sent = server.users.get(served) == credential
        second_code, second = captured(lambda: tool.run(root, store.endpoint, server.url, types))
        rerun_quiet = server.created_users == first_users and server.grants == first_grants
        server.models.add(model_2)
        server.experiments[experiment_2] = ("8", "active")
        third_code, third = captured(lambda: tool.run(root, store.endpoint, server.url, types))
        later_grants = {(served, "registered_model", model_2), (served, "experiment", "8")}
        late_ok = third_code == 0 and later_grants <= set(server.grants) and {key for key in server.grants if key not in later_grants} == set(first_grants)
        server.grants[grant_model_1] = "EDIT"
        edit_before = dict(server.grants)
        edit_code, edit = captured(lambda: tool.run(root, store.endpoint, server.url, types))
        edit_kept = server.grants == edit_before
        server.grants[grant_model_1] = "READ"
        server.grants[grant_experiment_1] = "DENY"
        deny_code, deny = captured(lambda: tool.run(root, store.endpoint, server.url, types))
        deny_kept = server.grants[grant_experiment_1] == "DENY"
        removed_after_all = list(server.removed_hits)
        leaked = files_holding([admin, credential], [Path(scratch), root / "data"], began) if credential else ["unchecked"]
    with (
        StandIn(values={admin_name: admin}, shared={}) as store,
        MlflowAuthStandIn(admin, models={model_1}, experiments={experiment_1: ("7", "active")}) as server,
    ):
        read_code, read = captured(lambda: tool.run(root, store.endpoint, server.url, ("HAI_1",)))
        read_ok = read_code == 0 and server.grants == {} and "mlflow_user: ready" in read and f"user created {served}" in read
    other_password = "another-" + os.urandom(8).hex()
    with (
        StandIn(values={admin_name: admin}, shared={}) as store,
        MlflowAuthStandIn(admin, models={model_1}, experiments={experiment_1: ("7", "active")}) as server,
    ):
        server.users[served] = other_password
        mismatch_code, mismatch = captured(lambda: tool.run(root, store.endpoint, server.url, ("HAI_1",)))
        mismatch_ok = (
            mismatch_code == 1
            and server.users[served] == other_password
            and store.values.get(credential_name) not in (None, other_password)
            and server.grants == {}
            and "not overwritten" in mismatch
            and "mlflow_user: ready" not in mismatch
            and other_password not in mismatch
            and store.values.get(credential_name, "-") not in mismatch
        )
    with (
        StandIn(values={admin_name: admin, user_name: served, credential_name: other_password + "-stored"}, shared={}) as store,
        MlflowAuthStandIn(admin, models={model_1}, experiments={experiment_1: ("7", "active")}) as server,
    ):
        server.users[served] = other_password
        stored_code, stored = captured(lambda: tool.run(root, store.endpoint, server.url, ("HAI_1",)))
        stored_ok = stored_code == 1 and server.users[served] == other_password and server.grants == {} and "mlflow_user: ready" not in stored
    with (
        StandIn(values={admin_name: admin}, shared={}) as store,
        MlflowAuthStandIn(admin, models={model_1}, experiments={experiment_1: ("7", "active")}) as server,
    ):
        server.users[served] = "x"
        server.slow_body_seconds = 0.03
        check = tool.account_check(root)
        run_seconds = check.RUN_SECONDS
        check.RUN_SECONDS = 0.6
        began_slow = time.monotonic()
        try:
            slow_code, slow = captured(lambda: tool.run(root, store.endpoint, server.url, ("HAI_1",)))
        finally:
            check.RUN_SECONDS = run_seconds
        slow_seconds = time.monotonic() - began_slow
        slow_ok = slow_code == 1 and slow_seconds < 3.0 and "deadline" in slow and "mlflow_user: ready" not in slow
    with (
        StandIn(values={admin_name: admin, user_name: served, credential_name: "stored-" + admin}, shared={}) as store,
        MlflowAuthStandIn(admin, models={model_1}, experiments={experiment_1: ("7", "deleted")}) as server,
    ):
        server.users[served] = store.values[credential_name]
        server.grants[grant_experiment_1] = "DENY"
        trash_code, trash = captured(lambda: tool.run(root, store.endpoint, server.url, ("HAI_1",)))
        trash_ok = (
            trash_code == 1 and server.grants[grant_experiment_1] == "DENY" and experiment_1 in trash and "DENY" in trash and "mlflow_user: ready" not in trash
        )
    stalls: list[bool] = []
    for http11, setup_delay in ((False, 0.0), (True, 0.0), (False, 0.3), (True, 0.3)):
        with (
            StandIn(values={admin_name: admin, user_name: served, credential_name: "stored-" + admin}, shared={}) as store,
            MlflowAuthStandIn(admin, models={model_1}, experiments={experiment_1: ("7", "active")}) as server,
        ):
            server.users[served] = store.values[credential_name]
            server.stall_path, server.stall_http11, server.stall_seconds = "/api/3.0/mlflow/users/permissions/get", http11, STALL_SECONDS
            server.first_byte_seconds = FIRST_BYTE_SECONDS
            check = tool.account_check(root)
            run_seconds, ensure_account = check.RUN_SECONDS, check.ensure_account
            check.RUN_SECONDS = 0.4
            interval: dict[str, float] = {}

            def measured(*arguments: Any, _ensure: Callable[..., Any] = ensure_account, _interval: dict[str, float] = interval) -> Any:
                """The shared account check, with the absolute deadline it was given and the moment it ended; setup time is outside both."""
                _interval["deadline"] = arguments[5]
                try:
                    return _ensure(*arguments)
                finally:
                    _interval["ended"] = time.monotonic()

            try:
                with (
                    mock.patch.object(check, "ensure_account", measured),
                    mock.patch.object(secret_store, "open_store", slow_open(secret_store.open_store, setup_delay)),
                ):
                    stall_code, stall = captured(lambda: tool.run(root, store.endpoint, server.url, ("HAI_1",)))
            finally:
                check.RUN_SECONDS = run_seconds
            late = interval.get("ended", 0.0) - interval.get("deadline", 0.0)
            stalls.append(
                stall_code == 1
                and "deadline" in stall
                and "mlflow_user: ready" not in stall
                and 0.0 <= late < DEADLINE_ALLOWANCE
                and not server.release.is_set()
            )
    with StandIn(shared={}) as store, MlflowAuthStandIn(admin) as server:
        no_admin_code, no_admin = captured(lambda: tool.run(root, store.endpoint, server.url, types))
        no_admin_quiet = server.log == []
    with StandIn(values={admin_name: "not-" + admin}, shared={}) as store, MlflowAuthStandIn(admin) as server:
        wrong_code, wrong = captured(lambda: tool.run(root, store.endpoint, server.url, types))
        wrong_text_clean = admin not in wrong and "not-" + admin not in wrong and "401" in wrong
    refused_addresses = (
        "http://10.0.0.5:5000",
        "https://mlflow.example.invalid",
        "http://user:pass@127.0.0.1:5000",
        "http://127.0.0.1.evil.example:5000",
        "ftp://127.0.0.1:5000",
    )
    refused: list[bool] = []
    for address in refused_addresses:
        with StandIn(values={admin_name: admin}, shared={}) as store:
            code, text = captured(functools.partial(tool.run, root, store.endpoint, address, types))
            refused.append(code == 1 and "loopback" in text and admin not in text and all(name != admin_name for _, name in store.requests()))
    with StandIn(values={admin_name: admin}, shared={}) as store:
        down_address = free_loopback_address()
        down_code, down = captured(lambda: tool.run(root, store.endpoint, down_address, types))
        bad_type_code, bad_type = captured(lambda: tool.run(root, store.endpoint, free_loopback_address(), ("HAI_9",)))
    with MlflowAuthStandIn(admin) as server:
        old_route_answered = old_route_status(server.url, admin) == (404, True) and len(server.removed_hits) == 1
    with tempfile.TemporaryDirectory(prefix="serving_types_cases_") as folder:
        inputs = Path(folder) / "services.json"
        inputs.write_text(
            json.dumps({"schema_version": 1, "root": "services", "variables": {"model_api_hai_types": "HAI_1,HAI_2", "streamlit_hai_types": "HAI_1"}})
        )
        from_file = tool.served_types(None, inputs, {})
        flat = Path(folder) / "flat.json"
        flat.write_text(json.dumps({"model_api_hai_types": "HAI_1,HAI_2"}))
        envelope = {"schema_version": 1, "root": "services"}

        def written(name: str, text: str) -> Path:
            path = Path(folder) / name
            path.write_text(text)
            return path

        def reviewed(name: str, variables: dict[str, Any]) -> Path:
            return written(name, json.dumps({**envelope, "variables": variables}))

        default_text = secret_store.terraform_variables(root / "infra/services/model_api.tf")["model_api_hai_types"]
        # A reviewed file without the setting serves the default that the services stack declares (the schema requires no setting).
        from_empty = tool.served_types(None, reviewed("empty.json", {}), {})
        from_other = tool.served_types(None, reviewed("other.json", {"streamlit_hai_types": "HAI_1", "model_api_enabled": True}), {})
        not_json = written("not_json.json", "[")
        duplicate_setting = written(
            "duplicate_setting.json",
            '{"schema_version": 1, "root": "services", "variables": {"model_api_hai_types": "HAI_1", "model_api_hai_types": "HAI_2"}}',
        )
        duplicate_envelope = written("duplicate_envelope.json", '{"schema_version": 1, "root": "services", "root": "services", "variables": {}}')
        file_refused = [
            refusal_of(functools.partial(tool.served_types, None, path, {}))
            for path in (
                written("flat.json", json.dumps({"model_api_hai_types": "HAI_1,HAI_2"})),
                not_json,
                reviewed("unknown_setting.json", {"model_api_hai_types": "HAI_1", "no_such_setting": 1}),
                reviewed("empty_value.json", {"model_api_hai_types": ""}),
                reviewed("bad_value.json", {"model_api_hai_types": "HAI_9"}),
                reviewed("repeated_type.json", {"model_api_hai_types": "HAI_1,HAI_1"}),
                written("null_value.json", json.dumps({**envelope, "variables": {"model_api_hai_types": None}})),
                written("array.json", "[]"),
                Path(folder) / "absent.json",
            )
        ]
        duplicate_refused = [refusal_of(functools.partial(tool.served_types, None, path, {})) for path in (duplicate_setting, duplicate_envelope)]
        from_flag = tool.served_types("HAI_3", inputs, {"MODEL_API_HAI_TYPES": "HAI_4"})
        from_environment = tool.served_types(None, None, {"MODEL_API_HAI_TYPES": "HAI_4,HAI_5"})
        bad_inputs: list[tuple[str | None, Path | None, dict[str, str]]] = [(None, None, {}), ("HAI_9", None, {}), ("HAI_1,HAI_1", None, {}), ("", None, {})]
        types_refused = [refusal_of(functools.partial(tool.served_types, *arguments)) for arguments in bad_inputs]
    tool_source = Path(tool.__file__).read_text()
    return {
        "the first run exits 0 and creates the MLflow user with the stored credential": first_code == 0
        and first_users == [served]
        and bool(credential)
        and password_sent,
        "READ is granted on each existing served model and experiment, the experiment by its ID": first_grants
        == {grant_model_1: "READ", grant_experiment_1: "READ"},
        "a model or experiment that does not exist, or is deleted, is reported pending": all(f"{name} pending" in first for name in (model_2, experiment_2)),
        "the output names each granted object and ends with ready": f"READ on registered model {model_1} granted" in first
        and f"READ on experiment {experiment_1} granted" in first
        and "mlflow_user: ready" in first,
        "only the 3.17 routes and the lookups of the shared check were called": first_routes <= routes
        and ("POST", "/api/3.0/mlflow/users/permissions/grant") in first_routes,
        "the removed per-object permission routes were never called": removed_after_all == [] and old_route_answered,
        "the credential travels in a JSON body and never in an address": create_bodies == [("application/json", ("password", "username"))] and not in_address,
        "no output, log line or file holds the administrator password or the credential": bool(credential)
        and not any(secret in first + second + third + edit + deny + mismatch + stored + slow for secret in (admin, credential, other_password))
        and leaked == [],
        "a rerun exits 0, creates no user, changes no grant and says current and already": second_code == 0
        and rerun_quiet
        and f"user current {served}" in second
        and f"READ on registered model {model_1} already" in second,
        "after the first training run a rerun grants the missing objects and leaves the first grants": late_ok,
        "an existing grant of another permission is reported and left unchanged, with exit 0": edit_code == 0
        and edit_kept
        and f"EDIT on registered model {model_1} kept" in edit,
        "an explicit DENY refuses with exit 1, names the object, stays and is never reported ready": deny_code == 1
        and deny_kept
        and experiment_1 in deny
        and "DENY" in deny
        and "mlflow_user: ready" not in deny,
        "a server default of READ is accepted without any grant row and the account is ready": read_ok,
        "an existing user with another password than the stored credential exits 1, changes no password and no grant and is never reported ready": mismatch_ok,
        "the same refusal holds when the credential parameter already existed": stored_ok,
        "a slow answer body ends at the run deadline with exit 1 and no ready line": slow_ok,
        "an explicit DENY on a deleted experiment refuses with exit 1, names it, stays and is never reported ready": trash_ok,
        "a closing response that stalls after one byte, as HTTP/1.0 and HTTP/1.1, also after a slow store start, ends at its own deadline": stalls
        == [True] * 4,
        "a missing administrator parameter exits 1, names it and calls no MLflow route": no_admin_code == 1 and admin_name in no_admin and no_admin_quiet,
        "a wrong administrator password exits 1 with the status and without any password": wrong_code == 1 and wrong_text_clean,
        "an address off the loopback, with a user, or with another scheme exits 1 before the administrator parameter is read": len(refused)
        == len(refused_addresses)
        and all(refused),
        "an MLflow server that is not there exits 1 within the deadline and names the address": down_code == 1 and down_address.removeprefix("http://") in down,
        "an unsupported served type exits 1 before any request": bad_type_code == 1 and "HAI_9" in bad_type,
        "the served types come from the flag, then the services input file, then the environment": from_file == ("HAI_1", "HAI_2")
        and from_flag == ("HAI_3",)
        and from_environment == ("HAI_4", "HAI_5"),
        "no served type, an unknown one, a repeat and an empty list are refused": all(message is not None for message in types_refused),
        "the services input file is read under variables; a flat or invalid file, a bad value, bad JSON and a missing file are refused": from_file
        == ("HAI_1", "HAI_2")
        and all(message is not None for message in file_refused),
        "a reviewed file without the setting serves the default of the services stack": tuple(default_text.split(",")) == from_empty == from_other
        and default_text == "HAI_1",
        "a repeated JSON member, in the variables or in the envelope, is refused as the deployment reader refuses it, without echoing a value": all(
            message is not None and "duplicate" in message and "HAI_2" not in message for message in duplicate_refused
        ),
        "the tool holds no account or grant logic of its own": not hasattr(tool, "MlflowAuth")
        and "users/permissions" not in tool_source
        and "users/create" not in tool_source
        and "http.client" not in tool_source,
    }


def slow_open(open_store: Callable[..., Any], seconds: float) -> Callable[..., Any]:
    """``secret_store.open_store`` after a pause, as a slow start of the store before the account check."""

    def opened(*arguments: Any, **options: Any) -> Any:
        time.sleep(seconds)
        return open_store(*arguments, **options)

    return opened


def refusal_of(call: Callable[[], Any]) -> str | None:
    """The text of the tool's refusal, or ``None`` when the call succeeds."""
    from scripts.infrastructure import create_serving_secrets as tool

    try:
        call()
    except tool.ServingSecretsError as error:
        return str(error)
    return None


def old_route_status(url: str, admin: str) -> tuple[int, bool]:
    """The status and HTML flag of one call to a per-object permission route that MLflow 3.17.2 removed, so a case knows the stand-in would show a call."""
    import http.client

    token = base64.b64encode(f"admin:{admin}".encode()).decode()
    connection = http.client.HTTPConnection("127.0.0.1", int(url.rpartition(":")[2]), timeout=5)
    try:
        connection.request("GET", "/api/2.0/mlflow/registered-models/permissions/get?name=x", headers={"Authorization": "Basic " + token})
        answer = connection.getresponse()
        return answer.status, answer.read().startswith(b"<!doctype html>")
    finally:
        connection.close()


def release_pipe(pipe: Path, seconds: float = 5.0) -> None:
    """Open the write end of a pipe so that a reader blocked on it returns; give up after ``seconds`` when no reader is waiting."""
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        try:
            os.close(os.open(pipe, os.O_WRONLY | os.O_NONBLOCK))
            return
        except OSError:
            time.sleep(0.05)


def scan_with_pipe(root: Path) -> dict[str, bool]:
    """A scan for a secret value meets a named pipe that nobody writes to, ends at its deadline without opening it and still reads the files beside it."""
    needle = "pipe-" + os.urandom(8).hex()
    found: list[list[str]] = []
    with tempfile.TemporaryDirectory(prefix="pipe_scan_cases_") as scratch:
        folder = Path(scratch)
        (folder / "holds.txt").write_text(needle)
        os.mkfifo(folder / "pipe")
        scan = threading.Thread(target=lambda: found.append(files_holding([needle], [folder], 0.0)), name="pipe-scan", daemon=True)
        scan.start()
        scan.join(timeout=5)
        ended = not scan.is_alive()
        if not ended:
            release_pipe(folder / "pipe")
            scan.join(timeout=5)
        holder = str(folder / "holds.txt")
    return {
        "a scan ends at its deadline although the folder holds a named pipe with no writer": ended,
        "the scan still reads the regular file beside the pipe": found == [[holder]],
    }


def grown_size(real: Callable[[int], os.stat_result], small: int) -> Callable[[int], os.stat_result]:
    """An ``os.fstat`` that tells a regular file's size as ``small`` bytes: the file then grows past it between the look and the read."""

    def fstat(descriptor: int) -> os.stat_result:
        info = real(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_size <= small:
            return info
        fields = list(tuple(info))
        fields[stat.ST_SIZE] = small
        return os.stat_result(tuple(fields))

    return fstat


def scan_coverage(root: Path) -> dict[str, bool]:
    """A scan for a secret value reads a file of any size, names a link it cannot cover and bounds every read; a Terraform input that is no file is refused."""
    needle = "cover-" + os.urandom(8).hex()
    mebibyte = 1024 * 1024
    with tempfile.TemporaryDirectory(prefix="scan_coverage_cases_") as scratch, tempfile.TemporaryDirectory(prefix="scan_outside_cases_") as outside:
        folder, elsewhere = Path(scratch), Path(outside)
        began = time.time() - 1
        large = folder / "large.bin"
        with large.open("wb") as handle:
            handle.seek(9 * mebibyte)
            handle.write(needle.encode())
        straddling = folder / "straddling.bin"
        with straddling.open("wb") as handle:
            handle.seek(mebibyte - len(needle) // 2)
            handle.write(needle.encode())
            handle.seek(3 * mebibyte)
            handle.write(b"end")
        (elsewhere / "held.txt").write_text(needle)
        (folder / "link_to_file").symlink_to(elsewhere / "held.txt")
        (folder / "link_to_folder").symlink_to(elsewhere, target_is_directory=True)
        (folder / "inside_target.txt").write_text("nothing")
        (folder / "link_inside").symlink_to(folder / "inside_target.txt")
        growing = Path(scratch) / "growing"
        growing.mkdir()
        (growing / "grows.bin").write_bytes(b"x" * (2 * mebibyte) + needle.encode())
        names = {"large": str(large), "straddling": str(straddling)}
        found = files_holding([needle], [folder], began)
        with mock.patch("os.fstat", grown_size(os.fstat, 10)):
            grown = files_holding([needle], [growing], began)
        late = files_holding([needle], [folder], time.time() + 1)
        unbounded = files_holding([needle], [folder], began, seconds=0.0)
        pipe_tree = Path(scratch) / "tf_tree"
        (pipe_tree / "infra").mkdir(parents=True)
        os.mkfifo(pipe_tree / "infra" / "main.tf")
        offenders: list[list[str]] = []
        reader = threading.Thread(target=lambda: offenders.append(source_offenders(pipe_tree)), name="tf-pipe", daemon=True)
        reader.start()
        reader.join(timeout=5)
        reader_ended = not reader.is_alive()
        if not reader_ended:
            release_pipe(pipe_tree / "infra" / "main.tf")
            reader.join(timeout=5)
        listed = offenders[0] if offenders else []
    return {
        "a regular file over 8 MiB that holds the value is found": names["large"] in found,
        "a value across a read boundary of a large file is found": names["straddling"] in found,
        "a link to a file outside the scan roots is named as not covered": any("link_to_file" in item and "not covered" in item for item in found),
        "a link to a folder outside the scan roots is named as not covered": any("link_to_folder" in item and "not covered" in item for item in found),
        "a link to a file inside the scan roots is no report": not any("link_inside" in item for item in found),
        "a link older than the run's start is no report": not any("not covered" in item for item in late),
        "a file that grew past the size it had at its look is reported as changed": any("grows.bin" in item and "changed" in item for item in grown),
        "a scan past its deadline reports itself unfinished": any("not finished" in item for item in unbounded),
        "a scan inside its deadline reports nothing unfinished": not any("not finished" in item for item in found),
        "a Terraform input that is a named pipe ends the source check at its deadline": reader_ended,
        "a Terraform input that is a named pipe is an offender, not skipped": listed == ["infra/main.tf (not a regular file)"],
    }


CASES: dict[str, Case] = {
    "store_behaviour": store_behaviour,
    "store_races": store_races,
    "store_deadline": store_deadline,
    "terraform_names": terraform_names,
    "saved_plan": saved_plan,
    "openmetadata_passwords": openmetadata_passwords,
    "model_api_redis_password": model_api_redis_password,
    "redis_exporter_password": redis_exporter_password,
    "postgres_exporter_password": postgres_exporter_password,
    "secret_writes_stay_out_of_debug_logs": secret_writes_stay_out_of_debug_logs,
    "serving_secrets_store": serving_secrets_store,
    "serving_secrets_mlflow": serving_secrets_mlflow,
    "scan_with_pipe": scan_with_pipe,
    "scan_coverage": scan_coverage,
}


def main(argv: list[str] | None = None) -> int:
    """Run every case, print one line per check and write nothing."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--root", type=Path, default=REPO_ROOT, help="the repository folder to check")
    parser.add_argument("--json", action="store_true", help="print the results as JSON")
    arguments = parser.parse_args(sys.argv[1:] if argv is None else argv)
    results = {name: function(arguments.root) for name, function in CASES.items()}
    if arguments.json:
        sys.stdout.write(json.dumps(results, indent=2, sort_keys=True) + "\n")
    else:
        for name, checks in results.items():
            for check, ok in checks.items():
                sys.stdout.write(f"{'PASS' if ok else 'FAIL'} {name}: {check}\n")
    return 0 if all(all(checks.values()) for checks in results.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
