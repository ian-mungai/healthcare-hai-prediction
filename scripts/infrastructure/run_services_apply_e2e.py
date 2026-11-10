"""Check the launch hold steps of ``scripts.infrastructure.services_apply`` offline.

Launch hold during services applies (owner A, Nov 10 2026): every services apply sets a launch hold before it changes anything, the hold is cleared only
after the ECS services reach steady state, and it stays set after a failed apply until the owner resolves it. The cases drive ``services_apply.Steps``
with a stand-in parameter store, a stand-in steady-state check and a virtual clock, in process. No AWS call, no Terraform run and no process start is
made. The order around the real ``terraform.sh --services apply`` (hold, apply, settle; a failed apply keeps the hold; an unset hold stops the apply)
is checked by the services cases of ``scripts.infrastructure.run_e2e``.

Run from the repository root: ``.venv/bin/python -m scripts.infrastructure.run_services_apply_e2e``. It prints one line per case and exits 1 when a case
fails.
"""

from __future__ import annotations

import argparse
import contextlib
import importlib
import io
import json
import re
import sys
import traceback
from collections.abc import Callable
from pathlib import Path
from typing import Any
from unittest import mock

from scripts.process import CompletedProcess

ROOT = Path(__file__).resolve().parents[2]
HOLD = "/healthcare_hai_prediction/host_launch_hold"
LOCK = "/healthcare_hai_prediction/services_apply_lock"
OWN_LOCK = "this-host:1:20261110T221000Z:12345678"  # an apply lock value in the token shape, not a credential
TOKEN = re.compile(r"[A-Za-z0-9._-]+:[0-9]+:[0-9]{8}T[0-9]{6}Z:[0-9a-f]{8}")


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


class Store:
    """The hold parameter: every put and read recorded with the virtual time."""

    def __init__(self, clock: Clock, value: str | None = None, fail_put: bool = False) -> None:
        self.clock, self.value, self.fail_put = clock, value, fail_put
        self.log: list[tuple[float, str, str | None]] = []

    def put(self, value: str) -> None:
        self.log.append((self.clock.now, "put", value))
        if self.fail_put:
            raise RuntimeError("stand-in put refused")
        self.value = value

    def get(self) -> str | None:
        self.log.append((self.clock.now, "get", self.value))
        return self.value


def steps(store: Store, clock: Clock, stable: Callable[[], bool]) -> Any:
    module = importlib.import_module("scripts.infrastructure.services_apply")
    require(hasattr(module, "Steps"), "services_apply has no Steps that a later GitHub Actions apply can reuse")
    lines: list[str] = []
    return module.Steps(put=store.put, get=store.get, stable=stable, clock=clock, report=lines.append), module


def hold_is_written_and_settles_before_the_apply() -> None:
    """hold writes a held value and verifies it, then waits the settle window, so a launch that read a clear hold just before has sent its
    RunTask before the apply changes anything."""
    clock = Clock()
    store = Store(clock, "clear")
    current, module = steps(store, clock, lambda: True)
    start = clock.now
    current.hold("terraform.sh --services apply reviewed.tfplan")
    puts = [entry for entry in store.log if entry[1] == "put"]
    require(len(puts) == 1 and str(puts[0][2]).startswith("held") and "reviewed.tfplan" in str(puts[0][2]), f"the hold was not written: {store.log}")
    require(any(entry[1] == "get" and entry[2] == puts[0][2] for entry in store.log), f"the hold was not read back: {store.log}")
    require(clock.now - start >= module.HOLD_SETTLE_SECONDS >= 30, f"hold returned before the settle window: {clock.now - start} s")


def settle_clears_only_after_steady_state() -> None:
    """settle polls until every service is stable, then writes clear and reads it back. A launch during the wait still sees the hold."""
    clock = Clock()
    store = Store(clock, "held by services_apply")
    answers = iter([False, False, True])
    current, _ = steps(store, clock, lambda: next(answers))
    current.settle()
    puts = [entry for entry in store.log if entry[1] == "put"]
    require([entry[2] for entry in puts] == ["clear"] and store.value == "clear", f"settle did not clear the hold once: {store.log}")
    require(puts[0][0] > 1000.0, "settle cleared the hold before polling the services")


def settle_that_never_reaches_steady_state_keeps_the_hold() -> None:
    """When the services stay unstable past the deadline, or a rollout fails, settle raises and the hold stays."""
    for stable in (lambda: False, lambda: (_ for _ in ()).throw(RuntimeError("service airflow rollout FAILED"))):
        clock = Clock()
        store = Store(clock, "held by services_apply")
        current, module = steps(store, clock, stable)
        try:
            current.settle()
        except module.ServicesApplyError as error:
            require("launch hold stays" in str(error), f"the failure does not say the hold stays: {error}")
        else:
            raise AssertionError("settle returned without steady state")
        require(store.value == "held by services_apply" and not [entry for entry in store.log if entry[1] == "put"], f"the hold was changed: {store.log}")
        require(clock.now - 1000.0 <= module.STABLE_SECONDS + 60, f"settle waited past its bound: {clock.now - 1000.0} s")


def resolve_needs_a_statement_and_steady_state() -> None:
    """resolve, after a failed apply: an empty statement or unstable services leave the hold; with a statement and stable services it clears it."""
    clock = Clock()
    store = Store(clock, "held by services_apply")
    current, module = steps(store, clock, lambda: False)
    for statement in ("", "   "):
        try:
            current.resolve(statement)
        except module.ServicesApplyError:
            pass
        else:
            raise AssertionError("resolve cleared the hold without a statement")
    try:
        current.resolve("checked the failed apply and the services")
    except module.ServicesApplyError:
        pass
    else:
        raise AssertionError("resolve cleared the hold while the services were not stable")
    require(store.value == "held by services_apply", f"resolve changed the hold before it was allowed: {store.log}")
    current, _ = steps(store, clock, lambda: True)
    current.resolve("checked the failed apply and the services")
    require(store.value == "clear", f"resolve did not clear the hold: {store.log}")


def a_hold_that_cannot_be_written_raises() -> None:
    """A refused put raises, so terraform.sh stops before the apply."""
    clock = Clock()
    store = Store(clock, "clear", fail_put=True)
    current, module = steps(store, clock, lambda: True)
    try:
        current.hold("terraform.sh --services apply reviewed.tfplan")
    except module.ServicesApplyError:
        return
    raise AssertionError("a refused hold write did not raise")


def the_hold_name_is_the_terraform_default() -> None:
    """The command writes the parameter the services stack names in the Airflow environment and the IAM stack grants."""
    module = importlib.import_module("scripts.infrastructure.services_apply")
    pattern = r'variable "host_launch_hold_parameter_name" \{.*?default\s*=\s*"([^"]+)"'
    for path in ("infra/services/variables.tf", "infra/iam/airflow_launch.tf"):
        match = re.search(pattern, (ROOT / path).read_text(), re.DOTALL)
        require(match is not None and match.group(1) == HOLD, f"{path} does not default the hold name to {HOLD}")
    require(module.hold_parameter_name() == HOLD, f"the command writes {module.hold_parameter_name()}, not {HOLD}")


SETTINGS = {
    "aws_profile": "example_project_dev",
    "aws_region": "eu-west-1",
    "data_bucket_name": "example-project-data-dev",
    "project_name": "example_project",
    "environment": "dev",
}


class AwsStandIn:
    """The AWS command line the real module runs through ``AwsCli``: Parameter Store put and get of one value, every command recorded, any other
    command refused."""

    def __init__(self, value: str | None = None, lock: str | None = None) -> None:
        self.value = value
        self.lock = lock
        self.commands: list[list[str]] = []

    def __call__(self, command: list[str], **_: Any) -> CompletedProcess[str]:
        self.commands.append(list(command))
        operation = command[1:3]
        name = command[command.index("--name") + 1] if "--name" in command else ""
        if name == LOCK:
            return self.lock_call(command, operation)
        if operation == ["ssm", "put-parameter"] and name == HOLD:
            self.value = command[command.index("--value") + 1]
            return CompletedProcess(command, 0, json.dumps({"Version": 1}), "")
        if operation == ["ssm", "get-parameter"] and name == HOLD:
            if self.value is None:
                return CompletedProcess(command, 254, "", "An error occurred (ParameterNotFound) when calling the GetParameter operation")
            return CompletedProcess(command, 0, json.dumps({"Parameter": {"Name": HOLD, "Value": self.value}}), "")
        return CompletedProcess(command, 1, "", "An error occurred (AccessDenied) when calling an unexpected operation")

    def lock_call(self, command: list[str], operation: list[str]) -> CompletedProcess[str]:
        """The apply lock as AWS keeps it: a put without --overwrite creates it or fails with ParameterAlreadyExists, and an overwrite is refused
        here so a lock is never replaced."""
        if operation == ["ssm", "put-parameter"]:
            if "--overwrite" in command:
                return CompletedProcess(command, 1, "", "An error occurred (AccessDenied) when calling the PutParameter operation")
            if self.lock is not None:
                return CompletedProcess(command, 254, "", "An error occurred (ParameterAlreadyExists) when calling the PutParameter operation")
            self.lock = command[command.index("--value") + 1]
            return CompletedProcess(command, 0, json.dumps({"Version": 1}), "")
        if operation == ["ssm", "get-parameter"]:
            if self.lock is None:
                return CompletedProcess(command, 254, "", "An error occurred (ParameterNotFound) when calling the GetParameter operation")
            return CompletedProcess(command, 0, json.dumps({"Parameter": {"Name": LOCK, "Value": self.lock}}), "")
        if operation == ["ssm", "delete-parameter"]:
            if self.lock is None:
                return CompletedProcess(command, 254, "", "An error occurred (ParameterNotFound) when calling the DeleteParameter operation")
            self.lock = None
            return CompletedProcess(command, 0, "", "")
        return CompletedProcess(command, 1, "", "An error occurred (AccessDenied) when calling an unexpected operation")


def run_real(arguments: list[str], aws: AwsStandIn) -> tuple[int, list[str]]:
    """Run the real ``services_apply.main`` on a fresh stack: the configuration is synthetic, the services stack has no outputs yet (reading them
    fails), the AWS command line is the stand-in and the settle wait does not sleep. Returns the exit code and the services output reads."""
    module = importlib.import_module("scripts.infrastructure.services_apply")
    session = importlib.import_module("scripts.infrastructure.host_session")
    config = importlib.import_module("scripts.infrastructure.render_project_config")
    output_reads: list[str] = []

    def no_outputs(*_: Any, **__: Any) -> Any:
        output_reads.append("load_names")
        raise config.ConfigurationError("The Terraform output service_names is missing or has the wrong type.")

    patches = [
        mock.patch.object(session, "load_names", no_outputs),
        mock.patch.object(session, "load_configuration", lambda *_, **__: (dict(SETTINGS), {})),
        mock.patch.object(session, "run_argv", aws),  # under the real tolerant_runner, as the AWS command line answers
        mock.patch.object(module.Clock, "sleep", staticmethod(lambda _seconds: None)),
    ]
    for name, value in (("load_configuration", lambda *_, **__: (dict(SETTINGS), {})),):
        if hasattr(module, name):
            patches.append(mock.patch.object(module, name, value))
    for patch in patches:
        patch.start()
    try:
        try:
            return module.main(arguments), output_reads
        except SystemExit as error:  # argparse refuses an unknown command or option with exit 2
            return int(error.code or 0), output_reads
    finally:
        for patch in reversed(patches):
            patch.stop()


def hold_on_a_fresh_stack_needs_no_services_outputs() -> None:
    """The first services apply on a fresh stack: the real ``hold`` writes and reads back the hold through Parameter Store alone. It reads no
    services output, which does not exist yet."""
    aws = AwsStandIn()
    code, output_reads = run_real(["hold", "--reason", "terraform.sh --services apply reviewed.tfplan"], aws)
    require(code == 0, f"hold failed on a fresh stack (exit {code}; services output reads {output_reads})")
    require(not output_reads, f"hold read the services outputs: {output_reads}")
    operations = [command[1:3] for command in aws.commands]
    require(operations == [["ssm", "put-parameter"], ["ssm", "get-parameter"]], f"hold sent other AWS calls: {operations}")
    put = aws.commands[0]
    require("--overwrite" in put and put[put.index("--type") + 1] == "String", f"the hold is not an overwriting String put: {put}")
    require(str(aws.value).startswith("held by services_apply") and "reviewed.tfplan" in str(aws.value), f"the hold value is wrong: {aws.value}")
    require(
        all("--profile" in command and command[command.index("--profile") + 1] == SETTINGS["aws_profile"] for command in aws.commands),
        "a call left the project profile",
    )


def settle_on_a_fresh_stack_keeps_the_hold() -> None:
    """settle and resolve need the services outputs to check steady state. Without them they fail as a configuration error and write nothing, so
    the hold stays."""
    for arguments in (["settle", "--token", OWN_LOCK], ["resolve", "--statement", "checked", "--token", OWN_LOCK]):
        aws = AwsStandIn("held by services_apply", lock=OWN_LOCK)
        code, output_reads = run_real(arguments, aws)
        require(code == 2 and output_reads, f"{arguments[0]} did not fail on the missing services outputs: exit {code}, reads {output_reads}")
        require(
            aws.value == "held by services_apply" and not any(command[1:3] == ["ssm", "put-parameter"] for command in aws.commands),
            f"{arguments[0]} changed the hold",
        )


def run_real_output(arguments: list[str], aws: AwsStandIn) -> tuple[int, str, str]:
    """``run_real`` with the printed output and error text."""
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        code, _ = run_real(arguments, aws)
    return code, out.getvalue(), err.getvalue()


def lock_is_created_once_with_a_unique_token() -> None:
    """Apply lock (owner A, Nov 10 2026): ``lock`` creates the lock parameter without overwrite, its value a unique token (host, process ID, UTC
    time, random part), and prints only the token."""
    aws = AwsStandIn()
    code, out, err = run_real_output(["lock", "--reason", "terraform.sh --services apply reviewed.tfplan"], aws)
    require(code == 0, f"lock failed on a free lock: exit {code} {err}")
    require(TOKEN.fullmatch(out.strip() or "") is not None and aws.lock == out.strip(), f"the token is not printed or stored: {out!r} {aws.lock!r}")
    puts = [command for command in aws.commands if command[1:3] == ["ssm", "put-parameter"]]
    require(len(puts) == 1 and "--overwrite" not in puts[0], f"the lock was not created once without overwrite: {puts}")


def a_second_apply_is_refused_while_the_lock_exists() -> None:
    """While another apply holds the lock, ``lock`` exits 1, names the holder's token and changes nothing."""
    holder = "other-host:4242:20261110T220000Z:0badc0de"
    aws = AwsStandIn(lock=holder)
    code, out, err = run_real_output(["lock", "--reason", "terraform.sh --services apply reviewed.tfplan"], aws)
    require(code == 1 and not out.strip(), f"a second apply was not refused: exit {code} {out!r}")
    require(holder in err, f"the refusal does not name the holder: {err}")
    require(aws.lock == holder and aws.value is None, "the refused lock changed the lock or the hold")


def settle_never_deletes_another_token() -> None:
    """settle with a token that does not hold the lock changes nothing: the hold stays set and the other lock stays."""
    holder = "other-host:4242:20261110T220000Z:0badc0de"
    aws = AwsStandIn(value="held by services_apply", lock=holder)
    code, _, err = run_real_output(["settle", "--token", "this-host:1:20261110T221000Z:12345678"], aws)
    require(code == 1, f"settle with another token did not fail: exit {code} {err}")
    require(aws.lock == holder and aws.value == "held by services_apply", "settle changed another apply's lock or the hold")
    require(not any(command[1:3] == ["ssm", "delete-parameter"] for command in aws.commands), "settle sent a delete for another token")


def settle_deletes_its_own_lock_after_clearing_the_hold() -> None:
    """settle with the lock's own token clears the hold after steady state, then deletes the lock."""
    module = importlib.import_module("scripts.infrastructure.services_apply")
    mine = OWN_LOCK
    aws = AwsStandIn(value="held by services_apply", lock=mine)
    with mock.patch.object(module.Steps, "wait_stable", lambda self: None):
        code, _, err = run_real_output(["settle", "--token", mine], aws)
    require(code == 0, f"settle with its own token failed: exit {code} {err}")
    require(aws.value == "clear" and aws.lock is None, f"settle did not clear the hold and delete its lock: {aws.value!r} {aws.lock!r}")
    order = [command[2] for command in aws.commands if command[1:3] in (["ssm", "put-parameter"], ["ssm", "delete-parameter"])]
    require(order == ["put-parameter", "delete-parameter"], f"the lock was not deleted after the hold was cleared: {order}")


def resolve_clears_the_hold_and_the_lock_with_its_token() -> None:
    """After a failed apply both stay. resolve without the token, or with another one, changes nothing; with the lock's token and stable
    services it clears the hold and deletes the lock."""
    module = importlib.import_module("scripts.infrastructure.services_apply")
    mine = OWN_LOCK
    for extra in ([], ["--token", "other-host:2:20261110T221000Z:87654321"]):
        aws = AwsStandIn(value="held by services_apply", lock=mine)
        with mock.patch.object(module.Steps, "services_settled", lambda self: True, create=True):
            code, _, _ = run_real_output(["resolve", "--statement", "checked", *extra], aws)
        require(code != 0 and aws.lock == mine and aws.value == "held by services_apply", f"resolve {extra} changed the hold or the lock")
    aws = AwsStandIn(value="held by services_apply", lock=mine)
    with mock.patch.object(module.Steps, "services_settled", lambda self: True, create=True):
        code, _, err = run_real_output(["resolve", "--statement", "checked the failed apply", "--token", mine], aws)
    require(code == 0 and aws.value == "clear" and aws.lock is None, f"resolve with the token did not clear both: exit {code} {err}")


def the_lock_name_is_the_terraform_default() -> None:
    """The command creates the parameter the IAM stack grants (infra/iam/airflow_launch.tf services_apply_lock_parameter_name)."""
    module = importlib.import_module("scripts.infrastructure.services_apply")
    match = re.search(
        r'variable "services_apply_lock_parameter_name" \{.*?default\s*=\s*"([^"]+)"', (ROOT / "infra/iam/airflow_launch.tf").read_text(), re.DOTALL
    )
    require(match is not None and match.group(1) == LOCK, f"infra/iam/airflow_launch.tf does not default the lock name to {LOCK}")
    require(getattr(module, "lock_parameter_name", lambda: None)() == LOCK, "the command does not create the lock the IAM stack grants")


CASES: dict[str, Callable[[], None]] = {
    "lock_is_created_once_with_a_unique_token": lock_is_created_once_with_a_unique_token,
    "a_second_apply_is_refused_while_the_lock_exists": a_second_apply_is_refused_while_the_lock_exists,
    "settle_never_deletes_another_token": settle_never_deletes_another_token,
    "settle_deletes_its_own_lock_after_clearing_the_hold": settle_deletes_its_own_lock_after_clearing_the_hold,
    "resolve_clears_the_hold_and_the_lock_with_its_token": resolve_clears_the_hold_and_the_lock_with_its_token,
    "the_lock_name_is_the_terraform_default": the_lock_name_is_the_terraform_default,
    "hold_on_a_fresh_stack_needs_no_services_outputs": hold_on_a_fresh_stack_needs_no_services_outputs,
    "settle_on_a_fresh_stack_keeps_the_hold": settle_on_a_fresh_stack_keeps_the_hold,
    "hold_is_written_and_settles_before_the_apply": hold_is_written_and_settles_before_the_apply,
    "settle_clears_only_after_steady_state": settle_clears_only_after_steady_state,
    "settle_that_never_reaches_steady_state_keeps_the_hold": settle_that_never_reaches_steady_state_keeps_the_hold,
    "resolve_needs_a_statement_and_steady_state": resolve_needs_a_statement_and_steady_state,
    "a_hold_that_cannot_be_written_raises": a_hold_that_cannot_be_written_raises,
    "the_hold_name_is_the_terraform_default": the_hold_name_is_the_terraform_default,
}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", choices=sorted(CASES))
    arguments = parser.parse_args()
    passed = failed = 0
    for name, case in CASES.items():
        if arguments.case and name != arguments.case:
            continue
        try:
            case()
        except Exception as error:  # every case failure is reported, whatever its type
            failed += 1
            sys.stdout.write(f"FAIL {name}: {type(error).__name__}: {error}\n")
            traceback.print_exc(limit=2, file=sys.stderr)
        else:
            passed += 1
            sys.stdout.write(f"ok {name}\n")
    sys.stdout.write(f"{passed} passed, {failed} failed\n")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
