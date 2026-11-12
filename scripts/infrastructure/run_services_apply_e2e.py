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
import hashlib
import importlib
import io
import json
import re
import sys
import tempfile
import traceback
from collections.abc import Callable, Iterator, Mapping
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
        self.lock: Any = None  # the apply lock the steps use, set by steps()

    def put(self, value: str) -> None:
        self.log.append((self.clock.now, "put", value))
        if self.fail_put:
            raise RuntimeError("stand-in put refused")
        self.value = value

    def get(self) -> str | None:
        self.log.append((self.clock.now, "get", self.value))
        return self.value


class MemoryLock:
    """The apply lock in memory: create fails while it exists, as AWS answers ParameterAlreadyExists."""

    def __init__(self, value: str | None = None) -> None:
        self.value = value

    def create(self, value: str) -> None:
        module = importlib.import_module("scripts.infrastructure.services_apply")
        if self.value is not None:
            raise module.LockHeld
        self.value = value

    def get(self) -> str | None:
        return self.value

    def delete(self) -> None:
        self.value = None


MIGRATION = "held for a host job cap change"
FINISH = "services_apply finish"


def steps(store: Store, clock: Clock, stable: Callable[[], bool], lock: MemoryLock | None = None, registered: Callable[[], str | None] | None = None) -> Any:
    """Steps over the stand-in hold and an apply lock held by this apply's token (``OWN_LOCK``) unless another lock is given."""
    module = importlib.import_module("scripts.infrastructure.services_apply")
    require(hasattr(module, "Steps"), "services_apply has no Steps that a later GitHub Actions apply can reuse")
    lines: list[str] = []
    held = MemoryLock(OWN_LOCK) if lock is None else lock
    store.lock = held
    store_lock = module.LockStore(create=held.create, get=held.get, delete=held.delete)
    return module.Steps(put=store.put, get=store.get, stable=stable, clock=clock, report=lines.append, lock=store_lock, registered=registered), module


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
    current.settle(OWN_LOCK)
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
            current.settle(OWN_LOCK)
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
            current.resolve(statement, OWN_LOCK)
        except module.ServicesApplyError:
            pass
        else:
            raise AssertionError("resolve cleared the hold without a statement")
    try:
        current.resolve("checked the failed apply and the services", OWN_LOCK)
    except module.ServicesApplyError:
        pass
    else:
        raise AssertionError("resolve cleared the hold while the services were not stable")
    require(store.value == "held by services_apply", f"resolve changed the hold before it was allowed: {store.log}")
    current, _ = steps(store, clock, lambda: True)
    current.resolve("checked the failed apply and the services", OWN_LOCK)
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

    patches: list[Any] = [
        mock.patch.object(session, "load_names", no_outputs),
        mock.patch.object(session, "load_configuration", lambda *_, **__: (dict(SETTINGS), {})),
        mock.patch.object(session, "run_argv", aws),  # under the real tolerant_runner, as the AWS command line answers
        mock.patch.object(module.Clock, "sleep", staticmethod(lambda _seconds: None)),
    ]
    if hasattr(module, "load_configuration"):
        patches.append(mock.patch.object(module, "load_configuration", lambda *_, **__: (dict(SETTINGS), {})))
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
    require(operations == [["ssm", "get-parameter"], ["ssm", "put-parameter"], ["ssm", "get-parameter"]], f"hold sent other AWS calls: {operations}")
    put = aws.commands[1]
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
        require(code == 2 and bool(output_reads), f"{arguments[0]} did not fail on the missing services outputs: exit {code}, reads {output_reads}")
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
    with mock.patch.object(module.Steps, "wait_stable", lambda self, token: None):
        code, _, err = run_real_output(["settle", "--token", mine], aws)
    require(code == 0, f"settle with its own token failed: exit {code} {err}")
    require(aws.value == "clear" and aws.lock is None, f"settle did not clear the hold and delete its lock: {aws.value!r} {aws.lock!r}")
    order = [command[2] for command in aws.commands if command[1:3] in (["ssm", "put-parameter"], ["ssm", "delete-parameter"])]
    require(order == ["put-parameter", "delete-parameter"], f"the lock was not deleted after the hold was cleared: {order}")


def pending_migration() -> tuple[Store, Clock, Any, list[str]]:
    """The state after ``settle`` kept the hold for a host job cap change: the hold names the change, the lock is this apply's, the jobs apply is pending.
    ``answers`` is what the registration check says, newest last; it starts with the jobs apply not done."""
    clock = Clock()
    store = Store(clock, "held by services_apply")
    answers = ["family silver_host is registered at 23552 MiB, the services give a job 22528 MiB"]
    current, _ = steps(store, clock, lambda: True, registered=lambda: answers[-1] if answers[-1] else None)
    current.settle(OWN_LOCK, keep_hold=True)
    return store, clock, current, answers


def settle_keeps_a_hold_that_only_the_jobs_registration_ends() -> None:
    """``settle`` with keep_hold waits for steady state and then rewrites the hold to the host job cap change form and keeps the lock."""
    clock = Clock()
    store = Store(clock, "held by services_apply")
    answers = iter([False, True])
    current, _ = steps(store, clock, lambda: next(answers))
    current.settle(OWN_LOCK, keep_hold=True)
    require(clock.now > 1000.0, "settle with keep_hold returned before polling the services")
    require(str(store.value).startswith(MIGRATION), f"the kept hold does not name the host job cap change: {store.value!r}")
    require(store.lock.value == OWN_LOCK, "settle with keep_hold released the apply lock")


def no_plain_path_clears_a_pending_migration_hold() -> None:
    """Plain settle, resolve with the token and resolve without one all refuse while the jobs apply is pending: the hold and the lock stay, the
    resolve without a token does not even take the lock, and every refusal prints the finish command with the lock's token."""
    module = importlib.import_module("scripts.infrastructure.services_apply")
    for label, call in (
        ("settle", lambda current: current.settle(OWN_LOCK)),
        ("resolve with the token", lambda current: current.resolve("services stable; jobs have not been applied", OWN_LOCK)),
        ("resolve without a token", lambda current: current.resolve("services stable; jobs have not been applied")),
    ):
        store, _, current, _ = pending_migration()
        held, writes = store.value, len([entry for entry in store.log if entry[1] == "put"])
        try:
            call(current)
        except module.ServicesApplyError as error:
            require(
                FINISH in str(error) and f"--token '{OWN_LOCK}'" in str(error), f"{label}: the refusal does not give the finish command with the token: {error}"
            )
        else:
            raise AssertionError(f"{label} cleared a pending host job cap change")
        require(store.value == held and store.lock.value == OWN_LOCK, f"{label}: the refusal changed the hold or the lock")
        require(len([entry for entry in store.log if entry[1] == "put"]) == writes, f"{label}: the refusal wrote the hold")


def finish_clears_the_hold_only_after_the_jobs_registration() -> None:
    """finish refuses while the registration check names a gap (the jobs apply not run, or failed), while the services are unstable, with another token,
    and when no migration is pending; it clears the hold and the lock once the registration check passes."""
    module = importlib.import_module("scripts.infrastructure.services_apply")
    store, clock, current, answers = pending_migration()
    for label, token in (("with the jobs apply not run", OWN_LOCK), ("with another token", "other-host:2:20261110T221000Z:87654321")):
        try:
            current.finish(token, "jobs applied at the new cap")
        except module.ServicesApplyError as error:
            if token == OWN_LOCK:
                require("registered at 23552 MiB" in str(error), f"finish {label} does not name the gap: {error}")
        else:
            raise AssertionError(f"finish {label} cleared the hold")
        require(str(store.value).startswith(MIGRATION) and store.lock.value == OWN_LOCK, f"finish {label} changed the hold or the lock")
    answers.append("family gold_host is registered at 23552 MiB, the services give a job 22528 MiB")  # a failed jobs apply left one family behind
    try:
        current.finish(OWN_LOCK, "jobs applied at the new cap")
    except module.ServicesApplyError as error:
        require("gold_host" in str(error), f"finish after a partly failed jobs apply does not name the family: {error}")
    else:
        raise AssertionError("finish cleared the hold after a failed jobs apply")
    unstable, _, again, _ = pending_migration()
    again.stable = lambda: False
    again.clock = Clock()
    try:
        again.finish(OWN_LOCK, "jobs applied at the new cap")
    except module.ServicesApplyError:
        pass
    else:
        raise AssertionError("finish cleared the hold while the services were unstable")
    require(str(unstable.value).startswith(MIGRATION), "finish with unstable services changed the hold")
    answers.append("")  # the jobs apply registered every host revision at the current services binding
    current.finish(OWN_LOCK, "jobs applied at the new cap")
    require(store.value == "clear" and store.lock.value is None, f"finish after the registration did not clear both: {store.value!r} {store.lock.value!r}")
    plain = Store(Clock(), "held by services_apply")
    other, _ = steps(plain, Clock(), lambda: True, registered=lambda: None)
    try:
        other.finish(OWN_LOCK, "jobs applied at the new cap")
    except module.ServicesApplyError as error:
        require("settle" in str(error) or "resolve" in str(error), f"finish without a pending change does not point to resolve: {error}")
    else:
        raise AssertionError("finish cleared a hold that is not a host job cap change")
    require(plain.value == "held by services_apply", "finish changed a hold that is not a host job cap change")
    unchecked = Store(Clock(), MIGRATION + " by services_apply at 2026-11-11T00:00:00Z: jobs apply pending")
    bare, _ = steps(unchecked, Clock(), lambda: True)
    try:
        bare.finish(OWN_LOCK, "jobs applied at the new cap")
    except module.ServicesApplyError:
        pass
    else:
        raise AssertionError("finish cleared the hold with no registration check configured")


def a_services_apply_can_continue_under_the_pending_lock() -> None:
    """A failed jobs apply may need the services restored or fixed: lock with the continue token returns the same token only while that token holds
    the lock and the hold is the host job cap change form. Otherwise it refuses and changes nothing. The hold of the continued apply is again the
    change form, so no plain settle or resolve can end it."""
    module = importlib.import_module("scripts.infrastructure.services_apply")
    store, _, current, _ = pending_migration()
    require(current.lock(continue_token=OWN_LOCK) == OWN_LOCK, "lock did not continue under the pending lock")
    current.hold("terraform.sh --services apply restore.tfplan", migration=True)
    require(str(store.value).startswith(MIGRATION), f"the continued hold is not the change form: {store.value!r}")
    for token in ("other-host:2:20261110T221000Z:87654321", "not a token"):
        try:
            current.lock(continue_token=token)
        except module.ServicesApplyError:
            pass
        else:
            raise AssertionError(f"lock continued with {token!r}")
    plain = Store(Clock(), "held by services_apply")
    other, _ = steps(plain, Clock(), lambda: True)
    try:
        other.lock(continue_token=OWN_LOCK)
    except module.ServicesApplyError:
        pass
    else:
        raise AssertionError("lock continued without a pending host job cap change")
    refused, _ = steps(Store(Clock(), MIGRATION + " by services_apply at 2026-11-11T00:00:00Z: x"), Clock(), lambda: True, lock=MemoryLock(OWN_LOCK))
    try:
        refused.lock()
    except module.ServicesApplyError as error:
        require(FINISH in str(error) and f"--token '{OWN_LOCK}'" in str(error), f"a second apply is not pointed to finish: {error}")
    else:
        raise AssertionError("a second apply took the lock during a pending host job cap change")


def registration_problem_reads_every_host_revision() -> None:
    """The real registration check: every active host family's latest revision must carry the current services binding on its init container and
    the job memory of that binding as its task memory; no host family at all, an inactive one, a missing label and a stale memory are each named."""
    module = importlib.import_module("scripts.infrastructure.services_apply")
    binding = "job_memory_mib=22528 usable_memory_mib=30720 system_reserve_mib=2048 service_reservations_mib=5120"
    old = binding.replace("22528", "23552")

    def definition(label: str | None, memory: str = "22528", status: str = "ACTIVE") -> dict[str, Any]:
        init = {"name": "init", "dockerLabels": {"hai.host_capacity": label} if label else {}}
        return {"status": status, "memory": memory, "containerDefinitions": [init, {"name": "job"}]}

    good = {"example_dev_job_silver_host": definition(binding), "example_dev_job_gold_host": definition(binding)}
    require(module.registration_problem(good, binding) is None, "a registered set at the current binding is refused")
    require("no host" in str(module.registration_problem({}, binding)), "no registered host family is not named")
    for label, bad, named in (
        ("a stale binding", {**good, "example_dev_job_gold_host": definition(old, "23552")}, "gold_host"),
        ("a missing label", {**good, "example_dev_job_gold_host": definition(None)}, "gold_host"),
        ("a stale task memory", {**good, "example_dev_job_silver_host": definition(binding, "23552")}, "silver_host"),
        ("an inactive revision", {**good, "example_dev_job_silver_host": definition(binding, status="INACTIVE")}, "silver_host"),
    ):
        problem = module.registration_problem(bad, binding)
        require(problem is not None and named in problem, f"{label} is not named: {problem}")


BINDING = "job_memory_mib=22528 usable_memory_mib=30720 system_reserve_mib=2048 service_reservations_mib=5120"
PENDING = MIGRATION + " by services_apply at 2026-11-11T00:00:00Z: jobs apply pending"
PREFIX = "example_project_dev_job_"
ALL_FAMILIES = ["bronze", "drift", "features", "gold", "metadata", "promote", "serve", "silver", "train"]


def host_revision(binding: str = BINDING, memory: str = "22528", status: str = "ACTIVE") -> dict[str, Any]:
    """The latest revision of a host family as ECS describes it: status, task memory and the capacity binding on the init container."""
    return {"status": status, "memory": memory, "containerDefinitions": [{"name": "init", "dockerLabels": {"hai.host_capacity": binding}}]}


@contextlib.contextmanager
def rendered_jobs_inputs(content: dict[str, Any] | None) -> Iterator[Path]:
    """The rendered jobs inputs file that the last jobs plan and apply wrote, in a temporary folder; ``None`` leaves it absent."""
    module = importlib.import_module("scripts.infrastructure.services_apply")
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "deployment.auto.tfvars.json"
        if content is not None:
            path.write_text(json.dumps(content))
        with mock.patch.object(module, "JOBS_INPUTS_RENDERED", path, create=True):
            yield path


FROZEN_MARK = " | frozen "


def frozen_suffix(families: list[str], binding: str | None = BINDING, project: str = "example_project", environment: str = "dev") -> str:
    """The frozen record the hold of a pending change carries after the reason: the selected host families, the digest of the reviewed jobs inputs, the
    project identity and, once the services applied, the capacity binding."""
    record: dict[str, Any] = {"jobs_sha256": "0" * 64, "families": sorted(families), "project": project, "environment": environment}
    if binding is not None:
        record["binding"] = binding
    return FROZEN_MARK + json.dumps(record, sort_keys=True, separators=(",", ":"))


def frozen_record(value: Any) -> dict[str, Any]:
    """The frozen record parsed from a hold value, or an assertion error."""
    text = str(value)
    require(FROZEN_MARK in text, f"the hold carries no frozen record: {text!r}")
    return dict(json.loads(text.rsplit(FROZEN_MARK, 1)[1]))


def rendered_values(selection: dict[str, Any], binding: str = BINDING, project: str = "example_project", environment: str = "dev") -> dict[str, Any]:
    """The jobs inputs file a render writes: the identity, the reviewed selection and, while host jobs or the publisher are on, the capacity binding."""
    values: dict[str, Any] = {"project_name": project, "environment": environment, **selection}
    if selection.get("host_jobs_enabled") is True or selection.get("publisher_job_enabled") is True:
        values["host_capacity_binding"] = binding
    return values


def reviewed_jobs_inputs(directory: Path, variables: dict[str, Any]) -> tuple[Path, str]:
    """The reviewed jobs preparation inputs in the file form the owner passes to the jobs stack, and the SHA-256 of its bytes."""
    path = directory / "jobs_inputs.json"
    path.write_text(json.dumps({"schema_version": 1, "root": "jobs", "variables": variables}, sort_keys=True) + "\n")
    return path, hashlib.sha256(path.read_bytes()).hexdigest()


def jobs_registration_reads_the_project_host_families() -> None:
    """The reader behind finish asks ECS for the active families of the project prefix and describes the latest revision of each host family of the
    frozen selection (the publisher too); the services binding comes from the services output. A frozen family that ECS does not list is named, and
    a listed family the selection does not hold is not described."""
    module = importlib.import_module("scripts.infrastructure.services_apply")
    asked: list[tuple[str, list[str]]] = []
    revision = host_revision()

    class Cli:
        def __init__(self, listed: list[str]) -> None:
            self.listed = listed

        def call_bounded(self, service: str, operation: str, arguments: list[str], timeout: float = 0) -> dict[str, Any]:
            asked.append((f"{service} {operation}", arguments))
            if operation == "list-task-definition-families":
                return {"families": self.listed}
            return {"taskDefinition": revision}

    everything = ["example_dev_job_silver_host", "example_dev_job_silver", "example_dev_job_gold_host", "example_dev_job_train_host"]
    settings = {"project_name": "example", "environment": "dev"}
    frozen = json.loads(frozen_suffix(["example_dev_job_gold_host", "example_dev_job_silver_host"], project="example")[len(FROZEN_MARK) :])
    with (
        mock.patch("scripts.infrastructure.render_jobs_config.host_job_room", lambda: (22528, BINDING)),
        rendered_jobs_inputs(rendered_values({"host_jobs_enabled": True, "host_jobs": ["gold", "silver"]}, project="example")),
    ):
        problem = module.jobs_registration_problem(Cli(everything), settings, frozen)
        require(problem is None, f"matching revisions are refused: {problem}")
        require(
            asked[0] == ("ecs list-task-definition-families", ["--family-prefix", "example_dev_job_", "--status", "ACTIVE"]),
            f"the families were not listed by prefix: {asked}",
        )
        described = [arguments[1] for operation, arguments in asked if operation == "ecs describe-task-definition"]
        require(
            described == ["example_dev_job_gold_host", "example_dev_job_silver_host"],
            f"not every selected host family was described, or an unselected or Fargate one was: {described}",
        )
        problem = module.jobs_registration_problem(Cli(["example_dev_job_silver_host", "example_dev_job_silver"]), settings, frozen)
        require(problem is not None and "gold_host" in problem, f"a selected family that ECS does not list is not named: {problem}")
        revision["memory"] = "23552"
        problem = module.jobs_registration_problem(Cli(everything), settings, frozen)
        require(problem is not None and "23552" in problem, f"a stale task memory is not named: {problem}")


class EcsStandIn(AwsStandIn):
    """``AwsStandIn`` that also answers the two ECS reads of the registration check. ``families`` maps a family name to the revision
    ``describe-task-definition`` returns; a family set to ``None`` has no active revision, so ECS does not list it."""

    def __init__(self, families: Mapping[str, dict[str, Any] | None], **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.families = families

    def __call__(self, command: list[str], **extra: Any) -> CompletedProcess[str]:
        operation = command[1:3]
        if operation == ["ecs", "list-task-definition-families"]:
            self.commands.append(list(command))
            listed = [name for name, found in self.families.items() if found is not None]
            return CompletedProcess(command, 0, json.dumps({"families": listed}), "")
        if operation == ["ecs", "describe-task-definition"]:
            self.commands.append(list(command))
            return CompletedProcess(command, 0, json.dumps({"taskDefinition": self.families[command[command.index("--task-definition") + 1]]}), "")
        return super().__call__(command, **extra)


def finish_over_ecs(
    rendered: dict[str, Any] | None, families: Mapping[str, dict[str, Any] | None], frozen_names: list[str], binding: str = BINDING
) -> tuple[int, str, EcsStandIn]:
    """The real ``finish`` command over the stand-in ECS and the given rendered jobs inputs, with stable services and a pending host job cap change
    whose frozen record holds ``frozen_names`` (family names without the project prefix and the ``_host`` suffix)."""
    module = importlib.import_module("scripts.infrastructure.services_apply")
    value = PENDING + frozen_suffix([f"{PREFIX}{name}_host" for name in frozen_names])
    aws = EcsStandIn(families, value=value, lock=OWN_LOCK)
    with (
        rendered_jobs_inputs(rendered),
        mock.patch("scripts.infrastructure.render_jobs_config.host_job_room", lambda: (22528, binding)),
        mock.patch.object(module.Steps, "services_settled", lambda self: True, create=True),
    ):
        code, _, err = run_real_output(["finish", "--token", OWN_LOCK, "--statement", "jobs applied at the new cap"], aws)
    return code, err, aws


def finish_needs_every_expected_host_family() -> None:
    """The real finish command binds to the host families of the frozen selection, the publisher too: one that ECS does not list, one whose revision
    is inactive, a missing publisher and a stale family each refuse, name the family and leave the hold and the lock. The complete set clears both. The
    default selection is the nine families; a family the frozen selection does not hold does not matter; no rendered inputs refuse."""
    names = ["gold", "publisher", "silver"]
    selected = rendered_values({"host_jobs_enabled": True, "host_jobs": ["gold", "silver"], "publisher_job_enabled": True})
    everything = {f"{PREFIX}silver_host": host_revision(), f"{PREFIX}gold_host": host_revision(), f"{PREFIX}publisher_host": host_revision()}
    for label, families, named in (
        ("only silver registered", {f"{PREFIX}silver_host": host_revision()}, "gold_host"),
        ("an expected family missing", {**everything, f"{PREFIX}gold_host": None}, "gold_host"),
        ("an expected family inactive", {**everything, f"{PREFIX}gold_host": host_revision(status="INACTIVE")}, "gold_host"),
        ("the publisher missing", {**everything, f"{PREFIX}publisher_host": None}, "publisher_host"),
        ("an expected family stale", {**everything, f"{PREFIX}gold_host": host_revision(memory="23552")}, "gold_host"),
    ):
        code, err, aws = finish_over_ecs(selected, families, names)
        require(code == 1 and named in err, f"finish with {label}: exit {code} {err}")
        require(str(aws.value).startswith(PENDING) and aws.lock == OWN_LOCK, f"finish with {label} changed the hold or the lock: {aws.value!r} {aws.lock!r}")
    code, err, aws = finish_over_ecs(selected, everything, names)
    require(code == 0 and aws.value == "clear" and aws.lock is None, f"finish with every expected family did not clear both: exit {code} {err} {aws.value!r}")
    nine = {f"{PREFIX}{name}_host": host_revision() for name in ALL_FAMILIES}
    default = rendered_values({"host_jobs_enabled": True})
    code, err, aws = finish_over_ecs(default, {**nine, f"{PREFIX}train_host": None}, ALL_FAMILIES)
    require(
        code == 1 and "train_host" in err and str(aws.value).startswith(PENDING), f"finish with the default selection and one family missing: exit {code} {err}"
    )
    code, err, aws = finish_over_ecs(default, nine, ALL_FAMILIES)
    require(code == 0 and aws.value == "clear", f"finish with the default selection and all nine registered: exit {code} {err}")
    code, err, aws = finish_over_ecs(
        rendered_values({"host_jobs_enabled": True, "host_jobs": ["silver"]}),
        {f"{PREFIX}silver_host": host_revision(), f"{PREFIX}gold_host": host_revision(memory="23552")},
        ["silver"],
    )
    require(code == 0 and aws.value == "clear", f"finish refused over a family the frozen selection does not hold: exit {code} {err}")
    code, err, aws = finish_over_ecs(None, {f"{PREFIX}silver_host": host_revision()}, ["silver"])
    require(code == 1 and str(aws.value).startswith(PENDING) and aws.lock == OWN_LOCK, f"finish with no rendered inputs: exit {code} {err}")


def run_command_path(arguments: list[str], aws: AwsStandIn, rendered: dict[str, Any] | None = None, binding: str = BINDING) -> tuple[int, str, str]:
    """A real command with stable services, the services output ``binding`` and the given rendered jobs inputs; the steady-state wait does not sleep."""
    module = importlib.import_module("scripts.infrastructure.services_apply")
    with (
        rendered_jobs_inputs(rendered),
        mock.patch("scripts.infrastructure.render_jobs_config.host_job_room", lambda: (22528, binding)),
        mock.patch.object(module.Steps, "services_settled", lambda self: True, create=True),
        mock.patch.object(module.Steps, "wait_stable", lambda self, token: None),
    ):
        return run_real_output(arguments, aws)


def start_change(aws: AwsStandIn, directory: Path, variables: dict[str, Any]) -> tuple[Path, str]:
    """Start a host job cap change as the wrapper does for its services apply: the hold of the change over the reviewed jobs inputs and their digest,
    then the settle that keeps the hold once the services are stable."""
    inputs, digest = reviewed_jobs_inputs(directory, variables)
    code, _, err = run_command_path(
        ["hold", "--reason", "terraform.sh --services apply reviewed.tfplan", "--migration", "--jobs-inputs", str(inputs), "--jobs-sha256", digest], aws
    )
    require(code == 0 and str(aws.value).startswith(MIGRATION), f"the change did not start: exit {code} {aws.value!r} {err}")
    code, _, err = run_command_path(["settle", "--token", OWN_LOCK, "--keep-hold"], aws)
    require(code == 0 and str(aws.value).startswith(MIGRATION) and aws.lock == OWN_LOCK, f"settle --keep-hold: exit {code} {aws.value!r} {err}")
    return inputs, digest


def the_migration_start_freezes_the_selection_binding_and_identity() -> None:
    """The hold that starts a host job cap change takes the reviewed jobs inputs by path and digest and records, in the hold value that only this change
    owns, the selected host families (the publisher too), the digest, the project identity and, after the services settled, the capacity binding. A plain
    hold and a continued hold without inputs keep that record; a continued hold with other inputs replaces it and drops the old binding until the next
    settle. A wrong digest, inputs without the change form and one flag without the other change nothing."""
    selection = {"host_jobs_enabled": True, "host_jobs": ["gold", "silver"], "publisher_job_enabled": True}
    with tempfile.TemporaryDirectory() as directory:
        aws = AwsStandIn(lock=OWN_LOCK)
        inputs, digest = reviewed_jobs_inputs(Path(directory), selection)
        code, _, err = run_command_path(["hold", "--reason", "a services apply", "--migration", "--jobs-inputs", str(inputs), "--jobs-sha256", digest], aws)
        require(code == 0 and str(aws.value).startswith(MIGRATION) and "a services apply" in str(aws.value), f"the start hold: exit {code} {aws.value!r} {err}")
        families = [f"{PREFIX}gold_host", f"{PREFIX}publisher_host", f"{PREFIX}silver_host"]
        frozen = frozen_record(aws.value)
        require(
            frozen == {"jobs_sha256": digest, "families": families, "project": "example_project", "environment": "dev"},
            f"the frozen record before the services settled is wrong: {frozen}",
        )
        code, _, err = run_command_path(["settle", "--token", OWN_LOCK, "--keep-hold"], aws)
        frozen = frozen_record(aws.value)
        require(
            code == 0 and frozen.get("binding") == BINDING and frozen["families"] == families, f"settle --keep-hold lost or missed the record: {frozen} {err}"
        )
        for label, arguments in (
            ("a plain hold", ["hold", "--reason", "retry"]),
            ("a continued hold without inputs", ["hold", "--reason", "retry", "--migration"]),
        ):
            code, _, err = run_command_path(arguments, aws)
            require(
                code == 0 and frozen_record(aws.value) == frozen and str(aws.value).startswith(MIGRATION),
                f"{label} changed the frozen record: {aws.value!r} {err}",
            )
        (Path(directory) / "other").mkdir()
        other, other_digest = reviewed_jobs_inputs(Path(directory) / "other", {"host_jobs_enabled": True, "host_jobs": ["silver"]})
        code, _, err = run_command_path(["hold", "--reason", "restart", "--migration", "--jobs-inputs", str(other), "--jobs-sha256", other_digest], aws)
        replaced = frozen_record(aws.value)
        require(
            code == 0 and replaced["families"] == [f"{PREFIX}silver_host"] and replaced["jobs_sha256"] == other_digest and "binding" not in replaced,
            f"a continued hold with other inputs did not replace the record: {replaced} {err}",
        )
        before = aws.value
        for label, arguments in (
            ("a wrong digest", ["hold", "--reason", "x", "--migration", "--jobs-inputs", str(inputs), "--jobs-sha256", "0" * 64]),
            ("inputs without the change form", ["hold", "--reason", "x", "--jobs-inputs", str(inputs), "--jobs-sha256", digest]),
            ("a digest without inputs", ["hold", "--reason", "x", "--migration", "--jobs-sha256", digest]),
            ("inputs without a digest", ["hold", "--reason", "x", "--migration", "--jobs-inputs", str(inputs)]),
        ):
            code, _, err = run_command_path(arguments, aws)
            require(code != 0 and aws.value == before, f"{label} was accepted or changed the hold: exit {code} {aws.value!r} {err}")


def finish_checks_the_frozen_selection_not_a_later_render() -> None:
    """The review sequence through the real commands: the change starts over silver and gold, a partial jobs update registers silver only, then a
    render that selects silver alone follows with no apply. Finish refuses and names the unfinished gold family, whether gold is stale or missing,
    and leaves the hold and the lock. It also refuses when every frozen family is registered but the render disagrees with the frozen record, carries
    another binding or another project identity, or when the services binding is no longer the frozen one, and when the change froze nothing. The
    complete registration with the matching render clears both, and a deselection that was frozen stays ignored."""
    both = {"host_jobs_enabled": True, "host_jobs": ["gold", "silver"]}
    silver_only = rendered_values({"host_jobs_enabled": True, "host_jobs": ["silver"]})
    registered = {f"{PREFIX}silver_host": host_revision(), f"{PREFIX}gold_host": host_revision()}

    def finish(
        frozen_variables: dict[str, Any], families: Mapping[str, dict[str, Any] | None], rendered: dict[str, Any] | None, binding: str = BINDING
    ) -> tuple[int, str, EcsStandIn, Any]:
        with tempfile.TemporaryDirectory() as directory:
            aws = EcsStandIn(families, lock=OWN_LOCK)
            start_change(aws, Path(directory), frozen_variables)
            before = aws.value
            code, _, err = run_command_path(["finish", "--token", OWN_LOCK, "--statement", "jobs applied at the new cap"], aws, rendered, binding)
        return code, err, aws, before

    for label, families in (
        ("gold stale after the partial update", {**registered, f"{PREFIX}gold_host": host_revision(memory="23552")}),
        ("gold missing after the partial update", {**registered, f"{PREFIX}gold_host": None}),
    ):
        code, err, aws, before = finish(both, families, silver_only)
        require(code == 1 and "gold_host" in err, f"finish with {label} and a narrower render: exit {code} {err}")
        require(aws.value == before and aws.lock == OWN_LOCK, f"finish with {label} changed the hold or the lock: {aws.value!r} {aws.lock!r}")
    code, err, aws, before = finish(both, registered, silver_only)
    require(
        code == 1 and "gold_host" in err and "render" in err and aws.value == before,
        f"a narrower render with every frozen family registered: exit {code} {err}",
    )
    for label, rendered, binding in (
        ("another binding in the render", rendered_values(both, binding=BINDING.replace("22528", "23552")), BINDING),
        ("another project in the render", rendered_values(both, project="other_project"), BINDING),
        ("another environment in the render", rendered_values(both, environment="prod"), BINDING),
        ("a services binding that is not the frozen one", rendered_values(both), BINDING.replace("22528", "23552")),
    ):
        code, err, aws, before = finish(both, registered, rendered, binding)
        require(code == 1 and aws.value == before and aws.lock == OWN_LOCK, f"finish with {label}: exit {code} {err} {aws.value!r}")
    code, err, aws, _ = finish(both, registered, rendered_values(both))
    require(code == 0 and aws.value == "clear" and aws.lock is None, f"finish with the frozen selection fully registered did not clear both: exit {code} {err}")
    code, err, aws, _ = finish(
        {"host_jobs_enabled": True, "host_jobs": ["silver"]}, {**registered, f"{PREFIX}gold_host": host_revision(memory="23552")}, silver_only
    )
    require(code == 0 and aws.value == "clear", f"a frozen deselection was not ignored: exit {code} {err}")
    publisher = {"host_jobs_enabled": True, "host_jobs": ["silver"], "publisher_job_enabled": True}
    code, err, aws, before = finish(publisher, {f"{PREFIX}silver_host": host_revision(), f"{PREFIX}publisher_host": None}, rendered_values(publisher))
    require(code == 1 and "publisher_host" in err and aws.value == before, f"a frozen publisher that is not registered: exit {code} {err}")


def finish_refuses_a_change_that_froze_nothing() -> None:
    """The review sequence over a change that was started without reviewed jobs inputs, so nothing was frozen: the services settle, a partial jobs update
    leaves gold stale, and a render that selects silver alone follows. Finish refuses and says what the change lacks, instead of taking the render as the
    selection, and leaves the hold and the lock."""
    both = {"host_jobs_enabled": True, "host_jobs": ["gold", "silver"]}
    stale_gold = {f"{PREFIX}silver_host": host_revision(), f"{PREFIX}gold_host": host_revision(memory="23552")}
    narrower = rendered_values({"host_jobs_enabled": True, "host_jobs": ["silver"]})
    for label, value in (("a legacy marker", PENDING), ("a change started without inputs", None)):
        aws = EcsStandIn(stale_gold, value=value, lock=OWN_LOCK)
        if value is None:
            code, _, err = run_command_path(["hold", "--reason", "no inputs given", "--migration"], aws)
            require(code == 0 and str(aws.value).startswith(MIGRATION) and FROZEN_MARK not in str(aws.value), f"{label}: the start hold: exit {code} {err}")
            code, _, err = run_command_path(["settle", "--token", OWN_LOCK, "--keep-hold"], aws)
            require(code == 0 and aws.lock == OWN_LOCK, f"{label}: settle --keep-hold: exit {code} {err}")
        before = aws.value
        code, _, err = run_command_path(["finish", "--token", OWN_LOCK, "--statement", "jobs applied at the new cap"], aws, narrower)
        require(code == 1 and "frozen" in err, f"{label}: finish did not refuse for the missing frozen selection: exit {code} {err}")
        require(aws.value == before and aws.lock == OWN_LOCK, f"{label}: finish changed the hold or the lock: {aws.value!r} {aws.lock!r}")
        code, _, err = run_command_path(["finish", "--token", OWN_LOCK, "--statement", "jobs applied at the new cap"], aws, rendered_values(both))
        require(code == 1 and aws.value == before, f"{label}: finish cleared a change over a stale gold family: exit {code} {err}")


def a_plain_hold_never_downgrades_a_pending_host_job_cap_change() -> None:
    """The real hold command rewrites the hold of a pending host job cap change in the change form, so the plain settle and both resolve forms
    still refuse afterwards, print the finish command and leave the hold and the lock. Without a pending change the plain hold stays plain."""
    module = importlib.import_module("scripts.infrastructure.services_apply")
    store, _, current, _ = pending_migration()
    current.hold("retry the services apply")
    require(str(store.value).startswith(MIGRATION), f"a plain hold in process downgraded the pending change: {store.value!r}")
    for later in (
        ["settle", "--token", OWN_LOCK],
        ["resolve", "--statement", "services stable", "--token", OWN_LOCK],
        ["resolve", "--statement", "services stable"],
    ):
        aws = AwsStandIn(value=PENDING, lock=OWN_LOCK)
        code, _, err = run_real_output(["hold", "--reason", "retry the services apply"], aws)
        require(code == 0 and str(aws.value).startswith(MIGRATION), f"the plain hold command downgraded the pending change: exit {code} {aws.value!r} {err}")
        require("retry the services apply" in str(aws.value), f"the rewritten hold lost its reason: {aws.value!r}")
        with (
            mock.patch.object(module.Steps, "services_settled", lambda self: True, create=True),
            mock.patch.object(module.Steps, "wait_stable", lambda self, token: None),
        ):
            code, _, err = run_real_output(later, aws)
        require(
            code == 1 and FINISH in err and f"--token '{OWN_LOCK}'" in err and str(aws.value).startswith(MIGRATION) and aws.lock == OWN_LOCK,
            f"{later[0]} after a plain hold released a pending change: exit {code} {err} {aws.value!r} {aws.lock!r}",
        )
    for before in (None, "clear", "held by services_apply at 2026-11-11T00:00:00Z: an earlier apply"):
        aws = AwsStandIn(value=before)
        code, _, err = run_real_output(["hold", "--reason", "a services apply"], aws)
        require(
            code == 0 and str(aws.value).startswith("held by services_apply") and "a services apply" in str(aws.value),
            f"hold over {before!r}: exit {code} {aws.value!r} {err}",
        )


def finish_through_the_command() -> None:
    """The command forms. ``settle --keep-hold`` leaves the change form; plain ``settle`` and ``resolve`` exit 1 and change nothing; ``finish`` exits 1
    while the registration check names a gap and 0 once it passes, and then ``resolve`` is no longer needed."""
    module = importlib.import_module("scripts.infrastructure.services_apply")
    mine = OWN_LOCK
    aws = AwsStandIn(value="held by services_apply", lock=mine)
    with mock.patch.object(module.Steps, "wait_stable", lambda self, token: None):
        code, out, err = run_real_output(["settle", "--token", mine, "--keep-hold"], aws)
    require(code == 0 and str(aws.value).startswith(MIGRATION) and aws.lock == mine, f"settle --keep-hold: exit {code} {aws.value!r} {aws.lock!r} {err}")
    require(FINISH in out + err and f"--token '{mine}'" in out + err, f"settle --keep-hold does not print the finish command with the token: {out} {err}")
    held = aws.value
    for arguments in (["settle", "--token", mine], ["resolve", "--statement", "x", "--token", mine], ["resolve", "--statement", "x"]):
        with (
            mock.patch.object(module.Steps, "services_settled", lambda self: True, create=True),
            mock.patch.object(module.Steps, "wait_stable", lambda self, token: None),
        ):
            code, _, err = run_real_output(arguments, aws)
        require(
            code == 1 and FINISH in err and aws.value == held and aws.lock == mine,
            f"{arguments[0]} cleared or accepted a pending host job cap change: exit {code} {err}",
        )
    gap = mock.patch.object(module, "jobs_registration_problem", lambda *_: "family silver_host is registered at 23552 MiB")
    with gap, mock.patch.object(module.Steps, "services_settled", lambda self: True, create=True):
        code, _, err = run_real_output(["finish", "--token", mine, "--statement", "jobs applied at the new cap"], aws)
    require(code == 1 and "silver_host" in err and aws.value == held and aws.lock == mine, f"finish before the registration: exit {code} {err}")
    with (
        mock.patch.object(module, "jobs_registration_problem", lambda *_: None),
        mock.patch.object(module.Steps, "services_settled", lambda self: True, create=True),
    ):
        code, _, err = run_real_output(["finish", "--token", mine, "--statement", "jobs applied at the new cap"], aws)
    require(
        code == 0 and aws.value == "clear" and aws.lock is None,
        f"finish after the registration did not clear both: exit {code} {aws.value!r} {aws.lock!r} {err}",
    )


def the_hold_command_can_write_the_change_form() -> None:
    """``hold --migration`` and ``lock --continue-token`` exist as commands, so the wrapper can run a continued apply."""
    module = importlib.import_module("scripts.infrastructure.services_apply")
    parsed = module.build_parser().parse_args(["hold", "--reason", "x", "--migration"])
    require(parsed.migration is True, "hold has no --migration option")
    parsed = module.build_parser().parse_args(["lock", "--reason", "x", "--continue-token", OWN_LOCK])
    require(parsed.continue_token == OWN_LOCK, "lock has no --continue-token option")
    parsed = module.build_parser().parse_args(["finish", "--token", OWN_LOCK, "--statement", "x"])
    require(parsed.command == "finish", "there is no finish command")


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


def settle_requires_and_checks_its_token() -> None:
    """settle needs this apply's token in the method itself: none, an empty one, a malformed one or another apply's token changes nothing."""
    for token in (None, "", "not a token", "other-host:2:20261110T221000Z:87654321"):
        clock = Clock()
        store = Store(clock, "held by services_apply")
        current, module = steps(store, clock, lambda: True)
        try:
            current.settle(token)
        except module.ServicesApplyError:
            pass
        else:
            raise AssertionError(f"settle accepted the token {token!r}")
        require(store.value == "held by services_apply" and store.lock.value == OWN_LOCK, f"settle with {token!r} changed the hold or the lock")


def resolve_without_a_token_takes_the_lock_first() -> None:
    """resolve without a token takes the apply lock itself (create without overwrite) before anything else, clears the hold, then deletes its own
    lock. While another apply holds the lock it changes nothing."""
    clock = Clock()
    store = Store(clock, "held by services_apply")
    created: list[str] = []

    class Recording(MemoryLock):
        def create(self, value: str) -> None:
            created.append(value)
            require(store.value == "held by services_apply", "resolve wrote the hold before it took the lock")
            super().create(value)

    lock = Recording()
    current, module = steps(store, clock, lambda: True, lock)
    current.resolve("checked the failed apply and the services")
    require(len(created) == 1 and TOKEN.fullmatch(created[0]) is not None, f"resolve did not take the lock with a new token: {created}")
    require(store.value == "clear" and lock.value is None, f"resolve did not clear the hold and delete its own lock: {store.value!r} {lock.value!r}")
    clock = Clock()
    store = Store(clock, "held by another apply")
    other = MemoryLock("other-host:2:20261110T221000Z:87654321")
    current, module = steps(store, clock, lambda: True, other)
    try:
        current.resolve("checked")
    except module.ServicesApplyError:
        pass
    else:
        raise AssertionError("resolve without a token cleared a hold while another apply holds the lock")
    require(store.value == "held by another apply" and other.value == "other-host:2:20261110T221000Z:87654321", "resolve changed another apply's state")


def resolve_cannot_clear_a_hold_another_machine_just_set() -> None:
    """Forced order on the real module: resolve without a token starts while no lock exists; another machine then takes the lock and sets its
    hold. resolve must not write clear over that hold."""
    other = "other-host:2:20261110T221000Z:87654321"

    class Racing(AwsStandIn):
        raced = False

        def lock_call(self, command: list[str], operation: list[str]) -> CompletedProcess[str]:
            answer = super().lock_call(command, operation)
            if not self.raced:
                self.raced = True
                if self.lock is None:  # the other machine's create succeeds only while no lock exists
                    self.lock, self.value = other, "held by services_apply on other-host"
            return answer

    module = importlib.import_module("scripts.infrastructure.services_apply")
    aws = Racing(value="held by services_apply")
    with mock.patch.object(module.Steps, "services_settled", lambda self: True, create=True):
        run_real_output(["resolve", "--statement", "checked"], aws)
    if aws.lock == other:
        require(aws.value == "held by services_apply on other-host", f"resolve cleared the hold another machine set: {aws.value!r}")
    hold_puts = [command for command in aws.commands if command[1:3] == ["ssm", "put-parameter"] and HOLD in command]
    lock_ops = [i for i, command in enumerate(aws.commands) if LOCK in command]
    require(not hold_puts or (bool(lock_ops) and lock_ops[0] < aws.commands.index(hold_puts[0])), "resolve wrote the hold before touching the lock")


def a_settle_failure_names_the_lock_token() -> None:
    """A failed rollout or a steady-state timeout during settle keeps the hold and the lock, and the printed resolve command carries this
    apply's lock token, so following it is not refused while the lock is held."""
    for stable in (lambda: (_ for _ in ()).throw(RuntimeError("service airflow rollout FAILED")), lambda: False):
        clock = Clock()
        store = Store(clock, "held by services_apply")
        current, module = steps(store, clock, stable)
        try:
            current.settle(OWN_LOCK)
        except module.ServicesApplyError as error:
            require(f"--token '{OWN_LOCK}'" in str(error), f"the settle failure does not give the resolve command with the token: {error}")
        else:
            raise AssertionError("settle returned without steady state")
        require(store.value == "held by services_apply" and store.lock.value == OWN_LOCK, "a failed settle changed the hold or the lock")


CASES: dict[str, Callable[[], None]] = {
    "a_settle_failure_names_the_lock_token": a_settle_failure_names_the_lock_token,
    "settle_requires_and_checks_its_token": settle_requires_and_checks_its_token,
    "resolve_without_a_token_takes_the_lock_first": resolve_without_a_token_takes_the_lock_first,
    "resolve_cannot_clear_a_hold_another_machine_just_set": resolve_cannot_clear_a_hold_another_machine_just_set,
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
    "settle_keeps_a_hold_that_only_the_jobs_registration_ends": settle_keeps_a_hold_that_only_the_jobs_registration_ends,
    "no_plain_path_clears_a_pending_migration_hold": no_plain_path_clears_a_pending_migration_hold,
    "finish_clears_the_hold_only_after_the_jobs_registration": finish_clears_the_hold_only_after_the_jobs_registration,
    "a_services_apply_can_continue_under_the_pending_lock": a_services_apply_can_continue_under_the_pending_lock,
    "registration_problem_reads_every_host_revision": registration_problem_reads_every_host_revision,
    "jobs_registration_reads_the_project_host_families": jobs_registration_reads_the_project_host_families,
    "finish_through_the_command": finish_through_the_command,
    "finish_needs_every_expected_host_family": finish_needs_every_expected_host_family,
    "the_migration_start_freezes_the_selection_binding_and_identity": the_migration_start_freezes_the_selection_binding_and_identity,
    "finish_checks_the_frozen_selection_not_a_later_render": finish_checks_the_frozen_selection_not_a_later_render,
    "finish_refuses_a_change_that_froze_nothing": finish_refuses_a_change_that_froze_nothing,
    "a_plain_hold_never_downgrades_a_pending_host_job_cap_change": a_plain_hold_never_downgrades_a_pending_host_job_cap_change,
    "the_hold_command_can_write_the_change_form": the_hold_command_can_write_the_change_form,
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
