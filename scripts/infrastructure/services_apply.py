"""Hold host launches around every services apply: ``lock``, ``hold``, ``settle``, ``resolve`` and ``status``.

Launch hold during services applies (owner A, Nov 10 2026). A services apply can change the host job cap and the service reservations while a host job
is between its launch preflight and its RunTask call. So every services apply sets a launch hold before it changes anything, and clears it only after
the ECS services reach steady state. ``bash scripts/infrastructure/terraform.sh --services apply <PLAN>`` runs ``hold`` before ``terraform apply`` and
``settle`` after a successful apply. A failed apply, or services that do not reach steady state, leave the hold set; the owner checks what happened
and runs ``resolve`` with a statement. A later GitHub Actions apply can run the same three steps around its own apply.

One services apply runs at a time, across machines: ``lock`` creates the apply lock parameter (``services_apply_lock_parameter_name`` of the IAM
stack, default ``/healthcare_hai_prediction/services_apply_lock``) without overwrite, valued with a unique token, before the hold. While another apply
holds it AWS refuses the create and the apply does not start. ``settle`` and ``resolve`` delete the lock only with its own token.

The hold is one Parameter Store value, ``host_launch_hold_parameter_name`` of the services stack (default ``/healthcare_hai_prediction/host_launch_hold``).
It is not managed by Terraform, so no apply can clear it. ``clear`` lets host launches run; any other value, or no parameter, holds them: the launch
preflight refuses every host launch, and the fit probe rereads the hold before every RunTask.

Run from the repository root, for example: ``.venv/bin/python -m scripts.infrastructure.services_apply resolve --statement '<what you checked>'``. The
profile and region come from ``.env``. Every AWS call goes through ``AwsCli`` with the project profile, and every wait has a deadline.
"""

from __future__ import annotations

import argparse
import os
import re
import secrets
import socket
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from orchestration.ecs_preflight import LAUNCH_HOLD_CLEAR
from scripts.acquisition.s3_store import AwsCli, StorageError
from scripts.infrastructure.host_session import tolerant_runner
from scripts.infrastructure.render_project_config import REPO_ROOT, ConfigurationError, load_configuration

SERVICES_VARIABLES = REPO_ROOT / "infra" / "services" / "variables.tf"
IAM_LAUNCH_VARIABLES = REPO_ROOT / "infra" / "iam" / "airflow_launch.tf"
# The window between a fit check that read a clear hold and its RunTask reply: one RunTask attempt ends within 5 s to connect and 10 s to read
# (orchestration.ecs_adapter.send_config). Waiting twice that after writing the hold lets every such call finish before the apply changes anything.
HOLD_SETTLE_SECONDS = 30
STABLE_SECONDS = 1200
POLL_SECONDS = 15
CALL_SECONDS = 60
RESOLVE_COMMAND = "python -m scripts.infrastructure.services_apply resolve --statement '<what you checked>'"


class ServicesApplyError(Exception):
    """A step that could not finish. The launch hold stays as it was."""


class HoldStoreError(Exception):
    """A Parameter Store call for the hold failed; ``code`` is the AWS error code or a fixed category."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(f"Parameter Store: {code}")


def hold_parameter_name() -> str:
    """The launch hold parameter name: the default of ``host_launch_hold_parameter_name`` in the services stack, which also renders it into the
    Airflow environment."""
    match = re.search(r'variable "host_launch_hold_parameter_name" \{.*?default\s*=\s*"([^"]+)"', SERVICES_VARIABLES.read_text(), re.DOTALL)
    if match is None:
        raise ConfigurationError(f"{SERVICES_VARIABLES.name} has no default for host_launch_hold_parameter_name")
    return match.group(1)


def lock_parameter_name() -> str:
    """The apply lock parameter name: the default of ``services_apply_lock_parameter_name`` in the IAM stack, which grants the project user its
    create, read and delete."""
    match = re.search(r'variable "services_apply_lock_parameter_name" \{.*?default\s*=\s*"([^"]+)"', IAM_LAUNCH_VARIABLES.read_text(), re.DOTALL)
    if match is None:
        raise ConfigurationError(f"{IAM_LAUNCH_VARIABLES.name} has no default for services_apply_lock_parameter_name")
    return match.group(1)


def new_token() -> str:
    """A unique apply lock token: host, process ID, UTC time and a random part."""
    host = re.sub(r"[^A-Za-z0-9._-]", "-", socket.gethostname())[:60] or "host"
    return f"{host}:{os.getpid()}:{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}:{secrets.token_hex(4)}"


@dataclass(frozen=True)
class LockStore:
    """The apply lock parameter: ``create`` puts it without overwrite and raises ``LockHeld`` when it exists, ``get`` reads it or None, and
    ``delete`` removes it (a missing parameter is not an error)."""

    create: Callable[[str], None]
    get: Callable[[], str | None]
    delete: Callable[[], None]


class LockHeld(Exception):
    """Another services apply holds the apply lock."""


class Clock:
    """The time source of every wait; a case replaces it to run time forward without sleeping."""

    monotonic: Callable[[], float] = staticmethod(time.monotonic)
    sleep: Callable[[float], None] = staticmethod(time.sleep)


class Steps:
    """The steps around one services apply. ``put`` and ``get`` write and read the hold parameter, ``stable`` is True once every ECS service is
    settled (it raises on a failed rollout), and ``report`` prints one line per finished step."""

    def __init__(
        self,
        put: Callable[[str], None],
        get: Callable[[], str | None],
        stable: Callable[[], bool],
        clock: Any = None,
        report: Callable[[str], None] | None = None,
        lock: LockStore | None = None,
    ) -> None:
        self.put, self.get, self.stable, self.lock_store = put, get, stable, lock
        self.clock = clock or Clock()
        self.report = report or (lambda line: sys.stdout.write(line + "\n"))

    def write(self, value: str) -> None:
        """Write the hold value and read it back, or raise."""
        try:
            self.put(value)
            found = self.get()
        except Exception as error:  # any refusal or transport error of the store leaves the hold unknown
            raise ServicesApplyError(f"the launch hold could not be written ({type(error).__name__}: {str(error)[:200]})") from None
        if found != value:
            raise ServicesApplyError("the launch hold did not read back as written")

    def hold(self, reason: str) -> None:
        """Set the hold before the apply changes anything, then wait the settle window so a RunTask that a fit check allowed just before has its reply."""
        stamp = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        self.write(f"held by services_apply at {stamp}: {reason.strip()[:200]}")
        self.clock.sleep(HOLD_SETTLE_SECONDS)
        self.report(f"launch hold: set, waited {HOLD_SETTLE_SECONDS} s for any RunTask already allowed")

    def lock(self) -> str:
        """Take the apply lock before the hold: create the lock parameter without overwrite, valued with a new token, and return the token. While
        another apply holds it AWS refuses the create, and this raises naming the holder; nothing changes."""
        if self.lock_store is None:
            raise ServicesApplyError("no apply lock store is configured")
        token = new_token()
        try:
            self.lock_store.create(token)
        except LockHeld:
            holder = self.lock_store.get()
            raise ServicesApplyError(
                f"another services apply holds the apply lock ({holder}), so this apply did not start. Wait for it to finish. If it failed, check it "
                f"and run {RESOLVE_COMMAND} --token '{holder}'"
            ) from None
        except Exception as error:  # a refused or failed create: the lock is not known to be this apply's
            raise ServicesApplyError(f"the apply lock could not be taken ({type(error).__name__}: {str(error)[:200]})") from None
        return token

    def require_lock(self, token: str) -> None:
        """Refuse unless the apply lock holds exactly this token."""
        if self.lock_store is None:
            return
        found = self.lock_store.get()
        if found != token:
            raise ServicesApplyError(f"the apply lock is held by {found or 'no apply'}, not by {token}. Nothing was changed")

    def release(self, token: str) -> None:
        """Delete the apply lock only while it still holds this token. No other apply can take the lock while it holds this token, so the read and
        the delete cannot remove another apply's lock."""
        if self.lock_store is None:
            return
        self.require_lock(token)
        self.lock_store.delete()
        self.report("apply lock: released")

    def services_settled(self) -> bool:
        """True once every ECS service is settled. A missing services output is a configuration error; any other failure is not settled."""
        try:
            return bool(self.stable())
        except ConfigurationError:
            raise
        except Exception as error:  # the same refusal as in wait_stable
            raise ServicesApplyError(f"the services are not stable ({type(error).__name__}). The launch hold stays set") from None

    def wait_stable(self) -> None:
        """Poll until every ECS service is settled, within ``STABLE_SECONDS``; a failed rollout or the deadline raises and the hold stays."""
        end = self.clock.monotonic() + STABLE_SECONDS
        while True:
            try:
                settled = bool(self.stable())
            except ConfigurationError:
                raise  # the services outputs are missing: a configuration error (exit 2), and the hold stays
            except Exception as error:  # a failed rollout or a failed describe call: the services are not known to be stable
                raise ServicesApplyError(
                    f"the services did not reach steady state ({type(error).__name__}: {str(error)[:200]}). The launch hold stays set; check the "
                    f"services, then run {RESOLVE_COMMAND}"
                ) from None
            if settled:
                return
            if self.clock.monotonic() + POLL_SECONDS > end:
                raise ServicesApplyError(
                    f"the services did not reach steady state within {STABLE_SECONDS} s. The launch hold stays set; check the services, then run "
                    f"{RESOLVE_COMMAND}"
                )
            self.clock.sleep(POLL_SECONDS)

    def settle(self, token: str | None = None) -> None:
        """After a successful apply: check that this apply still holds the apply lock, wait for steady state, clear the hold, then delete the lock.
        A lock held by another token changes nothing."""
        if token is not None:
            self.require_lock(token)
        self.wait_stable()
        self.write(LAUNCH_HOLD_CLEAR)
        self.report("launch hold: cleared after the services reached steady state")
        if token is not None:
            self.release(token)

    def resolve(self, statement: str, token: str | None = None) -> None:
        """After a failed apply or settle: with the owner's statement of what was checked, the token of the apply lock when one is held, and only
        while every service is settled now, clear the hold, then delete the lock."""
        if not statement.strip():
            raise ServicesApplyError("resolve needs --statement with what you checked. The launch hold stays set")
        held = self.lock_store.get() if self.lock_store is not None else None
        if held is not None and held != token:
            raise ServicesApplyError(f"the apply lock is held by {held}. Give that token with --token to resolve it. Nothing was changed")
        if not self.services_settled():
            raise ServicesApplyError("the services are not stable yet. The launch hold stays set; run resolve again once they are")
        self.write(LAUNCH_HOLD_CLEAR)
        self.report(f"launch hold: cleared by the owner ({statement.strip()[:200]})")
        if held is not None and token is not None:
            self.release(token)


def open_steps() -> Steps:
    """The steps with the project profile. The hold is written through Parameter Store with only the ``.env`` configuration, so ``hold`` works on a
    fresh stack before the services stack has any output. The steady-state check opens the host session, which reads the platform and services
    outputs, only when ``settle`` or ``resolve`` first calls it."""
    settings, _ = load_configuration(REPO_ROOT / ".env")
    cli = AwsCli(settings, tolerant_runner)
    name = hold_parameter_name()

    def call(operation: str, arguments: list[str]) -> dict[str, Any]:
        try:
            return cli.call_bounded("ssm", operation, ["--name", name, *arguments], timeout=CALL_SECONDS)
        except StorageError as error:
            raise HoldStoreError(error.code) from None

    def put(value: str) -> None:
        call("put-parameter", ["--type", "String", "--value", value, "--overwrite"])

    def get() -> str | None:
        try:
            return str(call("get-parameter", [])["Parameter"]["Value"])
        except HoldStoreError as error:
            if error.code == "ParameterNotFound":
                return None
            raise

    lock_name = lock_parameter_name()

    def lock_call(operation: str, arguments: list[str]) -> dict[str, Any]:
        try:
            return cli.call_bounded("ssm", operation, ["--name", lock_name, *arguments], timeout=CALL_SECONDS)
        except StorageError as error:
            raise HoldStoreError(error.code) from None

    def lock_create(value: str) -> None:
        try:
            lock_call("put-parameter", ["--type", "String", "--value", value])  # no --overwrite: AWS refuses while the lock exists
        except HoldStoreError as error:
            if error.code == "ParameterAlreadyExists":
                raise LockHeld from None
            raise

    def lock_get() -> str | None:
        try:
            return str(lock_call("get-parameter", [])["Parameter"]["Value"])
        except HoldStoreError as error:
            if error.code == "ParameterNotFound":
                return None
            raise

    def lock_delete() -> None:
        try:
            lock_call("delete-parameter", [])
        except HoldStoreError as error:
            if error.code != "ParameterNotFound":
                raise

    sessions: list[Any] = []

    def stable() -> bool:
        if not sessions:
            from scripts.infrastructure.host_session import open_session  # reads the services outputs, which settle and resolve need

            sessions.append(open_session())
        return bool(sessions[0].services_stable())

    return Steps(put=put, get=get, stable=stable, lock=LockStore(create=lock_create, get=lock_get, delete=lock_delete))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    lock = commands.add_parser("lock", help="take the apply lock before the hold and print its token (terraform.sh runs it)")
    lock.add_argument("--reason", required=True, help="what is being applied, for example the saved plan")
    hold = commands.add_parser("hold", help="set the launch hold before a services apply (terraform.sh runs it)")
    hold.add_argument("--reason", required=True, help="what is being applied, for example the saved plan")
    settle = commands.add_parser("settle", help="after a successful services apply: wait for steady state, clear the hold, release the lock")
    settle.add_argument("--token", required=True, help="the apply lock token that lock printed")
    resolve = commands.add_parser("resolve", help="after a failed apply or settle: clear the hold and the lock once the services are stable")
    resolve.add_argument("--statement", required=True, help="what you checked")
    resolve.add_argument("--token", help="the apply lock token, required while a lock is held")
    commands.add_parser("status", help="print the launch hold and the apply lock, read only")
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run one subcommand: 0 on success, 1 on a failed step (the hold stays as it was), 2 on a configuration error."""
    arguments = build_parser().parse_args(argv)
    try:
        steps = open_steps()
        if arguments.command == "lock":
            sys.stdout.write(f"{steps.lock()}\n")
        elif arguments.command == "hold":
            steps.hold(arguments.reason)
        elif arguments.command == "settle":
            steps.settle(arguments.token)
        elif arguments.command == "resolve":
            steps.resolve(arguments.statement, arguments.token)
        else:
            lock = steps.lock_store.get() if steps.lock_store is not None else None
            sys.stdout.write(f"launch hold: {steps.get()}\napply lock: {lock}\n")
    except ConfigurationError as error:
        sys.stderr.write(f"configuration: {error}\n")
        return 2
    except ServicesApplyError as error:
        sys.stderr.write(f"services_apply {arguments.command}: {error}\n")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
