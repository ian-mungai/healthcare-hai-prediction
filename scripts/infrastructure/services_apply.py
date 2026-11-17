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
preflight refuses every host launch, and the fit probe rereads the hold before every RunTask. This command writes any other value as one JSON object with
separate fields (``state``, ``by``, ``at``, ``reason`` and, for a host job cap change, ``frozen``), so the preflight needs no parser and reason text is only
ever the reason. A hold that earlier code wrote as text reads as a pending change without a record when it starts with the change marker, and as a plain hold
otherwise. A value that repeats a member name at any depth is malformed, never read from its last repeat.

Changing the host job cap, and the move of the cap from Parameter Store to the Airflow environment (Nov 11 2026). The cap is the services input
``host_job_cap_mib``. The jobs stack must register host revisions at the new cap after every change, and the launch preflight refuses a revision whose job
memory is above the cap, so the services apply and the jobs apply are one change. The hold of that change names it (``held for a host job cap change``).
Plain ``settle`` and ``resolve`` refuse to clear it and print the ``finish`` command, and a plain ``hold`` keeps the name of a pending change when it
rewrites the hold. The hold that starts the change takes the reviewed jobs inputs (``--jobs-inputs`` and their ``--jobs-sha256``) and freezes, in the hold
value that only this change owns, the host families they select (``host_jobs`` while ``host_jobs_enabled``, and the publisher while
``publisher_job_enabled``), the digest, the project identity and, once the services settled, the capacity binding. ``finish`` clears the hold and the lock
only when the services are stable, the services give the frozen binding and every frozen family has an active latest revision that carries that binding and
its job memory. It never takes a later render as the selection: it refuses when the rendered jobs inputs (``infra/jobs/deployment.auto.tfvars.json``, which
step 5 wrote) disagree with the frozen selection, binding or identity.

Before step 1, choose the cap. The committed default of ``host_job_cap_mib`` is 22800, the room the services leave with every default service on. To
change the cap for good, change that default in infra/services/variables.tf and commit it, and save SERVICES_INPUTS with an empty ``variables`` object. To
try one other cap, save SERVICES_INPUTS as a Git-ignored overrides file with ``"host_job_cap_mib": 22528`` (or your cap) in its ``variables``; an
overrides file never reaches main. Save the jobs overrides as JOBS_INPUTS, with an empty ``variables`` object when you override nothing: the hold freezes
the host families they select, which are the committed ``host_jobs`` and the publisher unless the file turns them off or lists others.
Run every command from the repository root in one shell.

    set -euo pipefail
    PY=.venv/bin/python
    TF=scripts/infrastructure/terraform.sh
    SERVICES_INPUTS=data/local_keep/host_job_cap/services_inputs.json
    JOBS_INPUTS=data/local_keep/host_job_cap/jobs_inputs.json
    SERVICES_SHA="$(shasum -a 256 "$SERVICES_INPUTS" | cut -d ' ' -f 1)"
    JOBS_SHA="$(shasum -a 256 "$JOBS_INPUTS" | cut -d ' ' -f 1)"
    SERVICES_FLAGS=(--services --preparation-inputs "$SERVICES_INPUTS" --preparation-sha256 "$SERVICES_SHA")
    JOBS_FLAGS=(--jobs --preparation-inputs "$JOBS_INPUTS" --preparation-sha256 "$JOBS_SHA")

1. Check that nothing runs. In the Airflow UI (Browse, DAG runs) no run is in state running, and ``status`` shows a clear hold and no lock. For the move,
   the release that carries the new launch code is checked out on the host first (the host session start with its code run). New code with the old
   container environment, and old code with the new one, both fail the build of the launch services at once ("is not set in the Airflow service
   environment"), before any AWS call, so neither mix starts a task. Only code and environment of the same release launch.

    "$PY" -m scripts.infrastructure.services_apply status

2. Plan the services stack with the reviewed inputs and their digest.

    bash "$TF" "${SERVICES_FLAGS[@]}" plan -out=host_job_cap.tfplan

3. Apply that plan. It takes the lock, sets the change hold with the reviewed jobs inputs frozen in it, applies, waits for steady state, adds the
   services binding to the frozen record and keeps the hold and the lock. For the move, this apply deletes the retired Parameter Store parameter, which
   needs the project user's current grant on it. Then read the lock token.

    bash "$TF" "${SERVICES_FLAGS[@]}" --keep-launch-hold --jobs-inputs "$JOBS_INPUTS" --jobs-sha256 "$JOBS_SHA" apply host_job_cap.tfplan
    TOKEN="$("$PY" -m scripts.infrastructure.services_apply status | sed -n 's/^apply lock: //p')"

4. For the move only: apply the IAM stack and the project user policies. They drop the grants on the retired parameter, so they come after step 3.

    : "${ADMIN_PROFILE:?export ADMIN_PROFILE, the administrator AWS profile name, first}"
    bash "$TF" --iam --admin-profile "$ADMIN_PROFILE" plan -out=host_job_cap.tfplan
    bash "$TF" --iam --admin-profile "$ADMIN_PROFILE" apply host_job_cap.tfplan
    DIGEST="$("$PY" -m infra.iam.scripts.manage_policies plan --admin-profile "$ADMIN_PROFILE" | sed -n 's/^Plan digest: //p')"
    export CONFIRM_IAM_POLICIES=apply-healthcare-hai-prediction-policies
    "$PY" -m infra.iam.scripts.manage_policies apply --admin-profile "$ADMIN_PROFILE" --digest "$DIGEST"

5. Plan and apply the jobs stack, so every host revision carries the new cap as its job memory.

    bash "$TF" "${JOBS_FLAGS[@]}" plan -out=host_job_cap.tfplan
    bash "$TF" "${JOBS_FLAGS[@]}" apply host_job_cap.tfplan

6. End the change. It reads the registered revisions of the host families frozen in step 3 and refuses, naming the first family that has no active
   revision or does not match, until step 5 has registered them all. Run it from the same checkout as step 5, which rendered the jobs inputs.

    "$PY" -m scripts.infrastructure.services_apply finish --token "$TOKEN" --statement 'jobs applied at the new cap'

Recovery. A failed jobs apply leaves the hold: fix the cause and repeat step 5. A failed services apply also leaves it (the wrapper prints the finish
command): if the services changed in part, correct the inputs, repeat step 2 and apply under the same lock, then continue from step 4. The frozen record
stays. A continued hold must keep every frozen family and the identity: ``--jobs-inputs`` and ``--jobs-sha256`` given to it again replace the record only
with a wider or equal selection, and inputs that drop a family are refused. To deselect a family, apply the original selection, run ``finish``, then
deselect in a separate jobs change. A hold that a malformed value or earlier code (text, no record) left is repaired by ``hold --migration`` with fresh
reviewed inputs and digest, then ``settle --keep-hold``.

    bash "$TF" "${SERVICES_FLAGS[@]}" --continue-launch-hold "$TOKEN" apply host_job_cap.tfplan

The window is from the hold of step 3 to ``finish``: the preflight refuses every host launch, so a run started in it fails and nothing is lost; start it
again after step 6. Without ``--keep-launch-hold`` the hold ends at steady state, and with a lower cap the next launch is refused by the revision check
(a failed run, no task) until the jobs apply has registered revisions at the new cap.

Run from the repository root, for example: ``.venv/bin/python -m scripts.infrastructure.services_apply resolve --statement '<what you checked>'``. The
profile and region come from ``.env``. Every AWS call goes through ``AwsCli`` with the project profile, and every wait has a deadline.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import secrets
import socket
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
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
TOKEN = re.compile(r"[A-Za-z0-9._-]+:[0-9]+:[0-9]{8}T[0-9]{6}Z:[0-9a-f]{8}")
RESOLVE_COMMAND = "python -m scripts.infrastructure.services_apply resolve --statement '<what you checked>'"
# The hold of a host job cap change names it, so no plain settle or resolve ends it: only finish does, after the jobs apply registered every host revision.
MIGRATION_HOLD = "held for a host job cap change"
FINISH_STATEMENT = "jobs applied at the new cap"
HOST_CAPACITY_LABEL = "hai.host_capacity"
JOB_MEMORY = re.compile(r"job_memory_mib=([1-9][0-9]*) ")
# The jobs inputs the last jobs plan and apply rendered (render_jobs_config), and the jobs stack file that defaults the selected host families.
JOBS_INPUTS_RENDERED = REPO_ROOT / "infra" / "jobs" / "deployment.auto.tfvars.json"
JOBS_HOST_VARIABLES = REPO_ROOT / "infra" / "jobs" / "host.tf"
JOBS_PUBLISHER_VARIABLES = REPO_ROOT / "infra" / "jobs" / "publisher.tf"
# The host table: which families reserve their own size (a model set) instead of the whole job room.
HOST_TABLE = REPO_ROOT / "config" / "orchestration" / "host_jobs.json"
FAMILY_NAME = re.compile(r"[a-z][a-z0-9_]*")
# The hold value written by this command is one JSON object with separate fields, so reason text is only ever the reason: "state" (HELD or
# MIGRATION_HOLD), "by", "at", "reason" and, for a pending host job cap change, "frozen": the host families the reviewed jobs inputs select, their
# digest, the project identity and the services capacity binding. Only that field is a record. The launch preflight needs no parser: exactly "clear"
# means no hold and any other value holds every launch.
HELD = "held"
FROZEN_KEYS = {"jobs_sha256", "families", "project", "environment", "binding"}
HOLD_KEYS = {"state", "by", "at", "reason", "frozen"}
SHA256_HEX = re.compile(r"[0-9a-f]{64}")
PROJECT_NAME = re.compile(r"[a-z][a-z0-9_]{2,79}")
MALFORMED = (
    "the launch hold value is malformed, so it is not trusted and nothing was changed. Freeze the reviewed jobs inputs again with "
    "hold --migration --jobs-inputs and --jobs-sha256"
)


def finish_command(token: str) -> str:
    """The exact command that ends a pending host job cap change once the jobs apply registered every host revision."""
    return f"python -m scripts.infrastructure.services_apply finish --token '{token}' --statement '{FINISH_STATEMENT}'"


def reserved_memory(family: str) -> int | None:
    """The task memory in MiB that the host table gives a family that reserves its own size (a model set), or None for a family that reserves the whole
    job room. ``family`` is the full definition family name, ``<project>_<environment>_job_<job>_host``."""
    found = re.search(r"_job_([a-z][a-z0-9_]*)_host$", family)
    need = json.loads(HOST_TABLE.read_text())["jobs"].get(found.group(1) if found else "", {})
    return int(need["memory_mib"]) if need.get("reserve") else None


def registration_problem(definitions: dict[str, dict[str, Any]], binding: str) -> str | None:
    """None when the latest revision of every given host family is active, carries ``binding`` on its init container and has the job memory of
    ``binding`` as its task memory; else a sentence naming the first family that does not. No host family at all is a problem."""
    job = JOB_MEMORY.match(binding)
    if job is None:
        return f"the services capacity binding {binding[:80]!r} is not in the rendered form"
    if not definitions:
        return "no host job revision is registered"
    for family, found in sorted(definitions.items()):
        init = next((item for item in found.get("containerDefinitions") or [] if isinstance(item, dict) and item.get("name") == "init"), {})
        labels = init.get("dockerLabels")
        label = str(labels.get(HOST_CAPACITY_LABEL) or "") if isinstance(labels, dict) else ""
        if found.get("status") != "ACTIVE":
            return f"{family} has no active revision"
        own = reserved_memory(family)  # a model set reserves its own memory; every other family the whole job room
        expected = binding if own is None else JOB_MEMORY.sub(f"job_memory_mib={own} ", binding, count=1)
        if label != expected:
            return f"{family} is bound to {label[:120] or 'no capacity binding'}, but the services give {expected}"
        memory = job[1] if own is None else str(own)
        if str(found.get("memory", "")) != memory:
            return f"{family} is registered at {str(found.get('memory', ''))[:20]} MiB, the services give a job {memory} MiB"
    return None


def committed_switch(source: Path, name: str) -> bool:
    """The committed default of a true or false jobs input, which applies while the jobs overrides do not name it."""
    match = re.search(rf'variable "{name}" \{{.*?default\s*=\s*(true|false)\b', source.read_text(), re.DOTALL)
    if match is None:
        raise ConfigurationError(f"{source.name} has no default for {name}")
    return match.group(1) == "true"


def selected_host_families(values: dict[str, Any], prefix: str) -> list[str]:
    """The host task definition families that jobs inputs select, each as ``<prefix><family>_host``: ``host_jobs`` while ``host_jobs_enabled`` is true, and
    the publisher while ``publisher_job_enabled`` is true. An input that the jobs overrides do not name keeps its committed default in the jobs stack."""
    bad = "the host_jobs of the jobs inputs are not a list of family names"
    names: list[Any] = []
    if values.get("host_jobs_enabled", committed_switch(JOBS_HOST_VARIABLES, "host_jobs_enabled")) is True:
        selected = values.get("host_jobs")
        if selected is None:
            match = re.search(r'variable "host_jobs" \{.*?default\s*=\s*\[([^\]]*)\]', JOBS_HOST_VARIABLES.read_text(), re.DOTALL)
            if match is None:
                raise ConfigurationError(f"{JOBS_HOST_VARIABLES.name} has no default for host_jobs")
            selected = re.findall(r'"([^"]+)"', match.group(1))
        if not isinstance(selected, list):
            raise ServicesApplyError(bad)
        names.extend(selected)
    if values.get("publisher_job_enabled", committed_switch(JOBS_PUBLISHER_VARIABLES, "publisher_job_enabled")) is True:
        names.append("publisher")
    if not all(isinstance(name, str) and FAMILY_NAME.fullmatch(name) for name in names):
        raise ServicesApplyError(bad)
    return sorted({f"{prefix}{name}_host" for name in names})


def rendered_jobs_values() -> dict[str, Any]:
    """The jobs inputs the last jobs plan or apply rendered. A missing or unreadable file is an error, so finish never runs against a guessed selection."""
    try:
        rendered = json.loads(JOBS_INPUTS_RENDERED.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        raise ServicesApplyError(
            f"the rendered jobs inputs {JOBS_INPUTS_RENDERED.name} could not be read. Plan the jobs stack again with the reviewed inputs"
        ) from None
    if not isinstance(rendered, dict):
        raise ServicesApplyError(
            f"the rendered jobs inputs {JOBS_INPUTS_RENDERED.name} are not one JSON object. Plan the jobs stack again with the reviewed inputs"
        )
    return rendered


def valid_frozen(record: Any) -> bool:
    """True when ``record`` is exactly the frozen record: a complete lowercase SHA-256, a project and environment that the configuration allows, a list of
    only strings that are sorted, unique and full family names of that project and environment and, when present, a capacity binding in the rendered form.
    Every type is checked before anything is hashed or sorted, so no shape raises."""
    from scripts.infrastructure.render_jobs_config import BINDING_FORM  # one form of the binding for the renderer and the record

    if not isinstance(record, dict) or not {"jobs_sha256", "families", "project", "environment"} <= set(record) <= FROZEN_KEYS:
        return False
    digest, families, project, environment = record["jobs_sha256"], record["families"], record["project"], record["environment"]
    if not (isinstance(digest, str) and SHA256_HEX.fullmatch(digest) and isinstance(project, str) and PROJECT_NAME.fullmatch(project)):
        return False
    if environment not in ("dev", "prod") or not isinstance(families, list) or not all(isinstance(family, str) for family in families):
        return False
    if families != sorted(set(families)):
        return False
    name = re.compile(f"{re.escape(project)}_{environment}_job_[a-z][a-z0-9_]*_host")
    if not all(name.fullmatch(family) for family in families):
        return False
    return "binding" not in record or (isinstance(record["binding"], str) and BINDING_FORM.fullmatch(record["binding"]) is not None)


def hold_value(state: str, reason: str, stamp: str, record: dict[str, Any] | None = None) -> str:
    """The hold value: one JSON object with the state, the writer, the time, the reason and, for a pending change, the frozen record as its own field."""
    fields: dict[str, Any] = {"state": state, "by": "services_apply", "at": stamp, "reason": reason}
    if record is not None:
        fields["frozen"] = record
    return json.dumps(fields)


@dataclass(frozen=True)
class HoldView:
    """What a hold value says: ``pending`` while it names a host job cap change, its trusted frozen ``record`` (None when it has none), and ``malformed``
    when the value claims the structured form but is not exactly it. A malformed value counts as pending, so no plain command clears it."""

    pending: bool = False
    record: dict[str, Any] | None = None
    malformed: bool = False


def no_repeated_members(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """The ``object_pairs_hook`` of the hold value: a member name that repeats in one object, at any depth, is an error, because ``json`` would keep only
    the last repeat and could turn a pending change into a plain hold or swap its frozen selection."""
    names = [name for name, _ in pairs]
    if len(names) != len(set(names)):
        raise ValueError("a member name repeats")
    return dict(pairs)


def read_hold_value(value: str | None) -> HoldView:
    """Read a hold value. None and a plain hold are not pending. A text value of earlier code that starts with the change marker is pending without a
    record, whatever follows the marker, so text is never authority; the owner freezes the reviewed jobs inputs again."""
    if value is None:
        return HoldView()
    if not value.lstrip().startswith(("{", "[")):
        return HoldView(pending=value.startswith(MIGRATION_HOLD))
    try:
        fields = json.loads(value, object_pairs_hook=no_repeated_members)
    except ValueError:
        return HoldView(pending=True, malformed=True)
    if not isinstance(fields, dict) or not {"state", "by", "at", "reason"} <= set(fields) <= HOLD_KEYS:
        return HoldView(pending=True, malformed=True)
    if not all(isinstance(fields[key], str) for key in ("state", "by", "at", "reason")) or fields["state"] not in (HELD, MIGRATION_HOLD):
        return HoldView(pending=True, malformed=True)
    pending = fields["state"] == MIGRATION_HOLD
    if "frozen" in fields and not (pending and valid_frozen(fields["frozen"])):
        return HoldView(pending=True, malformed=True)
    return HoldView(pending=pending, record=dict(fields["frozen"]) if "frozen" in fields else None)


def jobs_registration_problem(cli: Any, settings: dict[str, str], frozen: dict[str, Any] | None) -> str | None:
    """Check the host families that the change froze when it started, never a later render. Refuse when nothing was frozen; return a sentence when the
    project identity or the services binding differ from the frozen ones, when a frozen family has no active latest revision at the frozen binding
    (``registration_problem``), or when the rendered jobs inputs disagree with the frozen selection, binding or identity; else None."""
    from scripts.infrastructure.render_jobs_config import host_job_room  # reads the services outputs only when finish first needs them

    if frozen is None:
        raise ServicesApplyError(
            "the host job cap change has no frozen jobs selection (a hold that earlier code wrote as text carries none that is trusted), so finish has "
            "nothing to check. Run hold --migration with --jobs-inputs and --jobs-sha256, then settle --keep-hold with the lock token, then the jobs apply"
        )
    binding = frozen.get("binding")
    if not binding:
        raise ServicesApplyError(
            "the frozen record has no services capacity binding yet. Run settle --keep-hold with the lock token once the services are stable"
        )
    if (frozen["project"], frozen["environment"]) != (settings["project_name"], settings["environment"]):
        return f"the change froze project {frozen['project']} environment {frozen['environment']}, not {settings['project_name']} {settings['environment']}"
    _, current = host_job_room()
    if current != binding:
        return f"the services now give {current}, but the change froze {binding}"
    prefix = f"{settings['project_name']}_{settings['environment']}_job_"
    expected = sorted(frozen["families"])
    try:
        listed = cli.call_bounded("ecs", "list-task-definition-families", ["--family-prefix", prefix, "--status", "ACTIVE"], timeout=CALL_SECONDS)
        active = {str(name) for name in listed.get("families", [])}
        definitions = {
            name: cli.call_bounded("ecs", "describe-task-definition", ["--task-definition", name], timeout=CALL_SECONDS).get("taskDefinition", {})
            if name in active
            else {}
            for name in expected
        }
    except StorageError as error:
        raise ServicesApplyError(f"the registered host revisions could not be read ({error.code})") from None
    problem = registration_problem(definitions, binding)
    if problem is not None:
        return problem
    rendered = rendered_jobs_values()
    if (rendered.get("project_name"), rendered.get("environment")) != (settings["project_name"], settings["environment"]):
        return "the rendered jobs inputs belong to another project identity than the change froze"
    selected = selected_host_families(rendered, prefix)
    if selected != expected:
        differing = sorted(set(selected) ^ set(expected))
        return f"the rendered jobs inputs select other host families than the change froze (they differ on {', '.join(differing)})"
    if expected and rendered.get("host_capacity_binding") != binding:
        return f"the rendered jobs inputs carry the binding {str(rendered.get('host_capacity_binding'))[:120]}, not the frozen {binding}"
    return None


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
    settled (it raises on a failed rollout), and ``report`` prints one line per finished step. ``freeze`` turns the path and digest of the reviewed jobs
    inputs into the frozen record that the hold of a host job cap change carries, and ``binding`` reads the services capacity binding."""

    def __init__(
        self,
        put: Callable[[str], None],
        get: Callable[[], str | None],
        stable: Callable[[], bool],
        clock: Any = None,
        report: Callable[[str], None] | None = None,
        lock: LockStore | None = None,
        registered: Callable[[], str | None] | None = None,
        binding: Callable[[], str] | None = None,
        freeze: Callable[[str, str], dict[str, Any]] | None = None,
    ) -> None:
        self.put, self.get, self.stable, self.lock_store = put, get, stable, lock
        self.registered = registered
        self.binding, self.freeze = binding, freeze
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

    def read_hold(self) -> str | None:
        """The hold value. An unreadable hold raises, so it is never read as clear."""
        try:
            return self.get()
        except Exception as error:  # a refused or failed read leaves the hold unknown
            raise ServicesApplyError(f"the launch hold could not be read ({type(error).__name__}: {str(error)[:200]})") from None

    def hold_view(self) -> HoldView:
        """The current hold value, read."""
        return read_hold_value(self.read_hold())

    def pending_change(self) -> bool:
        """True while the hold is the host job cap change form, which only ``finish`` ends."""
        return self.hold_view().pending

    def pending_record(self) -> dict[str, Any] | None:
        """The frozen record of the pending host job cap change, or None when the hold is not that form or carries none. A malformed hold refuses."""
        view = self.hold_view()
        if view.malformed:
            raise ServicesApplyError(MALFORMED)
        return view.record

    @staticmethod
    def pending_refusal(action: str, token: str | None) -> ServicesApplyError:
        """The refusal of a plain ``action`` while a host job cap change awaits its jobs apply, with the exact command that ends it."""
        end = f"After the jobs apply, run {finish_command(token)}" if token else "The apply lock is missing; read status and start a new services apply"
        return ServicesApplyError(
            f"{action} does not clear the launch hold of a host job cap change, because the jobs apply is pending. The hold stays set. {end}"
        )

    def hold(self, reason: str, migration: bool = False, frozen: dict[str, Any] | None = None) -> None:
        """Set the hold before the apply changes anything, then wait the settle window so a RunTask that a fit check allowed just before has its reply.
        With ``migration`` the hold names a host job cap change, which only ``finish`` ends; ``frozen`` is then the record of the reviewed jobs inputs that
        the change starts with. It replaces an earlier record only when it keeps every frozen family and the identity. A hold over a pending host job cap
        change keeps its name and its record, so a plain hold never turns it into a hold that settle or resolve can clear. The reason is a field of its
        own, so no reason text can become a record."""
        stamp = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        if frozen is not None and not migration:
            raise ServicesApplyError("a frozen jobs selection belongs only to the hold of a host job cap change (--migration)")
        view = self.hold_view()
        if view.malformed and frozen is None:
            raise ServicesApplyError(MALFORMED)  # fresh reviewed inputs repair it; nothing else carries a malformed value over
        record: dict[str, Any] | None
        if frozen is not None:
            existing = view.record
            dropped = sorted(set(existing["families"]) - set(frozen["families"])) if existing is not None else []
            if existing is not None and (dropped or (existing["project"], existing["environment"]) != (frozen["project"], frozen["environment"])):
                raise ServicesApplyError(
                    f"the pending host job cap change froze {', '.join(dropped) or 'another project identity'}, which these jobs inputs do not keep. A "
                    "continued hold must keep every frozen family and the identity. To deselect a family, apply the original selection, run finish, "
                    "then deselect in a separate jobs change. Nothing was changed"
                )
            named, record = True, frozen
        else:
            named, record = migration or view.pending, view.record
        self.write(hold_value(MIGRATION_HOLD if named else HELD, reason.strip()[:200], stamp, record if named else None))
        self.clock.sleep(HOLD_SETTLE_SECONDS)
        self.report(f"launch hold: set, waited {HOLD_SETTLE_SECONDS} s for any RunTask already allowed")

    def lock(self, continue_token: str | None = None) -> str:
        """Take the apply lock before the hold: create the lock parameter without overwrite, valued with a new token, and return the token. While
        another apply holds it AWS refuses the create, and this raises naming the holder; nothing changes. With ``continue_token`` it takes no new lock:
        it returns that token while it holds the lock and a host job cap change is pending, so a failed jobs apply can be answered with another services
        apply (a fix or a restore) under the same lock; anything else raises."""
        if self.lock_store is None:
            raise ServicesApplyError("no apply lock store is configured")
        if continue_token is not None:
            if TOKEN.fullmatch(continue_token) is None or self.lock_store.get() != continue_token or not self.pending_change():
                raise ServicesApplyError("no host job cap change is pending under that apply lock token, so there is nothing to continue. Nothing was changed")
            return continue_token
        token = new_token()
        try:
            self.lock_store.create(token)
        except LockHeld:
            holder = self.lock_store.get()
            if holder and self.pending_change():
                raise ServicesApplyError(
                    f"a host job cap change holds the apply lock ({holder}), so this apply did not start. After its jobs apply, run {finish_command(holder)}"
                ) from None
            raise ServicesApplyError(
                f"another services apply holds the apply lock ({holder}), so this apply did not start. Wait for it to finish. If it failed, check it "
                f"and run {RESOLVE_COMMAND} --token '{holder}'"
            ) from None
        except Exception as error:  # a refused or failed create: the lock is not known to be this apply's
            raise ServicesApplyError(f"the apply lock could not be taken ({type(error).__name__}: {str(error)[:200]})") from None
        return token

    def require_lock(self, token: str | None) -> tuple[LockStore, str]:
        """Refuse unless an apply lock store is configured and the lock holds exactly this well-formed token; return the store and the token."""
        if self.lock_store is None:
            raise ServicesApplyError("no apply lock store is configured, so the launch hold is not changed")
        if not token or TOKEN.fullmatch(token) is None:
            raise ServicesApplyError(f"{token!r} is not an apply lock token. Nothing was changed")
        found = self.lock_store.get()
        if found != token:
            raise ServicesApplyError(f"the apply lock is held by {found or 'no apply'}, not by {token}. Nothing was changed")
        return self.lock_store, token

    def release(self, token: str) -> None:
        """Delete the apply lock only while it still holds this token. While it holds this token no other apply can create it (the create has no
        overwrite), so the read and the delete cannot remove another apply's lock. The one window left: someone deletes the lock by hand (outside
        this command) and another apply creates it between this read and this delete; then this delete removes that apply's lock."""
        store, _ = self.require_lock(token)
        store.delete()
        self.report("apply lock: released")

    def services_settled(self) -> bool:
        """True once every ECS service is settled. A missing services output is a configuration error; any other failure is not settled."""
        try:
            return bool(self.stable())
        except ConfigurationError:
            raise
        except Exception as error:  # the same refusal as in wait_stable
            raise ServicesApplyError(f"the services are not stable ({type(error).__name__}). The launch hold stays set") from None

    def wait_stable(self, token: str) -> None:
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
                    f"services, then run {RESOLVE_COMMAND} --token '{token}'"
                ) from None
            if settled:
                return
            if self.clock.monotonic() + POLL_SECONDS > end:
                raise ServicesApplyError(
                    f"the services did not reach steady state within {STABLE_SECONDS} s. The launch hold stays set; check the services, then run "
                    f"{RESOLVE_COMMAND} --token '{token}'"
                )
            self.clock.sleep(POLL_SECONDS)

    def settle(self, token: str | None, keep_hold: bool = False) -> None:
        """After a successful apply: require this apply's own token on the apply lock, wait for steady state, clear the hold, then delete the lock.
        No token, a malformed one or a lock held by another token changes nothing. With ``keep_hold`` it waits for steady state and then leaves the
        hold and the lock set, for a change that needs the jobs stack applied next (the host job cap); ``finish`` with the token ends them."""
        _, token = self.require_lock(token)
        if not keep_hold and self.pending_change():
            raise self.pending_refusal("settle", token)
        self.wait_stable(token)
        if keep_hold:
            self.hold_for_change(token)
            return
        self.write(LAUNCH_HOLD_CLEAR)
        self.report("launch hold: cleared after the services reached steady state")
        self.release(token)

    def hold_for_change(self, token: str) -> None:
        """Rewrite the hold to the host job cap change form after the services reached steady state, and keep the lock until ``finish``. The frozen record
        stays and gains the services capacity binding, which is final once the services are stable."""
        record = self.pending_record()
        if record is not None and self.binding is not None:
            record = {**record, "binding": self.binding()}
        stamp = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        self.write(hold_value(MIGRATION_HOLD, "jobs apply pending", stamp, record))
        self.report(f"launch hold: kept set for a host job cap change. After the jobs apply, run {finish_command(token)}")

    def finish(self, token: str | None, statement: str) -> None:
        """End a host job cap change: with this apply's own token, a pending change, stable services and every host revision registered at the
        current services binding (the jobs apply done), clear the hold and delete the lock. Anything else changes nothing and says what is missing."""
        if not statement.strip():
            raise ServicesApplyError("finish needs --statement with what you checked. The launch hold stays set")
        _, token = self.require_lock(token)
        if not self.pending_change():
            raise ServicesApplyError(
                "no host job cap change is pending, so there is nothing to finish. Use settle after an apply or resolve after a failed one"
            )
        if not self.services_settled():
            raise ServicesApplyError(f"the services are not stable yet. The launch hold stays set; run {finish_command(token)} again once they are")
        if self.registered is None:
            raise ServicesApplyError("no check of the registered host revisions is configured, so the launch hold stays set")
        try:
            problem = self.registered()
        except ServicesApplyError:
            raise
        except Exception as error:  # a refused read or an unreadable services output leaves the registration unknown
            raise ServicesApplyError(
                f"the registered host revisions could not be checked ({type(error).__name__}: {str(error)[:200]}). The launch hold stays set"
            ) from None
        if problem is not None:
            raise ServicesApplyError(
                f"the jobs apply has not registered every host revision at the current services ({problem}). The launch hold stays set; apply the jobs "
                f"stack, then run {finish_command(token)}"
            )
        self.write(LAUNCH_HOLD_CLEAR)
        self.report(f"launch hold: cleared after the host revisions matched the services ({statement.strip()[:200]})")
        self.release(token)

    def resolve(self, statement: str, token: str | None = None) -> None:
        """After a failed apply or settle, with the owner's statement of what was checked. No path writes "clear" without owning the apply lock:
        with ``token`` the lock must hold it (the failed apply's lock); without one, resolve takes the lock itself first (create without
        overwrite), so while another apply holds it nothing changes. Then, only while every service is settled, it clears the hold and deletes the
        lock it owns. A lock resolve took itself is deleted again when the services are not settled; the failed apply's lock stays."""
        if not statement.strip():
            raise ServicesApplyError("resolve needs --statement with what you checked. The launch hold stays set")
        if token is not None:
            self.require_lock(token)
            if self.pending_change():
                raise self.pending_refusal("resolve", token)
            owned, taken = token, False
        else:
            if self.pending_change():
                raise self.pending_refusal("resolve", self.lock_store.get() if self.lock_store is not None else None)
            owned, taken = self.lock(), True
        try:
            settled = self.services_settled()
        except BaseException:
            if taken:
                self.release(owned)
            raise
        if not settled:
            if taken:
                self.release(owned)
            raise ServicesApplyError("the services are not stable yet. The launch hold stays set; run resolve again once they are")
        self.write(LAUNCH_HOLD_CLEAR)
        self.report(f"launch hold: cleared by the owner ({statement.strip()[:200]})")
        self.release(owned)


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

    def services_binding() -> str:
        from scripts.infrastructure.render_jobs_config import host_job_room  # reads the services outputs, which settle needs once the services are stable

        return host_job_room()[1]

    def freeze(path: str, sha256: str) -> dict[str, Any]:
        from scripts.infrastructure.capture_reader import CaptureError
        from scripts.infrastructure.render_jobs_config import preparation_values

        try:
            values = preparation_values(Path(path), sha256, "jobs")
        except CaptureError as error:
            raise ConfigurationError(str(error)) from None
        prefix = f"{settings['project_name']}_{settings['environment']}_job_"
        return {
            "jobs_sha256": sha256,
            "families": selected_host_families(values, prefix),
            "project": settings["project_name"],
            "environment": settings["environment"],
        }

    steps = Steps(
        put=put, get=get, stable=stable, lock=LockStore(create=lock_create, get=lock_get, delete=lock_delete), binding=services_binding, freeze=freeze
    )
    steps.registered = lambda: jobs_registration_problem(cli, settings, steps.pending_record())
    return steps


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    lock = commands.add_parser("lock", help="take the apply lock before the hold and print its token (terraform.sh runs it)")
    lock.add_argument("--reason", required=True, help="what is being applied, for example the saved plan")
    lock.add_argument("--continue-token", help="continue under the lock of a pending host job cap change instead of taking a new one")
    hold = commands.add_parser("hold", help="set the launch hold before a services apply (terraform.sh runs it)")
    hold.add_argument("--reason", required=True, help="what is being applied, for example the saved plan")
    hold.add_argument("--migration", action="store_true", help="name a host job cap change, which only finish ends")
    hold.add_argument("--jobs-inputs", help="with --migration: the reviewed jobs inputs file, whose host families the change freezes for finish")
    hold.add_argument("--jobs-sha256", help="with --jobs-inputs: the SHA-256 of that file's bytes")
    settle = commands.add_parser("settle", help="after a successful services apply: wait for steady state, clear the hold, release the lock")
    settle.add_argument("--token", required=True, help="the apply lock token that lock printed")
    settle.add_argument("--keep-hold", action="store_true", help="wait for steady state but keep the hold and the lock until finish (the jobs apply follows)")
    finish = commands.add_parser(
        "finish", help="after the jobs apply of a host job cap change: clear the hold and the lock once every host revision matches the services"
    )
    finish.add_argument("--token", required=True, help="the apply lock token that settle --keep-hold printed")
    finish.add_argument("--statement", required=True, help="what you checked")
    resolve = commands.add_parser("resolve", help="after a failed apply or settle: clear the hold and the lock once the services are stable")
    resolve.add_argument("--statement", required=True, help="what you checked")
    resolve.add_argument("--token", help="the apply lock token, required while a lock is held")
    commands.add_parser("status", help="print the launch hold and the apply lock, read only")
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run one subcommand: 0 on success, 1 on a failed step (the hold stays as it was), 2 on a configuration error."""
    parser = build_parser()
    arguments = parser.parse_args(argv)
    if arguments.command == "hold":
        if (arguments.jobs_inputs is None) != (arguments.jobs_sha256 is None):
            parser.error("--jobs-inputs and --jobs-sha256 go together")
        if arguments.jobs_inputs is not None and not arguments.migration:
            parser.error("--jobs-inputs and --jobs-sha256 belong to the hold of a host job cap change (--migration)")
    try:
        steps = open_steps()
        if arguments.command == "lock":
            sys.stdout.write(f"{steps.lock(continue_token=arguments.continue_token)}\n")
        elif arguments.command == "hold":
            if arguments.jobs_inputs is not None and steps.freeze is None:
                raise ServicesApplyError("no reader of the reviewed jobs inputs is configured")
            frozen = steps.freeze(arguments.jobs_inputs, arguments.jobs_sha256) if arguments.jobs_inputs is not None and steps.freeze is not None else None
            steps.hold(arguments.reason, migration=arguments.migration, frozen=frozen)
        elif arguments.command == "finish":
            steps.finish(arguments.token, arguments.statement)
        elif arguments.command == "settle":
            steps.settle(arguments.token, keep_hold=arguments.keep_hold)
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
