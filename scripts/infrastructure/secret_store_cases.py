"""Offline cases for the Secrets Manager module and the Terraform names, against a loopback stand-in; no AWS service is contacted.

Run from the repository root: ``.venv/bin/python -m scripts.infrastructure.secret_store_cases [--root <folder>]``. The cases are
one process, take seconds and start no container. ``scripts/infrastructure/run_secret_store_e2e.py`` runs them and keeps the report.
"""

from __future__ import annotations

import argparse
import contextlib
import io
import json
import os
import re
import sys
import tempfile
import threading
import time
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any

from scripts.infrastructure import render_project_config, secret_store
from scripts.infrastructure.secret_store_stand_in import StandIn

REPO_ROOT = Path(__file__).resolve().parents[2]
SECRET_RESOURCE = re.compile(r'^\s*(resource|data|ephemeral)\s+"aws_secretsmanager_[a-z_]*"', re.MULTILINE)
NAME = secret_store.POSTGRES_PASSWORD

Case = Callable[[Path], dict[str, bool]]


def files_holding(values: Iterable[str], roots: Iterable[Path], since: float) -> list[str]:
    """Every file under the roots written since ``since`` whose bytes hold one of the values."""
    needles = [value.encode() for value in values if len(value) > 8]
    found = []
    for root in roots:
        for folder, names, files in os.walk(root):
            names[:] = [name for name in names if name not in (".git", ".venv", ".tools", "node_modules", ".terraform")]
            for name in files:
                path = Path(folder) / name
                try:
                    if path.is_symlink() or path.stat().st_mtime < since or path.stat().st_size > 8 * 1024 * 1024:
                        continue
                    content = path.read_bytes()
                except OSError:
                    continue
                if any(needle in content for needle in needles):
                    found.append(str(path))
    return found


def refusal(call: Callable[[], Any]) -> str | None:
    """The text of the secret-store error a call raises, or ``None`` when it succeeds."""
    try:
        call()
    except secret_store.SecretStoreError as error:
        return str(error)
    return None


def store_behaviour(root: Path) -> dict[str, bool]:
    """First use creates, a rerun changes nothing, an empty value is filled, a missing permission names the secret and no value is in any message."""
    began = time.time() - 1
    sentinel = "sentinel-" + os.urandom(8).hex()
    printed = io.StringIO()
    with StandIn() as stand_in, contextlib.redirect_stdout(printed), contextlib.redirect_stderr(printed):
        store = secret_store.open_store(root, stand_in.endpoint)
        value, created = store.ensure(NAME, lambda: sentinel)
        writes_first = stand_in.writes()
        again, created_again = store.ensure(NAME, lambda: "must-not-be-used")
        writes_rerun = stand_in.writes()
        stand_in.values[secret_store.AIRFLOW_FERNET_KEY] = ""
        writes_before_empty = stand_in.writes()
        empty_refusal = refusal(lambda: store.ensure(secret_store.AIRFLOW_FERNET_KEY, lambda: sentinel + "-refused"))
        empty_unchanged = stand_in.writes() == writes_before_empty and stand_in.values[secret_store.AIRFLOW_FERNET_KEY] == ""
        filled_now = store.fill_empty(secret_store.AIRFLOW_FERNET_KEY, sentinel + "-filled")
        filled_again = store.fill_empty(secret_store.AIRFLOW_FERNET_KEY, sentinel + "-must-not-replace")
        filled = stand_in.values[secret_store.AIRFLOW_FERNET_KEY]
        store.put(secret_store.AIRFLOW_JWT_SECRET, sentinel + "-one")
        store.put(secret_store.AIRFLOW_JWT_SECRET, sentinel + "-two")
        missing = refusal(lambda: store.get(secret_store.MARQUEZ_DB_PASSWORD))
        before_wrong = len(stand_in.log)
        wrong = refusal(lambda: store.put("other/name", sentinel))
        wrong_read = refusal(lambda: store.find("healthcare-hai-prediction/airflow-admin"))
        requests_for_wrong = len(stand_in.log) - before_wrong
        stand_in.denied.add("CreateSecret")
        denied_create = refusal(lambda: store.ensure(secret_store.POLARIS_ROOT_SECRET, lambda: sentinel + "-denied"))
        stand_in.denied.clear()
        stand_in.denied.add("GetSecretValue")
        denied_read = refusal(lambda: store.get(NAME))
        stand_in.denied.clear()
        real_create = store.client.create_secret

        def racing(**arguments: Any) -> Any:
            stand_in.values[arguments["Name"]] = "winner-" + sentinel
            return real_create(**arguments)

        store.client.create_secret = racing
        raced, raced_created = store.ensure(secret_store.POLARIS_READER_CLIENT_ID, lambda: "loser-" + sentinel)
        store.client.create_secret = real_create
        stored_names = set(stand_in.values)
        refused_endpoint = refusal(lambda: secret_store.open_store(root, "http://example.org:4566"))
        ignores_endpoint_settings = secret_store.CLIENT_OPTIONS.get("ignore_configured_endpoint_urls") is True and secret_store.client_config() is not None
        messages = [str(text) for text in (missing, wrong, wrong_read, denied_create, denied_read, refused_endpoint, empty_refusal)]
    return {
        "first use creates the secret with the generated value": created and value == sentinel and writes_first == 1,
        "a rerun reads the stored value and changes nothing": not created_again and again == sentinel and writes_rerun == writes_first,
        "ensure refuses an existing empty secret and writes nothing": empty_refusal is not None
        and secret_store.AIRFLOW_FERNET_KEY in empty_refusal
        and empty_unchanged,
        "the recovery operation fills an empty secret once and never replaces a value": filled_now and not filled_again and filled == sentinel + "-filled",
        "a new value replaces the old one": stand_in.values[secret_store.AIRFLOW_JWT_SECRET] == sentinel + "-two",
        "a missing secret fails and names it": missing is not None and secret_store.MARQUEZ_DB_PASSWORD in missing,
        "a name outside the 13 managed names is refused before any request": wrong is not None and wrong_read is not None and requests_for_wrong == 0,
        "a missing create permission fails visibly and names the secret and the action": denied_create is not None
        and secret_store.POLARIS_ROOT_SECRET in denied_create
        and "creating" in denied_create,
        "a missing read permission fails visibly and names the secret": denied_read is not None and NAME in denied_read,
        "a concurrent create leaves the winner's value": not raced_created and raced == "winner-" + sentinel,
        "the client allows two requests in all, not two retries": secret_store.CLIENT_OPTIONS["retries"] == {"total_max_attempts": 2, "mode": "standard"},
        "only managed names reach the stand-in": stored_names <= set(secret_store.MANAGED),
        "an endpoint off the loopback address is refused": refused_endpoint is not None,
        "the client ignores endpoint URLs from the environment and the AWS files": ignores_endpoint_settings,
        "no message holds a secret value": not any(sentinel in message for message in messages),
        "nothing is printed": printed.getvalue() == "",
        "no file under the scratch folder or the repository holds a secret value": not files_holding(
            [sentinel, "winner-" + sentinel], [Path(tempfile.gettempdir()), root], began
        ),
    }


def store_races(root: Path) -> dict[str, bool]:
    """Two initializers get one value; a loser waits for the winner and never writes; an empty secret is never filled by ensure."""
    sentinel = "race-" + os.urandom(8).hex()
    with StandIn() as stand_in:
        store = secret_store.open_store(root, stand_in.endpoint)
        barrier = threading.Barrier(2)
        returned: dict[str, tuple[str, bool]] = {}
        real_create = store.client.create_secret

        def meet(**arguments: Any) -> Any:
            with contextlib.suppress(threading.BrokenBarrierError):
                barrier.wait(timeout=5)
            return real_create(**arguments)

        store.client.create_secret = meet

        def initialize(label: str) -> None:
            returned[label] = store.ensure(NAME, lambda: f"{sentinel}-{label}")

        threads = [threading.Thread(target=initialize, args=(label,)) for label in ("one", "two")]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)
        store.client.create_secret = real_create
        stored = stand_in.values.get(NAME)
        creates = [entry for entry in stand_in.changed() if entry[1] == NAME]
        values = {value for value, _ in returned.values()}
        created_flags = sorted(flag for _, flag in returned.values())

        valueless = secret_store.AIRFLOW_JWT_SECRET
        stand_in.valueless.add(valueless)
        valueless_refusal = refusal(lambda: store.ensure(valueless, lambda: sentinel + "-valueless"))
        valueless_unchanged = stand_in.changed() == creates

        slow = secret_store.POLARIS_ROOT_SECRET
        timers: list[threading.Timer] = []

        def collide_late(**arguments: Any) -> Any:
            stand_in.values[arguments["Name"]] = ""
            timers.append(threading.Timer(0.6, lambda: stand_in.values.__setitem__(slow, "late-" + sentinel)))
            timers[0].start()
            return real_create(**arguments)

        store.client.create_secret = collide_late
        began = time.monotonic()
        waited, waited_created = store.ensure(slow, lambda: "must-not-be-used", wait=5)
        waited_seconds = time.monotonic() - began
        for timer in timers:
            timer.join()

        stuck = secret_store.POLARIS_READER_CLIENT_ID

        def collide(**arguments: Any) -> Any:
            stand_in.values[arguments["Name"]] = ""
            return real_create(**arguments)

        store.client.create_secret = collide
        changes_before = len(stand_in.changed())
        began = time.monotonic()
        never = refusal(lambda: store.ensure(stuck, lambda: sentinel + "-never", wait=0.6))
        never_seconds = time.monotonic() - began
        store.client.create_secret = real_create
        never_wrote = len(stand_in.changed()) == changes_before and stand_in.values[stuck] == ""
    return {
        "two initializers return one value": len(values) == 1 and stored in values,
        "exactly one initializer created the secret and the stored value is the one returned": created_flags == [False, True] and len(creates) == 1,
        "an existing secret without a value is refused by ensure and left unchanged": valueless_refusal is not None and valueless_unchanged,
        "a loser waits for the winner's value and returns it without writing": waited == "late-" + sentinel
        and not waited_created
        and 0.5 <= waited_seconds < 4,
        "a winner that never writes a value ends the wait at the deadline and nothing is written": never is not None
        and secret_store.POLARIS_READER_CLIENT_ID in never
        and 0.5 <= never_seconds < 4
        and never_wrote,
    }


def store_deadline(root: Path) -> dict[str, bool]:
    """A stalled fetch returns at the deadline naming each unread secret, and a healthy fetch returns every value."""
    sentinel = "fetch-" + os.urandom(8).hex()
    names = [secret_store.POSTGRES_PASSWORD, secret_store.POLARIS_ROOT_SECRET, secret_store.MARQUEZ_DB_PASSWORD]
    with StandIn({names[0]: sentinel + "-a", names[1]: sentinel + "-b"}) as stand_in:
        healthy = secret_store.fetch_all(root, stand_in.endpoint, names, 10)
        stand_in.stalled.add("GetSecretValue")
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


def terraform_names(root: Path) -> dict[str, bool]:
    """The module's names equal the Terraform defaults, and no Terraform file declares a secret or a secret version."""
    declared = secret_store.terraform_defaults(root)
    offenders = [
        str(path.relative_to(root))
        for path in sorted((root / "infra").rglob("*.tf"))
        if ".terraform" not in path.parts and SECRET_RESOURCE.search(path.read_text())
    ]
    family = ("aws_secretsmanager_secret", "aws_secretsmanager_secret_version", "aws_secretsmanager_random_password", "aws_secretsmanager_secrets")
    return {
        "the source check matches every kind of block of the Secrets Manager types": all(
            SECRET_RESOURCE.search(f'{kind} "{name}" "x" {{}}') for kind in ("resource", "data", "ephemeral") for name in family
        ),
        "the 13 variable defaults equal the module's names in order": declared == list(secret_store.MANAGED),
        "the 13 names are different": len(set(secret_store.MANAGED)) == 13,
        "every name sits under the project prefix": all(name.startswith(secret_store.PREFIX) for name in secret_store.MANAGED),
        "no Terraform file declares a secret, a secret version or a secret data source": not offenders,
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
    """The saved-plan check accepts a plan without Secrets Manager objects and rejects every type of that family it meets."""
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
    refusals = [plan_refusal(plan) for plan in (with_resource, with_version, with_data, with_data_state, with_password)]
    return {
        "a plan without Secrets Manager objects is accepted": plan_refusal(clean_plan()) is None,
        "a plan that adds a secret resource is refused and names it": refusals[0] is not None and "aws_secretsmanager_secret.planted" in refusals[0],
        "a plan that adds a secret version inside a module is refused": refusals[1] is not None and "aws_secretsmanager_secret_version.planted" in refusals[1],
        "a plan that reads a secret through a data source is refused": refusals[2] is not None
        and "data.aws_secretsmanager_secret_version.planted" in refusals[2],
        "a data source only in the prior state is refused": refusals[3] is not None,
        "a plan with the random password data source is refused and names it": refusals[4] is not None
        and "data.aws_secretsmanager_random_password.planted" in refusals[4],
    }


CASES: dict[str, Case] = {
    "store_behaviour": store_behaviour,
    "store_races": store_races,
    "store_deadline": store_deadline,
    "terraform_names": terraform_names,
    "saved_plan": saved_plan,
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
