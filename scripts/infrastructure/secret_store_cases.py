"""Offline cases for the Parameter Store module and the Terraform names, against a loopback stand-in; no AWS service is contacted.

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

from botocore.exceptions import ClientError

from scripts.infrastructure import render_project_config, secret_store
from scripts.infrastructure.secret_store_stand_in import StandIn

REPO_ROOT = Path(__file__).resolve().parents[2]
SECRET_RESOURCE = re.compile(r'^\s*(resource|data|ephemeral)\s+"(aws_secretsmanager_[a-z_]*|aws_ssm_parameter[a-z_]*)"', re.MULTILINE)
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
        missing = refusal(lambda: store.get(secret_store.MARQUEZ_DB_PASSWORD))
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
        "a missing secret fails and names it": missing is not None and secret_store.MARQUEZ_DB_PASSWORD in missing,
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
            [sentinel, "winner-" + sentinel], [Path(tempfile.gettempdir()), root], began
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
            timer.join()

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
    names = [secret_store.POSTGRES_PASSWORD, secret_store.POLARIS_ROOT_SECRET, secret_store.MARQUEZ_DB_PASSWORD]
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


def source_offenders(root: Path) -> list[str]:
    """The Terraform files that declare a secret or a parameter; a data source of a public AWS parameter (``/aws/service/...``) is no offender."""
    found = []
    for path in sorted((root / "infra").rglob("*.tf")):
        if ".terraform" in path.parts:
            continue
        text = path.read_text()
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
        "the 14 variable defaults equal the module's managed names in order": declared == list(secret_store.MANAGED),
        "the 14 managed names and the read-only name are all different": len(set(ordered)) == len(ordered) == 15,
        "every name is a slash path under the project prefix with underscores": all(
            re.fullmatch(r"/healthcare_hai_prediction/[a-z0-9_]+", name) for name in ordered
        ),
        "the shared keys keep their Secrets Manager names and are not parameters": sorted(name or "" for name in shared_defaults) == sorted(secret_store.SHARED)
        and not any(name.startswith("/") for name in secret_store.SHARED),
        "no Terraform file declares a secret, a secret version, a parameter or a secret data source": not offenders,
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
