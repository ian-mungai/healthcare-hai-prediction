"""Create the serving secrets once in Parameter Store and give the model API's MLflow user its READ grants; no value is printed, logged or kept in a file.

Run from the repository root with the project profile; the owner opens a port forward to the MLflow tracking server first (``--mlflow-url`` must be a
loopback address)::

    .venv/bin/python -m scripts.infrastructure.create_serving_secrets --hai-types HAI_1,HAI_2 --mlflow-url http://127.0.0.1:5000

1. ``grafana_admin_password``: a random SecureString, created when absent.
2. ``streamlit_model_api_key`` (random) and ``model_api_key_hashes``, the JSON object ``{"streamlit": "<SHA-256 of the key>"}`` that
   ``scripts/serving/keys.py`` reads. Both are created when absent. Hashes that already exist are never changed: without a ``streamlit`` entry, with another
   hash or with a page key that is missing, the tool refuses and names the parameter.
3. ``model_api_mlflow_user`` (value ``model_api``) and ``model_api_mlflow_credential`` (random), created when absent. With ``--mlflow-url`` the tool then
   reads the administrator password from ``mlflow_admin_password`` into memory only, creates the MLflow user (the credential travels in a JSON body, never in
   an address or the command line) and grants READ on registered model ``hai_model_<T>`` and experiment ``hai_training_<T>`` of each served type.
   A model or experiment that does not exist yet (or an experiment in the trash) is reported ``pending``; run the tool again after the first training run.
   A permission of another kind that the user already holds is reported and never changed; an explicit DENY or NO_PERMISSIONS row is a refusal.

The served types come from ``--hai-types`` (the ``MODEL_API_HAI_TYPES`` text), else ``--preparation-inputs`` (the reviewed services input file, key
``model_api_hai_types``), else the ``MODEL_API_HAI_TYPES`` environment variable. The output says ``created`` or ``present`` for each secret and gives one line
for each MLflow object. A secret that cannot be set refuses by name, the other independent values are still made and the exit code is 1.

MLflow API, read from the 3.17.2 wheel (mlflow-3.17.2-py3-none-any.whl, SHA-256 4e44c03791beb518d8b0a40b0982d958c2634d902f46d67d7907c82107f21fac)
under ``mlflow/server/auth/``:

- ``routes.py:7`` ``CREATE_USER`` is ``/api/2.0/mlflow/users/create``; ``__init__.py:7264`` ``create_user`` needs a JSON body with ``username`` and
  ``password`` (``:7265``), is routed at ``:8949`` and gated by ``validate_can_create_user`` (``:4713``).
- ``routes.py:23`` ``GRANT_USER_PERMISSION`` is ``/api/3.0/mlflow/users/permissions/grant``; ``__init__.py:7482`` ``grant_user_permission`` reads
  ``username``, ``resource_type``, ``resource_id`` and ``permission`` and is routed at ``:8881``. ``sqlalchemy_store.py:448-481`` inserts one row and raises
  ``RESOURCE_ALREADY_EXISTS`` when the user already has a row on the resource, so a grant never changes a permission.
- ``routes.py:27`` ``GET_USER_PERMISSION`` is ``/api/3.0/mlflow/users/permissions/get``; ``__init__.py:7503`` ``get_user_permission`` answers
  ``{"allowed", "permission"}`` and answers the server default for a resource that does not exist (``:7508-7510``), so the tool checks that the object exists
  first. ``permissions.py:24-76`` lists the permission names READ, USE, EDIT, MANAGE, DENY and NO_PERMISSIONS; the resource types are ``registered_model``
  and ``experiment`` (``permissions.py:183``), and an experiment is named by its ID.
- ``handlers.py:8617`` (``GetExperimentByName``, missing: ``RESOURCE_DOES_NOT_EXIST`` at ``:1869-1874``) and ``handlers.py:8658``
  (``GetRegisteredModel``) are the lookups for an object's existence.
- ``/api/2.0/mlflow/registered-models/permissions/*`` and ``/api/2.0/mlflow/experiments/permissions/*`` are in neither ``routes.py`` nor ``__init__.py``:
  3.17.2 removed them and a call gets a 404 HTML page. The tool never calls them.

The tool cannot check that an MLflow user that already exists has the password stored in ``model_api_mlflow_credential``: it never changes a user.
"""

from __future__ import annotations

import argparse
import base64
import functools
import http.client
import json
import os
import re
import secrets
import sys
import time
import urllib.parse
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

from scripts.infrastructure import secret_store
from scripts.sdk_logs import quiet_sdk_logs
from scripts.serving import keys

REPO_ROOT = Path(__file__).resolve().parents[2]
CONTRACT = "data_contracts/training/model_contract.json"
MLFLOW_USER_VALUE = "model_api"
ADMIN_USER = "admin"
PAGE_KEY_ID = "streamlit"
PAGE_KEY_BYTES = 24
TYPE_LIST = re.compile(r"HAI_[1-6](,HAI_[1-6]){0,5}")
ERROR_CODE = re.compile(r"[A-Z_]{1,64}")
GRANT = "READ"
CREATE_USER = "/api/2.0/mlflow/users/create"
GET_MODEL = "/api/2.0/mlflow/registered-models/get"
GET_EXPERIMENT = "/api/2.0/mlflow/experiments/get-by-name"
GET_PERMISSION = "/api/3.0/mlflow/users/permissions/get"
GRANT_PERMISSION = "/api/3.0/mlflow/users/permissions/grant"
REQUEST_SECONDS = 10.0
RUN_SECONDS = 120.0
MAX_ANSWER_BYTES = 1024 * 1024


class ServingSecretsError(RuntimeError):
    """A serving secret or MLflow step was refused or failed; the message names the secret or object and the action, never a value."""


def new_page_key() -> str:
    """The page's model API key: 24 random bytes as hex, the text the page presents and the key ring hashes."""
    return secrets.token_hex(PAGE_KEY_BYTES)


def served_types(flag: str | None, inputs: Path | None, environ: Mapping[str, str]) -> tuple[str, ...]:
    """The served HAI types from the flag, else the reviewed services input file, else ``MODEL_API_HAI_TYPES``; distinct and from HAI_1 to HAI_6."""
    if flag is not None:
        text = flag
    elif inputs is not None:
        try:
            text = json.loads(inputs.read_text())["model_api_hai_types"]
        except (OSError, ValueError, KeyError, TypeError):
            raise ServingSecretsError("the services input file is not readable or has no model_api_hai_types") from None
    elif environ.get("MODEL_API_HAI_TYPES") is not None:
        text = environ["MODEL_API_HAI_TYPES"]
    else:
        raise ServingSecretsError("no served HAI types: pass --hai-types or --preparation-inputs, or set MODEL_API_HAI_TYPES")
    types = tuple(text.split(",")) if isinstance(text, str) else ()
    if not isinstance(text, str) or not TYPE_LIST.fullmatch(text) or len(set(types)) != len(types):
        raise ServingSecretsError("the served HAI types must be distinct types from HAI_1 to HAI_6 separated by commas")
    return types


def object_names(root: Path, types: tuple[str, ...]) -> dict[str, tuple[str, str]]:
    """The registered model and experiment name of each type, from the model contract; a type the contract lacks is refused."""
    try:
        contract = json.loads((root / CONTRACT).read_text())
        models, experiments = contract["registered_models"], contract["experiments"]
    except (OSError, ValueError, KeyError, TypeError):
        raise ServingSecretsError(f"{CONTRACT} is missing or incomplete") from None
    unknown = [item for item in types if item not in models or item not in experiments]
    if unknown:
        raise ServingSecretsError(f"{', '.join(unknown)} is not a served HAI type of the model contract")
    return {item: (models[item], experiments[item]) for item in types}


def loopback_address(url: str) -> tuple[str, int]:
    """The host and port of an ``http`` loopback address without a user, path or query; anything else is refused before a password is read."""
    try:
        parsed = urllib.parse.urlparse(url)
        port = parsed.port or 80
    except ValueError:
        raise ServingSecretsError("the MLflow address is not valid") from None
    if (
        parsed.scheme != "http"
        or parsed.hostname not in secret_store.LOOPBACK
        or parsed.username
        or parsed.password
        or parsed.path not in ("", "/")
        or parsed.query
    ):
        raise ServingSecretsError(
            "the MLflow address must be an http loopback address such as http://127.0.0.1:5000 (open a port forward); it must not hold a user"
        )
    return str(parsed.hostname), port


def say(line: str) -> None:
    sys.stdout.write(line + "\n")


def state(created: bool) -> str:
    return "created" if created else "present"


def ensure_grafana(store: secret_store.SecretStore) -> bool:
    _, created = store.ensure(secret_store.GRAFANA_ADMIN_PASSWORD, secret_store.new_password)
    say(f"{secret_store.GRAFANA_ADMIN_PASSWORD}: {state(created)}")
    return True


def check_hashes(text: str, page_key: str) -> None:
    """Refuse hashes that are not a key ring or whose ``streamlit`` entry is not the SHA-256 of the page key; the message names the parameter only."""
    name = secret_store.MODEL_API_KEY_HASHES
    try:
        entries = json.loads(text)
        keys.KeyRing.from_hashes(entries)
    except (ValueError, TypeError, AttributeError, keys.KeyRingInvalid):
        raise ServingSecretsError(f"{name}: is not a JSON object of key identifiers to SHA-256 hashes; left unchanged") from None
    if PAGE_KEY_ID not in entries:
        raise ServingSecretsError(f"{name}: has no {PAGE_KEY_ID} entry for {secret_store.STREAMLIT_MODEL_API_KEY}; left unchanged")
    if entries[PAGE_KEY_ID] != keys.hash_key(page_key):
        raise ServingSecretsError(f"{name}: its {PAGE_KEY_ID} entry is not the SHA-256 of {secret_store.STREAMLIT_MODEL_API_KEY}; left unchanged")


def ensure_page_key(store: secret_store.SecretStore) -> bool:
    """Create the page key and the hashes that name it; existing hashes that do not match the key are refused, never overwritten."""
    key_name, hashes_name = secret_store.STREAMLIT_MODEL_API_KEY, secret_store.MODEL_API_KEY_HASHES
    if store.find(hashes_name) is not None and store.find(key_name) is None:
        raise ServingSecretsError(f"{hashes_name}: exists and {key_name} does not, so no new key can match its {PAGE_KEY_ID} entry; left unchanged")
    page_key, key_created = store.ensure(key_name, new_page_key)
    hashes, hashes_created = store.ensure(hashes_name, lambda: json.dumps({PAGE_KEY_ID: keys.hash_key(page_key)}))
    check_hashes(hashes, page_key)
    say(f"{key_name}: {state(key_created)}")
    say(f"{hashes_name}: {state(hashes_created)}")
    return True


def ensure_mlflow_login(store: secret_store.SecretStore) -> tuple[str, str]:
    """Create the model API's MLflow user name and credential parameters and return both values."""
    user, user_created = store.ensure(secret_store.MODEL_API_MLFLOW_USER, lambda: MLFLOW_USER_VALUE)
    credential, credential_created = store.ensure(secret_store.MODEL_API_MLFLOW_CREDENTIAL, secret_store.new_password)
    say(f"{secret_store.MODEL_API_MLFLOW_USER}: {state(user_created)}")
    say(f"{secret_store.MODEL_API_MLFLOW_CREDENTIAL}: {state(credential_created)}")
    return user, credential


class MlflowAuth:
    """The MLflow 3.17 auth routes the tool calls, as the administrator over one loopback address; each call has its own deadline and nothing is logged."""

    def __init__(self, host: str, port: int, admin_password: str) -> None:
        self.host, self.port = host, port
        self.authorization = "Basic " + base64.b64encode(f"{ADMIN_USER}:{admin_password}".encode()).decode()
        self.end = time.monotonic() + RUN_SECONDS

    def call(
        self, action: str, method: str, path: str, query: dict[str, str] | None = None, body: dict[str, str] | None = None
    ) -> tuple[int, str, dict[str, Any]]:
        """One request; the status, the error code (empty when the answer has none) and the JSON object of the answer. Redirects are not followed."""
        if time.monotonic() >= self.end:
            raise ServingSecretsError(f"{action} did not finish: the run exceeded {RUN_SECONDS:g} s")
        target = path + ("?" + urllib.parse.urlencode(query) if query else "")
        headers = {"Authorization": self.authorization, "Accept": "application/json"}
        payload = None
        if body is not None:
            payload = json.dumps(body).encode()
            headers["Content-Type"] = "application/json"
        connection = http.client.HTTPConnection(self.host, self.port, timeout=REQUEST_SECONDS)
        try:
            connection.request(method, target, body=payload, headers=headers)
            answer = connection.getresponse()
            raw = answer.read(MAX_ANSWER_BYTES)
        except (OSError, http.client.HTTPException) as error:
            raise ServingSecretsError(f"{action} failed: the MLflow server at {self.host}:{self.port} did not answer ({type(error).__name__})") from None
        finally:
            connection.close()
        try:
            loaded = json.loads(raw)
        except ValueError:
            loaded = {}
        loaded = loaded if isinstance(loaded, dict) else {}
        code = loaded.get("error_code")
        return answer.status, code if isinstance(code, str) and ERROR_CODE.fullmatch(code) else "", loaded

    def fail(self, action: str, status: int, code: str) -> ServingSecretsError:
        return ServingSecretsError(f"{action} failed: HTTP {status}{' ' + code if code else ''}")

    def create_user(self, username: str, credential: str) -> bool:
        """Create the user with the credential in a JSON body; true when created, false when it already exists."""
        action = f"creating the MLflow user {username}"
        status, code, _ = self.call(action, "POST", CREATE_USER, body={"username": username, "password": credential})
        if status == 200:
            return True
        if status == 400 and code == "RESOURCE_ALREADY_EXISTS":
            return False
        raise self.fail(action, status, code)

    def model_exists(self, name: str) -> bool:
        action = f"looking up registered model {name}"
        status, code, _ = self.call(action, "GET", GET_MODEL, {"name": name})
        if status == 200:
            return True
        if status == 404 and code == "RESOURCE_DOES_NOT_EXIST":
            return False
        raise self.fail(action, status, code)

    def experiment_id(self, name: str) -> str | None:
        """The experiment's ID, or ``None`` when it does not exist or is in the trash."""
        action = f"looking up experiment {name}"
        status, code, loaded = self.call(action, "GET", GET_EXPERIMENT, {"experiment_name": name})
        if status == 404 and code == "RESOURCE_DOES_NOT_EXIST":
            return None
        found = loaded.get("experiment") if status == 200 else None
        if not isinstance(found, dict) or not isinstance(found.get("experiment_id"), str) or not found["experiment_id"]:
            raise self.fail(action, status, code)
        return None if found.get("lifecycle_stage") == "deleted" else found["experiment_id"]

    def permission(self, username: str, resource_type: str, resource_id: str, label: str) -> str:
        """The permission the user effectively has on the object."""
        action = f"reading the permission of {username} on {label}"
        query = {"username": username, "resource_type": resource_type, "resource_id": resource_id}
        status, code, loaded = self.call(action, "GET", GET_PERMISSION, query)
        if status != 200 or not isinstance(loaded.get("permission"), str):
            raise self.fail(action, status, code)
        return loaded["permission"]

    def grant(self, username: str, resource_type: str, resource_id: str, label: str) -> bool:
        """Insert the READ row; true when inserted, false when the user already has a row on the object."""
        action = f"granting {GRANT} to {username} on {label}"
        body = {"username": username, "resource_type": resource_type, "resource_id": resource_id, "permission": GRANT}
        status, code, _ = self.call(action, "POST", GRANT_PERMISSION, body=body)
        if status == 200:
            return True
        if status == 400 and code == "RESOURCE_ALREADY_EXISTS":
            return False
        raise self.fail(action, status, code)


def grant_read(client: MlflowAuth, username: str, resource_type: str, resource_id: str, label: str) -> bool:
    """Give the user READ on one existing object unless it holds a permission already; return false when the object needs an owner decision."""
    held = client.permission(username, resource_type, resource_id, label)
    if held in (GRANT, "NO_PERMISSIONS"):
        if client.grant(username, resource_type, resource_id, label):
            say(f"{label}: {GRANT} granted")
            return True
        if held == GRANT:
            say(f"{label}: {GRANT} present")
            return True
        held = "NO_PERMISSIONS"
    if held in ("DENY", "NO_PERMISSIONS"):
        sys.stderr.write(f"{label}: holds {held}, which blocks the model API; left unchanged\n")
        return False
    say(f"{label}: holds {held}, left unchanged")
    return True


def grant_served_objects(client: MlflowAuth, username: str, credential: str, names: dict[str, tuple[str, str]]) -> bool:
    """Create the user, then grant READ on each served model and experiment that exists; return false when any object was refused."""
    say(f"mlflow user {username}: {state(client.create_user(username, credential))}")
    clean = True
    for hai_type, (model, experiment) in names.items():
        model_label, experiment_label = f"registered model {model}", f"experiment {experiment}"
        if client.model_exists(model):
            clean = grant_read(client, username, "registered_model", model, model_label) and clean
        else:
            say(f"{model_label}: pending, rerun after the first training run of {hai_type}")
        experiment_id = client.experiment_id(experiment)
        if experiment_id is not None:
            clean = grant_read(client, username, "experiment", experiment_id, experiment_label) and clean
        else:
            say(f"{experiment_label}: pending, rerun after the first training run of {hai_type}")
    return clean


def attempt(call: Callable[[], Any]) -> Any:
    """The call's result, or ``None`` after the refusal went to standard error; a refusal of one value never stops the independent ones."""
    try:
        return call()
    except (secret_store.SecretStoreError, ServingSecretsError) as error:
        sys.stderr.write(f"{error}\n")
        return None


def run(root: Path, endpoint: str | None = None, mlflow_url: str | None = None, hai_types: tuple[str, ...] = ()) -> int:
    """Create the absent secrets and, with ``mlflow_url``, the MLflow user and grants. Return 0, or 1 after a refusal or failure; ``endpoint`` is for tests.

    The AWS SDK writes the body of a put request, which holds the value, to its debug log, so its debug logs stay off for the whole run.
    """
    with quiet_sdk_logs():
        return run_steps(root, endpoint, mlflow_url, hai_types)


def run_steps(root: Path, endpoint: str | None, mlflow_url: str | None, hai_types: tuple[str, ...]) -> int:
    """The steps of ``run``, with the SDK debug logs already off."""
    try:
        names = object_names(root, hai_types) if mlflow_url else {}
        address = loopback_address(mlflow_url) if mlflow_url else None
        store = secret_store.open_store(root, endpoint)
    except (secret_store.SecretStoreError, ServingSecretsError) as error:
        sys.stderr.write(f"{error}\n")
        return 1
    failed = False
    for step in (ensure_grafana, ensure_page_key):
        failed = attempt(functools.partial(step, store)) is None or failed
    login = attempt(lambda: ensure_mlflow_login(store))
    if login is None:
        return 1
    if address is not None:
        host, port = address
        failed = not attempt(lambda: grant_serving_user(store, host, port, login, names)) or failed
    return 1 if failed else 0


def grant_serving_user(store: secret_store.SecretStore, host: str, port: int, login: tuple[str, str], names: dict[str, tuple[str, str]]) -> bool:
    """Read the MLflow administrator password into memory and grant the serving user its READ rights; false when an object was refused."""
    return grant_served_objects(MlflowAuth(host, port, store.get(secret_store.MLFLOW_ADMIN_PASSWORD)), login[0], login[1], names)


def parser() -> argparse.ArgumentParser:
    """The command line; no option takes a secret."""
    command = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    command.add_argument(
        "--root", type=Path, default=REPO_ROOT, help="the repository folder that holds the rendered deployment variables and the model contract"
    )
    command.add_argument("--hai-types", help="the served HAI types as the MODEL_API_HAI_TYPES text, for example HAI_1,HAI_2")
    command.add_argument("--preparation-inputs", type=Path, help="the reviewed services input file; its model_api_hai_types names the served types")
    command.add_argument(
        "--mlflow-url", help="a loopback address of the MLflow tracking server (a port forward): create the model API's MLflow user and its READ grants"
    )
    return command


def main(argv: list[str] | None = None, endpoint: str | None = None) -> int:
    """Run the tool for the command line; ``endpoint`` is only for a loopback stand-in in tests."""
    arguments = parser().parse_args(sys.argv[1:] if argv is None else argv)
    types: tuple[str, ...] = ()
    if arguments.mlflow_url:
        try:
            types = served_types(arguments.hai_types, arguments.preparation_inputs, os.environ)
        except ServingSecretsError as error:
            sys.stderr.write(f"{error}\n")
            return 1
    return run(arguments.root, endpoint, arguments.mlflow_url, types)


if __name__ == "__main__":
    raise SystemExit(main())
