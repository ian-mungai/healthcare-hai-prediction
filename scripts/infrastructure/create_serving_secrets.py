"""Create the serving secrets once in Parameter Store and run the shared model API account check; no value is printed, logged or kept in a file.

Run from the repository root with the project profile; the owner opens a port forward to the MLflow tracking server first (``--mlflow-url`` must be a
loopback address)::

    .venv/bin/python -m scripts.infrastructure.create_serving_secrets --hai-types HAI_1,HAI_2 --mlflow-url http://127.0.0.1:5000

1. ``grafana_admin_password``: a random SecureString, created when absent.
2. ``streamlit_model_api_key`` (random) and ``model_api_key_hashes``, the JSON object ``{"streamlit": "<SHA-256 of the key>"}`` that
   ``scripts/serving/keys.py`` reads. Both are created when absent. Hashes that already exist are never changed: without a ``streamlit`` entry, with another
   hash or with a page key that is missing, the tool refuses and names the parameter.
3. ``model_api_mlflow_user`` (value ``model_api``) and ``model_api_mlflow_credential`` (random), created when absent. With ``--mlflow-url`` the tool then
   reads the administrator password from ``mlflow_admin_password`` into memory only and calls ``ensure_account`` of ``infra/services/scripts/mlflow_user.py``,
   the one owner of the model API account: that check creates the MLflow user (the credential travels in a JSON body, never in an address or the command
   line), logs in with the stored credential and refuses a user that has another password, grants READ on registered model ``hai_model_<T>`` and experiment
   ``hai_training_<T>`` of each served type and refuses a DENY. The restore task (``mlflow_user``) runs the same check after every host start. This tool holds
   no account or grant logic of its own.

The served types come from ``--hai-types`` (the ``MODEL_API_HAI_TYPES`` text), else ``--preparation-inputs`` (the reviewed services input file, key
``model_api_hai_types``), else the ``MODEL_API_HAI_TYPES`` environment variable. The output says ``created`` or ``present`` for each secret and gives the
check's lines for the account. A secret that cannot be set refuses by name, the other independent values are still made and the exit code is 1.
The account check ends at one run deadline of ``RUN_SECONDS`` for the whole MLflow step, response bodies included.
"""

from __future__ import annotations

import argparse
import functools
import importlib.util
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
ACCOUNT_CHECK = "infra/services/scripts/mlflow_user.py"
MLFLOW_USER_VALUE = "model_api"
ADMIN_USER = "admin"
PAGE_KEY_ID = "streamlit"
PAGE_KEY_BYTES = 24
TYPE_LIST = re.compile(r"HAI_[1-6](,HAI_[1-6]){0,5}")


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


@functools.cache
def account_check(root: Path) -> Any:
    """The shared account check module, loaded from its file in the services scripts (the task runs the same file)."""
    path = root / ACCOUNT_CHECK
    spec = importlib.util.spec_from_file_location("mlflow_user_check", path)
    if spec is None or spec.loader is None:
        raise ServingSecretsError(f"{ACCOUNT_CHECK} cannot be loaded")
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    except (OSError, SyntaxError) as error:
        raise ServingSecretsError(f"{ACCOUNT_CHECK} cannot be loaded ({type(error).__name__})") from None
    return module


def check_account(root: Path, store: secret_store.SecretStore, host: str, port: int, login: tuple[str, str], hai_types: tuple[str, ...]) -> bool:
    """Read the MLflow administrator password into memory and run the shared account check; a refusal of the check is this tool's refusal."""
    check = account_check(root)
    address = f"http://[{host}]:{port}" if ":" in host else f"http://{host}:{port}"
    try:
        check.ensure_account(
            address, (ADMIN_USER, store.get(secret_store.MLFLOW_ADMIN_PASSWORD)), login[0], login[1], hai_types, time.monotonic() + check.RUN_SECONDS
        )
    except check.Refused as refusal:
        raise ServingSecretsError(f"the MLflow account check at {host}:{port} refused: {refusal}") from None
    return True


def attempt(call: Callable[[], Any]) -> Any:
    """The call's result, or ``None`` after the refusal went to standard error; a refusal of one value never stops the independent ones."""
    try:
        return call()
    except (secret_store.SecretStoreError, ServingSecretsError) as error:
        sys.stderr.write(f"{error}\n")
        return None


def run(root: Path, endpoint: str | None = None, mlflow_url: str | None = None, hai_types: tuple[str, ...] = ()) -> int:
    """Create the absent secrets and, with ``mlflow_url``, run the shared account check. Return 0, or 1 after a refusal or failure; ``endpoint`` is for tests.

    The AWS SDK writes the body of a put request, which holds the value, to its debug log, so its debug logs stay off for the whole run.
    """
    with quiet_sdk_logs():
        return run_steps(root, endpoint, mlflow_url, hai_types)


def run_steps(root: Path, endpoint: str | None, mlflow_url: str | None, hai_types: tuple[str, ...]) -> int:
    """The steps of ``run``, with the SDK debug logs already off."""
    try:
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
        failed = attempt(lambda: check_account(root, store, host, port, login, hai_types)) is None or failed
    return 1 if failed else 0


def parser() -> argparse.ArgumentParser:
    """The command line; no option takes a secret."""
    command = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    command.add_argument(
        "--root", type=Path, default=REPO_ROOT, help="the repository folder that holds the rendered deployment variables and the account check"
    )
    command.add_argument("--hai-types", help="the served HAI types as the MODEL_API_HAI_TYPES text, for example HAI_1,HAI_2")
    command.add_argument("--preparation-inputs", type=Path, help="the reviewed services input file; its model_api_hai_types names the served types")
    command.add_argument("--mlflow-url", help="a loopback address of the MLflow tracking server (a port forward): run the model API account check")
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
