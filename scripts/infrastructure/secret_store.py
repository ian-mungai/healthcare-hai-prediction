"""Read, create and put the project's managed secrets in AWS Secrets Manager; a value lives in memory and nowhere else.

The 13 names are the committed defaults of the root variables in ``infra/iam/variables.tf``; ``terraform_defaults`` reads them back so a check can
compare the two. The project profile and region come from the rendered ``infra/deployment.auto.tfvars.json``. Errors name the secret and the action,
never a value.
"""

from __future__ import annotations

import base64
import binascii
import json
import re
import secrets
import threading
import time
import urllib.parse
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

PREFIX = "healthcare-hai-prediction/"
POSTGRES_PASSWORD = PREFIX + "postgres-password"
POLARIS_ROOT_SECRET = PREFIX + "polaris-root-secret"
POLARIS_READER_CLIENT_ID = PREFIX + "polaris-reader-client-id"
POLARIS_READER_CLIENT_SECRET = PREFIX + "polaris-reader-client-secret"
POLARIS_GOLD_CLIENT_ID = PREFIX + "polaris-gold-client-id"
POLARIS_GOLD_CLIENT_SECRET = PREFIX + "polaris-gold-client-secret"
POLARIS_SILVER_CLIENT_ID = PREFIX + "polaris-silver-client-id"
POLARIS_SILVER_CLIENT_SECRET = PREFIX + "polaris-silver-client-secret"
AIRFLOW_DB_PASSWORD = PREFIX + "airflow-db-password"
AIRFLOW_FERNET_KEY = PREFIX + "airflow-fernet-key"
AIRFLOW_JWT_SECRET = PREFIX + "airflow-jwt-secret"
AIRFLOW_API_SECRET_KEY = PREFIX + "airflow-api-secret-key"
MARQUEZ_DB_PASSWORD = PREFIX + "marquez-db-password"
AIRFLOW_ADMIN = PREFIX + "airflow-admin"
MANAGED = (
    POSTGRES_PASSWORD,
    POLARIS_ROOT_SECRET,
    POLARIS_READER_CLIENT_ID,
    POLARIS_READER_CLIENT_SECRET,
    POLARIS_GOLD_CLIENT_ID,
    POLARIS_GOLD_CLIENT_SECRET,
    POLARIS_SILVER_CLIENT_ID,
    POLARIS_SILVER_CLIENT_SECRET,
    AIRFLOW_DB_PASSWORD,
    AIRFLOW_FERNET_KEY,
    AIRFLOW_JWT_SECRET,
    AIRFLOW_API_SECRET_KEY,
    MARQUEZ_DB_PASSWORD,
)
DEPLOYMENT = "infra/deployment.auto.tfvars.json"
VARIABLES = "infra/iam/variables.tf"
LOOPBACK = {"127.0.0.1", "localhost"}
STAND_IN_CREDENTIAL = "stand-in"
CLIENT_OPTIONS: dict[str, Any] = {
    "retries": {"total_max_attempts": 2, "mode": "standard"},
    "connect_timeout": 5,
    "read_timeout": 10,
    "ignore_configured_endpoint_urls": True,
}
FERNET_KEY_BYTES = 32
FETCH_DEADLINE_SECONDS = 30.0
COLLISION_WAIT_SECONDS = 10.0
COLLISION_POLL_SECONDS = 0.25


class SecretStoreError(RuntimeError):
    """A secret operation failed; the message names the secret and the action, never a value."""


class SecretMissing(SecretStoreError):
    """The secret does not exist or holds an empty value."""


def new_password() -> str:
    """A URL-safe random value for a database password or a client secret."""
    return secrets.token_urlsafe(32)


def new_fernet_key() -> str:
    """A new Fernet key: 32 random bytes, URL-safe base64."""
    return base64.urlsafe_b64encode(secrets.token_bytes(FERNET_KEY_BYTES)).decode()


def is_fernet_key(value: str) -> bool:
    """True when the value is the URL-safe base64 text of exactly 32 bytes that Fernet requires."""
    if not re.fullmatch(r"[A-Za-z0-9_-]{43}=", value):
        return False
    try:
        return len(base64.urlsafe_b64decode(value)) == FERNET_KEY_BYTES
    except (binascii.Error, ValueError):
        return False


def terraform_defaults(root: Path) -> list[str]:
    """The default of every ``*_name`` secret variable in ``infra/iam/variables.tf`` that is under the project prefix, in file order, without the
    shared administrator password, which the project only reads."""
    text = (root / VARIABLES).read_text()
    found = re.findall(r'variable "[a-z_]+_name" \{.*?default\s*=\s*"(' + re.escape(PREFIX) + r'[^"]+)"', text, flags=re.DOTALL)
    return [str(name) for name in found if name != AIRFLOW_ADMIN]


def project_settings(root: Path) -> dict[str, str]:
    """The project AWS profile and region from the rendered Terraform variables."""
    path = root / DEPLOYMENT
    try:
        values = json.loads(path.read_text())
        return {key: str(values[key]) for key in ("aws_profile", "aws_region")}
    except (OSError, KeyError, ValueError):
        raise SecretStoreError(f"{DEPLOYMENT} is missing or incomplete; render the configuration first") from None


class SecretStore:
    """The managed secrets through one Secrets Manager client."""

    def __init__(self, client: Any) -> None:
        self.client = client

    @staticmethod
    def check(name: str) -> None:
        """Refuse any name outside the 13 managed names before a request is made."""
        if name not in MANAGED:
            raise SecretStoreError(f"{name} is not a managed secret name")

    @staticmethod
    def code(error: ClientError | BotoCoreError) -> str:
        """The error code of a failed call, or the exception class when the call never reached the service."""
        return str(error.response.get("Error", {}).get("Code") or type(error).__name__) if isinstance(error, ClientError) else type(error).__name__

    def failure(self, action: str, name: str, error: ClientError | BotoCoreError) -> SecretStoreError:
        """The error for a failed call: the secret, the action and the error code, never a response body."""
        return SecretStoreError(f"{action} on secret {name} failed: {self.code(error)}")

    def read(self, name: str) -> str | None:
        """The stored value: ``None`` when the secret does not exist, an empty string when it exists with no value."""
        self.check(name)
        try:
            value = self.client.get_secret_value(SecretId=name).get("SecretString")
        except (ClientError, BotoCoreError) as error:
            if self.code(error) != "ResourceNotFoundException":
                raise self.failure("reading", name, error) from None
            return "" if self.exists(name) else None
        return value if isinstance(value, str) else ""

    def exists(self, name: str) -> bool:
        """True when the secret exists, with or without a value."""
        try:
            self.client.describe_secret(SecretId=name)
        except (ClientError, BotoCoreError) as error:
            if self.code(error) == "ResourceNotFoundException":
                return False
            raise self.failure("describing", name, error) from None
        return True

    def find(self, name: str) -> str | None:
        """The stored value, or ``None`` when the secret is missing or empty."""
        return self.read(name) or None

    def get(self, name: str) -> str:
        """The stored value; a missing or empty secret raises ``SecretMissing`` naming it."""
        value = self.find(name)
        if value is None:
            raise SecretMissing(f"secret {name} is missing or empty")
        return value

    def put(self, name: str, value: str) -> None:
        """Store a new value, creating the secret when it does not exist; callers that update one secret from several processes serialize first."""
        self.check(name)
        try:
            self.client.put_secret_value(SecretId=name, SecretString=value)
            return
        except (ClientError, BotoCoreError) as error:
            if self.code(error) != "ResourceNotFoundException":
                raise self.failure("putting a value to", name, error) from None
        try:
            self.client.create_secret(Name=name, SecretString=value)
        except (ClientError, BotoCoreError) as error:
            if self.code(error) != "ResourceExistsException":
                raise self.failure("creating", name, error) from None
            try:
                self.client.put_secret_value(SecretId=name, SecretString=value)
            except (ClientError, BotoCoreError) as retry:
                raise self.failure("putting a value to", name, retry) from None

    def ensure(self, name: str, make: Callable[[], str], wait: float = COLLISION_WAIT_SECONDS) -> tuple[str, bool]:
        """The stored value, generated and created first when the secret does not exist; the flag is true when this call created it.

        An existing secret with no value is refused: only ``fill_empty`` fills it. After a create collision this call never writes: it reads the
        winner's value for up to ``wait`` seconds.
        """
        value = self.read(name)
        if value:
            return value, False
        if value is not None:
            raise SecretStoreError(f"secret {name} exists but holds no value; fill it by hand or with the one-time secrets move, not with this command")
        made = make()
        try:
            self.client.create_secret(Name=name, SecretString=made)
            return made, True
        except (ClientError, BotoCoreError) as error:
            if self.code(error) != "ResourceExistsException":
                raise self.failure("creating", name, error) from None
        end = time.monotonic() + wait
        while True:
            winner = self.read(name)
            if winner:
                return winner, False
            if time.monotonic() >= end:
                raise SecretStoreError(f"secret {name} was created by another caller but holds no value after {wait:g} s")
            time.sleep(COLLISION_POLL_SECONDS)

    def fill_empty(self, name: str, value: str) -> bool:
        """The explicit recovery operation: store ``value`` only in a secret that exists with no value; true when it did."""
        if self.read(name) != "":
            return False
        self.put(name, value)
        return True


def within[T](seconds: float, call: Callable[[], T], what: str) -> T:
    """Run ``call`` in a thread and wait for it at most ``seconds``; a call still running then is abandoned and ``what`` is reported as not finished.

    An abandoned call ends by its own request limits. The call's own errors reach the caller unchanged.
    """
    outcome: list[T] = []
    failure: list[BaseException] = []

    def work() -> None:
        try:
            outcome.append(call())
        except BaseException as error:
            failure.append(error)

    thread = threading.Thread(target=work, name="secret-call", daemon=True)
    thread.start()
    thread.join(seconds)
    if failure:
        raise failure[0]
    if not outcome:
        raise SecretStoreError(f"{what} did not finish within {seconds:g} s")
    return outcome[0]


def fetch_all(root: Path, endpoint: str | None, names: Sequence[str], seconds: float = FETCH_DEADLINE_SECONDS) -> dict[str, str | SecretStoreError]:
    """Read each named secret under one absolute deadline that covers the client setup, every read and every retry.

    Each name maps to its value or to the error that stopped it (``SecretMissing`` for a missing or empty secret). The reads run in one thread
    that this call abandons at the deadline; that thread ends by its own request limits and its late results are dropped.
    """
    end = time.monotonic() + seconds
    results: dict[str, str | SecretStoreError] = {}
    lock = threading.Lock()
    abandoned = threading.Event()

    def record(name: str, outcome: str | SecretStoreError) -> None:
        with lock:
            if not abandoned.is_set():
                results[name] = outcome

    def work() -> None:
        try:
            store = open_store(root, endpoint)
        except SecretStoreError as error:
            for name in names:
                record(name, error)
            return
        for name in names:
            if abandoned.is_set() or time.monotonic() >= end:
                return
            try:
                record(name, store.get(name))
            except SecretStoreError as error:
                record(name, error)
            except Exception as error:
                record(name, SecretStoreError(f"reading secret {name} failed: {type(error).__name__}"))

    thread = threading.Thread(target=work, name="secret-fetch", daemon=True)
    thread.start()
    thread.join(max(0.0, end - time.monotonic()))
    with lock:
        abandoned.set()
        found = dict(results)
    return {name: found.get(name) or SecretStoreError(f"reading secret {name} did not finish within {seconds:g} s") for name in names}


def client_config() -> Config:
    """Short timeouts, two requests in all and no endpoint from the environment or the AWS files; only a test registry's stand-in changes the endpoint."""
    return Config(**CLIENT_OPTIONS)


def open_store(root: Path, endpoint: str | None = None) -> SecretStore:
    """The store for the project profile and region; ``endpoint`` is a loopback stand-in that only a test registry names."""
    import boto3

    settings = {"aws_profile": "", "aws_region": "us-west-2"} if endpoint else project_settings(root)
    config = client_config()
    if endpoint is not None and urllib.parse.urlparse(endpoint).hostname not in LOOPBACK:
        raise SecretStoreError("a stand-in endpoint must be on the loopback address")
    try:
        if endpoint is None:
            session = boto3.Session(profile_name=settings["aws_profile"], region_name=settings["aws_region"])
            return SecretStore(session.client("secretsmanager", config=config))
        session = boto3.Session(aws_access_key_id=STAND_IN_CREDENTIAL, aws_secret_access_key=STAND_IN_CREDENTIAL, region_name=settings["aws_region"])
        return SecretStore(session.client("secretsmanager", config=config, endpoint_url=endpoint))
    except (BotoCoreError, ValueError) as error:
        raise SecretStoreError(f"setting up the Secrets Manager client failed: {type(error).__name__}") from None
