"""Read, create and put the project's managed secrets in AWS Systems Manager Parameter Store; a value lives in memory and nowhere else.

The 16 managed names are the committed defaults of the root variables in ``infra/iam/variables.tf``; ``terraform_defaults`` reads them back so a check
can compare the two. Each is a SecureString parameter named ``/healthcare_hai_prediction/<name_with_underscores>`` on the standard tier, encrypted with
the AWS managed key ``aws/ssm``. The four shared API keys stay in Secrets Manager and this module only reads them. The project profile and region come
from the rendered ``infra/deployment.auto.tfvars.json``. Errors name the secret and the action, never a value.
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

PREFIX = "/healthcare_hai_prediction/"
POSTGRES_PASSWORD = PREFIX + "postgres_password"
POLARIS_ROOT_SECRET = PREFIX + "polaris_root_secret"
POLARIS_READER_CLIENT_ID = PREFIX + "polaris_reader_client_id"
POLARIS_READER_CLIENT_SECRET = PREFIX + "polaris_reader_client_secret"
POLARIS_GOLD_CLIENT_ID = PREFIX + "polaris_gold_client_id"
POLARIS_GOLD_CLIENT_SECRET = PREFIX + "polaris_gold_client_secret"
POLARIS_SILVER_CLIENT_ID = PREFIX + "polaris_silver_client_id"
POLARIS_SILVER_CLIENT_SECRET = PREFIX + "polaris_silver_client_secret"
AIRFLOW_DB_PASSWORD = PREFIX + "airflow_db_password"
AIRFLOW_FERNET_KEY = PREFIX + "airflow_fernet_key"
AIRFLOW_JWT_SECRET = PREFIX + "airflow_jwt_secret"
AIRFLOW_API_SECRET_KEY = PREFIX + "airflow_api_secret_key"
MARQUEZ_DB_PASSWORD = PREFIX + "marquez_db_password"
AIRFLOW_ADMIN = PREFIX + "airflow_admin"
REDIS_PASSWORD = PREFIX + "redis_password"
MLFLOW_DB_PASSWORD = PREFIX + "mlflow_db_password"
MLFLOW_ADMIN_PASSWORD = PREFIX + "mlflow_admin_password"
GRAFANA_ADMIN_PASSWORD = PREFIX + "grafana_admin_password"
MODEL_API_KEY_HASHES = PREFIX + "model_api_key_hashes"
MODEL_API_MLFLOW_USER = PREFIX + "model_api_mlflow_user"
MODEL_API_MLFLOW_CREDENTIAL = PREFIX + "model_api_mlflow_credential"
STREAMLIT_MODEL_API_KEY = PREFIX + "streamlit_model_api_key"
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
    REDIS_PASSWORD,
    MLFLOW_DB_PASSWORD,
    MLFLOW_ADMIN_PASSWORD,
    GRAFANA_ADMIN_PASSWORD,
    MODEL_API_KEY_HASHES,
    MODEL_API_MLFLOW_USER,
    MODEL_API_MLFLOW_CREDENTIAL,
    STREAMLIT_MODEL_API_KEY,
)
READ_ONLY = (AIRFLOW_ADMIN,)
SHARED = ("bls_api_key", "census_api_key", "hud_api_key", "jev_api_key")
SHARED_VARIABLES = ("census_secret_name", "bls_secret_name", "hud_secret_name", "jev_secret_name")
READ_ONLY_VARIABLES = ("airflow_admin_secret_name",)
JOB_READER_VARIABLES = ("aws_polaris_reader_client_id_name", "aws_polaris_reader_client_secret_name")
# The twelve AWS Polaris pair names: outside the 21 managed names, written only by the AWS catalog setup (scripts/lakehouse/aws_catalog_setup.py).
AWS_POLARIS_VARIABLES = (
    *JOB_READER_VARIABLES,
    *(f"aws_polaris_{identity}_client_{part}_name" for identity in ("e2e", "bronze", "silver", "gold", "serving") for part in ("id", "secret")),
)
ROLE_VARIABLES = ("lakehouse_reader_role_name", "lakehouse_writer_role_name")
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
MAX_VALUE_BYTES = 4096
PARAMETER_TIER = "Standard"
PARAMETER_TYPE = "SecureString"
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


def terraform_variables(variables: Path) -> dict[str, str]:
    """The string default of every root variable in a ``variables.tf``, by variable name, in file order; a variable without a string default is left out."""
    text = variables.read_text()
    found: dict[str, str] = {}
    for block in re.finditer(r'^variable "([a-z0-9_]+)" \{\n(.*?)^\}', text, flags=re.DOTALL | re.MULTILINE):
        default = re.search(r'^\s*default\s*=\s*"([^"]*)"', block.group(2), flags=re.MULTILINE)
        if default:
            found[block.group(1)] = default.group(1)
    return found


def terraform_defaults(root: Path) -> list[str]:
    """The default of every secret variable in ``infra/iam/variables.tf`` that is under the project prefix, in file order, without the
    shared administrator password, which the project only reads, and the ten AWS Polaris pair names, which have their own statements."""
    return [
        value
        for name, value in terraform_variables(root / VARIABLES).items()
        if value.startswith(PREFIX) and name not in (*READ_ONLY_VARIABLES, *AWS_POLARIS_VARIABLES)
    ]


def policy_names(variables: Path) -> dict[str, list[str]]:
    """The names the project user's policy files grant, read from the variable defaults and sorted: the placeholder name of each list.

    ``SHARED_SECRET_NAME`` is the four Secrets Manager keys (read only), ``READ_PARAMETER_NAME`` the 17 parameters the user may read and
    ``MANAGED_PARAMETER_NAME`` the 16 it may also put. The ten AWS Polaris pair names have their own statements and single-name placeholders.
    These variable defaults are the only source of the names: no policy file lists one.
    """
    defaults = terraform_variables(variables)
    missing = [name for name in (*SHARED_VARIABLES, *READ_ONLY_VARIABLES) if name not in defaults]
    if missing:
        raise SecretStoreError(f"{variables.name} has no default for: {', '.join(missing)}")
    managed = [value for name, value in defaults.items() if value.startswith(PREFIX) and name not in (*READ_ONLY_VARIABLES, *AWS_POLARIS_VARIABLES)]
    names = {
        "SHARED_SECRET_NAME": sorted(defaults[name] for name in SHARED_VARIABLES),
        "READ_PARAMETER_NAME": sorted([*(defaults[name] for name in READ_ONLY_VARIABLES), *managed]),
        "MANAGED_PARAMETER_NAME": sorted(managed),
    }
    empty = [placeholder for placeholder, values in names.items() if not values]
    if empty:
        raise SecretStoreError(f"{variables.name} holds no names for: {', '.join(empty)}")
    return names


def variable_names(variables: Path) -> dict[str, str]:
    """The single names that a policy template may take from one variable, by placeholder (the variable name in upper case).

    These are the two lakehouse role names and every parameter name under the project prefix, for example ``POSTGRES_PASSWORD_NAME``. The IAM
    stack passes the same variables to its own policy templates, so the Python checks and Terraform read one default per name.
    """
    defaults = terraform_variables(variables)
    missing = [name for name in (*ROLE_VARIABLES, *AWS_POLARIS_VARIABLES) if name not in defaults]
    if missing:
        raise SecretStoreError(f"{variables.name} has no default for: {', '.join(missing)}")
    wanted = [name for name, value in defaults.items() if name in (*ROLE_VARIABLES, *AWS_POLARIS_VARIABLES) or value.startswith(PREFIX)]
    return {name.upper(): defaults[name] for name in wanted}


def project_settings(root: Path) -> dict[str, str]:
    """The project AWS profile and region from the rendered Terraform variables."""
    path = root / DEPLOYMENT
    try:
        values = json.loads(path.read_text())
        return {key: str(values[key]) for key in ("aws_profile", "aws_region")}
    except (OSError, KeyError, ValueError):
        raise SecretStoreError(f"{DEPLOYMENT} is missing or incomplete; render the configuration first") from None


class SecretStore:
    """The managed secrets through one Parameter Store client; the shared API keys, read only, through a Secrets Manager client made on first use."""

    def __init__(self, parameters: Any, shared: Callable[[], Any] | None = None) -> None:
        self.parameters = parameters
        self.shared = shared
        self.shared_client: Any = None

    @staticmethod
    def check(name: str, write: bool = False) -> None:
        """Refuse a name outside the allowed set before a request: the 16 managed names for a write, with the read-only and shared names for a read."""
        allowed = MANAGED if write else (*MANAGED, *READ_ONLY, *SHARED)
        if name not in allowed:
            raise SecretStoreError(f"{name} is not a managed secret name")

    @staticmethod
    def code(error: ClientError | BotoCoreError) -> str:
        """The error code of a failed call, or the exception class when the call never reached the service."""
        return str(error.response.get("Error", {}).get("Code") or type(error).__name__) if isinstance(error, ClientError) else type(error).__name__

    def failure(self, action: str, name: str, error: ClientError | BotoCoreError) -> SecretStoreError:
        """The error for a failed call: the secret, the action and the error code, never a response body."""
        return SecretStoreError(f"{action} on secret {name} failed: {self.code(error)}")

    def read(self, name: str) -> str | None:
        """The stored value, or ``None`` when the secret does not exist; a parameter is read with decryption, a shared key from Secrets Manager."""
        self.check(name)
        try:
            if name in SHARED:
                if self.shared_client is None:
                    if self.shared is None:
                        raise SecretStoreError(f"{name} is a shared key and this store has no Secrets Manager client")
                    self.shared_client = self.shared()
                value = self.shared_client.get_secret_value(SecretId=name).get("SecretString")
            else:
                value = self.parameters.get_parameter(Name=name, WithDecryption=True).get("Parameter", {}).get("Value")
        except (ClientError, BotoCoreError) as error:
            if self.code(error) not in ("ParameterNotFound", "ResourceNotFoundException"):
                raise self.failure("reading", name, error) from None
            return None
        return value if isinstance(value, str) else ""

    def find(self, name: str) -> str | None:
        """The stored value, or ``None`` when the secret is missing or empty."""
        return self.read(name) or None

    def get(self, name: str) -> str:
        """The stored value; a missing or empty secret raises ``SecretMissing`` naming it."""
        value = self.find(name)
        if value is None:
            raise SecretMissing(f"secret {name} is missing or empty")
        return value

    def send(self, name: str, value: str, overwrite: bool) -> bool:
        """One ``put_parameter`` call: a SecureString on the standard tier under the AWS managed key; false when ``overwrite`` is off and the name exists."""
        self.check(name, write=True)
        if not value:
            raise SecretStoreError(f"secret {name} cannot hold an empty value")
        if len(value.encode()) > MAX_VALUE_BYTES:
            raise SecretStoreError(f"secret {name} value is larger than {MAX_VALUE_BYTES} bytes")
        try:
            self.parameters.put_parameter(Name=name, Value=value, Type=PARAMETER_TYPE, Tier=PARAMETER_TIER, Overwrite=overwrite)
        except (ClientError, BotoCoreError) as error:
            if not overwrite and self.code(error) == "ParameterAlreadyExists":
                return False
            raise self.failure("putting a value to" if overwrite else "creating", name, error) from None
        stored = self.read(name)
        if stored != value:
            raise SecretStoreError(f"readback of secret {name} after the write did not match what was written")
        return True

    def put(self, name: str, value: str) -> None:
        """Store a new value, creating the parameter when it does not exist, and read it back; callers that update one secret from many processes serialize."""
        self.send(name, value, overwrite=True)

    def ensure(self, name: str, make: Callable[[], str], wait: float = COLLISION_WAIT_SECONDS) -> tuple[str, bool]:
        """The stored value, generated and created first when the secret does not exist; the flag is true when this call created it.

        The create never overwrites: after a collision this call never writes and reads the winner's value for up to ``wait`` seconds.
        """
        self.check(name, write=True)
        value = self.read(name)
        if value:
            return value, False
        made = make()
        if self.send(name, made, overwrite=False):
            return made, True
        end = time.monotonic() + wait
        while True:
            winner = self.read(name)
            if winner:
                return winner, False
            if time.monotonic() >= end:
                raise SecretStoreError(f"secret {name} was created by another caller but cannot be read after {wait:g} s")
            time.sleep(COLLISION_POLL_SECONDS)


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
            return SecretStore(session.client("ssm", config=config), lambda: session.client("secretsmanager", config=config))
        session = boto3.Session(aws_access_key_id=STAND_IN_CREDENTIAL, aws_secret_access_key=STAND_IN_CREDENTIAL, region_name=settings["aws_region"])
        return SecretStore(
            session.client("ssm", config=config, endpoint_url=endpoint),
            lambda: session.client("secretsmanager", config=config, endpoint_url=endpoint),
        )
    except (BotoCoreError, ValueError) as error:
        raise SecretStoreError(f"setting up the Parameter Store client failed: {type(error).__name__}") from None
