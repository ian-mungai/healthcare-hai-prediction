"""What the run 3 task does with its role: read one API key for its matching collector, and store a filtered copy under ``raw/``.

The publisher task role reads three secrets and creates objects only under ``raw/``. Both switches are off by default. ``HAI_TASK_KEYS`` lets the task take
the keyed units and ``HAI_TASK_FILTERED`` lets it take the CMS owners units. A key is read when a request needs it, is never cached and is never put in the
environment. Every failure here is fixed text with no chained cause, so no provider message, key or path can reach a log, a traceback or a record.
"""

from __future__ import annotations

import base64
import json
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

from scripts.acquisition.api_keys import ApiKeyError, number_shaped
from scripts.acquisition.collection_layout import RAW_ROOT
from scripts.acquisition.s3_store import StorageError
from scripts.acquisition.stored_mirror import require_task_identity

KEYS_VARIABLE = "HAI_TASK_KEYS"
FILTERED_VARIABLE = "HAI_TASK_FILTERED"
BUCKET_VARIABLE = "DATA_BUCKET_NAME"
KEY_OF_MODE = {"bls_api_v2": "bls_api_key", "census_acs_api": "census_api_key", "census_acs_detailed": "census_api_key", "hud_api": "hud_api_key"}
ORIGINAL_FOLDER = "private_original"
CHUNK = 1024 * 1024
ClientFactory = Callable[[str, str], Any]


def default_client(service: str, region: str) -> Any:
    """The task role's clients. No other AWS client exists on this path."""
    import boto3
    from botocore.config import Config

    options = Config(retries={"total_max_attempts": 3, "mode": "standard"}, connect_timeout=5, read_timeout=60)
    session = boto3.session.Session(region_name=region)
    if service == "sts":
        return session.client("sts", config=options)
    if service == "secretsmanager":
        return session.client("secretsmanager", config=options)
    if service == "s3":
        return session.client("s3", config=options)
    raise ApiKeyError(f"the task makes no {service} client")


class TaskKeys:
    """Reads the one key of one collector mode, after checking that the caller is the publisher task role."""

    def __init__(self, mode: str, environment: Mapping[str, str], factory: ClientFactory = default_client) -> None:
        if mode not in KEY_OF_MODE:
            raise ApiKeyError("this collector mode reads no key")
        self.name, self.environment, self.factory = KEY_OF_MODE[mode], environment, factory

    def read_api_key(self, name: str) -> str:
        """The plain text of this collector's key. Another name is refused before any AWS call."""
        if name != self.name:
            raise ApiKeyError(f"{name} is not the key of this collector")
        region = self.environment["AWS_REGION"]
        require_task_identity(self.factory("sts", region), self.environment["EXPECTED_ACCOUNT_ID"], self.environment["TASK_ROLE_NAME"])
        try:
            value = self.factory("secretsmanager", region).get_secret_value(SecretId=name).get("SecretString")
        except Exception:
            raise ApiKeyError(f"reading secret {name} failed") from None
        if not isinstance(value, str) or not value:
            raise ApiKeyError(f"reading secret {name} failed: the secret is missing or empty")
        if number_shaped(value):
            raise ApiKeyError(f"reading secret {name} failed: rotate the key to one with a character a number cannot hold")
        return value


def _argument(arguments: list[str], name: str) -> str:
    return arguments[arguments.index(name) + 1]


class TaskStorage:
    """The project's storage calls with the task role, in the call shape of the command line adapter. It reads and creates objects under ``raw/`` only.

    It never stores a file from the original's folder, so a wrong path cannot send an original to S3. The identity check runs when the command line builds it.
    """

    checked_by_task_identity = True
    storage_policy: Mapping[str, Any] = {"enabled": False, "objects": {}, "evidence_objects": {}}

    def __init__(self, client: Any, configuration: Mapping[str, str]) -> None:
        self.client, self.configuration = client, dict(configuration)

    def call(self, service: str, operation: str, arguments: list[str] | None = None) -> dict:
        return self.call_bounded(service, operation, arguments, timeout=180)

    def call_bounded(self, service: str, operation: str, arguments: list[str] | None = None, *, timeout: float) -> dict:
        if service != "s3api" or operation not in {"head-object", "put-object", "get-object"}:
            raise StorageError(operation, "outside_task_storage")
        given = arguments or []
        key = _argument(given, "--key")
        if not key.startswith(RAW_ROOT):
            raise StorageError(operation, "outside_raw_root")
        named = {"Bucket": self.configuration["data_bucket_name"], "ExpectedBucketOwner": self.configuration["expected_account_id"], "Key": key}
        try:
            if operation == "head-object":
                return self._answer(self.client.head_object(**named, ChecksumMode="ENABLED"))
            if operation == "put-object":
                return self._put(named, given)
            return self._get(named, given)
        except StorageError:
            raise
        except Exception as error:
            raise StorageError(operation, _error_code(error)) from None

    @staticmethod
    def _answer(reply: Mapping[str, Any]) -> dict:
        return {name: reply[name] for name in ("VersionId", "ContentLength", "ChecksumSHA256", "ServerSideEncryption") if name in reply}

    def _put(self, named: dict, given: list[str]) -> dict:
        body = Path(_argument(given, "--body"))
        if ORIGINAL_FOLDER in body.parts:
            raise StorageError("put-object", "original_refused")
        if _argument(given, "--if-none-match") != "*":
            raise StorageError("put-object", "create_once_required")
        checksum = _argument(given, "--checksum-sha256")
        base64.b64decode(checksum, validate=True)
        with body.open("rb") as handle:
            return self._answer(
                self.client.put_object(
                    **named,
                    Body=handle,
                    IfNoneMatch="*",
                    ChecksumAlgorithm="SHA256",
                    ChecksumSHA256=checksum,
                    ServerSideEncryption=_argument(given, "--server-side-encryption"),
                    Metadata=json.loads(_argument(given, "--metadata")),
                )
            )

    def _get(self, named: dict, given: list[str]) -> dict:
        reply = self.client.get_object(**named, VersionId=_argument(given, "--version-id"), ChecksumMode="ENABLED")
        with Path(given[-1]).open("wb") as handle:
            while chunk := reply["Body"].read(CHUNK):
                handle.write(chunk)
        return self._answer(reply)


def _error_code(error: Exception) -> str:
    """The provider's short code, never its message."""
    response = getattr(error, "response", None)
    code = response.get("Error", {}).get("Code") if isinstance(response, Mapping) else None
    return code if isinstance(code, str) and code.isalnum() else "request_failed"


def storage_from_environment(environment: Mapping[str, str], factory: ClientFactory = default_client) -> TaskStorage:
    """The task's storage client, made only after the identity check passes."""
    region = environment["AWS_REGION"]
    require_task_identity(factory("sts", region), environment["EXPECTED_ACCOUNT_ID"], environment["TASK_ROLE_NAME"])
    configuration = {"data_bucket_name": environment[BUCKET_VARIABLE], "expected_account_id": environment["EXPECTED_ACCOUNT_ID"], "aws_region": region}
    return TaskStorage(factory("s3", region), configuration)
