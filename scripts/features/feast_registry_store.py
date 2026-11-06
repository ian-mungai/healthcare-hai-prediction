"""The Feast registry store of the AWS jobs: the one registry object in S3, read with GetObject and written with PutObject.

Feast's own ``S3RegistryStore`` calls HeadBucket before every read, and HeadBucket needs ``s3:ListBucket`` without a prefix condition. The task roles
hold GetObject and PutObject on the registry key and ListBucket only under the ``lakehouse/features/`` and ``lakehouse/feast/`` prefixes, so the stock
store ends with ``S3RegistryBucketForbiddenAccess``. ``feature_store_aws.yaml`` selects this store with ``registry_store_type``. It implements the
``RegistryStore`` interface of Feast 0.66.0 and 0.67.0 (``get_registry_proto``, ``update_registry_proto``, ``teardown``) over the same object and bytes, and
it never calls HeadBucket or ListObjectsV2.

An object that is not there is a ``FileNotFoundError``, which Feast turns into a new registry on a first apply. Every other error reaches the caller.
The client takes the task role's credentials and ``AWS_DEFAULT_REGION`` from the environment, as the stock store does.
The module needs the Feast and boto3 packages, so it is imported only inside the feature store and model API images.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from feast.infra.registry.registry_store import RegistryStore
from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto

CONNECT_SECONDS = 5
READ_SECONDS = 30
ATTEMPTS = 3
MISSING_CODES = frozenset({"NoSuchKey", "404"})


class S3ObjectRegistryStore(RegistryStore):
    """Feast's S3 registry location with the grants of a role that holds GetObject and PutObject on the registry key only."""

    def __init__(self, registry_config: Any, repo_path: Path) -> None:
        del repo_path
        uri = urlparse(str(registry_config.path))
        key = uri.path.lstrip("/")
        if uri.scheme != "s3" or not uri.hostname or not key:
            raise ValueError("the registry path must be an s3://<bucket>/<key> URI")
        self._uri = uri.geturl()
        self._bucket = uri.hostname
        self._key = key
        self._put_options = dict(registry_config.s3_additional_kwargs or {})
        settings = Config(connect_timeout=CONNECT_SECONDS, read_timeout=READ_SECONDS, retries={"total_max_attempts": ATTEMPTS, "mode": "standard"})
        self._client = boto3.client("s3", config=settings)

    def get_registry_proto(self) -> RegistryProto:
        try:
            body = self._client.get_object(Bucket=self._bucket, Key=self._key)["Body"].read()
        except ClientError as error:
            if error.response.get("Error", {}).get("Code") in MISSING_CODES:
                raise FileNotFoundError(f"Error while trying to locate Registry at path {self._uri}") from error
            raise
        registry_proto = RegistryProto()
        registry_proto.ParseFromString(body)
        return registry_proto

    def update_registry_proto(self, registry_proto: RegistryProto) -> None:
        registry_proto.version_id = str(uuid.uuid4())
        registry_proto.last_updated.FromDatetime(datetime.now(UTC))
        self._client.put_object(Bucket=self._bucket, Key=self._key, Body=registry_proto.SerializeToString(), **self._put_options)

    def teardown(self) -> None:
        self._client.delete_object(Bucket=self._bucket, Key=self._key)
