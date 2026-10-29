"""Compare an ECS task session with its metadata and export only hashed identity evidence."""

from __future__ import annotations

import hashlib
import http.client
import json
import os
import sys
from urllib.parse import urlsplit

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError


def main() -> int:
    """Make one bounded regional identity call and reject a mismatched role or task session."""
    try:
        metadata = urlsplit(os.environ["ECS_CONTAINER_METADATA_URI_V4"])
        if metadata.scheme != "http" or metadata.hostname != "169.254.170.2" or metadata.port not in (None, 80):
            raise ValueError("metadata_endpoint")
        connection = http.client.HTTPConnection("169.254.170.2", timeout=5)
        try:
            connection.request("GET", metadata.path.rstrip("/") + "/task")
            response = connection.getresponse()
            payload = response.read(32769)
            if response.status != 200 or len(payload) > 32768:
                raise ValueError("metadata_response")
            task_id = str(json.loads(payload)["TaskARN"]).rsplit("/", 1)[-1]
        finally:
            connection.close()
        client = boto3.client(
            "sts",
            region_name=os.environ["AWS_REGION"],
            config=Config(connect_timeout=5, read_timeout=5, retries={"total_max_attempts": 1}),
        )
        identity = client.get_caller_identity()
        role_id = os.environ["EXPECTED_ROLE_ID"]
        if not task_id or identity["UserId"] != role_id + ":" + task_id or identity["Arn"].rsplit("/", 1)[-1] != task_id:
            raise ValueError("identity_mismatch")
        evidence = {
            "identity_matches_task": True,
            "task_id_sha256": hashlib.sha256(task_id.encode()).hexdigest(),
            "role_id_sha256": hashlib.sha256(role_id.encode()).hexdigest(),
        }
        sys.stdout.write(json.dumps(evidence, sort_keys=True) + "\n")
        return 0
    except (BotoCoreError, ClientError, KeyError, ValueError, OSError, http.client.HTTPException):
        sys.stderr.write("The bounded task identity comparison failed. No identity values were logged.\n")
        return 1


if __name__ == "__main__":
    sys.exit(main())
