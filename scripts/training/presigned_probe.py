"""The MLflow 3.17.2 client over an artifact server that stores artifacts in S3, as a stand-in shared by the cases of every run artifact download."""

from __future__ import annotations

import os
from typing import Any

PROXY_SWITCH = "MLFLOW_ENABLE_PROXY_MULTIPART_DOWNLOAD"


class ProbingClient:
    """The MLflow 3.17.2 client over an artifact server that stores artifacts in S3.

    A client with no ``MLFLOW_ENABLE_PROXY_MULTIPART_DOWNLOAD`` setting asks ``/server-info``, which advertises presigned downloads for an S3 destination, and
    fetches each file straight from S3 with the presigned URL. The ECS task reaches the tracking server only. When that second request fails, MLflow hides the
    cause and ``RunsArtifactRepository.download_artifacts`` raises the one generic message. A client with the setting ``false`` streams the file through the
    tracking server, as the owner's port-forwarded request does. The setting is read when a download starts, so a case that drives the real function that opens
    the client sees the same result as the job: the setting must be ``false`` before the first download.
    """

    def __init__(self, tracking: Any) -> None:
        self.tracking = tracking
        self.direct_reads = 0

    def __getattr__(self, name: str) -> Any:
        return getattr(self.tracking, name)

    def download_artifacts(self, run_id: str, path: str, dst_path: str | None = None) -> str:
        if os.environ.get(PROXY_SWITCH, "").lower() != "false":
            self.direct_reads += 1
            raise Exception(f"Failed to download artifacts from path {path.rsplit('/', 1)[-1]!r}, please ensure that the path is correct.")
        return self.tracking.download_artifacts(run_id, path, dst_path)
