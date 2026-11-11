"""The ``serve_deploy`` job: deploy the request the launch derived and print the result. It runs in the training image, which holds the MLflow client and boto3.

The launch preflight checks the closed request the promotion chain or the form passed (the HAI type and the mode, canary or stable) and derives the rest: the
candidate version from the type's research record and the share from the job table. The task gets the derived request in SERVING_REQUEST and nothing else
names a version. The run id is the deployment id. The job reads the production alias and never moves it. A stable request writes the production version
alone. After a write the active
deployment is read back, and the job fails unless it is the one written.
A refusal exits with code 2 and a message that names the reason.
"""

from __future__ import annotations

import argparse
import importlib
import json
import os
import sys

from scripts import resource_peaks
from scripts.infrastructure import startup_check
from scripts.serving import deploy, manifests


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", required=True)
    args = parser.parse_args()
    try:
        startup_check.check_phase("serve", "serve")
        request = json.loads(os.environ["SERVING_REQUEST"])
        if not isinstance(request, dict) or set(request) != {"hai_type", "deploy_mode", "candidate_version", "percent"}:
            raise ValueError("the request names the HAI type, the mode, the candidate version and the percent")
        uri = os.environ["MODEL_API_MANIFEST_URI"]
        if not uri.startswith("s3://"):
            raise ValueError("MODEL_API_MANIFEST_URI must be an s3:// URI")
        bucket, _, prefix = uri.removeprefix("s3://").partition("/")
        config = importlib.import_module("botocore.config").Config(connect_timeout=3, read_timeout=5, retries={"max_attempts": 2, "mode": "standard"})
        store = manifests.S3ManifestStore(importlib.import_module("boto3").client("s3", config=config), bucket, prefix)
        outcome = deploy.run(
            registry=importlib.import_module("mlflow").MlflowClient(),
            manifests=store,
            hai_type=request["hai_type"],
            candidate_version=request["candidate_version"],
            percent=request["percent"],
            mode=request["deploy_mode"],
            deployment_id=args.run,
        )
    except (deploy.DeployRefused, startup_check.StartupCheckError, ValueError, KeyError, OSError, TypeError) as error:
        sys.stderr.write(f"serve_deploy refused: {error}\n")
        return 2
    result = {
        "status": outcome.status,
        "deployment_id": outcome.deployment.deployment_id,
        "stable_version": outcome.deployment.stable_version,
        "candidate_version": outcome.deployment.candidate_version,
        "percent": outcome.deployment.percent,
    }
    sys.stdout.write(json.dumps(result) + "\n")
    return 0


if __name__ == "__main__":
    resource_peaks.recorded(main)
