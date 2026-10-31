"""The ``serve_deploy`` job: read the owner's staged request, deploy and print the result. It runs in the training image, which holds the MLflow client
and boto3.

The request holds the HAI type, the mode, the candidate version and the percent. The AWS task gets it checked in SERVING_REQUEST, which the broker read once
from the staged record. A hand run may name a file with ``--request``. The run id is the deployment id. The job reads the production alias and never moves it.
A refusal exits with code 2 and a message that names the reason.
"""

from __future__ import annotations

import argparse
import importlib
import json
import os
import sys
from pathlib import Path

from orchestration import serving_contract
from scripts.serving import deploy, manifests


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", required=True)
    parser.add_argument("--request", type=Path, help="a request file; without it the request is read from SERVING_REQUEST")
    args = parser.parse_args()
    try:
        raw = json.loads(args.request.read_text() if args.request else os.environ["SERVING_REQUEST"])
        request = serving_contract.parameters_for("serving_deploy_dag", raw)
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
    except (deploy.DeployRefused, ValueError, KeyError, OSError, TypeError) as error:
        sys.stderr.write(f"serve_deploy refused: {error}\n")
        return 2
    sys.stdout.write(
        json.dumps(
            {
                "status": outcome.status,
                "deployment_id": outcome.deployment.deployment_id,
                "stable_version": outcome.deployment.stable_version,
                "candidate_version": outcome.deployment.candidate_version,
                "percent": outcome.deployment.percent,
            }
        )
        + "\n"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
