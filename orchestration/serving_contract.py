"""The fixed steps of the serving deployment DAG and the validation of its request.

The request is a record the owner stages beside the run's code, as for a promotion. The DAG carries no model parameter, so no caller can change what a deploy
does. The job that runs ``serve_deploy`` reads the record, checks it here and writes the deployment. It never moves the production alias.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

from orchestration.training_contract import HAI_TYPES

DAGS = ("serving_deploy_dag",)
SERVING_STEPS = ("acquire_run", "snapshot_serving", "serve_deploy", "run_record")
DEPLOY_MODES = ("canary", "rollback")
MAX_PERCENT = 50
REQUEST_FILE = "serving_request.json"
VERSION = re.compile(r"[1-9][0-9]{0,18}")


def parameters_for(dag_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
    """Validate a complete closed request: the HAI type, the mode, the candidate version and the share of hospital-windows it receives."""
    if dag_id not in DAGS:
        raise ValueError("serving parameters belong only to the serving deployment DAG")
    if set(raw) != {"hai_type", "candidate_version", "percent", "deploy_mode"}:
        raise ValueError("a serving request names the HAI type, the candidate version, the percent and the mode")
    if raw["hai_type"] not in HAI_TYPES or raw["deploy_mode"] not in DEPLOY_MODES:
        raise ValueError("the HAI type or the deploy mode is unsupported")
    version, percent = raw["candidate_version"], raw["percent"]
    if raw["deploy_mode"] == "rollback":
        if version is not None or isinstance(percent, bool) or percent != 0:
            raise ValueError("a rollback names no candidate and a percent of 0")
    else:
        if not isinstance(version, str) or not VERSION.fullmatch(version):
            raise ValueError("candidate_version must be a positive version string")
        if isinstance(percent, bool) or not isinstance(percent, int) or not 1 <= percent <= MAX_PERCENT:
            raise ValueError(f"percent must be a whole number from 1 to {MAX_PERCENT}")
    return dict(raw)
