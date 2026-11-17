r"""The owner command for the promotion lease of a model: show the lease and resolve a run that will never finish.

Run from the repository root with the project profile in .env (AWS_PROFILE and AWS_REGION), DATA_BUCKET_NAME (the project data bucket) and, for resolve, the
MLflow address and login in the shell:
.venv/bin/python -m scripts.training.promote_owner show --model <model> and
.venv/bin/python -m scripts.training.promote_owner resolve --model <model> --run <run> --owner <name> --reason <text>.
Resolve reads the production alias through the MLflow REST API with the Python standard library, so the host environment needs no MLflow package.

Reach MLflow from the owner's machine through the project Session Manager document of the MLflow web page, the only forward of port 5100 that the project
profile may start. The host must be running (host_session start). The server allows the Host header localhost:5100. Keep the forward open in a second
terminal while resolve runs. It finds the instance ID of the host, which the Auto Scaling group of the project lists, and forwards port 5100 (the commands are
for the dev environment):

INSTANCE=$(aws autoscaling describe-auto-scaling-groups --profile "$AWS_PROFILE" --region us-west-2 \
  --auto-scaling-group-names healthcare_hai_prediction_host_dev --query 'AutoScalingGroups[0].Instances[0].InstanceId' --output text)
aws ssm start-session --profile "$AWS_PROFILE" --region us-west-2 --target "$INSTANCE" \
  --document-name healthcare_hai_prediction_mlflow_forward_dev --parameters localPortNumber=5100

Then set MLFLOW_TRACKING_URI=http://localhost:5100, MLFLOW_TRACKING_USERNAME=admin and MLFLOW_TRACKING_PASSWORD in the shell of the owner command. The password
is the value of the SecureString parameter /healthcare_hai_prediction/mlflow_admin_password. The project profile in AWS_PROFILE may read it
(ssm:GetParameter in the project SSM policy), so read it with that profile into the variable and never print it or write it to a file. Show reads only the
data bucket and needs none of this. If the forward drops, open it again and run resolve again.

A run that sent its alias request and did not see it commit cannot be retried. Its retry fails with promotion_needs_resolution and keeps the lease,
because the MLflow server can still commit the earlier request after the task that sent it ended (promote has the rule). Automatic promotions of the
model stay blocked until the owner resolves the run:

1. Stop every sender of the run (the ECS task and the Airflow retry). show prints the lease, the run's records and the intent. A record
   attempt.json means the alias request was sent.
2. If a retry of the run finishes, retry it and no resolution is needed. A run without attempt.json finishes. One with it finishes only when production
   already names its version.
3. Otherwise run resolve. It refuses when the run holds no lease or already has a receipt. It then forces a new deployment of the MLflow ECS service
   and waits (up to 15 minutes) until every task that ran before is STOPPED and the new deployment is COMPLETED, so no request the old server accepted
   can still commit. It reads the production alias and the tags of its version, writes the Production record of the generation for that version
   (promote/production/<HAI_TYPE>/<generation>.json, as the promote action does; nothing when production names no version), writes
   promote/<run>/abandoned.json with that alias, so the run refuses to start again, then closes the generation. The output names the record, or none.
   With no record the features job keeps refusing online sets until a completed promotion of the model writes one. A failed or late restart, or a record
   that cannot be stored, fails the command and leaves the lease held: run it again. The owner reconciles the production alias by hand, and the next
   promotion then observes whatever production names.

The project profile needs ecs:UpdateService, ecs:DescribeServices, ecs:ListTasks and ecs:DescribeTasks on the MLflow service, and the get, create-only put and
list access to promote/ in the data bucket that the training task role has.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.training import mlflow_alias_read, production_record, promote_lease, promote_receipt, registry
from scripts.training.promote_receipt import ReceiptError

MLFLOW_SERVICE = "mlflow"
MODEL_CONTRACT = Path(__file__).resolve().parents[2] / "data_contracts/training/model_contract.json"


LOGIN_NAMES = ("MLFLOW_TRACKING_USERNAME", "MLFLOW_TRACKING_PASSWORD")


def mlflow_restarter(environ: Mapping[str, str]) -> Callable[[], None]:
    """The restart of the MLflow ECS service through the host session of the project profile. A failure raises ReceiptError."""

    def restart() -> None:
        missing = [name for name in ("MLFLOW_TRACKING_URI", *LOGIN_NAMES) if not environ.get(name)]
        if missing:
            raise ReceiptError(f"{', '.join(missing)} is not set, so the production alias cannot be read after the restart. The lease is still held")
        from scripts.infrastructure import host_session
        from scripts.infrastructure.render_project_config import ConfigurationError

        try:
            host_session.open_session().restart_service(MLFLOW_SERVICE)
        except (host_session.HostSessionError, ConfigurationError) as error:
            raise ReceiptError(f"MLflow was not restarted: {error}. The lease is still held and nothing was recorded") from None

    return restart


class ProductionAfterResolution:
    """The version production names after the restart, and the Production record of the resolved generation for it.

    ``alias`` reads the version and its tags in one call of the MLflow REST API at MLFLOW_TRACKING_URI. ``record`` writes the record of the generation with
    ``production_record.build`` and ``production_record.store``, as the promote action does: the infection type of the model, the version, the resolved run,
    the mode of its stored intent (``resolved`` when the run stored none, so it never sent an alias request), the lease generation and the version's
    ``feature_set`` and ``feature_set_schema_sha256`` tags. It writes nothing when production names no version. ``key`` is the record's key, or ``none``.
    A failure raises ReceiptError and the lease stays held."""

    def __init__(self, client: Any, bucket: str, model: str, environ: Mapping[str, str]) -> None:
        self.client, self.bucket, self.model, self.environ = client, bucket, model, environ
        self.tags: dict[str, str] = {}
        self.key = "none"

    def alias(self) -> str | None:
        try:
            found = mlflow_alias_read.read_version(
                self.environ.get("MLFLOW_TRACKING_URI", ""),
                self.model,
                registry.CHAMPION_ALIAS,
                self.environ.get("MLFLOW_TRACKING_USERNAME", ""),
                self.environ.get("MLFLOW_TRACKING_PASSWORD", ""),
            )
        except mlflow_alias_read.AliasReadError as error:
            raise ReceiptError(f"the production alias of {self.model} cannot be read: {error}. The lease is still held") from None
        if found is None:
            return None
        version, self.tags = found
        return version

    def record(self, lease: promote_lease.Lease, production: str | None) -> None:
        if production is None:
            return
        types = {model: hai_type for hai_type, model in json.loads(MODEL_CONTRACT.read_text())["registered_models"].items()}
        if self.model not in types:
            raise ReceiptError(f"{self.model} is not a registered model of the model contract. The lease is still held")
        records = promote_receipt.records_of(self.client, self.bucket, lease.run_id)
        mode = promote_receipt.stored_intent(self.client, self.bucket, lease.run_id)["mode"] if promote_receipt.INTENT in records else "resolved"
        request = {"model": self.model, "version": production, "run_id": lease.run_id, "mode": mode}
        document = production_record.build(types[self.model], request, lease.generation, self.tags, datetime.now(UTC).isoformat(timespec="seconds"))
        try:
            production_record.store(self.client, self.bucket, document)
        except ReceiptError as error:
            raise ReceiptError(f"{error}. The lease is still held and the run is not fenced") from None
        self.key = production_record.key_of(types[self.model], lease.generation)


def main(arguments: Sequence[str] | None = None, environ: Mapping[str, str] | None = None) -> int:
    """Show or resolve the lease of a model. Exit 0 on success, 2 on a refusal."""
    parser = argparse.ArgumentParser(prog="promote_owner", description=__doc__.splitlines()[0] if __doc__ else None)
    commands = parser.add_subparsers(dest="command", required=True)
    shown = commands.add_parser("show", help="print the lease of a model and the records of its holder")
    resolved = commands.add_parser("resolve", help="close the lease of a run that will never finish and fence the run")
    for command in (shown, resolved):
        command.add_argument("--model", required=True)
    resolved.add_argument("--run", required=True)
    resolved.add_argument("--owner", required=True)
    resolved.add_argument("--reason", required=True)
    options = parser.parse_args(arguments)
    environ = os.environ if environ is None else environ
    bucket = environ.get("DATA_BUCKET_NAME", "")
    if not bucket:
        sys.stderr.write("DATA_BUCKET_NAME is not set\n")
        return 2
    client = promote_receipt.s3_client()
    try:
        if options.command == "show":
            sys.stdout.write(json.dumps(promote_lease.report(client, bucket, options.model), sort_keys=True) + "\n")
        else:
            at = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
            production = ProductionAfterResolution(client, bucket, options.model, environ)
            lease = promote_lease.resolve(
                client, bucket, options.model, options.run, options.owner, options.reason, at, mlflow_restarter(environ), production.alias, production.record
            )
            outcome = {"resolved": lease.key, "run_id": options.run, "outcome": "abandoned", "production_record": production.key}
            sys.stdout.write(json.dumps(outcome, sort_keys=True) + "\n")
    except ReceiptError as error:
        sys.stderr.write(f"{error}\n")
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
