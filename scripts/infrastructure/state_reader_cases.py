"""Cases for the two state readers (``alert_email.state_addresses`` and ``host_session.services_state_serial``) against the real Terraform command line.

The stand-in terraform of the owner cases only models what its author expects. These cases run the pinned ``terraform`` in scratch stack folders with state in
the S3 backend that ``state_migration`` writes, against its loopback bucket stand-in (``StateBucket``) with fixture-only keys. Nothing leaves the machine:
the plugin folder is empty (the stacks declare no provider), the endpoints are the loopback bucket and no real profile, key or network is used. Each case reads
one situation through both readers: no state object (Terraform prints an empty state from ``state pull`` and fails ``state list``), the same with an empty local
file, a persisted state with and without a local file, a persisted state with no resources and a read that the bucket denies.
"""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any
from unittest import mock

from scripts.infrastructure import alert_email, host_session, state_migration
from scripts.infrastructure.state_migration_s3_stand_in import StateBucket
from scripts.process import CompletedProcess, run_command

PROFILE = "synthetic-review-profile"
KEY = "REVIEWONLYKEY"
INIT_SECONDS = 60
ROOT_OUTPUT = {"synthetic": {"value": "fixture", "type": "string"}}
ALERT_ADDRESSES = {alert_email.ALERTS_ADDRESS, alert_email.BUDGET_ADDRESS}


def state_document(serial: int, alerts: bool = False) -> dict[str, Any]:
    """A persisted state of version 4. With ``alerts`` it holds the two subscriptions of the alerts stack."""
    resources = (
        [
            {
                "mode": "managed",
                "type": "aws_sns_topic_subscription",
                "name": "email",
                "provider": 'provider["registry.terraform.io/hashicorp/aws"]',
                "instances": [
                    {"index_key": kind, "schema_version": 0, "attributes": {"id": f"synthetic-{kind}", "endpoint": "synthetic-fixture"}}
                    for kind in ("alerts", "budget")
                ],
            }
        ]
        if alerts
        else []
    )
    return {"version": 4, "terraform_version": "1.16.4", "serial": serial, "lineage": "synthetic-state", "outputs": {}, "resources": resources}


def terraform(folder: Path, arguments: list[str]) -> CompletedProcess[str]:
    return run_command("terraform", [f"-chdir={folder}", *arguments], timeout=INIT_SECONDS)


def readers() -> dict[str, Any]:
    """What each reader says now: the alerts addresses (a sorted list, ``None`` for no state or the status word of a stop) and the services serial."""
    try:
        found = alert_email.state_addresses()
        addresses: Any = None if found is None else sorted(found)
    except alert_email.Stop as error:
        addresses = str(error)
    serial = host_session.services_state_serial({"aws_profile": PROFILE, "aws_region": "us-west-2"}, lambda: 30.0)
    return {"alerts": addresses, "serial": serial}


def put(bucket: StateBucket, name: str, document: dict[str, Any]) -> None:
    """Store a persisted state of one stack in the loopback bucket, as the migration does."""
    bucket.objects[state_migration.state_key(name)] = json.dumps(document).encode()


def checks(root: Path) -> dict[str, bool]:
    """The checks of every situation. ``root`` is a scratch folder the caller removes."""
    results: dict[str, bool] = {}
    if run_command("terraform", ["version"], timeout=INIT_SECONDS).returncode:
        return {"the real terraform command runs": False}
    folder = root / "fixture"
    plugins = folder / "plugins_empty"
    plugins.mkdir(parents=True)
    empty_config, empty_credentials = root / "aws_config", root / "aws_credentials"
    empty_config.write_text("")
    empty_credentials.write_text("")
    environment = {key: os.environ[key] for key in ("PATH", "HOME", "TMPDIR", "LANG") if key in os.environ}
    environment.update(
        AWS_PROFILE=PROFILE,
        AWS_CONFIG_FILE=str(empty_config),
        AWS_SHARED_CREDENTIALS_FILE=str(empty_credentials),
        AWS_EC2_METADATA_DISABLED="true",
        AWS_REGION="us-west-2",
        AWS_MAX_ATTEMPTS="1",
        CHECKPOINT_DISABLE="1",
        NO_PROXY="127.0.0.1,localhost",
    )
    alerts, services = folder / "infra/alerts", folder / "infra/services"
    original = Path.cwd()
    with StateBucket({KEY: "project"}) as bucket, mock.patch.dict(os.environ, environment, clear=True):
        os.environ.update(AWS_ENDPOINT_URL_S3=bucket.endpoint, AWS_ENDPOINT_URL_STS=bucket.endpoint, AWS_ENDPOINT_URL_IAM=bucket.endpoint)
        profile_config = folder / "backend_profile"
        profile_config.write_text("[profile review-loopback]\nregion = us-west-2\n")
        for name, path in (("alerts", alerts), ("services", services)):
            path.mkdir(parents=True)
            (path / "main.tf").write_text('terraform {\n  required_version = "~> 1.16.1"\n}\n')
            (path / "backend.tf").write_text(state_migration.backend_text(name))
            options = folder / f"backend_{name}.hcl"
            options.write_text(
                f'bucket = "{bucket.name}"\nregion = "us-west-2"\nuse_path_style = true\naccess_key = "{KEY}"\nsecret_key = "synthetic-fixture-only"\n'
                f'profile = "review-loopback"\nskip_requesting_account_id = true\nshared_config_files = ["{profile_config}"]\n'
            )
            started = terraform(path, ["init", "-reconfigure", "-input=false", "-no-color", f"-backend-config={options}", f"-plugin-dir={plugins}"])
            results[f"the {name} stack initializes against the loopback bucket"] = started.returncode == 0
        if not all(results.values()):
            return results
        try:
            os.chdir(folder)
            with mock.patch.object(host_session, "SERVICES_DIRECTORY", services.resolve()):
                missing = readers()
                results["no state object: the alerts reader reports no state"] = missing["alerts"] is None
                results["no state object: the made up empty state is no serial"] = missing["serial"] is None
                for path in (alerts, services):
                    (path / "terraform.tfstate").write_text("")
                empty = readers()
                results["no state object and an empty local file: the alerts reader reports no state"] = empty["alerts"] is None
                results["no state object and an empty local file: no serial"] = empty["serial"] is None
                for path in (alerts, services):
                    (path / "terraform.tfstate").unlink()
                put(bucket, "alerts", state_document(61, alerts=True))
                put(bucket, "services", state_document(62))
                kept = readers()
                results["a persisted state with no local file: the alerts reader lists both subscriptions"] = kept["alerts"] == sorted(ALERT_ADDRESSES)
                results["a persisted state with no local file: the serial is read"] = kept["serial"] == 62
                for path in (alerts, services):
                    (path / "terraform.tfstate").write_text("")
                local = readers()
                results["a persisted state and an empty local file: the same answers"] = local == kept
                put(bucket, "alerts", state_document(9))
                zero = readers()
                results["a persisted state with no resources is a state, not an absent one"] = zero["alerts"] == []
                for label, document in (
                    ("no lineage", {"version": 4, "terraform_version": "1.16.4", "serial": 0, "lineage": "", "outputs": {}, "resources": []}),
                    (
                        "no lineage and a root output",
                        {"version": 4, "terraform_version": "1.16.4", "serial": 0, "lineage": "", "resources": [], "outputs": ROOT_OUTPUT},
                    ),
                ):
                    put(bucket, "alerts", document)
                    put(bucket, "services", document)
                    written = readers()
                    results[f"a persisted state with serial 0 and {label}: the alerts reader finds a state with no addresses"] = written["alerts"] == []
                    results[f"a persisted state with serial 0 and {label}: the serial is 0"] = written["serial"] == 0
                bucket.objects.clear()
                gone = readers()
                results["no object again after the written ones: no state"] = gone["alerts"] is None and gone["serial"] is None
                put(bucket, "alerts", state_document(9))
                put(bucket, "services", state_document(62))
                bucket.faults.deny_all = True
                denied = readers()
                results["a denied read is a failure, not an absent state"] = denied["alerts"] == "state: failed"
                results["a denied read gives no serial"] = denied["serial"] is None
                bucket.faults.deny_all = False
        finally:
            os.chdir(original)
        writes = [seen for seen in bucket.requests() if seen.method in {"PUT", "DELETE"}]
        results["the readers write nothing to the bucket"] = not writes
    return results


def run() -> dict[str, bool]:
    with tempfile.TemporaryDirectory(prefix="state_readers_") as folder:
        return checks(Path(folder))
