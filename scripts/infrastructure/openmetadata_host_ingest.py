"""Run the Glue mirror and dbt ingestion of the metadata job on the host, against the persistent OpenMetadata catalog.

Runs inside the ingestion image as ``python -m scripts.infrastructure.openmetadata_host_ingest --config-dir D --work-dir W glue dbt``. The dbt source needs
``FEATURES_RUN``, the ID of the features run, and ingests the dbt copy of each layer run of the chain that run recorded (``openmetadata_chain``), never a latest
copy. The ``raw_files`` source, last, writes the raw files of the bronze run that the silver run of that chain was bound to (the input run in its launch claim)
onto the bronze tables (``openmetadata_raw_files``); a silver run started on ``latest`` names none, which is said and not an error. The job is a bridge
task that holds only this one container, so the catalog is not on loopback: ``OM_API`` names the catalog API on the host's private address, and the bot token
arrives in ``OM_INGESTION_TOKEN`` as a task secret. The token is never written to a file. Each source goes through ``openmetadata_ingest.main``, so its refusal
of a profiling or sampling configuration, its variable check and its dbt download stay the ones of the catalog task. A dbt layer run without a
``manifest.json`` is skipped by that source (exit 3 there); the later sources still run and the task then ends with 3, so the task fails and its alert fires.
Exit codes: 0 done, 1 a source failed, 2 refused, 3 done but a dbt layer was skipped for want of its manifest.json.
Output names variables and exception classes and never prints a value.
"""

from __future__ import annotations

import argparse
import ipaddress
import os
import re
import sys
from collections.abc import Callable, Mapping
from functools import partial
from pathlib import Path
from typing import Any

from scripts import resource_peaks
from scripts.infrastructure import openmetadata_chain as chain
from scripts.infrastructure import openmetadata_ingest as ingest
from scripts.infrastructure import startup_check

RAW_FILES = "raw_files"  # the source that writes each bronze table's raw files onto the table (openmetadata_raw_files), after the Glue and dbt sources
# The catalog is reached over the bridge at the host's address: an http URL whose host is a private IPv4 address, port 8585 and the path /api, and nothing else.
API_ADDRESS = re.compile(r"http://([0-9]{1,3}(?:\.[0-9]{1,3}){3}):8585/api")
PRIVATE_NETWORKS = tuple(ipaddress.IPv4Network(network) for network in ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"))


def catalog_address_is_valid(api: str) -> bool:
    """True only for http://<RFC 1918 IPv4 address>:8585/api. An octet over 255 or with a leading zero, a public, loopback or link-local address, another port
    and any other path are refused, so the token can only go to the host's own catalog."""
    match = API_ADDRESS.fullmatch(api)
    if match is None:
        return False
    try:
        host = ipaddress.IPv4Address(match.group(1))
    except ValueError:
        return False
    return any(host in network for network in PRIVATE_NETWORKS)


def main(
    argv: list[str],
    env: Mapping[str, str] | None = None,
    workflow: Callable[[dict[str, Any]], None] | None = None,
    s3_client: Any = None,
    http: Callable[[str, str, Mapping[str, str], bytes | None], tuple[int, Any]] | None = None,
) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("sources", nargs="+", choices=(*ingest.SOURCES, RAW_FILES))
    parser.add_argument("--config-dir", type=Path, required=True)
    parser.add_argument("--work-dir", type=Path, required=True)
    options = parser.parse_args(argv)
    environment = dict(os.environ if env is None else env)
    api = environment.get("OM_API", "")
    token = environment.get("OM_INGESTION_TOKEN", "")
    if not catalog_address_is_valid(api):
        sys.stderr.write("openmetadata_host_ingest: OM_API must be http://<private IPv4 address>:8585/api, with a 10., 172.16 to 172.31 or 192.168. address\n")
        return 2
    if not token:
        sys.stderr.write("openmetadata_host_ingest: OM_INGESTION_TOKEN is not set\n")
        return 2
    try:
        startup_check.check_phase("metadata", "ingest")
    except startup_check.StartupCheckError as refusal:
        sys.stderr.write(f"openmetadata_host_ingest: {refusal}\n")
        return 1
    run = ingest.run_workflow if workflow is None else workflow
    layers: dict[str, str] = {}
    bronze: str | None = None
    if "dbt" in options.sources or RAW_FILES in options.sources:
        # The dbt source ingests the copies of the three layer runs of the chain that FEATURES_RUN records, and the raw files source writes the files of the
        # bronze run that the chain's silver run was bound to. There is no latest and no fixed key to fall back to.
        features_run = environment.get("FEATURES_RUN", "")
        if not chain.RUN_ID.fullmatch(features_run):
            sys.stderr.write("openmetadata_host_ingest: FEATURES_RUN must be the ID of the features run whose chain is ingested; there is no latest\n")
            return 2
        client = s3_client
        if client is None:
            import boto3

            client = boto3.client("s3", region_name=environment.get("AWS_REGION"))
        try:
            layers = chain.layer_runs(client, environment.get("DATA_BUCKET_NAME", ""), features_run)
            if RAW_FILES in options.sources:
                bronze = chain.bronze_run(client, environment.get("DATA_BUCKET_NAME", ""), layers["silver"])
        except chain.ChainError as error:
            sys.stderr.write(f"openmetadata_host_ingest: {error}\n")
            return 1

    def aimed(config: dict[str, Any]) -> None:
        server = config["workflowConfig"]["openMetadataServerConfig"]
        server["hostPort"] = api
        server["securityConfig"]["jwtToken"] = token
        run(config)

    skipped_layers = False
    for source in options.sources:
        if source == RAW_FILES:
            if bronze is None:
                sys.stderr.write(
                    "openmetadata_host_ingest: the silver run was started without a bronze run, so no bronze run is recorded and no raw files are written\n"
                )
                continue
            from scripts.infrastructure import openmetadata_raw_files as raw_files  # imports this module, so it is loaded here

            code = raw_files.write_runs(api, token, client, environment.get("DATA_BUCKET_NAME", ""), [bronze], raw_files.http_request if http is None else http)
            if code:
                return code
            continue
        # The empty device stands in for the token file: the token comes from the variable and is set in ``aimed`` after the placeholders are filled.
        code = ingest.main(
            [
                source,
                "--config-dir",
                str(options.config_dir),
                "--token-file",
                os.devnull,
                "--work-dir",
                str(options.work_dir),
                *(item for layer, run_id in layers.items() if source == "dbt" for item in ("--dbt-run", f"{layer}={run_id}")),
            ],
            env=environment,
            workflow=aimed,
            s3_client=s3_client,
        )
        if code == ingest.MISSING_MANIFEST_EXIT:
            skipped_layers = True
        elif code:
            return code
    return ingest.MISSING_MANIFEST_EXIT if skipped_layers else 0


if __name__ == "__main__":
    resource_peaks.recorded(partial(main, sys.argv[1:]))
