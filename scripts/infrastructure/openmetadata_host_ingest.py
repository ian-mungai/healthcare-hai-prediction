"""Run the Polaris and dbt ingestion of the metadata job on the host, against the persistent OpenMetadata catalog.

Runs inside the ingestion image as ``python -m scripts.infrastructure.openmetadata_host_ingest --config-dir D --work-dir W polaris dbt``. The job is a bridge
task that holds only this one container, so the catalog is not on loopback: ``OM_API`` names the catalog API on the host's private address, and the bot token
arrives in ``OM_INGESTION_TOKEN`` as a task secret. The token is never written to a file. Each source goes through ``openmetadata_ingest.main``, so its refusal
of a profiling or sampling configuration, its variable check and its dbt download stay the ones of the catalog task. Exit codes: 0 done, 1 a source failed,
2 refused. Output names variables and exception classes and never prints a value.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

from scripts.infrastructure import openmetadata_ingest as ingest

# A private IPv4 address and a port: the catalog is reached over the bridge by the host address, never by a name that could resolve elsewhere or by loopback.
API_ADDRESS = re.compile(r"http://(?!127\.)(?:[0-9]{1,3}\.){3}[0-9]{1,3}:[0-9]{2,5}/api")


def main(argv: list[str], env: Mapping[str, str] | None = None, workflow: Callable[[dict[str, Any]], None] | None = None, s3_client: Any = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("sources", nargs="+", choices=ingest.SOURCES)
    parser.add_argument("--config-dir", type=Path, required=True)
    parser.add_argument("--work-dir", type=Path, required=True)
    options = parser.parse_args(argv)
    environment = dict(os.environ if env is None else env)
    api = environment.get("OM_API", "")
    token = environment.get("OM_INGESTION_TOKEN", "")
    if not API_ADDRESS.fullmatch(api):
        sys.stderr.write("openmetadata_host_ingest: OM_API must be http://<private IPv4 address>:<port>/api and not a loopback address\n")
        return 2
    if not token:
        sys.stderr.write("openmetadata_host_ingest: OM_INGESTION_TOKEN is not set\n")
        return 2
    run = ingest.run_workflow if workflow is None else workflow

    def aimed(config: dict[str, Any]) -> None:
        server = config["workflowConfig"]["openMetadataServerConfig"]
        server["hostPort"] = api
        server["securityConfig"]["jwtToken"] = token
        run(config)

    for source in options.sources:
        # The empty device stands in for the token file: the token comes from the variable and is set in ``aimed`` after the placeholders are filled.
        code = ingest.main(
            [source, "--config-dir", str(options.config_dir), "--token-file", os.devnull, "--work-dir", str(options.work_dir)],
            env=environment,
            workflow=aimed,
            s3_client=s3_client,
        )
        if code:
            return code
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
