"""Run one OpenMetadata ingestion source of the catalog task, after proving its configuration has no profiler and no sampling.

Runs inside the ingestion image as ``python openmetadata_ingest.py glue|dbt --config-dir D --token-file F --work-dir W``. The persistent catalog
gives the token through ``--token-env NAME`` and its address through ``--server-url URL`` instead. OpenMetadata 2.0.7 has no Iceberg connector, so the
``glue`` source reads the gold and features tables from their read-only Glue mirror with the task role (owner decision of Nov 7 2026). The ``dbt`` source
names, in its copy of the manifest, the Glue database of each gold and features model, so that dbt attaches to those tables.
It refuses a configuration that profiles, samples
or reads rows before it fills any placeholder, so a value from the environment can never switch one on. Some tables can hold personal fields outside
the Public research-data scope, which is why this guard exists beside the read-only role and the server setting that disables ingestion pipelines.
Exit codes: 0 done, 1 the run failed, 2 the configuration was refused. Output names variables and exception classes and never prints a value.
"""

from __future__ import annotations

import argparse
import importlib
import json
import os
import re
import sys
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

SOURCES = ("glue", "dbt")
ALLOWED_SOURCE_TYPES = frozenset({"glue", "dbt"})
# The script's own setting in dbt.json: the OpenMetadata database and the Glue schema of each mirrored dbt model folder. It never reaches the workflow.
MAPPING_KEY = "hai_glue_mapping"
ALLOWED_PIPELINES = frozenset({"DatabaseMetadata", "DBT"})
# Normalized key names (lower case, letters and digits only) that start profiling, sampling, classification or row reads.
BANNED_KEYS = frozenset(
    {
        "profiler",
        "profile",
        "profilesample",
        "profilesampletype",
        "sampling",
        "samplingmethod",
        "samplingmethodtype",
        "randomizedsample",
        "generatesampledata",
        "ingestsampledata",
        "storesampledata",
        "sampledata",
        "sampledatacount",
        "sampledatastorageconfig",
        "computemetrics",
        "computetablemetrics",
        "computecolumnmetrics",
        "processingengine",
        "enableautoclassification",
        "autoclassification",
        "processor",
        "testsuite",
    }
)
OFF_VALUES = frozenset({"false", "0", "off", "no", "none", "null", ""})
PLACEHOLDER = re.compile(r"\$\{([A-Z][A-Z0-9_]*)\}")
SERVER_URL = re.compile(r"^http://(?:[0-9]{1,3}\.){3}[0-9]{1,3}:[0-9]{2,5}/api$")
DBT_FILES = ("manifest.json", "catalog.json", "run_results.json")
DBT_PREFIX = "metadata/dbt/"
DBT_PATH_KEYS = {"catalog.json": "dbtCatalogFilePath", "run_results.json": "dbtRunResultsFilePath"}


def explicitly_off(value: Any) -> bool:
    """True for a value that plainly switches a feature off: false, zero, nothing or the text false."""
    if isinstance(value, dict | list):
        return False
    return value is None or str(value).strip().lower() in OFF_VALUES


def violations(node: Any, path: str = "") -> list[str]:
    """Name every key or setting of an ingestion configuration that profiles, samples or reads rows. An empty list means none was found."""
    found: list[str] = []
    if isinstance(node, dict):
        for key, value in node.items():
            here = f"{path}.{key}" if path else str(key)
            if re.sub(r"[^a-z0-9]", "", str(key).lower()) in BANNED_KEYS and not explicitly_off(value):
                found.append(f"{here} is on")
            found.extend(violations(value, here))
        source = node.get("source")
        if isinstance(source, dict):
            if str(source.get("type", "")).lower() not in ALLOWED_SOURCE_TYPES:
                found.append(f"{path + '.' if path else ''}source.type {source.get('type')!r} is not an allowed source")
            pipeline = ((source.get("sourceConfig") or {}).get("config") or {}).get("type")
            if pipeline not in ALLOWED_PIPELINES:
                found.append(f"{path + '.' if path else ''}source.sourceConfig.config.type {pipeline!r} is not a metadata-only pipeline")
    elif isinstance(node, list):
        for index, item in enumerate(node):
            found.extend(violations(item, f"{path}[{index}]"))
    return found


def fill(node: Any, values: Mapping[str, str], missing: set[str]) -> Any:
    """Replace each ``${NAME}`` in the strings of a parsed configuration. A value stays data and is never parsed again. Unknown names go into ``missing``."""
    if isinstance(node, dict):
        return {key: fill(value, values, missing) for key, value in node.items()}
    if isinstance(node, list):
        return [fill(item, values, missing) for item in node]
    if isinstance(node, str):

        def replace(match: re.Match[str]) -> str:
            if match.group(1) not in values:
                missing.add(match.group(1))
                return match.group(0)
            return values[match.group(1)]

        return PLACEHOLDER.sub(replace, node)
    return node


def fetch_dbt(s3: Any, bucket: str, folder: Path) -> list[str]:
    """Download the dbt artifacts into ``folder`` and return the names that were not found. A missing manifest is a failure for the caller."""
    folder.mkdir(parents=True, exist_ok=True)
    absent = []
    for name in DBT_FILES:
        try:
            s3.download_file(bucket, DBT_PREFIX + name, str(folder / name))
        except Exception:
            absent.append(name)
            (folder / name).unlink(missing_ok=True)
    return absent


def place_models(folder: Path, mapping: Mapping[str, Any]) -> int:
    """Point every dbt model of a mirrored layer at its Glue table in the downloaded manifest and catalog; return how many models moved.

    dbt runs on DuckDB, so a model's database is ``staging`` and its schema ``main``. OpenMetadata 2.0.7 finds the table of a dbt node as
    ``service.database.schema.table`` and its ``catalogMapping`` can change only the database. The layer is the model's folder under the project
    (``fqn[1]``). Silver models, tests, seeds and sources keep what dbt wrote: no Glue table exists for them.
    """
    schemas = mapping["schemas"]
    manifest_path = folder / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    moved: dict[str, str] = {}
    for key, node in (manifest.get("nodes") or {}).items():
        fqn = node.get("fqn") or []
        if node.get("resource_type") == "model" and len(fqn) > 2 and fqn[1] in schemas:
            node["database"], node["schema"] = mapping["database"], schemas[fqn[1]]
            moved[key] = schemas[fqn[1]]
    manifest_path.write_text(json.dumps(manifest))
    catalog_path = folder / "catalog.json"
    if catalog_path.is_file():
        catalog = json.loads(catalog_path.read_text())
        for key, schema in moved.items():
            metadata = ((catalog.get("nodes") or {}).get(key) or {}).get("metadata")
            if isinstance(metadata, dict):
                metadata["database"], metadata["schema"] = mapping["database"], schema
        catalog_path.write_text(json.dumps(catalog))
    return len(moved)


def run_workflow(config: dict[str, Any]) -> None:
    """Run the OpenMetadata metadata workflow and raise when it recorded a failure."""
    workflow = importlib.import_module("metadata.workflow.metadata").MetadataWorkflow.create(config)
    workflow.execute()
    workflow.raise_from_status()
    workflow.print_status()
    workflow.stop()


def main(argv: list[str], env: Mapping[str, str] | None = None, workflow: Callable[[dict[str, Any]], None] | None = None, s3_client: Any = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", choices=SOURCES)
    parser.add_argument("--config-dir", type=Path, required=True)
    parser.add_argument("--token-file", type=Path, help="file that holds the ingestion bot token (the finite catalog task)")
    parser.add_argument("--token-env", help="name of the environment variable that holds the ingestion bot token (the persistent catalog)")
    parser.add_argument("--server-url", help="OpenMetadata API address that replaces the one in the configuration, for example http://10.0.1.20:8585/api")
    parser.add_argument("--work-dir", type=Path, required=True)
    options = parser.parse_args(argv)
    environment = dict(os.environ if env is None else env)
    run = run_workflow if workflow is None else workflow
    config = json.loads((options.config_dir / f"{options.source}.json").read_text())
    mapping = config.pop(MAPPING_KEY, None)
    refused = violations(config)
    if refused:
        sys.stderr.write(f"openmetadata_ingest: refused {options.source}.json: {'; '.join(refused)}\n")
        return 2
    if (options.token_file is None) == (options.token_env is None):
        sys.stderr.write("openmetadata_ingest: give exactly one token source, --token-file or --token-env\n")
        return 2
    if options.server_url is not None and not SERVER_URL.fullmatch(options.server_url):
        sys.stderr.write("openmetadata_ingest: the server address must be an http address with a private IPv4 address, a port and the path /api\n")
        return 2
    values = dict(environment)
    if options.token_env is not None:
        values["OM_INGESTION_TOKEN"] = environment.get(options.token_env, "").strip()
        if not values["OM_INGESTION_TOKEN"]:
            sys.stderr.write(f"openmetadata_ingest: the variable {options.token_env} is not set\n")
            return 2
    else:
        try:
            values["OM_INGESTION_TOKEN"] = options.token_file.read_text().strip()
        except OSError:
            sys.stderr.write("openmetadata_ingest: the ingestion token file cannot be read\n")
            return 2
    if options.server_url is not None:
        config["workflowConfig"]["openMetadataServerConfig"]["hostPort"] = options.server_url
    if options.source == "dbt":
        folder = options.work_dir / "dbt"
        client = s3_client
        if client is None:
            import boto3

            client = boto3.client("s3", region_name=environment.get("AWS_REGION"))
        absent = fetch_dbt(client, environment.get("DATA_BUCKET_NAME", ""), folder)
        if "manifest.json" in absent:
            sys.stderr.write("openmetadata_ingest: manifest.json is missing from the dbt artifacts\n")
            return 1
        if mapping is not None:
            try:
                moved = place_models(folder, mapping)
            except (OSError, ValueError, KeyError, TypeError, AttributeError) as error:
                sys.stderr.write(f"openmetadata_ingest: the dbt artifacts cannot be mapped to the Glue tables: {type(error).__name__}\n")
                return 1
            sys.stdout.write(f"openmetadata_ingest: {moved} dbt models point at their Glue tables\n")
        values["DBT_ARTIFACT_DIR"] = str(folder)
        settings = config["source"]["sourceConfig"]["config"]["dbtConfigSource"]
        for name in absent:
            settings.pop(DBT_PATH_KEYS[name], None)
    missing: set[str] = set()
    filled = fill(config, values, missing)
    if missing:
        sys.stderr.write(f"openmetadata_ingest: these variables are not set: {', '.join(sorted(missing))}\n")
        return 2
    try:
        run(filled)
    except Exception as error:
        sys.stderr.write(f"openmetadata_ingest: {options.source} failed: {type(error).__name__}\n")
        return 1
    sys.stdout.write(f"openmetadata_ingest: {options.source} done\n")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
