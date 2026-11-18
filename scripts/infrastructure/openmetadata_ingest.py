"""Run one OpenMetadata ingestion source of the catalog task, after proving its configuration has no profiler and no sampling.

Runs inside the ingestion image as ``python openmetadata_ingest.py glue|dbt|mlflow --config-dir D --token-file F --work-dir W``. The persistent catalog
gives the token through ``--token-env NAME`` and its address through ``--server-url URL`` instead. OpenMetadata 2.0.7 has no Iceberg connector, so the
``glue`` source reads the tables of every layer, bronze included, from their read-only Glue mirror with the task role (owner decision of Nov 7 2026). The
``mlflow`` source reads the registered models of the project's MLflow registry with the login in MLFLOW_TRACKING_USERNAME and MLFLOW_TRACKING_PASSWORD. The
``dbt`` source names, in its copy of the manifest, the Glue database of each model and of each bronze source, so that dbt attaches to those tables.
A saved dbt copy that still names a ``hai_mirror_<layer>`` schema anywhere (a run from before the rename: any text or key of its three files) is refused with
exit 1 before any workflow starts, and is never translated. It refuses a configuration that profiles, samples or reads rows before it fills any
placeholder, so a value from the environment can never switch one on. Some tables can hold personal fields outside the Public research-data scope, which is
why this guard exists beside the read-only role and the server setting that disables ingestion pipelines.
A layer run whose dbt artifacts have no ``manifest.json`` is skipped and never fails the other layers' ingestion (owner decision of Nov 17 2026): each layer
with a manifest is ingested, then one line names each skipped layer and its prefix and the run ends with exit 3, so the Airflow metadata task fails
(it passes only on exit 0) and its failure alert fires and the gap is never silent. A refused read or a stale copy still stops with exit 1 before
anything is sent.
Exit codes: 0 done, 1 the run failed, 2 the configuration was refused, 3 done for the layers with a manifest and at least one layer skipped for want of one
(every layer skipped sends nothing). Output names variables and exception classes and never prints a value.
"""

from __future__ import annotations

import argparse
import importlib
import json
import os
import re
import sys
from collections.abc import Callable, Iterator, Mapping
from pathlib import Path
from typing import Any

from orchestration.training_contract import RUN_ID as RUN_ID_PATTERN
from orchestration.training_contract import STORED_RUN as STORED_RUN_PATTERN
from scripts.infrastructure import openmetadata_metrics

SOURCES = ("glue", "dbt", "mlflow")
ALLOWED_SOURCE_TYPES = frozenset({"glue", "dbt", "mlflow"})
# The script's own setting in dbt.json: the OpenMetadata database and the Glue schema of each mirrored dbt model folder. It never reaches the workflow.
MAPPING_KEY = "hai_glue_mapping"
ALLOWED_PIPELINES = frozenset({"DatabaseMetadata", "DBT", "MlModelMetadata"})
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
SQL_RELATION = re.compile(r'(?<![\w."])"?([A-Za-z0-9_]+)"?\s*\.\s*"?([A-Za-z0-9_]+)"?\s*\.\s*"?([A-Za-z0-9_]+)"?')  # a three-part relation of compiled SQL
DBT_LAYERS = ("silver", "gold", "features")  # one dbt workflow per layer run, in chain order
DBT_RUN_PREFIX = "metadata/dbt/{layer}/{run}/"  # the write-once copy of each layer run (scripts/lakehouse/layer_run.py)
OLD_MIRROR_PREFIX = "hai_mirror_"  # the Glue databases of the mirror before the rename of Nov 17 2026 (hai_mirror_<layer>)
DBT_RUN = re.compile(rf"^(silver|gold|features)=({RUN_ID_PATTERN})$")
# The layer runs that the host ingestion resolves from stored pins, the stamp form of before the run ID redesign too (the command takes ``DBT_RUN``).
STORED_DBT_RUN = re.compile(rf"^(silver|gold|features)=({STORED_RUN_PATTERN})$")
MISSING_MANIFEST_EXIT = 3  # at least one dbt batch had no manifest.json and was skipped; the others were ingested


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


NOT_FOUND = frozenset({"404", "NoSuchKey", "NotFound"})


class FetchFailed(RuntimeError):
    """A dbt artifact could not be read for a reason other than its absence."""


def fetch_dbt(s3: Any, bucket: str, folder: Path, prefix: str = DBT_PREFIX) -> list[str]:
    """Download the dbt artifacts under ``prefix`` into ``folder`` and return the names that do not exist. A missing manifest makes the caller skip the batch.

    Only a genuine not-found is absent. A refused read, a timeout or any other failure raises ``FetchFailed``, so the ingestion never runs on a partial set
    of files that it took for complete."""
    from botocore.exceptions import BotoCoreError, ClientError

    folder.mkdir(parents=True, exist_ok=True)
    absent = []
    for name in DBT_FILES:
        try:
            s3.download_file(bucket, prefix + name, str(folder / name))
        except ClientError as error:
            (folder / name).unlink(missing_ok=True)
            code = str(error.response.get("Error", {}).get("Code", ""))
            if code not in NOT_FOUND:
                raise FetchFailed(f"{prefix}{name} cannot be read: {code or 'ClientError'}") from None
            absent.append(name)
        except (BotoCoreError, OSError) as error:
            (folder / name).unlink(missing_ok=True)
            raise FetchFailed(f"{prefix}{name} cannot be read: {type(error).__name__}") from None
    return absent


class StaleArtifacts(ValueError):
    """A downloaded dbt copy names a Glue schema that the mirror config no longer has, so it comes from a run before a rename."""


def stale_relation(text: Any, mapping: Mapping[str, Any]) -> str:
    """The first three-part relation in ``text`` whose schema is a ``hai_mirror_`` one, or is of the mirror's database and not a mirror schema; else ``""``."""
    schemas = set(mapping["schemas"].values())
    for match in SQL_RELATION.finditer(text) if isinstance(text, str) else ():
        database, schema, table = match.groups()
        if schema.startswith(OLD_MIRROR_PREFIX) or (database == mapping["database"] and schema not in schemas):
            return f"{database}.{schema}.{table}"
    return ""


def stale_place(database: Any, schema: Any, mapping: Mapping[str, Any]) -> str:
    """``database.schema`` when a node placed there names a schema the mirror config no longer has; else ``""``."""
    if isinstance(schema, str) and (
        schema.startswith(OLD_MIRROR_PREFIX) or (database == mapping["database"] and schema not in set(mapping["schemas"].values()))
    ):
        return f"{database}.{schema}"
    return ""


def old_names_pattern(mapping: Mapping[str, Any]) -> re.Pattern[str]:
    """The old mirror schema names (``hai_mirror_<layer>``, one per layer the mapping lists) as whole identifiers, in any case, quoted or not, any qualifier."""
    names = sorted(f"{OLD_MIRROR_PREFIX}{layer}" for layer in mapping["schemas"])
    return re.compile(r"(?<![A-Za-z0-9_])(?:" + "|".join(re.escape(name) for name in names) + r")(?![A-Za-z0-9_])", re.IGNORECASE)


def old_name_places(document: Any, pattern: re.Pattern[str]) -> Iterator[tuple[str, str]]:
    """Each ``(path, name)`` where a key or a string value anywhere in ``document`` holds an old mirror schema name, in key order.

    The OpenMetadata dbt connector reads many fields (compiled and raw SQL, relation names, descriptions, column and test text) and may read more in a later
    release, so no field list decides: every key and every string is searched."""
    stack: list[tuple[str, Any]] = [("", document)]
    while stack:
        path, node = stack.pop()
        if isinstance(node, dict):
            children = []
            for key in sorted(node, key=str):
                here = f"{path}.{key}" if path else str(key)
                found = pattern.search(str(key))
                if found:
                    yield here, found.group(0)
                children.append((here, node[key]))
            stack.extend(reversed(children))
        elif isinstance(node, list):
            stack.extend(reversed([(f"{path}[{index}]", item) for index, item in enumerate(node)]))
        elif isinstance(node, str):
            found = pattern.search(node)
            if found:
                yield path, found.group(0)


def check_current_names(folder: Path, mapping: Mapping[str, Any]) -> None:
    """Raise ``StaleArtifacts`` for the first place (in key order) of the downloaded copy that still names a Glue schema outside the mirror config.

    A copy written before the mirror databases took the plain layer names carries ``hai_lakehouse.hai_mirror_<layer>.<table>`` in node placements, relation
    names and compiled SQL. The connector would parse that SQL for lineage and name deleted entities, so the copy is refused and never translated: only runs
    built after the rename are valid (owner decision of Nov 17 2026). Two checks run over ``manifest.json``, then ``catalog.json`` and ``run_results.json``
    when present. The placement check refuses a node placed in a schema of the mirror database that the mirror config does not list. The name check
    refuses any key or string value of the three files that holds an old name ``hai_mirror_<layer>`` as a whole identifier, in any form or qualifier."""
    manifest = json.loads((folder / "manifest.json").read_text())
    for section in ("nodes", "sources"):
        for key in sorted(manifest.get(section) or {}):
            node = manifest[section][key]
            found = stale_place(node.get("database"), node.get("schema"), mapping)
            for field in ("relation_name", "compiled_code", "compiled_sql"):
                found = found or stale_relation(node.get(field), mapping)
            if found:
                raise StaleArtifacts(f"manifest.json node {key} names {found}")
    documents = {"manifest.json": manifest}
    if (folder / "catalog.json").is_file():
        catalog = json.loads((folder / "catalog.json").read_text())
        for section in ("nodes", "sources"):
            for key in sorted(catalog.get(section) or {}):
                metadata = (catalog[section][key] or {}).get("metadata") or {}
                found = stale_place(metadata.get("database"), metadata.get("schema"), mapping)
                if found:
                    raise StaleArtifacts(f"catalog.json node {key} names {found}")
        documents["catalog.json"] = catalog
    if (folder / "run_results.json").is_file():
        results = json.loads((folder / "run_results.json").read_text())
        for item in results.get("results") or []:
            found = stale_relation(item.get("relation_name"), mapping)
            if found:
                raise StaleArtifacts(f"run_results.json node {item.get('unique_id')} names {found}")
        documents["run_results.json"] = results
    pattern = old_names_pattern(mapping)
    for name, document in documents.items():
        for path, found in old_name_places(document, pattern):
            raise StaleArtifacts(f"{name} {path} names {found}")


def place_models(folder: Path, mapping: Mapping[str, Any]) -> int:
    """Point every dbt model of a mirrored layer, and every source of a mirrored namespace, at its Glue table in the downloaded manifest and catalog;
    return how many nodes moved.

    dbt runs on DuckDB, so a model's database is ``staging`` and its schema ``main``, and a bronze source's database is ``lakehouse``. OpenMetadata 2.0.7
    finds the table of a dbt node as ``service.database.schema.table`` and its ``catalogMapping`` can change only the database. The layer is the model's
    folder under the project (``fqn[1]``) and the namespace of a source is its schema (``bronze``). Tests, seeds and the models of other folders keep what
    dbt wrote: no Glue table exists for them.
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
    sources: dict[str, str] = {}
    relations: dict[tuple[str, str, str], str] = {}
    for key, node in (manifest.get("sources") or {}).items():
        if node.get("resource_type") == "source" and node.get("schema") in schemas:
            table = str(node.get("identifier") or node["name"])
            relations[(str(node.get("database", "")), str(node["schema"]), table)] = f'"{mapping["database"]}"."{schemas[node["schema"]]}"."{table}"'
            sources[key] = schemas[node["schema"]]
            node["database"], node["schema"] = mapping["database"], sources[key]
    if relations:
        # The compiled SQL of every node names a source as the build did (lakehouse.bronze.<table>, quoted or not). OpenMetadata's query lineage looks an
        # explicit database and schema up before anything else, so the SQL names the mirror table the source was just placed at. A rewritten copy has none left.
        for node in (manifest.get("nodes") or {}).values():
            for field in ("compiled_code", "compiled_sql"):
                if isinstance(node.get(field), str):
                    node[field] = SQL_RELATION.sub(lambda match: relations.get((match.group(1), match.group(2), match.group(3)), match.group(0)), node[field])
    manifest_path.write_text(json.dumps(manifest))
    catalog_path = folder / "catalog.json"
    if catalog_path.is_file():
        catalog = json.loads(catalog_path.read_text())
        for section, placed in (("nodes", moved), ("sources", sources)):
            for key, schema in placed.items():
                metadata = ((catalog.get(section) or {}).get(key) or {}).get("metadata")
                if isinstance(metadata, dict):
                    metadata["database"], metadata["schema"] = mapping["database"], schema
        catalog_path.write_text(json.dumps(catalog))
    return len(moved) + len(sources)


def fill_dbt(config: dict[str, Any], folder: Path) -> dict[str, Any]:
    """The dbt configuration with ``${DBT_ARTIFACT_DIR}`` set to the folder of one fetched copy; the other placeholders stay for ``fill``."""
    return fill(config, {"DBT_ARTIFACT_DIR": str(folder)}, set())


def run_workflow(config: dict[str, Any]) -> None:
    """Run the OpenMetadata metadata workflow and raise when it recorded a failure."""
    workflow = importlib.import_module("metadata.workflow.metadata").MetadataWorkflow.create(config)
    workflow.execute()
    openmetadata_metrics.record(config, workflow)
    workflow.raise_from_status()
    workflow.print_status()
    workflow.stop()


def main(
    argv: list[str],
    env: Mapping[str, str] | None = None,
    workflow: Callable[[dict[str, Any]], None] | None = None,
    s3_client: Any = None,
    stored_layer_runs: bool = False,
) -> int:
    """Ingest one source. ``stored_layer_runs`` is for the host ingestion, which names the layer runs of the stored pins of a chain: they may be of the stamp
    form of before the run ID redesign. The command line takes the run ID form alone."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", choices=SOURCES)
    parser.add_argument("--config-dir", type=Path, required=True)
    parser.add_argument("--token-file", type=Path, help="file that holds the ingestion bot token (the finite catalog task)")
    parser.add_argument("--token-env", help="name of the environment variable that holds the ingestion bot token (the persistent catalog)")
    parser.add_argument("--server-url", help="OpenMetadata API address that replaces the one in the configuration, for example http://10.0.1.20:8585/api")
    parser.add_argument("--work-dir", type=Path, required=True)
    parser.add_argument(
        "--dbt-run",
        action="append",
        default=[],
        metavar="LAYER=RUN",
        help="dbt only: ingest the copy of one layer run from metadata/dbt/<layer>/<run>/, once per layer; without it the fixed keys are read",
    )
    options = parser.parse_args(argv)
    layer_runs: dict[str, str] = {}
    for item in options.dbt_run:
        match = (STORED_DBT_RUN if stored_layer_runs else DBT_RUN).fullmatch(item)
        if options.source != "dbt" or match is None or match.group(1) in layer_runs:
            sys.stderr.write(f"openmetadata_ingest: --dbt-run {item!r} must be LAYER=RUN, once per layer of {', '.join(DBT_LAYERS)}, for the dbt source only\n")
            return 2
        layer_runs[match.group(1)] = match.group(2)
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
    configs: list[tuple[str, dict[str, Any]]] = [(options.source, config)]
    skipped: list[tuple[str, str]] = []

    def report_skipped() -> None:
        for label, prefix in skipped:
            sys.stderr.write(
                f"openmetadata_ingest: skipped {label} ({prefix}): manifest.json is missing from its dbt artifacts, so nothing of it was ingested\n"
            )

    if options.source == "dbt":
        client = s3_client
        if client is None:
            import boto3

            client = boto3.client("s3", region_name=environment.get("AWS_REGION"))
        batches = [
            (layer, options.work_dir / "dbt" / layer, DBT_RUN_PREFIX.format(layer=layer, run=layer_runs[layer])) for layer in DBT_LAYERS if layer in layer_runs
        ]
        configs = []
        for label, folder, prefix in batches or [("dbt", options.work_dir / "dbt", DBT_PREFIX)]:
            try:
                absent = fetch_dbt(client, environment.get("DATA_BUCKET_NAME", ""), folder, prefix)
            except FetchFailed as error:
                sys.stderr.write(f"openmetadata_ingest: {error}\n")
                return 1
            if "manifest.json" in absent:
                skipped.append((label, prefix))
                continue
            if mapping is not None:
                try:
                    check_current_names(folder, mapping)
                except StaleArtifacts as error:
                    sys.stderr.write(
                        f"openmetadata_ingest: the dbt artifacts of {label} ({prefix}) are from a run before the mirror rename and are refused, "
                        f"nothing was sent: {error}. Ingest the dbt artifacts of a run built after the rename.\n"
                    )
                    return 1
                except (OSError, ValueError, KeyError, TypeError, AttributeError) as error:
                    sys.stderr.write(f"openmetadata_ingest: the dbt artifacts of {label} cannot be read: {type(error).__name__}\n")
                    return 1
                try:
                    moved = place_models(folder, mapping)
                except (OSError, ValueError, KeyError, TypeError, AttributeError) as error:
                    sys.stderr.write(f"openmetadata_ingest: the dbt artifacts of {label} cannot be mapped to the Glue tables: {type(error).__name__}\n")
                    return 1
                sys.stdout.write(f"openmetadata_ingest: {moved} dbt models and sources of {label} point at their Glue tables\n")
            layer_config = json.loads(json.dumps(config))
            settings = layer_config["source"]["sourceConfig"]["config"]["dbtConfigSource"]
            for name in absent:
                settings.pop(DBT_PATH_KEYS[name], None)
            configs.append((label, fill_dbt(layer_config, folder)))
    filled_configs = []
    for label, item in configs:
        missing: set[str] = set()
        filled = fill(item, values, missing)
        if missing:
            sys.stderr.write(f"openmetadata_ingest: these variables are not set: {', '.join(sorted(missing))}\n")
            return 2
        filled_configs.append((label, filled))
    for label, filled in filled_configs:
        try:
            run(filled)
        except Exception as error:
            sys.stderr.write(f"openmetadata_ingest: {label} failed: {type(error).__name__}\n")
            report_skipped()
            return 1
    report_skipped()
    if skipped:
        return MISSING_MANIFEST_EXIT
    sys.stdout.write(f"openmetadata_ingest: {options.source} done\n")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
