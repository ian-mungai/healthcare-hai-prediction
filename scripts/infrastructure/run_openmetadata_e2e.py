"""Exercise the on-demand OpenMetadata catalog task offline: its ingestion guard, bootstrap, deadline wrapper, read-only policy and DAG.

Nothing here reaches AWS, Docker or a real OpenMetadata server. HTTP stand-ins on loopback sockets answer the routes the scripts call, so the cases prove the
scripts' own behavior and not the real server's. Every case is a function that raises ``AssertionError`` on a defect. The runner writes one record per run.
"""

from __future__ import annotations

import argparse
import contextlib
import copy
import io
import json
import os
import re
import sys
import tempfile
import threading
import time
from collections.abc import Callable, Iterator, Mapping
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from unittest import mock

from scripts.infrastructure.openmetadata_bootstrap import ADMIN_EMAIL
from scripts.process import run_command

REPO = Path(__file__).resolve().parents[2]
INGESTION = REPO / "config" / "metadata" / "ingestion"
OWNER_POLICY = REPO / "infra" / "iam" / "owner_policies" / "openmetadata_catalog_read.json"
DAG_FILE = REPO / "dags" / "metadata_ingest_dag.py"
ADMIN_VALUE = "Synthetic-Admin-1!"
BEARER_VALUE = "synthetic.jwt.token"
CLIENT = ("synthetic-client-id", "synthetic-client-secret")
MUTANTS: dict[str, Callable[[dict[str, Any]], None]] = {
    "profiler pipeline type": lambda c: c["source"]["sourceConfig"]["config"].update(type="Profiler"),
    "sample data on": lambda c: c["source"]["sourceConfig"]["config"].update(generateSampleData=True),
    "underscore spelling": lambda c: c["source"]["sourceConfig"]["config"].update(profile_sample=10),
    "sample count as text": lambda c: c["source"]["sourceConfig"]["config"].update(sampleDataCount="50"),
    "orm profiler processor": lambda c: c.update(processor={"type": "orm-profiler", "config": {}}),
    "metrics on": lambda c: c["source"]["sourceConfig"]["config"].update(computeMetrics=True),
    "auto classification": lambda c: c["source"]["sourceConfig"]["config"].update(enableAutoClassification=True),
    "nested sampling block": lambda c: c["source"]["sourceConfig"]["config"].update(extra={"sampling": {"method": "BERNOULLI"}}),
    "unknown source type": lambda c: c["source"].update(type="postgres"),
    "iceberg source type": lambda c: c["source"].update(type="iceberg"),
}


def require(condition: object, message: object) -> None:
    if not condition:
        raise AssertionError(message)


def load(name: str) -> dict[str, Any]:
    document = json.loads((INGESTION / f"{name}.json").read_text())
    require(isinstance(document, dict), f"{name}.json is not a mapping")
    return document


@contextlib.contextmanager
def quiet() -> Iterator[tuple[io.StringIO, io.StringIO]]:
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        yield out, err


class Stand(ThreadingHTTPServer):
    """A loopback HTTP stand-in that records every request and answers from a route table."""

    def __init__(self, routes: Callable[[str, str, dict[str, Any]], tuple[int, Any]]) -> None:
        self.calls: list[tuple[str, str, dict[str, Any]]] = []
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def handle_any(self) -> None:
                length = int(self.headers.get("Content-Length") or 0)
                raw = self.rfile.read(length) if length else b""
                body = json.loads(raw) if raw else {}
                outer.calls.append((self.command, self.path, body))
                status, payload = routes(self.command, self.path, body)
                data = payload.encode() if isinstance(payload, str) else json.dumps(payload).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            do_GET = do_POST = do_PUT = handle_any

            def log_message(self, *_: Any) -> None:
                return

        super().__init__(("127.0.0.1", 0), Handler)

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.server_address[1]}"


@contextlib.contextmanager
def serving(routes: Callable[[str, str, dict[str, Any]], tuple[int, Any]]) -> Iterator[Stand]:
    server = Stand(routes)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def real_configs_have_no_profiler_or_sampling() -> None:
    from scripts.infrastructure import openmetadata_ingest as ingest

    names = sorted(path.stem for path in INGESTION.glob("*.json"))
    require({"glue", "dbt"} <= set(names) and "polaris" not in names, f"wrong ingestion configs, found {names}")
    for name in names:
        found = ingest.violations(load(name))
        require(found == [], f"{name}.json: {found}")


def every_profiler_or_sampling_mutant_is_refused() -> None:
    from scripts.infrastructure import openmetadata_ingest as ingest

    for label, mutate in MUTANTS.items():
        config = copy.deepcopy(load("glue"))
        mutate(config)
        require(ingest.violations(config), f"mutant not refused: {label}")
    clean = copy.deepcopy(load("glue"))
    clean["source"]["sourceConfig"]["config"].update(generateSampleData=False, computeMetrics=False)
    require(ingest.violations(clean) == [], "an explicit false must stay allowed")


def run_ingest(args: list[str], env: dict[str, str], workflow: Callable[[dict[str, Any]], None], **extra: Any) -> tuple[int, str, str]:
    from scripts.infrastructure import openmetadata_ingest as ingest

    with quiet() as (out, err):
        code = ingest.main(args, env=env, workflow=workflow, **extra)
    return code, out.getvalue(), err.getvalue()


def ingest_environment(root: Path) -> tuple[list[str], dict[str, str]]:
    token_file = root / "ingestion_token"
    token_file.write_text(BEARER_VALUE + "\n")
    env = {
        "POLARIS_URI": "http://10.0.1.20:8181/api/catalog",
        "DATA_BUCKET_NAME": "example-project-data-dev",
        "AWS_REGION": "us-west-2",
    }
    return ["--config-dir", str(INGESTION), "--token-file", str(token_file), "--work-dir", str(root / "work")], env


def ingest_substitutes_values_and_keeps_secrets_out_of_output() -> None:
    seen: list[dict[str, Any]] = []
    with tempfile.TemporaryDirectory() as folder:
        args, env = ingest_environment(Path(folder))
        code, out, err = run_ingest(["glue", *args], env, seen.append)
    require(code == 0, (code, err))
    require(len(seen) == 1, "the workflow must run once")
    text = json.dumps(seen[0])
    require(
        seen[0]["source"]["serviceConnection"]["config"]["awsConfig"]["awsRegion"] == "us-west-2" and BEARER_VALUE in text, "values must reach the workflow"
    )
    require("${" not in text, "a placeholder was left")
    require(BEARER_VALUE not in out + err, "a secret reached the output")


def ingest_takes_the_token_from_the_environment_and_the_persistent_server() -> None:
    seen: list[dict[str, Any]] = []
    with tempfile.TemporaryDirectory() as folder:
        args, env = ingest_environment(Path(folder))
        args = [argument for index, argument in enumerate(args) if argument != "--token-file" and (index == 0 or args[index - 1] != "--token-file")]
        env["OM_INGESTION_TOKEN"] = BEARER_VALUE
        code, out, err = run_ingest(["glue", *args, "--token-env", "OM_INGESTION_TOKEN", "--server-url", "http://10.0.1.20:8585/api"], env, seen.append)
    require(code == 0, (code, err))
    require(len(seen) == 1, "the workflow must run once")
    server = seen[0]["workflowConfig"]["openMetadataServerConfig"]
    require(
        server["hostPort"] == "http://10.0.1.20:8585/api" and server["securityConfig"]["jwtToken"] == BEARER_VALUE,
        "the persistent server and the token must reach the workflow",
    )
    require(BEARER_VALUE not in out + err, "the token reached the output")


def ingest_refuses_a_missing_token_source_or_a_wrong_server_url() -> None:
    with tempfile.TemporaryDirectory() as folder:
        args, env = ingest_environment(Path(folder))
        without_file = [argument for index, argument in enumerate(args) if argument != "--token-file" and (index == 0 or args[index - 1] != "--token-file")]
        env["OM_INGESTION_TOKEN"] = BEARER_VALUE
        none_code, _, none_err = run_ingest(["glue", *without_file], env, lambda _: None)
        empty_code, _, empty_err = run_ingest(["glue", *without_file, "--token-env", "OM_UNSET"], env, lambda _: None)
        bad_url_code, out, err = run_ingest(
            ["glue", *without_file, "--token-env", "OM_INGESTION_TOKEN", "--server-url", "https://example.invalid/api"], env, lambda _: None
        )
    require(none_code == 2 and "token" in none_err.lower(), (none_code, none_err))
    require(empty_code == 2 and "OM_UNSET" in empty_err, (empty_code, empty_err))
    require(bad_url_code == 2 and "server" in err.lower() and BEARER_VALUE not in out + err, (bad_url_code, err))


def ingest_refuses_a_profiler_config_before_the_workflow_starts() -> None:
    seen: list[dict[str, Any]] = []
    with tempfile.TemporaryDirectory() as folder:
        root = Path(folder)
        args, env = ingest_environment(root)
        config = load("glue")
        config["source"]["sourceConfig"]["config"]["generateSampleData"] = True
        bad = root / "bad"
        bad.mkdir()
        (bad / "glue.json").write_text(json.dumps(config))
        args[args.index("--config-dir") + 1] = str(bad)
        code, _, err = run_ingest(["glue", *args], env, seen.append)
    require(code == 2 and seen == [], (code, seen))
    require("generatesampledata" in err.lower(), err)


def ingest_names_a_missing_variable_but_not_a_value() -> None:
    with tempfile.TemporaryDirectory() as folder:
        args, env = ingest_environment(Path(folder))
        del env["AWS_REGION"]
        code, out, err = run_ingest(["glue", *args], env, lambda _: None)
    require(code == 2, code)
    require("AWS_REGION" in err and BEARER_VALUE not in out + err, err)


def ingest_fails_when_the_workflow_fails_without_echoing_the_config() -> None:
    def broken(config: dict[str, Any]) -> None:
        raise RuntimeError(f"upstream said {config['source']['serviceConnection']}")

    with tempfile.TemporaryDirectory() as folder:
        args, env = ingest_environment(Path(folder))
        code, out, err = run_ingest(["glue", *args], env, broken)
    require(code == 1, code)
    require("RuntimeError" in err and BEARER_VALUE not in out + err, err)


class FakeS3:
    def __init__(self, keys: dict[str, bytes], page: int = 1000, deny: set[str] | None = None) -> None:
        self.keys = keys
        self.requested: list[str] = []
        self.page = page
        self.deny = deny or set()
        self.listings: list[str] = []

    def list_objects_v2(self, *, Bucket: str, Prefix: str = "", ContinuationToken: str | None = None, **_: Any) -> dict[str, Any]:
        """A paged listing in key order: ``page`` keys per answer, the next start in ``NextContinuationToken``."""
        self.listings.append(f"{Bucket}/{Prefix}")
        names = sorted(key for key in self.keys if key.startswith(Prefix))
        start = int(ContinuationToken or 0)
        chunk = names[start : start + self.page]
        found: dict[str, Any] = {"Contents": [{"Key": key} for key in chunk], "IsTruncated": start + self.page < len(names)}
        if found["IsTruncated"]:
            found["NextContinuationToken"] = str(start + self.page)
        return found

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        import io

        from botocore.exceptions import ClientError

        self.requested.append(f"{Bucket}/{Key}")
        if Key in self.deny:
            raise ClientError({"Error": {"Code": "AccessDenied", "Message": "Access Denied"}}, "GetObject")
        if Key not in self.keys:
            raise ClientError({"Error": {"Code": "NoSuchKey", "Message": "The specified key does not exist."}}, "GetObject")
        return {"Body": io.BytesIO(self.keys[Key])}

    def download_file(self, bucket: str, key: str, target: str) -> None:
        self.requested.append(f"{bucket}/{key}")
        if key not in self.keys:
            from botocore.exceptions import ClientError

            raise ClientError({"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject")  # what boto3's download_file raises for a missing key
        Path(target).write_bytes(self.keys[key])


def ingest_dbt_downloads_the_artifacts_and_fails_without_a_manifest() -> None:
    keys = {f"metadata/dbt/{name}": b"{}" for name in ("manifest.json", "catalog.json", "run_results.json")}
    seen: list[dict[str, Any]] = []
    with tempfile.TemporaryDirectory() as folder:
        args, env = ingest_environment(Path(folder))
        s3 = FakeS3(keys)
        code, _, err = run_ingest(["dbt", *args], env, seen.append, s3_client=s3)
        require(code == 0, (code, err))
        require(sorted(s3.requested) == sorted(f"example-project-data-dev/{key}" for key in keys), s3.requested)
        require(seen and str(Path(folder) / "work") in json.dumps(seen[0]), "the workflow must read the downloaded files")
        seen.clear()
        del keys["metadata/dbt/manifest.json"]
        code, _, err = run_ingest(["dbt", *args], env, seen.append, s3_client=FakeS3(keys))
    require(code == 1 and seen == [] and "manifest.json" in err, (code, err))


def glue_config_reads_the_two_mirror_databases_with_the_task_role() -> None:
    """OpenMetadata 2.0.7 has no Iceberg connector (open-metadata PR 26365), so the tables come from the Glue mirror (owner decision of Nov 7 2026)."""
    glue = load("glue")
    mirror = json.loads((REPO / "config/lakehouse/athena_mirror.json").read_text())
    connection = glue["source"]["serviceConnection"]["config"]
    filters = glue["source"]["sourceConfig"]["config"]["schemaFilterPattern"]
    require(glue["source"]["type"] == "glue" and connection["type"] == "Glue", "the source must be the OpenMetadata Glue connector")
    require(
        set(connection["awsConfig"]) == {"awsRegion"},
        f"the connector must use the task role's credentials, not keys, a profile or a role: {connection['awsConfig']}",
    )
    require(connection.get("databaseName") == "hai_lakehouse", "one OpenMetadata database, named like the Polaris warehouse, instead of the AWS account ID")
    require(
        filters == {"includes": [f"^{spec['database']}$" for spec in mirror["layers"].values()]},
        f"the connector reads exactly the mirror databases of config/lakehouse/athena_mirror.json: {filters}",
    )
    require(load("dbt")["source"]["serviceName"] == glue["source"]["serviceName"] == "glue_mirror", "dbt must attach to the Glue service's tables")


def dbt_manifest() -> dict[str, Any]:
    def model(layer: str, name: str) -> dict[str, Any]:
        return {"resource_type": "model", "name": name, "fqn": ["hai", layer, name], "database": "staging", "schema": "main", "alias": name}

    return {
        "nodes": {
            "model.hai.dim_state": model("gold", "dim_state"),
            "model.hai.feature_matrix_hospital": model("features", "feature_matrix_hospital"),
            "model.hai.stg_measure": model("silver", "stg_measure"),
            "model.dbt_project_evaluator.fct_check": {**model("silver", "fct_check"), "fqn": ["dbt_project_evaluator", "marts", "fct_check"]},
            "test.hai.not_null_dim_state": {
                "resource_type": "test",
                "name": "not_null_dim_state",
                "fqn": ["hai", "gold", "x"],
                "database": "staging",
                "schema": "main",
            },
        },
        "sources": {"source.hai.bronze.measure": {"resource_type": "source", "database": "lakehouse", "schema": "bronze", "name": "measure"}},
    }


def ingest_dbt_points_gold_and_features_models_at_the_glue_tables() -> None:
    """dbt runs on DuckDB, so every model says database staging and schema main, and a bronze source says database lakehouse. OpenMetadata 2.0.7 looks a dbt
    node up as service.database.schema.table and its catalogMapping changes only the database, so the copy of the manifest the workflow reads names the Glue
    database of each mirrored layer: the models by their folder, the bronze sources by their schema."""
    manifest = dbt_manifest()
    catalog = {"nodes": {key: {"metadata": {"database": "staging", "schema": "main", "name": node["name"]}} for key, node in manifest["nodes"].items()}}
    catalog["sources"] = {"source.hai.bronze.measure": {"metadata": {"database": "lakehouse", "schema": "bronze", "name": "measure"}}}
    keys = {"metadata/dbt/manifest.json": json.dumps(manifest).encode(), "metadata/dbt/catalog.json": json.dumps(catalog).encode()}
    seen: list[dict[str, Any]] = []
    read: dict[str, Any] = {}

    def workflow(config: dict[str, Any]) -> None:
        seen.append(config)
        source = config["source"]["sourceConfig"]["config"]["dbtConfigSource"]
        read["manifest"] = json.loads(Path(source["dbtManifestFilePath"]).read_text())
        read["catalog"] = json.loads(Path(source["dbtCatalogFilePath"]).read_text())

    with tempfile.TemporaryDirectory() as folder:
        args, env = ingest_environment(Path(folder))
        code, _, err = run_ingest(["dbt", *args], env, workflow, s3_client=FakeS3(keys))
    require(code == 0 and len(seen) == 1, (code, err))
    require("hai_glue_mapping" not in json.dumps(seen[0]), "the mapping is the script's setting and must not reach the workflow")
    nodes, cat = read["manifest"]["nodes"], read["catalog"]["nodes"]
    place = {key: (node["database"], node["schema"]) for key, node in nodes.items()}
    require(place["model.hai.dim_state"] == ("hai_lakehouse", "gold"), place)
    require(place["model.hai.feature_matrix_hospital"] == ("hai_lakehouse", "features"), place)
    require(place["model.hai.stg_measure"] == ("hai_lakehouse", "silver"), place)
    require(
        place["test.hai.not_null_dim_state"] == ("staging", "main") and place["model.dbt_project_evaluator.fct_check"] == ("staging", "main"),
        "tests and models of other folders or packages stay as dbt wrote them",
    )
    source = read["manifest"]["sources"]["source.hai.bronze.measure"]
    require(
        (source["database"], source["schema"], source["name"]) == ("hai_lakehouse", "bronze", "measure"),
        f"a bronze source is a table of bronze: {source}",
    )
    require(
        read["catalog"]["sources"]["source.hai.bronze.measure"]["metadata"]["schema"] == "bronze",
        "the catalog entry of a bronze source moves with it",
    )
    require((cat["model.hai.dim_state"]["metadata"]["database"], cat["model.hai.dim_state"]["metadata"]["schema"]) == ("hai_lakehouse", "gold"), cat)
    require(cat["model.hai.stg_measure"]["metadata"]["schema"] == "silver", "the catalog entry of a silver model moves with it")
    require(cat["model.dbt_project_evaluator.fct_check"]["metadata"]["schema"] == "main", "the catalog entry of a package model stays")


HOST_API = "http://10.0.1.20:8585/api"


def run_host_ingest(args: list[str], env: dict[str, str], workflow: Callable[[dict[str, Any]], None], **extra: Any) -> tuple[int, str, str]:
    from scripts.infrastructure import openmetadata_host_ingest as host_ingest

    with quiet() as (out, err):
        code = host_ingest.main(args, env=env, workflow=workflow, **extra)
    return code, out.getvalue(), err.getvalue()


SILVER_RUN, GOLD_RUN, FEATURES_RUN = "20261104T100000Z-5111e201", "20261104T110000Z-601d0001", "20261104T120000Z-feed0001"


def chain_keys(**change: Any) -> dict[str, bytes]:
    """The pin files that record one chain (features run to gold run to silver run) and one dbt copy per layer run, as the layer jobs leave them."""

    def pin(run: str, source: str, namespace: str) -> bytes:
        return json.dumps({"schema": 1, "run_id": run, "mode": "real", "namespace": namespace, "source_run_id": source, "snapshot_set": {"t": 1}}).encode()

    keys = {
        f"lakehouse/layer_runs/features/{FEATURES_RUN}/gold_pins.json": pin(FEATURES_RUN, GOLD_RUN, "gold"),
        f"lakehouse/layer_runs/gold/{GOLD_RUN}/silver_pins.json": pin(GOLD_RUN, SILVER_RUN, "silver"),
    }
    for layer, run in (("silver", SILVER_RUN), ("gold", GOLD_RUN), ("features", FEATURES_RUN)):
        keys[f"metadata/dbt/{layer}/{run}/manifest.json"] = json.dumps({"layer": layer}).encode()
    for key, value in change.items():
        if value is None:
            keys.pop(key, None)
        else:
            keys[key] = value
    return keys


def host_ingest_environment(root: Path, **changes: str | None) -> tuple[list[str], dict[str, str]]:
    """The command line and variables of the host ingestion container: no token file, the catalog address and the token as variables."""
    _, env = ingest_environment(root)
    env.update(OM_API=HOST_API, OM_INGESTION_TOKEN=BEARER_VALUE, FEATURES_RUN=FEATURES_RUN)
    for name, value in changes.items():
        if value is None:
            env.pop(name, None)
        else:
            env[name] = value
    return ["--config-dir", str(INGESTION), "--work-dir", str(root / "work"), "glue", "dbt"], env


BRONZE_RUN = "20261104T100000Z-abcdef01"
BRONZE_KEY = f"lakehouse/bronze_runs/{BRONZE_RUN}/check.json"


def bronze_record(files: dict[str, int], run: str = BRONZE_RUN, version: str = "v1") -> dict[str, Any]:
    """A bronze run record in the shape the bronze check phase writes, with ``files[table]`` loaded objects for each table."""
    tables = sorted(files)
    objects = [
        {
            "table": table,
            "bucket": "example-acquisition-bucket",
            "key": f"cms/{table}/file_{index:03d}.csv",
            "version_id": f"{version}-{index}",
            "sha256": f"{index:064x}",
            "byte_count": 10 + index,
        }
        for table in tables
        for index in range(files[table])
    ]
    return {
        "schema_version": 1,
        "run_id": run,
        "target": "real",
        "namespaces": ["bronze", "bronze_dictionary"],
        "tables": tables,
        "report_sha256": "d" * 64,
        "metadata_generation": "generation-0001",
        "snapshots": {table: 50 + index for index, table in enumerate(tables)},
        "objects": objects,
    }


class FakeCatalog:
    """A stand-in of OpenMetadata's table API: a lookup by name and a JSON patch of the description, with every call kept."""

    def __init__(self, descriptions: dict[str, str]) -> None:
        self.tables = {f"glue_mirror.hai_lakehouse.bronze.{name}": {"id": f"id-{name}", "description": text} for name, text in descriptions.items()}
        self.calls: list[tuple[str, str, dict[str, str], bytes | None]] = []
        self.fail_patch = False

    def __call__(self, method: str, url: str, headers: Mapping[str, str], body: bytes | None) -> tuple[int, Any]:
        self.calls.append((method, url, dict(headers), body))
        path = url.split("/api/v1/tables/", 1)[1]
        if method == "GET":
            name = path.split("?", 1)[0].removeprefix("name/")
            table = self.tables.get(name)
            return (200, dict(table)) if table else (404, None)
        if self.fail_patch:
            return 500, None
        table = next(item for item in self.tables.values() if item["id"] == path)
        operations = json.loads(body or b"[]")
        require(len(operations) == 1 and operations[0]["op"] == "add" and operations[0]["path"] == "/description", operations)
        table["description"] = operations[0]["value"]
        return 200, None

    def patches(self) -> int:
        return len([call for call in self.calls if call[0] == "PATCH"])


def raw_files_environment(**changes: str) -> dict[str, str]:
    return {"OM_API": HOST_API, "OM_INGESTION_TOKEN": BEARER_VALUE, "DATA_BUCKET_NAME": "example-project-data-dev", **changes}


def run_raw_files(args: list[str], env: dict[str, str], keys: dict[str, bytes], catalog: FakeCatalog) -> tuple[int, str, str]:
    from scripts.infrastructure import openmetadata_raw_files as writer

    with quiet() as (out, err):
        code = writer.main(args, env=env, s3_client=FakeS3(keys), http=catalog)
    return code, out.getvalue(), err.getvalue()


def raw_files_are_written_onto_each_bronze_table() -> None:
    """Each table the bronze run loaded gets its object keys, versions, SHA-256, sizes, run and snapshot in a marked block of its description, over the
    catalog API with the bot token, and nothing about the token reaches the output."""
    keys = {BRONZE_KEY: json.dumps(bronze_record({"cms_hai_hospital": 2, "cms_hai_state": 1})).encode()}
    catalog = FakeCatalog({"cms_hai_hospital": "", "cms_hai_state": ""})
    code, out, err = run_raw_files(["--bronze-run", BRONZE_RUN], raw_files_environment(), keys, catalog)
    require(code == 0 and "2 written" in out, (code, out, err))
    hospital = catalog.tables["glue_mirror.hai_lakehouse.bronze.cms_hai_hospital"]["description"]
    for needle in (BRONZE_RUN, "s3://example-acquisition-bucket/cms/cms_hai_hospital/file_000.csv", "version v1-1", f"sha256 {1:064x}", "bytes 11", "2 files"):
        require(needle in hospital, f"the description lacks {needle!r}: {hospital}")
    require(hospital.startswith("<!-- hai-raw-files:start -->") and hospital.endswith("<!-- hai-raw-files:end -->"), "the block is marked")
    require(all(call[2]["Authorization"] == f"Bearer {BEARER_VALUE}" for call in catalog.calls), "every call carries the bot token")
    require(any(call[0] == "PATCH" and call[2]["Content-Type"] == "application/json-patch+json" for call in catalog.calls), "a JSON patch")
    require(BEARER_VALUE not in out + err, "the token reached the output")


def unit_completion(root: str, unit: str, run: str, files: list[tuple[str, str, str, int]], retrieved: str = "2026-10-01T09:00:00Z") -> tuple[str, bytes]:
    """The key and bytes of one unit completion record as the acquisition writer stores them, naming ``files`` (key, version, SHA-256, size) as its data."""
    from scripts.acquisition import completion_record as records

    key = records.unit_key({"unit": unit})
    data = [
        {"key": name, "version_id": version, "sha256": digest, "byte_count": size, "role": "data", "stored_file_name": name.rsplit("/", 1)[1]}
        for name, version, digest, size in files
    ]
    receipt = {"key": f"cms/receipts/{unit}.json", "version_id": f"r-{unit}", "sha256": records.sha256_text(f"receipt {unit}".encode()), "byte_count": 100}
    manifest = {"key": f"cms/manifests/{unit}.json", "version_id": f"m-{unit}", "sha256": records.sha256_text(f"manifest {unit}".encode()), "byte_count": 50}
    record = records.build(
        key=key, source_id=f"source_{unit}", collection="cms/hospitals", mode="real", run_id=run, code_sha256="c" * 64, bundle_sha256="b" * 64,
        retrieved_at_utc=retrieved, receipt=receipt, manifests=[manifest], data_objects=data,
    )  # fmt: skip
    return records.record_key(root, key, record["payload_sha256"]), records.encode(record)


def object_of(table: str, index: int, version: str = "v1") -> tuple[str, str, str, int]:
    return f"cms/{table}/file_{index:03d}.csv", f"{version}-{index}", f"{index:064x}", 10 + index


ACQ_RUN_A, ACQ_RUN_B = "20261001T090000Z-aaaaaaa1", "20261002T090000Z-bbbbbbb2"


def raw_files_name_their_receipt_and_acquisition_run() -> None:
    """Each file of a bronze table is matched, by object key and exact version, to the unit completion record that names it, and the table's text carries
    that record's acquisition run, unit, source, collection, retrieval time and receipt (key, version, SHA-256). Only the deployed capture root raw/ is
    read (infra/jobs/publisher.tf capture_root), the records are paged, and a repeat of the run writes nothing."""
    from scripts.acquisition import completion_record as records

    key_a, body_a = unit_completion("raw/", "alpha", ACQ_RUN_A, [object_of("cms_hai_hospital", 0), object_of("cms_hai_hospital", 1)])
    key_b, body_b = unit_completion("raw/", "beta", ACQ_RUN_B, [object_of("cms_hai_state", 0)], "2026-10-02T09:00:00Z")
    from scripts.acquisition import raw_root

    require(
        key_b.startswith(raw_root.RAW_ROOT) and 'capture_root          = "raw/"' in (REPO / "infra/jobs/publisher.tf").read_text(),
        "raw/ is the deployed capture root",
    )
    keys = {BRONZE_KEY: json.dumps(bronze_record({"cms_hai_hospital": 2, "cms_hai_state": 1})).encode(), key_a: body_a, key_b: body_b}
    catalog = FakeCatalog({"cms_hai_hospital": "", "cms_hai_state": ""})
    s3 = FakeS3(keys, page=1)
    from scripts.infrastructure import openmetadata_raw_files as writer

    with quiet() as (out, err):
        code = writer.main(["--bronze-run", BRONZE_RUN], env=raw_files_environment(), s3_client=s3, http=catalog)
    require(code == 0, (code, err.getvalue()))
    hospital = catalog.tables["glue_mirror.hai_lakehouse.bronze.cms_hai_hospital"]["description"]
    unit_a = records.unit_key({"unit": "alpha"})
    for needle in (
        f"acquisition run {ACQ_RUN_A}",
        f"unit {unit_a}",
        "source source_alpha",
        "collection cms/hospitals",
        "retrieved 2026-10-01T09:00:00Z",
        "receipt s3://example-project-data-dev/cms/receipts/alpha.json version r-alpha",
        f"sha256 {records.sha256_text(b'receipt alpha')}",
    ):
        require(needle in hospital, f"the description lacks {needle!r}: {hospital}")
    require("2 of 2 files have an acquisition record" in hospital, hospital)
    state = catalog.tables["glue_mirror.hai_lakehouse.bronze.cms_hai_state"]["description"]
    require(f"acquisition run {ACQ_RUN_B}" in state and f"unit {records.unit_key({'unit': 'beta'})}" in state, state)
    require(s3.listings and all(item.endswith("/raw/ingestion_units/") for item in s3.listings), f"only the capture root raw/ is listed: {s3.listings}")
    again = FakeS3(keys, page=1)
    with quiet() as (out, err):
        require(writer.main(["--bronze-run", BRONZE_RUN], env=raw_files_environment(), s3_client=again, http=catalog) == 0, err.getvalue())
    require(catalog.patches() == 2 and "2 unchanged" in out.getvalue(), f"a repeat wrote again: {catalog.patches()} patches, {out.getvalue()}")
    require(BEARER_VALUE not in out.getvalue() + err.getvalue(), "the token reached the output")


def raw_files_without_a_completion_record_say_so() -> None:
    """A file that no completion record names with that exact version is listed with the plain statement that none does, and the header counts the files that
    have a record. The write still happens: a table is not left without its files because one of them predates the records."""
    key_a, body_a = unit_completion("raw/", "alpha", ACQ_RUN_A, [object_of("cms_hai_hospital", 0)])
    other_version = unit_completion("raw/", "gamma", ACQ_RUN_B, [object_of("cms_hai_hospital", 1, version="old")])
    keys = {BRONZE_KEY: json.dumps(bronze_record({"cms_hai_hospital": 2})).encode(), key_a: body_a, other_version[0]: other_version[1]}
    catalog = FakeCatalog({"cms_hai_hospital": ""})
    code, out, err = run_raw_files(["--bronze-run", BRONZE_RUN], raw_files_environment(), keys, catalog)
    text = catalog.tables["glue_mirror.hai_lakehouse.bronze.cms_hai_hospital"]["description"]
    require(code == 0 and "1 of 2 files have an acquisition record" in text, (code, err, text))
    require(text.count("no completion record names this file version") == 1 and f"acquisition run {ACQ_RUN_B}" not in text, text)


def acquisition_records_that_are_wrong_stop_the_write() -> None:
    """A completion record that is renamed, edited, unreadable or denied stops the write by its key, a listing past the bound fails, nothing is written to the
    catalog, and no secret reaches the output."""
    from scripts.infrastructure import openmetadata_raw_files as writer

    key_a, body_a = unit_completion("raw/", "alpha", ACQ_RUN_A, [object_of("cms_hai_hospital", 0)])
    record = json.loads(body_a)
    record["run_id"] = ""
    cases: dict[str, tuple[dict[str, bytes], set[str], str]] = {
        "copied under another key": (
            {key_a.replace("ingestion_units/", "ingestion_units/" + "0" * 64 + "/"): body_a},
            set(),
            "differs from the unit and payload",
        ),
        "edited": ({key_a: json.dumps(record).encode()}, set(), "empty or non-text"),
        "not JSON": ({key_a: b"[nope"}, set(), "not JSON"),
        "denied": ({key_a: body_a}, {key_a}, "AccessDenied"),
    }
    for label, (records, deny, phrase) in cases.items():
        keys = {BRONZE_KEY: json.dumps(bronze_record({"cms_hai_hospital": 1})).encode()} | records
        catalog = FakeCatalog({"cms_hai_hospital": ""})
        with quiet() as (_, err):
            code = writer.main(["--bronze-run", BRONZE_RUN], env=raw_files_environment(), s3_client=FakeS3(keys, deny=deny), http=catalog)
        stored = err.getvalue()
        require(code == 1 and catalog.patches() == 0 and phrase in stored and BEARER_VALUE not in stored, f"{label}: code {code}, {stored}")
        require("ingestion_units/" in stored, f"{label}: the refusal names the record key: {stored}")
    keys = {BRONZE_KEY: json.dumps(bronze_record({"cms_hai_hospital": 1})).encode()}
    for index in range(3):
        keys.update([unit_completion("raw/", f"unit{index}", ACQ_RUN_A, [object_of("cms_hai_hospital", 5 + index)])])
    catalog = FakeCatalog({"cms_hai_hospital": ""})
    with quiet() as (_, err), mock.patch.object(writer, "MAX_RECORDS", 2):
        code = writer.main(["--bronze-run", BRONZE_RUN], env=raw_files_environment(), s3_client=FakeS3(keys, page=2), http=catalog)
    require(code == 1 and catalog.patches() == 0 and "more than 2 completion records" in err.getvalue(), (code, err.getvalue()))


def an_object_named_by_two_records_takes_the_earliest_capture() -> None:
    """Two completion records can name one stored object version (a later capture found it unchanged). The acquisition run is the one that stored it: the
    record with the earliest retrieval time, so the text does not change when a later record is added."""
    first = unit_completion("raw/", "alpha", ACQ_RUN_A, [object_of("cms_hai_hospital", 0)], "2026-10-01T09:00:00Z")
    later = unit_completion("raw/", "delta", ACQ_RUN_B, [object_of("cms_hai_hospital", 0)], "2026-10-05T09:00:00Z")
    base = {BRONZE_KEY: json.dumps(bronze_record({"cms_hai_hospital": 1})).encode(), first[0]: first[1]}
    texts = []
    for extra in ({}, {later[0]: later[1]}):
        catalog = FakeCatalog({"cms_hai_hospital": ""})
        run_raw_files(["--bronze-run", BRONZE_RUN], raw_files_environment(), base | extra, catalog)
        texts.append(catalog.tables["glue_mirror.hai_lakehouse.bronze.cms_hai_hospital"]["description"])
    require(texts[0] == texts[1] and f"acquisition run {ACQ_RUN_A}" in texts[0] and ACQ_RUN_B not in texts[0], texts)


def a_bucket_root_completion_record_is_not_read() -> None:
    """The deployed capture root is raw/, so the writer lists and reads ``raw/ingestion_units/`` only. A record at the bucket root, valid or not, is never
    listed or read: it neither attributes a file nor stops the write, and a role that grants no bucket-root read is enough."""
    from scripts.infrastructure import openmetadata_raw_files as writer

    require(writer.CAPTURE_ROOTS == ("raw/",), f"the writer's roots: {writer.CAPTURE_ROOTS}")
    legacy = unit_completion("", "alpha", ACQ_RUN_A, [object_of("cms_hai_hospital", 0)])
    broken = ("ingestion_units/" + "0" * 64 + "/" + "1" * 64 + ".json", b"[not a record")
    current = unit_completion("raw/", "delta", ACQ_RUN_B, [object_of("cms_hai_hospital", 1)])
    keys = {BRONZE_KEY: json.dumps(bronze_record({"cms_hai_hospital": 2})).encode(), legacy[0]: legacy[1], broken[0]: broken[1], current[0]: current[1]}
    catalog = FakeCatalog({"cms_hai_hospital": ""})
    s3 = FakeS3(keys)
    with quiet() as (_, err):
        code = writer.main(["--bronze-run", BRONZE_RUN], env=raw_files_environment(), s3_client=s3, http=catalog)
    text = catalog.tables["glue_mirror.hai_lakehouse.bronze.cms_hai_hospital"]["description"]
    require(code == 0, (code, err.getvalue()))
    require(
        not [item for item in s3.requested if "/ingestion_units/" in item and "/raw/ingestion_units/" not in item],
        f"a bucket-root record was read: {s3.requested}",
    )
    require(all(item.endswith("/raw/ingestion_units/") for item in s3.listings), f"a bucket-root listing: {s3.listings}")
    require(f"acquisition run {ACQ_RUN_B}" in text and f"acquisition run {ACQ_RUN_A}" not in text, text)
    require("1 of 2 files have an acquisition record" in text and text.count("no completion record names this file version") == 1, text)


def description_after(files: dict[str, bytes]) -> str:
    catalog = FakeCatalog({"cms_hai_hospital": ""})
    code, _, err = run_raw_files(["--bronze-run", BRONZE_RUN], raw_files_environment(), files, catalog)
    require(code == 0, (code, err))
    return catalog.tables["glue_mirror.hai_lakehouse.bronze.cms_hai_hospital"]["description"]


def sub_second_capture_times_are_compared_as_instants() -> None:
    """Retrieval times are compared as instants, not as text: ``...09:00:00.100000Z`` is 0.1 s after ``...09:00:00Z`` although its text sorts before it
    (a point sorts before ``Z``). Two records name one object version, and the earlier capture's run and receipt are the ones written."""
    first = unit_completion("raw/", "alpha", ACQ_RUN_A, [object_of("cms_hai_hospital", 0)], "2026-10-01T09:00:00Z")
    later = unit_completion("raw/", "delta", ACQ_RUN_B, [object_of("cms_hai_hospital", 0)], "2026-10-01T09:00:00.100000Z")
    text = description_after({BRONZE_KEY: json.dumps(bronze_record({"cms_hai_hospital": 1})).encode(), first[0]: first[1], later[0]: later[1]})
    require(f"acquisition run {ACQ_RUN_A}" in text and "cms/receipts/alpha.json" in text, text)
    require(ACQ_RUN_B not in text and "cms/receipts/delta.json" not in text, f"the later capture was chosen: {text}")


def equal_capture_instants_choose_by_unit_then_payload() -> None:
    """Two records with the same instant, written differently (``Z``, ``.000000Z``, ``+00:00``), name one object version. The choice does not depend on how
    the times are written or on the order the records are listed: the smaller unit key wins, and for one unit the smaller payload digest."""
    from scripts.acquisition import completion_record as records

    forms = ("2026-10-01T09:00:00Z", "2026-10-01T09:00:00.000000Z", "2026-10-01T09:00:00+00:00")
    units = {"alpha": ACQ_RUN_A, "delta": ACQ_RUN_B}
    wanted = min(units, key=lambda unit: records.unit_key({"unit": unit}))
    for first, second in ((forms[0], forms[1]), (forms[1], forms[2]), (forms[2], forms[0])):
        a = unit_completion("raw/", "alpha", ACQ_RUN_A, [object_of("cms_hai_hospital", 0)], first)
        d = unit_completion("raw/", "delta", ACQ_RUN_B, [object_of("cms_hai_hospital", 0)], second)
        texts = set()
        for order in ((a, d), (d, a)):
            files = {BRONZE_KEY: json.dumps(bronze_record({"cms_hai_hospital": 1})).encode()} | {key: body for key, body in order}
            texts.add(description_after(files))
        require(len(texts) == 1, f"the choice changed with the listing order for {first} and {second}")
        text = texts.pop()
        require(
            f"acquisition run {units[wanted]}" in text and f"cms/receipts/{wanted}.json" in text,
            f"the smaller unit key, {wanted}, was not chosen for {first} and {second}: {text}",
        )


def a_completion_record_without_a_usable_time_stops_the_write() -> None:
    """A retrieval time that is not an ISO 8601 date-time with an offset (text, or no offset) cannot be ordered, so it stops the write by the record's key
    and nothing is written. An offset other than UTC is an instant like any other."""
    for label, stamp in (("text", "yesterday"), ("no offset", "2026-10-01T09:00:00"), ("too long", "2026-10-01T09:00:00Z" + "0" * 40)):
        record = unit_completion("raw/", "alpha", ACQ_RUN_A, [object_of("cms_hai_hospital", 0)], stamp)
        catalog = FakeCatalog({"cms_hai_hospital": ""})
        files = {BRONZE_KEY: json.dumps(bronze_record({"cms_hai_hospital": 1})).encode(), record[0]: record[1]}
        code, _, err = run_raw_files(["--bronze-run", BRONZE_RUN], raw_files_environment(), files, catalog)
        require(code == 1 and catalog.patches() == 0 and record[0] in err and "not a date-time with an offset" in err, f"{label}: code {code}, {err}")
    shifted = unit_completion("raw/", "alpha", ACQ_RUN_A, [object_of("cms_hai_hospital", 0)], "2026-10-01T10:30:00+01:00")  # 09:30 UTC
    same = unit_completion("raw/", "delta", ACQ_RUN_B, [object_of("cms_hai_hospital", 0)], "2026-10-01T09:45:00Z")
    text = description_after({BRONZE_KEY: json.dumps(bronze_record({"cms_hai_hospital": 1})).encode(), shifted[0]: shifted[1], same[0]: same[1]})
    require(f"acquisition run {ACQ_RUN_A}" in text, f"09:30 UTC is earlier than 09:45 UTC: {text}")


def raw_files_write_is_idempotent_and_keeps_other_text() -> None:
    """A repeat of the same run writes nothing. Text around the block stays. A later run replaces the block only, so one block is left."""
    keys = {BRONZE_KEY: json.dumps(bronze_record({"cms_hai_hospital": 2})).encode()}
    catalog = FakeCatalog({"cms_hai_hospital": "Hospital measures from CMS."})
    require(run_raw_files(["--bronze-run", BRONZE_RUN], raw_files_environment(), keys, catalog)[0] == 0 and catalog.patches() == 1, "the first run writes")
    first = catalog.tables["glue_mirror.hai_lakehouse.bronze.cms_hai_hospital"]["description"]
    require(first.startswith("Hospital measures from CMS.\n\n<!-- hai-raw-files:start -->"), f"the existing text is kept: {first[:80]}")
    code, out, _ = run_raw_files(["--bronze-run", BRONZE_RUN], raw_files_environment(), keys, catalog)
    require(code == 0 and catalog.patches() == 1 and "1 unchanged" in out, f"a repeat wrote again: {catalog.patches()} patches, {out}")
    later = "20261105T100000Z-abcdef02"
    keys[f"lakehouse/bronze_runs/{later}/check.json"] = json.dumps(bronze_record({"cms_hai_hospital": 3}, later, "w")).encode()
    require(run_raw_files(["--bronze-run", later], raw_files_environment(), keys, catalog)[0] == 0 and catalog.patches() == 2, "a later run writes once")
    second = catalog.tables["glue_mirror.hai_lakehouse.bronze.cms_hai_hospital"]["description"]
    require(
        second.count("hai-raw-files:start") == 1 and later in second and BRONZE_RUN not in second and second.startswith("Hospital measures from CMS."), second
    )


def raw_files_list_is_capped() -> None:
    """A table with more files than the cap lists the first ones in key order and says how many more, and names the run record."""
    keys = {BRONZE_KEY: json.dumps(bronze_record({"cms_hai_hospital": 60})).encode()}
    catalog = FakeCatalog({"cms_hai_hospital": ""})
    require(run_raw_files(["--bronze-run", BRONZE_RUN], raw_files_environment(), keys, catalog)[0] == 0, "written")
    text = catalog.tables["glue_mirror.hai_lakehouse.bronze.cms_hai_hospital"]["description"]
    require(text.count("- s3://") == 50 and "and 10 more files" in text and BRONZE_KEY in text and "60 files" in text, text[-300:])


def raw_files_writer_refuses_and_fails_visibly() -> None:
    """An unusable catalog address, a missing token or a run that is not a run ID is refused before any call. A missing or wrong record, a table absent from
    the catalog, a refused update and an unreachable catalog each end with exit code 1 that names the cause and never the token."""
    from scripts.infrastructure import openmetadata_raw_files as writer

    good = {BRONZE_KEY: json.dumps(bronze_record({"cms_hai_hospital": 1})).encode()}
    for label, changes, args in (
        ("address", {"OM_API": "http://127.0.0.1:8585/api"}, ["--bronze-run", BRONZE_RUN]),
        ("token", {"OM_INGESTION_TOKEN": ""}, ["--bronze-run", BRONZE_RUN]),
        ("latest", {}, ["--bronze-run", "latest"]),
    ):
        catalog = FakeCatalog({"cms_hai_hospital": ""})
        code, _, err = run_raw_files(args, raw_files_environment(**changes), good, catalog)
        require(code == 2 and not catalog.calls and BEARER_VALUE not in err, f"{label}: code {code}, {err}")
    catalog = FakeCatalog({"cms_hai_hospital": ""})
    code, _, err = run_raw_files(["--bronze-run", BRONZE_RUN], raw_files_environment(), {}, catalog)
    require(code == 1 and BRONZE_KEY in err and "missing" in err and not catalog.calls, (code, err))
    wrong = {BRONZE_KEY: json.dumps(bronze_record({"cms_hai_hospital": 1}, run="20261104T100000Z-ffffffff")).encode()}
    code, _, err = run_raw_files(["--bronze-run", BRONZE_RUN], raw_files_environment(), wrong, FakeCatalog({}))
    require(code == 1 and BRONZE_KEY in err and "another run" in err, (code, err))
    two = {BRONZE_KEY: json.dumps(bronze_record({"cms_hai_hospital": 1, "cms_hai_state": 1})).encode()}
    catalog = FakeCatalog({"cms_hai_hospital": ""})
    code, out, err = run_raw_files(["--bronze-run", BRONZE_RUN], raw_files_environment(), two, catalog)
    require(code == 1 and "1 written" in out and "cms_hai_state" in err, f"an absent table is named and the others are written: {code}, {out}, {err}")
    broken = FakeCatalog({"cms_hai_hospital": ""})
    broken.fail_patch = True
    code, _, err = run_raw_files(["--bronze-run", BRONZE_RUN], raw_files_environment(), good, broken)
    require(code == 1 and "HTTP 500" in err and BEARER_VALUE not in err, (code, err))

    def unreachable(method: str, url: str, headers: Mapping[str, str], body: bytes | None) -> tuple[int, Any]:
        raise writer.WriteFailed("the catalog did not answer: ConnectionRefusedError")

    with quiet() as (_, stderr):
        code = writer.main(["--bronze-run", BRONZE_RUN], env=raw_files_environment(), s3_client=FakeS3(good), http=unreachable)
    require(code == 1 and "ConnectionRefusedError" in stderr.getvalue() and BEARER_VALUE not in stderr.getvalue(), (code, stderr.getvalue()))


def host_ingest_aims_both_sources_at_the_persistent_catalog() -> None:
    seen: list[dict[str, Any]] = []
    with tempfile.TemporaryDirectory() as folder:
        args, env = host_ingest_environment(Path(folder))
        code, out, err = run_host_ingest(args, env, seen.append, s3_client=FakeS3(chain_keys()))
        require(code == 0, (code, err))
        require(len(seen) == 4, f"glue once and dbt once per layer of the chain, got {len(seen)}")
        for config in seen:
            server = config["workflowConfig"]["openMetadataServerConfig"]
            require(server["hostPort"] == HOST_API, f"the workflow does not name the persistent catalog: {server['hostPort']}")
            require(server["securityConfig"]["jwtToken"] == BEARER_VALUE, "the workflow does not carry the ingestion token")
            require("127.0.0.1" not in json.dumps(config) and "${" not in json.dumps(config), "a loopback address or placeholder is left")
        require(seen[0]["source"]["type"] == "glue", "the Glue source must run first")
        require(BEARER_VALUE not in out + err, "a secret reached the output")
        written = [path for path in (Path(folder) / "work").rglob("*") if path.is_file()]
        require(written, "the dbt artifacts must be in the work folder, or this check proves nothing")
        for path in written:
            require(BEARER_VALUE.encode() not in path.read_bytes(), f"the token was written to disk: {path.name}")


def chain_follows_the_recorded_pins() -> None:
    """From the features run, the gold run is the one its gold pins name and the silver run the one the gold run's silver pins name; each pin file must be
    of the run it was read from. The bronze run is not recorded by silver's pins, so none is returned."""
    from scripts.infrastructure import openmetadata_chain as chain

    s3 = FakeS3(chain_keys())
    found = chain.layer_runs(s3, "example-project-data-dev", FEATURES_RUN)
    require(found == {"silver": SILVER_RUN, "gold": GOLD_RUN, "features": FEATURES_RUN}, found)
    require(sorted(s3.requested) == sorted(f"example-project-data-dev/{key}" for key in chain_keys() if key.endswith("_pins.json")), s3.requested)


def chain_refuses_a_missing_or_mismatched_pin() -> None:
    """A pin file that is missing, unreadable, of another run, of another namespace or mode, or that names no run stops the chain by its key. Nothing falls back
    to a latest run."""
    from scripts.infrastructure import openmetadata_chain as chain

    gold_pin = f"lakehouse/layer_runs/features/{FEATURES_RUN}/gold_pins.json"
    silver_pin = f"lakehouse/layer_runs/gold/{GOLD_RUN}/silver_pins.json"
    good = json.loads(chain_keys()[gold_pin])
    cases: dict[str, tuple[dict[str, bytes | None], str, str]] = {
        "missing": ({gold_pin: None}, gold_pin, "missing"),
        "silver pin missing": ({silver_pin: None}, silver_pin, "missing"),
        "another run": ({gold_pin: json.dumps({**good, "run_id": GOLD_RUN}).encode()}, gold_pin, "another run"),
        "another namespace": ({gold_pin: json.dumps({**good, "namespace": "silver"}).encode()}, gold_pin, "namespace"),
        "not real mode": ({gold_pin: json.dumps({**good, "mode": "publish_fixture"}).encode()}, gold_pin, "mode"),
        "latest": ({gold_pin: json.dumps({**good, "source_run_id": "latest"}).encode()}, gold_pin, "not a run"),
        "no source": ({gold_pin: json.dumps({key: value for key, value in good.items() if key != "source_run_id"}).encode()}, gold_pin, "keys"),
        "not json": ({gold_pin: b"[nope"}, gold_pin, "not JSON"),
    }
    for label, (change, key, phrase) in cases.items():
        try:
            chain.layer_runs(FakeS3(chain_keys(**change)), "example-project-data-dev", FEATURES_RUN)
        except chain.ChainError as error:
            require(key in str(error) and phrase in str(error), f"{label}: the refusal must name {key} and '{phrase}': {error}")
        else:
            raise AssertionError(f"{label}: a bad pin was accepted")
    for run in ("latest", "", "20261104T120000Z-FEED0001", f"{FEATURES_RUN}/x"):
        try:
            chain.layer_runs(FakeS3(chain_keys()), "example-project-data-dev", run)
        except chain.ChainError as error:
            require("not a run" in str(error), (run, str(error)))
        else:
            raise AssertionError(f"the features run {run!r} was accepted")


def silver_claim(input_run: str, run: str = SILVER_RUN, **change: Any) -> bytes:
    """The launch claim of the silver job as the launch library writes it, bound to the bronze run it pinned."""
    claim = {
        "event": "claim",
        "job": "silver",
        "run_id": run,
        "attempt": 1,
        "schema_version": 1,
        "bound": {"code_digest": "a" * 64, "request_hash": "b" * 64, "input_run": input_run},
    } | change
    return json.dumps(claim).encode()


SILVER_CLAIM_KEY = f"lakehouse/launch_records/{SILVER_RUN}/silver/claim.json"


def chain_names_the_bronze_run_of_silvers_claim() -> None:
    """The bronze run of a chain is the input run that the silver job's launch claim bound; ``latest`` names none, and a claim that is missing, of another run
    or job, or bound to something that is not a run ID stops the chain by its key."""
    from scripts.infrastructure import openmetadata_chain as chain

    bucket = "example-project-data-dev"
    require(chain.bronze_run(FakeS3(chain_keys() | {SILVER_CLAIM_KEY: silver_claim(BRONZE_RUN)}), bucket, SILVER_RUN) == BRONZE_RUN, "the bound bronze run")
    require(chain.bronze_run(FakeS3(chain_keys() | {SILVER_CLAIM_KEY: silver_claim("latest")}), bucket, SILVER_RUN) is None, "latest names no bronze run")
    cases = {
        "missing": ({}, "missing"),
        "another run": ({SILVER_CLAIM_KEY: silver_claim(BRONZE_RUN, run=GOLD_RUN)}, "another run"),
        "another job": ({SILVER_CLAIM_KEY: silver_claim(BRONZE_RUN, job="gold")}, "another job"),
        "not a run": ({SILVER_CLAIM_KEY: silver_claim("yesterday")}, "not a run"),
        "not JSON": ({SILVER_CLAIM_KEY: b"[nope"}, "not JSON"),
    }
    for label, (change, phrase) in cases.items():
        try:
            chain.bronze_run(FakeS3(chain_keys() | change), bucket, SILVER_RUN)
        except chain.ChainError as error:
            require(SILVER_CLAIM_KEY in str(error) and phrase in str(error), f"{label}: the refusal must name the claim and '{phrase}': {error}")
        else:
            raise AssertionError(f"{label}: a bad claim was accepted")


def host_ingest_writes_the_bronze_files_after_the_dbt_layers() -> None:
    """With the raw_files source the host ingestion also writes the raw files of the bronze run that silver's claim names, after the Glue and dbt sources,
    over the catalog API with the token."""
    seen: list[str] = []
    catalog = FakeCatalog({"cms_hai_hospital": ""})

    def workflow(config: dict[str, Any]) -> None:
        seen.append(config["source"]["type"])

    keys = chain_keys() | {SILVER_CLAIM_KEY: silver_claim(BRONZE_RUN), BRONZE_KEY: json.dumps(bronze_record({"cms_hai_hospital": 2})).encode()}
    with tempfile.TemporaryDirectory() as folder:
        args, env = host_ingest_environment(Path(folder))
        env["DATA_BUCKET_NAME"] = "example-project-data-dev"
        from scripts.infrastructure import openmetadata_host_ingest as host_ingest

        with quiet() as (out, err):
            code = host_ingest.main([*args, "raw_files"], env=env, workflow=workflow, s3_client=FakeS3(keys), http=catalog)
    require(code == 0, (code, err.getvalue()))
    require(seen == ["glue", "dbt", "dbt", "dbt"], f"the catalog sources ran first: {seen}")
    description = catalog.tables["glue_mirror.hai_lakehouse.bronze.cms_hai_hospital"]["description"]
    require(BRONZE_RUN in description and "2 files" in description and catalog.patches() == 1, description[:200])
    require(BEARER_VALUE not in out.getvalue() + err.getvalue(), "the token reached the output")


def host_ingest_skips_the_bronze_files_when_silver_ran_on_latest() -> None:
    """A silver claim bound to ``latest`` names no bronze run: the files are not written, the task says so on standard error, and it does not fail. A claim
    that is missing or wrong stops the task before any workflow or catalog call."""
    keys = chain_keys() | {SILVER_CLAIM_KEY: silver_claim("latest")}
    seen: list[dict[str, Any]] = []
    catalog = FakeCatalog({"cms_hai_hospital": ""})
    from scripts.infrastructure import openmetadata_host_ingest as host_ingest

    with tempfile.TemporaryDirectory() as folder:
        args, env = host_ingest_environment(Path(folder))
        with quiet() as (_, err):
            code = host_ingest.main([*args, "raw_files"], env=env, workflow=seen.append, s3_client=FakeS3(keys), http=catalog)
    require(code == 0 and not catalog.calls and "no bronze run" in err.getvalue(), (code, err.getvalue(), catalog.calls))
    broken = FakeS3(chain_keys())
    seen.clear()
    with tempfile.TemporaryDirectory() as folder:
        args, env = host_ingest_environment(Path(folder))
        with quiet() as (_, err):
            code = host_ingest.main([*args, "raw_files"], env=env, workflow=seen.append, s3_client=broken, http=catalog)
    require(code == 1 and not seen and not catalog.calls and SILVER_CLAIM_KEY in err.getvalue(), (code, len(seen), err.getvalue()))


def host_ingest_ingests_the_three_recorded_layers() -> None:
    """The dbt source runs once for each layer of the chain that FEATURES_RUN records, on that layer run's copy and no other, and reads no fixed key."""
    seen: list[dict[str, Any]] = []
    read: list[str] = []

    def workflow(config: dict[str, Any]) -> None:
        seen.append(config)
        source = config["source"]["sourceConfig"]["config"].get("dbtConfigSource")
        if source is not None:
            read.append(json.loads(Path(source["dbtManifestFilePath"]).read_text())["layer"])

    s3 = FakeS3(chain_keys() | {"metadata/dbt/manifest.json": b'{"layer": "fixed_gold"}'})
    with tempfile.TemporaryDirectory() as folder:
        args, env = host_ingest_environment(Path(folder))
        code, _, err = run_host_ingest(args, env, workflow, s3_client=s3)
    require(code == 0, (code, err))
    require(read == ["silver", "gold", "features"], f"dbt ingested {read}")
    require(not [key for key in s3.requested if key.endswith("metadata/dbt/manifest.json")], f"a fixed key was read: {s3.requested}")


def host_ingest_fails_when_a_chain_run_names_the_old_mirror_schemas() -> None:
    """A chain run saved before the mirror rename fails the dbt step like a missing manifest does: exit 1, no dbt workflow, the earlier Glue step kept."""
    stale = json.dumps(
        {
            "nodes": {
                "model.hai.int_x": {
                    "resource_type": "model",
                    "schema": "hai_mirror_silver",
                    "compiled_code": 'select * from "hai_lakehouse"."hai_mirror_silver"."t"',
                }
            }
        }
    ).encode()
    seen: list[dict[str, Any]] = []
    s3 = FakeS3(chain_keys(**{f"metadata/dbt/silver/{SILVER_RUN}/manifest.json": stale}))
    with tempfile.TemporaryDirectory() as folder:
        args, env = host_ingest_environment(Path(folder))
        code, _, err = run_host_ingest(args, env, seen.append, s3_client=s3)
    types = [config["source"]["sourceConfig"]["config"]["type"] for config in seen]
    require(code == 1 and types == ["DatabaseMetadata"], f"glue ran, dbt did not: code {code}, workflows {types}")
    require(f"silver/{SILVER_RUN}" in err and "model.hai.int_x" in err and "hai_mirror_silver" in err, err)


def host_ingest_never_falls_back_to_latest() -> None:
    """Without FEATURES_RUN, with one that is not a run ID, or with a chain that does not hold, the dbt source stops before any workflow runs."""
    for label, changes, expected in (
        ("unset", {"FEATURES_RUN": None}, 2),
        ("empty", {"FEATURES_RUN": ""}, 2),
        ("latest", {"FEATURES_RUN": "latest"}, 2),
    ):
        seen: list[dict[str, Any]] = []
        s3 = FakeS3(chain_keys() | {"metadata/dbt/manifest.json": b"{}"})
        with tempfile.TemporaryDirectory() as folder:
            args, env = host_ingest_environment(Path(folder), **changes)
            code, _, err = run_host_ingest([*args[:-2], "dbt"], env, seen.append, s3_client=s3)
        require(code == expected and not seen and "FEATURES_RUN" in err, f"{label}: code {code}, {len(seen)} workflows, {err}")
        require(not s3.requested, f"{label}: S3 was read: {s3.requested}")
    broken = FakeS3(chain_keys(**{f"lakehouse/layer_runs/gold/{GOLD_RUN}/silver_pins.json": None}))
    seen = []
    with tempfile.TemporaryDirectory() as folder:
        args, env = host_ingest_environment(Path(folder))
        code, _, err = run_host_ingest([*args[:-2], "dbt"], env, seen.append, s3_client=broken)
    require(code == 1 and not seen and "silver_pins.json" in err, f"a missing pin stops the task: code {code}, {err}")
    require(not [key for key in broken.requested if key.startswith("example-project-data-dev/metadata/dbt/")], "no dbt copy is read before the chain holds")


def host_ingest_refuses_an_unusable_catalog_address() -> None:
    refused = {
        "loopback": "http://127.0.0.1:8585/api",
        "missing": None,
        "no api path": "http://10.0.1.20:8585",
        "a name": "http://catalog.example.invalid:8585/api",
        "tls": "https://10.0.1.20:8585/api",
        "public": "http://203.0.113.20:8585/api",
        "link local": "http://169.254.169.254:8585/api",
        "octet over 255": "http://10.0.1.256:8585/api",
        "first octet over 255": "http://999.0.1.20:8585/api",
        "leading zero": "http://10.0.01.20:8585/api",
        "another port": "http://10.0.1.20:8586/api",
        "port over 65535": "http://10.0.1.20:99999/api",
        "no port": "http://10.0.1.20/api",
        "path with a slash": "http://10.0.1.20:8585/api/",
        "another path": "http://10.0.1.20:8585/apix",
        "query": "http://10.0.1.20:8585/api?x=1",
        "user information": "http://10.0.1.20:8585@203.0.113.20:8585/api",
        "below 172.16": "http://172.15.255.255:8585/api",
        "above 172.31": "http://172.32.0.1:8585/api",
        "below 10": "http://9.255.255.255:8585/api",
        "above 10": "http://11.0.0.1:8585/api",
        "below 192.168": "http://192.167.255.255:8585/api",
        "above 192.168": "http://192.169.0.1:8585/api",
        "space": " http://10.0.1.20:8585/api",
    }
    for label, address in refused.items():
        seen: list[dict[str, Any]] = []
        with tempfile.TemporaryDirectory() as folder:
            args, env = host_ingest_environment(Path(folder), OM_API=address)
            code, out, err = run_host_ingest(args, env, seen.append)
        require(code == 2 and seen == [], f"{label}: code {code}, workflows {len(seen)}")
        require("OM_API" in err and BEARER_VALUE not in out + err, f"{label}: {err}")


def host_ingest_accepts_each_private_range_on_the_catalog_port() -> None:
    for address in (
        "http://10.0.1.20:8585/api",
        "http://10.0.0.0:8585/api",
        "http://10.255.255.255:8585/api",
        "http://172.16.0.0:8585/api",
        "http://172.31.255.255:8585/api",
        "http://192.168.0.1:8585/api",
        "http://192.168.255.255:8585/api",
    ):
        seen: list[dict[str, Any]] = []
        with tempfile.TemporaryDirectory() as folder:
            args, env = host_ingest_environment(Path(folder), OM_API=address)
            args = [item for item in args if item != "dbt"]
            code, _, err = run_host_ingest(args, env, seen.append)
        require(code == 0 and len(seen) == 1, f"{address}: code {code}, workflows {len(seen)}, {err}")
        require(seen[0]["workflowConfig"]["openMetadataServerConfig"]["hostPort"] == address, f"{address}: the workflow was aimed elsewhere")


def host_ingest_refuses_a_missing_token() -> None:
    for label, token in {"missing": None, "empty": ""}.items():
        seen: list[dict[str, Any]] = []
        with tempfile.TemporaryDirectory() as folder:
            args, env = host_ingest_environment(Path(folder), OM_INGESTION_TOKEN=token)
            code, _, err = run_host_ingest(args, env, seen.append)
        require(code == 2 and seen == [], f"{label}: code {code}, workflows {len(seen)}")
        require("OM_INGESTION_TOKEN" in err, f"{label}: {err}")


def host_ingest_stops_at_the_first_failing_source() -> None:
    seen: list[dict[str, Any]] = []

    def broken(config: dict[str, Any]) -> None:
        seen.append(config)
        raise RuntimeError(f"upstream said {config['workflowConfig']}")

    with tempfile.TemporaryDirectory() as folder:
        args, env = host_ingest_environment(Path(folder))
        code, out, err = run_host_ingest(args, env, broken, s3_client=FakeS3(chain_keys()))
    require(code == 1 and len(seen) == 1, (code, len(seen)))
    require("RuntimeError" in err and BEARER_VALUE not in out + err, err)


def host_ingest_keeps_the_profiler_refusal() -> None:
    seen: list[dict[str, Any]] = []
    with tempfile.TemporaryDirectory() as folder:
        root = Path(folder)
        args, env = host_ingest_environment(root)
        config = load("glue")
        config["source"]["sourceConfig"]["config"]["generateSampleData"] = True
        bad = root / "bad"
        bad.mkdir()
        (bad / "glue.json").write_text(json.dumps(config))
        args[args.index("--config-dir") + 1] = str(bad)
        code, _, err = run_host_ingest(args, env, seen.append, s3_client=FakeS3(chain_keys()))
    require(code == 2 and seen == [], (code, seen))
    require("generatesampledata" in err.lower(), err)


def om_routes(state: dict[str, Any]) -> Callable[[str, str, dict[str, Any]], tuple[int, Any]]:
    """The routes of a stand-in OpenMetadata server. The paths are the bootstrap script's assumptions about the real server."""

    def route(method: str, path: str, body: dict[str, Any]) -> tuple[int, Any]:
        if path == "/api/v1/system/version":
            return (200, {"version": "stand-in"}) if time.monotonic() >= state.get("healthy_at", 0) else (503, {})
        if path == "/api/v1/users/login":
            import base64

            if body.get("email") == ADMIN_EMAIL and base64.b64decode(body["password"]).decode() == state["password"]:
                return 200, {"accessToken": "login.session.token"}
            return 401, {}
        if path == "/api/v1/users/changePassword":
            state["changes"] = state.get("changes", 0) + 1
            if body.get("oldPassword") != state["password"]:
                return 400, {}
            state["password"] = body["newPassword"]
            return 200, "Password Updated Successfully"
        if path == "/api/v1/bots/name/ingestion-bot":
            return 200, {"botUser": {"id": "bot-user-id"}}
        if path == "/api/v1/users/auth-mechanism/bot-user-id":
            return 200, {"config": {"JWTToken": BEARER_VALUE}}
        return 404, {}

    return route


def run_bootstrap(args: list[str], env: dict[str, str]) -> tuple[int, str, str]:
    from scripts.infrastructure import openmetadata_bootstrap as bootstrap

    with quiet() as (out, err):
        code = bootstrap.main(args, env=env)
    return code, out.getvalue(), err.getvalue()


def bootstrap_changes_the_default_password_once_and_hands_over_a_token() -> None:
    require(ADMIN_EMAIL == "admin@example.org", "the administrator login is on the reserved domain the server is configured with")
    state: dict[str, Any] = {"password": "admin", "healthy_at": time.monotonic() + 1.0}
    with serving(om_routes(state)) as server, tempfile.TemporaryDirectory() as folder:
        root = Path(folder)
        args = [
            "--base",
            server.url + "/api",
            "--token-file",
            str(root / "token"),
            "--abort-file",
            str(root / "abort"),
            "--deadline-seconds",
            "20",
            "--poll-seconds",
            "1",
        ]
        env = {"OM_ADMIN_PASSWORD": ADMIN_VALUE}
        code, out, err = run_bootstrap(args, env)
        require(code == 0, (code, err))
        require(state["password"] == ADMIN_VALUE and state["changes"] == 1, state)
        require((root / "token").read_text().strip() == BEARER_VALUE, '(root / "token").read_text().strip() == BEARER_VALUE')
        require(oct((root / "token").stat().st_mode & 0o777) == "0o600", "the token file must be owner-only")
        require(not (root / "abort").exists(), 'not (root / "abort").exists()')
        code, out2, err2 = run_bootstrap(args, env)
        require(code == 0 and state["changes"] == 1, "a rerun must not change the password again")
    for secret in (ADMIN_VALUE, BEARER_VALUE, "login.session.token"):
        require(secret not in out + err + out2 + err2, "a secret reached the output")


def bootstrap_marks_an_abort_when_the_server_never_gets_healthy() -> None:
    state: dict[str, Any] = {"password": "admin", "healthy_at": time.monotonic() + 3600}
    with serving(om_routes(state)) as server, tempfile.TemporaryDirectory() as folder:
        root = Path(folder)
        args = [
            "--base",
            server.url + "/api",
            "--token-file",
            str(root / "token"),
            "--abort-file",
            str(root / "abort"),
            "--deadline-seconds",
            "2",
            "--poll-seconds",
            "1",
        ]
        started = time.monotonic()
        code, _, err = run_bootstrap(args, {"OM_ADMIN_PASSWORD": ADMIN_VALUE})
        require(code == 1 and (root / "abort").exists() and not (root / "token").exists(), (code, err))
        require(time.monotonic() - started < 15, "the wait must end at its deadline")


def bootstrap_marks_an_abort_when_no_known_password_logs_in() -> None:
    state: dict[str, Any] = {"password": "Some-Other-1!"}
    with serving(om_routes(state)) as server, tempfile.TemporaryDirectory() as folder:
        root = Path(folder)
        args = [
            "--base",
            server.url + "/api",
            "--token-file",
            str(root / "token"),
            "--abort-file",
            str(root / "abort"),
            "--deadline-seconds",
            "5",
            "--poll-seconds",
            "1",
        ]
        code, out, err = run_bootstrap(args, {"OM_ADMIN_PASSWORD": ADMIN_VALUE})
        require(code == 1 and (root / "abort").exists(), (code, err))
        require(state.get("changes", 0) == 0 and ADMIN_VALUE not in out + err, 'state.get("changes", 0) == 0 and ADMIN_VALUE not in out + err')


def bootstrap_refuses_a_missing_password() -> None:
    with tempfile.TemporaryDirectory() as folder:
        root = Path(folder)
        code, _, err = run_bootstrap(
            ["--base", "http://127.0.0.1:9/api", "--token-file", str(root / "t"), "--abort-file", str(root / "a"), "--deadline-seconds", "1"], {}
        )
    require(code == 1 and "OM_ADMIN_PASSWORD" in err, (code, err))


def bounded_environment(root: Path, deadline_in: int) -> dict[str, str]:
    """The environment of one wrapper run: a deadline file the given number of seconds from now, an abort file path, a 1 second poll and a 3 second grace."""
    (root / "deadline").write_text(f"{int(time.time()) + deadline_in}\n")
    return {
        **os.environ,
        "OM_DEADLINE_FILE": str(root / "deadline"),
        "OM_ABORT_FILE": str(root / "abort"),
        "OM_POLL_SECONDS": "1",
        "OM_STOP_GRACE_SECONDS": "3",
        "OM_MARKER": str(root / "ran"),
    }


def bounded_wrapper_stops_a_server_at_its_deadline_with_exit_zero() -> None:
    with tempfile.TemporaryDirectory() as folder:
        env = bounded_environment(Path(folder), 3)
        started = time.monotonic()
        done = run_command("sh", ["scripts/infrastructure/openmetadata_bounded_run.sh", "0", "sleep", "300"], cwd=REPO, env=env, timeout=60)
    require(done.returncode == 0 and time.monotonic() - started < 15, (done.returncode, time.monotonic() - started))


def bounded_wrapper_passes_on_a_crash_and_a_clean_exit() -> None:
    with tempfile.TemporaryDirectory() as folder:
        env = bounded_environment(Path(folder), 60)
        crashed = run_command("sh", ["scripts/infrastructure/openmetadata_bounded_run.sh", "0", "sh", "-c", "exit 3"], cwd=REPO, env=env, timeout=60)
        clean = run_command("sh", ["scripts/infrastructure/openmetadata_bounded_run.sh", "0", "true"], cwd=REPO, env=env, timeout=60)
    require(crashed.returncode == 3 and clean.returncode == 0, (crashed.returncode, clean.returncode))


def bounded_wrapper_refuses_to_start_after_the_deadline() -> None:
    with tempfile.TemporaryDirectory() as folder:
        env = bounded_environment(Path(folder), -10)
        done = run_command("sh", ["scripts/infrastructure/openmetadata_bounded_run.sh", "0", "sh", "-c", 'touch "$OM_MARKER"'], cwd=REPO, env=env, timeout=60)
        require(done.returncode == 1 and not (Path(folder) / "ran").exists(), "a command must not start after its deadline")


def bounded_wrapper_stops_on_the_abort_marker_with_exit_one() -> None:
    with tempfile.TemporaryDirectory() as folder:
        root = Path(folder)
        env = bounded_environment(root, 600)
        timer = threading.Timer(1.5, root.joinpath("abort").write_text, ["bootstrap failed\n"])
        timer.start()
        started = time.monotonic()
        done = run_command("sh", ["scripts/infrastructure/openmetadata_bounded_run.sh", "0", "sleep", "300"], cwd=REPO, env=env, timeout=60)
        timer.join()
    require(done.returncode == 1 and time.monotonic() - started < 15, (done.returncode, time.monotonic() - started))


def bounded_wrapper_extra_seconds_outlive_the_server_deadline() -> None:
    with tempfile.TemporaryDirectory() as folder:
        env = bounded_environment(Path(folder), 2)
        started = time.monotonic()
        done = run_command("sh", ["scripts/infrastructure/openmetadata_bounded_run.sh", "4", "sleep", "300"], cwd=REPO, env=env, timeout=60)
        took = time.monotonic() - started
    require(done.returncode == 0 and 5 <= took < 20, (done.returncode, took))


def bounded_wrapper_exits_zero_when_stopped_after_the_deadline() -> None:
    with tempfile.TemporaryDirectory() as folder:
        env = bounded_environment(Path(folder), 2)
        done = run_command(
            "sh",
            ["scripts/infrastructure/openmetadata_bounded_run.sh", "300", "sh", "-c", "sleep 4; kill -TERM $PPID; exec sleep 300"],
            cwd=REPO,
            env=env,
            timeout=60,
        )
    require(done.returncode == 0, f"a sibling stopped by ECS after the deadline must exit 0, got {done.returncode}")


def bounded_wrapper_fails_when_stopped_before_the_deadline() -> None:
    with tempfile.TemporaryDirectory() as folder:
        env = bounded_environment(Path(folder), 600)
        done = run_command(
            "sh",
            ["scripts/infrastructure/openmetadata_bounded_run.sh", "300", "sh", "-c", "sleep 1; kill -TERM $PPID; exec sleep 300"],
            cwd=REPO,
            env=env,
            timeout=60,
        )
    require(done.returncode == 1, f"a stop before the deadline is a failure, got {done.returncode}")


def migrate_script_writes_the_abort_marker_after_its_last_try() -> None:
    with tempfile.TemporaryDirectory() as folder:
        env = {**bounded_environment(Path(folder), 600), "OM_MIGRATE_TRIES": "2", "OM_MIGRATE_SLEEP": "1"}
        done = run_command("sh", ["scripts/infrastructure/openmetadata_migrate.sh", "false"], cwd=REPO, env=env, timeout=60)
        marker = Path(folder) / "abort"
        require(done.returncode == 1, f"the exhausted migration must fail, got {done.returncode}")
        require(marker.exists() and "migration" in marker.read_text(), "the exhausted migration must write the abort marker")


def migrate_script_passes_a_migration_that_succeeds_on_a_later_try() -> None:
    with tempfile.TemporaryDirectory() as folder:
        env = {**bounded_environment(Path(folder), 600), "OM_MIGRATE_TRIES": "3", "OM_MIGRATE_SLEEP": "1"}
        script = 'test -e "$OM_MARKER" || { touch "$OM_MARKER"; exit 1; }'
        done = run_command("sh", ["scripts/infrastructure/openmetadata_migrate.sh", "sh", "-c", script], cwd=REPO, env=env, timeout=60)
        require(done.returncode == 0 and not (Path(folder) / "abort").exists(), f"a later success must pass without an abort, got {done.returncode}")


def catalog_policy_reads_dbt_artifacts_and_nothing_else() -> None:
    from infra.iam.scripts.render_policy import render_template
    from scripts.infrastructure.policy_evaluator import Request, decide

    values = {
        "AWS_REGION": "us-west-2",
        "AWS_ACCOUNT_ID": "111111111111",
        "PROJECT_NAME": "example_project",
        "ENVIRONMENT": "dev",
        "DATA_BUCKET_NAME": "example-project-data-dev",
    }
    document = render_template(OWNER_POLICY.read_text(), values, names={})
    bucket = "arn:aws:s3:::example-project-data-dev"
    # The Glue connector reads the mirror registrations (owner decision of Nov 7 2026), so no Iceberg metadata file stays readable.
    allowed = [
        ("s3:GetObject", f"{bucket}/metadata/dbt/manifest.json", {}),
        ("s3:ListBucket", bucket, {"s3:prefix": ("metadata/dbt/",)}),
    ]
    refused = [
        ("s3:GetObject", f"{bucket}/lakehouse/silver/hospital/metadata/00001.metadata.json", {}),
        ("s3:GetObject", f"{bucket}/lakehouse/gold/dim_hospital/metadata/snap-1.avro", {}),
        ("s3:GetObjectVersion", f"{bucket}/lakehouse/gold/dim_hospital/metadata/00001.metadata.json", {}),
        ("s3:ListBucket", bucket, {"s3:prefix": ("lakehouse/silver/hospital/metadata/",)}),
        ("s3:GetObject", f"{bucket}/lakehouse/silver/hospital/data/00000-0.parquet", {}),
        ("s3:GetObject", f"{bucket}/lakehouse/bronze/raw/file.csv", {}),
        ("s3:GetObject", f"{bucket}/lakehouse/run_stage/run/inputs/code.tar.gz", {}),
        ("s3:GetObject", f"{bucket}/drift/run/drift_hac.json", {}),
        ("s3:PutObject", f"{bucket}/metadata/dbt/manifest.json", {}),
        ("s3:PutObject", f"{bucket}/lakehouse/silver/hospital/metadata/new.json", {}),
        ("s3:DeleteObject", f"{bucket}/lakehouse/silver/hospital/metadata/00001.metadata.json", {}),
        ("s3:ListBucket", bucket, {"s3:prefix": ("lakehouse/silver/hospital/data/",)}),
        ("s3:ListBucket", bucket, {"s3:prefix": ("",)}),
        ("ssm:GetParameter", "arn:aws:ssm:us-west-2:111111111111:parameter/example_project/polaris_root_secret", {}),
        ("ecs:RunTask", "*", {}),
    ]
    for action, resource, context in allowed:
        require(decide([document], Request(action, resource, context)).allowed, f"should allow {action} {resource}")
    for action, resource, context in refused:
        require(not decide([document], Request(action, resource, context)).allowed, f"should refuse {action} {resource}")


class Recorder:
    """Records the objects the DAG file builds when airflow's names are stand-ins."""

    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
        self.tasks: list[dict[str, Any]] = []

    def __enter__(self) -> Recorder:
        Recorder.current = self
        return self

    def __exit__(self, *_: Any) -> None:
        return None

    current: Recorder


def dag_runs_one_broker_step_with_bounds_and_no_schedule() -> None:
    import importlib.util
    import types

    sdk = types.ModuleType("airflow.sdk")
    calls: list[str] = []

    def task(*args: Any, **kwargs: Any) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        def wrap(function: Callable[..., Any]) -> Callable[..., Any]:
            Recorder.current.tasks.append({"name": function.__name__, **kwargs})
            return lambda *_arguments, **_options: None

        return wrap

    for name, value in {
        "DAG": Recorder,
        "Param": lambda *a, **k: {"args": a, **k},
        "Asset": object,
        "TriggerRule": types.SimpleNamespace(ALL_DONE="all_done"),
        "get_current_context": lambda: {},
        "task": task,
    }.items():
        setattr(sdk, name, value)
    runtime = types.ModuleType("orchestration.dag_runtime")

    def record_step(template: str) -> str:
        calls.append(template)
        return "succeeded"

    runtime.__dict__.update(STEP_TIMEOUT=None, run_step=record_step, pool_settings=lambda family=None: {"pool": "stand_in_pool"})
    saved = {name: sys.modules.get(name) for name in ("airflow", "airflow.sdk", "orchestration.dag_runtime")}
    sys.modules.update({"airflow": types.ModuleType("airflow"), "airflow.sdk": sdk, "orchestration.dag_runtime": runtime})
    try:
        spec = importlib.util.spec_from_file_location("metadata_ingest_dag_under_test", DAG_FILE)
        if spec is None or spec.loader is None:
            raise AssertionError("the DAG file cannot be loaded")
        loaded = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(loaded)
        dag = Recorder.current
    finally:
        for name, value in saved.items():
            if value is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = value
    require(dag.kwargs["dag_id"] == "metadata_ingest_dag" and dag.kwargs["schedule"] is None, dag.kwargs)
    require(dag.kwargs["max_active_runs"] == 1 and dag.kwargs["default_args"] == {"retries": 0}, dag.kwargs)
    require([item["name"] for item in dag.tasks] == ["metadata_run"], dag.tasks)
    step = dag.tasks[0]
    require(step["retries"] == 0 and 0 < step["execution_timeout"].total_seconds() <= 5 * 3600, step)
    require(step["pool"] == "stand_in_pool", f"the step must run in the pool the runtime names: {step}")
    require(dag.kwargs["dagrun_timeout"].total_seconds() <= 6 * 3600, "the DAG run must have a bound")
    params = dag.kwargs["params"]
    require(set(params) == {"run_id", "mode", "features_run"}, f"the form is {sorted(params)}")
    features = params["features_run"]
    require(
        features["args"] == () and "default" not in features and features["type"] == "string" and features["pattern"] == params["run_id"]["pattern"],
        f"features_run is a required run ID with no default (never latest): {features}",
    )
    require(calls == [], "importing the DAG must not start a step")
    require(loaded.LIFETIME_CEILING_SECONDS == 14400, "the DAG bound must follow the Terraform lifetime ceiling")


def images_file_names_exactly_the_images_the_task_definition_takes() -> None:
    document = json.loads((REPO / "config" / "metadata" / "images.json").read_text())
    images = document["images"]
    require(set(images) == {"server", "ingestion", "search", "database"}, sorted(images))
    require(
        {name: images[name].get("release") for name in ("server", "ingestion", "search")} == {"server": "2.0.7", "ingestion": "2.0.7", "search": "3.3.2"},
        "selected releases",
    )
    for name, entry in images.items():
        require(entry["ecr_repository"] == f"<project>_openmetadata_{name}_<environment>", entry)
        if "release" in entry:
            require(entry["arm64"].startswith("verified Nov 3 2026"), f"{name}: a selected release states the date of its Arm check")
            require(
                entry["selected_reference"] == f"{entry['upstream']}:{entry['release']}",
                f"{name}: the selected reference must be the upstream name and the release",
            )
            require(
                entry["digest"].startswith("TO_RESOLVE") or re.fullmatch(r"sha256:[0-9a-f]{64}", entry["digest"]),
                f"{name}: the digest is TO_RESOLVE or a sha256 digest",
            )
        else:
            require(entry["arm64"].startswith("unverified"), f"{name}: the Arm claim must be stated as unverified")
        require(entry["upstream"] and "@sha256:" not in entry["upstream"] and entry["push_by"] == "digest", entry)


def project_policies_pass_the_catalog_role_and_only_the_catalog_forward_document() -> None:
    from infra.iam.scripts.render_policy import render_template, terraform_names
    from scripts.infrastructure.policy_evaluator import Request, decide

    values = {
        "AWS_REGION": "us-west-2",
        "AWS_ACCOUNT_ID": "111111111111",
        "PROJECT_NAME": "example_project",
        "ENVIRONMENT": "dev",
        "DATA_BUCKET_NAME": "example-project-data-dev",
    }
    folder = Path(os.environ.get("OM_POLICY_DIRECTORY", str(REPO / "infra" / "iam" / "policies")))
    texts = [path.read_text() for path in sorted(folder.glob("*.json")) if path.name.endswith(("_iam_policy.json", "_ssmforward_policy.json"))]
    spare = {name: "/example_project/spare" for text in texts for name in re.findall(r"\$\{([A-Z][A-Z0-9_]*)\}", text) if name not in values}
    documents = [render_template(text, values, {**terraform_names(), **spare}) for text in texts]
    role = "arn:aws:iam::111111111111:role/example_project_job_openmetadata_task_dev"
    forward = "arn:aws:ssm:us-west-2:111111111111:document/example_project_catalog_forward_dev"
    generic = "arn:aws:ssm:us-west-2::document/AWS-StartPortForwardingSessionToRemoteHost"
    tasks = {"iam:PassedToService": ("ecs-tasks.amazonaws.com",)}
    require(
        any(decide([document], Request("iam:PassRole", role, tasks)).allowed for document in documents),
        "the project user must pass the catalog task role to ECS tasks",
    )
    require(
        not any(decide([document], Request("iam:PassRole", role, {"iam:PassedToService": ("ec2.amazonaws.com",)})).allowed for document in documents),
        "only ECS tasks may receive it",
    )
    require(
        any(decide([document], Request("ssm:StartSession", forward)).allowed for document in documents), "the project catalog forward document must be allowed"
    )
    require(
        not any(decide([document], Request("ssm:StartSession", generic)).allowed for document in documents),
        "the general remote-host forward must not be granted",
    )
    other = "arn:aws:ssm:us-west-2::document/AWS-StartInteractiveCommand"
    require(not any(decide([document], Request("ssm:StartSession", other)).allowed for document in documents), "no other session document may be added")


CASES: list[Callable[[], None]] = [
    glue_config_reads_the_two_mirror_databases_with_the_task_role,
    ingest_dbt_points_gold_and_features_models_at_the_glue_tables,
    bounded_wrapper_exits_zero_when_stopped_after_the_deadline,
    bounded_wrapper_fails_when_stopped_before_the_deadline,
    migrate_script_writes_the_abort_marker_after_its_last_try,
    migrate_script_passes_a_migration_that_succeeds_on_a_later_try,
    project_policies_pass_the_catalog_role_and_only_the_catalog_forward_document,
    images_file_names_exactly_the_images_the_task_definition_takes,
    real_configs_have_no_profiler_or_sampling,
    every_profiler_or_sampling_mutant_is_refused,
    ingest_substitutes_values_and_keeps_secrets_out_of_output,
    ingest_takes_the_token_from_the_environment_and_the_persistent_server,
    ingest_refuses_a_missing_token_source_or_a_wrong_server_url,
    ingest_refuses_a_profiler_config_before_the_workflow_starts,
    ingest_names_a_missing_variable_but_not_a_value,
    ingest_fails_when_the_workflow_fails_without_echoing_the_config,
    ingest_dbt_downloads_the_artifacts_and_fails_without_a_manifest,
    raw_files_are_written_onto_each_bronze_table,
    raw_files_write_is_idempotent_and_keeps_other_text,
    raw_files_list_is_capped,
    raw_files_name_their_receipt_and_acquisition_run,
    raw_files_without_a_completion_record_say_so,
    acquisition_records_that_are_wrong_stop_the_write,
    an_object_named_by_two_records_takes_the_earliest_capture,
    sub_second_capture_times_are_compared_as_instants,
    equal_capture_instants_choose_by_unit_then_payload,
    a_completion_record_without_a_usable_time_stops_the_write,
    a_bucket_root_completion_record_is_not_read,
    raw_files_writer_refuses_and_fails_visibly,
    chain_follows_the_recorded_pins,
    chain_names_the_bronze_run_of_silvers_claim,
    host_ingest_writes_the_bronze_files_after_the_dbt_layers,
    host_ingest_skips_the_bronze_files_when_silver_ran_on_latest,
    chain_refuses_a_missing_or_mismatched_pin,
    host_ingest_ingests_the_three_recorded_layers,
    host_ingest_fails_when_a_chain_run_names_the_old_mirror_schemas,
    host_ingest_never_falls_back_to_latest,
    host_ingest_aims_both_sources_at_the_persistent_catalog,
    host_ingest_refuses_an_unusable_catalog_address,
    host_ingest_accepts_each_private_range_on_the_catalog_port,
    host_ingest_refuses_a_missing_token,
    host_ingest_stops_at_the_first_failing_source,
    host_ingest_keeps_the_profiler_refusal,
    bootstrap_changes_the_default_password_once_and_hands_over_a_token,
    bootstrap_marks_an_abort_when_the_server_never_gets_healthy,
    bootstrap_marks_an_abort_when_no_known_password_logs_in,
    bootstrap_refuses_a_missing_password,
    bounded_wrapper_stops_a_server_at_its_deadline_with_exit_zero,
    bounded_wrapper_passes_on_a_crash_and_a_clean_exit,
    bounded_wrapper_refuses_to_start_after_the_deadline,
    bounded_wrapper_stops_on_the_abort_marker_with_exit_one,
    bounded_wrapper_extra_seconds_outlive_the_server_deadline,
    catalog_policy_reads_dbt_artifacts_and_nothing_else,
    dag_runs_one_broker_step_with_bounds_and_no_schedule,
]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-directory", type=Path, default=None)
    parser.add_argument("--only", default=None, help="run the one case with this name")
    options = parser.parse_args()
    selected = [case for case in CASES if options.only in (None, case.__name__)]
    records = []
    for case in selected:
        started = time.monotonic()
        try:
            case()
            outcome, detail = "passed", ""
        except Exception as error:
            outcome, detail = "failed", f"{type(error).__name__}: {error}"[:400]
        records.append({"case": case.__name__, "outcome": outcome, "seconds": round(time.monotonic() - started, 2), "detail": detail})
        sys.stdout.write(f"{outcome:6} {case.__name__} {detail}\n")
    failed = [record for record in records if record["outcome"] != "passed"]
    if options.output_directory is not None:
        options.output_directory.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        (options.output_directory / f"openmetadata_e2e_{stamp}.json").write_text(
            json.dumps({"finished_at": stamp, "mode": "full" if options.only is None else "iteration", "cases": records}, indent=2) + "\n"
        )
    sys.stdout.write(f"{len(records) - len(failed)} passed, {len(failed)} failed\n")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
