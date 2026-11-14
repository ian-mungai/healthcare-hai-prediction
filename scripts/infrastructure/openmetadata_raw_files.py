"""Write the raw files that each bronze table was loaded from onto the table in the OpenMetadata catalog.

OpenMetadata 2.0.7 can show a raw S3 file as lineage only through a container entity that an S3 storage connector made, which this project does not
run (owner decision "Raw-file lineage route", Nov 13 2026). So the files are written as text on the table: for each table a bronze run loaded, the
object keys, the object versions, the SHA-256 and the sizes that the run record ``lakehouse/bronze_runs/<run>/check.json`` lists, in a block of the
table's description between two markers. The block is replaced by the next run's, text around it is kept, and a block that would not change is not
written, so repeating a run changes nothing.

    python -m scripts.infrastructure.openmetadata_raw_files --bronze-run <run> [--bronze-run <run> ...]

The catalog address and the bot token come from ``OM_API`` and ``OM_INGESTION_TOKEN`` and the bucket from ``DATA_BUCKET_NAME``, as for the host
ingestion. Exit codes: 0 done, 1 a record or a table failed, 2 refused. Output names tables, keys and exception classes and never prints the token.

Recorded gaps: the receipt ids and the acquisition run are not in the run record. They are in the unit completion records beside the raw objects,
which need a mapping from table to unit and a read grant on the raw prefix, and they are not written here. The bronze run IDs are not recorded by
silver's pins either, so the caller names them.
"""

from __future__ import annotations

import argparse
import http.client
import json
import os
import re
import sys
import urllib.parse
from collections.abc import Callable, Mapping, Sequence
from typing import Any

from scripts.infrastructure import openmetadata_host_ingest as host_ingest

START = "<!-- hai-raw-files:start -->"
END = "<!-- hai-raw-files:end -->"
MAX_FILES = 50
SERVICE, DATABASE, SCHEMA = "glue_mirror", "hai_lakehouse", "hai_mirror_bronze"
RUN_ID = re.compile(r"[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}")
RECORD_KEY = "lakehouse/bronze_runs/{run}/check.json"
TIMEOUT_SECONDS = 20.0
BLOCK = re.compile(re.escape(START) + r".*?" + re.escape(END), re.S)

Http = Callable[[str, str, Mapping[str, str], bytes | None], tuple[int, Any]]


class WriteFailed(RuntimeError):
    """A record or a table could not be handled. The message names it and never holds the token."""


def block_text(table: str, run: str, key: str, objects: Sequence[Mapping[str, Any]], snapshot: int) -> str:
    """The marked block of one table: its run, snapshot and the files it was loaded from, at most ``MAX_FILES`` of them in key order."""
    files = sorted(objects, key=lambda item: (item["key"], item["version_id"]))
    lines = [
        START,
        f"Raw files loaded into {table} by bronze run {run} (table snapshot {snapshot}): {len(files)} files, {sum(item['byte_count'] for item in files)} bytes",
    ]
    lines += [
        f"- s3://{item['bucket']}/{item['key']} version {item['version_id']} sha256 {item['sha256']} bytes {item['byte_count']}" for item in files[:MAX_FILES]
    ]
    if len(files) > MAX_FILES:
        lines.append(f"and {len(files) - MAX_FILES} more files; the full list is the run record {key}.")
    lines.append(END)
    return "\n".join(lines)


def merged(description: str, block: str) -> str:
    """The description with its marked block replaced by ``block``, or ``block`` added after the existing text."""
    if BLOCK.search(description):
        return BLOCK.sub(lambda _: block, description, count=1)
    return f"{description}\n\n{block}" if description else block


def run_blocks(s3: Any, bucket: str, run: str) -> dict[str, str]:
    """The block of each table the bronze run loaded, from its run record."""
    from botocore.exceptions import BotoCoreError, ClientError

    from orchestration import raw_lineage
    from orchestration.ecs_lineage import EvidenceUnavailable, LineageRefused, S3Evidence

    key = RECORD_KEY.format(run=run)
    try:
        record = S3Evidence(s3, bucket).read(key)
        if record is None:
            raise WriteFailed(f"{key}: missing")
        objects, snapshots = raw_lineage.loaded_objects(record, key, run)
    except (LineageRefused, EvidenceUnavailable) as error:
        raise WriteFailed(str(error)) from None
    except (ClientError, BotoCoreError) as error:
        raise WriteFailed(f"{key}: {type(error).__name__}") from None
    return {table: block_text(table, run, key, [item for item in objects if item["table"] == table], snapshots[table]) for table in sorted(snapshots)}


def http_request(method: str, url: str, headers: Mapping[str, str], body: bytes | None) -> tuple[int, Any]:
    """One bounded request. A status answer is returned with its JSON body (None when there is none); a refused connection or a timeout is raised as
    ``WriteFailed`` by its class only."""
    parts = urllib.parse.urlsplit(url)
    connection = http.client.HTTPConnection(parts.hostname or "", parts.port, timeout=TIMEOUT_SECONDS)
    try:
        connection.request(method, parts.path + (f"?{parts.query}" if parts.query else ""), body=body, headers=dict(headers))
        response = connection.getresponse()
        data = response.read(8 * 1024**2)
    except (OSError, http.client.HTTPException) as error:
        raise WriteFailed(f"the catalog did not answer: {type(error).__name__}") from None
    finally:
        connection.close()
    try:
        return response.status, json.loads(data) if data else None
    except ValueError:
        return response.status, None


def write_blocks(api: str, token: str, blocks: Mapping[str, str], http: Http = http_request) -> dict[str, list[str]]:
    """Put each block on its table. Returns the tables written, left unchanged and absent from the catalog. A table is written only when its description
    would change, so a repeat writes nothing."""
    result: dict[str, list[str]] = {"written": [], "unchanged": [], "absent": []}
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    for table, block in blocks.items():
        fqn = urllib.parse.quote(f"{SERVICE}.{DATABASE}.{SCHEMA}.{table}", safe=".")
        status, found = http("GET", f"{api}/v1/tables/name/{fqn}?fields=description", headers, None)
        if status == 404:
            result["absent"].append(table)
            continue
        if status != 200 or not isinstance(found, dict) or "id" not in found:
            raise WriteFailed(f"{table}: the catalog answered HTTP {status} to the table lookup")
        current = found.get("description") or ""
        wanted = merged(current, block)
        if wanted == current:
            result["unchanged"].append(table)
            continue
        patch = json.dumps([{"op": "add", "path": "/description", "value": wanted}]).encode()
        status, _ = http("PATCH", f"{api}/v1/tables/{found['id']}", {**headers, "Content-Type": "application/json-patch+json"}, patch)
        if status != 200:
            raise WriteFailed(f"{table}: the catalog answered HTTP {status} to the description update")
        result["written"].append(table)
    return result


def write_runs(api: str, token: str, s3: Any, bucket: str, runs: Sequence[str], http: Http = http_request) -> int:
    """Write the raw files of each bronze run onto its tables and say what happened on standard output and error. Returns 0, or 1 when a record or a table
    failed or a table is not in the catalog."""
    failed = False
    for run in sorted(set(runs)):
        try:
            result = write_blocks(api, token, run_blocks(s3, bucket, run), http)
        except WriteFailed as error:
            sys.stderr.write(f"openmetadata_raw_files: {error}\n")
            failed = True
            continue
        sys.stdout.write(
            f"openmetadata_raw_files: run {run}: {len(result['written'])} written, {len(result['unchanged'])} unchanged, {len(result['absent'])} absent\n"
        )
        if result["absent"]:
            sys.stderr.write(
                f"openmetadata_raw_files: not in the catalog: {', '.join(result['absent'][:5])}{' and more' if len(result['absent']) > 5 else ''}\n"
            )
            failed = True
    return 1 if failed else 0


def main(argv: list[str], env: Mapping[str, str] | None = None, s3_client: Any = None, http: Http = http_request) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bronze-run", action="append", required=True, default=[])
    options = parser.parse_args(argv)
    environment = dict(os.environ if env is None else env)
    api, token = environment.get("OM_API", ""), environment.get("OM_INGESTION_TOKEN", "")
    if not host_ingest.catalog_address_is_valid(api):
        sys.stderr.write("openmetadata_raw_files: OM_API must be http://<private IPv4 address>:8585/api, with a 10., 172.16 to 172.31 or 192.168. address\n")
        return 2
    if not token:
        sys.stderr.write("openmetadata_raw_files: OM_INGESTION_TOKEN is not set\n")
        return 2
    runs = sorted(set(options.bronze_run))
    if not all(RUN_ID.fullmatch(run) for run in runs):
        sys.stderr.write("openmetadata_raw_files: every --bronze-run must be a run ID; there is no latest\n")
        return 2
    client = s3_client
    if client is None:
        import boto3

        client = boto3.client("s3", region_name=environment.get("AWS_REGION"))
    return write_runs(api, token, client, environment.get("DATA_BUCKET_NAME", ""), runs, http)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
