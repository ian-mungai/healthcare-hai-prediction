"""Write the raw files that each bronze table was loaded from onto the table in the OpenMetadata catalog.

OpenMetadata 2.0.7 can show a raw S3 file as lineage only through a container entity that an S3 storage connector made, which this project does not
run (owner decision "Raw-file lineage route", Nov 13 2026). So the files are written as text on the table: for each table a bronze run loaded, the
object keys, the object versions, the SHA-256 and the sizes that the run record ``lakehouse/bronze_runs/<run>/check.json`` lists, in a block of the
table's description between two markers. The block is replaced by the next run's, text around it is kept, and a block that would not change is not
written, so repeating a run changes nothing.

    python -m scripts.infrastructure.openmetadata_raw_files --bronze-run <run> [--bronze-run <run> ...]

The catalog address and the bot token come from ``OM_API`` and ``OM_INGESTION_TOKEN`` and the bucket from ``DATA_BUCKET_NAME``, as for the host
ingestion. Exit codes: 0 done, 1 a record or a table failed, 2 refused. Output names tables, keys and exception classes and never prints the token.

Acquisition identities: the run record names each loaded file by key and exact version. Each file is matched, by that key and version, to the unit
completion record that names it (``scripts/acquisition/completion_record.py``, under ``raw/ingestion_units/``, the deployed capture root,
read by listing and checked against its own key). The table's text then carries, for each unit that stored its files, the acquisition run (``run_id``
of the record), the unit, the source, the collection, the retrieval time and the receipt object the record names (key, version, SHA-256). Where two
records name one object version, the earliest capture is the one that stored it: retrieval times are compared as instants, and equal instants by unit
key and then payload digest. A file that no record names says so on its line.

Recorded limits: the receipt is identified by the object the completion record names, not by the snapshot ID inside the receipt body, which would need
one more read for each unit. The bronze run is the one the caller names (the host task takes it from silver's launch claim).
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
from datetime import datetime
from typing import Any

from scripts.acquisition.raw_root import RAW_ROOT
from scripts.infrastructure import openmetadata_host_ingest as host_ingest

START = "<!-- hai-raw-files:start -->"
END = "<!-- hai-raw-files:end -->"
MAX_FILES = 50
MAX_UNITS = 20  # acquisition lines of one table; more are counted
MAX_RECORDS = 20_000  # unit completion records read for one run
RECORD_BYTES = 2 * 1024**2
CAPTURE_ROOTS = (RAW_ROOT,)  # the deployed capture root (infra/jobs/publisher.tf capture_root): the only place this writer looks for completion records
SERVICE, DATABASE, SCHEMA = "glue_mirror", "hai_lakehouse", "hai_mirror_bronze"
RUN_ID = re.compile(r"[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}")
RECORD_KEY = "lakehouse/bronze_runs/{run}/check.json"
TIMEOUT_SECONDS = 20.0
BLOCK = re.compile(re.escape(START) + r".*?" + re.escape(END), re.S)

Http = Callable[[str, str, Mapping[str, str], bytes | None], tuple[int, Any]]


class WriteFailed(RuntimeError):
    """A record or a table could not be handled. The message names it and never holds the token."""


Acquisition = Mapping[tuple[str, str], Mapping[str, Any]]  # (object key, object version) -> the completion record that stored that version first


def record_key_of(info: Mapping[str, Any]) -> tuple[datetime, str, str]:
    """The order of the captures: the retrieval instant, then the unit key and the payload digest, so two records with one instant have one winner."""
    return (info["instant"], str(info["unit_key"]), str(info["payload_sha256"]))


def instant(value: Any, key: str) -> datetime:
    """A retrieval time as a timezone-aware instant. ``...09:00:00Z`` and ``...09:00:00.100000Z`` order by their value, not by their text. A time that is not
    an ISO 8601 date-time with an offset cannot be ordered, so it stops the write by the record's key."""
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00")) if isinstance(value, str) and len(value) <= 40 else None
    except ValueError:
        parsed = None
    if parsed is None or parsed.utcoffset() is None:
        raise WriteFailed(f"{key}: the retrieval time {str(value)[:50]!r} is not a date-time with an offset")
    return parsed


def acquisition_index(s3: Any, bucket: str) -> dict[tuple[str, str], dict[str, Any]]:
    """Every object version that a unit completion record of the bucket names, with the earliest record that names it (the capture that stored it).

    The records are read from ``raw/ingestion_units/``, the deployed capture root, by their listing, never by a guess of a unit key. A record at the bucket
    root is never listed or read. Each is checked against its own key (``completion_record.parse``). A listing past ``MAX_RECORDS``, an unreadable record or
    a refused one
    stops the write by its key.
    """
    from botocore.exceptions import BotoCoreError, ClientError

    from scripts.acquisition import completion_record as records

    found: list[tuple[str, str]] = []
    for root in CAPTURE_ROOTS:
        prefix = f"{root}{records.UNITS_FOLDER}"
        token: str | None = None
        while True:
            try:
                page = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, **({"ContinuationToken": token} if token else {}))
            except ClientError as error:
                raise WriteFailed(f"{prefix}: {error.response.get('Error', {}).get('Code', 'ClientError')}") from None
            except BotoCoreError as error:
                raise WriteFailed(f"{prefix}: {type(error).__name__}") from None
            found += [(root, item["Key"]) for item in page.get("Contents", [])]
            if len(found) > MAX_RECORDS:
                raise WriteFailed(f"{prefix}: more than {MAX_RECORDS} completion records")
            if not page.get("IsTruncated"):
                break
            token = page["NextContinuationToken"]
    index: dict[tuple[str, str], dict[str, Any]] = {}
    for root, key in sorted(found):
        try:
            body = s3.get_object(Bucket=bucket, Key=key)["Body"].read(RECORD_BYTES + 1)
        except ClientError as error:
            raise WriteFailed(f"{key}: {error.response.get('Error', {}).get('Code', 'ClientError')}") from None
        except BotoCoreError as error:
            raise WriteFailed(f"{key}: {type(error).__name__}") from None
        if len(body) > RECORD_BYTES:
            raise WriteFailed(f"{key}: larger than {RECORD_BYTES} bytes")
        try:
            record = records.parse(body, key=key, root=root)
        except records.RecordError as error:
            raise WriteFailed(f"{key}: {error}") from None
        info = {name: record[name] for name in ("unit_key", "payload_sha256", "run_id", "source_id", "collection", "retrieved_at_utc", "receipt")}
        info["instant"] = instant(record["retrieved_at_utc"], key)
        for item in records.objects_named(record):
            current = index.get((item["key"], item["version_id"]))
            if current is None or record_key_of(info) < record_key_of(current):
                index[(item["key"], item["version_id"])] = info
    return index


def acquisition_lines(files: Sequence[Mapping[str, Any]], acquisition: Acquisition, bucket: str) -> list[str]:
    """One line for each unit completion record that stored files of the table: its acquisition run, unit, source, collection, retrieval time and receipt."""
    units: dict[tuple[datetime, str, str], tuple[Mapping[str, Any], int]] = {}
    for item in files:
        info = acquisition.get((item["key"], item["version_id"]))
        if info is not None:
            key = record_key_of(info)
            units[key] = (info, units.get(key, (info, 0))[1] + 1)
    lines = []
    for key in sorted(units)[:MAX_UNITS]:
        info, count = units[key]
        receipt = info["receipt"]
        lines.append(
            f"Stored by acquisition run {info['run_id']}, unit {info['unit_key']}, source {info['source_id']}, collection {info['collection']}, "
            f"retrieved {info['retrieved_at_utc']}; receipt s3://{bucket}/{receipt['key']} version {receipt['version_id']} "
            f"sha256 {receipt['sha256']}: {count} files"
        )
    if len(units) > MAX_UNITS:
        lines.append(f"and {len(units) - MAX_UNITS} more acquisition records.")
    return lines


def block_text(
    table: str, run: str, key: str, objects: Sequence[Mapping[str, Any]], snapshot: int, acquisition: Acquisition | None = None, bucket: str = ""
) -> str:
    """The marked block of one table: its run, snapshot, the acquisition run and receipt of each unit that stored its files, and the files it was loaded from,
    at most ``MAX_FILES`` of them in key order. A file that no completion record names with that exact version says so."""
    files = sorted(objects, key=lambda item: (item["key"], item["version_id"]))
    known = acquisition or {}
    recorded = sum(1 for item in files if (item["key"], item["version_id"]) in known)
    lines = [
        START,
        f"Raw files loaded into {table} by bronze run {run} (table snapshot {snapshot}): {len(files)} files, "
        f"{sum(item['byte_count'] for item in files)} bytes; {recorded} of {len(files)} files have an acquisition record",
        *acquisition_lines(files, known, bucket),
    ]
    lines += [
        f"- s3://{item['bucket']}/{item['key']} version {item['version_id']} sha256 {item['sha256']} bytes {item['byte_count']}"
        + ("" if (item["key"], item["version_id"]) in known else "; no completion record names this file version")
        for item in files[:MAX_FILES]
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
    acquisition = acquisition_index(s3, bucket)
    return {
        table: block_text(table, run, key, [item for item in objects if item["table"] == table], snapshots[table], acquisition, bucket)
        for table in sorted(snapshots)
    }


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
