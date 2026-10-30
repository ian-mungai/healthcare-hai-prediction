"""Copy Marquez job lineage into OpenMetadata as table lineage, for the catalog task.

OpenMetadata has no Marquez connector. This step reads the datasets and lineage graphs from the Marquez HTTP API, maps each Marquez dataset name to an
OpenMetadata table name with the pattern in ``config/metadata/ingestion/marquez.json`` and adds one edge for each pair of an input and an output dataset of
a job. A dataset whose name does not match counts as unmapped and gets no edge. Adding an edge twice leaves one edge. Marquez being unreachable is a failure
(exit 1), so the task shows it. Runs in the ingestion image: ``python openmetadata_marquez.py --marquez URL``. The Marquez and OpenMetadata routes are unproven
against the real servers. Output holds counts and names of steps, never a token.
"""

from __future__ import annotations

import argparse
import http.client
import json
import re
import sys
import time
import urllib.parse
from collections.abc import Mapping
from pathlib import Path
from typing import Any

PAGE = 200
MAX_PAGES = 10000


class Unreachable(Exception):
    """A server did not answer."""


def call(method: str, url: str, body: Mapping[str, Any] | None, headers: Mapping[str, str], timeout: float) -> tuple[int, Any]:
    """Send one JSON request to a plain-HTTP address and return the status and parsed body. A missing answer raises ``Unreachable``."""
    parts = urllib.parse.urlsplit(url)
    connection = http.client.HTTPConnection(parts.hostname or "", parts.port, timeout=timeout)
    try:
        connection.request(method, parts.path + (f"?{parts.query}" if parts.query else ""), json.dumps(body) if body is not None else None, dict(headers))
        reply = connection.getresponse()
        raw = reply.read()
        return reply.status, json.loads(raw) if raw else {}
    except (OSError, http.client.HTTPException, ValueError):
        raise Unreachable(parts.netloc) from None
    finally:
        connection.close()


def listing(marquez: str, path: str, key: str, timeout: float, limit_at: float) -> list[dict[str, Any]]:
    """Every row of a paged Marquez listing, read until a short page or the reported total. Passing ``limit_at`` (a monotonic time) raises ``Unreachable``."""
    rows: list[dict[str, Any]] = []
    separator = "&" if "?" in path else "?"
    for _ in range(MAX_PAGES):
        if time.monotonic() >= limit_at:
            raise Unreachable("the listing did not end before the deadline")
        status, page = call("GET", f"{marquez}{path}{separator}limit={PAGE}&offset={len(rows)}", None, {}, timeout)
        if status != 200:
            raise Unreachable(path)
        batch = page.get(key, [])
        rows.extend(batch)
        total = page.get("totalCount")
        if len(batch) < PAGE or (isinstance(total, int) and len(rows) >= total):
            return rows
    raise Unreachable("the listing did not end within the page limit")


def dataset_ids(marquez: str, timeout: float, limit_at: float) -> list[str]:
    """Every Marquez dataset as a lineage node id, ``dataset:<namespace>:<name>``, over all pages of namespaces and of datasets."""
    found: list[str] = []
    for namespace in listing(marquez, "/api/v1/namespaces", "namespaces", timeout, limit_at):
        name = namespace["name"]
        for item in listing(marquez, f"/api/v1/namespaces/{urllib.parse.quote(name, safe='')}/datasets", "datasets", timeout, limit_at):
            found.append(f"dataset:{name}:{item['name']}")
    return found


def job_edges(marquez: str, node: str, timeout: float) -> set[tuple[str, str]]:
    """The (input dataset, output dataset) pairs of every job next to one dataset in the lineage graph."""
    status, reply = call("GET", f"{marquez}/api/v1/lineage?nodeId={urllib.parse.quote(node, safe='')}&depth=1", None, {}, timeout)
    if status != 200:
        raise Unreachable("marquez lineage")
    pairs: set[tuple[str, str]] = set()
    for item in reply.get("graph", []):
        if item.get("type") != "JOB":
            continue
        inputs = [edge["origin"] for edge in item.get("inEdges", []) if str(edge["origin"]).startswith("dataset:")]
        outputs = [edge["destination"] for edge in item.get("outEdges", []) if str(edge["destination"]).startswith("dataset:")]
        pairs.update((source, target) for source in inputs for target in outputs)
    return pairs


def table_name(node: str, settings: Mapping[str, str]) -> str | None:
    """The OpenMetadata table name of a Marquez dataset node, or None when the dataset name does not match the pattern."""
    name = node.split(":", 2)[2]
    match = re.fullmatch(settings["dataset_pattern"], name)
    if match is None:
        return None
    return settings["fqn_template"].format(service=settings["service"], database=settings["database"], **match.groupdict())


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--marquez", required=True)
    parser.add_argument("--om-base", default="http://127.0.0.1:8585/api")
    parser.add_argument("--token-file", type=Path, default=Path("/run/om/ingestion_token"))
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--timeout-seconds", type=float, default=10)
    parser.add_argument("--deadline-seconds", type=float, default=300)
    options = parser.parse_args(argv)
    settings = json.loads(options.config.read_text())
    auth = {"Authorization": f"Bearer {options.token_file.read_text().strip()}", "Content-Type": "application/json"}
    try:
        pairs: set[tuple[str, str]] = set()
        limit_at = time.monotonic() + options.deadline_seconds
        for node in dataset_ids(options.marquez, options.timeout_seconds, limit_at):
            pairs |= job_edges(options.marquez, node, options.timeout_seconds)
    except Unreachable as error:
        sys.stderr.write(f"openmetadata_marquez: marquez did not answer ({error}) or its listing hit the deadline\n")
        return 1
    added = unmapped = absent = 0
    identifiers: dict[str, str | None] = {}

    def table_id(fqn: str) -> str | None:
        if fqn not in identifiers:
            status, reply = call("GET", f"{options.om_base}/v1/tables/name/{urllib.parse.quote(fqn, safe='')}", None, auth, options.timeout_seconds)
            identifiers[fqn] = reply.get("id") if status == 200 else None
        return identifiers[fqn]

    try:
        for source, target in sorted(pairs):
            names = (table_name(source, settings), table_name(target, settings))
            if None in names:
                unmapped += 1
                continue
            ids = [table_id(str(name)) for name in names]
            if None in ids:
                absent += 1
                continue
            edge = {"edge": {"fromEntity": {"id": ids[0], "type": "table"}, "toEntity": {"id": ids[1], "type": "table"}}}
            status, _ = call("PUT", f"{options.om_base}/v1/lineage", edge, auth, options.timeout_seconds)
            if status not in (200, 201):
                sys.stderr.write(f"openmetadata_marquez: OpenMetadata refused an edge with status {status}\n")
                return 1
            added += 1
    except Unreachable:
        sys.stderr.write("openmetadata_marquez: OpenMetadata did not answer\n")
        return 1
    sys.stdout.write(f"openmetadata_marquez: edges={added} unmapped={unmapped} table_missing={absent}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
