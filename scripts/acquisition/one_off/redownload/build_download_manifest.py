"""Write the manifest of every file runs 1 and 2 downloaded, so a file left out of a later run can be found and added back.

Runs 1 and 2 share one frozen queue (data/redownload_checks/20260929 and 20261001, byte for byte the same). Each queue entry names one
stored capture by its receipt. Every artifact of that receipt is one downloaded file. For each file the manifest keeps file metadata
only: source, dataset, address, retrieval time, SHA-256, size, the stored objects (key and version, never the bucket name), the bronze
tables that select them (config/lakehouse/bronze_tables.json) and whether a dbt model in silver, gold or features reads that table.

Inputs are local records (not in the repository): the run 2 queue and its lock, each capture's receipt.json and
s3_collections_reconciliation.json. The dbt models and the bronze table map come from one named commit, unpacked with
``git archive <commit> dbt config/lakehouse/bronze_tables.json``. dbt is not installed on the host, so the usage is read from the model
text: a model reads a bronze table it names in ``canonical_rows``, ``source`` or a quoted argument, and depends on every model it names
in quotes, including the ACS staging models its ``acs_tables()`` loop references. No network or AWS call is made, and a rerun with the
same inputs writes the same bytes.

Usage (repository root):
    git archive <commit> dbt config/lakehouse/bronze_tables.json | tar -x -C <new folder>
    python3 -m scripts.acquisition.one_off.redownload.build_download_manifest --data-root <checkout holding data/> \
        --dbt-tree <new folder> --dbt-commit <commit> --output config/acquisition/downloaded_files_runs_1_2.json
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from collections import Counter
from pathlib import Path

from scripts.acquisition.data_paths import current

ROOT = Path(__file__).resolve().parents[4]

RUN1 = "data/redownload_checks/20260929/queue.json"
RUN2 = "data/redownload_checks/20261001/queue.json"
# A capture moved from data/<folder> to data/datasets/<folder> before the moves record existed for some folders.
LEGACY_PREFIX = ("data/", "data/datasets/")


def read(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


METADATA_SOURCES = {"column_map", "file_metadata", "file_preambles", "stored_copies"}


def dbt_usage(tree: Path, commit: str) -> dict:
    """Each bronze table a dbt model reads, its readers and the gold or features models their refs reach."""
    models = {path.stem: (path.relative_to(tree / "dbt/models").parts[0], path.read_text(encoding="utf-8")) for path in (tree / "dbt/models").rglob("*.sql")}
    macros = "\n".join(path.read_text(encoding="utf-8") for path in sorted((tree / "dbt/macros").rglob("*.sql")))
    found = re.search(r"macro acs_tables\(\).*?endmacro", macros, re.S)
    acs = re.findall(r"'([a-z0-9_]+)'", found.group(0)) if found else []
    sources = (tree / "dbt/models/silver/staging/_bronze__sources.yml").read_text(encoding="utf-8")
    bronze = set(re.findall(r"^      - name: ([a-z0-9_]+)$", sources, re.M)) - METADATA_SOURCES
    depends: dict[str, set[str]] = {}
    reads: dict[str, set[str]] = {}
    for name, (_, text) in models.items():
        body = re.sub(r"\{#.*?#\}", "", re.sub(r"--[^\n]*", "", text), flags=re.S)
        quoted = set(re.findall(r"['\"]([a-z0-9_]+)['\"]", body))
        named = quoted & set(models)
        read_tables = (quoted & bronze) | set(re.findall(r"canonical_rows\(\s*['\"]([a-z0-9_]+)['\"]", body))
        if "acs_tables()" in body:
            read_tables |= set(acs)
            if "ref('stg_' ~ table)" in body:
                named |= {f"stg_{table}" for table in acs}
        depends[name], reads[name] = named - {name}, read_tables
    children: dict[str, set[str]] = {}
    for name, named in depends.items():
        for parent in named:
            children.setdefault(parent, set()).add(name)

    def published(start: str) -> set[str]:
        reached, stack, seen = set(), [start], {start}
        while stack:
            name = stack.pop()
            if models[name][0] in {"gold", "features"}:
                reached.add(name)
            for child in children.get(name, set()) - seen:
                seen.add(child)
                stack.append(child)
        return reached

    tables: dict[str, dict[str, set[str]]] = {}
    for name in models:
        for table in reads[name]:
            entry = tables.setdefault(table, {"readers": set(), "published_models": set()})
            entry["readers"].add(name)
            entry["published_models"] |= published(name)
    return {"origin_main": commit, "tables": {table: {key: sorted(value) for key, value in entry.items()} for table, entry in sorted(tables.items())}}


def receipt_path(data_root: Path, recorded: str) -> Path | None:
    """Where the recorded receipt lives now, or None when no candidate exists."""
    for candidate in (current(recorded), recorded.replace(*LEGACY_PREFIX, 1)):
        if (data_root / candidate).is_file():
            return data_root / candidate
    return None


def tables_for(key: str, member: str | None, snapshot_id: str, tables: list[dict]) -> list[str]:
    """The bronze tables whose collection and selector take this stored object, as scripts.lakehouse.bronze.select decides."""
    parts = key.split("/")
    if len(parts) < 5 or parts[2] not in {"datasets", "references"}:
        return []
    # A reference document's key has no dataset folder: references/capture_id=<snapshot>/<sha>/<name>.
    collection, dataset_id = f"{parts[0]}/{parts[1]}", parts[3] if parts[2] == "datasets" else ""
    names = {parts[-1]} | ({member, member.rsplit("/", 1)[-1]} if member else set())
    found = []
    for table in tables:
        if table.get("collection") != collection:
            continue
        if "snapshot_pattern" in table and re.search(table["snapshot_pattern"], snapshot_id) is None:
            continue
        if "dataset_ids" in table:
            if dataset_id in table["dataset_ids"]:
                found.append(table["table"])
        elif any(re.search(table["member_pattern"], name) for name in names):
            found.append(table["table"])
    return sorted(set(found))


def dictionary_tables(tables: list[dict]) -> dict[str, list[str]]:
    """Each publisher dictionary table and the data tables it documents."""
    documented: dict[str, list[str]] = {}
    for table in tables:
        dictionary = table.get("dictionary")
        if isinstance(dictionary, dict) and dictionary.get("table"):
            documented.setdefault(dictionary["table"], []).append(table["table"])
    return {name: sorted(set(found)) for name, found in documented.items()}


def build(data_root: Path, dbt_tree: Path, dbt_commit: str) -> tuple[dict, list[str]]:
    run1, run2 = (data_root / RUN1).read_bytes(), (data_root / RUN2).read_bytes()
    if run1 != run2:
        raise SystemExit("The run 1 and run 2 queues differ; the manifest assumes one shared queue.")
    lock = read((data_root / RUN2).with_name("queue.lock.json"))
    if hashlib.sha256(run2).hexdigest() != lock["queue_sha256"]:
        raise SystemExit("The run 2 queue differs from its lock.")
    tables = read(dbt_tree / "config/lakehouse/bronze_tables.json")["tables"]
    usage = dbt_usage(dbt_tree, dbt_commit)
    # Every dbt model is published to the lakehouse, staging views included, so a table any model reads is used.
    used = {name for name, found in usage["tables"].items() if found["readers"]}
    read_by_dbt = set(usage["tables"])
    documents = dictionary_tables(tables)
    files: list[dict] = []
    unaccounted: list[str] = []
    for unit in json.loads(run2)["queue"]:
        path = receipt_path(data_root, unit["receipt"])
        if path is None:
            unaccounted.append(f"{unit['snapshot_id']}: receipt not found")
            continue
        receipt = read(path)
        record = path.parent / "s3_collections_reconciliation.json"
        stored = read(record)["objects"] if record.is_file() else []
        acquisition = receipt["acquisition"]
        lineage = json.loads(receipt["lineage"]["extraction_or_query"])
        requests = lineage.get("requests") or []
        # An API capture stores the dataset it built from its pages (observations.csv and the like), not the pages one to one, so a capture is
        # used when any object it stored is.
        capture_tables = sorted(
            {name for item in stored for name in tables_for(item["object"]["key"], item.get("archive_member"), unit["snapshot_id"], tables)}
        )
        capture_documented = {name for table in capture_tables for name in documents.get(table, [])}
        capture_used = bool(set(capture_tables) & used) or bool(capture_documented & used)
        for index, artifact in enumerate(receipt["artifacts"]):
            request = requests[index] if len(requests) == len(receipt["artifacts"]) else {}
            objects = sorted(
                (
                    {
                        "key": item["object"]["key"],
                        "version_id": item["object"].get("version_id"),
                        "sha256": item["object"]["sha256"],
                        "bytes": item["object"]["byte_count"],
                        **({"archive_member": item["archive_member"]} if "archive_member" in item else {}),
                    }
                    for item in stored
                    # A stored object is the artifact itself or, for an archive, one of its members, which names the archive's digest.
                    if item.get("storage_path") == artifact["storage_path"]
                    or ("parent_sha256" in item and item["parent_sha256"] == artifact["sha256"])
                    or ("parent_sha256" not in item and item.get("artifact_id") == artifact.get("artifact_id"))
                ),
                key=lambda item: (item["key"], item.get("archive_member", "")),
            )
            selected = sorted({name for item in objects for name in tables_for(item["key"], item.get("archive_member"), unit["snapshot_id"], tables)})
            documented = sorted({name for table in selected for name in documents.get(table, [])})
            files.append(
                {
                    "source_id": unit["source_id"],
                    "snapshot_id": unit["snapshot_id"],
                    "disposition": unit["disposition"],
                    "mode": unit["mode"],
                    "role": artifact["role"],
                    "dataset_ids": sorted({item["key"].split("/")[3] for item in objects if len(item["key"].split("/")) > 3}),
                    "file_name": artifact["stored_file_name"],
                    "url": request.get("requested_url") or acquisition.get("requested_url") or unit["url"],
                    "retrieved_at_utc": request.get("retrieved_at_utc") or acquisition.get("retrieved_at_utc"),
                    "sha256": artifact["sha256"],
                    "bytes": artifact["byte_count"],
                    "stored_objects": objects,
                    "bronze_tables": selected,
                    "documents_tables": documented,
                    "dbt_reads_table": bool(set(selected) & read_by_dbt),
                    "used_downstream": bool(set(selected) & used) or bool(set(documented) & used),
                    "capture_bronze_tables": capture_tables,
                    "capture_used_downstream": capture_used,
                }
            )
            if not objects:
                unaccounted.append(f"{unit['snapshot_id']} {artifact['storage_path']}: no stored object recorded")
    files.sort(key=lambda item: (item["source_id"], item["snapshot_id"], item["file_name"], item["sha256"]))
    document = {
        "kind": "downloaded_files_runs_1_2",
        "version": 1,
        "queue_sha256": lock["queue_sha256"],
        "dbt_main_commit": usage["origin_main"],
        "used_downstream_rule": (
            "a bronze table that selects the file, or that the file documents as a publisher dictionary, "
            "is read by a dbt model in silver, gold or features; every model, staging views included, is published to the lakehouse"
        ),
        "counts": {
            "queue_entries": len(json.loads(run2)["queue"]),
            "files": len(files),
            "files_used_downstream": sum(item["used_downstream"] for item in files),
            "by_source": dict(sorted(Counter(item["source_id"] for item in files).items())),
        },
        "files": files,
    }
    return document, unaccounted


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--data-root", type=Path, default=ROOT)
    parser.add_argument("--dbt-tree", type=Path, required=True, help="a folder holding dbt/ and config/lakehouse/bronze_tables.json of one commit")
    parser.add_argument("--dbt-commit", required=True, help="the commit the tree was unpacked from, recorded in the manifest")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    document, unaccounted = build(args.data_root, args.dbt_tree, args.dbt_commit)
    body = json.dumps(document, indent=1, sort_keys=True).encode() + b"\n"
    if args.output.exists() and args.output.read_bytes() != body:
        raise SystemExit(f"{args.output} exists with other content; refusing to replace it.")
    args.output.write_bytes(body)
    sys.stdout.write(json.dumps({"files": len(document["files"]), "unaccounted": len(unaccounted)}) + "\n")
    for line in unaccounted:
        sys.stderr.write(line + "\n")


if __name__ == "__main__":
    main()
