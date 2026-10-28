"""The consumer of the sealed copy for the gold publish.

The export job hashes the mounted sealed copy inside its container (the quality image, network none) and writes the consumer result with that digest. It exports
the 69 gold tables to Parquet files and a manifest only when the digest equals the expected one. The launcher accepts the result by ``accept_consumer`` of
``scripts/lakehouse/gold_publish.py`` and records the digest of that exact result; no later step runs against another result.

    python -m scripts.lakehouse.gold_export --run <run ID> --sealed-sha256 <digest of the sealed copy>

The run's publish folder is ``/workspace/out/publish`` and the sealed copy ``/workspace/sealed/sealed.duckdb``. The result holds names, counts and hashes only.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from scripts.lakehouse.gold_publish import DEFAULT_PUBLISH_DIR, SCHEMA, Plan, export_files_match, ident, load_plan, remove_tree, sha256_file, write_record
from scripts.lakehouse.gold_publish_record import RESULT_NAME, RUN_ID, SHA256, PublishError, read_record

DEFAULT_SEALED = "/workspace/sealed/sealed.duckdb"


DUCKDB_COLUMNS = "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'main' AND table_name = {name} ORDER BY ordinal_position"
DUCKDB_ROWS = "SELECT count(*) FROM main.{table}"
DUCKDB_DISTINCT = "SELECT count(DISTINCT struct_pack({pairs})) FROM main.{table}"
DUCKDB_COPY = "COPY (SELECT * FROM main.{table}) TO {path} (FORMAT PARQUET)"


def duck_ident(name: str) -> str:
    return '"' + ident(name) + '"'


def duck_literal(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def export_sealed(sealed: Path, expected_sha256: str, publish_dir: Path, plan: Plan, run_id: str) -> dict[str, Any]:
    """The consumer. Hash the mounted file, write the consumer result with that digest and, only when it equals the expected digest, export.

    A digest that differs writes the result with ``exported`` false and no export, then raises: the launcher side refuses the result
    (``accept_consumer``) and no later step has an export to read. Returns the consumer result.
    """
    if not RUN_ID.fullmatch(run_id) or not SHA256.fullmatch(expected_sha256):
        raise PublishError("export needs a run ID and a lower-case hexadecimal SHA-256")
    in_container = sha256_file(sealed)
    result: dict[str, Any] = {
        "schema": SCHEMA,
        "run_id": run_id,
        "expected_sha256": expected_sha256,
        "in_container_sha256": in_container,
        "exported": False,
        "manifest_sha256": None,
    }
    result_path = publish_dir / RESULT_NAME
    export_dir = publish_dir / "export"
    if in_container != expected_sha256:
        write_record(result_path, result)
        raise PublishError("the sealed copy mounted in the container differs from the sealed build: nothing is exported")
    if (export_dir / "manifest.json").is_file():
        manifest, manifest_sha = read_record(export_dir / "manifest.json")
        if manifest.get("sealed_sha256") == expected_sha256 and manifest.get("run_id") == run_id and export_files_match(export_dir, manifest):
            result.update({"exported": True, "manifest_sha256": manifest_sha, "reused": True})
            write_record(result_path, result)
            return result
    import duckdb

    staging = publish_dir / "export.partial"
    remove_tree(staging)
    staging.mkdir(parents=True)
    connection = duckdb.connect(str(sealed), read_only=True)
    try:
        tables: dict[str, Any] = {}
        for name, key in plan.tables.items():
            columns = [(str(row[0]), str(row[1])) for row in connection.execute(DUCKDB_COLUMNS.format(name=duck_literal(name))).fetchall()]
            if not columns:
                raise PublishError(f"the sealed copy has no table main.{name}")
            missing = [column for column in key if column not in {c for c, _ in columns}]
            if missing:
                raise PublishError(f"{name}: the merge key column {missing[0]} is not in the sealed table")
            rows = int(connection.execute(DUCKDB_ROWS.format(table=duck_ident(name))).fetchall()[0][0])
            pairs = ", ".join(f"{duck_ident(column)} := {duck_ident(column)}" for column in key)
            distinct = int(connection.execute(DUCKDB_DISTINCT.format(pairs=pairs, table=duck_ident(name))).fetchall()[0][0])
            if distinct != rows:
                raise PublishError(f"{name}: {rows - distinct} rows repeat the merge key {list(key)}")
            file = staging / f"{name}.parquet"
            connection.execute(DUCKDB_COPY.format(table=duck_ident(name), path=duck_literal(str(file))))
            tables[name] = {
                "rows": rows,
                "columns": [[column, kind] for column, kind in columns],
                "merge_key": list(key),
                "file": file.name,
                "file_sha256": sha256_file(file),
                "bytes": file.stat().st_size,
            }
    finally:
        connection.close()
    manifest = {"schema": SCHEMA, "run_id": run_id, "sealed_sha256": expected_sha256, "in_container_sha256": in_container, "tables": tables}
    manifest_sha = write_record(staging / "manifest.json", manifest)
    remove_tree(export_dir)
    os.replace(staging, export_dir)
    result.update({"exported": True, "manifest_sha256": manifest_sha})
    write_record(result_path, result)
    return result


def main(argv: Sequence[str] | None = None) -> int:
    """Run the export. Prints one JSON line with the digest outcome and the manifest hash; a refusal prints its reason to standard error and exits 1."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--run", required=True)
    parser.add_argument("--sealed-sha256", required=True)
    parser.add_argument("--publish-dir", default=DEFAULT_PUBLISH_DIR)
    parser.add_argument("--sealed", default=DEFAULT_SEALED)
    args = parser.parse_args(list(sys.argv[1:] if argv is None else argv))
    try:
        result = export_sealed(Path(args.sealed), args.sealed_sha256, Path(args.publish_dir), load_plan(), args.run)
    except PublishError as error:
        sys.stderr.write(f"gold export: {error}\n")
        return 1
    sys.stdout.write(json.dumps({"step": "export", "exported": result["exported"], "manifest_sha256": result["manifest_sha256"]}) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
