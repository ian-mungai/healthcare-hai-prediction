"""Validate controller manifests or execute the approved transfer through bounded task adapters."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from orchestration.controller_manifest import Document, TransferError, digest, validate_manifest, verify_tree
from orchestration.controller_transfer import LogicalDatabases, ObjectStore, Transfer


def execute_transfer(
    *,
    manifest: Mapping[str, Any],
    source: Path,
    destination: Path,
    receipt_path: Path,
    store: ObjectStore,
    databases: LogicalDatabases,
    database_specs: list[Document],
    import_options: Mapping[str, Any],
    privacy: Mapping[str, Any],
    deadline: float,
) -> Document:
    """Run one frozen transfer and return its persisted closed-admission receipt.

    Only the separately approved transfer task supplies object and database ports. The ordinary controller receives neither.
    This function performs no IAM mutation, service activation or owner authority transition.
    """
    adapter = Transfer(
        manifest=manifest,
        source=source,
        destination=destination,
        receipt_path=receipt_path,
        store=store,
        databases=databases,
        database_specs=database_specs,
        import_options=import_options,
        privacy=privacy,
    )
    return adapter.run(deadline=deadline)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    check = commands.add_parser("verify-manifest", help="check the exact frozen source without uploading")
    check.add_argument("--manifest", type=Path, required=True)
    check.add_argument("--source", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        manifest = validate_manifest(json.loads(args.manifest.read_bytes()))
        verify_tree(manifest, args.source)
    except (TransferError, OSError, ValueError) as error:
        code = error.code if isinstance(error, TransferError) else "manifest_io"
        sys.stderr.write(code + "\n")
        return 1
    sys.stdout.write(json.dumps({"verified": True, "manifest_sha256": digest(manifest)}) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
