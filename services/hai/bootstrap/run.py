"""Verify staged job bytes and archives from an isolated, baked Python entry point."""

from __future__ import annotations

import argparse
import hashlib
import importlib
import importlib.util
import os
import sys
import time
from pathlib import Path
from typing import Any


def baked_package() -> Any:
    """Load the package only from this entry point's baked directory under python -I."""
    if not sys.flags.isolated:
        raise RuntimeError("isolated_python_required")
    directory = Path(__file__).resolve().parent
    spec = importlib.util.spec_from_file_location("hai_bootstrap", directory / "__init__.py", submodule_search_locations=[str(directory)])
    if spec is None or spec.loader is None:
        raise RuntimeError("baked_package_missing")
    package = importlib.util.module_from_spec(spec)
    sys.modules["hai_bootstrap"] = package
    spec.loader.exec_module(package)
    integrity = importlib.import_module("hai_bootstrap.integrity")
    importlib.import_module("hai_bootstrap.outputs")
    importlib.import_module("hai_bootstrap.redaction")
    importlib.import_module("hai_bootstrap.transfer")
    return integrity


def prepare(job_path: Path, expected_sha256: str, inputs: Path, workspace: Path, expected: dict[str, Any], deadline: float, integrity: Any) -> dict[str, Any]:
    """Verify inputs and immutable archive trees without executing a staged byte."""
    with integrity.held_file(job_path.parent, job_path.name) as descriptor:
        raw = os.pread(descriptor, integrity.MAX_RECORD + 1, 0)
    if len(raw) > integrity.MAX_RECORD or hashlib.sha256(raw).hexdigest() != expected_sha256:
        raise integrity.StageError("job_digest", 71)
    job = integrity.parse_json(raw)
    if not isinstance(job, dict) or any(job.get(key) != value for key, value in expected.items()):
        raise integrity.StageError("job_identity", 71)
    transfer = importlib.import_module("hai_bootstrap.transfer")
    references = [transfer.FileRef(**item) for item in job["inputs"]]
    logical_paths = [reference.path for reference in references]
    if len(logical_paths) != len(set(logical_paths)):
        raise integrity.StageError("duplicate_input")
    documents = [reference.document() for reference in sorted(references, key=lambda item: item.path)]
    if hashlib.sha256(integrity.canonical(documents)).hexdigest() != job["input_map_sha256"]:
        raise integrity.StageError("input_map_digest", 71)
    for reference in references:
        with integrity.held_file(inputs, reference.path) as descriptor:
            digest, size = integrity.digest_descriptor(descriptor, deadline)
        if (digest, size) != (reference.sha256, reference.size):
            raise integrity.StageError("input_digest", 71)
    if workspace.exists() or workspace.is_symlink():
        raise integrity.StageError("workspace_exists")
    workspace.mkdir(mode=0o700)
    archives = job.get("archives")
    if not isinstance(archives, dict) or set(archives) != {"code", "dbt"}:
        raise integrity.StageError("archive_contract")
    for label in ("code", "dbt"):
        archive = archives[label]
        members = tuple(integrity.Member(**item) for item in archive["members"])
        if archive["input_path"] not in logical_paths:
            raise integrity.StageError("archive_contract")
        tree = workspace / label
        integrity.extract_archive(inputs / archive["input_path"], members, tree, deadline)
        digest = integrity.code_digest(tree, members, deadline) if label == "code" else integrity.dbt_digest(tree, deadline)
        expected_digest = job["code_sha256"] if label == "code" else job["dbt_tree_sha256"]
        if digest != expected_digest:
            raise integrity.StageError("code_digest", 71)
        if label == "code":
            source = job["declaration_source"]
            by_name = {member.path: member for member in members}
            for path_key, sha_key in (("path", "sha256"), ("schema_path", "schema_sha256")):
                member = by_name.get(source[path_key])
                if member is None or member.sha256 != source[sha_key]:
                    raise integrity.StageError("declaration_source", 71)
        integrity.make_read_only(tree, deadline)
    return {
        "status": "verified_tree",
        "execution": "held_for_worker_and_identity_gates",
        "input_count": len(references),
        "code_sha256": job["code_sha256"],
        "dbt_tree_sha256": job["dbt_tree_sha256"],
    }


def main() -> int:
    """Check baked verification dependencies or prepare an externally pinned job tree."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--self-check", action="store_true")
    parser.add_argument("--job", type=Path)
    parser.add_argument("--job-sha256")
    parser.add_argument("--inputs", type=Path)
    parser.add_argument("--workspace", type=Path)
    parser.add_argument("--run-id")
    parser.add_argument("--generation", type=int)
    parser.add_argument("--attempt-id")
    parser.add_argument("--try-id")
    parser.add_argument("--template")
    parser.add_argument("--timeout-seconds", type=float, default=120.0)
    args = parser.parse_args()
    try:
        integrity = baked_package()
        if args.self_check:
            sys.stdout.write('{"status":"verified_dependencies","scope":"archive_verification_only"}\n')
            return 0
        if (
            any(
                value is None
                for value in (args.job, args.job_sha256, args.inputs, args.workspace, args.run_id, args.generation, args.attempt_id, args.try_id, args.template)
            )
            or args.timeout_seconds <= 0
        ):
            parser.error("a job digest, inputs, fresh workspace and every external identity are required")
        expected = {"run_id": args.run_id, "generation": args.generation, "attempt_id": args.attempt_id, "try_id": args.try_id, "template": args.template}
        result = prepare(args.job, args.job_sha256, args.inputs, args.workspace, expected, time.monotonic() + args.timeout_seconds, integrity)
        sys.stdout.write(integrity.canonical(result).decode() + "\n")
        return 0
    except (RuntimeError, ValueError, OSError, KeyError, TypeError, ImportError) as error:
        exit_code = getattr(error, "exit_code", 79)
        code = getattr(error, "code", "bootstrap_verification_failed")
        sys.stderr.write('{"error":"' + code + '"}\n')
        return int(exit_code)


if __name__ == "__main__":
    sys.exit(main())
