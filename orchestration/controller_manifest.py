"""Validate explicit controller record manifests without archiving checkout or dataset trees."""

from __future__ import annotations

import copy
import hashlib
import json
import os
import re
import stat
import time
from collections.abc import Iterable, Mapping
from pathlib import Path, PurePosixPath
from typing import Any

Document = dict[str, Any]
ROLES = {
    "registry_identity": r"registry/registry_id",
    "registry": r"registry/registry\.json",
    "registry_archive": r"registry/history/[A-Za-z0-9_.-]+\.(?:json|jsonl)",
    "ledger": r"ledger/[A-Za-z0-9_.-]+\.jsonl",
    "dispatch_marker": r"ledger/[A-Za-z0-9_.-]+\.sent",
    "owner_intent": r"registry/controller_commands/[A-Za-z0-9_.-]+\.json",
    "broker_record": r"records/(?:[A-Za-z0-9_.-]+/)?[A-Za-z0-9_.-]+\.json",
    "forward": r"forwards/(?:[A-Za-z0-9_.-]+/)?[A-Za-z0-9_.-]+\.json",
    "journal": r"(?:catalog_journal|dispatch|lakehouse/catalog_journal)/(?:[A-Za-z0-9_.-]+/)*[A-Za-z0-9_.-]+\.json",
    "worker_history": r"(?:registry/workers|worker_history/[A-Za-z0-9_.-]+)\.jsonl",
    "provider_envelope": r"airflow/lineage_provider/(?:[A-Za-z0-9_.-]+/)*[A-Za-z0-9_.-]+\.json",
    "lineage_outbox": r"lineage/outbox\.sqlite",
    "sanitized_log": r"(?:(?:registry/launcher|broker/broker)|(?:job_logs|airflow/logs)/(?:[A-Za-z0-9_.=:@+-]+/)*[A-Za-z0-9_.=:@+-]+)\.log",
    "reviewed_config": r"config/(?:controller|ecs_target|lineage)\.json",
}
CLASSIFICATIONS = {"Internal", "Confidential", "Restricted", "Synthetic"}
RECORD_FIELDS = {"role", "classification", "source_root", "relative_path", "size", "sha256"}


class TransferError(RuntimeError):
    """A stable, sanitized refusal that leaves controller admission closed."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


def encoded(value: object) -> bytes:
    """Serialize metadata deterministically without recording credentials or database rows."""
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode()


def digest(value: object) -> str:
    """Name an immutable manifest or conversion plan by its exact canonical bytes."""
    return hashlib.sha256(encoded(value)).hexdigest()


def sha256(value: object) -> str:
    if not isinstance(value, str) or re.fullmatch(r"[a-f0-9]{64}", value) is None:
        raise TransferError("manifest_schema")
    return value


def identifier(value: object) -> str:
    if not isinstance(value, str) or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", value) is None or value in (".", ".."):
        raise TransferError("manifest_schema")
    return value


def allowed_member(role: object, relative_path: object) -> str:
    """Refuse excluded names before opening a member, even if its declared role is trusted."""
    if not isinstance(relative_path, str) or not relative_path or "\\" in relative_path:
        raise TransferError("excluded_member")
    path = PurePosixPath(relative_path)
    parts = tuple(part.lower() for part in path.parts)
    name = path.name.lower()
    if (
        path.is_absolute()
        or path.as_posix() != relative_path
        or any(part in (".", "..", ".env", ".aws", "secrets", "raw", "downloads", "copies", "runs", "sealed", "postgres") for part in parts)
        or any(
            name.endswith(suffix)
            for suffix in (".duckdb", ".duckdb.wal", ".tfstate", ".tfstate.backup", ".tfvars", ".tfvars.json", ".sock", ".lock", ".pem", ".key")
        )
        or "tfstate" in name
        or name.startswith((".env.", "token", "broker_token", "launcher_key", "credential"))
        or relative_path == "config/acquisition/source_registry.json"
        or any("hcai" in part and "financial" in part or "cms" in part and "contact" in part for part in parts)
        or not isinstance(role, str)
        or role not in ROLES
        or re.fullmatch(ROLES[role], relative_path) is None
    ):
        raise TransferError("excluded_member")
    return relative_path


def safe_root(root: Path) -> None:
    """Reject a symlink in the root or any of its existing parent components."""
    if not root.is_absolute():
        raise TransferError("manifest_root")
    for part in (root, *root.parents):
        if part.is_symlink():
            raise TransferError("symlink_member")


def member_path(root: Path, relative_path: str) -> Path:
    safe_root(root)
    candidate = root
    for part in PurePosixPath(relative_path).parts:
        candidate /= part
        if candidate.is_symlink():
            raise TransferError("symlink_member")
    return candidate


def file_hash(path: Path, *, deadline: float | None = None) -> tuple[int, str]:
    value = hashlib.sha256()
    size = 0
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            if deadline is not None and time.monotonic() >= deadline:
                raise TransferError("transfer_deadline")
            value.update(chunk)
            size += len(chunk)
    return size, value.hexdigest()


def validate_manifest(value: object) -> Document:
    """Validate the closed manifest schema and every positive role allowlist entry."""
    if (
        not isinstance(value, dict)
        or set(value) != {"version", "transfer_id", "source_machine", "records"}
        or type(value["version"]) is not int
        or value["version"] != 1
    ):
        raise TransferError("manifest_schema")
    identifier(value["transfer_id"])
    identifier(value["source_machine"])
    if not isinstance(value["records"], list) or not value["records"]:
        raise TransferError("manifest_schema")
    names: set[str] = set()
    roots: set[str] = set()
    for record in value["records"]:
        if not isinstance(record, dict) or set(record) != RECORD_FIELDS:
            raise TransferError("manifest_schema")
        name = allowed_member(record["role"], record["relative_path"])
        if name in names or record["classification"] not in CLASSIFICATIONS or type(record["size"]) is not int or record["size"] < 0:
            raise TransferError("manifest_schema")
        if not isinstance(record["source_root"], str) or not Path(record["source_root"]).is_absolute():
            raise TransferError("manifest_root")
        sha256(record["sha256"])
        names.add(name)
        roots.add(record["source_root"])
    if len(roots) != 1 or not {"registry/registry_id", "registry/registry.json"}.issubset(names):
        raise TransferError("manifest_schema")
    return copy.deepcopy(value)


def tree_members(root: Path) -> set[str]:
    """Inspect names and file types without following links or opening unexpected files."""
    safe_root(root)
    if not root.is_dir():
        raise TransferError("manifest_root")
    found: set[str] = set()
    for directory, folders, files in os.walk(root, followlinks=False):
        for name in (*folders, *files):
            path = Path(directory) / name
            mode = path.lstat().st_mode
            if stat.S_ISLNK(mode):
                raise TransferError("symlink_member")
            if stat.S_ISREG(mode):
                found.add(path.relative_to(root).as_posix())
            elif not stat.S_ISDIR(mode):
                raise TransferError("excluded_member")
    return found


def verify_tree(manifest: Mapping[str, Any], root: Path, *, side: str = "source", deadline: float | None = None) -> None:
    """Require exact membership, sizes and hashes in both transfer directions."""
    checked = validate_manifest(dict(manifest))
    expected = {record["relative_path"]: record for record in checked["records"]}
    if tree_members(root) != set(expected):
        raise TransferError(f"{side}_membership")
    for name, record in expected.items():
        if file_hash(member_path(root, name), deadline=deadline) != (record["size"], record["sha256"]):
            raise TransferError("member_digest")


def build_manifest(root: Path, *, transfer_id: str, source_machine: str, declarations: Iterable[Mapping[str, str]]) -> Document:
    """Freeze an explicit declaration list; never discover records by recursively archiving a checkout."""
    declarations = list(declarations)
    for declaration in declarations:
        if set(declaration) != {"relative_path", "role", "classification"}:
            raise TransferError("manifest_schema")
        allowed_member(declaration["role"], declaration["relative_path"])
    records = []
    for declaration in sorted(declarations, key=lambda item: item["relative_path"]):
        path = member_path(root, declaration["relative_path"])
        if not path.is_file():
            raise TransferError("source_membership")
        size, hash_value = file_hash(path)
        records.append({**declaration, "source_root": str(root), "size": size, "sha256": hash_value})
    result = validate_manifest({"version": 1, "transfer_id": transfer_id, "source_machine": source_machine, "records": records})
    verify_tree(result, root)
    return result
