"""Shared set-up of a Polaris table writer principal: ``gold_writer_setup`` and ``silver_writer_setup`` hold only their names, namespaces and secret names.

A writer may write the tables of its own namespaces and nothing else: no catalog-wide management, no namespace creation or removal and no table
removal. The root credentials make the token and the set-up requests only; they are never passed on. The writer's client ID and secret go straight
to Parameter Store, the same way the read-only principal's do (``principal_credentials``), and reach a job only through the launcher's
``lakehouse_write`` class, which reads them from Parameter Store into memory.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from scripts.infrastructure import secret_store
from scripts.lakehouse import principal_credentials

CATALOG_PRIVILEGES = ("CATALOG_READ_PROPERTIES",)
NAMESPACE_PRIVILEGES = (
    "NAMESPACE_LIST",
    "NAMESPACE_READ_PROPERTIES",
    "TABLE_LIST",
    "TABLE_CREATE",
    "TABLE_READ_PROPERTIES",
    "TABLE_WRITE_PROPERTIES",
    "TABLE_READ_DATA",
    "TABLE_WRITE_DATA",
)
FORBIDDEN_PRIVILEGES = ("CATALOG_MANAGE_CONTENT", "CATALOG_MANAGE_ACCESS", "NAMESPACE_CREATE", "NAMESPACE_DROP", "TABLE_DROP", "TABLE_FULL_METADATA")

Request = Callable[..., tuple[int, Any]]


@dataclass(frozen=True)
class Writer:
    """One writer principal: its name, the namespaces it may write and the Parameter Store names that hold its client ID and secret."""

    name: str
    namespaces: tuple[str, ...]
    secrets: tuple[str, str]


class SetupError(RuntimeError):
    """A set-up request failed; the message names the action and the HTTP status, never a credential."""


def check(status: int, action: str, allowed: tuple[int, ...] = (200, 201, 204)) -> None:
    if status not in allowed:
        raise SetupError(f"{action} failed (HTTP {status})")


def catalog_paths(writer: Writer, catalog: str) -> dict[str, str]:
    return {
        "roles": "/api/management/v1/principal-roles",
        "catalog_roles": f"/api/management/v1/catalogs/{catalog}/catalog-roles",
        "grants": f"/api/management/v1/catalogs/{catalog}/catalog-roles/{writer.name}/grants",
        "assign_catalog_role": f"/api/management/v1/principal-roles/{writer.name}/catalog-roles/{catalog}",
        "principals": "/api/management/v1/principals",
        "principal": f"/api/management/v1/principals/{writer.name}",
        "reset": f"/api/management/v1/principals/{writer.name}/reset",
        "principal_roles": f"/api/management/v1/principals/{writer.name}/principal-roles",
        "namespaces": f"/api/catalog/v1/{catalog}/namespaces",
    }


def grants(writer: Writer) -> list[dict[str, Any]]:
    """Every grant of the writer's catalog role: catalog read, and the table privileges on each of its namespaces."""
    found: list[dict[str, Any]] = [{"grant": {"type": "catalog", "privilege": privilege}} for privilege in CATALOG_PRIVILEGES]
    for namespace in writer.namespaces:
        found += [{"grant": {"type": "namespace", "namespace": [namespace], "privilege": privilege}} for privilege in NAMESPACE_PRIVILEGES]
    return found


def planned_requests(writer: Writer, catalog: str) -> list[tuple[str, str, dict[str, Any] | None]]:
    """The requests of a first run, in order, without any credential."""
    path = catalog_paths(writer, catalog)
    steps: list[tuple[str, str, dict[str, Any] | None]] = [
        ("POST", path["roles"], {"principalRole": {"name": writer.name}}),
        ("POST", path["catalog_roles"], {"catalogRole": {"name": writer.name}}),
    ]
    steps += [("POST", path["namespaces"], {"namespace": [namespace], "properties": {}}) for namespace in writer.namespaces]
    steps += [("PUT", path["grants"], grant) for grant in grants(writer)]
    steps += [
        ("PUT", path["assign_catalog_role"], {"catalogRole": {"name": writer.name}}),
        ("GET", path["principal"], None),
        ("POST", path["principals"], {"principal": {"name": writer.name}, "credentialRotationRequired": False}),
        ("PUT", path["principal_roles"], {"principalRole": {"name": writer.name}}),
    ]
    return steps


def ensure_writer(writer: Writer, access: str, catalog: str, vault: secret_store.SecretStore, request: Request, lock_path: Path) -> str:
    """Create the writer principal, its roles, its namespaces and the grants; safe to repeat. Returns how the credentials were obtained.

    ``vault`` holds the writer's client ID and secret in Parameter Store. A stored pair is kept only when Polaris accepts it; otherwise a new pair
    is issued and stored. ``lock_path`` serializes updates of the writer principal.
    """
    path = catalog_paths(writer, catalog)
    status, _ = request("POST", path["roles"], access, body={"principalRole": {"name": writer.name}})
    check(status, "creating the writer principal role", (200, 201, 409))
    status, _ = request("POST", path["catalog_roles"], access, body={"catalogRole": {"name": writer.name}})
    check(status, "creating the writer catalog role", (200, 201, 409))
    for namespace in writer.namespaces:
        status, _ = request("POST", path["namespaces"], access, body={"namespace": [namespace], "properties": {}})
        check(status, f"creating namespace {namespace}", (200, 201, 409))
    for grant in grants(writer):
        status, _ = request("PUT", path["grants"], access, body=grant)
        check(status, f"granting {grant['grant']['privilege']} to the writer")
    status, _ = request("PUT", path["assign_catalog_role"], access, body={"catalogRole": {"name": writer.name}})
    check(status, "assigning the writer catalog role")
    outcome = principal_credentials.ensure_credentials(
        principal=writer.name, names=writer.secrets, vault=vault, request=request, access=access, check=check, lock_path=lock_path
    )
    status, _ = request("PUT", path["principal_roles"], access, body={"principalRole": {"name": writer.name}})
    check(status, "assigning the writer principal role")
    return outcome


def main(writer: Writer, description: str, argv: list[str] | None = None) -> int:
    """Print the plan (``--dry-run``) or run the set-up against the local Polaris, keeping the writer's pair in Parameter Store."""
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--dry-run", action="store_true", help="print the requests and send none")
    args = parser.parse_args(sys.argv[1:] if argv is None else argv)
    from scripts.lakehouse import catalog

    if args.dry_run:
        for method, path, body in planned_requests(writer, catalog.CATALOG):
            sys.stdout.write(f"{method} {path} {json.dumps(body, sort_keys=True) if body else ''}".rstrip() + "\n")
        return 0
    try:
        access = catalog.token(catalog.credentials())
        outcome = ensure_writer(writer, access, catalog.CATALOG, catalog.vault(), catalog.request, catalog.STATE / "principal_locks" / f"{writer.name}.lock")
    except (SetupError, catalog.CatalogError, secret_store.SecretStoreError, principal_credentials.CredentialError, OSError, KeyError) as error:
        sys.stderr.write(f"{writer.name.replace('_', ' ')} setup: {error}\n")
        return 1
    sys.stdout.write(f"{writer.name} principal {outcome}; it may write the {' and '.join(writer.namespaces)} namespaces only.\n")
    return 0
