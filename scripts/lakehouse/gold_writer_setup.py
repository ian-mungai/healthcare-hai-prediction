"""Set up the ``gold_writer`` principal on the local Polaris catalog.

Run ``.venv/bin/python -m scripts.lakehouse.gold_writer_setup`` with the catalog up (``scripts.lakehouse.catalog up``). ``--dry-run`` prints the
requests it would send and sends none. The principal may write the tables of the ``gold`` and ``gold_e2e`` namespaces and nothing else: no
catalog-wide management, no namespace creation or removal and no table removal. The root credentials make the token and the set-up requests
only; they are never passed on. The writer's client ID and secret go to the owner-only catalog credentials file, the same way the read-only
principal's do, and reach a job only through the launcher's ``lakehouse_write`` class (``POLARIS_GOLD_WRITER_CLIENT_ID`` and ``_SECRET`` in the
compose environment file the launcher reads on the host).

The exact privilege list is a phase 2 finding: the write spike on ``gold_e2e`` proves it against Polaris before the first real publish.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable
from typing import Any

WRITER = "gold_writer"
NAMESPACES = ("gold", "gold_e2e")
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

WRITER_KEYS = ("POLARIS_GOLD_WRITER_CLIENT_ID", "POLARIS_GOLD_WRITER_CLIENT_SECRET")
Request = Callable[..., tuple[int, Any]]


class SetupError(RuntimeError):
    """A set-up request failed; the message names the action and the HTTP status, never a credential."""


def check(status: int, action: str, allowed: tuple[int, ...] = (200, 201, 204)) -> None:
    if status not in allowed:
        raise SetupError(f"{action} failed (HTTP {status})")


def catalog_paths(catalog: str) -> dict[str, str]:
    return {
        "roles": "/api/management/v1/principal-roles",
        "catalog_roles": f"/api/management/v1/catalogs/{catalog}/catalog-roles",
        "grants": f"/api/management/v1/catalogs/{catalog}/catalog-roles/{WRITER}/grants",
        "assign_catalog_role": f"/api/management/v1/principal-roles/{WRITER}/catalog-roles/{catalog}",
        "principals": "/api/management/v1/principals",
        "principal": f"/api/management/v1/principals/{WRITER}",
        "reset": f"/api/management/v1/principals/{WRITER}/reset",
        "principal_roles": f"/api/management/v1/principals/{WRITER}/principal-roles",
        "namespaces": f"/api/catalog/v1/{catalog}/namespaces",
    }


def grants() -> list[dict[str, Any]]:
    """Every grant of the writer's catalog role: catalog read, and the table privileges on each of the two namespaces."""
    found: list[dict[str, Any]] = [{"grant": {"type": "catalog", "privilege": privilege}} for privilege in CATALOG_PRIVILEGES]
    for namespace in NAMESPACES:
        found += [{"grant": {"type": "namespace", "namespace": [namespace], "privilege": privilege}} for privilege in NAMESPACE_PRIVILEGES]
    return found


def planned_requests(catalog: str) -> list[tuple[str, str, dict[str, Any] | None]]:
    """The requests of a first run, in order, without any credential."""
    path = catalog_paths(catalog)
    steps: list[tuple[str, str, dict[str, Any] | None]] = [
        ("POST", path["roles"], {"principalRole": {"name": WRITER}}),
        ("POST", path["catalog_roles"], {"catalogRole": {"name": WRITER}}),
    ]
    steps += [("POST", path["namespaces"], {"namespace": [namespace], "properties": {}}) for namespace in NAMESPACES]
    steps += [("PUT", path["grants"], grant) for grant in grants()]
    steps += [
        ("PUT", path["assign_catalog_role"], {"catalogRole": {"name": WRITER}}),
        ("GET", path["principal"], None),
        ("POST", path["principals"], {"principal": {"name": WRITER}, "credentialRotationRequired": False}),
        ("PUT", path["principal_roles"], {"principalRole": {"name": WRITER}}),
    ]
    return steps


def ensure_writer(access: str, catalog: str, known: dict[str, str], request: Request, store: Callable[[dict[str, str]], None]) -> str:
    """Create the writer principal, its roles, the two namespaces and the grants; safe to repeat. Returns how the credentials were obtained.

    ``known`` holds the credentials file's values; ``store`` writes them back with the writer's pair added (owner-only file).
    """
    path = catalog_paths(catalog)
    status, _ = request("POST", path["roles"], access, body={"principalRole": {"name": WRITER}})
    check(status, "creating the writer principal role", (200, 201, 409))
    status, _ = request("POST", path["catalog_roles"], access, body={"catalogRole": {"name": WRITER}})
    check(status, "creating the writer catalog role", (200, 201, 409))
    for namespace in NAMESPACES:
        status, _ = request("POST", path["namespaces"], access, body={"namespace": [namespace], "properties": {}})
        check(status, f"creating namespace {namespace}", (200, 201, 409))
    for grant in grants():
        status, _ = request("PUT", path["grants"], access, body=grant)
        check(status, f"granting {grant['grant']['privilege']} to the writer")
    status, _ = request("PUT", path["assign_catalog_role"], access, body={"catalogRole": {"name": WRITER}})
    check(status, "assigning the writer catalog role")
    status, _ = request("GET", path["principal"], access)
    if status == 404:
        status, body = request("POST", path["principals"], access, body={"principal": {"name": WRITER}, "credentialRotationRequired": False})
        check(status, "creating the writer principal")
        outcome = "created"
    elif status == 200 and known.get(WRITER_KEYS[1]):
        body, outcome = None, "already present"
    else:
        check(status, "reading the writer principal")
        status, body = request("POST", path["reset"], access, body={})
        check(status, "resetting the writer credentials")
        outcome = "credentials reset"
    if body is not None:
        issued = body["credentials"]
        store({**known, WRITER_KEYS[0]: issued["clientId"], WRITER_KEYS[1]: issued["clientSecret"]})
    status, _ = request("PUT", path["principal_roles"], access, body={"principalRole": {"name": WRITER}})
    check(status, "assigning the writer principal role")
    return outcome


def main(argv: list[str] | None = None) -> int:
    """Print the plan (``--dry-run``) or run the set-up against the local Polaris."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dry-run", action="store_true", help="print the requests and send none")
    args = parser.parse_args(sys.argv[1:] if argv is None else argv)
    from scripts.lakehouse import catalog

    if args.dry_run:
        for method, path, body in planned_requests(catalog.CATALOG):
            sys.stdout.write(f"{method} {path} {json.dumps(body, sort_keys=True) if body else ''}".rstrip() + "\n")
        return 0
    try:
        known = catalog.credentials()
        access = catalog.token(known)
        outcome = ensure_writer(access, catalog.CATALOG, known, catalog.request, lambda values: catalog.write_private(catalog.SECRETS, values))
        catalog.write_compose_env(catalog.deployment())
    except (SetupError, catalog.CatalogError, OSError, KeyError) as error:
        sys.stderr.write(f"gold writer setup: {error}\n")
        return 1
    sys.stdout.write(f"gold_writer principal {outcome}; it may write the {' and '.join(NAMESPACES)} namespaces only.\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
