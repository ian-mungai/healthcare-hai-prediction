"""Set up the ``gold_writer`` principal on the local Polaris catalog.

Run ``.venv/bin/python -m scripts.lakehouse.gold_writer_setup`` with the catalog up (``scripts.lakehouse.catalog up``). ``--dry-run`` prints the
requests it would send and sends none. The principal may write the tables of the ``gold`` and ``gold_e2e`` namespaces and nothing else: no
catalog-wide management, no namespace creation or removal and no table removal. The root credentials make the token and the set-up requests
only; they are never passed on. The writer's client ID and secret go straight to AWS Secrets Manager, the same way the read-only
principal's do, and reach a job only through the launcher's ``lakehouse_write`` class (``POLARIS_GOLD_WRITER_CLIENT_ID`` and ``_SECRET``, which the
launcher reads from Secrets Manager into memory).

The exact privilege list is a phase 2 finding: the write spike on ``gold_e2e`` proves it against Polaris before the first real publish.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from scripts.infrastructure import secret_store
from scripts.lakehouse import polaris_writer, principal_credentials

GOLD = polaris_writer.Writer("gold_writer", ("gold", "gold_e2e"), ("POLARIS_GOLD_WRITER_CLIENT_ID", "POLARIS_GOLD_WRITER_CLIENT_SECRET"))
WRITER_SECRETS = (secret_store.POLARIS_GOLD_CLIENT_ID, secret_store.POLARIS_GOLD_CLIENT_SECRET)


def planned_requests(catalog: str) -> list[tuple[str, str, dict[str, Any] | None]]:
    """The requests of a first run, in order, without any credential."""
    return polaris_writer.planned_requests(GOLD, catalog)


def ensure_writer(access: str, catalog: str, vault: secret_store.SecretStore, request: polaris_writer.Request, lock_path: Path) -> str:
    """Create the writer principal, its roles, the two namespaces and the grants; safe to repeat. Returns how the credentials were obtained.

    ``vault`` holds the writer's client ID and secret in Secrets Manager. A stored pair is kept only when Polaris accepts it; otherwise a new pair
    is issued and stored. ``lock_path`` serializes updates of the writer principal.
    """
    path = polaris_writer.catalog_paths(GOLD, catalog)
    status, _ = request("POST", path["roles"], access, body={"principalRole": {"name": GOLD.name}})
    polaris_writer.check(status, "creating the writer principal role", (200, 201, 409))
    status, _ = request("POST", path["catalog_roles"], access, body={"catalogRole": {"name": GOLD.name}})
    polaris_writer.check(status, "creating the writer catalog role", (200, 201, 409))
    for namespace in GOLD.namespaces:
        status, _ = request("POST", path["namespaces"], access, body={"namespace": [namespace], "properties": {}})
        polaris_writer.check(status, f"creating namespace {namespace}", (200, 201, 409))
    for grant in polaris_writer.grants(GOLD):
        status, _ = request("PUT", path["grants"], access, body=grant)
        polaris_writer.check(status, f"granting {grant['grant']['privilege']} to the writer")
    status, _ = request("PUT", path["assign_catalog_role"], access, body={"catalogRole": {"name": GOLD.name}})
    polaris_writer.check(status, "assigning the writer catalog role")
    outcome = principal_credentials.ensure_credentials(
        principal=GOLD.name, names=WRITER_SECRETS, vault=vault, request=request, access=access, check=polaris_writer.check, lock_path=lock_path
    )
    status, _ = request("PUT", path["principal_roles"], access, body={"principalRole": {"name": GOLD.name}})
    polaris_writer.check(status, "assigning the writer principal role")
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
        access = catalog.token(catalog.credentials())
        outcome = ensure_writer(access, catalog.CATALOG, catalog.vault(), catalog.request, catalog.STATE / "principal_locks" / f"{GOLD.name}.lock")
    except (polaris_writer.SetupError, catalog.CatalogError, secret_store.SecretStoreError, principal_credentials.CredentialError, OSError, KeyError) as error:
        sys.stderr.write(f"gold writer setup: {error}\n")
        return 1
    sys.stdout.write(f"{GOLD.name} principal {outcome}; it may write the {' and '.join(GOLD.namespaces)} namespaces only.\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
