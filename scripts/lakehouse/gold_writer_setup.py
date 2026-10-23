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

import sys
from pathlib import Path
from typing import Any

from scripts.infrastructure import secret_store
from scripts.lakehouse import polaris_writer

WRITER_SECRETS = (secret_store.POLARIS_GOLD_CLIENT_ID, secret_store.POLARIS_GOLD_CLIENT_SECRET)
GOLD = polaris_writer.Writer("gold_writer", ("gold", "gold_e2e"), WRITER_SECRETS)


def planned_requests(catalog: str) -> list[tuple[str, str, dict[str, Any] | None]]:
    """The requests of a first run, in order, without any credential."""
    return polaris_writer.planned_requests(GOLD, catalog)


def ensure_writer(access: str, catalog: str, vault: secret_store.SecretStore, request: polaris_writer.Request, lock_path: Path) -> str:
    """Create the gold writer principal, its roles, the two namespaces and the grants; safe to repeat. Returns how the credentials were obtained."""
    return polaris_writer.ensure_writer(GOLD, access, catalog, vault, request, lock_path)


def main(argv: list[str] | None = None) -> int:
    """Print the plan (``--dry-run``) or run the set-up against the local Polaris."""
    return polaris_writer.main(GOLD, __doc__.splitlines()[0], argv)


if __name__ == "__main__":
    sys.exit(main())
