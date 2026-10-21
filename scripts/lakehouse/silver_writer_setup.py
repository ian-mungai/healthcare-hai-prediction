"""Set up the ``silver_writer`` principal on the local Polaris catalog.

Run ``.venv/bin/python -m scripts.lakehouse.silver_writer_setup`` once with the catalog up (``scripts.lakehouse.catalog up``) before the first real
silver publish. ``--dry-run`` prints the requests it would send and sends none. The principal may write the tables of the ``silver`` and
``silver_e2e`` namespaces and nothing else: no access to ``gold``, ``gold_e2e`` or bronze, no catalog-wide management, no namespace creation or
removal and no table removal. The root credentials make the token and the set-up requests only; they are never passed on. The writer's client ID
and secret go to the owner-only catalog credentials file next to the gold writer's pair, and reach a job only through the launcher
(``POLARIS_SILVER_WRITER_CLIENT_ID`` and ``_SECRET`` in the compose environment file the launcher reads on the host).

The silver publish uses the gold publish engine, so the privilege list is the gold writer's; the write spike on ``silver_e2e`` proves it against
Polaris at the first real silver publish.
"""

from __future__ import annotations

import sys

from scripts.lakehouse import polaris_writer

SILVER = polaris_writer.Writer("silver_writer", ("silver", "silver_e2e"), ("POLARIS_SILVER_WRITER_CLIENT_ID", "POLARIS_SILVER_WRITER_CLIENT_SECRET"))


def main(argv: list[str] | None = None) -> int:
    """Print the plan (``--dry-run``) or run the set-up against the local Polaris."""
    return polaris_writer.main(SILVER, __doc__.splitlines()[0], argv)


if __name__ == "__main__":
    sys.exit(main())
