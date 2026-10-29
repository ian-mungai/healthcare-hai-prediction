"""Set up the ``features_writer`` principal on the local Polaris catalog.

Run ``.venv/bin/python -m scripts.features.features_writer_setup --dry-run`` to print the requests. The principal may write the tables of the ``features`` and
``features_e2e`` namespaces and nothing else: no access to ``gold``, ``gold_e2e``, ``silver``, bronze or the other writers' namespaces, no catalog-wide
management, no namespace removal and no table removal. The set-up creates both namespaces. The root credentials make the token and the set-up requests only;
they are never passed on. The writer's client ID and secret reach a job only through the launcher (``POLARIS_FEATURES_WRITER_CLIENT_ID`` and ``_SECRET``).

The features publish uses the gold publish engine, so the privilege list is the gold writer's; the write spike on ``features_e2e`` proves it against Polaris
at the first real features publish. The owner runs the set-up once, after the managed secrets for the pair exist (``scripts/infrastructure/secret_store.py``
and the Terraform variables ``polaris_features_client_id_name`` and ``polaris_features_client_secret_name``); until then ``polaris_writer.main`` refuses
a real run, as it does for the silver writer.
"""

from __future__ import annotations

import sys

from scripts.lakehouse import polaris_writer

FEATURES = polaris_writer.Writer(
    "features_writer", ("features", "features_e2e"), ("POLARIS_FEATURES_WRITER_CLIENT_ID", "POLARIS_FEATURES_WRITER_CLIENT_SECRET")
)


def main(argv: list[str] | None = None) -> int:
    """Print the plan (``--dry-run``) or run the set-up against the local Polaris."""
    return polaris_writer.main(FEATURES, __doc__.splitlines()[0], argv)


if __name__ == "__main__":
    sys.exit(main())
