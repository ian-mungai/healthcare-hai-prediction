"""Create the password of the model API's read-only Redis user in Parameter Store (owner decision D18).

The value is generated in memory, written once as a SecureString and never printed, logged or kept in a file. A rerun reads the parameter and changes
nothing: the create never overwrites. Terraform holds only the name. The Redis task creates the user from this password and the model API presents it.

Run from the repository root with the project profile: ``.venv/bin/python -m scripts.infrastructure.create_model_api_redis_password``
"""

from __future__ import annotations

import sys
from pathlib import Path

from scripts.infrastructure import secret_store

REPO_ROOT = Path(__file__).resolve().parents[2]
NAME = secret_store.MODEL_API_REDIS_PASSWORD


def run(root: Path, endpoint: str | None = None) -> int:
    """Create the parameter when it is missing and say which. Return 0, or 1 with an error that names the secret. ``endpoint`` is for tests."""
    try:
        store = secret_store.open_store(root, endpoint)
        _, created = store.ensure(NAME, secret_store.new_password)
    except secret_store.SecretStoreError as error:
        sys.stderr.write(f"{NAME}: {error}\n")
        return 1
    sys.stdout.write(f"{NAME}: {'created' if created else 'already exists, left unchanged'}\n")
    return 0


def main() -> int:
    return run(REPO_ROOT)


if __name__ == "__main__":
    sys.exit(main())
