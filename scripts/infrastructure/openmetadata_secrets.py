"""Create the persistent catalog's two passwords once, straight into Parameter Store; run: ``.venv/bin/python -m scripts.infrastructure.openmetadata_secrets``.

The administrator password and the catalog database password are SecureString parameters named in ``infra/iam/openmetadata.tf`` and
``infra/iam/openmetadata_persistent.tf``. Each is generated in memory only when its parameter is absent, written without overwrite and read back.
A rerun finds both and changes nothing. The output names each secret and says ``created`` or ``present``; no value is printed, logged or saved to a file.
"""

from __future__ import annotations

import argparse
import secrets
import string
import sys
from pathlib import Path

from scripts.infrastructure import secret_store

REPO_ROOT = Path(__file__).resolve().parents[2]
ADMIN_PASSWORD = secret_store.PREFIX + "openmetadata_admin_password"
DB_PASSWORD = secret_store.PREFIX + "openmetadata_db_password"
NAMES = (ADMIN_PASSWORD, DB_PASSWORD)
ADMIN_LENGTH = 40
SPECIAL = "!#%*+=-_"


def new_admin_password() -> str:
    """A random 40-character password with at least one uppercase letter, lowercase letter, digit and special character, as OpenMetadata requires."""
    groups = (string.ascii_uppercase, string.ascii_lowercase, string.digits, SPECIAL)
    characters = [secrets.choice(group) for group in groups]
    characters += [secrets.choice("".join(groups)) for _ in range(ADMIN_LENGTH - len(groups))]
    secrets.SystemRandom().shuffle(characters)
    return "".join(characters)


def ensure_passwords(store: secret_store.SecretStore) -> dict[str, bool]:
    """Create each absent password and return, by parameter name, whether this call created it."""
    makers = {ADMIN_PASSWORD: new_admin_password, DB_PASSWORD: secret_store.new_password}
    return {name: store.ensure(name, makers[name])[1] for name in NAMES}


def main(argv: list[str] | None = None, endpoint: str | None = None) -> int:
    """Create the absent passwords and return 0, or 1 when the store refuses or cannot be reached; ``endpoint`` is only for a loopback stand-in in tests."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    parser.add_argument("--root", type=Path, default=REPO_ROOT, help="the repository folder that holds the rendered deployment variables")
    arguments = parser.parse_args(sys.argv[1:] if argv is None else argv)
    try:
        created = ensure_passwords(secret_store.open_store(arguments.root, endpoint, writable=NAMES))
    except secret_store.SecretStoreError as error:
        sys.stderr.write(f"{error}\n")
        return 1
    for name, new in created.items():
        sys.stdout.write(f"{name}: {'created' if new else 'present'}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
