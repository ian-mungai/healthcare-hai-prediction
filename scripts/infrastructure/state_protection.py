"""The state administrator the owner approved for the storage stack, kept so that every later storage plan and apply carries it.

The storage stack turns the state prefix of the project bucket on with two inputs, ``terraform_state_enabled`` and ``terraform_state_admin_principals``.
A storage plan rendered without them removes the bucket's state statements and lifecycle rules, so the wrapper (``scripts/infrastructure/terraform.sh``)
reads the approved administrator from ``plans/state_migration/state_protection.json`` (ignored, owner only) and passes it to the renderer on every storage
render. Run from the repository root:

    .venv/bin/python -m scripts.infrastructure.state_protection resolve [--state-admin role/NAME]
    .venv/bin/python -m scripts.infrastructure.state_protection remember --state-admin role/NAME

``resolve`` prints the administrator the render must carry (empty when none was approved and none was given) and refuses a different administrator than
the approved one. ``remember`` keeps the administrator; the wrapper calls it after the saved plan of an apply passed its checks. To turn the protection
off on purpose, remove the record by hand and plan that change as its own owner step. Exit code 0 is done, 2 is a refusal (the reason is on stderr).
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
RECORD = Path("plans/state_migration/state_protection.json")
# The same rule as the terraform_state_admin_principals validation in infra/variables.tf and the --state-admin option of the renderer.
STATE_ADMIN = re.compile(r"(user|role)/[A-Za-z0-9_+=,.@/-]+")


class Refused(Exception):
    """A refusal with a reason that is safe to print."""


def valid(name: object) -> bool:
    """True for role/NAME or user/NAME with no wildcard, no full ARN and no 12-digit account ID."""
    return isinstance(name, str) and STATE_ADMIN.fullmatch(name) is not None and re.search(r"[0-9]{12}", name) is None


def read(path: Path) -> str | None:
    """The approved administrator, or None when no record exists. An unreadable or invalid record is a refusal."""
    if not path.is_file():
        return None
    try:
        names = json.loads(path.read_text(encoding="utf-8"))["terraform_state_admin_principals"]
    except (OSError, ValueError, KeyError, TypeError):
        raise Refused(f"the approved state protection inputs cannot be read ({path}); repair or remove the record") from None
    if not isinstance(names, list) or len(names) != 1 or not valid(names[0]):
        raise Refused(f"the approved state protection inputs are not valid ({path}): one role/NAME or user/NAME is expected")
    return str(names[0])


def write(path: Path, principal: str) -> None:
    """Keep the administrator, owner only. The write replaces the file in one step."""
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.with_name(f".{path.name}.writing")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
        handle.write(json.dumps({"terraform_state_admin_principals": [principal]}, indent=2) + "\n")
    os.replace(temporary, path)


def resolve(path: Path, given: str | None) -> str:
    """The administrator a storage render must carry: the approved one, or the given one when none is approved yet."""
    if given is not None and not valid(given):
        raise Refused("--state-admin takes role/NAME or user/NAME with no wildcard, no full ARN and no 12-digit account ID")
    approved = read(path)
    if approved is not None and given is not None and given != approved:
        raise Refused("--state-admin differs from the approved state administrator on record; the state protection is not changed here")
    return given or approved or ""


def main(arguments: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=("resolve", "remember"))
    parser.add_argument("--state-admin")
    parser.add_argument("--record", type=Path, default=REPO_ROOT / RECORD, help="Where the approved administrator is kept (the E2E uses a synthetic root).")
    parsed = parser.parse_args(arguments)
    try:
        if parsed.command == "remember":
            if parsed.state_admin is None:
                raise Refused("remember needs --state-admin")
            write(parsed.record, resolve(parsed.record, parsed.state_admin))
        else:
            sys.stdout.write(f"{resolve(parsed.record, parsed.state_admin)}\n")
    except Refused as refusal:
        sys.stderr.write(f"state protection: {refusal}\n")
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
