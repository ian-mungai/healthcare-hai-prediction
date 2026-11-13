"""One-time development step: move the two MMD collection plans to the rewritten legacy registry and the redacted publisher page.

The plans pin the registry hash, the digest of the saved ``map.html`` reference and, for the additions plan, the digest of the additions lock. Each pin
changes as text, so the layout of every plan stays byte for byte. The plan digests are constants in ``mmd_api_contract``; the caller updates them.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path

PLACEHOLDER = "(redacted)"
MAILBOX = re.compile(r"[A-Za-z0-9._%+-]+@(?:[A-Za-z0-9-]+\.)+[A-Za-z]{2,}")
PAGE = "map.html"


def redact_page(text: str) -> str:
    """Replace every mailbox string in the saved publisher page with the placeholder."""
    return MAILBOX.sub(PLACEHOLDER, text)


def repin_plan(raw: bytes, old_registry: str, new_registry: str, new_page_sha256: str, new_additions_lock_sha256: str) -> bytes:
    """Return the plan bytes with its registry hash, page digest and (when present) additions lock digest moved."""
    plan = json.loads(raw)
    if plan["registry_sha256"] != old_registry:
        raise ValueError("The plan does not pin the old registry")
    text = raw.decode("utf-8").replace(old_registry, new_registry)
    text = text.replace(plan["references"][PAGE]["sha256"], new_page_sha256)
    if "registry_additions_lock_sha256" in plan:
        text = text.replace(plan["registry_additions_lock_sha256"], new_additions_lock_sha256)
    return text.encode("utf-8")


def main() -> None:
    """Write the redacted page and the two moved plans into a new OUTPUT folder and print their digests."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("page", type=Path)
    parser.add_argument("plan", type=Path)
    parser.add_argument("additions_plan", type=Path)
    parser.add_argument("additions_lock", type=Path, help="the rewritten additions lock")
    parser.add_argument("old_registry")
    parser.add_argument("new_registry")
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    page = redact_page(args.page.read_text(encoding="utf-8")).encode("utf-8")
    page_sha = hashlib.sha256(page).hexdigest()
    lock_sha = hashlib.sha256(args.additions_lock.read_bytes()).hexdigest()
    args.output.mkdir(parents=True, exist_ok=False)
    outputs = {PAGE: page}
    for source in (args.plan, args.additions_plan):
        outputs[source.name] = repin_plan(source.read_bytes(), args.old_registry, args.new_registry, page_sha, lock_sha)
    for name, content in outputs.items():
        (args.output / name).write_bytes(content)
    sys.stdout.write(json.dumps({name: hashlib.sha256(content).hexdigest() for name, content in outputs.items()}, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
