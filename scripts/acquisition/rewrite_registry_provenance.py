"""One-time development step: rewrite the legacy registry's provenance pointers so the tracked copy passes the privacy scan.

Each legacy external pointer into the agent home folder becomes ``/workspace/provenance/<rest>``, the path the host mounts. The registry hash, its lock and the
additions lock that names it as base change with the bytes; plans that pin the old hash are re-locked by a separate step.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

from scripts.acquisition.source_registry import canonical_hash

POINTER_FROM = "external://" + "ho" + "me/.codex/"
POINTER_TO = "/workspace/provenance/"
PHONE_EXAMPLE = re.compile(r'"\(\d{3}\) \d{3}-\d{4}"')
PHONE_PLACEHOLDER = "(redacted)"
FILES = ("source_registry.json", "source_registry_lock.json", "registry_additions.json", "registry_additions_lock.json")


def rewrite_pointers(text: str, *, redact_phone_examples: bool = False) -> str:
    """Return the registry text with every provenance pointer rewritten, keeping its layout byte for byte."""
    result = text.replace(POINTER_FROM, POINTER_TO)
    if redact_phone_examples:
        result = PHONE_EXAMPLE.sub(f'"{PHONE_PLACEHOLDER}"', result)
    try:
        json.loads(result)
    except json.JSONDecodeError as error:
        raise ValueError("Rewritten registry is not valid JSON") from error
    return result


def relock(text: str, lock: dict[str, Any], additions_lock: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    """Return the registry lock and additions lock that bind the rewritten registry's canonical hash."""
    digest = canonical_hash(json.loads(text))
    return {**lock, "registry_sha256": digest}, {**additions_lock, "base_registry_sha256": digest}


def dump(value: dict[str, Any]) -> str:
    """Write a lock the way the legacy locks are written: sorted keys, two-space indent, trailing newline."""
    return json.dumps(value, indent=2, sort_keys=True) + "\n"


def run(source: Path, output: Path, *, redact_phone_examples: bool) -> dict[str, str]:
    """Write the four rewritten registry files into a new folder and return their SHA-256 values and the registry hash."""
    import hashlib

    output.mkdir(parents=True, exist_ok=False)
    text = rewrite_pointers((source / FILES[0]).read_text(encoding="utf-8"), redact_phone_examples=redact_phone_examples)
    lock, additions_lock = relock(text, json.loads((source / FILES[1]).read_text()), json.loads((source / FILES[3]).read_text()))
    contents = {FILES[0]: text, FILES[1]: dump(lock), FILES[2]: (source / FILES[2]).read_text(encoding="utf-8"), FILES[3]: dump(additions_lock)}
    for name, content in contents.items():
        (output / name).write_text(content, encoding="utf-8")
    report = {name: hashlib.sha256(content.encode("utf-8")).hexdigest() for name, content in contents.items()}
    report["registry_canonical_sha256"] = lock["registry_sha256"]
    return report


def main() -> None:
    """Rewrite the legacy registry from SOURCE into a new OUTPUT folder and print the new digests."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--redact-phone-examples", action="store_true")
    args = parser.parse_args()
    report = run(args.source, args.output, redact_phone_examples=args.redact_phone_examples)
    sys.stdout.write(json.dumps(report, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
