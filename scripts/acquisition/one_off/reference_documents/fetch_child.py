"""Download one file with the shared transport and print the result as JSON; ``fetch_support`` runs this in its own process and ends it at the deadline.

    .venv/bin/python -m scripts.acquisition.one_off.reference_documents.fetch_child --url URL --file-name NAME --format pdf --captures DIR \
        --attempts 2 --timeout-seconds 30 --max-seconds 180 --max-bytes 67108864
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import sys
from pathlib import Path

from scripts.acquisition.transport import CaptureError, Limits, download


def main(argv: list[str] | None = None) -> int:
    """Download into ``--captures`` and print the transport's result; a request the transport refuses prints its reason to stderr and returns 2."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--url", required=True)
    parser.add_argument("--file-name", required=True)
    parser.add_argument("--format", required=True)
    parser.add_argument("--captures", type=Path, required=True)
    parser.add_argument("--attempts", type=int, required=True)
    parser.add_argument("--timeout-seconds", type=float, required=True)
    parser.add_argument("--max-seconds", type=float, required=True)
    parser.add_argument("--max-bytes", type=int, required=True)
    args = parser.parse_args(argv)
    try:
        limits = Limits(attempts=args.attempts, timeout_seconds=args.timeout_seconds, max_seconds=args.max_seconds, max_bytes=args.max_bytes)
        result = download(args.url, args.captures / args.file_name, args.file_name, args.format, "reference", limits)
    except CaptureError as error:
        sys.stderr.write(f"{error}\n")
        return 2
    sys.stdout.write(json.dumps(dataclasses.asdict(result), default=str) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
