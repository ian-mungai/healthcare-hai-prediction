"""Export the validated sealed silver build through the shared publish consumer."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path

from scripts.lakehouse.gold_export import DEFAULT_SEALED, export_sealed
from scripts.lakehouse.gold_publish import DEFAULT_PUBLISH_DIR, load_plan
from scripts.lakehouse.gold_publish_record import PublishError

CONTRACT = Path(__file__).resolve().parents[2] / "data_contracts/lakehouse/silver_publish.json"


def main(argv: Sequence[str] | None = None) -> int:
    """Export the silver contract's tables only after the mounted file matches the sealed digest."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", required=True)
    parser.add_argument("--sealed-sha256", required=True)
    parser.add_argument("--publish-dir", default=DEFAULT_PUBLISH_DIR)
    parser.add_argument("--sealed", default=DEFAULT_SEALED)
    args = parser.parse_args(argv)
    try:
        result = export_sealed(Path(args.sealed), args.sealed_sha256, Path(args.publish_dir), load_plan(CONTRACT), args.run)
    except PublishError as error:
        sys.stderr.write(f"silver export: {error}\n")
        return 1
    sys.stdout.write(json.dumps({"exported": result["exported"], "manifest_sha256": result["manifest_sha256"]}) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
