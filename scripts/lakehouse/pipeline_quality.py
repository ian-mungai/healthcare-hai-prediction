"""Bind a successful quality process to the exact result bytes it produced."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from orchestration.pipeline_protocol import read_record


def main() -> int:
    """Run the fixed quality gate and emit its result digest for launcher acceptance."""
    parser = argparse.ArgumentParser(description=__doc__, add_help=False)
    parser.add_argument("--kind", required=True, choices=("silver", "gold", "cross", "features"))
    options, arguments = parser.parse_known_args()
    kind = options.kind
    original = sys.argv[1:]
    try:
        sys.argv[1:] = arguments
        if kind == "features":
            from scripts.features import feature_validation

            result = feature_validation.main()
        else:
            from scripts.lakehouse import gold_reconcile, silver_quality

            result = gold_reconcile.main() if kind == "cross" else silver_quality.main()
    finally:
        sys.argv[1:] = original
    record, digest = read_record(Path("/workspace/out"), "result.json")
    if result:
        failure = {
            "quality_kind": kind,
            "error": record.get("error"),
            "counts": record.get("counts"),
            "missing_tables": record.get("missing_tables"),
            "failed_expectations": [
                {key: entry.get(key) for key in ("id", "table", "level", "raised_exception")}
                for entry in record.get("expectations", [])
                if entry.get("level") == "fail"
            ],
        }
        sys.stderr.write(json.dumps(failure) + "\n")
        return result
    sys.stdout.write(json.dumps({"result_sha256": digest, "build_file_sha256": record.get("build_file_sha256")}) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
