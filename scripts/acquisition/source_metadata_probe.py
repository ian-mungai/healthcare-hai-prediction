"""Derive four dataset IDs in a fresh process, with the named table rule of source_metadata as it is or switched off (a probe of run_registry_additions_e2e)."""

from __future__ import annotations

import argparse
import json
import sys
from importlib.machinery import SourceFileLoader
from pathlib import Path

from scripts.acquisition import source_metadata as module
from scripts.acquisition.source_registry import load_registry

IDS = ["acs_b16005", "acs_b25070", "svi", "acs_zz999"]
# The one condition of the named table rule; the switched-off copy lives in the work folder and the repository source is never rewritten.
CONDITION = "if not rules and dataset_id in named_tables:"


def main() -> int:
    """Print the placements and the unplaced IDs of IDS as JSON."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("real", "removed"), required=True)
    parser.add_argument("--workdir", type=Path, required=True)
    arguments = parser.parse_args()
    if arguments.mode == "removed":
        source = Path(module.__file__).read_text(encoding="utf-8")
        if source.count(CONDITION) != 1:
            sys.stderr.write("the named table rule needs exactly one condition line\n")
            return 2
        changed = arguments.workdir / "source_metadata_without_the_named_table_rule.py"
        changed.write_text(source.replace(CONDITION, "if False:", 1), encoding="utf-8")
        # The standard loader runs the changed copy in the original module namespace, so the derivation uses its globals and imports.
        SourceFileLoader(module.__name__, str(changed)).exec_module(module)
    placements, unresolved = module.derive(IDS, load_registry())
    sys.stdout.write(json.dumps({"placements": placements, "unresolved": unresolved}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
