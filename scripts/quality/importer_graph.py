"""Print the modules that directly import given modules, read from the import graph that grimp builds for a source tree.

Run it with the Python of the pinned import-linter, which carries grimp, and with ``-P`` so that this folder does not join the import path:

    PYTHONPATH=<root> .tools/import-linter/bin/python -P scripts/quality/importer_graph.py --root <root> --module scripts.process [--module ...]

Each top-level folder of ``--root`` that holds Python files is a root package (namespace packages included). A module that the tree does not hold is
skipped. The output is one JSON list of module names. scripts/quality/typecheck_importers.py starts this file and maps the names back to paths.
"""

from __future__ import annotations

import argparse
import importlib
import json
import sys
from pathlib import Path


def importers(root: Path, modules: list[str]) -> list[str]:
    """The modules of the tree at ``root`` that directly import any of ``modules``, sorted."""
    packages = sorted(folder.name for folder in root.iterdir() if folder.is_dir() and not folder.name.startswith(".") and any(folder.rglob("*.py")))
    if not packages:
        return []
    grimp = importlib.import_module("grimp")
    graph = grimp.build_graph(*packages, include_external_packages=False)
    found: set[str] = set()
    for module in modules:
        if module in graph.modules:
            found |= set(graph.find_modules_that_directly_import(module))
    return sorted(found - set(modules))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--module", action="append", default=[])
    args = parser.parse_args()
    sys.stdout.write(json.dumps(importers(args.root.resolve(), args.module)) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
