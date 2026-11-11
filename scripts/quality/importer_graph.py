"""Print the modules that directly import given modules, read from the import graph that grimp builds for a source tree.

Run it with the Python of the pinned import-linter, which carries grimp, and with ``-P`` so that this folder does not join the import path:

    .tools/import-linter/bin/python -P scripts/quality/importer_graph.py --root <root> [--source-root src ...] --module scripts.process [--module ...]

Each top-level folder of ``--root`` that holds Python files is a root package (namespace packages included). Each ``--source-root`` (a folder of ``--root``
that the project declares as an import root, such as ``src``) adds its own top-level folders as root packages, named without the source folder. A module
that the tree does not hold is skipped. The output is one JSON list of module names.
scripts/quality/typecheck_importers.py starts this file and maps the names back to paths.
"""

from __future__ import annotations

import argparse
import importlib
import json
import sys
from pathlib import Path


def importers(root: Path, modules: list[str], source_roots: list[str]) -> list[str]:
    """The modules of the tree at ``root`` that directly import any of ``modules``, sorted."""
    searched = [root, *[root / folder for folder in source_roots if (root / folder).is_dir()]]
    skipped = set(source_roots)
    packages: set[str] = set()
    for base in searched:
        for folder in base.iterdir():
            inside = folder.relative_to(root).as_posix()
            if folder.is_dir() and not folder.name.startswith(".") and inside not in skipped and any(folder.rglob("*.py")):
                packages.add(folder.name)
    if not packages:
        return []
    sys.path[:0] = [str(base) for base in searched]
    grimp = importlib.import_module("grimp")
    graph = grimp.build_graph(*sorted(packages), include_external_packages=False)
    found: set[str] = set()
    for module in modules:
        if module in graph.modules:
            found |= set(graph.find_modules_that_directly_import(module))
    return sorted(found - set(modules))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--source-root", action="append", default=[])
    parser.add_argument("--module", action="append", default=[])
    args = parser.parse_args()
    sys.stdout.write(json.dumps(importers(args.root.resolve(), args.module, args.source_root)) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
