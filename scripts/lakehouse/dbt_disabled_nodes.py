"""Find the models and tests of the dbt project that dbt disabled on its own.

dbt disables a node whose ``ref`` names a model that does not exist, with a warning and no failure, and a disabled test is not a skipped result: the build
passes with the check gone. This module reads the manifest of a compile (lint target, no catalog) or of a finished build and names every node of the project
that is disabled, whatever the reason. The project's own package (dbt_project_evaluator) disables its models by configuration and is not a finding.
``--check`` (the default) compiles the committed project and exits 1 with the node names when any is disabled.
"""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
from collections.abc import Mapping
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
DBT = ROOT / ".tools" / "sqlfluff" / "bin" / "dbt"
PROJECT = "hai"
COMPILE_SECONDS = 300


class DisabledNodesError(RuntimeError):
    """The compile failed, so no manifest exists to read."""


def compile_manifest(project: Path, target: Path) -> dict[str, Any]:
    """Compile the dbt project with the lint target (an in-memory DuckDB with nothing attached) and return its manifest."""
    from scripts.process import run_command

    environment = {
        "DUCKDB_MEMORY_LIMIT": "2GB",
        "JOB_THREADS": "1",
        "DBT_PACKAGES_INSTALL_PATH": str((ROOT / "data/analytics/dbt/dbt_packages").resolve()),
    }
    result = run_command(
        str(DBT),
        ["compile", "--target", "lint", "--project-dir", str(project), "--profiles-dir", str(project), "--target-path", str(target)],
        env=environment,
        timeout=COMPILE_SECONDS,
    )
    if result.returncode != 0:
        raise DisabledNodesError(f"dbt compile failed with code {result.returncode}: {result.stdout.strip()[-300:]}")
    return dict(json.loads((target / "manifest.json").read_text()))


def disabled_nodes(manifest: Mapping[str, Any]) -> list[str]:
    """The unique IDs of the project's nodes that are disabled: nodes whose config says so (the form dbt gives a test with a missing reference) and the
    entries of the manifest's disabled section that belong to the project."""
    found = {key for key, node in manifest.get("nodes", {}).items() if node.get("config", {}).get("enabled") is False and f".{PROJECT}." in key}
    found |= {key for key in manifest.get("disabled", {}) if f".{PROJECT}." in key}
    return sorted(found)


def main(argv: list[str] | None = None) -> int:
    """Compile the committed project and fail when any node of it is disabled."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    parser.add_argument("--manifest", type=Path, help="read this manifest.json instead of compiling the project")
    args = parser.parse_args(sys.argv[1:] if argv is None else argv)
    if args.manifest is not None:
        manifest = dict(json.loads(args.manifest.read_text()))
    else:
        with tempfile.TemporaryDirectory(prefix="dbt_disabled_") as folder:
            manifest = compile_manifest(ROOT / "dbt", Path(folder) / "target")
    found = disabled_nodes(manifest)
    for name in found:
        sys.stderr.write(f"dbt disabled {name}: a ref or a config left it out of every build, so its check does not run\n")
    if found:
        sys.stderr.write(f"{len(found)} disabled nodes. Fix the reference that names a missing model.\n")
        return 1
    sys.stdout.write("dbt disabled nodes: none\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
