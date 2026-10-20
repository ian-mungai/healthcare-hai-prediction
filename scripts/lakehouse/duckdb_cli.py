"""Resolve the project's pinned native DuckDB CLI for light metadata checks."""

from __future__ import annotations

import json
from functools import cache
from pathlib import Path

from scripts.process import run_command


@cache
def duckdb_binary() -> Path:
    """Resolve only the project CLI and refuse a version different from the committed tool pin."""
    root = Path(__file__).resolve().parents[2]
    binary = root / ".tools/bin/duckdb"
    pin = json.loads((root / "config/quality_tools.json").read_text())["tools"]["duckdb"]["version"]
    result = run_command(str(binary), ["--version"], timeout=10, check=True)
    if result.stdout.split()[0] != "v" + pin:
        raise ValueError("Project DuckDB CLI differs from config/quality_tools.json; run the approved installer.")
    return binary
