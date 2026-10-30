"""Read and check the tracked drift configuration (project decision 21).

``python -m monitoring.drift_config --check [--config PATH]`` validates the file against ``data_contracts/monitoring/drift.schema.json`` and exits 0, or exits 1
and names the file and the key that is wrong. The schema pins the decision's values (drift share 0.5, the Kolmogorov-Smirnov and chi-square tests, p below 0.05,
only the primary population triggers), so changing one of them needs a change to the decision and to the schema in the same commit. Run it from the repository
root as a module.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / "config/monitoring/drift.json"
SCHEMA = ROOT / "data_contracts/monitoring/drift.schema.json"


class ConfigError(ValueError):
    """The configuration is missing, unreadable or breaks its schema; the message names the file and the key."""


def load(path: Path = CONFIG) -> tuple[dict[str, Any], str]:
    """Return the validated configuration and the SHA-256 of its file."""
    try:
        raw = path.read_bytes()
    except OSError as error:
        raise ConfigError(f"{path}: cannot read the file: {error.strerror or error}") from None
    try:
        config = json.loads(raw)
    except ValueError as error:
        raise ConfigError(f"{path}: not valid JSON: {error}") from None
    errors = sorted(Draft202012Validator(json.loads(SCHEMA.read_text())).iter_errors(config), key=lambda error: [str(part) for part in error.path])
    if errors:
        first = errors[0]
        where = ".".join(str(part) for part in first.path) or "top level"
        raise ConfigError(f"{path}: {where}: {first.message}")
    return config, hashlib.sha256(raw).hexdigest()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--check", action="store_true", required=True, help="validate the configuration and exit")
    parser.add_argument("--config", type=Path, default=CONFIG, help="the configuration file (default: the tracked file)")
    arguments = parser.parse_args(argv)
    try:
        _, digest = load(arguments.config)
    except ConfigError as error:
        sys.stderr.write(f"drift config invalid: {error}\n")
        return 1
    sys.stdout.write(f"drift config ok: {arguments.config} sha256 {digest[:12]}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
