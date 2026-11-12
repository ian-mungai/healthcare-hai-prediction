"""Print one validated setting of the project's ``.env`` so that a shell command can set a variable from it.

The value comes from ``render_project_config.load_configuration``, the dotenv parser that every project script uses for ``.env``: quoted and unquoted values,
``export`` prefixes, spaces around the equals sign and comments are read the same way, and the whole file is validated. A missing or invalid file prints
the reason on standard error and nothing on standard output.

Run from the repository root: ``.venv/bin/python -m scripts.infrastructure.project_setting aws_profile``.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from scripts.infrastructure.render_project_config import ConfigurationError, load_configuration

SETTINGS = ("aws_profile", "aws_region")


def main(argv: list[str] | None = None) -> int:
    """Print the named setting and return 0, or return 1 with the reason when ``.env`` cannot be read or is invalid."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("name", choices=SETTINGS)
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    arguments = parser.parse_args(argv)
    try:
        storage, _ = load_configuration(arguments.env_file)
    except ConfigurationError as error:
        sys.stderr.write(f"project_setting: {error}\n")
        return 1
    sys.stdout.write(f"{storage[arguments.name]}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
