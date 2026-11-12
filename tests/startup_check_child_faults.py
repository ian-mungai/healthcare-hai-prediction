"""The child entry of the isolation tests: it installs one synthetic fault from STARTUP_CHECK_TEST_FAULT, then runs the real child."""

from __future__ import annotations

import importlib
import os
import sys
from pathlib import Path


def main(argv: list[str]) -> int:
    importlib.import_module("tests.startup_check_faults").install(os.environ["STARTUP_CHECK_TEST_FAULT"], Path(argv[1]))
    return importlib.import_module("scripts.infrastructure.startup_check_child").main(argv)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
