"""A test helper process that writes a synthetic name through every output route, with or without the worker's silencing."""

from __future__ import annotations

import logging
import os
import sys
import traceback
import warnings


def main() -> None:
    if sys.argv[1] == "silenced":
        from scripts.acquisition import private_worker

        private_worker.silence()
    name = os.environ["NOISY_NAME"]
    sys.stdout.write(name + "\n")
    sys.stderr.write(name)
    os.write(1, name.encode())
    os.write(2, name.encode())
    logging.basicConfig()
    logging.getLogger("noise").error(name)
    warnings.warn(name, stacklevel=1)
    traceback.print_stack()
    raise ValueError(name)


if __name__ == "__main__":
    main()
