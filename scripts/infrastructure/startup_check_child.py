"""The child process of the startup check: it runs one request and writes only records to its result file.

``startup_check.run_child`` starts it as ``python -s -B -P -m scripts.infrastructure.startup_check_child <folder>``, with a minimal environment. The folder is
private to the check: it holds ``request.json`` and gets ``results.jsonl``. The child's standard output and error are captured by the parent and dropped, so
whatever a dependency logs, warns, dumps or prints stays here.
"""

from __future__ import annotations

import importlib
import json
import os
import sys
from pathlib import Path


def main(argv: list[str]) -> int:
    """Run the request in the folder and write its records to the result file. Any failure ends with exit code 1 and no result."""
    folder = Path(argv[1])
    try:
        startup_check = importlib.import_module("scripts.infrastructure.startup_check")
        request = json.loads((folder / startup_check.REQUEST_NAME).read_text(encoding="utf-8"))
        channel = os.open(folder / startup_check.RESULT_NAME, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        data = "".join(line + "\n" for line in startup_check.dump(startup_check.execute(request))).encode("utf-8")
        view = memoryview(data)
        while view:
            view = view[os.write(channel, view) :]
        os.close(channel)
    except BaseException:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
