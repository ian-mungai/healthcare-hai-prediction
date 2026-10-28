"""Plant one defect at a time in ``host_session`` and require the cases to catch each; run: ``.venv/bin/python -m scripts.infrastructure.host_session_plants``.

Each plant replaces one piece of the module's source in memory (the file stays unchanged) and reruns every case. A plant is caught when a check
fails or a case raises. The run exits 1 when any plant survives or its text is not found.
"""

from __future__ import annotations

import importlib
import importlib.util
import sys
import tempfile
from pathlib import Path

MODULE = "scripts.infrastructure.host_session"
SOURCE = Path(__file__).with_name("host_session.py")
PLANTS: dict[str, tuple[str, str]] = {
    "drops tag propagation": ('"TASK_DEFINITION"', '"NONE"'),
    "starts a second one-shot task": ("        if running:\n", "        if False:\n"),
    "leaves the host on after a failure": (
        "            else:\n                self.host_off()\n            raise",
        "            else:\n                pass\n            raise",
    ),
    "ignores a FAILED rollout": ('deployment.get("rolloutState") == FAILED_ROLLOUT', 'deployment.get("rolloutState") == "NEVER"'),
    "skips the registration wait": ("len(self.active_instances()) == 1", "True"),
    "puts postgres last": ("key != FIRST_SERVICE", "key == FIRST_SERVICE"),
    "accepts a nonzero exit": ("code != 0 for code", "code == 99 for code"),
    "allows a second bootstrap": ("self.marker.exists() and not again", "False"),
    "bootstraps before services are stable": ("not self.services_stable(gate)", "False"),
    "never gives up on failed polls": ("failed >= FAILED_POLLS_ALLOWED", "failed >= 10**6"),
    "does not verify the termination": ('not group.get("Instances")', "True"),
    "ignores an interruption": ("except BaseException", "except Exception"),
    "ignores pending tasks": (' and not service.get("pendingCount")', ""),
    "does not force the deployment": ('"--force-new-deployment"]', '"--no-force-new-deployment"]'),
    "prints the task ARN": ('self.step(key, f"exit codes {exits}")', 'self.step(key, f"exit codes {exits} {task}")'),
}


def caught(source: str) -> bool:
    """True when the cases detect the planted source."""
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "host_session_planted.py"
        path.write_text(source, encoding="utf-8")
        spec = importlib.util.spec_from_file_location(MODULE, path)
        if spec is None or spec.loader is None:
            raise ImportError(MODULE)
        planted = importlib.util.module_from_spec(spec)
        sys.modules[MODULE] = planted
        spec.loader.exec_module(planted)
    cases = importlib.import_module("scripts.infrastructure.host_session_cases")
    importlib.reload(cases)
    for function in cases.CASES.values():
        try:
            if not all(function().values()):
                return True
        except Exception:
            return True
    return False


def main() -> int:
    """Run every plant and return 1 when any survives."""
    original = SOURCE.read_text(encoding="utf-8")
    survivors = 0
    try:
        for name, (old, new) in PLANTS.items():
            if old not in original:
                sys.stdout.write(f"MISSING {name}\n")
                survivors += 1
                continue
            result = caught(original.replace(old, new, 1))
            sys.stdout.write(f"{'CAUGHT' if result else 'SURVIVED'} {name}\n")
            survivors += not result
    finally:
        sys.modules.pop(MODULE, None)
    sys.stdout.write(f"{len(PLANTS) - survivors} of {len(PLANTS)} plants caught\n")
    return 1 if survivors else 0


if __name__ == "__main__":
    raise SystemExit(main())
