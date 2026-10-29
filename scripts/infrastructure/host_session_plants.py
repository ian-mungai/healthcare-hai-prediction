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
        "        try:\n            with deferred_signals(self.step):\n                self.host_off()\n        except HostSessionError",
        "        try:\n            pass\n        except HostSessionError",
    ),
    "lets a second signal cut the cleanup short": (
        "            with deferred_signals(self.step):\n                self.host_off()",
        "            self.host_off()",
    ),
    "leaves the host on after a failed bootstrap": ('self.stop_after_failure("bootstrap", keep_on)', "pass"),
    "ignores an interruption": (
        '        finally:\n            if not succeeded:\n                self.stop_after_failure("start", keep_on)',
        '        except HostSessionError:\n            self.stop_after_failure("start", keep_on)\n            raise',
    ),
    "ignores the termination signal": ("raise TerminationRequested(signal.Signals(number).name)", "return"),
    "launches without a client token": ('            "--client-token",\n            token,\n', ""),
    "keeps the token after a confirmed launch": ("            del self.launch_tokens[key]\n            return launched", "            return launched"),
    "treats a lost reply as absent": ("if UNCONFIRMED not in str(error):", "if True:"),
    "accepts a success after the deadline": ("if value and self.clock.now() <= end:", "if value:"),
    "gives every call the full limit": ("timeout=remaining)", "timeout=CALL_SECONDS)"),
    "sleeps past the deadline": ("self.clock.sleep(min(every, end - self.clock.now()))", "self.clock.sleep(every)"),
    "prints the stopped reason": ("category(stopped_task.get('stoppedReason'), STOPPED_REASONS)", "stopped_task.get('stoppedReason')"),
    "prints the rollout reason": ('category(deployment.get("rolloutStateReason", ""), ROLLOUT_REASONS)', 'deployment.get("rolloutStateReason", "")'),
    "prints the RunTask reason": ('run_task_code(item.get("reason"))', 'str(item.get("reason"))'),
    "ignores a FAILED rollout": ('deployment.get("rolloutState") == FAILED_ROLLOUT', 'deployment.get("rolloutState") == "NEVER"'),
    "skips the registration wait": ('"host registration", ready,', '"host registration", lambda: True,'),
    "skips the capacity provider wait": ('active[0].get("capacityProviderName") != self.names.provider', "False"),
    "puts postgres last": ("key != FIRST_SERVICE", "key == FIRST_SERVICE"),
    "accepts a nonzero exit": ("code != 0 for code", "code == 99 for code"),
    "allows a second bootstrap": ("self.marker.exists() and not again", "False"),
    "bootstraps before services are stable": ("not self.services_stable(gate)", "False"),
    "never gives up on failed polls": ("failed >= FAILED_POLLS_ALLOWED", "failed >= 10**6"),
    "does not verify the termination": ('not group.get("Instances")', "True"),
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
        except cases.CASE_ERRORS:
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
