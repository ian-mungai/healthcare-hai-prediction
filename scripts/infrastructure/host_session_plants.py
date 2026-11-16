"""Plant one defect at a time in ``host_session`` and in the group signal test and require the cases to catch each.

Run: ``.venv/bin/python -m scripts.infrastructure.host_session_plants``. Each plant replaces one piece of a module's source in memory (the file stays
unchanged) and reruns the cases. A plant is caught when a check fails or a case raises. The run exits 1 when any plant survives or its text is not
found. The group signal plants load the planted test module in place of ``host_session_group_signal`` and rerun the terminal interrupt case.
"""

from __future__ import annotations

import importlib
import importlib.util
import re
import sys
import tempfile
from collections.abc import Callable
from pathlib import Path

MODULE = "scripts.infrastructure.host_session"
SOURCE = Path(__file__).with_name("host_session.py")
SIGNAL_MODULE = "scripts.infrastructure.host_session_group_signal"
SIGNAL_SOURCE = Path(__file__).with_name("host_session_group_signal.py")
KILL_AFTER_REAP = ("with contextlib.suppress(ProcessLookupError, PermissionError):", "    os.killpg(driver.pid, signal.SIGKILL)")


def insert_before(pattern: str, lines: tuple[str, ...], last: bool = False) -> Callable[[str], str]:
    """A plant that inserts ``lines`` before the first (or last) source line that matches ``pattern``, at the indentation of that line."""

    def plant(source: str) -> str:
        found = list(re.finditer(rf"^(?P<indent>[ ]*){pattern}", source, flags=re.MULTILINE))
        if not found:
            return source
        chosen = found[-1 if last else 0]
        return source[: chosen.start()] + "".join(f"{chosen['indent']}{line}\n" for line in lines) + source[chosen.start() :]

    return plant


def replace_once(old: str, new: str) -> Callable[[str], str]:
    """A plant that replaces the first occurrence of ``old`` with ``new``."""
    return lambda source: source.replace(old, new, 1)


# Each plant of the group signal test restores a signal that follows the reap: in the cleanup of the caller after the driver was supervised (the
# place where the first version of the test sent it), at the end of the case, and in the cleanup of the driver after its own reap.
SIGNAL_PLANTS: dict[str, Callable[[str], str]] = {
    "kills the group in the caller after the driver was supervised": insert_before(r"status = driver\.status$", KILL_AFTER_REAP),
    "kills the group at the end of the case": insert_before(r"return facts$", KILL_AFTER_REAP),
    "kills the group in the cleanup of the supervising helper": insert_before(r"return driver$", KILL_AFTER_REAP),
    "kills the group between the driver runs": insert_before(r"facts: dict\[str, Any\] = \{$", KILL_AFTER_REAP),
    "kills the group after the cleanup reap": replace_once(
        "        if self.reap():\n            return\n", "        if self.reap():\n            os.killpg(self.pid, signal.SIGKILL)\n            return\n"
    ),
}

PLANTS: dict[str, tuple[str, str]] = {
    "drops tag propagation": ('"TASK_DEFINITION"', '"NONE"'),
    "starts a second one-shot task": ("        if running:\n", "        if False:\n"),
    "reuses a running task of another run or digest": ("        if running and not self.same_code(key, running, overrides):\n", "        if False:\n"),
    "launches before the running task of other code stopped": (
        'self.wait(f"{key} running task stop", lambda: self.all_stopped(key, running), seconds, TASK_POLL_SECONDS)',
        "pass",
    ),
    "runs the one-shot's calls without its deadline": (
        "        self.deadline = end if outer is None else min(end, outer)\n        try:\n            return self.run_one_shot(",
        "        try:\n            return self.run_one_shot(",
    ),
    "compares only the run ID": ('CODE_ENVIRONMENT = ("CODE_RUN_ID", "CODE_SHA256", "CODE_MODE")', 'CODE_ENVIRONMENT = ("CODE_RUN_ID", "CODE_MODE")'),
    "compares only the digest": ('CODE_ENVIRONMENT = ("CODE_RUN_ID", "CODE_SHA256", "CODE_MODE")', 'CODE_ENVIRONMENT = ("CODE_SHA256", "CODE_MODE")'),
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
    "skips host off after an interrupted catalog drain": (
        "        try:\n            self.openmetadata_stop()\n        finally:\n            with deferred_signals(self.step):\n                self.host_off()",
        "        self.openmetadata_stop()\n        self.host_off()",
    ),
    "lets a signal cut the host off after a catalog drain": (
        "        finally:\n            with deferred_signals(self.step):\n                self.host_off()\n\n    def stop_after_failure",
        "        finally:\n            self.host_off()\n\n    def stop_after_failure",
    ),
    "keeps every aws call in the session of the launched command": ("run_argv(*args, own_session=True, **kwargs)", "run_argv(*args, **kwargs)"),
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
    "accepts any capacity provider": ('active[0].get("capacityProviderName") != self.names.provider', 'not active[0].get("capacityProviderName")'),
    "puts postgres last": ("key != FIRST_SERVICE", "key == FIRST_SERVICE"),
    "accepts a nonzero exit": ("code != 0 for code", "code == 99 for code"),
    "allows a second bootstrap": ("self.marker.exists() and not again", "False"),
    "bootstraps before services are stable": ("not self.services_stable(gate)", "False"),
    "never gives up on failed polls": ("failed >= FAILED_POLLS_ALLOWED", "failed >= 10**6"),
    "does not verify the termination": ('not group.get("Instances")', "True"),
    "ignores pending tasks": (' and not service.get("pendingCount")', ""),
    "does not force the deployment": ('"--force-new-deployment"]', '"--no-force-new-deployment"]'),
    "refresh ignores a running job task": ("        if tasks:\n            shown", "        if False:\n            shown"),
    "refresh stops the host after a failure": (
        "            raise\n\n    def bootstrap(self, again",
        "            self.host_off()\n            raise\n\n    def bootstrap(self, again",
    ),
    "refresh accepts an instance outside the capacity provider": (
        '        if active[0].get("capacityProviderName") != self.names.provider:\n            raise HostSessionError("refresh refused',
        '        if False:\n            raise HostSessionError("refresh refused',
    ),
    "refresh turns the host on": (
        '        self.step("services_state", self.state_report())\n        self.check_refreshable(code)',
        '        self.host_on()\n        self.step("services_state", self.state_report())\n        self.check_refreshable(code)',
    ),
    "refresh does not print the state serial": (
        '        self.step("services_state", self.state_report())\n        self.check_refreshable(code)',
        "        self.check_refreshable(code)",
    ),
    "refresh redeploys a service that ECS does not have": (
        '        if absent:\n            raise HostSessionError(\n                f"refresh refused',
        '        if False:\n            raise HostSessionError(\n                f"refresh refused',
    ),
    "refresh accepts code the stack cannot place": (
        "import TaskCleanup\n\n        if code is not None and CODE_FETCH not in self.names.one_shot:\n            raise",
        "import TaskCleanup\n\n        if False:\n            raise",
    ),
    "refresh drops its code option": ('arguments.command in {"start", "refresh"} and', 'arguments.command == "start" and'),
    "names carry no state serial": ("state_serial=services_state_serial(SERVICES_DIRECTORY),", "state_serial=None,"),
    "status raises on an absent service": (
        "        found, absent = self.lookup_services()\n        services = {",
        "        found, absent = self.service_states(), []\n        services = {",
    ),
    "any describe-services failure counts as an absent service": ('any(item.get("reason") != MISSING for item in described.get("failures", []))', "False"),
    "prints the task ARN": ('self.step(key, f"exit codes {exits}")', 'self.step(key, f"exit codes {exits} {task}")'),
    "reloads the online store before the services are stable": (
        "            self.deploy_services()\n            self.reload_online_store(identity)",
        "            self.reload_online_store(identity)\n            self.deploy_services()",
    ),
    "never reloads the online store": ("            self.reload_online_store(identity)\n", ""),
    "reloads only the first online set": ("for model in online_sets():", "for model in online_sets()[:1]:"),
    "ignores a failed reload": ('raise HostSessionError(f"online store reload of set {model} failed: {error}") from None', "pass"),
    "launches the reload without its phase": ('"LAYER_PHASE": RELOAD_PHASE,', '"LAYER_PHASE": "",'),
    "takes a running features task for the reload": (
        "if key.startswith(ONLINE_RELOAD):\n            return False",
        "if key.startswith(ONLINE_RELOAD):\n            return True",
    ),
    "reads the identity without the log": (
        'found = self.wait("code identity log", lambda: self.identity_line(task), CODE_IDENTITY_SECONDS, TASK_POLL_SECONDS)',
        'found = ("20260101T000000Z-00000000", "0" * 64)',
    ),
    "invents the identity of a host with no record": ("raise HostSessionError(missing) from None", 'return "20260101T000000Z-00000000", "0" * 64'),
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
    importlib.reload(importlib.import_module("scripts.infrastructure.host_session_refresh_cases"))
    cases = importlib.import_module("scripts.infrastructure.host_session_cases")
    importlib.reload(cases)
    for function in cases.CASES.values():
        try:
            if not all(function().values()):
                return True
        except (*cases.CASE_ERRORS, KeyboardInterrupt, planted.TerminationRequested):
            return True
    return False


def signal_plant_caught(source: str) -> bool:
    """True when the terminal interrupt case detects the planted group signal test."""
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "host_session_group_signal_planted.py"
        path.write_text(source, encoding="utf-8")
        spec = importlib.util.spec_from_file_location(SIGNAL_MODULE, path)
        if spec is None or spec.loader is None:
            raise ImportError(SIGNAL_MODULE)
        planted = importlib.util.module_from_spec(spec)
        sys.modules[SIGNAL_MODULE] = planted
        spec.loader.exec_module(planted)
    package = importlib.import_module("scripts.infrastructure")
    names = vars(package)
    original = names.get("host_session_group_signal")
    names["host_session_group_signal"] = planted
    try:
        cases = importlib.import_module("scripts.infrastructure.host_session_cases")
        importlib.reload(cases)
        try:
            return not all(cases.CASES["terminal_interrupt_during_host_off"]().values())
        except (*cases.CASE_ERRORS, KeyboardInterrupt, OSError):
            return True
    finally:
        names["host_session_group_signal"] = original
        sys.modules.pop(SIGNAL_MODULE, None)
        importlib.reload(importlib.import_module("scripts.infrastructure.host_session_cases"))


def main() -> int:
    """Run every plant and return 1 when any survives."""
    original = SOURCE.read_text(encoding="utf-8")
    signal_original = SIGNAL_SOURCE.read_text(encoding="utf-8")
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
    for name, plant in SIGNAL_PLANTS.items():
        planted = plant(signal_original)
        if planted == signal_original:
            sys.stdout.write(f"MISSING {name}\n")
            survivors += 1
            continue
        result = signal_plant_caught(planted)
        sys.stdout.write(f"{'CAUGHT' if result else 'SURVIVED'} {name}\n")
        survivors += not result
    total = len(PLANTS) + len(SIGNAL_PLANTS)
    sys.stdout.write(f"{total - survivors} of {total} plants caught\n")
    return 1 if survivors else 0


if __name__ == "__main__":
    raise SystemExit(main())
