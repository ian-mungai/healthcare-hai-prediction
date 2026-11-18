"""Plant one defect at a time in ``host_session``, in the group signal test and in the reload check and require the cases to catch each.

Run: ``.venv/bin/python -m scripts.infrastructure.host_session_plants``. Each plant replaces one piece of a module's source in memory (the file stays
unchanged) and reruns the cases. A plant is caught when a check fails or a case raises. The reload plants load the planted ``online_reload`` in place of the
module and rerun the E34 reload cases. The run exits 1 when any plant survives or its text is not
found. The group signal plants load the planted test module in place of ``host_session_group_signal`` and rerun the terminal interrupt case.
"""

from __future__ import annotations

import contextlib
import importlib
import importlib.util
import io
import os
import re
import sys
import tempfile
from collections.abc import Callable
from pathlib import Path

MODULE = "scripts.infrastructure.host_session"
SOURCE = Path(__file__).with_name("host_session.py")
RELOAD_MODULE = "scripts.features.online_reload"
RELOAD_SOURCE = Path(__file__).parents[1] / "features" / "online_reload.py"
SIGNAL_MODULE = "scripts.infrastructure.host_session_group_signal"
SIGNAL_SOURCE = Path(__file__).with_name("host_session_group_signal.py")
ENTRY_SOURCE = "HOST_SESSION_ENTRY_SOURCE"  # host_session_entry_stand_in.SOURCE: the child process loads this file as host_session
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
    "stop skips the job task check": ("        else:\n            self.check_no_job_tasks(wait_seconds)\n", "        else:\n            pass\n"),
    "stop still checks after --force-stop": (
        '        if force_stop:\n            self.step("stop_forced"',
        '        if False:\n            self.step("stop_forced"',
    ),
    "stop reads a failed listing as no job task running": (
        "                except HostSessionError as error:\n                    failed += 1\n"
        "                    if failed >= FAILED_POLLS_ALLOWED or self.clock.now() >= end:",
        "                except HostSessionError as error:\n                    return\n                    failed += 1\n"
        "                    if failed >= FAILED_POLLS_ALLOWED or self.clock.now() >= end:",
    ),
    "stop never waits for a job task": ("end, failed = self.clock.now() + max(wait_seconds, 0), 0", "end, failed = self.clock.now(), 0"),
    "stop refuses at the first listing although it may wait": (
        "                    now = self.clock.now()\n                    if now >= end:",
        "                    now = self.clock.now()\n                    if True:",
    ),
    "stop gives up the wait without a refusal": (
        "                self.clock.sleep(min(POLL_SECONDS, end - self.clock.now()))\n        except (KeyboardInterrupt",
        "                return\n        except (KeyboardInterrupt",
    ),
    "stop turns the host off after an interrupted wait": (
        'run stop again")\n            raise',
        'run stop again")\n            self.host_off()\n            raise',
    ),
    "stop prints the task ARN": ('shown = f"{task.family} {task.suffix}"', 'shown = f"{task.family} {task.arn}"'),
    "stop after a failure waits for job tasks": (
        '        self.step(f"{command}_failed", "the host is left on" if keep_on else "stopping the host")',
        '        self.step(f"{command}_failed", "the host is left on" if keep_on else "stopping the host")\n        self.check_no_job_tasks(0)',
    ),
    "the command line runs a second copy of the module": ("    raise SystemExit(packaged_main())", "    raise SystemExit(main())"),
    "names carry no state serial": ("state_serial=services_state_serial(SERVICES_DIRECTORY),", "state_serial=None,"),
    "status raises on an absent service": (
        "        found, absent = self.lookup_services()\n        services = {",
        "        found, absent = self.service_states(), []\n        services = {",
    ),
    "any describe-services failure counts as an absent service": ('any(item.get("reason") != MISSING for item in described.get("failures", []))', "False"),
    "prints the task ARN": ('self.step(key, f"exit codes {exits}")', 'self.step(key, f"exit codes {exits} {task}")'),
    "reloads the online store before the services are stable": (
        "            self.deploy_services()\n            self.catalog_recovery()\n            self.mlflow_user()\n"
        "            self.reload_online_store(identity)",
        "            self.reload_online_store(identity)\n            self.catalog_recovery()\n            self.mlflow_user()\n"
        "            self.deploy_services()",
    ),
    "never reloads the online store": ("            self.reload_online_store(identity)\n", ""),
    "reloads only the first online set": ("for model in online_sets():", "for model in online_sets()[:1]:"),
    "ignores a failed reload": ('raise HostSessionError(f"online store reload of set {model} failed: {error}") from None', "pass"),
    "stops start for a skipped reload": (
        'self.step("online_store_reload_skipped", f"{model}: {outcome[\'reason\']}")',
        'raise HostSessionError(f"online store reload of set {model} failed: skipped")',
    ),
    "reports a skipped reload as reloaded": ('elif outcome["outcome"] == "skipped":', 'elif outcome["outcome"] == "never":'),
    "reports no set that a result confirmed as skipped": ("        if skipped:\n", "        if False:\n"),
    "reports no set whose outcome is unknown": ("        if unknown:\n", "        if False:\n"),
    "reports an unknown outcome as a confirmed skip": ("                unknown.append(model)\n", "                skipped.append(model)\n"),
    "reports an unreadable reload result as reloaded": (
        "            if outcome is None:\n                unknown.append(model)\n",
        "            if False:\n                unknown.append(model)\n",
    ),
    "takes the last result line": (
        "return results[0] if len(results) == 1 and complete_result(results[0]) else None",
        "return results[-1] if complete_result(results[-1]) else None",
    ),
    "takes the first of several result lines": (
        "return results[0] if len(results) == 1 and complete_result(results[0]) else None",
        "return results[0] if complete_result(results[0]) else None",
    ),
    "reads only the first page of a log": (
        "if not isinstance(following, str) or not following or following == token:",
        "if True:",
    ),
    "ends a log at an empty page": (
        "if not isinstance(following, str) or not following or following == token:",
        'if not isinstance(following, str) or not following or following == token or not page.get("events"):',
    ),
    "takes the pages read before the deadline as the whole log": (
        "                raise\n            events += page",
        "                return events\n            events += page",
    ),
    "takes a result line without the fields of its outcome": (
        "return results[0] if len(results) == 1 and complete_result(results[0]) else None",
        "return results[0] if len(results) == 1 else None",
    ),
    "takes a skipped result without its reason and held runs": (
        'return text("reason") and isinstance(held, list) and all(isinstance(run, str) and CODE_RUN_ID.fullmatch(run) for run in held)',
        "return True",
    ),
    "takes a loaded result without its run, base run and load ID": (
        'return text("load_id") and all(text(name) and CODE_RUN_ID.fullmatch(document[name]) for name in ("run_id", "base_run"))',
        "return True",
    ),
    "launches the reload without its phase": ('"LAYER_PHASE": RELOAD_PHASE,', '"LAYER_PHASE": "",'),
    "takes a running features task for the reload": (
        "if key.startswith(ONLINE_RELOAD) or key == CATALOG_SETUP:\n            return False",
        "if key.startswith(ONLINE_RELOAD):\n            return True\n        if key == CATALOG_SETUP:\n            return False",
    ),
    "reads the identity without the log": (
        'found = self.wait("code identity log", lambda: self.identity_line(task), CODE_IDENTITY_SECONDS, TASK_POLL_SECONDS)',
        'found = ("20260101T000000Z-00000000", "0" * 64)',
    ),
    "invents the identity of a host with no record": ("raise CodeMissing(missing) from None", 'return "20260101T000000Z-00000000", "0" * 64'),
    "never runs the mlflow user task": (
        "            self.mlflow_user()\n            self.reload_online_store(identity)",
        "            self.reload_online_store(identity)",
    ),
    "never writes the code record": ("        self.write_code_record(identity)\n        return identity", "        return identity"),
    "never reads the stored code record": ("            recorded = self.read_code_record()\n", "            recorded = None\n"),
    "never checks the postgres setup": ("return self.postgres_setup() and self.services_stable()", "return self.services_stable()"),
    "ignores the exit code of a setup container": ('if state.get("lastStatus") == "STOPPED" and state.get("exitCode") not in (0, None):', "if False:"),
    "treats an unfinished setup as complete": ("        return not unfinished", "        return True"),
    "counts a task of an older task definition": ('if current and task.get("taskDefinitionArn") not in (None, current):', "if False:"),
    "never runs the catalog recovery": ("            self.catalog_recovery()\n", ""),
    "recovers the catalog although nothing is owed": (
        '        if (restored is False and marker in ("absent", "off") and not local) '
        'or (not self.setup_task and marker != "present" and not local):\n            return\n',
        "",
    ),
    "ignores a pending record of an earlier start": (
        'if (restored is False and marker in ("absent", "off") and not local) or (not self.setup_task and marker != "present" and not local):',
        "if restored is False:",
    ),
    "never writes the pending marker": ('        if marker != "present" and not self.write_pending_marker():', "        if False:"),
    "recovers after a failed marker write": (
        '        if marker != "present" and not self.write_pending_marker():',
        '        if marker != "present" and not (self.write_pending_marker() or True):',
    ),
    "keeps no local record of the obligation": ('self.pending_file.write_text("catalog recovery pending\\n", encoding="utf-8")', "pass"),
    "ignores the local record": ("        local = self.pending_file.exists()", "        local = False"),
    "never clears the pending records": ("        self.clear_pending()\n", ""),
    "skips the recovery when the restore log cannot be read": (
        "        except HostSessionError:\n            return None\n        return any(SETUP_RESTORED_WORDS",
        "        except HostSessionError:\n            return False\n        return any(SETUP_RESTORED_WORDS",
    ),
    "skips the recovery when the marker cannot be read": ('return "absent" if "NoSuchKey" in str(error) else "unreadable"', 'return "absent"'),
    "parses the displayed cut of a catalog setup result": (
        "[:CATALOG_RESULT_LIMIT] for line in lines if CATALOG_LINE",
        "[:CATALOG_LINE_LIMIT] for line in lines if CATALOG_LINE",
    ),
    "returns the displayed text of a catalog setup result": (
        '            self.step("catalog_setup_result", shown(line))\n        return lines',
        '            self.step("catalog_setup_result", shown(line))\n        return [shown(line) for line in lines]',
    ),
    "ignores a failed catalog restore": (
        '        except HostSessionError as error:\n            raise HostSessionError(f"catalog restore failed: {error}") from None\n'
        "        counts = restore_counts(lines)",
        "        except HostSessionError:\n            return\n        counts = restore_counts(lines)",
    ),
    "runs another catalog setup step as the restore": (
        'self.catalog_setup("restore", seconds=CATALOG_RESTORE_SECONDS)',
        'self.catalog_setup("check", seconds=CATALOG_RESTORE_SECONDS)',
    ),
    "takes any result line as the restore reply": (
        '        raise HostSessionError("catalog restore failed: the task\'s result line is not the restore reply")\n    return counts',
        "        return {}\n    return counts",
    ),
}


# Each plant of the reload module weakens one check that lets a skip happen (the reload cases E34 must catch it).
RELOAD_PLANTS: dict[str, tuple[str, str]] = {
    "skips with an unresolved promotion": ("            production()\n", ""),
    "skips without checking the held run": ("            check_held_run(model, skipped[0], set_hash, holds)\n", ""),
    "takes a held run whose base run is no run ID": (
        '    if not problems and not reads.RUN_ID.fullmatch(hold["base_run"]):\n        problems = ["base_run"]\n',
        "",
    ),
    "takes an older release whose base run is no run ID": ('        if not reads.RUN_ID.fullmatch(record["base_run"]):\n', "        if False:\n"),
    "takes a held run without its base run": ('("base_run", "set_schema_sha256"))', '("set_schema_sha256",))'),
    "takes a held run on another schema": ('{"feature_set": model, "run_id": run, "set_schema_sha256": set_hash}', '{"feature_set": model, "run_id": run}'),
    "takes any text as the schema hash of a load record": (
        "    if not isinstance(recorded, str) or not SHA256.fullmatch(recorded):\n        return False\n",
        "",
    ),
    "takes any text as the schema hash of a release record": ('if not SHA256.fullmatch(record["set_schema_sha256"]):', "if False:"),
}


def reload_caught(source: str) -> bool:
    """True when the reload cases (E34) detect the planted ``online_reload`` source; the real module is put back afterwards."""
    package = importlib.import_module("scripts.features")
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "online_reload_planted.py"
        path.write_text(source, encoding="utf-8")
        spec = importlib.util.spec_from_file_location(RELOAD_MODULE, path)
        if spec is None or spec.loader is None:
            raise ImportError(RELOAD_MODULE)
        planted = importlib.util.module_from_spec(spec)
        sys.modules[RELOAD_MODULE] = planted
        try:
            spec.loader.exec_module(planted)
            cases = importlib.import_module("scripts.features.feast_cases")
            harness = importlib.import_module("scripts.features.feast_harness")
            selected = [name for name in cases.LOCAL_CASES if name.startswith("E34_")]
            with contextlib.redirect_stderr(io.StringIO()):
                report = harness.run_cases(cases.LOCAL_CASES, selected)
            return bool(report["verdict"] != "pass")
        finally:
            sys.modules.pop(RELOAD_MODULE, None)
            package.__dict__.pop("online_reload", None)
            importlib.import_module("scripts.features.online_reload")


def caught(source: str) -> bool:
    """True when the cases detect the planted source. The cases that start the command line as a child process load the planted file too
    (``HOST_SESSION_ENTRY_SOURCE``), and the file stays until the last case has run."""
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "host_session_planted.py"
        path.write_text(source, encoding="utf-8")
        spec = importlib.util.spec_from_file_location(MODULE, path)
        if spec is None or spec.loader is None:
            raise ImportError(MODULE)
        planted = importlib.util.module_from_spec(spec)
        sys.modules[MODULE] = planted
        spec.loader.exec_module(planted)
        os.environ[ENTRY_SOURCE] = str(path)
        try:
            importlib.reload(importlib.import_module("scripts.infrastructure.aws_tasks"))  # it must raise the planted module's HostSessionError
            importlib.reload(importlib.import_module("scripts.infrastructure.host_session_refresh_cases"))
            importlib.reload(importlib.import_module("scripts.infrastructure.host_session_stop_cases"))
            cases = importlib.import_module("scripts.infrastructure.host_session_cases")
            importlib.reload(cases)
            for function in cases.CASES.values():
                try:
                    if not all(function().values()):
                        return True
                except (*cases.CASE_ERRORS, KeyboardInterrupt, planted.TerminationRequested):
                    return True
            return False
        finally:
            os.environ.pop(ENTRY_SOURCE, None)


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
    reload_original = RELOAD_SOURCE.read_text(encoding="utf-8")
    for name, (old, new) in RELOAD_PLANTS.items():
        if old not in reload_original:
            sys.stdout.write(f"MISSING {name}\n")
            survivors += 1
            continue
        result = reload_caught(reload_original.replace(old, new, 1))
        sys.stdout.write(f"{'CAUGHT' if result else 'SURVIVED'} {name}\n")
        survivors += not result
    total = len(PLANTS) + len(SIGNAL_PLANTS) + len(RELOAD_PLANTS)
    sys.stdout.write(f"{total - survivors} of {total} plants caught\n")
    return 1 if survivors else 0


if __name__ == "__main__":
    raise SystemExit(main())
