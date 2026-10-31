"""Plant source-contract defects without starting workers or treating runtime defects as proved."""

from __future__ import annotations

import argparse
import ast
import hashlib
import importlib.util
import json
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.orchestration.sdk_runtime_cases import ROOT, SOURCE_PATHS, source_checks

SOURCES = SOURCE_PATHS


@dataclass(frozen=True)
class Mutation:
    """One exact source edit and the independently named assertion that must reject it."""

    name: str
    path: str
    before: str
    after: str
    check: str


MUTATIONS = (
    Mutation(
        "catalog_relay_sends_self_group_stop",
        "orchestration/catalog_transport_child.py",
        "def _relay(target_port: int, listen_port: int, deadline: float) -> None:\n",
        "def _relay(target_port: int, listen_port: int, deadline: float) -> None:\n    os.killpg(os.getpgrp(), signal.SIGKILL)\n",
        "orchestration/catalog_transport_child.py:signals_only_in_owned_stop_or_self_monitor",
    ),
    Mutation("sdk_import_in_facade", "orchestration/ecs_api.py", "import secrets", "import boto3\nimport secrets", "facade_has_no_sdk_or_async_call_path"),
    Mutation(
        "thread_import_in_facade", "orchestration/ecs_api.py", "import secrets", "import threading\nimport secrets", "facade_has_no_sdk_or_async_call_path"
    ),
    Mutation("async_import_in_facade", "orchestration/ecs_api.py", "import secrets", "import asyncio\nimport secrets", "facade_has_no_sdk_or_async_call_path"),
    Mutation(
        "pool_import_in_facade",
        "orchestration/ecs_api.py",
        "import secrets",
        "import concurrent.futures\nimport secrets",
        "facade_has_no_sdk_or_async_call_path",
    ),
    Mutation(
        "host_reap_outside_stop",
        "scripts/process_group.py",
        "def group_state(",
        "def extra_reap(handle):\n    os.waitpid(handle.pid, 0)\n\ndef group_state(",
        "scripts/process_group.py:only_guarded_stop_reaps",
    ),
    Mutation(
        "task_reap_outside_stop",
        "services/hai/bootstrap/supervisor.py",
        "def _members(",
        "def extra_reap(handle):\n    os.waitpid(handle.pid, 0)\n\ndef _members(",
        "services/hai/bootstrap/supervisor.py:only_guarded_stop_reaps",
    ),
    Mutation(
        "host_catch_all_reaper",
        "scripts/process_group.py",
        "def group_state(",
        "def extra_reap(handle):\n    os.waitpid(-1, 0)\n\ndef group_state(",
        "scripts/process_group.py:no_catch_all_reaper",
    ),
    Mutation(
        "forbidden_portable_entry_point",
        "scripts/process_group.py",
        "return str(_portable.group_state(group))",
        "return str(_portable.run(group))",
        "portable_helper_names_closed",
    ),
    Mutation(
        "recovery_sends_signal",
        "orchestration/worker_recovery.py",
        "def _utc(",
        "def recovered_signal(record):\n    os.killpg(record['pgid'], signal.SIGKILL)\n\ndef _utc(",
        "recovery:recovered_signal:no_signal_or_reap",
    ),
    Mutation(
        "removed_owner_stop_name_sends_signal",
        "orchestration/worker_recovery.py",
        "def _utc(",
        "def owner_stop_confirmed(record):\n    os.killpg(record['pgid'], 9)\n\ndef _utc(",
        "orchestration/worker_recovery.py:signals_only_in_owned_stop_or_self_monitor",
    ),
    Mutation(
        "removed_workers_command_name_sends_signal",
        "orchestration/worker_recovery.py",
        "def _utc(",
        "def workers_command(record):\n    os.killpg(record['pgid'], 9)\n\ndef _utc(",
        "orchestration/worker_recovery.py:signals_only_in_owned_stop_or_self_monitor",
    ),
    Mutation(
        "host_signal_outside_lock",
        "scripts/process_group.py",
        "def group_state(",
        "def extra_signal(handle):\n    _portable.signal_group(handle.pgid, signal.SIGKILL)\n\ndef group_state(",
        "scripts/process_group.py:signals_and_reaps_hold_handle_lock",
    ),
    Mutation(
        "task_signal_outside_lock",
        "services/hai/bootstrap/supervisor.py",
        "def _members(",
        "def extra_signal(handle):\n    _portable.signal_group(handle.pgid, signal.SIGKILL)\n\ndef _members(",
        "services/hai/bootstrap/supervisor.py:signals_and_reaps_hold_handle_lock",
    ),
    Mutation(
        "host_reaped_guard_removed",
        "scripts/process_group.py",
        'if handle.state == "read_only":',
        'if handle.state == "unreachable_state":',
        "scripts/process_group.py:reaped_state_refuses_signals",
    ),
    Mutation(
        "task_reaped_guard_removed",
        "services/hai/bootstrap/supervisor.py",
        'if handle.state == "read_only":',
        'if handle.state == "unreachable_state":',
        "services/hai/bootstrap/supervisor.py:reaped_state_refuses_signals",
    ),
    Mutation(
        "sweep_sends_signal",
        "orchestration/worker_recovery.py",
        "            _, observations = self.inspect(deadline=deadline, discover=True)",
        "            os.killpg(1, 9)\n            _, observations = self.inspect(deadline=deadline, discover=True)",
        "recovery:sweep:no_signal_or_reap",
    ),
    Mutation(
        "pool_catch_all_reaper",
        "orchestration/worker_pool.py",
        "MAX_ACTIVE = 4",
        "def extra_reap():\n    os.waitpid(-1, 0)\n\nMAX_ACTIVE = 4",
        "orchestration/worker_pool.py:no_catch_all_reaper",
    ),
    Mutation(
        "recovery_bare_killpg",
        "orchestration/worker_recovery.py",
        "            _, observations = self.inspect(deadline=deadline, discover=True)",
        "            from os import killpg as stop_group\n"
        "            stop_group(1, 9)\n            _, observations = self.inspect(deadline=deadline, discover=True)",
        "recovery:sweep:no_signal_or_reap",
    ),
    Mutation(
        "recovery_imported_signal_group",
        "orchestration/worker_recovery.py",
        "            _, observations = self.inspect(deadline=deadline, discover=True)",
        "            from scripts.process_group import signal_group\n"
        "            signal_group(1, 9)\n            _, observations = self.inspect(deadline=deadline, discover=True)",
        "recovery:sweep:no_signal_or_reap",
    ),
    Mutation(
        "pool_signal_outside_stop",
        "orchestration/worker_pool.py",
        '        """Retry only owned unreaped leaders. Read-only handles cannot signal anything."""',
        '        """Retry only owned unreaped leaders. Read-only handles cannot signal anything."""\n        os.killpg(1, 9)',
        "orchestration/worker_pool.py:signals_only_in_owned_stop_or_self_monitor",
    ),
    Mutation(
        "recovery_unused_signal_import",
        "orchestration/worker_recovery.py",
        "import os",
        "import os\nfrom os import killpg as unused_stop",
        "recovery:signal_imports_closed",
    ),
    Mutation(
        "host_bare_signal_before_lock",
        "scripts/process_group.py",
        "    previous_interrupts = signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGINT})",
        "    from os import killpg as stop_group\n    stop_group(handle.pgid, signal.SIGKILL)\n"
        "    previous_interrupts = signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGINT})",
        "scripts/process_group.py:signals_and_reaps_hold_handle_lock",
    ),
)


@dataclass(frozen=True)
class StopMutation:
    """A defect planted in the task supervisor's stop and the fake-backed stop case that must fail for it."""

    name: str
    before: str
    after: str
    check: str


SUPERVISOR = "services/hai/bootstrap/supervisor.py"
SUPERVISOR_SIBLINGS = ("services/hai/deadline_run.py", "orchestration/worker_protocol.py", "orchestration/worker_stderr.py")
RETRY_SLEEP = "time.sleep(min(0.1, max(0.0, deadline - time.monotonic())))\n"
STOP_MUTATIONS = (
    StopMutation(
        "task_stop_gives_up_on_a_failed_listing",
        f'                            reason = "listing_unavailable"\n                            {RETRY_SLEEP}                            continue\n',
        '                            return {"status": "survivor", "confirmed": False}\n',
        "task_transient_listing_failure_is_retried",
    ),
    StopMutation(
        "task_stop_gives_up_on_an_unreapable_leader",
        f'                    reason = "leader_not_reapable"\n                    {RETRY_SLEEP}',
        '                    return {"status": "survivor", "confirmed": False}\n',
        "task_zombie_leader_with_exiting_threads_is_retried",
    ),
    StopMutation(
        "task_stop_confirms_an_unproven_group_at_the_deadline",
        '"confirmed": False, "reason": reason}\n        finally:',
        '"confirmed": True, "reason": reason}\n        finally:',
        "task_persistent_listing_failure_is_unconfirmed_at_the_deadline",
    ),
    StopMutation(
        "task_stop_waits_past_its_deadline",
        "                while time.monotonic() < deadline:\n                    if not ready_to_reap:",
        "                while time.monotonic() < deadline + 100:\n                    if not ready_to_reap:",
        "task_unreapable_leader_is_unconfirmed_at_the_deadline",
    ),
)


def stop_mutation_rows(run_folder: Path) -> list[dict[str, Any]]:
    """Plant each stop defect in a copy of the supervisor and require the named fake-backed stop case to fail, after a passing baseline."""
    from scripts.orchestration.sdk_owned_cases import _retry_cases

    source = (ROOT / SUPERVISOR).read_text()

    def checks_of(text: str, label: str) -> dict[str, bool]:
        root = run_folder / label
        for relative in SUPERVISOR_SIBLINGS:
            (root / relative).parent.mkdir(parents=True, exist_ok=True)
            (root / relative).write_text((ROOT / relative).read_text())
        target = root / SUPERVISOR
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text)
        spec = importlib.util.spec_from_file_location(f"hai_stop_mutation_{label}", target)
        if spec is None or spec.loader is None:
            raise RuntimeError("stop_mutation_loader_unavailable")
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        return _retry_cases(module)

    baseline = checks_of(source, "stop_baseline")
    if not baseline or not all(baseline.values()):
        raise RuntimeError("stop_mutations_require_passing_baseline")
    rows = []
    for mutation in STOP_MUTATIONS:
        if source.count(mutation.before) != 1:
            raise ValueError("stop_mutation_location_changed")
        changed = source.replace(mutation.before, mutation.after, 1)
        ast.parse(changed)
        observed = checks_of(changed, mutation.name)
        rows.append(
            {
                "name": mutation.name,
                "source": SUPERVISOR,
                "assertion": mutation.check,
                "observed": observed.get(mutation.check),
                "caught": observed.get(mutation.check) is False,
                "mutant_sha256": hashlib.sha256(changed.encode()).hexdigest(),
                "proof": "fake-backed stop cases, not the Linux kernel",
            }
        )
    return rows


def run(output: Path) -> dict[str, Any]:
    """Require a passing baseline and an intended failed assertion for every planted source defect."""
    baseline = source_checks()
    if not baseline or not all(baseline.values()):
        raise RuntimeError("source_mutations_require_passing_baseline")
    run_folder = output.parent / "static_mutations" / output.stem
    run_folder.mkdir(parents=True, exist_ok=True)
    label = run_folder / "retention.json"
    if not label.exists():
        label.write_text(
            json.dumps(
                {
                    "class": 3,
                    "purpose": "Isolated source mutation payload",
                    "created": datetime.now(UTC).isoformat(),
                    "end_condition": "Mutation evidence and its review are accepted",
                },
                indent=2,
            )
            + "\n"
        )
    frozen = {path: (ROOT / path).read_text() for path in SOURCES}
    rows = []
    for mutation in MUTATIONS:
        folder = run_folder / mutation.name
        for relative, source in frozen.items():
            target = folder / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(source)
        source = frozen[mutation.path]
        if source.count(mutation.before) != 1:
            raise ValueError("static_mutation_location_changed")
        changed = source.replace(mutation.before, mutation.after, 1)
        ast.parse(changed)
        (folder / mutation.path).write_text(changed)
        observed = source_checks(folder)
        caught = mutation.check in observed and observed[mutation.check] is False
        rows.append(
            {
                "name": mutation.name,
                "source": mutation.path,
                "assertion": mutation.check,
                "observed": observed.get(mutation.check),
                "caught": caught,
                "mutant_sha256": hashlib.sha256(changed.encode()).hexdigest(),
                "saved_source": str(folder / mutation.path),
                "proof": "source contract only",
            }
        )
    case_source = (ROOT / "scripts/orchestration/sdk_runtime_cases.py").read_text()
    if case_source.count("    return checks\n") != 1:
        raise ValueError("case_mutation_location_changed")
    changed_case = case_source.replace("    return checks\n", "    return {name: True for name in checks}\n", 1)
    case_file = run_folder / "test_discards_failures.py"
    case_file.write_text(changed_case)
    spec = importlib.util.spec_from_file_location("hai_static_case_mutation", case_file)
    if spec is None or spec.loader is None:
        raise RuntimeError("static_case_loader_unavailable")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    bad_subject = run_folder / "sdk_import_in_facade"
    normal_rejects = source_checks(bad_subject)["facade_has_no_sdk_or_async_call_path"] is False
    mutant_accepts = module.source_checks(bad_subject)["facade_has_no_sdk_or_async_call_path"] is True
    rows.append(
        {
            "name": "test_discards_failures",
            "assertion": "known_bad_subject_must_fail",
            "caught": normal_rejects and mutant_accepts,
            "normal_checker_rejects": normal_rejects,
            "mutated_checker_incorrectly_accepts": mutant_accepts,
            "saved_source": str(case_file),
            "proof": "test sensitivity only",
        }
    )
    rows.extend(stop_mutation_rows(run_folder))
    unchanged = all((ROOT / path).read_text() == source for path, source in frozen.items())
    return {
        "classification": "Synthetic",
        "baseline": baseline,
        "mutations": rows,
        "failed": [row["name"] for row in rows if not row["caught"]],
        "production_sources_unchanged": unchanged,
        "source_sha256": {path: hashlib.sha256(source.encode()).hexdigest() for path, source in frozen.items()},
        "checker_sha256": hashlib.sha256(case_source.encode()).hexdigest(),
        "runtime_mutations": "stop mutants ran against fake observations only; process and Linux mutants pending lead execution",
        "limits": ["No process, HTTP, terminal, socket, descendant or Linux mutation executed", "A syntax or import failure never counts as a caught mutation"],
        "cleanup": "done; source copies retained as local evidence",
    }


def main() -> int:
    """Write a repeatable source-only mutation report."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    report = run(args.output)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    sys.stdout.write(json.dumps({"report": str(args.output), "failed": report["failed"], "mutations": len(report["mutations"])}) + "\n")
    return int(bool(report["failed"]) or not report["production_sources_unchanged"])


if __name__ == "__main__":
    sys.exit(main())
