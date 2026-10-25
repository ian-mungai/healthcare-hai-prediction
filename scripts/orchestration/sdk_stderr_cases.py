"""Exercise reader failures through real workers and every owning cleanup path."""

from __future__ import annotations

import argparse
import array
import contextlib
import importlib.util
import json
import os
import shutil
import socket
import sys
import tempfile
import threading
import time
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any
from unittest.mock import patch

from orchestration.ecs_api import EcsApi, WorkerConfig
from orchestration.worker_stderr import StderrReader
from scripts.orchestration.sdk_standins import Scenario, WorkerStandin
from scripts.process import TimeoutExpired, run_command
from scripts.process_group import group_state, process_snapshot

ROOT = Path(__file__).resolve().parents[2]
PARAMS = {"cluster": "synthetic", "tasks": ["synthetic_task"], "include": ["TAGS"]}
FILE_CAP = 8 * 1024 * 1024
TRUNCATION_MARKER = b"\n[stderr truncated]\n"


class HeldStderr:
    """Retain the worker's actual stderr writer until the case explicitly releases it."""

    def __init__(self) -> None:
        self.folder = Path(tempfile.mkdtemp(prefix=".b_", dir=ROOT))
        self.path = self.folder / "s"
        self.listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.listener.bind(str(self.path))
        self.listener.listen(1)
        self.listener.settimeout(0.05)
        self.ready = threading.Event()
        self.stop = threading.Event()
        self.writer: int | None = None
        self.failure: str | None = None
        self.thread = threading.Thread(target=self._receive, name="synthetic_stderr_holder")

    def _receive(self) -> None:
        deadline = time.monotonic() + 5
        try:
            while not self.stop.is_set() and time.monotonic() < deadline:
                try:
                    peer, _ = self.listener.accept()
                except TimeoutError:
                    continue
                with peer:
                    peer.settimeout(max(0.01, deadline - time.monotonic()))
                    _, ancillary, _, _ = peer.recvmsg(1, socket.CMSG_SPACE(array.array("i").itemsize))
                    for level, kind, data in ancillary:
                        if level == socket.SOL_SOCKET and kind == socket.SCM_RIGHTS:
                            descriptors = array.array("i")
                            descriptors.frombytes(data[: array.array("i").itemsize])
                            self.writer = descriptors[0]
                            self.ready.set()
                            return
                raise ValueError("synthetic_stderr_writer_missing")
            self.failure = "synthetic_stderr_holder_deadline"
        except (OSError, ValueError):
            self.failure = "synthetic_stderr_holder_failed"

    def __enter__(self) -> HeldStderr:
        self.thread.start()
        return self

    def release(self) -> None:
        """Close only the duplicate writer received from the case's worker."""
        if self.writer is not None:
            os.close(self.writer)
            self.writer = None

    def __exit__(self, *_: object) -> None:
        self.stop.set()
        self.thread.join(timeout=1)
        self.release()
        self.listener.close()
        if self.thread.is_alive():
            raise RuntimeError("synthetic_stderr_holder_survived")
        shutil.rmtree(self.folder)

    def prelude(self) -> str:
        """Return synthetic child setup that transfers its writer without another process."""
        return (
            "import array, socket\n"
            "with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as peer:\n"
            "    peer.settimeout(3)\n"
            f"    peer.connect({str(self.path)!r})\n"
            "    peer.sendmsg([b'x'], [(socket.SOL_SOCKET, socket.SCM_RIGHTS, array.array('i', [2]))])\n"
        )


def _worker(folder: Path, *, prelude: str = "", body: bytes = b"", gate: Path | None = None) -> Path:
    script = folder / "worker.py"
    wait = f"until = time.monotonic() + 5\nwhile not os.path.exists({str(gate)!r}) and time.monotonic() < until: time.sleep(0.01)\n" if gate is not None else ""
    script.write_text(
        "import importlib.util, os, time\n"
        f"spec = importlib.util.spec_from_file_location('frames', {str(ROOT / 'orchestration/worker_protocol.py')!r})\n"
        "protocol = importlib.util.module_from_spec(spec)\nspec.loader.exec_module(protocol)\n"
        + prelude
        + "request = protocol.read_frame(0, time.monotonic() + 5)\n"
        + f"body = {body!r}\nwhile body:\n    count = os.write(2, body[:65536])\n    body = body[count:]\n"
        + wait
        + "protocol.write_frame(1, {'id': request['id'], 'ok': True, 'result': {'tasks': [], 'failures': []}}, time.monotonic() + 3)\n"
        + "protocol.read_frame(0, time.monotonic() + 5)\n"
    )
    return script.resolve()


def _join_readers(handles: Any) -> bool:
    deadline = time.monotonic() + 1
    for handle in handles:
        reader = handle.stderr_reader
        if reader is not None:
            reader.finish(deadline=deadline)
            if reader._thread.ident is not None:
                reader._thread.join(timeout=max(0.0, deadline - time.monotonic()))
    if not all(handle.stderr_reader is not None and not handle.stderr_reader.is_alive and group_state(handle.pgid) == "gone" for handle in handles):
        raise RuntimeError("synthetic_stderr_cleanup_unconfirmed")
    return True


def idle_case(folder: Path) -> dict[str, Any]:
    """Keep an aged pooled reader live, then prove retirement and next-session admission."""
    registry = folder / "idle_registry"
    checks: dict[str, bool] = {}
    with (
        patch("orchestration.worker_pool.MAX_AGE_SECONDS", 5.0),
        WorkerStandin(Scenario("describe_tasks", "normal", b'{"tasks":[],"failures":[]}')) as peer,
    ):
        api = EcsApi(WorkerConfig("synthetic", test_endpoint=peer.endpoint), registry)
        try:
            first = api.call("describe_tasks", PARAMS, deadline=time.monotonic() + 4)
            handle = api.handles[0]
            reader = handle.stderr_reader
            until = time.monotonic() + 15.6
            while time.monotonic() < until:
                time.sleep(min(0.05, max(0.0, until - time.monotonic())))
            checks["aged_idle_reader_stays_live"] = reader is not None and reader.is_alive and reader.failure is None
            checks["idle_does_not_pause_dispatch"] = not api.dispatch_paused
            second = api.call("describe_tasks", PARAMS, deadline=time.monotonic() + 4)
            checks["aged_worker_is_replaced"] = first["ok"] and second["ok"] and len(api.handles) == 2
            checks["recycle_does_not_pause_dispatch"] = not api.dispatch_paused
        finally:
            cleaned = api.close(deadline=time.monotonic() + 5)
        checks["aged_pool_close_confirmed"] = cleaned["confirmed"]
        checks["aged_pool_groups_absent"] = _join_readers(api.handles)
        checks["aged_pool_ledger_closed"] = not api.ledger.open_entries(deadline=time.monotonic() + 1)
        fresh = EcsApi(WorkerConfig("synthetic", test_endpoint=peer.endpoint), registry)
        try:
            checks["next_session_dispatch_available"] = not fresh.dispatch_paused
        finally:
            checks["next_session_cleanup_confirmed"] = fresh.close(deadline=time.monotonic() + 5)["confirmed"]
    return {"case": "aged_idle_reader", "checks": checks}


def owner_exit_case(folder: Path) -> dict[str, Any]:
    """Require interpreter exit without close to end its real pooled worker within fixed deadlines."""
    scratch = Path(tempfile.mkdtemp(prefix="owner_exit_", dir=folder))
    state_path = scratch / "owner_state.json"
    started = time.monotonic()
    returncode = None
    timed_out = False
    with WorkerStandin(Scenario("describe_tasks", "normal", b'{"tasks":[],"failures":[]}')) as peer:
        try:
            done = run_command(
                sys.executable,
                ["-B", "-m", "scripts.orchestration.sdk_stderr_cases", "--endpoint", peer.endpoint, "--folder", str(scratch)],
                cwd=ROOT,
                env={"PATH": os.defpath, "TMPDIR": str(scratch), "PYTHONDONTWRITEBYTECODE": "1"},
                timeout=6,
            )
            returncode = done.returncode
        except TimeoutExpired:
            timed_out = True
        owner_elapsed = time.monotonic() - started
        state = json.loads(state_path.read_text()) if state_path.exists() else {}
        pgid = state.get("worker_pgid")
        until = time.monotonic() + 3
        while isinstance(pgid, int) and group_state(pgid) != "gone" and time.monotonic() < until:
            time.sleep(min(0.02, max(0.0, until - time.monotonic())))
        worker_gone = isinstance(pgid, int) and group_state(pgid) == "gone"
        if isinstance(pgid, int) and not worker_gone:
            raise RuntimeError("owner_exit_worker_cleanup_unconfirmed")
    return {
        "case": "owner_exit_without_close",
        "checks": {
            "owner_reached_live_pooled_worker": all(state.get(name) is True for name in ("reply_ok", "pooled_idle", "worker_alive", "reader_alive")),
            "owner_exits_within_deadline": not timed_out and owner_elapsed < 6,
            "owner_exits_successfully": returncode == 0,
            "worker_group_absent_after_owner_exit": worker_gone,
        },
        "owner_elapsed_s": owner_elapsed,
        "owner_deadline_s": 6,
        "worker_cleanup_deadline_s": 3,
        "owner_returncode": returncode,
        "owner_timed_out": timed_out,
        "observed": state,
    }


def unfinished_pool_case(folder: Path) -> dict[str, Any]:
    """Require EOF even when the owned process group has already disappeared."""
    scratch = Path(tempfile.mkdtemp(prefix="unfinished_pool_", dir=folder))
    checks: dict[str, bool] = {}
    with HeldStderr() as held:
        api = EcsApi(WorkerConfig("synthetic", worker=_worker(scratch, prelude=held.prelude()), test_endpoint="http://127.0.0.1:1"), scratch / "registry")
        try:
            reply = api.call("describe_tasks", PARAMS, deadline=time.monotonic() + 4)
            checks["unfinished_worker_reached_reply"] = reply["ok"] and held.ready.wait(timeout=1)
            reader = api.handles[0].stderr_reader
            started = time.monotonic()
            cleaned = api.close(deadline=started + 0.4)
            checks["pool_refuses_unfinished_reader"] = not cleaned["confirmed"]
            checks["unfinished_reader_does_not_claim_completion"] = reader is not None and not reader.complete
            checks["reader_finish_refuses_missing_eof"] = reader is not None and not reader.finish(deadline=time.monotonic() + 0.1)
            checks["unfinished_reader_pauses_dispatch"] = api.dispatch_paused
            checks["unfinished_pool_keeps_ledger_open"] = bool(api.ledger.open_entries(deadline=time.monotonic() + 1))
            checks["unfinished_pool_cleanup_bounded"] = time.monotonic() <= started + 1.5
        finally:
            held.release()
            api.close(deadline=time.monotonic() + 2)
            checks["physical_cleanup_confirmed"] = _join_readers(api.handles)
    return {"case": "unfinished_pool_stderr", "checks": checks}


def write_failure_case(folder: Path) -> dict[str, Any]:
    """Observe a real destination error before allowing the worker to return its reply."""
    scratch = Path(tempfile.mkdtemp(prefix="write_failure_", dir=folder))
    gate = scratch / "reply_allowed"
    api = EcsApi(
        WorkerConfig("synthetic", worker=_worker(scratch, body=b"x" * 65536 + b"synthetic diagnostic", gate=gate), test_endpoint="http://127.0.0.1:1"),
        scratch / "registry",
    )
    original = StderrReader.start

    def failed_destination(reader: StderrReader) -> None:
        os.close(reader._output)
        reader._output = os.open(reader.path, os.O_RDONLY)
        original(reader)

    checks: dict[str, bool] = {}
    try:
        with patch.object(StderrReader, "start", failed_destination), ThreadPoolExecutor(max_workers=1) as executor:
            call = executor.submit(api.call, "describe_tasks", PARAMS, deadline=time.monotonic() + 5)
            until = time.monotonic() + 3
            while time.monotonic() < until:
                if api.handles and api.handles[0].stderr_reader is not None and api.handles[0].stderr_reader.failure is not None:
                    break
                time.sleep(0.01)
            reader = api.handles[0].stderr_reader if api.handles else None
            checks["destination_write_failed"] = reader is not None and reader.failure == "worker_stderr_write_failed"
            checks["reader_failure_pauses_before_release"] = api.dispatch_paused and not api.pool.failed
            gate.write_text("reply allowed\n")
            reply = call.result(timeout=5)
            checks["failed_reader_is_not_reused"] = all(worker.status != "idle" for worker in api.pool.workers)
            checks["failed_reader_cleanup_not_success"] = reply.get("error", {}).get("code") == "worker_cleanup_unconfirmed"
    finally:
        gate.touch(exist_ok=True)
        api.close(deadline=time.monotonic() + 3)
        checks["physical_cleanup_confirmed"] = _join_readers(api.handles)
    return {"case": "stderr_destination_failure", "checks": checks}


def file_cap_case(folder: Path) -> dict[str, Any]:
    """Drain beyond the fixed disk budget while preserving one truncation marker."""
    scratch = Path(tempfile.mkdtemp(prefix="stderr_cap_", dir=folder))
    payload = b"x" * (FILE_CAP + 3 * 65536)
    api = EcsApi(WorkerConfig("synthetic", worker=_worker(scratch, body=payload), test_endpoint="http://127.0.0.1:1"), scratch / "registry")
    try:
        reply = api.call("describe_tasks", PARAMS, deadline=time.monotonic() + 5)
    finally:
        cleaned = api.close(deadline=time.monotonic() + 5)
    reader = api.handles[0].stderr_reader
    evidence = reader.snapshot() if reader is not None else {}
    data = reader.path.read_bytes() if reader is not None else b""
    physical = _join_readers(api.handles)
    return {
        "case": "stderr_file_cap",
        "checks": {
            "large_stderr_does_not_block_reply": reply["ok"],
            "stderr_input_drained_past_cap": evidence.get("bytes_read") == len(payload),
            "sanitized_file_within_cap": len(data) <= FILE_CAP,
            "one_truncation_marker": data.count(TRUNCATION_MARKER) == 1 and data.endswith(TRUNCATION_MARKER),
            "snapshot_reports_truncation": evidence.get("truncated") is True,
            "cleanup_confirmed": cleaned["confirmed"] and physical,
        },
        "reader": evidence,
    }


def unfinished_transport_case(folder: Path) -> dict[str, Any]:
    """Exercise the real relay close while another owner retains its stderr writer."""
    from orchestration.catalog_transport import CatalogTransport
    from scripts.process_group import spawn_owned

    scratch = Path(tempfile.mkdtemp(prefix="unfinished_transport_", dir=folder))
    checks: dict[str, bool] = {}
    with HeldStderr() as held:
        script = scratch / "child.py"
        script.write_text((ROOT / "orchestration/catalog_transport_child.py").read_text().replace("import argparse\n", held.prelude() + "import argparse\n", 1))
        (scratch / "worker_protocol.py").write_bytes((ROOT / "orchestration/worker_protocol.py").read_bytes())
        (scratch / "sdk_worker.schema.json").write_bytes((ROOT / "data_contracts/orchestration/sdk_worker.schema.json").read_bytes())

        def start(command: Any, *args: Any, **kwargs: Any) -> Any:
            command = [*command]
            command[3] = str(script)
            return spawn_owned(command, *args, **kwargs)

        transport = CatalogTransport(scratch / "registry")
        try:
            with patch("orchestration.catalog_transport.spawn_owned", start):
                transport.start_relay(1, deadline=time.monotonic() + 5)
            checks["unfinished_relay_started"] = held.ready.wait(timeout=1)
            reader = transport.handles[0].stderr_reader
            checks["transport_reader_finish_refuses_missing_eof"] = reader is not None and not reader.finish(deadline=time.monotonic() + 0.15)
            if reader is not None:
                reader._thread.join(timeout=0.2)
            started = time.monotonic()
            checks["transport_refuses_unfinished_reader"] = not transport.close(deadline=started + 1)
            checks["transport_unfinished_close_bounded"] = time.monotonic() < started + 1.5
            checks["transport_unfinished_pauses_dispatch"] = transport.dispatch_paused
        finally:
            held.release()
            transport.close(deadline=time.monotonic() + 2)
            checks["physical_cleanup_confirmed"] = _join_readers(transport.handles)
    return {"case": "unfinished_transport_stderr", "checks": checks}


@contextlib.contextmanager
def _supervisor_observation(module: Any) -> Iterator[None]:
    if os.uname().sysname == "Linux":
        yield
        return

    def members(group: int, deadline: float) -> dict[int, str] | None:
        observed = process_snapshot(deadline)
        return {member.pid: member.state for member in observed.group(group)} if observed.status == "seen" else None

    with patch.object(module, "_members", members):
        yield


def unfinished_supervisor_case(folder: Path) -> dict[str, Any]:
    """Reach supervisor finish with a real child and an unfinished diagnostic stream."""
    scratch = Path(tempfile.mkdtemp(prefix="unfinished_supervisor_", dir=folder))
    spec = importlib.util.spec_from_file_location("hai_reader_supervisor", ROOT / "services/hai/bootstrap/supervisor.py")
    if spec is None or spec.loader is None:
        raise RuntimeError("synthetic_supervisor_loader_unavailable")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    checks: dict[str, bool] = {}
    with HeldStderr() as held, _supervisor_observation(module):
        supervisor = module.Supervisor("synthetic", worker=_worker(scratch, prelude=held.prelude()), stderr_folder=scratch / "stderr")
        try:
            handle = supervisor._spawn(time.monotonic() + 5, withhold=False)
            checks["supervisor_writer_retained"] = held.ready.wait(timeout=2)
            started = time.monotonic()
            outcome = supervisor._finish(handle, started + 0.4)
            checks["supervisor_refuses_unfinished_reader"] = outcome.get("confirmed") is False and outcome.get("status") == "worker_stderr_unconfirmed"
            checks["supervisor_unfinished_close_bounded"] = time.monotonic() < started + 1.5
        finally:
            held.release()
            supervisor.close(deadline=time.monotonic() + 3)
            checks["physical_cleanup_confirmed"] = _join_readers(supervisor.handles)
    return {"case": "unfinished_supervisor_stderr", "checks": checks, "portable_process_observation": os.uname().sysname != "Linux"}


def failure_cases(folder: Path) -> list[dict[str, Any]]:
    """Cover missing EOF and real output failure without accepting a cleanup claim."""
    return [
        unfinished_pool_case(folder),
        write_failure_case(folder),
        unfinished_transport_case(folder),
        unfinished_supervisor_case(folder),
        owner_exit_case(folder),
    ]


def executable_cases(folder: Path) -> list[dict[str, Any]]:
    """Refuse an unrelated executable at both real process-start boundaries."""
    from orchestration.catalog_transport import CatalogTransport

    rows = []
    for owner in ("pool", "tunnel"):
        scratch = Path(tempfile.mkdtemp(prefix=f"executable_{owner}_", dir=folder))
        marker = scratch / "unexpected_program_ran"
        executable = scratch / "unapproved_program"
        executable.write_text(f"#!{sys.executable}\nfrom pathlib import Path\nPath({str(marker)!r}).touch()\n")
        executable.chmod(0o700)
        checks: dict[str, bool] = {}
        if owner == "pool":
            api = EcsApi(WorkerConfig("synthetic", python=str(executable.resolve()), test_endpoint="http://127.0.0.1:1"), scratch / "registry")
            try:
                reply = api.call("describe_tasks", PARAMS, deadline=time.monotonic() + 3)
                checks["pool_refuses_unapproved_program"] = not api.handles and reply.get("error", {}).get("code") == "worker_spawn_failed"
            finally:
                cleaned = api.close(deadline=time.monotonic() + 3)
                checks["physical_cleanup_confirmed"] = _join_readers(api.handles) and cleaned["confirmed"]
        else:
            transport = CatalogTransport(scratch / "registry")
            transport.deadline = time.monotonic() + 3
            refused = False
            try:
                transport._spawn(
                    "tunnel",
                    [
                        "--target-port",
                        "1",
                        "--aws-executable",
                        str(executable.resolve()),
                        "--instance",
                        "i-00000000",
                        "--profile",
                        "synthetic",
                        "--region",
                        "synthetic",
                    ],
                    transport.deadline,
                )
            except EOFError:
                refused = True
            finally:
                cleaned_transport = transport.close(deadline=time.monotonic() + 3)
                checks["physical_cleanup_confirmed"] = _join_readers(transport.handles) and cleaned_transport
            checks["tunnel_refuses_unapproved_program"] = refused
        checks["unapproved_program_never_started"] = not marker.exists()
        rows.append({"case": f"{owner}_executable_allowlist", "checks": checks})
    return rows


def main() -> int:
    """Run the synthetic pooled owner to normal interpreter exit without an explicit close."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--folder", type=Path, required=True)
    args = parser.parse_args()
    api = EcsApi(WorkerConfig("synthetic", test_endpoint=args.endpoint), args.folder / "registry")
    reply = api.call("describe_tasks", PARAMS, deadline=time.monotonic() + 4)
    worker = api.pool.workers[0]
    state = {
        "owner_pid": os.getpid(),
        "worker_pid": worker.handle.pid,
        "worker_pgid": worker.handle.pgid,
        "reply_ok": reply["ok"],
        "pooled_idle": worker.status == "idle" and not worker.one_shot,
        "worker_alive": group_state(worker.handle.pgid) == "alive",
        "reader_alive": worker.handle.stderr_reader is not None and worker.handle.stderr_reader.is_alive,
    }
    (args.folder / "owner_state.json").write_text(json.dumps(state) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
