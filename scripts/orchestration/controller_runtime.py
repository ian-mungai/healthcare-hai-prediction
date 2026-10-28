"""Run the launcher, broker and completion worker together without a local container backend.

The deployment supplies absolute nonsecret roots and injects CONTROLLER_BROKER_TOKEN in memory.
Secure-token publication and the nonblocking ECS facade must be connected before dispatch is available.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import signal
import sys
import threading
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from orchestration import launcher
from orchestration.lineage import CompletionWorker
from scripts.lakehouse import run_lock
from scripts.orchestration.broker import Broker, BrokerPaths, Listener, load_templates

START_SECONDS = 10.0
STOP_SECONDS = 30.0


def load_config(path: Path, *, test_registry: str | None = None) -> launcher.Config:
    """Load explicit controller roots without Docker, Mac home resolution or deployment-variable reads."""
    return launcher.resolve_config(test_registry, controller_config=path)


class Controller:
    """Own the controller components in one process and retain them until bounded cleanup is confirmed."""

    def __init__(self, config: launcher.Config) -> None:
        if config.local_backend != "disabled":
            raise launcher.LauncherError("controller runtime requires the local backend disabled", "config")
        self.config = config
        self.server = launcher.Launcher(config)
        self.client = launcher.LauncherClient(config, start=False)
        self.thread = threading.Thread(target=self._serve, name="controller-launcher")
        self.broker: Broker | None = None
        self.listener: Listener | None = None
        self.worker: CompletionWorker | None = None
        self.failure: BaseException | None = None
        self.result: int | None = None
        self.started = False
        self.closed = False

    def _serve(self) -> None:
        try:
            self.result = self.server.serve()
        except (launcher.LauncherError, launcher.ClientError, OSError, run_lock.RegistryError) as error:
            self.failure = error
        finally:
            if self.failure is None:
                self.failure = sys.exception()

    def start(self, *, broker_token: str | None = None, port: int = 18790, lineage_port: int | None = None) -> dict[str, Any]:
        """Start owned components and return controller readiness within the startup budget.

        Omit ``broker_token`` only for launcher-only verification. Production supplies the injected broker credential.
        Credentials remain in memory. This method never rotates secrets or starts or stops external services.
        """
        if self.started or self.closed:
            raise launcher.LauncherError("controller start requires a fresh runtime", "config")
        self.server.controller_preflight()
        self.thread.start()
        self.started = True
        ready = False
        try:
            deadline = time.monotonic() + START_SECONDS
            view: dict[str, Any] = {}
            while time.monotonic() < deadline:
                if not self.thread.is_alive():
                    if isinstance(self.failure, launcher.LauncherError):
                        raise self.failure
                    raise launcher.LauncherError("controller launcher stopped during startup", "startup")
                try:
                    view = self.client.call("status", {"what": "launcher"}, timeout=0.2, budget=min(0.2, deadline - time.monotonic()))
                except launcher.ClientError as error:
                    if not error.transport:
                        raise
                else:
                    if view.get("status") == "serving":
                        break
                time.sleep(min(0.02, max(0, deadline - time.monotonic())))
            else:
                raise launcher.LauncherError(f"controller readiness exceeded {START_SECONDS:g} seconds", "startup")
            if broker_token is not None:
                if not broker_token:
                    raise launcher.LauncherError("injected broker credential is empty", "config")
                artifacts = self.config.artifacts_root
                if artifacts is None:
                    raise launcher.LauncherError("controller artifacts root is absent", "config")
                paths = BrokerPaths(self.config.registry, artifacts)
                self.broker = Broker(paths, load_templates(), self.client, repo_root=self.config.root, runs_root=self.config.runs_root)
                self.broker.start()
                self.listener = Listener(self.broker, "127.0.0.1", port, broker_token)
                self.listener.start()
                provider_spool = artifacts / "airflow" / "lineage_provider"
                provider_spool.mkdir(parents=True, exist_ok=True, mode=0o700)
                self.worker = CompletionWorker(
                    paths.records,
                    provider_spool,
                    artifacts / "lineage" / "outbox.sqlite",
                    port=lineage_port,
                )
                self.worker.start()
            ready = True
            return view
        finally:
            if not ready:
                self.close()

    def close(self) -> None:
        """Stop only owned controller components and confirm every owned service thread has stopped."""
        if self.closed:
            return
        deadline = time.monotonic() + STOP_SECONDS
        self.server.running = False
        with contextlib.suppress(OSError):
            os.write(self.server.wake_write, b"x")
        errors = []
        if self.worker is not None:
            try:
                self.worker.stop()
            except TimeoutError:
                errors.append("completion worker did not stop")
        if self.listener is not None:
            self.listener.stop()
            if self.listener.thread.is_alive():
                errors.append("broker listener did not stop")
        if self.started:
            self.thread.join(timeout=max(0, deadline - time.monotonic()))
            if self.thread.is_alive():
                errors.append("launcher did not stop")
        else:
            self.server.pool.shutdown(wait=False, cancel_futures=True)
        if errors:
            raise TimeoutError(f"controller cleanup exceeded {STOP_SECONDS:g} seconds: {', '.join(errors)}")
        if self.broker is not None:
            self.broker.close()
        self.server.selector.close()
        os.close(self.server.wake_read)
        os.close(self.server.wake_write)
        self.closed = True


def main(argv: list[str] | None = None) -> int:
    """Run the foreground controller. Deployment integration supplies its secure-token and ECS providers."""
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("--config", type=Path, required=True, help="absolute registry, run and artifact roots")
    cli.add_argument("--test-registry", help="a disposable test registry matching registry_root")
    cli.add_argument("--broker-port", type=int, default=18790)
    cli.add_argument("--lineage-port", type=int)
    args = cli.parse_args(argv)
    token = os.environ.get("CONTROLLER_BROKER_TOKEN")
    if not token:
        sys.stderr.write("controller: injected broker credential is missing\n")
        return launcher.REFUSED_EXIT
    controller: Controller | None = None
    stopping = threading.Event()
    previous: dict[int, Any] = {}

    def stop(_number: int, _frame: Any) -> None:
        stopping.set()

    try:
        controller = Controller(load_config(args.config, test_registry=args.test_registry))
        for number in (signal.SIGINT, signal.SIGTERM):
            previous[number] = signal.signal(number, stop)
        view: Mapping[str, Any] = controller.start(broker_token=token, port=args.broker_port, lineage_port=args.lineage_port)
        sys.stdout.write(json.dumps({"controller": "serving", "readiness": view}) + "\n")
        sys.stdout.flush()
        while controller.thread.is_alive() and not stopping.wait(0.2):
            pass
        if controller.failure is not None:
            sys.stderr.write(f"controller launcher stopped: {type(controller.failure).__name__}\n")
            return launcher.ERROR_EXIT
        return controller.result or 0
    except launcher.LauncherError as error:
        sys.stderr.write(f"controller: {error} [{error.code}]\n")
        return launcher.REFUSED_EXIT
    except (launcher.ClientError, OSError, ValueError, TimeoutError) as error:
        sys.stderr.write(f"controller: {type(error).__name__}\n")
        return launcher.REFUSED_EXIT
    finally:
        if controller is not None:
            controller.close()
        for signum, handler in previous.items():
            signal.signal(signum, handler)


if __name__ == "__main__":
    sys.exit(main())
