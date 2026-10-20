"""Inspect the pinned Airflow provider through disposable real-daemon test launchers."""

from __future__ import annotations

import hashlib
import json
import os
import sys
import tempfile
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from orchestration import launcher
from scripts.orchestration.offline_checks import image_problems
from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]


def wait(predicate: Any, seconds: float, label: str) -> Any:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.2)
    raise RuntimeError(f"deadline waiting for {label}")


@contextmanager
def test_launcher(images: dict[str, str], evidence: list[dict[str, Any]]) -> Iterator[launcher.LauncherClient]:
    root = Path(tempfile.mkdtemp(prefix="hlp_", dir="/tmp"))
    registry = root / "registry"
    registry.mkdir(mode=0o700)
    settings = {
        "docker_socket": launcher.docker_socket_path(),
        "images": images,
        "token_root": str(root / "tokens"),
        "runs_root": str(root / "runs"),
        "lakehouse_state": str(root / "lakehouse"),
        "allowed_roots": [str(root)],
    }
    (registry / "launcher_test.json").write_text(json.dumps(settings))
    config = launcher.resolve_config(str(registry))
    if len(os.fsencode(config.socket_path)) > 100:
        raise RuntimeError("test registry socket path exceeds the portable Unix-socket bound")
    client = launcher.LauncherClient(config)
    client.launch_nonce = uuid.uuid4().hex
    pid = 0
    record: dict[str, Any] = {"registry": str(registry), "cleanup": "pending"}
    evidence.append(record)
    try:
        client.connect().close()
        state = client.call("status", {"what": "launcher"}, timeout=10)
        pid = int(state["pid"])
        record["pid"] = pid
        wait(lambda: client.call("ping", timeout=10).get("status") == "serving", 90, "test launcher readiness")
        wait(lambda: not client.call("status", {"what": "launcher"}, timeout=10)["containers"], 30, "probe removal")
        yield client
    finally:
        client.start = False
        if not pid:
            record["cleanup"] = "startup failed before status; process check required"
        else:
            state = client.call("status", {"what": "launcher"}, timeout=10)
            if state["open_attempts"] or state["containers"] or state["networks"]:
                raise RuntimeError("test launcher still owns work; cleanup is incomplete")
            record.update(containers_left=state["containers"], networks_left=state["networks"], open_attempts=state["open_attempts"])
            client.call("shutdown", {"owner": True, "launch_nonce": client.launch_nonce}, {"kind": "any", "owner": True}, timeout=10)

            def stopped() -> bool:
                try:
                    os.kill(pid, 0)
                except ProcessLookupError:
                    return True
                return False

            wait(stopped, 15, "owned launcher shutdown")
            record["cleanup"] = "done"


def main() -> int:
    platform = json.loads((ROOT / "config/orchestration/platform.json").read_text())
    output = ROOT / "data/e2e/lineage_provider" / datetime.now(UTC).strftime("%Y%m%dT%H%M%S_%fZ")
    output.mkdir(parents=True)
    result: dict[str, Any] = {"revision": run_command("git", ["rev-parse", "HEAD"], cwd=ROOT, timeout=10).stdout.strip(), "launchers": [], "status": "stop"}
    try:
        with test_launcher({platform["airflow"]["image"]: platform["airflow"]["base_image"]}, result["launchers"]) as client:
            base = client.call("image", {"service": "airflow-check", "compose": "orchestration", "checkout": str(ROOT)}, timeout=60)
            result["base_image"] = {"reference": base["image"], "id": base["id"], "layers": base["inspect"]["RootFS"]["Layers"]}
            if base["image"] != platform["airflow"]["base_image"]:
                raise RuntimeError("base inspection did not use the pinned digest")
        with test_launcher({}, result["launchers"]) as client:
            image = client.call("image", {"service": "airflow-check", "compose": "orchestration", "history": True, "checkout": str(ROOT)}, timeout=60)
            problems = image_problems(image, platform)
            layers = image["inspect"]["RootFS"]["Layers"]
            base_layers = result["base_image"]["layers"]
            if problems or layers[: len(base_layers)] != base_layers:
                raise RuntimeError("Airflow wrapper does not prove the pinned base plus deadline-only layers")
            result["wrapper_image"] = {"reference": image["image"], "id": image["id"], "layers": layers}
            allocation = client.allocate()
            try:
                answer = client.run_attempt(client.principal_alloc(allocation), "orchestration_check", "lineage-provider", [], {}, checkout=ROOT, seconds=300)
                lines = [line.removeprefix("IN_IMAGE ") for line in answer.stdout.splitlines() if line.startswith("IN_IMAGE ")]
                if not answer.ok or not lines:
                    result["attempt"] = {"exit_code": answer.exit_code, "reason": answer.reason, "tail": (answer.stderr or answer.stdout)[-1000:]}
                    raise RuntimeError("pinned-image metadata check failed")
                checked = json.loads(lines[-1])
                result["provider"] = checked["constraints"]["lineage_packages"]
                result["in_image_checks"] = checked
                if checked.get("lineage_transport", {}).get("ok") is not True:
                    raise RuntimeError("pinned-image local transport and safe projection check failed")
            finally:
                if client.release_allocation(allocation) != launcher.ReleaseOutcome.RELEASED:
                    raise RuntimeError("provider allocation was retained")
            if not result["provider"].get("apache-airflow-providers-openlineage"):
                raise RuntimeError("OpenLineage provider missing from pinned official image; Open Choice 1 requires owner decision")
            result["status"] = "pass"
    except Exception as error:
        result["error"] = f"{type(error).__name__}: {error}"
    result["source_sha256"] = {
        name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest()
        for name in ("scripts/orchestration/in_image_checks.py", "scripts/orchestration/run_lineage_provider_e2e.py", "orchestration/launcher.py")
    }
    (output / "report.json").write_text(json.dumps(result, indent=2) + "\n")
    sys.stdout.write(
        json.dumps({"status": result["status"], "report": str(output / "report.json"), "provider": result.get("provider"), "error": result.get("error")}) + "\n"
    )
    return 0 if result["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
