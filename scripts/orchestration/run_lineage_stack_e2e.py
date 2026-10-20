"""Run only the digest-pinned Marquez service stack on a disposable test launcher.

The reachability probes run from this host only. They never show how another device on the LAN is routed, so Verification 5 stays pending in every report.
``--hold-seconds`` keeps the passing stack up for a bounded window so the owner can try its ports from a second device.
"""

from __future__ import annotations

import argparse
import http.client
import json
import socket
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from orchestration import launcher
from scripts.orchestration import net_checks
from scripts.orchestration.run_lineage_provider_e2e import test_launcher, wait

ROOT = Path(__file__).resolve().parents[2]
HOLD_LIMITS = (1, 3600)
HOLD_POLL_SECONDS = 30
VERIFICATION_5 = {
    "status": "pending",
    "reason": (
        "needs a second device on the same LAN to try this host's IPv4 and IPv6 addresses on the Marquez API and UI ports; "
        "the probes in this report ran from this host only (owner session before the C3d merge)"
    ),
}


def free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def get(port: int, path: str) -> int:
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    try:
        connection.request("GET", path)
        response = connection.getresponse()
        response.read(2**20)
        return response.status
    finally:
        connection.close()


def host_coverage(probes: dict[str, dict[str, Any]]) -> str:
    """Name what the probes covered: this host's own addresses in each family, never a separate device."""
    both = all(row["ipv4_tried"] and row["ipv6_tried"] for row in probes.values())
    return "host_addresses_both_families" if both else "host_addresses_one_family: an address family is unavailable"


def hold_seconds(value: str) -> int:
    seconds = int(value)
    if not HOLD_LIMITS[0] <= seconds <= HOLD_LIMITS[1]:
        raise argparse.ArgumentTypeError(f"must be between {HOLD_LIMITS[0]} and {HOLD_LIMITS[1]} seconds")
    return seconds


def hold_stack(client: launcher.LauncherClient, output: Path, ports: dict[str, int], seconds: int) -> dict[str, str]:
    """Keep the stack up until the deadline or until the release file appears, and fail if it leaves the up phase.

    The addresses and ports go to an ignored file in the run folder, never to the terminal or the report.
    """
    started = datetime.now(UTC)
    release = output / "release"
    ends = time.monotonic() + seconds
    addresses = [str(target) for _, _, target in net_checks.non_loopback_addresses()]
    (output / "hold.json").write_text(
        json.dumps({"ports": ports, "addresses": addresses, "started_utc": started.isoformat(), "seconds": seconds, "release_file": str(release)}, indent=2)
        + "\n"
    )
    sys.stdout.write(json.dumps({"hold": str(output / "hold.json"), "seconds": seconds}) + "\n")
    sys.stdout.flush()
    next_check = time.monotonic() + HOLD_POLL_SECONDS
    while time.monotonic() < ends and not release.exists():
        time.sleep(1)
        if time.monotonic() >= next_check:
            if client.call("status", {"what": "lineage"}, timeout=10)["phase"] != "up":
                raise RuntimeError("Marquez left the up phase during the hold")
            next_check = time.monotonic() + HOLD_POLL_SECONDS
    return {"started_utc": started.isoformat(), "ended_utc": datetime.now(UTC).isoformat(), "ended_by": "release file" if release.exists() else "deadline"}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hold-seconds", type=hold_seconds, help="keep the passing stack up this long for a second-device check (1 to 3600)")
    args = parser.parse_args()
    output = ROOT / "data/e2e/lineage_stack" / datetime.now(UTC).strftime("%Y%m%dT%H%M%S_%fZ")
    output.mkdir(parents=True)
    result: dict[str, Any] = {"status": "stop", "launchers": [], "cached_images": [], "verification_5": VERIFICATION_5}
    try:
        with test_launcher({}, result["launchers"]) as client:
            for service, row in launcher.ComposeModel(ROOT / launcher.LINEAGE_COMPOSE).services.items():
                reference = row["image"]
                if "@sha256:" not in reference:
                    raise RuntimeError("service image is not pinned by digest")
                answer = client.call("image", {"service": service, "compose": "lineage", "checkout": str(ROOT)}, timeout=15)
                if answer["image"] != reference:
                    raise RuntimeError("cached service image differs from the pinned digest")
                result["cached_images"].append({"reference": reference, "id": answer["id"]})
            state = client.config.registry.path.parent / "lineage"
            for name in ("postgres", "private", "config"):
                (state / name).mkdir(mode=0o700, parents=True)
            api_port, web_port = free_port(), free_port()
            if api_port == web_port:
                raise RuntimeError("temporary service ports collided")
            params = {"state": str(state), "api_port": api_port, "web_port": web_port, "checkout": str(ROOT)}
            try:
                client.call("lineage_up", params, timeout=15)

                def ready() -> Any:
                    record = client.call("status", {"what": "lineage"}, timeout=10)
                    if record["phase"] == "down":
                        raise RuntimeError("lineage startup failed: " + record.get("error", "no recorded cause"))
                    return record if record["phase"] == "up" else None

                record = wait(ready, 300, "Marquez service health")
                result["services"] = {name: record[name] for name in ("phase", "health", "service_phases", "settings")}
                result["http"] = {"api": get(api_port, "/api/v1/namespaces"), "web": get(web_port, "/healthcheck")}
                if set(result["http"].values()) != {200}:
                    raise RuntimeError("Marquez API or UI did not return HTTP 200")
                addresses = net_checks.non_loopback_addresses()
                probes = {name: {"ipv4_tried": 0, "ipv6_tried": 0, "all_refused": True} for name in ("api", "web")}
                for name, port in (("api", api_port), ("web", web_port)):
                    for family, _, target in addresses:
                        field = "ipv4_tried" if family == socket.AF_INET else "ipv6_tried"
                        probes[name][field] += 1
                        probes[name]["all_refused"] = bool(probes[name]["all_refused"]) and not net_checks.reachable(family, target, port)
                    if net_checks.ipv6_loopback_open(port) or not probes[name]["all_refused"]:
                        raise RuntimeError("lineage endpoint accepts a connection outside IPv4 loopback")
                result["host_address_probes"] = probes
                result["host_coverage"] = host_coverage(probes)
                if args.hold_seconds is not None:
                    result["hold"] = hold_stack(client, output, {"api": api_port, "web": web_port}, args.hold_seconds)
                result["status"] = "pass"
            finally:
                client.call("lineage_down", timeout=15)
                wait(lambda: client.call("status", {"what": "lineage"}, timeout=10)["phase"] == "down", 120, "Marquez service teardown")
    except Exception as error:
        result["error"] = f"{type(error).__name__}: {error}"
    (output / "report.json").write_text(json.dumps(result, indent=2) + "\n")
    sys.stdout.write(json.dumps({"status": result["status"], "report": str(output / "report.json"), "error": result.get("error")}) + "\n")
    return 0 if result["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
