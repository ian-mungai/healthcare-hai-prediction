"""Probe real class networks and proxy policy through an isolated test-registry launcher."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import socket
import sys
import tempfile
import threading
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest import mock

from orchestration import launcher
from orchestration.network_policy import CLASSES, PROXY_IMAGE
from orchestration.pipeline_protocol import PipelineError, read_bytes
from scripts.lakehouse.run_gold_publish_e2e import Session
from scripts.orchestration import traffic_probe
from scripts.orchestration.run_transform_e2e import new_run_id

ROOT = Path(__file__).resolve().parents[2]
LISTEN_SECONDS = 1800


class HostListener:
    """Accept and close every connection to one host port and remember the peers, so a denied path is seen from the host side too."""

    def __init__(self) -> None:
        self.server = socket.socket()
        self.server.bind(("", 0))
        self.server.listen(16)
        self.server.settimeout(0.5)
        self.port = int(self.server.getsockname()[1])
        self.peers: list[str] = []
        self.stopped = threading.Event()
        self.thread = threading.Thread(target=self.serve, daemon=True)
        self.thread.start()

    def serve(self) -> None:
        deadline = time.monotonic() + LISTEN_SECONDS
        while not self.stopped.is_set() and time.monotonic() < deadline:
            try:
                connection, peer = self.server.accept()
            except TimeoutError:
                continue
            except OSError:
                return
            connection.close()
            self.peers.append(str(peer[0]))

    def wait_for(self, count: int, seconds: float) -> bool:
        """Wait until the listener has recorded the given number of connections."""
        deadline = time.monotonic() + seconds
        while len(self.peers) < count and time.monotonic() < deadline:
            time.sleep(0.05)
        return len(self.peers) >= count

    def close(self) -> None:
        self.stopped.set()
        self.thread.join(5)
        self.server.close()


def ready(client: launcher.LauncherClient, method: str, principal: dict[str, Any], terminal: str, evidence: dict[str, Any]) -> dict[str, Any] | None:
    """Return a method's ready result and fail visibly on a failed network transition."""
    result: dict[str, Any] = client.call(method, {}, principal)
    if result.get("phase") in ("failed", "ended"):
        if result.get("attempt_id") and client.config.registry.kind == "test":
            folder = client.config.registry.path / "services" / str(result["attempt_id"])
            try:
                log = read_bytes(folder, "log_tail.txt").decode(errors="replace")[-6000:]
            except (OSError, PipelineError):
                log = "no saved service log"
            evidence["startup_failure"] = {"attempt_id": result["attempt_id"], "logs": log, "proxy": result.get("proxy")}
        raise RuntimeError(f"{method} ended before {terminal}")
    return result if result.get("phase") == terminal else None


# What the kernel reports in an offline container on Docker Desktop (Oct 20 2026): link type, flags and operational state of each allowed device.
LOOPBACK = {"name": "lo", "type": "772", "flags": "0x9", "operstate": "unknown"}
FALLBACKS = [
    {"name": name, "type": kind, "flags": flags, "operstate": "down"}
    for name, kind, flags in (
        ("tunl0", "768", "0x80"),
        ("gre0", "778", "0x80"),
        ("gretap0", "1", "0x1002"),
        ("erspan0", "1", "0x1002"),
        ("ip_vti0", "768", "0x80"),
        ("ip6_vti0", "769", "0x80"),
        ("sit0", "776", "0x80"),
        ("ip6tnl0", "769", "0x80"),
        ("ip6gre0", "823", "0x80"),
    )
]
BRIDGE = {"name": "eth0", "type": "1", "flags": "0x1003", "operstate": "up"}
LOOPBACK_ROW = "00000000000000000000000000000001 01 80 10 80       lo"
IPV6_ROW_FOR_SIT0 = "fe800000000000000000000000000001 09 40 20 80     sit0"


def interface_problems_of(
    devices: list[dict[str, str]], ipv4: dict[str, str] | None = None, ipv6_rows: list[str] | None = None, broken: dict[str, str] | None = None
) -> list[str]:
    """Write the devices as a synthetic sysfs tree and interface table, read them through interface_list() and return the rule's problems.

    ``broken`` maps ``<device>/<file>`` to a fact to make unreadable (a folder replaces the file); ``ipv6_rows`` is None for no table.
    """
    with tempfile.TemporaryDirectory(prefix="interface_rule_") as folder:
        root = Path(folder)
        for device in devices:
            (root / "sys" / device["name"]).mkdir(parents=True)
            for file, key in (("type", "type"), ("flags", "flags"), ("operstate", "operstate")):
                if f"{device['name']}/{file}" in (broken or {}):
                    (root / "sys" / device["name"] / file).mkdir()
                else:
                    (root / "sys" / device["name"] / file).write_text(device[key] + "\n")
        table = root / "if_inet6"
        if ipv6_rows is not None and "if_inet6" in (broken or {}):
            table.mkdir()
        elif ipv6_rows is not None:
            table.write_text("\n".join(ipv6_rows) + "\n")
        found = traffic_probe.interface_list(
            names=lambda: [(index, device["name"]) for index, device in enumerate(devices, 1)],
            ipv4=lambda name: (ipv4 or {}).get(name),
            sysfs=root / "sys",
            ipv6_table=table,
        )
    return traffic_probe.interface_problems(found)


def interface_rule_cases() -> list[dict[str, Any]]:
    """The offline interface rule driven through interface_list() on synthetic devices, each case judged against its intended cause."""
    offline = [LOOPBACK, *FALLBACKS]
    ipv4 = {"lo": "127.0.0.1"}
    ipv6 = [LOOPBACK_ROW]

    def device(name: str, **changes: str) -> list[dict[str, str]]:
        return [LOOPBACK, *[{**item, **changes} if item["name"] == name else item for item in FALLBACKS]]

    def case(name: str, expected: str | None, problems: list[str]) -> dict[str, Any]:
        found = not problems if expected is None else any(expected in problem for problem in problems)
        return {"case": name, "expected": expected or "no problem", "problems": problems, "ok": found}

    cases = [
        case("the loopback alone passes", None, interface_problems_of([LOOPBACK], ipv4, ipv6)),
        case("the loopback with every fallback device as the kernel reports them passes", None, interface_problems_of(offline, ipv4, ipv6)),
        case("a kernel without an IPv6 table has no IPv6 address and passes", None, interface_problems_of(offline, {"lo": "127.0.0.1"})),
        case("a device named tunl0 with the Ethernet link type fails", "tunl0: link type", interface_problems_of(device("tunl0", type="1"), ipv4, ipv6)),
        case("sit0 with the link type of tunl0 fails", "sit0: link type", interface_problems_of(device("sit0", type="768"), ipv4, ipv6)),
        case("a fallback device with state unknown fails", "tunl0: state", interface_problems_of(device("tunl0", operstate="unknown"), ipv4, ipv6)),
        case("a fallback device with state up fails", "gre0: state", interface_problems_of(device("gre0", operstate="up"), ipv4, ipv6)),
        case("a fallback device with the up flag fails", "sit0: the device is up", interface_problems_of(device("sit0", flags="0x81"), ipv4, ipv6)),
        case("a fallback device with an IPv4 address fails", "sit0: the device holds", interface_problems_of(offline, {"sit0": "10.0.0.2"}, [LOOPBACK_ROW])),
        case(
            "a fallback device with an IPv6 address fails",
            "sit0: the device holds",
            interface_problems_of(offline, ipv4, [LOOPBACK_ROW, IPV6_ROW_FOR_SIT0]),
        ),
        case(
            "an attached bridge network (eth0 up with an address) fails",
            "eth0: not an allowed device",
            interface_problems_of([*offline, BRIDGE], {"eth0": "172.23.0.3"}),
        ),
        case(
            "an unlisted device that is down without address fails",
            "dummy0: not an allowed device",
            interface_problems_of([*offline, {**BRIDGE, "name": "dummy0", "flags": "0x80", "operstate": "down"}]),
        ),
        case("a namespace without the loopback fails", "lo: the loopback is missing", interface_problems_of(FALLBACKS)),
        case("a loopback with a non-loopback address fails", "lo: address", interface_problems_of(offline, {"lo": "10.0.0.2"})),
        case("a loopback with state down fails", "lo: state", interface_problems_of([{**LOOPBACK, "operstate": "down"}], ipv4, ipv6)),
    ]
    for fact, expected in (("type", "lo: link type"), ("flags", "lo: flags"), ("operstate", "lo: state")):
        cases.append(case(f"a loopback with an unreadable {fact} fails", expected, interface_problems_of(offline, ipv4, ipv6, broken={f"lo/{fact}": "x"})))
    for fact, expected in (("type", "tunl0: link type"), ("flags", "tunl0: flags"), ("operstate", "tunl0: state")):
        cases.append(
            case(f"a fallback device with an unreadable {fact} fails", expected, interface_problems_of(offline, ipv4, ipv6, broken={f"tunl0/{fact}": "x"}))
        )
    for label, rows, broken in (
        ("a row with too few fields", [LOOPBACK_ROW, "00000000000000000000000000000001 01 80"], None),
        ("a row with a short address", [LOOPBACK_ROW, "0001 01 80 10 80 lo"], None),
        ("a row with a non-hex address", [LOOPBACK_ROW, "0000000000000000000000000000000z 01 80 10 80 lo"], None),
        ("a blank row", [LOOPBACK_ROW, ""], None),
        ("a row for a device that does not exist", [LOOPBACK_ROW, "00000000000000000000000000000001 01 80 10 80 ghost0"], None),
        ("an unreadable table", [LOOPBACK_ROW], {"if_inet6": "x"}),
    ):
        cases.append(case(f"{label} in the IPv6 table fails", "addresses cannot be read", interface_problems_of(offline, {"lo": "127.0.0.1"}, rows, broken)))
    return cases


def device_list_mutant() -> dict[str, Any]:
    """Add eth0 to the production device list in memory: the cases must notice, so the list cannot change unseen."""
    with mock.patch.dict(traffic_probe.FALLBACK_LINK_TYPES, {"eth0": 1}):
        failed = [case["case"] for case in interface_rule_cases() if not case["ok"]]
    return {"added": "eth0", "failed_cases": failed, "ok": bool(failed)}


def main() -> int:
    """Run all class traffic probes and report their cleanup and source identities."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()
    if not getattr(launcher, "NETWORK_CLASSES_ENFORCED", ()):
        parser.error("apply the proposed launcher patch in a private review copy first")
    folder = ROOT / "data/e2e/network_classes" / datetime.now(UTC).strftime("%Y%m%dT%H%M%S_%fZ")
    folder.mkdir(parents=True)
    session = Session()
    run_id = new_run_id()
    principal: dict[str, Any] = {}
    catalog: dict[str, Any] | None = None
    report: dict[str, Any] = {"run_id": run_id, "scope": "real Docker bridge isolation and Squid; test registry; no AWS", "probes": []}
    report["interface_rule"] = interface_rule_cases()
    report["interface_list_mutant"] = device_list_mutant()
    listener = HostListener()
    report["host_listener"] = {"port": listener.port, "platform": platform.system(), "control": False, "connections_from_probes": []}
    control_peers = 0
    try:
        with socket.create_connection(("1.1.1.1", 443), 3):
            report["host_external_positive_control"] = True
        with socket.socket() as closed:
            closed.bind(("127.0.0.1", 0))
            closed_port = int(closed.getsockname()[1])
        report["host_listener"]["control"] = (
            traffic_probe.direct("127.0.0.1", listener.port) and listener.wait_for(1, 5) and not traffic_probe.direct("127.0.0.1", closed_port)
        )
        control_peers = len(listener.peers)
        if not report["host_listener"]["control"]:
            raise RuntimeError("the host listener control failed: the probe's connect function must reach a listening host port and not a closed one")
        session.start()
        if session.client.release_allocation(session.allocation) != launcher.ReleaseOutcome.RELEASED:
            raise RuntimeError("the setup allocation was not released before run-owned probes")
        session.allocation = ""
        report["proxy_image"] = session.client.call("pull", {"reference": PROXY_IMAGE}, {"kind": "any"}, timeout=600)
        session.client.call(
            "acquire", {"domain": "transform", "run_id": run_id, "mode": "publish_fixture", "holder": "network_classes_e2e", "checkout": str(ROOT)}
        )
        principal = session.client.principal_run(run_id)
        markers = session.config.runs_root / run_id / "network_sentinels"
        markers.mkdir(parents=True)
        for role in CLASSES:
            marker = markers / f"{role}.txt"
            marker.write_text(f"{role}:{os.urandom(16).hex()}:{listener.port}")
            marker.chmod(0o444)
        session.wait(lambda: ready(session.client, "pipeline_network_up", principal, "ready", report), 660, "the class networks and proxy are ready")
        catalog = session.wait(lambda: ready(session.client, "pipeline_test_catalog", principal, "started", report), 180, "the synthetic catalog is running")
        for role in (*CLASSES, "offline"):
            result = session.client.run_attempt(
                principal, "pipeline_probe", f"traffic-{role}", [], {"class": role, "case": "traffic"}, checkout=ROOT, seconds=180
            )
            observation = json.loads(result.stdout.strip().splitlines()[-1]) if result.stdout.strip() else {}
            ok = result.ok and observation.get("ok") is True and observation.get("class") == role
            report["probes"].append(
                {
                    "class": role,
                    "ok": ok,
                    "checks": observation.get("checks", {}),
                    "reason": result.reason,
                    "exit_code": result.exit_code,
                    "observation": observation,
                    "stderr": result.stderr[-4000:],
                }
            )
            if not ok:
                raise RuntimeError(f"{role}: the traffic or credential boundary failed")
    except Exception as error:
        report["error"] = f"{type(error).__name__}: {error}"[:400]
    finally:
        if principal:
            try:
                session.wait(lambda: ready(session.client, "pipeline_network_down", principal, "down", report), 660, "the class proxy and networks are removed")
                if report.get("error") and catalog is not None:
                    service_folder = session.config.registry.path / "services" / str(catalog["attempt_id"])
                    try:
                        report["catalog_log"] = read_bytes(service_folder, "log_tail.txt").decode(errors="replace")[-4000:]
                    except (OSError, PipelineError):
                        report["catalog_log"] = "no saved catalog log"
                session.client.call("finalize", {}, principal)
                record = session.temp / "network_run.json"
                record.write_text(json.dumps({"run_id": run_id}) + "\n")
                session.client.call("release", {"record": str(record)}, principal, timeout=300)
            except Exception as error:
                report["release_error"] = f"{type(error).__name__}: {error}"[:400]
        report["cleanup"] = session.close()
        listener.close()
        report["host_listener"]["connections_from_probes"] = listener.peers[control_peers:]
    report["source_sha256"] = {
        name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest()
        for name in (
            "orchestration/launcher.py",
            "orchestration/network_policy.py",
            "orchestration/pipeline_render.py",
            "scripts/orchestration/traffic_probe.py",
            "scripts/orchestration/run_network_classes_e2e.py",
        )
    }
    report["host_gateway"] = {
        "isolation": "UNVERIFIED",
        "pending_reason": "container routing to the host listener is unproven on Docker Desktop for Mac (amendment 233); a pass needs a Linux Docker host",
        "violation": bool(report["host_listener"]["connections_from_probes"])
        or any(any(probe["observation"].get("host_gateway", {}).values()) for probe in report["probes"]),
    }
    report["pending"] = ["host_gateway_isolation"]
    report["complete"] = False
    report["ok"] = (
        not report.get("error")
        and not report.get("release_error")
        and not report["cleanup"]["errors"]
        and len(report["probes"]) == 3
        and all(probe["ok"] for probe in report["probes"])
        and all(case["ok"] for case in report["interface_rule"])
        and report["interface_list_mutant"]["ok"]
        and report["host_listener"]["control"]
        and not report["host_gateway"]["violation"]
    )
    (folder / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    sys.stdout.write(json.dumps({"ok": report["ok"], "pending": report["pending"], "report": str(folder / "report.json")}) + "\n")
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
