"""Exercise class isolation inside a launcher-owned container using fixed, non-AWS targets."""

from __future__ import annotations

import argparse
import errno
import fcntl
import ipaddress
import json
import os
import re
import select
import socket
import struct
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

from orchestration.network_policy import CLASSES

TIMEOUT = 3.0
PROXY = "http://hai-egress:3128"
HOST_NAME = "host.docker.internal"
TUNNEL = re.compile(r"Tunnel connection failed: ([0-9]{3})")
NET_CLASS = Path("/sys/class/net")
IPV6_TABLE = Path("/proc/net/if_inet6")
IFF_UP = 0x1
# Kernel link types (ARPHRD_ values in linux/if_arp.h) of the loopback and of the fallback devices that the Linux tunnel modules create in every
# network namespace (seen in an offline container on Docker Desktop, Oct 20 2026). Each one is down and holds no address until a tunnel is set up.
LOOPBACK_TYPE = 772
FALLBACK_LINK_TYPES = {
    "tunl0": 768,
    "gre0": 778,
    "gretap0": 1,
    "erspan0": 1,
    "ip_vti0": 768,
    "ip6_vti0": 769,
    "sit0": 776,
    "ip6tnl0": 769,
    "ip6gre0": 823,
}
IPV6_ROW = re.compile(r"[0-9a-f]{32} [0-9a-f]{2} [0-9a-f]{2} [0-9a-f]{2} [0-9a-f]{2} +\S+")
UNREADABLE = "unreadable"


def direct(host: str, port: int) -> bool:
    """Return whether a direct connection succeeds within three seconds."""
    try:
        with socket.create_connection((host, port), TIMEOUT):
            return True
    except OSError:
        return False


def request(url: str, proxy: bool) -> tuple[int, bytes, str]:
    """Make a bounded request without ambient proxy or credential handlers.

    Returns the status, the first bytes and the stage that produced the status: "response" for an HTTP response from the proxy or from a target
    reached through it, "tunnel_refused" when the proxy refused the CONNECT (the status is the proxy's), "connect_failed" when no connection was made.
    """
    handler = urllib.request.ProxyHandler({"http": PROXY, "https": PROXY} if proxy else {})
    opener = urllib.request.build_opener(handler)
    try:
        with opener.open(url, timeout=TIMEOUT) as response:
            return response.status, response.read(128), "response"
    except urllib.error.HTTPError as error:
        return error.code, b"", "response"
    except urllib.error.URLError as error:
        tunnel = TUNNEL.search(str(error.reason))
        return (int(tunnel.group(1)), b"", "tunnel_refused") if tunnel else (0, b"", "connect_failed")
    except OSError:
        return 0, b"", "connect_failed"


def https_outcome(status: int, stage: str) -> str:
    """Name what an HTTPS result through the proxy means: a CONNECT refusal by the proxy, a connection never made or a target that answered."""
    if stage == "tunnel_refused":
        return "tunnel_denied" if status == 403 else f"tunnel_refused_{status}"
    return "connect_failed" if stage == "connect_failed" else "target_reached"


def kernel_ipv4(name: str) -> str | None:
    """The IPv4 address of an interface, or None when it holds none."""
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
        try:
            return socket.inet_ntoa(fcntl.ioctl(probe.fileno(), 0x8915, struct.pack("256s", name.encode()[:15]))[20:24])
        except OSError as error:
            if error.errno == errno.EADDRNOTAVAIL:
                return None
            raise


def ipv6_addresses(table: Path, names: set[str]) -> dict[str, list[str]] | None:
    """The IPv6 addresses by interface name from the text table of the kernel, or None when the table is unreadable or any row is malformed.

    A kernel without IPv6 has no table and so no address.
    """
    try:
        rows = table.read_text().splitlines()
    except FileNotFoundError:
        return {}
    except OSError:
        return None
    found: dict[str, list[str]] = {}
    for row in rows:
        if not IPV6_ROW.fullmatch(row):
            return None
        address, *_, name = row.split()
        if name not in names:
            return None
        found.setdefault(name, []).append(str(ipaddress.IPv6Address(bytes.fromhex(address))))
    return found


def interface_list(
    names: Callable[[], list[tuple[int, str]]] = socket.if_nameindex,
    ipv4: Callable[[str], str | None] = kernel_ipv4,
    sysfs: Path = NET_CLASS,
    ipv6_table: Path = IPV6_TABLE,
) -> list[dict[str, object]]:
    """Every network interface of this namespace with its flags, state, link type and addresses; a fact that cannot be read is recorded as
    `unreadable`, never as absent. The arguments name where the facts come from, so a test can supply its own."""
    listed = names()
    ipv6 = ipv6_addresses(ipv6_table, {name for _, name in listed})
    found: list[dict[str, object]] = []
    for index, name in listed:
        entry: dict[str, object] = {"index": index, "name": name}
        for key, file in (("flags", "flags"), ("operstate", "operstate"), ("link_type", "type")):
            try:
                entry[key] = (sysfs / name / file).read_text().strip()
            except OSError:
                entry[key] = UNREADABLE
        try:
            address = ipv4(name)
            entry["addresses"] = [UNREADABLE] if ipv6 is None else ([address] if address else []) + ipv6.get(name, [])
        except OSError:
            entry["addresses"] = [UNREADABLE]
        found.append(entry)
    return found


def integer(value: object, base: int) -> int | None:
    """The integer a recorded fact holds, or None when it is unreadable or not a number of that base."""
    try:
        return int(str(value), base)
    except ValueError:
        return None


def is_loopback_address(address: str) -> bool:
    try:
        return ipaddress.ip_address(address).is_loopback
    except ValueError:
        return False


def device_problems(entry: dict[str, object], expected_type: int, loopback: bool) -> list[str]:
    """What is wrong with one allowed device: its link type, flags, operational state or addresses."""
    name = entry["name"]
    problems = []
    link_type = integer(entry.get("link_type"), 10)
    if link_type != expected_type:
        problems.append(f"{name}: link type {entry.get('link_type')!r}, expected {expected_type}")
    flags = integer(entry.get("flags"), 16)
    if flags is None:
        problems.append(f"{name}: flags {entry.get('flags')!r} cannot be read")
    elif not loopback and flags & IFF_UP:
        problems.append(f"{name}: the device is up (flags {entry.get('flags')})")
    states = ("unknown", "up") if loopback else ("down",)
    if entry.get("operstate") not in states:
        problems.append(f"{name}: state {entry.get('operstate')!r}, expected {' or '.join(states)}")
    addresses = entry.get("addresses")
    if not isinstance(addresses, list) or UNREADABLE in addresses:
        problems.append(f"{name}: addresses cannot be read")
    elif loopback and not all(is_loopback_address(str(address)) for address in addresses):
        problems.append(f"{name}: address {addresses} is not a loopback address")
    elif not loopback and addresses:
        problems.append(f"{name}: the device holds the address {addresses}")
    return problems


def interface_problems(found: list[dict[str, object]]) -> list[str]:
    """Why the namespace is not offline: it must hold the loopback and nothing else but kernel fallback tunnel devices that are down, have the
    link type of their name and hold no address. An empty list means offline."""
    problems = [] if any(entry.get("name") == "lo" for entry in found) else ["lo: the loopback is missing"]
    for entry in found:
        name = entry.get("name")
        if name == "lo":
            problems += device_problems(entry, LOOPBACK_TYPE, loopback=True)
        elif isinstance(name, str) and name in FALLBACK_LINK_TYPES:
            problems += device_problems(entry, FALLBACK_LINK_TYPES[name], loopback=False)
        else:
            problems.append(f"{name}: not an allowed device")
    return problems


def host_port(marker: str) -> int:
    """The host listener port the runner appended to the class marker as its third field, or 0 when there is none."""
    fields = marker.strip().split(":")
    return int(fields[2]) if len(fields) == 3 and fields[2].isdecimal() and 0 < int(fields[2]) < 65536 else 0


def gateway_addresses(route_table: str) -> list[str]:
    """Addresses through which a container could reach its host, from the text of `/proc/net/route`: the default gateway and the first address of each
    connected network."""
    found: list[str] = []
    for line in route_table.splitlines()[1:]:
        fields = line.split()
        destination, gateway = (ipaddress.IPv4Address(struct.pack("<L", int(fields[index], 16))) for index in (1, 2))
        candidate = gateway if int(gateway) else destination + 1 if int(destination) else None
        if candidate is not None and str(candidate) not in found:
            found.append(str(candidate))
    return found


def resolves(name: str) -> bool:
    """Return whether the name resolves inside this container."""
    try:
        socket.gethostbyname(name)
    except OSError:
        return False
    return True


class Catalog(BaseHTTPRequestHandler):
    """Serve a fixed positive-control response on the synthetic catalog endpoint."""

    def do_GET(self) -> None:
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"hai-network-positive-control")

    def log_message(self, format: str, *args: object) -> None:
        """Keep request details out of the probe output."""


def main() -> int:
    """Serve the synthetic catalog or report bounded traffic and credential checks."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", required=True, choices=("traffic", "catalog"))
    parser.add_argument("--class", required=True, dest="role", choices=(*CLASSES, "offline"))
    args = parser.parse_args()
    if args.case == "catalog":
        servers = []
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as interface:
            for _, name in socket.if_nameindex():
                if name == "lo":
                    continue
                try:
                    address = fcntl.ioctl(interface.fileno(), 0x8915, struct.pack("256s", name.encode()[:15]))[20:24]
                except OSError as error:
                    if error.errno == errno.EADDRNOTAVAIL:
                        continue
                    raise
                server = HTTPServer((socket.inet_ntoa(address), 8181), Catalog)
                server.timeout = 1
                servers.append(server)
        if len(servers) != 2:
            raise RuntimeError("the synthetic catalog must have exactly the two class interfaces")
        deadline = time.monotonic() + 300
        try:
            while time.monotonic() < deadline:
                ready, _, _ = select.select(servers, [], [], 1)
                for server in ready:
                    server.handle_request()
        finally:
            for server in servers:
                server.server_close()
        return 0
    catalog, body, _ = request("http://polaris:8181/", proxy=False)
    deadline = time.monotonic() + 15
    while args.role != "offline" and catalog != 200 and time.monotonic() < deadline:
        time.sleep(0.25)
        catalog, body, _ = request("http://polaris:8181/", proxy=False)
    denied, _, _ = request("http://example.org/", proxy=True)
    denied_tls, _, stage_tls = request("https://example.org/", proxy=True)
    outcome_tls = https_outcome(denied_tls, stage_tls)
    marker = Path("/run/class_sentinel")
    text = marker.read_text() if marker.is_file() else ""
    marker_ok = not marker.exists() if args.role == "offline" else text.startswith(args.role + ":")
    port = host_port(text)
    targets = [HOST_NAME, *gateway_addresses(Path("/proc/net/route").read_text())] if port else []
    reached = {target: direct(target, port) for target in targets}
    checks = {
        "catalog_positive_control": (catalog == 200 and body == b"hai-network-positive-control") if args.role != "offline" else catalog == 0,
        "proxy_denies_http": denied == 403 if args.role != "offline" else denied == 0,
        "proxy_denies_https": outcome_tls == ("tunnel_denied" if args.role != "offline" else "connect_failed"),
        "direct_dns_https_blocked": not direct("example.org", 443),
        "direct_ip_https_blocked": not direct("1.1.1.1", 443),
        "class_mount_only": marker_ok and not Path("/run/pipeline/aws/credentials").exists() and not Path("/workspace/out/network_sentinels").exists(),
        "no_ambient_credentials": not any(key.startswith(("AWS_", "POLARIS_", "DBT_ENV_SECRET_")) for key in os.environ),
    }
    interfaces = interface_list()
    problems = interface_problems(interfaces) if args.role == "offline" else []
    if args.role == "offline":
        checks["offline_has_only_loopback"] = not problems
    else:
        checks["host_listener_port_known"] = port > 0
    sys.stdout.write(
        json.dumps(
            {
                "class": args.role,
                "checks": checks,
                "catalog_status": catalog,
                "catalog_body": body.decode(errors="replace"),
                "proxy_http_status": denied,
                "proxy_https_status": denied_tls,
                "proxy_https_outcome": outcome_tls,
                "host_port": port,
                "interfaces": interfaces,
                "interface_problems": problems,
                "host_name_resolves": resolves(HOST_NAME) if port else False,
                "host_gateway": reached,
                "host_gateway_isolation": "UNVERIFIED" if args.role != "offline" else "not applicable",
                "ok": all(checks.values()),
            }
        )
        + "\n"
    )
    return 0 if all(checks.values()) else 1


if __name__ == "__main__":
    sys.exit(main())
