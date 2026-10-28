"""The bronze cutover's container probe: from inside one Mac Docker network, try the cutover's tunnel and relay ports on every host route.

The owner runs it once per Docker network while ``route-check`` holds the route open, with the command that ``route-check`` prints
(``scripts.lakehouse.cutover_route.probe_command``). It reads itself from standard input, so it needs only a Python 3 in a local image and
imports nothing from the repository:

    python -I - <NONCE> <TUNNEL_PORT> <RELAY_PORT> < scripts/lakehouse/mac_route_probe.py

It prints one JSON line: the run's nonce, the ports, the targets it tried and each target and port it reached. Every connection attempt ends
after three seconds. It never prints an address other than a target it tried.
"""

from __future__ import annotations

import ipaddress
import json
import socket
import struct
import sys
from pathlib import Path

TIMEOUT = 3.0
HOST_NAME = "host.docker.internal"
ROUTE_TABLE = Path("/proc/net/route")


def reaches(host: str, port: int) -> bool:
    """Whether a direct connection to the host and port succeeds within three seconds."""
    try:
        with socket.create_connection((host, port), TIMEOUT):
            return True
    except OSError:
        return False


def host_targets(route_table: Path = ROUTE_TABLE) -> list[str]:
    """The loopback, the Docker host name when it resolves, the default gateway and the first address of each connected network."""
    found = ["127.0.0.1"]
    try:
        socket.gethostbyname(HOST_NAME)
        found.append(HOST_NAME)
    except OSError:
        pass
    try:
        rows = route_table.read_text().splitlines()[1:]
    except OSError:
        rows = []
    for line in rows:
        fields = line.split()
        destination, gateway = (ipaddress.IPv4Address(struct.pack("<L", int(fields[index], 16))) for index in (1, 2))
        candidate = gateway if int(gateway) else destination + 1 if int(destination) else None
        if candidate is not None and str(candidate) not in found:
            found.append(str(candidate))
    return found


def report(nonce: str, ports: list[int], targets: list[str]) -> dict[str, object]:
    """Try every target on every port once and name what was reached."""
    return {"nonce": nonce, "ports": ports, "targets": targets, "reached": [[target, port] for target in targets for port in ports if reaches(target, port)]}


def main(argv: list[str]) -> int:
    if len(argv) != 3 or not all(value.isdecimal() and 0 < int(value) < 65536 for value in argv[1:]):
        sys.stderr.write("usage: python -I - <NONCE> <TUNNEL_PORT> <RELAY_PORT>\n")
        return 2
    sys.stdout.write(json.dumps(report(argv[0], [int(argv[1]), int(argv[2])], host_targets())) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
