"""Network reachability checks for the loopback-only listeners: which addresses of this Mac refuse a connection to a port (failure mode 931).

``ifconfig`` is read once; every IPv4 and IPv6 address except the loopback is tried with a short timeout. A refused or timed-out connection counts as
unreachable. A second device on the same network is not available to these checks (the report lists that as not run)."""

from __future__ import annotations

import contextlib
import re
import socket

from scripts.process import run_command

CONNECT_SECONDS = 2.0


def interface_listing() -> str:
    """The text of ``ifconfig`` (macOS and most Linux hosts with net-tools), else of ``ip addr``; empty when neither program exists."""
    for program, arguments in (("ifconfig", []), ("ip", ["-o", "addr", "show"])):
        try:
            result = run_command(program, arguments, timeout=30)
        except OSError:
            continue
        if result.returncode == 0:
            return result.stdout
    return ""


def non_loopback_addresses() -> list[tuple[int, str, str]]:
    """(family, address, connect target) for each address of this host except 127.x and ::1; a link-local IPv6 address carries its interface scope.

    The interface listing is read once; the host name's own addresses are added, so a host with neither listing program still tries its main address."""
    found: dict[str, tuple[int, str, str]] = {}
    interface = ""
    for line in interface_listing().splitlines():
        if line and not line[0].isspace() and ":" in line:
            interface = line.split(":")[0].split()[-1] if line[0].isdigit() else line.split(":")[0]
        v4 = re.search(r"\binet (\d+\.\d+\.\d+\.\d+)", line)
        v6 = re.search(r"\binet6 ([0-9a-f:]+)", line)
        if v4 and not v4.group(1).startswith("127."):
            found[v4.group(1)] = (socket.AF_INET, v4.group(1), v4.group(1))
        elif v6 and v6.group(1) != "::1":
            address = v6.group(1)
            found[address] = (socket.AF_INET6, address, f"{address}%{interface}" if address.startswith("fe80") and interface else address)
    with contextlib.suppress(OSError):
        for _, _, _, _, sockaddr in socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET):
            address = str(sockaddr[0])
            if not address.startswith("127."):
                found.setdefault(address, (socket.AF_INET, address, address))
    return list(found.values())


def reachable(family: int, target: str, port: int) -> bool:
    """True when a TCP connection to the target address and port succeeds."""
    probe = socket.socket(family, socket.SOCK_STREAM)
    probe.settimeout(CONNECT_SECONDS)
    try:
        probe.connect(socket.getaddrinfo(target, port, family, socket.SOCK_STREAM)[0][4])
    except OSError:
        return False
    finally:
        probe.close()
    return True


def unreachable_elsewhere(port: int) -> dict[str, bool]:
    """For each non-loopback address, True when the connection to the port was refused or timed out."""
    return {address: not reachable(family, target, port) for family, address, target in non_loopback_addresses()}


def ipv6_loopback_open(port: int) -> bool:
    """True when ``::1`` accepts a connection on the port."""
    return reachable(socket.AF_INET6, "::1", port)
