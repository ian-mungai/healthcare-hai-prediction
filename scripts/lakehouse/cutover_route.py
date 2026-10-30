"""The bronze cutover's Mac-side route observations (owner decision, Oct 28 2026, for this one attended cutover only).

``route-check`` makes three observations of the bound host, in order, and records each result with the SHA-256 of its sanitized evidence:

1. ``host_without_open_ingress``: the host is one running instance with the project tag, and every security group on it or on any of its
   interfaces admits inbound traffic only from security groups of the same account and VPC that carry the project tag, never from an IPv4
   or IPv6 range or a prefix list. The host keeps its public address (the network has no NAT gateway), so the security groups keep the
   catalog port private.
2. ``catalog_port_only``: the owned tunnel forwards only the catalog port 8181 of the bound host, the owned relay forwards only to the
   tunnel, the transport owns exactly these two processes and the catalog answers through the relay.
3. ``mac_containers_refused``: while the route is open, no Mac Docker network reaches the tunnel or relay port. Docker runs only in the owner's
   attended terminal: ``route-check`` prints ``probe_command`` and waits, with a deadline, for the network list that command writes last.

Evidence holds no address, credential or response body. The ECS class-network matrix is not observed here.
"""

from __future__ import annotations

import hashlib
import json
import re
import secrets
import socket
import time
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

from orchestration.catalog_transport import CatalogTransport, TransportSpec
from scripts.acquisition.s3_store import AwsCli, StorageError
from scripts.lakehouse.catalog_journal import durable_directory, private_directory

CATALOG_PORT = 8181
CATALOG_ANSWERS = (200, 401)
PROBE_SECONDS = 900.0
PROBE_POLL_SECONDS = 2.0
NETWORK_LIST = "networks.txt"
CLEANUP_SECONDS = 2.0
CLEANUP_UNCONFIRMED = "cleanup_unconfirmed.txt"
# Removal and the stop of the running call each take at most CLEANUP_SECONDS, plus the kill and the file write.
CLEANUP_CEILING_SECONDS = 3 * CLEANUP_SECONDS
NETWORK_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}")
STATUS_LINE = re.compile(rb"HTTP/1\.[01] ([0-9]{3}) ")
Observation = tuple[bool | None, bytes]


def evidence(document: Mapping[str, Any]) -> bytes:
    return json.dumps(document, sort_keys=True, separators=(",", ":")).encode()


def security_group_ids(host: Mapping[str, Any]) -> list[str]:
    """The IDs of every security group on the instance and on each of its interfaces, in first-seen order."""
    found = [item.get("GroupId") for item in host.get("SecurityGroups") or []]
    for interface in host.get("NetworkInterfaces") or []:
        found += [item.get("GroupId") for item in interface.get("Groups") or []]
    return list(dict.fromkeys(str(group) for group in found if isinstance(group, str) and group))


def source_group_ids(groups: list[Mapping[str, Any]], wanted: list[str]) -> list[str]:
    """The security groups that the wanted groups admit inbound traffic from, in first-seen order."""
    found = [
        pair.get("GroupId")
        for group in groups
        if group.get("GroupId") in wanted
        for rule in group.get("IpPermissions") or []
        for pair in rule.get("UserIdGroupPairs") or []
    ]
    return list(dict.fromkeys(str(group) for group in found if isinstance(group, str) and group))


def host_facts(
    described: Mapping[str, Any],
    instance: str,
    project_tag: str,
    groups: list[Mapping[str, Any]] | None = None,
    sources: list[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """The sanitized facts of the host, its security groups and their source groups: counts and flags only, never an address or a group ID."""
    reservations = described.get("Reservations") or []
    found = [item for reservation in reservations for item in reservation.get("Instances") or []]
    host = found[0] if len(found) == 1 else {}
    owners = {reservation.get("OwnerId") for reservation in reservations if reservation.get("Instances")}
    owner = owners.pop() if len(owners) == 1 else None
    wanted = security_group_ids(host)
    described_groups = [group for group in groups or [] if group.get("GroupId") in wanted]
    rules = [rule for group in described_groups for rule in group.get("IpPermissions") or []]
    pairs = [pair for rule in rules for pair in rule.get("UserIdGroupPairs") or []]
    tag = {"Key": "Project", "Value": project_tag}
    trusted = {
        group.get("GroupId")
        for group in sources or []
        if owner is not None
        and group.get("OwnerId") == owner
        and host.get("VpcId")
        and group.get("VpcId") == host.get("VpcId")
        and tag in (group.get("Tags") or [])
    }
    return {
        "instance": instance,
        "instances": len(found),
        "state": (host.get("State") or {}).get("Name"),
        "project_tag": tag in (host.get("Tags") or []),
        "public_address": bool(host.get("PublicIpAddress"))
        or any((interface.get("Association") or {}).get("PublicIp") for interface in host.get("NetworkInterfaces") or []),
        "security_groups": len(wanted),
        "security_groups_described": len({group.get("GroupId") for group in described_groups}),
        "range_sources": sum(len(rule.get(key) or []) for rule in rules for key in ("IpRanges", "Ipv6Ranges", "PrefixListIds")),
        "foreign_group_sources": sum(1 for pair in pairs if owner is None or pair.get("UserId") != owner or not pair.get("GroupId")),
        "untrusted_group_sources": sum(1 for pair in pairs if pair.get("GroupId") not in trusted),
    }


def host_accepted(facts: Mapping[str, Any], *, require_tag: bool = True) -> bool:
    """One running host whose every security group was read and admits inbound traffic only from project security groups of its VPC."""
    return (
        facts["instances"] == 1
        and facts["state"] == "running"
        and (facts["project_tag"] is True or not require_tag)
        and facts["security_groups"] >= 1
        and facts["security_groups_described"] == facts["security_groups"]
        and facts["range_sources"] == 0
        and facts["foreign_group_sources"] == 0
        and facts["untrusted_group_sources"] == 0
    )


def describe_host(aws: AwsCli, instance: str, project_tag: str, *, deadline: float, clock: Callable[[], float] = time.monotonic) -> dict[str, Any]:
    """Read the instance, its security groups, then the groups they admit, each call within what is left of the deadline; a failed call raises."""

    def left(operation: str) -> float:
        remaining = deadline - clock()
        if remaining <= 0:
            raise StorageError(operation, "deadline")
        return remaining

    described = aws.call_bounded("ec2", "describe-instances", ["--instance-ids", instance], timeout=left("describe-instances"))
    hosts = [item for reservation in described.get("Reservations") or [] for item in reservation.get("Instances") or []]
    wanted = security_group_ids(hosts[0]) if len(hosts) == 1 else []
    groups: list[Mapping[str, Any]] = []
    sources: list[Mapping[str, Any]] = []
    if wanted:
        reply = aws.call_bounded("ec2", "describe-security-groups", ["--group-ids", *wanted], timeout=left("describe-security-groups"))
        groups = list(reply.get("SecurityGroups") or [])
        source_ids = source_group_ids(groups, wanted)
        if source_ids:
            reply = aws.call_bounded("ec2", "describe-security-groups", ["--group-ids", *source_ids], timeout=left("describe-security-groups"))
            sources = list(reply.get("SecurityGroups") or [])
    return host_facts(described, instance, project_tag, groups, sources)


def observe_host(aws: AwsCli, instance: str, project_tag: str, *, deadline: float, clock: Callable[[], float] = time.monotonic) -> Observation:
    """Observation 1: one running project host with no open ingress; a failed describe call is not observed."""
    if clock() >= deadline:
        return None, evidence({"instance": instance, "not_observed": "deadline"})
    try:
        facts = describe_host(aws, instance, project_tag, deadline=deadline, clock=clock)
    except StorageError as error:
        return None, evidence({"instance": instance, "not_observed": f"describe call failed ({error.code})"})
    return host_accepted(facts), evidence(facts)


def catalog_status(port: int, *, deadline: float) -> int:
    """The HTTP status the catalog gives an unauthenticated configuration request on the loopback port, or 0 when none arrives by the deadline."""
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=max(0.01, deadline - time.monotonic())) as connection:
            connection.sendall(b"GET /api/catalog/v1/config HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n")
            received = b""
            while b"\r\n" not in received and len(received) < 256:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return 0
                connection.settimeout(remaining)
                chunk = connection.recv(256)
                if not chunk:
                    break
                received += chunk
    except OSError:
        return 0
    status = STATUS_LINE.match(received)
    return int(status.group(1)) if status else 0


def port_observation(spec: TransportSpec, status: int, processes: int) -> Observation:
    """Observation 2: the tunnel forwards only the catalog port, the relay only the tunnel, two owned processes and a catalog answer."""
    facts = {
        "instance": spec.instance,
        "remote_port": CATALOG_PORT,
        "tunnel_port": spec.tunnel_port,
        "relay_target_port": spec.tunnel_port,
        "relay_port": spec.relay_port,
        "owned_processes": processes,
        "catalog_status": status,
    }
    accepted = processes == 2 and status in CATALOG_ANSWERS and CATALOG_PORT not in (spec.tunnel_port, spec.relay_port)
    return accepted, evidence(facts)


def probe_command(folder: Path, nonce: str, ports: list[int], image: str, expires: int) -> str:
    """The owner's command: run the container probe once on every Mac Docker network, then write the network list that ends the wait.

    The command ends at ``expires`` (seconds since the epoch), the deadline of the step that waits for it: every Docker call runs in the
    background and, when the deadline passes first, the command removes its own named probe container, stops and writes no network list.
    The removal and the stop of the running call each get ``CLEANUP_SECONDS``; a call still running then is killed, and a container whose
    removal did not succeed in time is named on standard error and in ``CLEANUP_UNCONFIRMED``. The probe script reaches every background
    call through an explicit standard input redirection, as POSIX shells give background commands an empty input otherwise.
    """
    arguments = f'{nonce} {ports[0]} {ports[1]} "$network"'
    polls = int(CLEANUP_SECONDS / 0.2)
    return (
        f"probe='{folder}'\n"
        f"deadline={expires}\n"
        "reap() {\n"
        "  i=0\n"
        '  while kill -0 "$1" 2>/dev/null; do\n'
        f'    [ "$i" -lt {polls} ] || return 124\n'
        "    sleep 0.2; i=$((i + 1))\n"
        "  done\n"
        '  wait "$1"\n'
        "}\n"
        "bounded() {\n"
        '  name=$1; shift; "$@" <&0 & pid=$!\n'
        '  while kill -0 "$pid" 2>/dev/null; do\n'
        '    if [ "$(date +%s)" -ge "$deadline" ]; then\n'
        '      if [ -n "$name" ]; then\n'
        '        docker rm -f "$name" </dev/null >/dev/null 2>&1 & remover=$!\n'
        '        reap "$remover"; removed=$?\n'
        '        if [ "$removed" -ne 0 ]; then\n'
        '          [ "$removed" -ne 124 ] || kill -9 "$remover" 2>/dev/null\n'
        f'          echo "$name" >> "$probe/{CLEANUP_UNCONFIRMED}"\n'
        '          echo "route probe: cleanup could not confirm that container $name stopped; stop it with: docker rm -f $name" >&2\n'
        "        fi\n"
        "      fi\n"
        '      kill "$pid" 2>/dev/null; reap "$pid"\n'
        '      [ "$?" -ne 124 ] || { kill -9 "$pid" 2>/dev/null; wait "$pid"; }\n'
        '      echo "route probe: the step deadline passed; no network list written" >&2; exit 1\n'
        "    fi\n"
        "    sleep 0.2\n"
        "  done\n"
        '  wait "$pid"\n'
        "}\n"
        "bounded '' docker network ls --format '{{.Name}}' > \"$probe/networks.before\" || exit 1\n"
        'for network in $(cat "$probe/networks.before"); do\n'
        f'  bounded "hai_route_probe_{nonce}_$network" docker run --rm -i --pull=never --name "hai_route_probe_{nonce}_$network"'
        f' --network "$network" --entrypoint python3 {image} -I - {arguments}'
        ' < scripts/lakehouse/mac_route_probe.py > "$probe/$network.json"\n'
        "done\n"
        "bounded '' docker network ls --format '{{.Name}}' > \"$probe/networks.txt.partial\" || exit 1\n"
        'mv "$probe/networks.txt.partial" "$probe/networks.txt"\n'
    )


def wait_for_probe(folder: Path, *, deadline: float, poll: float = PROBE_POLL_SECONDS) -> bool:
    """Wait until the owner's command has written the network list; False when the deadline passes first."""
    while time.monotonic() < deadline:
        if (folder / NETWORK_LIST).is_file():
            return True
        time.sleep(max(0.0, min(poll, deadline - time.monotonic())))
    return (folder / NETWORK_LIST).is_file()


def unconfirmed_containers(folder: Path, nonce: str) -> list[str]:
    """The containers of this run (named with its nonce) that the owner's command reported its cleanup could not confirm stopped."""
    prefix = f"hai_route_probe_{nonce}_"
    try:
        lines = (folder / CLEANUP_UNCONFIRMED).read_text().splitlines()
    except OSError:
        return []
    return sorted({line.strip() for line in lines if line.strip().startswith(prefix)})


def wait_for_cleanup(folder: Path, nonce: str, *, deadline: float, poll: float = 0.2) -> bool:
    """Wait until the owner's command has reported containers of this run that its cleanup could not confirm stopped; False when none is
    reported in time. A report naming only other runs' containers is ignored."""
    while time.monotonic() < deadline:
        if unconfirmed_containers(folder, nonce):
            return True
        time.sleep(max(0.0, min(poll, deadline - time.monotonic())))
    return bool(unconfirmed_containers(folder, nonce))


def container_observation(folder: Path, nonce: str, ports: list[int]) -> Observation:
    """Observation 3: every listed Docker network has this run's result, made on that network with at least one target, and none reached a
    port; any gap is not observed."""
    if unconfirmed := unconfirmed_containers(folder, nonce):
        return None, evidence({"not_observed": "cleanup could not confirm that these probe containers stopped", "containers": unconfirmed})
    try:
        names = [line.strip() for line in (folder / NETWORK_LIST).read_text().splitlines() if line.strip()]
    except OSError:
        return None, evidence({"not_observed": "no network list"})
    if not names or not all(NETWORK_NAME.fullmatch(name) for name in names):
        return None, evidence({"not_observed": "the network list is empty or holds a name that is not plain", "networks": len(names)})
    networks: dict[str, Any] = {}
    problem = ""
    for name in names:
        try:
            raw = (folder / f"{name}.json").read_bytes()
            result = json.loads(raw)
        except (OSError, ValueError):
            networks[name] = {"result": "absent or unreadable"}
            problem = problem or f"no readable result for network {name}"
            continue
        reached = result.get("reached") if isinstance(result, dict) else None
        matches = (
            isinstance(result, dict)
            and result.get("nonce") == nonce
            and result.get("ports") == ports
            and result.get("network") == name
            and isinstance(result.get("targets"), list)
            and len(result["targets"]) > 0
        )
        if not matches or not isinstance(reached, list):
            problem = problem or f"the result of network {name} is not from this run on that network or tried no target"
        networks[name] = {
            "sha256": hashlib.sha256(raw).hexdigest(),
            "targets": len(result.get("targets") or []) if isinstance(result, dict) else 0,
            "reached": len(reached) if isinstance(reached, list) else None,
        }
    document: dict[str, Any] = {"ports": ports, "networks": networks}
    if problem:
        return None, evidence({**document, "not_observed": problem})
    return not any(item["reached"] for item in networks.values()), evidence(document)


class LiveRouteProbe:
    """The attended observations: the host from AWS, the owned route on this Mac and the container probe the owner runs."""

    def __init__(self, aws: AwsCli, spec: TransportSpec, project_tag: str, transport: Path, probe: Path, image: str, write: Callable[[str], object]) -> None:
        self.aws, self.spec, self.project_tag, self.probe, self.image, self.write = aws, spec, project_tag, probe, image, write
        self.registry = transport
        self.transport: CatalogTransport | None = None
        self.nonce = secrets.token_hex(16)
        self.passed: dict[str, bool | None] = {}

    def observe(self, name: str, *, deadline: float) -> Observation:
        if name == "host_without_open_ingress":
            result = observe_host(self.aws, self.spec.instance, self.project_tag, deadline=deadline)
        elif name == "catalog_port_only":
            result = (
                self.observe_port(deadline)
                if self.passed.get("host_without_open_ingress")
                else (None, evidence({"not_observed": "the host check did not pass"}))
            )
        elif name == "mac_containers_refused":
            result = self.observe_containers(deadline) if self.passed.get("catalog_port_only") else (None, evidence({"not_observed": "the route is not open"}))
        else:
            result = None, evidence({"not_observed": f"unknown observation {name}"})
        self.passed[name] = result[0]
        return result

    def observe_port(self, deadline: float) -> Observation:
        if CATALOG_PORT in (self.spec.tunnel_port, self.spec.relay_port):
            return port_observation(self.spec, 0, 0)
        private_directory(self.registry)
        self.transport = CatalogTransport(self.registry)
        try:
            endpoint = self.transport.start(self.spec, deadline=deadline, ready_deadline=min(deadline, time.monotonic() + 120))
        except (OSError, RuntimeError, TimeoutError, ValueError) as error:
            return None, evidence({"not_observed": f"the route did not start ({type(error).__name__})"})
        status = catalog_status(int(endpoint.rsplit(":", 1)[1]), deadline=min(deadline, time.monotonic() + 30))
        return port_observation(self.spec, status, len(self.transport.handles))

    def observe_containers(self, deadline: float) -> Observation:
        if self.transport is None:
            return None, evidence({"not_observed": "the route is not open"})
        durable_directory(self.probe)
        ports = [self.spec.tunnel_port, self.spec.relay_port]
        wait_deadline = min(deadline, time.monotonic() + PROBE_SECONDS)
        expires = int(time.time() + (wait_deadline - time.monotonic()))
        self.write(
            "Run this in another terminal at the repository root now, with an image that is already local and has python3 (no pull):\n\n"
            + probe_command(self.probe, self.nonce, ports, self.image, expires)
            + f"\nroute-check waits up to {int(wait_deadline - time.monotonic())} seconds for {self.probe / NETWORK_LIST}; the command"
            " stops its own container at the same deadline.\n"
        )
        if not wait_for_probe(self.probe, deadline=wait_deadline):
            # The owner's command stops its container within its own cleanup ceiling after the same deadline; keep the names it could not confirm.
            if wait_for_cleanup(self.probe, self.nonce, deadline=time.monotonic() + CLEANUP_CEILING_SECONDS):
                return container_observation(self.probe, self.nonce, ports)
            return None, evidence({"not_observed": "no container probe result before the deadline"})
        try:
            self.transport.check(deadline=deadline)
        except RuntimeError:
            return None, evidence({"not_observed": "the route closed during the container probe"})
        return container_observation(self.probe, self.nonce, ports)

    def close(self, *, deadline: float) -> bool:
        return self.transport is None or self.transport.close(deadline=deadline)
