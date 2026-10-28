"""The bronze cutover's Mac-side route observations (owner decision, Oct 28 2026, for this one attended cutover only).

``route-check`` makes three observations of the bound host, in order, and records each result with the SHA-256 of its sanitized evidence:

1. ``host_without_public_address``: the host is one running instance with the project tag and no public address on it or any interface.
2. ``catalog_port_only``: the owned tunnel forwards only the catalog port 8181 of the bound host, the owned relay forwards only to the
   tunnel, the transport owns exactly these two processes and the catalog answers through the relay.
3. ``mac_containers_refused``: while the route is open, no Mac Docker network reaches the tunnel or relay port. Docker runs only in the owner's
   attended terminal: ``route-check`` prints ``probe_command`` and waits, with a deadline, for the network list that command writes last.

Evidence holds no address, credential or response body. The ECS class-network matrix of ``docs/ecs_real_runs.md`` is not observed here and
stays required before any ECS job uses the catalog.
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
from scripts.lakehouse.catalog_journal import durable_directory

CATALOG_PORT = 8181
CATALOG_ANSWERS = (200, 401)
PROBE_SECONDS = 900.0
PROBE_POLL_SECONDS = 2.0
NETWORK_LIST = "networks.txt"
NETWORK_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}")
STATUS_LINE = re.compile(rb"HTTP/1\.[01] ([0-9]{3}) ")
Observation = tuple[bool | None, bytes]


def evidence(document: Mapping[str, Any]) -> bytes:
    return json.dumps(document, sort_keys=True, separators=(",", ":")).encode()


def host_facts(described: Mapping[str, Any], instance: str, project_tag: str) -> dict[str, Any]:
    """The sanitized facts of a `describe-instances` reply: the state, the project tag and whether any public address exists, never the address."""
    found = [item for reservation in described.get("Reservations") or [] for item in reservation.get("Instances") or []]
    host = found[0] if len(found) == 1 else {}
    return {
        "instance": instance,
        "instances": len(found),
        "state": (host.get("State") or {}).get("Name"),
        "project_tag": {"Key": "Project", "Value": project_tag} in (host.get("Tags") or []),
        "public_on_instance": bool(host.get("PublicIpAddress")),
        "public_on_interfaces": any((interface.get("Association") or {}).get("PublicIp") for interface in host.get("NetworkInterfaces") or []),
    }


def host_accepted(facts: Mapping[str, Any]) -> bool:
    return (
        facts["instances"] == 1
        and facts["state"] == "running"
        and facts["project_tag"] is True
        and not facts["public_on_instance"]
        and not facts["public_on_interfaces"]
    )


def observe_host(aws: AwsCli, instance: str, project_tag: str, *, deadline: float, clock: Callable[[], float] = time.monotonic) -> Observation:
    """Observation 1: one running project host without a public address; a failed describe call is not observed."""
    if clock() >= deadline:
        return None, evidence({"instance": instance, "not_observed": "deadline"})
    try:
        described = aws.call_bounded("ec2", "describe-instances", ["--instance-ids", instance], timeout=deadline - clock())
    except StorageError as error:
        return None, evidence({"instance": instance, "not_observed": f"describe-instances failed ({error.code})"})
    facts = host_facts(described, instance, project_tag)
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


def probe_command(folder: Path, nonce: str, ports: list[int], image: str) -> str:
    """The owner's command: run the container probe once on every Mac Docker network, then write the network list that ends the wait."""
    arguments = f'{nonce} {ports[0]} {ports[1]} "$network"'
    return (
        f"probe='{folder}'\n"
        "for network in $(docker network ls --format '{{.Name}}'); do\n"
        f'  docker run --rm -i --pull=never --network "$network" --entrypoint python3 {image} -I - {arguments}'
        ' < scripts/lakehouse/mac_route_probe.py > "$probe/$network.json"\n'
        "done\n"
        'docker network ls --format \'{{.Name}}\' > "$probe/networks.txt.partial" && mv "$probe/networks.txt.partial" "$probe/networks.txt"\n'
    )


def wait_for_probe(folder: Path, *, deadline: float, poll: float = PROBE_POLL_SECONDS) -> bool:
    """Wait until the owner's command has written the network list; False when the deadline passes first."""
    while time.monotonic() < deadline:
        if (folder / NETWORK_LIST).is_file():
            return True
        time.sleep(max(0.0, min(poll, deadline - time.monotonic())))
    return (folder / NETWORK_LIST).is_file()


def container_observation(folder: Path, nonce: str, ports: list[int]) -> Observation:
    """Observation 3: every listed Docker network has this run's result, made on that network with at least one target, and none reached a
    port; any gap is not observed."""
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
        if name == "host_without_public_address":
            result = observe_host(self.aws, self.spec.instance, self.project_tag, deadline=deadline)
        elif name == "catalog_port_only":
            result = (
                self.observe_port(deadline)
                if self.passed.get("host_without_public_address")
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
        durable_directory(self.registry)
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
        self.write(
            "Run this in another terminal at the repository root now, with an image that is already local and has python3 (no pull):\n\n"
            + probe_command(self.probe, self.nonce, ports, self.image)
            + f"\nroute-check waits up to {int(PROBE_SECONDS)} seconds for {self.probe / NETWORK_LIST}.\n"
        )
        if not wait_for_probe(self.probe, deadline=min(deadline, time.monotonic() + PROBE_SECONDS)):
            return None, evidence({"not_observed": "no container probe result before the deadline"})
        try:
            self.transport.check(deadline=deadline)
        except RuntimeError:
            return None, evidence({"not_observed": "the route closed during the container probe"})
        return container_observation(self.probe, self.nonce, ports)

    def close(self, *, deadline: float) -> bool:
        return self.transport is None or self.transport.close(deadline=deadline)
