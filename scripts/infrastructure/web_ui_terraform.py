"""The web interfaces of the AWS host as the Terraform declares them: the Session Manager documents, their fixed ports and the health path of each port.

Two readers share this module so that no list of web interfaces exists outside the Terraform: ``scripts/infrastructure/open_ui.py`` (the names it accepts and
``--list``) and ``tests/test_readme_web_interfaces.py`` (the consistency checks). It reads the Terraform files as text.

- ``forward_documents`` reads the key and fixed host port of each document: the ``web_ui_forwards`` map of ``infra/iam/web_ui_forwards.tf`` and the persistent
  OpenMetadata document of ``infra/iam/openmetadata_persistent.tf``.
- ``terraform_listeners`` reads every service container listener of ``infra/services`` as (service, container, port, protocol), with the HTTP health paths that
  the container's health check calls. ``health_path`` gives the path called on one port, or ``None`` when no health check calls one.

It does not read a bind address from the Terraform. Ports opened by a process inside a container without a port mapping, except the OpenSearch one, are not
visible to it.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SERVICES = REPO_ROOT / "infra" / "services"
FORWARDS = REPO_ROOT / "infra" / "iam" / "web_ui_forwards.tf"
OPENMETADATA_FORWARD = REPO_ROOT / "infra" / "iam" / "openmetadata_persistent.tf"
MAP_ENTRY = re.compile(r"^\s+(\w+)\s*=\s*\{ port = (\d+),", re.MULTILINE)
LOCAL_NUMBER = re.compile(r"^\s*(\w+)\s*=\s*(\d+)\s*$", re.MULTILINE)
CONTAINER_START = re.compile(r"merge\(local\.(?:airflow_)?container_defaults, \{")
ASSIGNMENT = re.compile(r"^\s*(\w+_containers)\s*=", re.MULTILINE)

Listeners = dict[tuple[str, str, int, str], dict[int, str]]


class WebUiTerraformError(ValueError):
    """The Terraform does not have the shape this reader expects."""


def forward_documents(forwards: Path = FORWARDS, persistent: Path = OPENMETADATA_FORWARD) -> dict[str, int]:
    """The key and fixed host port of each web interface document: the ``web_ui_forwards`` map and the persistent OpenMetadata document."""
    documents = {key: int(port) for key, port in MAP_ENTRY.findall(forwards.read_text())}
    text = persistent.read_text()
    name = re.search(r'name\s*=\s*format\(local\.platform_name, "(\w+)_forward"\)', text)
    port = re.search(r"properties = \{\s+portNumber\s*=\s*\"(\d+)\"", text)
    if name is None or port is None:
        raise WebUiTerraformError(f"{persistent.name} has no forward document with a fixed port")
    documents[name.group(1)] = int(port.group(1))
    return documents


def terraform_text(services: Path = SERVICES) -> dict[str, str]:
    return {path.name: path.read_text() for path in sorted(services.glob("*.tf"))}


def local_numbers(texts: dict[str, str]) -> dict[str, int]:
    return {name: int(value) for text in texts.values() for name, value in LOCAL_NUMBER.findall(text)}


def number(token: str, numbers: dict[str, int]) -> int:
    token = token.strip()
    return int(token) if token.isdigit() else numbers[token.removeprefix("local.").removeprefix("${local.").removesuffix("}")]


def service_containers(main_text: str) -> dict[str, str]:
    """Map each ``<name>_containers`` local to the service key that runs it."""
    keys = re.findall(r"^\s+(\w+) = \{\n\s+task_role_arn\b.*?\n\s+containers\s*=\s*local\.(\w+)", main_text, re.MULTILINE | re.DOTALL)
    return {containers: key for key, containers in keys}


def health_paths(block: str, numbers: dict[str, int]) -> dict[int, str]:
    """The HTTP health path of each port the container's health check calls, without its query."""
    found: dict[int, str] = {}
    for port, path in re.findall(r"http://127\.0\.0\.1:(\d+|\$\{local\.\w+\})(/[^\s'\"\\)]*)", block):
        found[number(port, numbers)] = path.split("?")[0]
    for port, path in re.findall(r"/dev/tcp/127\.0\.0\.1/(\d+); printf 'GET (\S+) HTTP", block):
        found[int(port)] = path
    return found


def terraform_listeners(texts: dict[str, str]) -> tuple[Listeners, dict[str, str]]:
    """Every service container listener as (service, container, port, protocol), with the health paths of its container."""
    numbers = local_numbers(texts)
    owners = service_containers(texts["main.tf"])
    listeners: Listeners = {}
    for text in texts.values():
        starts = [match.start() for match in CONTAINER_START.finditer(text)] + [len(text)]
        for begin, end in zip(starts, starts[1:], strict=False):
            block = text[begin:end]
            name = re.search(r'\bname\s*=\s*"(\w+)"', block)
            if name is None:
                continue
            assignments = ASSIGNMENT.findall(text[:begin])
            service = owners.get(assignments[-1]) if assignments else None
            ports: list[tuple[int, str]] = []
            mapping = re.search(r"portMappings\s*=\s*\[(.*?)\]", block, re.DOTALL)
            for container_port, host_port, protocol in re.findall(
                r'containerPort\s*=\s*([\w.]+),\s*hostPort\s*=\s*([\w.]+),\s*protocol\s*=\s*"(\w+)"', mapping.group(1) if mapping else ""
            ):
                if number(container_port, numbers) != number(host_port, numbers):
                    raise WebUiTerraformError(f"{name.group(1)}: the container and host ports differ")
                ports.append((number(host_port, numbers), protocol))
            search = re.search(r'name = "http\.port", value = tostring\(local\.(\w+)\)', block)
            if search:
                ports.append((numbers[search.group(1)], "tcp"))
            if service is None and ports:
                raise WebUiTerraformError(f"{name.group(1)} listens on {ports} but belongs to no service of main.tf")
            for port, protocol in ports:
                listeners[(str(service), name.group(1), port, protocol)] = health_paths(block, numbers)
    return listeners, owners


def health_path(port: int, listeners: Listeners) -> str | None:
    """The path the health check of the one container listening on ``port`` calls, or ``None`` when it calls none; no or several listeners raise an error."""
    found = [paths for (_service, _container, listening, protocol), paths in listeners.items() if listening == port and protocol == "tcp"]
    if len(found) != 1:
        raise WebUiTerraformError(f"{len(found)} service containers listen on port {port}; exactly one is expected")
    return found[0].get(port)
