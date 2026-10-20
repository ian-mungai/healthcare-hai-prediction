"""Closed runtime settings for the three launcher-managed lineage services."""

from __future__ import annotations

import re
from collections.abc import Mapping
from pathlib import Path

SERVICES = ("postgres", "api", "web")
SETTINGS = frozenset(
    {"HAI_LINEAGE_STATE", "HAI_LINEAGE_API_PORT", "HAI_LINEAGE_WEB_PORT", "HAI_LINEAGE_MEM_POSTGRES", "HAI_LINEAGE_MEM_API", "HAI_LINEAGE_MEM_WEB"}
)


def settings_for(service: str, values: Mapping[str, str]) -> dict[str, str]:
    """Accept only the recorded state, ports and computed memory settings; no environment lookup."""
    if service not in SERVICES:
        raise ValueError("unknown lineage service")
    selected = {name: value for name, value in values.items() if name.startswith("HAI_LINEAGE_")}
    if set(selected) != SETTINGS:
        raise ValueError("lineage settings are incomplete or contain an unknown field")
    if not Path(selected["HAI_LINEAGE_STATE"]).is_absolute():
        raise ValueError("lineage state must be an absolute folder")
    for field in SETTINGS - {"HAI_LINEAGE_STATE"}:
        value = selected[field]
        if len(value) > 19 or not re.fullmatch(r"[1-9][0-9]*", value) or int(value) > 2**63 - 1:
            raise ValueError("lineage ports and memory must be bounded positive decimal integers")
    ports = [int(selected[name]) for name in ("HAI_LINEAGE_API_PORT", "HAI_LINEAGE_WEB_PORT")]
    if any(not 1024 <= port <= 65535 for port in ports) or ports[0] == ports[1]:
        raise ValueError("lineage ports must be distinct and between 1024 and 65535")
    return selected


def validate_bindings(service: str, body: Mapping[str, object]) -> None:
    """Refuse every non-loopback host binding and all database publication in the final create spec."""
    host = body.get("HostConfig")
    if not isinstance(host, dict):
        raise ValueError("lineage service has no host configuration")
    bindings = host.get("PortBindings", {})
    if not isinstance(bindings, dict) or service == "postgres" and bindings:
        raise ValueError("lineage PostgreSQL must have no host port binding")
    for entries in bindings.values():
        if not isinstance(entries, list) or not entries:
            raise ValueError("lineage port binding is malformed")
        if any(not isinstance(entry, dict) or entry.get("HostIp") != "127.0.0.1" for entry in entries):
            raise ValueError("lineage HostIp must be 127.0.0.1")
