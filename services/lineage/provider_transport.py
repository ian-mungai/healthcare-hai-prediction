"""Native OpenLineage transport using the local sanitized spool, without HTTP or broker access."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from openlineage.client.transport.transport import Config, Transport

from . import provider_events


class SpoolConfig(Config):
    """Accept only the fixed, container-local spool path."""

    @classmethod
    def from_dict(cls, params: dict[str, Any]) -> SpoolConfig:
        """Validate transport settings before constructing the native provider transport."""
        if set(params) != {"type", "path"} or params["path"] != "/opt/hai/lineage_provider":
            raise ValueError("provider transport requires the local lineage spool")
        return cls()


class SafeTransport(Transport):
    """Keep sanitized provider events locally for independent host delivery."""

    kind = "hai-lineage-spool"
    config_class = SpoolConfig

    def __init__(self, config: SpoolConfig) -> None:
        self.config = config

    def emit(self, event: Any) -> None:
        """Store a bounded event and surface directory-permission failures to the native provider."""
        provider_events.emit(Path("/opt/hai/lineage_provider"), event)

    def close(self, timeout: float = 0) -> bool:
        """Return immediately because this transport starts no worker or pending request."""
        return True
