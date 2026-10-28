"""Closed egress policy and owned network identities for lakehouse jobs."""

from __future__ import annotations

import json
import re
import uuid
from pathlib import Path
from typing import Any

from scripts.lakehouse.gold_publish_record import RUN_ID

CLASSES = ("lakehouse_read", "lakehouse_write")
ENFORCED_CLASSES = (*CLASSES, "mlflow")
PROXY_IMAGE = "ubuntu/squid@sha256:8a3baed477e2c282ab8aa5edad442f69873246964f225c5c2ae8364b6610963c"
PROXY_ALIAS = "hai-egress"
LABEL_CLASS = "hai.network_class"
LABEL_RUN = "hai.pipeline.run"
REGION = re.compile(r"[a-z]{2}(?:-gov)?-[a-z]+-[1-9][0-9]?")


def names(registry_id: str, run_id: str) -> dict[str, str]:
    """Return deterministic network names for one validated consuming run."""
    if str(uuid.UUID(registry_id)) != registry_id or not RUN_ID.fullmatch(run_id):
        raise ValueError("network ownership has no canonical registry or run identity")
    prefix = f"hai-{registry_id[:8]}-{run_id.lower()}"
    return {"lakehouse_read": f"{prefix}-read", "lakehouse_write": f"{prefix}-write", "egress": f"{prefix}-egress"}


def endpoints(region: str) -> tuple[str, ...]:
    """Return only regional S3 host suffixes after validating the configured region."""
    if not REGION.fullmatch(region):
        raise ValueError("the configured AWS region is invalid")
    return (f".s3.{region}.amazonaws.com", f".s3.dualstack.{region}.amazonaws.com")


def squid_config(region: str) -> str:
    """Render a deny-by-default Squid policy for regional S3 HTTPS and the catalog port."""
    hosts = " ".join(endpoints(region))
    return f"""http_port 3128
acl CONNECT method CONNECT
acl tls_port port 443
acl catalog_port port 8181
acl catalog dstdomain polaris
acl s3 dstdomain {hosts}
http_access allow CONNECT tls_port s3
http_access allow catalog catalog_port
http_access deny all
cache deny all
cache_dir null /run/squid
access_log none
cache_store_log none
cache_log /dev/stderr
pid_filename none
forwarded_for delete
via off
shutdown_lifetime 1 seconds
"""


def load(path: Path) -> dict[str, Any]:
    """Require the checked-in proxy image and exactly the two supported job classes."""
    value: dict[str, Any] = json.loads(path.read_text())
    if value.get("version") != 1 or value.get("image") != PROXY_IMAGE or value.get("classes") != list(CLASSES):
        raise ValueError("the egress policy image or supported classes changed")
    return value


def check_network(value: dict[str, Any], expected_name: str, registry_id: str, run_id: str, internal: bool) -> None:
    """Refuse network adoption unless ownership, name and isolation all match."""
    labels = value.get("Labels") or {}
    if value.get("Name") != expected_name or labels.get("hai.registry_id") != registry_id or labels.get(LABEL_RUN) != run_id:
        raise ValueError("the existing network is not owned by this registry and run")
    if value.get("Driver") != "bridge" or bool(value.get("Internal")) != internal:
        raise ValueError("the existing network isolation differs from its class")
    if value.get("EnableIPv6") or (internal and (value.get("Options") or {}).get("com.docker.network.bridge.gateway_mode_ipv4") != "isolated"):
        raise ValueError("the class network has an IPv6 or host-gateway bypass")
