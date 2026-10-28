"""Closed settings of the launcher-managed online feature store (design section 4.2): one Redis container on its own internal network.

The launcher owns the service as the stack ``online_store``: one container ``hai-<registry>-svc-online-store`` from ``COMPOSE`` on the internal network
``hai-<registry>-online-store``, reachable only as ``redis`` by the Feast jobs that join that network. Nothing is persisted (``redis.conf``), so a restart
empties every database; database 1 holds Feast's entity hashes, database 2 the writer's lease and marker and database 0 stays empty. The password
reaches the container only in its environment: a test registry uses a password the launcher generates once in the test folder, the real registry
the secret the secret-store module names. This module holds the closed shapes; it starts nothing.
"""

from __future__ import annotations

import os
import re
import secrets
import stat
from collections.abc import Mapping
from pathlib import Path
from types import ModuleType
from typing import Any

STACK = "online_store"
OWNER = f"stack:{STACK}"
SERVICE = "online-store"
TEMPLATE = "online_store_service"
COMPOSE = "services/feature_store/docker-compose.online.yaml"
CONFIG = "services/feature_store/redis.conf"
ALIAS = "redis"
PORT = 6379
DATABASES = {"empty": 0, "features": 1, "control": 2}
IMAGE = "redis:8.10.1@sha256:2e940b1efbe5bae595d9cc7c4f5ecdc84497a63b2a8e1bfe7624a0fae2f05127"
CONFIG_TARGET = "/usr/local/etc/redis/redis.conf"
ENTRYPOINT = ["sh", "-c", f'exec redis-server {CONFIG_TARGET} --requirepass "$REDIS_PASSWORD" --maxmemory "$REDIS_MAXMEMORY"']
HEALTHCHECK = ["CMD-SHELL", 'REDISCLI_AUTH="$REDIS_PASSWORD" redis-cli -n 2 ping | grep -qx PONG']
# The container and Feast job environment variable that carries the Redis password; only its name appears in code.
AUTH_ENV = "REDIS_PASSWORD"
# The secret-store module's constant that names the real password; absent until the secret names change adds it (QUESTION FOR LEAD).
STORE_CONSTANT = "REDIS_PASSWORD"
SETTINGS = frozenset({"HAI_ONLINE_MEM", "HAI_ONLINE_MAXMEMORY"})
MEMORY_SHARE = 0.10
MEMORY_FLOOR = 256 * 1024**2
MAXMEMORY_SHARE = 0.75
START_SECONDS = 600
TEST_PASSWORD = ("online_store", "redis_password")
PASSWORD_PATTERN = re.compile(r"[A-Za-z0-9_-]{43}")
REQUIRED_CONFIG = {
    "port": str(PORT),
    "protected-mode": "yes",
    "databases": str(len(DATABASES)),
    "save": '""',
    "appendonly": "no",
    "maxmemory-policy": "noeviction",
}


class OnlineStoreError(ValueError):
    """The online store's settings, configuration, service shape or password are not the closed ones."""


def network_name(registry_id: str) -> str:
    """The internal network of the online store: Redis and the Feast jobs that join it, nothing else."""
    return f"hai-{registry_id[:8]}-online-store"


def container_name(registry_id: str) -> str:
    """The one Redis container of a registry."""
    return f"hai-{registry_id[:8]}-svc-{SERVICE}"


def limits(container_memory: int) -> dict[str, str]:
    """The two memory settings: the container limit and Redis ``maxmemory`` at 75 % of it, leaving room for Redis's own overhead."""
    if type(container_memory) is not int or container_memory < MEMORY_FLOOR:
        raise OnlineStoreError("the online store memory is below its floor")
    return {"HAI_ONLINE_MEM": str(container_memory), "HAI_ONLINE_MAXMEMORY": str(int(container_memory * MAXMEMORY_SHARE))}


def settings_for(values: Mapping[str, str]) -> dict[str, str]:
    """Accept exactly the recorded memory settings: bounded decimal integers, the container limit at its floor or above and ``maxmemory`` at 75 %."""
    selected = {name: value for name, value in values.items() if name.startswith("HAI_ONLINE_")}
    if set(selected) != SETTINGS:
        raise OnlineStoreError("online store settings are incomplete or contain an unknown field")
    for name, value in selected.items():
        if len(value) > 19 or not re.fullmatch(r"[1-9][0-9]*", value) or int(value) > 2**63 - 1:
            raise OnlineStoreError(f"{name} must be a bounded positive decimal integer")
    if limits(int(selected["HAI_ONLINE_MEM"])) != selected:
        raise OnlineStoreError("maxmemory must be 75 % of the online store's container limit")
    return selected


def check_config(text: str) -> dict[str, str]:
    """Require exactly the closed Redis directives: no persistence, no eviction, three databases, protected mode and nothing else."""
    found: dict[str, str] = {}
    for number, line in enumerate(text.splitlines(), 1):
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        name, _, value = stripped.partition(" ")
        name = name.lower()
        if name in found:
            raise OnlineStoreError(f"redis.conf line {number}: {name} is set twice")
        if name not in REQUIRED_CONFIG:
            raise OnlineStoreError(f"redis.conf line {number}: directive {name} is not allowed")
        found[name] = value.strip()
    if found != REQUIRED_CONFIG:
        raise OnlineStoreError("redis.conf must set exactly: " + ", ".join(f"{name} {value}" for name, value in REQUIRED_CONFIG.items()))
    return found


def validate_service(raw: Mapping[str, Any]) -> None:
    """Refuse every Compose shape of the Redis service other than the closed one: pinned image, fixed command, environment, one read-only mount."""
    expected = {
        "image": IMAGE,
        "entrypoint": ENTRYPOINT,
        "user": "999:999",
        "environment": {AUTH_ENV: "${REDIS_PASSWORD}", "REDIS_MAXMEMORY": "${HAI_ONLINE_MAXMEMORY}"},
        "mem_limit": "${HAI_ONLINE_MEM}",
        "volumes": [f"./redis.conf:{CONFIG_TARGET}:ro"],
        "networks": ["online"],
        "cap_drop": ["ALL"],
        "security_opt": ["no-new-privileges:true"],
    }
    if set(raw) != {*expected, "healthcheck"}:
        raise OnlineStoreError("the online store service has other Compose keys than its closed shape")
    for key, value in expected.items():
        if raw[key] != value:
            raise OnlineStoreError(f"the online store service's {key} differs from its closed shape")
    check = raw["healthcheck"]
    if not isinstance(check, dict) or check.get("test") != HEALTHCHECK:
        raise OnlineStoreError("the online store health check differs from its closed shape")


def validate_body(body: Mapping[str, Any], registry_id: str) -> None:
    """Refuse a create body with a host port, another network, a writable or extra mount, a password on the command line or a privilege."""
    host = body.get("HostConfig")
    if not isinstance(host, dict):
        raise OnlineStoreError("the online store has no host configuration")
    if host.get("PortBindings") or body.get("ExposedPorts"):
        raise OnlineStoreError("the online store publishes no host port")
    if host.get("NetworkMode") != network_name(registry_id) or host.get("Privileged"):
        raise OnlineStoreError("the online store runs only on its internal network, unprivileged")
    mounts = host.get("Mounts") or []
    if len(mounts) != 1 or mounts[0].get("Target") != CONFIG_TARGET or not mounts[0].get("ReadOnly") or host.get("Binds"):
        raise OnlineStoreError("the online store mounts exactly its configuration, read-only")
    if body.get("Entrypoint") != ENTRYPOINT or body.get("Cmd"):
        raise OnlineStoreError("the online store command differs from its closed shape")
    names = {str(entry).partition("=")[0] for entry in body.get("Env") or []}
    if names != {AUTH_ENV, "REDIS_MAXMEMORY"}:
        raise OnlineStoreError("the online store environment holds other variables than the password and maxmemory")


def secret_name(module: ModuleType) -> str | None:
    """The secret-store name of the real Redis password, or None while the secret-store module names none."""
    value = getattr(module, STORE_CONSTANT, None)
    return value if isinstance(value, str) and value else None


def test_password_path(folder: Path) -> Path:
    """Where a test registry keeps its generated Redis password: ``<test folder>/online_store/redis_password``."""
    return folder.joinpath(*TEST_PASSWORD)


def ensure_test_password(folder: Path) -> str:
    """Generate the test registry's Redis password once (owner-only folder and file, created exclusively) and return the stored value."""
    target = test_password_path(folder)
    target.parent.mkdir(mode=0o700, exist_ok=True)
    if target.parent.is_symlink() or stat.S_IMODE(target.parent.lstat().st_mode) & 0o077:
        raise OnlineStoreError("the test password folder is a link or not owner-only")
    try:
        descriptor = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    except FileExistsError:
        return read_test_password(folder)
    with os.fdopen(descriptor, "w") as stream:
        stream.write(secrets.token_urlsafe(32))
        stream.flush()
        os.fsync(stream.fileno())
    return read_test_password(folder)


def read_test_password(folder: Path) -> str:
    """Read the test registry's Redis password without following a link; it must be an owner-only file of this user with the generated form."""
    target = test_password_path(folder)
    try:
        descriptor = os.open(target, os.O_RDONLY | os.O_NOFOLLOW)
    except OSError as error:
        raise OnlineStoreError(f"the test Redis password cannot be read ({type(error).__name__})") from None
    try:
        status = os.fstat(descriptor)
        if not stat.S_ISREG(status.st_mode) or status.st_uid != os.getuid() or status.st_mode & 0o077:
            raise OnlineStoreError("the test Redis password is not an owner-only regular file")
        value = os.read(descriptor, 128).decode("ascii", "replace")
    finally:
        os.close(descriptor)
    if not PASSWORD_PATTERN.fullmatch(value):
        raise OnlineStoreError("the test Redis password does not have the generated form")
    return value
