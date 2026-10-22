"""Issue a Polaris principal's credentials once and keep the pair in Secrets Manager, whatever interrupts the update.

A stored pair is accepted only when Polaris itself accepts it, so a client ID and secret that belong to different issues, or a half-written pair,
are reset and stored again. Updates of one principal run one at a time under a file lock that holds no secret.
"""

from __future__ import annotations

import contextlib
import fcntl
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

from scripts.infrastructure import secret_store

OAUTH_ENDPOINT = "/api/catalog/v1/oauth/tokens"
MANAGEMENT = "/api/management/v1/principals"
LOCK_WAIT_SECONDS = 60.0
REFUSED_STATUSES = (400, 401, 403)
Request = Callable[..., tuple[int, Any]]
Check = Callable[[int, str, tuple[int, ...]], None]


class CredentialError(RuntimeError):
    """The pair could not be tested or updated; the message names the principal and the step, never a credential."""


@contextlib.contextmanager
def serialized(lock_path: Path, principal: str, seconds: float) -> Iterator[None]:
    """Hold the principal's lock, waiting at most ``seconds``."""
    lock_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    end = time.monotonic() + seconds
    with lock_path.open("a") as handle:
        while True:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= end:
                    raise CredentialError(f"another update of principal {principal} did not finish within {seconds:g} s") from None
                time.sleep(0.05)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def pair_accepted(request: Request, principal: str, client_id: str, client_secret: str) -> bool:
    """True when Polaris issues a token for the pair, False when it refuses the pair; any other answer is an error and changes nothing."""
    form = {"grant_type": "client_credentials", "client_id": client_id, "client_secret": client_secret, "scope": "PRINCIPAL_ROLE:ALL"}
    status, body = request("POST", OAUTH_ENDPOINT, form=form)
    if status == 200:
        return bool(body and "access_token" in body)
    if status in REFUSED_STATUSES:
        return False
    raise CredentialError(f"testing the stored credentials of principal {principal} failed (HTTP {status})")


def ensure_credentials(
    *,
    principal: str,
    names: tuple[str, str],
    vault: secret_store.SecretStore,
    request: Request,
    access: str,
    check: Check,
    lock_path: Path,
) -> str:
    """Make sure the principal exists and its working client ID and secret are in Secrets Manager; returns how the credentials were obtained.

    A stored pair that Polaris accepts is left alone. A missing, half-written or refused pair is replaced: the principal is created, or its
    credentials are reset, and both new values are stored.
    """
    path = f"{MANAGEMENT}/{principal}"
    with serialized(lock_path, principal, LOCK_WAIT_SECONDS):
        stored = [vault.find(name) for name in names]
        status, _ = request("GET", path, access)
        if status == 200 and stored[0] and stored[1] and pair_accepted(request, principal, stored[0], stored[1]):
            return "already present"
        if status == 404:
            status, body = request("POST", MANAGEMENT, access, body={"principal": {"name": principal}, "credentialRotationRequired": False})
            check(status, f"creating the {principal} principal", (200, 201, 204))
            outcome = "created"
        else:
            check(status, f"reading the {principal} principal", (200, 201, 204))
            status, body = request("POST", f"{path}/reset", access, body={})
            check(status, f"resetting the {principal} credentials", (200, 201, 204))
            outcome = "credentials reset"
        issued = body["credentials"]
        vault.put(names[0], issued["clientId"])
        vault.put(names[1], issued["clientSecret"])
        return outcome
