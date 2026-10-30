"""The MLflow model registry calls of the promotion check: find or create a registered model and one version per source run, and read the champion alias.

A create is sent once and never resent. Before ``create_model_version`` the call writes a pending marker, a registered model tag ``pending_create.<identity>``
whose value is the step key. The same identity travels in the create as the version tag ``create_identity``. A later try that finds a marker for its step key
searches the versions for that identity and adopts the one it finds. When it finds none, it creates nothing and fails with ``pending_create``.
"""

from __future__ import annotations

import uuid
from collections.abc import Mapping, Sequence
from typing import Any, Protocol

from scripts.training.run_tracking import TrackingError

PENDING = "pending_create."
CHAMPION_ALIAS = "production"
CHALLENGER_ALIAS = "challenger"


class RegistryStore(Protocol):
    """The MLflow client calls this module makes."""

    def get_registered_model(self, name: str) -> Any: ...

    def create_registered_model(self, name: str) -> Any: ...

    def search_model_versions(self, filter_string: str = "") -> Sequence[Any]: ...

    def create_model_version(self, name: str, source: str, run_id: str | None = None, tags: dict[str, str] | None = None) -> Any: ...

    def set_model_version_tag(self, name: str, version: str, key: str, value: str) -> None: ...

    def set_registered_model_alias(self, name: str, alias: str, version: str) -> None: ...

    def get_model_version_by_alias(self, name: str, alias: str) -> Any: ...

    def set_registered_model_tag(self, name: str, key: str, value: str) -> None: ...

    def delete_registered_model_tag(self, name: str, key: str) -> None: ...


def absent(error: Exception) -> bool:
    """Tell a missing registry entity from any other failure by MLflow's error code."""
    return getattr(error, "error_code", "") == "RESOURCE_DOES_NOT_EXIST"


def ensure_model(store: RegistryStore, name: str) -> Any:
    """Return the registered model, creating it once when it does not exist."""
    try:
        return store.get_registered_model(name)
    except Exception as error:
        if not absent(error):
            raise
    try:
        return store.create_registered_model(name)
    except Exception as error:
        if "already exists" not in str(error).lower() and getattr(error, "error_code", "") != "RESOURCE_ALREADY_EXISTS":
            raise
        return store.get_registered_model(name)


def versions_of(store: RegistryStore, name: str) -> list[Any]:
    """Return every version of a registered model."""
    return list(store.search_model_versions(f"name='{name}'"))


def champion(store: RegistryStore, name: str) -> Any | None:
    """Return the version the ``production`` alias names, or None when the alias is not set."""
    try:
        return store.get_model_version_by_alias(name, CHAMPION_ALIAS)
    except Exception as error:
        if absent(error):
            return None
        raise


def register(store: RegistryStore, name: str, source_run: str, step_key: str, tags: Mapping[str, str]) -> Any:
    """Return the one version of a source run, creating it once under a pending marker; a version with another bundle hash fails."""
    model = ensure_model(store, name)
    found = [version for version in versions_of(store, name) if version.run_id == source_run]
    markers = {key: value for key, value in dict(getattr(model, "tags", {}) or {}).items() if key.startswith(PENDING) and value == step_key}
    if len(found) > 1:
        raise TrackingError("model_version_conflict", f"{len(found)} versions of {name} come from the run {source_run}")
    if found:
        if found[0].tags.get("bundle_sha256") != tags["bundle_sha256"]:
            raise TrackingError("model_version_conflict", f"version {found[0].version} of {name} carries another bundle hash")
        for key in markers:
            store.delete_registered_model_tag(name, key)
        return found[0]
    if markers:
        identity = next(iter(markers)).removeprefix(PENDING)
        raise TrackingError("pending_create", f"an earlier create of {name} for {step_key} has no version yet (identity {identity}), so nothing was created")
    identity = uuid.uuid4().hex
    marker = PENDING + identity
    store.set_registered_model_tag(name, marker, step_key)
    version = store.create_model_version(name, f"runs:/{source_run}/model", run_id=source_run, tags={**tags, "create_identity": identity})
    store.delete_registered_model_tag(name, marker)
    return version
