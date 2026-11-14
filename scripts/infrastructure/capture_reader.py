"""Read an exported deployment capture as one bounded JSON object, with its byte digest, for the configuration renderers."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any


class CaptureError(ValueError):
    """Reject an incomplete deployment capture without disclosing its contents."""


def _pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise CaptureError("Capture contains a duplicate JSON field.")
        value[key] = item
    return value


def _constant(value: str) -> None:
    raise CaptureError("Capture contains a nonfinite JSON number.")


def _decode(content: str) -> Any:
    try:
        return json.loads(content, object_pairs_hook=_pairs, parse_constant=_constant)
    except (ValueError, RecursionError) as error:
        if isinstance(error, CaptureError):
            raise
        raise CaptureError("Capture contains invalid JSON.") from None


def _allowed_path(path: Path) -> None:
    for candidate in (path, path.resolve()):
        parts = candidate.parts
        if (
            any(part == ".env" or part.startswith(".env.") or part.endswith((".tfvars", ".tfvars.json")) or "terraform.tfstate" in part for part in parts)
            or "secrets" in parts
            or parts[-3:] == ("config", "acquisition", "source_registry.json")
        ):
            raise CaptureError("Use exported output captures, not protected deployment or credential files.")


def read_capture_with_digest(path: Path) -> tuple[dict[str, Any], str]:
    """Return one capture and its byte digest from the same bounded file read."""
    _allowed_path(path)
    try:
        with path.open("rb") as handle:
            content = handle.read(32 * 1024**2 + 1)
    except OSError:
        raise CaptureError("Could not read the selected capture.") from None
    if len(content) > 32 * 1024**2:
        raise CaptureError("A capture exceeds the 32 MiB record bound.")
    value = _decode(content.decode("utf-8"))
    if not isinstance(value, dict):
        raise CaptureError("A capture must contain a JSON object.")
    return value, hashlib.sha256(content).hexdigest()
