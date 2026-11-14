"""Transfer immutable files through an injected, deadline-enforcing storage transport."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Protocol

from .integrity import SHA256, StageError, safe_path


class StorageError(RuntimeError):
    """A sanitized transport error, never an SDK response body."""

    def __init__(self, code: str, status: int) -> None:
        super().__init__("storage_call_failed")
        self.code = code
        self.status = status


class Storage(Protocol):
    """A storage adapter whose calls and streamed file bodies obey the supplied deadline.

    File bodies travel through the named file slice or destination, never a reply
    frame. A production adapter must use owned workers rather than blocking SDK
    calls in the controller. This interface grants no identity or credential scope.
    """

    def invoke(self, operation: str, request: dict[str, Any], deadline: float) -> dict[str, Any]: ...


@dataclass(frozen=True)
class FileRef:
    """An immutable logical file reference with the exact storage version and content."""

    path: str
    key: str
    version_id: str
    sha256: str
    size: int

    def __post_init__(self) -> None:
        safe_path(self.path)
        safe_path(self.key)
        if not self.version_id or self.version_id.casefold() == "null" or not SHA256.fullmatch(self.sha256):
            raise StageError("invalid_reference")
        if type(self.size) is not int or self.size < 0:
            raise StageError("invalid_reference")

    def document(self) -> dict[str, Any]:
        """Return the canonical input-map entry."""
        return asdict(self)
