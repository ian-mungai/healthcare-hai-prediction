"""Declare complete read-job results and refuse every undeclared output."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Literal

from .integrity import MAX_RECORD, StageError, safe_path

TABLE = re.compile(r"[a-z][a-z0-9_]*")


@dataclass(frozen=True)
class Output:
    """One exact logical name, transfer classification and approved byte bound."""

    name: str
    kind: Literal["controller_record", "data"]
    max_bytes: int

    def __post_init__(self) -> None:
        safe_path(self.name)
        if self.kind not in ("controller_record", "data") or type(self.max_bytes) is not int or self.max_bytes < 0:
            raise StageError("invalid_declaration")
        if self.kind == "controller_record" and self.max_bytes > MAX_RECORD:
            raise StageError("record_bound")
