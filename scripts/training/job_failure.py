"""The named failure and exit codes that every training job action shares."""

from __future__ import annotations

EXIT_FAILED, EXIT_NOT_IMPLEMENTED, EXIT_DEADLINE = 1, 3, 4


class JobFailure(Exception):
    """A named, visible failure of one action."""

    def __init__(self, name: str, detail: str, code: int = EXIT_FAILED) -> None:
        super().__init__(detail)
        self.name, self.detail, self.code = name, detail, code
