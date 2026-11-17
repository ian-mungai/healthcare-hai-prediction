"""The copies of a Terraform stack that the offline serving proofs make: remove them once judged, so a run keeps its reports, not its provider binaries."""

from __future__ import annotations

import shutil
from pathlib import Path


def discard_copy(copy: Path, keep: bool) -> None:
    """Remove a copy of the stack, which holds the provider binary, unless it is evidence of a failure: the plans, states and reports stay either way."""
    if not keep:
        shutil.rmtree(copy)
