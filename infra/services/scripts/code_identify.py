"""Identity one-shot of the host: print the code run record of the release that /host/repo shows. It is the command of a code_fetch task and changes nothing."""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path

SELECTED = re.compile(r"repo_releases/([a-f0-9]{64})(\.r[0-9]+)?/code")
# A code run ID: a commit time and eight digits of the commit (staged by CI), or a run ID <name>_<8 hex>[_<attempt>] (the run its code is staged under).
RUN_ID = re.compile(r"[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}|[a-z]+(?:_[a-z]+)*_[a-f0-9]{8}(?:_[0-9]{1,2}){0,2}")


def run(host: Path) -> int:
    """Print ``code_fetch: identity run <run ID> code <digest>`` for the release that repo shows, or refuse. The record of another release is never used."""
    link = host / "repo"
    found = SELECTED.fullmatch(os.readlink(link)) if link.is_symlink() else None
    if found is None:
        sys.stderr.write("code_identify refused: no code run record: repo shows no staged release\n")
        return 1
    try:
        text = (host / "repo_releases" / f"{found.group(1)}{found.group(2) or ''}" / "run_id").read_text(encoding="utf-8")
    except OSError:
        sys.stderr.write(f"code_identify refused: no code run record for release {found.group(1)[:12]}\n")
        return 1
    if not text.endswith("\n") or not RUN_ID.fullmatch(text[:-1]):
        sys.stderr.write(f"code_identify refused: the code run record of release {found.group(1)[:12]} is not a run ID\n")
        return 1
    sys.stdout.write(f"code_identify: identity run {text[:-1]} code {found.group(1)}\n")
    return 0


if __name__ == "__main__":
    sys.exit(run(Path("/host")))
