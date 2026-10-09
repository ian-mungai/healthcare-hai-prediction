"""Read paths that records written before the dataset move still name (failure modes 224 to 229).

The local dataset folders live under data/datasets/. Records are evidence and keep the paths they were
written with; code that follows a recorded path asks this map where it lives now.
"""

import json
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
MAP_PATH = REPO_ROOT / "config/data_paths.json"


def moved(path: Path = MAP_PATH) -> dict[str, str]:
    """Return the moved folders, repository-relative old path to new path."""
    return dict(json.loads(path.read_text(encoding="utf-8"))["moved"])


def current(recorded: str, moves: dict[str, str] | None = None) -> str:
    """Return where a recorded path lives now; repository-relative and data/-relative paths are both mapped."""
    for old, new in (moved() if moves is None else moves).items():
        for before, after in ((old, new), (old.removeprefix("data/"), new.removeprefix("data/"))):
            if recorded == before or recorded.startswith(before + "/"):
                return after + recorded[len(before) :]
    return recorded
