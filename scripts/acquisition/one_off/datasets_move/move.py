"""Move the local dataset folders into data/datasets/ with one rename each; compare file counts and bytes (224)."""

import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

HERE = Path("data/lakehouse_planning/datasets_move_20261004")


def tally(root: Path) -> tuple[int, int]:
    files = size = 0
    for folder, _, names in os.walk(root):
        for name in names:
            path = Path(folder) / name
            if not path.is_symlink():
                files += 1
                size += path.stat().st_size
    return files, size


moved = json.loads(Path("config/data_paths.json").read_text())["moved"]
folders: dict[str, dict[str, object]] = {}
record = {"started_utc": datetime.now(UTC).isoformat(), "folders": folders}
for old, new in moved.items():
    source, target = Path(old), Path(new)
    if target.exists():
        raise SystemExit(f"{target} already exists; nothing moved for it")
    if not source.is_dir():
        raise SystemExit(f"{source} is missing")
    before = tally(source)
    target.parent.mkdir(exist_ok=True)
    os.rename(source, target)
    after = tally(target)
    folders[old] = {"to": new, "files": before[0], "bytes": before[1], "matches_after": before == after, "old_path_gone": not source.exists()}
    if before != after or source.exists():
        raise SystemExit(f"{old}: counts differ after the move")
record["finished_utc"] = datetime.now(UTC).isoformat()
(HERE / "move_record.json").write_text(json.dumps(record, indent=1) + "\n")
sys.stdout.write(json.dumps(record["folders"], indent=1) + "\n")
