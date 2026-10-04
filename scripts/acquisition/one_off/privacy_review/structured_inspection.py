"""Structured privacy inspection: receipt classifications and tabular schemas across the whole project.

Writes counts, paths and column names only; never cell values. Run from the repository root:

    .venv/bin/python scripts/acquisition/one_off/privacy_review/structured_inspection.py
"""

import collections
import hashlib
import json
import os
import re
import sys
from pathlib import Path
from typing import Any

OUT = Path("data/privacy_review/20260927/structured_inspection.json")
SKIP_DIRS = {".git", ".venv", ".mypy_cache", ".ruff_cache", ".pytest_cache", "__pycache__"}
TABULAR = {".csv", ".tsv", ".txt", ".dat"}
# Column names that can hold data about an identifiable person; facility, place and area names are excluded below.
PERSONAL = re.compile(
    r"(first.?name|last.?name|middle|surname|given.?name|full.?name|person|owner|officer|director|administrator|contact|"
    r"phone|telephone|fax|e.?mail|street|address|ssn|social.?security|birth|dob|signature|npi|physician|provider.?name|"
    r"patient|member.?id|beneficiary.?id|medical.?record|resident)",
    re.IGNORECASE,
)
PUBLIC_PLACE = re.compile(r"(county|state|city|facility|hospital|organization|org|agency|legal.?business|doing.?business|fips|zip)", re.IGNORECASE)


def walk() -> list[Path]:
    files: list[Path] = []
    for directory, subdirs, names in os.walk("."):
        subdirs[:] = [d for d in subdirs if d not in SKIP_DIRS and not directory.startswith("./data/privacy_review")]
        files.extend(Path(directory, n) for n in names if not Path(directory, n).is_symlink())
    return files


def receipts(files: list[Path]) -> dict:
    by_source: dict = collections.defaultdict(lambda: collections.Counter())
    for path in files:
        if path.name != "receipt.json":
            continue
        try:
            receipt = json.loads(path.read_text())
            governance, source = receipt.get("governance") or {}, receipt.get("source") or {}
        except (ValueError, UnicodeError, AttributeError):
            by_source["unparsed"]["receipts"] += 1
            continue
        if not isinstance(governance, dict):
            by_source["no_governance"]["receipts"] += 1
            continue
        key = str(source.get("source_record_id") or source.get("source_id") or "unknown")
        counter = by_source[key]
        counter["receipts"] += 1
        counter[f"contains_pii={governance.get('contains_pii')}"] += 1
        counter[f"contains_phi={governance.get('contains_phi')}"] += 1
        counter[f"access_class={governance.get('access_class')}"] += 1
    return {k: dict(v) for k, v in sorted(by_source.items())}


def header(path: Path) -> list[str] | None:
    with path.open("rb") as handle:
        first = handle.readline(1 << 20)
    text = first.decode("utf-8", errors="replace").strip("﻿\r\n")
    delimiter = "\t" if text.count("\t") > text.count(",") and text.count("\t") > 0 else ("|" if text.count("|") > text.count(",") else ",")
    columns = [c.strip().strip('"') for c in text.split(delimiter)]
    return columns if len(columns) > 1 else None


def schemas(files: list[Path]) -> dict:
    groups: dict = {}
    headerless: collections.Counter = collections.Counter()
    for path in files:
        if path.suffix.lower() not in TABULAR or path.stat().st_size == 0:
            continue
        columns = header(path)
        top = "/".join(path.parts[:4])
        if columns is None:
            headerless[top] += 1
            continue
        key = hashlib.sha256("\x1f".join(columns).encode()).hexdigest()[:16]
        group = groups.setdefault(key, {"files": 0, "bytes": 0, "sample_path": str(path), "folders": collections.Counter(), "columns": len(columns)})
        group["files"] += 1
        group["bytes"] += path.stat().st_size
        group["folders"][top] += 1
        if "personal_columns" not in group:
            group["personal_columns"] = [c[:80] for c in columns if PERSONAL.search(c) and not PUBLIC_PLACE.search(c)]
    for group in groups.values():
        group["folders"] = dict(group["folders"].most_common(5))
    flagged = {k: v for k, v in groups.items() if v["personal_columns"]}
    return {
        "schema_groups": len(groups),
        "files_with_header": sum(g["files"] for g in groups.values()),
        "headerless_files_by_folder": dict(headerless.most_common()),
        "flagged_groups": dict(sorted(flagged.items(), key=lambda kv: -kv[1]["files"])),
    }


def main() -> int:
    files = walk()
    result: dict[str, Any] = {"files_walked": len(files), "receipts_by_source": receipts(files), "tabular": schemas(files)}
    OUT.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    sys.stdout.write(f"walked={len(files)} flagged_groups={len(result['tabular']['flagged_groups'])}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
