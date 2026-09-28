"""Validate human/agent documentation-review evidence against the staged Git snapshot.

Run ``.venv/bin/python -m scripts.quality.documentation_review prepare --reviewer NAME --reviewed-at UTC`` to draft
pending entries after staging the changes. Read the documents, fill their outcomes and notes, keep the record local and
run ``.venv/bin/python -m scripts.quality.documentation_review check``. This checks evidence, not the truth of its
assertions.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import tempfile
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import cast

from scripts.process import TimeoutExpired, run_command

RECORD = ".documentation_review.json"
POLICY = "README.md, Quality Checks: documentation review before every commit"
SUFFIXES = {".md", ".mdx", ".markdown", ".rst", ".adoc", ".asciidoc", ".txt", ".html", ".htm", ".pdf", ".doc", ".docx", ".ppt", ".pptx", ".odt"}
DOC_FOLDERS = {"docs", "doc", "documentation", "guides", "runbooks"}
DOC_NAMES = {"readme", "changelog", "changes", "contributing", "license", "copying", "authors", "notice"}
OUTCOMES = {"current", "updated", "historical"}


class ReviewError(ValueError):
    """A missing, stale or invalid review, safe to report without document contents."""


def git(*args: str) -> str:
    """Read Git state without returning failed-command output that may contain document content."""
    result = run_command("git", args)
    if result.returncode:
        raise ReviewError("Git state could not be read; run from the repository root and resolve index errors")
    return result.stdout


def inventory() -> dict[str, tuple[str, str]]:
    """Return staged path -> (mode, blob), rejecting conflicts and excluding only the review record itself."""
    files = {}
    for entry in git("ls-files", "--stage", "-z").split("\0"):
        if not entry:
            continue
        metadata, path = entry.split("\t", 1)
        mode, blob, stage = metadata.split()
        if stage != "0":
            raise ReviewError(f"{path!r}: unresolved index conflict; resolve it before reviewing")
        if path != RECORD:
            files[path] = (mode, blob)
    return files


def snapshot(files: dict[str, tuple[str, str]]) -> str:
    """Hash canonical staged paths, modes and blob IDs, including source/configuration context."""
    rows = sorted([path, mode, blob] for path, (mode, blob) in files.items())
    payload = json.dumps(rows, ensure_ascii=True, separators=(",", ":")).encode()
    return hashlib.sha256(payload).hexdigest()


def is_document(path: str) -> bool:
    """Discover conventional documentation; custom formats can be explicitly included in the record."""
    file = PurePosixPath(path.lower())
    return file.suffix in SUFFIXES or file.stem in DOC_NAMES or bool(set(file.parts[:-1]) & DOC_FOLDERS)


def document_paths(files: dict[str, tuple[str, str]], extra: list[str]) -> list[str]:
    """Return complete discoverable and explicitly included documentation, rejecting unsupported links."""
    for path in extra:
        parts = PurePosixPath(path)
        if path == RECORD or parts.is_absolute() or ".." in parts.parts or parts.as_posix() != path or path not in files:
            raise ReviewError("extra_documents: each entry must be an existing staged repository-relative file")
    paths = sorted({path for path in files if is_document(path)} | set(extra))
    for path in paths:
        if files[path][0] not in {"100644", "100755"}:
            raise ReviewError(f"{path!r}: documentation links/submodules are unsupported; review and stage a regular file")
    for path in git("ls-files", "--others", "--exclude-standard", "-z").split("\0"):
        if path and is_document(path):
            raise ReviewError(f"{path!r}: untracked documentation is unreviewed; include it in the staged review")
    return paths


def object_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    """Reject duplicate JSON keys instead of silently taking the last value."""
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise ReviewError("duplicate JSON key; retain one unambiguous value per field")
        value[key] = item
    return value


def parse_record(content: str) -> dict[str, object]:
    """Read a JSON object without exposing invalid input in diagnostics."""
    try:
        value: object = json.loads(content, object_pairs_hook=object_pairs)
    except json.JSONDecodeError as error:
        raise ReviewError(f"invalid JSON at line {error.lineno}; repair the record") from error
    if not isinstance(value, dict):
        raise ReviewError("record must be a JSON object")
    return cast(dict[str, object], value)


def utc_time(value: object) -> str:
    """Require an explicit UTC timestamp; never generate nondeterministic review assertions."""
    if not isinstance(value, str) or len(value) != 20:
        raise ReviewError("reviewed_at_utc must use YYYY-MM-DDTHH:MM:SSZ")
    try:
        datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError as error:
        raise ReviewError("reviewed_at_utc must be a valid UTC date in YYYY-MM-DDTHH:MM:SSZ format") from error
    return value


def extras(value: object) -> list[str]:
    """Require an explicit, unique list of additional documentation paths."""
    if not isinstance(value, list) or not all(isinstance(path, str) for path in value):
        raise ReviewError("extra_documents must be a list of repository-relative paths")
    paths = cast(list[str], value)
    if len(paths) != len(set(paths)):
        raise ReviewError("extra_documents contains duplicate paths")
    return paths


def validate(record: dict[str, object], files: dict[str, tuple[str, str]]) -> int:
    """Check staged evidence coverage, freshness and resolved per-document outcomes."""
    expected_keys = {"schema_version", "reviewer", "reviewed_at_utc", "snapshot_sha256", "extra_documents", "documents"}
    if set(record) != expected_keys or type(record.get("schema_version")) is not int or record.get("schema_version") != 1:
        raise ReviewError("schema_version must be 1 and all six documented fields must be present, with no unknown fields")
    reviewer = record["reviewer"]
    if not isinstance(reviewer, str) or not reviewer.strip():
        raise ReviewError("reviewer must identify the person or agent that performed the review")
    utc_time(record["reviewed_at_utc"])
    paths = document_paths(files, extras(record["extra_documents"]))
    if record["snapshot_sha256"] != snapshot(files):
        raise ReviewError("staged files changed since review; review the changes and refresh the evidence")
    items = record["documents"]
    if not isinstance(items, list):
        raise ReviewError("documents must be a list of per-document review outcomes")
    seen = set()
    for item in items:
        if not isinstance(item, dict) or set(item) != {"path", "blob", "outcome", "notes"}:
            raise ReviewError("each document needs exactly path, blob, outcome and notes")
        path = item["path"]
        if not isinstance(path, str) or path not in paths or path in seen:
            raise ReviewError("documents inventory contains an extra, invalid or duplicate path")
        seen.add(path)
        if item["blob"] != files[path][1]:
            raise ReviewError(f"{path!r}: reviewed blob differs from the staged document; review its staged content")
        if not isinstance(item["outcome"], str) or item["outcome"] not in OUTCOMES:
            raise ReviewError(f"{path!r}: review unresolved; use current, updated or historical only after completing the review")
        if not isinstance(item["notes"], str) or not item["notes"].strip():
            raise ReviewError(f"{path!r}: describe what was checked and any correction or historical limitation")
    missing = set(paths) - seen
    if missing:
        raise ReviewError(f"{sorted(missing)[0]!r}: missing review outcome; review every inventoried document")
    return len(paths)


def prepare(files: dict[str, tuple[str, str]], reviewer: str, reviewed_at: str, extra: list[str], refresh: bool) -> None:
    """Draft pending evidence atomically; retries preserve an existing review for the same snapshot.

    Parameters
    ----------
    files : dict
        Current staged paths, modes and blob IDs.
    reviewer : str
        Person or agent that will perform the substantive review.
    reviewed_at : str
        Explicit UTC timestamp supplied by the reviewer.
    extra : list[str]
        Other project documentation not found by conventional path/extension discovery.
    refresh : bool
        Explicit permission to replace a stale record with a pending draft. This never marks a document reviewed.
    """
    utc_time(reviewed_at)
    if not reviewer.strip():
        raise ReviewError("provide a nonempty reviewer")
    paths = document_paths(files, extras(extra))
    current_snapshot = snapshot(files)
    target = Path(RECORD)
    if target.exists():
        old = parse_record(target.read_text())
        if old.get("snapshot_sha256") == current_snapshot and old.get("extra_documents") == extra:
            sys.stdout.write("Existing evidence for this snapshot preserved; complete any pending outcomes in the local record.\n")
            return
        if not refresh:
            raise ReviewError("existing evidence is stale; preserve needed history, then use prepare --refresh to draft pending outcomes")
    record = {
        "schema_version": 1,
        "reviewer": reviewer,
        "reviewed_at_utc": reviewed_at,
        "snapshot_sha256": current_snapshot,
        "extra_documents": extra,
        "documents": [{"path": path, "blob": files[path][1], "outcome": "pending", "notes": ""} for path in paths],
    }
    with tempfile.NamedTemporaryFile(mode="w", dir=".", prefix=".documentation_review_", suffix=".tmp", delete=False) as handle:
        temporary = Path(handle.name)
        handle.write(json.dumps(record, indent=2, ensure_ascii=True) + "\n")
    temporary.replace(target)
    sys.stdout.write(f"Drafted {len(paths)} pending document reviews in {RECORD}; no review has been attested.\n")


def main() -> int:
    """Run the staged-state validator or draft pending evidence; report actionable, content-free failures."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("check")
    commands.add_parser("check-untracked", help="verify local review evidence is absent from the Git index")
    draft = commands.add_parser("prepare")
    draft.add_argument("--reviewer", required=True)
    draft.add_argument("--reviewed-at", required=True)
    draft.add_argument("--extra-document", action="append", default=[])
    draft.add_argument("--refresh", action="store_true")
    args = parser.parse_args()
    try:
        if Path.cwd().resolve() != Path(git("rev-parse", "--show-toplevel").strip()).resolve():
            raise ReviewError("run from the repository root")
        files = inventory()
        if git("ls-files", "--", RECORD).strip():
            raise ReviewError("review evidence must stay local; remove it from the index with git rm --cached -- .documentation_review.json")
        if args.command == "check-untracked":
            sys.stdout.write("documentation review: local-only storage verified; substantive review is checked locally, not in hosted CI\n")
        elif args.command == "prepare":
            prepare(files, args.reviewer, args.reviewed_at, args.extra_document, args.refresh)
        else:
            if not Path(RECORD).is_file():
                raise ReviewError("no local review record; prepare and complete the ignored evidence")
            count = validate(parse_record(Path(RECORD).read_text()), files)
            sys.stdout.write(f"documentation review: PASS ({count} documents; staged snapshot matches)\n")
    except (ReviewError, OSError, UnicodeError, TimeoutExpired) as error:
        message = str(error) if isinstance(error, ReviewError) else "a required file or program is inaccessible; check local setup"
        sys.stderr.write(
            f"{RECORD}:1: documentation review: BLOCK: {message}\n"
            f"  Rule: {POLICY}\n"
            "  Fix: stage the intended files, prepare the review, inspect every document, complete outcomes/notes in the ignored local record.\n"
            "  If this blocks a valid change, fix the check or ask the repository owner; do not bypass it.\n"
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
