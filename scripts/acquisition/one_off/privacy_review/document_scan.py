"""Scan non-plain-text files for the privacy review: ZIP members, OOXML, OLE (.xls/.doc/.ppt), gzip and PDF text.

Counts pattern shapes per file and records metadata presence only; never stores matched values or text.
Run from the repository root; PDF text arrives on stdin as JSON lines from pdf_text.swift (PDFKit):

    find . -name .git -prune -o -name .mypy_cache -prune -o -path ./data/privacy_review -prune -o -iname '*.pdf' -type f -print \
      | swift scripts/acquisition/one_off/privacy_review/pdf_text.swift | .venv/bin/python scripts/acquisition/one_off/privacy_review/document_scan.py
"""

import collections
import gzip
import io
import json
import os
import re
import sys
import zipfile
from collections.abc import Callable
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

from scripts.process import run_command

HERE = Path("data/privacy_review/20260927")
OUT = HERE / "document_scan.json"
SKIP_DIRS = {".git", ".mypy_cache", ".ruff_cache", ".pytest_cache", "__pycache__"}
ARCHIVES = {".zip", ".xlsx", ".xlsm", ".docx", ".pptx", ".tfplan", ".whl", ".jar"}
OLE = {".xls", ".doc", ".ppt"}
GENERIC_MAILBOX = re.compile(
    rb"^(info|contact|support|help|quality|admin|office|questions?|webmaster|survey|data|team|program|compliance|mail|inquir|feedback|customer|no-?reply|press|media|"
    rb"hospital|census|cms|bls|hrsa|cdc|ahrq|hcai|oshpd|dph|doh|health|stats?|research|report)",
    re.IGNORECASE,
)


def patterns() -> dict[str, re.Pattern[bytes]]:
    account = next((ln.split("=", 1)[1].strip().strip('"') for ln in Path(".env").read_text().splitlines() if ln.startswith("AWS_ACCOUNT_ID=")), "")
    found = {
        "email": re.compile(rb"[A-Za-z0-9._%+-]{1,64}@[A-Za-z0-9.-]{1,253}\.[A-Za-z]{2,24}"),  # Bounded: no quadratic backtracking.
        "us_phone": re.compile(rb"\(?\b[2-9][0-9]{2}\)?[-. ][0-9]{3}[-. ][0-9]{4}\b"),
        "ssn_shape": re.compile(rb"\b[0-9]{3}-[0-9]{2}-[0-9]{4}\b"),
        "aws_arn_with_account": re.compile(rb"arn:aws:[a-z0-9-]+:[a-z0-9-]*:[0-9]{12}:"),
        "aws_access_key_id": re.compile(rb"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b"),
        "private_key_header": re.compile(rb"-----BEGIN [A-Z ]*PRIVATE KEY-----"),
        "local_home_path": re.compile(re.escape(os.path.expanduser("~").encode())),
    }
    # The owner's address comes from local Git settings, never from this file.
    email = run_command("git", ["config", "user.email"]).stdout.strip()
    if email:
        found["user_email"] = re.compile(re.escape(email.encode()), re.IGNORECASE)
    if account:
        found["project_account_id"] = re.compile(re.escape(account.encode()))
    return found


PATTERNS = patterns()


def count(data: bytes, into: collections.Counter) -> None:
    for name, pattern in PATTERNS.items():
        if name == "email" and b"@" not in data:
            continue
        matches = pattern.findall(data)
        if matches:
            into[name] += len(matches)
            if name == "email":
                for match in pattern.finditer(data):
                    local = match.group(0).split(b"@", 1)[0]
                    into["email_generic_mailbox" if GENERIC_MAILBOX.match(local) else "email_personal_like"] += 1


def scan_stream(read: Callable[[int], bytes], into: collections.Counter) -> None:
    tail = b""
    while block := read(8 << 20):
        count(tail + block, into)
        tail = block[-256:]  # Overlap so matches spanning chunks are seen; may double count a boundary match.


def scan_zip(path: Path | io.BytesIO, into: collections.Counter, depth: int = 0) -> None:
    with zipfile.ZipFile(path) as archive:
        for member in archive.infolist():
            if member.is_dir():
                continue
            into["members"] += 1
            name = member.filename.lower()
            if name.endswith("docprops/core.xml"):
                core = archive.read(member)
                if re.search(rb"<dc:creator>[^<]+</dc:creator>|<cp:lastModifiedBy>[^<]+</cp:lastModifiedBy>", core):
                    into["office_author_metadata"] += 1
            try:
                with archive.open(member) as handle:
                    if depth < 2 and Path(name).suffix in ARCHIVES:
                        scan_zip(io.BytesIO(handle.read()), into, depth + 1)  # In memory, so parallel workers never share a temp file.
                        continue
                    scan_stream(handle.read, into)
            except (zipfile.BadZipFile, NotImplementedError, OSError, EOFError, ValueError):
                into["unreadable_members"] += 1


def scan_ole(path: Path, into: collections.Counter) -> None:
    data = path.read_bytes()
    count(data, into)
    count(data.decode("utf-16-le", errors="ignore").encode("utf-8"), into)  # BIFF/Word strings are often UTF-16LE.
    if b"\x05\x00S\x00u\x00m\x00m\x00a\x00r\x00y\x00I\x00n\x00f\x00o" in data or b"SummaryInformation" in data:
        into["ole_summary_metadata_present"] += 1


def pdfs(paths: list[Path]) -> dict[str, collections.Counter]:
    """Read PDFKit JSON lines from stdin; every walked PDF without a record is reported as not processed."""
    results: dict[str, collections.Counter] = {}
    for line in sys.stdin:
        record = json.loads(line)
        counter: collections.Counter = collections.Counter()
        if "error" in record:
            counter["unreadable_pdf"] += 1
        else:
            counter["pdf_pages"] += record["pages"]
            counter["pdf_text_chars"] += len(record["text"])
            counter["pdf_locked"] += int(record["locked"])
            counter["pdf_author_metadata"] += int(record["has_author"])
            if not record["text"].strip():
                counter["pdf_no_text_layer"] += 1
            count(record["text"].encode("utf-8"), counter)
        results[str(Path(record["path"]))] = counter
    for path in paths:
        results.setdefault(str(path), collections.Counter({"pdf_not_processed": 1}))
    return results


def scan_one(name: str) -> tuple[str, dict[str, int]]:
    """Scan one archive, OLE or gzip file in a worker process."""
    path = Path(name)
    suffix = path.suffix.lower()
    counter: collections.Counter = collections.Counter()
    try:
        if suffix in ARCHIVES:
            scan_zip(path, counter)
        elif suffix in OLE:
            scan_ole(path, counter)
        else:
            with gzip.open(path) as handle:
                scan_stream(handle.read, counter)
    except (zipfile.BadZipFile, OSError, EOFError, ValueError) as error:
        counter[f"unreadable:{type(error).__name__}"] += 1
    return name, dict(counter)


def main() -> int:
    candidates: list[str] = []
    pdf_paths: list[Path] = []
    for directory, subdirs, names in os.walk("."):
        subdirs[:] = [d for d in subdirs if d not in SKIP_DIRS and not directory.startswith("./data/privacy_review")]
        for name in names:
            path = Path(directory, name)
            suffix = path.suffix.lower()
            if path.is_symlink():
                continue
            if suffix == ".pdf":
                pdf_paths.append(path)
            elif suffix in ARCHIVES or suffix in OLE or suffix == ".gz":
                candidates.append(str(path))
    candidates.sort(key=lambda n: -os.path.getsize(n))  # Largest first balances the workers.
    per_file: dict[str, dict[str, int]] = {}
    with ProcessPoolExecutor(max_workers=14) as pool:
        for name, counts in pool.map(scan_one, candidates, chunksize=2):
            per_file[name] = counts
    for pdf_path, pdf_counter in pdfs(pdf_paths).items():
        per_file[pdf_path] = dict(pdf_counter)
    OUT.write_text(json.dumps(per_file, indent=1, sort_keys=True) + "\n")
    totals: collections.Counter = collections.Counter()
    for counts in per_file.values():
        totals.update({k: 1 for k, v in counts.items() if v})
    sys.stdout.write(f"files={len(per_file)} files_with={dict(totals)}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
