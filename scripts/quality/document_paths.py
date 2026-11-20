"""Find a user directory path inside an Office or PDF file without ever reporting its value.

The privacy scan reads text files line by line and skips a file with NUL bytes as unreviewed. A workbook or a PDF holds text inside a package or a compressed
stream, where a workstation path can hide (an absolute path element of a workbook, a title in a PDF information dictionary). This module looks at the text
members of an OOXML package, at the bytes of a legacy Office file in both common encodings, and at a PDF's bytes and its decompressed streams. A hit is
reported by the member's position and kind (never its name, which a publisher's archive controls) or by ``(file)`` and ``(stream)`` only.
The caller passes the line test of the privacy scan, so the two scans agree on what a user directory path is: a Windows drive path under ``Users``,
``/Users/<name>`` and ``/home/<name>``, with placeholders and the macOS shared folder exempt.

Limits: a PDF text layer in a custom font encoding or an image of a path is not read, and an encrypted PDF stream is read as bytes only.
"""

from __future__ import annotations

import io
import re
import zipfile
import zlib
from collections.abc import Callable
from pathlib import PurePosixPath

OFFICE_SUFFIXES = frozenset({".xlsx", ".xlsm", ".docx", ".pptx"})
LEGACY_SUFFIXES = frozenset({".xls", ".doc", ".ppt"})
# The kinds the privacy scan reads: every Office, legacy Office and PDF file.
DOCUMENT_SUFFIXES = OFFICE_SUFFIXES | LEGACY_SUFFIXES | {".pdf"}
MAX_MEMBER_BYTES = 256 * 1024**2
MAX_STREAM_BYTES = 64 * 1024**2
# The part of a line around a possible path that is handed to the line test, so a one-line XML member costs only its matches.
WINDOW = (200, 600)
HINT = re.compile(rb"users|home|~|file:", re.IGNORECASE)
STREAM = re.compile(rb"stream\r?\n(.*?)\r?\nendstream", re.DOTALL)


class UnreadableDocument(ValueError):
    """A document that cannot be read as it is. Its text is fixed and holds nothing from the file: no member name, no decoder message."""


def views(body: bytes) -> list[str]:
    """The readings of a member that a person's path can take: UTF-8 or Latin-1 text, and UTF-16 either way round."""
    texts = [body.decode("utf-8", errors="replace") if b"\0" not in body else body.decode("latin-1")]
    if b"\0" in body or body[:2] in (b"\xff\xfe", b"\xfe\xff"):
        # Dropping the NUL bytes reads UTF-16 text of any alignment as ASCII: a legacy Office string starts at an odd offset as often as at an even one.
        texts += [body.decode("utf-16-le", errors="ignore"), body.decode("utf-16-be", errors="ignore"), body.replace(b"\0", b"").decode("latin-1")]
    if body[:2] in (b"\xff\xfe", b"\xfe\xff"):
        texts.append(body.decode("utf-16", errors="ignore"))
    return texts


def has_path(body: bytes, found: Callable[[str], bool]) -> bool:
    """Whether any reading of ``body`` holds a user directory path, judged by the privacy scan's own line test on the text around each hint."""
    if not HINT.search(body) and b"\0" not in body:
        return False
    for text in views(body):
        for match in re.finditer(r"users|home|~|file:", text, re.IGNORECASE):
            line = text[max(0, match.start() - WINDOW[0]) : match.end() + WINDOW[1]]
            if found(line):
                return True
    return False


def office_members(data: bytes) -> list[tuple[str, bytes]]:
    """Every member record of an Office package, in archive order, as ``(name, body)``. The name is untrusted: it only picks a fixed kind label.

    Two records with the same name (Office part names ignore case) are refused, so that no body hides behind another of one name. Any failure to open
    the archive or to read a record is refused with a fixed text, whatever the reader raised: the reader's own messages quote member names and bytes.
    The diagnostics name a member by its position and never by its name.
    """
    try:
        archive = zipfile.ZipFile(io.BytesIO(data))
    except Exception:
        raise UnreadableDocument("an Office package cannot be read as a ZIP archive") from None
    with archive:
        infos = archive.infolist()
        names = [info.filename.casefold() for info in infos]
        if len(set(names)) != len(names):
            raise UnreadableDocument("an Office package holds two members with the same name")
        members = []
        for position, info in enumerate(infos, 1):
            if info.file_size > MAX_MEMBER_BYTES:
                raise UnreadableDocument(f"member {position} is too large to scan")
            try:
                members.append((info.filename, archive.read(info)))
            except Exception:
                raise UnreadableDocument(f"member {position} cannot be read") from None
    return members


def member_location(position: int, name: str) -> str:
    """A fixed location for a member: its 1-based position and a kind from a closed list. The name itself is never part of it."""
    kind = {".xml": "xml", ".rels": "rels"}.get(PurePosixPath(name.casefold()).suffix, "other")
    return f"member {position} ({kind})"


def is_text(body: bytes) -> bool:
    """Whether almost every byte is printable ASCII or white space, which is how a plain content stream or a metadata stream reads."""
    return not body or sum(32 <= byte < 127 or byte in (9, 10, 13) for byte in body) >= 0.9 * len(body)


def pdf_streams(data: bytes) -> list[bytes]:
    """Every stream of a PDF, decompressed when it is zlib data. A stream that does not read as text is left out."""
    streams = []
    total = 0
    for match in STREAM.finditer(data):
        raw = match.group(1)
        try:
            body = zlib.decompressobj().decompress(raw, MAX_STREAM_BYTES)
        except zlib.error:
            body = raw
        # Image and font bytes spell a named tilde path by chance; only a stream that reads as text can hold a path.
        if not is_text(body):
            continue
        total += len(body)
        if total > 4 * MAX_STREAM_BYTES:
            raise UnreadableDocument("the PDF streams are too large to scan")
        streams.append(body)
    return streams


def members_with_user_path(data: bytes, suffix: str, found: Callable[[str], bool]) -> list[str]:
    """The members of an Office or PDF file that hold a user directory path: ``member <position> (<kind>)`` for a package, else ``(file)``, ``(stream)``.

    Raises UnreadableDocument (a ValueError), with a text that holds nothing from the file, for a package or a stream set that cannot be read,
    so that an unreadable file is never taken as clean.
    """
    suffix = suffix.lower()
    if suffix in OFFICE_SUFFIXES:
        return [member_location(position, name) for position, (name, body) in enumerate(office_members(data), 1) if has_path(body, found)]
    hits = []
    # The syntax of a PDF is read without its stream bodies: compressed bytes spell a named tilde path by chance, and the streams are read decoded below.
    if has_path(STREAM.sub(b"stream\nendstream", data) if suffix == ".pdf" else data, found):
        hits.append("(file)")
    if suffix == ".pdf" and any(has_path(stream, found) for stream in pdf_streams(data)):
        hits.append("(stream)")
    return sorted(hits)
