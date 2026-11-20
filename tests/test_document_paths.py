"""A user directory path inside an Office or PDF file is found by member and kind, and its value is never reported."""

from __future__ import annotations

import io
import struct
import warnings
import zipfile
import zlib
from collections.abc import Callable
from pathlib import Path

import pytest

from scripts.quality import document_paths, repo_checks
from tests.support import check

WINDOWS = "C:\\" + "Users\\synthetic_person\\Documents\\book.xlsx"
MAC = "/" + "Users/synthetic_person/Documents/book.xlsx"
# A named tilde path, as compressed bytes spell it by chance; built from parts so the file holds no such path itself.
TILDE_NAME = b"~" + b"abc" + b"/" + b"def"
LINUX = "/" + "home/synthetic_person/work/book.xlsx"


def office(members: dict[str, bytes]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, body in members.items():
            archive.writestr(name, body)
    return buffer.getvalue()


def workbook_with(path: str) -> bytes:
    xml = f'<workbook><mc:AlternateContent><mc:Choice Requires="x15"><x15ac:absPath url="{path}"/></mc:Choice></mc:AlternateContent></workbook>'
    return office({"[Content_Types].xml": b"<Types/>", "xl/workbook.xml": xml.encode(), "xl/sharedStrings.xml": b"<sst>hospital names only</sst>"})


def found(data: bytes, suffix: str) -> list[str]:
    return document_paths.members_with_user_path(data, suffix, repo_checks.home_path_found)


@pytest.mark.parametrize(
    "path",
    [WINDOWS, MAC, LINUX, WINDOWS.replace("\\", "/"), WINDOWS.replace("\\", "\\\\")],
    ids=["windows", "mac", "linux", "windows_slashes", "windows_escaped"],
)
def test_a_user_directory_path_in_an_office_member_is_found_by_member_and_never_reported(path: str) -> None:
    result = found(workbook_with(path), ".xlsx")
    check(result == ["member 2 (xml)"], f"found {result}")
    check(all("synthetic_person" not in item for item in result), "the value is never part of the result")


@pytest.mark.parametrize("suffix", [".xlsx", ".xlsm", ".docx", ".pptx"])
def test_every_office_package_kind_is_read_by_its_members(suffix: str) -> None:
    check(found(workbook_with(WINDOWS), suffix) == ["member 2 (xml)"], f"{suffix} package")


def test_a_clean_office_file_and_a_placeholder_path_pass() -> None:
    clean = office({"[Content_Types].xml": b"<Types/>", "xl/workbook.xml": b"<workbook/>"})
    check(found(clean, ".xlsx") == [], "a clean workbook has no finding")
    placeholder = workbook_with("C:\\" + "Users\\<user>\\Documents\\book.xlsx")
    check(found(placeholder, ".xlsx") == [], "a placeholder names no user")
    check(found(workbook_with("/" + "Users/Shared/book.xlsx"), ".xlsx") == [], "the macOS shared folder is no user's home")


def test_a_path_in_a_utf_16_member_or_a_legacy_workbook_is_found() -> None:
    check(found(office({"docProps/custom.xml": WINDOWS.encode("utf-16")}), ".xlsx") == ["member 1 (xml)"], "a UTF-16 member")
    legacy = b"\xd0\xcf\x11\xe0 header " + WINDOWS.encode("utf-16-le") + b" tail"
    check(found(legacy, ".xls") == ["(file)"], "a legacy workbook is read as bytes in both encodings")
    check(found(b"\xd0\xcf\x11\xe0 header only", ".xls") == [], "a clean legacy workbook passes")
    odd = b"\xd0\xcf\x11\xe0-" + WINDOWS.encode("utf-16-le") + b" tail"
    check(found(odd, ".xls") == ["(file)"], "a string at an odd offset is read as well")


def pdf_with(body: bytes, *, compressed: bool, info: bytes = b"") -> bytes:
    stream = zlib.compress(body) if compressed else body
    return b"%PDF-1.4\n1 0 obj\n<< /Length " + str(len(stream)).encode() + b" >>\nstream\n" + stream + b"\nendstream\nendobj\n" + info + b"%%EOF\n"


def test_a_path_in_a_pdf_stream_or_its_information_dictionary_is_found() -> None:
    text = f"BT ({WINDOWS.replace(chr(92), chr(92) * 2)}) Tj ET".encode()
    check(found(pdf_with(text, compressed=True), ".pdf") == ["(stream)"], "a compressed content stream")
    check(found(pdf_with(text, compressed=False), ".pdf") == ["(stream)"], "a plain stream is found as a stream, not as file syntax")
    binary = bytes(range(128, 256)) * 3 + TILDE_NAME + bytes(range(128, 200))
    check(found(pdf_with(binary, compressed=False), ".pdf") == [], "an undecodable binary stream is no text")
    noise = zlib.compress(b"x") + b"\n" + TILDE_NAME
    check(found(pdf_with(noise, compressed=False), ".pdf") == [], "tilde-like bytes inside a stream body are no path")
    info = f"trailer << /Title ({MAC}) >>\n".encode()
    check(found(pdf_with(b"BT (hello) Tj ET", compressed=True, info=info), ".pdf") == ["(file)"], "the information dictionary")
    check(found(pdf_with(b"BT (hello) Tj ET", compressed=True), ".pdf") == [], "a clean PDF passes")


def test_an_oversized_or_broken_member_is_not_trusted_as_clean() -> None:
    with pytest.raises(ValueError, match="cannot be read"):
        found(b"PK\x03\x04 not a zip", ".xlsx")


def test_the_privacy_scan_reports_a_document_by_member_without_the_value() -> None:
    data = workbook_with(WINDOWS)
    findings = repo_checks.document_findings("config/example.xlsx", data, [])
    check(len(findings) == 1 and findings[0].location == "config/example.xlsx!member 2 (xml)", f"{findings}")
    check("synthetic_person" not in repr(findings), "the value is not in the finding")
    allowed = [("home-path", "config/*.xlsx")]
    check(repo_checks.document_findings("config/example.xlsx", data, allowed) == [], "a reasoned allowlist entry still applies")


DUPLICATE_REFUSAL = "unreadable document: an Office package holds two members with the same name"


def named_archive(members: list[tuple[str, bytes]], method: int = zipfile.ZIP_DEFLATED) -> bytes:
    """An archive written member by member, so a name can repeat (the writer warns about a duplicate name and writes both records)."""
    buffer = io.BytesIO()
    with warnings.catch_warnings(), zipfile.ZipFile(buffer, "w", method) as archive:
        warnings.simplefilter("ignore", UserWarning)
        for name, body in members:
            archive.writestr(name, body)
    return buffer.getvalue()


def test_a_path_in_the_first_of_two_members_with_the_same_name_is_refused_and_so_are_two_clean_ones() -> None:
    xml = b'<workbook><x15ac:absPath url="' + WINDOWS.encode() + b'"/></workbook>'
    first_holds = named_archive([("xl/workbook.xml", xml), ("xl/workbook.xml", b"<workbook/>")])
    [finding] = repo_checks.document_findings("config/example.xlsx", first_holds, [])
    check(finding.location == "config/example.xlsx" and finding.problem == DUPLICATE_REFUSAL, f"{finding}")
    both_clean = named_archive([("xl/workbook.xml", b"<workbook/>"), ("xl/workbook.xml", b"<other/>")])
    [finding] = repo_checks.document_findings("config/example.xlsx", both_clean, [])
    check(finding.problem == DUPLICATE_REFUSAL, f"two clean members of one name are refused as a duplicate: {finding}")
    case_variant = named_archive([("xl/workbook.xml", b"<workbook/>"), ("XL/Workbook.xml", b"<other/>")])
    check(repo_checks.document_findings("config/example.xlsx", case_variant, [])[0].problem == DUPLICATE_REFUSAL, "Office part names ignore case")
    with pytest.raises(ValueError, match="two members with the same name"):
        found(first_holds, ".xlsx")


def test_every_member_record_is_read_even_when_a_name_is_a_directory_or_repeats_a_path_like_name() -> None:
    data = named_archive([("xl/", WINDOWS.encode()), ("xl/workbook.xml", b"<workbook/>")])
    check(found(data, ".xlsx") == ["member 1 (other)"], "a record named like a directory is read too")


def test_the_five_seeded_workbooks_and_the_legacy_workbook_pass_the_document_scan() -> None:
    seed = sorted(Path("config/acquisition/bootstrap_seed/manual_units").glob("AHRQ_SYSTEM__*/*.xls*"))
    check(len(seed) == 6, f"the seed holds 6 AHRQ workbooks: {len(seed)}")
    for path in seed:
        data = path.read_bytes()
        if data.startswith(repo_checks.LFS_POINTER):
            pytest.skip("the checkout holds a Git LFS pointer, not the file")
        check(repo_checks.document_findings(str(path), data, []) == [], f"{path.name} holds no user path and no duplicate member")


def captured(findings: list[repo_checks.Finding]) -> str:
    """Everything the privacy scan would print for the findings: location, problem, rule and fix."""
    return "\n".join(f"{item.location} {item.problem} {item.rule} {item.fix}" for item in findings)


def test_a_member_name_that_holds_a_user_path_is_never_printed() -> None:
    name = MAC.replace("book.xlsx", "named.xml")
    data = named_archive([("[Content_Types].xml", b"<Types/>"), (name, WINDOWS.encode())])
    findings = repo_checks.document_findings("config/example.xlsx", data, [])
    check(len(findings) == 1 and findings[0].location == "config/example.xlsx!member 2 (xml)", f"{findings}")
    printed = captured(findings)
    check("synthetic_person" not in printed and "Users" not in printed and "named.xml" not in printed, "no part of the member name or body is printed")


def test_an_unreadable_member_with_a_path_in_its_name_is_refused_without_printing_the_name() -> None:
    name = MAC.replace("book.xlsx", "named.xml")
    data = bytearray(named_archive([("[Content_Types].xml", b"<Types/>"), (name, b"<a>" + b"x" * 40 + b"</a>")], zipfile.ZIP_STORED))
    at = data.index(b"x" * 40)
    data[at] ^= 0xFF
    findings = repo_checks.document_findings("config/example.xlsx", bytes(data), [])
    check(len(findings) == 1 and findings[0].location == "config/example.xlsx", f"{findings}")
    check(findings[0].problem == "unreadable document: member 2 cannot be read", f"{findings[0].problem}")
    printed = captured(findings)
    check("synthetic_person" not in printed and "Users" not in printed and "named.xml" not in printed, "no part of the member name is printed")
    check(
        captured(repo_checks.document_findings("config/example.xlsx", b"PK\x03\x04 not a zip", [])).count("synthetic") == 0,
        "a broken archive prints a fixed text",
    )


def test_a_member_too_large_to_scan_is_named_by_its_index_only(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(document_paths, "MAX_MEMBER_BYTES", 10)
    name = MAC.replace("book.xlsx", "named.xml")
    findings = repo_checks.document_findings("config/example.xlsx", named_archive([(name, b"x" * 50)]), [])
    check(findings[0].problem == "unreadable document: member 1 is too large to scan", f"{findings[0].problem}")
    check("synthetic_person" not in captured(findings), "the name is not printed")


BAD_ARCHIVE = "unreadable document: an Office package cannot be read as a ZIP archive"
NAMED_MEMBER = MAC.replace("book.xlsx", "named.xml")


def with_named_member(method: int) -> tuple[bytearray, int, int, int]:
    """An archive of one member named with a user path, and the offsets of its local header, its central header and its stored data."""
    data = bytearray(named_archive([(NAMED_MEMBER, b"<info>" + WINDOWS.encode() + b"</info>")], method))
    local, central = data.index(b"PK\x03\x04"), data.index(b"PK\x01\x02")
    return data, local, central, local + 30 + sum(struct.unpack_from("<HH", data, local + 26))


def lzma_damaged() -> bytes:
    data, _, _, start = with_named_member(zipfile.ZIP_LZMA)
    data[start + 4] = 0xFF
    return bytes(data)


def extract_version_too_new() -> bytes:
    data, local, central, _ = with_named_member(zipfile.ZIP_DEFLATED)
    struct.pack_into("<H", data, local + 4, 70)
    struct.pack_into("<H", data, central + 6, 70)
    return bytes(data)


def bad_utf8_name(header: str) -> bytes:
    """A member whose name header claims UTF-8 and holds an invalid byte, in the local header or in the central directory."""
    data, local, central, _ = with_named_member(zipfile.ZIP_STORED)
    struct.pack_into("<H", data, local + 6, 0x800)
    struct.pack_into("<H", data, central + 8, 0x800)
    data[(local + 30) if header == "local" else (central + 46)] = 0xFF
    return bytes(data)


@pytest.mark.parametrize(
    ("label", "build", "problem"),
    [
        ("damaged_lzma_member", lzma_damaged, "unreadable document: member 1 cannot be read"),
        ("extract_version_too_new", extract_version_too_new, BAD_ARCHIVE),
        ("bad_utf8_name_in_the_local_header", lambda: bad_utf8_name("local"), "unreadable document: member 1 cannot be read"),
        ("bad_utf8_name_in_the_central_directory", lambda: bad_utf8_name("central"), BAD_ARCHIVE),
    ],
)
def test_every_other_unreadable_archive_ends_in_a_fixed_finding_without_text_from_the_archive(label: str, build: Callable[[], bytes], problem: str) -> None:
    [finding] = repo_checks.document_findings("config/example.xlsx", build(), [])
    check(finding.location == "config/example.xlsx" and finding.problem == problem, f"{label}: {finding}")
    printed = captured([finding]).lower()
    for part in ("synthetic_person", "users", "named.xml", "\\x", "0x", "utf-8", "codec", "position", "lzma"):
        check(part not in printed, f"{label}: the output holds {part!r}")


def test_a_failure_that_the_reader_does_not_name_is_still_a_fixed_finding(monkeypatch: pytest.MonkeyPatch) -> None:
    def explode(info: object) -> bytes:
        raise MemoryError("synthetic_person")

    monkeypatch.setattr(zipfile.ZipFile, "read", lambda self, info: explode(info))
    [finding] = repo_checks.document_findings("config/example.xlsx", named_archive([("xl/workbook.xml", b"<workbook/>")]), [])
    check(finding.problem == "unreadable document: member 1 cannot be read" and "synthetic_person" not in captured([finding]), f"{finding}")


@pytest.mark.parametrize(
    "failure", [ValueError("synthetic_person named.xml"), KeyError("synthetic_person"), UnicodeDecodeError("utf-8", b"\xff", 0, 1, "synthetic_person")]
)
def test_the_scan_reports_only_a_fixed_text_for_a_failure_the_document_check_did_not_normalize(monkeypatch: pytest.MonkeyPatch, failure: Exception) -> None:
    def fail(*_: object) -> list[str]:
        raise failure

    monkeypatch.setattr(document_paths, "members_with_user_path", fail)
    [finding] = repo_checks.document_findings("config/example.xlsx", b"PK", [])
    check(finding.problem == "unreadable document: the file cannot be read" and finding.location == "config/example.xlsx", f"{finding}")
    check("synthetic_person" not in captured([finding]) and "named.xml" not in captured([finding]), "nothing from the failure is printed")
