"""A user directory path inside an Office or PDF file is found by member and kind, and its value is never reported."""

from __future__ import annotations

import io
import zipfile
import zlib

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
    check(result == ["xl/workbook.xml"], f"found {result}")
    check(all("synthetic_person" not in item for item in result), "the value is never part of the result")


@pytest.mark.parametrize("suffix", [".xlsx", ".xlsm", ".docx", ".pptx"])
def test_every_office_package_kind_is_read_by_its_members(suffix: str) -> None:
    check(found(workbook_with(WINDOWS), suffix) == ["xl/workbook.xml"], f"{suffix} package")


def test_a_clean_office_file_and_a_placeholder_path_pass() -> None:
    clean = office({"[Content_Types].xml": b"<Types/>", "xl/workbook.xml": b"<workbook/>"})
    check(found(clean, ".xlsx") == [], "a clean workbook has no finding")
    placeholder = workbook_with("C:\\" + "Users\\<user>\\Documents\\book.xlsx")
    check(found(placeholder, ".xlsx") == [], "a placeholder names no user")
    check(found(workbook_with("/" + "Users/Shared/book.xlsx"), ".xlsx") == [], "the macOS shared folder is no user's home")


def test_a_path_in_a_utf_16_member_or_a_legacy_workbook_is_found() -> None:
    check(found(office({"docProps/custom.xml": WINDOWS.encode("utf-16")}), ".xlsx") == ["docProps/custom.xml"], "a UTF-16 member")
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
    check(len(findings) == 1 and findings[0].location == "config/example.xlsx!xl/workbook.xml", f"{findings}")
    check("synthetic_person" not in repr(findings), "the value is not in the finding")
    allowed = [("home-path", "config/*.xlsx")]
    check(repo_checks.document_findings("config/example.xlsx", data, allowed) == [], "a reasoned allowlist entry still applies")
