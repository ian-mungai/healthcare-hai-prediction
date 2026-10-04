"""Synthetic E2E for redact_text_copies.py: the real build command on generated files, and upload against an in-memory S3.

Run from the repository root; writes a new run folder and its artifact under data/e2e/privacy_redaction/:

    PYTHONPATH=. .venv/bin/python scripts/acquisition/one_off/privacy_review/run_redaction_e2e.py

Only generic synthetic names are used. No AWS or network call is made: the upload path runs in-process against a fake
client, because synthetic objects must never be written into the real data prefixes.
"""

from __future__ import annotations

import base64
import hashlib
import importlib.util
import json
import shutil
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.acquisition.s3_store import StorageError, encoded_json, fingerprint, write_once
from scripts.process import run_command

HERE = Path(__file__).resolve().parent
TOOL = HERE / "redact_text_copies.py"
POLICY = Path("data/privacy_review/20260927/redaction_policy.json")
SYNTHETIC = ["Alex Example", "Casey Sample", "Jordan Placeholder", "Riley Fixture", "Morgan Testcase", "Quinn Mockname", "Pat Unlisted", "Taylor Specimen"]
EMAILS = ["alex.example@example.org", "owner.contact@example.net", "person@example.com"]
R = "[REDACTED]"

WIDE = (
    "\ufeff_id,FAC_NO,FAC_NAME,PHONE,CEO,CEO_TITLE,WEB_SITE,OWNER,RPT_PREP,ORG_NAME,BED_LIC,NOTE\r\n"
    "1,106010001,Example General Hospital,555-0100,Alex Example,CEO,www.example.org,"
    "Example Health Inc,Jordan Placeholder,Example Accounting LLP,120,plain note\r\n"
    '"2","106010002","Sample Valley Hospital","555-0101","Casey Sample","PRESIDENT/CEO","alex.example@example.org","Casey Sample",'
    '"Riley Fixture","Riley Fixture","85","multi\r\nline note mentioning ALEX  EXAMPLE"\r\n'
    '3,106010003,"Third, Hospital",555-0102,N/A,,,Third Owner owner.contact@example.net,,,40,\r\n'
    "4,106010004,Fourth Hospital,555-0103,,,,,,,1e3,numbers 12345\r\n"
)
TAB = (
    "FAC_NO\tFAC_NAME\tCEO\tCEO_TITLE\tRPT_PREP\tOWNER\tBED_LIC\n"
    "106010001\tExample General Hospital\tAlex Example\tCEO\tJordan Placeholder\tExample Health Inc\t120\n"
    "106010005\tFifth Hospital\tTaylor Specimen\tADMINISTRATOR\tJordan Placeholder\tTaylor Specimen Holdings\t30\n"
    "106010006\tSixth Hospital\tCasey Sample\tCEO\tExample Health Inc\tSixth Owner\t10\n"
)
LONG = (
    "OSHPD_ID,RPE,PageColumnLine,Values\n"
    "106010001,2015,0.1.1,Example General Hospital\n"
    "106010001,2015,0.1.14,Morgan Testcase\n"
    "106010001,2015,0.1.23,Jordan Placeholder\n"
    "106010001,2015,0.1.25,555-010-0199x12\n"
    "106010001,2015,3.3.1.40,Quinn Mockname\n"
    "106010001,2015,3.3.1.62,X\n"
    "106010001,2015,3.3.2.40,Chief Financial Officer\n"
    "106010001,2015,7.91.110,Consulting by Quinn Mockname\n"
    "106010001,2015,5.1.1,12345.67\n"
    "106010001,2015,0.1.16,contact: person@example.com\n"
)
DICTIONARY = '\ufeff_id,Data Item,Definition\r\n1,CHIEF EXECUTIVE OFFICER,"The Chief Executive Officer (CEO) of the hospital."\r\n'
PACKAGE = json.dumps(
    {"contact_email": "data.team@example.gov", "resources": [{"schema": {"fields": [{"name": "CEO", "description": "Chief executive officer"}]}}]}
)
NS = 'xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"'
DEFINITION = (
    f'<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\r\n<pivotCacheDefinition {NS}><cacheFields count="4">'
    '<cacheField name="FAC_NO"><sharedItems/></cacheField>'
    '<cacheField name="FAC_NAME"><sharedItems count="2"><s v="Example General Hospital"/><s v="Sample Valley Hospital"/></sharedItems></cacheField>'
    '<cacheField name="CEO"><sharedItems containsBlank="1"/></cacheField>'
    '<cacheField name="BED_LIC"><sharedItems/></cacheField></cacheFields></pivotCacheDefinition>'
)
RECORDS = (
    f'<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\r\n<pivotCacheRecords {NS} count="3">'
    '<r><n v="106010001"/><x v="0"/><s v="Alex Example"/><n v="120"/></r>'
    '<r><n v="106010002"/><x v="1"/><s v="Pat Unlisted"/><n v="85"/></r>'
    '<r><n v="106010003"/><x v="0"/><m/><n v="40"/></r></pivotCacheRecords>'
)
STRINGS = (
    f'<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\r\n<sst {NS} count="4" uniqueCount="4">'
    "<si><t>Example General Hospital</t></si><si><t>Pat Unlisted</t></si>"
    '<si><t xml:space="preserve">Owner &amp; Alex Example</t></si><si><t>BED_LIC</t></si></sst>'
)


class FakeS3:
    """In-memory versioned bucket that answers the calls store_file makes."""

    def __init__(self) -> None:
        self.configuration = {"data_bucket_name": "synthetic-bucket"}
        self.objects: dict[str, tuple[str, bytes]] = {}
        self.puts = 0

    def call(self, service: str, operation: str, arguments: list[str] | None = None) -> dict[str, Any]:
        options = arguments or []
        if operation == "get-bucket-versioning":
            return {"Status": "Enabled"}
        key = options[options.index("--key") + 1]
        if operation == "head-object":
            if key not in self.objects:
                raise StorageError(operation, "404")
            return {"VersionId": self.objects[key][0]}
        if operation == "put-object":
            if key in self.objects:
                raise StorageError(operation, "PreconditionFailed")
            self.puts += 1
            self.objects[key] = (f"v{self.puts:04d}", Path(options[options.index("--body") + 1]).read_bytes())
            return {"VersionId": self.objects[key][0]}
        if operation == "get-object":
            version, body = self.objects[key]
            Path(options[-1]).write_bytes(body)
            digest = base64.b64encode(hashlib.sha256(body).digest()).decode()
            return {"VersionId": version, "ContentLength": len(body), "ChecksumSHA256": digest, "ServerSideEncryption": "AES256"}
        raise StorageError(operation, "unsupported_in_fake")


def target(path: Path, key_prefix: str, name: str) -> dict[str, Any]:
    digest, size = fingerprint(path)
    return {
        "key": f"{key_prefix}/{digest}/{name}",
        "version_id": "orig" + digest[:8],
        "sha256": digest,
        "bytes": size,
        "local_path": str(path),
        "format": path.suffix,
    }


def fixtures(root: Path) -> list[dict[str, Any]]:
    files = root / "fixtures"
    (files / "pivot").mkdir(parents=True)
    contents = {
        "wide.csv": WIDE,
        "tab.txt": TAB,
        "long.csv": LONG,
        "dictionary.csv": DICTIONARY,
        "datapackage.json": PACKAGE,
        "sharedStrings.xml": STRINGS,
        "pivot/pivotCacheDefinition1.xml": DEFINITION,
        "pivot/pivotCacheRecords1.xml": RECORDS,
    }
    for name, text in contents.items():
        (files / name).write_bytes(text.encode("utf-8"))
    ca = "example_pub/example_collection/datasets/ca/capture_id=SYN__1"
    targets = [target(files / name, ca, Path(name).name) for name in contents if name != "long.csv"]
    targets.append(target(files / "wide.csv", "example_pub/example_collection/datasets/ca/capture_id=SYN__2", "wide.csv"))
    targets.append(target(files / "tab.txt", "example_pub/example_collection/datasets/ca/capture_id=SYN__2", "tab_renamed.txt"))
    targets.append(target(files / "long.csv", "example_pub/example_collection/datasets/finance/capture_id=SYN__3", "long.csv"))
    return targets


def build(root: Path, name: str, targets: list[dict[str, Any]], output: Path | None = None) -> tuple[int, dict[str, Any], Path]:
    case = root / name
    case.mkdir()
    targets_path = case / "targets.json"
    write_once(targets_path, encoded_json({"objects": targets}))
    output = output or case / "output"
    result = run_command(sys.executable, [str(TOOL), "build", "--targets", str(targets_path), "--policy", str(POLICY), "--output", str(output)], timeout=120)
    text = result.stdout + result.stderr
    if any(value.casefold() in text.casefold() for value in SYNTHETIC + EMAILS):
        raise AssertionError(f"{name}: a synthetic personal value reached the command output")
    lines = result.stdout.splitlines()
    return result.returncode, json.loads(lines[-1]) if lines else {"status": "no_output", "stderr_lines": len(result.stderr.splitlines())}, output


def main() -> int:
    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    root = Path("data/e2e/privacy_redaction") / run_id
    root.mkdir(parents=True)
    cases: list[dict[str, Any]] = []
    status = "failed"

    def check(name: str, condition: object, detail: str = "") -> None:
        cases.append({"case": name, "passed": bool(condition), "detail": detail})
        if not condition:
            raise AssertionError(name)

    try:
        targets = fixtures(root)
        by_name = {Path(item["key"]).name + item["key"].split("/")[4]: item for item in targets}
        code, summary, output = build(root, "build_first", targets)
        check("build_exit_zero", code == 0 and summary["status"] == "passed", json.dumps(summary))
        report = json.loads((output / "build_report.json").read_text())
        statuses = {Path(item["key"]).name + item["key"].split("/")[4]: item["status"] for item in report["objects"]}
        expected = {"dictionary.csv": "no_personal_data", "datapackage.json": "no_personal_data", "pivotCacheDefinition1.xml": "no_personal_data"}
        check(
            "classification",
            all(statuses[name + "capture_id=SYN__1"] == value for name, value in expected.items())
            and all(statuses[name] == "replace" for name in ("wide.csvcapture_id=SYN__1", "wide.csvcapture_id=SYN__2", "tab.txtcapture_id=SYN__1"))
            and statuses["long.csvcapture_id=SYN__3"] == "replace"
            and statuses["sharedStrings.xmlcapture_id=SYN__1"] == "replace"
            and statuses["pivotCacheRecords1.xmlcapture_id=SYN__1"] == "replace",
            json.dumps(report["summary"]),
        )

        def copy(name: str, capture: str = "capture_id=SYN__1") -> bytes:
            return (output / "copies" / by_name[name + capture]["sha256"] / name).read_bytes()

        wide = copy("wide.csv").decode("utf-8")
        lines = WIDE.split("\r\n")
        check("wide_header_bom_and_unchanged_row_bytes", wide.startswith(lines[0] + "\r\n") and wide.endswith(lines[-2] + "\r\n"))
        check(
            "wide_named_fields_and_quoting",
            "1,106010001,Example General Hospital,555-0100,[REDACTED],CEO,www.example.org,"
            "Example Health Inc,[REDACTED],Example Accounting LLP,120,plain note\r\n" in wide,
        )
        check(
            "wide_all_quoted_record_keeps_quotes",
            '"2","106010002","Sample Valley Hospital","555-0101","[REDACTED]","PRESIDENT/CEO","[REDACTED]","[REDACTED]","[REDACTED]","[REDACTED]","85",'
            '"multi\r\nline note mentioning [REDACTED]"\r\n' in wide,
        )
        check("wide_placeholder_named_value_and_owner_email", '3,106010003,"Third, Hospital",555-0102,[REDACTED],,,Third Owner [REDACTED],,,40,\r\n' in wide)
        check("same_content_same_copy", by_name["wide.csvcapture_id=SYN__1"]["sha256"] == by_name["wide.csvcapture_id=SYN__2"]["sha256"])
        tab = copy("tab.txt").decode("utf-8")
        check("tab_delimited_and_cross_field_name", "106010005\tFifth Hospital\t[REDACTED]\tADMINISTRATOR\t[REDACTED]\t[REDACTED] Holdings\t30\n" in tab)
        check(
            "organization_in_named_field_redacted_there_but_kept_elsewhere",
            "106010001\tExample General Hospital\t[REDACTED]\tCEO\t[REDACTED]\tExample Health Inc\t120\n" in tab
            and "106010006\tSixth Hospital\t[REDACTED]\tCEO\t[REDACTED]\tSixth Owner\t10\n" in tab,
        )
        long = copy("long.csv", "capture_id=SYN__3").decode("utf-8")
        for line, value in [
            ("0.1.14", "[REDACTED]"),
            ("0.1.23", "[REDACTED]"),
            ("0.1.25", "[REDACTED]"),
            ("3.3.1.40", "[REDACTED]"),
            ("3.3.1.62", "X"),
            ("3.3.2.40", "Chief Financial Officer"),
            ("7.91.110", "Consulting by [REDACTED]"),
            ("5.1.1", "12345.67"),
            ("0.1.16", "contact: [REDACTED]"),
            ("0.1.1", "Example General Hospital"),
        ]:
            check(f"long_line_{line}", f"106010001,2015,{line},{value}\n" in long)
        records = copy("pivotCacheRecords1.xml").decode("utf-8")
        check(
            "pivot_name_field_redacted_even_when_unlisted_elsewhere",
            records.count('<s v="[REDACTED]"/>') == 2 and "<m/>" in records and '<x v="1"/>' in records,
        )
        strings = copy("sharedStrings.xml").decode("utf-8")
        check("shared_strings_escaped_and_scrubbed", "<t>[REDACTED]</t>" in strings and "Owner &amp; [REDACTED]" in strings and "<t>BED_LIC</t>" in strings)
        every = [path for path in output.rglob("*") if path.is_file()]
        leaked = [path.name for path in every if any(value.casefold() in path.read_text(encoding="utf-8").casefold() for value in SYNTHETIC + EMAILS)]
        check("no_synthetic_value_in_any_output_or_report", not leaked, ",".join(leaked))
        check(
            "dictionary_counts_only",
            report["dictionary"]["entries"] == 9 and report["dictionary"]["cross_field_entries"] == 8 and "Alex" not in json.dumps(report),
        )
        check("originals_unchanged", all(fingerprint(Path(item["local_path"])) == (item["sha256"], item["bytes"]) for item in targets))

        code, summary, again = build(root, "build_second", targets)
        second = json.loads((again / "build_report.json").read_text())
        check("rebuild_is_deterministic", code == 0 and second == report)
        check(
            "rebuild_copies_identical",
            sorted((p.relative_to(again).as_posix(), fingerprint(p)[0]) for p in again.rglob("*") if p.is_file())
            == sorted((p.relative_to(output).as_posix(), fingerprint(p)[0]) for p in output.rglob("*") if p.is_file()),
        )
        before = sorted((p.relative_to(output).as_posix(), fingerprint(p)[0]) for p in output.rglob("*") if p.is_file())
        code, summary, _ = build(root, "replay_into_existing_output", targets, output)
        after = sorted((p.relative_to(output).as_posix(), fingerprint(p)[0]) for p in output.rglob("*") if p.is_file())
        check("replay_into_existing_output_changes_nothing", code == 0 and before == after, json.dumps(summary))

        verify_args = [str(TOOL), "verify", "--targets", str(root / "build_first" / "targets.json"), "--policy", str(POLICY)]
        verified = run_command(sys.executable, [*verify_args, "--output", str(output)], timeout=120)
        check("verify_command_passes_unchanged_outputs", verified.returncode == 0, verified.stdout)
        for label, filename, old, new in [
            ("unrelated_text", "wide.csv", "Fourth Hospital", "[REDACTED]"),
            ("scientific_number", "wide.csv", "1e3", "[REDACTED]"),
            ("xml_number", "pivotCacheRecords1.xml", '<n v="120"/>', '<n v="999"/>'),
        ]:
            corrupted = root / f"corrupted_{label}"
            shutil.copytree(output, corrupted)
            digest = by_name[filename + "capture_id=SYN__1"]["sha256"]
            changed_copy = corrupted / "copies" / digest / filename
            changed_copy.write_bytes(changed_copy.read_bytes().replace(old.encode(), new.encode()))
            # The semantic check must reject corruption even when its claimed output hash is altered too.
            altered = json.loads((corrupted / "build_report.json").read_text())
            for entry in altered["objects"]:
                if entry["sha256"] == digest:
                    entry["redacted_sha256"], entry["redacted_bytes"] = fingerprint(changed_copy)
            (corrupted / "build_report.json").write_bytes(encoded_json(altered))
            result = run_command(sys.executable, [*verify_args, "--output", str(corrupted)], timeout=120)
            check(f"verify_rejects_{label}", result.returncode != 0 and "unexpected_change" in result.stdout, result.stdout)

        bad = root / "bad_fixtures"
        bad.mkdir()
        (bad / "latin.csv").write_bytes(b"FAC_NO,CEO\n1,Caf\xe9 Owner\n")
        (bad / "quote.csv").write_bytes(b'FAC_NO,CEO\n1,"unterminated\n')
        (bad / "named.json").write_bytes(json.dumps({"note": "Alex Example"}).encode())
        (bad / "broken.xml").write_bytes(b'<?xml version="1.0"?><sst><si><t>A &bogus; B</t></si></sst>')
        (bad / "doctype.xml").write_bytes(b'<?xml version="1.0"?><!DOCTYPE x [<!ENTITY e "x">]><sst><si><t>&e;</t></si></sst>')
        (bad / "book.xls").write_bytes(b"\xd0\xcf\x11\xe0")
        (bad / "lonely").mkdir()
        (bad / "lonely" / "pivotCacheRecords1.xml").write_bytes(RECORDS.encode())
        prefix = "example_pub/example_collection/datasets/ca/capture_id=BAD"
        changed = dict(targets[0], sha256="0" * 64, key=targets[0]["key"].replace(targets[0]["sha256"], "0" * 64))
        failures = [
            ("original_changed", "original_changed", [changed]),
            ("not_utf8", "not_utf8", [target(bad / "latin.csv", prefix, "latin.csv")]),
            ("unbalanced_quotes", "unbalanced_quotes", [target(bad / "quote.csv", prefix, "quote.csv")]),
            ("metadata_with_name", "metadata_contains_names", [target(bad / "named.json", prefix, "named.json"), by_name["wide.csvcapture_id=SYN__1"]]),
            ("unknown_entity", "unsupported_xml_entity", [target(bad / "broken.xml", prefix, "broken.xml")]),
            ("doctype_rejected", "xml_doctype_not_allowed", [target(bad / "doctype.xml", prefix, "doctype.xml")]),
            ("unsupported_format", "unsupported_format", [target(bad / "book.xls", prefix, "book.xls")]),
            ("pivot_without_definition", "pivot_definition_missing", [target(bad / "lonely" / "pivotCacheRecords1.xml", prefix, "pivotCacheRecords1.xml")]),
        ]
        for name, reason, selection in failures:
            code, summary, failed = build(root, name, selection)
            written = [p for p in (failed / "copies").rglob("*") if p.is_file()] if (failed / "copies").exists() else []
            check(f"stops_{name}", code != 0 and summary == {"status": "stopped", "reason": reason} and not written, json.dumps(summary))

        conflict = root / "conflict"
        conflict_targets = [by_name["tab.txtcapture_id=SYN__1"]]
        conflict.mkdir()
        write_once(conflict / "targets.json", encoded_json({"objects": conflict_targets}))
        planted = conflict / "output" / "copies" / conflict_targets[0]["sha256"] / "tab.txt"
        planted.parent.mkdir(parents=True)
        planted.write_bytes(b"different bytes\n")
        result = run_command(
            sys.executable,
            [str(TOOL), "build", "--targets", str(conflict / "targets.json"), "--policy", str(POLICY), "--output", str(conflict / "output")],
            timeout=120,
        )
        check(
            "stops_conflicting_existing_copy",
            result.returncode != 0 and '"conflicting_output"' in result.stdout and planted.read_bytes() == b"different bytes\n",
        )

        spec = importlib.util.spec_from_file_location("redact_text_copies", TOOL)
        if spec is None or spec.loader is None:
            raise AssertionError("tool import")
        tool = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(tool)
        s3 = FakeS3()
        record = root / "replacements.json"
        first = tool.upload(s3, output, record, POLICY)
        replaced = [item for item in report["objects"] if item["status"] == "replace"]
        check(
            "upload_creates_copy_and_manifest_per_object",
            first["created"] == {"copies": len(replaced), "manifests": len(replaced)} and first["replacements"] == len(replaced),
        )
        data = json.loads(record.read_text())
        keys = [item["redacted"]["key"] for item in data["replacements"]]
        check(
            "replacement_keys_use_redacted_dataset_and_own_hash", all("/datasets/ca_redacted/" in key or "/datasets/finance_redacted/" in key for key in keys)
        )
        check("same_bytes_under_another_name_uploaded", any(key.endswith("/tab_renamed.txt") for key in keys))
        check("replacement_key_never_equals_original", all(item["redacted"]["key"] != item["original"]["key"] for item in data["replacements"]))
        check("manifest_keys_under_manifests", all("/manifests/capture_id=" in item["manifest"]["key"] for item in data["replacements"]))
        stored = [body.decode("utf-8") for _version, body in s3.objects.values()]
        check("no_synthetic_value_in_s3", not any(value.casefold() in text.casefold() for text in stored for value in SYNTHETIC + EMAILS))
        second_upload = tool.upload(s3, output, record, POLICY)
        check("upload_rerun_creates_nothing", second_upload["created"] == {"copies": 0, "manifests": 0} and s3.puts == 2 * len(replaced))
        status = "passed"
    except AssertionError as error:
        cases.append({"case": "stopped_at", "passed": False, "detail": str(error)})
    finally:
        artifact = {
            "kind": "synthetic_redaction_e2e",
            "status": status,
            "completed_at_utc": datetime.now(UTC).isoformat(),
            "python_version": sys.version,
            "tool_sha256": fingerprint(TOOL)[0],
            "policy_sha256": fingerprint(POLICY)[0],
            "runner_sha256": fingerprint(Path(__file__).resolve())[0],
            "requirements_sha256": fingerprint(Path("requirements.txt"))[0],
            "cases": cases,
            "reproduce": "PYTHONPATH=. .venv/bin/python scripts/acquisition/one_off/privacy_review/run_redaction_e2e.py",
            "tested_boundary": "Build command as a subprocess on synthetic CSV, TXT, long-format, JSON and XML files; upload in-process against a fake S3.",
            "substitutions": ["FakeS3 replaces AWS for upload; the real upload is verified by version readback in store_file"],
            "untested_boundaries": ["Real AWS permissions and network", "Names that appear only in free text"],
            "cleanup": "Synthetic files kept in the run folder; nothing created in AWS.",
        }
        write_once(root / "artifact.json", encoded_json(artifact))
    sys.stdout.write(json.dumps({"status": status, "cases": len(cases), "artifact": str(root / "artifact.json")}) + "\n")
    return 0 if status == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
