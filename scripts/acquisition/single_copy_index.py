"""Build the starting index for single-copy storage from a read-only listing of the bucket.

The listing (``ListObjectVersions``) is the true list of stored files. Each payload key holds its SHA-256 and the listing gives the exact
version, the size and the storage time. The index names the current payload versions in the data scope (datasets and references) and the
evidence scope (audit), so no byte is copied when single-copy storage is switched on. When identical bytes sit under more than one key,
the index names the copy stored first and the report lists the others as aliases. The local download records are only a cross-check: the
command reports a stored object that no record names and a record whose object the listing does not hold, and it writes nothing while
any difference or other problem remains. A record names an object by its exact bucket, key and version: a capture record, an upload
record or, for an object neither names, one line of the object reasons file (key, version, byte count, SHA-256 and a written reason).
A saved bucket listing is never a name. Run from the repository root:

    .venv/bin/python -m scripts.acquisition.single_copy_index --output data/single_copy/index_<name>.json
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import shlex
import sys
import time
from collections import Counter, defaultdict
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.acquisition.s3_store import AwsCli, StorageError, encoded_json, write_once
from scripts.acquisition.single_copy import reference_scope
from scripts.acquisition.transport import CaptureError
from scripts.acquisition.verify_storage_records import collect, exact_size, load_empty_captures
from scripts.infrastructure.render_project_config import REPO_ROOT, ConfigurationError, load_configuration
from scripts.process import TimeoutExpired, run_command

DEFAULT_RECORDS = REPO_ROOT / "data/datasets"
DEFAULT_RETIREMENTS = (
    "data/privacy_review/*/retired_objects*.json",
    "data/lakehouse_planning/dedup_*/retired_objects*.json",
    "data/license_removal/*/*retired_objects*.json",
)
POLICY_PATH = "config/acquisition/storage_policy.json"
RECEIPT = "receipt.json"
RECONCILIATION = "s3_collections_reconciliation.json"
CONTRACT_VERSION = "4.0.0"
STORED_STATUS = "acquired_unvalidated"
VERIFIED_STATUSES = {"deleted_verified", "already_absent"}
HEX64 = re.compile(r"[a-f0-9]{64}")
EXAMPLES_PER_REASON = 50
PAGE_SIZE = 1000
DEFAULT_DEADLINE_SECONDS = 1800.0
Identity = tuple[str, str, str]
Reasons = dict[tuple[str, str], tuple[int, str, str]]


@dataclass
class Findings:
    """Every reason the command cannot write an index, counted by reason with up to 50 examples each."""

    counts: Counter[str] = field(default_factory=Counter)
    examples: defaultdict[str, list[str]] = field(default_factory=lambda: defaultdict(list))

    def add(self, reason: str, where: str, detail: str) -> None:
        self.counts[reason] += 1
        if len(self.examples[reason]) < EXAMPLES_PER_REASON:
            self.examples[reason].append(f"{where}: {detail}")

    def report(self) -> dict[str, Any]:
        return {reason: {"count": self.counts[reason], "examples": self.examples[reason]} for reason in sorted(self.counts)}


@dataclass(frozen=True)
class Version:
    """One entry of the listing."""

    key: str
    version_id: str
    size: int
    stored_at: datetime


@dataclass(frozen=True)
class Stored:
    """One current payload version, the scope its key puts it in and the checksum its key holds."""

    scope: str
    bucket: str
    version: Version
    sha256: str

    @property
    def identity(self) -> Identity:
        return (self.bucket, self.version.key, self.version.version_id)

    def index_record(self) -> dict[str, Any]:
        return {
            "bucket": self.bucket,
            "byte_count": self.version.size,
            "key": self.version.key,
            "sha256": self.sha256,
            "version_id": self.version.version_id,
        }


@dataclass
class Listing:
    """The current versions of the bucket, their identities (key and version) and the identities of every listed version and delete marker."""

    current: list[Version] = field(default_factory=list)
    identities: set[tuple[str, str]] = field(default_factory=set)
    listed: set[tuple[str, str]] = field(default_factory=set)


@dataclass(frozen=True)
class Retirement:
    """The bucket, byte count and SHA-256 a retirement line supplies; None for a field the line leaves out."""

    bucket: str | None
    size: int | None
    sha256: str | None


@dataclass
class Records:
    """What the local download records name: every stored object, and the snapshots a disposition explains."""

    named: set[Identity] = field(default_factory=set)
    uploads: dict[tuple[str, str], set[int | None]] = field(default_factory=dict)
    explained_snapshots: set[str] = field(default_factory=set)
    retired: dict[tuple[str, str], Retirement] = field(default_factory=dict)


@dataclass
class Tally:
    """Counts reported with the result."""

    captures: Counter[str] = field(default_factory=Counter)
    not_stored: Counter[str] = field(default_factory=Counter)
    listing: Counter[str] = field(default_factory=Counter)
    payload: Counter[str] = field(default_factory=Counter)
    cross_check: Counter[str] = field(default_factory=Counter)
    aliases: list[dict[str, Any]] = field(default_factory=list)
    listing_sha256: str = ""


def read_object(path: Path) -> dict[str, Any] | None:
    try:
        value = json.loads(path.read_bytes())
    except (OSError, ValueError):
        return None
    return value if isinstance(value, dict) else None


def stored_time(value: object) -> datetime | None:
    """Return a timezone-aware UTC time from listing text, or None when the text is not such a time."""
    try:
        moment = datetime.fromisoformat(value) if isinstance(value, str) else None
    except ValueError:
        return None
    return moment.astimezone(UTC) if moment is not None and moment.tzinfo is not None else None


def find_captures(root: Path, findings: Findings) -> list[Path]:
    """Return every folder that holds a receipt or a reconciliation record, without entering one; add a finding for each folder not read."""
    found: list[Path] = []

    def not_listed(error: OSError) -> None:
        findings.add("records_scan_error", Path(str(error.filename)).relative_to(root).as_posix() or ".", f"the folder cannot be listed ({error.strerror})")

    for folder, directories, files in root.walk(on_error=not_listed):
        if RECEIPT in files or RECONCILIATION in files:
            found.append(folder)
            directories.clear()
        else:
            directories.sort()
            for name in files:
                if (folder / name).is_symlink() and (folder / name).is_dir():
                    findings.add(
                        "records_scan_error", (folder / name).relative_to(root).as_posix(), "a folder link is not followed, so the folder was not read"
                    )
    return sorted(found)


def load_dispositions(path: Path | None, findings: Findings) -> dict[tuple[str, str], str]:
    """Return (capture path, receipt checksum) to reason for acquired captures a person confirmed have no storage record."""
    if path is None:
        return {}
    record = read_object(path)
    captures = record.get("captures") if record else None
    if record is None or record.get("kind") != "unstored_capture_dispositions" or not isinstance(captures, list):
        findings.add("disposition_invalid", path.name, "the file is unreadable or is not an unstored-capture disposition record")
        return {}
    accepted: dict[tuple[str, str], str] = {}
    for item in captures:
        where, checksum, reason = (item.get(name) for name in ("path", "receipt_sha256", "reason")) if isinstance(item, dict) else (None, None, None)
        if not (isinstance(where, str) and where and isinstance(checksum, str) and HEX64.fullmatch(checksum) and isinstance(reason, str) and reason.strip()):
            findings.add("disposition_invalid", path.name, "each line needs a capture path, its receipt checksum and a reason")
        elif (where, checksum) in accepted:
            findings.add("disposition_invalid", path.name, f"the capture {where} has more than one line")
        else:
            accepted[(where, checksum)] = reason
    return accepted


def load_object_dispositions(path: Path | None, findings: Findings) -> Reasons:
    """Return (key, version) to byte count, SHA-256 and reason for stored objects that no record names."""
    if path is None:
        return {}
    record = read_object(path)
    objects = record.get("objects") if record else None
    if record is None or record.get("kind") != "unnamed_object_dispositions" or not isinstance(objects, list):
        findings.add("object_disposition_invalid", path.name, "the file is unreadable or is not an unnamed-object disposition record")
        return {}
    accepted: Reasons = {}
    for item in objects:
        key, version, size, checksum, reason = (
            (item.get(name) for name in ("key", "version_id", "byte_count", "sha256", "reason")) if isinstance(item, dict) else (None,) * 5
        )
        if not (
            isinstance(key, str)
            and key
            and isinstance(version, str)
            and version
            and type(size) is int
            and size >= 0
            and isinstance(checksum, str)
            and HEX64.fullmatch(checksum)
            and isinstance(reason, str)
            and reason.strip()
        ):
            findings.add("object_disposition_invalid", path.name, "each line needs a key, a version, a byte count, a SHA-256 and a reason")
        elif (key, version) in accepted:
            findings.add("object_disposition_invalid", path.name, f"the object {key} version {version} has more than one line")
        else:
            accepted[(key, version)] = (size, checksum, reason)
    return accepted


def read_upload_records(arguments: argparse.Namespace, bucket: str, findings: Findings) -> dict[tuple[str, str], set[int | None]]:
    """Return the key and version of every upload record under the evidence root, read by the whole-bucket check's own reader."""
    root: Path = arguments.evidence_root
    if not root.is_dir():
        findings.add("evidence_root_missing", root.name, "the evidence root does not exist")
        return {}
    try:
        uploads, _listed_only, _walked = collect(root, bucket, load_empty_captures(arguments.empty_capture_dispositions, root), record_size=exact_size)
    except (CaptureError, ValueError, OSError, KeyError, TypeError) as error:
        findings.add("upload_records_unreadable", root.name, str(error))
        return {}
    return uploads


def retirement_fields(item: dict[str, Any]) -> Retirement | None:
    """Return the bucket, byte count and SHA-256 a retirement line supplies; None when a supplied field is not valid."""
    bucket, size, checksum = (item.get(name) for name in ("bucket", "bytes", "sha256"))
    if (
        ("bucket" in item and not (isinstance(bucket, str) and bucket))
        or ("bytes" in item and not (type(size) is int and size >= 0))
        or ("sha256" in item and not (isinstance(checksum, str) and HEX64.fullmatch(checksum)))
    ):
        return None
    return Retirement(bucket, size, checksum)


def merge_retirements(first: Retirement, second: Retirement) -> Retirement | None:
    """Return the fields that two lines for one identity supply together; None when they supply different values for a field."""
    if any(
        one is not None and other is not None and one != other
        for one, other in ((first.bucket, second.bucket), (first.size, second.size), (first.sha256, second.sha256))
    ):
        return None
    return Retirement(
        first.bucket if first.bucket is not None else second.bucket,
        first.size if first.size is not None else second.size,
        first.sha256 if first.sha256 is not None else second.sha256,
    )


def load_retirements(paths: Sequence[Path], findings: Findings) -> dict[tuple[str, str], Retirement]:
    """Return the key and version of every verified deletion with the fields its lines supply; a repeated identity must agree in every supplied field."""
    retired: dict[tuple[str, str], Retirement] = {}
    for path in paths:
        record = read_object(path)
        objects = record.get("objects") if record else None
        if not isinstance(objects, list):
            findings.add("retirement_unreadable", path.name, "the retirement record is missing, unreadable or has no object list")
            continue
        for item in objects:
            if not (
                isinstance(item, dict) and isinstance(item.get("key"), str) and item["key"] and isinstance(item.get("version_id"), str) and item["version_id"]
            ):
                findings.add("retirement_unreadable", path.name, "a retirement line has no key or version")
                continue
            identity = (item["key"], item["version_id"])
            fields = retirement_fields(item)
            if item.get("status") not in VERIFIED_STATUSES:
                findings.add("retirement_unverified", path.name, "a retirement line was not verified as deleted")
            elif fields is None:
                findings.add(
                    "retirement_invalid",
                    item["key"],
                    f"the retirement line for version {item['version_id']} has a bucket, byte count or SHA-256 that is not valid",
                )
            elif identity not in retired:
                retired[identity] = fields
            elif (merged := merge_retirements(retired[identity], fields)) is None:
                findings.add("retirement_conflict", item["key"], f"two retirement lines for version {item['version_id']} give different fields")
            else:
                retired[identity] = merged
    return retired


def named_objects(reconciliation: dict[str, Any], where: str, records: Records, findings: Findings) -> None:
    """Add the stored object of every line of a reconciliation record; refuse a line that does not name bucket, key and version."""
    lines = [reconciliation.get("objects"), reconciliation.get("manifests", [])]
    if not all(isinstance(group, list) for group in lines):
        findings.add("record_unreadable", where, "the reconciliation record has no list of objects or manifests")
        return
    for line in [*lines[0], *lines[1]]:
        stored = line.get("object") if isinstance(line, dict) else None
        bucket, key, version = (stored.get(name) if isinstance(stored, dict) else None for name in ("bucket", "key", "version_id"))
        if isinstance(bucket, str) and isinstance(key, str) and isinstance(version, str) and bucket and key and version:
            records.named.add((bucket, key, version))
        else:
            findings.add("record_unreadable", where, "a line of the reconciliation record has no bucket, key or version")


def read_capture(
    root: Path, folder: Path, dispositions: dict[tuple[str, str], str], used: set[tuple[str, str]], records: Records, tally: Tally, findings: Findings
) -> None:
    """Add the objects one capture folder names, or count it as not stored; add findings for what cannot be read."""
    where = folder.relative_to(root).as_posix()
    receipt = read_object(folder / RECEIPT)
    if receipt is None:
        findings.add("record_unreadable", where, "the receipt is missing or unreadable")
        return
    status, snapshot = receipt.get("snapshot_status"), receipt.get("snapshot_id")
    if (folder / RECONCILIATION).is_file():
        reconciliation = read_object(folder / RECONCILIATION)
        if (
            reconciliation is None
            or reconciliation.get("storage_contract_version") != CONTRACT_VERSION
            or not isinstance(snapshot, str)
            or reconciliation.get("snapshot_id") != snapshot
        ):
            findings.add("record_unreadable", where, "the reconciliation record is unreadable, has another storage contract or belongs to another snapshot")
        else:
            tally.captures["stored"] += 1
            named_objects(reconciliation, where, records, findings)
    elif status is None:
        tally.captures["attempt_records"] += 1
    elif not isinstance(status, str) or not isinstance(snapshot, str):
        findings.add("record_unreadable", where, "the receipt has a snapshot status or identifier that is not text")
    elif status != STORED_STATUS:
        tally.not_stored[status] += 1
    else:
        checksum = hashlib.sha256((folder / RECEIPT).read_bytes()).hexdigest()
        if (where, checksum) in dispositions:
            used.add((where, checksum))
            records.explained_snapshots.add(snapshot)
            tally.captures["dispositions_applied"] += 1
        else:
            findings.add("acquired_capture_without_record", where, f"an acquired capture (receipt {checksum}) has no reconciliation record and no disposition")


def read_records(arguments: argparse.Namespace, tally: Tally, findings: Findings) -> Records:
    """Read every local download record that the cross-check needs."""
    records = Records()
    root: Path = arguments.records
    if not root.is_dir():
        findings.add("records_folder_missing", root.name, "the records folder does not exist")
        return records
    paths = arguments.retirements if arguments.retirements is not None else sorted(path for pattern in DEFAULT_RETIREMENTS for path in REPO_ROOT.glob(pattern))
    records.retired = load_retirements(paths, findings)
    dispositions = load_dispositions(arguments.dispositions, findings)
    captures = find_captures(root, findings)
    if not captures and "records_scan_error" not in findings.counts:
        findings.add("no_captures_found", root.name, "no receipt or reconciliation record exists under the records folder")
    used: set[tuple[str, str]] = set()
    for folder in captures:
        read_capture(root, folder, dispositions, used, records, tally, findings)
    if "records_scan_error" not in findings.counts:
        for where, checksum in sorted(set(dispositions) - used):
            findings.add("disposition_unused", where, f"the line for receipt {checksum[:12]} matches no acquired capture without a record")
    return records


class DeadlineRunner:
    """Run AWS commands so that none outlives an overall deadline: the process limit and the client timeouts use the time left."""

    def __init__(self, run: Callable[..., Any], deadline_at: float) -> None:
        self.run, self.deadline_at = run, deadline_at

    def __call__(self, argv: list[str], **options: Any) -> Any:
        remaining = self.deadline_at - time.monotonic()
        if remaining <= 0:
            raise TimeoutExpired(argv, 0)
        bounded = list(argv)
        for flag in ("--cli-connect-timeout", "--cli-read-timeout"):
            position = bounded.index(flag) + 1
            bounded[position] = str(max(1, min(int(bounded[position]), int(remaining))))
        return self.run(bounded, **{**options, "timeout": min(options["timeout"], remaining)})


def parse_entry(entry: object, marker: bool) -> tuple[Version, bool] | None:
    """Return a listing entry and whether it is the latest entry of its key; None when it lacks a key, version, boolean, size or time."""
    if not isinstance(entry, dict):
        return None
    key, version_id, latest, size = entry.get("Key"), entry.get("VersionId"), entry.get("IsLatest"), 0 if marker else entry.get("Size")
    stored_at = stored_time(entry.get("LastModified"))
    if (
        not (isinstance(key, str) and key and isinstance(version_id, str) and version_id and isinstance(latest, bool) and type(size) is int and size >= 0)
        or stored_at is None
    ):
        return None
    return Version(key, version_id, size, stored_at), latest


def declares_more_results(page: dict[str, Any]) -> bool:
    """Return True when a page says that more results remain: a truncation flag other than false or a continuation marker other than empty."""
    flag = page.get("IsTruncated")
    return (flag is not None and flag is not False) or any(page.get(name) not in (None, "") for name in ("NextKeyMarker", "NextVersionIdMarker"))


def read_listing(client: AwsCli, deadline_at: float, tally: Tally, findings: Findings) -> Listing | None:
    """Read every page of the version listing; return None after the first finding, because a partial listing is never used."""
    listing = Listing()
    latest_keys: set[str] = set()
    tokens: set[str] = set()
    lines: list[str] = []
    token: str | None = None
    while True:
        try:
            page = client.call("s3api", "list-object-versions", ["--max-items", str(PAGE_SIZE), *(["--starting-token", token] if token else [])])
        except StorageError as error:
            late = error.code == "request_unconfirmed" and time.monotonic() >= deadline_at
            findings.add("listing_deadline" if late else "listing_failed", "aws", f"page {tally.listing['pages'] + 1} failed ({error.code})")
            return None
        tally.listing["pages"] += 1
        for name, marker in (("Versions", False), ("DeleteMarkers", True)):
            entries = page.get(name, [])
            for entry in entries if isinstance(entries, list) else [None]:
                parsed = parse_entry(entry, marker)
                if parsed is None or (parsed[0].key, parsed[0].version_id) in listing.listed or (parsed[1] and parsed[0].key in latest_keys):
                    findings.add("listing_malformed", "aws", f"page {tally.listing['pages']} has a {name} entry that is not valid or repeats an earlier entry")
                    return None
                version, latest = parsed
                listing.listed.add((version.key, version.version_id))
                lines.append(f"{version.key}\0{version.version_id}\0{marker}\0{latest}\0{version.size}\0{version.stored_at.isoformat()}")
                tally.listing["delete_markers" if marker else "versions"] += 1
                if latest:
                    latest_keys.add(version.key)
                    if not marker:
                        listing.current.append(version)
                        listing.identities.add((version.key, version.version_id))
        token = page.get("NextToken")
        if token is None:
            if declares_more_results(page):
                findings.add("listing_incomplete", "aws", f"page {tally.listing['pages']} has no continuation token and still declares more results")
                return None
            tally.listing_sha256 = hashlib.sha256("\n".join(sorted(lines)).encode()).hexdigest()
            return listing
        if not isinstance(token, str) or token in tokens:
            findings.add("listing_malformed", "aws", f"page {tally.listing['pages']} gives a continuation token that is not text or repeats an earlier one")
            return None
        tokens.add(token)


def project_identity_matches(identity: dict[str, Any], settings: dict[str, str]) -> bool:
    """Return True only for the same-named IAM user of the project in the configured account."""
    account, arn = settings["expected_account_id"], identity.get("Arn")
    return (
        identity.get("Account") == account
        and isinstance(arn, str)
        and arn.startswith(f"arn:aws:iam::{account}:user/")
        and arn.rsplit("/", 1)[-1] == settings["aws_profile"]
    )


def read_bucket(deadline: float, tally: Tally, findings: Findings) -> tuple[Listing, str] | None:
    """Check the project identity, then list the bucket; one deadline covers the identity call and every page."""
    deadline_at = time.monotonic() + deadline
    try:
        settings, _ = load_configuration(REPO_ROOT / ".env")
    except ConfigurationError as error:
        findings.add("configuration_invalid", ".env", str(error))
        return None
    client = AwsCli(settings)
    client.runner = DeadlineRunner(client.runner, deadline_at)
    try:
        identity = client.call("sts", "get-caller-identity")
    except StorageError as error:
        late = error.code == "request_unconfirmed" and time.monotonic() >= deadline_at
        findings.add("listing_deadline" if late else "identity_not_verified", "aws", f"the identity call failed ({error.code})")
        return None
    if not project_identity_matches(identity, settings):
        findings.add("identity_not_verified", "aws", "the project profile does not authenticate as its same-named IAM user in the configured account")
        return None
    listing = read_listing(client, deadline_at, tally, findings)
    return (listing, settings["data_bucket_name"]) if listing is not None else None


def payload_objects(listing: Listing, bucket: str, tally: Tally, findings: Findings) -> list[Stored]:
    """Return the current payload versions in the data and evidence scopes; manifests and the lakehouse folder are counted and left out."""
    stored: list[Stored] = []
    for version in listing.current:
        parts = version.key.split("/")
        if parts[0] == "lakehouse" or (len(parts) > 2 and parts[2] == "manifests"):
            tally.payload["not_payload"] += 1
            continue
        checksums = {part for part in parts if HEX64.fullmatch(part)}
        if len(checksums) != 1:
            findings.add("key_checksum_unreadable", version.key, "the key does not hold exactly one SHA-256")
            continue
        (checksum,) = checksums
        try:
            scope = reference_scope(version.key, checksum)
        except CaptureError:
            findings.add("unclassified_key", version.key, "the key is not in a data, reference or audit folder or has an empty part")
            continue
        if version.version_id == "null":
            findings.add("version_missing_or_null", version.key, "the current version has no usable version ID")
            continue
        stored.append(Stored(scope, bucket, version, checksum))
    return stored


def copy_report(item: Stored) -> dict[str, str]:
    return {"key": item.version.key, "version_id": item.version.version_id, "stored_at": item.version.stored_at.isoformat().replace("+00:00", "Z")}


def name_copies(stored: list[Stored], tally: Tally, findings: Findings) -> list[Stored]:
    """Name one copy for each checksum, the copy stored first (stored time, then key, then version); report the others as aliases."""
    by_checksum: defaultdict[str, list[Stored]] = defaultdict(list)
    for item in stored:
        by_checksum[item.sha256].append(item)
    kept: list[Stored] = []
    for checksum, copies in sorted(by_checksum.items()):
        ordered = sorted(copies, key=lambda item: (item.version.stored_at, item.version.key, item.version.version_id))
        names = "; ".join(f"{item.version.key} version {item.version.version_id}" for item in ordered)
        if len({item.scope for item in copies}) > 1:
            findings.add("checksum_in_both_scopes", checksum, f"the checksum is stored in the data and evidence scopes: {names}")
        if len({item.version.size for item in copies}) > 1:
            findings.add("checksum_sizes_disagree", checksum, f"the checksum is stored with more than one size: {names}")
        kept.append(ordered[0])
        if len(ordered) > 1:
            tally.payload["alias_copies"] += len(ordered) - 1
            tally.aliases.append(
                {"scope": ordered[0].scope, "sha256": checksum, "kept": copy_report(ordered[0]), "aliases": [copy_report(item) for item in ordered[1:]]}
            )
    return kept


def upload_sizes_valid(item: Stored, recorded_sizes: set[int | None], findings: Findings) -> bool:
    """Report an upload record size that is missing, invalid, conflicting or different from the listing; return whether the sizes are valid."""
    if None in recorded_sizes:
        findings.add("upload_record_size_invalid", item.version.key, f"version {item.version.version_id} has an upload record without one valid recorded size")
    elif len(recorded_sizes) > 1:
        findings.add("upload_record_sizes_conflict", item.version.key, f"version {item.version.version_id} has upload records with different sizes")
    elif recorded_sizes != {item.version.size}:
        findings.add(
            "upload_record_size_differs",
            item.version.key,
            f"version {item.version.version_id} is {item.version.size} bytes and its upload record says otherwise",
        )
    else:
        return True
    return False


def cross_check(stored: list[Stored], listing: Listing, records: Records, reasons: Reasons, bucket: str, tally: Tally, findings: Findings) -> None:
    """Report the differences between the local records and the listing in both directions; a stored object counts once, for the first source that names it."""
    tally.cross_check["objects_named_by_records"] = len(records.named)
    tally.cross_check["upload_record_identities"] = len(records.uploads)
    absent: set[tuple[str, str]] = set()
    for (key, version_id), retirement in sorted(records.retired.items()):
        if retirement.bucket is not None and retirement.bucket != bucket:
            findings.add("retirement_other_bucket", key, f"the retirement line for version {version_id} names another bucket")
        elif (key, version_id) in listing.listed:
            findings.add(
                "retired_object_still_listed", key, f"version {version_id} is retired and the bucket listing still holds it as a version or a delete marker"
            )
        else:
            absent.add((key, version_id))
    tally.cross_check["retirement_identities_absent"] = len(absent)
    for record_bucket, key, version_id in sorted(records.named):
        identity = (key, version_id)
        if record_bucket == bucket and identity in records.retired:
            tally.cross_check["retired_objects_absent"] += identity in absent
        elif record_bucket != bucket or identity not in listing.identities:
            findings.add("record_object_not_in_listing", key, f"version {version_id} is not a current version in the bucket listing")
    used: set[tuple[str, str]] = set()
    for item in stored:
        parts = item.version.key.split("/")
        where = (item.version.key, item.version.version_id)
        sizes_valid = where not in records.uploads or upload_sizes_valid(item, records.uploads[where], findings)
        if item.identity in records.named:
            tally.cross_check["stored_objects_named"] += 1
        elif where in records.uploads:
            if sizes_valid:
                tally.cross_check["stored_objects_named_by_upload_record"] += 1
        elif len(parts) > 3 and parts[2] == "audit" and parts[3] in records.explained_snapshots:
            tally.cross_check["stored_objects_explained_by_disposition"] += 1
        elif where in reasons and reasons[where][:2] == (item.version.size, item.sha256):
            used.add(where)
            tally.cross_check["stored_objects_explained_by_object_disposition"] += 1
        else:
            findings.add("stored_object_without_record", item.version.key, f"version {item.version.version_id} is stored and no local record names it")
    by_where = {(item.version.key, item.version.version_id): item for item in stored}
    for where, (size, checksum, _reason) in sorted(reasons.items()):
        match = by_where.get(where)
        if match is None or (match.version.size, match.sha256) != (size, checksum):
            findings.add(
                "object_disposition_unmatched",
                where[0],
                f"the line for version {where[1]} matches no stored payload object by key, version, byte count and SHA-256",
            )
        elif where not in used:
            findings.add(
                "object_disposition_unneeded", where[0], f"the line for version {where[1]} is not needed: a record or capture reason already names the object"
            )


def ignored_by_git(path: Path) -> bool:
    return run_command("git", ["check-ignore", "-q", "--", str(path)], cwd=REPO_ROOT, timeout=30).returncode == 0


def switch_on_text(index_path: Path, checksum: str) -> str:
    shown = index_path.relative_to(REPO_ROOT) if index_path.is_relative_to(REPO_ROOT) else index_path
    content = json.dumps({"single_copy_enabled": True, "bootstrap_index": shown.as_posix(), "bootstrap_index_sha256": checksum}, indent=2)
    return (
        f"Index written: {index_path}\nSHA-256: {checksum}\n\n"
        f"To switch single-copy storage on, run this from the repository root. It changes only {POLICY_PATH}. Keep that change local.\n\n"
        f"printf '%s\\n' {shlex.quote(content)} > {POLICY_PATH}\n\n"
        f"To switch it off again: git checkout -- {POLICY_PATH}\n"
    )


def index_document(kept: list[Stored]) -> dict[str, Any]:
    """Return the schema 2 index: sorted checksum maps and a generation digest of their canonical bytes."""
    maps: dict[str, Any] = {"objects": {}, "evidence_objects": {}}
    for item in kept:
        maps[item.scope][item.sha256] = item.index_record()
    return {"schema_version": 2, "generation": hashlib.sha256(encoded_json(maps)).hexdigest(), **maps}


def count_by_scope(items: list[Stored]) -> dict[str, int]:
    return {scope: sum(1 for item in items if item.scope == scope) for scope in ("objects", "evidence_objects")}


def main(argv: Sequence[str] | None = None) -> int:
    """List the bucket read-only, cross-check the local records and write the starting index; return 0 when written and 1 when refused."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    parser.add_argument("--records", type=Path, default=DEFAULT_RECORDS, help="folder of the local download records, used as the cross-check")
    parser.add_argument("--retirements", nargs="*", type=Path, help=f"retirement records; default: {', '.join(DEFAULT_RETIREMENTS)}")
    parser.add_argument("--dispositions", type=Path, help="reviewed acquired captures that have no storage record, bound to their path and receipt checksum")
    parser.add_argument("--evidence-root", type=Path, default=REPO_ROOT / "data", help="folder of the upload records, read as the whole-bucket check reads it")
    parser.add_argument(
        "--empty-capture-dispositions", type=Path, help="reviewed empty publisher captures under the evidence root, as for the whole-bucket check"
    )
    parser.add_argument(
        "--object-dispositions",
        type=Path,
        help="a written reason for each stored object that no record names, bound to its key, version, byte count and SHA-256",
    )
    parser.add_argument("--output", type=Path, required=True, help="new index file in an ignored folder")
    parser.add_argument("--deadline", type=float, default=DEFAULT_DEADLINE_SECONDS, help="seconds for the identity check and all listing requests together")
    arguments = parser.parse_args(argv)
    if arguments.deadline <= 0:
        parser.error("--deadline must be above 0")
    findings, tally = Findings(), Tally()
    output = arguments.output.resolve()
    if not ignored_by_git(output):
        findings.add("output_not_ignored", output.name, "the index must be written inside a folder Git ignores")
    records = read_records(arguments, tally, findings) if not findings.counts else Records()
    reasons = load_object_dispositions(arguments.object_dispositions, findings) if not findings.counts else {}
    fetched = read_bucket(arguments.deadline, tally, findings) if not findings.counts else None
    if fetched is not None:
        records.uploads = read_upload_records(arguments, fetched[1], findings)
    stored: list[Stored] = []
    kept: list[Stored] = []
    if fetched is not None and not findings.counts:
        listing, bucket = fetched
        stored = payload_objects(listing, bucket, tally, findings)
        kept = name_copies(stored, tally, findings)
        cross_check(stored, listing, records, reasons, bucket, tally, findings)
        if not stored and not findings.counts:
            findings.add("no_stored_payloads", "aws", "the bucket listing holds no current payload version")
    summary: dict[str, Any] = {
        "status": "refused" if findings.counts else "written",
        "captures": {**dict(sorted(tally.captures.items())), "not_stored": dict(sorted(tally.not_stored.items()))},
        "listing": {**dict(sorted(tally.listing.items())), "sha256": tally.listing_sha256},
        "payload": dict(sorted(tally.payload.items())),
        "copies": count_by_scope(stored),
        "entries": count_by_scope(kept),
        "cross_check": dict(sorted(tally.cross_check.items())),
        "aliases": tally.aliases,
        "refusals": findings.report(),
    }
    if not findings.counts:
        index = index_document(kept)
        content = encoded_json(index)
        try:
            write_once(output, content)
        except CaptureError:
            findings.add("output_differs", output.name, "the output file exists with other bytes; choose a new path")
            summary.update(status="refused", refusals=findings.report())
        else:
            checksum = hashlib.sha256(content).hexdigest()
            summary["index"] = {
                "path": str(output),
                "sha256": checksum,
                "bytes": len(content),
                "generation": index["generation"],
                "objects": len(index["objects"]),
                "evidence_objects": len(index["evidence_objects"]),
            }
            sys.stderr.write(switch_on_text(output, checksum))
    sys.stdout.write(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    return 1 if findings.counts else 0


if __name__ == "__main__":
    raise SystemExit(main())
