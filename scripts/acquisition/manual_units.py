"""The host's attempt for each ``approved_manual`` unit: one plain download of the unit's recorded URL, bounded and polite.

Units the publisher serves go through the capture path the run loop supplies. The rest keep their last S3 capture: each gets one create-once stale
record, and the run record and the notice list them. The attempt sends no form, no sign-in and no generated ID beyond the recorded URL, and only the
generic agent header of the reviewed transport.

* robots.txt is read once per host per run through the same transport (1 attempt, 10 s, 512 KiB). It is enforced for the four manual-unit hosts and only
  reported for every other host.
* Every request to a manual host is at least 5 s after the last one, retries and redirects included, one at a time. A redirect destination is checked
  against robots.txt before it is requested. After 2 units of a host end with 401, 403 or 429 the run stops asking that host.
* A unit with a stale record younger than 20 hours is skipped, so a repeat run on the same day sends nothing.
* A unit whose derived file is rebuilt (a HUD workbook or a WONDER export) takes its original from the seed folder when the run has one, and sends
  no request: the run stops before any request when the seed lacks the original or the specification records no digest of the derived file. The derived
  file is rebuilt by the contract and must equal the recorded SHA-256 and size; a mismatch makes the unit stale and hands nothing to the capture path.
* A stale record names, as the last capture, the data and other objects of the unit's newest completion record, and the specification's pointer only
  when the unit has no completion record. A completion record that is unreadable or refused stops the unit.
"""

from __future__ import annotations

import base64
import hashlib
import json
import shutil
import time
import urllib.parse
import urllib.request
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol
from urllib.robotparser import RobotFileParser

from scripts.acquisition import completion_record, manual_content, raw_root, redownload_controls, stale_record, transport
from scripts.acquisition.manual_content import ContentRefusal, ManualUnitError, Scopes
from scripts.acquisition.s3_store import StorageError

MANUAL_HOSTS = ("huduser.gov", "data.census.gov", "wonder.cdc.gov", "download.bls.gov")
ROBOTS_AGENT = "HistoricalSourceCapture"
ROBOTS_LIMITS = transport.Limits(attempts=1, timeout_seconds=10, max_bytes=512 * 1024)
SPACING_SECONDS = 5.0
REFUSAL_LIMIT = 2
REFUSAL_STATUSES = frozenset({401, 403, 429})
RETRY_INTERVAL = timedelta(hours=20)
RETRYABLE_STATUSES = frozenset({408, 429, 500, 502, 503, 504})
ABSENT_ROBOTS = frozenset({"http_404", "http_410"})
CREATE_CONFLICTS = frozenset({"412", "PreconditionFailed", "409", "ConditionalRequestConflict"})
STABLE_IDENTITY = ("unit_key", "collection", "source_id", "snapshot_id")
RUN_IDENTITY = (*STABLE_IDENTITY, "run_id", "code_sha256", "bundle_sha256")
TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
__all__ = ["ContentRefusal", "Context", "ManualUnitError", "Outcome", "Report", "Scopes", "attempt", "is_manual_host", "notice_text", "run"]


class Clock(Protocol):
    def now(self) -> datetime: ...

    def monotonic(self) -> float: ...

    def sleep(self, seconds: float) -> None: ...


class SystemClock:
    def now(self) -> datetime:
        return datetime.now(UTC)

    def monotonic(self) -> float:
        return time.monotonic()

    def sleep(self, seconds: float) -> None:
        time.sleep(seconds)


def is_manual_host(host: str) -> bool:
    """True for a manual-unit host or one of its subdomains, never for a look-alike that only ends with the same letters."""
    name = host.lower().rstrip(".")
    return any(name == suffix or name.endswith("." + suffix) for suffix in MANUAL_HOSTS)


def host_of(url: str) -> str:
    return (urllib.parse.urlsplit(url).hostname or "").lower()


def stamp(moment: datetime) -> str:
    return moment.astimezone(UTC).strftime(TIME_FORMAT)


def record_instant(value: object) -> datetime:
    """A recorded time as one instant, whichever ISO 8601 form with an offset a specification, receipt or completion record gave it."""
    try:
        moment = datetime.fromisoformat(str(value))
    except ValueError:
        moment = None
    if moment is None or moment.tzinfo is None:
        raise ManualUnitError("A last capture pointer needs retrieved_at_utc as an ISO 8601 time with a UTC offset")
    return moment


def record_time(value: object) -> str:
    """A recorded time in the stale record's form."""
    return stamp(record_instant(value))


class Observer(urllib.request.BaseHandler):
    """Sits below the redirect policy and sees every request the opener sends: the first, each retry and each redirect.

    A redirect destination is checked against robots.txt before it is sent. Every request waits for its host's spacing, is counted, and keeps the last
    answer's Retry-After header. ``first_url`` is the address the unit already passed; the robots.txt fetch itself passes none.
    """

    handler_order = 600

    def __init__(self, context: Context, first_url: str | None = None) -> None:
        self.context = context
        self.first_url = first_url
        self.retry_after: str | None = None
        self.blocked: str | None = None

    def https_request(self, request: Any) -> Any:
        host = host_of(request.full_url)
        if self.first_url is not None and request.full_url != self.first_url:
            self.blocked = self.context.gate.check(request.full_url)
            if self.blocked is not None:
                raise transport.CaptureError("A redirect destination is not allowed by robots.txt.")
        self.context.gate.wait_turn(host)
        self.context.count_request()
        self.context.gate.sent(host)
        return request

    def https_response(self, request: Any, response: Any) -> Any:
        self.retry_after = response.headers.get("Retry-After")
        return response

    def retry_after_too_long(self, limit: float) -> bool:
        """The transport ends a unit at once for a Retry-After above its limit or one it cannot read as seconds."""
        value = (self.retry_after or "").strip()
        return bool(value) and (not value.isdigit() or int(value) > limit)


@dataclass
class HostGate:
    """robots.txt, spacing and refusal counts for the hosts of one run. The robots result of each host is read again by every run."""

    context: Context
    results: dict[str, dict[str, Any]] = field(default_factory=dict)
    rules: dict[str, RobotFileParser] = field(default_factory=dict)
    last_sent: dict[str, float] = field(default_factory=dict)
    refusals: Counter[str] = field(default_factory=Counter)

    def read_robots(self, host: str) -> None:
        folder = self.context.scratch / "robots" / host
        outcome, status, digest = "unreadable", None, None
        try:
            result, _ = self.context.fetch(f"https://{host}/robots.txt", folder, "robots.txt", "txt", "reference", ROBOTS_LIMITS)
            status = result.http_status
            if result.complete:
                body = result.path.read_bytes()
                digest = hashlib.sha256(body).hexdigest()
                parser = RobotFileParser()
                parser.parse(body.decode("utf-8", errors="replace").splitlines())
                self.rules[host], outcome = parser, "read"
            elif result.failure in ABSENT_ROBOTS or (status == 200 and result.failure == "empty_response"):
                outcome = "absent"
        except (transport.CaptureError, OSError):
            outcome = "unreadable"
        finally:
            shutil.rmtree(folder, ignore_errors=True)
        self.results[host] = {
            "enforced": is_manual_host(host),
            "outcome": outcome,
            "http_status": status,
            "checked_at_utc": stamp(self.context.clock.now()),
            "robots_sha256": digest,
            "would_disallow": 0,
            "units": 0,
            "refusals": 0,
        }

    def check(self, url: str) -> str | None:
        """None when the run may request the URL, else the stale reason. A host that is only reported on never refuses; its disallowed paths are counted."""
        host = host_of(url)
        parser = self.rules.get(host)
        disallowed = parser is not None and not parser.can_fetch(ROBOTS_AGENT, url)
        result = self.results.get(host)
        if result is not None:
            result["would_disallow"] += int(disallowed)
        if not is_manual_host(host):
            return None
        if result is None or result["outcome"] == "unreadable":
            return "robots_unreadable"
        return "robots_disallowed" if disallowed else None

    def admit(self, url: str) -> str | None:
        """None when the run may request the unit's URL, else the stale reason."""
        host = host_of(url)
        if host not in self.results:
            self.read_robots(host)
        self.results[host]["units"] += 1
        reason = self.check(url)
        if reason is None and self.results[host]["enforced"] and self.refusals[host] >= REFUSAL_LIMIT:
            return "host_refused"
        return reason

    def wait_turn(self, host: str) -> None:
        """Hold the next request to a manual host until 5 s have passed since the last one started or ended."""
        if is_manual_host(host) and host in self.last_sent:
            wait = SPACING_SECONDS - (self.context.clock.monotonic() - self.last_sent[host])
            if wait > 0:
                self.context.clock.sleep(wait)

    def sent(self, host: str) -> None:
        self.last_sent[host] = self.context.clock.monotonic()

    def refused(self, host: str, status: int | None) -> None:
        if status in REFUSAL_STATUSES:
            self.refusals[host] += 1
            if host in self.results:
                self.results[host]["refusals"] = self.refusals[host]

    def report(self) -> dict[str, dict[str, Any]]:
        return {host: dict(result) for host, result in sorted(self.results.items())}


@dataclass
class Context:
    """What one run needs. ``capture`` is the run loop's normal capture path: it receives the checked files and returns its completion information."""

    client: Any
    root: str
    run_id: str
    code_sha256: str
    bundle_sha256: str
    scratch: Path
    capture: Callable[[dict, dict, dict[str, Path]], dict]
    unit_key: Callable[[dict], str]
    collection: Callable[[dict], str]
    clock: Clock = field(default_factory=SystemClock)
    handlers: tuple[urllib.request.BaseHandler, ...] = field(default_factory=lambda: (transport.PublicHTTPSHandler(),))
    scopes: Scopes = field(default_factory=Scopes)
    seed: Path | None = None
    on_request: Callable[[], None] | None = None
    limits: transport.Limits = field(default_factory=transport.Limits)
    requests: int = 0
    downloaded: int = 0
    gate: HostGate = field(init=False)

    def __post_init__(self) -> None:
        self.gate = HostGate(self)

    def count_request(self) -> None:
        self.requests += 1
        if self.on_request is not None:
            self.on_request()

    def fetch(
        self, url: str, folder: Path, name: str, expected: str, role: str, limits: transport.Limits, *, unit: bool = False
    ) -> tuple[transport.Download, Observer]:
        observer = Observer(self, url if unit else None)
        result = transport.download(url, folder, name, expected, role, limits, opener=transport.ReviewedTransport(observer, *self.handlers))
        self.downloaded += result.byte_count
        self.gate.sent(host_of(url))
        return result, observer


@dataclass(frozen=True)
class Outcome:
    """What happened to one unit: captured, stale, or skipped because a recent stale record already stands.

    ``record`` is the stale record that stands for the unit after the attempt: the one stored for this run, or the recent one that caused the skip.
    """

    snapshot_id: str
    source_id: str
    status: str
    reason: str | None = None
    http_status: int | None = None
    record: dict[str, Any] | None = None


@dataclass
class Report:
    outcomes: list[Outcome]
    counts: dict[str, int]
    stale_units: list[dict[str, Any]]
    missing_units: list[dict[str, Any]]
    robots: dict[str, dict[str, Any]]
    http_requests: int
    bytes_downloaded: int

    def run_record(self) -> dict[str, Any]:
        """The fields the run record gains: the stale and missing lists, the robots results and the request counters."""
        return {
            "stale_units": self.stale_units,
            "missing_units": self.missing_units,
            "robots": self.robots,
            "manual_counts": self.counts,
            "manual_http_requests": self.http_requests,
            "manual_bytes_downloaded": self.bytes_downloaded,
        }


def encoded_digest(digest: str) -> str:
    return base64.b64encode(bytes.fromhex(digest)).decode()


def pointers(spec: Mapping[str, Any]) -> list[dict[str, Any]]:
    """The last capture a specification names, in the stale record's artifact form. A specification without one names none."""
    last = spec.get("last_capture")
    if last is None:
        return []
    root = last.get("read_root") if isinstance(last, dict) else None
    if root not in stale_record.READ_ROOTS or not isinstance(last.get("artifacts"), list):
        raise ManualUnitError("A last capture pointer needs its read root and its artifacts")
    found = []
    for item in last["artifacts"]:
        key = item["key"] if str(item["key"]).startswith(root) else root + str(item["key"])
        found.append(
            {
                "key": key,
                "version_id": item["version_id"],
                "sha256": item["sha256"],
                "byte_count": item["byte_count"],
                "role": item["role"],
                "stored_file_name": item.get("stored_file_name") or key.rsplit("/", 1)[-1],
                "retrieved_at_utc": record_time(item["retrieved_at_utc"]),
                "read_root": root,
            }
        )
    return found


def scratch_for(context: Context, unit_key: str) -> Path:
    folder = context.scratch / "records" / unit_key[:16]
    folder.mkdir(parents=True, exist_ok=True)
    return folder


def read_completion(key: str, unit: dict, context: Context, local: Path) -> tuple[datetime, dict[str, Any]]:
    """One completion record of the unit, read, checked against its key and the unit, with its retrieval time as an instant. A fault stops the unit."""
    unit_key = context.unit_key(unit)
    try:
        context.client.call("s3api", "get-object", ["--key", key, str(local)])
    except StorageError as error:
        raise ManualUnitError(f"The completion record {key} of unit {unit_key} could not be read: {error.code}") from error
    try:
        record = completion_record.parse(local.read_bytes(), key=key, root=context.root)
        moment = record_instant(record["retrieved_at_utc"])
    except (completion_record.RecordError, ManualUnitError) as error:
        raise ManualUnitError(f"The completion record {key} of unit {unit_key} is refused: {error}") from error
    expected = unit_identity(unit, context)
    differing = [name for name in ("collection", "source_id") if record[name] != expected[name]]
    if differing:
        raise ManualUnitError(f"The completion record {key} does not belong to unit {unit_key}: its {', '.join(differing)} differ")
    nameless = [item["key"] for item in record["other_objects"] if not item["key"].rsplit("/", 1)[-1]]
    if nameless:
        raise ManualUnitError(f"The completion record {key} of unit {unit_key} is refused: {nameless[0]} has no stored file name")
    return moment, record


def newest_completion(unit: dict, context: Context) -> dict[str, Any] | None:
    """The unit's newest completion record by retrieval time, or None when it has none. Every record is read, so one that is refused stops the unit.

    Two records with the same retrieval time have no newest, so the unit stops too.
    """
    unit_key = context.unit_key(unit)
    listing = context.client.call("s3api", "list-objects-v2", ["--prefix", completion_record.unit_prefix(context.root, unit_key)])
    keys = sorted(item["Key"] for item in listing.get("Contents", []))
    folder = scratch_for(context, unit_key)
    found = sorted(
        ((*read_completion(key, unit, context, folder / f"completion_{number}.json"), key) for number, key in enumerate(keys)), key=lambda item: item[0]
    )
    if len(found) > 1 and found[-1][0] == found[-2][0]:
        raise ManualUnitError(f"Unit {unit_key} has two completion records with the same retrieved_at_utc: {found[-2][2]} and {found[-1][2]}")
    return found[-1][1] if found else None


def completion_pointers(record: Mapping[str, Any]) -> list[dict[str, Any]]:
    """The data and other objects of a completion record, in the stale record's artifact form.

    Another object carries no stored file name in the completion record, so the last segment of its key stands for it.
    """
    retrieved = record_time(record["retrieved_at_utc"])
    return [
        {
            "key": item["key"],
            "version_id": item["version_id"],
            "sha256": item["sha256"],
            "byte_count": item["byte_count"],
            "role": item["role"],
            "stored_file_name": item["stored_file_name"] if "stored_file_name" in item else item["key"].rsplit("/", 1)[-1],
            "retrieved_at_utc": retrieved,
            "read_root": raw_root.RAW_ROOT if item["key"].startswith(raw_root.RAW_ROOT) else "",
        }
        for item in [*record["data_objects"], *record["other_objects"]]
    ]


def last_capture(unit: dict, spec: Mapping[str, Any], context: Context) -> list[dict[str, Any]]:
    """The objects a stale record names: those of the unit's newest completion record, and the specification's pointer only when it has none."""
    completion = newest_completion(unit, context)
    return pointers(spec) if completion is None else completion_pointers(completion)


def verified(client: Any, found: Sequence[Mapping[str, Any]]) -> bool:
    """True when a HEAD of every named version matches its recorded checksum and length. No pointer means nothing to load, so not verified."""
    for item in found:
        try:
            head = client.call("s3api", "head-object", ["--key", item["key"], "--version-id", item["version_id"], "--checksum-mode", "ENABLED"])
        except StorageError:
            return False
        if head.get("VersionId") != item["version_id"] or head.get("ContentLength") != item["byte_count"]:
            return False
        if head.get("ChecksumSHA256") != encoded_digest(item["sha256"]):
            return False
    return bool(found)


def create_once(client: Any, key: str, body: bytes, folder: Path) -> bytes:
    """Create one object and return the bytes now stored there. A record is never replaced: after a create conflict the stored one is read back."""
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / "record.json"
    path.write_bytes(body)
    digest = hashlib.sha256(body).hexdigest()
    arguments = ["--key", key, "--body", str(path), "--if-none-match", "*", "--checksum-algorithm", "SHA256", "--checksum-sha256", encoded_digest(digest)]
    try:
        client.call("s3api", "put-object", [*arguments, "--server-side-encryption", "AES256", "--metadata", json.dumps({"sha256": digest})])
    except StorageError as conflict:
        if conflict.code not in CREATE_CONFLICTS:
            raise
        stored = folder / "stored.json"
        try:
            client.call("s3api", "get-object", ["--key", key, str(stored)])
        except StorageError as missing:
            raise ManualUnitError("A create conflict left no stored stale record to read back") from missing
        return stored.read_bytes()
    return body


def unit_identity(unit: dict, context: Context) -> dict[str, str]:
    """What a stale record of this unit says about the unit, in every run."""
    return {"unit_key": context.unit_key(unit), "collection": context.collection(unit), "source_id": unit["source_id"], "snapshot_id": unit["snapshot_id"]}


def mark_stale(unit: dict, spec: dict, reason: str, http_status: int | None, context: Context) -> dict[str, Any]:
    """Write the unit's create-once stale record for this run and return the record that is stored."""
    found = last_capture(unit, spec, context)
    key = context.unit_key(unit)
    record = stale_record.build(
        **unit_identity(unit, context),
        run_id=context.run_id,
        attempted_at_utc=stamp(context.clock.now()),
        reason=reason,
        http_status=http_status,
        code_sha256=context.code_sha256,
        bundle_sha256=context.bundle_sha256,
        last_capture_verified=verified(context.client, found),
        last_capture=found,
    )
    stored_key = stale_record.stale_key(context.root, key, context.run_id)
    stored = stale_record.parse(
        create_once(context.client, stored_key, stale_record.encode(record), context.scratch / "records" / key[:16]), key=stored_key, root=context.root
    )
    if any(stored[name] != record[name] for name in RUN_IDENTITY):
        raise ManualUnitError("The stored stale record differs from the unit and run it was written for")
    return stored


def newest_stale(unit: dict, context: Context) -> dict[str, Any] | None:
    """The unit's newest stale record, read and checked against the unit. Run IDs start with their UTC time, so the last key is the newest."""
    unit_key = context.unit_key(unit)
    prefix = stale_record.stale_prefix(context.root, unit_key)
    listing = context.client.call("s3api", "list-objects-v2", ["--prefix", prefix])
    keys = sorted(item["Key"] for item in listing.get("Contents", []))
    if not keys:
        return None
    local = scratch_for(context, unit_key) / "newest.json"
    context.client.call("s3api", "get-object", ["--key", keys[-1], str(local)])
    newest = stale_record.parse(local.read_bytes(), key=keys[-1], root=context.root)
    expected = unit_identity(unit, context)
    differing = [name for name in STABLE_IDENTITY if newest[name] != expected[name]]
    if differing:
        raise ManualUnitError(f"The newest stale record {keys[-1]} does not belong to unit {unit_key}: its {', '.join(differing)} differ")
    return newest


def classify(result: transport.Download, observer: Observer, limits: transport.Limits) -> tuple[str, int | None]:
    """The stale reason of a failed download and the HTTP status to record with it."""
    failure, status = str(result.failure), result.http_status
    if observer.blocked is not None:
        return observer.blocked, status
    if failure.startswith("http_"):
        if status in RETRYABLE_STATUSES and observer.retry_after_too_long(limits.max_retry_delay):
            return "rate_limited", status
        return "http_status", status
    if failure in transport.PAYLOAD_FAILURES:
        return "not_expected_format", status
    return "network", status


def fresh(path: Path) -> Path:
    shutil.rmtree(path, ignore_errors=True)
    path.mkdir(parents=True)
    return path


def staged(original: Path, derived: dict[str, bytes], spec: dict) -> dict[str, Path] | None:
    """The original and its derived files side by side in the unit's working folder, or None when they are not the specification's stored files."""
    files = {original.name: original}
    for derived_name, body in derived.items():
        files[derived_name] = original.parent / derived_name
        files[derived_name].write_bytes(body)
    return files if sorted(files) == sorted(spec["stored_data_files"]) else None


def download_unit(unit: dict, spec: dict, mode: str, name: str, context: Context) -> tuple[dict[str, Path] | None, str | None, int | None]:
    """Download and check one unit. Returns the files to capture, or no files with the stale reason and status."""
    host = host_of(unit["url"])
    folder = fresh(context.scratch / context.unit_key(unit)[:16] / "download")
    try:
        result, observer = context.fetch(unit["url"], folder, name, manual_content.expected_format(mode), "data", context.limits, unit=True)
    except transport.CaptureError:
        return None, "network", None
    context.gate.refused(host, result.http_status)
    if not result.complete:
        reason, status = classify(result, observer, context.limits)
        return None, reason, status
    try:
        derived = manual_content.accept(mode, result.path, name, context.scopes)
    except ContentRefusal as refusal:
        return None, refusal.reason, result.http_status
    files = staged(result.path, derived, spec)
    return (files, None, result.http_status) if files is not None else (None, "contract_failed", result.http_status)


def is_seeded(mode: str, context: Context) -> bool:
    """True when this unit takes its original from the seed folder: the run has one and the unit's derived file is rebuilt from its original."""
    return context.seed is not None and mode in manual_content.REBUILT_MODES


def seed_only_unit(unit: dict, spec: dict, mode: str, name: str, context: Context) -> tuple[dict[str, Path] | None, str | None]:
    """A unit of a seed-only mode: its original is bound to the locked plan and captured as it is. Sends no request, whatever happens.

    No seed folder is a stale unit ("seed_required"), and so is an absent original or a link ("seed_missing"): the unit is listed for the owner, and the
    other units still run. A file that the plan does not bind, or of another format than its name says, is stale with "contract_failed".
    """
    if context.seed is None:
        return None, "seed_required"
    original = seed_original(unit, name, context)
    try:
        # A link anywhere below the seed folder is refused, the folder of the unit and the file included: the file could be a copy of anything.
        redownload_controls.validate_path(original, context.seed)
    except ValueError:
        return None, "seed_missing"
    if not original.is_file():
        return None, "seed_missing"
    folder = fresh(context.scratch / context.unit_key(unit)[:16] / "download")
    staged_original = folder / name
    shutil.copyfile(original, staged_original)
    try:
        derived = manual_content.accept(mode, staged_original, name, context.scopes)
    except ContentRefusal as refusal:
        return None, refusal.reason
    files = staged(staged_original, derived, spec)
    return (files, None) if files is not None else (None, "contract_failed")


def seed_original(unit: dict, name: str, context: Context) -> Path:
    return (context.seed or Path()) / "manual_units" / unit["snapshot_id"] / name


def require_seed(unit: dict, spec: dict, mode: str, name: str, context: Context) -> None:
    """Refuse a seeded unit whose original is not a plain file in the seed or whose specification records no digest of its derived file."""
    original = seed_original(unit, name, context)
    if not original.is_file() or original.is_symlink():
        raise ManualUnitError(f"The seed lacks the original {name} of unit {unit['snapshot_id']}")
    if manual_content.recorded_derived(mode, spec) is None:
        raise ManualUnitError(f"Unit {unit['snapshot_id']} records no digest of its derived file {manual_content.MODES[mode][1]}")


def seed_unit(unit: dict, spec: dict, mode: str, name: str, context: Context) -> tuple[dict[str, Path] | None, str | None]:
    """Rebuild the unit's derived file from its seeded original. Returns the files to capture, or no files with the stale reason. Sends no request."""
    require_seed(unit, spec, mode, name, context)
    folder = fresh(context.scratch / context.unit_key(unit)[:16] / "download")
    original = folder / name
    shutil.copyfile(seed_original(unit, name, context), original)
    try:
        derived = manual_content.rebuild(mode, original, name, spec, context.scopes)
    except ContentRefusal as refusal:
        return None, refusal.reason
    files = staged(original, derived, spec)
    return (files, None) if files is not None else (None, "contract_failed")


def attempt(unit: dict, spec: dict, context: Context) -> Outcome:
    """One unit: skip it on a recent stale record, else download, check and capture it, or mark it stale."""
    mode, name = manual_content.plan_unit(unit, spec)
    key = context.unit_key(unit)
    try:
        newest = newest_stale(unit, context)
        if newest is not None and context.clock.now() - datetime.strptime(newest["attempted_at_utc"], TIME_FORMAT).replace(tzinfo=UTC) < RETRY_INTERVAL:
            return Outcome(unit["snapshot_id"], unit["source_id"], "skipped_recent_stale", newest["reason"], newest["http_status"], newest)
        files, status = None, None
        if mode in manual_content.SEED_ONLY_MODES:
            files, reason = seed_only_unit(unit, spec, mode, name, context)
        elif is_seeded(mode, context):
            files, reason = seed_unit(unit, spec, mode, name, context)
        elif (reason := context.gate.admit(unit["url"])) is None:
            files, reason, status = download_unit(unit, spec, mode, name, context)
        if files is not None:
            context.capture(unit, spec, files)
            return Outcome(unit["snapshot_id"], unit["source_id"], "captured", None, status)
        record = mark_stale(unit, spec, str(reason), status, context)
        return Outcome(unit["snapshot_id"], unit["source_id"], "stale", record["reason"], record["http_status"], record)
    finally:
        shutil.rmtree(context.scratch / key[:16], ignore_errors=True)
        shutil.rmtree(context.scratch / "records" / key[:16], ignore_errors=True)


def summary(record: Mapping[str, Any]) -> dict[str, Any]:
    """One stale record as a list entry: the unit, its source, the reason and the oldest retrieval date of its last capture."""
    dates = [item["retrieved_at_utc"] for item in record["last_capture"]]
    return {
        "unit_key": record["unit_key"],
        "snapshot_id": record["snapshot_id"],
        "source_id": record["source_id"],
        "reason": record["reason"],
        "last_retrieved_at_utc": min(dates) if dates else None,
    }


def split_lists(records: Sequence[Mapping[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Stale units, and apart from them the missing ones: a stale record whose last capture failed its HEAD check or names none."""
    stale = [summary(record) for record in records if record["last_capture_verified"]]
    missing = [summary(record) for record in records if not record["last_capture_verified"]]
    return stale, missing


def section(heading: str, items: list[dict[str, Any]]) -> list[str]:
    lines = [heading]
    for source in sorted({item["source_id"] for item in items}):
        mine = [item for item in items if item["source_id"] == source]
        oldest = min((item["last_retrieved_at_utc"] for item in mine if item["last_retrieved_at_utc"]), default=None)
        count = f"{len(mine)} unit" + ("" if len(mine) == 1 else "s")
        lines.append(f"{source}: {count}" + (f", oldest capture {oldest[:10]}" if oldest else ""))
    return lines


def notice_text(stale: list[dict[str, Any]], missing: list[dict[str, Any]]) -> str:
    """The notice's two sections, listed by source with unit counts and the oldest retrieval date. Empty when nothing is stale."""
    parts = []
    if stale:
        parts.append("\n".join(section("Stale: kept the last capture", stale)))
    if missing:
        parts.append("\n".join(section("Missing: no verified last capture", missing)))
    return "\n\n".join(parts)


def run(units: list[dict], specs: Mapping[str, dict], context: Context) -> Report:
    """Attempt every approved manual unit once. A unit or specification the handler cannot run stops the run before any request.

    The stale and missing lists hold every unit whose stale record stands, including the units skipped for a record younger than 20 hours.
    """
    if any(unit.get("disposition") != "approved_manual" for unit in units):
        raise ManualUnitError("The manual unit handler takes approved_manual units only")
    for unit in units:
        mode, name = manual_content.plan_unit(unit, specs.get(unit["snapshot_id"]))
        pointers(specs[unit["snapshot_id"]])
        if is_seeded(mode, context):
            require_seed(unit, specs[unit["snapshot_id"]], mode, name, context)
    outcomes = [attempt(unit, specs[unit["snapshot_id"]], context) for unit in units]
    shutil.rmtree(context.scratch / "robots", ignore_errors=True)
    standing = [outcome.record for outcome in outcomes if outcome.record is not None]
    stale, missing = split_lists(standing)
    counts = Counter(outcome.status for outcome in outcomes)
    return Report(
        outcomes,
        {
            "attempted": len(units) - counts["skipped_recent_stale"],
            "captured": counts["captured"],
            "stale": counts["stale"],
            "skipped_recent_stale": counts["skipped_recent_stale"],
        }
        | {"missing": len(missing)},
        stale,
        missing,
        context.gate.report(),
        context.requests,
        context.downloaded,
    )
