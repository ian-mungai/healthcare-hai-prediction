"""Run the frozen redownload queue into a fresh state root and compare it with the stored captures.

The queue (``data/redownload_checks/20260929/queue.json``) fixes every unit. File units are fetched with the
bounded downloader; plan-based units run the existing collector's capture path with a fresh root and storage
switched off; manual units are read from a new downloads folder. Nothing here writes to S3, and a stored
capture is never replaced: each unit ends with one write-once outcome.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import importlib
import io
import json
import os
import re
import shutil
import sys
import time
import traceback
import urllib.error
import urllib.parse
import zipfile
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from xml.etree import ElementTree

from defusedxml import ElementTree as SafeElementTree

from scripts.acquisition import bls_api_transport, data_paths
from scripts.acquisition import hud_xlsx_contract as download_metadata
from scripts.acquisition import redownload_controls as controls
from scripts.acquisition.data_paths import current
from scripts.acquisition.s3_store import StorageError, encoded_json, fingerprint, write_once
from scripts.acquisition.source_registry import REPO_ROOT, RegistryError, read_json, require
from scripts.acquisition.transport import CaptureError, Limits, download, known_failure

ACTIVE = ("api", "direct_download", "approved_manual")
BYTE_CAP = 128 * 2**30
BLS_LIMIT = 450
MAX_ATTEMPTS = 2
RETRYABLE_STATUS = {408, 500, 502, 503, 504}
RETRYABLE_FAILURE = {"URLError", "TimeoutError", "ConnectionResetError", "incomplete_read"}
FORMATS = {".csv": "csv", ".txt": "txt", ".zip": "zip", ".xlsx": "xlsx", ".xls": "xls", ".pdf": "pdf", ".json": "json", ".docx": "docx", ".gz": "gzip"}
PRIVATE_FLAG = "original_contains_personal_details"
MMD_API_FLAG = "browser_export_moved_to_api"
PRIVATE_COLLECTORS = {"cms_owners_org", "hcai_util_workbook", "onc_mu_hospital"}


Pause = controls.Pause


class Pending(ValueError):
    """A manual download is not in the folder yet; the unit keeps no outcome."""


@dataclass
class Run:
    """One harness invocation: the locked queue, the fresh state root and the injectable boundaries."""

    queue_path: Path
    state_root: Path
    base: Path = REPO_ROOT
    manual_folder: Path | None = None
    opener: Any = None
    adapters: dict[str, Callable[[dict, Run], Path]] | None = None
    api_request: Callable | None = None
    client_factory: Callable | None = None
    byte_cap: int = BYTE_CAP
    sleep: Callable = time.sleep
    cache: dict[str, Any] = field(default_factory=dict)

    def client(self) -> Any:
        """The project AWS client, created once and only for collectors that read an API key."""
        if "client" not in self.cache:
            if self.client_factory is not None:
                self.cache["client"] = self.client_factory()
            else:
                from scripts.acquisition.collect_bls_api import aws_runner
                from scripts.acquisition.s3_store import AwsCli
                from scripts.infrastructure.render_project_config import load_configuration

                verify_input(REPO_ROOT / ".env", self)
                self.cache["client"] = AwsCli(load_configuration(REPO_ROOT / ".env")[0], runner=aws_runner)
        return self.cache["client"]


def digest(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def load_queue(run: Run) -> list[dict]:
    """Return the queue only when it matches its lock and every active receipt is unchanged."""
    if "queue" not in run.cache:
        body = run.queue_path.read_bytes()
        lock = read_json(run.queue_path.with_name(run.queue_path.stem + ".lock.json"))
        require(digest(body) == lock["queue_sha256"], "Queue differs from its lock")
        document = json.loads(body)
        queue = document["queue"]
        require(len(queue) == lock["entries"] and len({unit["snapshot_id"] for unit in queue}) == len(queue), "Queue entries differ from its lock")
        for unit in queue:
            if unit["disposition"] in ACTIVE:
                require(digest((receipt_file(unit, run)).read_bytes()) == unit["receipt_sha256"], "Receipt changed since the queue was frozen")
        run.cache["queue"], run.cache["queue_sha256"] = queue, lock["queue_sha256"]
    return run.cache["queue"]


def runtime_controls(run: Run) -> dict:
    """Freeze operational inputs, including the reviewed code-version lists the collectors read, apart from status notes.

    The code-version lists are bound too (failure mode S16): a collector reads them while it runs, so a change after
    the review must invalidate it. New versions are therefore listed before the controls are frozen and reviewed.
    """
    queue = load_queue(run)
    code_paths = sorted((REPO_ROOT / "scripts/acquisition").rglob("*.py"))
    code_paths += [REPO_ROOT / "scripts/infrastructure/render_project_config.py", REPO_ROOT / "scripts/process.py", REPO_ROOT / "requirements.txt"]
    inputs = set((run.base / "config/acquisition").rglob("*.json"))
    from scripts.acquisition.collect_mmd_api import TEMPLATE

    template = run.base / TEMPLATE
    if template.is_file() or run.base == REPO_ROOT:
        inputs.add(template)
    if (run.base / ".env").is_file():
        inputs.add(run.base / ".env")
    document = read_json(run.queue_path)
    for relative in document.get("inputs_sha256", {}):
        # Status documentation is mutable; the immutable queue and this snapshot are the execution controls.
        if not relative.endswith(".md"):
            path = run.base / current(relative) if "/" in relative else run.base / "data/acquisition_planning/full_redownload_20260929" / relative
            require(digest(path.read_bytes()) == document["inputs_sha256"][relative], "Frozen queue input changed")
            inputs.add(path)
    for unit in queue:
        if unit["disposition"] in ACTIVE:
            receipt = receipt_file(unit, run)
            job = next((parent / "job.json" for parent in receipt.parents if (parent / "job.json").is_file()), None)
            if job is not None:
                inputs.add(job)
    scope = run_scope(run)
    return {
        "queue_sha256": run.cache["queue_sha256"],
        **({} if scope is None else {"scope_sources": scope}),
        "state_root": root_label(run),
        "code_sha256": {str(path.relative_to(REPO_ROOT)): digest(path.read_bytes()) for path in code_paths},
        "operational_sha256": {str(path.relative_to(run.base)): digest(path.read_bytes()) for path in sorted(inputs)},
        # The harness and the collectors resolve recorded paths through this map while they run (failure mode S46).
        "path_map_sha256": digest(data_paths.MAP_PATH.read_bytes()),
        "limits": {"byte_cap": BYTE_CAP, "bls_rolling_24h": BLS_LIMIT, "attempts_per_primary_page_probe": MAX_ATTEMPTS},
    }


def run_scope(run: Run) -> list[str] | None:
    """The run's approved sources from ``scope.json`` beside its queue (failure mode 52); None means the whole queue.

    The list is bound into the reviewed controls, so a change after review is refused (failure mode 53).
    """
    path = run.queue_path.with_name("scope.json")
    if not path.is_file():
        return None
    sources = read_json(path).get("sources")
    if not (isinstance(sources, list) and sources and all(isinstance(item, str) for item in sources)):
        raise RegistryError("Malformed run scope")
    require(sorted(set(sources)) == sources, "Run scope must be sorted and unique")
    queued = {unit["source_id"] for unit in load_queue(run)}
    require(set(sources) <= queued, "Run scope names a source outside the frozen queue")
    return list(sources)


def selected_sources(scope: list[str] | None, sources: set[str] | None) -> frozenset[str] | None:
    """No selection means the scope; a selection outside the scope is refused. Returns a private, unchangeable copy.

    The caller supplies the scope it trusts; the caller's own set is never used after this point (failure mode 59).
    """
    chosen = None if sources is None else frozenset(sources)
    if scope is None:
        return chosen
    if chosen is None:
        return frozenset(scope)
    require(chosen <= frozenset(scope), "Source selection is outside the run's approved scope")
    return chosen


def in_reviewed_scope(run: Run, source: str) -> bool:
    """Whether a source is inside the scope of the controls the review gate accepted; runs without a scope allow all."""
    scope = run.cache.get("controls", {}).get("scope_sources")
    return scope is None or source in scope


def preview_scope(run: Run) -> list[str] | None:
    """A preview uses the frozen controls' scope and refuses a scope file that differs from it (failure mode 57).

    Without frozen controls the scope file is read as is; the preview then reports its scope as unreviewed.
    """
    frozen_path = run_records(run)[0]
    if not frozen_path.is_file():
        return run_scope(run)
    frozen = read_json(frozen_path).get("scope_sources")
    require(run_scope(run) == frozen, "Run scope differs from the frozen controls")
    return frozen


def root_label(run: Run) -> str:
    """The state root as the frozen controls record it."""
    return str(run.state_root.absolute().relative_to(run.base if run.base == REPO_ROOT else run.base.parent))


def default_state_root(queue_path: Path) -> Path:
    """Each queue's run folder sits beside it, so choosing a queue never falls back to another run's state (failure mode 55)."""
    return queue_path.with_name("run")


RUN_1_QUEUE = "data/redownload_checks/20260929/queue.json"
RUN_1_REVIEW = "data/acquisition_planning/full_redownload_20260929/independent_review.json"


def run_records(run: Run) -> tuple[Path, Path]:
    """Each run's controls snapshot and independent review sit beside its own queue (failure modes 48-49).

    Run 1 predates this rule: its review stays at its original path, so its records are never moved or overwritten.
    """
    controls_path = run.queue_path.with_name("controls.json")
    if run.base == REPO_ROOT and run.queue_path.resolve() == (REPO_ROOT / RUN_1_QUEUE).resolve():
        return controls_path, REPO_ROOT / RUN_1_REVIEW
    return controls_path, run.queue_path.with_name("independent_review.json")


def review_gate(run: Run) -> str:
    """A separate passing review binds every runtime module, dependency, queue and operational input."""
    path, review_path = run_records(run)
    require(path.is_file(), "Execution controls need an independent review")
    actual = runtime_controls(run)
    require(read_json(path) == actual, "Execution controls changed; independent review required")
    sha = digest(encoded_json(actual))
    review = read_json(review_path)
    require(review.get("status") == "passed" and review.get("reviewed_controls_sha256") == sha, "Passing independent review missing or stale")
    require(run.byte_cap <= BYTE_CAP, "Byte cap exceeds approved limit")
    run.cache["controls"] = actual
    return sha


def verify_path_map(run: Run) -> None:
    """Refuse a path map changed since the gate, at each point where a recorded path is resolved (failure mode S46)."""
    expected = run.cache.get("controls", {}).get("path_map_sha256")
    if expected is not None:
        require(digest(data_paths.MAP_PATH.read_bytes()) == expected, "Path map changed")


def verify_input(path: Path, run: Run) -> None:
    """Refuse changes to the individual operational input at its point of use."""
    expected = run.cache.get("controls", {}).get("operational_sha256", {}).get(str(path.relative_to(run.base)))
    if expected is not None:
        require(digest(path.read_bytes()) == expected, "Operational input changed")


def validate_root(run: Run) -> None:
    """Enforce a fresh owned subtree and reject symlink traversal or overlap with historical captures."""
    root = run.state_root.absolute()
    approved = run.base / "data/redownload_checks" if run.base == REPO_ROOT else run.base.parent
    require(root.is_relative_to(approved.absolute()) and root != approved.absolute(), "State root outside approved redownload subtree")
    controls.validate_path(root, approved)
    for unit in load_queue(run):
        old = (receipt_file(unit, run)).parent.absolute()
        require(not root.is_relative_to(old) and not old.is_relative_to(root), "State root overlaps an original collection")
    if root.exists():
        require(not any(path.is_symlink() for path in root.rglob("*")), "Symlink in redownload staging path")
        require((root / "run.json").is_file() or not list(root.iterdir()), "Nonempty state root has no harness ownership record")


def started_at(run: Run) -> datetime:
    """Bind the state root to one queue and return when the run began; manual files must be newer."""
    run.state_root.mkdir(parents=True, mode=0o700, exist_ok=True)
    run.state_root.chmod(0o700)
    path = run.state_root / "run.json"
    if not path.exists():
        write_once(
            path,
            encoded_json(
                {"queue_sha256": run.cache["queue_sha256"], "controls_sha256": run.cache["controls_sha256"], "started_at_utc": datetime.now(UTC).isoformat()}
            ),
        )
    record = read_json(path)
    require(record["queue_sha256"] == run.cache["queue_sha256"], "State root belongs to another queue")
    require(record.get("controls_sha256") == run.cache["controls_sha256"], "State root belongs to another reviewed implementation")
    return datetime.fromisoformat(record["started_at_utc"])


def outcome(run: Run, snapshot_id: str) -> dict | None:
    path = run.state_root / "outcomes" / f"{snapshot_id}.json"
    return read_json(path) if path.exists() else None


def acknowledged(run: Run) -> set[str]:
    """Outcomes the user reviewed when resuming a source."""
    found: set[str] = set()
    for path in sorted((run.state_root / "resumes").glob("*.json")):
        found.update(read_json(path)["acknowledged"])
    return found


def stopped_sources(run: Run) -> set[str]:
    """Sources with an outcome other than an exact match that the user has not reviewed."""
    reviewed = acknowledged(run)
    stopped = set()
    for path in sorted((run.state_root / "outcomes").glob("*.json")):
        item = read_json(path)
        if item["outcome"] != "exact_match" and item["snapshot_id"] not in reviewed:
            stopped.add(item["source_id"])
    return stopped


def paused_sources(run: Run) -> set[str]:
    acknowledged_pauses = {identity for path in (run.state_root / "resumes").glob("*.json") for identity in read_json(path).get("pauses", [])}
    return {read_json(path)["source_id"] for path in sorted((run.state_root / "pauses").glob("*.json")) if path.name not in acknowledged_pauses}


def resume_source(run: Run, source_id: str, decision: str) -> None:
    """Record the user's review of a stopped source so its remaining units may run; outcomes are untouched."""
    require(bool(decision.strip()), "A resume needs the recorded decision")
    validate_root(run)
    run.cache["controls_sha256"] = review_gate(run)
    require(in_reviewed_scope(run, source_id), "Resume source is outside the run's approved scope")
    require((run.state_root / "run.json").is_file(), "Resume needs an existing harness-owned run")
    started_at(run)
    require(source_id in {unit["source_id"] for unit in load_queue(run)}, "Resume source outside the frozen queue")
    controls.validate_path(run.state_root / "resumes" / f"{source_id}.json", run.state_root)
    reviewed = acknowledged(run)
    pending = [
        item["snapshot_id"]
        for item in (read_json(path) for path in sorted((run.state_root / "outcomes").glob("*.json")))
        if item["source_id"] == source_id and item["outcome"] != "exact_match" and item["snapshot_id"] not in reviewed
    ]
    pauses = [path.name for path in sorted((run.state_root / "pauses").glob("*.json")) if read_json(path)["source_id"] == source_id]
    require(bool(pending or pauses), "Source has nothing to resume")
    count = len(list((run.state_root / "resumes").glob(f"{source_id}__*.json")))
    record = {"source_id": source_id, "decision": decision, "acknowledged": pending, "pauses": pauses, "recorded_at_utc": datetime.now(UTC).isoformat()}
    write_once(run.state_root / "resumes" / f"{source_id}__{count + 1:03}.json", encoded_json(record))


def bls_recent_requests(roots: list[Path], now: datetime) -> int:
    """Count counted BLS requests in the last 24 hours across the stored and fresh ledgers."""
    events = [read_json(path) for root in roots for path in sorted((root / "requests").glob("*.json"))]
    return sum(datetime.fromisoformat(event["reserved_at_utc"]) > now - timedelta(hours=24) for event in events)


def require_bls_budget(roots: list[Path], now: datetime, limit: int = BLS_LIMIT) -> None:
    if bls_recent_requests(roots, now) >= limit:
        raise Pause("BLS rolling 24-hour budget reached; resume after the oldest request expires")


def receipt_file(unit: dict, run: Run) -> Path:
    """The stored capture's receipt; queues frozen before the dataset move name its old folder [226]."""
    verify_path_map(run)
    return run.base / current(unit["receipt"])


def stored_receipt(unit: dict, run: Run) -> dict:
    return read_json(receipt_file(unit, run))


def lineage(unit: dict, run: Run) -> dict:
    return json.loads(stored_receipt(unit, run)["lineage"]["extraction_or_query"])


def collection_root(receipt: Path) -> Path:
    """The stored collection root: the folder that holds ``batches``."""
    return next(parent.parent for parent in receipt.parents if parent.name == "batches")


def is_private(unit: dict) -> bool:
    return any(flag.startswith(PRIVATE_FLAG) for flag in unit["flags"])


def record_private(run: Run, relative: str) -> None:
    """Keep the list of fresh originals that hold personal details, for the closeout deletion list."""
    path = run.state_root / "private_retention.json"
    originals = read_json(path)["originals"] if path.exists() else []
    originals = sorted(set([*originals, relative]))
    files = []
    for original in originals:
        folder = run.state_root / original
        for item in [folder, *folder.rglob("*")] if folder.is_dir() else [folder]:
            if item.exists():
                controls.validate_path(item, run.state_root)
                item.chmod(0o700 if item.is_dir() else 0o600)
                if item.is_file():
                    files.append({"path": str(item.relative_to(run.state_root)), "bytes": item.stat().st_size, "sha256": fingerprint(item)[0]})
    body = {"originals": originals, "files": files, "retention": "Delete after closeout verification, once the user confirms the exact list"}
    temporary = path.with_suffix(".tmp")
    temporary.write_bytes(encoded_json(body))
    os.replace(temporary, path)


def fetch(url: str, name: str, expected_format: str, role: str, size: int, folder: Path, run: Run) -> tuple[Any, int]:
    """Download one file with at most two attempts; HTTP 429 pauses instead of retrying."""
    limits = Limits(attempts=1, max_bytes=min(4 * 2**30, max(2 * size, 2**20)))
    result = None
    earlier = len(list(folder.glob("try_*"))) if folder.exists() else 0
    for attempt in range(1, MAX_ATTEMPTS + 1):
        result = download(url, folder / f"try_{earlier + attempt}", name, expected_format, role, limits, opener=run.opener)
        if result.http_status == 429:
            raise Pause("Publisher returned HTTP 429")
        retryable = result.http_status in RETRYABLE_STATUS or result.failure in RETRYABLE_FAILURE
        if result.complete or not retryable or attempt == MAX_ATTEMPTS:
            return result, attempt
        run.sleep(2**attempt)
    raise ValueError("No download attempt was made")


def compare_download(result: Any, attempts: int, stored_sha256: str, run: Run, stored: Path | None = None) -> dict:
    fields = {"attempts": attempts, "stored_sha256": stored_sha256, "fresh_sha256": result.sha256, "fresh_bytes": result.byte_count}
    fields["fresh_path"] = str(result.path.relative_to(run.state_root))
    if not result.complete:
        blocked = result.failure in {"size_limit", "html_instead_of_data", "unexpected_signed_response_media_type"}
        return fields | {"outcome": "blocked" if blocked else "unavailable", "reason": known_failure(result.failure)}
    if result.sha256 == stored_sha256 or stored is None or not stored.is_file():
        return fields | {"outcome": "exact_match" if result.sha256 == stored_sha256 else "changed_needs_review", "excluded_parts": []}
    differing, excluded = compare_files(stored, result.path, stored.name, stored_sha256)
    return fields | {"outcome": "changed_needs_review" if differing else "exact_match", "differing_data_files": differing, "excluded_parts": excluded}


def file_unit(unit: dict, run: Run) -> dict:
    """Fetch one published file and compare its bytes with the stored original."""
    artifact, expected_format, redirect = file_inputs(unit, run)
    if redirect:
        from scripts.acquisition.history_redirects import install_redirect

        install_redirect(unit["url"], redirect)
    private = is_private(unit)
    folder = run.state_root / ("private" if private else "files") / unit["snapshot_id"]
    if private:
        folder.mkdir(parents=True, mode=0o700, exist_ok=True)
        (run.state_root / "private").chmod(0o700)
    if private:
        record_private(run, str(folder.relative_to(run.state_root)))
    try:
        result, attempts = fetch(unit["url"], artifact["stored_file_name"], expected_format, artifact["role"], unit["bytes"], folder, run)
    finally:
        if private:
            record_private(run, str(folder.relative_to(run.state_root)))
    return compare_download(result, attempts, artifact["sha256"], run, receipt_file(unit, run).parent / artifact["storage_path"])


def page_unit(unit: dict, run: Run) -> dict:
    """Fetch every recorded API page of an unplanned paged capture and compare each page."""
    receipt = stored_receipt(unit, run)
    requests = json.loads(receipt["lineage"]["extraction_or_query"])["requests"]
    pages = [item for item in receipt["artifacts"] if item["role"] == "api_page"]
    require(len(requests) == len(pages) and bool(pages), "Recorded pages differ from the recorded requests")
    differing, attempts, total = [], 0, 0
    for index, (request, page) in enumerate(zip(requests, pages, strict=True), 1):
        folder = run.state_root / "files" / unit["snapshot_id"] / f"page_{index:04}"
        result, used = fetch(request["requested_url"], page["stored_file_name"], "json", "api_page", page["byte_count"], folder, run)
        attempts, total = max(attempts, used), total + result.byte_count
        if not result.complete:
            return compare_download(result, used, page["sha256"], run)
        if result.sha256 != page["sha256"]:
            differing.append(page["stored_file_name"])
    fields = {"attempts": attempts, "fresh_bytes": total, "differing_data_files": differing}
    return fields | {"outcome": "changed_needs_review" if differing else "exact_match"}


def archive_members(path: Path, expanded_cap: int = 2**30) -> dict[str, str] | None:
    """Member names and hashes of a zip archive; ``None`` for any other file."""
    if not zipfile.is_zipfile(path):
        return None
    require(path.stat().st_size <= 2**30, "ZIP compressed size budget exceeded")
    with zipfile.ZipFile(path) as archive:
        infos = archive.infolist()
        require(len(infos) <= 10000, "ZIP member count budget exceeded")
        require(len({info.filename for info in infos}) == len(infos), "Duplicate ZIP member")
        require(
            all(not Path(info.filename).is_absolute() and ".." not in Path(info.filename).parts and "\\" not in info.filename for info in infos),
            "Unsafe ZIP member",
        )
        require(
            sum(info.file_size for info in infos) <= expanded_cap and all(info.file_size <= expanded_cap for info in infos), "ZIP expansion budget exceeded"
        )
        members = {}
        for info in infos:
            sha, count = hashlib.sha256(), 0
            with archive.open(info) as handle:
                while block := handle.read(min(2**20, expanded_cap - count + 1)):
                    count += len(block)
                    require(count <= expanded_cap, "ZIP expansion budget exceeded")
                    sha.update(block)
            members[info.filename] = sha.hexdigest()
        return members


# Parts of a download that change with the download date, never with the published content [S17, S18]. One fixed format
# compares by content: an Office file's core properties. Every other file compares by bytes, saved web pages, Census
# table notes and WONDER exports included; a difference is flagged for review [S35] [S53].
# A file that starts with a byte-order mark compares by bytes, so the mark is never dropped [S52].
BYTE_ORDER_MARK = b"\xef\xbb\xbf"
# An Office file's core properties record when the publisher's generator built it: only the member path exactly
# docProps/core.xml, parsed, and only its root's own created and modified elements [S39] [S43].
OFFICE_CORE = "docProps/core.xml"
DCTERMS = "{http://purl.org/dc/terms/}"
# Only the Office core-properties root, with the prefixes the build-time elements use bound to their Office
# namespaces everywhere in the file; any other root or binding compares by bytes [S54].
CORE_PROPERTIES = "{http://schemas.openxmlformats.org/package/2006/metadata/core-properties}coreProperties"
OFFICE_BINDINGS = {
    "cp": "http://schemas.openxmlformats.org/package/2006/metadata/core-properties",
    "dcterms": "http://purl.org/dc/terms/",
    "xsi": "http://www.w3.org/2001/XMLSchema-instance",
}
XSI_TYPE = "{http://www.w3.org/2001/XMLSchema-instance}type"
STAMP_UTC = r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z"
BUILD_ELEMENT = re.compile(rf'<dcterms:(?P<kind>created|modified) xsi:type="dcterms:W3CDTF">(?P<stamp>{STAMP_UTC})</dcterms:(?P=kind)>')
# A stored copy that no longer has its receipt's bytes is never compared by content [S45].
STORED_DIFFERS = "(stored copy differs from its receipt)"
OFFICE_CORE_CAP = 2**20
# Larger files, and anything that is not strict UTF-8 text, compare by bytes [S22].
CONTENT_CAP = 256 * 2**20


def office_core_view(text: str, parts: set[str]) -> str | None:
    """The raw core properties with only the root's own created and modified times masked; ``None`` compares by bytes.

    Parsing (defusedxml) only confirms which elements carry the build time; the comparison keeps the raw text, so
    comments, processing instructions and namespace declarations still count [S43] [S44].
    """
    try:
        # defusedxml refuses entity declarations and external references before parsing.
        root = SafeElementTree.fromstring(text)
        bindings = [binding for _event, binding in SafeElementTree.iterparse(io.BytesIO(text.encode()), events=("start-ns",))]
    except (ElementTree.ParseError, ValueError):
        return None
    # Every declaration of the three prefixes, at any depth, must bind its Office namespace [S54].
    if root.tag != CORE_PROPERTIES or any(prefix in OFFICE_BINDINGS and uri != OFFICE_BINDINGS[prefix] for prefix, uri in bindings):
        return None
    if not set(OFFICE_BINDINGS) <= {prefix for prefix, _uri in bindings}:
        return None
    parsed = {}
    for kind in ("created", "modified"):
        found = [child for child in root if child.tag == f"{DCTERMS}{kind}"]
        if len(found) != 1 or len(found[0]) or not found[0].text or not re.fullmatch(STAMP_UTC, found[0].text):
            return None
        if found[0].attrib != {XSI_TYPE: "dcterms:W3CDTF"}:
            return None
        try:
            # A digit-shaped value that is not a real UTC time is content, not a build time [S53].
            datetime.strptime(found[0].text, "%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            return None
        parsed[kind] = found[0].text
    matches = list(BUILD_ELEMENT.finditer(text))
    # Each element's exact text must occur once, so the masked span is the parsed element and nothing else.
    if sorted(match["kind"] for match in matches) != ["created", "modified"] or any(match["stamp"] != parsed[match["kind"]] for match in matches):
        return None
    pieces, last = [], 0
    for match in matches:
        pieces += [text[last : match.start("stamp")], "<excluded>"]
        last = match.end("stamp")
    parts.add("document_build_time")
    return "".join(pieces) + text[last:]


def content_view(body: bytes | None, name: str) -> tuple[str, set[str]] | None:
    """The comparable content of an Office file's core properties and the named parts masked in it.

    ``None`` compares by bytes: every other file, saved web pages, Census table notes and WONDER exports included [S35] [S53].
    """
    if body is None or name != OFFICE_CORE or len(body) > OFFICE_CORE_CAP or body.startswith(BYTE_ORDER_MARK):
        return None
    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError:
        return None
    parts: set[str] = set()
    view = office_core_view(text, parts)
    return None if view is None else (view, parts)


def same_content(stored: bytes | None, fresh: bytes | None, name: str) -> tuple[bool, list[str]]:
    """Whether two downloads of one file publish the same content, and the named parts left out to decide it [S19]."""
    if stored is not None and stored == fresh:
        return True, []
    old, new = content_view(stored, name), content_view(fresh, name)
    if old is None or new is None or old[0] != new[0]:
        return False, []
    return True, sorted(old[1] | new[1])


def capped_body(path: Path) -> bytes | None:
    return path.read_bytes() if path.stat().st_size <= CONTENT_CAP else None


def member_body(path: Path, member: str) -> bytes | None:
    with zipfile.ZipFile(path) as archive:
        info = archive.getinfo(member)
        return archive.read(info) if info.file_size <= CONTENT_CAP else None


def archive_comments(path: Path) -> dict[str, bytes]:
    """The archive comment and each member's comment, which can carry notes or footnotes [S28]."""
    with zipfile.ZipFile(path) as archive:
        # Every entry, directories included [S36].
        members = {f"{info.filename} (comment)": info.comment for info in archive.infolist()}
        return {"(archive comment)": archive.comment} | members


ARCHIVE_SUFFIXES = frozenset({".zip", ".xlsx", ".docx"})


def is_archive(path: Path, name: str) -> bool:
    """Only a file named as a zip, workbook or document that begins with a zip local-file header is an archive [S41]."""
    with path.open("rb") as handle:
        return Path(name).suffix.lower() in ARCHIVE_SUFFIXES and handle.read(4) == b"PK\x03\x04"


def compare_files(stored: Path, fresh: Path, name: str, stored_sha256: str) -> tuple[list[str], list[str]]:
    """The differing file, member or comment names and the named parts left out, for two downloads of one file [S17].

    Archives compare member by member, since a publisher may rebuild an export archive on every request; their
    comments compare by bytes.
    """
    if fingerprint(stored)[0] != stored_sha256:
        return [STORED_DIFFERS], []
    if fingerprint(fresh)[0] == stored_sha256:
        return [], []
    old, new = (archive_members(path) if is_archive(path, name) else None for path in (stored, fresh))
    if old is None or new is None:
        same, excluded = same_content(capped_body(stored), capped_body(fresh), name)
        return ([] if same else [name]), excluded
    differing, parts = sorted(set(old) ^ set(new)), set()
    for member in sorted(set(old) & set(new)):
        if old[member] == new[member]:
            continue
        same, excluded = same_content(member_body(stored, member), member_body(fresh, member), member)
        if same:
            parts.update(excluded)
        else:
            differing.append(member)
    old_comments, new_comments = archive_comments(stored), archive_comments(fresh)
    differing += [key for key in sorted(set(old_comments) | set(new_comments)) if old_comments.get(key, b"") != new_comments.get(key, b"")]
    return sorted(differing), sorted(parts)


def same_address(origin: str, url: str) -> bool:
    """A recorded download origin is the unit's own address; only the fragment and a ``;jsessionid=`` segment are left out [S32]."""

    def key(value: str) -> tuple[str, str | None, int | None, str, str]:
        parts = urllib.parse.urlsplit(value)
        # Only an absent port takes the scheme default; an explicit port, 0 included, compares as written [S37].
        port = parts.port if parts.port is not None else {"http": 80, "https": 443}.get(parts.scheme.lower())
        return parts.scheme.lower(), parts.hostname, port, re.sub(r";jsessionid=[^/?#;]*", "", parts.path, flags=re.IGNORECASE), parts.query

    return key(origin) == key(url)


def manual_file_unit(unit: dict, run: Run, started: datetime) -> dict:
    """Accept a browser download only when it is new and its recorded origin is the unit's own address [S29].

    A file with the same content wins, whatever its download date [S17]; otherwise the first changed file from the
    unit's address is kept and recorded as changed for review [S20].
    """
    receipt_path = receipt_file(unit, run)
    artifact = read_json(receipt_path)["artifacts"][0]
    stored = receipt_path.parent / artifact["storage_path"]
    folder = run.manual_folder
    if folder is None:
        raise ValueError("Manual units need the manual downloads folder")

    def keep(path: Path, fields: dict) -> dict:
        target = run.state_root / "files" / unit["snapshot_id"] / path.name
        controls.writing(target, path.stat().st_size)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, target)
        return fields | {"fresh_path": str(target.relative_to(run.state_root))}

    changed: tuple[Path, dict] | None = None
    for path in sorted(item for item in folder.iterdir() if item.is_file() and not item.is_symlink()) if folder.is_dir() else []:
        if datetime.fromisoformat(download_metadata.download_created_at(path)) < started:
            continue
        require(path.stat().st_size <= min(run.byte_cap, 4 * 2**30), "Manual file exceeds byte budget")
        try:
            origins = download_metadata.read_origin(path)
        except ValueError:
            continue
        if not any(same_address(origin, unit["url"]) for origin in origins):
            continue
        differing, excluded = compare_files(stored, path, artifact["stored_file_name"], artifact["sha256"])
        if differing and changed is not None:
            continue
        fields = {"attempts": 1, "stored_sha256": artifact["sha256"], "fresh_sha256": fingerprint(path)[0], "fresh_bytes": path.stat().st_size}
        fields |= {"differing_data_files": differing, "excluded_parts": excluded}
        if not differing:
            return keep(path, fields | {"outcome": "exact_match"})
        changed = (path, fields | {"outcome": "changed_needs_review"})
    if changed is not None:
        return keep(*changed)
    raise Pending("Manual download not in the folder yet")


def manual_file(downloads: Path | None, name: str, started: datetime) -> Path:
    """A collector's manual download must exist in the new folder and be newer than the run."""
    if downloads is None:
        raise ValueError("Manual units need the manual downloads folder")
    path = downloads / name
    if path.is_symlink() or not path.is_file() or datetime.fromisoformat(download_metadata.download_created_at(path)) < started:
        raise Pending("Manual download not in the folder yet")
    return path


# Plan-based collectors: module with the locked plan, its loader, the planned group and the receipt's lineage key.
PLANNED = {
    "bls_api_v2": ("bls_api_contract", "load_plan", "batches", "batch_id"),
    "census_acs_api": ("census_acs_api_contract", "load_plans", "batches", "batch_id"),
    "census_acs_detailed": ("census_acs_detailed_contract", "load_plan", "batches", "batch_id"),
    "hud_api": ("hud_api_contract", "load_plans", "batches", "batch_id"),
    "hud_xlsx": ("hud_xlsx_contract", "load_plan", "batches", "batch_id"),
    "wonder_export": ("wonder_export_contract", "load_plan", "batches", "batch_id"),
    "cms_owners_org": ("cms_owners_contract", "load_plan", "releases", "release_id"),
    "hcai_util_workbook": ("hcai_util_contract", "load_plan", "years", "year_id"),
    "onc_mu_hospital": ("onc_mu_contract", "load_plan", None, "file_id"),
}


def planned(unit: dict, run: Run) -> tuple[dict, dict]:
    """Return the locked plan and the single planned item that produced a stored capture."""
    module_name, loader, group, key = PLANNED[unit["mode"]]
    verify_path_map(run)
    for relative in run.cache.get("controls", {}).get("operational_sha256", {}):
        if relative.startswith("config/acquisition/"):
            verify_input(run.base / relative, run)
    if f"plans:{module_name}" not in run.cache:
        # Loading a plan re-validates the registry, so each locked plan is loaded once per invocation.
        loaded = getattr(importlib.import_module(f"scripts.acquisition.{module_name}"), loader)()
        run.cache[f"plans:{module_name}"] = loaded if isinstance(loaded, list) else [loaded]
    plans = run.cache[f"plans:{module_name}"]
    identity = lineage(unit, run)[key]
    matches = [(plan, plan if group is None else item) for plan in plans for item in ([plan] if group is None else plan[group]) if item["id"] == identity]
    require(len(matches) == 1, "Stored capture does not match exactly one planned item")
    return matches[0]


def mmd_selection(unit: dict, run: Run) -> tuple[str, int]:
    """The measure and year of an MMD capture; a browser export is matched by its recorded condition, domain and year."""
    from scripts.acquisition.mmd_api_contract import PLANS, condition_for, load_plan, plan_for

    receipt = stored_receipt(unit, run)
    detail = json.loads(receipt["lineage"]["extraction_or_query"])
    if unit["mode"] == "mmd_api":
        measure, year = detail["measure_id"], int(detail["year"])
    else:
        chosen = receipt["acquisition"]["export_selections"]
        labels = (chosen["condition"], chosen["domain"])
        if "mmd_conditions" not in run.cache:
            run.cache["mmd_conditions"] = [item for sha in PLANS for item in load_plan(sha)["conditions"]]
        conditions = run.cache["mmd_conditions"]
        matches = [item["measure_id"] for item in conditions if (item["condition_label"], item["domain_label"]) == labels]
        require(len(matches) == 1 and chosen["geography"] == "County", "MMD browser export does not match exactly one planned county condition")
        measure, year = matches[0], int(chosen["year"])
    if f"mmd_plan:{measure}" not in run.cache:
        run.cache[f"mmd_plan:{measure}"] = plan_for(measure)[1]
    condition_for(run.cache[f"mmd_plan:{measure}"], measure, year)
    return measure, year


def fresh_root(unit: dict, run: Run) -> Path:
    return run.state_root / "collectors" / unit["mode"]


def adapt_bls(unit: dict, run: Run) -> Path:
    from scripts.acquisition import bls_api_transport as transport
    from scripts.acquisition import collect_bls_api as collector

    plan, batch = planned(unit, run)
    require_bls_budget([collection_root(receipt_file(unit, run)), fresh_root(unit, run)], datetime.now(UTC))
    try:
        result = collector.execute(plan, batch, fresh_root(unit, run), True, False, run.client(), {}, run.api_request or transport.request_live)
    except transport.QuotaPause as pause:
        raise Pause(str(pause)) from None
    return Path(result["receipt_path"])


def adapt_census_api(unit: dict, run: Run) -> Path:
    from scripts.acquisition import census_acs_api_transport as transport
    from scripts.acquisition import collect_census_acs_api as collector

    plan, batch = planned(unit, run)
    request = run.api_request or transport.request_live
    return Path(collector.execute(plan, batch, fresh_root(unit, run), True, False, run.client(), {}, request)["receipt_path"])


def adapt_census_detailed(unit: dict, run: Run) -> Path:
    from scripts.acquisition import collect_census_acs_detailed as collector

    plan, batch = planned(unit, run)
    request = run.api_request or collector.live_request
    return Path(collector.execute(plan, batch, fresh_root(unit, run), True, False, run.client(), {}, request)["receipt_path"])


def adapt_hud_api(unit: dict, run: Run) -> Path:
    from scripts.acquisition import collect_hud_api as collector
    from scripts.acquisition import hud_api_transport as transport

    plan, batch = planned(unit, run)
    request = run.api_request or transport.request_live
    return Path(collector.execute(plan, batch, fresh_root(unit, run), True, False, run.client(), {}, request)["receipt_path"])


def adapt_mmd(unit: dict, run: Run) -> Path:
    from scripts.acquisition import collect_mmd_api as collector

    measure, year = mmd_selection(unit, run)
    verify_input(run.base / collector.TEMPLATE, run)
    return Path(collector.execute(measure, year, True, False, root=run.state_root / "collectors" / "mmd_api")["receipt_path"])


def adapt_hud_xlsx(unit: dict, run: Run) -> Path:
    from scripts.acquisition import store_hud_xlsx as collector

    plan, batch = planned(unit, run)
    downloads = manual_file(run.manual_folder, batch["file_name"], run.cache["started_at"]).parent
    return Path(collector.execute(plan, batch, fresh_root(unit, run), downloads, False, None, {})["receipt_path"])


def adapt_wonder(unit: dict, run: Run) -> Path:
    from scripts.acquisition import store_wonder_export as collector
    from scripts.acquisition import wonder_export_contract as contract

    plan, batch = planned(unit, run)
    original = manual_file(run.manual_folder, batch["file_name"], run.cache["started_at"])
    if fingerprint(original) == (batch["sha256"], batch["bytes"]):
        return Path(collector.execute(plan, batch, fresh_root(unit, run), original.parent, False, None, {})["receipt_path"])
    # Every WONDER export records its own query time, so a fresh export never has the planned bytes. The collector's
    # origin and export checks run on it here, and the comparison with the stored export decides the outcome [S21].
    contract.sanitize_origins(download_metadata.read_origin(original), batch["database"])
    raw = original.read_bytes()
    derived, _ = contract.validate_export(raw, batch, plan)
    folder = fresh_root(unit, run) / "batches" / batch["id"] / "comparison"
    files = {f"raw/{contract.stored_name(batch)}": raw, "derived/county_year.csv": derived}
    for relative, body in files.items():
        controls.writing(folder / relative, len(body))
        write_once(folder / relative, body)
    artifacts = [{"role": "data", "stored_file_name": Path(name).name, "storage_path": name, "sha256": digest(body)} for name, body in files.items()]
    record = folder / "receipt.json"
    write_once(record, encoded_json({"kind": "redownload_comparison_record", "artifacts": artifacts}))
    return record


def adapt_cms_owners(unit: dict, run: Run) -> Path:
    from scripts.acquisition import store_cms_owners as collector

    plan, release = planned(unit, run)
    limits = Limits(attempts=MAX_ATTEMPTS)
    return Path(collector.execute(plan, release, fresh_root(unit, run), True, False, None, {}, run.opener, limits)["receipt_path"])


def adapt_hcai_util(unit: dict, run: Run) -> Path:
    from scripts.acquisition import store_hcai_util as collector

    plan, entry = planned(unit, run)
    limits = Limits(attempts=MAX_ATTEMPTS)
    return Path(collector.execute(plan, entry, fresh_root(unit, run), True, False, None, {}, run.opener, limits)["receipt_path"])


def adapt_onc_mu(unit: dict, run: Run) -> Path:
    from scripts.acquisition import store_onc_mu as collector

    plan, _ = planned(unit, run)
    # The plan raises the time and size bounds for this one large file; only the attempt count is lowered.
    limits = Limits(attempts=MAX_ATTEMPTS, timeout_seconds=120, max_seconds=plan["limits"]["max_seconds"], max_bytes=plan["limits"]["max_bytes"])
    return Path(collector.execute(plan, fresh_root(unit, run), True, False, None, {}, run.opener, limits)["receipt_path"])


ADAPTERS: dict[str, Callable[[dict, Run], Path]] = {
    "bls_api_v2": adapt_bls,
    "census_acs_api": adapt_census_api,
    "census_acs_detailed": adapt_census_detailed,
    "hud_api": adapt_hud_api,
    "mmd_api": adapt_mmd,
    "hud_xlsx": adapt_hud_xlsx,
    "wonder_export": adapt_wonder,
    "cms_owners_org": adapt_cms_owners,
    "hcai_util_workbook": adapt_hcai_util,
    "onc_mu_hospital": adapt_onc_mu,
}


# Most specific first; a category is always a value from this table, never a run-time name.
ERROR_CATEGORIES: tuple[tuple[type[BaseException], str], ...] = (
    (StorageError, "storage_contract"),
    (CaptureError, "capture_contract"),
    (RegistryError, "registry_contract"),
    (bls_api_transport.QuotaPause, "bls_quota"),
    (bls_api_transport.RetryableRequest, "retryable_request"),
    (json.JSONDecodeError, "json_decode"),
    (UnicodeDecodeError, "text_decode"),
    (zipfile.BadZipFile, "bad_zip"),
    (KeyError, "missing_key"),
    (FileNotFoundError, "file_not_found"),
    (PermissionError, "permission"),
    (TimeoutError, "timeout"),
    (urllib.error.URLError, "network"),
    (OSError, "os_error"),
    (ValueError, "validation"),
)


def error_category(error: BaseException) -> str:
    return next((category for kind, category in ERROR_CATEGORIES if isinstance(error, kind)), "other")


def diagnostic(error: BaseException, run: Run) -> dict:
    """Fixed error categories and reviewed code locations only: no exception text, source text or run-time names."""
    categories: list[str] = []
    seen: set[int] = set()
    current: BaseException | None = error
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        categories.append(error_category(current))
        current = current.__cause__ or current.__context__
    reviewed = run.cache.get("controls", {}).get("code_sha256", {})
    by_path = {(REPO_ROOT / relative).resolve(): relative for relative in reviewed}
    frames, unverified = [], 0
    for frame in traceback.extract_tb(error.__traceback__):
        relative = by_path.get(Path(frame.filename).resolve())
        # Hashed at each failure, so a file changed after review counts as unverified.
        if relative is None or not ((REPO_ROOT / relative).is_file() and digest((REPO_ROOT / relative).read_bytes()) == reviewed[relative]):
            unverified += 1
            continue
        # The path string comes from the reviewed snapshot, not from the frame; line numbers are integers.
        frames.append({"file": relative, "first_line": int(frame.lineno or 0), "last_line": int(frame.end_lineno or frame.lineno or 0)})
    return {"error_categories": categories, "frames": frames, "unverified_frames": unverified}


def record_diagnostic(run: Run, identity: str, error: Exception) -> None:
    """Keep the failure's type and code location in an owner-only local file; outcomes carry only fixed codes."""
    folder = run.state_root / "private" / "diagnostics"
    folder.mkdir(parents=True, mode=0o700, exist_ok=True)
    folder.chmod(0o700)
    path = folder / f"{identity}__{len(list(folder.glob(f'{identity}__*.json'))) + 1:02}.json"
    write_once(path, encoded_json(diagnostic(error, run)))
    path.chmod(0o600)
    # Diagnostics are deleted with the private originals at closeout, once the user confirms the exact list.
    record_private(run, str(folder.relative_to(run.state_root)))


def collector_unit(unit: dict, run: Run, adapter: Callable[[dict, Run], Path]) -> dict:
    """Run a collector's capture path in the fresh root and compare its derived data with the stored data."""
    receipt = receipt_file(unit, run)
    original = [item for item in read_json(receipt)["artifacts"] if item["role"] == "data"]
    stored = {(item["role"], item["stored_file_name"]): item["sha256"] for item in original}
    stored_paths = {(item["role"], item["stored_file_name"]): receipt.parent / item["storage_path"] for item in original if "storage_path" in item}
    require(len(stored) == len(original) and bool(stored), "Stored data artifact identities are empty or duplicated")
    if any(flag.startswith(MMD_API_FLAG) for flag in unit["flags"]):
        measure, year = mmd_selection(unit, run)
        name = f"mmd_ffs_county_{measure.lower().replace('.', '_')}_prevalence_{year}.csv"
        approved_names = {name}
        if measure == "C258.01":
            if year == 2023:
                approved_names.add("mmd_data.csv")
            elif 2012 <= year <= 2022:
                approved_names.add(f"mmd_ffs_county_ami_prevalence_{year}.csv")
        require(len(original) == 1 and original[0]["stored_file_name"] in approved_names, "MMD browser artifact differs from the reviewed mapping")
        stored = {("data", name): original[0]["sha256"]}
        stored_paths = {("data", name): receipt.parent / original[0]["storage_path"]} if "storage_path" in original[0] else {}
    try:
        receipt_path = adapter(unit, run)
    except (Pause, Pending, controls.BudgetExceeded):
        raise
    except (ValueError, OSError, KeyError) as error:
        record_diagnostic(run, unit["snapshot_id"], error)
        return {"attempts": 1, "outcome": "blocked", "reason": "collector_validation_failed", "error_category": error_category(error)}
    finally:
        # These collectors keep the publisher's original, which names people, in an owner-only folder under their root.
        if unit["mode"] in PRIVATE_COLLECTORS and (fresh_root(unit, run) / "private_original").exists():
            record_private(run, str((fresh_root(unit, run) / "private_original").relative_to(run.state_root)))
    controls.validate_path(receipt_path, run.state_root)
    fresh = [item for item in read_json(receipt_path)["artifacts"] if item["role"] == "data"]
    identities = {(item["role"], item["stored_file_name"]): item["sha256"] for item in fresh}
    fresh_paths = {(item["role"], item["stored_file_name"]): receipt_path.parent / item["storage_path"] for item in fresh if "storage_path" in item}
    differing, excluded = [], set()
    for key in sorted(set(stored) | set(identities)):
        if stored.get(key) == identities.get(key):
            continue
        # A data file present on both sides may differ only in its download date [S17].
        if key in stored_paths and key in fresh_paths and stored_paths[key].is_file() and fresh_paths[key].is_file():
            controls.validate_path(fresh_paths[key], run.state_root)
            names, parts = compare_files(stored_paths[key], fresh_paths[key], key[1], stored[key])
            if not names:
                excluded.update(parts)
                continue
        differing.append(key[1])
    same = not differing and set(identities) == set(stored) and len(identities) == len(fresh) and bool(stored)
    fields = {"attempts": 1, "fresh_receipt": str(receipt_path.absolute().relative_to(run.state_root.absolute())), "differing_data_files": differing}
    return fields | {"outcome": "exact_match" if same else "changed_needs_review", "excluded_parts": sorted(excluded)}


def handle(unit: dict, run: Run) -> dict:
    adapters = ADAPTERS if run.adapters is None else run.adapters
    mode = handler_mode(unit)
    if mode in adapters:
        return collector_unit(unit, run, adapters[mode])
    if unit["disposition"] == "approved_manual":
        return manual_file_unit(unit, run, run.cache["started_at"])
    if mode == "il_directory":
        return page_unit(unit, run)
    require(mode == "file", "Unit has no reviewed handler")
    return file_unit(unit, run)


def handler_mode(unit: dict) -> str:
    return "mmd_api" if any(flag.startswith(MMD_API_FLAG) for flag in unit["flags"]) else unit["mode"]


def file_inputs(unit: dict, run: Run) -> tuple[dict, str, str | None]:
    """The stored artifact, its reviewed format and any reviewed unsigned redirect for a file unit."""
    path = receipt_file(unit, run)
    artifact = read_json(path)["artifacts"][0]
    job = next((parent / "job.json" for parent in path.parents if (parent / "job.json").exists()), None)
    if job:
        verify_input(job, run)
    plan = read_json(job) if job else {}
    expected_format = plan.get("plan", {}).get("expected_format") or FORMATS.get(Path(artifact["stored_file_name"]).suffix.lower())
    if expected_format is None:
        raise ValueError("Stored file has no reviewed format")
    return artifact, expected_format, plan.get("candidate", {}).get("reviewed_unsigned_redirect")


def preflight(run: Run) -> dict:
    """Resolve every active unit's handler and planned inputs offline; any unresolved unit stops the run."""
    adapters = ADAPTERS if run.adapters is None else run.adapters
    counts: Counter[str] = Counter()
    for unit in (item for item in load_queue(run) if item["disposition"] in ACTIVE):
        mode = handler_mode(unit)
        if mode == "mmd_api":
            mmd_selection(unit, run)
            counts["collector:mmd_api"] += 1
        elif mode in adapters:
            if mode in PLANNED:
                planned(unit, run)
            counts[f"collector:{mode}"] += 1
        elif unit["disposition"] == "approved_manual":
            counts["manual_file"] += 1
        elif mode == "il_directory":
            counts["paged_api"] += 1
        else:
            require(mode == "file", "Unit has no reviewed handler")
            file_inputs(unit, run)
            counts["file"] += 1
    return {"resolved": sum(counts.values()), "handlers": dict(sorted(counts.items())), "requests_made": 0}


def used_bytes(run: Run) -> int:
    if "budget_used" not in run.cache:
        run.cache["budget_used"] = controls.charged(run.state_root)
    return run.cache["budget_used"]


def execute(
    run: Run, pilot: bool = False, allow_network: bool = True, sources: set[str] | None = None, max_units: int | None = None, log: Callable | None = None
) -> dict:
    """Process queue units in order; pilots are the first unit of each source, route and mode."""
    if not allow_network:
        require_readable_root(run)
        return execute_locked(run, pilot, False, selected_sources(preview_scope(run), sources), max_units, log)
    validate_root(run)
    run.cache["controls_sha256"] = review_gate(run)
    # The scope comes only from the controls the gate just accepted, never from a separate file read (failure mode 56).
    chosen = selected_sources(run.cache["controls"].get("scope_sources"), sources)
    # Bind ownership before opening the lock; a valid resume carries exactly the same review snapshot.
    run.cache["started_at"] = started_at(run)
    with (run.state_root / ".run.lock").open("a") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ValueError("Redownload state root already running") from None
        try:
            return execute_locked(run, pilot, True, chosen, max_units, log)
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def execute_locked(run: Run, pilot: bool, allow_network: bool, sources: frozenset[str] | None, max_units: int | None, log: Callable | None) -> dict:
    """Dispatch under one root lock, honoring persisted source pauses and every transport budget."""
    queue = load_queue(run)
    active = [unit for unit in queue if unit["disposition"] in ACTIVE]
    pilots: dict[tuple, str] = {}
    for unit in active:
        pilots.setdefault((unit["source_id"], unit["route"], unit["mode"]), unit["snapshot_id"])
    summary: dict[str, Any] = {"pilot": pilot, "active_units": len(active), "attempted": 0, "outcomes": {}, "paused": [], "pending_manual": 0}
    if not allow_network:
        done = {path.stem for path in (run.state_root / "outcomes").glob("*.json")} if run.state_root.exists() else set()
        chosen = [unit for unit in active if sources is None or unit["source_id"] in sources]
        reviewed_scope = run_records(run)[0].is_file()
        return summary | {"planned": sum(unit["snapshot_id"] not in done for unit in chosen), "scope_reviewed": reviewed_scope}
    reviewed, stopped, paused = acknowledged(run), stopped_sources(run), paused_sources(run)
    applicable = [identity for (source, _route, _mode), identity in pilots.items() if (sources is None or source in sources) and in_reviewed_scope(run, source)]
    stage_ready = all((item := outcome(run, identity)) is not None and (item["outcome"] == "exact_match" or identity in reviewed) for identity in applicable)
    summary["pilot_stage_pending"] = not stage_ready
    for unit in active:
        source, identity = unit["source_id"], unit["snapshot_id"]
        if outcome(run, identity) is not None or source in stopped or source in paused or (sources is not None and source not in sources):
            continue
        # Checked again for every unit at dispatch time, against the accepted controls only (failure mode 59).
        if not in_reviewed_scope(run, source):
            continue
        pilot_id = pilots[(source, unit["route"], unit["mode"])]
        if identity != pilot_id:
            first = outcome(run, pilot_id)
            if pilot or not stage_ready or first is None or (first["outcome"] != "exact_match" and pilot_id not in reviewed):
                continue
        if max_units is not None and summary["attempted"] >= max_units:
            break
        require(used_bytes(run) + unit["bytes"] <= run.byte_cap, "Byte cap would be exceeded")
        require(shutil.disk_usage(run.state_root).free > 2 * unit["bytes"] + 2**30, "Not enough free disk space for the next unit")
        try:
            boundary = controls.Boundary(run.state_root, run.base, identity, run.byte_cap, used=used_bytes(run))
            try:
                with controls.active(boundary):
                    fields = handle(unit, run)
            finally:
                run.cache["budget_used"] = boundary.used
        except Pending:
            summary["pending_manual"] += 1
            continue
        except Pause as pause:
            count = len(list((run.state_root / "pauses").glob("*.json")))
            delay = re.search(r"retry_after_seconds=([0-9]+)", str(pause))
            record = {
                "source_id": source,
                "snapshot_id": identity,
                "reason": "publisher_or_quota_pause",
                "retry_after_seconds": min(int(delay[1]), 86400) if delay else 0,
                "paused_at_utc": datetime.now(UTC).isoformat(),
            }
            write_once(run.state_root / "pauses" / f"{count + 1:06}.json", encoded_json(record))
            paused.add(source)
            summary["paused"].append(source)
            continue
        except controls.BudgetExceeded as failure:
            fields = {"outcome": "blocked", "reason": "byte_budget_exhausted" if "Byte cap" in str(failure) else "attempt_or_disk_budget_exhausted"}
        record = {"snapshot_id": identity, "source_id": source, "route": unit["route"], "mode": unit["mode"], "recorded_at_utc": datetime.now(UTC).isoformat()}
        write_once(run.state_root / "outcomes" / f"{identity}.json", encoded_json(record | fields))
        summary["attempted"] += 1
        summary["outcomes"][fields["outcome"]] = summary["outcomes"].get(fields["outcome"], 0) + 1
        if fields["outcome"] != "exact_match":
            stopped.add(source)
        if log is not None:
            log({"snapshot_id": identity, "source_id": source, "outcome": fields["outcome"]})
    return summary


def require_run_binding(run: Run) -> None:
    """The state root must belong to the chosen queue's own frozen controls (failure mode 61).

    Run 1 and run 2 share byte-identical queues, so the queue hash alone cannot tell them apart: the ownership
    record's controls hash must equal the canonical hash of the controls beside this queue, which bind this root.
    """
    frozen_path = run_records(run)[0]
    require(frozen_path.is_file(), "The chosen queue has no frozen controls")
    frozen = read_json(frozen_path)
    record = read_json(run.state_root / "run.json")
    require(record.get("controls_sha256") == digest(encoded_json(frozen)), "State root belongs to another run's controls")
    require(frozen.get("state_root") == root_label(run), "The chosen queue's controls bind a different state root")
    load_queue(run)
    require(record.get("queue_sha256") == run.cache["queue_sha256"], "State root belongs to another queue")


def require_readable_root(run: Run) -> None:
    """Persisted state is read only from the root the chosen queue's frozen controls bind (failure modes 62-63).

    An absent root has nothing to read and is never created here. An existing root must prove ownership; without
    frozen controls nothing can, so an existing ownership record is refused.
    """
    frozen_path = run_records(run)[0]
    if frozen_path.is_file():
        require(read_json(frozen_path).get("state_root") == root_label(run), "The chosen queue's controls bind a different state root")
    if not run.state_root.exists():
        return
    if (run.state_root / "run.json").is_file():
        require_run_binding(run)
    else:
        require(not any(run.state_root.iterdir()), "Nonempty state root has no harness ownership record")


def write_report(run: Run) -> tuple[dict, str]:
    """Write a dated report of the run's outcomes under its own state root."""
    validate_root(run)
    require((run.state_root / "run.json").is_file(), "Report needs an existing harness-owned run")
    document = report(run)
    name = datetime.now(UTC).strftime("report_%Y%m%dT%H%M%SZ.json")
    write_once(run.state_root / "reports" / name, encoded_json(document))
    return document, name


def report(run: Run) -> dict:
    """Account for every active unit from the outcome files alone, after the root proves it belongs to this run."""
    require_readable_root(run)
    queue = load_queue(run)
    active = [unit for unit in queue if unit["disposition"] in ACTIVE]
    results = {unit["snapshot_id"]: outcome(run, unit["snapshot_id"]) for unit in active}
    missing = [unit for unit in active if results[unit["snapshot_id"]] is None]
    return {
        "kind": "bounded_redownload_report",
        "queue_sha256": run.cache["queue_sha256"],
        "active_units": len(active),
        "outcomes": dict(sorted(Counter(item["outcome"] for item in results.values() if item is not None).items())),
        "not_exact": sorted(
            f"{item['source_id']}:{item['snapshot_id']}:{item['outcome']}" for item in results.values() if item and item["outcome"] != "exact_match"
        ),
        "matched_with_excluded_parts": dict(
            sorted(Counter(part for item in results.values() if item and item["outcome"] == "exact_match" for part in item.get("excluded_parts", [])).items())
        ),
        "not_attempted": sorted(unit["snapshot_id"] for unit in missing),
        "pending_manual": sorted(unit["snapshot_id"] for unit in missing if unit["disposition"] == "approved_manual"),
        "stopped_sources": sorted(stopped_sources(run)) if run.state_root.exists() else [],
        "paused_sources": sorted(paused_sources(run) & {unit["source_id"] for unit in missing}) if run.state_root.exists() else [],
        "fresh_bytes": used_bytes(run),
        "byte_measurement": "Conservative received bytes plus persisted copies; interrupted reservations remain charged; control ledgers excluded",
        "s3_writes": 0,
        "model_eligible": False,
    }


def main() -> None:
    """Command line: a dry run by default; ``--execute`` makes requests; ``--report`` writes a dated report."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--queue", type=Path, default=REPO_ROOT / "data/redownload_checks/20260929/queue.json")
    parser.add_argument("--state-root", type=Path, help="defaults to the run folder beside the queue")
    parser.add_argument("--manual-folder", type=Path)
    parser.add_argument("--pilot", action="store_true")
    parser.add_argument("--source", action="append")
    parser.add_argument("--max-units", type=int)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--report", action="store_true")
    parser.add_argument("--preflight", action="store_true")
    parser.add_argument("--resume-source")
    parser.add_argument("--decision")
    args = parser.parse_args()
    state_root = args.state_root or default_state_root(args.queue)
    run = Run(queue_path=args.queue, state_root=state_root, manual_folder=args.manual_folder)

    def emit(item: dict) -> None:
        sys.stdout.write(json.dumps(item, sort_keys=True) + "\n")
        sys.stdout.flush()

    if args.resume_source:
        load_queue(run)
        resume_source(run, args.resume_source, args.decision or "")
        emit({"resumed": args.resume_source})
        return
    if args.preflight:
        emit(preflight(run))
        return
    if args.report:
        document, name = write_report(run)
        emit({key: document[key] for key in ("active_units", "outcomes", "stopped_sources", "paused_sources")} | {"report": name})
        return
    sources = set(args.source) if args.source else None
    emit(execute(run, pilot=args.pilot, allow_network=args.execute, sources=sources, max_units=args.max_units, log=emit))


if __name__ == "__main__":
    main()
