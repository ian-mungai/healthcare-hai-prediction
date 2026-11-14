"""The monthly hold report: which files a publisher shows that the locked queue does not hold, named for the owner. Nothing is added to a queue.

A listing names the files a publisher shows now, each with its source, URL, role and collector mode. A file the locked queue already holds is counted and
not reported. Every other file is held: it is captured only after the owner reviews it and adds it to the queue, and no step of a DAG waits for that review.
Each held file carries notes that name what would keep it out of a queue today (a host outside the approved hosts file, an unknown file role, a collector
outside the plain task lane); a note changes nothing, because every new file is held. The module only reads records and builds the report: it fetches no
listing, requests no file, makes no AWS call and starts no capture. The report sits under the monthly run's record folder in ``plan_rebuild/``, as a
machine-readable ``report.json`` and a human-readable ``report.txt``.
"""

from __future__ import annotations

import re
import urllib.parse
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from scripts.acquisition import redownload
from scripts.acquisition.transport import CaptureError

REPORT_FOLDER = "plan_rebuild"
REPORT_JSON = "report.json"
REPORT_TEXT = "report.txt"
RUN_ID = re.compile(r"[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}")
# The file roles a listing source may name: the collected roles and the reference roles.
ROLES = frozenset({"data", "api_page", "dictionary", "methodology", "layout"})
ENTRY_KEYS = frozenset({"source_id", "url", "role", "mode"})
NOTES = {
    "listed_twice_differently": "the listing shows this file twice with different details",
    "host_not_approved": "its host is not on the approved hosts file, or it is not an HTTPS address",
    "role_unknown": "its file role is not a known role",
    "collector_not_task_lane": "its collector does not run in the plain task lane",
}
PAGE_REASONS = {
    "index_host_not_approved": "its host is not on the approved hosts file",
    "index_unreadable": "it could not be read",
    "too_many_files": "it lists too many files for one source",
    "outside_reviewed_paths": "it lies outside the reviewed download paths of its source",
}


@dataclass(frozen=True)
class Candidate:
    """One file of a listing with the facts the notes read."""

    source_id: str
    url: str
    role: str
    mode: str


@dataclass(frozen=True)
class Reference:
    """What the notes compare a candidate with: the approved hosts and the URLs of the locked queue."""

    hosts: frozenset[str]
    known_urls: frozenset[str]


@dataclass(frozen=True)
class Held:
    candidate: Candidate
    notes: tuple[str, ...]


@dataclass(frozen=True)
class Decision:
    """The files held for the owner with their notes, in URL order, and how many listed files the queue already held."""

    held: tuple[Held, ...]
    known: int


def read_entries(entries: object) -> list[Candidate]:
    """Read the entries of a listing, each a closed record of text. A record with a missing or extra key or a value that is not text stops the read."""
    if not isinstance(entries, list):
        raise CaptureError("The listing must be a list of entries.")
    found = []
    for entry in entries:
        if not isinstance(entry, dict) or set(entry) != ENTRY_KEYS or not all(isinstance(entry[key], str) for key in ENTRY_KEYS):
            raise CaptureError("A listing entry must hold exactly its source, url, role and mode, all text.")
        found.append(Candidate(entry["source_id"], entry["url"], entry["role"], entry["mode"]))
    return found


def reference_from(queue: Mapping[str, Any], hosts: Mapping[str, Any]) -> Reference:
    """The reference of a locked queue document and the approved hosts document. Every unit of the queue counts as known, whatever its disposition."""
    return Reference(frozenset(hosts["hosts"]), frozenset(str(unit["url"]) for unit in queue["queue"] if unit.get("url")))


def host_approved(url: str, hosts: frozenset[str]) -> bool:
    parts = urllib.parse.urlsplit(url)
    return parts.scheme == "https" and parts.hostname in hosts


def notes_of(found: Candidate, reference: Reference) -> tuple[str, ...]:
    """Every note the candidate earns, in the order of ``NOTES``."""
    notes = []
    if not host_approved(found.url, reference.hosts):
        notes.append("host_not_approved")
    if found.role not in ROLES:
        notes.append("role_unknown")
    if redownload.lane_of({"mode": found.mode, "disposition": "direct_download", "flags": []}) != redownload.TASK:
        notes.append("collector_not_task_lane")
    return tuple(notes)


def decide(candidates: Iterable[Candidate], reference: Reference) -> Decision:
    """Split the listing into files the queue holds and files held for the owner. The result does not depend on the order of the listing."""
    by_url: dict[str, list[Candidate]] = {}
    for found in candidates:
        by_url.setdefault(found.url, [])
        if found not in by_url[found.url]:
            by_url[found.url].append(found)
    held, known = [], 0
    for url in sorted(by_url):
        if url in reference.known_urls:
            known += 1
            continue
        variants = by_url[url]
        if len(variants) > 1:
            held.append(Held(sorted(variants, key=repr)[0], ("listed_twice_differently",)))
        else:
            held.append(Held(variants[0], notes_of(variants[0], reference)))
    return Decision(tuple(held), known)


def require_run(run_id: str) -> None:
    if not RUN_ID.fullmatch(run_id):
        raise CaptureError("The run id is not a run id.")


def report(decision: Decision, run_id: str, unreached: list[dict[str, str]], outside: list[dict[str, str]]) -> dict[str, Any]:
    """The machine-readable report: the count the queue already holds, each held file with its notes, the pages not read and the links outside the paths."""
    require_run(run_id)
    return {
        "kind": "plan_rebuild_report",
        "run_id": run_id,
        "known": decision.known,
        "held": [
            {
                "source_id": item.candidate.source_id,
                "url": item.candidate.url,
                "role": item.candidate.role,
                "mode": item.candidate.mode,
                "notes": list(item.notes),
            }
            for item in decision.held
        ],
        "unreached": unreached,
        "outside": outside,
    }


def report_text(decision: Decision, run_id: str, unreached: list[dict[str, str]], outside: list[dict[str, str]]) -> str:
    """The same report for the owner, who reviews the held files: what is new, and what could not be read."""
    require_run(run_id)
    lines = [
        f"Plan rebuild of run {run_id}",
        f"Held for review: {len(decision.held)}. Already in the queue: {decision.known}. Nothing was added to any queue.",
        "",
        "Held files are not captured until you review them.",
    ]
    for item in decision.held:
        lines.append(f"- {item.candidate.url} (source {item.candidate.source_id}, role {item.candidate.role}, collector {item.candidate.mode})")
        lines.extend(f"    Note: {NOTES[note]}." for note in item.notes)
    if unreached:
        lines.extend(["", f"Index pages not read: {len(unreached)}."])
        lines.extend(f"- {row['url']} (source {row['source_id']}): {PAGE_REASONS[row['reason']]}." for row in unreached)
    if outside:
        lines.extend(["", f"Links not listed: {len(outside)}."])
        lines.extend(f"- {row['url']} (source {row['source_id']}): {PAGE_REASONS[row['reason']]}." for row in outside)
    return "\n".join(lines) + "\n"


def report_keys(root: str, run_id: str) -> tuple[str, str]:
    """The storage keys of the machine-readable and the human-readable report of a monthly run under a capture root."""
    require_run(run_id)
    folder = f"{root}ingestion_runs/{run_id}/{REPORT_FOLDER}/"
    return f"{folder}{REPORT_JSON}", f"{folder}{REPORT_TEXT}"
