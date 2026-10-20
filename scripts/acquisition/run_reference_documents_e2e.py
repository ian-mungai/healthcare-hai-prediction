"""Repeatable offline verification of the reference-document fetch scripts: AHRQ PDFs (HTML served with HTTP 200 included) and the Alaska county-code check."""

from __future__ import annotations

import argparse
import base64
import contextlib
import hashlib
import http.client
import io
import json
import os
import signal
import socket
import sys
import tempfile
import threading
import time
import urllib.request
import zipfile
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from http import HTTPStatus
from pathlib import Path
from types import FrameType
from typing import Any, NoReturn

from scripts.acquisition.one_off.reference_documents import check_alaska_county_codes as alaska
from scripts.acquisition.one_off.reference_documents import fetch_reference_pdfs as pdfs
from scripts.acquisition.one_off.reference_documents import fetch_support as support
from scripts.acquisition.source_registry import REPO_ROOT
from scripts.acquisition.transport import Limits

FIXTURES = REPO_ROOT / "config/acquisition/e2e_inputs/reference_documents.json"
CODE_FILES = (
    "scripts/acquisition/one_off/reference_documents/fetch_support.py",
    "scripts/acquisition/one_off/reference_documents/fetch_child.py",
    "scripts/acquisition/one_off/reference_documents/fetch_reference_pdfs.py",
    "scripts/acquisition/one_off/reference_documents/check_alaska_county_codes.py",
)
PDF_URLS = ("https://publisher.example.org/V2021/ChangeLog_one.pdf", "https://publisher.example.org/V2022/ChangeLog_two.pdf")
MEMBER = alaska.SOURCES[0]["member"]
SIX_CODES = ("02040", "02080", "02120", "02140", "02210", "02260")
HISTORICAL_NAMES = {
    "02040": ["BARROW-NORTH SLOPE"],
    "02080": ["CORDOVA-MCCARTHY"],
    "02120": ["KENAI-COOK INLET"],
    "02140": ["KOBUK"],
    "02210": ["SEWARD"],
    "02260": ["VALDEZ-CHITNA-WHITTIER"],
}
SIX_CODE_SCENARIO = "six_codes_found_only_in_the_old_file_and_absent_from_cms_and_newer_files"
STAND_IN_ENV = "REFERENCE_DOCUMENTS_E2E_STAND_IN"
SITECUSTOMIZE = "from scripts.acquisition.run_reference_documents_e2e import install_stand_in\n\ninstall_stand_in()\n"
TRICKLE_PAUSE = 0.05
DEADLINE_SECONDS = 2.0
DEADLINE_LIMITS = Limits(attempts=2, timeout_seconds=1, max_seconds=DEADLINE_SECONDS, max_bytes=64 * 1024**2)
CUT_BOUND_SECONDS = 3 * DEADLINE_SECONDS
Routes = dict[str, tuple[int, str, bytes]]


class StandInHandler(urllib.request.HTTPSHandler):
    """The only network boundary, inside each download process: it answers each HTTPS request as its config says and logs it with the process id."""

    def __init__(self, config: dict[str, Any]) -> None:
        super().__init__()
        self.config = config

    def https_open(self, req: urllib.request.Request) -> http.client.HTTPResponse:
        with Path(self.config["log"]).open("a", encoding="utf-8") as handle:
            handle.write(json.dumps({"pid": os.getpid(), "url": req.full_url}) + "\n")
        match self.config["kind"]:
            case "routes":
                status, media, encoded = self.config["routes"][req.full_url]
                body = base64.b64decode(encoded)
                head = response_head(status, media, len(body))
                return self.answer(req, head + body, (0, 0), len(head))
            case "trickle":
                body = base64.b64decode(self.config["body"])
                head = response_head(200, "application/pdf", len(body))
                return self.answer(req, head + body, (self.config["head_pause"], self.config["body_pause"]), len(head))
            case "block":
                return self.block()
            case kind:
                raise ValueError(f"Unknown stand-in kind {kind}")

    def answer(self, req: urllib.request.Request, data: bytes, pauses: tuple[float, float], head_size: int) -> http.client.HTTPResponse:
        """Return the real ``HTTPResponse`` reader over a real socket that gets ``data`` one byte at a time: no read times out but the whole can take long."""
        reader, sender = socket.socketpair()
        reader.settimeout(req.timeout)
        threading.Thread(target=feed, args=(sender, data, head_size, pauses), daemon=True).start()
        response = http.client.HTTPResponse(reader)
        reader.close()
        response.begin()
        response.url = req.full_url
        return response

    def block(self) -> NoReturn:
        """Never return and ignore a request to terminate, like a name lookup stuck in C code."""
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        while True:
            time.sleep(60)


def response_head(status: int, media: str, length: int) -> bytes:
    """Return the status line and headers of a response."""
    return f"HTTP/1.1 {status} {HTTPStatus(status).phrase}\r\nContent-Type: {media}\r\nContent-Length: {length}\r\n\r\n".encode()


def feed(sender: socket.socket, data: bytes, head_size: int, pauses: tuple[float, float]) -> None:
    """Send ``data`` one byte at a time, pausing before each, then close; the first ``head_size`` bytes use the first pause."""
    with sender:
        for index, byte in enumerate(data):
            time.sleep(pauses[index >= head_size])
            try:
                sender.sendall(bytes([byte]))
            except OSError:
                return


def install_stand_in() -> None:
    """Replace urllib's opener builder in this process when a run has a stand-in config; runs from the ``sitecustomize`` of the download process only."""
    path = os.environ.get(STAND_IN_ENV)
    if path is None:
        return
    handler = StandInHandler(json.loads(Path(path).read_text(encoding="utf-8")))

    def stand_in_opener(*handlers: urllib.request.BaseHandler | Callable[[], urllib.request.BaseHandler]) -> urllib.request.OpenerDirector:
        opener = urllib.request.OpenerDirector()
        opener.add_handler(handler)
        return opener

    urllib.request.build_opener = stand_in_opener


@contextlib.contextmanager
def stand_in_process_environment(root: Path, config: dict[str, Any]) -> Iterator[Path]:
    """Point the download processes started inside the block at a stand-in described by ``config``; yield the request log and restore the environment after."""
    folder = Path(tempfile.mkdtemp(prefix="stand_in_", dir=root))
    log = folder / "requests.jsonl"
    log.touch()
    (folder / "sitecustomize.py").write_text(SITECUSTOMIZE, encoding="utf-8")
    (folder / "config.json").write_text(json.dumps({**config, "log": str(log)}), encoding="utf-8")
    saved = {name: os.environ.get(name) for name in ("PYTHONPATH", STAND_IN_ENV)}
    os.environ["PYTHONPATH"] = os.pathsep.join(item for item in (str(folder), str(REPO_ROOT), saved["PYTHONPATH"]) if item)
    os.environ[STAND_IN_ENV] = str(folder / "config.json")
    try:
        yield log
    finally:
        for name, value in saved.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


def routes_config(routes: Routes) -> dict[str, Any]:
    """Describe fixture routes for the stand-in."""
    return {"kind": "routes", "routes": {url: [status, media, base64.b64encode(body).decode()] for url, (status, media, body) in routes.items()}}


def process_exists(pid: int) -> bool:
    """Whether a process with this id is still there."""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def run(main: Callable[[list[str]], int], argv: list[str]) -> dict[str, Any]:
    """Call a script's real entry point and return its exit code, parsed JSON output and error text."""
    out, err = io.StringIO(), io.StringIO()
    code: Any = 0
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        try:
            code = main(argv)
        except SystemExit as exit_:
            code = exit_.code
    return {"exit": code, "json": json.loads(out.getvalue()) if out.getvalue().strip() else None, "stderr": err.getvalue()}


def candidates_file(path: Path, urls: tuple[str, ...], pins: tuple[str | None, ...] = (None, None), format_: str = "pdf") -> Path:
    """Write a history-route candidates file for the synthetic URLs."""
    items = [
        {"url": url, "format": format_, "role": "reference", "source_id": "PSI90", **({"expected_sha256": pin} if pin else {})}
        for url, pin in zip(urls, pins, strict=True)
    ]
    path.write_text(json.dumps({"candidates": items}))
    return path


def crosswalk_zip(text: str) -> bytes:
    """Build the CMS zip around a tab-delimited member."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(MEMBER, text.encode("latin-1"))
    return buffer.getvalue()


def alaska_routes(fx: dict[str, str]) -> Routes:
    """Serve the four crosswalk files from the fixtures."""
    csv_bodies = {1: fx["nber_old_csv"], 2: fx["nber_2011_csv"], 3: fx["nber_2020_csv"]}
    routes: Routes = {alaska.SOURCES[0]["url"]: (200, "application/zip", crosswalk_zip(fx["cms_crosswalk_txt"]))}
    for index, text in csv_bodies.items():
        routes[alaska.SOURCES[index]["url"]] = (200, "text/csv", text.encode())
    return routes


@contextlib.contextmanager
def short_deadline() -> Iterator[None]:
    """Give each file a two-second total deadline for the length of the block."""
    previous = support.LIMITS
    support.LIMITS = DEADLINE_LIMITS
    try:
        yield
    finally:
        support.LIMITS = previous


@dataclass
class Workspace:
    """The temporary folder, the fixtures and the scenarios recorded so far."""

    root: Path
    fx: dict[str, str]
    scenarios: list[dict[str, Any]] = field(default_factory=list)
    pdf: bytes = field(init=False)
    digest: str = field(init=False)
    good: Routes = field(init=False)

    def __post_init__(self) -> None:
        self.pdf = self.fx["pdf_body"].encode()
        self.digest = hashlib.sha256(self.pdf).hexdigest()
        self.good = {url: (200, "application/pdf", self.pdf) for url in PDF_URLS}

    def scenario(self, name: str, expected: Any, action: Callable[[], Any]) -> None:
        """Run one scenario and record whether its observation equals the expectation written beside it."""
        try:
            observed = action()
        except (ValueError, OSError, KeyError, TypeError, AssertionError) as error:
            observed = f"{type(error).__name__}: {error}"
        self.scenarios.append({"name": name, "passed": observed == expected, "expected": expected, "observed": observed})

    def served(self, config: dict[str, Any], action: Callable[[], dict[str, Any]]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        """Run ``action`` with download processes that use the stand-in ``config``; return its result and the requests the stand-in logged."""
        with stand_in_process_environment(self.root, config) as log:
            result = action()
        return result, [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines()]

    def pdf_run(
        self, name: str, config: dict[str, Any], urls: tuple[str, ...] = PDF_URLS, pins: tuple[str | None, ...] = (None, None), format_: str = "pdf"
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        """Run the PDF entry point on a candidates file for ``urls`` and a new or reused folder ``name``; return its result and the logged requests."""
        argv = ["--candidates", str(candidates_file(self.root / f"{name}_candidates.json", urls, pins, format_)), "--output", str(self.root / name)]
        return self.served(config, lambda: run(pdfs.main, argv))

    def fetch_pdfs(self, name: str, routes: Routes, **kwargs: Any) -> tuple[dict[str, Any], list[str]]:
        """Run the PDF entry point against fixture routes; return its result and the requested URLs."""
        result, requests = self.pdf_run(name, routes_config(routes), **kwargs)
        return result, [item["url"] for item in requests]

    def fetch_alaska(self, name: str, routes: Routes) -> tuple[dict[str, Any], list[str]]:
        """Run the county-code entry point against fixture routes; return its result and the requested URLs."""
        result, requests = self.served(routes_config(routes), lambda: run(alaska.main, ["--output", str(self.root / name)]))
        return result, [item["url"] for item in requests]

    def receipt(self, name: str) -> dict[str, Any]:
        """Return a run folder's saved receipt."""
        return json.loads((self.root / name / "receipt.json").read_text())

    def rejected(self, name: str, routes: Routes, **kwargs: Any) -> dict[str, Any]:
        """Return the exit code and the status and reason of each file after a run with a bad file."""
        result, _ = self.fetch_pdfs(name, routes, **kwargs)
        files = self.receipt(name)["files"]
        return {"exit": result["exit"], "statuses": [e["status"] for e in files], "reasons": [e["reason"] for e in files]}


def pdf_fetch_scenarios(ws: Workspace) -> None:
    """A clean fetch keeps every original unchanged and records its hash."""

    def accepted() -> dict[str, Any]:
        result, requested = ws.fetch_pdfs("pdf_ok", ws.good, pins=(ws.digest, None))
        files = ws.receipt("pdf_ok")["files"]
        stored = [(ws.root / "pdf_ok" / entry["path"]).read_bytes() for entry in files]
        return {
            "exit": result["exit"],
            "statuses": [entry["status"] for entry in files],
            "sha256": [entry["sha256"] for entry in files],
            "originals_kept_unchanged": stored == [ws.pdf, ws.pdf],
            "requests": requested,
        }

    ws.scenario(
        "pdf_accepted_with_receipt_hash_and_unchanged_original",
        {"exit": 0, "statuses": ["accepted", "accepted"], "sha256": [ws.digest] * 2, "originals_kept_unchanged": True, "requests": list(PDF_URLS)},
        accepted,
    )


def pdf_rejection_scenarios(ws: Workspace) -> None:
    """Each wrong file is rejected with its reason while the other PDF is still accepted."""
    html, pdf, other = ws.fx["ahrq_html_body"].encode(), ws.pdf, ws.good[PDF_URLS[1]]
    cases = (
        ("html_with_http_200_rejected_and_the_other_pdf_still_accepted", "html_200", (200, "text/html; charset=UTF-8", html), "html_instead_of_data"),
        ("html_labelled_application_pdf_rejected", "html_as_pdf", (200, "application/pdf", html), "html_instead_of_data"),
        ("real_pdf_served_as_text_html_rejected_by_media_type", "pdf_as_html", (200, "text/html", pdf), "unexpected_media_type"),
        ("pdf_with_bytes_before_the_header_rejected", "pdf_shifted", (200, "application/pdf", b"\n" + pdf), "unexpected_file_header"),
        ("http_404_rejected", "pdf_404", (404, "text/html", html), "http_404"),
    )

    def serving(folder: str, route: tuple[int, str, bytes]) -> Callable[[], dict[str, Any]]:
        return lambda: ws.rejected(folder, {PDF_URLS[0]: route, PDF_URLS[1]: other})

    for scenario_name, folder, route, reason in cases:
        ws.scenario(scenario_name, {"exit": 1, "statuses": ["rejected", "accepted"], "reasons": [reason, None]}, serving(folder, route))
    ws.scenario(
        "pinned_hash_mismatch_rejected_and_matching_pin_accepted",
        {"exit": 1, "statuses": ["rejected", "accepted"], "reasons": ["sha256_differs_from_pin", None]},
        lambda: ws.rejected("pdf_pin", ws.good, pins=("0" * 64, ws.digest)),
    )


def pdf_receipt_scenarios(ws: Workspace) -> None:
    """A rerun rechecks offline against the current candidate pins; the last case edits an original, so it runs last."""

    def rerun(pins: tuple[str | None, ...], cause: str | None = None) -> dict[str, Any]:
        receipt_path = ws.root / "pdf_ok/receipt.json"
        before = receipt_path.read_bytes()
        result, requested = ws.fetch_pdfs("pdf_ok", ws.good, pins=pins)
        observed: dict[str, Any] = {"exit": result["exit"], "requests": requested, "receipt_unchanged": before == receipt_path.read_bytes()}
        if cause is None:
            return {**observed, "requests_made": result["json"]["requests_made"]}
        return {**observed, "names_cause": cause in result["stderr"]}

    offline = {"exit": 0, "requests": [], "receipt_unchanged": True, "requests_made": False}
    ws.scenario("rerun_rechecks_offline_and_changes_nothing", offline, lambda: rerun((ws.digest, None)))
    ws.scenario("rerun_with_a_pin_that_still_matches_is_accepted_offline", offline, lambda: rerun((ws.digest, ws.digest)))
    ws.scenario(
        "rerun_with_a_changed_pin_refused_offline",
        {"exit": 2, "requests": [], "receipt_unchanged": True, "names_cause": True},
        lambda: rerun(("0" * 64, None), "sha256_differs_from_pin"),
    )

    def tampered() -> dict[str, Any]:
        target = ws.root / "pdf_ok" / ws.receipt("pdf_ok")["files"][0]["path"]
        target.chmod(0o644)
        target.write_bytes(ws.pdf + b"% edited\n")
        result, requested = ws.fetch_pdfs("pdf_ok", ws.good, pins=(ws.digest, None))
        return {"exit": result["exit"], "names_cause": "differs from its receipt" in result["stderr"], "requests": requested}

    ws.scenario("edited_original_detected_on_rerun", {"exit": 2, "names_cause": True, "requests": []}, tampered)


def pdf_refusal_scenarios(ws: Workspace) -> None:
    """A folder or candidate the script cannot trust is refused before any request."""

    def refused(name: str, cause: str, **kwargs: Any) -> dict[str, Any]:
        result, requested = ws.fetch_pdfs(name, ws.good, **kwargs)
        return {"exit": result["exit"], "names_cause": cause in result["stderr"], "requests": requested}

    def stray_folder() -> dict[str, Any]:
        (ws.root / "pdf_stray").mkdir()
        (ws.root / "pdf_stray/leftover.txt").write_text("x")
        return refused("pdf_stray", "no receipt")

    expected = {"exit": 2, "names_cause": True, "requests": []}
    ws.scenario("folder_with_files_and_no_receipt_refused_before_any_request", expected, stray_folder)
    ws.scenario("non_pdf_candidate_refused_before_any_request", expected, lambda: refused("pdf_wrong_kind", "not a reference-role PDF", format_="csv"))


def deadline_scenarios(ws: Workspace) -> None:
    """A download that does not finish is stopped at the file's total deadline, whatever it waits on; its finished attempts stay and nothing is accepted."""

    def stopped(name: str, config: dict[str, Any]) -> dict[str, Any]:
        with short_deadline():
            started = time.monotonic()
            result, requests = ws.pdf_run(name, config, urls=PDF_URLS[:1], pins=(None,))
            elapsed = time.monotonic() - started
        entry = ws.receipt(name)["files"][0]
        folder = ws.root / name / entry["path"]
        record = json.loads((folder / "deadline.json").read_text())
        return {
            "exit": result["exit"],
            "statuses": [entry["status"]],
            "reasons": [entry["reason"]],
            "sha256": [entry["sha256"]],
            "bytes": [entry["bytes"]],
            "ended_at_the_deadline": DEADLINE_SECONDS <= elapsed < CUT_BOUND_SECONDS,
            "deadline_record": {key: record[key] for key in ("failure", "url", "seconds")},
            "attempt_folders_kept": sorted(path.name for path in folder.glob("attempt_*")),
            "requests": len(requests),
            "process_gone": not any(process_exists(item["pid"]) for item in requests),
        }

    expected = {
        "exit": 1,
        "statuses": ["rejected"],
        "reasons": ["capture_deadline_exceeded"],
        "sha256": [None],
        "bytes": [None],
        "ended_at_the_deadline": True,
        "deadline_record": {"failure": "capture_deadline_exceeded", "url": PDF_URLS[0], "seconds": DEADLINE_SECONDS},
        "attempt_folders_kept": ["attempt_01"],
        "requests": 1,
        "process_gone": True,
    }
    body = base64.b64encode(ws.pdf).decode()
    trickle = {"kind": "trickle", "body": body}
    ws.scenario(
        "body_trickling_past_the_total_deadline_stopped_and_recorded",
        expected,
        lambda: stopped("deadline_body", {**trickle, "head_pause": 0, "body_pause": TRICKLE_PAUSE}),
    )
    ws.scenario(
        "headers_trickling_past_the_total_deadline_stopped_and_recorded",
        expected,
        lambda: stopped("deadline_head", {**trickle, "head_pause": TRICKLE_PAUSE, "body_pause": 0}),
    )
    ws.scenario(
        "name_lookup_style_block_that_ignores_termination_stopped_at_the_total_deadline",
        expected,
        lambda: stopped("deadline_block", {"kind": "block"}),
    )

    def outer_alarm_unaffected() -> dict[str, Any]:
        fired: list[int] = []

        def outer(signum: int, frame: FrameType | None) -> None:
            fired.append(signum)

        previous = signal.signal(signal.SIGALRM, outer)
        signal.setitimer(signal.ITIMER_REAL, 0.1, 0.1)
        try:
            stopped("deadline_timer", {"kind": "block"})
            interval = signal.getitimer(signal.ITIMER_REAL)[1]
            return {"fired_during_the_fetch": len(fired) >= 5, "interval_kept": interval == 0.1, "handler_kept": signal.getsignal(signal.SIGALRM) is outer}
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0)
            signal.signal(signal.SIGALRM, previous)

    ws.scenario(
        "an_outer_repeating_alarm_keeps_its_handler_and_interval_and_fires_during_a_fetch",
        {"fired_during_the_fetch": True, "interval_kept": True, "handler_kept": True},
        outer_alarm_unaffected,
    )


def crosswalk_scenarios(ws: Workspace) -> None:
    """The six codes are checked against an independently written list; the later cases reuse the folder of the first."""

    def found() -> dict[str, Any]:
        result, requested = ws.fetch_alaska("ak_ok", alaska_routes(ws.fx))
        report = json.loads((ws.root / "ak_ok/report.json").read_text())
        old = next(item for item in report["sources"] if item["id"] == "nber_msabea")
        return {
            "exit": result["exit"],
            "codes_checked": report["codes_checked"],
            "summary": report["summary"],
            "old_as_ssa": old["codes_as_ssa"],
            "old_as_fips": old["codes_as_fips"],
            "seward": old["found"]["02210"]["as_ssa"][0]["county"],
            "historical_names": report["historical_names"],
            "other_files_without_any_code": [item["found"] for item in report["sources"] if item["id"] != "nber_msabea"],
            "requests": len(requested),
        }

    summary = {
        "sources_with_any_code": ["nber_msabea"],
        "primary_source_has_any_code": False,
        "sources_with_all_six_codes_as_ssa": ["nber_msabea"],
        "sources_with_all_six_codes_as_fips": ["nber_msabea"],
    }
    ws.scenario(
        SIX_CODE_SCENARIO,
        {
            "exit": 0,
            "codes_checked": list(SIX_CODES),
            "summary": summary,
            "old_as_ssa": list(SIX_CODES),
            "old_as_fips": list(SIX_CODES),
            "seward": "SEWARD",
            "historical_names": {"source": "nber_msabea", "names": HISTORICAL_NAMES},
            "other_files_without_any_code": [{}, {}, {}],
            "requests": 4,
        },
        found,
    )

    def fips_only() -> dict[str, Any]:
        routes = alaska_routes(ws.fx) | {alaska.SOURCES[3]["url"]: (200, "text/csv", ws.fx["nber_fips_only_csv"].encode())}
        ws.fetch_alaska("ak_fips", routes)
        item = next(entry for entry in json.loads((ws.root / "ak_fips/report.json").read_text())["sources"] if entry["id"] == "nber_2020")
        return {"as_ssa": item["codes_as_ssa"], "as_fips": item["codes_as_fips"]}

    ws.scenario("code_in_the_fips_column_only_is_reported_as_fips", {"as_ssa": [], "as_fips": ["02080"]}, fips_only)

    def alaska_rerun() -> dict[str, Any]:
        before = (ws.root / "ak_ok/report.json").read_bytes()
        result, requested = ws.fetch_alaska("ak_ok", alaska_routes(ws.fx))
        return {
            "exit": result["exit"],
            "requests_made": result["json"]["requests_made"],
            "requests": requested,
            "report_unchanged": before == (ws.root / "ak_ok/report.json").read_bytes(),
        }

    ws.scenario(
        "alaska_rerun_rechecks_offline_with_identical_report", {"exit": 0, "requests_made": False, "requests": [], "report_unchanged": True}, alaska_rerun
    )


def crosswalk_failure_scenarios(ws: Workspace) -> None:
    """A wrong file or a changed layout fails visibly and writes no report."""

    def html_csv() -> dict[str, Any]:
        routes = alaska_routes(ws.fx) | {alaska.SOURCES[2]["url"]: (200, "text/html", ws.fx["ahrq_html_body"].encode())}
        result, _ = ws.fetch_alaska("ak_html", routes)
        return {
            "exit": result["exit"],
            "rejected": [(item["file_name"], item["reason"]) for item in result["json"]["rejected"]],
            "report_written": (ws.root / "ak_html/report.json").exists(),
        }

    ws.scenario(
        "html_served_for_a_crosswalk_csv_rejected_and_no_report_written",
        {"exit": 1, "rejected": [("ssa_fips_state_county2011.csv", "html_instead_of_data")], "report_written": False},
        html_csv,
    )

    def zip_as_html() -> dict[str, Any]:
        routes = alaska_routes(ws.fx) | {alaska.SOURCES[0]["url"]: (200, "text/html", crosswalk_zip(ws.fx["cms_crosswalk_txt"]))}
        result, _ = ws.fetch_alaska("ak_media", routes)
        return {"exit": result["exit"], "rejected": [(item["file_name"], item["reason"]) for item in result["json"]["rejected"]]}

    ws.scenario(
        "cms_zip_served_as_text_html_rejected_by_media_type",
        {"exit": 1, "rejected": [("fy2020-fr-county-cbsa-crosswalk.zip", "unexpected_media_type")]},
        zip_as_html,
    )

    def layout_change() -> dict[str, Any]:
        routes = alaska_routes(ws.fx) | {alaska.SOURCES[3]["url"]: (200, "text/csv", ws.fx["nber_renamed_column_csv"].encode())}
        result, _ = ws.fetch_alaska("ak_layout", routes)
        return {
            "exit": result["exit"],
            "names_cause": "column layout differs" in result["stderr"],
            "report_written": (ws.root / "ak_layout/report.json").exists(),
        }

    ws.scenario("renamed_column_fails_visibly_and_writes_no_report", {"exit": 2, "names_cause": True, "report_written": False}, layout_change)


def exercise(root: Path, fx: dict[str, str]) -> list[dict[str, Any]]:
    """Run every scenario through the scripts' real entry points against the saved fixtures."""
    ws = Workspace(root, fx)
    for group in (
        pdf_fetch_scenarios,
        pdf_rejection_scenarios,
        pdf_receipt_scenarios,
        pdf_refusal_scenarios,
        deadline_scenarios,
        crosswalk_scenarios,
        crosswalk_failure_scenarios,
    ):
        group(ws)
    return ws.scenarios


def main() -> None:
    """Write one immutable artifact for every E2E invocation."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    fixtures: dict[str, str] = json.loads(FIXTURES.read_text())
    with tempfile.TemporaryDirectory(prefix="reference_documents_e2e_") as directory:
        scenarios = exercise(Path(directory), fixtures)
    passed = all(item["passed"] for item in scenarios)
    report = {
        "status": "passed" if passed else "failed",
        "feature": "Reference-document fetch scripts: AHRQ PSI change log PDFs and the Alaska county-code crosswalk check",
        "run_at_utc": datetime.now(UTC).isoformat(),
        "python": sys.version.split()[0],
        "code_sha256": {name: hashlib.sha256((REPO_ROOT / name).read_bytes()).hexdigest() for name in (*CODE_FILES, "scripts/acquisition/transport.py")},
        "fixtures_sha256": hashlib.sha256(FIXTURES.read_bytes()).hexdigest(),
        "reproduce": f".venv/bin/python -m scripts.acquisition.run_reference_documents_e2e --output {args.output}",
        "boundary": (
            "Saved synthetic fixtures served by a stand-in that replaces urllib's opener builder inside each download process; "
            "the real transport, scripts and process launcher run. No publisher, AWS or bronze use."
        ),
        "cleanup": "Temporary folder removed on exit.",
        "scenarios": scenarios,
    }
    with args.output.open("x", encoding="utf-8") as handle:
        handle.write(json.dumps(report, indent=2, sort_keys=True) + "\n")
    sys.stdout.write(
        json.dumps({"status": report["status"], "passed": sum(item["passed"] for item in scenarios), "total": len(scenarios), "artifact": str(args.output)})
        + "\n"
    )
    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
