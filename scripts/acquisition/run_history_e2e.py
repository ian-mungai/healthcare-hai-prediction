"""Repeatable synthetic history-collector verification: redirect review, planning and capture-to-S3, without live downloads or AWS."""

import argparse
import contextlib
import hashlib
import io
import json
import sys
import tempfile
import urllib.error
import urllib.request
from datetime import UTC, datetime
from email.message import Message
from functools import partial
from pathlib import Path
from typing import Any
from unittest.mock import patch

from scripts.acquisition import bls_api_contract as bls_contract
from scripts.acquisition import history_redirects, history_routes, s3_store, transport
from scripts.acquisition.capture import capture
from scripts.acquisition.s3_store import encoded_json, write_once
from scripts.acquisition.source_registry import LOCK_PATH, REPO_ROOT, load_registry, read_json, require
from scripts.acquisition.tests.test_s3_store import OUTPUTS, SETTINGS, FakeS3

# The one reviewed publisher redirect: synthetic resources must stay on its host and in its bucket.
APPROVED_SOURCE, APPROVED_TARGET = next(iter(transport.SIGNED_REDIRECT_ROUTES.items()))
PUBLISHER = APPROVED_SOURCE.split("/dataset/")[0]
BUCKET = APPROVED_TARGET.split("/resources/")[0]
PDF = b"%PDF-1.4\nSynthetic historical reference\n%%EOF\n"
RULES = read_json(REPO_ROOT / "config/acquisition/planning_rules.json")


def resource(name: str) -> tuple[str, str]:
    """A synthetic publisher resource URL and its exact unsigned bucket destination."""
    return f"{PUBLISHER}/dataset/e2e/resource/{name}/download/{name}.pdf", f"{BUCKET}/resources/{name}/{name}.pdf"


def candidate(name: str, **changes: Any) -> dict:
    """A reference candidate shaped like the recovered California history candidates."""
    url, target = resource(name)
    return {
        "source_id": "CA",
        "source_ids": ["CA"],
        "role": "reference",
        "format": "pdf",
        "label": f"Synthetic documentation {name}",
        "release_date": None,
        "max_bytes": 1024 * 1024,
        "url": url,
        "reviewed_unsigned_redirect": target,
        "evidence": {"redirect_review": "Synthetic E2E resource", "redirect_observed_at": "2026-09-29T00:00:00+00:00"},
    } | changes


class Headers(Message):
    """HTTP headers with the case-insensitive lookups urllib responses provide."""

    def __init__(self, values: dict[str, str]) -> None:
        super().__init__()
        for key, value in values.items():
            self[key] = value


class Response(io.BytesIO):
    """A minimal HTTP response for the project downloader."""

    def __init__(self, data: bytes, url: str, status: int = 200, media: str = "application/pdf") -> None:
        super().__init__(data)
        self.status, self.url = status, url
        self.headers = Headers({"Content-Length": str(len(data)), "Content-Type": media})

    def geturl(self) -> str:
        return self.url


class Web:
    """Serves the synthetic PDF and counts requests; an override makes it misbehave."""

    def __init__(self) -> None:
        self.requests = 0
        self.override: Response | Exception | None = None

    def open(self, request: Any, timeout: float) -> Response:
        self.requests += 1
        if isinstance(self.override, Exception):
            raise self.override
        return self.override if self.override is not None else Response(PDF, request.full_url)


class RedirectWeb:
    """HEAD responses per URL: a signed redirect, a plain page, an HTTP error or a network failure."""

    def __init__(self, answers: dict[str, Any]) -> None:
        self.answers = answers
        self.requests: list[tuple[str, str]] = []

    def open(self, request: Any, timeout: float) -> Any:
        self.requests.append((request.get_method(), request.full_url))
        answer = self.answers[request.full_url]
        if isinstance(answer, Exception):
            raise answer
        status, location = answer
        if status in {302, 303, 307, 308}:
            raise urllib.error.HTTPError(request.full_url, status, "Found", Headers({"Location": location}), io.BytesIO())
        return Response(b"", request.full_url, status, "text/html")


def run_main(module: Any, argv: list[str]) -> list[dict]:
    """Run a collector's command line in-process and return its JSON output lines."""
    output = io.StringIO()
    with patch.object(sys, "argv", [module.__name__, *argv]), contextlib.redirect_stdout(output):
        module.main()
    return [json.loads(line) for line in output.getvalue().splitlines() if line.startswith("{")]


def exercise(root: Path) -> list[dict]:
    """Drive the history collectors through synthetic HTTP and storage boundaries."""
    scenarios: list[dict] = []

    def scenario(name: str, action: Any, error: str | None = None) -> None:
        try:
            action()
            scenarios.append({"name": name, "passed": error is None, "observed": "success", "expected": error or "success"})
        except (ValueError, OSError, KeyError, TypeError, StopIteration, IndexError, AssertionError) as failure:
            scenarios.append(
                {"name": name, "passed": error is not None and error in str(failure), "observed": str(failure)[:200], "expected": error or "success"}
            )

    original_routes = dict(transport.SIGNED_REDIRECT_ROUTES)
    try:
        redirect_scenarios(scenario, root)
        route_scenarios(scenario, root)
    finally:
        transport.SIGNED_REDIRECT_ROUTES.clear()
        transport.SIGNED_REDIRECT_ROUTES.update(original_routes)
    return scenarios


def redirect_scenarios(scenario: Any, root: Path) -> None:
    """Only an exact, unsigned redirect into the reviewed bucket is retained."""
    good, good_target = resource("e2e-good")
    plain, _ = resource("e2e-plain")
    missing, _ = resource("e2e-missing")
    offline, _ = resource("e2e-offline")
    other, other_target = resource("e2e-other")
    answers = {
        good: (302, good_target + "?X-Amz-Signature=synthetic"),
        plain: (200, None),
        missing: urllib.error.HTTPError(missing, 404, "Not Found", Headers({}), io.BytesIO()),
        offline: OSError("synthetic network failure"),
        other: (302, other_target.replace("/e2e-other/", "/not-this-resource/")),
    }
    candidates = root / "redirect_candidates.json"
    write_once(candidates, encoded_json({"candidates": [{"url": url, "evidence": {"source": "synthetic"}} for url in answers]}))
    output = root / "redirects.json"
    web = RedirectWeb(answers)

    def review(destination: Path) -> None:
        argv = ["history_redirects", "--candidates", str(candidates), "--output", str(destination)]
        with patch.object(urllib.request, "build_opener", lambda *_handlers: web), contextlib.redirect_stdout(io.StringIO()), patch.object(sys, "argv", argv):
            history_redirects.main()

    def reviewed() -> None:
        review(output)
        result = read_json(output)
        require([item["url"] for item in result["candidates"]] == [good], "Only the exact bucket redirect is retained")
        require(result["candidates"][0]["reviewed_unsigned_redirect"] == good_target, "Signed query string retained")
        statuses = {item["url"]: item["status"] for item in result["unresolved_redirects"]}
        # HTTP errors keep their status; network failures and out-of-bucket targets are never verified.
        expected = {plain: 200, missing: 404, offline: "redirect_not_verified", other: "redirect_not_verified"}
        require(statuses == expected, f"Unresolved statuses differ: {statuses}")
        require(all(method == "HEAD" for method, _ in web.requests), "Redirect review downloaded a body")
        require(transport.SIGNED_REDIRECT_ROUTES.get(good) == good_target and other not in transport.SIGNED_REDIRECT_ROUTES, "Redirect table differs")

    scenario("redirect_review_keeps_only_exact_unsigned_bucket_target", reviewed)

    def changed_rerun() -> None:
        answers[good] = OSError("publisher now unreachable")
        review(output)

    scenario("redirect_review_never_overwrites_evidence", changed_rerun, "Existing local evidence differs")


def route_scenarios(scenario: Any, root: Path) -> None:
    """Planning honours holds, exclusions, duplicates and byte budgets; execution stores once through fake S3."""
    registry, lock = load_registry(), read_json(LOCK_PATH)
    held = next(source["source_id"] for source in registry["sources"] if source["preferred_route"] == "access_hold")
    review_held = sorted(RULES["privacy_review_sources"] + RULES["large_file_review_sources"])[0]
    one = candidate("e2e-history-one")

    def extend(candidates: list[dict]) -> list[dict]:
        return history_routes.extend_registry(registry, lock, candidates, RULES)[2]

    def deduplicated() -> None:
        jobs = extend([one, dict(one)])
        require(len(jobs) == 1 and jobs[0]["plan"]["source_id"] == "CA", "Repeated URL produced a second job")

    scenario("repeated_candidate_url_planned_once", deduplicated)
    scenario("access_hold_source_rejected", lambda: extend([candidate("e2e-held", source_id=held)]), "access, privacy or large-file hold")
    scenario(
        "privacy_or_large_file_data_rejected",
        lambda: extend([candidate("e2e-review", source_id=review_held, role="data")]),
        "access, privacy or large-file hold",
    )
    scenario("user_excluded_source_rejected", lambda: extend([candidate("e2e-excluded", source_id="S26_CLH")]), "excluded by current user decision")

    def budget() -> None:
        jobs = extend([one, candidate("e2e-history-two")])
        selected, deferred, reserved = history_routes.bounded_jobs(jobs, 3 * 1024 * 1024)
        require(len(selected) == 1 and len(deferred) == 1 and reserved == 2 * 1024 * 1024, "Retry-inclusive byte budget not enforced")

    scenario("byte_budget_defers_second_job", budget)
    scenario(
        "oversized_file_limit_rejected", lambda: history_routes.bounded_jobs(extend([candidate("e2e-big", max_bytes=1024**3)]), 8 * 1024**3), "at most 512 MiB"
    )

    candidates = root / "history_candidates.json"
    write_once(candidates, encoded_json({"candidates": [one]}))
    state = root / "state"

    def dry_run() -> None:
        lines = run_main(history_routes, ["--candidates", str(candidates), "--state-root", str(state)])
        require(lines == [lines[0]] and lines[0]["validated_historical_jobs"] == 1 and not lines[0]["executing"], "Dry run summary differs")
        require(not state.exists(), "Dry run wrote state")

    scenario("dry_run_plans_without_effects", dry_run)
    web, client = Web(), FakeS3()

    def boundaries() -> contextlib.ExitStack:
        stack = contextlib.ExitStack()
        terraform = json.dumps(OUTPUTS)
        stack.enter_context(patch.object(history_routes, "load_configuration", lambda _path: (dict(SETTINGS), {})))
        stack.enter_context(patch.object(history_routes, "run_argv", lambda *_args, **_kwargs: type("Result", (), {"stdout": terraform})()))
        stack.enter_context(patch.object(history_routes, "AwsCli", lambda _settings: client))
        stack.enter_context(patch.object(history_routes, "capture", partial(capture, opener=web)))
        return stack

    def execute(state_root: Path) -> list[dict]:
        with boundaries():
            return run_main(history_routes, ["--candidates", str(candidates), "--state-root", str(state_root), "--execute", "--workers", "1"])

    def stored() -> None:
        events = execute(state)[1:]
        require([event["status"] for event in events] == ["stored_unvalidated"] and events[0]["model_eligible"] is False, f"Events differ: {events}")
        job = next((state / "jobs").iterdir())
        require((job / "completed.json").exists() and len(list((state / "events").iterdir())) == 1, "Completion evidence missing")
        require(web.requests == 1 and any(key.endswith(".pdf") for key in client.objects), "Reference not captured and stored once")
        require(transport.SIGNED_REDIRECT_ROUTES.get(one["url"]) == one["reviewed_unsigned_redirect"], "Reviewed redirect not installed")

    scenario("execute_captures_and_stores_through_fake_s3", stored)

    def rerun() -> None:
        objects, requests = dict(client.objects), web.requests
        lines = execute(state)
        require(lines[0]["validated_historical_jobs"] == 0 and len(lines) == 1, "Completed job planned again")
        require(client.objects == objects and web.requests == requests, "Rerun downloaded or stored again")

    scenario("rerun_skips_completed_job", rerun)

    def already_completed() -> None:
        job = extend([one])[0]
        event = history_routes.run_job(job, registry, None, state, SETTINGS, OUTPUTS)
        require(event["status"] == "already_completed" and event["live_s3_rechecked"] is False, "Completed job re-executed")

    scenario("completed_job_not_reexecuted", already_completed)

    def failed_download() -> None:
        web.override = Response(b"<html>error</html>", "", 500, "text/html")
        failed = root / "failed_state"
        events = execute(failed)[1:]
        web.override = None
        require([event["status"] for event in events] == ["blocked_download"], f"Failed download recorded as {events}")
        require(not list(failed.glob("jobs/*/completed.json")), "Failed download marked complete")

    scenario("failed_download_recorded_not_completed", failed_download)

    def wrong_bucket() -> None:
        url, target = resource("e2e-wrong-bucket")
        bad = candidate("e2e-wrong-bucket", reviewed_unsigned_redirect=target.replace(BUCKET, "https://s3.amazonaws.com/another-bucket"))
        path = root / "wrong_bucket_candidates.json"
        write_once(path, encoded_json({"candidates": [bad]}))
        wrong = root / "wrong_state"
        with boundaries():
            events = run_main(history_routes, ["--candidates", str(path), "--state-root", str(wrong), "--execute", "--workers", "1"])[1:]
        require([event["status"] for event in events] == ["pending_failure"] and "previously reviewed bucket" in events[0]["reason"], f"Events: {events}")
        require(url not in transport.SIGNED_REDIRECT_ROUTES, "Unreviewed redirect installed")

    scenario("redirect_outside_reviewed_bucket_fails_job", wrong_bucket)
    held_reference_scenarios(scenario, root, extend, boundaries)


def held_reference_scenarios(scenario: Any, root: Path, extend: Any, boundaries: Any) -> None:
    """A held source's reference document is planned and stored only with a binding its hold record releases (failure modes 1 to 5)."""
    terms_sha256 = hashlib.sha256(s3_store.TERMS_ACCEPTANCE_PATH.read_bytes()).hexdigest()
    hud = candidate("e2e-hud-reference", source_id="HUD", source_ids=["HUD"], release_binding="terms")

    def planned() -> None:
        bindings = extend([hud])[0]["plan"].get("lineage_bindings")
        require(bindings == {"terms_sha256": terms_sha256}, f"Binding not carried into the plan: {bindings}")

    scenario("held_reference_with_terms_binding_planned", planned)
    scenario(
        "held_data_with_binding_rejected",
        lambda: extend([candidate("e2e-hud-data", source_id="HUD", source_ids=["HUD"], role="data", release_binding="terms")]),
        "access, privacy or large-file hold",
    )
    scenario(
        "unknown_release_binding_rejected",
        lambda: extend([candidate("e2e-hud-other", source_id="HUD", source_ids=["HUD"], release_binding="other")]),
        "release binding",
    )
    scenario("binding_on_unheld_source_rejected", lambda: extend([candidate("e2e-ca-binding", release_binding="terms")]), "release binding")

    def execute(name: str, item: dict) -> list[dict]:
        path = root / f"{name}_candidates.json"
        write_once(path, encoded_json({"candidates": [item]}))
        with boundaries():
            return run_main(history_routes, ["--candidates", str(path), "--state-root", str(root / f"{name}_state"), "--execute", "--workers", "1"])[1:]

    def stored() -> None:
        events = execute("held", hud)
        require([event["status"] for event in events] == ["stored_unvalidated"], f"Events: {events}")
        lineage = json.loads(read_json(Path(events[0]["receipt_path"]))["lineage"]["extraction_or_query"])
        require(lineage.get("terms_sha256") == terms_sha256, "Receipt lineage lacks the terms binding")

    scenario("held_reference_stored_with_terms_binding", stored)

    def unreleased() -> None:
        wrong = candidate("e2e-hud-wrong", source_id="HUD", source_ids=["HUD"], release_binding="access_release")
        events = execute("wrong_binding", wrong)
        require([event["status"] for event in events] == ["pending_failure"] and "access hold" in events[0]["reason"], f"Events: {events}")

    scenario("binding_that_does_not_release_source_refused_at_storage", unreleased)
    release_scenarios(scenario, extend, execute)


def release_scenarios(scenario: Any, extend: Any, execute: Any) -> None:
    """New receipts bind the newest release record; receipts that bind an earlier record still verify (failure modes 15 to 18)."""
    paths = s3_store.ACCESS_RELEASE_PATHS
    newest, earliest = (hashlib.sha256(path.read_bytes()).hexdigest() for path in (paths[-1], paths[0]))
    hcai = candidate("e2e-hcai-reference", source_id="HCAI_UTIL", source_ids=["HCAI_UTIL"], release_binding="access_release")
    source = {"source_id": "HCAI_UTIL"}

    def planned() -> None:
        bindings = extend([hcai])[0]["plan"].get("lineage_bindings")
        require(len(paths) == 2 and bindings == {"access_release_sha256": newest}, f"Binding is not the newest record: {bindings}")

    scenario("documentation_release_bound_to_newest_record", planned)

    def stored() -> None:
        events = execute("hcai", hcai)
        require([event["status"] for event in events] == ["stored_unvalidated"], f"Events: {events}")

    scenario("held_documentation_stored_with_access_release", stored)
    scenario(
        "earlier_release_record_still_verifies",
        lambda: require(s3_store.released_by_record(source, {"access_release_sha256": earliest}), "Earlier record no longer releases its receipts"),
    )
    scenario(
        "unlisted_release_record_refused",
        lambda: require(not s3_store.released_by_record(source, {"access_release_sha256": "0" * 64}), "Unlisted record accepted"),
    )
    scenario(
        "documentation_release_does_not_admit_data",
        lambda: extend([hcai | {"url": hcai["url"] + "?data", "role": "data"}]),
        "access, privacy or large-file hold",
    )


def main() -> None:
    """Write one immutable artifact for every E2E invocation."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="history_e2e_") as directory:
        scenarios = exercise(Path(directory))
    passed = all(s["passed"] for s in scenarios)
    report = {
        "status": "passed" if passed else "failed",
        "feature": "Historical collectors: exact redirect review, held-source planning and capture-to-S3 with completion evidence",
        "run_at_utc": datetime.now(UTC).isoformat(),
        "python": sys.version.split()[0],
        "code_sha256": bls_contract.code_hashes(),
        "dependency_sha256": bls_contract.digest(Path("requirements.txt").read_bytes()),
        "reproduce": f".venv/bin/python -m scripts.acquisition.run_history_e2e --output {args.output}",
        "boundary": (
            "Synthetic PDF and HEAD redirects served by fake openers through the real collectors; "
            "fake versioned S3 and Terraform output; no live publisher, Terraform or AWS."
        ),
        "cleanup": "Temporary folder removed on exit; the reviewed redirect table is restored.",
        "scenarios": scenarios,
    }
    write_once(args.output, encoded_json(report))
    sys.stdout.write(
        json.dumps({"status": report["status"], "passed": sum(s["passed"] for s in scenarios), "total": len(scenarios), "artifact": str(args.output)}) + "\n"
    )
    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
