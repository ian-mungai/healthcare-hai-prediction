"""Adversarial offline scenarios for the independent redownload review findings."""

import http.client
import importlib.util
import io
import json
import linecache
import sys
import urllib.error
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import patch

from scripts.acquisition import bls_api_transport
from scripts.acquisition import redownload as harness
from scripts.acquisition.s3_store import encoded_json
from scripts.acquisition.source_registry import RegistryError, read_json, require
from scripts.acquisition.tests.test_s3_store import FakeS3


def approve_fixture(run: Any) -> None:
    """A synthetic review record exists only inside this scenario's temporary base."""
    controls = harness.runtime_controls(run)
    run.queue_path.with_name("controls.json").write_bytes(encoded_json(controls))
    run.queue_path.with_name("independent_review.json").write_bytes(
        encoded_json({"status": "passed", "reviewer": "synthetic E2E fixture", "reviewed_controls_sha256": harness.digest(encoded_json(controls))})
    )


def exercise(scenario: Any, root: Path, fixture_type: Any, web_type: Any) -> None:
    """Use real harness dispatch and real request boundaries with fake publisher responses."""

    def build(label: str, private: bool = False) -> tuple[Any, dict, Any]:
        base = root / label
        base.mkdir()
        fixture, web = fixture_type(base), web_type()
        unit = fixture.stored("ALPHA", "a.csv", b"a\n1\n", flags=[harness.PRIVATE_FLAG] if private else [])
        queue = fixture.freeze()
        run = harness.Run(queue, base / "data/redownload_checks/run", base=base, opener=web, sleep=lambda _n: None)
        approve_fixture(run)
        return run, unit, web

    def gate() -> None:
        run, unit, web = build("gate")
        run.queue_path.with_name("independent_review.json").write_bytes(encoded_json({"status": "failed"}))
        web.serve(unit["url"], (200, b"a\n1\n"))
        try:
            harness.execute(run)
        except ValueError:
            require(not web.requests and not run.state_root.exists(), "Rejected review created state or sent a request")
            return
        raise ValueError("Failed independent review was accepted")

    scenario("failed_independent_review_blocks_generic_file_before_state_or_network", gate)

    def stale() -> None:
        run, _unit, _web = build("stale")
        control = run.queue_path.with_name("controls.json")
        document = read_json(control)
        document["queue_sha256"] = "changed"
        control.write_bytes(encoded_json(document))
        harness.execute(run)

    scenario("changed_execution_controls_refuse_before_dispatch", stale, "Execution controls changed")

    def code_versions_bound() -> None:
        # Failure mode S16: a code-version list changed after the review invalidates it, like any other input.
        run, _unit, _web = build("code_versions_bound")
        listed = run.base / "config/acquisition/fixture_code_versions.json"
        listed.parent.mkdir(parents=True, exist_ok=True)
        listed.write_bytes(encoded_json({"versions": [{"code_sha256": {"scripts/x.py": "0" * 64}}]}))
        harness.execute(run)

    scenario("changed_code_version_list_refuses_before_dispatch", code_versions_bound, "Execution controls changed")

    def isolation() -> None:
        run, _unit, web = build("isolation")
        run.state_root = run.base / "stored"
        try:
            harness.execute(run)
        except ValueError:
            require(not web.requests, "Old root contacted publisher")
            return
        raise ValueError("Old collection root accepted")

    scenario("old_collection_root_is_rejected", isolation)

    def symlink() -> None:
        run, _unit, web = build("symlink")
        run.state_root.mkdir(parents=True)
        (run.state_root / "files").symlink_to(run.base / "stored", target_is_directory=True)
        try:
            harness.execute(run)
        except ValueError:
            require(not web.requests, "Symlinked root contacted publisher")
            return
        raise ValueError("Symlinked staging directory accepted")

    scenario("symlinked_staging_directory_is_rejected", symlink)

    def paused_private() -> None:
        run, unit, web = build("private_pause", True)
        web.serve(unit["url"], (429, b"private partial response"))
        harness.execute(run)
        harness.execute(harness.Run(run.queue_path, run.state_root, base=run.base, opener=web))
        require(len(web.requests) == 1, "Restart retried a durable 429 pause")
        retained = read_json(run.state_root / "private_retention.json")["originals"]
        require(retained == [f"private/{unit['snapshot_id']}"], "Paused private directory omitted")

    scenario("private_429_is_retained_and_stays_paused_after_restart", paused_private)

    def partial_retry() -> None:
        run, unit, web = build("private_retry", True)
        web.serve(unit["url"], (503, b"partial"), (200, b"a\n1\n"))
        harness.execute(run)
        retained = read_json(run.state_root / "private_retention.json")
        require(retained["originals"] == [f"private/{unit['snapshot_id']}"], "Retry did not retain whole private directory")
        require(len(retained["files"]) >= 4, "Private retry files missing from exact inventory")

    scenario("all_private_retry_attempts_are_in_the_deletion_inventory", partial_retry)

    def duplicate_zip() -> None:
        path = root / "duplicate.zip"
        with zipfile.ZipFile(path, "w") as archive:
            archive.writestr("data.csv", b"changed")
            archive.writestr("data.csv", b"expected")
        harness.archive_members(path)

    scenario("duplicate_zip_member_cannot_hide_changed_content", duplicate_zip, "Duplicate ZIP member")

    def expanded_zip() -> None:
        path = root / "expanded.zip"
        with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as archive:
            archive.writestr("data.csv", b"x" * 4096)
        harness.archive_members(path, expanded_cap=100)

    scenario("zip_expansion_is_bounded_before_reading", expanded_zip, "ZIP expansion budget")

    def quota_retry() -> None:
        from scripts.acquisition import redownload_controls as controls

        run, unit, _web = build("quota")
        old = run.base / "old"
        (old / "requests").mkdir(parents=True)
        for index in range(449):
            (old / "requests" / f"{index:06}.json").write_bytes(encoded_json({"reserved_at_utc": datetime.now(UTC).isoformat()}))
        guard = controls.Boundary(run.state_root, run.base, unit["snapshot_id"], run.byte_cap)
        calls: list[int] = []

        def request(*_args: Any) -> bytes:
            calls.append(1)
            raise bls_api_transport.RetryableRequest("synthetic transient failure")

        with controls.active(guard), patch.object(bls_api_transport, "collection_lock"):
            try:
                bls_api_transport.fetch(
                    {"endpoint": unit["url"], "request_limit_24h": 450, "request_spacing_seconds": 0},
                    {"id": "batch", "series_ids": [], "start_year": 2000, "end_year": 2000},
                    run.state_root / "collectors/bls",
                    True,
                    FakeS3(),
                    request,
                    sleep=lambda _n: None,
                )
            except (controls.Pause, bls_api_transport.QuotaPause):
                require(len(calls) == 1, "Combined quota allowed request 451")
                return
        raise ValueError("Quota was not stopped")

    scenario("real_bls_fetch_at_449_does_not_send_its_retry", quota_retry)

    def durable_attempts() -> None:
        from scripts.acquisition import redownload_controls as controls

        run, unit, _web = build("attempts")
        guard = controls.Boundary(run.state_root, run.base, unit["snapshot_id"], run.byte_cap)
        guard.attempt("primary")
        guard.attempt("primary")
        controls.Boundary(run.state_root, run.base, unit["snapshot_id"], run.byte_cap).attempt("primary")

    scenario("interrupted_attempts_remain_charged_on_restart", durable_attempts, "Attempt budget exhausted")

    def bytes_on_crash() -> None:
        from scripts.acquisition import redownload_controls as controls

        run, unit, _web = build("bytes")
        guard = controls.Boundary(run.state_root, run.base, unit["snapshot_id"], 10)
        guard.allocate(8, "read")
        controls.Boundary(run.state_root, run.base, unit["snapshot_id"], 10).allocate(3, "read")

    scenario("interrupted_byte_reservations_remain_charged_on_restart", bytes_on_crash, "Byte cap would be exceeded")

    marker = "synthetic-private-value"
    # Each credential form the delta review found surviving redaction, plus home paths that are not the current user's.
    credential_forms = [
        f"Authorization: Bearer {marker}",
        f"Authorization: Basic {marker}",
        f'{{"registrationkey": "{marker}"}}',
        f"password: {marker}",
        f'api_key="{marker}"',
        f"api_key='{marker}'",
        f"https://synthetic-user:{marker}@example.invalid/x",
        f"https://example.invalid/x?signature={marker}",
        f"token={marker}",
        # Forms the rereview found surviving pattern redaction.
        f'{{"access_token": "{marker}"}}',
        f"{{'refresh_token': '{marker}'}}",
        f"SecretAccessKey: {marker}",
        f'{{"password": "before\\"{marker}"}}',
        f'api_key="before\\"{marker}"',
        f"https://example.invalid/callback#access_token={marker}",
    ]
    person = "synthetic-person"
    home_forms = [
        f"/Users/{person}/private",
        f"/home/{person}/private",
        str(Path.home()),
        f"C:\\Users\\{person}\\private",
        f"C:\\users\\{person}\\private",
        f"c:/Users/{person}/private",
        f"C:\\USERS\\{person}",
    ]

    def persisted_text(run: Any) -> str:
        """Every file the harness wrote under its state root, so no artifact escapes inspection."""
        return "\n".join(path.read_text(errors="ignore") for path in sorted(run.state_root.rglob("*")) if path.is_file())

    def no_exception_text() -> None:
        run, unit, _web = build("exceptions")

        def adapter(*_args: Any) -> Path:
            raise ValueError(" ".join(credential_forms + home_forms))

        result = harness.collector_unit(unit, run, adapter)
        require(result["reason"] == "collector_validation_failed", "Free-form exception leaked into outcome")
        require(marker not in json.dumps(result), "Synthetic secret leaked into the outcome")
        text = persisted_text(run)
        require(marker not in text, "Synthetic secret persisted in a state-root file")
        require(not any(home in text for home in home_forms), "Home path persisted in a state-root file")

    scenario("collector_exceptions_store_only_fixed_public_error_codes", no_exception_text)

    this_file = str(Path(__file__).resolve().relative_to(harness.REPO_ROOT.resolve()))

    def diagnostics_of(run: Any, unit: dict) -> list[dict]:
        return [read_json(path) for path in sorted((run.state_root / "private" / "diagnostics").glob(f"{unit['snapshot_id']}__*.json"))]

    def private_diagnostic() -> None:
        run, unit, _web = build("diagnostics")
        harness.review_gate(run)
        folder = run.state_root / "private" / "diagnostics"
        for number, form in enumerate([*credential_forms, *home_forms], 1):
            raised_at: list[int] = []

            def adapter(*_args: Any, form: str = form, raised_at: list[int] = raised_at) -> Path:
                raised_at.append(sys._getframe().f_lineno + 1)
                raise RegistryError(f"MMD code binding is incomplete {form}")

            result = harness.collector_unit(unit, run, adapter)
            require(
                result == {"attempts": 1, "outcome": "blocked", "reason": "collector_validation_failed", "error_category": "registry_contract"},
                f"Outcome fields differ: {result}",
            )
            found = sorted(folder.glob(f"{unit['snapshot_id']}__*.json"))
            require(len(found) == number and all(path.stat().st_mode & 0o077 == 0 for path in found), "Diagnostic missing or readable by others")
            text = found[-1].read_text()
            require(marker not in text, f"Diagnostic kept a credential (form {number})")
            require(person not in text and str(Path.home()) not in text, f"Diagnostic kept a home path (form {number})")
            # Failure mode 43: only fixed categories and reviewed file paths with line numbers are kept.
            diagnostic = read_json(found[-1])
            require(
                set(diagnostic) == {"error_categories", "frames", "unverified_frames"} and diagnostic["error_categories"] == ["registry_contract"],
                f"Diagnostic shape differs: {sorted(diagnostic)}",
            )
            require(all(set(frame) == {"file", "first_line", "last_line"} for frame in diagnostic["frames"]), "Diagnostic frame keeps more than a location")
            reviewed = harness.runtime_controls(run)["code_sha256"]
            require(all(frame["file"] in reviewed for frame in diagnostic["frames"]), "Diagnostic frame names a file outside the reviewed snapshot")
            sites = [frame for frame in diagnostic["frames"] if frame["file"] == this_file and frame["first_line"] == raised_at[-1]]
            require(len(sites) == 1, "Diagnostic lost the raising code location")
        require(marker not in persisted_text(run) and person not in persisted_text(run), "Synthetic secret or home path persisted in a state-root file")
        retention = read_json(run.state_root / "private_retention.json")
        listed = {item["path"] for item in retention["files"]}
        expected = {str(path.relative_to(run.state_root)) for path in folder.glob("*.json")}
        require("private/diagnostics" in retention["originals"] and expected <= listed, "Diagnostics missing from the retention record")

        def chained(*_args: Any) -> Path:
            secret = f"password={marker}"
            try:
                raise KeyError(secret)
            except KeyError as cause:
                raise ValueError(secret) from cause

        harness.collector_unit(unit, run, chained)
        latest = diagnostics_of(run, unit)[-1]
        require(latest["error_categories"] == ["validation", "missing_key"] and marker not in json.dumps(latest), "Cause chain differs or kept a secret")

    scenario("collector_exception_diagnostic_keeps_only_categories_and_reviewed_locations", private_diagnostic)

    def runtime_strings() -> None:
        # Failure modes 43-45: cached source, function names and class names are run-time strings, never kept.
        run, unit, _web = build("runtime_strings")
        harness.review_gate(run)
        named = type(marker, (ValueError,), {})

        def adapter(*_args: Any) -> Path:
            raise named("ignored")

        adapter.__code__ = adapter.__code__.replace(co_name=marker)
        source = str(Path(__file__).resolve())
        saved = linecache.cache.get(source)
        lines = [f"{marker}\n"] * 5000
        linecache.cache[source] = (len("".join(lines)), None, lines, source)
        try:
            result = harness.collector_unit(unit, run, adapter)
        finally:
            if saved is None:
                linecache.cache.pop(source, None)
            else:
                linecache.cache[source] = saved
        require(result.get("error_category") == "validation" and "error_type" not in result, f"Outcome category differs: {result}")
        require(marker not in json.dumps(result) and marker not in persisted_text(run), "A run-time string persisted in a state-root file")
        require(diagnostics_of(run, unit)[-1]["error_categories"] == ["validation"], "Dynamically named subclass did not map to its base category")

        class Unlisted(Exception):
            pass

        def unlisted(*_args: Any) -> Path:
            raise KeyError("x") from Unlisted(marker)

        harness.collector_unit(unit, run, unlisted)
        require(diagnostics_of(run, unit)[-1]["error_categories"] == ["missing_key", "other"], "Unlisted exception class did not map to the constant category")

    scenario("runtime_source_function_and_class_names_never_persist", runtime_strings)

    def transport_names() -> None:
        # Failure mode 47: the shared downloader records fixed failure codes, never a run-time class name.
        cases = [
            ("os", OSError, "OSError", 1),
            ("refused", ConnectionRefusedError, "OSError", 1),
            # The downloader reads an HTTPError as a response; one with no body fails the response checks.
            ("http", urllib.error.HTTPError, "CaptureError", 1),
            ("url", urllib.error.URLError, "URLError", 2),
            ("timeout", TimeoutError, "TimeoutError", 2),
            ("reset", ConnectionResetError, "ConnectionResetError", 2),
        ]
        for label, base_type, code, attempts in cases:
            run, unit, web = build(f"transport_{label}")
            named = type(marker, (base_type,), {})
            error = named("https://x.example", 599, "x", None, None) if base_type is urllib.error.HTTPError else named("x")

            def raising(request: Any, timeout: float, error: BaseException = error, web: Any = web) -> Any:
                web.requests.append(request.full_url)
                raise error

            web.open = raising
            harness.execute(run)
            result = harness.outcome(run, unit["snapshot_id"]) or {}
            require(result.get("reason") == code and result.get("attempts") == attempts, f"Transport outcome differs for {label}: {result}")
            failures = {read_json(path)["failure"] for path in run.state_root.rglob("transport.json")}
            require(failures == {code}, f"transport.json failure differs for {label}: {failures}")
            require(marker not in persisted_text(run), f"A run-time class name persisted in a state-root file ({label})")
        run, unit, _web = build("transport_unknown")
        forged = type("Download", (), {"complete": False, "failure": marker, "sha256": "0" * 64, "byte_count": 0, "path": run.state_root / "x"})()
        result = harness.compare_download(forged, 1, "0" * 64, run)
        require(result["reason"] == "unrecognised_failure", f"Unknown failure code reached the outcome: {result}")

    scenario("generic_download_exceptions_store_only_fixed_failure_codes", transport_names)

    def unverified_frames() -> None:
        # Failure mode 43: a frame counts only when its file is in the reviewed snapshot with unchanged bytes.
        run, unit, _web = build("unverified_frames")

        def adapter(*_args: Any) -> Path:
            raise RegistryError("x")

        harness.collector_unit(unit, run, adapter)
        first = diagnostics_of(run, unit)[-1]
        require(not first["frames"] and first["unverified_frames"] > 0, "Frames were kept without a reviewed snapshot")
        harness.review_gate(run)
        run.cache["controls"] = run.cache["controls"] | {"code_sha256": run.cache["controls"]["code_sha256"] | {this_file: "0" * 64}}
        harness.collector_unit(unit, run, adapter)
        changed = diagnostics_of(run, unit)[-1]
        require(
            all(frame["file"] != this_file for frame in changed["frames"]) and changed["unverified_frames"] > 0,
            "A file whose bytes differ from review was kept",
        )
        outside = root / "unverified_frames" / "scripts" / "outside.py"
        outside.parent.mkdir(parents=True)
        outside.write_text("def fail():\n    raise ValueError('x')\n")
        spec = importlib.util.spec_from_file_location("synthetic_outside", outside)
        if spec is None or spec.loader is None:
            raise ValueError("Synthetic module did not load")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        # The file changes after import, as in the review's probe.
        outside.write_text(f"def fail():\n    raise ValueError('{marker}')\n")
        harness.collector_unit(unit, run, lambda *_args: module.fail())
        latest = diagnostics_of(run, unit)[-1]
        require(all(frame["file"] in run.cache["controls"]["code_sha256"] for frame in latest["frames"]), "A file outside the reviewed snapshot was kept")
        require(marker not in persisted_text(run) and "outside.py" not in persisted_text(run), "Text from a file outside review persisted")

    scenario("frames_outside_or_changed_since_review_are_only_counted", unverified_frames)

    def receipt_reference() -> None:
        # Failure mode 46: receipt references are state-root relative, and a receipt outside the root is refused.
        run, unit, _web = build("receipt_reference")
        stored = read_json(run.base / unit["receipt"])

        def inside(*_args: Any) -> Path:
            path = run.state_root / "collectors" / "x" / "receipt.json"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(encoded_json({"artifacts": stored["artifacts"]}))
            return path

        result = harness.collector_unit(unit, run, inside)
        require(result["fresh_receipt"] == "collectors/x/receipt.json", f"Receipt reference is not state-root relative: {result['fresh_receipt']}")

        def outside(*_args: Any) -> Path:
            path = run.base / "elsewhere" / "receipt.json"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(encoded_json({"artifacts": stored["artifacts"]}))
            return path

        try:
            harness.collector_unit(unit, run, outside)
        except ValueError:
            return
        raise ValueError("A receipt outside the state root was accepted")

    scenario("fresh_receipt_references_are_state_root_relative", receipt_reference)

    def run_records() -> None:
        # Failure modes 48-49: each run's controls and review sit beside its own queue; run 1 keeps its original files.
        repo = harness.REPO_ROOT
        run1 = harness.Run(repo / "data/redownload_checks/20260929/queue.json", repo / "data/redownload_checks/20260929/run", base=repo)
        run2 = harness.Run(repo / "data/redownload_checks/20261001/queue.json", repo / "data/redownload_checks/20261001/run", base=repo)
        synthetic, _unit, _web = build("run_records")
        records = getattr(harness, "run_records", None)
        if records is None:
            raise ValueError("The harness has no per-run controls and review paths")
        require(
            records(run1) == (run1.queue_path.with_name("controls.json"), repo / "data/acquisition_planning/full_redownload_20260929/independent_review.json"),
            "Run 1 records moved",
        )
        require(
            records(run2) == (run2.queue_path.with_name("controls.json"), run2.queue_path.with_name("independent_review.json")),
            "Run 2 does not use its own records",
        )
        require(records(synthetic)[1] == synthetic.queue_path.with_name("independent_review.json"), "Synthetic review path changed")
        require(records(run2)[1] != records(run1)[1], "Run 2 would inherit run 1's review")

    scenario("each_run_reads_its_own_controls_and_review", run_records)

    def scoped(label: str) -> tuple[Any, dict, dict, Any]:
        """Two sources in the queue; the run's scope file approves only ALPHA (failure modes 52-54)."""
        base = root / label
        base.mkdir()
        fixture, web = fixture_type(base), web_type()
        alpha = fixture.stored("ALPHA", "a.csv", b"a\n1\n")
        beta = fixture.stored("BETA", "b.csv", b"b\n1\n")
        queue = fixture.freeze()
        queue.with_name("scope.json").write_bytes(encoded_json({"sources": ["ALPHA"]}))
        run = harness.Run(queue, base / "data/redownload_checks/run", base=base, opener=web, sleep=lambda _n: None)
        approve_fixture(run)
        web.serve(alpha["url"], (200, b"a\n1\n"))
        web.serve(beta["url"], (200, b"b\n1\n"))
        return run, alpha, beta, web

    def scope_default() -> None:
        run, alpha, beta, web = scoped("scope_default")
        require(harness.runtime_controls(run).get("scope_sources") == ["ALPHA"], "Scope is not bound into the controls")
        preview = harness.execute(run, allow_network=False)
        require(preview.get("planned") == 1, f"Dry run ignores the scope: {preview}")
        harness.execute(run)
        require(web.requests == [alpha["url"]] and harness.outcome(run, beta["snapshot_id"]) is None, f"Out-of-scope source dispatched: {web.requests}")

    scenario("run_scope_is_the_default_selection_for_dispatch_and_preview", scope_default)

    def scope_refusal() -> None:
        run, _alpha, _beta, web = scoped("scope_refusal")
        try:
            harness.execute(run, sources={"BETA"})
        except ValueError:
            require(not web.requests and not run.state_root.exists(), "Refused selection created state or sent a request")
            return
        raise ValueError("A source outside the approved scope was accepted")

    scenario("source_outside_the_run_scope_is_refused_before_state_or_network", scope_refusal)

    def scope_changed() -> None:
        run, _alpha, _beta, _web = scoped("scope_changed")
        run.queue_path.with_name("scope.json").write_bytes(encoded_json({"sources": ["ALPHA", "BETA"]}))
        harness.execute(run)

    scenario("scope_changed_after_review_is_refused", scope_changed, "Execution controls changed")

    def transient_scope(pilot: bool) -> None:
        # Failure mode 56: an expansion restored before the review gate must not widen dispatch.
        run, alpha, beta, web = scoped(f"transient_scope_{'pilot' if pilot else 'full'}")
        scope_file = run.queue_path.with_name("scope.json")
        scope_file.write_bytes(encoded_json({"sources": ["ALPHA", "BETA"]}))
        original = harness.review_gate

        def gate_after_restore(gated: Any) -> str:
            scope_file.write_bytes(encoded_json({"sources": ["ALPHA"]}))
            return original(gated)

        with patch.object(harness, "review_gate", gate_after_restore):
            harness.execute(run, pilot=pilot)
        require(web.requests == [alpha["url"]] and harness.outcome(run, beta["snapshot_id"]) is None, f"Transient scope widened dispatch: {web.requests}")

    scenario("transient_scope_expansion_cannot_widen_full_dispatch", lambda: transient_scope(False))
    scenario("transient_scope_expansion_cannot_widen_pilot_dispatch", lambda: transient_scope(True))

    def preview_scope() -> None:
        # Failure mode 57: with frozen controls, a preview refuses an expanded or missing scope file.
        run, _alpha, _beta, _web = scoped("preview_scope")
        scope_file = run.queue_path.with_name("scope.json")
        refused = 0
        for change in ("expand", "delete"):
            if change == "expand":
                scope_file.write_bytes(encoded_json({"sources": ["ALPHA", "BETA"]}))
            else:
                scope_file.unlink()
            try:
                harness.execute(run, allow_network=False)
            except ValueError:
                refused += 1
        require(refused == 2, f"A preview accepted a scope that differs from the frozen controls ({refused} of 2 refused)")

    scenario("preview_refuses_a_scope_that_differs_from_the_frozen_controls", preview_scope)

    def caller_widens(pilot: bool) -> None:
        # Failure mode 59: a caller-owned list widened after the check (here by the progress log) must not dispatch.
        run, alpha, beta, web = scoped(f"caller_widens_{'pilot' if pilot else 'full'}")
        chosen = {"ALPHA"}

        def widen(_event: dict) -> None:
            chosen.add("BETA")

        chosen.add("ALPHA")
        harness.execute(run, pilot=pilot, sources=chosen, log=widen)
        harness.execute(run, pilot=pilot, sources={"ALPHA"})
        require(web.requests == [alpha["url"]] and harness.outcome(run, beta["snapshot_id"]) is None, f"A widened caller list dispatched: {web.requests}")

    scenario("caller_widened_list_cannot_dispatch_in_full_run", lambda: caller_widens(False))
    scenario("caller_widened_list_cannot_dispatch_in_pilot", lambda: caller_widens(True))

    def resume_scope() -> None:
        # Failure mode 60: resume refuses a source outside the reviewed scope, even with a pending outcome.
        run, _alpha, beta, _web = scoped("resume_scope")
        harness.execute(run)
        record = {"snapshot_id": beta["snapshot_id"], "source_id": "BETA", "outcome": "changed_needs_review"}
        (run.state_root / "outcomes" / f"{beta['snapshot_id']}.json").write_bytes(encoded_json(record))
        try:
            harness.resume_source(run, "BETA", "synthetic decision")
        except ValueError:
            require(not list((run.state_root / "resumes").glob("BETA__*.json")), "A refused resume still wrote a record")
            return
        raise ValueError("Resume accepted a source outside the reviewed scope")

    scenario("resume_refuses_a_source_outside_the_reviewed_scope", resume_scope)

    def report_binding() -> None:
        # Failure mode 61: a report refuses a state root owned by another run's controls and writes nothing there.
        run, _alpha, _beta, _web = scoped("report_binding")
        harness.execute(run)
        second_queue = run.queue_path.parent / "second" / "queue.json"
        second_queue.parent.mkdir()
        for name in ("queue.json", "queue.lock.json", "scope.json"):
            second_queue.with_name(name).write_bytes(run.queue_path.with_name(name).read_bytes())
        second = harness.Run(second_queue, run.base / "data/redownload_checks/second_run", base=run.base, opener=run.opener, sleep=lambda _n: None)
        approve_fixture(second)
        before = sorted(path.relative_to(run.state_root) for path in run.state_root.rglob("*"))
        crossed = harness.Run(second_queue, run.state_root, base=run.base)
        refused = False
        try:
            harness.write_report(crossed)
        except ValueError:
            refused = True
        after = sorted(path.relative_to(run.state_root) for path in run.state_root.rglob("*"))
        require(refused and before == after, "A report used another run's state root or wrote into it")
        own = harness.Run(run.queue_path, run.state_root, base=run.base)
        document, name = harness.write_report(own)
        require(document["active_units"] == 2 and (run.state_root / "reports" / name).is_file(), "A same-run report no longer works")

    scenario("report_refuses_a_state_root_owned_by_another_run", report_binding)

    def second_run(label: str, frozen: bool = True) -> tuple[Any, Any]:
        """A first run with outcomes, and a second queue byte-identical to it with its own controls and no state yet."""
        run, _alpha, _beta, _web = scoped(label)
        harness.execute(run)
        second_queue = run.queue_path.parent / "second" / "queue.json"
        second_queue.parent.mkdir()
        for name in ("queue.json", "queue.lock.json", "scope.json"):
            second_queue.with_name(name).write_bytes(run.queue_path.with_name(name).read_bytes())
        second = harness.Run(second_queue, run.base / "data/redownload_checks/second_run", base=run.base)
        if frozen:
            approve_fixture(second)
        return run, second

    def tree(root: Path) -> dict[str, str]:
        return {str(path.relative_to(root)): harness.digest(path.read_bytes()) for path in root.rglob("*") if path.is_file()}

    def preview_binding() -> None:
        # Failure mode 62: a preview reads persisted state only from the root the queue's frozen controls bind.
        run, second = second_run("preview_binding")
        before = tree(run.state_root)
        refused = []
        for root in (run.state_root, run.base / "data/redownload_checks/third_run"):
            try:
                harness.execute(harness.Run(second.queue_path, root, base=run.base), allow_network=False)
            except ValueError:
                refused.append(root.name)
        output = io.StringIO()
        original = harness.Run
        argv = ["redownload", "--queue", str(second.queue_path), "--state-root", str(run.state_root)]
        with (
            patch.object(harness, "Run", side_effect=lambda *a, **kw: original(*a, **dict(kw, base=run.base))),
            patch.object(sys, "argv", argv),
            patch.object(sys, "stdout", output),
        ):
            try:
                harness.main()
            except ValueError:
                refused.append("command_line")
        require(refused == ["run", "third_run", "command_line"], f"A preview read a root its queue's controls do not bind: refused {refused}")
        own = harness.execute(second, allow_network=False)
        require(own.get("planned") == 1 and not second.state_root.exists(), f"A fresh own-root preview changed: {own}")
        require(harness.execute(run, allow_network=False).get("planned") == 0, "A same-run preview no longer reads its own outcomes")
        require(tree(run.state_root) == before and not output.getvalue(), "A refused preview wrote state or printed a plan")

    scenario("preview_refuses_a_state_root_owned_by_another_run", preview_binding)

    def unfrozen_preview_binding() -> None:
        # Failure mode 62: without frozen controls nothing proves which run owns an existing root, so it is refused.
        run, second = second_run("unfrozen_preview_binding", frozen=False)
        try:
            harness.execute(harness.Run(second.queue_path, run.state_root, base=run.base), allow_network=False)
        except ValueError:
            require(harness.execute(second, allow_network=False).get("planned") == 1, "A fresh unfrozen preview no longer works")
            return
        raise ValueError("An unfrozen preview read an existing state root it cannot prove it owns")

    scenario("unfrozen_preview_refuses_an_existing_owned_root", unfrozen_preview_binding)

    def report_reader_binding() -> None:
        # Failure mode 63: the report reader itself refuses a foreign root before reading anything.
        run, second = second_run("report_reader_binding")
        try:
            harness.report(harness.Run(second.queue_path, run.state_root, base=run.base))
        except ValueError:
            own = harness.report(harness.Run(run.queue_path, run.state_root, base=run.base))
            require(own["outcomes"] == {"exact_match": 1}, f"A same-run report no longer reads its outcomes: {own['outcomes']}")
            return
        raise ValueError("A direct report call aggregated another run's outcomes")

    scenario("report_reader_refuses_a_state_root_owned_by_another_run", report_reader_binding)

    def cli_default_root() -> None:
        # Failure mode 58: drive the command line with --queue only and check the root it resolves.
        run, _alpha, _beta, _web = scoped("cli_default_root")
        # The fixture's queue sits outside data/redownload_checks, so freeze controls that bind the default root (failure mode 62).
        approve_fixture(harness.Run(run.queue_path, run.queue_path.with_name("run"), base=run.base))
        seen: list[Any] = []
        original = harness.execute

        def capture(cli_run: Any, **options: Any) -> dict:
            seen.append(cli_run)
            local = harness.Run(cli_run.queue_path, cli_run.state_root, base=run.base, opener=run.opener, sleep=lambda _n: None)
            return original(local, **{key: value for key, value in options.items() if key != "log"})

        output = io.StringIO()
        with (
            patch.object(harness, "execute", capture),
            patch.object(sys, "argv", ["redownload", "--queue", str(run.queue_path)]),
            patch.object(sys, "stdout", output),
        ):
            harness.main()
        require(
            len(seen) == 1 and seen[0].state_root == run.queue_path.with_name("run"), "The command line does not default to the run folder beside the queue"
        )
        require(json.loads(output.getvalue().strip().splitlines()[-1]).get("planned") == 1, f"CLI preview count differs: {output.getvalue()[-200:]}")

    scenario("command_line_defaults_to_the_run_folder_beside_the_queue", cli_default_root)

    def mmd_binding() -> None:
        from scripts.acquisition import collect_mmd_api, mmd_api_contract

        # The fingerprint the collector writes today must pass the real verify_capture; failure modes 29 and 42.
        class PastBinding(Exception):
            """Raised by the first step after the code binding checks, so reaching it proves the binding was accepted."""

        current = collect_mmd_api.current_code_hashes()
        historical = dict.fromkeys(mmd_api_contract.CODE_FILES, "0" * 64)

        def verify(code_sha256: dict) -> str:
            lineage = {
                "plan_sha256": mmd_api_contract.PLAN_SHA256,
                "mode": "mmd_api",
                "model_eligible": False,
                "review_decision": "hold_synthetic",
                "code_sha256": code_sha256,
                "year": 2023,
                "measure_id": "SYNTHETIC",
            }

            def past(*_args: Any) -> dict:
                raise PastBinding

            # Synthetic approval in memory only: the production catalog is never read or written.
            with (
                patch.object(mmd_api_contract, "load_plan", lambda _sha: {}),
                patch.object(mmd_api_contract, "reviewed_code_versions", lambda: [current, historical]),
                patch.object(mmd_api_contract, "condition_for", past),
            ):
                try:
                    mmd_api_contract.verify_capture({}, {"source_id": "MMD"}, lineage, root, evidence_only=False)
                except PastBinding:
                    return "accepted"
                except RegistryError as error:
                    return str(error)
            return "no error"

        require(verify(current) == "accepted", f"verify_capture rejects the current MMD fingerprint: {verify(current)}")
        require(verify(historical) == "accepted", "verify_capture rejects the historical MMD fingerprint")
        for changed in ({}, dict(list(historical.items())[1:]), historical | {"scripts/acquisition/extra.py": "0" * 64}):
            require(verify(changed) == "MMD code binding is incomplete", "An incomplete or widened MMD fingerprint was accepted")
        unreviewed = dict.fromkeys(current, "1" * 64)
        require(verify(unreviewed) == "MMD capture implementation changed; review before reuse", "An unreviewed MMD fingerprint was accepted")

    scenario("mmd_collector_fingerprint_is_accepted_by_verify_capture", mmd_binding)

    def live_rate_limit(mode: str) -> None:
        from scripts.acquisition import census_acs_api_transport, collect_census_acs_detailed, collect_mmd_api, hud_api_transport
        from scripts.acquisition import redownload_controls as controls

        run, unit, _web = build(f"live_429_{mode}")
        guard = controls.Boundary(run.state_root, run.base, unit["snapshot_id"], run.byte_cap)
        calls: list[int] = []

        class Response(io.BytesIO):
            status = 429

            def getheader(self, _name: str, default: str = "") -> str:
                return default

        class Connection:
            def __init__(self, *_args: Any, **_options: Any) -> None:
                pass

            def request(self, *_args: Any, **_options: Any) -> None:
                calls.append(1)

            def getresponse(self) -> Response:
                return Response(b"private response must not be read")

            def close(self) -> None:
                pass

        client = FakeS3()
        client.overrides["get-secret-value"] = {"SecretString": json.dumps({"api_key": "s" * 24})}
        with controls.active(guard), patch.object(http.client, "HTTPSConnection", Connection):
            try:
                if mode == "bls":
                    bls_api_transport.fetch(
                        {
                            "endpoint": "https://api.bls.gov/publicAPI/v2/timeseries/data/",
                            "credential_reference": "bls_api_key",
                            "request_limit_24h": 450,
                            "request_spacing_seconds": 0,
                        },
                        {"id": "batch", "series_ids": [], "start_year": 2000, "end_year": 2000},
                        run.state_root,
                        True,
                        client,
                        sleep=lambda _n: None,
                    )
                elif mode == "census":
                    census_acs_api_transport.fetch(
                        {"request_spacing_seconds": 0, "endpoint": census_acs_api_transport.contract.ENDPOINT, "credential_reference": "census_api_key"},
                        {"id": "batch", "table": "DP02"},
                        run.state_root,
                        True,
                        client,
                        sleep=lambda _n: None,
                    )
                elif mode == "detailed":
                    collect_census_acs_detailed.fetch(
                        {"request_spacing_seconds": 0, "credential_reference": "census_api_key"},
                        {"id": "batch", "table": "B17001", "year": 2010},
                        run.state_root,
                        True,
                        client,
                        None,
                        sleep=lambda _n: None,
                    )
                elif mode == "hud":
                    hud_api_transport.fetch(
                        {"request_spacing_seconds": 0, "endpoint": hud_api_transport.contract.ENDPOINT, "credential_reference": "hud_api_key"},
                        {"id": "batch", "type": 2, "query": "All", "year": 2021, "quarter": 1},
                        run.state_root,
                        True,
                        client,
                        sleep=lambda _n: None,
                    )
                else:
                    collect_mmd_api.fetch(run.state_root, "primary", "https://data.cms.gov/data-api/v1/mmd-tool/?year=2020", True)
            except controls.Pause:
                require(len(calls) == 1, "Real live API retried 429")
                require(not list((run.state_root / ".budgets").glob("*.settled.json")), "Rate-limit response body was read")
                return
        raise ValueError("Real live API did not pause")

    for mode in ("bls", "census", "detailed", "hud", "mmd"):
        scenario(f"real_{mode}_live_transport_pauses_before_retry_or_body", lambda mode=mode: live_rate_limit(mode))

    def detailed_attempt_cap() -> None:
        from scripts.acquisition import collect_census_acs_detailed as collector
        from scripts.acquisition import redownload_controls as controls

        run, unit, _web = build("detailed_attempts")
        calls: list[int] = []

        def failing(*_args: Any) -> bytes:
            calls.append(1)
            raise bls_api_transport.RetryableRequest("synthetic transient error")

        guard = controls.Boundary(run.state_root, run.base, unit["snapshot_id"], run.byte_cap)
        with controls.active(guard), patch.object(collector, "live_request", failing):
            try:
                collector.fetch({"request_spacing_seconds": 0}, {"id": "primary"}, run.state_root, True, FakeS3(), None, sleep=lambda _n: None)
            except controls.BudgetExceeded:
                require(len(calls) == 2, "Default detailed Census made its native third request")
                return
        raise ValueError("Detailed attempt cap did not refuse")

    scenario("real_detailed_census_default_request_has_a_durable_two_attempt_cap", detailed_attempt_cap)

    def artifact_identity() -> None:
        run, unit, _web = build("artifact_identity")
        stored_path = run.base / unit["receipt"]
        stored = read_json(stored_path)
        stored["artifacts"].append(stored["artifacts"][0] | {"stored_file_name": "b.csv", "sha256": harness.digest(b"b\n2\n")})
        stored_path.write_bytes(encoded_json(stored))

        def swapped(*_args: Any) -> Path:
            path = run.state_root / "collector.json"
            path.parent.mkdir(parents=True, exist_ok=True)
            artifacts = [
                stored["artifacts"][0] | {"sha256": stored["artifacts"][1]["sha256"]},
                stored["artifacts"][1] | {"sha256": stored["artifacts"][0]["sha256"]},
            ]
            path.write_bytes(encoded_json({"artifacts": artifacts}))
            return path

        result = harness.collector_unit(unit, run, swapped)
        require(
            result["outcome"] == "changed_needs_review" and result["differing_data_files"] == ["a.csv", "b.csv"], "Swapped artifacts passed by hash multiset"
        )

    scenario("swapped_equal_hash_multiset_is_a_changed_capture", artifact_identity)

    def operational_job() -> None:
        run, unit, web = build("operational_job")
        job = (run.base / unit["receipt"]).parent / "job.json"
        job.write_bytes(encoded_json({"plan": {"expected_format": "csv"}}))
        approve_fixture(run)
        job.write_bytes(encoded_json({"plan": {"expected_format": "zip"}}))
        try:
            harness.execute(run)
        except ValueError:
            require(not web.requests and not run.state_root.exists(), "Changed operational job sent a request")
            return
        raise ValueError("Changed operational job accepted")

    scenario("operational_job_change_invalidates_independent_review", operational_job)

    def response_cap() -> None:
        from scripts.acquisition import redownload_controls as controls

        run, unit, _web = build("response_cap")
        guard = controls.Boundary(run.state_root, run.base, unit["snapshot_id"], 8)
        with controls.active(guard):
            raw = controls.read(io.BytesIO(b"123456789"), 32)
            require(raw == b"12345678", "Read exceeded remaining byte budget")
            controls.writing(run.state_root / "cache.json", len(raw))

    scenario("received_collector_bytes_and_cache_copies_share_one_cap", response_cap, "Byte cap would be exceeded")

    def private_collector_429(mode: str) -> None:
        from scripts.acquisition import redownload_controls as controls
        from scripts.acquisition import store_cms_owners, store_hcai_util, store_onc_mu
        from scripts.acquisition.transport import Limits

        run, unit, web = build(f"private_429_{mode}")
        unit["mode"] = mode
        web.serve(unit["url"], (429, b"private rate limit response"))
        bounds = Limits(attempts=2)

        def adapter(_unit: dict, _run: Any) -> Path:
            top = harness.fresh_root(unit, run)
            if mode == "cms_owners_org":
                store_cms_owners.fetch({"id": "release", "url": unit["url"], "period_start": "2020-01-01"}, top, True, web, bounds)
            elif mode == "hcai_util_workbook":
                with patch.object(store_hcai_util, "install_redirect"):
                    store_hcai_util.fetch({"year": 2020, "url": unit["url"], "redirect_target": "synthetic"}, top, True, web, bounds)
            else:
                store_onc_mu.fetch({"url": unit["url"]}, top, True, web, bounds)
            raise ValueError("Private 429 did not pause")

        with controls.active(controls.Boundary(run.state_root, run.base, unit["snapshot_id"], run.byte_cap)):
            try:
                harness.collector_unit(unit, run, adapter)
            except controls.Pause:
                require(len(web.requests) == 1, "Private collector retried 429")
                retained = read_json(run.state_root / "private_retention.json")["originals"]
                require(retained == [f"collectors/{mode}/private_original"], "Private collector pause not retained")
                return
        raise ValueError("Private collector rate-limit pause missing")

    for mode in ("cms_owners_org", "hcai_util_workbook", "onc_mu_hospital"):
        scenario(f"real_{mode}_429_pauses_once_and_finalizes_private_retention", lambda mode=mode: private_collector_429(mode))

    def mmd_browser_identity() -> None:
        run, unit, _web = build("mmd_browser_identity")
        unit["flags"] = [harness.MMD_API_FLAG]
        stored_path = run.base / unit["receipt"]
        stored = read_json(stored_path)
        stored["artifacts"][0]["stored_file_name"] = "mmd_data.csv"
        stored_path.write_bytes(encoded_json(stored))

        def reconstructed(_unit: dict, _run: Any) -> Path:
            path = run.state_root / "receipt.json"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(
                encoded_json(
                    {"artifacts": [{"role": "data", "stored_file_name": "mmd_ffs_county_c258_01_prevalence_2023.csv", "sha256": harness.digest(b"a\n1\n")}]}
                )
            )
            return path

        with patch.object(harness, "mmd_selection", return_value=("C258.01", 2023)):
            result = harness.collector_unit(unit, run, reconstructed)
        require(result["outcome"] == "exact_match", "Approved MMD browser-to-API mapping rejected equal derived content")

    scenario("approved_mmd_browser_api_mapping_uses_the_same_measure_year_identity", mmd_browser_identity)

    def all_mmd_browser_identities() -> None:
        # This is the approved frozen metadata contract, covering all three published layouts.
        run, unit, _web = build("all_mmd_browser_identities")
        unit["flags"] = [harness.MMD_API_FLAG]
        cases = [("C258.01", 2023, "mmd_data.csv")]
        cases += [("C258.01", year, f"mmd_ffs_county_ami_prevalence_{year}.csv") for year in range(2012, 2023)]
        for measure, years in (
            ("C258.02", range(2012, 2024)),
            ("C258.03", range(2022, 2024)),
            ("C258.04", range(2012, 2024)),
            ("C258.05", range(2012, 2024)),
            ("C258.06", range(2022, 2024)),
        ):
            cases += [(measure, year, f"mmd_ffs_county_{measure.lower().replace('.', '_')}_prevalence_{year}.csv") for year in years]
        require(len(cases) == 52, "Frozen MMD metadata contract changed")

        def compare(case_unit: dict, case_run: Any, measure: str, year: int) -> dict:
            old = harness.stored_receipt(case_unit, case_run)
            canonical = f"mmd_ffs_county_{measure.lower().replace('.', '_')}_prevalence_{year}.csv"

            def reconstructed(_unit: dict, _run: Any) -> Path:
                target = run.state_root / "receipt.json"
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(encoded_json({"artifacts": [{"role": "data", "stored_file_name": canonical, "sha256": old["artifacts"][0]["sha256"]}]}))
                return target

            return harness.collector_unit(case_unit, case_run, reconstructed)

        for measure, year, name in cases:
            old_path = run.base / unit["receipt"]
            old = read_json(old_path)
            old["artifacts"][0]["stored_file_name"] = name
            old_path.write_bytes(encoded_json(old))
            with patch.object(harness, "mmd_selection", return_value=(measure, year)):
                result = compare(unit, run, measure, year)
            require(result["outcome"] == "exact_match", "Approved frozen MMD identity failed comparison")
        # On the acquisition Mac also verify actual immutable receipt metadata, without source bodies.
        frozen = harness.REPO_ROOT / "data/redownload_checks/20260929/queue.json"
        if frozen.is_file():
            real_run = harness.Run(frozen, run.state_root, base=harness.REPO_ROOT)
            mapped = [item for item in harness.load_queue(real_run) if any(flag.startswith(harness.MMD_API_FLAG) for flag in item["flags"])]
            require(len(mapped) == 52, "Actual frozen MMD scope changed")
            for item in mapped:
                measure, year = harness.mmd_selection(item, real_run)
                require(compare(item, real_run, measure, year)["outcome"] == "exact_match", "Actual frozen MMD receipt failed comparison")

    scenario("all_52_frozen_mmd_browser_receipt_identities_compare_without_network", all_mmd_browser_identities)

    def unapproved_mmd_name() -> None:
        run, unit, _web = build("unapproved_mmd_name")
        unit["flags"] = [harness.MMD_API_FLAG]
        path = run.base / unit["receipt"]
        old = read_json(path)
        old["artifacts"][0]["stored_file_name"] = "unapproved.csv"
        path.write_bytes(encoded_json(old))
        with patch.object(harness, "mmd_selection", return_value=("C258.01", 2023)):
            harness.collector_unit(unit, run, lambda *_args: (_ for _ in ()).throw(ValueError("Adapter should not run")))

    scenario("unapproved_mmd_browser_filename_is_rejected_before_adapter", unapproved_mmd_name, "MMD browser artifact")

    def mmd_template_changed() -> None:
        from scripts.acquisition.collect_mmd_api import TEMPLATE

        run, unit, web = build("mmd_template_changed")
        template = run.base / TEMPLATE
        template.parent.mkdir(parents=True, exist_ok=True)
        template.write_bytes(encoded_json({"metadata": "original"}))
        approve_fixture(run)
        template.write_bytes(encoded_json({"metadata": "changed"}))
        web.serve(unit["url"], (200, b"a\n1\n"))
        try:
            harness.execute(run)
        except ValueError:
            require(not web.requests and not run.state_root.exists(), "Changed MMD template was used before review")
            return
        raise ValueError("MMD template was not bound to review")

    scenario("mmd_template_metadata_change_invalidates_review", mmd_template_changed)

    def alternate_state_root() -> None:
        run, unit, web = build("alternate_state")
        web.serve(unit["url"], (200, b"a\n1\n"))
        run.state_root = run.state_root.with_name("another_run")
        try:
            harness.execute(run)
        except ValueError:
            require(not web.requests and not run.state_root.exists(), "Different root reset the frozen run")
            return
        raise ValueError("Alternate state root was accepted without an amended review")

    scenario("changing_state_root_requires_an_amended_execution_review", alternate_state_root)

    def unsafe_report_root() -> None:
        run, _unit, _web = build("unsafe_report")
        run.state_root = run.base / "stored"
        with (
            patch.object(harness, "Run", return_value=run),
            patch("sys.argv", ["redownload", "--queue", str(run.queue_path), "--state-root", str(run.state_root), "--report"]),
        ):
            try:
                harness.main()
            except ValueError:
                require(not (run.state_root / "reports").exists(), "Report mutated an original collection")
                return
        raise ValueError("Report wrote outside an owned redownload root")

    scenario("report_cli_cannot_write_into_an_original_collection", unsafe_report_root)

    def launcher_change() -> None:
        run, unit, web = build("launcher_change")
        web.serve(unit["url"], (200, b"a\n1\n"))
        original_read = Path.read_bytes
        launcher = harness.REPO_ROOT / "scripts/process.py"

        def read_changed(path: Path) -> bytes:
            return original_read(path) + b"\n# synthetic changed launcher\n" if path == launcher else original_read(path)

        with patch.object(Path, "read_bytes", read_changed):
            try:
                harness.execute(run)
            except ValueError:
                require(not web.requests and not run.state_root.exists(), "Changed launcher was used without a new review")
                return
        raise ValueError("Changed external launcher was not bound to the review")

    scenario("external_process_launcher_change_invalidates_review", launcher_change)

    def environment_change() -> None:
        run, unit, web = build("environment_change")
        settings = run.base / ".env"
        settings.write_text("PROJECT_NAME=synthetic_project\n")
        approve_fixture(run)
        settings.write_text("PROJECT_NAME=another_synthetic_project\n")
        web.serve(unit["url"], (200, b"a\n1\n"))
        try:
            harness.execute(run)
        except ValueError:
            require(not web.requests and not run.state_root.exists(), "Changed runtime settings were not bound")
            return
        raise ValueError("Changed runtime settings were accepted")

    scenario("runtime_settings_change_invalidates_review_without_logging_values", environment_change)

    def path_map_change(after_gate: bool) -> None:
        # Failure mode S46 (R3P2-CONTROLS-015): the map that resolves recorded receipt paths is bound to the review.
        run, unit, web = build(f"path_map_{'after_gate' if after_gate else 'before_gate'}")
        web.serve(unit["url"], (200, b"a\n1\n"))
        original_read = Path.read_bytes
        map_path = harness.data_paths.MAP_PATH
        changed = {"on": not after_gate}
        original_gate = harness.review_gate

        def read_changed(path: Path) -> bytes:
            body = original_read(path)
            if path == map_path and changed["on"]:
                document = json.loads(body)
                document["moved"] = {**document["moved"], "data/synthetic_old": "data/synthetic_new"}
                return json.dumps(document).encode()
            return body

        def gate_then_change(run_: Any) -> str:
            sha = original_gate(run_)
            changed["on"] = True
            return sha

        with patch.object(Path, "read_bytes", read_changed), patch.object(harness, "review_gate", gate_then_change):
            try:
                harness.execute(run)
            except ValueError as error:
                expected = "Path map changed" if after_gate else "Execution controls changed"
                require(expected in str(error), f"Unexpected refusal: {error}")
                require(not web.requests, "Changed path map was used for a request")
                if not after_gate:
                    require(not run.state_root.exists(), "Changed path map created state")
                return
        raise ValueError("Changed path map was accepted")

    scenario("path_map_changed_before_the_gate_invalidates_review", lambda: path_map_change(False))
    scenario("path_map_changed_after_the_gate_stops_before_dispatch", lambda: path_map_change(True))
