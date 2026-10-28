"""Drive small Synthetic pipelines through real modules and in-process launcher handlers."""

from __future__ import annotations

import copy
import hashlib
import io
import json
import os
import shutil
import sys
import tempfile
import time
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from dataclasses import replace
from functools import partial
from pathlib import Path
from typing import Any
from unittest.mock import Mock, create_autospec, patch

from orchestration import dbt_lineage, ecs_target, launcher, remote_run
from orchestration import pipeline_protocol as protocol
from orchestration.interface import SubmissionRequest
from orchestration.remote_run import updated_result_map
from scripts.lakehouse import gold_export, gold_publish, provenance, run_lock
from scripts.orchestration import pipeline_control, pipeline_seal
from scripts.orchestration.broker import Broker, BrokerPaths, load_templates
from scripts.orchestration.ecs_fake_api import FakeEcs
from scripts.orchestration.ecs_fake_s3 import Principal
from scripts.orchestration.ecs_stage_runtime_cases import Storage
from scripts.orchestration.run_pipeline_flow_e2e import RUN, plan, rejected, require
from scripts.orchestration.run_runstore_e2e import Fixture, load_mutant
from scripts.process import run_command
from services.hai.bootstrap.integrity import canonical, run_prefix, try_prefix
from services.hai.bootstrap.outputs import Declaration, publish_outputs, read_declaration
from services.hai.bootstrap.transfer import FileRef, Transfer

ROOT = Path(__file__).resolve().parents[2]
SIZE = ecs_target.Size(1024, 4096, 21)
SILVER = ("int_silver_build_marker", "silver_one", "silver_two")


@contextmanager
def arguments(values: list[str], environment: Mapping[str, str] | None = None) -> Iterator[None]:
    with patch.object(sys, "argv", ["pipeline_fixture", *values]), patch.dict(os.environ, dict(environment or {})):
        yield


def invoke(function: Callable[[], int], values: list[str], out: Path, environment: Mapping[str, str] | None = None) -> None:
    out.mkdir(parents=True, exist_ok=True)
    stdout, stderr = io.StringIO(), io.StringIO()
    with arguments(values, environment), redirect_stdout(stdout), redirect_stderr(stderr):
        code = function()
    (out / "stdout.txt").write_text(stdout.getvalue())
    (out / "stderr.txt").write_text(stderr.getvalue())
    require(code == 0, f"real pipeline command exited {code}: {stderr.getvalue()[-1000:]} {stdout.getvalue()[-1000:]}")


def project(folder: Path) -> tuple[Path, str]:
    snapshot = folder / "snapshot"
    target = snapshot / "project"
    target.mkdir(parents=True)
    (snapshot / "packages").mkdir()
    base = ROOT / "tests/orchestration/fixtures/tiny"
    shutil.copy2(base / "dbt_project.yml", target / "dbt_project.yml")
    output = {"type": "duckdb", "path": "{{ env_var('STAGING_E2E_CASE') }}/staging.duckdb", "threads": 1}
    profiles = {"tiny": {"target": "fixture", "outputs": {"fixture": output, "pinned_silver": output}}}
    (target / "profiles.yml").write_text(json.dumps(profiles))
    for layer, names in (("silver", SILVER), ("gold", ("gold_one",))):
        destination = target / "models" / layer
        destination.mkdir(parents=True)
        for name in names:
            shutil.copy2(base / "models" / (name + ".sql"), destination / (name + ".sql"))
    shutil.copy2(base / "models/schema.yml", target / "models/schema.yml")
    return target, provenance.tree_digest(snapshot)


class PipelineFixture:
    """Own one test registry, real pipeline records and isolated task folders.

    The client routes broker calls to real launcher acceptance handlers. RunStore
    injection supplies the pending launcher hook without replacing acceptance.
    Fixture-quality records are used only by explicitly named contract cases.
    """

    def __init__(self, folder: Path) -> None:
        self.base = folder
        self.fixture = Fixture(folder / "controller")
        self.folder = self.fixture.folder
        self.registry = self.fixture.registry
        self.deadline = time.monotonic() + 180
        self.transfer = Transfer(Storage(self.fixture.s3))
        self.store = self.fixture.store
        self.store.transfer = self.transfer
        self.store.deadline = self.deadline
        self.ecs = FakeEcs(self.registry.state_file)
        self.project, self.source_sha = project(folder)
        self.bronze_sha = hashlib.sha256(b"Synthetic bronze fixture, ids 1 and 2").hexdigest()
        source_record = folder / "frozen_source.json"
        source_record.write_bytes(canonical({"dbt_tree_sha256": self.source_sha, "bronze_sha256": self.bronze_sha}))
        self.source_ref = self.transfer.upload(source_record, run_prefix(RUN, 1) + "inputs/files/frozen_source.json", "frozen_source.json", self.deadline)
        self.counter = 0
        self.trace: list[dict[str, Any]] = []
        self.validation_ids: dict[str, str] = {}
        self.engine = object.__new__(launcher.Launcher)
        self.engine.cfg = Mock(runs_root=self.folder.parent)
        self.engine.registry = self.registry
        client = create_autospec(launcher.LauncherClient, instance=True, config=Mock(runs_root=self.folder.parent))
        client.configure_mock(
            **{
                "call.side_effect": self.route,
                "principal_run.side_effect": lambda run_id: {"kind": "run", "run_id": run_id, "token": "synthetic_fixture_token"},
            }
        )
        self.broker = Broker(BrokerPaths(self.registry, folder / "broker"), load_templates(), client, repo_root=ROOT, runs_root=self.folder.parent)
        self.change(lambda state: state["attempts"].clear())
        self.change(lambda state: state["copies"].update({"fixture_snapshot": {"dbt_tree_sha256": self.source_sha}}))
        self.change(lambda state: state["runs"][RUN].update(mode="publish_fixture", snapshot="fixture_snapshot", fixture_pins_sha256=self.bronze_sha))
        self.broker.start()

    def state(self) -> dict[str, Any]:
        return self.fixture.state()

    def change(self, edit: Callable[[dict[str, Any]], None]) -> None:
        self.fixture.change(edit)

    def close(self) -> None:
        self.broker.close()

    def route(self, method: str, params: dict[str, Any], principal: dict[str, Any] | None = None, request_id: str | None = None, **kwargs: Any) -> Any:
        if method == "status":
            if params["what"] == "launcher":
                return {"status": "serving", "ecs_ready": True}
            return copy.deepcopy(self.state()["attempts"][params["id"]])
        if method == "logs":
            return {"text": "Synthetic pipeline fixture task released"}
        handlers = {"pipeline_accept_seal": self.engine.m_pipeline_accept_seal, "pipeline_accept_export": self.engine.m_pipeline_accept_export}
        require(method in handlers, "fixture received an unexpected launcher method: " + method)
        with run_lock.transaction(self.registry) as state:
            context = launcher.Ctx(Mock(), method, request_id or "synthetic_request", params, principal or {}, "synthetic", state)
            try:
                with patch.object(protocol, "LocalRunStore", lambda folder: self.store):
                    return handlers[method](context)
            except launcher.LauncherError as error:
                raise launcher.ClientError(str(error), error.code) from None

    def start(self, template: str, params: dict[str, Any], *, fallback: bool = False, interrupt: bool = False) -> tuple[dict[str, Any], Path]:
        self.counter += 1
        identifier = f"flow_{self.counter:03}_{template}"
        logs = self.base / "attempts" / identifier
        logs.mkdir(parents=True)
        attempt = {
            "id": identifier,
            "owner": "run:" + RUN,
            "generation": 1,
            "template": template,
            "params": params,
            "phase": "queued",
            "reason": "",
            "exit_code": None,
            "cancel_intent": False,
            "created_at": f"2026-10-24T00:00:{self.counter:02}Z",
            "deadline_utc": 1_000,
            "log_tail": str(logs / "log_tail.txt"),
            "n": self.counter,
            "fixture_inputs": [self.source_ref.document()],
        }
        ecs_target.initialize(attempt, "auto", 0, None)
        self.change(lambda state: state["attempts"].update({identifier: attempt}))
        view = ecs_target.Availability(portable=True)
        if fallback:
            view = replace(view, reason="cloud_cap", usage=ecs_target.Usage(tasks=3))
        decision = ecs_target.select(attempt, "offline", SIZE, view, 1)
        require(decision.target == ("docker" if fallback else "ecs"), "AWS-first placement or safe fallback changed")
        placement = ecs_target.reserve(attempt, SIZE, decision, 1, view.usage)
        self.change(lambda state: state["attempts"].update({identifier: copy.deepcopy(attempt)}))
        if placement["target"] == "ecs":
            self.ecs.dispatch(attempt, placement)
        if interrupt:
            require(placement["capacity_provider"] == "FARGATE_SPOT", "interruption did not reach Spot")
            self.ecs.states[placement["try_id"]] = "fenced"
            first_try = placement["try_id"]
            ecs_target.fenced(attempt, first_try, 2, no_start=True)
            view = replace(view, reason="capacity", observed_at=2, no_start=True)
            decision = ecs_target.select(attempt, "offline", SIZE, view, 2)
            require(decision.provider == "FARGATE", "confirmed pre-start Spot failure did not select on demand")
            placement = ecs_target.reserve(attempt, SIZE, decision, 2, ecs_target.Usage())
            self.change(lambda state: state["attempts"].update({identifier: copy.deepcopy(attempt)}))
            self.ecs.dispatch(attempt, placement)
            require(first_try != placement["try_id"], "on-demand retry reused a try")
        out = self.base / "tasks" / placement["try_id"] / "out"
        out.mkdir(parents=True)
        return attempt, out

    def input(self, logical: str, target: Path, *, sealed_layer: str | None = None) -> FileRef:
        run = self.state()["runs"][RUN]
        if sealed_layer is None:
            saved = run["stage"]["objects"][logical]
            reference = FileRef(logical, **{name: saved[name] for name in ("key", "version_id", "sha256", "size")})
        else:
            saved = run["stage"]["sealed"][sealed_layer]
            reference = FileRef(logical, saved["source_key"], saved["source_version_id"], saved["sha256"], saved["size"])
        target.parent.mkdir(parents=True, exist_ok=True)
        self.transfer.download(reference, target, self.deadline)
        return reference

    def finish(self, attempt: dict[str, Any], out: Path, declaration: Declaration, *, inputs: tuple[str, ...] = ()) -> dict[str, Any]:
        placement = attempt["execution"]["placements"][-1]
        identifier = placement["try_id"]
        task_arn = ":".join(("arn", "aws", "ecs", "us-west-2", "0" * 12, "task/synthetic/" + hashlib.sha256(identifier.encode()).hexdigest()[:32]))
        source_refs = [FileRef(**value).document() for value in attempt.pop("fixture_inputs", [])]
        job = {
            "run_id": RUN,
            "generation": 1,
            "attempt_id": attempt["id"],
            "try_id": identifier,
            "template": attempt["template"],
            "inputs": source_refs,
            "input_map_sha256": hashlib.sha256(canonical(sorted(source_refs, key=lambda item: item["path"]))).hexdigest(),
            "expected_outputs": declaration.document(),
        }
        digest = hashlib.sha256(canonical(job)).hexdigest()
        writer = Principal("writer", "synthetic_writer", try_prefix(RUN, 1, identifier), 1e20)
        transfer = Transfer(Storage(self.fixture.s3, writer), reader=Storage(self.fixture.s3))
        for name in ("stdout.txt", "stderr.txt"):
            if not (out / name).exists():
                (out / name).write_bytes(b"")
        marker = publish_outputs(
            out,
            job,
            digest,
            task_arn,
            declaration,
            inputs,
            transfer,
            {},
            self.deadline,
            exit_code=0,
            timings={"execute_seconds": 0.0},
            minimum_free_disk_bytes=1024,
        )
        files = sorted(str(path.relative_to(out)) for path in out.rglob("*") if path.is_file())
        require(marker["exit_code"] == 0, f"result contract refused the real module output: exit {marker['exit_code']}, files {files}")
        local = placement["target"] == "docker"
        placement.update(state="complete", task_arn=None if local else task_arn, started=True)
        attempt.update(phase="ended", reason="exited", exit_code=0)
        if local:
            attempt["container_id"] = hashlib.sha256(identifier.encode()).hexdigest()
        self.change(lambda state: state["attempts"].update({attempt["id"]: copy.deepcopy(attempt)}))
        if local:
            self.local_result(attempt, marker, out)
        else:
            self.store.materialize(job, marker, declaration, digest, {})
        self.trace.append(
            {
                "template": attempt["template"],
                "attempt": attempt["id"],
                "try_id": identifier,
                "target": placement["target"],
                "input_map_sha256": job["input_map_sha256"],
                "input_versions": source_refs,
            }
        )
        return {"job": job, "marker": marker, "declaration": declaration, "job_sha256": digest, "out": out}

    def late_result(self, payload: dict[str, Any]) -> bool:
        """Submit a valid but fenced first-try result after the on-demand result applied."""
        attempt = self.state()["attempts"][payload["job"]["attempt_id"]]
        placements = attempt["execution"]["placements"]
        require(
            len(placements) == 2 and placements[0]["no_start"] and placements[1]["capacity_provider"] == "FARGATE",
            "retry lacks confirmed Spot no-start evidence",
        )
        old_job = {**payload["job"], "try_id": placements[0]["try_id"]}
        require(
            old_job["input_map_sha256"] == payload["job"]["input_map_sha256"] and old_job["inputs"] == payload["job"]["inputs"],
            "retry changed immutable references",
        )
        digest = hashlib.sha256(canonical(old_job)).hexdigest()
        identifier = placements[0]["try_id"]
        writer = Principal("writer", "synthetic_late_writer", try_prefix(RUN, 1, identifier), 1e20)
        marker = publish_outputs(
            payload["out"],
            old_job,
            digest,
            ":".join(("arn", "aws", "ecs", "us-west-2", "0" * 12, "task/synthetic/" + hashlib.sha256(identifier.encode()).hexdigest()[:32])),
            payload["declaration"],
            (),
            Transfer(Storage(self.fixture.s3, writer), reader=Storage(self.fixture.s3)),
            {},
            self.deadline,
            exit_code=0,
            timings={"execute_seconds": 0.0},
            minimum_free_disk_bytes=1024,
        )
        before = self.state()
        rejected("result_identity", lambda: self.store.materialize(old_job, marker, payload["declaration"], digest, {}))
        require(self.state() == before, "late first try overwrote on-demand output")
        return True

    def local_result(self, attempt: dict[str, Any], marker: dict[str, Any], out: Path) -> None:
        """Model the pending local-result integration with real pinned transfers and records."""
        entries = {}
        logs = Path(attempt["log_tail"]).parent
        for item in marker["objects"]:
            reference = FileRef(item["name"], item["key"], item["version_id"], item["sha256"], item["size"])
            self.transfer.verify_reference(reference, self.deadline)
            if reference.path in ("stdout.txt", "stderr.txt"):
                shutil.copy2(out / reference.path, logs / reference.path)
            else:
                entries[reference.path] = {key: item[key] for key in ("key", "version_id", "sha256", "size")} | {"try_id": attempt["execution"]["live_try"]}
                if item["kind"] == "controller_record":
                    destination = self.folder / reference.path
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(out / reference.path, destination)
        (logs / "log_tail.txt").write_bytes((logs / "stdout.txt").read_bytes())

        def update(state: dict[str, Any]) -> None:
            stage = state["runs"][RUN].setdefault("stage", {"generation": 1, "objects": {}})
            stage["objects"] = updated_result_map(stage["objects"], entries, attempt["template"])

        self.change(update)

    def build(self, layer: str, *, fallback: bool = False, interrupt: bool = False) -> dict[str, Any]:
        if self.state()["runs"][RUN].get("stage"):
            self.store.clear_build_artifacts(self.state()["runs"][RUN])
        attempt, out = self.start("pipeline_dbt", {"layer": layer}, fallback=fallback, interrupt=interrupt)
        input_sha = self.bronze_sha if layer == "silver" else self.state()["runs"][RUN]["silver_pin"]["sha256"]
        args = [
            "--layer",
            layer,
            "--mode",
            "publish_fixture",
            "--run",
            RUN,
            "--folder",
            str(out),
            "--project",
            str(self.project),
            "--source-sha256",
            self.source_sha,
            "--input-sha256",
            input_sha,
        ]
        if layer == "gold":
            refs = []
            for name in sorted(self.state()["runs"][RUN]["stage"]["objects"]):
                if name.startswith("silver_input/"):
                    refs.append(self.input(name, out / name).document())
            attempt["fixture_inputs"].extend(refs)
            args += ["--extract-sha256", self.state()["runs"][RUN]["silver_input"]["sha256"]]
        environment = {
            "STAGING_E2E_CASE": str(out),
            "DBT_TARGET_PATH": str(out / "target"),
            "DBT_LOG_PATH": str(out / "logs"),
            "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
            "DUCKDB_MEMORY_LIMIT": "256MB",
            "JOB_THREADS": "1",
            **dict(entry.split("=", 1) for entry in dbt_lineage.capture_environment(attempt["id"])),
        }
        require(provenance.tree_digest(self.project.parent) == self.source_sha, "frozen fixture source changed before build")
        result = run_command(sys.executable, ["-B", "-m", "scripts.lakehouse.pipeline_build", *args], cwd=ROOT, env={**os.environ, **environment}, timeout=90)
        (out / "stdout.txt").write_text(result.stdout)
        (out / "stderr.txt").write_text(result.stderr)
        require(result.returncode == 0, f"real pipeline build exited {result.returncode}: {result.stderr[-1000:]} {result.stdout[-1000:]}")
        manifest = out / "target/manifest.json"
        if layer == "silver":
            require(manifest.stat().st_size < 7_999_899, "tiny manifest unexpectedly exceeds the representative record")
            with manifest.open("ab") as stream:
                stream.write(b" " * (7_999_899 - manifest.stat().st_size))
        payload = self.finish(
            attempt,
            out,
            read_declaration("pipeline_dbt", {"layer": layer}, (), 1 << 30, 1),
            inputs=tuple(str(path.relative_to(out)) for path in out.glob("silver_input/*/*.parquet")) + ("silver_input/manifest.json",),
        )
        captured = self.base / "captured" / attempt["id"]
        capture = dbt_lineage.capture(self.folder / "target", captured, attempt["id"])
        manifest_record, results_record = dbt_lineage.verified_documents(captured, capture, attempt["id"])
        receipt, _ = protocol.read_record(self.folder, "build_receipt.json")
        require(
            manifest_record["metadata"]["invocation_id"] == results_record["metadata"]["invocation_id"] == receipt["invocation_id"],
            "unchanged dbt reader lost the invocation binding",
        )
        self.broker_attempt(layer + "_build", attempt)
        return payload

    def broker_attempt(self, template: str, attempt: dict[str, Any], *, build_file: dict[str, str] | None = None) -> None:
        request = SubmissionRequest(template, RUN, "publish_fixture", 1, "fixture_pipeline", template, "fixture_dag_run")
        attempt["execution"]["live_try"] = None
        self.change(lambda state: state["attempts"][attempt["id"]]["execution"].update(live_try=None))
        attempt["caller_identity"] = request.identity()
        self.change(lambda state: state["attempts"][attempt["id"]].update(caller_identity=request.identity()))
        self.broker.append(
            {
                "event": "intent",
                "submission_id": request.submission_id,
                **request.as_dict(),
                "spec_sha256": "1" * 64,
                "code_sha256": self.broker.templates_sha256,
                "inputs": {},
                "reservation_id": None,
                "reserved_memory_bytes": None,
                "container_name": request.container_name,
                "lineage": {"version": 1, "inputs": [], "outputs": []},
            }
        )
        if build_file is not None:
            self.broker.finish(request, 0, build_file=build_file)
        else:
            self.broker.apply_attempt(request, {"attempt_id": attempt["id"]})
        lines = self.broker.lines_of(request)
        require(lines[-1]["event"] == "finished" and lines[-1]["exit_code"] == 0, "real broker did not finish " + template)

    def quality(self, kind: str, *, native: bool) -> None:
        attempt, out = self.start(
            "pipeline_validate_" + kind, {"database": str(self.folder / "staging.duckdb"), "out_host": str(self.folder / ("quality_" + kind))}
        )
        reference = self.input("staging.duckdb", out.parent / "inputs/staging.duckdb")
        attempt["fixture_inputs"].append(reference.document())
        if native:
            from scripts.lakehouse import gold_checks, pipeline_quality

            contract = gold_checks.Contract(
                (gold_checks.Fact("gold_one", "silver_two", "one row per id", (gold_checks.Column("id", "id", "INTEGER"),), (), (), ()),),
                (),
                (),
                {"silver_two": [("id", "INTEGER")]},
            )
            suites = self.base / ("suite_" + kind)
            suites.mkdir()
            table = "gold_one" if kind == "gold" else "silver_two"
            (suites / "suite.json").write_bytes(
                canonical(
                    {
                        "name": "synthetic_" + kind,
                        "expectations": [
                            {
                                "id": table + ".id_unique",
                                "table": table,
                                "type": "ExpectColumnValuesToBeUnique",
                                "kwargs": {"column": "id"},
                                "severity": "structural",
                            }
                        ],
                    }
                )
            )
            baseline = ROOT / "tests/orchestration/fixtures/tiny_ge/baselines.json"
            real_reader = protocol.read_record
            with (
                patch.object(pipeline_quality, "read_record", lambda root, name: real_reader(out, name)),
                patch.object(gold_checks, "load_contract", lambda: contract),
            ):
                invoke(
                    pipeline_quality.main,
                    [
                        "--kind",
                        kind,
                        "--database",
                        str(out.parent / "inputs/staging.duckdb"),
                        "--suites",
                        str(suites),
                        "--baselines",
                        str(baseline),
                        "--output",
                        str(out / "result.json"),
                    ],
                    out,
                )
        else:
            record: dict[str, Any] = {"build_file_sha256": reference.sha256, "counts": {"pass": 1, "warn": 0, "fail": 0}}
            if kind == "cross":
                record["reconciliation"] = {"facts": 1, "facts_in_contract": 1, "problems": {"reconcile_rows": []}}
            (out / "result.json").write_bytes(canonical(record))
            digest = hashlib.sha256((out / "result.json").read_bytes()).hexdigest()
            (out / "stdout.txt").write_bytes(canonical({"result_sha256": digest, "build_file_sha256": reference.sha256}) + b"\n")
        self.finish(attempt, out, read_declaration("pipeline_validate_" + kind, {}, (), 0, 0))
        self.validation_ids[kind] = attempt["id"]
        build_file = {
            "file": str(self.folder / "staging.duckdb"),
            "sha256": reference.sha256,
            "run_id": RUN,
            "submission_id": kind + "_validate",
            "attempt_id": attempt["id"],
        }
        name = "cross_layer_checks" if kind == "cross" else kind + "_validate"
        self.broker_attempt(name, attempt, build_file=build_file)

    def seal_layer(self, layer: str) -> dict[str, Any]:
        attempt, out = self.start("pipeline_seal", {"layer": layer})
        source = self.input("staging.duckdb", out.parent / "inputs/staging.duckdb")
        attempt["fixture_inputs"].append(source.document())
        request = {
            "run_id": RUN,
            "layer": layer,
            "generation": 1,
            "validation_attempt": self.validation_ids[layer],
            "sha256": source.sha256,
            "source": source.document(),
        }
        request_file = out.parent / "request.json"
        request_file.write_bytes(canonical(request))
        invoke(
            lambda: pipeline_seal.main(["--request", str(request_file), "--inputs", str(out.parent / "inputs"), "--out", str(out), "--timeout-seconds", "30"]),
            [],
            out,
        )
        payload = self.finish(attempt, out, pipeline_seal.declaration())
        self.store.seal(layer, source.sha256)
        saved = self.state()
        self.store.seal(layer, source.sha256)
        require(self.state() == saved, "repeat remote seal changed registry state")
        require(not self.state()["runs"][RUN].get("layer_completions", {}).get(layer), "store minted launcher completion authority")
        control = SubmissionRequest("seal_" + layer, RUN, "publish_fixture", 1, "fixture_pipeline", "seal_" + layer, "fixture_dag_run")
        pipeline_control.perform(self.broker, control, "seal_" + layer, {}, store=self.store)
        completion = protocol.accepted_completion(self.state()["runs"][RUN], layer)
        require(completion["validation_attempt"] == self.validation_ids[layer], "launcher accepted an unrelated validation")
        return payload

    def export(self, layer: str, *, release: bool = True, fallback: bool = False) -> gold_publish.Context:
        attempt, out = self.start("pipeline_export", {"layer": layer}, fallback=fallback)
        sealed = out.parent / "inputs/staging.duckdb"
        source = self.input("staging.duckdb", sealed, sealed_layer=layer)
        attempt["fixture_inputs"].append(source.document())
        tables = SILVER if layer == "silver" else ("gold_one",)
        selected = plan(tables)
        with patch.object(gold_export, "load_plan", lambda: selected):
            invoke(
                lambda: gold_export.main(["--run", RUN, "--sealed-sha256", source.sha256, "--sealed", str(sealed), "--publish-dir", str(out / "publish")]),
                [],
                out,
            )
        self.finish(attempt, out, read_declaration("pipeline_export", {"layer": layer}, tables, 1 << 30, 0))
        self.broker_attempt(layer + "_export", attempt)
        acceptance = self.state()["runs"][RUN]["layer_exports"][layer]
        if release:
            shutil.rmtree(out.parent)
            require(not out.exists(), "export task folder survived release")
        fresh = self.base / ("fresh_" + layer)
        fresh.mkdir()
        for name in sorted(self.state()["runs"][RUN]["stage"]["objects"]):
            if name.startswith("publish/"):
                self.input(name, fresh / name)
        context = gold_publish.Context(None, selected, RUN, "publish_fixture", fresh / "publish", source.sha256, accepted_result_sha256=acceptance["sha256"])
        manifest, _, _ = gold_publish.verified_export(context)
        require(set(manifest["tables"]) == set(tables), "fresh stage-down lost a contract table")
        require(
            all(item["rows"] == (1 if name == "int_silver_build_marker" else 2) for name, item in manifest["tables"].items()),
            "fixture export row counts changed",
        )
        return context

    def silver_extract(self, context: gold_publish.Context) -> None:
        manifest, _, exports = gold_publish.verified_export(context)
        pin = {
            "schema": 1,
            "run_id": RUN,
            "mode": "publish_fixture",
            "namespace": "silver_e2e",
            "source_run_id": RUN,
            "snapshot_set": {name: index + 1 for index, name in enumerate(SILVER)},
        }
        pin_raw = canonical(pin)
        pin_sha = hashlib.sha256(pin_raw).hexdigest()
        files = {}
        objects = {}
        for name, details in manifest["tables"].items():
            logical = "silver_input/" + name + "/part.parquet"
            source = exports / details["file"]
            reference = self.transfer.upload(source, run_prefix(RUN, 1) + "inputs/files/" + logical, logical, self.deadline)
            objects[logical] = {key: value for key, value in reference.document().items() if key != "path"} | {"try_id": "fixture_extract"}
            files[name + "/part.parquet"] = details["file_sha256"]
        extract = {"run_id": RUN, "pins_sha256": pin_sha, "snapshot_set": pin["snapshot_set"], "files": files}
        path = self.base / "extract_manifest.json"
        path.write_bytes(canonical(extract))
        reference = self.transfer.upload(path, run_prefix(RUN, 1) + "inputs/files/silver_input/manifest.json", "silver_input/manifest.json", self.deadline)
        objects[reference.path] = {key: value for key, value in reference.document().items() if key != "path"} | {"try_id": "fixture_extract"}
        self.change(lambda state: state["runs"][RUN]["stage"]["objects"].update(objects))
        self.change(
            lambda state: state["runs"][RUN].update(
                silver_pin={"sha256": pin_sha, "source_run_id": RUN}, silver_input={"sha256": reference.sha256, "pins_sha256": pin_sha}
            )
        )


def run_flow(folder: Path, name: str, *, native_quality: bool = True) -> dict[str, Any]:
    if native_quality:
        __import__("great_expectations")
    owned = PipelineFixture(folder)
    try:
        build = owned.build("silver", fallback=name == "F3", interrupt=name == "F2")
        late_refused = owned.late_result(build) if name == "F2" else False
        require(len(protocol.read_bytes(owned.folder, "target/manifest.json")) == 7_999_899, "representative manifest was not materialized")
        owned.quality("silver", native=native_quality)
        owned.seal_layer("silver")
        silver = owned.export("silver")
        owned.silver_extract(silver)
        owned.build("gold")
        owned.quality("gold", native=native_quality)
        owned.quality("cross", native=native_quality)
        owned.seal_layer("gold")
        context = owned.export("gold", fallback=name == "F3")
        manifest, manifest_sha, exports = gold_publish.verified_export(context)
        defects = {}
        for defect in ("missing", "replaced", "extra", "manifest"):
            fresh = folder / ("consumer_defect_" + defect)
            shutil.copytree(context.publish_dir, fresh)
            damaged = replace(context, publish_dir=fresh)
            file = fresh / "export/gold_one.parquet"
            if defect == "missing":
                file.unlink()
            elif defect == "replaced":
                file.write_bytes(b"x" * file.stat().st_size)
            elif defect == "extra":
                (fresh / "export/extra.parquet").write_bytes(b"extra Synthetic Parquet")
            else:
                file = fresh / "export/manifest.json"
                file.write_bytes(file.read_bytes() + b" ")
            cause = "consumer result does not match" if defect == "manifest" else "exported file"
            rejected(cause, partial(gold_publish.verified_export, damaged))
            defects[defect] = "refused"
        run = owned.state()["runs"][RUN]
        require(set(run["layer_completions"]) == {"silver", "gold"} and set(run["layer_exports"]) == {"silver", "gold"}, "flow did not persist both layers")
        require(not (owned.folder / "staging.duckdb").exists(), "remote database materialized on the Mac")
        targets = [item["target"] for item in owned.trace]
        if name == "F3":
            require(targets[0] == "docker" and targets[-1] == "docker" and "ecs" in targets[1:-1], "mixed flow lost per-job placement")
        return {
            "flow": name,
            "native_quality": native_quality,
            "late_result_refused": late_refused,
            "trace": owned.trace,
            "layer_completions": run["layer_completions"],
            "layer_exports": run["layer_exports"],
            "export_manifest_sha256": manifest_sha,
            "export_rows": {key: item["rows"] for key, item in manifest["tables"].items()},
            "consumer_defects": defects,
            "all_pinned_exports_redownloaded_after_task_release": True,
            "docker_baseline": "pending lead run",
            "synthetic_silver_snapshot_ids": True,
        }
    finally:
        owned.close()


def seed_acceptance(owned: PipelineFixture) -> tuple[dict[str, Any], dict[str, Any]]:
    import duckdb

    attempt, out = owned.start("pipeline_dbt", {"layer": "gold"})
    with duckdb.connect(str(out / "staging.duckdb")) as connection:
        connection.execute("create table gold_one as select 1 as id union all select 2 as id")
    pin_sha = hashlib.sha256(b"Synthetic pinned silver input").hexdigest()
    owned.change(lambda state: state["runs"][RUN].update(silver_pin={"sha256": pin_sha}))
    receipt = {"run_id": RUN, "layer": "gold", "mode": "publish_fixture", "source_sha256": owned.source_sha, "input_sha256": pin_sha}
    (out / "build_receipt.json").write_bytes(canonical(receipt))
    (out / "target").mkdir()
    for name in ("run_results.json", "manifest.json"):
        (out / "target" / name).write_bytes(canonical({"metadata": {"invocation_id": "synthetic_invocation"}, "results": [{"status": "success"}]}))
    (out / "stdout.txt").write_bytes(canonical({"receipt_sha256": hashlib.sha256(canonical(receipt)).hexdigest()}) + b"\n")
    build = owned.finish(attempt, out, read_declaration("pipeline_dbt", {"layer": "gold"}, (), 1 << 30, 0))
    owned.quality("gold", native=False)
    owned.quality("cross", native=False)
    seal = owned.seal_layer("gold")
    return build, seal


def rebind_seal_source(owned: PipelineFixture, seal: Mapping[str, Any], version_id: str) -> None:
    """Keep all incoming seal bindings coherent while changing its pinned source version."""
    state = owned.state()
    returned = state["runs"][RUN]["stage"]["objects"]["seal.json"]
    record, _ = protocol.read_record(owned.folder, "seal.json")
    record["source_version_id"] = version_id
    raw = canonical(record) + b"\n"
    digest = hashlib.sha256(raw).hexdigest()
    head = owned.fixture.s3.seed(returned["key"], raw, {"sha256": digest})
    (owned.folder / "seal.json").write_bytes(raw)
    logs = Path(state["attempts"][seal["job"]["attempt_id"]]["log_tail"]).parent
    (logs / "stdout.txt").write_bytes(canonical({"seal_sha256": digest}) + b"\n")

    def update(current: dict[str, Any]) -> None:
        stage = current["runs"][RUN]["stage"]
        stage["objects"]["staging.duckdb"]["version_id"] = version_id
        stage["objects"]["seal.json"].update(version_id=head.version_id, sha256=digest, size=len(raw))
        stage["sealed"]["gold"] = record

    owned.change(update)


def acceptance_defect(folder: Path, defect: str) -> dict[str, Any]:
    owned = PipelineFixture(folder)
    try:
        build, seal = seed_acceptance(owned)
        owned.change(lambda state: state["runs"][RUN].pop("layer_completions"))
        digest = owned.state()["runs"][RUN]["stage"]["sealed"]["gold"]["sha256"]
        if defect == "missing_seal":
            (owned.folder / "seal.json").unlink()
            rejected("remote seal evidence is unreadable", lambda: owned.store.seal("gold", digest))
        elif defect == "receipt":
            logs = Path(owned.state()["attempts"][seal["job"]["attempt_id"]]["log_tail"]).parent
            (logs / "stdout.txt").write_bytes(b'{"seal_sha256":"wrong"}\n')
            rejected("seal_receipt", lambda: owned.store.seal("gold", digest))
        elif defect == "source":
            source = owned.state()["runs"][RUN]["stage"]["objects"]["staging.duckdb"]
            model = owned.fixture.s3
            raw = model.get(Principal("project", "synthetic_project"), source["key"], source["version_id"])
            replacement = model.seed(source["key"], raw, {"sha256": source["sha256"]})
            owned.change(lambda state: state["runs"][RUN]["stage"]["objects"]["staging.duckdb"].update(version_id=replacement.version_id))
            rejected("seal_binding", lambda: owned.store.seal("gold", digest))
        elif defect in ("running", "task_arn"):
            key, value = ("state", "running") if defect == "running" else ("task_arn", "")
            owned.change(lambda state: state["attempts"][seal["job"]["attempt_id"]]["execution"]["placements"][-1].update({key: value}))
            rejected("result_identity", lambda: owned.store.seal("gold", digest))
        elif defect == "store_conflict":
            previous = owned.state()["runs"][RUN]["stage"]["sealed"]["gold"]
            model = owned.fixture.s3
            raw = model.get(Principal("project", "synthetic_project"), previous["source_key"], previous["source_version_id"])
            replacement = model.seed(previous["source_key"], raw, {"sha256": digest})
            owned.change(lambda state: state["runs"][RUN]["stage"]["sealed"]["gold"].update(source_version_id=replacement.version_id))
            rejected("seal_conflict", lambda: owned.store.seal("gold", digest))
        elif defect == "seal_head":
            returned = owned.state()["runs"][RUN]["stage"]["objects"]["seal.json"]
            raw = (owned.folder / "seal.json").read_bytes()
            replacement = owned.fixture.s3.seed(returned["key"], raw, {"sha256": "0" * 64})
            owned.change(lambda state: state["runs"][RUN]["stage"]["objects"]["seal.json"].update(version_id=replacement.version_id))
            rejected("stage_conflict", lambda: owned.store.seal("gold", digest))
        elif defect == "source_head":
            source = owned.state()["runs"][RUN]["stage"]["objects"]["staging.duckdb"]
            model = owned.fixture.s3
            raw = model.get(Principal("project", "synthetic_project"), source["key"], source["version_id"])
            replacement = model.seed(source["key"], raw, {"sha256": "0" * 64})
            rebind_seal_source(owned, seal, replacement.version_id)
            rejected("stage_conflict", lambda: owned.store.seal("gold", digest))
        elif defect in ("validation_hash", "malformed_validation"):
            record, _ = protocol.read_record(owned.folder, "quality_gold/result.json")
            record["build_file_sha256"] = "0" * 64 if defect == "validation_hash" else "malformed"
            raw = canonical(record)
            (owned.folder / "quality_gold/result.json").write_bytes(raw)
            attempt = owned.state()["attempts"][owned.validation_ids["gold"]]
            logs = Path(attempt["log_tail"]).parent
            (logs / "stdout.txt").write_bytes(canonical({"result_sha256": hashlib.sha256(raw).hexdigest()}) + b"\n")
            if defect == "validation_hash":
                rejected("seal_binding", lambda: owned.store.seal("gold", digest))
            else:
                params = {"layer": "gold", "attempt_id": owned.validation_ids["gold"], "cross_attempt_id": owned.validation_ids["cross"]}
                rejected("the sealed bytes are not the quality attempt's validated bytes", lambda: owned.route("pipeline_accept_seal", params, {"run_id": RUN}))
        elif defect == "malformed_seal":
            (owned.folder / "seal.json").write_bytes(b"{")
            rejected("remote seal evidence is malformed", lambda: owned.store.seal("gold", digest))
        elif defect == "coding_error":
            try:
                with patch.object(remote_run, "read_record", side_effect=AttributeError("Synthetic reader coding error")):
                    owned.store.seal("gold", digest)
            except AttributeError as error:
                require(str(error) == "Synthetic reader coding error", "the original coding error changed")
            else:
                raise AssertionError("the reader coding error did not propagate")
        elif defect == "generation":
            owned.change(lambda state: state["runs"][RUN].update(generation=2))
            rejected("stale run generation", lambda: owned.store.seal("gold", digest))
        elif defect == "cancelled":
            owned.change(lambda state: state["attempts"][seal["job"]["attempt_id"]].update(cancel_intent=True))
            rejected("result_identity", lambda: owned.store.seal("gold", digest))
        elif defect == "stale_try":
            owned.change(lambda state: state["attempts"][seal["job"]["attempt_id"]]["execution"].update(live_try="different_try"))
            rejected("result_identity", lambda: owned.store.seal("gold", digest))
        else:
            params = {"layer": "gold", "attempt_id": owned.validation_ids["gold"], "cross_attempt_id": owned.validation_ids["cross"]}
            if defect == "validation":
                owned.change(lambda state: state["attempts"][owned.validation_ids["gold"]].update(exit_code=1))
                cause = "did not finish successfully"
            elif defect == "build":
                owned.change(lambda state: state["attempts"][build["job"]["attempt_id"]].update(exit_code=1))
                cause = "latest frozen-source build did not succeed"
            elif defect == "frozen":
                owned.change(lambda state: state["copies"]["fixture_snapshot"].update(dbt_tree_sha256="0" * 64))
                cause = "frozen source or accepted inputs"
            elif defect == "cross":
                owned.change(lambda state: state["attempts"][owned.validation_ids["cross"]].update(exit_code=1))
                cause = "cross-layer attempt did not succeed"
            elif defect == "consumer_digest":
                owned.route("pipeline_accept_seal", params, {"run_id": RUN})
                context = owned.export("gold")
                damaged = replace(context, sealed_sha256="0" * 64)
                rejected("differs from the completion", lambda: gold_publish.verified_export(damaged))
                return {"defect": defect, "refused": True}
            else:
                raise AssertionError("unknown acceptance defect")
            rejected(cause, lambda: owned.route("pipeline_accept_seal", params, {"run_id": RUN}))
        require(not owned.state()["runs"][RUN].get("layer_completions", {}).get("gold"), "rejected evidence wrote a layer completion")
        return {"defect": defect, "refused": True}
    finally:
        owned.close()


def placement_defect(folder: Path, defect: str) -> dict[str, Any]:
    attempt: dict[str, Any] = {"id": "placement_fixture", "deadline_utc": 1000.0}
    ecs_target.initialize(attempt, "auto", 0, None)
    view = ecs_target.Availability(portable=True)
    decision = ecs_target.select(attempt, "offline", SIZE, view, 1)
    require(decision.target == "ecs" and decision.provider == "FARGATE_SPOT", "AWS was not first")
    if defect == "aws_first":
        return {"target": decision.target, "provider": decision.provider}
    placement = ecs_target.reserve(attempt, SIZE, decision, 1, ecs_target.Usage())
    if defect == "duplicate":
        rejected("live placement already exists", lambda: ecs_target.reserve(attempt, SIZE, decision, 2, ecs_target.Usage()))
    elif defect in ("writer", "started"):
        placement.update(started=True, state="running")
        before = copy.deepcopy(attempt["execution"])
        decision = ecs_target.select(
            attempt, "lakehouse_write" if defect == "writer" else "offline", SIZE, replace(view, reason="capacity", observed_at=2, no_start=True), 2
        )
        require(decision.action == "reconcile" and attempt["execution"] == before, "live task relocated after start")
        ecs_target.fenced(attempt, placement["try_id"], 3, no_start=False)
        decision = ecs_target.select(
            attempt, "lakehouse_write" if defect == "writer" else "offline", SIZE, replace(view, reason="cloud_cap", usage=ecs_target.Usage(tasks=3)), 4
        )
        require(decision.action == "refuse", "started task replayed after fencing")
    elif defect == "stale_bottleneck":
        ecs_target.fenced(attempt, placement["try_id"], 2, no_start=True)
        decision = ecs_target.select(attempt, "offline", SIZE, replace(view, reason="breaker", observed_at=1), 100)
        require(decision.action == "wait" and decision.reason == "stale_bottleneck", "stale bottleneck authorized fallback")
    elif defect == "unverified":
        from scripts.orchestration.ecs_fake_s3 import FakeS3

        model = FakeS3(run_prefix(RUN, 1))
        raw = b"unverified Synthetic transfer"
        key = run_prefix(RUN, 1) + "inputs/files/input.duckdb"
        head = model.seed(key, raw, {"sha256": hashlib.sha256(raw).hexdigest()})
        reference = FileRef("input.duckdb", key, head.version_id, "0" * 64, len(raw))
        rejected("input_digest", lambda: Transfer(Storage(model)).download(reference, folder / "input.duckdb", time.monotonic() + 10))
        require(not (folder / "input.duckdb").exists(), "unverified transfer became visible")
    else:
        raise AssertionError("unknown placement defect")
    return {"defect": defect, "refused": True}


def mutations(folder: Path) -> list[dict[str, Any]]:
    """Prove named cases reject generated source defects after unchanged lint checks."""
    from scripts.orchestration.run_pipeline_flow_e2e import final_consumer, seal_case

    probes: tuple[tuple[str, str, tuple[tuple[str, str], ...], Any, str, Callable[[Path], dict[str, Any]], str], ...] = (
        (
            "omit_seal_hash",
            "scripts/orchestration/pipeline_seal.py",
            (('if (digest, size) != (request["sha256"], source.size):', "if digest and size != source.size:"),),
            pipeline_seal,
            "seal",
            partial(seal_case, defect="bytes"),
            "missing refusal: seal_digest",
        ),
        (
            "wrong_generation_prefix",
            "scripts/orchestration/pipeline_seal.py",
            (('source.key.startswith(prefix + "tries/")', 'source.key.startswith(prefix.rsplit("/g", 1)[0] + "/g1/tries/")'),),
            pipeline_seal,
            "evidence",
            partial(seal_case, defect="generation"),
            "missing refusal: seal_binding",
        ),
        (
            "write_undeclared_result",
            "scripts/orchestration/pipeline_seal.py",
            (('    raw = canonical(record) + b"\\n"\n', '    raw = canonical(record) + b"\\n"\n    (outputs / "consumer_result.json").write_bytes(b"{}")\n'),),
            pipeline_seal,
            "seal",
            partial(seal_case, defect="none"),
            "seal wrote an undeclared file",
        ),
        (
            "omit_seal_conflict",
            "scripts/orchestration/pipeline_seal.py",
            (("if size != len(raw) or digest != hashlib.sha256(raw).hexdigest():", "if size == 0 and digest != hashlib.sha256(raw).hexdigest():"),),
            pipeline_seal,
            "_existing",
            partial(seal_case, defect="conflict"),
            "missing refusal: seal_conflict",
        ),
        (
            "omit_seal_receipt",
            "orchestration/remote_run.py",
            (('or output_record(attempt).get("seal_sha256") != digest', "or not output_record(attempt)"),),
            remote_run,
            "RemoteRunStore",
            partial(acceptance_defect, defect="receipt"),
            "missing refusal: seal_receipt",
        ),
        (
            "omit_source_binding",
            "orchestration/remote_run.py",
            (("if record != wanted or validation[", "if set(record) != set(wanted) or validation["),),
            remote_run,
            "RemoteRunStore",
            partial(acceptance_defect, defect="source"),
            "missing refusal: seal_binding",
        ),
        (
            "retain_previous_exports",
            "orchestration/remote_run.py",
            (
                (
                    'if template != "pipeline_export" or not path.startswith("publish/export/")',
                    'if template != "pipeline_export" or not path.startswith("publish/obsolete_export/")',
                ),
            ),
            remote_run,
            "updated_result_map",
            partial(run_flow, name="F1", native_quality=False),
            "an exported file changed after the consumer wrote it",
        ),
        (
            "accept_extra_export",
            "scripts/lakehouse/gold_publish.py",
            (("{path.name for path in export_dir.iterdir()} != expected", "not expected <= {path.name for path in export_dir.iterdir()}"),),
            gold_publish,
            "export_files_match",
            partial(final_consumer, defect="extra"),
            "missing refusal: exported file",
        ),
        (
            "duplicate_live_placement",
            "orchestration/ecs_target.py",
            (
                (
                    'if execution["live_try"] is not None:\n        raise PlacementError("a live placement already exists")',
                    'if execution["live_try"] is not None and not decision.target:\n        raise PlacementError("a live placement already exists")',
                ),
            ),
            ecs_target,
            "reserve",
            partial(placement_defect, defect="duplicate"),
            "missing refusal: live placement already exists",
        ),
        (
            "unverified_input_transfer",
            "services/hai/bootstrap/transfer.py",
            (("if (digest, size) != (reference.sha256, reference.size):", "if size != reference.size or not digest:"),),
            Transfer,
            "download",
            partial(placement_defect, defect="unverified"),
            "missing refusal: input_digest",
        ),
        (
            "relocate_started_writer",
            "orchestration/ecs_target.py",
            (
                ('if job_class == "lakehouse_write":', 'if job_class == "disabled_writer_class":'),
                (
                    'if any(item["started"] or not item["no_start"] for item in previous):',
                    'if any(not item["started"] and not item["no_start"] for item in previous):',
                ),
            ),
            ecs_target,
            "select",
            partial(placement_defect, defect="writer"),
            "started task replayed after fencing",
        ),
        (
            "omit_complete_seal_placement",
            "orchestration/remote_run.py",
            (('                    or placements[-1].get("state") != "complete"\n', ""),),
            remote_run,
            "RemoteRunStore",
            partial(acceptance_defect, defect="running"),
            "missing refusal: result_identity",
        ),
        (
            "omit_store_seal_conflict",
            "orchestration/remote_run.py",
            (
                (
                    '                previous = stage.setdefault("sealed", {}).get(layer)\n'
                    "                if previous is not None and previous != record:\n"
                    '                    raise StageError("seal_conflict", 71)\n',
                    '                stage.setdefault("sealed", {})\n',
                ),
            ),
            remote_run,
            "RemoteRunStore",
            partial(acceptance_defect, defect="store_conflict"),
            "missing refusal: seal_conflict",
        ),
        (
            "omit_returned_seal_head",
            "orchestration/remote_run.py",
            (('                self._head(FileRef("seal.json", returned["key"], returned["version_id"], digest, returned["size"]))\n', ""),),
            remote_run,
            "RemoteRunStore",
            partial(acceptance_defect, defect="seal_head"),
            "missing refusal: stage_conflict",
        ),
        (
            "omit_seal_source_head",
            "orchestration/remote_run.py",
            (
                ("                reference, wanted = evidence(", "                _, wanted = evidence("),
                ("                self._head(reference)\n                self._head(FileRef(", "                self._head(FileRef("),
            ),
            remote_run,
            "RemoteRunStore",
            partial(acceptance_defect, defect="source_head"),
            "missing refusal: stage_conflict",
        ),
        (
            "omit_validation_expected_hash",
            "orchestration/remote_run.py",
            (
                ("                validation = validation_evidence(", "                validation_evidence("),
                (' or validation["sha256"] != expected', ""),
            ),
            remote_run,
            "RemoteRunStore",
            partial(acceptance_defect, defect="validation_hash"),
            "missing refusal: seal_binding",
        ),
        (
            "omit_seal_task_arn",
            "orchestration/remote_run.py",
            (('                    or not placements[-1].get("task_arn")\n', ""),),
            remote_run,
            "RemoteRunStore",
            partial(acceptance_defect, defect="task_arn"),
            "missing refusal: result_identity",
        ),
        (
            "accept_symlink_export",
            "scripts/lakehouse/gold_publish.py",
            (("if file.is_symlink() or not file.is_file() or sha256_file(file)", "if not file.is_file() or sha256_file(file)"),),
            gold_publish,
            "export_files_match",
            partial(final_consumer, defect="symlink"),
            "missing refusal: exported file",
        ),
    )
    size_cases = (
        ("consumer", partial(final_consumer, defect="replaced")),
        ("contract_F1", partial(run_flow, name="F1", native_quality=False)),
        ("contract_F3", partial(run_flow, name="F3", native_quality=False)),
    )
    probes += tuple(
        (
            "size_only_export_" + name,
            "scripts/lakehouse/gold_publish.py",
            (('sha256_file(file) != item["file_sha256"]', 'file.stat().st_size != item["bytes"]'),),
            gold_publish,
            "export_files_match",
            case,
            "missing refusal: exported file",
        )
        for name, case in size_cases
    )
    folder.mkdir()
    results = []
    for name, source, replacements, target, attribute, case, expected in probes:
        with tempfile.TemporaryDirectory(prefix=name + "_", dir=folder) as temporary:
            scratch = Path(temporary).resolve()
            baseline = scratch / "baseline"
            baseline.mkdir()
            case(baseline)
            module, evidence = load_mutant(ROOT / source, replacements, scratch, name)
            fixture = scratch / "mutant_fixture"
            fixture.mkdir()
            observed = ""
            replacement = module.Transfer.download if target is Transfer else getattr(module, attribute)
            with patch.object(target, attribute, replacement):
                try:
                    case(fixture)
                except (AssertionError, RuntimeError, OSError, launcher.ClientError, ecs_target.PlacementError) as error:
                    observed = str(error)
            results.append({"case": name, "ok": observed == expected, "baseline": "passed", "expected": expected, "observed": observed, **evidence})
            (folder / "results.json").write_bytes(canonical(results) + b"\n")
    return results
