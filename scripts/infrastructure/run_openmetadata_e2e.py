"""Exercise the on-demand OpenMetadata catalog task offline: its ingestion guard, bootstrap, Marquez step, deadline wrapper, read-only policy and DAG.

Nothing here reaches AWS, Docker or a real OpenMetadata server. HTTP stand-ins on loopback sockets answer the routes the scripts call, so the cases prove the
scripts' own behavior and not the real server's. Every case is a function that raises ``AssertionError`` on a defect. The runner writes one record per run.
"""

from __future__ import annotations

import argparse
import contextlib
import copy
import io
import json
import os
import re
import sys
import tempfile
import threading
import time
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from scripts.process import run_command

REPO = Path(__file__).resolve().parents[2]
INGESTION = REPO / "config" / "metadata" / "ingestion"
OWNER_POLICY = REPO / "infra" / "iam" / "owner_policies" / "openmetadata_catalog_read.json"
DAG_FILE = REPO / "dags" / "metadata_ingest_dag.py"
ADMIN_VALUE = "Synthetic-Admin-1!"
BEARER_VALUE = "synthetic.jwt.token"
CLIENT = ("synthetic-client-id", "synthetic-client-secret")
MUTANTS: dict[str, Callable[[dict[str, Any]], None]] = {
    "profiler pipeline type": lambda c: c["source"]["sourceConfig"]["config"].update(type="Profiler"),
    "sample data on": lambda c: c["source"]["sourceConfig"]["config"].update(generateSampleData=True),
    "underscore spelling": lambda c: c["source"]["sourceConfig"]["config"].update(profile_sample=10),
    "sample count as text": lambda c: c["source"]["sourceConfig"]["config"].update(sampleDataCount="50"),
    "orm profiler processor": lambda c: c.update(processor={"type": "orm-profiler", "config": {}}),
    "metrics on": lambda c: c["source"]["sourceConfig"]["config"].update(computeMetrics=True),
    "auto classification": lambda c: c["source"]["sourceConfig"]["config"].update(enableAutoClassification=True),
    "nested sampling block": lambda c: c["source"]["sourceConfig"]["config"].update(extra={"sampling": {"method": "BERNOULLI"}}),
    "unknown source type": lambda c: c["source"].update(type="postgres"),
}


def require(condition: object, message: object) -> None:
    if not condition:
        raise AssertionError(message)


def load(name: str) -> dict[str, Any]:
    document = json.loads((INGESTION / f"{name}.json").read_text())
    require(isinstance(document, dict), f"{name}.json is not a mapping")
    return document


@contextlib.contextmanager
def quiet() -> Iterator[tuple[io.StringIO, io.StringIO]]:
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        yield out, err


class Stand(ThreadingHTTPServer):
    """A loopback HTTP stand-in that records every request and answers from a route table."""

    def __init__(self, routes: Callable[[str, str, dict[str, Any]], tuple[int, Any]]) -> None:
        self.calls: list[tuple[str, str, dict[str, Any]]] = []
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def handle_any(self) -> None:
                length = int(self.headers.get("Content-Length") or 0)
                raw = self.rfile.read(length) if length else b""
                body = json.loads(raw) if raw else {}
                outer.calls.append((self.command, self.path, body))
                status, payload = routes(self.command, self.path, body)
                data = json.dumps(payload).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            do_GET = do_POST = do_PUT = handle_any

            def log_message(self, *_: Any) -> None:
                return

        super().__init__(("127.0.0.1", 0), Handler)

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.server_address[1]}"


@contextlib.contextmanager
def serving(routes: Callable[[str, str, dict[str, Any]], tuple[int, Any]]) -> Iterator[Stand]:
    server = Stand(routes)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def real_configs_have_no_profiler_or_sampling() -> None:
    from scripts.infrastructure import openmetadata_ingest as ingest

    names = sorted(path.stem for path in INGESTION.glob("*.json"))
    require({"polaris", "dbt", "marquez"} <= set(names), f"missing ingestion configs, found {names}")
    for name in names:
        found = ingest.violations(load(name))
        require(found == [], f"{name}.json: {found}")


def every_profiler_or_sampling_mutant_is_refused() -> None:
    from scripts.infrastructure import openmetadata_ingest as ingest

    for label, mutate in MUTANTS.items():
        config = copy.deepcopy(load("polaris"))
        mutate(config)
        require(ingest.violations(config), f"mutant not refused: {label}")
    clean = copy.deepcopy(load("polaris"))
    clean["source"]["sourceConfig"]["config"].update(generateSampleData=False, computeMetrics=False)
    require(ingest.violations(clean) == [], "an explicit false must stay allowed")


def run_ingest(args: list[str], env: dict[str, str], workflow: Callable[[dict[str, Any]], None], **extra: Any) -> tuple[int, str, str]:
    from scripts.infrastructure import openmetadata_ingest as ingest

    with quiet() as (out, err):
        code = ingest.main(args, env=env, workflow=workflow, **extra)
    return code, out.getvalue(), err.getvalue()


def ingest_environment(root: Path) -> tuple[list[str], dict[str, str]]:
    token_file = root / "ingestion_token"
    token_file.write_text(BEARER_VALUE + "\n")
    env = {
        "POLARIS_URI": "http://10.0.1.20:8181/api/catalog",
        "POLARIS_CLIENT_ID": CLIENT[0],
        "POLARIS_CLIENT_SECRET": CLIENT[1],
        "DATA_BUCKET_NAME": "example-project-data-dev",
        "AWS_REGION": "us-west-2",
    }
    return ["--config-dir", str(INGESTION), "--token-file", str(token_file), "--work-dir", str(root / "work")], env


def ingest_substitutes_values_and_keeps_secrets_out_of_output() -> None:
    seen: list[dict[str, Any]] = []
    with tempfile.TemporaryDirectory() as folder:
        args, env = ingest_environment(Path(folder))
        code, out, err = run_ingest(["polaris", *args], env, seen.append)
    require(code == 0, (code, err))
    require(len(seen) == 1, "the workflow must run once")
    text = json.dumps(seen[0])
    require(env["POLARIS_URI"] in text and CLIENT[0] in text and CLIENT[1] in text and BEARER_VALUE in text, "values must reach the workflow")
    require("${" not in text, "a placeholder was left")
    for secret in (CLIENT[0], CLIENT[1], BEARER_VALUE):
        require(secret not in out + err, "a secret reached the output")


def ingest_refuses_a_profiler_config_before_the_workflow_starts() -> None:
    seen: list[dict[str, Any]] = []
    with tempfile.TemporaryDirectory() as folder:
        root = Path(folder)
        args, env = ingest_environment(root)
        config = load("polaris")
        config["source"]["sourceConfig"]["config"]["generateSampleData"] = True
        bad = root / "bad"
        bad.mkdir()
        (bad / "polaris.json").write_text(json.dumps(config))
        args[args.index("--config-dir") + 1] = str(bad)
        code, _, err = run_ingest(["polaris", *args], env, seen.append)
    require(code == 2 and seen == [], (code, seen))
    require("generatesampledata" in err.lower(), err)


def ingest_names_a_missing_variable_but_not_a_value() -> None:
    with tempfile.TemporaryDirectory() as folder:
        args, env = ingest_environment(Path(folder))
        del env["POLARIS_CLIENT_SECRET"]
        code, out, err = run_ingest(["polaris", *args], env, lambda _: None)
    require(code == 2, code)
    require("POLARIS_CLIENT_SECRET" in err and CLIENT[0] not in out + err, err)


def ingest_fails_when_the_workflow_fails_without_echoing_the_config() -> None:
    def broken(config: dict[str, Any]) -> None:
        raise RuntimeError(f"upstream said {config['source']['serviceConnection']}")

    with tempfile.TemporaryDirectory() as folder:
        args, env = ingest_environment(Path(folder))
        code, out, err = run_ingest(["polaris", *args], env, broken)
    require(code == 1, code)
    require("RuntimeError" in err and CLIENT[1] not in out + err and BEARER_VALUE not in out + err, err)


class FakeS3:
    def __init__(self, keys: dict[str, bytes]) -> None:
        self.keys = keys
        self.requested: list[str] = []

    def download_file(self, bucket: str, key: str, target: str) -> None:
        self.requested.append(f"{bucket}/{key}")
        if key not in self.keys:
            raise FileNotFoundError(key)
        Path(target).write_bytes(self.keys[key])


def ingest_dbt_downloads_the_artifacts_and_fails_without_a_manifest() -> None:
    keys = {f"metadata/dbt/{name}": b"{}" for name in ("manifest.json", "catalog.json", "run_results.json")}
    seen: list[dict[str, Any]] = []
    with tempfile.TemporaryDirectory() as folder:
        args, env = ingest_environment(Path(folder))
        s3 = FakeS3(keys)
        code, _, err = run_ingest(["dbt", *args], env, seen.append, s3_client=s3)
        require(code == 0, (code, err))
        require(sorted(s3.requested) == sorted(f"example-project-data-dev/{key}" for key in keys), s3.requested)
        require(seen and str(Path(folder) / "work") in json.dumps(seen[0]), "the workflow must read the downloaded files")
        seen.clear()
        del keys["metadata/dbt/manifest.json"]
        code, _, err = run_ingest(["dbt", *args], env, seen.append, s3_client=FakeS3(keys))
    require(code == 1 and seen == [] and "manifest.json" in err, (code, err))


def om_routes(state: dict[str, Any]) -> Callable[[str, str, dict[str, Any]], tuple[int, Any]]:
    """The routes of a stand-in OpenMetadata server. The paths are the bootstrap script's assumptions about the real server."""

    def route(method: str, path: str, body: dict[str, Any]) -> tuple[int, Any]:
        if path == "/api/v1/system/version":
            return (200, {"version": "stand-in"}) if time.monotonic() >= state.get("healthy_at", 0) else (503, {})
        if path == "/api/v1/users/login":
            import base64

            if body.get("email") == "admin@open-metadata.org" and base64.b64decode(body["password"]).decode() == state["password"]:
                return 200, {"accessToken": "login.session.token"}
            return 401, {}
        if path == "/api/v1/users/changePassword":
            state["changes"] = state.get("changes", 0) + 1
            if body.get("oldPassword") != state["password"]:
                return 400, {}
            state["password"] = body["newPassword"]
            return 200, {}
        if path == "/api/v1/bots/name/ingestion-bot":
            return 200, {"botUser": {"id": "bot-user-id"}}
        if path == "/api/v1/users/auth-mechanism/bot-user-id":
            return 200, {"config": {"JWTToken": BEARER_VALUE}}
        return 404, {}

    return route


def run_bootstrap(args: list[str], env: dict[str, str]) -> tuple[int, str, str]:
    from scripts.infrastructure import openmetadata_bootstrap as bootstrap

    with quiet() as (out, err):
        code = bootstrap.main(args, env=env)
    return code, out.getvalue(), err.getvalue()


def bootstrap_changes_the_default_password_once_and_hands_over_a_token() -> None:
    state: dict[str, Any] = {"password": "admin", "healthy_at": time.monotonic() + 1.0}
    with serving(om_routes(state)) as server, tempfile.TemporaryDirectory() as folder:
        root = Path(folder)
        args = [
            "--base",
            server.url + "/api",
            "--token-file",
            str(root / "token"),
            "--abort-file",
            str(root / "abort"),
            "--deadline-seconds",
            "20",
            "--poll-seconds",
            "1",
        ]
        env = {"OM_ADMIN_PASSWORD": ADMIN_VALUE}
        code, out, err = run_bootstrap(args, env)
        require(code == 0, (code, err))
        require(state["password"] == ADMIN_VALUE and state["changes"] == 1, state)
        require((root / "token").read_text().strip() == BEARER_VALUE, '(root / "token").read_text().strip() == BEARER_VALUE')
        require(oct((root / "token").stat().st_mode & 0o777) == "0o600", "the token file must be owner-only")
        require(not (root / "abort").exists(), 'not (root / "abort").exists()')
        code, out2, err2 = run_bootstrap(args, env)
        require(code == 0 and state["changes"] == 1, "a rerun must not change the password again")
    for secret in (ADMIN_VALUE, BEARER_VALUE, "login.session.token"):
        require(secret not in out + err + out2 + err2, "a secret reached the output")


def bootstrap_marks_an_abort_when_the_server_never_gets_healthy() -> None:
    state: dict[str, Any] = {"password": "admin", "healthy_at": time.monotonic() + 3600}
    with serving(om_routes(state)) as server, tempfile.TemporaryDirectory() as folder:
        root = Path(folder)
        args = [
            "--base",
            server.url + "/api",
            "--token-file",
            str(root / "token"),
            "--abort-file",
            str(root / "abort"),
            "--deadline-seconds",
            "2",
            "--poll-seconds",
            "1",
        ]
        started = time.monotonic()
        code, _, err = run_bootstrap(args, {"OM_ADMIN_PASSWORD": ADMIN_VALUE})
        require(code == 1 and (root / "abort").exists() and not (root / "token").exists(), (code, err))
        require(time.monotonic() - started < 15, "the wait must end at its deadline")


def bootstrap_marks_an_abort_when_no_known_password_logs_in() -> None:
    state: dict[str, Any] = {"password": "Some-Other-1!"}
    with serving(om_routes(state)) as server, tempfile.TemporaryDirectory() as folder:
        root = Path(folder)
        args = [
            "--base",
            server.url + "/api",
            "--token-file",
            str(root / "token"),
            "--abort-file",
            str(root / "abort"),
            "--deadline-seconds",
            "5",
            "--poll-seconds",
            "1",
        ]
        code, out, err = run_bootstrap(args, {"OM_ADMIN_PASSWORD": ADMIN_VALUE})
        require(code == 1 and (root / "abort").exists(), (code, err))
        require(state.get("changes", 0) == 0 and ADMIN_VALUE not in out + err, 'state.get("changes", 0) == 0 and ADMIN_VALUE not in out + err')


def bootstrap_refuses_a_missing_password() -> None:
    with tempfile.TemporaryDirectory() as folder:
        root = Path(folder)
        code, _, err = run_bootstrap(
            ["--base", "http://127.0.0.1:9/api", "--token-file", str(root / "t"), "--abort-file", str(root / "a"), "--deadline-seconds", "1"], {}
        )
    require(code == 1 and "OM_ADMIN_PASSWORD" in err, (code, err))


def marquez_routes() -> Callable[[str, str, dict[str, Any]], tuple[int, Any]]:
    graph = {
        "graph": [
            {
                "id": "job:file:build_silver",
                "type": "JOB",
                "data": {},
                "inEdges": [{"origin": "dataset:file:silver/hospital", "destination": "job:file:build_silver"}],
                "outEdges": [
                    {"origin": "job:file:build_silver", "destination": "dataset:file:gold/dim_hospital"},
                    {"origin": "job:file:build_silver", "destination": "dataset:file:tmp/scratch_file"},
                ],
            },
        ]
    }

    def route(method: str, path: str, body: dict[str, Any]) -> tuple[int, Any]:
        if path.startswith("/api/v1/namespaces?"):
            return 200, {"namespaces": [{"name": "file"}]}
        if path.startswith("/api/v1/namespaces/file/datasets"):
            return 200, {"datasets": [{"name": "silver/hospital"}, {"name": "gold/dim_hospital"}]}
        if path.startswith("/api/v1/lineage"):
            return 200, graph
        return 404, {}

    return route


def om_lineage_routes(edges: list[dict[str, Any]]) -> Callable[[str, str, dict[str, Any]], tuple[int, Any]]:
    def route(method: str, path: str, body: dict[str, Any]) -> tuple[int, Any]:
        if path.startswith("/api/v1/tables/name/"):
            return 200, {"id": "id-of-" + path.rsplit("/", 1)[-1]}
        if path == "/api/v1/lineage":
            edges.append(body)
            return 200, {}
        return 404, {}

    return route


def run_marquez(args: list[str]) -> tuple[int, str, str]:
    from scripts.infrastructure import openmetadata_marquez as marquez

    with quiet() as (out, err):
        code = marquez.main(args)
    return code, out.getvalue(), err.getvalue()


def marquez_step_adds_mapped_edges_and_reports_the_unmapped() -> None:
    edges: list[dict[str, Any]] = []
    with serving(marquez_routes()) as marquez, serving(om_lineage_routes(edges)) as om, tempfile.TemporaryDirectory() as folder:
        token = Path(folder) / "token"
        token.write_text(BEARER_VALUE)
        args = ["--marquez", marquez.url, "--om-base", om.url + "/api", "--token-file", str(token), "--config", str(INGESTION / "marquez.json")]
        code, out, err = run_marquez(args)
        require(code == 0, (code, err))
        require(len(edges) == 1, edges)
        edge = edges[0]["edge"]
        require(edge["fromEntity"]["type"] == "table" and edge["toEntity"]["type"] == "table", edge)
        require("unmapped" in out and "1" in out, out)
        edges.clear()
        run_marquez(args)
        require(len(edges) == 1, "a rerun must send the same edge again, which OpenMetadata treats as the same edge")


def marquez_step_fails_visibly_when_marquez_is_down() -> None:
    edges: list[dict[str, Any]] = []
    with serving(om_lineage_routes(edges)) as om, tempfile.TemporaryDirectory() as folder:
        token = Path(folder) / "token"
        token.write_text(BEARER_VALUE)
        args = [
            "--marquez",
            "http://127.0.0.1:9",
            "--om-base",
            om.url + "/api",
            "--token-file",
            str(token),
            "--config",
            str(INGESTION / "marquez.json"),
            "--timeout-seconds",
            "2",
        ]
        code, _, err = run_marquez(args)
    require(code == 1 and edges == [] and "marquez" in err.lower(), (code, err))


def paged_marquez_routes(namespaces: int, datasets: int, ignore_offset: bool = False) -> Callable[[str, str, dict[str, Any]], tuple[int, Any]]:
    """A Marquez stand-in that pages its listings by limit and offset. The only connected pair sits at the end of the last page of the last namespace."""
    names = [f"ns{index:03d}" for index in range(namespaces)] + ["file"]
    rows = [f"filler/set_{index:03d}" for index in range(datasets - 2)] + ["silver/late_a", "gold/late_b"]
    graph = {
        "graph": [
            {
                "id": "job:file:late_job",
                "type": "JOB",
                "inEdges": [{"origin": "dataset:file:silver/late_a", "destination": "job:file:late_job"}],
                "outEdges": [{"origin": "job:file:late_job", "destination": "dataset:file:gold/late_b"}],
            }
        ]
    }

    def window(items: list[Any], query: str) -> list[Any]:
        values = dict(part.split("=") for part in query.split("&") if "=" in part)
        limit, offset = int(values.get("limit", "100")), 0 if ignore_offset else int(values.get("offset", "0"))
        return items[offset : offset + limit]

    def route(method: str, path: str, body: dict[str, Any]) -> tuple[int, Any]:
        base, _, query = path.partition("?")
        if base == "/api/v1/namespaces":
            return 200, {"namespaces": [{"name": name} for name in window(names, query)]}
        if base == "/api/v1/namespaces/file/datasets":
            return 200, {"datasets": [{"name": name} for name in window(rows, query)], **({} if ignore_offset else {"totalCount": len(rows)})}
        if base.startswith("/api/v1/namespaces/") and base.endswith("/datasets"):
            return 200, {"datasets": [], "totalCount": 0}
        if base == "/api/v1/lineage":
            return 200, graph if "late_a" in query else {"graph": []}
        return 404, {}

    return route


def marquez_step_pages_namespaces_and_datasets_to_the_end() -> None:
    edges: list[dict[str, Any]] = []
    with serving(paged_marquez_routes(205, 202)) as marquez, serving(om_lineage_routes(edges)) as om, tempfile.TemporaryDirectory() as folder:
        token = Path(folder) / "token"
        token.write_text(BEARER_VALUE)
        args = ["--marquez", marquez.url, "--om-base", om.url + "/api", "--token-file", str(token), "--config", str(INGESTION / "marquez.json")]
        code, out, err = run_marquez(args)
    require(code == 0 and len(edges) == 1, f"the edge beyond the first pages was dropped: exit {code}, edges {len(edges)}, {out.strip()} {err.strip()}")


def marquez_step_stops_at_its_deadline_when_paging_never_ends() -> None:
    edges: list[dict[str, Any]] = []
    with serving(paged_marquez_routes(1, 300, ignore_offset=True)) as marquez, serving(om_lineage_routes(edges)) as om, tempfile.TemporaryDirectory() as folder:
        token = Path(folder) / "token"
        token.write_text(BEARER_VALUE)
        args = [
            "--marquez",
            marquez.url,
            "--om-base",
            om.url + "/api",
            "--token-file",
            str(token),
            "--config",
            str(INGESTION / "marquez.json"),
            "--deadline-seconds",
            "3",
        ]
        started = time.monotonic()
        code, _, err = run_marquez(args)
    require(
        code == 1 and time.monotonic() - started < 30 and "deadline" in err.lower(), f"an endless listing must end at the deadline: exit {code}, {err.strip()}"
    )


def polaris_and_marquez_configs_use_the_same_database_name() -> None:
    catalog = load("polaris")["source"]["serviceConnection"]["config"]["catalog"]
    require(
        catalog.get("databaseName") == load("marquez")["database"],
        f"the Iceberg catalog database name is {catalog.get('databaseName')!r} and the mapper looks for {load('marquez')['database']!r}",
    )


def bounded_environment(root: Path, deadline_in: int) -> dict[str, str]:
    """The environment of one wrapper run: a deadline file the given number of seconds from now, an abort file path, a 1 second poll and a 3 second grace."""
    (root / "deadline").write_text(f"{int(time.time()) + deadline_in}\n")
    return {
        **os.environ,
        "OM_DEADLINE_FILE": str(root / "deadline"),
        "OM_ABORT_FILE": str(root / "abort"),
        "OM_POLL_SECONDS": "1",
        "OM_STOP_GRACE_SECONDS": "3",
        "OM_MARKER": str(root / "ran"),
    }


def bounded_wrapper_stops_a_server_at_its_deadline_with_exit_zero() -> None:
    with tempfile.TemporaryDirectory() as folder:
        env = bounded_environment(Path(folder), 3)
        started = time.monotonic()
        done = run_command("sh", ["scripts/infrastructure/openmetadata_bounded_run.sh", "0", "sleep", "300"], cwd=REPO, env=env, timeout=60)
    require(done.returncode == 0 and time.monotonic() - started < 15, (done.returncode, time.monotonic() - started))


def bounded_wrapper_passes_on_a_crash_and_a_clean_exit() -> None:
    with tempfile.TemporaryDirectory() as folder:
        env = bounded_environment(Path(folder), 60)
        crashed = run_command("sh", ["scripts/infrastructure/openmetadata_bounded_run.sh", "0", "sh", "-c", "exit 3"], cwd=REPO, env=env, timeout=60)
        clean = run_command("sh", ["scripts/infrastructure/openmetadata_bounded_run.sh", "0", "true"], cwd=REPO, env=env, timeout=60)
    require(crashed.returncode == 3 and clean.returncode == 0, (crashed.returncode, clean.returncode))


def bounded_wrapper_refuses_to_start_after_the_deadline() -> None:
    with tempfile.TemporaryDirectory() as folder:
        env = bounded_environment(Path(folder), -10)
        done = run_command("sh", ["scripts/infrastructure/openmetadata_bounded_run.sh", "0", "sh", "-c", 'touch "$OM_MARKER"'], cwd=REPO, env=env, timeout=60)
        require(done.returncode == 1 and not (Path(folder) / "ran").exists(), "a command must not start after its deadline")


def bounded_wrapper_stops_on_the_abort_marker_with_exit_one() -> None:
    with tempfile.TemporaryDirectory() as folder:
        root = Path(folder)
        env = bounded_environment(root, 600)
        timer = threading.Timer(1.5, root.joinpath("abort").write_text, ["bootstrap failed\n"])
        timer.start()
        started = time.monotonic()
        done = run_command("sh", ["scripts/infrastructure/openmetadata_bounded_run.sh", "0", "sleep", "300"], cwd=REPO, env=env, timeout=60)
        timer.join()
    require(done.returncode == 1 and time.monotonic() - started < 15, (done.returncode, time.monotonic() - started))


def bounded_wrapper_extra_seconds_outlive_the_server_deadline() -> None:
    with tempfile.TemporaryDirectory() as folder:
        env = bounded_environment(Path(folder), 2)
        started = time.monotonic()
        done = run_command("sh", ["scripts/infrastructure/openmetadata_bounded_run.sh", "4", "sleep", "300"], cwd=REPO, env=env, timeout=60)
        took = time.monotonic() - started
    require(done.returncode == 0 and 5 <= took < 20, (done.returncode, took))


def bounded_wrapper_exits_zero_when_stopped_after_the_deadline() -> None:
    with tempfile.TemporaryDirectory() as folder:
        env = bounded_environment(Path(folder), 2)
        done = run_command(
            "sh",
            ["scripts/infrastructure/openmetadata_bounded_run.sh", "300", "sh", "-c", "sleep 4; kill -TERM $PPID; exec sleep 300"],
            cwd=REPO,
            env=env,
            timeout=60,
        )
    require(done.returncode == 0, f"a sibling stopped by ECS after the deadline must exit 0, got {done.returncode}")


def bounded_wrapper_fails_when_stopped_before_the_deadline() -> None:
    with tempfile.TemporaryDirectory() as folder:
        env = bounded_environment(Path(folder), 600)
        done = run_command(
            "sh",
            ["scripts/infrastructure/openmetadata_bounded_run.sh", "300", "sh", "-c", "sleep 1; kill -TERM $PPID; exec sleep 300"],
            cwd=REPO,
            env=env,
            timeout=60,
        )
    require(done.returncode == 1, f"a stop before the deadline is a failure, got {done.returncode}")


def migrate_script_writes_the_abort_marker_after_its_last_try() -> None:
    with tempfile.TemporaryDirectory() as folder:
        env = {**bounded_environment(Path(folder), 600), "OM_MIGRATE_TRIES": "2", "OM_MIGRATE_SLEEP": "1"}
        done = run_command("sh", ["scripts/infrastructure/openmetadata_migrate.sh", "false"], cwd=REPO, env=env, timeout=60)
        marker = Path(folder) / "abort"
        require(done.returncode == 1, f"the exhausted migration must fail, got {done.returncode}")
        require(marker.exists() and "migration" in marker.read_text(), "the exhausted migration must write the abort marker")


def migrate_script_passes_a_migration_that_succeeds_on_a_later_try() -> None:
    with tempfile.TemporaryDirectory() as folder:
        env = {**bounded_environment(Path(folder), 600), "OM_MIGRATE_TRIES": "3", "OM_MIGRATE_SLEEP": "1"}
        script = 'test -e "$OM_MARKER" || { touch "$OM_MARKER"; exit 1; }'
        done = run_command("sh", ["scripts/infrastructure/openmetadata_migrate.sh", "sh", "-c", script], cwd=REPO, env=env, timeout=60)
        require(done.returncode == 0 and not (Path(folder) / "abort").exists(), f"a later success must pass without an abort, got {done.returncode}")


def catalog_policy_reads_metadata_files_and_nothing_else() -> None:
    from infra.iam.scripts.render_policy import render_template
    from scripts.infrastructure.policy_evaluator import Request, decide

    values = {
        "AWS_REGION": "us-west-2",
        "AWS_ACCOUNT_ID": "111111111111",
        "PROJECT_NAME": "example_project",
        "ENVIRONMENT": "dev",
        "DATA_BUCKET_NAME": "example-project-data-dev",
    }
    document = render_template(OWNER_POLICY.read_text(), values, names={})
    bucket = "arn:aws:s3:::example-project-data-dev"
    allowed = [
        ("s3:GetObject", f"{bucket}/lakehouse/silver/hospital/metadata/00001.metadata.json", {}),
        ("s3:GetObject", f"{bucket}/lakehouse/gold/dim_hospital/metadata/snap-1.avro", {}),
        ("s3:GetObject", f"{bucket}/metadata/dbt/manifest.json", {}),
        ("s3:ListBucket", bucket, {"s3:prefix": ("lakehouse/silver/hospital/metadata/",)}),
        ("s3:ListBucket", bucket, {"s3:prefix": ("metadata/dbt/",)}),
    ]
    refused = [
        ("s3:GetObject", f"{bucket}/lakehouse/silver/hospital/data/00000-0.parquet", {}),
        ("s3:GetObject", f"{bucket}/lakehouse/bronze/raw/file.csv", {}),
        ("s3:GetObject", f"{bucket}/lakehouse/run_stage/run/inputs/code.tar.gz", {}),
        ("s3:GetObject", f"{bucket}/drift/run/drift_hac.json", {}),
        ("s3:PutObject", f"{bucket}/metadata/dbt/manifest.json", {}),
        ("s3:PutObject", f"{bucket}/lakehouse/silver/hospital/metadata/new.json", {}),
        ("s3:DeleteObject", f"{bucket}/lakehouse/silver/hospital/metadata/00001.metadata.json", {}),
        ("s3:ListBucket", bucket, {"s3:prefix": ("lakehouse/silver/hospital/data/",)}),
        ("s3:ListBucket", bucket, {"s3:prefix": ("",)}),
        ("ssm:GetParameter", "arn:aws:ssm:us-west-2:111111111111:parameter/example_project/polaris_root_secret", {}),
        ("ecs:RunTask", "*", {}),
    ]
    for action, resource, context in allowed:
        require(decide([document], Request(action, resource, context)).allowed, f"should allow {action} {resource}")
    for action, resource, context in refused:
        require(not decide([document], Request(action, resource, context)).allowed, f"should refuse {action} {resource}")


class Recorder:
    """Records the objects the DAG file builds when airflow's names are stand-ins."""

    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
        self.tasks: list[dict[str, Any]] = []

    def __enter__(self) -> Recorder:
        Recorder.current = self
        return self

    def __exit__(self, *_: Any) -> None:
        return None

    current: Recorder


def dag_runs_one_broker_step_with_bounds_and_no_schedule() -> None:
    import importlib.util
    import types

    sdk = types.ModuleType("airflow.sdk")
    calls: list[str] = []

    def task(*args: Any, **kwargs: Any) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        def wrap(function: Callable[..., Any]) -> Callable[..., Any]:
            Recorder.current.tasks.append({"name": function.__name__, **kwargs})
            return lambda *call_args, **call_kwargs: None

        return wrap

    for name, value in {
        "DAG": Recorder,
        "Param": lambda *a, **k: {"args": a, **k},
        "Asset": object,
        "TriggerRule": types.SimpleNamespace(ALL_DONE="all_done"),
        "get_current_context": lambda: {},
        "task": task,
    }.items():
        setattr(sdk, name, value)
    runtime = types.ModuleType("orchestration.dag_runtime")

    def record_step(template: str) -> str:
        calls.append(template)
        return "succeeded"

    runtime.__dict__.update(STEP_TIMEOUT=None, run_step=record_step)
    saved = {name: sys.modules.get(name) for name in ("airflow", "airflow.sdk", "orchestration.dag_runtime")}
    sys.modules.update({"airflow": types.ModuleType("airflow"), "airflow.sdk": sdk, "orchestration.dag_runtime": runtime})
    try:
        spec = importlib.util.spec_from_file_location("metadata_ingest_dag_under_test", DAG_FILE)
        if spec is None or spec.loader is None:
            raise AssertionError("the DAG file cannot be loaded")
        loaded = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(loaded)
        dag = Recorder.current
    finally:
        for name, value in saved.items():
            if value is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = value
    require(dag.kwargs["dag_id"] == "metadata_ingest_dag" and dag.kwargs["schedule"] is None, dag.kwargs)
    require(dag.kwargs["max_active_runs"] == 1 and dag.kwargs["default_args"] == {"retries": 0}, dag.kwargs)
    require([item["name"] for item in dag.tasks] == ["metadata_run"], dag.tasks)
    step = dag.tasks[0]
    require(step["retries"] == 0 and 0 < step["execution_timeout"].total_seconds() <= 5 * 3600, step)
    require(dag.kwargs["dagrun_timeout"].total_seconds() <= 6 * 3600, "the DAG run must have a bound")
    require(calls == [], "importing the DAG must not start a step")
    require(loaded.LIFETIME_CEILING_SECONDS == 14400, "the DAG bound must follow the Terraform lifetime ceiling")


def images_file_names_exactly_the_images_the_task_definition_takes() -> None:
    document = json.loads((REPO / "config" / "metadata" / "images.json").read_text())
    images = document["images"]
    require(set(images) == {"server", "ingestion", "search", "database"}, sorted(images))
    for name, entry in images.items():
        require(entry["ecr_repository"] == f"<project>_openmetadata_{name}_<environment>", entry)
        require(entry["arm64"].startswith("unverified"), f"{name}: the Arm claim must be stated as unverified")
        require(entry["upstream"] and "@sha256:" not in entry["upstream"] and entry["push_by"] == "digest", entry)


def project_policies_pass_the_catalog_role_and_only_the_catalog_forward_document() -> None:
    from infra.iam.scripts.render_policy import render_template, terraform_names
    from scripts.infrastructure.policy_evaluator import Request, decide

    values = {
        "AWS_REGION": "us-west-2",
        "AWS_ACCOUNT_ID": "111111111111",
        "PROJECT_NAME": "example_project",
        "ENVIRONMENT": "dev",
        "DATA_BUCKET_NAME": "example-project-data-dev",
    }
    folder = Path(os.environ.get("OM_POLICY_DIRECTORY", str(REPO / "infra" / "iam" / "policies")))
    texts = [path.read_text() for path in sorted(folder.glob("*.json")) if path.name.endswith(("_iam_policy.json", "_ssm_policy.json"))]
    spare = {name: "/example_project/spare" for text in texts for name in re.findall(r"\$\{([A-Z][A-Z0-9_]*)\}", text) if name not in values}
    documents = [render_template(text, values, {**terraform_names(), **spare}) for text in texts]
    role = "arn:aws:iam::111111111111:role/example_project_job_openmetadata_task_dev"
    forward = "arn:aws:ssm:us-west-2:111111111111:document/example_project_catalog_forward_dev"
    generic = "arn:aws:ssm:us-west-2::document/AWS-StartPortForwardingSessionToRemoteHost"
    tasks = {"iam:PassedToService": ("ecs-tasks.amazonaws.com",)}
    require(
        any(decide([document], Request("iam:PassRole", role, tasks)).allowed for document in documents),
        "the project user must pass the catalog task role to ECS tasks",
    )
    require(
        not any(decide([document], Request("iam:PassRole", role, {"iam:PassedToService": ("ec2.amazonaws.com",)})).allowed for document in documents),
        "only ECS tasks may receive it",
    )
    require(
        any(decide([document], Request("ssm:StartSession", forward)).allowed for document in documents), "the project catalog forward document must be allowed"
    )
    require(
        not any(decide([document], Request("ssm:StartSession", generic)).allowed for document in documents),
        "the general remote-host forward must not be granted",
    )
    other = "arn:aws:ssm:us-west-2::document/AWS-StartInteractiveCommand"
    require(not any(decide([document], Request("ssm:StartSession", other)).allowed for document in documents), "no other session document may be added")


CASES: list[Callable[[], None]] = [
    marquez_step_pages_namespaces_and_datasets_to_the_end,
    marquez_step_stops_at_its_deadline_when_paging_never_ends,
    polaris_and_marquez_configs_use_the_same_database_name,
    bounded_wrapper_exits_zero_when_stopped_after_the_deadline,
    bounded_wrapper_fails_when_stopped_before_the_deadline,
    migrate_script_writes_the_abort_marker_after_its_last_try,
    migrate_script_passes_a_migration_that_succeeds_on_a_later_try,
    project_policies_pass_the_catalog_role_and_only_the_catalog_forward_document,
    images_file_names_exactly_the_images_the_task_definition_takes,
    real_configs_have_no_profiler_or_sampling,
    every_profiler_or_sampling_mutant_is_refused,
    ingest_substitutes_values_and_keeps_secrets_out_of_output,
    ingest_refuses_a_profiler_config_before_the_workflow_starts,
    ingest_names_a_missing_variable_but_not_a_value,
    ingest_fails_when_the_workflow_fails_without_echoing_the_config,
    ingest_dbt_downloads_the_artifacts_and_fails_without_a_manifest,
    bootstrap_changes_the_default_password_once_and_hands_over_a_token,
    bootstrap_marks_an_abort_when_the_server_never_gets_healthy,
    bootstrap_marks_an_abort_when_no_known_password_logs_in,
    bootstrap_refuses_a_missing_password,
    marquez_step_adds_mapped_edges_and_reports_the_unmapped,
    marquez_step_fails_visibly_when_marquez_is_down,
    bounded_wrapper_stops_a_server_at_its_deadline_with_exit_zero,
    bounded_wrapper_passes_on_a_crash_and_a_clean_exit,
    bounded_wrapper_refuses_to_start_after_the_deadline,
    bounded_wrapper_stops_on_the_abort_marker_with_exit_one,
    bounded_wrapper_extra_seconds_outlive_the_server_deadline,
    catalog_policy_reads_metadata_files_and_nothing_else,
    dag_runs_one_broker_step_with_bounds_and_no_schedule,
]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-directory", type=Path, default=None)
    parser.add_argument("--only", default=None, help="run the one case with this name")
    options = parser.parse_args()
    selected = [case for case in CASES if options.only in (None, case.__name__)]
    records = []
    for case in selected:
        started = time.monotonic()
        try:
            case()
            outcome, detail = "passed", ""
        except Exception as error:
            outcome, detail = "failed", f"{type(error).__name__}: {error}"[:400]
        records.append({"case": case.__name__, "outcome": outcome, "seconds": round(time.monotonic() - started, 2), "detail": detail})
        sys.stdout.write(f"{outcome:6} {case.__name__} {detail}\n")
    failed = [record for record in records if record["outcome"] != "passed"]
    if options.output_directory is not None:
        options.output_directory.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        (options.output_directory / f"openmetadata_e2e_{stamp}.json").write_text(
            json.dumps({"finished_at": stamp, "mode": "full" if options.only is None else "iteration", "cases": records}, indent=2) + "\n"
        )
    sys.stdout.write(f"{len(records) - len(failed)} passed, {len(failed)} failed\n")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
