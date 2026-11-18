"""Run the catalog restore against a stand-in backup folder and a stand-in Polaris and keep the report under data/e2e/catalog_restore/.

Run from the repository root: ``.venv/bin/python -m scripts.lakehouse.run_catalog_restore_e2e``. The stand-in Polaris answers the Iceberg REST routes
the step uses (load, drop and register a table, read a namespace) and keeps the tables in a dictionary. The cases of the ``restore`` step of
``aws_catalog_setup`` put it behind a loopback HTTP server, so the step's own request path runs. No AWS service, tunnel or credential is used.
"""

from __future__ import annotations

import contextlib
import hashlib
import io
import json
import sys
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, cast

from scripts.lakehouse import aws_catalog_setup as setup
from scripts.lakehouse import catalog_restore as restore
from scripts.lakehouse.catalog_restore import CATALOG, RestoreError
from scripts.lakehouse.catalog_setup_state import Deadline

REPO_ROOT = Path(__file__).resolve().parents[2]
OUTPUT = REPO_ROOT / "data" / "e2e" / "catalog_restore"
BUCKET = "example-project-data-dev"
# A run ID holds no time, so the newest snapshot of a folder is the one stored last: RUN_B is stored after RUN_A and sorts before it.
RUN_A = "gold_bbbbbbbb"
RUN_B = "gold_aaaaaaaa"
ACCESS = "stand-in-token"


MAIN: dict[str, int] = {}
RUNS_AT: dict[str, dict[str, Any]] = {}


def snapshot_id(namespace: str, table: str, sequence: int, variant: str = "") -> int:
    return int(hashlib.sha256(f"{namespace}.{table}.{sequence}{variant}".encode()).hexdigest()[:12], 16)


def location(namespace: str, table: str, sequence: int, variant: str = "") -> str:
    name = uuid.uuid5(uuid.NAMESPACE_URL, f"{namespace}.{table}.{sequence}{variant}")
    found = f"s3://{BUCKET}/lakehouse/{namespace}/{table}/metadata/{sequence:05d}-{name}.metadata.json"
    MAIN[found] = snapshot_id(namespace, table, sequence, variant)
    return found


def committed(run: str, namespace: str, tables: dict[str, int | None], *, wrong_main: str = "") -> bytes:
    """The post-commit recovery snapshot of one publish: per table the metadata file whose main is the committed snapshot, and the snapshot set."""
    entries: dict[str, Any] = {}
    snapshot_set: dict[str, int] = {}
    for name, seq in tables.items():
        if seq is None:
            entries[name] = {"metadata_file": None, "main_snapshot_id": None}
            continue
        found = location(namespace, name, seq)
        entries[name] = {"metadata_file": found, "main_snapshot_id": MAIN[found]}
        if name != "publish_runs":
            snapshot_set[name] = MAIN[found] + (1 if name == wrong_main else 0)
    if "publish_runs" in entries and entries["publish_runs"]["metadata_file"]:
        RUNS_AT[entries["publish_runs"]["metadata_file"]] = {"run_id": run, "snapshot_set": dict(snapshot_set)}
    document = {
        "schema": 2,
        "kind": "committed",
        "run_id": run,
        "namespace": namespace,
        "runs_table": "publish_runs",
        "snapshot_set": snapshot_set,
        "tables": entries,
    }
    return json.dumps(document).encode()


def backup(run: str, namespace: str, tables: dict[str, int | None]) -> bytes:
    """The older pre-commit backup (schema 1): its metadata files are captured before the fast-forward, so the restore must never use it."""
    body = {
        "schema": 1,
        "run_id": run,
        "namespace": namespace,
        "tables": {
            name: {"metadata_file": None if seq is None else location(namespace, name, seq), "refs": {}, "snapshots": []} for name, seq in tables.items()
        },
    }
    return json.dumps(body).encode()


class StandInStore:
    """Backup objects by key."""

    def __init__(self, objects: dict[str, bytes]) -> None:
        self.objects = objects

    def names(self, prefix: str) -> list[str]:
        return [key for key in self.objects if key.startswith(prefix) and "/" not in key[len(prefix) :]]

    def read(self, key: str) -> bytes:
        return self.objects[key]


class StandInPolaris:
    """The catalog: ``tables`` maps (namespace, table) to its metadata file. Every call is kept in ``calls``."""

    def __init__(self, namespaces: tuple[str, ...], tables: dict[tuple[str, str], str] | None = None) -> None:
        self.namespaces, self.tables = set(namespaces), dict(tables or {})
        self.calls: list[tuple[str, str]] = []
        self.refuse_register: set[str] = set()
        self.refuse_drop = False

    def __call__(
        self, method: str, path: str, token: str | None = None, body: dict[str, Any] | None = None, form: dict[str, str] | None = None
    ) -> tuple[int, Any]:
        self.calls.append((method, path))
        if token != ACCESS:
            return 401, None
        base = f"/api/catalog/v1/{CATALOG}/namespaces/"
        if not path.startswith(base):
            return 404, None
        parts = path[len(base) :].split("/")
        namespace = parts[0]
        if namespace not in self.namespaces:
            return 404, None
        if len(parts) == 1:
            return 200, {"namespace": [namespace]}
        if parts[1] == "tables" and method == "GET":
            found = self.tables.get((namespace, parts[2]))
            if not found:
                return 404, None
            main = MAIN.get(found)
            return 200, {"metadata-location": found, "metadata": {"current-snapshot-id": main, "refs": {"main": {"snapshot-id": main, "type": "branch"}}}}
        if parts[1] == "tables" and method == "DELETE":
            if self.refuse_drop or (namespace, parts[2]) not in self.tables:
                return 403, None
            del self.tables[(namespace, parts[2])]
            return 204, None
        if parts[1] == "register" and method == "POST":
            if body is None:
                return 400, None
            if (namespace, body["name"]) in self.tables:
                return 409, None
            if (
                body["name"] in self.refuse_register
                and body["metadata-location"] != self.tables.get((namespace, body["name"]))
                and body["metadata-location"] in self.refused_locations
            ):
                return 400, None
            self.tables[(namespace, body["name"])] = body["metadata-location"]
            return 200, {}
        return 404, None

    refused_locations: set[str] = set()


def folder(objects: dict[str, bytes], prefix: str, run: str, namespace: str, tables: dict[str, int | None], *, wrong_main: str = "") -> None:
    objects[f"{prefix}catalog_committed_{run}.json"] = committed(run, namespace, tables, wrong_main=wrong_main)


def standard_store() -> dict[str, bytes]:
    objects: dict[str, bytes] = {}
    folder(objects, "lakehouse/catalog_backups/", RUN_A, "gold", {"dim_county": 3, "dim_site": 4, "publish_runs": 9})
    folder(objects, "lakehouse/silver/catalog_backups/", RUN_A, "silver", {"stg_hospital": 5})
    folder(objects, "lakehouse/features/catalog_backups/", RUN_A, "features", {"feature_m1": 2})
    return objects


def run_all(polaris: StandInPolaris, objects: dict[str, bytes], *, apply: bool = True, seconds: float = 600.0) -> tuple[int, list[dict[str, str]], str]:
    ticks = iter(range(10_000))
    try:
        items = restore.read_items(StandInStore(objects), BUCKET)
        rows = restore.restore_items(polaris, ACCESS, items, apply=apply, deadline=seconds, clock=lambda: float(next(ticks)))
    except RestoreError as error:
        return 1, [], str(error)
    return 0, rows, ""


def outcomes(rows: list[dict[str, str]]) -> dict[str, str]:
    return {row["table"]: row["outcome"].split(" (")[0] for row in rows}


def case_registers_every_absent_table() -> dict[str, bool]:
    polaris = StandInPolaris(("gold", "silver", "features"))
    code, rows, _ = run_all(polaris, standard_store())
    return {
        "the exit code is 0": code == 0,
        "every table is registered at the backup's file": polaris.tables
        == {
            ("gold", "dim_county"): location("gold", "dim_county", 3),
            ("gold", "dim_site"): location("gold", "dim_site", 4),
            ("gold", "publish_runs"): location("gold", "publish_runs", 9),
            ("silver", "stg_hospital"): location("silver", "stg_hospital", 5),
            ("features", "feature_m1"): location("features", "feature_m1", 2),
        },
        "each outcome is registered": set(outcomes(rows).values()) == {"registered"},
    }


def case_a_rerun_changes_nothing() -> dict[str, bool]:
    polaris = StandInPolaris(("gold", "silver", "features"))
    run_all(polaris, standard_store())
    before, calls = dict(polaris.tables), len(polaris.calls)
    code, rows, _ = run_all(polaris, standard_store())
    writes = [call for call in polaris.calls[calls:] if call[0] != "GET"]
    return {
        "the exit code is 0": code == 0,
        "every outcome is unchanged": set(outcomes(rows).values()) == {"unchanged"},
        "nothing was written and the tables are the same": writes == [] and polaris.tables == before,
    }


def case_an_older_table_is_advanced() -> dict[str, bool]:
    polaris = StandInPolaris(
        ("gold", "silver", "features"),
        {("gold", "dim_county"): location("gold", "dim_county", 1), ("gold", "publish_runs"): location("gold", "publish_runs", 9)},
    )
    code, rows, _ = run_all(polaris, standard_store())
    done = outcomes(rows)
    return {
        "the exit code is 0": code == 0,
        "the older table now points at the backup's file": polaris.tables[("gold", "dim_county")] == location("gold", "dim_county", 3),
        "the report says advanced, registered and unchanged": done["gold.dim_county"] == "advanced"
        and done["silver.stg_hospital"] == "registered"
        and done["gold.publish_runs"] == "unchanged",
        "the older entry was dropped without purge": ("DELETE", f"/api/catalog/v1/{CATALOG}/namespaces/gold/tables/dim_county") in polaris.calls,
    }


def case_a_newer_catalog_entry_is_left_alone() -> dict[str, bool]:
    polaris = StandInPolaris(("gold", "silver", "features"), {("gold", "dim_county"): location("gold", "dim_county", 7)})
    code, rows, _ = run_all(polaris, standard_store())
    return {
        "the exit code is 0": code == 0,
        "the table is unchanged": polaris.tables[("gold", "dim_county")] == location("gold", "dim_county", 7),
        "the report says the catalog is ahead": outcomes(rows)["gold.dim_county"] == "catalog ahead",
        "nothing was dropped": not any(call[0] == "DELETE" for call in polaris.calls),
    }


def case_the_same_sequence_with_another_file_is_a_conflict() -> dict[str, bool]:
    polaris = StandInPolaris(("gold", "silver", "features"), {("gold", "dim_county"): location("gold", "dim_county", 3, "x")})
    code, rows, _ = run_all(polaris, standard_store())
    return {
        "the exit code is 0": code == 0,
        "the table is unchanged": polaris.tables[("gold", "dim_county")] == location("gold", "dim_county", 3, "x"),
        "the report says conflict": outcomes(rows)["gold.dim_county"] == "conflict",
    }


def case_a_refused_registration_puts_the_older_entry_back() -> dict[str, bool]:
    old, new = location("gold", "dim_county", 1), location("gold", "dim_county", 3)
    polaris = StandInPolaris(("gold", "silver", "features"), {("gold", "dim_county"): old})
    polaris.refuse_register, polaris.refused_locations = {"dim_county"}, {new}
    code, rows, message = run_all(polaris, standard_store())
    return {
        "the exit code is 1": code == 1,
        "the older entry is registered again": polaris.tables[("gold", "dim_county")] == old,
        "the message says so": "registered again" in message and "dim_county" in message,
    }


def case_a_failed_rollback_is_loud() -> dict[str, bool]:
    old, new = location("gold", "dim_county", 1), location("gold", "dim_county", 3)
    polaris = StandInPolaris(("gold", "silver", "features"), {("gold", "dim_county"): old})
    polaris.refuse_register, polaris.refused_locations = {"dim_county"}, {new, old}
    code, _, message = run_all(polaris, standard_store())
    return {
        "the exit code is 1": code == 1,
        "the message says it could not be registered again": "COULD NOT BE REGISTERED AGAIN" in message and "by hand" in message,
    }


def case_a_refused_drop_leaves_the_table() -> dict[str, bool]:
    old = location("gold", "dim_county", 1)
    polaris = StandInPolaris(("gold", "silver", "features"), {("gold", "dim_county"): old})
    polaris.refuse_drop = True
    code, _, message = run_all(polaris, standard_store())
    return {
        "the exit code is 1": code == 1,
        "the table is unchanged": polaris.tables[("gold", "dim_county")] == old,
        "the message names the table": "dim_county" in message,
    }


def case_dry_run_writes_nothing() -> dict[str, bool]:
    polaris = StandInPolaris(("gold", "silver", "features"), {("gold", "dim_county"): location("gold", "dim_county", 1)})
    code, rows, _ = run_all(polaris, standard_store(), apply=False)
    return {
        "the exit code is 0": code == 0,
        "no write was sent": all(call[0] == "GET" for call in polaris.calls),
        "the report says would": outcomes(rows)["gold.dim_county"].startswith("would") and outcomes(rows)["silver.stg_hospital"] == "would register",
    }


def case_a_missing_namespace_stops_before_any_write() -> dict[str, bool]:
    polaris = StandInPolaris(("gold", "silver"))
    code, _, message = run_all(polaris, standard_store())
    return {
        "the exit code is 1": code == 1,
        "the message names the namespace and the setup command": "features" in message and "aws_catalog_setup principal" in message,
        "nothing was written": polaris.tables == {} and all(call[0] == "GET" for call in polaris.calls),
    }


def case_only_the_newest_backup_of_a_folder_counts() -> dict[str, bool]:
    objects = standard_store()
    folder(objects, "lakehouse/catalog_backups/", RUN_B, "gold", {"dim_county": 4, "publish_runs": 10})
    polaris = StandInPolaris(("gold", "silver", "features"))
    run_all(polaris, objects)
    return {
        "the newest backup's files are registered": polaris.tables[("gold", "dim_county")] == location("gold", "dim_county", 4)
        and polaris.tables[("gold", "publish_runs")] == location("gold", "publish_runs", 10),
        "the other folders are read as well": ("silver", "stg_hospital") in polaris.tables and ("features", "feature_m1") in polaris.tables,
    }


def case_a_table_with_no_metadata_file_is_skipped() -> dict[str, bool]:
    objects: dict[str, bytes] = {}
    folder(objects, "lakehouse/catalog_backups/", RUN_A, "gold", {"dim_county": 3, "empty_table": None})
    polaris = StandInPolaris(("gold",))
    code, rows, _ = run_all(polaris, objects)
    return {"the exit code is 0": code == 0, "only the table with a file is registered": list(polaris.tables) == [("gold", "dim_county")] and len(rows) == 1}


def case_malformed_snapshots_are_refused() -> dict[str, bool]:
    checks = {}
    key = "lakehouse/catalog_backups/catalog_committed_" + RUN_A + ".json"
    bad_bucket = standard_store()
    bad_bucket[key] = committed(RUN_A, "gold", {"dim_county": 3}).replace(BUCKET.encode(), b"other-bucket")
    wrong_run = standard_store()
    wrong_run[key] = committed(RUN_B, "gold", {"dim_county": 3})
    not_json = standard_store()
    not_json[key] = b"{"
    duplicate = standard_store()
    folder(duplicate, "lakehouse/silver/catalog_backups/", RUN_A, "gold", {"dim_county": 1})
    wrong_main = standard_store()
    folder(wrong_main, "lakehouse/catalog_backups/", RUN_A, "gold", {"dim_county": 3, "dim_site": 4, "publish_runs": 9}, wrong_main="dim_site")
    not_committed = standard_store()
    not_committed[key] = json.dumps({**json.loads(committed(RUN_A, "gold", {"dim_county": 3})), "kind": "candidate"}).encode()
    for name, objects in (
        ("another bucket", bad_bucket),
        ("another run id", wrong_run),
        ("not JSON", not_json),
        ("a table in two folders", duplicate),
        ("a snapshot set that differs from the table mains", wrong_main),
        ("a snapshot that is not marked committed", not_committed),
    ):
        polaris = StandInPolaris(("gold", "silver", "features"))
        code, _, _ = run_all(polaris, objects)
        checks[f"{name}: refused and nothing written"] = code == 1 and polaris.tables == {}
    return checks


def case_pre_commit_backups_are_never_used() -> dict[str, bool]:
    objects = {"lakehouse/catalog_backups/catalog_backup_" + RUN_A + ".json": backup(RUN_A, "gold", {"dim_county": 3, "publish_runs": 9})}
    polaris = StandInPolaris(("gold", "silver", "features"))
    code, rows, _ = run_all(polaris, objects)
    return {
        "the exit code is 0": code == 0,
        "nothing is registered from a backup that was captured before the commit": polaris.tables == {} and rows == [],
        "the catalog was not even read": polaris.calls == [],
    }


def reader_sees_one_committed_set(polaris: StandInPolaris, namespace: str = "gold", tables: tuple[str, ...] = ("dim_county", "dim_site")) -> bool:
    """What a reader does: read the newest publish_runs row, then read only the snapshots it names. True when every table's main is that set."""
    runs = polaris.tables.get((namespace, "publish_runs"))
    head = RUNS_AT.get(str(runs))
    if head is None:
        return False
    mains = {name: MAIN.get(polaris.tables.get((namespace, name), "")) for name in tables}
    return mains == {name: head["snapshot_set"].get(name) for name in tables}


def case_a_reader_sees_the_committed_set_after_the_restore() -> dict[str, bool]:
    objects = standard_store()
    folder(objects, "lakehouse/catalog_backups/", RUN_B, "gold", {"dim_county": 5, "dim_site": 6, "publish_runs": 11})
    polaris = StandInPolaris(
        ("gold", "silver", "features"),
        {
            ("gold", "dim_county"): location("gold", "dim_county", 3),
            ("gold", "dim_site"): location("gold", "dim_site", 4),
            ("gold", "publish_runs"): location("gold", "publish_runs", 9),
        },
    )
    before = reader_sees_one_committed_set(polaris)
    code, rows, _ = run_all(polaris, objects)
    head = RUNS_AT[str(polaris.tables[("gold", "publish_runs")])]
    return {
        "the catalog before the restore shows the older committed set": before and RUNS_AT[location("gold", "publish_runs", 9)]["run_id"] == RUN_A,
        "the exit code is 0": code == 0,
        "the newest committed run is the one a reader finds": head["run_id"] == RUN_B,
        "every table's main is the snapshot that run names": reader_sees_one_committed_set(polaris),
        "the restore advanced all three gold tables": {row["table"]: row["outcome"].split(" (")[0] for row in rows if row["table"].startswith("gold.")}
        == {"gold.dim_county": "advanced", "gold.dim_site": "advanced", "gold.publish_runs": "advanced"},
    }


def case_a_table_whose_main_differs_from_the_set_is_not_left_visible() -> dict[str, bool]:
    objects = standard_store()
    registered = location("gold", "dim_county", 3)
    MAIN[registered] = MAIN[registered] + 7
    polaris = StandInPolaris(("gold", "silver", "features"))
    code, _, message = run_all(polaris, objects)
    return {
        "the exit code is 1": code == 1,
        "the message names the table and the two snapshots": "dim_county" in message and "main snapshot" in message,
        "the wrongly resolved table is not left in the catalog": ("gold", "dim_county") not in polaris.tables,
    }


def case_no_backup_means_nothing_to_do() -> dict[str, bool]:
    polaris = StandInPolaris(("gold",))
    code, rows, _ = run_all(polaris, {})
    return {"the exit code is 0": code == 0, "no table and no call": rows == [] and polaris.calls == []}


def case_the_deadline_stops_the_run() -> dict[str, bool]:
    polaris = StandInPolaris(("gold", "silver", "features"))
    code, rows, message = run_all(polaris, standard_store(), seconds=3.0)
    return {"the exit code is 1": code == 1, "the message names the deadline": "deadline" in message, "not every table was registered": len(polaris.tables) < 5}


@contextlib.contextmanager
def served(polaris: StandInPolaris) -> Iterator[int]:
    """The stand-in catalog behind a loopback HTTP server: the token route answers the stand-in token, every other route goes to the stand-in."""

    class Handler(BaseHTTPRequestHandler):
        def do_request(self) -> None:
            length = int(self.headers.get("Content-Length") or 0)
            raw = self.rfile.read(length) if length else b""
            if self.path == setup.OAUTH_PATH:
                status, reply = 200, {"access_token": ACCESS}
            else:
                token = (self.headers.get("Authorization") or "").removeprefix("Bearer ")
                status, reply = polaris(self.command, self.path, token or None, json.loads(raw) if raw else None)
            body = json.dumps(reply).encode() if reply is not None else b""
            self.send_response(status)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        do_GET = do_POST = do_DELETE = do_request

        def log_message(self, format: str, *args: Any) -> None:
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield int(server.server_address[1])
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=10)


class StandInServices:
    """The services of the ``restore`` step as the task has them: a store, the task identity check, the root secret and the loopback port of the catalog."""

    bucket = BUCKET
    region = "us-west-2"

    def __init__(self, objects: dict[str, bytes], port: int, *, identity: bool = True) -> None:
        self.objects, self.polaris_port, self.identity = objects, port, identity
        self.verified, self.reads = 0, 0

    def verify_identity(self, deadline: Deadline) -> None:
        self.verified += 1
        if not self.identity:
            raise setup.SetupError("identity", "the caller is not the catalog setup task role")

    def root_secret(self) -> str:
        return "stand-in-root-secret"

    def backup_store(self, left: float) -> StandInStore:
        self.reads += 1
        return StandInStore(self.objects)


def run_step(argv: list[str], services: StandInServices) -> tuple[int, str, str]:
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        try:
            code = setup.main(argv, cast(setup.Services, services))
        except SystemExit as error:
            code = int(error.code) if isinstance(error.code, int) else 1
    return code, out.getvalue(), err.getvalue()


def case_the_restore_step_reports_counts_and_exits() -> dict[str, bool]:
    polaris = StandInPolaris(("gold", "silver", "features"), {("gold", "dim_county"): location("gold", "dim_county", 1)})
    with served(polaris) as port:
        services = StandInServices(standard_store(), port)
        code, out, err = run_step(["--seconds", "100", "restore"], services)
    reply = json.loads(out) if out else {}
    return {
        "the exit code is 0 and the reply holds the counts": code == 0 and reply.get("counts") == {"advanced": 1, "registered": 4},
        "the reply names the step and says it applied": reply.get("step") == "restore" and reply.get("applied") is True,
        "the task identity was checked once before the backups were read": services.verified == 1 and services.reads == 1,
        "the tables are at the backup's files": polaris.tables[("gold", "dim_county")] == location("gold", "dim_county", 3) and len(polaris.tables) == 5,
        "no credential or digest in the output": "stand-in-token" not in out + err
        and "stand-in-root-secret" not in out + err
        and hashlib.sha256(b"x").hexdigest() not in out,
    }


def case_the_restore_step_dry_run_writes_nothing() -> dict[str, bool]:
    polaris = StandInPolaris(("gold", "silver", "features"), {("gold", "dim_county"): location("gold", "dim_county", 1)})
    with served(polaris) as port:
        code, out, _ = run_step(["restore", "--dry-run"], StandInServices(standard_store(), port))
    reply = json.loads(out) if out else {}
    return {
        "the exit code is 0": code == 0,
        "the reply says nothing was applied": reply.get("applied") is False,
        "no write reached the catalog": all(call[0] == "GET" for call in polaris.calls),
        "the report says would": reply.get("counts") == {"would advance": 1, "would register": 4},
    }


def case_the_restore_step_refuses_a_caller_that_is_not_the_task_role() -> dict[str, bool]:
    polaris = StandInPolaris(("gold", "silver", "features"))
    with served(polaris) as port:
        services = StandInServices(standard_store(), port, identity=False)
        code, out, err = run_step(["restore"], services)
    return {
        "the exit code is 1": code == 1,
        "the refusal names the identity": "(identity)" in err,
        "no backup was read and the catalog was not called": services.reads == 0 and polaris.calls == [],
        "nothing is printed as a result": out == "",
    }


class SlowStore:
    """A backup store whose listing takes ``seconds`` of real time and holds no snapshot: the folders are empty, so nothing is left to restore."""

    def __init__(self, seconds: float) -> None:
        self.seconds = seconds

    def names(self, prefix: str) -> list[str]:
        time.sleep(self.seconds)
        return []

    def read(self, key: str) -> bytes:
        raise AssertionError("an empty folder has nothing to read")


class SlowServices(StandInServices):
    """The services of the step with a store that lists slowly."""

    def __init__(self, port: int, seconds: float) -> None:
        super().__init__({}, port)
        self.seconds = seconds

    def backup_store(self, left: float) -> Any:
        return SlowStore(self.seconds)


def case_an_empty_restore_whose_listings_use_the_whole_budget_is_refused() -> dict[str, bool]:
    started = time.monotonic()
    with served(StandInPolaris(("gold", "silver", "features"))) as port:
        code, out, err = run_step(["--seconds", "0.1", "restore"], SlowServices(port, 0.08))
    return {
        "the exit code is 1": code == 1,
        "the refusal names the deadline": "(deadline)" in err and err.startswith("aws catalog setup restore"),
        "no success reply is printed": out == "",
        "the step ends close to its deadline": time.monotonic() - started < 1.5,
    }


def case_a_listing_that_never_returns_ends_at_the_deadline() -> dict[str, bool]:
    started = time.monotonic()
    with served(StandInPolaris(("gold", "silver", "features"))) as port:
        code, out, err = run_step(["--seconds", "0.3", "restore"], SlowServices(port, 5.0))
    return {
        "the exit code is 1": code == 1,
        "the refusal names the deadline": "(deadline)" in err,
        "no success reply is printed": out == "",
        "the step ends at its deadline, not when the listing ends": time.monotonic() - started < 2.5,
    }


def case_an_empty_restore_checks_the_deadline_after_reading_the_folders() -> dict[str, bool]:
    ticks = {"now": 0.0}

    class ClockStore:
        def names(self, prefix: str) -> list[str]:
            ticks["now"] += 2.0
            return []

        def read(self, key: str) -> bytes:
            raise AssertionError("an empty folder has nothing to read")

    try:
        restore.restore_catalog(ClockStore(), BUCKET, StandInPolaris(("gold",)), ACCESS, apply=True, deadline=1.0, clock=lambda: ticks["now"])
        refused = ""
    except RestoreError as error:
        refused = str(error)
    return {"the restore is refused with the deadline named": "deadline" in refused}


def case_a_failed_restore_step_prints_its_refusal_line() -> dict[str, bool]:
    polaris = StandInPolaris(("gold", "silver"))
    with served(polaris) as port:
        code, out, err = run_step(["restore"], StandInServices(standard_store(), port))
    return {
        "the exit code is 1": code == 1,
        "the refusal line starts with the step and names the code": err.startswith("aws catalog setup restore (namespace)"),
        "the line names the namespace": "features" in err,
        "nothing was written": polaris.tables == {} and out == "",
    }


CASES: dict[str, Callable[[], dict[str, bool]]] = {
    name.removeprefix("case_"): function for name, function in sorted(globals().items()) if name.startswith("case_")
}
CASE_ERRORS = (AssertionError, LookupError, OSError, TypeError, ValueError, AttributeError, NameError, ImportError, RestoreError)


def main() -> int:
    """Run every case, save the outcomes and return 1 when any check failed."""
    results: dict[str, dict[str, bool]] = {}
    for name, function in CASES.items():
        try:
            results[name] = function()
        except CASE_ERRORS as error:
            results[name] = {f"the case ended with {type(error).__name__}: {error}": False}
    passed = all(all(checks.values()) for checks in results.values())
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    OUTPUT.mkdir(parents=True, exist_ok=True)
    (OUTPUT / f"report_{stamp}.json").write_text(json.dumps({"run_at_utc": stamp, "results": results, "passed": passed}, indent=2, sort_keys=True) + "\n")
    for name, checks in results.items():
        failed = [check for check, ok in checks.items() if not ok]
        sys.stdout.write(f"{'PASS' if not failed else 'FAIL'} {name}{': ' + '; '.join(failed) if failed else ''}\n")
    sys.stdout.write(f"{sum(all(checks.values()) for checks in results.values())} of {len(results)} cases passed\n")
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
