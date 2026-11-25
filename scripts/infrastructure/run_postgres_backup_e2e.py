"""Run the Postgres backup and restore scripts of the services stack against stand-ins and keep the report under data/e2e/postgres_backup/.

Run from the repository root: ``.venv/bin/python -m scripts.infrastructure.run_postgres_backup_e2e``. The scripts are the ones the services stack puts in the
task definitions (``infra/services/scripts/pg_backup_dump.sh``, ``pg_backup_upload.py``, ``pg_restore_fetch.py`` and ``pg_databases.sh``). The two Python
scripts run in this process against a stand-in S3 client with write-once keys. The two shell scripts run as the real ``sh`` programs with fake ``pg_dump``,
``pg_restore`` and ``psql`` programs first on ``PATH`` (``postgres_backup_fakes``). No AWS service is contacted, no container starts and no Postgres runs.
The real programs are proved by ``run_postgres_restore_real_e2e`` against a scratch server that the lead starts.
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import shutil
import stat
import sys
import tempfile
import time
import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest import mock

from scripts.infrastructure.postgres_backup_fakes import BUCKET, StandInS3, fake_dump, install_fakes, log_lines, server, server_state
from scripts.process import run_command

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = REPO_ROOT / "infra" / "services" / "scripts"
OUTPUT = REPO_ROOT / "data" / "e2e" / "postgres_backup"
RUN_A = "20261117T120000Z"
RUN_B = "20261117T180000Z"
RUN_C = "20261118T000000Z"
TASK = "0123456789abcdef0123456789abcdef"
OTHER_TASK = "fedcba9876543210fedcba9876543210"
DATABASES = ("airflow", "mlflow", "polaris")
LEGACY_UPLOADER = REPO_ROOT / "scripts" / "infrastructure" / "postgres_backup_legacy_uploader_41033e5.py"  # the uploader before markers recorded their members


def load(name: str) -> Any:
    """A script of the services stack as a module, loaded from the file the task definition embeds."""
    path = SCRIPTS / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise FileNotFoundError(path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def legacy_uploader() -> Any:
    """The upload script as it was before markers recorded their members (a frozen copy, so no Git history is needed): a restore meets the markers it wrote."""
    spec = importlib.util.spec_from_file_location("pg_backup_upload_legacy", LEGACY_UPLOADER)
    if spec is None or spec.loader is None:
        raise FileNotFoundError(LEGACY_UPLOADER)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def ready_folder(backups: Path, run: str, tables: dict[str, int], *, truncate: str = "", declare: dict[str, int] | None = None) -> Path:
    """A ``<run>.ready`` folder as the dump script leaves it: one dump per database and ``manifest.tsv`` with the tables and bytes of each."""
    folder = backups / f"{run}.ready"
    folder.mkdir(parents=True)
    lines = []
    for name, count in tables.items():
        data = fake_dump(count)
        lines.append(f"{name}\t{(declare or {}).get(name, count)}\t{len(data)}")
        (folder / f"{name}.dump").write_bytes(data[: len(data) // 2] if name == truncate else data)
    (folder / "manifest.tsv").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return folder


def uploaded(client: StandInS3, run: str, name: str) -> bool:
    return f"pg_backups/{name}/{run}.dump" in client.objects


def marker_of(client: StandInS3, run: str) -> dict[str, Any] | None:
    data = client.objects.get(f"pg_backups/complete/{run}.json")
    return None if data is None else json.loads(data)


def upload_case(
    prepare: Callable[[Path], Any], *, client: StandInS3 | None = None, clock: Callable[[], float] | None = None, expected: tuple[str, ...] = DATABASES
) -> tuple[int, StandInS3, Path]:
    """Run the upload script once over a fresh backup folder that ``prepare`` fills; returns the exit code, the client and the folder."""
    script = load("pg_backup_upload")
    client = client or StandInS3()
    backups = Path(tempfile.mkdtemp(prefix="pg_backups_"))
    prepare(backups)
    code = script.run(backups, BUCKET, client, expected, clock or (lambda: 0.0))
    return code, client, backups


def status_of(backups: Path, name: str = "upload_result.json") -> dict[str, Any]:
    path = backups / name
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}


def case_upload_writes_every_dump_then_the_marker() -> dict[str, bool]:
    code, client, backups = upload_case(lambda b: ready_folder(b, RUN_A, {"airflow": 3, "mlflow": 2, "polaris": 5}))
    marker = marker_of(client, RUN_A) or {}
    keys = [put["key"] for put in client.puts]
    return {
        "the exit code is 0": code == 0,
        "every dump has its own key under its database": all(uploaded(client, RUN_A, name) for name in DATABASES),
        "the marker is the last object written": keys[-1] == f"pg_backups/complete/{RUN_A}.json" and keys.count(keys[-1]) == 1,
        "the marker lists each dump with its hash, size and tables": sorted(marker.get("databases", {})) == list(DATABASES)
        and marker["databases"]["polaris"]["sha256"] == hashlib.sha256(fake_dump(5)).hexdigest()
        and marker["databases"]["polaris"]["bytes"] == len(fake_dump(5))
        and marker["databases"]["polaris"]["tables"] == 5,
        "the marker records the membership of the set when it was written": marker.get("members") == sorted(DATABASES),
        "every put is write-once and under pg_backups/": all(put["if_none_match"] == "*" and put["key"].startswith("pg_backups/") for put in client.puts),
        "the ready folder is removed": not (backups / f"{RUN_A}.ready").exists(),
        "the result says uploaded": status_of(backups).get("result") == "uploaded" and status_of(backups).get("run") == RUN_A,
    }


def case_upload_a_rerun_after_an_interruption_adds_nothing_twice() -> dict[str, bool]:
    client = StandInS3()
    client.fail_put_after = 2
    backups = Path(tempfile.mkdtemp(prefix="pg_backups_"))
    ready_folder(backups, RUN_A, {"airflow": 3, "mlflow": 2, "polaris": 5})
    script = load("pg_backup_upload")
    first = script.run(backups, BUCKET, client, DATABASES, lambda: 0.0)
    after_first = dict(client.objects)
    client.fail_put_after = None
    second = script.run(backups, BUCKET, client, DATABASES, lambda: 0.0)
    third = script.run(backups, BUCKET, client, DATABASES, lambda: 0.0)
    return {
        "the interrupted run fails and writes no marker": first == 1 and f"pg_backups/complete/{RUN_A}.json" not in after_first,
        "the first two dumps stayed": len(after_first) == 2,
        "the rerun finishes the set and the marker is written once": second == 0 and marker_of(client, RUN_A) is not None,
        "nothing was replaced": all(client.objects[key] == data for key, data in after_first.items()),
        "a third run has nothing to do": third == 0 and sum(put["key"].endswith(".json") for put in client.puts) == 1,
    }


def case_upload_refuses_an_empty_dump() -> dict[str, bool]:
    code, client, backups = upload_case(lambda b: ready_folder(b, RUN_A, {"airflow": 3, "mlflow": 0, "polaris": 5}))
    return {
        "the exit code is 1": code == 1,
        "nothing is uploaded": not client.objects and not client.puts,
        "the reason names the database and says empty": "mlflow" in status_of(backups).get("reason", "") and "empty" in status_of(backups).get("reason", ""),
        "the set is set aside and not retried": (backups / f"{RUN_A}.refused").is_dir() and not (backups / f"{RUN_A}.ready").exists(),
    }


def case_upload_refuses_a_truncated_dump() -> dict[str, bool]:
    code, client, backups = upload_case(lambda b: ready_folder(b, RUN_A, {"airflow": 3, "mlflow": 2, "polaris": 5}, truncate="polaris"))
    return {
        "the exit code is 1": code == 1,
        "nothing is uploaded": not client.objects,
        "the reason names the database and says truncated": "polaris" in status_of(backups).get("reason", "")
        and "truncated" in status_of(backups).get("reason", ""),
        "the set is set aside": (backups / f"{RUN_A}.refused").is_dir(),
    }


def case_upload_refuses_a_file_that_is_not_a_dump() -> dict[str, bool]:
    def prepare(backups: Path) -> None:
        folder = ready_folder(backups, RUN_A, {"airflow": 3})
        data = b"x" * len(fake_dump(3))
        (folder / "airflow.dump").write_bytes(data)

    code, client, backups = upload_case(prepare, expected=("airflow",))
    return {
        "the exit code is 1": code == 1,
        "nothing is uploaded": not client.objects,
        "the reason says it is not a dump": "not a dump" in status_of(backups).get("reason", ""),
    }


def case_upload_refuses_a_manifest_that_omits_an_expected_database() -> dict[str, bool]:
    code, client, backups = upload_case(lambda b: ready_folder(b, RUN_A, {"mlflow": 2, "polaris": 5}))
    return {
        "the exit code is 1": code == 1,
        "nothing is uploaded and no marker is written": not client.puts and marker_of(client, RUN_A) is None,
        "the set is set aside as refused and the reason names the databases": (backups / f"{RUN_A}.refused").is_dir()
        and "airflow" in status_of(backups).get("reason", ""),
    }


def case_upload_refuses_a_manifest_that_lists_another_database() -> dict[str, bool]:
    code, client, backups = upload_case(lambda b: ready_folder(b, RUN_A, {"airflow": 3, "mlflow": 2, "polaris": 5, "extra": 1}))
    return {
        "the exit code is 1": code == 1,
        "no marker is written": marker_of(client, RUN_A) is None,
        "the set is set aside as refused": (backups / f"{RUN_A}.refused").is_dir(),
    }


def case_one_pass_upload_takes_only_the_set_its_dump_just_wrote() -> dict[str, bool]:
    folder, client = Path(tempfile.mkdtemp(prefix="pg_final_")), StandInS3()
    databases = {"airflow": 3, "polaris": 5}
    older = ready_folder(folder / "backup", RUN_A, {"airflow": 3, "polaris": 5}, truncate="polaris")
    first = stamped_pass(folder, databases, client)
    untouched = older.is_dir() and not (folder / "backup" / f"{RUN_A}.refused").exists() and not uploaded(client, RUN_A, "airflow")
    service_pass = load("pg_backup_upload").run(folder / "backup", BUCKET, client, tuple(databases), lambda: 0.0)
    return {
        "the one pass dumped and uploaded the new set although an older set is damaged": first[:2] == (0, 0) and marker_of(client, first[2]) is not None,
        "the older damaged set was not read, renamed or uploaded": untouched,
        "the service pass of the same folder still refuses the older set": service_pass == 1 and (folder / "backup" / f"{RUN_A}.refused").is_dir(),
    }


def case_one_pass_upload_refuses_when_its_dump_left_no_set() -> dict[str, bool]:
    folder, client = Path(tempfile.mkdtemp(prefix="pg_final_")), StandInS3()
    backups = folder / "backup"
    script = load("pg_backup_upload")
    refusals = {}
    for label, text in (
        ("no result file", None),
        ("a refused result", json.dumps({"result": "refused", "run": RUN_A, "reason": "x", "task": TASK, "at_utc": "y"})),
        ("a damaged result", "{"),
        ("a dumped result of another task", json.dumps({"result": "dumped", "run": RUN_A, "reason": "", "task": OTHER_TASK, "at_utc": "y"})),
        ("a dumped result of no task", json.dumps({"result": "dumped", "run": RUN_A, "reason": "", "at_utc": "y"})),
    ):
        backups.mkdir(exist_ok=True)
        (backups / "last_result.json").unlink(missing_ok=True)
        if text is not None:
            (backups / "last_result.json").write_text(text, encoding="utf-8")
        try:
            script.dumped_run(backups, TASK)
            refusals[f"{label}: the one pass refuses"] = False
        except script.Refused:
            refusals[f"{label}: the one pass refuses"] = True
    ready_folder(backups, RUN_B, {"airflow": 3})
    named = script.run(backups, BUCKET, client, ("airflow",), lambda: 0.0, only=RUN_A)
    (backups / "last_result.json").write_text(json.dumps({"result": "dumped", "run": RUN_A, "reason": "", "task": TASK, "at_utc": "y"}), encoding="utf-8")
    refusals["a task ID that is not set or has another form: the one pass refuses"] = all(
        refused(script, backups, task) for task in ("", "not-an-id", TASK.upper())
    )
    return {
        **refusals,
        "a result of this task names its set": script.dumped_run(backups, TASK) == RUN_A,
        "a named set that is not in the folder fails the pass and uploads nothing": named == 1 and not client.puts,
    }


def named(script: Any, backups: Path, task: str) -> str:
    """The run ID that ``dumped_run`` names for ``task``, or an empty text when it refuses."""
    try:
        return str(script.dumped_run(backups, task))
    except script.Refused:
        return ""


def refused(script: Any, backups: Path, task: str) -> bool:
    """True when ``dumped_run`` refuses ``task``."""
    return not named(script, backups, task)


def case_upload_never_replaces_an_existing_key() -> dict[str, bool]:
    client = StandInS3()
    other = fake_dump(9)
    client.objects[f"pg_backups/mlflow/{RUN_A}.dump"] = other
    code, client, backups = upload_case(lambda b: ready_folder(b, RUN_A, {"airflow": 3, "mlflow": 2, "polaris": 5}), client=client)
    return {
        "the exit code is 1": code == 1,
        "the existing object is unchanged": client.objects[f"pg_backups/mlflow/{RUN_A}.dump"] == other,
        "no marker is written": marker_of(client, RUN_A) is None,
        "the reason says the set did not write the key": "did not write it" in status_of(backups).get("reason", ""),
        "the set is set aside": (backups / f"{RUN_A}.refused").is_dir(),
    }


def case_upload_needs_no_read_permission() -> dict[str, bool]:
    code, client, _ = upload_case(lambda b: ready_folder(b, RUN_A, {"airflow": 3, "polaris": 5}), expected=("airflow", "polaris"))
    return {"the upload never reads or lists": code == 0 and not client.gets and client.heads == 0}


def case_upload_treats_a_client_library_error_as_a_failed_try() -> dict[str, bool]:
    from botocore.exceptions import ParamValidationError

    client = StandInS3()
    client.put_error = ParamValidationError(report="Unknown parameter in input: IfNoneMatch")
    backups = Path(tempfile.mkdtemp(prefix="pg_backups_"))
    ready_folder(backups, RUN_A, {"airflow": 3})
    script = load("pg_backup_upload")
    codes = [script.run(backups, BUCKET, client, ("airflow",), lambda: 0.0) for _ in range(3)]
    return {"each try fails and none raises": codes == [1, 1, 1], "the set is set aside as failed": (backups / f"{RUN_A}.failed").is_dir()}


def case_upload_stops_at_its_deadline() -> dict[str, bool]:
    ticks = iter([0.0, 0.0, 5000.0] + [5000.0] * 50)
    code, client, backups = upload_case(lambda b: ready_folder(b, RUN_A, {"airflow": 3, "mlflow": 2, "polaris": 5}), clock=lambda: next(ticks))
    return {
        "the exit code is 1": code == 1,
        "no marker is written": marker_of(client, RUN_A) is None,
        "the reason names the deadline": "deadline" in status_of(backups).get("reason", ""),
        "the set stays ready for the next try": (backups / f"{RUN_A}.ready").is_dir(),
    }


def case_upload_sets_a_set_aside_after_three_failed_tries() -> dict[str, bool]:
    client = StandInS3()
    client.fail_put_after = 0
    backups = Path(tempfile.mkdtemp(prefix="pg_backups_"))
    ready_folder(backups, RUN_A, {"airflow": 3})
    script = load("pg_backup_upload")
    codes = [script.run(backups, BUCKET, client, ("airflow",), lambda: 0.0) for _ in range(3)]
    return {
        "each try fails": codes == [1, 1, 1],
        "the third try sets the set aside as failed": (backups / f"{RUN_A}.failed").is_dir() and not (backups / f"{RUN_A}.ready").exists(),
    }


def case_upload_takes_the_oldest_set_first_and_leaves_partial_sets() -> dict[str, bool]:
    def prepare(backups: Path) -> None:
        ready_folder(backups, RUN_B, {"airflow": 4})
        ready_folder(backups, RUN_A, {"airflow": 3})
        (backups / f"{RUN_C}.partial").mkdir()

    code, client, backups = upload_case(prepare, expected=("airflow",))
    markers = [put["key"] for put in client.puts if "/complete/" in put["key"]]
    return {
        "the exit code is 0": code == 0,
        "the older set is marked first": markers == [f"pg_backups/complete/{RUN_A}.json", f"pg_backups/complete/{RUN_B}.json"],
        "a partial set is never read": (backups / f"{RUN_C}.partial").is_dir() and not any(RUN_C in key for key in client.objects),
    }


def put_set(
    client: StandInS3,
    run: str,
    tables: dict[str, int],
    *,
    skip: str = "",
    damage: str = "",
    bad_marker: bool = False,
    members: tuple[str, ...] | None = None,
    legacy: bool = False,
) -> None:
    """A completed set in the stand-in bucket as the upload script leaves it; ``skip`` leaves a dump out, ``damage`` changes its bytes. The marker records
    ``members`` (default: the databases of ``tables``) as the membership of the set when it was written; ``legacy`` writes a marker that records none."""
    entries = {}
    for name, count in tables.items():
        data = fake_dump(count)
        key = f"pg_backups/{name}/{run}.dump"
        entries[name] = {"key": key, "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest(), "tables": count}
        if name != skip:
            client.objects[key] = data[:-1] if name == damage else data
    document: dict[str, Any] = {"schema": 1, "run": run, "databases": entries}
    if not legacy:
        document["members"] = sorted(tables if members is None else members)
    body = b"not json" if bad_marker else json.dumps(document).encode()
    client.objects[f"pg_backups/complete/{run}.json"] = body


def put_marker(client: StandInS3, run: str, document: Any, *, run_in_key: str | None = None) -> None:
    """A marker object with exactly the given document, for the malformed-marker cases."""
    client.objects[f"pg_backups/complete/{run_in_key or run}.json"] = json.dumps(document).encode()


def entry(run: str, name: str, data: bytes, **changes: Any) -> dict[str, Any]:
    """The marker entry of a dump, with its dump stored in the bucket; ``changes`` replace fields."""
    return {"key": f"pg_backups/{name}/{run}.dump", "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest(), "tables": 3, **changes}


def fetch(client: StandInS3, databases: list[str], clock: Callable[[], float] | None = None) -> tuple[int, Path]:
    script = load("pg_restore_fetch")
    target = Path(tempfile.mkdtemp(prefix="pg_restore_"))
    code = script.run(BUCKET, databases, target, client, clock or (lambda: 0.0))
    return code, target


def case_fetch_with_no_completed_backup_restores_nothing() -> dict[str, bool]:
    client = StandInS3()
    client.objects[f"pg_backups/airflow/{RUN_A}.dump"] = fake_dump(3)
    code, target = fetch(client, list(DATABASES))
    return {"the exit code is 0": code == 0, "no file is written": not any(target.iterdir()), "no dump was read": not client.gets}


def case_fetch_takes_the_newest_completed_set() -> dict[str, bool]:
    client = StandInS3()
    put_set(client, RUN_A, {"airflow": 3, "mlflow": 2, "polaris": 5})
    put_set(client, RUN_B, {"airflow": 4, "mlflow": 2, "polaris": 6})
    client.objects[f"pg_backups/airflow/{RUN_C}.dump"] = fake_dump(9)
    code, target = fetch(client, list(DATABASES))
    return {
        "the exit code is 0": code == 0,
        "each dump is the newest completed one": all(
            (target / f"{name}.dump").read_bytes() == fake_dump(count) for name, count in (("airflow", 4), ("mlflow", 2), ("polaris", 6))
        ),
        "a dump without a marker is ignored": not any(RUN_C in key for key in client.gets),
        "the run is recorded": (target / "RUN").read_text(encoding="utf-8").strip() == RUN_B,
    }


def case_fetch_reads_only_the_requested_databases() -> dict[str, bool]:
    client = StandInS3()
    put_set(client, RUN_A, {"airflow": 3, "mlflow": 2, "polaris": 5})
    code, target = fetch(client, ["airflow", "mlflow"])
    return {
        "the exit code is 0": code == 0,
        "only the requested dumps are written": sorted(path.name for path in target.glob("*.dump")) == ["airflow.dump", "mlflow.dump"],
        "the others were not downloaded": not any("polaris" in key for key in client.gets),
    }


def case_fetch_falls_back_to_the_older_set_when_the_newest_is_damaged() -> dict[str, bool]:
    client = StandInS3()
    put_set(client, RUN_A, {"airflow": 3, "mlflow": 2, "polaris": 5})
    put_set(client, RUN_B, {"airflow": 4, "mlflow": 2, "polaris": 6}, damage="mlflow")
    code, target = fetch(client, list(DATABASES))
    return {
        "the exit code is 0": code == 0,
        "the older set is written whole": (target / "RUN").read_text(encoding="utf-8").strip() == RUN_A
        and (target / "airflow.dump").read_bytes() == fake_dump(3),
        "nothing of the damaged set stays": not any(
            (target / f"{name}.dump").read_bytes() == fake_dump(count) for name, count in (("airflow", 4), ("polaris", 6))
        ),
    }


def case_fetch_skips_a_newest_marker_with_an_empty_manifest() -> dict[str, bool]:
    client = StandInS3()
    put_set(client, RUN_A, {"airflow": 3, "mlflow": 2})
    put_marker(client, RUN_B, {"schema": 1, "run": RUN_B, "databases": {}})
    code, target = fetch(client, ["airflow", "mlflow"])
    return {
        "the exit code is 0": code == 0,
        "the older whole set is used": (target / "RUN").read_text(encoding="utf-8").strip() == RUN_A,
        "its dumps are written": (target / "airflow.dump").read_bytes() == fake_dump(3),
    }


def case_fetch_skips_a_marker_that_lacks_an_expected_database() -> dict[str, bool]:
    client = StandInS3()
    put_set(client, RUN_A, {"airflow": 3, "mlflow": 2, "polaris": 5})
    put_set(client, RUN_B, {"airflow": 4, "polaris": 6}, members=DATABASES)
    code, target = fetch(client, list(DATABASES))
    alone = StandInS3()
    put_set(alone, RUN_B, {"airflow": 4, "polaris": 6}, members=DATABASES)
    refused, other = fetch(alone, list(DATABASES))
    older = StandInS3()
    put_set(older, RUN_B, {"airflow": 4, "polaris": 6}, legacy=True)
    legacy_code, legacy_target = fetch(older, list(DATABASES))
    return {
        "the newer marker that records mlflow but lacks it is skipped and the older whole set is used": code == 0
        and (target / "RUN").read_text(encoding="utf-8").strip() == RUN_A
        and (target / "mlflow.dump").read_bytes() == fake_dump(2),
        "with no whole set the fetch fails and writes no dump": refused == 1 and not list(other.glob("*.dump")),
        "a marker that records no membership restores the requested databases it holds": legacy_code == 0
        and (legacy_target / "airflow.dump").is_file()
        and not (legacy_target / "mlflow.dump").exists(),
    }


def case_fetch_takes_an_older_whole_set_after_a_database_is_enabled() -> dict[str, bool]:
    client = StandInS3()
    written = {"polaris": 5, "airflow": 4, "mlflow": 3, "mlflow_auth": 2}
    put_set(client, RUN_A, written)
    current = [*written, "openmetadata_db"]
    code, target = fetch(client, current)
    taken = (target / "RUN").read_text(encoding="utf-8").strip() if (target / "RUN").exists() else ""
    written_dumps = all((target / f"{name}.dump").is_file() and (target / f"{name}.dump").read_bytes() == fake_dump(count) for name, count in written.items())
    no_new_dump = not (target / "openmetadata_db.dump").exists()
    folder = Path(tempfile.mkdtemp(prefix="pg_restore_run_"))
    environment = restore_environment(folder, FRESH, {}, RESTORE_DIR=str(target), PG_DATABASES=f"{LIST} openmetadata_db:mlflow")
    restored, output = databases_shell(environment, folder)
    state = server_state(folder / "server.json")["databases"]
    narrower = StandInS3()
    put_set(narrower, RUN_A, written, members=(*written, "openmetadata_db"))
    refused, refused_target = fetch(narrower, current)
    return {
        "the fetch takes the older set that was whole when it was written": code == 0 and taken == RUN_A,
        "every database of that set is written": written_dumps,
        "no dump is written for the new database": no_new_dump,
        "the databases step restores the old ones and creates the new one empty": restored == 0
        and all(state.get(name, {}).get("tables") == count for name, count in written.items())
        and state.get("openmetadata_db", {}).get("tables") == 0
        and "openmetadata_db created empty" in output,
        "a set that recorded the new database but lacks its dump is still refused": refused == 1 and not list(refused_target.glob("*.dump")),
    }


def case_fetch_restores_an_earlier_uploaders_marker_after_a_database_is_enabled() -> dict[str, bool]:
    client = StandInS3()
    written = {"polaris": 5, "airflow": 4, "mlflow": 3, "mlflow_auth": 2}
    folder = ready_folder(Path(tempfile.mkdtemp(prefix="pg_legacy_writes_")), RUN_A, written)
    legacy_uploader().upload_set(folder, BUCKET, client, lambda: 0.0)
    marker = marker_of(client, RUN_A) or {}
    same_code, same_target = fetch(client, list(written))
    current = [*written, "openmetadata_db"]
    code, target = fetch(client, current)
    taken = (target / "RUN").read_text(encoding="utf-8").strip() if (target / "RUN").exists() else ""
    restored = all((target / f"{name}.dump").is_file() and (target / f"{name}.dump").read_bytes() == fake_dump(count) for name, count in written.items())
    no_new_dump = not (target / "openmetadata_db.dump").exists()
    work = Path(tempfile.mkdtemp(prefix="pg_restore_run_"))
    environment = restore_environment(work, FRESH, {}, RESTORE_DIR=str(target), PG_DATABASES=f"{LIST} openmetadata_db:mlflow")
    shell, output = databases_shell(environment, work)
    state = server_state(work / "server.json")["databases"]
    return {
        "the earlier uploader wrote a marker with no membership record": sorted(marker.get("databases", {})) == sorted(written) and "members" not in marker,
        "the same list restores it": same_code == 0 and all((same_target / f"{name}.dump").is_file() for name in written),
        "the list with the new database restores the databases the marker holds": code == 0 and taken == RUN_A and restored and no_new_dump,
        "the databases step restores them and creates the new database empty": shell == 0
        and all(state.get(name, {}).get("tables") == count for name, count in written.items())
        and state.get("openmetadata_db", {}).get("tables") == 0
        and "openmetadata_db created empty" in output,
    }


def case_fetch_refuses_a_marker_with_a_bad_membership() -> dict[str, bool]:
    checks = {}
    data = fake_dump(3)
    bad: dict[str, dict[str, Any]] = {
        "a membership that is text": {"members": "airflow"},
        "an empty membership": {"members": []},
        "a name in another form": {"members": ["airflow", "Bad-Name"]},
        "a membership that lists a database with no entry": {"members": ["airflow", "mlflow"]},
        "an entry the membership does not list": {"members": ["mlflow"]},
    }
    for name, extra in bad.items():
        client = StandInS3()
        put_set(client, RUN_A, {"airflow": 3})
        client.objects[f"pg_backups/airflow/{RUN_B}.dump"] = data
        put_marker(client, RUN_B, {"schema": 1, "run": RUN_B, "databases": {"airflow": entry(RUN_B, "airflow", data)}, **extra})
        code, target = fetch(client, ["airflow"])
        checks[f"{name}: the older set is used"] = code == 0 and (target / "RUN").read_text(encoding="utf-8").strip() == RUN_A
    return checks


def case_fetch_skips_malformed_markers_and_falls_back() -> dict[str, bool]:
    checks = {}
    data = fake_dump(3)
    bad = {
        "another run in the body": lambda c: put_marker(c, RUN_B, {"schema": 1, "run": RUN_A, "databases": {"airflow": entry(RUN_B, "airflow", data)}}),
        "another schema": lambda c: put_marker(c, RUN_B, {"schema": 2, "run": RUN_B, "databases": {"airflow": entry(RUN_B, "airflow", data)}}),
        "databases is a list": lambda c: put_marker(c, RUN_B, {"schema": 1, "run": RUN_B, "databases": []}),
        "a digest that is not hex": lambda c: put_marker(
            c, RUN_B, {"schema": 1, "run": RUN_B, "databases": {"airflow": entry(RUN_B, "airflow", data, sha256="zz")}}
        ),
        "a size that is text": lambda c: put_marker(c, RUN_B, {"schema": 1, "run": RUN_B, "databases": {"airflow": entry(RUN_B, "airflow", data, bytes="9")}}),
        "no table counted": lambda c: put_marker(c, RUN_B, {"schema": 1, "run": RUN_B, "databases": {"airflow": entry(RUN_B, "airflow", data, tables=0)}}),
        "none of the requested databases": lambda c: put_marker(c, RUN_B, {"schema": 1, "run": RUN_B, "databases": {"other": entry(RUN_B, "other", data)}}),
    }
    for name, make in bad.items():
        client = StandInS3()
        put_set(client, RUN_A, {"airflow": 3})
        client.objects[f"pg_backups/airflow/{RUN_B}.dump"] = data
        make(client)
        code, target = fetch(client, ["airflow"])
        checks[f"{name}: the older set is used"] = code == 0 and (target / "RUN").read_text(encoding="utf-8").strip() == RUN_A
    return checks


def case_fetch_reads_each_dump_in_bounded_chunks() -> dict[str, bool]:
    client = StandInS3()
    big = b"PGDMP" + b"x" * (3 * 1024 * 1024)
    client.objects[f"pg_backups/airflow/{RUN_A}.dump"] = big
    put_marker(client, RUN_A, {"schema": 1, "run": RUN_A, "databases": {"airflow": entry(RUN_A, "airflow", big)}})
    code, target = fetch(client, ["airflow"])
    dump_reads = client.reads[1:]
    return {
        "the exit code is 0 and the dump is whole": code == 0 and (target / "airflow.dump").read_bytes() == big,
        "no read asks for the whole body": bool(dump_reads) and all(size is not None and 0 < size <= 1024 * 1024 for size in dump_reads),
        "the marker is read with a bound too": client.reads[0] is not None and client.reads[0] > 0,
    }


def case_fetch_stops_a_dump_longer_than_its_marker_says() -> dict[str, bool]:
    client = StandInS3()
    declared = b"PGDMP" + b"x" * 100
    client.objects[f"pg_backups/airflow/{RUN_A}.dump"] = declared + b"y" * (5 * 1024 * 1024)
    put_marker(client, RUN_A, {"schema": 1, "run": RUN_A, "databases": {"airflow": entry(RUN_A, "airflow", declared)}})
    code, target = fetch(client, ["airflow"])
    return {
        "the exit code is 1": code == 1,
        "no dump is written": not list(target.glob("*.dump")) and not list(target.glob("*.partial/*")),
        "the reads stopped soon after the declared size": all(size is not None for size in client.reads[1:])
        and sum(size or 0 for size in client.reads[1:]) <= 2 * 1024 * 1024,
    }


def case_fetch_refuses_when_every_set_is_damaged() -> dict[str, bool]:
    client = StandInS3()
    put_set(client, RUN_A, {"airflow": 3}, skip="airflow")
    put_set(client, RUN_B, {"airflow": 4}, damage="airflow")
    put_set(client, RUN_C, {"airflow": 4}, bad_marker=True)
    code, target = fetch(client, ["airflow"])
    return {"the exit code is 1": code == 1, "no dump is written": not list(target.glob("*.dump")), "no RUN file is written": not (target / "RUN").exists()}


def case_fetch_fails_visibly_when_the_bucket_cannot_be_read() -> dict[str, bool]:
    client = StandInS3()
    put_set(client, RUN_A, {"airflow": 3})
    client.fail_list = True
    listing_code, _ = fetch(client, ["airflow"])
    client.fail_list = False
    client.fail_get = True
    get_code, target = fetch(client, ["airflow"])
    return {"a failed listing exits 1": listing_code == 1, "a failed read exits 1 and writes no dump": get_code == 1 and not list(target.glob("*.dump"))}


def case_fetch_stops_at_its_deadline() -> dict[str, bool]:
    client = StandInS3()
    put_set(client, RUN_A, {"airflow": 3, "mlflow": 2})
    ticks = iter([0.0, 0.0, 9999.0] + [9999.0] * 50)
    code, target = fetch(client, ["airflow", "mlflow"], clock=lambda: next(ticks))
    return {"the exit code is 1": code == 1, "no RUN file is written": not (target / "RUN").exists()}


def shell_environment(environment: dict[str, str], folder: Path) -> dict[str, str]:
    """The process environment of a reviewed shell script: the fake programs first on PATH and the fake log in the folder."""
    bin_folder = install_fakes(folder)
    return {**os.environ, **environment, "PATH": f"{bin_folder}{os.pathsep}{os.environ['PATH']}", "FAKE_LOG": str(folder / "fake.log")}


def dump_shell(environment: dict[str, str], folder: Path) -> tuple[int, str]:
    """Run ``pg_backup_dump.sh`` with the fake programs; returns the exit code and everything it printed."""
    result = run_command("sh", ["pg_backup_dump.sh"], cwd=SCRIPTS, env=shell_environment(environment, folder), timeout=60)
    return result.returncode, result.stdout + result.stderr


def databases_shell(environment: dict[str, str], folder: Path) -> tuple[int, str]:
    """Run ``pg_databases.sh`` with the fake programs; returns the exit code and everything it printed."""
    result = run_command("sh", ["pg_databases.sh"], cwd=SCRIPTS, env=shell_environment(environment, folder), timeout=60)
    return result.returncode, result.stdout + result.stderr


def dump_environment(folder: Path, databases: dict[str, int], **extra: str) -> dict[str, str]:
    backups = folder / "backup"
    backups.mkdir(exist_ok=True)
    return {
        "FAKE_SERVER": str(server(folder, databases)),
        "BACKUP_DIR": str(backups),
        "PG_DATABASES": " ".join(databases),
        "PG_BACKUP_ONCE": "1",
        "BACKUP_FIRST_DELAY_SECONDS": "0",
        "DUMP_SECONDS": "20",
        **extra,
    }


def ready_sets(folder: Path) -> list[Path]:
    return sorted((folder / "backup").glob("*.ready"))


def case_dump_leaves_one_ready_set_with_a_manifest() -> dict[str, bool]:
    folder = Path(tempfile.mkdtemp(prefix="pg_dump_"))
    code, output = dump_shell(dump_environment(folder, {"airflow": 3, "mlflow": 2, "polaris": 5}), folder)
    sets = ready_sets(folder)
    manifest = (sets[0] / "manifest.tsv").read_text(encoding="utf-8").splitlines() if sets else []
    return {
        "the exit code is 0": code == 0,
        "one ready set exists": len(sets) == 1,
        "it holds a dump per database": sorted(path.name for path in sets[0].glob("*.dump")) == [f"{name}.dump" for name in DATABASES] if sets else False,
        "the manifest has the tables and bytes of each": manifest
        == [f"{name}\t{count}\t{len(fake_dump(count))}" for name, count in (("airflow", 3), ("mlflow", 2), ("polaris", 5))],
        "no partial folder stays": not list((folder / "backup").glob("*.partial")),
        "the result says dumped": '"result":"dumped"' in (folder / "backup" / "last_result.json").read_text(encoding="utf-8").replace(" ", ""),
    }


def case_dump_refuses_an_empty_database() -> dict[str, bool]:
    folder = Path(tempfile.mkdtemp(prefix="pg_dump_"))
    code, output = dump_shell(dump_environment(folder, {"airflow": 3, "mlflow": 0, "polaris": 5}), folder)
    return {
        "the exit code is 1": code == 1,
        "no ready set exists": not ready_sets(folder),
        "the output names the database and says empty": "mlflow" in output and "empty" in output,
        "no partial folder stays": not list((folder / "backup").glob("*.partial")),
        "the result says refused": "refused" in (folder / "backup" / "last_result.json").read_text(encoding="utf-8"),
    }


def case_dump_refuses_a_truncated_dump() -> dict[str, bool]:
    folder = Path(tempfile.mkdtemp(prefix="pg_dump_"))
    code, output = dump_shell(dump_environment(folder, {"airflow": 3, "polaris": 5}, FAKE_TRUNCATE_DUMP="polaris"), folder)
    return {
        "the exit code is 1": code == 1,
        "no ready set exists": not ready_sets(folder),
        "the output names the database and says unreadable": "polaris" in output and "unreadable" in output,
    }


def case_dump_names_a_failed_pg_dump() -> dict[str, bool]:
    folder = Path(tempfile.mkdtemp(prefix="pg_dump_"))
    code, output = dump_shell(dump_environment(folder, {"airflow": 3, "polaris": 5}, FAKE_FAIL_DUMP="airflow"), folder)
    return {
        "the exit code is 1": code == 1,
        "no ready set exists": not ready_sets(folder),
        "the output names the database and the failed dump": "airflow" in output and "pg_dump failed" in output,
    }


def case_dump_stops_a_dump_that_passes_its_deadline() -> dict[str, bool]:
    folder = Path(tempfile.mkdtemp(prefix="pg_dump_"))
    code, output = dump_shell(dump_environment(folder, {"airflow": 3}, FAKE_DUMP_SLEEP="30", DUMP_SECONDS="1"), folder)
    return {"the exit code is 1": code == 1, "no ready set exists": not ready_sets(folder), "the output says the dump failed": "pg_dump failed" in output}


def case_dump_refuses_a_set_whose_manifest_line_cannot_be_written() -> dict[str, bool]:
    folder = Path(tempfile.mkdtemp(prefix="pg_dump_"))
    code, output = dump_shell(dump_environment(folder, {"airflow": 3, "mlflow": 2}, FAKE_BLOCK_MANIFEST="airflow"), folder)
    result = json.loads((folder / "backup" / "last_result.json").read_text(encoding="utf-8"))
    return {
        "the exit code is 1": code == 1,
        "no ready set exists, so the upload step has nothing to mark": not ready_sets(folder),
        "the output and the result name the manifest and the database": "manifest line" in output
        and "airflow" in result.get("reason", "")
        and result.get("result") == "refused",
    }


def case_dump_removes_leftovers_of_an_interrupted_run() -> dict[str, bool]:
    folder = Path(tempfile.mkdtemp(prefix="pg_dump_"))
    environment = dump_environment(folder, {"airflow": 3})
    (folder / "backup" / f"{RUN_A}.partial").mkdir()
    (folder / "backup" / f"{RUN_A}.partial" / "airflow.dump").write_bytes(b"x")
    code, _ = dump_shell(environment, folder)
    return {
        "the exit code is 0": code == 0,
        "the old partial folder is gone": not (folder / "backup" / f"{RUN_A}.partial").exists(),
        "one new ready set exists": len(ready_sets(folder)) == 1,
    }


def stamped_pass(folder: Path, databases: dict[str, int], client: StandInS3, *, fail_put_after: int | None = None, **extra: str) -> tuple[int, int, str]:
    """One pass of the dump and upload steps as the final backup one-shot runs them: one pass each, no first delay, the upload only after a dump that exited 0.

    The stand-in server is set to ``databases`` first. Returns the dump exit code, the upload exit code (-1 when the upload did not run) and the run ID
    of the set the dump left (empty when it left none)."""
    time.sleep(1.1)  # the run ID has a resolution of one second, so two passes need different seconds
    server(folder, databases)
    task = uuid.uuid4().hex  # the launch of the one-shot passes a new ID to both containers
    environment = dump_environment(folder, databases, PG_BACKUP_TASK_ID=task, **extra)
    dump_code, _ = dump_shell(environment, folder)
    sets = ready_sets(folder)
    run = sets[-1].name.removesuffix(".ready") if sets else ""
    if dump_code != 0:
        return dump_code, -1, run
    client.fail_put_after = fail_put_after
    script = load("pg_backup_upload")
    return dump_code, script.run(folder / "backup", BUCKET, client, tuple(databases), lambda: 0.0, only=script.dumped_run(folder / "backup", task)), run


def restored_run(client: StandInS3, databases: dict[str, int]) -> tuple[str, dict[str, bytes]]:
    """The run ID the restore fetch takes from the bucket and the dumps it wrote."""
    client.fail_put_after = None
    code, target = fetch(client, list(databases))
    dumps = {path.stem: path.read_bytes() for path in target.glob("*.dump")}
    return ((target / "RUN").read_text(encoding="utf-8").strip() if code == 0 and (target / "RUN").exists() else ""), dumps


def case_final_pass_after_an_interval_pass_becomes_the_restore_point() -> dict[str, bool]:
    folder, client = Path(tempfile.mkdtemp(prefix="pg_final_")), StandInS3()
    interval = {"airflow": 3, "polaris": 5}
    final = {"airflow": 4, "polaris": 5}
    first = stamped_pass(folder, interval, client)
    second = stamped_pass(folder, final, client)
    chosen, dumps = restored_run(client, final)
    return {
        "both passes dumped and uploaded": first[:2] == (0, 0) and second[:2] == (0, 0),
        "the two sets have different run IDs": bool(first[2]) and bool(second[2]) and first[2] != second[2],
        "each set has its own marker": all(marker_of(client, run) is not None for run in (first[2], second[2])),
        "the restore takes the final set, not the interval set": chosen == second[2],
        "the dumps it wrote are the final ones (the changed airflow dump)": dumps.get("airflow") == fake_dump(4) and dumps.get("polaris") == fake_dump(5),
        "the backup folder holds no leftover ready set": not ready_sets(folder),
    }


def case_final_set_that_stopped_before_its_marker_is_never_restored() -> dict[str, bool]:
    folder, client = Path(tempfile.mkdtemp(prefix="pg_final_")), StandInS3()
    interval = {"airflow": 3, "polaris": 5}
    final = {"airflow": 4, "polaris": 5}
    first = stamped_pass(folder, interval, client)
    second = stamped_pass(folder, final, client, fail_put_after=len(client.puts) + 1)
    partial_keys = [key for key in client.objects if second[2] and second[2] in key]
    unmarked = marker_of(client, second[2]) is None
    chosen, dumps = restored_run(client, final)
    retry = load("pg_backup_upload").run(folder / "backup", BUCKET, client, tuple(final), lambda: 0.0, only=second[2])
    retried, retried_dumps = restored_run(client, final)
    return {
        "the interval set is complete": first[:2] == (0, 0),
        "the final upload failed after one dump key and wrote no marker": second[1] == 1 and len(partial_keys) == 1 and unmarked,
        "the restore ignores the incomplete final set and takes the interval set": chosen == first[2] and dumps.get("airflow") == fake_dump(3),
        "a rerun of the upload completes the final set without replacing a key": retry == 0 and marker_of(client, second[2]) is not None,
        "the restore then takes the completed final set": retried == second[2] and retried_dumps.get("airflow") == fake_dump(4),
    }


def case_refused_final_dump_leaves_the_interval_set_as_the_restore_point() -> dict[str, bool]:
    folder, client = Path(tempfile.mkdtemp(prefix="pg_final_")), StandInS3()
    interval = {"airflow": 3, "polaris": 5}
    first = stamped_pass(folder, interval, client)
    puts = len(client.puts)
    second = stamped_pass(folder, interval, client, FAKE_FAIL_DUMP="airflow")
    chosen, _ = restored_run(client, interval)
    return {
        "the interval set is complete": first[:2] == (0, 0),
        "the final dump step exits 1 and the upload step does not run": second[0] == 1 and second[1] == -1,
        "the failed dump leaves no ready set and uploads nothing": not ready_sets(folder) and len(client.puts) == puts,
        "the restore still takes the interval set": chosen == first[2],
    }


def case_dump_without_a_published_result_exits_1_and_the_old_pointer_is_refused() -> dict[str, bool]:
    folder, client = Path(tempfile.mkdtemp(prefix="pg_final_")), StandInS3()
    databases = {"airflow": 3, "polaris": 5}
    first, _ = dump_shell(dump_environment(folder, databases, PG_BACKUP_TASK_ID=TASK), folder)
    older = ready_sets(folder)
    time.sleep(1.1)  # the second pass needs another second for its run ID
    backups = folder / "backup"
    (backups / "last_result.json.new").mkdir()  # the temporary name of the result cannot be written
    second, output = dump_shell(dump_environment(folder, databases, PG_BACKUP_TASK_ID=OTHER_TASK), folder)
    script = load("pg_backup_upload")
    pointer = json.loads((backups / "last_result.json").read_text(encoding="utf-8"))
    return {
        "the first pass dumped and published its pointer with its task ID": first == 0 and pointer.get("task") == TASK and pointer.get("run") == older[0].stem,
        "the second pass exits 1 and says that the result cannot be published": second == 1 and "cannot be published" in output,
        "the second pass still left its set, which the service uploads later": len(ready_sets(folder)) == 2,
        "the pointer is still the first pass's, so the one pass of the second task refuses it": refused(script, backups, OTHER_TASK),
        "the first task's own pointer is still read whole": named(script, backups, TASK) == older[0].stem,
        "nothing was uploaded": not client.puts,
    }


def case_dump_publishes_the_result_whole_under_a_temporary_name() -> dict[str, bool]:
    folder = Path(tempfile.mkdtemp(prefix="pg_dump_"))
    code, _ = dump_shell(dump_environment(folder, {"airflow": 3}, PG_BACKUP_TASK_ID=TASK), folder)
    backups = folder / "backup"
    text = (backups / "last_result.json").read_text(encoding="utf-8") if (backups / "last_result.json").exists() else ""
    return {
        "the exit code is 0": code == 0,
        "the pointer is one JSON object that names the run and the task": json.loads(text or "{}").get("task") == TASK
        and json.loads(text or "{}").get("result") == "dumped",
        "no temporary result file stays": not (backups / "last_result.json.new").exists(),
    }


def pin_clock(folder: Path, run: str) -> None:
    """A ``date`` program that answers ``run`` for every call, so two sequential passes get the same run key."""
    shim = install_fakes(folder) / "date"
    shim.write_text(f"#!/bin/sh\nprintf '%s\\n' {run}\n", encoding="utf-8")
    shim.chmod(shim.stat().st_mode | stat.S_IXUSR)


def case_dump_refuses_a_run_key_that_already_has_a_ready_set() -> dict[str, bool]:
    folder, client = Path(tempfile.mkdtemp(prefix="pg_final_")), StandInS3()
    pin_clock(folder, RUN_A)
    interval, _ = dump_shell(dump_environment(folder, {"airflow": 3, "polaris": 5}), folder)
    final, output = dump_shell(dump_environment(folder, {"airflow": 7, "polaris": 5}, PG_BACKUP_TASK_ID=TASK), folder)
    backups = folder / "backup"
    ready = backups / f"{RUN_A}.ready"
    pointer = json.loads((backups / "last_result.json").read_text(encoding="utf-8"))
    script = load("pg_backup_upload")
    return {
        "the interval pass left its set": interval == 0 and (ready / "airflow.dump").read_bytes() == fake_dump(3),
        "the second pass with the same run key exits 1 and says the set exists": final == 1 and "already exists" in output,
        "the new set is not nested inside the earlier one, which is unchanged": (ready / "airflow.dump").read_bytes() == fake_dump(3)
        and not list(ready.glob("*.partial")),
        "no partial folder stays": not list(backups.glob("*.partial")),
        "the pointer says refused, so the pointer of the final task never names a set": pointer.get("result") == "refused" and pointer.get("task") == TASK,
        "the one pass upload of that task refuses the pointer and uploads nothing": refused(script, backups, TASK) and not client.puts,
    }


def observed_publication(folder: Path) -> dict[str, Any]:
    """Run the dump over an old complete pointer, with a ``mv`` that copies the pointer to a file at the moment the new pointer replaces it and a ``cp`` that
    shows a half written pointer to the same observer when the pointer is published by a copy over the old one. Returns the exit code, what was observed
    and the final pointer."""
    backups = folder / "backup"
    backups.mkdir(exist_ok=True)
    (backups / "last_result.json").write_text(
        json.dumps({"result": "previous", "run": RUN_A, "reason": "", "task": "", "at_utc": RUN_A}) + "\n", encoding="utf-8"
    )
    observed = folder / "observed.json"
    shims = install_fakes(folder)
    real = {name: shutil.which(name) for name in ("mv", "cp", "cat", "head")}
    programs = {
        "mv": f'case "$*" in\n  *last_result.json.new*) {real["cat"]} "$BACKUP_DIR/last_result.json" > "$OBSERVED" ;;\nesac\nexec {real["mv"]} "$@"\n',
        "cp": f'case "${{1:-}}" in\n  *last_result.json.new*)\n    {real["head"]} -c 8 "$1" > "$2"\n    {real["cat"]} "$2" > "$OBSERVED"\n    ;;\nesac\n'
        f'exec {real["cp"]} "$@"\n',
    }
    for name, body in programs.items():
        shim = shims / name
        shim.write_text("#!/bin/sh\n" + body, encoding="utf-8")
        shim.chmod(shim.stat().st_mode | stat.S_IXUSR)
    code, _ = dump_shell(dump_environment(folder, {"airflow": 3}, PG_BACKUP_TASK_ID=TASK, OBSERVED=str(observed)), folder)
    return {
        "code": code,
        "observed": observed.read_text(encoding="utf-8") if observed.exists() else "",
        "final": json.loads((backups / "last_result.json").read_text(encoding="utf-8")),
    }


def case_dump_keeps_the_old_complete_pointer_until_the_new_one_replaces_it() -> dict[str, bool]:
    folder = Path(tempfile.mkdtemp(prefix="pg_dump_"))
    seen = observed_publication(folder)
    try:
        previous = json.loads(seen["observed"])
    except ValueError:
        previous = {}
    return {
        "the dump exits 0": seen["code"] == 0,
        "the pointer was observed at the moment of publication": seen["observed"] != "",
        "what a reader sees until the replacement is installed is the whole old pointer": previous.get("result") == "previous" and previous.get("run") == RUN_A,
        "the new pointer then names the task": seen["final"].get("task") == TASK and seen["final"].get("result") == "dumped",
    }


def case_one_pass_main_uploads_only_the_set_of_its_own_dump() -> dict[str, bool]:
    folder, client = Path(tempfile.mkdtemp(prefix="pg_final_")), StandInS3()
    databases = {"airflow": 3, "polaris": 5}
    backups = folder / "backup"
    ready_folder(backups, RUN_A, databases, truncate="polaris")  # an older set that is damaged and is the service's to upload
    ready_folder(backups, RUN_B, databases)  # an older whole set, which the service uploads too
    server(folder, databases)
    task = uuid.uuid4().hex
    dumped, _ = dump_shell(dump_environment(folder, databases, PG_BACKUP_TASK_ID=task), folder)
    run = ready_sets(folder)[-1].name.removesuffix(".ready")
    environment = {
        "DATA_BUCKET_NAME": BUCKET,
        "PG_DATABASES": " ".join(databases),
        "BACKUP_DIR": str(backups),
        "PG_BACKUP_ONCE": "1",
        "PG_BACKUP_TASK_ID": task,
    }
    fake_boto3 = mock.Mock(client=mock.Mock(return_value=client))
    script = load("pg_backup_upload")
    with mock.patch.dict(os.environ, environment), mock.patch.dict(sys.modules, {"boto3": fake_boto3}):
        code = script.main()
    uploaded_runs = {key.split("/")[-1].removesuffix(".dump").removesuffix(".json") for key in client.objects}
    stale = {**environment, "PG_BACKUP_TASK_ID": OTHER_TASK}
    with mock.patch.dict(os.environ, stale), mock.patch.dict(sys.modules, {"boto3": fake_boto3}):
        stale_code = script.main()
    return {
        "the dump step exited 0": dumped == 0,
        "main in one pass mode exits 0": code == 0,
        "it uploaded the set of its own dump and wrote its marker": marker_of(client, run) is not None and uploaded(client, run, "airflow"),
        "it uploaded nothing of the older sets and did not read or rename them": uploaded_runs == {run}
        and (backups / f"{RUN_A}.ready").is_dir()
        and (backups / f"{RUN_B}.ready").is_dir(),
        "the same pass under another task ID refuses the pointer and exits 1": stale_code == 1,
    }


ROLES = "airflow:AIRFLOW_DB_PASSWORD mlflow:MLFLOW_DB_PASSWORD"
LIST = "polaris:polaris airflow:airflow mlflow:mlflow mlflow_auth:mlflow"


def restore_environment(folder: Path, databases: dict[str, int], dumps: dict[str, int], **extra: str) -> dict[str, str]:
    restore = folder / "restore"
    restore.mkdir(exist_ok=True)
    for name, count in dumps.items():
        (restore / f"{name}.dump").write_bytes(fake_dump(count))
    return {
        "FAKE_SERVER": str(server(folder, databases)),
        "RESTORE_DIR": str(restore),
        "PG_DATABASES": LIST,
        "PG_ROLES": ROLES,
        "RESET_SCHEMAS": "polaris_schema",
        "RESTORE_SECONDS": "20",
        "AIRFLOW_DB_PASSWORD": "a-secret-value",
        "MLFLOW_DB_PASSWORD": "b-secret-value",
        "PGDATABASE": "polaris",
        **extra,
    }


def restore_run(databases: dict[str, int], dumps: dict[str, int], **extra: str) -> tuple[int, str, Path]:
    folder = Path(tempfile.mkdtemp(prefix="pg_restore_run_"))
    code, output = databases_shell(restore_environment(folder, databases, dumps, **extra), folder)
    return code, output, folder


FRESH = {"postgres": 0, "polaris": 0}
FRESH_DUMPS = {"polaris": 7, "airflow": 11, "mlflow": 4, "mlflow_auth": 2}


def case_restore_fills_a_fresh_server_from_the_dumps() -> dict[str, bool]:
    code, output, folder = restore_run(FRESH, FRESH_DUMPS)
    state = server_state(folder / "server.json")
    databases = state["databases"]
    return {
        "the exit code is 0": code == 0,
        "every database holds its dump": all(databases.get(name, {}).get("tables") == count for name, count in FRESH_DUMPS.items()),
        "no temporary database stays": not any(name.endswith("__restore") for name in databases),
        "the roles were created before any restore": state["roles"] == ["airflow", "mlflow"] and log_lines(folder / "fake.log")[0] == "psql roles",
        "the output reports each restore": all(f"restored database {name}" in output for name in FRESH_DUMPS),
        "no password reaches the output": "secret-value" not in output,
    }


def case_restore_never_touches_a_database_with_data() -> dict[str, bool]:
    databases = {"postgres": 0, "polaris": 9, "airflow": 5, "mlflow": 3, "mlflow_auth": 1}
    code, output, folder = restore_run(databases, FRESH_DUMPS)
    log = log_lines(folder / "fake.log")
    return {
        "the exit code is 0": code == 0,
        "no pg_restore ran": not any(line.startswith("pg_restore") for line in log),
        "the tables are unchanged": all(
            server_state(folder / "server.json")["databases"][name]["tables"] == count for name, count in databases.items() if name != "postgres"
        ),
        "the output says the databases were left as they are": output.count("left as it is") == 4,
    }


def case_restore_only_fills_the_empty_ones() -> dict[str, bool]:
    databases = {"postgres": 0, "polaris": 9, "airflow": 5}
    code, output, folder = restore_run(databases, FRESH_DUMPS)
    state = server_state(folder / "server.json")["databases"]
    return {
        "the exit code is 0": code == 0,
        "the filled databases are unchanged": state["polaris"]["tables"] == 9 and state["airflow"]["tables"] == 5,
        "the missing ones are restored": state["mlflow"]["tables"] == 4 and state["mlflow_auth"]["tables"] == 2,
    }


def case_restore_without_a_dump_creates_empty_databases() -> dict[str, bool]:
    code, output, folder = restore_run(FRESH, {})
    state = server_state(folder / "server.json")["databases"]
    return {
        "the exit code is 0": code == 0,
        "the missing databases exist and are empty": all(state.get(name, {}).get("tables") == 0 for name in ("airflow", "mlflow", "mlflow_auth")),
        "no pg_restore ran": not any(line.startswith("pg_restore") for line in log_lines(folder / "fake.log")),
        "the output says no completed dump": "no completed dump" in output,
    }


def case_restore_failure_leaves_no_half_restored_database() -> dict[str, bool]:
    code, output, folder = restore_run(FRESH, FRESH_DUMPS, FAKE_FAIL_RESTORE="mlflow")
    state = server_state(folder / "server.json")["databases"]
    return {
        "the exit code is 1": code == 1,
        "the output names the database and the step": "restore refused" in output and "mlflow" in output,
        "no missing database was made visible": not any(name in state for name in ("airflow", "mlflow", "mlflow_auth")),
        "no temporary database stays": not any(name.endswith("__restore") for name in state),
        "the empty database that exists was not filled": state["polaris"]["tables"] == 0,
    }


def case_restore_refuses_a_restore_that_lost_a_table() -> dict[str, bool]:
    code, output, folder = restore_run(FRESH, FRESH_DUMPS, FAKE_SHORT_RESTORE="airflow")
    state = server_state(folder / "server.json")["databases"]
    return {
        "the exit code is 1": code == 1,
        "the output names the database and the table counts": "airflow" in output and "tables" in output and "11" in output and "10" in output,
        "the database is not made visible": "airflow" not in state and not any(name.endswith("__restore") for name in state),
    }


def case_restore_is_idempotent() -> dict[str, bool]:
    folder = Path(tempfile.mkdtemp(prefix="pg_restore_run_"))
    environment = restore_environment(folder, FRESH, FRESH_DUMPS)
    first, _ = databases_shell(environment, folder)
    before = server_state(folder / "server.json")
    restores = sum(line.startswith("pg_restore") for line in log_lines(folder / "fake.log"))
    second, output = databases_shell(environment, folder)
    return {
        "both runs exit 0": first == 0 and second == 0,
        "the second run changes nothing": server_state(folder / "server.json")["databases"] == before["databases"],
        "the second run restores nothing": sum(line.startswith("pg_restore") for line in log_lines(folder / "fake.log")) == restores,
        "the second run says the databases have data": output.count("left as it is") == 4,
    }


def case_restore_recovers_after_a_leftover_temporary_database() -> dict[str, bool]:
    folder = Path(tempfile.mkdtemp(prefix="pg_restore_run_"))
    environment = restore_environment(folder, {"postgres": 0, "polaris": 0, "airflow__restore": 3}, FRESH_DUMPS)
    code, output = databases_shell(environment, folder)
    state = server_state(folder / "server.json")["databases"]
    return {
        "the exit code is 0": code == 0,
        "the leftover is replaced by the restored database": state.get("airflow", {}).get("tables") == 11 and "airflow__restore" not in state,
    }


CASES: dict[str, Callable[[], dict[str, bool]]] = {
    "dump_leaves_one_ready_set_with_a_manifest": case_dump_leaves_one_ready_set_with_a_manifest,
    "dump_without_a_published_result_exits_1_and_the_old_pointer_is_refused": case_dump_without_a_published_result_exits_1_and_the_old_pointer_is_refused,
    "dump_publishes_the_result_whole_under_a_temporary_name": case_dump_publishes_the_result_whole_under_a_temporary_name,
    "dump_refuses_a_run_key_that_already_has_a_ready_set": case_dump_refuses_a_run_key_that_already_has_a_ready_set,
    "dump_keeps_the_old_complete_pointer_until_the_new_one_replaces_it": case_dump_keeps_the_old_complete_pointer_until_the_new_one_replaces_it,
    "one_pass_main_uploads_only_the_set_of_its_own_dump": case_one_pass_main_uploads_only_the_set_of_its_own_dump,
    "final_pass_after_an_interval_pass_becomes_the_restore_point": case_final_pass_after_an_interval_pass_becomes_the_restore_point,
    "final_set_that_stopped_before_its_marker_is_never_restored": case_final_set_that_stopped_before_its_marker_is_never_restored,
    "refused_final_dump_leaves_the_interval_set_as_the_restore_point": case_refused_final_dump_leaves_the_interval_set_as_the_restore_point,
    "dump_names_a_failed_pg_dump": case_dump_names_a_failed_pg_dump,
    "dump_refuses_a_set_whose_manifest_line_cannot_be_written": case_dump_refuses_a_set_whose_manifest_line_cannot_be_written,
    "upload_refuses_a_manifest_that_omits_an_expected_database": case_upload_refuses_a_manifest_that_omits_an_expected_database,
    "upload_refuses_a_manifest_that_lists_another_database": case_upload_refuses_a_manifest_that_lists_another_database,
    "one_pass_upload_takes_only_the_set_its_dump_just_wrote": case_one_pass_upload_takes_only_the_set_its_dump_just_wrote,
    "one_pass_upload_refuses_when_its_dump_left_no_set": case_one_pass_upload_refuses_when_its_dump_left_no_set,
    "fetch_skips_a_marker_that_lacks_an_expected_database": case_fetch_skips_a_marker_that_lacks_an_expected_database,
    "fetch_takes_an_older_whole_set_after_a_database_is_enabled": case_fetch_takes_an_older_whole_set_after_a_database_is_enabled,
    "fetch_refuses_a_marker_with_a_bad_membership": case_fetch_refuses_a_marker_with_a_bad_membership,
    "fetch_restores_an_earlier_uploaders_marker_after_a_database_is_enabled": case_fetch_restores_an_earlier_uploaders_marker_after_a_database_is_enabled,
    "dump_refuses_a_truncated_dump": case_dump_refuses_a_truncated_dump,
    "dump_refuses_an_empty_database": case_dump_refuses_an_empty_database,
    "dump_removes_leftovers_of_an_interrupted_run": case_dump_removes_leftovers_of_an_interrupted_run,
    "dump_stops_a_dump_that_passes_its_deadline": case_dump_stops_a_dump_that_passes_its_deadline,
    "fetch_fails_visibly_when_the_bucket_cannot_be_read": case_fetch_fails_visibly_when_the_bucket_cannot_be_read,
    "fetch_falls_back_to_the_older_set_when_the_newest_is_damaged": case_fetch_falls_back_to_the_older_set_when_the_newest_is_damaged,
    "fetch_reads_only_the_requested_databases": case_fetch_reads_only_the_requested_databases,
    "fetch_refuses_when_every_set_is_damaged": case_fetch_refuses_when_every_set_is_damaged,
    "fetch_stops_at_its_deadline": case_fetch_stops_at_its_deadline,
    "fetch_takes_the_newest_completed_set": case_fetch_takes_the_newest_completed_set,
    "fetch_with_no_completed_backup_restores_nothing": case_fetch_with_no_completed_backup_restores_nothing,
    "restore_failure_leaves_no_half_restored_database": case_restore_failure_leaves_no_half_restored_database,
    "restore_fills_a_fresh_server_from_the_dumps": case_restore_fills_a_fresh_server_from_the_dumps,
    "restore_is_idempotent": case_restore_is_idempotent,
    "restore_never_touches_a_database_with_data": case_restore_never_touches_a_database_with_data,
    "restore_only_fills_the_empty_ones": case_restore_only_fills_the_empty_ones,
    "restore_recovers_after_a_leftover_temporary_database": case_restore_recovers_after_a_leftover_temporary_database,
    "restore_refuses_a_restore_that_lost_a_table": case_restore_refuses_a_restore_that_lost_a_table,
    "restore_without_a_dump_creates_empty_databases": case_restore_without_a_dump_creates_empty_databases,
    "upload_a_rerun_after_an_interruption_adds_nothing_twice": case_upload_a_rerun_after_an_interruption_adds_nothing_twice,
    "upload_needs_no_read_permission": case_upload_needs_no_read_permission,
    "upload_never_replaces_an_existing_key": case_upload_never_replaces_an_existing_key,
    "upload_refuses_a_file_that_is_not_a_dump": case_upload_refuses_a_file_that_is_not_a_dump,
    "upload_refuses_a_truncated_dump": case_upload_refuses_a_truncated_dump,
    "upload_refuses_an_empty_dump": case_upload_refuses_an_empty_dump,
    "upload_sets_a_set_aside_after_three_failed_tries": case_upload_sets_a_set_aside_after_three_failed_tries,
    "upload_stops_at_its_deadline": case_upload_stops_at_its_deadline,
    "upload_takes_the_oldest_set_first_and_leaves_partial_sets": case_upload_takes_the_oldest_set_first_and_leaves_partial_sets,
    "upload_writes_every_dump_then_the_marker": case_upload_writes_every_dump_then_the_marker,
    "fetch_reads_each_dump_in_bounded_chunks": case_fetch_reads_each_dump_in_bounded_chunks,
    "fetch_skips_a_newest_marker_with_an_empty_manifest": case_fetch_skips_a_newest_marker_with_an_empty_manifest,
    "fetch_skips_malformed_markers_and_falls_back": case_fetch_skips_malformed_markers_and_falls_back,
    "fetch_stops_a_dump_longer_than_its_marker_says": case_fetch_stops_a_dump_longer_than_its_marker_says,
    "upload_treats_a_client_library_error_as_a_failed_try": case_upload_treats_a_client_library_error_as_a_failed_try,
}
if len(CASES) != sum(name.startswith("case_") for name in globals()):
    raise SystemExit("a case_ function is missing from CASES")
CASE_ERRORS = (AssertionError, LookupError, OSError, TypeError, ValueError, AttributeError, NameError, ImportError)


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
