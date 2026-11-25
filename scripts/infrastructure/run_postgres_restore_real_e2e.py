"""Prove the dump, upload, fetch and restore scripts against a real scratch PostgreSQL server: dump it, wipe it, restore it and compare.

This is a heavy run for the lead. It needs a scratch PostgreSQL 17 server that nothing else uses and the client programs ``psql``, ``pg_dump`` and
``pg_restore`` of version 17 on PATH (``PG_BIN`` names a folder that holds them first). Start the server by any means you choose, for example the pinned
image of config/metadata/images.json on the host network with POSTGRES_USER and POSTGRES_DB set to polaris. This runner starts no container and calls no
Docker. Environment: ``PGHOST`` (127.0.0.1 or localhost), ``PGPORT``, ``PGUSER`` (a superuser), ``PGPASSWORD``, ``PGDATABASE=polaris`` and ``PG_BIN``.

The runner refuses a server that is not empty before it changes anything: no relation outside the system schemas in ``polaris``, no schema in ``polaris``
besides ``public``, none of the databases ``airflow``, ``mlflow`` and ``mlflow_auth`` (or a ``__restore`` copy) and none of the roles ``airflow`` and
``mlflow``. It owns what it creates: one table with a unique name in each database, and the roles and databases the reviewed script makes. It removes
exactly those at the end.

Steps: the reviewed ``pg_databases.sh`` creates the roles and empty databases. The runner seeds its table in each database under the intended owner
(``airflow``, ``mlflow``, ``mlflow`` and the superuser for ``polaris``) and asserts the owners. ``pg_backup_dump.sh`` dumps each database once and
``pg_backup_upload.py`` uploads the set to a stand-in S3 bucket. The runner hashes the column definitions and the content of every row, drops the
databases and the polaris table, and ``pg_restore_fetch.py`` with ``pg_databases.sh`` restore them. The runner compares every hash and the owners. A second
run of ``pg_databases.sh`` must leave everything as it is. Only S3 is a stand-in (``postgres_backup_fakes.StandInS3``): PostgreSQL and its client
programs are real. SQL text is fixed; the table name reaches it as a psql variable.
"""

from __future__ import annotations

import json
import os
import re
import sys
import tempfile
import uuid
from datetime import UTC, datetime
from pathlib import Path

from scripts.infrastructure.postgres_backup_fakes import BUCKET, StandInS3
from scripts.infrastructure.run_postgres_backup_e2e import SCRIPTS, load
from scripts.process import run_command

REPO_ROOT = Path(__file__).resolve().parents[2]
OUTPUT = REPO_ROOT / "data" / "e2e" / "postgres_restore_real"
DATABASES = ("polaris", "airflow", "mlflow", "mlflow_auth")
OWNERS = {"polaris": None, "airflow": "airflow", "mlflow": "mlflow", "mlflow_auth": "mlflow"}
PROJECT_ROLES = ("airflow", "mlflow")
TABLE = f"restore_proof_{uuid.uuid4().hex[:8]}"
IDENTIFIER = re.compile(r"[a-z][a-z0-9_]*")
ROWS = 50
SEED = (
    'CREATE TABLE public.:"tbl" (id int PRIMARY KEY, label text NOT NULL, amount numeric(10, 2), note text, seen_at timestamptz);\n'
    "INSERT INTO public.:\"tbl\" SELECT g, current_database() || '-' || g, g * 1.25, CASE WHEN g % 7 = 0 THEN NULL ELSE 'ünï-' || g || E'\\ttab' END, "
    "timestamptz '2026-01-01 00:00:00+00' + g * interval '1 hour' FROM generate_series(1, 50) g;\n"
)
CONTENT_SQL = "SELECT md5(coalesce(string_agg(t::text, E'\\n' ORDER BY t::text), '')) || ':' || count(*) FROM public.:\"tbl\" t;"
COLUMNS_SQL = (
    "SELECT md5(string_agg(column_name || ' ' || data_type || ' ' || is_nullable || ' ' || coalesce(numeric_precision::text, '') || ',' || "
    "coalesce(numeric_scale::text, ''), ';' ORDER BY ordinal_position)) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = :'tbl';"
)
OWNER_SQL = "SELECT tableowner FROM pg_tables WHERE schemaname = 'public' AND tablename = :'tbl';"
RELATIONS_SQL = (
    "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relkind IN ('r','p','v','m','S','f') "
    "AND n.nspname NOT IN ('pg_catalog','information_schema') AND n.nspname NOT LIKE 'pg_toast%';"
)


class Refused(Exception):
    """The environment is not an empty scratch server."""


def psql(database: str, statement: str, **variables: str) -> str:
    """Run fixed SQL text on a database; ``tbl`` is always the proof table, other variables are named by the caller and reach the text as psql variables."""
    with tempfile.NamedTemporaryFile("w", suffix=".sql", delete=False) as handle:
        handle.write(statement)
    arguments = ["-X", "-q", "-v", "ON_ERROR_STOP=1", "-v", f"tbl={TABLE}", *[item for key, value in variables.items() for item in ("-v", f"{key}={value}")]]
    result = run_command("psql", [*arguments, "-At", "-d", database, "-f", handle.name], timeout=60)
    Path(handle.name).unlink(missing_ok=True)
    if result.returncode != 0:
        raise Refused(f"psql failed on {database}: {result.stderr.strip()[:200]}")
    return result.stdout.strip()


def databases_script(environment: dict[str, str]) -> tuple[int, str]:
    """Run ``pg_databases.sh`` against the scratch server."""
    result = run_command("sh", ["pg_databases.sh"], cwd=SCRIPTS, env={**os.environ, **environment}, timeout=300)
    return result.returncode, result.stdout + result.stderr


def dump_script(environment: dict[str, str]) -> tuple[int, str]:
    """Run one pass of ``pg_backup_dump.sh`` against the scratch server."""
    result = run_command("sh", ["pg_backup_dump.sh"], cwd=SCRIPTS, env={**os.environ, **environment}, timeout=300)
    return result.returncode, result.stdout + result.stderr


def require_empty_server() -> None:
    """Refuse unless nothing of the project exists on the server, before any change."""
    if os.environ.get("PGHOST") not in {"127.0.0.1", "localhost"} or os.environ.get("PGDATABASE") != "polaris":
        raise Refused("PGHOST must be 127.0.0.1 or localhost and PGDATABASE must be polaris")
    if not IDENTIFIER.fullmatch(TABLE):
        raise Refused("the proof table name is not a plain identifier")
    found = psql(
        "postgres", "SELECT string_agg(datname, ',') FROM pg_database WHERE datname IN ('airflow', 'mlflow', 'mlflow_auth') OR datname LIKE '%\\_\\_restore'"
    )
    if found:
        raise Refused(f"the server already holds databases: {found}")
    roles = psql("postgres", "SELECT string_agg(rolname, ',') FROM pg_roles WHERE rolname IN ('airflow', 'mlflow')")
    if roles:
        raise Refused(f"the server already holds roles: {roles}")
    if psql("polaris", RELATIONS_SQL) != "0":
        raise Refused("the polaris database already holds relations")
    schemas = psql(
        "polaris", "SELECT string_agg(nspname, ',') FROM pg_namespace WHERE nspname NOT IN ('public', 'information_schema') AND nspname NOT LIKE 'pg\\_%'"
    )
    if schemas:
        raise Refused(f"the polaris database already holds schemas: {schemas}")


def seed(name: str) -> None:
    """Create the proof table in a database under its intended owner and fill it."""
    owner = OWNERS[name]
    psql(name, ('SET ROLE :"owner";\n' if owner else "") + SEED, owner=owner or "")


def snapshot() -> dict[str, dict[str, str]]:
    """Per database: the owner of the table, the hash of its column definitions and the hash and count of its rows."""
    return {name: {"owner": psql(name, OWNER_SQL), "columns": psql(name, COLUMNS_SQL), "content": psql(name, CONTENT_SQL)} for name in DATABASES}


def cleanup(created_roles: bool) -> None:
    """Remove exactly what this run made."""
    for name in ("airflow", "mlflow", "mlflow_auth"):
        psql("postgres", 'DROP DATABASE IF EXISTS :"db" WITH (FORCE);', db=name)
    psql("polaris", 'DROP TABLE IF EXISTS public.:"tbl";')
    psql("polaris", "DROP SCHEMA IF EXISTS polaris_schema")
    if created_roles:
        for role in PROJECT_ROLES:
            psql("postgres", 'DROP ROLE IF EXISTS :"role";', role=role)


def main() -> int:
    if os.environ.get("PG_BIN"):
        os.environ["PATH"] = os.environ["PG_BIN"] + os.pathsep + os.environ["PATH"]
    require_empty_server()
    work = Path(tempfile.mkdtemp(prefix="pg_real_e2e_"))
    restore, backup = work / "restore", work / "backup"
    restore.mkdir()
    backup.mkdir()
    environment = {
        "PG_DATABASES": " ".join(f"{name}:{OWNERS[name] or os.environ['PGUSER']}" for name in DATABASES),
        "PG_ROLES": "airflow:AIRFLOW_DB_PASSWORD mlflow:MLFLOW_DB_PASSWORD",
        "AIRFLOW_DB_PASSWORD": "synthetic-airflow-value",
        "MLFLOW_DB_PASSWORD": "synthetic-mlflow-value",
        "RESET_SCHEMAS": "polaris_schema",
        "RESTORE_DIR": str(restore),
        "RESTORE_SECONDS": "300",
    }
    results: dict[str, bool] = {}
    created_roles = False
    try:
        code, output = databases_script(environment)
        created_roles = True
        results["the first setup creates the roles and empty databases"] = code == 0 and "created empty" in output
        for name in DATABASES:
            seed(name)
        before = snapshot()
        results["every table is owned by its intended owner before the dump"] = {name: row["owner"] for name, row in before.items()} == {
            "polaris": os.environ["PGUSER"],
            "airflow": "airflow",
            "mlflow": "mlflow",
            "mlflow_auth": "mlflow",
        }
        results["every table holds its rows"] = all(row["content"].endswith(f":{ROWS}") for row in before.values())
        dump_env = {
            "PG_DATABASES": " ".join(DATABASES),
            "BACKUP_DIR": str(backup),
            "PG_BACKUP_ONCE": "1",
            "BACKUP_FIRST_DELAY_SECONDS": "0",
            "DUMP_SECONDS": "300",
        }
        code, output = dump_script(dump_env)
        results["the dump step leaves one ready set"] = code == 0 and len(list(backup.glob("*.ready"))) == 1
        client = StandInS3()
        results["the upload step writes the set and the marker"] = load("pg_backup_upload").run(
            backup, BUCKET, client, tuple(DATABASES), lambda: 0.0
        ) == 0 and any("/complete/" in key for key in client.objects)
        for name in ("airflow", "mlflow", "mlflow_auth"):
            psql("postgres", 'DROP DATABASE :"db";', db=name)
        psql("polaris", 'DROP TABLE public.:"tbl";')
        fetched = load("pg_restore_fetch").run(BUCKET, list(DATABASES), restore, client, lambda: 0.0)
        code, output = databases_script(environment)
        results["the fetch and the restore succeed"] = fetched == 0 and code == 0 and "restored database" in output
        after = snapshot()
        results["every table has its column definitions back"] = {name: row["columns"] for name, row in after.items()} == {
            name: row["columns"] for name, row in before.items()
        }
        results["every table has its row contents back"] = {name: row["content"] for name, row in after.items()} == {
            name: row["content"] for name, row in before.items()
        }
        results["every table has its owner back"] = {name: row["owner"] for name, row in after.items()} == {name: row["owner"] for name, row in before.items()}
        results["no temporary database stays"] = psql("postgres", "SELECT count(*) FROM pg_database WHERE datname LIKE '%\\_\\_restore'") == "0"
        for name in DATABASES:
            psql(name, "INSERT INTO public.:\"tbl\" (id, label) VALUES (1000, 'kept');")
        code, output = databases_script(environment)
        results["a second setup leaves databases with data as they are"] = (
            code == 0
            and output.count("left as it is") == len(DATABASES)
            and all(psql(name, 'SELECT count(*) FROM public.:"tbl" WHERE id = 1000;') == "1" for name in DATABASES)
        )
    finally:
        cleanup(created_roles)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    OUTPUT.mkdir(parents=True, exist_ok=True)
    (OUTPUT / f"report_{stamp}.json").write_text(
        json.dumps({"run_at_utc": stamp, "results": results, "passed": all(results.values())}, indent=2, sort_keys=True) + "\n"
    )
    for name, ok in results.items():
        sys.stdout.write(f"{'PASS' if ok else 'FAIL'} {name}\n")
    return 0 if all(results.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
