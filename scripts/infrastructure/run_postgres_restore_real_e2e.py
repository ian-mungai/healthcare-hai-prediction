"""Prove the dump, upload, fetch and restore scripts against a real scratch PostgreSQL server: dump it, wipe it, restore it and compare.

This is a heavy run for the lead. It needs a scratch PostgreSQL 17 server that nothing else uses and the client programs ``psql``, ``pg_dump`` and
``pg_restore`` of version 17 on PATH (``PG_BIN`` names a folder that holds them first). Start the server by any means you choose, for example the pinned
image of config/metadata/images.json on the host network with POSTGRES_USER and POSTGRES_DB set to polaris. This runner starts no container and calls no
Docker. Environment: ``PGHOST`` (127.0.0.1 or localhost), ``PGPORT``, ``PGUSER`` (a superuser), ``PGPASSWORD``, ``PGDATABASE=polaris`` and ``PG_BIN``.
The runner refuses a server that already holds the databases it creates, so it never touches real data.

Steps: the reviewed ``pg_databases.sh`` creates the roles and empty databases. The runner seeds tables and rows. ``pg_backup_dump.sh`` dumps each database
once and ``pg_backup_upload.py`` uploads the set to a stand-in S3 bucket. The runner records a hash of every table, drops the databases and the polaris
table, and ``pg_restore_fetch.py`` with ``pg_databases.sh`` restore them. The runner compares every hash. A second run of ``pg_databases.sh`` must leave
everything as it is. Only S3 is a stand-in (``postgres_backup_fakes.StandInS3``): PostgreSQL and its client programs are real.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path

from scripts.infrastructure.postgres_backup_fakes import BUCKET, StandInS3
from scripts.infrastructure.run_postgres_backup_e2e import SCRIPTS, load
from scripts.process import run_command

REPO_ROOT = Path(__file__).resolve().parents[2]
OUTPUT = REPO_ROOT / "data" / "e2e" / "postgres_restore_real"
DATABASES = ("polaris", "airflow", "mlflow", "mlflow_auth")
OWNERS = {"polaris": "polaris", "airflow": "airflow", "mlflow": "mlflow", "mlflow_auth": "mlflow"}
HASH_SQL = (
    "SELECT coalesce(md5(string_agg(t.table_name || ':' || (xpath('/row/c/text()', query_to_xml(format('SELECT count(*) AS c FROM %I.%I', t.table_schema, "
    "t.table_name), false, true, '')))[1]::text, ',' ORDER BY t.table_name)), 'none') FROM information_schema.tables t "
    "WHERE t.table_schema NOT IN ('pg_catalog', 'information_schema') AND t.table_type = 'BASE TABLE'"
)


class Refused(Exception):
    """The environment is not a safe scratch server."""


def psql(database: str, statement: str) -> str:
    result = run_command("psql", ["-X", "-v", "ON_ERROR_STOP=1", "-At", "-d", database, "-c", statement], timeout=60)
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


def main() -> int:
    if os.environ.get("PG_BIN"):
        os.environ["PATH"] = os.environ["PG_BIN"] + os.pathsep + os.environ["PATH"]
    if os.environ.get("PGHOST") not in {"127.0.0.1", "localhost"} or os.environ.get("PGDATABASE") != "polaris":
        raise Refused("PGHOST must be 127.0.0.1 or localhost and PGDATABASE must be polaris")
    existing = psql("postgres", "SELECT string_agg(datname, ',') FROM pg_database WHERE datname IN ('airflow', 'mlflow', 'mlflow_auth')")
    if existing:
        raise Refused(f"the server already holds databases of the project: {existing}")
    work = Path(tempfile.mkdtemp(prefix="pg_real_e2e_"))
    restore, backup = work / "restore", work / "backup"
    restore.mkdir()
    backup.mkdir()
    environment = {
        "PG_DATABASES": " ".join(f"{name}:{OWNERS[name]}" for name in DATABASES),
        "PG_ROLES": "airflow:AIRFLOW_DB_PASSWORD mlflow:MLFLOW_DB_PASSWORD",
        "AIRFLOW_DB_PASSWORD": "synthetic-airflow-value",
        "MLFLOW_DB_PASSWORD": "synthetic-mlflow-value",
        "RESET_SCHEMAS": "polaris_schema",
        "RESTORE_DIR": str(restore),
        "RESTORE_SECONDS": "300",
    }
    results: dict[str, bool] = {}
    code, output = databases_script(environment)
    results["the first setup creates the roles and empty databases"] = code == 0 and "created empty" in output
    for name in DATABASES:
        psql(name, "CREATE TABLE IF NOT EXISTS public.seed (id int PRIMARY KEY, label text)")
        psql(name, "INSERT INTO public.seed SELECT g, current_database() || '-' || g FROM generate_series(1, 50) g ON CONFLICT DO NOTHING")
    before = {name: psql(name, HASH_SQL) for name in DATABASES}
    dump_env = {"PG_DATABASES": " ".join(DATABASES), "BACKUP_DIR": str(backup), "PG_BACKUP_ONCE": "1", "BACKUP_FIRST_DELAY_SECONDS": "0", "DUMP_SECONDS": "300"}
    code, output = dump_script(dump_env)
    results["the dump step leaves one ready set"] = code == 0 and len(list(backup.glob("*.ready"))) == 1
    client = StandInS3()
    results["the upload step writes the set and the marker"] = load("pg_backup_upload").run(backup, BUCKET, client, lambda: 0.0) == 0 and any(
        "/complete/" in key for key in client.objects
    )
    for name in ("airflow", "mlflow", "mlflow_auth"):
        psql("postgres", f'DROP DATABASE "{name}"')
    psql("polaris", "DROP TABLE public.seed")
    fetched = load("pg_restore_fetch").run(BUCKET, list(DATABASES), restore, client, lambda: 0.0)
    code, output = databases_script(environment)
    results["the fetch and the restore succeed"] = fetched == 0 and code == 0 and "restored database" in output
    after = {name: psql(name, HASH_SQL) for name in DATABASES}
    results["every database has its tables and row counts back"] = after == before and all(value != "none" for value in after.values())
    results["no temporary database stays"] = psql("postgres", "SELECT count(*) FROM pg_database WHERE datname LIKE '%__restore'") == "0"
    results["the owner of a restored table is the database owner"] = psql("airflow", "SELECT tableowner FROM pg_tables WHERE tablename = 'seed'") == "airflow"
    for name in DATABASES:
        psql(name, "INSERT INTO public.seed VALUES (1000, 'kept')")
    code, output = databases_script(environment)
    results["a second setup leaves databases with data as they are"] = (
        code == 0
        and output.count("left as it is") == len(DATABASES)
        and all(psql(name, "SELECT count(*) FROM public.seed WHERE id = 1000") == "1" for name in DATABASES)
    )
    for name in ("airflow", "mlflow", "mlflow_auth"):
        psql("postgres", f'DROP DATABASE IF EXISTS "{name}"')
    psql("polaris", "DROP TABLE IF EXISTS public.seed")
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
