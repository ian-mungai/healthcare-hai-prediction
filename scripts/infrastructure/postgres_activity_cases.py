"""Cases for what the Postgres monitoring role can see of the bootstrap session (review of 84b3722..7fc9030, finding 2).

The monitoring role holds ``pg_monitor``, which includes ``pg_read_all_stats``: it shows the text of other sessions' running statements. The bootstrap session
of ``infra/services/postgres.tf`` sends the role passwords as statements (``ALTER ROLE ... PASSWORD``), so from the second start on, when the monitoring role
exists, a statement that waits for a lock shows its password text to that role. The fix is ``SET track_activities = off`` as the first statement of the session.

The first case reads the session's SQL from the Terraform file and requires that statement before the first password. The second case starts a native
PostgreSQL cluster on loopback with synthetic text in place of passwords, holds a row lock on one role, runs the session's SQL so that its first ``ALTER ROLE``
waits, and reads ``pg_stat_activity`` as the monitoring role. A planted copy without the statement must show the text, so the case can fail. The second case
skips with a named reason when PostgreSQL is not installed.
"""

from __future__ import annotations

import re
import shutil
import socket
import tempfile
from collections.abc import Callable
from pathlib import Path

from scripts.process import run_command

POSTGRES_FILE = "infra/services/postgres.tf"
MAIN_BLOCK = re.compile(r"postgres_databases_main = <<-EOT\n(.*?)\n  EOT", re.DOTALL)
HEADER = "exec psql -X <<'SQL'"
OFF = "SET track_activities = off;"
AIRFLOW_PROBE = "synthetic-airflow-probe-text"
MARQUEZ_PROBE = "synthetic-marquez-probe-text"
LIMIT_SECONDS = 60.0
SEARCH = (
    "/opt/homebrew/opt/postgresql@17/bin",
    "/opt/homebrew/opt/postgresql@16/bin",
    "/opt/homebrew/opt/postgresql@15/bin",
    "/usr/local/bin",
    "/usr/bin",
    "/bin",
)
SCRIPT = r"""
set -u
dir=$(mktemp -d)
data="$dir/data"
cleanup() {
  pg_ctl -D "$data" -m immediate stop >/dev/null 2>&1
  wait 2>/dev/null
  cd /
  rm -rf "$dir"
}
trap cleanup EXIT
initdb -D "$data" -U polaris --auth=trust -E UTF8 >/dev/null 2>&1 || { echo "setup=initdb failed"; exit 0; }
pg_ctl -D "$data" -o "-p $HAI_PORT -c listen_addresses=127.0.0.1 -c unix_socket_directories=$dir" -w -t 20 -l "$dir/server.log" start >/dev/null 2>&1 \
  || { echo "setup=start failed $(tail -c 300 "$dir/server.log" | tr "\n" " ")"; exit 0; }
export PGHOST=127.0.0.1 PGPORT=$HAI_PORT PGUSER=polaris
psql -X -d postgres -q -c "CREATE DATABASE polaris" -c "CREATE ROLE airflow LOGIN" -c "CREATE ROLE postgres_exporter LOGIN" \
  -c "GRANT pg_monitor TO postgres_exporter" >/dev/null 2>&1 || { echo "setup=roles failed"; exit 0; }
export PGDATABASE=polaris
psql -X -q -c "BEGIN; SELECT 1 FROM pg_authid WHERE rolname = 'airflow' FOR UPDATE; SELECT pg_sleep(8); COMMIT;" >/dev/null 2>&1 &
sleep 1.5
AIRFLOW_DB_PASSWORD="$HAI_AIRFLOW" MARQUEZ_DB_PASSWORD="$HAI_MARQUEZ" psql -X -q -f "$HAI_SQL" >/dev/null 2>&1 &
sleep 2
query="SELECT count(*) FROM pg_stat_activity WHERE query LIKE '%${HAI_AIRFLOW}%' AND pid <> pg_backend_pid()"
seen=$(PGUSER=postgres_exporter psql -X -d postgres -At -c "$query" 2>&1 | head -1)
waiting=$(psql -X -d postgres -At -c "SELECT count(*) FROM pg_locks WHERE NOT granted" 2>&1 | head -1)
echo "seen=$seen"
echo "waiting=$waiting"
wait
echo "done=1"
"""


def session_sql(root: Path) -> str:
    """The SQL that the bootstrap session sends, dedented, from the Terraform file."""
    found = MAIN_BLOCK.search((root / POSTGRES_FILE).read_text(encoding="utf-8"))
    if found is None:
        return ""
    lines = [line[4:] if line.startswith("    ") else line for line in found.group(1).splitlines()]
    start = lines.index(HEADER) + 1 if HEADER in lines else 0
    return "\n".join(line for line in lines[start:] if line != "SQL") + "\n"


def free_port() -> int:
    with socket.socket() as found:
        found.bind(("127.0.0.1", 0))
        return int(found.getsockname()[1])


def probe(sql: str) -> dict[str, str]:
    """What the monitoring role sees while the session's first password statement waits for a lock."""
    with tempfile.TemporaryDirectory(prefix="hai_pg_sql_") as scratch:
        path = Path(scratch) / "session.sql"
        path.write_text(sql, encoding="utf-8")
        env = {
            "PATH": ":".join(SEARCH),
            "LC_ALL": "C",
            "HAI_PORT": str(free_port()),
            "HAI_SQL": str(path),
            "HAI_AIRFLOW": AIRFLOW_PROBE,
            "HAI_MARQUEZ": MARQUEZ_PROBE,
        }
        done = run_command("bash", ["-c", SCRIPT], env=env, timeout=LIMIT_SECONDS)
    return {name: value for name, _, value in (line.partition("=") for line in done.stdout.splitlines()) if name}


def the_session_stops_reporting_its_activity_before_any_password(root: Path) -> dict[str, bool]:
    sql = session_sql(root)
    first_password = sql.find("PASSWORD")
    return {
        "the session SQL is in the Terraform file": "ALTER ROLE airflow PASSWORD" in sql,
        "activity tracking is turned off": OFF in sql,
        "it is turned off before the first \\getenv": 0 <= sql.find(OFF) < sql.find("\\getenv"),
        "it is turned off before the first password statement": 0 <= sql.find(OFF) < first_password,
    }


def the_monitoring_role_cannot_read_a_waiting_password_statement(root: Path) -> dict[str, bool]:
    if any(shutil.which(name, path=":".join(SEARCH)) is None for name in ("initdb", "pg_ctl", "postgres", "psql")):
        return {"PostgreSQL 15 or later is not installed here, so the native check is skipped (the session SQL case above still ran)": True}
    sql = session_sql(root)
    shown = probe(sql)
    planted = probe(sql.replace(OFF + "\n", "", 1))
    return {
        "the native cluster started": "setup" not in shown and "setup" not in planted,
        "the first password statement waited for the lock": shown.get("waiting", "0") == "1" and planted.get("waiting", "0") == "1",
        "the monitoring role sees no password text of the session": shown.get("seen") == "0",
        "the session still finished": shown.get("done") == "1",
        "the planted copy without the statement shows the password, so this case can fail": planted.get("seen") == "1",
    }


CASES: dict[str, Callable[[Path], dict[str, bool]]] = {
    "postgres_the_session_stops_reporting_its_activity_before_any_password": the_session_stops_reporting_its_activity_before_any_password,
    "postgres_the_monitoring_role_cannot_read_a_waiting_password_statement": the_monitoring_role_cannot_read_a_waiting_password_statement,
}
