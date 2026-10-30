"""Start the MLflow server with basic authentication (the one process the MLflow container runs).

    python -I /opt/mlflow/start_server.py <mlflow server options>

Every service of the host shares the host network, so a loopback listener is reachable from every other container. The server therefore asks for a login on
every request and grants nothing by default: ``default_permission`` is ``NO_PERMISSIONS``. The administrator user is created from the Parameter Store
password that the task injects as MLFLOW_ADMIN_PASSWORD. The training jobs and the owner hold the same password. The authentication settings live in a file
that only this user can read. The file is written to the temporary file system of this container, and the server process does not keep the password in its
environment. MLflow reads ``admin_password`` only when it creates the administrator, so a changed parameter does not change an existing user.
"""

from __future__ import annotations

import importlib
import os
import re
import sys
import tempfile
from collections.abc import Callable, MutableMapping, Sequence
from pathlib import Path

FOLDER = Path(tempfile.gettempdir())
FILE = "mlflow_auth.ini"
PASSWORD = re.compile(r"[A-Za-z0-9_-]{16,128}")
USER = re.compile(r"[a-z][a-z0-9_]{2,31}")
DATABASE = re.compile(r"postgresql\+psycopg2://[a-z_]+@127\.0\.0\.1:5432/[a-z_]+")
AUTHORIZATION = "mlflow.server.auth:authenticate_request_basic_auth"


class StartRefused(Exception):
    """The server cannot start safely."""


def setting(environ: MutableMapping[str, str], name: str, pattern: re.Pattern[str], default: str | None = None) -> str:
    """Return a variable that has the expected form."""
    value = environ.get(name, default or "")
    if not pattern.fullmatch(value):
        raise StartRefused(f"{name} is not set or has another form")
    return value


def write_private(path: Path, text: str) -> None:
    """Write the file for its owner alone, replacing an earlier start's file."""
    partial = path.with_name(f".{path.name}.partial")
    descriptor = os.open(partial, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, "w") as handle:
        handle.write(text)
    os.replace(partial, path)


def launch(arguments: Sequence[str]) -> None:
    """Run the MLflow command line in this process, so the container's init process signals the server directly."""
    importlib.import_module("mlflow.cli").cli.main(args=list(arguments), standalone_mode=False)


def main(argv: Sequence[str], environ: MutableMapping[str, str] | None = None, run: Callable[[Sequence[str]], object] = launch, folder: Path = FOLDER) -> int:
    """Write the authentication settings and start ``mlflow server`` with them, or refuse and report the cause."""
    environ = os.environ if environ is None else environ
    try:
        if any(argument == "--app-name" or argument.startswith("--app-name=") for argument in argv):
            raise StartRefused("the server options must not contain --app-name. This script starts the basic-auth app")
        password = setting(environ, "MLFLOW_ADMIN_PASSWORD", PASSWORD)
        user = setting(environ, "MLFLOW_ADMIN_USERNAME", USER, "admin")
        database = setting(environ, "MLFLOW_AUTH_DATABASE_URI", DATABASE)
        path = folder / FILE
        lines = [
            "[mlflow]",
            "default_permission = NO_PERMISSIONS",
            f"database_uri = {database}",
            f"authorization_function = {AUTHORIZATION}",
            f"admin_username = {user}",
            f"admin_password = {password}",
        ]
        write_private(path, "\n".join(lines) + "\n")
    except (StartRefused, OSError) as refusal:
        sys.stderr.write(f"mlflow start refused: {refusal}\n")
        return 1
    del environ["MLFLOW_ADMIN_PASSWORD"]
    environ["MLFLOW_AUTH_CONFIG_PATH"] = str(path)
    run(["server", "--app-name", "basic-auth", *argv])
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
