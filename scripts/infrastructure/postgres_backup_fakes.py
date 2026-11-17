"""Stand-ins for the Postgres backup and restore cases: an S3 client with write-once keys and fake ``pg_dump``, ``pg_restore`` and ``psql`` programs.

The fake programs are small Python files that read and write one JSON "server" file. They answer only the statements the reviewed scripts in
``infra/services/scripts`` send, so a changed statement fails a case by name. A dump of the fake server is the five bytes ``PGDMP`` followed by JSON.
"""

from __future__ import annotations

import io
import json
import re
import stat
from pathlib import Path
from typing import Any

from botocore.exceptions import ClientError

BUCKET = "example-project-data-dev"
MAGIC = b"PGDMP"

PG_DUMP = """#!/usr/bin/env python3
import json, os, sys, time
state = json.load(open(os.environ["FAKE_SERVER"]))
args = sys.argv[1:]
database = next(a.split("=", 1)[1] for a in args if a.startswith("--dbname="))
target = next(a.split("=", 1)[1] for a in args if a.startswith("--file="))
if "--format=custom" not in args:
    sys.exit("pg_dump: the fake serves only the custom format")
if os.environ.get("FAKE_DUMP_SLEEP"):
    time.sleep(float(os.environ["FAKE_DUMP_SLEEP"]))
if database in os.environ.get("FAKE_FAIL_DUMP", "").split():
    sys.exit(f'pg_dump: error: connection to database "{database}" failed')
if database not in state["databases"]:
    sys.exit(f'pg_dump: error: database "{database}" does not exist')
body = json.dumps({"tables": state["databases"][database]["tables"]}).encode()
if database in os.environ.get("FAKE_TRUNCATE_DUMP", "").split():
    body = body[: len(body) // 2]
open(target, "wb").write(b"PGDMP" + body)
"""

PG_RESTORE = """#!/usr/bin/env python3
import json, os, sys
args = sys.argv[1:]
path = args[-1]
raw = open(path, "rb").read()
def tables():
    if not raw.startswith(b"PGDMP"):
        sys.exit("pg_restore: error: input file does not appear to be a valid archive")
    try:
        return json.loads(raw[5:])["tables"]
    except ValueError:
        sys.exit("pg_restore: error: unexpected end of file")
if "--list" in args:
    for index in range(tables()):
        print(f"{200 + index}; 1259 {16400 + index} TABLE public t{index} owner")
        print(f"{300 + index}; 0 {16400 + index} TABLE DATA public t{index} owner")
    print("; Archive created at fake")
    sys.exit(0)
if any(a.startswith("--file=") for a in args):
    tables()
    sys.exit(0)
database = next(a.split("=", 1)[1] for a in args if a.startswith("--dbname="))
for flag in ("--single-transaction", "--exit-on-error", "--no-password"):
    if flag not in args:
        sys.exit(f"pg_restore: the fake needs {flag}")
state = json.load(open(os.environ["FAKE_SERVER"]))
with open(os.environ["FAKE_LOG"], "a") as log:
    log.write(f"pg_restore {database}\\n")
if database.removesuffix("__restore") in os.environ.get("FAKE_FAIL_RESTORE", "").split():
    sys.exit(f'pg_restore: error: could not execute query in "{database}"')
if database not in state["databases"]:
    sys.exit(f'pg_restore: error: database "{database}" does not exist')
count = tables()
if database.removesuffix("__restore") in os.environ.get("FAKE_SHORT_RESTORE", "").split():
    count -= 1
state["databases"][database]["tables"] = count
json.dump(state, open(os.environ["FAKE_SERVER"], "w"))
"""

PSQL = """#!/usr/bin/env python3
import json, os, re, sys
args = sys.argv[1:]
state = json.load(open(os.environ["FAKE_SERVER"]))
log = open(os.environ["FAKE_LOG"], "a")
def save():
    json.dump(state, open(os.environ["FAKE_SERVER"], "w"))
database = os.environ.get("PGDATABASE", "polaris")
statement = None
index = 0
while index < len(args):
    if args[index] == "-d":
        database = args[index + 1]
        index += 1
    elif args[index] == "-c":
        statement = args[index + 1]
        index += 1
    index += 1
if statement is None:
    script = sys.stdin.read()
    if "\\\\getenv" in script and "PASSWORD" in script:
        log.write("psql roles\\n")
        for role in re.findall(r"CREATE ROLE (\\w+) LOGIN", script):
            if role not in state["roles"]:
                state["roles"].append(role)
        save()
        sys.exit(0)
    sys.exit("psql: the fake read an unknown script")
log.write(f"psql {database}: {statement}\\n")
def name_of(text, pattern):
    return re.search(pattern, text).group(1)
if statement.startswith("SELECT 1 FROM pg_database"):
    print(1 if name_of(statement, r"datname = '([^']+)'") in state["databases"] else "")
elif "FROM pg_class" in statement:
    if database not in state["databases"]:
        sys.exit(f'psql: error: database "{database}" does not exist')
    print(state["databases"][database]["tables"] + state["databases"][database].get("other", 0))
elif statement.startswith("DROP DATABASE IF EXISTS"):
    state["databases"].pop(name_of(statement, r'"([^"]+)"'), None)
    save()
elif statement.startswith("CREATE DATABASE"):
    new = name_of(statement, r'CREATE DATABASE "([^"]+)"')
    if new in state["databases"]:
        sys.exit(f'psql: error: database "{new}" already exists')
    state["databases"][new] = {"tables": 0}
    save()
elif statement.startswith("ALTER DATABASE"):
    old, new = re.findall(r'"([^"]+)"', statement)
    if new in state["databases"] or old not in state["databases"]:
        sys.exit("psql: error: cannot rename")
    state["databases"][new] = state["databases"].pop(old)
    save()
elif statement.startswith("DROP SCHEMA IF EXISTS") or statement.startswith("CREATE SCHEMA IF NOT EXISTS"):
    pass
else:
    sys.exit(f"psql: the fake does not know: {statement}")
"""


class StandInS3:
    """An S3 client with the calls of the backup scripts: a key may be created once under ``IfNoneMatch="*"``, and a call can be made to fail."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.metadata: dict[str, dict[str, str]] = {}
        self.puts: list[dict[str, Any]] = []
        self.gets: list[str] = []
        self.heads = 0
        self.fail_put_after: int | None = None
        self.fail_get = False
        self.fail_list = False

    @staticmethod
    def error(code: str, operation: str) -> ClientError:
        return ClientError({"Error": {"Code": code, "Message": "stand-in"}}, operation)

    def put_object(self, **arguments: Any) -> dict[str, Any]:
        if self.fail_put_after is not None and len(self.puts) >= self.fail_put_after:
            raise ConnectionError("stand-in connection lost")
        key, body = arguments["Key"], arguments["Body"]
        data = body.read() if hasattr(body, "read") else bytes(body)
        self.puts.append({"key": key, "if_none_match": arguments.get("IfNoneMatch"), "bucket": arguments["Bucket"]})
        if arguments.get("IfNoneMatch") == "*" and key in self.objects:
            raise self.error("PreconditionFailed", "PutObject")
        self.objects[key] = data
        self.metadata[key] = dict(arguments.get("Metadata", {}))
        return {}

    def head_object(self, **arguments: Any) -> dict[str, Any]:
        self.heads += 1
        key = arguments["Key"]
        if key not in self.objects:
            raise self.error("404", "HeadObject")
        return {"ContentLength": len(self.objects[key]), "Metadata": self.metadata[key]}

    def get_object(self, **arguments: Any) -> dict[str, Any]:
        key = arguments["Key"]
        self.gets.append(key)
        if self.fail_get:
            raise ConnectionError("stand-in connection lost")
        if key not in self.objects:
            raise self.error("NoSuchKey", "GetObject")
        return {"Body": io.BytesIO(self.objects[key]), "ContentLength": len(self.objects[key])}

    def list_objects_v2(self, **arguments: Any) -> dict[str, Any]:
        if self.fail_list:
            raise ConnectionError("stand-in connection lost")
        prefix = arguments["Prefix"]
        return {"Contents": [{"Key": key, "Size": len(data)} for key, data in sorted(self.objects.items()) if key.startswith(prefix)], "IsTruncated": False}


def fake_dump(tables: int) -> bytes:
    """The bytes the fake ``pg_dump`` writes for a database of ``tables`` tables."""
    return MAGIC + json.dumps({"tables": tables}).encode()


def install_fakes(folder: Path) -> Path:
    """Write the three fake programs into ``folder/bin`` and return that folder."""
    bin_folder = folder / "bin"
    bin_folder.mkdir(parents=True, exist_ok=True)
    for name, text in (("pg_dump", PG_DUMP), ("pg_restore", PG_RESTORE), ("psql", PSQL)):
        path = bin_folder / name
        path.write_text(text, encoding="utf-8")
        path.chmod(path.stat().st_mode | stat.S_IXUSR)
    return bin_folder


def server(folder: Path, databases: dict[str, int]) -> Path:
    """The fake server file: each name in ``databases`` exists with that many tables."""
    path = folder / "server.json"
    path.write_text(json.dumps({"databases": {name: {"tables": count} for name, count in databases.items()}, "roles": []}), encoding="utf-8")
    return path


def server_state(path: Path) -> dict[str, Any]:
    """The fake server as saved."""
    return json.loads(path.read_text(encoding="utf-8"))


def log_lines(path: Path) -> list[str]:
    """The statements the fake programs logged, in order."""
    return path.read_text(encoding="utf-8").splitlines() if path.exists() else []


SAFE_NAME = re.compile(r"[a-z][a-z0-9_]*")
