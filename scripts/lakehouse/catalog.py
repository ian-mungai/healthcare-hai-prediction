"""Start, check and stop the local Apache Polaris Iceberg catalog, and create the project's catalog once.

Run from the repository root:

    .venv/bin/python -m scripts.lakehouse.catalog up
    .venv/bin/python -m scripts.lakehouse.catalog status
    .venv/bin/python -m scripts.lakehouse.catalog job smoke
    .venv/bin/python -m scripts.lakehouse.catalog job bronze_e2e
    .venv/bin/python -m scripts.lakehouse.catalog job bronze -- --group hai
    .venv/bin/python -m scripts.lakehouse.catalog job pins -- pin --run <run_id>
    .venv/bin/python -m scripts.lakehouse.catalog down
    .venv/bin/python -m scripts.lakehouse.catalog release   # after a heavy run: stop, restart Docker Desktop
    scripts/lakehouse/query.sh                      # interactive DuckDB shell, bronze tables attached read-only

This script is a launcher client: it makes no Docker call. The launcher (orchestration/launcher.py) creates the containers from
``docker-compose.yaml`` and the template table. ``up`` is idempotent. On first use it generates the PostgreSQL password and the Polaris
root secret into the ignored, owner-only ``data/lakehouse/secrets/catalog.env`` and never prints them. It asks the launcher to start the
database, bootstrap the Polaris realm once and start Polaris, then creates the ``hai_lakehouse`` catalog whose only allowed
location is ``lakehouse/`` in the project bucket. A catalog that already exists must match that location exactly.
It also creates a read-only principal for the DuckDB viewer once and stores its credentials in the same file; a
principal whose stored secret is missing has its credentials reset, never recreated. ``down`` stops the containers and
keeps the database files, so the tables survive a restart. ``release`` frees the memory Docker's VM keeps after a heavy
run: it stops the catalog and restarts Docker Desktop, but only when nothing of the registry runs and no foreign container runs;
otherwise it changes nothing and names the reason (failure modes 545 to 547). A catalog job runs as an attempt of the template
``spark_read`` (``pins``), ``spark_write`` (the loaders) or ``spark_e2e`` (the throwaway-catalog checks).
"""

from __future__ import annotations

import argparse
import http.client
import json
import os
import secrets
import sys
import urllib.parse
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from orchestration import launcher
from scripts.lakehouse import run_lock

REPO_ROOT = Path(__file__).resolve().parents[2]
# The catalog, its credentials and its database belong to the main checkout, where the registry and the launcher live.
STATE = run_lock.main_checkout(REPO_ROOT) / "data/lakehouse"
SECRETS = STATE / "secrets/catalog.env"
COMPOSE_ENV = STATE / "secrets/compose.env"
BOOTSTRAPPED = STATE / "polaris_bootstrapped"
DEPLOYMENT = REPO_ROOT / "infra/deployment.auto.tfvars.json"
CATALOG = "hai_lakehouse"
READER = "lakehouse_reader"
# Read-only privileges for the viewer; writes fail at the catalog even if a client allowed them (failure mode 56).
READER_PRIVILEGES = ("CATALOG_READ_PROPERTIES", "NAMESPACE_LIST", "NAMESPACE_READ_PROPERTIES", "TABLE_LIST", "TABLE_READ_PROPERTIES", "TABLE_READ_DATA")
POLARIS_HOST = "127.0.0.1"
POLARIS_PORT = 8181
# The test registry a test of this script passes; the real run never sets it.
TEST_REGISTRY: str | None = None
# Job name -> template. The launcher holds the job table (modules, mounts and argument grammar): orchestration/launcher.py SPARK_JOBS.
JOBS = {name: spec["template"] for name, spec in launcher.SPARK_JOBS.items()}


class CatalogError(RuntimeError):
    """A setup step failed; the message never contains a credential."""


def read_env(path: Path) -> dict[str, str]:
    """Read KEY=VALUE lines from an owner-only file."""
    return dict(line.split("=", 1) for line in path.read_text().splitlines() if "=" in line)


def write_private(path: Path, values: dict[str, str]) -> None:
    """Write KEY=VALUE lines readable only by the owner, creating owner-only parent folders."""
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    path.parent.chmod(0o700)
    temporary = path.with_suffix(".tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, "w") as handle:
        handle.write("".join(f"{key}={value}\n" for key, value in values.items()))
    temporary.replace(path)


def credentials() -> dict[str, str]:
    """Return the local catalog credentials, generating them once."""
    if not SECRETS.is_file():
        write_private(
            SECRETS,
            {"POSTGRES_PASSWORD": secrets.token_urlsafe(32), "POLARIS_CLIENT_ID": "root", "POLARIS_CLIENT_SECRET": secrets.token_urlsafe(32)},
        )
    if SECRETS.stat().st_mode & 0o077:
        raise CatalogError(f"{SECRETS.relative_to(REPO_ROOT)} must be readable only by its owner (chmod 600)")
    return read_env(SECRETS)


def deployment() -> dict[str, str]:
    """Return the project's AWS profile, region and bucket from the rendered Terraform variables."""
    values = json.loads(DEPLOYMENT.read_text())
    return {key: str(values[key]) for key in ("aws_profile", "aws_region", "data_bucket_name")}


def system_environment() -> dict[str, str]:
    """Return only the operating-system variables a child process needs, never the caller's project settings."""
    return {name: os.environ[name] for name in ("PATH", "HOME")}


def request(method: str, path: str, token: str | None = None, body: dict[str, Any] | None = None, form: dict[str, str] | None = None) -> tuple[int, Any]:
    """Call the local Polaris API and return the status code and decoded JSON body."""
    headers = {"Accept": "application/json", "Polaris-Realm": "POLARIS"}
    data = None
    if token:
        headers["Authorization"] = f"Bearer {token}"
    if body is not None:
        headers["Content-Type"] = "application/json"
        data = json.dumps(body).encode()
    if form is not None:
        headers["Content-Type"] = "application/x-www-form-urlencoded"
        data = urllib.parse.urlencode(form).encode()
    connection = http.client.HTTPConnection(POLARIS_HOST, POLARIS_PORT, timeout=30)
    try:
        connection.request(method, path, body=data, headers=headers)
        response = connection.getresponse()
        text = response.read().decode()
    finally:
        connection.close()
    if response.status >= 400:
        return response.status, None
    return response.status, json.loads(text) if text else None


def token(secret: dict[str, str]) -> str:
    """Exchange the root client credentials for a short-lived access token."""
    form = {
        "grant_type": "client_credentials",
        "client_id": secret["POLARIS_CLIENT_ID"],
        "client_secret": secret["POLARIS_CLIENT_SECRET"],
        "scope": "PRINCIPAL_ROLE:ALL",
    }
    status, body = request("POST", "/api/catalog/v1/oauth/tokens", form=form)
    if status != 200 or not body or "access_token" not in body:
        raise CatalogError(f"Polaris refused the root credentials (HTTP {status})")
    return str(body["access_token"])


def ensure_catalog(access: str, settings: dict[str, str]) -> str:
    """Create the catalog once; refuse an existing catalog whose location differs."""
    location = f"s3://{settings['data_bucket_name']}/lakehouse/"
    status, body = request("GET", f"/api/management/v1/catalogs/{CATALOG}", access)
    if status == 200:
        storage = body["storageConfigInfo"]
        if body["properties"].get("default-base-location") != location or storage.get("allowedLocations") != [location] or not storage.get("stsUnavailable"):
            raise CatalogError(f"catalog {CATALOG} exists with a different location or credential mode; resolve it before loading")
        outcome = "already present"
    elif status == 404:
        catalog = {
            "name": CATALOG,
            "type": "INTERNAL",
            "readOnly": False,
            "properties": {"default-base-location": location},
            "storageConfigInfo": {"storageType": "S3", "allowedLocations": [location], "region": settings["aws_region"], "stsUnavailable": True},
        }
        status, _ = request("POST", "/api/management/v1/catalogs", access, body={"catalog": catalog})
        if status not in (200, 201):
            raise CatalogError(f"creating catalog {CATALOG} failed (HTTP {status})")
        outcome = "created"
    else:
        raise CatalogError(f"reading catalog {CATALOG} failed (HTTP {status})")
    grant = {"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}}
    status, _ = request("PUT", f"/api/management/v1/catalogs/{CATALOG}/catalog-roles/catalog_admin/grants", access, body=grant)
    if status not in (200, 201):
        raise CatalogError(f"granting table management on {CATALOG} failed (HTTP {status})")
    return outcome


def ensure_ok(status: int, action: str, allowed: tuple[int, ...] = (200, 201, 204)) -> None:
    """Raise with the HTTP status only when a management call fails."""
    if status not in allowed:
        raise CatalogError(f"{action} failed (HTTP {status})")


def ensure_reader(access: str, secret: dict[str, str]) -> str:
    """Create the read-only viewer principal and its roles once; return how its credentials were obtained."""
    status, _ = request("POST", "/api/management/v1/principal-roles", access, body={"principalRole": {"name": READER}})
    ensure_ok(status, "creating the reader principal role", (200, 201, 409))
    status, _ = request("POST", f"/api/management/v1/catalogs/{CATALOG}/catalog-roles", access, body={"catalogRole": {"name": READER}})
    ensure_ok(status, "creating the reader catalog role", (200, 201, 409))
    for privilege in READER_PRIVILEGES:
        grant = {"grant": {"type": "catalog", "privilege": privilege}}
        status, _ = request("PUT", f"/api/management/v1/catalogs/{CATALOG}/catalog-roles/{READER}/grants", access, body=grant)
        ensure_ok(status, f"granting {privilege} to the reader")
    status, _ = request("PUT", f"/api/management/v1/principal-roles/{READER}/catalog-roles/{CATALOG}", access, body={"catalogRole": {"name": READER}})
    ensure_ok(status, "assigning the reader catalog role")
    status, _ = request("GET", f"/api/management/v1/principals/{READER}", access)
    if status == 404:
        status, body = request("POST", "/api/management/v1/principals", access, body={"principal": {"name": READER}, "credentialRotationRequired": False})
        ensure_ok(status, "creating the reader principal")
        outcome = "created"
    elif status == 200 and secret.get("POLARIS_READER_CLIENT_SECRET"):
        body, outcome = None, "already present"
    else:
        ensure_ok(status, "reading the reader principal")
        status, body = request("POST", f"/api/management/v1/principals/{READER}/reset", access, body={})
        ensure_ok(status, "resetting the reader credentials")
        outcome = "credentials reset"
    if body is not None:
        issued = body["credentials"]
        write_private(SECRETS, {**secret, "POLARIS_READER_CLIENT_ID": issued["clientId"], "POLARIS_READER_CLIENT_SECRET": issued["clientSecret"]})
    status, _ = request("PUT", f"/api/management/v1/principals/{READER}/principal-roles", access, body={"principalRole": {"name": READER}})
    ensure_ok(status, "assigning the reader principal role")
    return outcome


def write_compose_env(settings: dict[str, str]) -> None:
    """Write the env file the launcher reads by name: the owner-only credentials and the deployment settings. The service memory limits
    come from the launcher's own plan, not from this file."""
    write_private(COMPOSE_ENV, {**credentials(), "AWS_PROFILE": settings["aws_profile"], "AWS_REGION": settings["aws_region"]})


def client() -> launcher.LauncherClient:
    """A client of the launcher for the real registry (or the test registry of a test)."""
    return launcher.load_client(TEST_REGISTRY)


def up() -> None:
    """Start the catalog services through the launcher and make sure the project's catalog and its read-only viewer principal exist."""
    secret, settings = credentials(), deployment()
    write_compose_env(settings)
    (STATE / "polaris_db").mkdir(mode=0o700, parents=True, exist_ok=True)
    try:
        link = client()
        link.call("catalog_up")
        record = link.wait_for("catalog", "", "phase", ("up", "down"), {"kind": "any"}, 900)
    except launcher.ClientError as error:
        raise CatalogError(f"the launcher refused the catalog start: {error}") from error
    if record.get("phase") != "up":
        raise CatalogError(f"the catalog did not start: {record.get('error') or record.get('phase')}")
    access = token(secret)
    outcome = ensure_catalog(access, settings)
    reader = ensure_reader(access, secret)
    write_compose_env(settings)
    sys.stdout.write(
        f"Polaris is running on 127.0.0.1:8181; catalog {CATALOG} {outcome}, limited to lakehouse/ in the project bucket; read-only principal {reader}.\n"
    )


def status() -> None:
    """Report the catalog's state without printing credentials."""
    try:
        record = client().call("status", {"what": "catalog"})
    except launcher.ClientError as error:
        raise CatalogError(str(error)) from error
    services = ", ".join(f"{name}={attempt}" for name, attempt in sorted((record.get("services") or {}).items())) or "none"
    sys.stdout.write(f"catalog {record.get('phase', 'down')}; services: {services}\n")


def job(name: str, job_args: list[str]) -> None:
    """Run one lakehouse job module as an attempt of its template, attached to the catalog when it needs one."""
    if name not in JOBS:
        raise CatalogError(f"unknown job {name!r}; choose one of {', '.join(sorted(JOBS))}")
    template = JOBS[name]
    if template != "spark_e2e":
        up()
    elif not COMPOSE_ENV.is_file():
        write_compose_env(deployment())
    link = client()
    kind = link.config.registry.kind
    try:
        plan = link.call("plan")
        budget = plan["budget"]
        sys.stdout.write(
            f"Spark heap {plan['environment']['SPARK_JOB_MEMORY']}: Docker {budget['total'] / 1024**3:.1f} GiB, running containers "
            f"{budget['used_by_containers'] / 1024**3:.1f} GiB, headroom {budget['headroom'] / 1024**3:.0f} GiB, "
            f"Mac {'not capping' if budget['mac_available'] is None else f'{budget["mac_available"] / 1024**3:.1f} GiB available'}, "
            f"container {int(plan['environment']['JOB_MEMORY_LIMIT']) / 1024**3:.1f} GiB, workers {plan['environment']['JOB_THREADS']}\n"
        )
        result = run_job(link, kind, name, template, job_args)
    except launcher.ClientError as error:
        raise CatalogError(f"the launcher refused job {name}: {error}") from error
    sys.stdout.write(result.stdout)
    sys.stderr.write(result.stderr)
    if not result.ok:
        raise CatalogError(f"job {name} ended {result.reason} with exit code {result.exit_code}")


def run_job(link: launcher.LauncherClient, kind: str, name: str, template: str, job_args: list[str]) -> launcher.AttemptResult:
    """Pick the owner the template needs, run the attempt and give the owner back."""
    launch = f"job-{name}-{uuid.uuid4().hex[:8]}"
    if template == "spark_e2e":
        allocation = link.allocate()
        try:
            return link.run_attempt(link.principal_alloc(allocation), template, launch, job_args, {"job": name})
        finally:
            link.release_allocation(allocation)
    if name == "pins":
        run_id = job_args[job_args.index("--run") + 1] if "--run" in job_args else ""
        return link.run_attempt(link.principal_run(run_id), template, launch, job_args, {"job": name})
    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
    link.call(
        "acquire",
        {
            "domain": "acquisition",
            "run_id": run_id,
            "mode": "fixture" if kind == "test" else "real",
            "write_prefixes": [str(launcher.SPARK_JOBS[name].get("prefix") or "lakehouse/")],
            "holder": "manual",
        },
    )
    principal = link.principal_run(run_id)
    try:
        return link.run_attempt(principal, template, launch, job_args, {"job": name})
    finally:
        record = link.config.runs_root / run_id / "job_record.json"
        record.parent.mkdir(parents=True, exist_ok=True)
        record.write_text(json.dumps({"run_id": run_id}))
        link.call("finalize", {}, principal)
        link.call("release", {"record": str(record)}, principal, timeout=300)


def down() -> None:
    """Stop the catalog containers and keep the catalog database."""
    try:
        link = client()
        link.call("catalog_down")
        link.wait_for("catalog", "", "phase", ("down",), {"kind": "any"}, 300)
    except launcher.ClientError as error:
        raise CatalogError(f"the launcher refused the catalog stop: {error}") from error
    sys.stdout.write("Catalog services stopped; the database in data/lakehouse/polaris_db is kept.\n")


def release() -> str:
    """Restart Docker Desktop to free the VM's memory when nothing of this registry and nothing foreign runs; a refusal raises [545] to [547]."""
    try:
        link = client()
        if (link.call("status", {"what": "catalog"}) or {}).get("phase") in ("up", "starting"):
            down()
        link.call("docker_restart", {"owner": True}, {"kind": "any", "owner": True}, timeout=900)
    except launcher.ClientError as error:
        raise CatalogError(f"Docker not restarted: {error}") from error
    return "Docker restarted; the catalog is stopped."


def main() -> int:
    """Parse the command and report failures without credentials."""
    global TEST_REGISTRY
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--test-registry", help="a test registry folder (for tests of this script only)")
    parser.add_argument("command", choices=["up", "status", "job", "down", "release"])
    parser.add_argument("job_name", nargs="?", help="job to run with the job command")
    parser.add_argument("job_args", nargs=argparse.REMAINDER, help="arguments passed to the job after --")
    args = parser.parse_args()
    TEST_REGISTRY = args.test_registry
    try:
        if args.command == "job":
            job(args.job_name or "", [value for value in args.job_args if value != "--"])
        elif args.command == "release":
            sys.stdout.write(release() + "\n")
        else:
            {"up": up, "status": status, "down": down}[args.command]()
    except (CatalogError, OSError, KeyError, json.JSONDecodeError, run_lock.Refused, run_lock.RegistryError) as error:
        sys.stderr.write(f"lakehouse catalog: {error}\n")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
