"""Start, check and stop the local Apache Polaris Iceberg catalog, and create the project's catalog once.

Run from the repository root:

    .venv/bin/python -m scripts.lakehouse.catalog up
    .venv/bin/python -m scripts.lakehouse.catalog status
    .venv/bin/python -m scripts.lakehouse.catalog job smoke
    .venv/bin/python -m scripts.lakehouse.catalog job bronze_e2e
    .venv/bin/python -m scripts.lakehouse.catalog job bronze -- --group hai
    .venv/bin/python -m scripts.lakehouse.catalog down
    scripts/lakehouse/query.sh                      # interactive DuckDB shell, bronze tables attached read-only

``up`` is idempotent. On first use it generates the PostgreSQL password and the Polaris root secret into the ignored,
owner-only ``data/lakehouse/secrets/catalog.env`` (owner decision, Oct 2 2026) and never prints them. It starts the
database, bootstraps the Polaris realm once, starts Polaris and creates the ``hai_lakehouse`` catalog whose only allowed
location is ``lakehouse/`` in the project bucket. A catalog that already exists must match that location exactly.
It also creates a read-only principal for the DuckDB viewer once and stores its credentials in the same file; a
principal whose stored secret is missing has its credentials reset, never recreated. ``down`` stops the containers and
keeps the database files, so the tables survive a restart.
"""

from __future__ import annotations

import argparse
import http.client
import json
import os
import secrets
import sys
import urllib.parse
from pathlib import Path
from typing import Any

from scripts.lakehouse import memory_budget
from scripts.process import run_command

REPO_ROOT = Path(__file__).resolve().parents[2]
STATE = REPO_ROOT / "data/lakehouse"
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
COMPOSE_TIMEOUT = 3600
# Docker needs the operating system's search path and home folder; no project setting is passed to it.
SYSTEM_VARIABLES = ("PATH", "HOME")
# Job name -> (module, needs the running Polaris catalog). Synthetic end-to-end tests use a throwaway local catalog.
JOBS = {
    "smoke": ("scripts.lakehouse.smoke", True),
    "bronze_e2e": ("scripts.lakehouse.run_bronze_e2e", False),
    "bronze": ("scripts.lakehouse.bronze", True),
    "dictionary": ("scripts.lakehouse.dictionary", True),
    "checksums": ("scripts.lakehouse.checksums", True),
}


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
    """Return only the operating-system variables docker compose needs, never the caller's project settings."""
    return {name: os.environ[name] for name in SYSTEM_VARIABLES}


def compose(*args: str, env: dict[str, str]) -> str:
    """Run docker compose with the private env file, never the repository's .env."""
    command = ["compose", "--project-directory", str(REPO_ROOT), "-f", str(REPO_ROOT / "docker-compose.yaml"), "--env-file", str(COMPOSE_ENV), *args]
    result = run_command("docker", command, cwd=REPO_ROOT, env=env, timeout=COMPOSE_TIMEOUT)
    if result.returncode:
        # A job's own report goes to stdout; show it before failing. Jobs print counts only, never credentials.
        sys.stdout.write(result.stdout)
        detail = [line for line in result.stderr.strip().splitlines() if "WARN" not in line and "Stage" not in line][-1:] or ["no output"]
        raise CatalogError(f"docker compose {' '.join(args[:2])} failed: {detail[0]}")
    return result.stdout


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
    """Write the env file docker compose reads, from the owner-only credentials and the deployment settings."""
    write_private(COMPOSE_ENV, {**credentials(), "AWS_PROFILE": settings["aws_profile"], "AWS_REGION": settings["aws_region"]})


def up() -> None:
    """Start the catalog services and make sure the project's catalog and its read-only viewer principal exist."""
    secret, settings = credentials(), deployment()
    write_compose_env(settings)
    (STATE / "polaris_db").mkdir(mode=0o700, parents=True, exist_ok=True)
    env = system_environment()
    compose("up", "--detach", "--wait", "polaris-db", env=env)
    if not BOOTSTRAPPED.exists():
        compose("--profile", "setup", "run", "--rm", "polaris-bootstrap", env=env)
        BOOTSTRAPPED.write_text("Polaris realm POLARIS bootstrapped; delete only together with data/lakehouse/polaris_db.\n")
    compose("up", "--detach", "--wait", "polaris", env=env)
    access = token(secret)
    outcome = ensure_catalog(access, settings)
    reader = ensure_reader(access, secret)
    write_compose_env(settings)
    sys.stdout.write(
        f"Polaris is running on 127.0.0.1:8181; catalog {CATALOG} {outcome}, limited to lakehouse/ in the project bucket; read-only principal {reader}.\n"
    )


def status() -> None:
    """Report the containers' state without printing credentials."""
    env = system_environment()
    if not COMPOSE_ENV.is_file():
        sys.stdout.write("Not set up yet; run the up command.\n")
        return
    sys.stdout.write(compose("ps", "--format", "{{.Service}}\t{{.State}}\t{{.Health}}", env=env))


def job(name: str, job_args: list[str]) -> None:
    """Run one lakehouse job module inside the Spark container, attached to the catalog when it needs one."""
    if name not in JOBS:
        raise CatalogError(f"unknown job {name!r}; choose one of {', '.join(sorted(JOBS))}")
    module, needs_catalog = JOBS[name]
    env = system_environment()
    if needs_catalog:
        up()
    elif not COMPOSE_ENV.is_file():
        write_compose_env(deployment())
    isolation = [] if needs_catalog else ["--no-deps"]
    # Computed now, after Polaris is up, from every running container; the container has no default [486] [489] [490].
    try:
        budget = memory_budget.current()
    except memory_budget.BudgetError as error:
        raise CatalogError(f"no Spark heap: {error}") from error
    sys.stdout.write(
        f"Spark heap {budget.spark_setting}: Docker {budget.total / memory_budget.GIB:.1f} GiB, running containers "
        f"{budget.used_by_containers / memory_budget.GIB:.1f} GiB, headroom {budget.headroom / memory_budget.GIB:.0f} GiB, "
        f"Mac {'not capping' if budget.mac_available is None else f'{budget.mac_available / memory_budget.GIB:.1f} GiB available'}\n"
    )
    env = {**env, "SPARK_JOB_MEMORY": budget.spark_setting}
    sys.stdout.write(compose("--profile", "job", "run", "--rm", *isolation, "spark", "python", "-m", module, *job_args, env=env))


def down() -> None:
    """Stop the containers and keep the catalog database."""
    env = system_environment()
    if COMPOSE_ENV.is_file():
        compose("--profile", "setup", "--profile", "job", "--profile", "query", "down", env=env)
    sys.stdout.write("Catalog services stopped; the database in data/lakehouse/polaris_db is kept.\n")


def main() -> int:
    """Parse the command and report failures without credentials."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("command", choices=["up", "status", "job", "down"])
    parser.add_argument("job_name", nargs="?", help="job to run with the job command")
    parser.add_argument("job_args", nargs=argparse.REMAINDER, help="arguments passed to the job after --")
    args = parser.parse_args()
    try:
        if args.command == "job":
            job(args.job_name or "", [value for value in args.job_args if value != "--"])
        else:
            {"up": up, "status": status, "down": down}[args.command]()
    except (CatalogError, OSError, KeyError, json.JSONDecodeError) as error:
        sys.stderr.write(f"lakehouse catalog: {error}\n")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
