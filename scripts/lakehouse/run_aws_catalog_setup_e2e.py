"""Check the AWS catalog setup with stand-ins only: a loopback Polaris, the loopback Parameter Store stand-in and the real owned relay.

Run from the repository root: ``.venv/bin/python -m scripts.lakehouse.run_aws_catalog_setup_e2e --out <NEW_DIRECTORY> [--mutations]``. No AWS
service is contacted and no container starts. Each case states one failure mode of the setup and its required behavior. ``--mutations`` plants
each named defect in the setup module in turn and requires its intended case to fail. The report holds names and outcomes only, never a value.
"""

from __future__ import annotations

import argparse
import contextlib
import io
import json
import re
import secrets
import shutil
import sys
import tempfile
import threading
import time
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from unittest import mock

from orchestration.catalog_transport import CatalogTransport, TransportSpec
from scripts.infrastructure import secret_store
from scripts.infrastructure.secret_store_stand_in import CIPHERTEXT_MARKER, StandIn
from scripts.lakehouse import aws_catalog_setup as setup

REPO_ROOT = Path(__file__).resolve().parents[2]
BUCKET = "example-lakehouse-bucket"
REGION = "us-west-2"
RUN_SECONDS = 120.0
CASE_SECONDS = 60.0
STALL_SECONDS = 3.0
# The Mac Polaris port, written out so a changed constant in the setup module cannot hide a defect.
MAC_PORT = 8181


class Polaris:
    """A loopback stand-in for the Polaris management and token endpoints, with the faults the cases plant."""

    def __init__(self, root_secret: str) -> None:
        self.root_secret = root_secret
        self.tokens: set[str] = set()
        self.catalog: dict[str, Any] | None = None
        self.calls: list[tuple[str, str]] = []
        self.faults: set[str] = set()
        self.lock = threading.Lock()
        stand_in = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, format: str, *args: Any) -> None:
                return

            def reply(self, status: int, body: Any = None) -> None:
                data = json.dumps(body).encode() if body is not None else b""
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def handle_any(self) -> None:
                length = int(self.headers.get("Content-Length") or 0)
                raw = self.rfile.read(length) if length else b""
                with stand_in.lock:
                    stand_in.calls.append((self.command, self.path))
                status, body, drop = stand_in.answer(self.command, self.path, self.headers, raw)
                if drop:
                    self.close_connection = True
                    return
                self.reply(status, body)

            do_GET = do_POST = do_PUT = handle_any

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    @property
    def port(self) -> int:
        return int(self.server.server_address[1])

    def count(self, method: str, path: str) -> int:
        with self.lock:
            return sum(1 for call in self.calls if call == (method, path))

    def answer(self, method: str, path: str, headers: Any, raw: bytes) -> tuple[int, Any, bool]:
        if headers.get("Polaris-Realm") != "POLARIS":
            return 400, None, False
        if method == "POST" and path == setup.OAUTH_PATH:
            form = dict(item.split("=", 1) for item in raw.decode().split("&")) if raw else {}
            if form.get("client_id") != "root" or "root_refused" in self.faults:
                return 401, None, False
            if form.get("client_secret") != self.root_secret:
                return 401, None, False
            token = secrets.token_hex(16)
            self.tokens.add(token)
            return 200, {"access_token": token, "token_type": "bearer"}, False
        if headers.get("Authorization", "").removeprefix("Bearer ") not in self.tokens:
            return 401, None, False
        if "stall" in self.faults:
            time.sleep(STALL_SECONDS)
        if path == setup.CATALOG_PATH and method == "GET":
            return (200, self.catalog, False) if self.catalog is not None else (404, None, False)
        if path == "/api/management/v1/catalogs" and method == "POST":
            body = json.loads(raw)["catalog"]
            if "conflict" in self.faults:
                self.catalog = body
                return 409, None, False
            if self.catalog is not None:
                return 409, None, False
            self.catalog = body
            return 201, body, "lose_create_reply" in self.faults
        if path == f"{setup.CATALOG_PATH}/catalog-roles/catalog_admin/grants" and method == "PUT":
            return (201, None, False) if self.catalog is not None else (404, None, False)
        return 404, None, False

    def __enter__(self) -> Polaris:
        self.thread.start()
        return self

    def __exit__(self, *_: object) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=10)


class Services:
    """Stand-in services: the stand-in Parameter Store as the project profile's vault and the stand-in Polaris as the AWS catalog."""

    bucket = BUCKET
    region = REGION

    def __init__(self, ssm: StandIn, polaris: Polaris, execute: setup.Execute = setup.direct, relay: Path | None = None) -> None:
        self.ssm, self.polaris, self.execute, self.relay = ssm, polaris, execute, relay
        self.decryption: list[bool] = []
        self.closed: list[bool] = []

    def vault(self) -> setup.PairVault:
        parameters = secret_store.open_store(REPO_ROOT, self.ssm.endpoint).parameters
        real = parameters.get_parameter
        seen = self.decryption

        def spy(**kwargs: Any) -> Any:
            seen.append(bool(kwargs.get("WithDecryption")))
            return real(**kwargs)

        parameters.get_parameter = spy
        return setup.PairVault(parameters)

    @contextlib.contextmanager
    def remote(self, instance: str, *, deadline: float) -> Iterator[setup.Remote]:
        if self.relay is None:
            yield setup.Remote(self.polaris.port, self.execute)
            return
        transport = CatalogTransport(self.relay)
        try:
            endpoint = transport.start_relay(self.polaris.port, deadline=deadline)
            route = setup.Route(transport, endpoint, deadline=deadline)
            yield setup.Remote(route.port, route.execute)
        finally:
            self.closed.append(transport.close(deadline=time.monotonic() + 30))


@contextlib.contextmanager
def world(seeded: dict[str, str] | None = None, faults: set[str] | None = None) -> Iterator[tuple[StandIn, Polaris, str]]:
    root = "root-" + secrets.token_hex(12)
    with StandIn({secret_store.POLARIS_ROOT_SECRET: root, **(seeded or {})}) as ssm, Polaris(root) as polaris:
        polaris.faults |= faults or set()
        yield ssm, polaris, root


def run_main(argv: list[str], services: Any) -> tuple[int, str, str]:
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        code = setup.main(argv, services)
    return code, out.getvalue(), err.getvalue()


SECRETS_SEEN: list[str] = []
OUTPUTS: list[str] = []


def call(argv: list[str], services: Any, root: str) -> tuple[int, str, str]:
    SECRETS_SEEN.append(root)
    code, out, err = run_main(["--seconds", str(RUN_SECONDS), *argv], services)
    OUTPUTS.append(out + err)
    return code, out, err


def outcome(out: str) -> Any:
    return json.loads(out) if out.strip() else None


Case = Callable[[Path], dict[str, bool]]


def case_catalog_created(_: Path) -> dict[str, bool]:
    """P10: an absent AWS catalog is created once with the Mac body on the project bucket, and table management is granted to catalog_admin."""
    with world() as (ssm, polaris, root):
        code, out, _err = call(["catalog", "--instance", "i-0123456789abcdef0"], Services(ssm, polaris), root)
        return {
            "the step succeeds": code == 0,
            "the outcome is created": (outcome(out) or {}).get("outcome") == "created",
            "one create was sent": polaris.count("POST", "/api/management/v1/catalogs") == 1,
            "the body is the Mac catalog body on the project bucket": polaris.catalog == setup.catalog_body(BUCKET, REGION),
            "table management was granted": polaris.count("PUT", f"{setup.CATALOG_PATH}/catalog-roles/catalog_admin/grants") == 1,
            "no parameter was written": ssm.writes() == 0,
        }


def case_catalog_rerun(_: Path) -> dict[str, bool]:
    """Retry: a rerun over an existing matching catalog sends no create and reports already present."""
    with world() as (ssm, polaris, root):
        services = Services(ssm, polaris)
        first = call(["catalog", "--instance", "i-0123456789abcdef0"], services, root)
        second = call(["catalog", "--instance", "i-0123456789abcdef0"], services, root)
        return {
            "both runs succeed": first[0] == 0 and second[0] == 0,
            "the rerun reports already present": (outcome(second[1]) or {}).get("outcome") == "already present",
            "only one create was ever sent": polaris.count("POST", "/api/management/v1/catalogs") == 1,
        }


def case_catalog_differs(_: Path) -> dict[str, bool]:
    """P10: an existing catalog with another location is refused and never changed."""
    with world() as (ssm, polaris, root):
        polaris.catalog = setup.catalog_body("another-bucket", REGION)
        before = json.dumps(polaris.catalog, sort_keys=True)
        code, _out, err = call(["catalog", "--instance", "i-0123456789abcdef0"], Services(ssm, polaris), root)
        return {
            "the step fails": code == 1,
            "the error names the location check": "(catalog_location)" in err,
            "no create was sent": polaris.count("POST", "/api/management/v1/catalogs") == 0,
            "no grant was sent": polaris.count("PUT", f"{setup.CATALOG_PATH}/catalog-roles/catalog_admin/grants") == 0,
            "the catalog is unchanged": json.dumps(polaris.catalog, sort_keys=True) == before,
        }


def case_lost_create_reply(_: Path) -> dict[str, bool]:
    """Lost reply: the create lands but its reply is lost; the run fails without a second create and the rerun adopts the catalog."""
    with world(faults={"lose_create_reply"}) as (ssm, polaris, root):
        services = Services(ssm, polaris)
        first = call(["catalog", "--instance", "i-0123456789abcdef0"], services, root)
        creates_after_failure = polaris.count("POST", "/api/management/v1/catalogs")
        polaris.faults.clear()
        second = call(["catalog", "--instance", "i-0123456789abcdef0"], services, root)
        return {
            "the first run fails visibly": first[0] == 1 and "(catalog_http)" in first[2],
            "the create was sent once": creates_after_failure == 1,
            "the rerun reports already present": second[0] == 0 and (outcome(second[1]) or {}).get("outcome") == "already present",
            "no second create was ever sent": polaris.count("POST", "/api/management/v1/catalogs") == 1,
        }


def case_create_conflict(_: Path) -> dict[str, bool]:
    """Two runs: a create that meets a conflict reads the catalog back and verifies it instead of failing or retrying the create."""
    with world(faults={"conflict"}) as (ssm, polaris, root):
        code, out, _err = call(["catalog", "--instance", "i-0123456789abcdef0"], Services(ssm, polaris), root)
        return {
            "the step succeeds": code == 0,
            "the outcome is already present": (outcome(out) or {}).get("outcome") == "already present",
            "one create was sent": polaris.count("POST", "/api/management/v1/catalogs") == 1,
            "the catalog was read back": polaris.count("GET", setup.CATALOG_PATH) == 2,
        }


def case_mac_port(_: Path) -> dict[str, bool]:
    """P9: no request or route may use the Mac Polaris port, so the tool can never reach the Mac catalog."""
    checks: dict[str, bool] = {}
    try:
        setup.TunnelRequest(MAC_PORT, deadline=time.monotonic() + 5)
        checks["a request on the Mac port is refused"] = False
    except setup.SetupError as error:
        checks["a request on the Mac port is refused"] = error.code == "network"
    for label, ports in (("tunnel", (MAC_PORT, 18182)), ("relay", (18181, MAC_PORT))):
        try:
            setup.check_ports(TransportSpec("i-0123456789abcdef0", "project", REGION, *ports))
            checks[f"the {label} on the Mac port is refused"] = False
        except setup.SetupError as error:
            checks[f"the {label} on the Mac port is refused"] = error.code == "network"
    return checks


def case_root_refused(_: Path) -> dict[str, bool]:
    """P1: Polaris refuses the root credentials; the error carries the HTTP status only and nothing is created."""
    with world(faults={"root_refused"}) as (ssm, polaris, root):
        code, out, err = call(["catalog", "--instance", "i-0123456789abcdef0"], Services(ssm, polaris), root)
        return {
            "the step fails": code == 1,
            "the error names authentication and the status": "(authentication)" in err and "HTTP 401" in err,
            "the root secret is not in the output": root not in out + err,
            "no create was sent": polaris.count("POST", "/api/management/v1/catalogs") == 0,
        }


def case_stalled_catalog(_: Path) -> dict[str, bool]:
    """Every wait has a deadline: a catalog that stops answering fails the run near its own deadline, not at the stand-in's pace."""
    with world(faults={"stall"}) as (ssm, polaris, root):
        began = time.monotonic()
        code, _out, err = run_main(["--seconds", "1", "catalog", "--instance", "i-0123456789abcdef0"], Services(ssm, polaris))
        SECRETS_SEEN.append(root)
        OUTPUTS.append(err)
        took = time.monotonic() - began
        return {
            "the step fails": code == 1,
            "it names the lost reply or the deadline": "(catalog_http)" in err or "(deadline)" in err,
            "it stops within two seconds": took < 2.0,
            "no create was sent": polaris.count("POST", "/api/management/v1/catalogs") == 0,
        }


def case_route_lost(_: Path) -> dict[str, bool]:
    """A lost route fails the run visibly and is never reopened inside it; no request reaches the catalog."""

    transport = mock.Mock(spec=CatalogTransport)
    transport.execute.side_effect = RuntimeError("catalog_transport_unavailable")
    with world() as (ssm, polaris, root):
        route = setup.Route(transport, f"http://127.0.0.1:{polaris.port}", deadline=time.monotonic() + 30)
        code, _out, err = call(["catalog", "--instance", "i-0123456789abcdef0"], Services(ssm, polaris, route.execute), root)
        return {
            "the step fails": code == 1,
            "it names the route": "(transport)" in err,
            "the route was tried once": transport.execute.call_count == 1,
            "no request reached the catalog": not polaris.calls,
        }


def case_real_relay(out: Path) -> dict[str, bool]:
    """The real owned relay in front of the stand-in catalog: the catalog is created through it and every relay process is confirmed gone."""
    folder = out / "relay_ledger"
    folder.mkdir(mode=0o700)
    with world() as (ssm, polaris, root):
        services = Services(ssm, polaris, relay=folder)
        code, out_text, _ = call(["catalog", "--instance", "i-0123456789abcdef0"], services, root)
        return {
            "the step succeeds through the relay": code == 0 and (outcome(out_text) or {}).get("outcome") == "created",
            "the relay closed with every process confirmed gone": services.closed == [True],
        }


def case_profile_refused(_: Path) -> dict[str, bool]:
    """P2: a profile that does not resolve to the project user stops the tool before any Parameter Store or catalog call."""
    from scripts.infrastructure import render_project_config

    def refuse(_: dict[str, str]) -> None:
        raise render_project_config.ConfigurationError("the caller is not the project user")

    services = setup.LiveServices.__new__(setup.LiveServices)
    with (
        mock.patch.object(render_project_config, "load_configuration", return_value=({"aws_profile": "admin", "aws_region": REGION}, {})),
        mock.patch.object(render_project_config, "verify_project_identity", refuse),
        mock.patch("boto3.Session", side_effect=AssertionError("no AWS client may be made")) as session,
    ):
        code, out, err = run_main(["check"], services)
        OUTPUTS.append(out + err)
        return {"the tool stops": code == 1, "the error names the profile": "(profile)" in err, "no AWS client was made": not session.called}


def case_check_metadata(_: Path) -> dict[str, bool]:
    """P6: the check reports all ten names by exact name with type and version, reads nothing with decryption and prints no value."""
    pairs = setup.configured_pairs()
    seeded = {pairs["reader"][0]: "reader-id-" + secrets.token_hex(6), pairs["reader"][1]: "reader-secret-" + secrets.token_hex(6)}
    seeded[pairs["gold"][1]] = "gold-secret-" + secrets.token_hex(6)
    with world(seeded) as (ssm, polaris, root):
        services = Services(ssm, polaris)
        code, out, err = call(["check"], services, root)
        found = {item["name"]: item for item in (outcome(out) or {}).get("parameters", [])}
        SECRETS_SEEN.extend(seeded.values())
        return {
            "the check succeeds": code == 0,
            "all ten names are reported": set(found) == set(setup.writable_names()),
            "presence matches the store": all(found[name]["present"] == (name in seeded) for name in found),
            "present names report SecureString and a version": all(found[name].get("type") == "SecureString" and found[name].get("version") for name in seeded),
            "no read asked for decryption": bool(services.decryption) and not any(services.decryption),
            "no value or ciphertext is printed": not any(value in out + err for value in seeded.values()) and CIPHERTEXT_MARKER not in out + err,
            "the root secret was not read": (("GetParameter", secret_store.POLARIS_ROOT_SECRET) not in ssm.requests()),
            "nothing was written": ssm.writes() == 0,
            "no catalog request was sent": not polaris.calls,
        }


def case_check_catalog(_: Path) -> dict[str, bool]:
    """The check with an instance reads the catalog and changes nothing."""
    with world() as (ssm, polaris, root):
        services = Services(ssm, polaris)
        absent = call(["check", "--instance", "i-0123456789abcdef0"], services, root)
        polaris.catalog = setup.catalog_body(BUCKET, REGION)
        present = call(["check", "--instance", "i-0123456789abcdef0"], services, root)
        return {
            "an absent catalog is reported": absent[0] == 0 and (outcome(absent[1]) or {}).get("catalog") == "absent",
            "a present catalog is reported": present[0] == 0 and (outcome(present[1]) or {}).get("catalog") == "present",
            "no create or grant was sent": not [call for call in polaris.calls if call[0] in ("POST", "PUT") and call[1] != setup.OAUTH_PATH],
        }


def case_vault_scope(_: Path) -> dict[str, bool]:
    """P2, P12: the vault writes only the ten AWS pair names, refuses the Mac and root names before any request and reads the root secret."""
    pairs = setup.configured_pairs()
    with world() as (ssm, polaris, root):
        vault = Services(ssm, polaris).vault()
        refused = []
        for name in (secret_store.POLARIS_SILVER_CLIENT_ID, secret_store.POLARIS_GOLD_CLIENT_SECRET, secret_store.POLARIS_ROOT_SECRET, "/other/name"):
            try:
                vault.put(name, "value-" + secrets.token_hex(4))
                refused.append(False)
            except secret_store.SecretStoreError:
                refused.append(True)
        before = len(ssm.requests())
        value = "silver-id-" + secrets.token_hex(6)
        SECRETS_SEEN.append(value)
        vault.put(pairs["silver"][0], value)
        try:
            vault.find(secret_store.POSTGRES_PASSWORD)
            other_read = False
        except secret_store.SecretStoreError:
            other_read = True
        return {
            "the Mac, root and other names are refused": all(refused),
            "a refused write sends no request": before == 0,
            "an AWS pair name is written and read back": ssm.values.get(pairs["silver"][0]) == value,
            "the root secret is readable": vault.find(secret_store.POLARIS_ROOT_SECRET) == root,
            "another managed name is not readable": other_read,
        }


def case_pair_names(out: Path) -> dict[str, bool]:
    """P12: pair names that overlap a Mac name, repeat or lose their own leaf are refused before any call."""
    source = (REPO_ROOT / secret_store.VARIABLES).read_text()
    checks: dict[str, bool] = {"the committed names are ten distinct names": len(setup.writable_names()) == 10}
    plants = {
        "a Mac name": ("/healthcare_hai_prediction/aws_polaris_silver_client_id", "/healthcare_hai_prediction/polaris_silver_client_id"),
        "a repeated name": ("/healthcare_hai_prediction/aws_polaris_gold_client_id", "/healthcare_hai_prediction/aws_polaris_silver_client_id"),
        "another leaf": ("/healthcare_hai_prediction/aws_polaris_e2e_client_secret", "/healthcare_hai_prediction/aws_polaris_e2e_secret"),
    }
    for label, (old, new) in plants.items():
        path = out / f"variables_{re.sub(r'[^a-z]', '_', label)}.tf"
        path.write_text(source.replace(f'default     = "{old}"', f'default     = "{new}"', 1))
        try:
            setup.pair_names(path)
            checks[f"{label} is refused"] = False
        except setup.SetupError as error:
            checks[f"{label} is refused"] = error.code == "parameters"
    return checks


CASES: dict[str, Case] = {
    "catalog_created": case_catalog_created,
    "catalog_rerun": case_catalog_rerun,
    "catalog_differs": case_catalog_differs,
    "lost_create_reply": case_lost_create_reply,
    "create_conflict": case_create_conflict,
    "mac_port": case_mac_port,
    "root_refused": case_root_refused,
    "stalled_catalog": case_stalled_catalog,
    "route_lost": case_route_lost,
    "real_relay": case_real_relay,
    "profile_refused": case_profile_refused,
    "check_metadata": case_check_metadata,
    "check_catalog": case_check_catalog,
    "vault_scope": case_vault_scope,
    "pair_names": case_pair_names,
}


def no_secret_case(_: Path) -> dict[str, bool]:
    """P1: no root secret, token or stored value appears in any output of the cases above."""
    text = "\n".join(OUTPUTS)
    return {"no secret appears in any output": bool(OUTPUTS) and not any(value and value in text for value in SECRETS_SEEN)}


def run_cases(out: Path, deadline: float) -> dict[str, dict[str, bool]]:
    results: dict[str, dict[str, bool]] = {}
    for name, case in {**CASES, "no_secret_in_output": no_secret_case}.items():
        if time.monotonic() >= deadline:
            results[name] = {"the case ran before the suite deadline": False}
            continue
        folder = out / name
        folder.mkdir(mode=0o700)
        try:
            results[name] = case(folder)
        except Exception as error:
            results[name] = {f"the case ended with {type(error).__name__}": False}
    return results


# Each planted defect and the case that must fail when it is in place.
MUTATIONS: dict[str, tuple[str, Callable[[], contextlib.AbstractContextManager[Any]]]] = {
    "the location check accepts any catalog": ("catalog_differs", lambda: mock.patch.object(setup, "matches", lambda body, bucket: True)),
    "the check reads with decryption": (
        "check_metadata",
        lambda: mock.patch.object(
            setup.PairVault, "describe", lambda self, name: {"name": name, "present": self.find(name) is not None, "type": "SecureString", "version": 1}
        ),
    ),
    "the vault writes any name": ("vault_scope", lambda: mock.patch.object(setup.PairVault, "check", staticmethod(lambda name, write=False: None))),
    "the Mac port is allowed": ("mac_port", lambda: mock.patch.object(setup, "MAC_POLARIS_PORT", 1)),
    "a conflict is a failure": ("create_conflict", lambda: mock.patch.object(setup, "verify_catalog", lambda *a: "absent")),
}


def run_mutations(out: Path, deadline: float) -> dict[str, dict[str, Any]]:
    found: dict[str, dict[str, Any]] = {}
    for label, (intended, plant) in MUTATIONS.items():
        folder = out / f"mutation_{len(found)}"
        folder.mkdir(mode=0o700)
        with plant():
            case = CASES[intended]
            try:
                checks = case(folder)
            except Exception as error:
                checks = {f"ended with {type(error).__name__}": False}
        found[label] = {"intended_case": intended, "caught": not all(checks.values()), "failed_checks": [name for name, ok in checks.items() if not ok]}
        if time.monotonic() >= deadline:
            break
    return found


def main() -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--out", type=Path, required=True, help="a new directory for the report")
    parser.add_argument("--mutations", action="store_true", help="also plant each defect and require its intended case to fail")
    args = parser.parse_args()
    out = args.out.expanduser().absolute()
    out.mkdir(mode=0o700, parents=True, exist_ok=False)
    deadline = time.monotonic() + (CASE_SECONDS * (len(CASES) + 1))
    work = Path(tempfile.mkdtemp(prefix="aws_catalog_setup_", dir=out))
    try:
        results = run_cases(work, deadline)
        mutations = run_mutations(work, deadline) if args.mutations else {}
    finally:
        shutil.rmtree(work, ignore_errors=True)
    passed = all(all(checks.values()) for checks in results.values()) and all(item["caught"] for item in mutations.values())
    passed = passed and (not args.mutations or len(mutations) == len(MUTATIONS))
    report = {
        "run_at_utc": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "classification": "synthetic",
        "limits": ["Stand-in Polaris and Parameter Store only; no AWS service, tunnel or container", "the real owned relay is exercised"],
        "repeat_command": [".venv/bin/python", "-m", "scripts.lakehouse.run_aws_catalog_setup_e2e", "--out", "<NEW_DIRECTORY>", "--mutations"],
        "results": results,
        "mutations": mutations,
        "passed": passed,
        "cleanup": "done: stand-ins stopped, relay processes confirmed gone, work folder removed",
    }
    (out / "report.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    for name, checks in results.items():
        for check, ok in checks.items():
            sys.stdout.write(f"{'PASS' if ok else 'FAIL'} {name}: {check}\n")
    for label, item in mutations.items():
        sys.stdout.write(f"{'CAUGHT' if item['caught'] else 'MISSED'} {label} (case {item['intended_case']})\n")
    sys.stdout.write(f"aws catalog setup e2e: {'passed' if passed else 'failed'}; report {out / 'report.json'}\n")
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
