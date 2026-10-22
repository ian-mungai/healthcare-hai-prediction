"""A loopback stand-in for STS ``GetCallerIdentity`` and Secrets Manager ``GetSecretValue`` that records which access key signed each request.

The collector key reader's child process talks to it through its test-only loopback endpoint. Synthetic AWS files give each profile a stand-in access
key; the stand-in maps each key to an account and ARN. Nothing leaves the machine and no real AWS file is read.

``InterceptingProxy`` is a loopback HTTPS proxy that answers ``CONNECT`` itself, presents a synthetic certificate and relays the decrypted traffic
to one stand-in, so a test can tell which CA bundle a client trusted and whether a man in the middle could read a request or an answer.
"""

from __future__ import annotations

import json
import re
import socket
import socketserver
import ssl
import threading
import time
import urllib.parse
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, cast

from scripts.process import run_command

STALL_LIMIT_SECONDS = 60.0
PROXY_STEP_SECONDS = 10.0
PROXY_HEAD_LIMIT = 65536
SIGNING_KEY = re.compile(r"Credential=([A-Z0-9]+)/")


@dataclass(frozen=True)
class Request:
    """One request as it arrived: the action, the access key that signed it, the secret name and the monotonic arrival time."""

    action: str
    key: str
    name: str
    at: float


class Server(ThreadingHTTPServer):
    """The HTTP server; it carries the stand-in whose state the handler reads."""

    stand_in: AwsStandIn

    def handle_error(self, request: Any, client_address: Any) -> None:
        return


class Handler(BaseHTTPRequestHandler):
    """Answers the STS query protocol and the Secrets Manager JSON protocol for the two read actions."""

    def log_message(self, format: str, *args: Any) -> None:
        return

    def reply(self, status: int, body: bytes, content_type: str, error: str = "") -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        if error:
            self.send_header("x-amzn-ErrorType", error)
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self) -> None:
        stand_in = cast(Server, self.server).stand_in
        body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        signed = SIGNING_KEY.search(self.headers.get("Authorization", ""))
        key = signed.group(1) if signed else ""
        target = self.headers.get("X-Amz-Target", "")
        if target:
            action, name = target.rpartition(".")[2], str(json.loads(body or b"{}").get("SecretId", ""))
        else:
            action, name = urllib.parse.parse_qs(body.decode()).get("Action", [""])[0], ""
        stand_in.arrived(Request(action, key, name, time.monotonic()))
        if action in stand_in.stalled:
            stand_in.released.wait(STALL_LIMIT_SECONDS)
        if action in stand_in.held_until:
            time.sleep(min(STALL_LIMIT_SECONDS, max(0.0, stand_in.held_until[action] - time.time())))
        if action == "GetCallerIdentity":
            if key not in stand_in.identities:
                error = b"<ErrorResponse><Error><Type>Sender</Type><Code>InvalidClientTokenId</Code><Message>unknown</Message></Error></ErrorResponse>"
                self.reply(403, error, "text/xml")
                return
            account, arn = stand_in.identities[key]
            result = f"<Arn>{arn}</Arn><UserId>AIDASTANDIN</UserId><Account>{account}</Account>"
            document = (
                '<GetCallerIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">'
                f"<GetCallerIdentityResult>{result}</GetCallerIdentityResult>"
                "<ResponseMetadata><RequestId>stand-in</RequestId></ResponseMetadata></GetCallerIdentityResponse>"
            )
            self.reply(200, document.encode(), "text/xml")
        elif action == "GetSecretValue" and key in stand_in.identities and name in stand_in.values:
            answer = {"ARN": f"arn:aws:secretsmanager:us-west-2:stand-in:secret:{name}", "Name": name, "SecretString": stand_in.values[name]}
            self.reply(200, json.dumps(answer).encode(), "application/x-amz-json-1.1")
        elif action == "GetSecretValue" and key in stand_in.identities:
            self.reply(400, b'{"__type": "ResourceNotFoundException", "Message": "missing"}', "application/x-amz-json-1.1", "ResourceNotFoundException")
        else:
            self.reply(400, b'{"__type": "AccessDeniedException", "Message": "denied"}', "application/x-amz-json-1.1", "AccessDeniedException")


class AwsStandIn:
    """``identities`` maps an access key to its account and ARN; ``values`` maps a secret name to its value; an action in ``stalled`` waits for
    ``released`` (at most 60 s) after it is recorded; an action in ``held_until`` is answered no earlier than that wall-clock time (at most 60 s
    later)."""

    def __init__(self, identities: dict[str, tuple[str, str]], values: dict[str, str]) -> None:
        self.identities = dict(identities)
        self.values = dict(values)
        self.stalled: set[str] = set()
        self.held_until: dict[str, float] = {}
        self.released = threading.Event()
        self.lock = threading.Lock()
        self.log: list[Request] = []
        self.server = Server(("127.0.0.1", 0), Handler)
        self.server.stand_in = self
        self.thread = threading.Thread(target=self.server.serve_forever, name="aws-stand-in", daemon=True)

    @property
    def endpoint(self) -> str:
        """The base URL the key reader's child process uses for both services."""
        return f"http://127.0.0.1:{self.server.server_address[1]}"

    def arrived(self, request: Request) -> None:
        with self.lock:
            self.log.append(request)

    def requests(self, action: str | None = None) -> list[Request]:
        """Every request that arrived, or those of one action, in order."""
        with self.lock:
            return [request for request in self.log if action is None or request.action == action]

    def __enter__(self) -> AwsStandIn:
        self.thread.start()
        return self

    def __exit__(self, *_: object) -> None:
        self.released.set()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=10)


def write_profile(
    home: Path,
    profile: str,
    region: str,
    key: str | None,
    credential_process: str | None = None,
    ca_bundle: Path | None = None,
    default_ca_bundle: Path | None = None,
) -> dict[str, str]:
    """Write synthetic ``.aws/credentials`` (when ``key`` is given) and ``.aws/config`` files under ``home`` for one profile; return their paths
    as the ``AWS_SHARED_CREDENTIALS_FILE`` and ``AWS_CONFIG_FILE`` settings that would select them. ``ca_bundle`` is the profile's trusted
    certificate file; ``default_ca_bundle``, when given, adds a ``default`` profile that trusts that file."""
    folder = home / ".aws"
    folder.mkdir(parents=True, exist_ok=True)
    credentials, config = folder / "credentials", folder / "config"
    if key is None:
        credentials.unlink(missing_ok=True)
    else:
        credentials.write_text(f"[{profile}]\naws_access_key_id = {key}\naws_secret_access_key = stand-in-secret\n")
    process = f"credential_process = {credential_process}\n" if credential_process else ""
    bundle = f"ca_bundle = {ca_bundle}\n" if ca_bundle else ""
    default = f"[default]\nca_bundle = {default_ca_bundle}\n" if default_ca_bundle else ""
    config.write_text(f"{default}[profile {profile}]\nregion = {region}\n{process}{bundle}")
    return {"AWS_SHARED_CREDENTIALS_FILE": str(credentials), "AWS_CONFIG_FILE": str(config)}


def refreshing_process(folder: Path, before: str, after: str, moment: int) -> str:
    """Write a credential process under ``folder`` and return its command: before ``moment`` (whole seconds since the epoch) it returns the
    ``before`` access key, expiring 900 s after ``moment``, so the SDK's advisory refresh window opens at ``moment``; from ``moment`` on it returns
    the ``after`` access key, expiring an hour later."""
    folder.mkdir(parents=True, exist_ok=True)
    for label, key, expires in (("before", before, moment + 900), ("after", after, moment + 3600)):
        expiration = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(expires))
        document = {"Version": 1, "AccessKeyId": key, "SecretAccessKey": "stand-in-secret", "Expiration": expiration}
        (folder / f"{label}.json").write_text(json.dumps(document))
    script = folder / "credentials.sh"
    script.write_text(f'if [ "$(/bin/date +%s)" -lt {moment} ]; then /bin/cat "{folder}/before.json"; else /bin/cat "{folder}/after.json"; fi\n')
    return f"/bin/sh {script}"


def write_certificate(folder: Path, name: str, hosts: list[str]) -> tuple[Path, Path]:
    """Write a synthetic self-signed certificate valid for ``hosts`` and its key under ``folder`` with the OpenSSL tool; return their paths.

    The certificate is its own trust anchor, so a CA bundle that holds it trusts a server that presents it and no other."""
    folder.mkdir(parents=True, exist_ok=True)
    names = "\n".join(f"DNS.{number} = {host}" for number, host in enumerate(hosts, start=1))
    settings = folder / f"{name}.cnf"
    settings.write_text(
        f"[req]\ndistinguished_name = names\nprompt = no\n[names]\nCN = stand-in {name}\n"
        f"[extensions]\nbasicConstraints = critical,CA:TRUE\nkeyUsage = digitalSignature,keyCertSign\nsubjectAltName = @hosts\n[hosts]\n{names}\n"
    )
    certificate, key = folder / f"{name}.pem", folder / f"{name}.key"
    arguments = ["req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "2", "-config", str(settings), "-extensions", "extensions"]
    run_command("openssl", [*arguments, "-keyout", str(key), "-out", str(certificate)], timeout=60, check=True)
    return certificate, key


class ProxyHandler(socketserver.BaseRequestHandler):
    """Answers one ``CONNECT``, performs the TLS handshake with the synthetic certificate and relays each decrypted request to the stand-in."""

    def read_message(self, connection: Any, headers_only: bool) -> bytes:
        """One HTTP message from ``connection``: the head, plus the body its ``Content-Length`` announces; b"" at end of stream."""
        data = b""
        while b"\r\n\r\n" not in data:
            chunk = connection.recv(65536)
            if not chunk or len(data) > PROXY_HEAD_LIMIT:
                return data
            data += chunk
        if headers_only:
            return data
        head, _, body = data.partition(b"\r\n\r\n")
        length = re.search(rb"(?im)^content-length:\s*(\d+)", head)
        while length and len(body) < int(length.group(1)):
            chunk = connection.recv(65536)
            if not chunk:
                break
            body += chunk
        return head + b"\r\n\r\n" + body

    def handle(self) -> None:
        proxy = cast(Any, self.server).proxy
        self.request.settimeout(PROXY_STEP_SECONDS)
        try:
            head = self.read_message(self.request, headers_only=True)
            words = head.split(b"\r\n", 1)[0].decode("latin-1").split()
            if len(words) != 3 or words[0] != "CONNECT":
                self.request.sendall(b"HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\n\r\n")
                return
            proxy.record_target(words[1])
            self.request.sendall(b"HTTP/1.1 200 Connection Established\r\n\r\n")
            try:
                secure = proxy.context.wrap_socket(self.request, server_side=True)
            except OSError:
                proxy.record_refused_handshake()
                return
            with secure:
                while True:
                    message = self.read_message(secure, headers_only=False)
                    if not message:
                        return
                    proxy.record_intercepted(message)
                    with socket.create_connection(("127.0.0.1", proxy.upstream_port), timeout=PROXY_STEP_SECONDS) as upstream:
                        upstream.sendall(message)
                        answer = b""
                        while chunk := upstream.recv(65536):
                            answer += chunk
                    proxy.record_intercepted(answer)
                    secure.sendall(answer)
        except OSError:
            return


class ProxyServer(socketserver.ThreadingTCPServer):
    """The proxy's listener; it carries the proxy whose state the handler records."""

    allow_reuse_address = True
    daemon_threads = True
    proxy: InterceptingProxy

    def handle_error(self, request: Any, client_address: Any) -> None:
        return


class InterceptingProxy:
    """A loopback HTTPS proxy that terminates TLS with the certificate at ``certificate`` and relays the decrypted requests to the stand-in on
    ``upstream_port``. It records the ``CONNECT`` targets, the handshakes the client refused and every decrypted byte, in both directions."""

    def __init__(self, certificate: Path, key: Path, upstream_port: int) -> None:
        self.context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        self.context.load_cert_chain(certificate, key)
        self.upstream_port = upstream_port
        self.lock = threading.Lock()
        self.targets: list[str] = []
        self.refused_handshakes = 0
        self.intercepted = b""
        self.server = ProxyServer(("127.0.0.1", 0), ProxyHandler)
        self.server.proxy = self
        self.thread = threading.Thread(target=self.server.serve_forever, name="intercepting-proxy", daemon=True)

    @property
    def url(self) -> str:
        """The value for ``HTTPS_PROXY``."""
        return f"http://127.0.0.1:{self.server.server_address[1]}"

    def record_target(self, target: str) -> None:
        with self.lock:
            self.targets.append(target)

    def record_refused_handshake(self) -> None:
        with self.lock:
            self.refused_handshakes += 1

    def record_intercepted(self, data: bytes) -> None:
        with self.lock:
            self.intercepted += data

    def __enter__(self) -> InterceptingProxy:
        self.thread.start()
        return self

    def __exit__(self, *_: object) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=10)
