"""A loopback stand-in for the project bucket, STS and the bucket reads of the state migration, for its E2E.

Real Terraform and boto3 talk to it through ``AWS_ENDPOINT_URL_S3`` and ``AWS_ENDPOINT_URL_STS``, so the backend block, the lock file and the copy make the
same calls they make against AWS. It answers path-style S3 requests (object PUT with ``If-None-Match``, GET, HEAD, DELETE, ListObjectsV2 and the versioning,
policy and lifecycle reads) and the STS ``GetCallerIdentity`` query. Each access key belongs to a role. The stand-in enforces the state prefix rule of the
bucket policy by the key's role: the iam stack's objects answer only to the administrator key, every other stack's objects only to the project key. Every
request is recorded with its role.

Faults the cases switch on: ``drop_put`` (the connection closes before the object is stored), ``corrupt_get`` (a read returns another serial),
``race_put`` (another writer creates the key just before a write arrives) and ``deny_all``.
"""

from __future__ import annotations

import hashlib
import json
import re
import threading
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, cast
from urllib.parse import parse_qs, urlsplit

BUCKET = "example-project-data-dev"
ACCOUNT = "".join(["1"] * 12)
SIGNING_KEY = re.compile(r"Credential=([A-Z0-9]+)/")
STATE_PREFIX = "terraform_state/"
IAM_PREFIX = "terraform_state/iam/"
XML = "application/xml"


@dataclass(frozen=True)
class Seen:
    """One request as it arrived: the role of the signing key, the method, the object key (empty for a bucket call) and the query names."""

    role: str
    method: str
    key: str
    query: tuple[str, ...]
    status: int


@dataclass
class Faults:
    drop_put: set[str] = field(default_factory=set)
    corrupt_get: set[str] = field(default_factory=set)
    deny_all: bool = False
    race_put: dict[str, bytes] = field(default_factory=dict)


class Server(ThreadingHTTPServer):
    daemon_threads = True
    stand_in: StateBucket

    def handle_error(self, request: Any, client_address: Any) -> None:
        return


def decode_chunked(body: bytes) -> bytes:
    """The payload of an ``aws-chunked`` body: chunk signatures and the trailing checksum are dropped."""
    out, position = bytearray(), 0
    while position < len(body):
        end = body.find(b"\r\n", position)
        if end < 0:
            break
        size = int(body[position:end].split(b";")[0], 16)
        if size == 0:
            break
        out += body[end + 2 : end + 2 + size]
        position = end + 2 + size + 2
    return bytes(out)


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, format: str, *args: Any) -> None:
        return

    @property
    def bucket(self) -> StateBucket:
        return cast(Server, self.server).stand_in

    def role(self) -> str:
        match = SIGNING_KEY.search(self.headers.get("Authorization", ""))
        return self.bucket.roles.get(match.group(1), "unknown") if match else "anonymous"

    def reply(self, status: int, body: bytes = b"", content_type: str = XML, headers: dict[str, str] | None = None) -> int:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", "0" if self.command == "HEAD" else str(len(body)))
        for name, value in (headers or {}).items():
            self.send_header(name, value)
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)
        return status

    def error(self, status: int, code: str) -> int:
        return self.reply(status, f"<Error><Code>{code}</Code><Message>stand-in</Message></Error>".encode())

    def read_body(self) -> bytes:
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length) if length else b""
        if "aws-chunked" in self.headers.get("Content-Encoding", "") or self.headers.get("x-amz-content-sha256", "").startswith("STREAMING"):
            return decode_chunked(body)
        return body

    def allowed(self, role: str, key: str) -> bool:
        if self.bucket.faults.deny_all or role in ("unknown", "anonymous"):
            return False
        if key.startswith(IAM_PREFIX):
            return role == "administrator"
        if key.startswith(STATE_PREFIX):
            return role == "project"
        return True

    def handle_any(self) -> None:
        parts = urlsplit(self.path)
        query = parse_qs(parts.query, keep_blank_values=True)
        role, method = self.role(), self.command
        if method == "POST" and "Action=GetCallerIdentity" in self.read_body().decode():
            account, arn = self.bucket.identities.get(role, (ACCOUNT, "arn:aws:iam::" + ACCOUNT + ":user/unknown"))
            body = (
                '<GetCallerIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/"><GetCallerIdentityResult>'
                f"<Arn>{arn}</Arn><UserId>AIDASTANDIN</UserId><Account>{account}</Account></GetCallerIdentityResult>"
                "<ResponseMetadata><RequestId>stand-in</RequestId></ResponseMetadata></GetCallerIdentityResponse>"
            )
            self.bucket.record(Seen(role, "STS", "", ("GetCallerIdentity",), 200))
            self.reply(200, body.encode())
            return
        path = parts.path.lstrip("/")
        name, _, key = path.partition("/")
        status = self.dispatch(role, method, name, key, query)
        self.bucket.record(Seen(role, method, key, tuple(sorted(query)), status))

    def dispatch(self, role: str, method: str, name: str, key: str, query: dict[str, list[str]]) -> int:
        if name != BUCKET:
            return self.error(404, "NoSuchBucket")
        if not key:
            return self.bucket_call(role, method, query)
        if not self.allowed(role, key):
            self.read_body()
            return self.error(403, "AccessDenied")
        store = self.bucket
        with store.guard:
            if method == "PUT":
                body = self.read_body()
                if key in store.faults.drop_put:
                    self.close_connection = True
                    return 0
                if key in store.faults.race_put:
                    store.objects[key] = store.faults.race_put.pop(key)
                if self.headers.get("If-None-Match") == "*" and key in store.objects:
                    return self.error(412, "PreconditionFailed")
                store.objects[key] = body
                return self.reply(200, headers={"ETag": '"' + hashlib.md5(body, usedforsecurity=False).hexdigest() + '"'})
            if method in ("GET", "HEAD"):
                if key not in store.objects:
                    return self.error(404, "NoSuchKey")
                body = store.objects[key]
                if key in store.faults.corrupt_get:
                    body = body.replace(b'"serial": ', b'"serial": 9')
                headers = {"ETag": '"' + hashlib.md5(body, usedforsecurity=False).hexdigest() + '"', "Content-Length": str(len(body))}
                self.send_response(200)
                self.send_header("Content-Type", "application/octet-stream")
                for header, value in headers.items():
                    self.send_header(header, value)
                self.end_headers()
                if method == "GET":
                    self.wfile.write(body)
                return 200
            if method == "DELETE":
                store.objects.pop(key, None)
                return self.reply(204)
        return self.error(405, "MethodNotAllowed")

    def bucket_call(self, role: str, method: str, query: dict[str, list[str]]) -> int:
        store = self.bucket
        if store.faults.deny_all or role in ("unknown", "anonymous"):
            return self.error(403, "AccessDenied")
        if method != "GET":
            return self.error(405, "MethodNotAllowed")
        if "versioning" in query:
            return self.reply(
                200,
                f'<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>{store.versioning}</Status></VersioningConfiguration>'.encode(),
            )
        if "policy" in query:
            if store.policy is None:
                return self.error(404, "NoSuchBucketPolicy")
            return self.reply(200, json.dumps(store.policy).encode(), "application/json")
        if "lifecycle" in query:
            rules = "".join(f"<Rule><ID>{name}</ID><Status>Enabled</Status><Filter><Prefix>x</Prefix></Filter></Rule>" for name in store.lifecycle)
            return self.reply(200, f'<LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">{rules}</LifecycleConfiguration>'.encode())
        prefix = (query.get("prefix") or [""])[0]
        delimiter = (query.get("delimiter") or [""])[0]
        keys = sorted(key for key in store.objects if key.startswith(prefix))
        contents = "".join(
            f"<Contents><Key>{key}</Key><Size>{len(store.objects[key])}</Size></Contents>"
            for key in keys
            if not delimiter or delimiter not in key[len(prefix) :]
        )
        return self.reply(
            200,
            (
                f'<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>{BUCKET}</Name><Prefix>{prefix}</Prefix>'
                f"<KeyCount>{len(keys)}</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>{contents}</ListBucketResult>"
            ).encode(),
        )

    do_GET = do_PUT = do_HEAD = do_DELETE = do_POST = handle_any


class StateBucket:
    """The bucket, its access keys and its recorded requests. Use as a context manager; ``endpoint`` is the loopback URL."""

    def __init__(self, roles: dict[str, str], identities: dict[str, tuple[str, str]] | None = None) -> None:
        self.roles = roles
        self.identities = identities or {}
        self.objects: dict[str, bytes] = {}
        self.seen: list[Seen] = []
        self.faults = Faults()
        self.versioning = "Enabled"
        self.policy: dict[str, Any] | None = None
        self.lifecycle: list[str] = []
        self.guard = threading.Lock()
        self.server = Server(("127.0.0.1", 0), Handler)
        self.server.stand_in = self
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    @property
    def endpoint(self) -> str:
        return f"http://127.0.0.1:{self.server.server_address[1]}"

    def record(self, seen: Seen) -> None:
        with self.guard:
            self.seen.append(seen)

    def requests(self, method: str | None = None, role: str | None = None) -> list[Seen]:
        with self.guard:
            return [seen for seen in self.seen if (method is None or seen.method == method) and (role is None or seen.role == role)]

    def __enter__(self) -> StateBucket:
        self.thread.start()
        return self

    def __exit__(self, *_: object) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=10)
