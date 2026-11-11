"""Serve the stand-in ECS, CloudWatch Logs, SSM and S3 objects over loopback HTTP, so real boto3 clients reach them through an endpoint override.

The offline cases call the fakes (``ecs_broker_fake``, ``ecs_launch_fake``) as Python objects. The real-image DAG test (``real_image_dags.py``) needs the
production client code to run too: ``airflow_steps.services()`` builds real boto3 clients, and the Amazon provider operator runs in the Airflow image.
``AwsBridge`` listens on ``127.0.0.1`` inside the container and answers the AWS JSON protocol (ECS, SSM, Logs, dispatched by ``X-Amz-Target``) and the
path-style S3 REST calls the launch library makes (GetObject, PutObject with ``If-Match`` and ``If-None-Match``). The container has no network: only
its own loopback is reachable. Credentials are fake and nothing is signed or checked.

An operation is the snake-case method of the same name on the fake, called with the request's members as keyword arguments, which are the names boto3
takes. A ``ClientError`` the fake raises becomes the error reply of that service. A reply that holds a datetime goes out as epoch seconds, which is how
the JSON protocol carries a timestamp.
"""

from __future__ import annotations

import json
import re
import threading
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, cast
from urllib.parse import parse_qs, unquote, urlsplit
from xml.sax.saxutils import escape

from botocore.exceptions import ClientError

TARGETS = {"AmazonEC2ContainerServiceV20141113": "ecs", "AmazonSSM": "ssm", "Logs_20140328": "logs"}
SNAKE = re.compile(r"(?<!^)(?=[A-Z])")
STATUS_OF = {"NoSuchKey": 404, "PreconditionFailed": 412, "AccessDenied": 403}
MAX_BODY = 8 * 1024 * 1024


def snake(name: str) -> str:
    """``DescribeTasks`` as ``describe_tasks``."""
    return SNAKE.sub("_", name).lower()


def encoded(value: Any) -> Any:
    """The JSON form of a fake's reply: a datetime is epoch seconds."""
    if isinstance(value, datetime):
        return value.timestamp()
    raise TypeError(f"{type(value).__name__} is not JSON")


def dechunked(body: bytes) -> bytes:
    """The payload of an ``aws-chunked`` body (``<hex size>[;extensions]\\r\\n<data>\\r\\n`` per chunk, a zero chunk, then trailer lines), which the SDK sends
    when it adds a checksum to an upload."""
    out, at = b"", 0
    while True:
        end = body.index(b"\r\n", at)
        size = int(body[at:end].split(b";")[0], 16)
        if size == 0:
            return out
        out += body[end + 2 : end + 2 + size]
        at = end + 2 + size + 2


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    @property
    def bridge(self) -> Bridge:
        return cast("Bridge", self.server)

    def log_message(self, format: str, *args: Any) -> None:
        return None

    def body(self) -> bytes:
        size = int(self.headers.get("Content-Length") or 0)
        if size > MAX_BODY:
            raise ValueError("the request body is too large for the bridge")
        return self.rfile.read(size)

    def reply(self, status: int, body: bytes, kind: str, headers: dict[str, str] | None = None) -> None:
        self.send_response(status)
        self.send_header("Content-Type", kind)
        self.send_header("Content-Length", str(len(body)))
        for name, value in (headers or {}).items():
            self.send_header(name, value)
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def do_POST(self) -> None:
        raw = self.body()
        target = self.headers.get("X-Amz-Target", "")
        service, _, operation = target.partition(".")
        fake = self.bridge.services.get(TARGETS.get(service, ""))
        if fake is None or not operation:
            self.reply(400, json.dumps({"__type": "UnknownOperationException", "message": target}).encode(), "application/x-amz-json-1.1")
            return
        with self.bridge.lock:
            self.bridge.requests.append((TARGETS[service], operation, json.loads(raw or b"{}")))
            try:
                answer = getattr(fake, snake(operation))(**json.loads(raw or b"{}"))
            except ClientError as error:
                code = error.response["Error"]["Code"]
                self.reply(400, json.dumps({"__type": code, "message": error.response["Error"]["Message"]}).encode(), "application/x-amz-json-1.1")
                return
        self.reply(200, json.dumps(answer, default=encoded).encode(), "application/x-amz-json-1.1")

    def object_of(self) -> tuple[str, str]:
        bucket, _, key = unquote(self.path.split("?", 1)[0]).lstrip("/").partition("/")
        return bucket, key

    def s3_error(self, error: ClientError) -> None:
        code = error.response["Error"]["Code"]
        xml = f"<?xml version='1.0'?><Error><Code>{code}</Code><Message>{error.response['Error']['Message']}</Message></Error>"
        self.reply(STATUS_OF.get(code, 400), xml.encode(), "application/xml")

    def list_objects(self, bucket: str, query: dict[str, list[str]]) -> None:
        """ListObjectsV2: the fake's keys under the prefix as the XML reply, with the continuation token when the page is cut."""
        arguments: dict[str, Any] = {"Bucket": bucket, "Prefix": query.get("prefix", [""])[0]}
        if "max-keys" in query:
            arguments["MaxKeys"] = int(query["max-keys"][0])
        if "continuation-token" in query:
            arguments["ContinuationToken"] = query["continuation-token"][0]
        with self.bridge.lock:
            self.bridge.requests.append(("s3", "ListObjectsV2", {"Bucket": bucket, "Prefix": arguments["Prefix"]}))
            try:
                answer = self.bridge.s3_for(arguments["Prefix"]).list_objects_v2(**arguments)
            except ClientError as error:
                self.s3_error(error)
                return
        contents = "".join(f"<Contents><Key>{escape(item['Key'])}</Key></Contents>" for item in answer.get("Contents", []))
        token = f"<NextContinuationToken>{escape(answer['NextContinuationToken'])}</NextContinuationToken>" if answer.get("NextContinuationToken") else ""
        xml = (
            f"<?xml version='1.0'?><ListBucketResult><Name>{escape(bucket)}</Name><Prefix>{escape(arguments['Prefix'])}</Prefix>"
            f"<KeyCount>{len(answer.get('Contents', []))}</KeyCount><MaxKeys>{arguments.get('MaxKeys', 1000)}</MaxKeys>"
            f"<IsTruncated>{'true' if answer.get('IsTruncated') else 'false'}</IsTruncated>{contents}{token}</ListBucketResult>"
        )
        self.reply(200, xml.encode(), "application/xml")

    def do_GET(self) -> None:
        bucket, key = self.object_of()
        query = parse_qs(urlsplit(self.path).query)
        if not key and query.get("list-type") == ["2"]:
            self.list_objects(bucket, query)
            return
        with self.bridge.lock:
            self.bridge.requests.append(("s3", "GetObject", {"Bucket": bucket, "Key": key}))
            try:
                answer = self.bridge.s3_for(key).get_object(Bucket=bucket, Key=key)
            except ClientError as error:
                self.s3_error(error)
                return
        body = answer["Body"].read()
        self.reply(200, body, "application/octet-stream", {"ETag": str(answer.get("ETag", '"stand-in"'))})

    def do_PUT(self) -> None:
        bucket, key = self.object_of()
        raw = self.body()
        body = dechunked(raw) if "aws-chunked" in self.headers.get("Content-Encoding", "") else raw
        arguments: dict[str, Any] = {"Bucket": bucket, "Key": key, "Body": body}
        if self.headers.get("If-None-Match"):
            arguments["IfNoneMatch"] = self.headers["If-None-Match"]
        if self.headers.get("If-Match"):
            arguments["IfMatch"] = self.headers["If-Match"]
        with self.bridge.lock:
            self.bridge.requests.append(("s3", "PutObject", {"Bucket": bucket, "Key": key}))
            try:
                answer = self.bridge.s3_for(key).put_object(**arguments)
            except ClientError as error:
                self.s3_error(error)
                return
        self.reply(200, b"", "application/xml", {"ETag": str(answer.get("ETag", '"stand-in"'))})


class Bridge(ThreadingHTTPServer):
    """One loopback server for the fakes of a case. ``services`` maps ``ecs``, ``ssm`` and ``logs`` to the fake that answers the service. The two S3
    fakes share the bucket: keys under ``record_prefix`` go to ``records``, every other key to ``stage``. ``requests`` lists every request in order as
    (service, operation, members)."""

    daemon_threads = True

    def __init__(self, services: dict[str, Any], stage: Any, records: Any, record_prefix: str) -> None:
        super().__init__(("127.0.0.1", 0), Handler)
        self.services, self.stage, self.records, self.record_prefix = services, stage, records, record_prefix
        self.lock = threading.Lock()
        self.requests: list[tuple[str, str, dict[str, Any]]] = []
        self.thread = threading.Thread(target=self.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True)

    @property
    def endpoint(self) -> str:
        return f"http://127.0.0.1:{self.server_address[1]}"

    def s3_for(self, key: str) -> Any:
        return self.records if key.startswith(self.record_prefix) else self.stage

    def __enter__(self) -> Bridge:
        self.thread.start()
        return self

    def __exit__(self, *_: object) -> None:
        self.shutdown()
        self.server_close()
        self.thread.join(timeout=10)
