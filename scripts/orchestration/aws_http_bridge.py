"""Serve the stand-in ECS, CloudWatch Logs, SSM and S3 objects over loopback HTTP, so real boto3 clients reach them through an endpoint override.

The offline cases call the fakes (``ecs_broker_fake``, ``ecs_launch_fake``) as Python objects. The real-image DAG test (``real_image_dags.py``) needs the
production client code to run too: ``airflow_steps.services()`` builds real boto3 clients, and the Amazon provider operator runs in the Airflow image.
``Bridge`` listens on ``127.0.0.1`` inside the container and answers the AWS JSON protocol (ECS, SSM, Logs, dispatched by ``X-Amz-Target``) and the
path-style S3 REST calls the launch library makes (GetObject, PutObject with ``If-Match`` and ``If-None-Match``, ListObjectsV2). The container has no
network: only its own loopback is reachable.

The bridge is as strict as AWS is for what the launch library sends, so a request AWS would refuse fails here and not in the live run:

- Each request carries a SigV4 ``Authorization`` header whose credential names the fake access key, the configured region and the service it reaches. The
  signature itself is not verified (the fake secret is public), so a wrong secret passes.
- Only the operations the launch library calls are served (ECS: RunTask, DescribeTasks, ListTasks, StopTask, DescribeTaskDefinition,
  ListContainerInstances, DescribeContainerInstances; SSM: GetParameter; Logs: GetLogEvents). Any other operation is answered ``AccessDeniedException``, as
  IAM answers an action the role does not hold, so a new call shows as a failure to review.
- The request fields have the limits the bundled service models state (task ids, counts, enums, name patterns, lengths). The RunTask rules are in
  ``ecs_broker_fake.check_run_task``, which the offline cases use too.
- S3 serves one bucket (``NoSuchBucket`` for any other), only keys under the launch record prefix and the staged input prefixes (``AccessDenied`` for the
  rest) and keys of at most 1024 bytes. SSM serves the names the fake lists and refuses the others as the launch role would. Logs serves one log group.

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
OPERATIONS = {
    "ecs": frozenset({"RunTask", "DescribeTasks", "ListTasks", "StopTask", "DescribeTaskDefinition", "ListContainerInstances", "DescribeContainerInstances"}),
    "ssm": frozenset({"GetParameter"}),
    "logs": frozenset({"GetLogEvents"}),
}
STATUS_OF = {"NoSuchKey": 404, "NoSuchBucket": 404, "PreconditionFailed": 412, "AccessDenied": 403, "KeyTooLongError": 400, "AuthorizationHeaderMalformed": 400}
SNAKE = re.compile(r"(?<!^)(?=[A-Z])")
CREDENTIAL = re.compile(
    r"AWS4-HMAC-SHA256 Credential=(?P<key>[^/]+)/(?P<date>[0-9]{8})/(?P<region>[^/]+)/(?P<service>[^/]+)/aws4_request,.*Signature=[0-9a-f]{64}"
)
TASK_ID = re.compile(r"(arn:aws:ecs:[a-z0-9-]+:[0-9]{12}:task/[A-Za-z0-9_-]{1,255}/)?[0-9a-f]{32}")
INSTANCE_ID = re.compile(r"(arn:aws:ecs:[a-z0-9-]+:[0-9]{12}:container-instance/[A-Za-z0-9_-]{1,255}/)?[A-Za-z0-9-]{1,64}")
TASK_DEFINITION = re.compile(r"[A-Za-z0-9_-]{1,255}(:[0-9]+)?|arn:aws:ecs:[a-z0-9-]+:[0-9]{12}:task-definition/[A-Za-z0-9_-]{1,255}:[0-9]+")
STARTED_BY = re.compile(r"[A-Za-z0-9/_-]{1,128}")
PARAMETER_NAME = re.compile(r"[A-Za-z0-9_./-]{1,1011}")
LOG_GROUP = re.compile(r"[.\-_/#A-Za-z0-9]{1,512}")
LOG_STREAM = re.compile(r"[^:*]{1,512}")
DESIRED_STATUS = frozenset({"RUNNING", "PENDING", "STOPPED"})
INSTANCE_STATUS = frozenset({"ACTIVE", "DRAINING", "REGISTERING", "DEREGISTERING", "REGISTRATION_FAILED"})
MAX_BODY = 8 * 1024 * 1024
KEY_LIMIT = 1024


class Refusal(Exception):
    """An answer a service gives to a request it refuses: the error code and message, and the HTTP status."""

    def __init__(self, code: str, message: str, status: int = 400) -> None:
        super().__init__(message)
        self.code, self.message, self.status = code, message, status


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


def invalid(message: str) -> Refusal:
    return Refusal("InvalidParameterException", message)


def check_list(items: Any, pattern: re.Pattern[str], name: str, limit: int = 100) -> None:
    """A list of 1 to ``limit`` identifiers that match ``pattern``."""
    if not isinstance(items, list) or not 1 <= len(items) <= limit or any(not isinstance(item, str) or not pattern.fullmatch(item) for item in items):
        raise invalid(f"{name} must hold 1 to {limit} identifiers of the form ECS accepts.")


def check_choice(value: Any, choices: frozenset[str], name: str) -> None:
    if value is not None and value not in choices:
        raise invalid(f"{name} must be one of {', '.join(sorted(choices))}.")


def check_page(arguments: dict[str, Any]) -> None:
    if "maxResults" in arguments and not 1 <= arguments["maxResults"] <= 100:
        raise invalid("maxResults must be between 1 and 100.")


def check_ecs(operation: str, arguments: dict[str, Any]) -> None:
    """The ECS rules for the fields the launch library sends, other than RunTask (``ecs_broker_fake.check_run_task``)."""
    if operation == "DescribeTasks":
        check_list(arguments.get("tasks"), TASK_ID, "tasks")
        for item in arguments.get("include", []):
            check_choice(item, frozenset({"TAGS"}), "include")
    elif operation == "ListTasks":
        check_choice(arguments.get("desiredStatus"), DESIRED_STATUS, "desiredStatus")
        check_page(arguments)
        if "startedBy" in arguments and not STARTED_BY.fullmatch(str(arguments["startedBy"])):
            raise invalid("startedBy allows only letters, numbers, hyphens, slashes and underscores, up to 128 characters.")
    elif operation == "StopTask":
        check_list([arguments.get("task")], TASK_ID, "task", 1)
        if len(str(arguments.get("reason", ""))) > 255:
            raise invalid("reason holds at most 255 characters.")
    elif operation == "DescribeTaskDefinition":
        if not TASK_DEFINITION.fullmatch(str(arguments.get("taskDefinition", ""))):
            raise invalid("taskDefinition is not a family name with an optional revision, or an ARN.")
    elif operation == "ListContainerInstances":
        check_page(arguments)
        check_choice(arguments.get("status"), INSTANCE_STATUS, "status")
    elif operation == "DescribeContainerInstances":
        check_list(arguments.get("containerInstances"), INSTANCE_ID, "containerInstances")


def check_ssm(arguments: dict[str, Any]) -> None:
    if not PARAMETER_NAME.fullmatch(str(arguments.get("Name", ""))):
        raise Refusal("ValidationException", "The parameter name has characters that SSM does not allow.")


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

    def authenticate(self, service: str) -> None:
        """Refuse a request that is not signed for this access key, region and service (the signature is not verified)."""
        found = CREDENTIAL.match(self.headers.get("Authorization", ""))
        s3 = service == "s3"
        if found is None:
            raise Refusal("AccessDenied" if s3 else "MissingAuthenticationTokenException", "The request carries no SigV4 credential.", 403 if s3 else 400)
        if found["key"] != self.bridge.access_key:
            raise Refusal("InvalidAccessKeyId" if s3 else "UnrecognizedClientException", "The security token included in the request is invalid.", 403)
        if found["region"] != self.bridge.region or found["service"] != service:
            message = f"The credential is scoped to {found['region']}/{found['service']}, not {self.bridge.region}/{service}."
            raise Refusal("AuthorizationHeaderMalformed" if s3 else "InvalidSignatureException", message, 400 if s3 else 403)

    def json_error(self, error: Refusal) -> None:
        self.reply(error.status, json.dumps({"__type": error.code, "message": error.message}).encode(), "application/x-amz-json-1.1")

    def do_POST(self) -> None:
        raw = self.body()
        target = self.headers.get("X-Amz-Target", "")
        prefix, _, operation = target.partition(".")
        name = TARGETS.get(prefix, "")
        fake = self.bridge.services.get(name)
        try:
            if fake is None or not operation:
                raise Refusal("UnknownOperationException", target)
            self.authenticate(name)
            arguments = json.loads(raw or b"{}")
            if operation not in OPERATIONS[name]:
                raise Refusal("AccessDeniedException", f"The role is not authorized to perform {name}:{operation}; the launch library does not call it.")
            self.bridge.check_json(name, operation, arguments)
        except Refusal as error:
            self.json_error(error)
            return
        with self.bridge.lock:
            self.bridge.requests.append((name, operation, arguments))
            try:
                answer = getattr(fake, snake(operation))(**arguments)
            except ClientError as error:
                code = error.response["Error"]["Code"]
                self.reply(400, json.dumps({"__type": code, "message": error.response["Error"]["Message"]}).encode(), "application/x-amz-json-1.1")
                return
        self.reply(200, json.dumps(answer, default=encoded).encode(), "application/x-amz-json-1.1")

    def object_of(self) -> tuple[str, str]:
        bucket, _, key = unquote(self.path.split("?", 1)[0]).lstrip("/").partition("/")
        return bucket, key

    def s3_error(self, error: ClientError | Refusal) -> None:
        code, message = (error.code, error.message) if isinstance(error, Refusal) else (error.response["Error"]["Code"], error.response["Error"]["Message"])
        xml = f"<?xml version='1.0'?><Error><Code>{code}</Code><Message>{escape(message)}</Message></Error>"
        self.reply(error.status if isinstance(error, Refusal) else STATUS_OF.get(code, 400), xml.encode(), "application/xml")

    def list_objects(self, bucket: str, query: dict[str, list[str]]) -> None:
        """ListObjectsV2: the fake's keys under the prefix as the XML reply, with the continuation token when the page is cut."""
        arguments: dict[str, Any] = {"Bucket": bucket, "Prefix": query.get("prefix", [""])[0]}
        if "max-keys" in query:
            arguments["MaxKeys"] = int(query["max-keys"][0])
            if not 0 <= arguments["MaxKeys"] <= 1000:
                raise Refusal("InvalidArgument", "Argument maxKeys must be an integer between 0 and 1000")
        if "continuation-token" in query:
            arguments["ContinuationToken"] = query["continuation-token"][0]
        self.bridge.check_key(arguments["Prefix"])
        with self.bridge.lock:
            self.bridge.requests.append(("s3", "ListObjectsV2", {"Bucket": bucket, "Prefix": arguments["Prefix"]}))
            answer = self.bridge.s3_for(arguments["Prefix"]).list_objects_v2(**arguments)
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
        try:
            self.authenticate("s3")
            self.bridge.check_bucket(bucket)
            if not key and query.get("list-type") == ["2"]:
                self.list_objects(bucket, query)
                return
            self.bridge.check_key(key)
            with self.bridge.lock:
                self.bridge.requests.append(("s3", "GetObject", {"Bucket": bucket, "Key": key}))
                answer = self.bridge.s3_for(key).get_object(Bucket=bucket, Key=key)
        except (Refusal, ClientError) as error:
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
        try:
            self.authenticate("s3")
            self.bridge.check_bucket(bucket)
            self.bridge.check_key(key)
            with self.bridge.lock:
                self.bridge.requests.append(("s3", "PutObject", {"Bucket": bucket, "Key": key}))
                answer = self.bridge.s3_for(key).put_object(**arguments)
        except (Refusal, ClientError) as error:
            self.s3_error(error)
            return
        self.reply(200, b"", "application/xml", {"ETag": str(answer.get("ETag", '"stand-in"'))})


class Bridge(ThreadingHTTPServer):
    """One loopback server for the fakes of a case. ``services`` maps ``ecs``, ``ssm`` and ``logs`` to the fake that answers the service. The two S3 fakes
    share ``bucket``: keys under ``record_prefix`` go to ``records``, keys under one of ``stage_prefixes`` to ``stage``, and any other key is refused.
    Requests are signed for ``access_key`` and ``region``, and Logs serves ``log_group`` only. ``requests`` lists every request in order as (service,
    operation, members)."""

    daemon_threads = True

    def __init__(
        self,
        services: dict[str, Any],
        stage: Any,
        records: Any,
        record_prefix: str,
        *,
        bucket: str,
        region: str,
        access_key: str,
        log_group: str,
        stage_prefixes: tuple[str, ...],
    ) -> None:
        super().__init__(("127.0.0.1", 0), Handler)
        self.services, self.stage, self.records, self.record_prefix = services, stage, records, record_prefix
        self.bucket, self.region, self.access_key, self.log_group, self.stage_prefixes = bucket, region, access_key, log_group, stage_prefixes
        self.lock = threading.Lock()
        self.requests: list[tuple[str, str, dict[str, Any]]] = []
        self.thread = threading.Thread(target=self.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True)

    @property
    def endpoint(self) -> str:
        return f"http://127.0.0.1:{self.server_address[1]}"

    def s3_for(self, key: str) -> Any:
        return self.records if key.startswith(self.record_prefix) else self.stage

    def check_bucket(self, bucket: str) -> None:
        if bucket != self.bucket:
            raise Refusal("NoSuchBucket", f"The specified bucket does not exist: {bucket}", 404)

    def check_key(self, key: str) -> None:
        """Refuse a key longer than S3 allows and a key outside the prefixes this bridge serves."""
        if len(key.encode()) > KEY_LIMIT:
            raise Refusal("KeyTooLongError", "Your key is too long", 400)
        if not key.startswith((self.record_prefix, *self.stage_prefixes)):
            raise Refusal("AccessDenied", "Access Denied", 403)

    def check_json(self, service: str, operation: str, arguments: dict[str, Any]) -> None:
        """The service rules for a JSON request, before the fake sees it."""
        if service == "ecs":
            check_ecs(operation, arguments)
        elif service == "ssm":
            check_ssm(arguments)
        else:
            if arguments.get("logGroupName") != self.log_group or not LOG_GROUP.fullmatch(str(arguments.get("logGroupName"))):
                raise Refusal("ResourceNotFoundException", "The specified log group does not exist.")
            if not LOG_STREAM.fullmatch(str(arguments.get("logStreamName", ""))):
                raise invalid("logStreamName must be 1 to 512 characters without a colon or an asterisk.")
            if "limit" in arguments and not 1 <= arguments["limit"] <= 10000:
                raise invalid("limit must be between 1 and 10000.")

    def __enter__(self) -> Bridge:
        self.thread.start()
        return self

    def __exit__(self, *_: object) -> None:
        self.shutdown()
        self.server_close()
        self.thread.join(timeout=10)
