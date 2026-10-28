"""Run closed SDK operations in one killable process group. Frames and service bodies are never logged."""

from __future__ import annotations

import argparse
import base64
import contextlib
import hashlib
import importlib.util
import io
import logging
import os
import queue
import resource
import select
import signal
import stat
import threading
import time
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, BinaryIO
from urllib.parse import urlsplit


def _protocol() -> Any:
    path = Path(__file__).with_name("worker_protocol.py")
    spec = importlib.util.spec_from_file_location("hai_worker_protocol", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("worker_protocol_unavailable")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


protocol = _protocol()
SERVICE_CODES = frozenset(
    {
        "AccessDenied",
        "AccessDeniedException",
        "InvalidParameterException",
        "InvalidParameterValue",
        "Throttling",
        "ThrottlingException",
        "SlowDown",
        "TooManyRequestsException",
        "InternalFailure",
        "InternalError",
        "ServiceUnavailable",
        "ServiceUnavailableException",
        "ParameterNotFound",
        "ParameterVersionNotFound",
        "NoSuchKey",
        "NoSuchVersion",
        "NoSuchUpload",
        "PreconditionFailed",
        "ConditionalRequestConflict",
        "ConflictException",
        "RequestTimeout",
        "ExpiredToken",
        "ExpiredTokenException",
        "InvalidClientTokenId",
        "ResourceNotFoundException",
    }
)
THROTTLE_CODES = frozenset({"Throttling", "ThrottlingException", "SlowDown", "TooManyRequestsException"})


class Refused(ValueError):
    """A closed operation failed a local file or identity contract."""


class PartReader(io.RawIOBase):
    """Expose one bounded byte range to the SDK without sending file bodies through frames."""

    def __init__(self, stream: BinaryIO, offset: int, length: int) -> None:
        self.stream = stream
        self.offset = offset
        self.length = length
        self.position = 0
        self.stream.seek(offset)

    def read(self, size: int = -1) -> bytes:
        """Read within the selected part."""
        count = self.length - self.position if size < 0 else min(size, self.length - self.position)
        chunk = self.stream.read(count)
        self.position += len(chunk)
        return chunk

    def seek(self, offset: int, whence: int = os.SEEK_SET) -> int:
        """Seek within the part for SDK checksum and signing passes."""
        position = offset if whence == os.SEEK_SET else self.position + offset if whence == os.SEEK_CUR else self.length + offset
        if not 0 <= position <= self.length:
            raise Refused("part_seek_outside_range")
        self.stream.seek(self.offset + position)
        self.position = position
        return position

    def tell(self) -> int:
        """Return the offset relative to the part."""
        return self.position

    def readable(self) -> bool:
        """Permit SDK reads."""
        return True

    def __len__(self) -> int:
        return self.length


def _identity(stream: BinaryIO, expected: Mapping[str, int]) -> None:
    status = os.fstat(stream.fileno())
    if not stat.S_ISREG(status.st_mode) or {"size": status.st_size, "mtime_ns": status.st_mtime_ns, "inode": status.st_ino} != dict(expected):
        raise Refused("source_file_identity_changed")


def _versioned(params: Mapping[str, Any]) -> dict[str, Any]:
    return {"Bucket": params["bucket"], "Key": params["key"], **({"VersionId": params["version_id"]} if "version_id" in params else {})}


def _date(response: Mapping[str, Any]) -> str:
    value = response.get("ResponseMetadata", {}).get("HTTPHeaders", {}).get("date")
    if not isinstance(value, str) or not value:
        raise Refused("service_date_missing")
    return value


def _failures(response: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [{name: row[name] for name in ("arn", "reason", "detail") if row.get(name)} for row in response.get("failures", [])]


class Sdk:
    """Keep clients only for normal reusable replies. Temporary sessions never enter this cache."""

    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.clients: dict[str, Any] = {}
        self.session: Any = None
        self.profile_checked = False

    def _session(self) -> Any:
        import boto3

        if self.session is None:
            if self.args.test_endpoint:
                self.session = boto3.Session("synthetic_access_key", "synthetic_secret_key", region_name=self.args.region)
            else:
                self.session = boto3.Session(profile_name=self.args.profile, region_name=self.args.region)
        if not self.profile_checked:
            profiles = self.session._session.full_config.get("profiles", {})
            profile = self.session.profile_name
            visited = set()
            while profile:
                if profile in visited:
                    raise Refused("credential_profile_cycle")
                visited.add(profile)
                configured = profiles.get(profile, {})
                if "credential_process" in configured:
                    raise Refused("credential_process_refused")
                profile = configured.get("source_profile")
            self.profile_checked = True
        return self.session

    def _client(self, operation: str, params: Mapping[str, Any]) -> Any:
        from botocore.config import Config

        service = (
            "s3"
            if operation.startswith("s3_")
            else "sts"
            if operation in {"assume_role", "caller_identity"}
            else "ssm"
            if operation == "get_parameter"
            else "logs"
            if operation == "logs_get_events"
            else "ecs"
        )
        endpoint = self.args.test_control_endpoint if self.args.test_control_endpoint and operation in protocol.CONTROL_OPERATIONS else self.args.test_endpoint
        config = Config(retries={"total_max_attempts": 1, "mode": "standard"}, connect_timeout=3, read_timeout=30, s3={"addressing_style": "path"})
        session = params.get("session")
        if session:
            expiration = datetime.fromisoformat(session["expiration_utc"].replace("Z", "+00:00"))
            if expiration.tzinfo is None or expiration <= datetime.now(UTC):
                raise Refused("session_expired")
            return self._session().client(
                service,
                endpoint_url=endpoint,
                config=config,
                aws_access_key_id=session["access_key_id"],
                aws_secret_access_key=session["secret_access_key"],
                aws_session_token=session["session_token"],
            )
        key = service + (endpoint or "")
        if key not in self.clients:
            self.clients[key] = self._session().client(service, endpoint_url=endpoint, config=config)
        return self.clients[key]

    def call(self, operation: str, params: Mapping[str, Any]) -> dict[str, Any]:
        """Run exactly one SDK request using the validated closed operation."""
        self.request_entered = False
        client = self._client(operation, params)
        try:
            try:
                return self._execute(client, operation, params)
            finally:
                if "session" in params:
                    client.close()
        except (OSError, ValueError, KeyError, TypeError, OverflowError):
            if self.request_entered and operation in protocol.MUTATING:
                raise protocol.ProtocolError("operation_result_mismatch") from None
            raise

    def _request(self, client: Any, method: str, **params: Any) -> Any:
        request = getattr(client, method)
        self.request_entered = True
        return request(**params)

    def _execute(self, client: Any, operation: str, p: Mapping[str, Any]) -> dict[str, Any]:
        if operation == "run_task":
            response = self._request(client, "run_task", **p)
            return {"task_arns": [row["taskArn"] for row in response.get("tasks", [])], "failures": _failures(response)}
        if operation == "stop_task":
            row = self._request(client, "stop_task", **p)["task"]
            return {"task_arn": row["taskArn"], "last_status": row["lastStatus"]}
        if operation == "describe_tasks":
            response = self._request(client, "describe_tasks", **p)
            tasks = []
            for row in response.get("tasks", []):
                item = {
                    "task_arn": row["taskArn"],
                    "started_by": row.get("startedBy", ""),
                    "group": row.get("group", ""),
                    "last_status": row["lastStatus"],
                    "containers": [
                        {
                            **{"name": child["name"]},
                            **({"exit_code": child["exitCode"]} if "exitCode" in child else {}),
                            **({"reason": child["reason"]} if child.get("reason") else {}),
                        }
                        for child in row.get("containers", [])
                    ],
                    "tags": row.get("tags", []),
                }
                item.update(
                    {destination: row[source] for source, destination in (("stopCode", "stop_code"), ("stoppedReason", "stopped_reason")) if row.get(source)}
                )
                tasks.append(item)
            return {"tasks": tasks, "failures": _failures(response)}
        if operation == "list_tasks":
            response = self._request(client, "list_tasks", **p)
            return {"task_arns": response.get("taskArns", []), **({"next_token": response["nextToken"]} if response.get("nextToken") else {})}
        if operation == "assume_role":
            response = self._request(
                client, "assume_role", RoleArn=p["role_arn"], RoleSessionName=p["session_name"], Policy=p["policy_json"], DurationSeconds=p["duration_seconds"]
            )
            row = response["Credentials"]
            return {
                "credentials": {
                    "access_key_id": row["AccessKeyId"],
                    "secret_access_key": row["SecretAccessKey"],
                    "session_token": row["SessionToken"],
                    "expiration_utc": row["Expiration"].isoformat(),
                },
                "date_utc": _date(response),
            }
        if operation == "caller_identity":
            response = self._request(client, "get_caller_identity")
            return {"account": response["Account"], "arn": response["Arn"], "user_id": response["UserId"], "date_utc": _date(response)}
        if operation == "get_parameter":
            row = self._request(client, "get_parameter", Name=p["name"], WithDecryption=True)["Parameter"]
            name, version = p["name"].rsplit(":", 1)
            if row.get("Name") != name or row.get("Version") != int(version):
                raise protocol.ProtocolError("parameter_version_mismatch")
            return {"name": name, "version": int(version), "value": row["Value"]}
        if operation == "logs_get_events":
            response = self._request(
                client,
                "get_log_events",
                logGroupName=p["log_group"],
                logStreamName=p["log_stream"],
                limit=1000,
                **({"nextToken": p["next_token"]} if "next_token" in p else {}),
            )
            return {
                "events": [{"timestamp_ms": row["timestamp"], "message": row["message"]} for row in response.get("events", [])],
                **({"next_token": response["nextForwardToken"]} if response.get("nextForwardToken") else {}),
            }
        if operation == "s3_head":
            row = self._request(client, "head_object", **_versioned(p))
            return {"version_id": row["VersionId"], "size": row["ContentLength"], "sha256": row["Metadata"]["sha256"]}
        if operation == "s3_list":
            row = self._request(
                client,
                "list_objects_v2",
                Bucket=p["bucket"],
                Prefix=p["prefix"],
                MaxKeys=1000,
                **({"ContinuationToken": p["continuation_token"]} if "continuation_token" in p else {}),
            )
            return {
                "objects": [{"key": item["Key"], "size": item["Size"]} for item in row.get("Contents", [])],
                **({"next_token": row["NextContinuationToken"]} if row.get("NextContinuationToken") else {}),
            }
        if operation == "s3_put_small":
            body = p["body_utf8"].encode("utf-8")
            digest = hashlib.sha256(body).hexdigest()
            if digest != p["sha256"]:
                raise Refused("body_digest_mismatch")
            row = self._request(
                client,
                "put_object",
                Bucket=p["bucket"],
                Key=p["key"],
                Body=body,
                IfNoneMatch="*",
                Metadata={"sha256": digest},
                ChecksumSHA256=base64.b64encode(bytes.fromhex(digest)).decode(),
            )
            return {"version_id": row["VersionId"], "size": len(body), "sha256": digest}
        if operation == "s3_get_small":
            row = self._request(client, "get_object", **_versioned(p))
            with contextlib.closing(row["Body"]) as stream:
                body = stream.read(protocol.MAX_FRAME + 1)
            if len(body) > protocol.MAX_FRAME:
                raise Refused("small_object_limit")
            return {"body_utf8": body.decode("utf-8", "strict"), "version_id": row["VersionId"], "size": len(body), "sha256": hashlib.sha256(body).hexdigest()}
        if operation == "s3_download":
            return self._download(client, p)
        if operation == "s3_upload_part":
            return self._upload_part(client, p)
        if operation == "s3_mpu_create":
            row = self._request(
                client, "create_multipart_upload", Bucket=p["bucket"], Key=p["key"], Metadata={"sha256": p["sha256"]}, ChecksumAlgorithm="SHA256"
            )
            return {"upload_id": row["UploadId"]}
        if operation == "s3_mpu_complete":
            row = self._request(
                client,
                "complete_multipart_upload",
                Bucket=p["bucket"],
                Key=p["key"],
                UploadId=p["upload_id"],
                MultipartUpload={
                    "Parts": [{"PartNumber": part["part_number"], "ETag": part["etag"], "ChecksumSHA256": part["checksum_sha256"]} for part in p["parts"]]
                },
                IfNoneMatch="*",
            )
            return {"version_id": row["VersionId"], "etag": row["ETag"]}
        if operation == "s3_mpu_abort":
            self._request(client, "abort_multipart_upload", Bucket=p["bucket"], Key=p["key"], UploadId=p["upload_id"])
            return {"aborted": True}
        if operation == "s3_mpu_list":
            row = self._request(
                client,
                "list_multipart_uploads",
                Bucket=p["bucket"],
                Prefix=p["prefix"],
                MaxUploads=1000,
                **({"KeyMarker": p["key_marker"]} if "key_marker" in p else {}),
                **({"UploadIdMarker": p["upload_id_marker"]} if "upload_id_marker" in p else {}),
            )
            return {
                "uploads": [{"key": item["Key"], "upload_id": item["UploadId"]} for item in row.get("Uploads", [])],
                **({"next_key_marker": row["NextKeyMarker"]} if row.get("NextKeyMarker") else {}),
                **({"next_upload_id_marker": row["NextUploadIdMarker"]} if row.get("NextUploadIdMarker") else {}),
            }
        if operation == "s3_delete":
            row = self._request(client, "delete_object", Bucket=p["bucket"], Key=p["key"])
            return {"delete_marker": row.get("DeleteMarker", False), "version_id": row["VersionId"]}
        raise Refused("unknown_operation")

    def _download(self, client: Any, p: Mapping[str, Any]) -> dict[str, Any]:
        path = Path(p["temp_path"])
        if not path.is_absolute() or path.name in {"", ".", ".."}:
            raise Refused("download_path_must_be_absolute")
        parent = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
        created: int | None = None
        try:
            status = os.fstat(parent)
            if status.st_uid != os.getuid() or status.st_mode & 0o077:
                raise Refused("download_parent_must_be_private")
            fd = os.open(path.name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=parent)
            created = os.fstat(fd).st_ino
            digest = hashlib.sha256()
            size = 0
            with os.fdopen(fd, "wb") as output:
                row = self._request(client, "get_object", **_versioned(p))
                with contextlib.closing(row["Body"]) as stream:
                    if row.get("VersionId") != p["version_id"]:
                        raise Refused("object_version_mismatch")
                    while chunk := stream.read(1024 * 1024):
                        size += len(chunk)
                        if size > p["expected_size"]:
                            raise Refused("download_size_mismatch")
                        output.write(chunk)
                        digest.update(chunk)
                output.flush()
                os.fsync(output.fileno())
            if size != p["expected_size"] or digest.hexdigest() != p["expected_sha256"]:
                raise Refused("download_integrity_mismatch")
            return {"temp_path": str(path), "version_id": row["VersionId"], "size": size, "sha256": digest.hexdigest()}
        except BaseException:
            if created is not None:
                with contextlib.suppress(FileNotFoundError):
                    if os.stat(path.name, dir_fd=parent, follow_symlinks=False).st_ino == created:
                        os.unlink(path.name, dir_fd=parent)
            raise
        finally:
            os.close(parent)

    def _upload_part(self, client: Any, p: Mapping[str, Any]) -> dict[str, Any]:
        fd = os.open(p["path"], os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
        with os.fdopen(fd, "rb") as source:
            _identity(source, p["file_identity"])
            if p["offset"] + p["length"] > p["file_identity"]["size"]:
                raise Refused("part_outside_file")
            part = PartReader(source, p["offset"], p["length"])
            digest = hashlib.sha256()
            while chunk := part.read(1024 * 1024):
                digest.update(chunk)
            if part.tell() != p["length"]:
                raise Refused("source_file_short_read")
            _identity(source, p["file_identity"])
            part.seek(0)
            checksum = base64.b64encode(digest.digest()).decode()
            row = self._request(
                client,
                "upload_part",
                Bucket=p["bucket"],
                Key=p["key"],
                UploadId=p["upload_id"],
                PartNumber=p["part_number"],
                Body=part,
                ContentLength=p["length"],
                ChecksumSHA256=checksum,
            )
            _identity(source, p["file_identity"])
            return {"part_number": p["part_number"], "etag": row["ETag"], "checksum_sha256": checksum}


class Monitor:
    """Read the control pipe independently of any SDK provider or streaming body."""

    def __init__(self) -> None:
        self.requests: queue.Queue[dict[str, Any]] = queue.Queue(maxsize=1)
        self.lock = threading.Lock()
        self.busy = False
        self.deadline = time.monotonic() + 60
        self.thread = threading.Thread(target=self.run, name="sdk_worker_control", daemon=True)

    def run(self) -> None:
        """Kill this worker's own group on EOF, incomplete frames, overlap or its fixed expiry."""
        try:
            while True:
                with self.lock:
                    deadline = self.deadline
                if time.monotonic() >= deadline:
                    break
                readable, _, _ = select.select([0], [], [], min(0.25, max(0.0, deadline - time.monotonic())))
                if not readable:
                    continue
                frame = protocol.read_frame(0, min(deadline, time.monotonic() + 2))
                protocol.validate(frame)
                with self.lock:
                    if self.busy:
                        break
                    self.busy = True
                    self.deadline = time.monotonic() + frame["budget_s"] + 2
                self.requests.put_nowait(frame)
        except (OSError, EOFError, ValueError, TimeoutError, queue.Full):
            pass
        os.killpg(os.getpgrp(), signal.SIGKILL)

    def send_reply(self, reply: dict[str, Any]) -> None:
        """Publish a reply and clear the in-flight state in one monitor critical section."""
        with self.lock:
            protocol.write_frame(1, reply, time.monotonic() + 2)
            self.busy = False
            self.deadline = time.monotonic() + 60


def _endpoint(value: str) -> str:
    parsed = urlsplit(value)
    if (
        parsed.scheme != "http"
        or parsed.hostname != "127.0.0.1"
        or parsed.username
        or parsed.password
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
        or not parsed.port
    ):
        raise argparse.ArgumentTypeError("Synthetic endpoints must be loopback HTTP listeners.")
    return value


def main() -> int:
    """Run the isolated worker. Its parent owns signals, reaping and recovery."""
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    logging.disable(logging.CRITICAL)
    for fd in (0, 1, 2):
        os.set_inheritable(fd, False)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hai-worker", required=True)
    parser.add_argument("--region", required=True)
    parser.add_argument("--profile")
    parser.add_argument("--test-endpoint", type=_endpoint)
    parser.add_argument("--test-control-endpoint", type=_endpoint)
    parser.add_argument("--one-shot", action="store_true")
    args = parser.parse_args()
    if args.test_control_endpoint and not args.test_endpoint:
        return 70
    monitor = Monitor()
    monitor.thread.start()
    sdk = Sdk(args)
    from botocore.exceptions import BotoCoreError, ClientError

    while True:
        request = monitor.requests.get(timeout=61)
        operation = request["op"]
        request_id = request["id"]
        try:
            result = sdk.call(operation, request["params"])
            reply = {"id": request_id, "ok": True, "result": result}
            protocol.validate(reply, operation, request_id)
        except ClientError as error:
            metadata = error.response.get("ResponseMetadata", {})
            raw_code = error.response.get("Error", {}).get("Code", "")
            code = raw_code if raw_code in SERVICE_CODES else "service_error"
            kind = "throttle" if raw_code in THROTTLE_CODES else "service"
            reply = protocol.error_reply(
                request_id,
                operation,
                kind,
                code,
                http_status=metadata.get("HTTPStatusCode"),
                request_id_hash=protocol.request_hash(metadata.get("RequestId", "")),
            )
        except BotoCoreError:
            reply = protocol.error_reply(request_id, operation, "network", "sdk_network_error")
        except protocol.ProtocolError:
            reply = protocol.error_reply(request_id, operation, "protocol", "operation_result_mismatch")
        except Refused as error:
            code = str(error)
            reply = protocol.error_reply(request_id, operation, "bad_request", code)
        except (OSError, ValueError, KeyError, TypeError, OverflowError):
            reply = protocol.error_reply(request_id, operation, "bad_request", "operation_contract_failed")
        protocol.validate(reply, operation, request_id)
        monitor.send_reply(reply)
        if args.one_shot or protocol.secret_bearing(operation, request["params"]) or reply.get("error", {}).get("kind") in {"network", "protocol"}:
            return 0


if __name__ == "__main__":
    try:
        os._exit(main())
    except (OSError, ValueError, TimeoutError, EOFError, queue.Empty):
        os._exit(70)
