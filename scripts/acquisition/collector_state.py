"""Conditional, version-pinned collector state selected explicitly by an admitted caller."""

from __future__ import annotations

import hashlib
import tempfile
import time
from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from pathlib import Path, PurePosixPath
from typing import Any, Protocol

from scripts.acquisition.s3_store import AwsCli, StorageError, encoded_json, write_once

PREFIX = "acquisition_state/v1/"
ADMISSION_KEY = PREFIX + "collectors/admission.json"
QUOTA_KEYS = {name: PREFIX + f"quotas/{name}.json" for name in ("bls_v1", "bls_v2")}
CAPS = {"bls_v1": 25, "bls_v2": 450}


class StateError(ValueError):
    """Durable state cannot authorize another collector effect."""


class StateMissing(StateError):
    """A required state object or pinned version is missing."""


class StateConflict(StateError):
    """A conditional write lost its comparison and cannot overwrite the winner."""


class StateUnavailable(StateError):
    """Storage is unavailable within the current absolute deadline."""

    def __init__(self, message: str, *, retryable: bool = False) -> None:
        super().__init__(message)
        self.retryable = retryable


class UnconfirmedWrite(StateError):
    """A write may have succeeded and requires authoritative readback."""


class QuotaExceeded(StateError):
    """The separate rolling quota cannot admit the requested weight."""


def require(condition: bool, message: str) -> None:
    """Refuse invalid state with a value-free diagnostic."""
    if not condition:
        raise StateError(message)


def digest(body: bytes) -> str:
    """Return a byte digest independent of S3 ETags."""
    return hashlib.sha256(body).hexdigest()


def document(body: bytes) -> dict[str, Any]:
    """Parse strict finite state JSON without echoing input values."""
    from scripts.acquisition.source_registry import parse_json

    try:
        return parse_json(body, "collector state")
    except ValueError:
        raise StateError("Invalid state JSON") from None


def relative_path(value: str) -> str:
    """Validate a declared metadata path before any storage or filesystem operation."""
    path = PurePosixPath(value)
    require(
        bool(value)
        and path.as_posix() == value
        and not path.is_absolute()
        and ".." not in path.parts
        and all(part not in {"private_original", "raw", "secrets", ".aws"} and not part.startswith(".") for part in path.parts)
        and path.suffix == ".json",
        "State path is not permitted",
    )
    return value


@dataclass(frozen=True)
class StoredObject:
    """Bytes returned from one exact version, with an opaque conditional-write token."""

    key: str
    version_id: str
    etag: str
    body: bytes

    def reference(self) -> dict[str, Any]:
        """Return the immutable version, hash and size used in a checkpoint."""
        return {"key": self.key, "version_id": self.version_id, "sha256": digest(self.body), "bytes": len(self.body)}


class ObjectStore(Protocol):
    """A versioned store with mandatory conditions and an absolute call deadline."""

    def get(self, key: str, version_id: str | None = None, *, deadline: float) -> StoredObject: ...
    def put(self, key: str, body: bytes, *, if_match: str | None, if_none_match: bool, deadline: float) -> StoredObject: ...


class S3StateStore:
    """Use the existing scoped CLI adapter for state writes and exact-version reads."""

    def __init__(self, client: AwsCli) -> None:
        self.client = client

    def _call(self, operation: str, arguments: list[str], deadline: float) -> dict:
        remaining = deadline - time.monotonic()
        require(remaining > 0, "State storage deadline expired")
        try:
            return self.client.call_bounded("s3api", operation, arguments, timeout=min(180, remaining))
        except StorageError as error:
            if error.code in {"NoSuchKey", "NoSuchVersion"}:
                raise StateMissing("Pinned state object missing") from None
            if error.code in {"PreconditionFailed", "ConditionalRequestConflict"}:
                raise StateConflict("Conditional state write conflicted") from None
            if operation == "put-object" and error.code in {"request_unconfirmed", "RequestTimeout", "InternalError", "ServiceUnavailable"}:
                raise UnconfirmedWrite("State write unconfirmed") from None
            raise StateUnavailable(
                "State storage unavailable", retryable=error.code in {"SlowDown", "InternalError", "ServiceUnavailable", "request_unconfirmed"}
            ) from None

    def get(self, key: str, version_id: str | None = None, *, deadline: float) -> StoredObject:
        """Read the requested version and reject an absent version identifier."""
        with tempfile.TemporaryDirectory(prefix="collector_state_get_") as folder:
            output = Path(folder) / "body"
            arguments = ["--key", key, *(["--version-id", version_id] if version_id else []), str(output)]
            result = self._call("get-object", arguments, deadline)
            require(result.get("VersionId") not in {None, "", "null"} and bool(result.get("ETag")), "State object version missing")
            require(version_id is None or result["VersionId"] == version_id, "State object version differs")
            return StoredObject(key, result["VersionId"], result["ETag"], output.read_bytes())

    def put(self, key: str, body: bytes, *, if_match: str | None, if_none_match: bool, deadline: float) -> StoredObject:
        """Write only control-state keys using exactly one conditional-write mode."""
        require(key.startswith(PREFIX) and ".." not in PurePosixPath(key).parts, "State write prefix is not permitted")
        require(if_none_match != (if_match is not None), "State write requires one condition")
        with tempfile.TemporaryDirectory(prefix="collector_state_put_") as folder:
            source = Path(folder) / "body"
            source.write_bytes(body)
            condition = ["--if-none-match", "*"] if if_none_match else ["--if-match", str(if_match)]
            token = Path(folder).name
            try:
                result = self._call("put-object", ["--key", key, "--body", str(source), "--metadata", f"collector-state-write={token}", *condition], deadline)
            except StateConflict:
                output = Path(folder) / "readback"
                result = self._call("get-object", ["--key", key, str(output)], deadline)
                if result.get("Metadata", {}).get("collector-state-write") != token or output.read_bytes() != body:
                    raise
            require(result.get("VersionId") not in {None, "", "null"} and bool(result.get("ETag")), "State object version missing")
            return StoredObject(key, result["VersionId"], result["ETag"], body)


@dataclass(frozen=True)
class StateBinding:
    """Reviewed collector identity, logical root and explicit permitted file inventory."""

    authority_epoch: str
    collector_id: str
    logical_root: str
    plan_sha256: str
    code_sha256: str
    allowed_paths: tuple[str, ...]

    def record(self) -> dict[str, Any]:
        """Return the canonical binding verified on every resume and checkpoint."""
        from scripts.acquisition.s3_store import component

        component(self.collector_id)
        require(bool(self.authority_epoch) and bool(self.logical_root), "State binding missing")
        require(
            all(len(value) == 64 and all(c in "0123456789abcdef" for c in value) for value in (self.plan_sha256, self.code_sha256)),
            "State binding hash invalid",
        )
        require(len(set(self.allowed_paths)) == len(self.allowed_paths), "State permitted paths repeat")
        result = asdict(self)
        result["allowed_paths"] = sorted(relative_path(path) for path in self.allowed_paths)
        return result


@dataclass(frozen=True)
class Owner:
    """The controller-admitted submission and exact task identity."""

    submission_id: str
    task_arn: str

    def record(self) -> dict[str, str]:
        """Return the owner carried by both collector and family admission heads."""
        require(bool(self.submission_id) and bool(self.task_arn), "State owner missing")
        return asdict(self)


@dataclass(frozen=True)
class StateFile:
    """One declared metadata file or permitted public transport envelope."""

    path: str
    body: bytes
    kind: str = "metadata"
    required_versions: tuple[dict[str, Any], ...] = ()


class CollectorState:
    """Own conditional state and reservations without granting network or deployment authority."""

    def __init__(
        self,
        store: ObjectStore,
        binding: StateBinding,
        owner: Owner,
        root: Path,
        deadline: float,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.store, self.binding, self.owner, self.root = store, binding, owner, root.absolute()
        self.deadline, self.clock, self.sleep = deadline, clock, sleep
        self.bound = binding.record()
        self.owner_record = owner.record()
        self.head_key = PREFIX + f"collectors/{binding.collector_id}/heads/current.json"
        self.head: dict[str, Any] = {}
        self.manifest: dict[str, Any] = {}
        self.head_object: StoredObject | None = None
        self.blocked = False

    def _time_left(self) -> float:
        remaining = self.deadline - self.clock()
        require(remaining > 0, "Collector state deadline expired; admission retained")
        return remaining

    def _read(self, key: str, version_id: str | None = None) -> StoredObject:
        for attempt in range(3):
            self._time_left()
            try:
                value = self.store.get(key, version_id, deadline=self.deadline)
                require(value.key == key and value.version_id not in {None, "", "null"} and bool(value.etag), "State object version missing")
                require(version_id is None or value.version_id == version_id, "State object version differs")
                return value
            except StateUnavailable as error:
                if not error.retryable or attempt == 2:
                    self.blocked = True
                    raise
                self.sleep(min(0.05 * 2**attempt, self._time_left()))
        raise StateError("State read retry exhausted")

    def _verify(self, reference: dict[str, Any]) -> StoredObject:
        require(set(reference) == {"key", "version_id", "sha256", "bytes"}, "State version reference invalid")
        require(isinstance(reference["version_id"], str) and reference["version_id"] not in {"", "null"}, "State object version missing")
        value = self._read(reference["key"], reference["version_id"])
        require(value.reference() == reference, "State checksum or byte count differs")
        return value

    def _put(self, key: str, body: bytes, etag: str | None = None) -> StoredObject:
        for attempt in range(3):
            self._time_left()
            try:
                result = self.store.put(key, body, if_match=etag, if_none_match=etag is None, deadline=self.deadline)
                verified = self._read(key, result.version_id)
                require(verified.body == body and verified.etag == result.etag, "State write readback checksum differs")
                return verified
            except StateConflict:
                self.blocked = True
                self._read(key)
                raise
            except UnconfirmedWrite:
                try:
                    observed = self._read(key)
                    require(observed.body == body, "State write unconfirmed; authoritative value differs")
                    return self._verify(observed.reference())
                except StateError:
                    self.blocked = True
                    raise StateError("State write unconfirmed; admission retained") from None
            except StateUnavailable as error:
                if not error.retryable or attempt == 2:
                    self.blocked = True
                    raise
                self.sleep(min(0.05 * 2**attempt, self._time_left()))
        raise StateError("State write retry exhausted")

    def _immutable(self, key: str, body: bytes) -> StoredObject:
        try:
            existing = self._read(key)
        except StateMissing:
            return self._put(key, body)
        require(existing.body == body, "Immutable state content differs")
        return self._verify(existing.reference())

    def _seed(self, key: str, value: dict, fields: tuple[str, ...]) -> StoredObject:
        try:
            existing = self._read(key)
        except StateMissing:
            return self._put(key, encoded_json(value))
        observed = document(existing.body)
        require(all(observed.get(field) == value[field] for field in fields), "Prepared state binding or quota import differs")
        return existing

    def _reservations(self, ledger: str, entries: list[dict], now: datetime) -> int:
        require(now.tzinfo is not None and now.utcoffset() is not None, "BLS quota ledger clock invalid")
        used, seen = 0, set()
        for item in entries:
            try:
                stamp = datetime.fromisoformat(item["reserved_at_utc"])
                identity = item["reservation_id"]
                weight = item["reserved_requests"] if ledger == "bls_v1" else 1
                require(stamp.tzinfo is not None and stamp <= now, "BLS quota ledger clock invalid")
                require(isinstance(identity, str) and bool(identity) and identity not in seen, "Quota reservation identity invalid")
                require(type(weight) is int and weight > 0, "Quota reservation weight invalid")
            except (KeyError, TypeError, ValueError) as error:
                if isinstance(error, StateError):
                    raise
                raise StateError("BLS quota ledger clock or reservation invalid") from None
            seen.add(identity)
            if stamp > now - timedelta(hours=24):
                used += weight
        return used

    def prepare(self, bls_v1: list[dict], bls_v2: list[dict], *, inventory_complete: bool, now: datetime, files: Iterable[StateFile] = ()) -> None:
        """Import reviewed quota history into non-admitting state, never as implicit recovery."""
        require(inventory_complete, "A complete quota inventory is required before bootstrap")
        files = list(files)
        require(len({item.path for item in files}) == len(files), "Checkpoint permitted paths repeat")
        for item in files:
            self._file(item)
        try:
            admission = document(self._read(ADMISSION_KEY).body)
        except StateMissing:
            try:
                prior = document(self._read(self.head_key).body)
            except StateMissing:
                pass
            else:
                require(prior.get("status") == "idle" and prior.get("generation") == 0, "Admission state missing; reviewed recovery required")
        else:
            require(
                admission.get("authority_epoch") == self.binding.authority_epoch and isinstance(admission.get("collectors"), list),
                "Admission authority binding differs",
            )
            for key in QUOTA_KEYS.values():
                self._read(key)
            if self.binding.collector_id in admission["collectors"]:
                self._read(self.head_key)
        for ledger, entries in (("bls_v1", bls_v1), ("bls_v2", bls_v2)):
            self._reservations(ledger, entries, now)
        for ledger, entries in (("bls_v1", bls_v1), ("bls_v2", bls_v2)):
            value = {
                "schema_version": 1,
                "authority_epoch": self.binding.authority_epoch,
                "ledger": ledger,
                "import_sha256": digest(encoded_json({"reservations": entries})),
                "reservations": entries,
                "last_reserved_at_utc": now.isoformat(),
            }
            self._seed(QUOTA_KEYS[ledger], value, ("schema_version", "authority_epoch", "ledger", "import_sha256"))
        entries = []
        for item in sorted(files, key=lambda value: value.path):
            category = "public_cache" if item.kind == "public_cache" else "artifacts"
            stored = self._immutable(PREFIX + f"collectors/{self.binding.collector_id}/{category}/{digest(item.body)}.json", item.body)
            entries.append({"path": item.path, "kind": item.kind, "object": stored.reference(), "required_versions": list(item.required_versions)})
        manifest = {"schema_version": 1, "binding": self.bound, "generation": 0, "previous_checkpoint": None, "files": entries}
        checkpoint = self._immutable(
            PREFIX + f"collectors/{self.binding.collector_id}/checkpoints/0/{digest(encoded_json(manifest))}.json", encoded_json(manifest)
        )
        head = {
            "schema_version": 1,
            "binding": self.bound,
            "generation": 0,
            "previous_checkpoint": None,
            "checkpoint": checkpoint.reference(),
            "bootstrap_sha256": digest(encoded_json(manifest)),
            "owner": None,
            "status": "idle",
        }
        self._seed(self.head_key, head, ("schema_version", "binding", "bootstrap_sha256"))
        slot = self._seed(
            ADMISSION_KEY,
            {
                "schema_version": 1,
                "authority_epoch": self.binding.authority_epoch,
                "owner": None,
                "collector_id": None,
                "collectors": [self.binding.collector_id],
                "status": "idle",
            },
            ("schema_version", "authority_epoch"),
        )
        admission = document(slot.body)
        if self.binding.collector_id not in admission["collectors"]:
            admission["collectors"] = sorted([*admission["collectors"], self.binding.collector_id])
            self._put(ADMISSION_KEY, encoded_json(admission), slot.etag)

    def _head(self) -> tuple[StoredObject, dict[str, Any]]:
        value = self._read(self.head_key)
        record = document(value.body)
        require(record.get("schema_version") == 1 and record.get("binding") == self.bound, "Collector state binding differs")
        return value, record

    def _load_manifest(self, head: dict) -> dict[str, Any]:
        manifest = document(self._verify(head["checkpoint"]).body)
        require(manifest.get("schema_version") == 1 and manifest.get("binding") == self.bound, "Checkpoint binding differs")
        require(
            manifest.get("generation") == head["generation"] and manifest.get("previous_checkpoint") == head["previous_checkpoint"],
            "Checkpoint generation differs",
        )
        require(isinstance(manifest.get("files"), list), "Checkpoint file inventory invalid")
        return manifest

    def _verified_files(self, manifest: dict) -> dict[str, bytes]:
        ready: dict[str, bytes] = {}
        for entry in manifest["files"]:
            require(set(entry) == {"path", "kind", "object", "required_versions"}, "Unexpected checkpoint manifest entry")
            item = StateFile(entry["path"], self._verify(entry["object"]).body, entry["kind"], tuple(entry["required_versions"]))
            self._file(item)
            require(item.path not in ready, "Checkpoint permitted paths repeat")
            ready[item.path] = item.body
        return ready

    def claim(self) -> None:
        """Claim an idle family and collector using observed ETags, without a lease expiry."""
        value, head = self._head()
        slot = self._read(ADMISSION_KEY)
        admission = document(slot.body)
        require(
            admission.get("authority_epoch") == self.binding.authority_epoch and self.binding.collector_id in admission.get("collectors", []),
            "Admission authority binding differs",
        )
        recovering = (
            admission.get("owner") == self.owner_record
            and admission.get("collector_id") == self.binding.collector_id
            and admission.get("status") != "releasing"
        )
        require(
            (admission.get("owner") is None or recovering) and head.get("owner") is None and head.get("status") == "idle",
            "Collector already active; admission retained",
        )
        self._verified_files(self._load_manifest(head))
        if not recovering:
            admission.update(owner=self.owner_record, collector_id=self.binding.collector_id, status="active")
            self._put(ADMISSION_KEY, encoded_json(admission), slot.etag)
        head.update(owner=self.owner_record, status="active")
        self.head_object = self._put(self.head_key, encoded_json(head), value.etag)
        self.head, self.manifest = head, self._load_manifest(head)

    def resume(self) -> None:
        """Attach the same admitted owner to pinned durable state after losing scratch."""
        value, head = self._head()
        slot = document(self._read(ADMISSION_KEY).body)
        if (
            head.get("owner") is None
            and head.get("status") == "idle"
            and slot.get("owner") == self.owner_record
            and slot.get("collector_id") == self.binding.collector_id
            and slot.get("status") != "releasing"
        ):
            self.claim()
            return
        require(head.get("owner") == self.owner_record and head.get("status") == "active", "Collector owner binding differs")
        require(
            slot.get("status") != "releasing"
            and slot.get("authority_epoch") == self.binding.authority_epoch
            and slot.get("owner") == self.owner_record
            and slot.get("collector_id") == self.binding.collector_id,
            "Admission owner binding differs",
        )
        manifest = self._load_manifest(head)
        self._verified_files(manifest)
        self.head_object, self.head, self.manifest = value, head, manifest

    def _owned(self) -> None:
        require(not self.blocked, "Collector state blocked; reconcile before further effects")
        value, head = self._head()
        require(self.head_object is not None and value.etag == self.head_object.etag, "Collector state changed; reconcile before further effects")
        require(head.get("owner") == self.owner_record and head.get("status") == "active", "Collector owner binding differs")
        slot = document(self._read(ADMISSION_KEY).body)
        require(
            slot.get("status") != "releasing"
            and slot.get("owner") == self.owner_record
            and slot.get("collector_id") == self.binding.collector_id
            and slot.get("authority_epoch") == self.binding.authority_epoch,
            "Admission owner binding differs",
        )

    def verify_active(self, root: Path) -> None:
        """Verify the admitted owner and exact scratch mapping before collector effects."""
        require(root.absolute() == self.root, "Collector scratch root binding differs")
        self._owned()

    def read_file(self, path: str) -> bytes | None:
        """Return the pinned bytes of one declared checkpoint file when it exists."""
        self._owned()
        for entry in self.manifest["files"]:
            if entry["path"] == path:
                return self._verify(entry["object"]).body
        return None

    def reserve(self, ledger: str, identity: str, limit: int, now: datetime, *, weight: int = 1) -> str:
        """Commit a conservative quota charge before HTTP, preserving both ledger caps."""
        self._owned()
        require(ledger in CAPS and type(limit) is int and 0 < limit <= CAPS[ledger], "BLS quota cap differs")
        require(bool(identity) and type(weight) is int and weight > 0 and (ledger == "bls_v1" or weight == 1), "BLS reservation weight invalid")
        self._verified_files(self.manifest)
        stored = self._read(QUOTA_KEYS[ledger])
        quota = document(stored.body)
        require(
            quota.get("schema_version") == 1 and quota.get("authority_epoch") == self.binding.authority_epoch and quota.get("ledger") == ledger,
            "Quota authority binding differs",
        )
        used = self._reservations(ledger, quota["reservations"], now)
        require(datetime.fromisoformat(quota["last_reserved_at_utc"]) <= now, "BLS quota ledger clock regressed")
        if used + weight > limit:
            raise QuotaExceeded("BLS rolling 24-hour request budget exhausted; no request sent")
        identifier = digest(encoded_json({"owner": self.owner_record, "ledger": ledger, "identity": identity, "previous_version_id": stored.version_id}))
        item: dict[str, Any] = {"reservation_id": identifier, "batch_id": identity, "reserved_at_utc": now.isoformat()}
        if ledger == "bls_v1":
            item["reserved_requests"] = weight
        quota["reservations"] = [entry for entry in quota["reservations"] if datetime.fromisoformat(entry["reserved_at_utc"]) > now - timedelta(hours=24)]
        quota["reservations"].append(item)
        quota["last_reserved_at_utc"] = now.isoformat()
        self._put(QUOTA_KEYS[ledger], encoded_json(quota), stored.etag)
        return identifier

    def _file(self, item: StateFile) -> None:
        path = relative_path(item.path)
        require(path in self.binding.allowed_paths and item.kind in {"metadata", "public_cache", "completion"}, "State file is not permitted")
        value = document(item.body)
        if item.kind == "public_cache":
            require(path.startswith("batches/") and path.endswith("/transport.json"), "Public cache path is not permitted")
            require(set(value) == {"endpoint", "request", "retrieved_at_utc", "sha256", "bytes", "body_hex", "http_status"}, "Public cache metadata differs")
            try:
                body = bytes.fromhex(value["body_hex"])
            except (TypeError, ValueError):
                raise StateError("Public cache metadata invalid") from None
            require(digest(body) == value["sha256"] and len(body) == value["bytes"], "Public cache checksum differs")
        else:

            def permitted(content: Any) -> bool:
                if isinstance(content, dict):
                    return all(
                        str(key).lower() not in {"body_hex", "registrationkey", "api_key", "password", "secret", "token", "credential", "cookie"}
                        and permitted(value)
                        for key, value in content.items()
                    )
                return all(permitted(value) for value in content) if isinstance(content, list) else True

            require(permitted(value), "State metadata contains a prohibited field")
        if PurePosixPath(path).name.startswith("completed"):
            require(item.kind == "completion", "Completion metadata requires completion versions")
        require(item.kind != "completion" or bool(item.required_versions), "Completion requires verified completion versions")
        for reference in item.required_versions:
            self._verify(reference)

    def checkpoint(self, files: Iterable[StateFile]) -> None:
        """Publish immutable declared files, then advance one conditional manifest head."""
        self._owned()
        files = list(files)
        require(len({item.path for item in files}) == len(files), "Checkpoint permitted paths repeat")
        for item in files:
            self._file(item)
        entries = {entry["path"]: entry for entry in self.manifest["files"]}
        changed = False
        for item in files:
            if item.path in entries:
                existing = entries[item.path]
                require(
                    self._verify(existing["object"]).body == item.body
                    and existing["kind"] == item.kind
                    and existing["required_versions"] == list(item.required_versions),
                    "State write-once file differs",
                )
                continue
            category = "public_cache" if item.kind == "public_cache" else "artifacts"
            stored = self._immutable(PREFIX + f"collectors/{self.binding.collector_id}/{category}/{digest(item.body)}.json", item.body)
            entries[item.path] = {"path": item.path, "kind": item.kind, "object": stored.reference(), "required_versions": list(item.required_versions)}
            changed = True
        if not changed:
            return
        generation = self.head["generation"] + 1
        manifest = {
            "schema_version": 1,
            "binding": self.bound,
            "generation": generation,
            "previous_checkpoint": self.head["checkpoint"],
            "files": [entries[path] for path in sorted(entries)],
        }
        body = encoded_json(manifest)
        stored = self._immutable(PREFIX + f"collectors/{self.binding.collector_id}/checkpoints/{generation}/{digest(body)}.json", body)
        head = self.head | {"generation": generation, "previous_checkpoint": self.head["checkpoint"], "checkpoint": stored.reference()}
        if self.head_object is None:
            raise StateError("Collector has no claimed head")
        self.head_object = self._put(self.head_key, encoded_json(head), self.head_object.etag)
        self.head, self.manifest = head, manifest

    def hydrate(self) -> list[str]:
        """Verify the complete manifest and all versions before writing replacement scratch."""
        self._owned()
        manifest = self._load_manifest(self.head)
        ready = self._verified_files(manifest)
        for path, body in ready.items():
            target = self.root / path
            require(
                not any(item.is_symlink() for item in (target, *target.parents) if item.is_relative_to(self.root)), "State materialization contains a symlink"
            )
            require(not target.exists() or target.read_bytes() == body, "State write-once materialization differs")
        for path, body in ready.items():
            write_once(self.root / path, body)
        self.manifest = manifest
        return sorted(ready)

    @contextmanager
    def run(self, root: Path) -> Iterator[None]:
        """Claim and hydrate the reviewed scratch mapping without releasing on context exit."""
        require(root.absolute() == self.root, "Collector scratch root binding differs")
        self.claim()
        self.hydrate()
        yield

    def release(self, *, task_stopped: bool) -> None:
        """Finish a recoverable release only after confirmed task stop and version readback."""
        require(task_stopped, "Admission requires confirmed task stop")
        value, head = self._head()
        slot = self._read(ADMISSION_KEY)
        record = document(slot.body)
        require(
            record.get("owner") == self.owner_record
            and record.get("collector_id") == self.binding.collector_id
            and record.get("authority_epoch") == self.binding.authority_epoch,
            "Admission owner binding differs",
        )
        if record.get("status") == "releasing":
            require(
                (head.get("owner") == self.owner_record and head.get("status") == "active") or (head.get("owner") is None and head.get("status") == "idle"),
                "Collector owner binding differs",
            )
        else:
            require(head.get("owner") is not None or head.get("status") != "idle", "Collector claim incomplete; call resume() before release()")
            self._owned()
        self._verified_files(self._load_manifest(head))
        if record.get("status") != "releasing":
            record["status"] = "releasing"
            slot = self._put(ADMISSION_KEY, encoded_json(record), slot.etag)
        if head.get("owner") is not None:
            head = head | {"owner": None, "status": "idle"}
            value = self._put(self.head_key, encoded_json(head), value.etag)
        record.update(owner=None, collector_id=None, status="idle")
        self._put(ADMISSION_KEY, encoded_json(record), slot.etag)
        self.head_object, self.head = value, head
