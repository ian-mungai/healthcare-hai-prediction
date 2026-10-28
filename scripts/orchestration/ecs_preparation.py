"""Supply deterministic in-memory service contracts for remote-run preparation."""

from __future__ import annotations

import re
from dataclasses import dataclass, replace


class ApiRefused(RuntimeError):
    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


@dataclass
class FakeClock:
    now: float = 0

    def advance(self, seconds: float) -> None:
        if seconds < 0:
            raise ValueError("the stand-in clock cannot move backwards")
        self.now += seconds

    def require_before(self, deadline: float) -> None:
        if self.now >= deadline:
            raise ApiRefused("Deadline")


@dataclass(frozen=True)
class ObjectVersion:
    key: str
    version_id: str
    body: bytes


class FakeS3:
    def __init__(self, clock: FakeClock | None = None) -> None:
        self.clock = clock if clock is not None else FakeClock()
        self.objects: dict[str, list[ObjectVersion]] = {}
        self.serial = 0

    def seed(self, key: str, body: bytes) -> ObjectVersion:
        self.serial += 1
        version = ObjectVersion(key, f"v{self.serial:04d}", body)
        self.objects.setdefault(key, []).append(version)
        return version

    def put(self, key: str, body: bytes, *, deadline: float, if_none_match: bool = True) -> ObjectVersion:
        self.clock.require_before(deadline)
        if not if_none_match:
            raise ApiRefused("AccessDenied")
        if key in self.objects:
            raise ApiRefused("PreconditionFailed")
        return self.seed(key, body)

    def get(self, key: str, version_id: str, *, deadline: float) -> ObjectVersion:
        self.clock.require_before(deadline)
        for item in self.objects.get(key, []):
            if item.version_id == version_id:
                return item
        raise ApiRefused("NoSuchVersion")


@dataclass(frozen=True)
class CatalogTable:
    namespace: str
    name: str
    table_uuid: str
    metadata_key: str
    metadata_version: str
    current_snapshot: int
    retained_snapshots: tuple[int, ...]


class FakeCatalog:
    def __init__(self, clock: FakeClock | None = None) -> None:
        self.clock = clock if clock is not None else FakeClock()
        self.tables: dict[tuple[str, str], CatalogTable] = {}
        self.available = True

    def ready(self, deadline: float) -> None:
        self.clock.require_before(deadline)
        if not self.available:
            raise ApiRefused("CatalogUnavailable")

    def register(self, table: CatalogTable, *, deadline: float) -> str:
        self.ready(deadline)
        key = (table.namespace, table.name)
        existing = self.tables.get(key)
        if existing is not None:
            if existing != table:
                raise ApiRefused("RegistrationConflict")
            return "unchanged"
        self.tables[key] = table
        return "registered"

    def load(self, namespace: str, name: str, *, deadline: float) -> CatalogTable:
        self.ready(deadline)
        try:
            return self.tables[(namespace, name)]
        except KeyError:
            raise ApiRefused("NoSuchTable") from None

    def advance_head(self, namespace: str, name: str, snapshot: int) -> None:
        existing = self.tables[(namespace, name)]
        self.tables[(namespace, name)] = replace(existing, current_snapshot=snapshot, retained_snapshots=(*existing.retained_snapshots, snapshot))

    def read_snapshot(self, namespace: str, name: str, snapshot: int, *, deadline: float) -> int:
        table = self.load(namespace, name, deadline=deadline)
        if snapshot not in table.retained_snapshots:
            raise ApiRefused("NoSuchSnapshot")
        return snapshot


@dataclass(frozen=True)
class FakeTask:
    task_id: str
    try_id: str
    status: str = "PENDING"
    desired_status: str = "RUNNING"


class FakeEcs:
    def __init__(self, clock: FakeClock | None = None) -> None:
        self.clock = clock if clock is not None else FakeClock()
        self.tasks: dict[str, FakeTask] = {}
        self.tokens: dict[str, tuple[str, str]] = {}
        self.lose_next_run_reply = False

    def run_task(self, try_id: str, client_token: str, *, deadline: float) -> FakeTask:
        self.clock.require_before(deadline)
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,100}", try_id):
            raise ApiRefused("InvalidParameterException")
        existing = self.tokens.get(client_token)
        if existing is not None:
            old_try, task_id = existing
            if old_try != try_id:
                raise ApiRefused("ClientTokenConflict")
            return self.tasks[task_id]
        task = FakeTask(f"task_{len(self.tasks) + 1:04d}", try_id)
        self.tokens[client_token] = (try_id, task.task_id)
        self.tasks[task.task_id] = task
        if self.lose_next_run_reply:
            self.lose_next_run_reply = False
            raise ApiRefused("ReplyLost")
        return task

    def stop_task(self, task_id: str, *, deadline: float, confirmed: bool = True) -> FakeTask:
        self.clock.require_before(deadline)
        task = replace(self.tasks[task_id], desired_status="STOPPED", status="STOPPED" if confirmed else "STOPPING")
        self.tasks[task_id] = task
        return task

    def list_tasks(
        self, *, deadline: float, started_by: str | None = None, desired_status: str | None = None, next_token: int = 0
    ) -> tuple[list[FakeTask], int | None]:
        self.clock.require_before(deadline)
        if started_by is not None and desired_status is not None:
            raise ApiRefused("InvalidParameterException")
        selected = [task for task in self.tasks.values() if task.desired_status == (desired_status or "RUNNING")]
        if started_by is not None:
            selected = [task for task in selected if task.try_id == started_by]
        page = selected[next_token : next_token + 100]
        following = next_token + 100
        return page, following if following < len(selected) else None
