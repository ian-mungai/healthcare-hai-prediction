"""A fake Docker Engine on a Unix socket, for the launcher's end-to-end runs (scripts/lakehouse/run_launcher_e2e.py).

It speaks the part of the Engine API the launcher uses: containers (create, start, stop, kill, remove, inspect, list, logs, stats, attach),
networks, volumes, images, ``info``, ``_ping`` and the events stream. Its state lives in the test process, so a launcher that is killed
leaves the fake daemon as it was, and a request that was accepted completes whether or not its caller is still there.

The test controls the daemon's timing: a *gate* pauses a request before or after the daemon applies its side effect, an event can be
published before or after the reply, the event connections can be dropped, the subscription can be registered late (the response
headers go out first), the daemon can go away for a while like a restarting Docker Desktop, and a created container can
be given a script (how long it runs and how it ends). Every call is recorded.
"""

from __future__ import annotations

import contextlib
import json
import socket
import socketserver
import threading
import time
import uuid
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

GATE_TIMEOUT = 90.0


class Gate:
    """Pauses matching requests at a checkpoint until the test releases it."""

    def __init__(self, op: str, when: str, match: Callable[[dict[str, Any]], bool] | None, once: bool) -> None:
        self.op = op
        self.when = when
        self.match = match
        self.once = once
        self.reached = threading.Event()
        self.opened = threading.Event()
        self.hits = 0
        self.consumed = False

    def release(self) -> None:
        """Let the paused request go on."""
        self.opened.set()

    def wait_reached(self, seconds: float = 30.0) -> bool:
        """Wait until a request is paused here."""
        return self.reached.wait(seconds)


class Server(socketserver.ThreadingUnixStreamServer):
    """The listening socket; each request has its own thread."""

    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, path: str, daemon: FakeDocker) -> None:
        self.fake = daemon
        super().__init__(path, Handler)


def frame(stream: int, payload: bytes) -> bytes:
    """One Docker stream frame."""
    return bytes([stream, 0, 0, 0]) + len(payload).to_bytes(4, "big") + payload


class Handler(BaseHTTPRequestHandler):
    """Routes one Engine API request to the fake."""

    protocol_version = "HTTP/1.1"
    server: Server

    def log_message(self, format: str, *args: Any) -> None:
        """No access log."""

    def address_string(self) -> str:
        """No client address on a Unix socket."""
        return "unix"

    def reply(self, status: int, body: Any = None) -> None:
        """Send a JSON reply (or an empty one)."""
        data = b"" if body is None else json.dumps(body).encode()
        try:
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(data)
            self.wfile.flush()
        except OSError:
            pass
        self.close_connection = True

    def do_GET(self) -> None:
        self.route("GET")

    def do_POST(self) -> None:
        self.route("POST")

    def do_DELETE(self) -> None:
        self.route("DELETE")

    def route(self, method: str) -> None:
        """Dispatch on the path after the version prefix."""
        fake = self.server.fake
        parsed = urlparse(self.path)
        parts = [p for p in parsed.path.split("/") if p]
        if parts and parts[0].startswith("v1."):
            parts = parts[1:]
        query = {key: values[-1] for key, values in parse_qs(parsed.query).items()}
        length = int(self.headers.get("Content-Length") or 0)
        body = json.loads(self.rfile.read(length)) if length else None
        fake.record(method, "/" + "/".join(parts), query, body)
        if fake.is_down() and parts[:1] != ["_test"]:
            self.close_connection = True
            self.connection.close()
            return
        with contextlib.suppress(BrokenPipeError):
            fake.dispatch(self, method, parts, query, body)


class FakeDocker:
    """The fake daemon: state, gates, events and scripts."""

    def __init__(self, path: Path, mem_total: int = 32 * 1024**3, cpus: int = 8) -> None:
        self.path = path
        self.lock = threading.RLock()
        self.containers: dict[str, dict[str, Any]] = {}
        self.networks: dict[str, dict[str, Any]] = {}
        self.images: dict[str, dict[str, Any]] = {}
        self.calls: list[dict[str, Any]] = []
        self.event_log: list[dict[str, Any]] = []
        self.subscribers: list[Any] = []
        self.gates: list[Gate] = []
        self.mem_total = mem_total
        self.cpus = cpus
        self.usage: dict[str, int] = {}
        self.event_mode = "after_reply"
        self.suppress_probe_events = False
        self.subscribe_gate: Gate | None = None
        self.down_until = 0.0
        self.script: Callable[[dict[str, Any]], tuple[float, int] | None] = lambda container: (0.2, 0)
        self.on_start: Callable[[dict[str, Any]], None] | None = None
        self.health_delay = 0.2
        self.health_status = "healthy"
        self.fail_remove = 0
        self.fail_list = False
        self.stdout: dict[str, str] = {}
        self.create_bodies: dict[str, dict[str, Any]] = {}
        self.stopping = False
        self.timers: list[threading.Timer] = []
        self.server = Server(str(path), self)
        self.thread = threading.Thread(target=self.server.serve_forever, name="fake-docker", daemon=True)
        self.thread.start()

    # -- bookkeeping -----------------------------------------------------------------------------------------------

    def record(self, method: str, path: str, query: dict[str, str], body: Any) -> None:
        """Remember a call for the test's assertions."""
        entry = {"t": time.time(), "method": method, "path": path, "query": dict(query)}
        if body and path == "/containers/create":
            entry["name"] = query.get("name")
        parts = path.split("/")
        if len(parts) >= 3 and parts[1] == "containers" and parts[2] not in ("create", "json"):
            found = self.find(parts[2])
            entry["target"] = found["Name"].lstrip("/") if found else None
        with self.lock:
            self.calls.append(entry)

    def calls_matching(self, method: str, prefix: str) -> list[dict[str, Any]]:
        """Recorded calls with a method and a path prefix."""
        with self.lock:
            return [c for c in self.calls if c["method"] == method and c["path"].startswith(prefix)]

    def is_down(self) -> bool:
        """True while the daemon is pretending to restart."""
        return time.time() < self.down_until

    def close(self) -> None:
        """Stop serving and drop every connection."""
        self.stopping = True
        for gate in list(self.gates):
            gate.release()
        self.drop_events()
        for timer in self.timers:
            timer.cancel()
        self.server.shutdown()
        self.server.server_close()

    # -- gates -----------------------------------------------------------------------------------------------------

    def add_gate(self, op: str, when: str = "before", match: Callable[[dict[str, Any]], bool] | None = None, once: bool = True) -> Gate:
        """Pause requests of an operation ``before`` the daemon applies it or ``after`` it applied and before it replies."""
        gate = Gate(op, when, match, once)
        with self.lock:
            self.gates.append(gate)
        return gate

    def checkpoint(self, op: str, when: str, context: dict[str, Any]) -> None:
        """Called by the handlers at each side effect."""
        with self.lock:
            candidates = [g for g in self.gates if g.op == op and g.when == when and not g.consumed and (g.match is None or g.match(context))]
            if not candidates:
                return
            gate = candidates[0]
            gate.hits += 1
            if gate.once:
                gate.consumed = True
        gate.reached.set()
        gate.opened.wait(GATE_TIMEOUT)

    # -- events ----------------------------------------------------------------------------------------------------

    def publish(self, action: str, container: dict[str, Any], **extra: str) -> None:
        """Publish a container event to every subscriber."""
        labels = container["Config"]["Labels"]
        if self.suppress_probe_events and "hai.probe" in labels:
            return
        attributes = {**labels, "name": container["Name"].lstrip("/"), "image": container["Config"]["Image"], **extra}
        event = {"Type": "container", "Action": action, "Actor": {"ID": container["Id"], "Attributes": attributes}, "time": int(time.time())}
        with self.lock:
            self.event_log.append({"action": action, "id": container["Id"], "name": container["Name"].lstrip("/"), "at": time.time()})
            targets = list(self.subscribers)
        for subscriber in targets:
            subscriber.put(event)

    def drop_events(self) -> None:
        """Close every events connection."""
        with self.lock:
            targets = list(self.subscribers)
            self.subscribers.clear()
        for subscriber in targets:
            subscriber.put(None)

    def restart(self, down_seconds: float, stop_containers: bool = True) -> None:
        """Pretend Docker Desktop restarts: events drop and nothing answers for a while; containers stop unless the test keeps them."""
        self.down_until = time.time() + down_seconds
        self.drop_events()
        with self.lock:
            for container in self.containers.values():
                if stop_containers and container["State"]["Running"]:
                    container["State"].update({"Running": False, "Status": "exited", "ExitCode": 137})

    def create_rogue(self, name: str, labels: dict[str, str], running: bool = False, image: str | None = None) -> str:
        """Add a container the launcher did not create, as a same-user process calling Docker would, and publish its create event."""
        container_id = uuid.uuid4().hex + uuid.uuid4().hex
        chosen = image or next(iter(self.images))
        container = {
            "Id": container_id,
            "Name": "/" + name,
            "Created": time.time(),
            "Config": {
                "Image": chosen,
                "Entrypoint": None,
                "Cmd": None,
                "Env": [],
                "Labels": dict(labels),
                "WorkingDir": "",
                "User": "",
                "Tty": False,
                "OpenStdin": False,
                "Healthcheck": None,
            },
            "HostConfig": {"Memory": 0, "NetworkMode": "default", "Init": None, "PortBindings": None},
            "Mounts": [],
            "State": {"Status": "running" if running else "created", "Running": running, "ExitCode": 0, "Health": None},
            "_body": {},
            "_stdin": b"",
        }
        with self.lock:
            self.containers[container_id] = container
        self.publish("create", container)
        return container_id

    # -- dispatch --------------------------------------------------------------------------------------------------

    def dispatch(self, handler: Handler, method: str, parts: list[str], query: dict[str, str], body: Any) -> None:
        """Route by path."""
        if parts == ["_ping"]:
            handler.reply(200, None)
        elif parts == ["info"]:
            handler.reply(200, {"MemTotal": self.mem_total, "NCPU": self.cpus})
        elif parts[:1] == ["_test"]:
            if parts[1:] == ["restart"]:
                self.restart(float(query.get("down", "1")), query.get("keep") != "1")
            handler.reply(200, {"ok": True})
        elif parts[:1] == ["events"]:
            self.events(handler, query)
        elif parts[:1] == ["containers"]:
            self.containers_api(handler, method, parts[1:], query, body)
        elif parts[:1] == ["networks"]:
            self.networks_api(handler, method, parts[1:], query, body)
        elif parts == ["volumes"]:
            handler.reply(200, {"Volumes": []})
        elif parts[:1] == ["images"] and len(parts) >= 3 and parts[-1] == "json":
            name = "/".join(parts[1:-1])
            image = self.images.get(name)
            handler.reply(200 if image else 404, image or {"message": f"No such image: {name}"})
        else:
            handler.reply(404, {"message": f"fake daemon has no route {method} /{'/'.join(parts)}"})

    @staticmethod
    def label_filter(query: dict[str, str]) -> list[str]:
        """The label filters of a query."""
        raw = query.get("filters")
        return list(json.loads(raw).get("label", [])) if raw else []

    @staticmethod
    def labels_match(labels: dict[str, str], wanted: list[str]) -> bool:
        """True when every wanted ``key`` or ``key=value`` is present."""
        for item in wanted:
            key, equals, value = item.partition("=")
            if key not in labels or (equals and labels[key] != value):
                return False
        return True

    def find(self, ref: str) -> dict[str, Any] | None:
        """A container by ID, ID prefix or name."""
        with self.lock:
            if ref in self.containers:
                return self.containers[ref]
            for container in self.containers.values():
                if container["Name"].lstrip("/") == ref or container["Id"].startswith(ref):
                    return container
        return None

    # -- events stream ---------------------------------------------------------------------------------------------

    def events(self, handler: Handler, query: dict[str, str]) -> None:
        """Stream events: the response headers go out first; the subscription may be registered late (a test seam)."""
        import queue

        wanted = self.label_filter(query)
        handler.send_response(200)
        handler.send_header("Content-Type", "application/json")
        handler.send_header("Transfer-Encoding", "chunked")
        handler.send_header("Connection", "close")
        handler.end_headers()
        handler.wfile.flush()
        if self.subscribe_gate is not None:
            gate, self.subscribe_gate = self.subscribe_gate, None
            gate.reached.set()
            gate.opened.wait(GATE_TIMEOUT)
        events: queue.Queue[dict[str, Any] | None] = queue.Queue()
        with self.lock:
            self.subscribers.append(events)
        try:
            while not self.stopping:
                try:
                    event = events.get(timeout=0.5)
                except queue.Empty:
                    continue
                if event is None:
                    break
                labels = event["Actor"]["Attributes"]
                if wanted and not self.labels_match(labels, wanted):
                    continue
                data = json.dumps(event).encode() + b"\n"
                handler.wfile.write(f"{len(data):x}\r\n".encode() + data + b"\r\n")
                handler.wfile.flush()
        except OSError:
            pass
        finally:
            with self.lock:
                if events in self.subscribers:
                    self.subscribers.remove(events)
            handler.close_connection = True

    # -- containers ------------------------------------------------------------------------------------------------

    def listing(self, container: dict[str, Any]) -> dict[str, Any]:
        """The list-endpoint form of a container."""
        return {
            "Id": container["Id"],
            "Names": [container["Name"]],
            "Image": container["Config"]["Image"],
            "State": container["State"]["Status"],
            "Labels": container["Config"]["Labels"],
        }

    def containers_api(self, handler: Handler, method: str, parts: list[str], query: dict[str, str], body: Any) -> None:
        """Container routes."""
        if parts == ["json"]:
            if self.fail_list:
                handler.reply(500, {"message": "the fake daemon fails listings"})
                return
            wanted = self.label_filter(query)
            show_all = query.get("all") == "1"
            with self.lock:
                items = [
                    self.listing(c)
                    for c in self.containers.values()
                    if (show_all or c["State"]["Running"]) and self.labels_match(c["Config"]["Labels"], wanted)
                ]
            self.checkpoint("list", "after", {"labels": wanted, "all": show_all})
            handler.reply(200, items)
        elif parts == ["create"] and method == "POST":
            self.create(handler, query, body)
        elif len(parts) == 2 and parts[1] == "json":
            container = self.find(parts[0])
            handler.reply(200 if container else 404, self.inspect_form(container) if container else {"message": "No such container"})
        elif len(parts) == 1 and method == "DELETE":
            self.remove(handler, parts[0])
        elif len(parts) == 2 and parts[1] == "start":
            self.start(handler, parts[0])
        elif len(parts) == 2 and parts[1] in ("stop", "kill"):
            self.stop(handler, parts[0], parts[1])
        elif len(parts) == 2 and parts[1] == "logs":
            self.logs(handler, parts[0], query)
        elif len(parts) == 2 and parts[1] == "stats":
            container = self.find(parts[0])
            usage = self.usage.get(container["Name"].lstrip("/"), 100 * 1024**2) if container else 0
            handler.reply(200 if container else 404, {"memory_stats": {"usage": usage, "stats": {"inactive_file": 0}}})
        elif len(parts) == 2 and parts[1] == "attach":
            self.attach(handler, parts[0])
        else:
            handler.reply(404, {"message": "fake daemon: no such container route"})

    @staticmethod
    def inspect_form(container: dict[str, Any]) -> dict[str, Any]:
        """The inspect-endpoint form; the daemon adds defaults and the image's own environment."""
        return {**{k: v for k, v in container.items() if not k.startswith("_")}, "Config": dict(container["Config"])}

    def create(self, handler: Handler, query: dict[str, str], body: dict[str, Any]) -> None:
        """Create a container: name conflicts and missing images are refused as the daemon does."""
        name = query.get("name", "")
        context = {"name": name, "labels": body.get("Labels", {}), "body": body}
        self.checkpoint("create", "before", context)
        with self.lock:
            if any(c["Name"].lstrip("/") == name for c in self.containers.values()):
                handler.reply(409, {"message": f"Conflict. The container name /{name} is already in use"})
                return
            if body["Image"] not in self.images:
                handler.reply(404, {"message": f"No such image: {body['Image']}"})
                return
            container_id = uuid.uuid4().hex + uuid.uuid4().hex
            self.create_bodies[name] = body
            host = body.get("HostConfig", {})
            image = self.images[body["Image"]]
            defaults = image["Config"]
            # The daemon merges the image's configuration into the container's: the environment by name, the user, the working folder and the
            # health check when the request leaves them empty, and the entrypoint and command by the daemon's rule, as the real Engine does
            # (checked on Docker Desktop, amendment A83): an absent or null entrypoint keeps the image's, an empty list clears it, and the
            # command follows the image unless the request sets its own command or a non-empty entrypoint. Zero health-check values are dropped.
            env = dict(item.split("=", 1) for item in defaults.get("Env", []))
            env.update(dict(item.split("=", 1) for item in body.get("Env") or []))
            requested = body.get("Entrypoint")
            command = list(body.get("Cmd") or [])
            if requested:
                entrypoint = list(requested)
            else:
                command = command or list(defaults.get("Cmd") or [])
                entrypoint = [] if requested == [] else list(defaults.get("Entrypoint") or [])
            health = {key: value for key, value in (body.get("Healthcheck") or {}).items() if value not in (0, None)}
            config = {
                "Image": body["Image"],
                "Entrypoint": entrypoint or None,
                "Cmd": command or None,
                "Env": [f"{key}={value}" for key, value in env.items()],
                "Labels": {**defaults["Labels"], **body.get("Labels", {})},
                "WorkingDir": body.get("WorkingDir") or defaults.get("WorkingDir", ""),
                "Hostname": body.get("Hostname", "abc123"),
                "User": body.get("User") or defaults.get("User", ""),
                "Tty": bool(body.get("Tty")),
                "OpenStdin": bool(body.get("OpenStdin")),
                "Healthcheck": health or defaults.get("Healthcheck"),
            }
            container = {
                "Id": container_id,
                "Image": image["Id"],
                "Name": "/" + name,
                "Created": time.time(),
                "Config": config,
                "HostConfig": {
                    "RestartPolicy": dict(host.get("RestartPolicy") or {"Name": ""}),
                    "AutoRemove": bool(host.get("AutoRemove")),
                    "Memory": host.get("Memory", 0),
                    "NetworkMode": host.get("NetworkMode", "default"),
                    "Init": host.get("Init"),
                    "PortBindings": host.get("PortBindings"),
                    "Tmpfs": host.get("Tmpfs"),
                    "ExtraHosts": host.get("ExtraHosts"),
                },
                "Mounts": [{"Type": m["Type"], "Source": m["Source"], "Destination": m["Target"], "RW": not m.get("ReadOnly")} for m in host.get("Mounts", [])],
                "State": {"Status": "created", "Running": False, "ExitCode": 0, "Health": {"Status": "starting"} if body.get("Healthcheck") else None},
                "_body": body,
                "_stdin": b"",
            }
            self.containers[container_id] = container
        if self.event_mode == "before_reply":
            self.publish("create", container)
        self.checkpoint("create", "after", context)
        handler.reply(201, {"Id": container_id, "Warnings": []})
        if self.event_mode != "before_reply":
            self.publish("create", container)

    def start(self, handler: Handler, ref: str) -> None:
        """Start a container and schedule its scripted end and health."""
        container = self.find(ref)
        if container is None:
            handler.reply(404, {"message": "No such container"})
            return
        context = {"name": container["Name"].lstrip("/"), "labels": container["Config"]["Labels"]}
        self.checkpoint("start", "before", context)
        with self.lock:
            if container["State"]["Running"]:
                handler.reply(304, None)
                return
            container["State"].update({"Status": "running", "Running": True, "ExitCode": 0})
        self.publish("start", container)
        if self.on_start is not None:
            self.on_start(container)
        self.schedule(container)
        self.checkpoint("start", "after", context)
        handler.reply(204, None)

    def schedule(self, container: dict[str, Any]) -> None:
        """Timers for the container's health and its scripted end."""
        if container["Config"].get("Healthcheck"):
            timer = threading.Timer(self.health_delay, self.set_health, args=(container,))
            timer.daemon = True
            timer.start()
            self.timers.append(timer)
        plan = self.script(container)
        if plan is not None:
            seconds, code = plan
            timer = threading.Timer(seconds, self.finish, args=(container, code))
            timer.daemon = True
            timer.start()
            self.timers.append(timer)

    def set_health(self, container: dict[str, Any]) -> None:
        """Mark a running container with a health check healthy (or as the test set)."""
        with self.lock:
            if container["State"]["Running"] and container["State"]["Health"] is not None:
                container["State"]["Health"] = {"Status": self.health_status}

    def finish(self, container: dict[str, Any], code: int, action: str = "die") -> None:
        """End a running container with an exit code."""
        with self.lock:
            if not container["State"]["Running"]:
                return
            container["State"].update({"Status": "exited", "Running": False, "ExitCode": code})
        self.publish(action, container, exitCode=str(code))

    def stop(self, handler: Handler, ref: str, kind: str) -> None:
        """Stop or kill a container."""
        container = self.find(ref)
        if container is None:
            handler.reply(404, {"message": "No such container"})
            return
        context = {"name": container["Name"].lstrip("/"), "labels": container["Config"]["Labels"], "kind": kind}
        self.checkpoint("stop", "before", context)
        with self.lock:
            running = container["State"]["Running"]
        if not running:
            handler.reply(304 if kind == "stop" else 409, None)
            return
        if kind == "kill":
            self.publish("kill", container)
        self.finish(container, 143 if kind == "stop" else 137)
        self.publish("stop", container)
        self.checkpoint("stop", "after", context)
        handler.reply(204, None)

    def remove(self, handler: Handler, ref: str) -> None:
        """Remove a container (forced removal stops a running one first); a test may make removals fail."""
        container = self.find(ref)
        if container is None:
            handler.reply(404, {"message": "No such container"})
            return
        context = {"name": container["Name"].lstrip("/"), "labels": container["Config"]["Labels"]}
        self.checkpoint("remove", "before", context)
        with self.lock:
            if self.fail_remove > 0:
                self.fail_remove -= 1
                handler.reply(500, {"message": "the fake daemon fails this removal"})
                return
            was_running = container["State"]["Running"]
        if was_running:
            self.finish(container, 137)
        with self.lock:
            self.containers.pop(container["Id"], None)
        self.publish("destroy", container)
        self.checkpoint("remove", "after", context)
        handler.reply(204, None)

    def logs(self, handler: Handler, ref: str, query: dict[str, str]) -> None:
        """The container's recorded output as a multiplexed stream."""
        container = self.find(ref)
        if container is None:
            handler.reply(404, {"message": "No such container"})
            return
        self.checkpoint("logs", "before", {"name": container["Name"].lstrip("/"), "labels": container["Config"]["Labels"]})
        out, err = container.get("_out", ""), container.get("_err", "")
        tail = query.get("tail", "all")
        if tail != "all":
            out = "".join(out.splitlines(keepends=True)[-int(tail) :])
            err = "".join(err.splitlines(keepends=True)[-int(tail) :])
        data = (frame(1, out.encode()) if out else b"") + (frame(2, err.encode()) if err else b"")
        handler.send_response(200)
        handler.send_header("Content-Type", "application/vnd.docker.raw-stream")
        handler.send_header("Content-Length", str(len(data)))
        handler.send_header("Connection", "close")
        handler.end_headers()
        handler.wfile.write(data)
        handler.close_connection = True

    def attach(self, handler: Handler, ref: str) -> None:
        """A hijacked attach: record the input and answer each input with an output frame."""
        container = self.find(ref)
        if container is None:
            handler.reply(404, {"message": "No such container"})
            return
        handler.wfile.write(b"HTTP/1.1 101 UPGRADED\r\nContent-Type: application/vnd.docker.raw-stream\r\nConnection: Upgrade\r\nUpgrade: tcp\r\n\r\n")
        handler.wfile.flush()
        connection: socket.socket = handler.connection
        connection.settimeout(0.5)
        tty = container["Config"]["Tty"]
        try:
            while not self.stopping and container["State"]["Running"]:
                try:
                    data = connection.recv(4096)
                except TimeoutError:
                    continue
                if not data:
                    break
                with self.lock:
                    container["_stdin"] += data
                reply = b"echo:" + data
                connection.sendall(reply if tty else frame(1, reply))
        except OSError:
            pass
        handler.close_connection = True

    # -- networks --------------------------------------------------------------------------------------------------

    def networks_api(self, handler: Handler, method: str, parts: list[str], query: dict[str, str], body: Any) -> None:
        """Network routes."""
        if not parts and method == "GET":
            wanted = self.label_filter(query)
            with self.lock:
                handler.reply(200, [n for n in self.networks.values() if self.labels_match(n["Labels"], wanted)])
        elif parts == ["create"]:
            context = {"name": body["Name"], "labels": body.get("Labels", {})}
            self.checkpoint("net_create", "before", context)
            with self.lock:
                if any(n["Name"] == body["Name"] for n in self.networks.values()):
                    handler.reply(409, {"message": "network with this name already exists"})
                    return
                network_id = uuid.uuid4().hex
                self.networks[network_id] = {"Id": network_id, "Name": body["Name"], "Labels": body.get("Labels", {})}
            self.checkpoint("net_create", "after", context)
            handler.reply(201, {"Id": network_id})
        elif len(parts) == 1 and method == "DELETE":
            self.checkpoint("net_remove", "before", {"id": parts[0]})
            with self.lock:
                existed = self.networks.pop(parts[0], None) is not None
            handler.reply(204 if existed else 404, None)
        else:
            handler.reply(404, {"message": "fake daemon: no such network route"})
