"""The guard container of a layer task: it ends the task as soon as a phase has failed, or when the task has run past its time limit.

The phases of a layer task start one after the other, each after the success of the one before, and only the last phase is essential. When a phase exits
with an error, the phases after it never start, and Amazon ECS does not stop a task whose essential container has not started: the task stays PENDING until
somebody stops it. The guard is a second essential container with no dependency. It reads the task metadata endpoint, and when a phase is STOPPED with an
exit code other than zero, or STOPPED with no exit code (a phase that never started because its dependency failed), it exits with an error. An essential
container that exits stops the whole task. It acts on the second reading of the same failure, so a reply caught between two states cannot end a good run, and
an endpoint that does not answer or answers with something else never ends a task.

The time limit is the job deadline plus the time the fetch phase may take, so a task that nobody supervises (a hand-started one has no broker) cannot run on
for ever. ``GUARD_SECONDS`` is the limit and ``GUARD_INTERVAL_SECONDS`` the time between two readings."""

from __future__ import annotations

import http.client
import json
import os
import sys
import time
import urllib.parse

NAME = "guard"


def failed_phase(task: object) -> str:
    """The name of a container that stopped with an error or never started, or an empty text. A reply of another shape names none."""
    containers = task.get("Containers") if isinstance(task, dict) else None
    for container in containers if isinstance(containers, list) else []:
        if not isinstance(container, dict) or container.get("Name") == NAME or container.get("Type", "NORMAL") != "NORMAL":
            continue
        if container.get("KnownStatus") == "STOPPED" and container.get("ExitCode") != 0:
            return str(container.get("Name"))
    return ""


def read_task() -> object:
    """The task metadata of the version 4 endpoint that Fargate sets in the environment of every container."""
    address = urllib.parse.urlsplit(os.environ["ECS_CONTAINER_METADATA_URI_V4"])
    connection = http.client.HTTPConnection(address.hostname or "", address.port or 80, timeout=5)
    try:
        connection.request("GET", address.path.rstrip("/") + "/task")
        reply = connection.getresponse()
        if reply.status != 200:
            raise OSError("the metadata endpoint did not answer")
        return json.loads(reply.read())
    finally:
        connection.close()


def say(text: str) -> None:
    sys.stdout.write(text + "\n")
    sys.stdout.flush()


def main() -> int:
    interval = float(os.environ.get("GUARD_INTERVAL_SECONDS", "10"))
    limit = float(os.environ["GUARD_SECONDS"])
    started, previous = time.monotonic(), ""
    while time.monotonic() - started < limit:
        try:
            found = failed_phase(read_task())
        except (OSError, ValueError, http.client.HTTPException):
            found = ""
        if found and found == previous:
            say(f"guard: phase {found} failed, so the task stops")
            return 1
        previous = found
        time.sleep(interval)
    say("guard: the task passed its time limit, so it stops")
    return 1


if __name__ == "__main__":
    sys.exit(main())
