"""The guard container of a layer task: it ends the task as soon as a phase has failed, or when the task has run past its time limit.

The phases of a layer task start one after the other, each after the success of the one before, and only the last phase is essential. When a phase exits
with an error, the phases after it never start, and Amazon ECS does not stop a task whose essential container has not started: the task stays PENDING until
somebody stops it. The guard is a second essential container with no dependency. It reads the task metadata endpoint, and when a phase is STOPPED with an
exit code other than zero, or STOPPED with no exit code (a phase that never started because its dependency failed), it exits with an error. An essential
container that exits stops the whole task, whatever its exit code, so the guard exits only for those two reasons and when ECS stops the task (SIGTERM, exit 0).

The guard never ends a healthy task. It acts on the second reading of the same failure, so a reply caught between two states cannot end a good run. An endpoint
that does not answer, answers late or answers with something else, a missing or invalid setting and any other error in a reading only make it log that it cannot
read, on the first failure and then every 30th, and try again. It logs once when it can read, so the log of the guard shows whether it watches at all.

The time limit is the job deadline plus a margin, so a task that nobody supervises (a hand-started one has no broker) cannot run on for ever. Settings:
``GUARD_SECONDS`` is the limit, ``GUARD_INTERVAL_SECONDS`` the time between two readings and ``GUARD_READ_SECONDS`` the limit of one reading."""

from __future__ import annotations

import http.client
import json
import os
import signal
import sys
import time
import urllib.parse

NAME = "guard"
BLIND_REPORT_EVERY = 30


def number(text: str, default: float) -> float:
    """A positive number from the text of a setting, or the default when it is empty, missing or invalid."""
    try:
        value = float(text)
    except ValueError:
        return default
    return value if value > 0 else default


def failed_phase(task: object) -> str:
    """The name of a container that stopped with an error or never started, or an empty text. A reply of another shape names none."""
    containers = task.get("Containers") if isinstance(task, dict) else None
    for container in containers if isinstance(containers, list) else []:
        if not isinstance(container, dict) or container.get("Name") == NAME or container.get("Type", "NORMAL") != "NORMAL":
            continue
        if container.get("KnownStatus") == "STOPPED" and container.get("ExitCode") != 0:
            return str(container.get("Name"))
    return ""


def read_task(seconds: float) -> object:
    """The task metadata of the version 4 endpoint that Fargate sets in the environment of every container."""
    address = urllib.parse.urlsplit(os.environ["ECS_CONTAINER_METADATA_URI_V4"])
    connection = http.client.HTTPConnection(address.hostname or "", address.port or 80, timeout=seconds)
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


def stopped(*_: object) -> None:
    """ECS stops the task: the essential phase has ended. The guard leaves with exit 0, so a good run has no container with an error."""
    raise SystemExit(0)


def main() -> int:
    signal.signal(signal.SIGTERM, stopped)
    interval = number(os.environ.get("GUARD_INTERVAL_SECONDS", ""), 10.0)
    limit = number(os.environ.get("GUARD_SECONDS", ""), 22500.0)
    seconds = number(os.environ.get("GUARD_READ_SECONDS", ""), 5.0)
    started, previous, seen, blind = time.monotonic(), "", False, 0
    say(f"guard: watching the phases of this task for {limit:.0f} seconds")
    while time.monotonic() - started < limit:
        found = ""
        try:
            found = failed_phase(read_task(seconds))
            if not seen:
                say("guard: the task metadata can be read")
            seen, blind = True, 0
        except Exception as error:
            blind += 1
            if blind == 1 or blind % BLIND_REPORT_EVERY == 0:
                say(f"guard: cannot read the task metadata ({type(error).__name__}), {blind} readings in a row")
        if found and found == previous:
            say(f"guard: phase {found} failed, so the task stops")
            return 1
        previous = found
        time.sleep(interval)
    say("guard: the task passed its time limit, so it stops")
    return 1


if __name__ == "__main__":
    sys.exit(main())
