"""A stand-in for the ``aws`` command, used by the group SIGINT case in ``host_session_group_signal``.

It answers two autoscaling commands from a state file under the directory named by ``HOST_SESSION_FAKE_STATE``. A capacity change lands only after an
in-flight window, so a command that is killed during the window changes nothing. The change also records the process and session identifiers of the
command. It imports nothing from the repository, so it runs with ``python -I host_session_fake_aws.py <aws arguments>``.
"""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

STATE_ENV = "HOST_SESSION_FAKE_STATE"
GROUP = "group-signal-host-group"
IN_FLIGHT_SECONDS = 1.5


def option(arguments: list[str], name: str) -> str:
    """The value that follows ``name`` on the command line."""
    return arguments[arguments.index(name) + 1]


def answer(arguments: list[str]) -> int:
    """Answer one ``aws`` command from the state file; a capacity change lands only after its in-flight window, so a killed command changes nothing."""
    root = Path(os.environ[STATE_ENV])
    state_file = root / "group.json"
    service, operation = arguments[1], arguments[2]
    if (service, operation) == ("autoscaling", "describe-auto-scaling-groups"):
        state = json.loads(state_file.read_text(encoding="utf-8"))
        group = {"AutoScalingGroupName": GROUP, "DesiredCapacity": state["desired"], "Instances": [{"InstanceId": item} for item in state["instances"]]}
        sys.stdout.write(json.dumps({"AutoScalingGroups": [group]}))
        return 0
    if (service, operation) == ("autoscaling", "set-desired-capacity"):
        (root / "requested").write_text(operation, encoding="utf-8")
        (root / "session.json").write_text(json.dumps({"pid": os.getpid(), "sid": os.getsid(0)}), encoding="utf-8")
        time.sleep(IN_FLIGHT_SECONDS)
        desired = int(option(arguments, "--desired-capacity"))
        state_file.write_text(json.dumps({"desired": desired, "instances": [] if desired == 0 else ["i-stand-in"]}), encoding="utf-8")
        return 0
    sys.stderr.write("unknown command\n")
    return 252


if __name__ == "__main__":
    raise SystemExit(answer(sys.argv))
