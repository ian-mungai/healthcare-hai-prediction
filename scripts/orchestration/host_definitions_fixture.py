"""Write ``infra/jobs/tests/host_definitions.json``: the containers of the nine host task definitions as Terraform evaluates them.

The Airflow mode cases (``airflow_mode_cases``) read that file to know which containers each host definition holds. The file is not typed by hand and not
read from the Terraform text: this command runs the stack test ``tests/host_definitions.tftest.hcl`` offline and reads the planned ``container_definitions``
of every ``aws_ecs_task_definition.host`` from the plan that ``terraform test -json -verbose`` prints. The same test fails when the file differs from what
Terraform plans, so a change of ``infra/jobs/host.tf`` that moves a container fails ``terraform test`` until this command is run again. Run from the
repository root after ``terraform -chdir=infra/jobs init -backend=false -input=false``:
``.venv/bin/python -m scripts.orchestration.host_definitions_fixture``.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]
STACK = "infra/jobs"
TEST_FILE = "tests/host_definitions.tftest.hcl"
FIXTURE = "tests/host_definitions.json"
SCRATCH_WIPE = "find /workspace /tmp -mindepth 1 -delete && "
AWS_VARIABLES = ("AWS_PROFILE", "AWS_DEFAULT_PROFILE", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_SECURITY_TOKEN")
DEADLINE_SECONDS = 600


def offline_environment() -> dict[str, str]:
    """The caller's environment without AWS credentials or profiles, with empty AWS configuration files, as the Terraform check group runs the stacks."""
    # Terraform also reads arguments from TF_CLI_ARGS*; drop them so only this generator's own arguments reach the test, as the stack wrapper does.
    environment = {name: value for name, value in os.environ.items() if name not in AWS_VARIABLES and not name.startswith("TF_CLI_ARGS")}
    environment.update(
        AWS_CONFIG_FILE="/dev/null", AWS_SHARED_CREDENTIALS_FILE="/dev/null", AWS_EC2_METADATA_DISABLED="true", TF_IN_AUTOMATION="1", TF_INPUT="0"
    )
    return environment


def planned_definitions(output: str) -> dict[str, list[dict[str, Any]]]:
    """The container list of each host family from the plan event of a ``terraform test -json -verbose`` run."""
    found: dict[str, list[dict[str, Any]]] = {}
    for line in output.splitlines():
        if not line.startswith("{"):
            continue
        event = json.loads(line)
        if event.get("type") != "test_plan":
            continue
        for change in event["test_plan"]["resource_changes"]:
            if change["type"] == "aws_ecs_task_definition" and change["name"] == "host":
                found[change["index"]] = json.loads(change["change"]["after"]["container_definitions"])
    if not found:
        raise ValueError("the Terraform test printed no plan of aws_ecs_task_definition.host")
    return found


def summary(definitions: dict[str, list[dict[str, Any]]]) -> dict[str, dict[str, list[str]]]:
    """Per family, the container names in order and the names of the containers whose first command starts with the scratch wipe."""
    return {
        family: {
            "containers": [container["name"] for container in containers],
            "start_with_scratch_wipe": [container["name"] for container in containers if container["command"][0].startswith(SCRATCH_WIPE)],
        }
        for family, containers in sorted(definitions.items())
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--stack", type=Path, default=ROOT / STACK, help="the initialised jobs stack; default infra/jobs")
    arguments = parser.parse_args()
    stack = arguments.stack.resolve()
    done = run_command(
        "terraform", [f"-chdir={stack}", "test", "-json", "-verbose", f"-filter={TEST_FILE}"], env=offline_environment(), timeout=DEADLINE_SECONDS
    )
    try:
        written = summary(planned_definitions(done.stdout))
    except ValueError as error:
        # A fixed reason and the exit code only: Terraform's own output can repeat values from the environment or the stack.
        sys.stderr.write(f"{error}; terraform exit {done.returncode}\n")
        sys.stderr.write(f"Initialise the stack first: terraform -chdir={stack} init -backend=false -input=false\n")
        return 1
    (stack / FIXTURE).write_text(json.dumps(written, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    sys.stdout.write(f"wrote {stack / FIXTURE}: {len(written)} host definitions\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
