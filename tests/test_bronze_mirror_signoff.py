"""The sign-off of a bronze run's Glue mirror needs the recorded outcome and the successful result of the matching task.

``show`` reads only what the check phase recorded, so it cannot see a retry whose record was never written (the role lost its grant, or the task
stopped before it wrote). That is a documented limit of the reader: the last recorded outcome can be an older ``ok``. The sign-off therefore also
requires the described task of the run (the output of ``aws ecs describe-tasks``) to be stopped with every container at exit 0, to be the bronze job's
own task definition and to carry this run's ID."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from scripts.lakehouse import bronze_mirror, bronze_mirror_signoff
from tests.support import check
from tests.test_bronze_mirror_attempts import BUCKET, FAILED, OK, RUN, Client, receipt, record, report

JOBS = json.loads((Path(__file__).resolve().parents[1] / "config/orchestration/ecs_broker_jobs.json").read_text(encoding="utf-8"))["jobs"]["bronze"]
DEFINITION = "arn:aws:ecs:us-west-2:111111111111:task-definition/example_project_dev_job_bronze_host:7"
TASK = "arn:aws:ecs:us-west-2:111111111111:task/example_cluster/" + "a" * 32


def described(run: str = RUN, exits: dict[str, int] | None = None, status: str = "STOPPED", definition: str = DEFINITION, tasks: int = 1) -> dict[str, Any]:
    """The describe-tasks output of one bronze task: the init container and every container of the job catalog entry, each with its exit code."""
    codes = {name: 0 for name in ("init", *JOBS["containers"])} | (exits or {})
    task = {
        "taskArn": TASK,
        "taskDefinitionArn": definition,
        "lastStatus": status,
        "containers": [{"name": name, "exitCode": code} for name, code in codes.items()],
        "overrides": {
            "containerOverrides": [
                {"name": "init"},
                *(
                    {"name": name, "environment": [{"name": key, "value": run if key == "RUN_ID" else "x"} for key in variables]}
                    for name, variables in JOBS["containers"].items()
                ),
            ]
        },
    }
    return {"tasks": [task] * tasks, "failures": []}


def chain(client: Client, mirror: dict[str, Any]) -> None:
    document, digest = report(mirror)
    receipt(client, document, digest)
    record(client, mirror)


def signoff(client: Client, tmp_path: Path, describe: object, capsys: pytest.CaptureFixture[str], run: str = RUN) -> tuple[int, str]:
    path = tmp_path / "describe.json"
    path.write_text(json.dumps(describe), encoding="utf-8")
    code = bronze_mirror_signoff.main(["--run", run, "--bucket", BUCKET, "--describe", str(path)], client=client)
    return code, capsys.readouterr().out


def test_the_signoff_passes_with_an_ok_record_and_the_successful_task(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    client = Client()
    chain(client, OK)
    code, out = signoff(client, tmp_path, described(), capsys)
    check(code == 0 and "signoff: ok" in out and "task: ok" in out, f"{code} {out}")


def test_the_stale_reader_case_is_refused_by_the_task_result(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """The documented limit: an earlier ok record, a retry whose record was never written. The reader still says ok, the failed task refuses the sign-off."""
    client = Client()
    chain(client, OK)
    verdict = bronze_mirror.acceptance(client, BUCKET, RUN)
    check(verdict["state"] == "ok", f"the reader no longer shows the stale ok that this case documents: {verdict['state']}")
    code, out = signoff(client, tmp_path, described(exits={"check": 1}), capsys)
    check(code == 1 and "signoff: refused" in out and "container check did not exit 0" in out and "acceptance: ok" in out, f"{code} {out}")


def test_a_failed_mirror_record_refuses_the_signoff_though_the_task_succeeded(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    client = Client()
    chain(client, FAILED)
    code, out = signoff(client, tmp_path, described(), capsys)
    check(code == 1 and "signoff: refused" in out and "acceptance: failed" in out and "task: ok" in out, f"{code} {out}")


def test_an_unavailable_mirror_record_refuses_the_signoff(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    client = Client()
    document, digest = report(OK)
    receipt(client, document, digest)
    code, out = signoff(client, tmp_path, described(), capsys)
    check(code == 1 and "signoff: refused" in out and "acceptance: unavailable" in out, f"{code} {out}")


@pytest.mark.parametrize(
    ("describe", "problem"),
    [
        (described(status="RUNNING"), "the task is not STOPPED"),
        (described(exits={"init": 2}), "container init did not exit 0"),
        (described(exits={"load": 1}), "container load did not exit 0"),
        (described(tasks=2), "exactly one task"),
        (
            described(definition="arn:aws:ecs:us-west-2:111111111111:task-definition/example_project_dev_job_silver_host:7"),
            "not the bronze job's task definition",
        ),
        (described(run="bronze_0a1b2c3d"), "does not carry this run's ID"),
        ({"tasks": [], "failures": [{"reason": "MISSING"}]}, "the describe call reported failures"),
        ([], "the describe output is not an object"),
    ],
)
def test_a_task_that_is_not_the_runs_successful_task_refuses_the_signoff(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], describe: object, problem: str
) -> None:
    client = Client()
    chain(client, OK)
    code, out = signoff(client, tmp_path, describe, capsys)
    check(code == 1 and "signoff: refused" in out and problem in out, f"{problem}: {code} {out}")


def test_a_describe_file_that_is_missing_or_not_json_refuses_with_a_fixed_text(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    client = Client()
    chain(client, OK)
    missing = bronze_mirror_signoff.main(["--run", RUN, "--bucket", BUCKET, "--describe", str(tmp_path / "none.json")], client=client)
    broken = tmp_path / "broken.json"
    broken.write_text("{not json secret-token", encoding="utf-8")
    unreadable = bronze_mirror_signoff.main(["--run", RUN, "--bucket", BUCKET, "--describe", str(broken)], client=client)
    out = capsys.readouterr().out
    check(missing == 1 and unreadable == 1 and "signoff: refused" in out and "secret-token" not in out, out)


def test_show_says_it_is_not_a_signoff(capsys: pytest.CaptureFixture[str]) -> None:
    client = Client()
    chain(client, OK)
    code = bronze_mirror.main(["show", "--run", RUN, "--bucket", BUCKET], client=client)
    printed = capsys.readouterr()
    check(code == 0 and "acceptance: ok" in printed.out, printed.out)
    check("not a sign-off" in printed.err and "bronze_mirror_signoff" in printed.err, printed.err)
