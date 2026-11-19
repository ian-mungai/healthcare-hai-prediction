"""The sign-off of a bronze run's Glue mirror needs the recorded outcome and the successful result of the matching task.

``show`` reads only what the check phase recorded, so it cannot see a retry whose record was never written (the role lost its grant, or the task
stopped before it wrote). That is a documented limit of the reader: the last recorded outcome can be an older ``ok``. The sign-off therefore also
requires the described task of the run (the output of ``aws ecs describe-tasks``) to be stopped with every container at exit 0, to be the bronze job's
own task definition and to carry this run's ID.

A second limit sits under the first: with both mirror grants withdrawn in full after a recorded ``ok``, the retry exits 0 as unavailable and records nothing,
so the stopped task and the old ``ok`` both pass. The sign-off therefore also requires the latest task's loader report: its SHA-256 must equal the report
digest of the LAST stored attempt. A report that is missing, unreadable, matches no attempt or matches an earlier attempt refuses the sign-off."""

from __future__ import annotations

import hashlib
import json
import weakref
from pathlib import Path
from typing import Any

import pytest

from scripts.lakehouse import bronze_mirror, bronze_mirror_signoff
from tests.support import check
from tests.test_bronze_mirror_attempts import BUCKET, FAILED, OK, RUN, Client, receipt, record, report

JOBS = json.loads((Path(__file__).resolve().parents[1] / "config/orchestration/ecs_broker_jobs.json").read_text(encoding="utf-8"))["jobs"]["bronze"]
# The loader report of the last attempt that ``chain`` stored for a client: what the latest task would have written.
LATEST: weakref.WeakKeyDictionary[Client, bytes] = weakref.WeakKeyDictionary()
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


def report_bytes(mirror: dict[str, Any], stamp: str = "20261118T230000Z") -> bytes:
    """The bytes of a loader report whose SHA-256 is the digest ``report`` computes, as the file the owner reads from the task."""
    document, _ = report(mirror, stamp)
    return json.dumps(document, sort_keys=True).encode()


def chain(client: Client, mirror: dict[str, Any]) -> None:
    document, digest = report(mirror)
    receipt(client, document, digest)
    record(client, mirror)
    LATEST[client] = report_bytes(mirror)


def signoff(
    client: Client, tmp_path: Path, describe: object, capsys: pytest.CaptureFixture[str], run: str = RUN, loader_report: bytes | None | str = "latest"
) -> tuple[int, str]:
    """The sign-off with the latest task's describe output and loader report: by default the last chained attempt's report, or ``None`` for none."""
    path = tmp_path / "describe.json"
    path.write_text(json.dumps(describe), encoding="utf-8")
    arguments = ["--run", run, "--bucket", BUCKET, "--describe", str(path)]
    if loader_report == "latest":
        loader_report = LATEST.get(client, report_bytes(OK))
    if isinstance(loader_report, bytes):
        (tmp_path / "loader_report.json").write_bytes(loader_report)
        arguments += ["--report", str(tmp_path / "loader_report.json")]
    code = bronze_mirror_signoff.main(arguments, client=client)
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


UNAVAILABLE = {
    "status": "unavailable",
    "reason": "alpha_data failed: AccessDeniedException from Glue",
    "tables": {"alpha_data": "failed: AccessDeniedException from Glue"},
}


def test_a_full_grant_withdrawal_after_a_recorded_ok_refuses_the_signoff(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """The round 3 reproduction. A recorded ok, then both mirror grants withdrawn: the retry exits 0, its loader report says unavailable and no attempt is
    stored. The reader still says ok and the latest task passed, but the latest report matches no stored attempt, so the sign-off is refused."""
    client = Client()
    chain(client, OK)
    retry_report = report_bytes(UNAVAILABLE, "20261118T231500Z")
    code, out = signoff(client, tmp_path, described(), capsys, loader_report=retry_report)
    check(code == 1 and "signoff: refused" in out and "acceptance: ok" in out and "task: ok" in out, f"{code} {out}")
    check("matches no stored attempt" in out, out)


def test_a_signoff_without_a_loader_report_is_refused(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """Without the report there is no evidence for the latest task: an ok record and a successful task are not enough."""
    client = Client()
    chain(client, OK)
    code, out = signoff(client, tmp_path, described(), capsys, loader_report=None)
    check(code == 1 and "signoff: refused" in out and "no loader report was given" in out, f"{code} {out}")


def test_a_report_that_is_an_earlier_attempt_and_not_the_latest_is_refused(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    client = Client()
    chain(client, OK)
    record(client, FAILED, "20261118T231500Z")
    code, out = signoff(client, tmp_path, described(), capsys, loader_report=report_bytes(OK))
    check(code == 1 and "signoff: refused" in out and "an earlier attempt, not the latest" in out, f"{code} {out}")


def test_the_report_of_the_latest_attempt_passes_when_there_are_several(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    client = Client()
    chain(client, FAILED)
    record(client, OK, "20261118T231500Z")
    code, out = signoff(client, tmp_path, described(), capsys, loader_report=report_bytes(OK, "20261118T231500Z"))
    check(code == 0 and "signoff: ok" in out and "report: ok" in out, f"{code} {out}")


@pytest.mark.parametrize("content", [b"", b"{not json", b"[]", b'"text"'])
def test_a_report_that_is_not_a_json_object_is_refused_with_a_fixed_text(tmp_path: Path, capsys: pytest.CaptureFixture[str], content: bytes) -> None:
    client = Client()
    chain(client, OK)
    code, out = signoff(client, tmp_path, described(), capsys, loader_report=content)
    check(code == 1 and "signoff: refused" in out and "the loader report is not a JSON object" in out and "not json" not in out, f"{code} {out}")


def test_a_report_file_that_is_missing_or_a_link_is_refused(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    client = Client()
    chain(client, OK)
    path = tmp_path / "describe.json"
    path.write_text(json.dumps(described()), encoding="utf-8")
    (tmp_path / "real.json").write_bytes(report_bytes(OK))
    (tmp_path / "link.json").symlink_to(tmp_path / "real.json")
    base = ["--run", RUN, "--bucket", BUCKET, "--describe", str(path), "--report"]
    missing = bronze_mirror_signoff.main([*base, str(tmp_path / "none.json")], client=client)
    linked = bronze_mirror_signoff.main([*base, str(tmp_path / "link.json")], client=client)
    out = capsys.readouterr().out
    check(missing == 1 and linked == 1 and out.count("the loader report cannot be read") == 2 and "signoff: ok" not in out, out)


def test_a_report_is_not_matched_when_the_attempt_records_cannot_be_read(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    client = Client()
    chain(client, OK)
    client.objects[bronze_mirror.attempt_key(RUN, 1)] = b"{}"
    code, out = signoff(client, tmp_path, described(), capsys)
    check(code == 1 and "acceptance: unavailable" in out and "signoff: refused" in out and "signoff: ok" not in out, f"{code} {out}")


def test_before_the_grant_a_run_with_no_attempt_is_not_signed_off(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """The rollout case: no attempt was ever recorded. The retry exits 0, and the sign-off says nothing is registered yet: the acceptance is unavailable
    and no report can match a stored attempt."""
    client = Client()
    document, digest = report(UNAVAILABLE)
    receipt(client, document, digest)
    code, out = signoff(client, tmp_path, described(), capsys, loader_report=report_bytes(UNAVAILABLE))
    check(code == 1 and "acceptance: unavailable" in out and "no mirror attempt record exists" in out and "signoff: refused" in out, f"{code} {out}")
    check("no stored attempt" in out, out)


def test_the_report_time_is_shown_only_in_its_fixed_form(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """The texts echo nothing from the files: a report time that is not a real time is not printed."""
    client = Client()
    document, _ = report(OK)
    receipt(client, document, report(OK)[1])
    record(client, OK)
    hostile = json.dumps(document | {"run_at_utc": "secret-token"}, sort_keys=True).encode()
    client.objects[bronze_mirror.attempt_key(RUN, 1)] = json.dumps(bronze_mirror.attempt_record(RUN, document, hashlib.sha256(hostile).hexdigest(), 1)).encode()
    code, out = signoff(client, tmp_path, described(), capsys, loader_report=hostile)
    check("secret-token" not in out and "no valid time" in out, out)
