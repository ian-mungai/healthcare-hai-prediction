"""Run the IAM preflight through its real entry points against a stand-in ``aws`` that answers from the rendered policy files, and write the report.

Run from the repository root: ``.venv/bin/python -m scripts.infrastructure.run_iam_preflight_e2e --output-directory <NEW_DIRECTORY>``.
No AWS call is made. The stand-in ``aws`` executable sits first on ``PATH`` and decides each simulation with the offline policy evaluator over a copy of
``infra/iam``, so a planted policy defect reaches the preflight the way a real role change would. The report records each case, its defect, the outcome and
the number of stand-in calls.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import stat
import sys
import tempfile
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from scripts.infrastructure.iam_preflight_policies import INFRA, OFFLINE_VALUES, TABLE, load_table
from scripts.infrastructure.render_project_config import REPO_ROOT
from scripts.process import run_command

PROFILE = f"{OFFLINE_VALUES['PROJECT_NAME']}_{OFFLINE_VALUES['ENVIRONMENT']}"
ENV_TEXT = (
    f"AWS_ACCOUNT_ID={OFFLINE_VALUES['AWS_ACCOUNT_ID']}\nAWS_REGION={OFFLINE_VALUES['AWS_REGION']}\nENVIRONMENT={OFFLINE_VALUES['ENVIRONMENT']}\n"
    f"PROJECT_NAME={OFFLINE_VALUES['PROJECT_NAME']}\nS3_BUCKET={OFFLINE_VALUES['DATA_BUCKET_NAME']}\nAWS_PROFILE={PROFILE}\n"
)
STAND_IN = '#!/bin/sh\nexec "$IAM_PREFLIGHT_PYTHON" -m scripts.infrastructure.iam_preflight_stand_in "$@"\n'
READ_ONLY = {("sts", "get-caller-identity"), ("iam", "simulate-principal-policy")}
LIVE_MODULE = "scripts.infrastructure.iam_preflight"
OFFLINE_MODULE = "scripts.infrastructure.iam_preflight_policies"
SECONDS = 120


@dataclass
class Run:
    """The outcome of one command: its exit status, output and elapsed seconds."""

    code: int
    out: str
    err: str
    seconds: float
    calls: list[dict[str, Any]] = field(default_factory=list)


def plant(infra: Path, file: str, old: str, new: str) -> None:
    """Change one text in a copy of the IAM stack; the old text must exist, or the defect would silently not be planted."""
    path = infra / file
    text = path.read_text(encoding="utf-8")
    if old not in text:
        raise RuntimeError(f"cannot plant the defect: {old!r} is not in {file}")
    path.write_text(text.replace(old, new), encoding="utf-8")


def execute(root: Path, module: str, arguments: list[str], environment: dict[str, str]) -> Run:
    started = time.monotonic()
    result = run_command(sys.executable, ["-m", module, *arguments], cwd=REPO_ROOT, env=environment, timeout=SECONDS)
    log = root / "calls.jsonl"
    calls = [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines()] if log.is_file() else []
    log.unlink(missing_ok=True)
    return Run(result.returncode, result.stdout, result.stderr, time.monotonic() - started, calls)


def environment_for(root: Path, infra: Path, mode: str) -> dict[str, str]:
    bin_directory = root / "bin"
    bin_directory.mkdir(exist_ok=True)
    stand_in = bin_directory / "aws"
    stand_in.write_text(STAND_IN, encoding="utf-8")
    stand_in.chmod(stand_in.stat().st_mode | stat.S_IXUSR)
    environment = {key: value for key, value in os.environ.items() if not key.startswith("AWS_")}
    environment.update(
        PATH=f"{bin_directory}{os.pathsep}{environment.get('PATH', '')}",
        IAM_PREFLIGHT_PYTHON=sys.executable,
        IAM_PREFLIGHT_INFRA=str(infra),
        IAM_PREFLIGHT_MODE=mode,
        IAM_PREFLIGHT_LOG=str(root / "calls.jsonl"),
    )
    return environment


def count(run: Run, word: str) -> int:
    return sum(1 for line in run.out.splitlines() if line.startswith(word + " "))


def cases(root: Path, rows: int) -> list[tuple[str, str, Callable[[], Run], Callable[[Run], list[str]]]]:
    """Each case: name, defect, the command run and the check of its result, which returns the failed expectations."""
    env_file = root / "project.env"
    env_file.write_text(ENV_TEXT, encoding="utf-8")

    def planted(edits: list[tuple[str, str, str]]) -> Path:
        infra = root / f"infra_{len(list(root.glob('infra_*')))}"
        shutil.copytree(INFRA, infra, ignore=shutil.ignore_patterns(".terraform*", "tests"))
        for edit in edits:
            plant(infra, *edit)
        return infra

    def live_run(infra: Path = INFRA, mode: str = "ok", extra: list[str] | None = None, env_path: Path = env_file) -> Callable[[], Run]:
        return lambda: execute(root, LIVE_MODULE, ["--env-file", str(env_path), *(extra or [])], environment_for(root, infra, mode))

    def offline_run(infra: Path) -> Callable[[], Run]:
        return lambda: execute(root, OFFLINE_MODULE, ["--infra", str(infra)], environment_for(root, infra, "ok"))

    def expect(run: Run, code: int, **counts: int) -> list[str]:
        failed = [f"exit {run.code}, expected {code}"] if run.code != code else []
        failed += [f"{word} lines {count(run, word)}, expected {number}" for word, number in counts.items() if count(run, word) != number]
        return failed

    def reads_only(run: Run) -> list[str]:
        return [f"call {call['service']} {call['operation']} is not read-only" for call in run.calls if (call["service"], call["operation"]) not in READ_ONLY]

    no_reader_silver = [("roles.tf", '"lakehouse/silver/", "lakehouse/gold/"]', '"lakehouse/gold/"]')]
    wrong_action = [("owner_policies/job_execution.json", '"Action": "ssm:GetParameters",', '"Action": "ssm:GetParameter",')]
    no_layer_policy = [("job_writer.tf", "job_writer_layer_storage", "job_writer_layer_removed")]
    input_in_comment = [
        ("job_writer.tf", 'resource "aws_iam_role_policy" "job_writer_layer_input" {', '# resource "aws_iam_role_policy" "job_writer_layer_input" {')
    ]
    layer_runs_always_on = [
        (
            "job_writer.tf",
            "job_layer_run_prepared_roles = var.job_layer_runs_prepared ? aws_iam_role.job_write_layer_task : {}",
            "job_layer_run_prepared_roles = aws_iam_role.job_write_layer_task",
        )
    ]
    drift_flag_removed = [("drift.tf", "drift_policies = var.drift_prepared ? {", "drift_policies = true ? {")]
    return [
        (
            "offline real tree",
            "none",
            offline_run(INFRA),
            lambda r: expect(r, 0),
        ),
        (
            "offline reader loses silver",
            "silver removed from the reader prefixes",
            offline_run(planted(no_reader_silver)),
            lambda r: expect(r, 1) + ([] if "reader s3:GetObject" in r.out and "lakehouse/silver/" in r.out else ["silver row not named"]),
        ),
        (
            "offline execution role loses a parameter action",
            "ssm:GetParameters becomes ssm:GetParameter",
            offline_run(planted(wrong_action)),
            lambda r: expect(r, 1) + ([] if "job_execution ssm:GetParameters" in r.out else ["job_execution row not named"]),
        ),
        (
            "offline layer input policy is only in a comment",
            "the resource that attaches the layer input read is commented out",
            offline_run(planted(input_in_comment)),
            lambda r: expect(r, 1) + ([] if "job_writer_layer_input" in r.out and "no resource" in r.out else ["attachment not named"]),
        ),
        (
            "offline layer run grants lose their flag",
            "the layer run roles no longer depend on job_layer_runs_prepared",
            offline_run(planted(layer_runs_always_on)),
            lambda r: expect(r, 1) + ([] if "job_layer_runs_prepared" in r.out else ["flag not named"]),
        ),
        (
            "offline drift grants lose their flag",
            "the drift policies no longer depend on drift_prepared",
            offline_run(planted(drift_flag_removed)),
            lambda r: expect(r, 1) + ([] if "drift_prepared" in r.out else ["flag not named"]),
        ),
        (
            "offline policy no longer attached",
            "Terraform stops naming the layer storage policy",
            offline_run(planted(no_layer_policy)),
            lambda r: expect(r, 1) + ([] if "no longer names owner_policies/job_writer_layer_storage.json" in r.out else ["attachment not named"]),
        ),
        (
            "live real tree",
            "none",
            live_run(),
            lambda r: (
                expect(r, 0, PASS=rows, FAIL=0)
                + reads_only(r)
                + ([] if r.calls and r.calls[0]["operation"] == "get-caller-identity" else ["no identity check first"])
            ),
        ),
        (
            "live grant removed",
            "silver removed from the reader prefixes",
            live_run(planted(no_reader_silver)),
            lambda r: expect(r, 1, FAIL=2) + ([] if "FAIL reader s3:GetObject" in r.out else ["silver failure not named"]),
        ),
        (
            "live simulation denied",
            "the project profile lacks iam:SimulatePrincipalPolicy",
            live_run(mode="denied"),
            lambda r: (
                expect(r, 1, PASS=0, FAIL=rows)
                + ([] if "iam:SimulatePrincipalPolicy" in r.out and "applies" in r.out else ["denied message does not name the grant and the apply"])
            ),
        ),
        (
            "live deadline",
            "the simulation never answers",
            live_run(mode="hang", extra=["--call-seconds", "2", "--total-seconds", "6"]),
            lambda r: (
                expect(r, 1, PASS=0, FAIL=rows)
                + ([] if r.seconds < 40 else [f"took {r.seconds:.0f} s"])
                + ([] if "deadline" in r.out else ["deadline not named"])
            ),
        ),
        (
            "live invalid response",
            "the simulation answers with text that is not JSON",
            live_run(mode="garbage"),
            lambda r: expect(r, 1, PASS=0, FAIL=rows),
        ),
        (
            "live wrong identity",
            "the profile authenticates as another user",
            live_run(mode="other_user"),
            lambda r: (
                expect(r, 2) + ([] if not any(call["operation"] == "simulate-principal-policy" for call in r.calls) else ["simulated under the wrong identity"])
            ),
        ),
        (
            "live missing env file",
            "the env file does not exist",
            live_run(env_path=root / "absent.env"),
            lambda r: expect(r, 2) + ([] if not r.calls else ["called AWS without configuration"]),
        ),
        (
            "live read-only check of the whole run",
            "none",
            live_run(),
            lambda r: reads_only(r) + ([] if r.calls else ["no calls logged"]),
        ),
    ]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-directory", type=Path, required=True, help="New directory for the report; an existing path is refused.")
    arguments = parser.parse_args()
    if arguments.output_directory.exists():
        sys.stderr.write("The output directory exists; pass a new path.\n")
        return 2
    rows = len(load_table(TABLE)["checks"])
    results = []
    with tempfile.TemporaryDirectory(prefix="iam_preflight_e2e_") as scratch:
        root = Path(scratch)
        for name, defect, run, check in cases(root, rows):
            outcome = run()
            failed = check(outcome)
            results.append(
                {
                    "case": name,
                    "defect": defect,
                    "passed": not failed,
                    "failed_expectations": failed,
                    "exit": outcome.code,
                    "seconds": round(outcome.seconds, 1),
                    "pass_lines": count(outcome, "PASS"),
                    "fail_lines": count(outcome, "FAIL"),
                    "first_fail_line": next((line for line in outcome.out.splitlines() if line.startswith("FAIL")), ""),
                    "stand_in_calls": len(outcome.calls),
                    "stderr": outcome.err.strip()[-300:],
                }
            )
            sys.stdout.write(f"{'OK  ' if not failed else 'FAIL'} {name}: {failed or ''}\n")
    arguments.output_directory.mkdir(parents=True)
    report = {
        "tool": "iam_preflight",
        "table_rows": rows,
        "commands": {
            "live": ".venv/bin/python -m scripts.infrastructure.iam_preflight --env-file <ENV_FILE>",
            "offline": ".venv/bin/python -m scripts.infrastructure.iam_preflight_policies",
        },
        "substitution": "the aws executable is a stand-in that evaluates the rendered infra/iam policies offline; no AWS call was made",
        "results": results,
    }
    (arguments.output_directory / "iam_preflight_e2e.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    failed_cases = [item["case"] for item in results if not item["passed"]]
    sys.stdout.write(f"{len(results) - len(failed_cases)} of {len(results)} cases passed\n")
    return 1 if failed_cases else 0


if __name__ == "__main__":
    raise SystemExit(main())
