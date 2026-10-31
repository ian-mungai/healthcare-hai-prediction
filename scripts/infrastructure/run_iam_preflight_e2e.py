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


def cases(root: Path, rows: int, silver: int) -> list[tuple[str, str, Callable[[], Run], Callable[[Run], list[str]]]]:
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
    gold_backup_removed = [("job_writer.tf", 'prefixes        = ["gold", "catalog_backups"]', 'prefixes        = ["gold"]')]
    silver_pass_removed = [
        (
            "policies/healthcare_hai_prediction_iam_policy.json",
            '        "arn:aws:iam::${AWS_ACCOUNT_ID}:role/${PROJECT_NAME}_job_write_silver_task_${ENVIRONMENT}",\n',
            "",
        )
    ]
    quality_pull_removed = [
        ("owner_policies/job_execution.json", '        "arn:aws:ecr:${AWS_REGION}:${AWS_ACCOUNT_ID}:repository/${PROJECT_NAME}_quality_${ENVIRONMENT}",\n', "")
    ]
    run_task_other_family = [
        (
            "policies/healthcare_hai_prediction_ecs_policy.json",
            '"Sid": "RunTaggedProjectTasks",\n      "Effect": "Allow",\n      "Action": [\n        "ecs:RunTask"\n      ],\n'
            '      "Resource": "arn:aws:ecs:${AWS_REGION}:${AWS_ACCOUNT_ID}:task-definition/${PROJECT_NAME}_${ENVIRONMENT}_*:*"',
            '"Sid": "RunTaggedProjectTasks",\n      "Effect": "Allow",\n      "Action": [\n        "ecs:RunTask"\n      ],\n'
            '      "Resource": "arn:aws:ecs:${AWS_REGION}:${AWS_ACCOUNT_ID}:task-definition/${PROJECT_NAME}_${ENVIRONMENT}_job_train:*"',
        )
    ]
    self_simulation_removed = [("policies/healthcare_hai_prediction_iam_policy.json", '        "arn:aws:iam::${AWS_ACCOUNT_ID}:user/${aws:username}",\n', "")]
    group = "arn:aws:logs:${AWS_REGION}:${AWS_ACCOUNT_ID}:log-group:${PROJECT_NAME}_jobs_${ENVIRONMENT}"
    deny_init_logs = [
        (
            "owner_policies/job_execution.json",
            '"Sid": "WriteJobLogs",',
            '"Sid": "DenyInitLogs",\n      "Effect": "Deny",\n      "Action": ["logs:CreateLogStream", "logs:PutLogEvents"],\n'
            f'      "Resource": "{group}:log-stream:silver/init/*"\n    }},\n    {{\n      "Sid": "WriteJobLogs",',
        )
    ]
    deny_publish_uploads = [
        (
            "owner_policies/job_writer_layer_upload.json",
            '"Sid": "LayerRunObjects",',
            '"Sid": "DenyPublishUploads",\n      "Effect": "Deny",\n      "Action": ["s3:PutObject"],\n'
            '      "Resource": "arn:aws:s3:::*/lakehouse/layer_runs/silver/*/publish/*"\n    },\n    {\n      "Sid": "LayerRunObjects",',
        )
    ]
    no_owner_listing = [("policies/healthcare_hai_prediction_s3_policy.json", '        "s3:ListBucketVersions",\n', "")]
    deny_evidence_reads = [
        (
            "policies/healthcare_hai_prediction_s3_policy.json",
            '"Sid": "InspectApprovedAcquisitionBucket",',
            '"Sid": "DenyEvidenceReads",\n      "Effect": "Deny",\n      "Action": ["s3:GetObject"],\n'
            '      "Resource": "arn:aws:s3:::*/lakehouse/layer_runs/*"\n    },\n    {\n      "Sid": "InspectApprovedAcquisitionBucket",',
        )
    ]
    deny_stage_uploads = [
        (
            "policies/healthcare_hai_prediction_s3_policy.json",
            '"Sid": "InspectApprovedAcquisitionBucket",',
            '"Sid": "DenyStageUploads",\n      "Effect": "Deny",\n      "Action": ["s3:PutObject"],\n'
            '      "Resource": "arn:aws:s3:::*/lakehouse/run_stage/*"\n    },\n    {\n      "Sid": "InspectApprovedAcquisitionBucket",',
        )
    ]
    logs_granted_twice = [
        (
            "owner_policies/job_execution.json",
            '"Sid": "WriteJobLogs",',
            '"Sid": "WriteAnyLogs",\n      "Effect": "Allow",\n      "Action": ["logs:CreateLogStream", "logs:PutLogEvents"],\n'
            f'      "Resource": "{group}:log-stream:*"\n    }},\n    {{\n      "Sid": "WriteJobLogs",',
        )
    ]
    containers = ("init", "fetch", "pin", "build", "validate", "verify", "export", "publish", "upload")
    evidence = ("layer_run.json", "pins.json", "publish/candidates.json", "publish/reconcile.json", "publish/backup.json", "publish/commit.json")
    silver_scope = ["--scope", "silver"]

    def failed(run: Run, *words: str) -> list[str]:
        """Each word must appear in a FAIL line, so that the denied call is named by role, action and resource."""
        lines = [line for line in run.out.splitlines() if line.startswith("FAIL ")]
        return [f"no FAIL line names {word}" for word in words if not any(word in line for line in lines)]

    silver_roles = {"job_execution", "write_silver", "project_user"}

    def silver_roles_only(run: Run) -> list[str]:
        named = {line.split()[1] for line in run.out.splitlines() if line.startswith(("PASS ", "FAIL "))}
        return [f"role {role} is outside the silver path" for role in sorted(named - silver_roles)] + [
            f"role {role} has no silver check" for role in sorted(silver_roles - named)
        ]

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
            "live silver scope real tree",
            "none",
            live_run(extra=silver_scope),
            lambda r: (
                expect(r, 0, PASS=silver, FAIL=0)
                + reads_only(r)
                + silver_roles_only(r)
                + ([] if len(r.calls) == silver + 1 else [f"{len(r.calls)} stand-in calls, expected {silver + 1}"])
                + ([] if "silver checks passed" in r.out else ["the summary does not name the scope"])
            ),
        ),
        (
            "live silver scope ignores a gold defect",
            "gold loses its catalog backup prefix",
            live_run(planted(gold_backup_removed), extra=silver_scope),
            lambda r: expect(r, 0, PASS=silver, FAIL=0),
        ),
        (
            "live whole table sees the same gold defect",
            "gold loses its catalog backup prefix",
            live_run(planted(gold_backup_removed)),
            lambda r: expect(r, 1) + ([] if "FAIL write_gold" in r.out and "catalog_backups" in r.out else ["gold failure not named"]),
        ),
        (
            "live silver scope names a denied PassRole",
            "the project user may not pass the silver task role",
            live_run(planted(silver_pass_removed), extra=silver_scope),
            lambda r: (
                expect(r, 1, PASS=silver - 1, FAIL=1)
                + (
                    []
                    if any(line.startswith("FAIL project_user iam:PassRole") and "job_write_silver_task" in line for line in r.out.splitlines())
                    else ["denied call not named"]
                )
            ),
        ),
        (
            "live silver scope names each denied image pull",
            "the execution role may not pull the quality image",
            live_run(planted(quality_pull_removed), extra=silver_scope),
            lambda r: (
                expect(r, 1, PASS=silver - 2, FAIL=2)
                + [
                    f"{action} on the quality repository not named"
                    for action in ("ecr:BatchGetImage", "ecr:GetDownloadUrlForLayer")
                    if not any(line.startswith(f"FAIL job_execution {action}") and "_quality_" in line for line in r.out.splitlines())
                ]
            ),
        ),
        (
            "live silver scope names a RunTask refused for the silver family",
            "the project user may run only the train task definition",
            live_run(planted(run_task_other_family), extra=silver_scope),
            lambda r: (
                expect(r, 1, PASS=silver - 1, FAIL=1) + ([] if "FAIL project_user ecs:RunTask" in r.out and "job_silver" in r.out else ["RunTask not named"])
            ),
        ),
        (
            "live silver scope needs the user to simulate itself",
            "the project user may simulate roles but not its own user",
            live_run(planted(self_simulation_removed), extra=silver_scope),
            lambda r: expect(r, 1, FAIL=1) + ([] if "FAIL project_user iam:SimulatePrincipalPolicy" in r.out else ["self simulation not named"]),
        ),
        (
            "live silver scope names each container whose log stream is denied",
            "init may not create or write its log stream",
            live_run(planted(deny_init_logs), extra=silver_scope),
            lambda r: (
                expect(r, 1, PASS=silver - 2, FAIL=2)
                + failed(r, "FAIL job_execution logs:CreateLogStream", "FAIL job_execution logs:PutLogEvents", "log-stream:silver/init/")
            ),
        ),
        (
            "live silver scope covers a log stream of every container",
            "the execution role may write only the publish log stream",
            live_run(
                planted([("owner_policies/job_execution.json", "_jobs_${ENVIRONMENT}:log-stream:*", "_jobs_${ENVIRONMENT}:log-stream:silver/publish/*")]),
                extra=silver_scope,
            ),
            lambda r: (
                expect(r, 1, FAIL=2 * (len(containers) - 1))
                + failed(r, *(f"log-stream:silver/{name}/" for name in containers if name != "publish"))
                + ([] if not any("silver/publish/" in line for line in r.out.splitlines() if line.startswith("FAIL ")) else ["the publish stream failed"])
            ),
        ),
        (
            "live silver scope names each denied publish upload",
            "uploads under publish are denied",
            live_run(planted(deny_publish_uploads), extra=silver_scope),
            lambda r: (
                expect(r, 1, FAIL=5)
                + failed(
                    r,
                    *(
                        f"layer_runs/silver/example_run/publish/{name}"
                        for name in ("candidates.json", "reconcile.json", "backup.json", "commit.json", "export/manifest.json")
                    ),
                )
            ),
        ),
        (
            "live silver scope names the owner version listing",
            "the project user may not list object versions",
            live_run(planted(no_owner_listing), extra=silver_scope),
            lambda r: expect(r, 1, FAIL=1) + failed(r, "FAIL project_user s3:ListBucketVersions"),
        ),
        (
            "live silver scope names each denied evidence read",
            "the project user may not read the uploaded run records",
            live_run(planted(deny_evidence_reads), extra=silver_scope),
            lambda r: (
                expect(r, 1, FAIL=len(evidence) + 2)
                + failed(r, *(f"layer_runs/silver/example_run/{name}" for name in (*evidence, "publish/export/manifest.json", "silver_snapshot.json")))
            ),
        ),
        (
            "live silver scope names each denied code upload",
            "the project user may not stage the code archive",
            live_run(planted(deny_stage_uploads), extra=silver_scope),
            lambda r: expect(r, 1, FAIL=2) + failed(r, "inputs/code.tar.gz", "inputs/code.sha256"),
        ),
        (
            "offline row granted by two statements",
            "a second statement also grants the log streams, so a mutant on one grant leaves the row passing",
            offline_run(planted(logs_granted_twice)),
            lambda r: expect(r, 1) + ([] if "but the row declares one grant" in r.out and "WriteAnyLogs" in r.out else ["double grant not named"]),
        ),
        (
            "live unknown scope",
            "the scope bronze has no table",
            live_run(extra=["--scope", "bronze"]),
            lambda r: expect(r, 2) + ([] if not r.calls else ["called AWS with an unknown scope"]),
        ),
        (
            "offline project user loses the silver PassRole",
            "the project user may not pass the silver task role",
            offline_run(planted(silver_pass_removed)),
            lambda r: expect(r, 1) + ([] if "project_user iam:PassRole" in r.out else ["project user row not named"]),
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
    parser.add_argument(
        "--case", action="append", default=[], help="Run only the cases whose name contains this text; repeat it to name more. The default runs every case."
    )
    arguments = parser.parse_args()
    if arguments.output_directory.exists():
        sys.stderr.write("The output directory exists; pass a new path.\n")
        return 2
    checks = load_table(TABLE)["checks"]
    rows, silver = len(checks), sum(1 for row in checks if row.get("scope") == "silver")
    results = []
    with tempfile.TemporaryDirectory(prefix="iam_preflight_e2e_") as scratch:
        root = Path(scratch)
        for name, defect, run, check in cases(root, rows, silver):
            if arguments.case and not any(text in name for text in arguments.case):
                continue
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
        "silver_rows": silver,
        "commands": {
            "live": ".venv/bin/python -m scripts.infrastructure.iam_preflight --env-file <ENV_FILE>",
            "silver": ".venv/bin/python -m scripts.infrastructure.iam_preflight --env-file <ENV_FILE> --scope silver",
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
