"""Plant qualification defects in disposable source copies and retain named-case evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.orchestration.aws_qualify_cases import SOURCES
from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]
DEFECTS = {
    "production_namespace": ("orchestration/aws_qualify.py", '"gold": "gold_e2e"', '"gold": "gold"', ("binding_gold_commit",)),
    "namespace_override": (
        "orchestration/aws_qualify.py",
        'set(params) != {"layer", "step"}',
        'not {"layer", "step"} <= set(params)',
        ("namespace_override", "prefix_override"),
    ),
    "production_prefix": (
        "orchestration/aws_qualify.py",
        'table_prefix = f"lakehouse/{namespace}/"',
        'table_prefix = f"lakehouse/{layer}/"',
        ("binding_gold_commit",),
    ),
    "gold_backup_shared": (
        "orchestration/aws_qualify.py",
        'backup_prefix = table_prefix + "catalog_backups/"',
        'backup_prefix = "lakehouse/catalog_backups/"',
        ("binding_gold_backup",),
    ),
    "fixture_class": (
        "orchestration/pipeline_render.py",
        'if mode == "publish_fixture":\n        return "offline"',
        'if mode == "publish_fixture":\n        return "lakehouse_write"',
        ("fixture_stays_offline",),
    ),
    "m1_reader_role": (
        "orchestration/aws_qualify.py",
        '"task_role": copy.deepcopy(target["job_roles"][selected.task_role_key])',
        '"task_role": copy.deepcopy(target["job_roles"]["read_task"])',
        ("case_completion",),
    ),
    "m1_missing_role": (
        "orchestration/aws_qualify.py",
        '        "task_role": copy.deepcopy(target["job_roles"][selected.task_role_key]),\n',
        '        "removed_role": copy.deepcopy(target["job_roles"][selected.task_role_key]),\n',
        ("case_completion",),
    ),
    "m1_validation_removed": (
        "orchestration/ecs_target.py",
        (
            '            if execution.get("admission", {}).get("mode") == "aws_qualify":\n                from orc'
            "hestration import aws_qualify\n\n                aws_qualify.validate_record(execution, placement)\n"
        ),
        "",
        ("missing_m1_refused",),
    ),
    "command_binding_removed": (
        "orchestration/aws_qualify.py",
        '    _command(job, selected, admission["run_id"])\n',
        "",
        ("command_namespace_override", "command_fixture"),
    ),
    "principal_check_removed": (
        "orchestration/aws_qualify.py",
        'sorted(secrets, key=lambda item: item["name"]) != expected_secrets or sorted(job["secret_names"]) != ["POLARIS_CLIENT_ID", "POLARIS_CLIENT_SECRET"]',
        'sorted(secrets, key=lambda item: item["name"]) == expected_secrets and job["secret_names"] == []',
        ("wrong_principal",),
    ),
    "authorization_check_removed": (
        "orchestration/aws_qualify.py",
        (
            '    if job.get("writer_authorization") != attempt["execution"]["writer_authorization"]:\n        rais'
            'e PlacementError("qualification authorization differs from admission")\n'
        ),
        "",
        ("changed_writer_evidence",),
    ),
    "m1_consistency_removed": (
        "orchestration/aws_qualify.py",
        """    admission = execution["admission"]
    selected = binding("pipeline_publish", {"layer": admission["layer"], "step": job["step"]}, admission["mode"])
    role = record["task_role"]
    suffix = "job_read_task" if selected.job_class == "lakehouse_read" else "job_write_e2e_task"
    if any(record[key] != value for key, value in selected.document().items()) or (
        record["run_id"] != admission["run_id"]
        or record["try_id"] != placement["try_id"]
        or record["try_id"] != dispatch["request"]["startedBy"]
        or record["target"] != placement["target"]
        or record["job_class"] != admission["job_class"]
        or record["task_definition_arn"] != dispatch["request"]["taskDefinition"]
        or record["task_definition_arn"] != dispatch["definition"].get("task_definition_arn")
        or record["job_class"] != dispatch["definition"].get("job_class")
        or record["capacity_provider"] != placement["capacity_provider"]
        or record["capacity_provider"] != dispatch["request"]["capacityProviderStrategy"][0]["capacityProvider"]
        or record["task_role_key"] != dispatch["definition"].get("task_role_key")
        or re.fullmatch(re.escape(dispatch["project_tag"] + "_" + suffix) + r"(?:_[a-z0-9]+)*", role["name"]) is None
        or not role["arn"].endswith("/" + role["name"])
        or role["arn"].split(":")[1:5] != [record["task_definition_arn"].split(":")[1], "iam", "", record["task_definition_arn"].split(":")[4]]
        or role["unique_id"] != job["task_role_id"]
    ):
        raise PlacementError("qualification M1 dispatch record differs from its persisted request")
""",
        "",
        (
            "replay_refuses_role_name",
            "replay_refuses_role_arn",
            "replay_refuses_role_account",
            "replay_refuses_role_unique_id",
            "replay_refuses_step",
            "replay_refuses_run_id",
            "replay_refuses_try_id",
            "replay_refuses_task_definition_arn",
        ),
    ),
    "m1_role_name_check_disabled": (
        "orchestration/aws_qualify.py",
        'role["name"]) is None\n        or not role["arn"]',
        'role["name"]) is False\n        or not role["arn"]',
        ("replay_refuses_role_name", "replay_refuses_role_project"),
    ),
    "m1_role_arn_check_removed": (
        "orchestration/aws_qualify.py",
        '        or not role["arn"].endswith("/" + role["name"])\n',
        "",
        ("replay_refuses_role_arn",),
    ),
    "m1_role_arn_scope_check_removed": (
        "orchestration/aws_qualify.py",
        '        or role["arn"].split(":")[1:5] != [record["task_definition_arn"].split(":")[1], "iam", "", record["task_definition_arn"].split(":")[4]]\n',
        "",
        ("replay_refuses_role_account",),
    ),
    "m1_unique_id_check_removed": (
        "orchestration/aws_qualify.py",
        '        or role["unique_id"] != job["task_role_id"]\n',
        "",
        ("replay_refuses_role_unique_id", "replay_refuses_job_role_id"),
    ),
    "m1_step_from_record": (
        "orchestration/aws_qualify.py",
        'selected = binding("pipeline_publish", {"layer": admission["layer"], "step": job["step"]}, admission["mode"])',
        'selected = binding("pipeline_publish", {"layer": admission["layer"], "step": record["step"]}, admission["mode"])',
        ("replay_refuses_step", "replay_refuses_job_step"),
    ),
    "m1_inputs_not_saved": (
        "orchestration/ecs_dispatch.py",
        '                placement["dispatch"]["qualification_job"] = {"step": job["qualification"]["step"], "task_role_id": job["task_role_id"]}\n',
        "",
        ("case_completion",),
    ),
    "m1_request_try_check_removed": (
        "orchestration/aws_qualify.py",
        '        or record["try_id"] != dispatch["request"]["startedBy"]\n',
        "",
        ("replay_refuses_request_try_id",),
    ),
    "m1_definition_arn_check_removed": (
        "orchestration/aws_qualify.py",
        '        or record["task_definition_arn"] != dispatch["definition"].get("task_definition_arn")\n',
        "",
        ("replay_refuses_definition_arn",),
    ),
    "m1_definition_class_check_removed": (
        "orchestration/aws_qualify.py",
        '        or record["job_class"] != dispatch["definition"].get("job_class")\n',
        "",
        ("replay_refuses_definition_class",),
    ),
    "unsupported_template_binding": (
        "orchestration/ecs_target.py",
        '        if template != "pipeline_publish":\n            return None\n',
        "",
        ("launcher_refuses_gold_export", "no_qualification_row_gold_export", "no_qualification_row_pipeline_dbt"),
    ),
    "replay_case_same_step": (
        "scripts/orchestration/aws_qualify_cases.py",
        '("step", {"step": "candidates"}),',
        '("step", {"step": "commit"}),',
        ("replay_refuses_step",),
    ),
    "launcher_case_no_submit": (
        "scripts/orchestration/aws_qualify_cases.py",
        "            server.m_submit(context)\n",
        "            dict(context.params)\n",
        ("launcher_refuses_gold_export", "launcher_refuses_pipeline_dbt"),
    ),
}


def main() -> int:
    """Run an unchanged control and each defect without altering candidate or lint settings."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    subject_paths = (*SOURCES, "scripts/orchestration/aws_qualify_defects.py", "pyproject.toml")
    before = {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in subject_paths}
    records: dict[str, Any] = {}
    with tempfile.TemporaryDirectory(prefix="qualification_defects_") as scratch:
        tree = Path(scratch) / "tree"
        tree.mkdir()
        ignore = shutil.ignore_patterns(
            "__pycache__", "*.pyc", ".env", ".env.*", "*.tfvars.json", "*terraform.tfstate*", "secrets", ".aws", "source_registry.json"
        )
        for folder in ("orchestration", "scripts", "services", "data_contracts", "config/orchestration"):
            shutil.copytree(ROOT / folder, tree / folder, ignore=ignore)
        for name in ("pyproject.toml", "docker-compose.yaml"):
            shutil.copy2(ROOT / name, tree / name)
        for name in ("control", *DEFECTS):
            source_name, old, new, expected = DEFECTS.get(name, ("", "", "", ()))
            source = tree / source_name if source_name else None
            original = source.read_text() if source is not None else ""
            try:
                if source is not None:
                    if original.count(old) != 1:
                        raise ValueError("mutation anchor changed: " + name)
                    source.write_text(original.replace(old, new))
                    formatted = run_command(str(ROOT / ".venv/bin/ruff"), ["format", source_name], cwd=tree, timeout=30)
                    linted = run_command(str(ROOT / ".venv/bin/ruff"), ["check", "--no-cache", source_name], cwd=tree, timeout=30)
                    if formatted.returncode or linted.returncode:
                        records[name] = {
                            "status": "blocked_before_load",
                            "format_exit": formatted.returncode,
                            "lint_exit": linted.returncode,
                            "diagnostics": formatted.stdout + formatted.stderr + linted.stdout + linted.stderr,
                        }
                        continue
                output = args.output.parent / ("defect_" + name + ".json")
                run = run_command(
                    str(ROOT / ".venv/bin/python"), ["-B", "-m", "scripts.orchestration.aws_qualify_cases", "--output", str(output)], cwd=tree, timeout=60
                )
                (args.output.parent / ("defect_" + name + ".log")).write_text(run.stdout + run.stderr)
                observed = json.loads(output.read_text()) if output.exists() else {}
                caught = (
                    run.returncode == 0 and observed.get("failed") == []
                    if name == "control"
                    else run.returncode == 1 and set(expected) <= set(observed.get("failed", []))
                )
                if name in {"m1_missing_role", "m1_inputs_not_saved"}:
                    caught = caught and (observed.get("exception") or {}).get("message") == "qualification M1 dispatch record is incomplete"
                if name == "m1_reader_role":
                    caught = (
                        caught and (observed.get("exception") or {}).get("message") == "qualification M1 dispatch record differs from its persisted request"
                    )
                records[name] = {
                    "exit": run.returncode,
                    "expected_cases": list(expected),
                    "failed": observed.get("failed"),
                    "exception": observed.get("exception"),
                    "caught": caught,
                    "format_exit": 0 if source_name else None,
                    "lint_exit": 0 if source_name else None,
                    "source_sha256": hashlib.sha256(source.read_bytes()).hexdigest() if source else None,
                }
            finally:
                if source is not None:
                    source.write_text(original)
    unchanged = before == {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in subject_paths}
    report = {
        "time": datetime.now(UTC).isoformat(),
        "command": [sys.executable, "-m", "scripts.orchestration.aws_qualify_defects", *sys.argv[1:]],
        "source_sha256": before,
        "records": records,
        "candidate_unchanged": unchanged,
        "cleanup": "done",
        "limits": "Disposable code copies and synthetic transports. No native writer qualification or AWS execution.",
    }
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    sys.stdout.write(
        json.dumps(
            {
                "report": str(args.output),
                "caught": sum(row.get("caught", False) for name, row in records.items() if name != "control"),
                "candidate_unchanged": unchanged,
            }
        )
        + "\n"
    )
    return int(not unchanged or any(not row.get("caught") for row in records.values()))


if __name__ == "__main__":
    sys.exit(main())
