"""Check dispatch defects in disposable source copies without touching the candidate."""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import shutil
import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.orchestration.ecs_dispatch_cases import SOURCES
from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]


def altered(name: str, source: str) -> str:
    """Plant one named contract defect in a disposable copy."""
    if name == "write_retried":
        tree = ast.parse(source)
        node = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "writer_retry_blocked")
        lines = source.splitlines(keepends=True)
        lines[node.lineno - 1 : node.end_lineno] = ["def writer_retry_blocked(execution: Mapping[str, Any]) -> bool:\n    return False\n"]
        return "".join(lines)
    if name in ("authority_recheck_removed", "intent_deadline_removed"):
        before = (
            "            ecs_admission.current(state, attempt, registry_kind=self.registry_kind)\n"
            if name == "authority_recheck_removed"
            else '            if self.clock.monotonic() >= deadline:\n                raise PlacementError("ECS operation deadline expired")\n'
        )
        if source.count(before) != 2:
            raise ValueError("mutation anchor changed: " + name)
        index = source.rfind(before)
        return source[:index] + source[index + len(before) :]
    payloads = {
        "container_command_override": 'request["overrides"]["containerOverrides"][0]["command"] = ["true"]',
        "task_role_override": 'request["overrides"]["taskRoleArn"] = "synthetic_role"',
        "extra_request_field": 'request["group"] = "synthetic_group"',
        "extra_environment_field": 'request["overrides"]["containerOverrides"][0]["environment"][0]["extra"] = "synthetic"',
    }
    if name in payloads:
        before = "        return request\n"
        if source.count(before) != 1:
            raise ValueError("mutation anchor changed: " + name)
        return source.replace(before, "        " + payloads[name] + "\n" + before)
    replacements = {
        "writer_evidence_reused": [
            (
                '                    and previous["writer_authorization"]["publish_approval"] == authorization["publish_approval"]',
                '                    and previous.get("writer_authorization") == authorization',
            )
        ],
        "non_write_authorization_allowed": [
            ('        elif authorization is not None:\n            raise PlacementError("non-write admission cannot carry writer authorization")\n', "")
        ],
        "writer_template_reused": [
            (
                '                    and previous.get("admission") == admission',
                '                    and prior.get("template") == attempt["template"]\n                    and previous.get("admission") == admission',
            )
        ],
        "writer_params_reused": [
            (
                '                    and previous.get("admission") == admission',
                '                    and prior.get("params") == attempt["params"]\n                    and previous.get("admission") == admission',
            )
        ],
        "offline_reads_authority": [
            ('if job_class.startswith("lakehouse_") else None', 'if job_class in ("offline", "lakehouse_read", "lakehouse_write") else None')
        ],
        "seal_reads_authority": [
            ('if job_class.startswith("lakehouse_") else None', 'if job_class in ("seal", "lakehouse_read", "lakehouse_write") else None')
        ],
        "commit_deadline_removed": [
            ('        if self.clock.monotonic() >= deadline:\n            raise PlacementError("ECS operation deadline expired")\n', "")
        ],
        "service_refusal_not_unknown": [('return {"state": "unknown", "try_id": try_id}', 'return {"state": "fenced", "try_id": try_id}')],
        "stale_epoch_admitted": [
            ('("epoch", "state", "target", "frozen", "cutover_id")', '("state", "target", "frozen", "cutover_id")'),
            ('if execution["catalog_epoch"] != record["epoch"]:', "if False:"),
        ],
        "extra_override": [
            (
                "        return request\n",
                '        request["overrides"]["containerOverrides"][0]["environment"].append({"name": "HAI_EXTRA", "value": "synthetic"})\n'
                "        return request\n",
            )
        ],
        "mismatched_class": [
            ('target["job_network"]["security_group_ids"][CLASS_NETWORKS[job_class]]', 'target["job_network"]["security_group_ids"]["lakehouse"]')
        ],
    }
    for before, after in replacements[name]:
        expected_count = 2 if name in ("offline_reads_authority", "seal_reads_authority") else 1
        if source.count(before) != expected_count:
            raise ValueError("mutation anchor changed: " + name)
        source = source.replace(before, after, 1)
    return source


def main() -> int:
    """Save control, lint and named-case evidence for each required planted defect."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    before = {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in SOURCES}
    python = ROOT / ".venv/bin/python"
    ruff = ROOT / ".venv/bin/ruff"
    records: dict[str, Any] = {}
    names = {
        "writer_evidence_reused": (
            "orchestration/ecs_admission.py",
            ("writer_changed_qualification_real_refused", "writer_changed_layer_activation_real_refused", "writer_changed_qualification_aws_qualify_refused"),
        ),
        "non_write_authorization_allowed": (
            "orchestration/ecs_admission.py",
            ("offline_writer_authorization_refused", "lakehouse_read_writer_authorization_refused"),
        ),
        "stale_epoch_admitted": ("orchestration/ecs_admission.py", ("stale_epoch",)),
        "write_retried": ("orchestration/ecs_target.py", ("write_no_retry_fenced",)),
        "extra_override": ("orchestration/ecs_dispatch.py", ("canonical_request",)),
        "mismatched_class": ("orchestration/ecs_dispatch.py", ("canonical_request",)),
        "writer_template_reused": ("orchestration/ecs_admission.py", ("writer_changed_template_refused",)),
        "writer_params_reused": ("orchestration/ecs_admission.py", ("writer_changed_params_refused",)),
        "offline_reads_authority": ("orchestration/ecs_admission.py", ("offline_authority_absent", "offline_authority_malformed")),
        "seal_reads_authority": ("orchestration/ecs_admission.py", ("seal_authority_absent", "seal_authority_malformed")),
        "container_command_override": ("orchestration/ecs_dispatch.py", ("canonical_request",)),
        "task_role_override": ("orchestration/ecs_dispatch.py", ("canonical_request",)),
        "extra_request_field": ("orchestration/ecs_dispatch.py", ("canonical_request",)),
        "extra_environment_field": ("orchestration/ecs_dispatch.py", ("canonical_request",)),
        "authority_recheck_removed": ("orchestration/ecs_dispatch.py", ("authority_rechecked_inside_intent",)),
        "intent_deadline_removed": ("orchestration/ecs_dispatch.py", ("deadline_at_intent_enter",)),
        "commit_deadline_removed": ("orchestration/ecs_dispatch.py", ("deadline_at_intent_commit",)),
        "service_refusal_not_unknown": ("orchestration/ecs_dispatch.py", ("known_service_refusal_retains_unknown",)),
    }
    with tempfile.TemporaryDirectory(prefix="dispatch_defects_", dir=args.output.parent) as temporary:
        tree = Path(temporary) / "tree"
        tree.mkdir()
        ignore = shutil.ignore_patterns(
            "__pycache__", "*.pyc", ".env", ".env.*", "*.tfvars.json", "*terraform.tfstate*", "secrets", ".aws", "source_registry.json"
        )
        for folder in ("orchestration", "scripts", "services", "data_contracts", "config/orchestration"):
            shutil.copytree(ROOT / folder, tree / folder, ignore=ignore)
        for name in ("pyproject.toml", "docker-compose.yaml"):
            shutil.copy2(ROOT / name, tree / name)
        for name in ("control", *names):
            source_name, expected = names.get(name, ("", ()))
            original = (tree / source_name).read_text() if source_name else ""
            try:
                if source_name:
                    (tree / source_name).write_text(altered(name, original))
                    formatted = run_command(str(ruff), ["format", source_name], cwd=tree, timeout=30)
                    linted = run_command(str(ruff), ["check", source_name], cwd=tree, timeout=30)
                    if formatted.returncode or linted.returncode:
                        records[name] = {
                            "status": "blocked_before_load",
                            "format_exit": formatted.returncode,
                            "lint_exit": linted.returncode,
                            "diagnostics": linted.stdout + linted.stderr,
                        }
                        continue
                output = args.output.parent / (name + ".json")
                run = run_command(str(python), ["-B", "-m", "scripts.orchestration.ecs_dispatch_cases", "--output", str(output)], cwd=tree, timeout=60)
                (args.output.parent / (name + ".log")).write_text(run.stdout + run.stderr)
                observed = json.loads(output.read_text()) if output.exists() else {}
                records[name] = {
                    "exit": run.returncode,
                    "expected_cases": list(expected) if name != "control" else ["all cases pass"],
                    "failed": observed.get("failed"),
                    "exception": observed.get("exception"),
                    "caught": run.returncode == 0 and observed.get("failed") == []
                    if name == "control"
                    else run.returncode == 1 and set(expected) <= set(observed.get("failed", [])),
                    "format_exit": 0 if source_name else None,
                    "lint_exit": 0 if source_name else None,
                }
            finally:
                if source_name:
                    (tree / source_name).write_text(original)
    unchanged = before == {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in SOURCES}
    report = {
        "time": datetime.now(UTC).isoformat(),
        "source_sha256": before,
        "records": records,
        "candidate_unchanged": unchanged,
        "cleanup": "done",
        "limits": "Disposable copies, actual renderer commands and fake ECS and STS only. No live writer qualification or cloud acceptance.",
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
