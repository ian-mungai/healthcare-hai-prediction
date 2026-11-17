"""Exercise reviewed jobs inputs and wrapper dispatch with synthetic files and inert executables."""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import re
import shutil
import stat
import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]
SOURCES = (
    "scripts/__init__.py",
    "scripts/process.py",
    "scripts/infrastructure/__init__.py",
    "scripts/infrastructure/terraform.sh",
    "scripts/infrastructure/render_project_config.py",
    "scripts/infrastructure/capture_reader.py",
    "scripts/infrastructure/render_jobs_config.py",
    "scripts/infrastructure/run_jobs_preparation_e2e.py",
    "data_contracts/infrastructure/jobs_preparation.schema.json",
)


def _environment() -> dict[str, str]:
    return {key: value for key, value in os.environ.items() if not key.startswith(("AWS_", "TF_"))}


def _preparation(root: str = "jobs") -> dict[str, Any]:
    return {
        "schema_version": 1,
        "root": root,
        "variables": {"publisher_job_enabled": False} if root == "jobs" else {},
    }


# TEST FIXTURE ONLY, not measured values: the services output host_capacity_binding for a 30720 MiB usable host, the platform outputs and the recorded digests.
BINDING = "job_memory_mib=23552 usable_memory_mib=30720 system_reserve_mib=2048 service_reservations_mib=5120"
ADDRESS = "10.40.0.20"
TOPIC = "arn:aws:sns:us-west-2:111111111111:example_project_alerts_dev"
DIGESTS = {
    "analytics": "a",
    "quality": "b",
    "spark": "c",
    "training": "d",
    "feature_store": "e",
    "drift": "f",
    "openmetadata_ingestion": "1",
}


def _derived() -> dict[str, Any]:
    """The inputs the renderer builds from .env, the recorded digests and the stack outputs for the synthetic project, written out in full."""
    account, registry = "111111111111", "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_"

    def role(name: str) -> str:
        return f"arn:aws:iam::{account}:role/example_project_job_{name}_dev"

    def image(repository: str) -> str:
        return f"{registry}{repository}_dev@sha256:{DIGESTS[repository] * 64}"

    return {
        "alerts_topic_arn": TOPIC,
        "drift_images": {"drift": image("drift"), "feature_store": image("feature_store"), "training": image("training")},
        "drift_roles": {"drift_task": role("drift_task"), "execution": role("execution")},
        "host_capacity_binding": BINDING,
        "host_job_memory_mib": 23552,
        "host_private_ip": ADDRESS,
        "images": {"analytics": image("analytics"), "quality": image("quality"), "spark": image("spark")},
        "job_role_arns": {name: role(name) for name in ("execution", "publisher_task", "write_features_task", "write_gold_task", "write_silver_task")},
        "log_group_name": "example_project_jobs_dev",
        "openmetadata_images": {"ingestion": image("openmetadata_ingestion")},
        "openmetadata_roles": {"execution": role("execution"), "openmetadata_task": role("openmetadata_task")},
        "serve_roles": {"serve_task": role("serve_task")},
        "train_images": {"feature_store": image("feature_store"), "training": image("training")},
        "train_roles": {"execution": role("execution"), "train_task": role("train_task")},
        "writer_parameter_names": {
            f"write_{layer}_task": [f"/example_project/aws_polaris_{layer}_client_{part}" for part in ("id", "secret")]
            for layer in ("features", "gold", "silver")
        },
    }


def _stand_in(
    tool: Path,
    calls: Path,
    platform_directory: str,
    binding: tuple[str, int] = (json.dumps(BINDING), 0),
    address: tuple[str, int] = (json.dumps(ADDRESS), 0),
    topic: tuple[str, int] = (json.dumps(TOPIC), 0),
) -> None:
    """Write an inert terraform: it logs its arguments, prints plan.json for ``show`` and otherwise answers the stack output the renderer asks for."""
    tool.write_text(
        f"#!{sys.executable}\nimport json, sys\nfrom pathlib import Path\n"
        f"open({str(calls)!r}, 'a').write(json.dumps(sys.argv[1:]) + '\\n')\n"
        "if 'show' in sys.argv[1:]:\n"
        "    sys.stdout.write((Path(__file__).parent / 'plan.json').read_text())\n    sys.exit(0)\n"
        f"if sys.argv[1] != {'-chdir=' + platform_directory!r}:\n"
        f"    sys.stdout.write({binding[0]!r})\n    sys.exit({binding[1]})\n"
        "if sys.argv[-1] == 'alerts_topic_arn':\n"
        f"    sys.stdout.write({topic[0]!r})\n    sys.exit({topic[1]})\n"
        f"sys.stdout.write({address[0]!r})\nsys.exit({address[1]})\n"
    )
    tool.chmod(0o700)


def _renderer_cases(folder: Path) -> dict[str, bool]:
    workspace = folder / "renderer"
    for name in SOURCES:
        source = ROOT / name
        if source.exists():
            target = workspace / name
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, target)
    environment_file = workspace / "synthetic_environment.txt"
    environment_file.write_text(
        "AWS_REGION=us-west-2\nAWS_PROFILE=example_project_dev\nAWS_ACCOUNT_ID=111111111111\n"
        "PROJECT_NAME=example_project\nENVIRONMENT=dev\nS3_BUCKET=example-project-data-dev\n"
    )
    prep = workspace / "reviewed_preparation.json"
    record = workspace / "infra/services/images.auto.tfvars.json"
    record.parent.mkdir(parents=True)
    record.write_text(json.dumps({"image_digests": {name: "sha256:" + character * 64 for name, character in DIGESTS.items()}}))
    binary = workspace / "bin"
    binary.mkdir()
    (binary / "plan.json").write_text("{}")
    _stand_in(binary / "terraform", binary / "calls.jsonl", str((workspace / "infra/platform").resolve()))
    stand_in_environment = _environment() | {"PATH": str(binary) + os.pathsep + os.environ.get("PATH", "")}
    values = {
        "aws_region": "us-west-2",
        "aws_profile": "example_project_dev",
        "expected_account_id": "111111111111",
        "project_name": "example_project",
        "environment": "dev",
        "data_bucket_name": "example-project-data-dev",
    }
    output = workspace / "infra/jobs/deployment.auto.tfvars.json"

    def invoke(document: dict[str, Any], *extra: str, digest: str | None = None) -> Any:
        prep.write_text(json.dumps(document, sort_keys=True) + "\n")
        sha256 = hashlib.sha256(prep.read_bytes()).hexdigest() if digest is None else digest
        return run_command(
            sys.executable,
            [
                "-m",
                "scripts.infrastructure.render_jobs_config",
                "--env-file",
                str(environment_file),
                "--preparation-inputs",
                str(prep),
                "--preparation-sha256",
                sha256,
                *extra,
            ],
            cwd=workspace,
            env=stand_in_environment,
            timeout=15,
        )

    document = _preparation()
    result = invoke(document)
    values = values | _derived()
    expected = values | document["variables"]
    checks = {"reviewed_preparation_is_rendered": result.returncode == 0 and output.exists() and json.loads(output.read_text()) == expected}
    if not checks["reviewed_preparation_is_rendered"]:
        (folder / "renderer_failure.txt").write_text(result.stdout + result.stderr)
        return checks
    original = output.read_bytes()
    checks["reviewed_preparation_is_private"] = stat.S_IMODE(output.stat().st_mode) == 0o600
    checks["reviewed_preparation_repeats"] = invoke(document).returncode == 0 and output.read_bytes() == original
    checks["reviewed_preparation_check"] = invoke(document, "--check").returncode == 0
    layers = ("silver", "gold", "features", "bronze")
    writer_documents = {}
    for selected in (*(tuple([layer]) for layer in layers), layers):
        label = "all" if len(selected) > 1 else selected[0]
        writer_document = document | {
            "variables": document["variables"]
            | {
                "job_role_arns": {"execution": "arn:aws:iam::111111111111:role/example_project_job_execution_dev"}
                | {"write_" + layer + "_task": "arn:aws:iam::111111111111:role/example_project_job_write_" + layer + "_task_dev" for layer in selected},
                "writer_parameter_names": {
                    "write_" + layer + "_task": ["/example_project/aws_polaris_" + layer + "_client_" + suffix for suffix in ("id", "secret")]
                    for layer in selected
                },
                "images": {"spark": "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_spark_dev@sha256:" + "c" * 64},
            }
        }
        result = invoke(writer_document)
        rendered = values | writer_document["variables"]
        checks[label + "_writer_inputs_render"] = result.returncode == 0 and json.loads(output.read_text()) == rendered
        writer_documents[label] = writer_document
    for index, layer in enumerate(layers):
        other = layers[(index + 1) % len(layers)]
        for field, replacement in (
            ("job_role_arns", {"write_" + layer + "_task": "arn:aws:iam::111111111111:role/example_project_job_write_" + other + "_task_dev"}),
            (
                "writer_parameter_names",
                {"write_" + layer + "_task": ["/example_project/aws_polaris_" + other + "_client_" + suffix for suffix in ("id", "secret")]},
            ),
        ):
            altered = copy.deepcopy(writer_documents[layer])
            altered["variables"][field] = replacement
            before = output.read_bytes()
            result = invoke(altered)
            checks[layer + "_writer_refuses_swapped_" + field] = (
                result.returncode == 2 and "preparation contract" in result.stderr and output.read_bytes() == before
            )
    execution = "arn:aws:iam::111111111111:role/example_project_job_execution_dev"
    publisher = document | {
        "variables": document["variables"]
        | {
            "publisher_job_enabled": True,
            "job_role_arns": {"execution": execution, "publisher_task": "arn:aws:iam::111111111111:role/example_project_job_publisher_task_dev"},
        }
    }
    checks["publisher_inputs_render_in_the_host_room_cases"] = True  # the publisher takes the job room of the services stack, so _host_room_cases renders it
    for label, arn in (
        ("another_role", "arn:aws:iam::111111111111:role/example_project_job_write_silver_task_dev"),
        ("wildcard_name", "arn:aws:iam::111111111111:role/example_project_job_publisher_task_*"),
        ("wildcard_role", "arn:aws:iam::111111111111:role/*"),
        ("other_environment", "arn:aws:iam::111111111111:role/example_project_job_publisher_task_test"),
    ):
        altered = copy.deepcopy(publisher)
        altered["variables"]["job_role_arns"]["publisher_task"] = arn
        before = output.read_bytes()
        result = invoke(altered)
        checks["publisher_refuses_" + label] = result.returncode == 2 and "preparation contract" in result.stderr and output.read_bytes() == before
    checks["reader_inputs_reset_after_writer_cases"] = invoke(document).returncode == 0 and output.read_bytes() == original
    unused_writer = document | {
        "variables": document["variables"] | {"job_role_arns": {"try_writer": "arn:aws:iam::111111111111:role/example_project_job_try_writer_dev"}}
    }
    result = invoke(unused_writer)
    checks["unused_writer_input_is_refused"] = result.returncode == 2 and "preparation contract" in result.stderr and output.read_bytes() == original
    checks["reader_inputs_reset_after_role_probe"] = invoke(document).returncode == 0 and output.read_bytes() == original
    account = "111111111111"
    registry = account + ".dkr.ecr.us-west-2.amazonaws.com/example_project_"
    training_variables: dict[str, Any] = {
        "train_enabled": True,
        "train_images": {"training": registry + "training_dev@sha256:" + "d" * 64, "feature_store": registry + "feature_store_dev@sha256:" + "e" * 64},
        "train_roles": {
            "execution": "arn:aws:iam::" + account + ":role/example_project_job_execution_dev",
            "train_task": "arn:aws:iam::" + account + ":role/example_project_job_train_task_dev",
        },
        "mlflow_admin_password_name": "/example_project/mlflow_admin_password",
        "log_group_name": "example_project_jobs_dev",
    }
    training_document = document | {"variables": document["variables"] | training_variables}
    result = invoke(training_document)
    checks["training_inputs_render"] = result.returncode == 0 and json.loads(output.read_text()) == values | training_document["variables"]
    for name, change in (
        (
            "training_image_of_another_repository",
            {"train_images": training_variables["train_images"] | {"analytics": registry + "analytics_dev@sha256:" + "a" * 64}},
        ),
        ("training_image_by_tag", {"train_images": training_variables["train_images"] | {"training": registry + "training_dev:latest"}}),
        (
            "training_role_of_another_layer",
            {"train_roles": training_variables["train_roles"] | {"train_task": "arn:aws:iam::" + account + ":role/example_project_job_write_gold_task_dev"}},
        ),
        (
            "training_role_key_of_another_layer",
            {"train_roles": training_variables["train_roles"] | {"write_gold_task": training_variables["train_roles"]["train_task"]}},
        ),
        ("training_password_name_of_another_secret", {"mlflow_admin_password_name": "/example_project/aws_polaris_reader_client_secret"}),
    ):
        altered = training_document | {"variables": training_document["variables"] | change}
        before = output.read_bytes()
        result = invoke(altered)
        checks[name + "_is_refused"] = result.returncode == 2 and "preparation contract" in result.stderr and output.read_bytes() == before
    checks["training_inputs_reset_restores_bytes"] = invoke(document).returncode == 0 and output.read_bytes() == original
    for name, altered, digest, cause in (
        ("changed_reviewed_bytes", document, "0" * 64, "preparation SHA-256"),
        ("base_identity_override", document | {"variables": document["variables"] | {"aws_profile": "example_admin"}}, None, "preparation contract"),
        ("unknown_preparation_input", document | {"variables": document["variables"] | {"unreviewed_secret": "synthetic"}}, None, "preparation contract"),
        ("wrong_preparation_root", document | {"root": "services"}, None, "preparation contract"),
    ):
        result = invoke(altered, digest=digest)
        checks[name] = result.returncode == 2 and cause in result.stderr and output.read_bytes() == original
    prep.write_text('{"schema_version":1,"schema_version":1,"root":"jobs","variables":{}}')
    result = run_command(
        sys.executable,
        [
            "-m",
            "scripts.infrastructure.render_jobs_config",
            "--env-file",
            str(environment_file),
            "--preparation-inputs",
            str(prep),
            "--preparation-sha256",
            hashlib.sha256(prep.read_bytes()).hexdigest(),
        ],
        cwd=workspace,
        env=stand_in_environment,
        timeout=15,
    )
    checks["duplicate_preparation_field"] = result.returncode == 2 and "duplicate JSON field" in result.stderr and output.read_bytes() == original
    env = stand_in_environment
    declared: dict[str, Any] = {name: {} for name in expected} | {"optional_setting": {"default": False}}
    plan: dict[str, Any] = {
        "complete": True,
        "errored": False,
        "variables": {key: {"value": value} for key, value in (expected | {"optional_setting": False}).items()},
        "configuration": {"root_module": {"variables": declared}},
    }
    prep.write_text(json.dumps(document, sort_keys=True) + "\n")
    args = [
        "-m",
        "scripts.infrastructure.render_jobs_config",
        "--env-file",
        str(environment_file),
        "--preparation-inputs",
        str(prep),
        "--preparation-sha256",
        hashlib.sha256(prep.read_bytes()).hexdigest(),
        "--check-plan",
        str(workspace / "reviewed.tfplan"),
    ]
    for name, altered, accepted in (
        ("reviewed_preparation_plan", plan, True),
        ("changed_preparation_plan", copy.deepcopy(plan), False),
        ("unreviewed_default_plan", copy.deepcopy(plan), False),
    ):
        if name == "changed_preparation_plan":
            altered["variables"]["publisher_job_enabled"]["value"] = True
        if name == "unreviewed_default_plan":
            altered["variables"]["optional_setting"]["value"] = True
        (binary / "plan.json").write_text(json.dumps(altered))
        result = run_command(sys.executable, args, cwd=workspace, env=env, timeout=15)
        checks[name] = result.returncode == (0 if accepted else 2) and (accepted or "saved jobs plan" in result.stderr)
    bronze = writer_documents["bronze"]
    raw_read = bronze | {"variables": bronze["variables"] | {"bronze_read_environment": {"HAI_READ_RAW_ROOT": "true"}}}
    enabled = invoke(raw_read)
    rendered = json.loads(output.read_text()) if output.exists() else {}
    checks["raw_read_environment_is_rendered"] = enabled.returncode == 0 and rendered.get("bronze_read_environment") == {"HAI_READ_RAW_ROOT": "true"}
    for label, wrong in (
        ("another_name", {"HAI_OTHER": "true"}),
        ("another_value", {"HAI_READ_RAW_ROOT": "yes"}),
        ("an_extra_name", {"HAI_READ_RAW_ROOT": "true", "HAI_OTHER": "1"}),
    ):
        refused = invoke(bronze | {"variables": bronze["variables"] | {"bronze_read_environment": wrong}})
        checks["raw_read_environment_" + label + "_is_refused"] = refused.returncode == 2
    off = invoke(bronze)
    checks["raw_read_environment_off_renders_without_the_field"] = off.returncode == 0 and "bronze_read_environment" not in json.loads(output.read_text())
    checks.update(_host_room_cases(workspace, environment_file, prep, output, document, values, publisher["variables"]))
    checks.update(_host_address_cases(workspace, environment_file, prep, output, document, values, publisher["variables"]))
    return checks


def _host_room_cases(
    workspace: Path, environment_file: Path, prep: Path, output: Path, document: dict[str, Any], values: dict[str, Any], publisher_variables: dict[str, Any]
) -> dict[str, bool]:
    """Every render gets the job room that the services stack outputs; the renderer refuses a missing or unusable figure and writes nothing."""
    binary = workspace / "host_room_bin"
    binary.mkdir()
    tool = binary / "terraform"
    calls = binary / "calls.jsonl"
    platform_directory = str((workspace / "infra/platform").resolve())

    def answer(text: str, code: int) -> None:
        _stand_in(tool, calls, platform_directory, binding=(text, code))

    env = _environment() | {"PATH": str(binary) + os.pathsep + os.environ.get("PATH", "")}

    def stack(name: str) -> str:
        """The folder of one Terraform stack in the fixture workspace: the directory the renderer hands Terraform as -chdir (no repository file is read)."""
        return str((workspace / "infra" / name).resolve())

    def invoke(variables: dict[str, Any]) -> Any:
        prep.write_text(json.dumps(document | {"variables": variables}, sort_keys=True) + "\n")
        return run_command(
            sys.executable,
            [
                "-m",
                "scripts.infrastructure.render_jobs_config",
                "--env-file",
                str(environment_file),
                "--preparation-inputs",
                str(prep),
                "--preparation-sha256",
                hashlib.sha256(prep.read_bytes()).hexdigest(),
            ],
            cwd=workspace,
            env=env,
            timeout=15,
        )

    def asked() -> list[list[str]]:
        return [json.loads(line) for line in calls.read_text().splitlines()] if calls.exists() else []

    room_ask = ["-chdir=" + stack("services"), "output", "-json", "host_capacity_binding"]
    host_variables = document["variables"] | {"host_jobs_enabled": True, "host_jobs": ["silver"]}
    checks: dict[str, bool] = {}
    answer(json.dumps(BINDING), 0)
    result = invoke(host_variables)
    checks["host_jobs_get_the_services_job_room_and_its_binding"] = (
        result.returncode == 0 and json.loads(output.read_text()) == values | host_variables and asked().count(room_ask) == 1
    )
    lowered = BINDING.replace("23552", "22528")
    answer(json.dumps(lowered), 0)
    result = invoke(host_variables)
    checks["host_jobs_get_a_cap_below_the_room"] = result.returncode == 0 and json.loads(output.read_text()) == values | host_variables | {
        "host_job_memory_mib": 22528,
        "host_capacity_binding": lowered,
    }
    sets = [f"features_m{number}" for number in range(1, 7)]
    answer(json.dumps(BINDING), 0)
    result = invoke(host_variables | {"host_jobs": ["features", "gold", "silver", *sets]})
    checks["host_jobs_accept_the_six_model_set_families"] = result.returncode == 0 and json.loads(output.read_text())["host_jobs"] == [
        "features",
        "gold",
        "silver",
        *sets,
    ]
    answer(json.dumps(BINDING), 0)
    result = invoke(host_variables | {"host_jobs": ["features", "features_m7"]})
    checks["host_jobs_refuse_a_seventh_model_set"] = result.returncode != 0
    answer(json.dumps(BINDING), 0)
    result = invoke(host_variables)
    rendered = output.read_bytes()
    for name, text, code in (
        ("a_failed_services_output", "", 1),
        ("a_binding_that_is_not_text", "23552", 0),
        ("a_binding_in_another_form", json.dumps("job_memory_mib=23552"), 0),
        ("a_binding_above_the_room", json.dumps(BINDING.replace("23552", "24576")), 0),
        ("a_job_room_below_one_job", json.dumps("job_memory_mib=2048 usable_memory_mib=9216 system_reserve_mib=2048 service_reservations_mib=5120"), 0),
        ("a_null_binding", "null", 0),
    ):
        answer(text, code)
        result = invoke(host_variables)
        checks["host_room_refuses_" + name] = result.returncode == 2 and "job room" in result.stderr and output.read_bytes() == rendered
    # host_usable_memory_mib defaults to the current host size (owner decision, Nov 10 2026), so the refusal asks for a services apply, never for the input.
    checks["host_room_refusal_asks_for_a_services_apply_not_the_usable_memory_input"] = (
        "apply the services stack" in result.stderr.lower() and "host_usable_memory_mib" not in result.stderr
    )
    # The publisher is a host job: its revision carries the binding and its task memory is the job room.
    calls.unlink(missing_ok=True)
    answer(json.dumps(BINDING), 0)
    result = invoke(publisher_variables)
    checks["publisher_gets_the_services_job_room_and_its_binding"] = (
        result.returncode == 0 and json.loads(output.read_text()) == values | publisher_variables and asked().count(room_ask) == 1
    )
    rendered = output.read_bytes()
    for name, text, code in (("a_failed_services_output", "", 1), ("a_null_binding", "null", 0)):
        answer(text, code)
        result = invoke(publisher_variables)
        checks["publisher_room_refuses_" + name] = result.returncode == 2 and "job room" in result.stderr and output.read_bytes() == rendered
    # The committed defaults turn host jobs and the publisher on, so a render reads the room even when an override turns both off.
    calls.unlink(missing_ok=True)
    answer(json.dumps(BINDING), 0)
    result = invoke(document["variables"] | {"host_jobs_enabled": False, "publisher_job_enabled": False})
    checks["the_job_room_is_read_for_every_render"] = (
        result.returncode == 0 and asked().count(room_ask) == 1 and json.loads(output.read_text())["host_capacity_binding"] == BINDING
    )
    return checks


def _hand_written_host_addresses(folder: Path = ROOT / "infra/jobs") -> list[str]:
    """Return every IPv4 literal in the jobs stack files of a folder, so none of the host's addresses is typed by hand."""
    found: list[str] = []
    for source in sorted(folder.rglob("*")):
        if source.suffix not in (".tf", ".py") or ".terraform" in source.parts or "tests" in source.parts:
            continue
        text = source.read_text(encoding="utf-8")
        found += [f"{source.name}:{literal}" for literal in re.findall(r"\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b(?!/)", text)]
    return found


def _scan_flags_every_stack_file(folder: Path) -> bool:
    """Plant an address expression in each top-level jobs stack file of a copy, one file at a time; the scan must name exactly that file."""
    folder.mkdir()
    sources = sorted((ROOT / "infra/jobs").glob("*.tf"))
    for source in sources:
        shutil.copyfile(source, folder / source.name)
    flagged = bool(sources)
    for source in sources:
        original = (folder / source.name).read_text(encoding="utf-8")
        (folder / source.name).write_text(original + '\nlocals {\n  planted_host_address = "http://10.40.0.88:8585/api"\n}\n', encoding="utf-8")
        flagged = flagged and _hand_written_host_addresses(folder) == [f"{source.name}:10.40.0.88"]
        (folder / source.name).write_text(original, encoding="utf-8")
    return flagged and _hand_written_host_addresses(folder) == []


def _host_address_cases(
    workspace: Path, environment_file: Path, prep: Path, output: Path, document: dict[str, Any], values: dict[str, Any], publisher_variables: dict[str, Any]
) -> dict[str, bool]:
    """Every render gets the host address and the alerts topic from the platform outputs; the override file cannot type either."""
    binary = workspace / "host_address_bin"
    binary.mkdir()
    tool = binary / "terraform"
    calls = binary / "calls.jsonl"
    platform_directory = str((workspace / "infra/platform").resolve())

    def answer(address_text: str, address_code: int, topic_text: str | None = None, topic_code: int = 0) -> None:
        _stand_in(
            tool, calls, platform_directory, address=(address_text, address_code), topic=(json.dumps(TOPIC) if topic_text is None else topic_text, topic_code)
        )

    env = _environment() | {"PATH": str(binary) + os.pathsep + os.environ.get("PATH", "")}

    def invoke(variables: dict[str, Any]) -> Any:
        prep.write_text(json.dumps(document | {"variables": variables}, sort_keys=True) + "\n")
        return run_command(
            sys.executable,
            [
                "-m",
                "scripts.infrastructure.render_jobs_config",
                "--env-file",
                str(environment_file),
                "--preparation-inputs",
                str(prep),
                "--preparation-sha256",
                hashlib.sha256(prep.read_bytes()).hexdigest(),
            ],
            cwd=workspace,
            env=env,
            timeout=15,
        )

    def platform_asks() -> list[list[str]]:
        asked = [json.loads(line) for line in calls.read_text().splitlines()] if calls.exists() else []
        return [call for call in asked if call[0] == "-chdir=" + platform_directory]

    ask = ["-chdir=" + platform_directory, "output", "-json", "host_private_ip"]
    topic_ask = ["-chdir=" + platform_directory, "output", "-json", "alerts_topic_arn"]
    metadata = document["variables"] | {"host_jobs_enabled": True, "host_jobs": ["metadata", "silver"]}
    checks: dict[str, bool] = {}
    answer(json.dumps("10.40.0.20"), 0)
    for name, variables in (
        ("an_override_free_render", document["variables"]),
        ("the_metadata_host_job", metadata),
        ("host_jobs_turned_off", document["variables"] | {"host_jobs_enabled": False}),
        ("the_publisher_job_alone", publisher_variables),
    ):
        calls.unlink(missing_ok=True)
        result = invoke(variables)
        rendered_inputs = json.loads(output.read_text()) if result.returncode == 0 else {}
        checks[f"{name}_gets_host_private_ip_and_the_alerts_topic_from_the_platform_outputs"] = (
            result.returncode == 0
            and platform_asks() == [ask, topic_ask]
            and rendered_inputs.get("host_private_ip") == "10.40.0.20"
            and rendered_inputs.get("alerts_topic_arn") == TOPIC
            and "10.40.0.20" not in result.stdout + result.stderr
            and TOPIC not in result.stdout + result.stderr
        )
    rendered = output.read_bytes()
    for name, text, code in (
        ("a_failed_platform_output", "", 1),
        ("a_null_topic", "null", 0),
        ("a_topic_that_is_not_text", "20", 0),
        ("an_empty_topic", json.dumps(""), 0),
        ("a_topic_with_a_wildcard", json.dumps("arn:aws:sns:us-west-2:111111111111:*"), 0),
        ("a_topic_from_another_region", json.dumps("arn:aws:sns:eu-west-1:111111111111:example_project_alerts_dev"), 0),
        ("a_topic_from_another_account", json.dumps(f"arn:aws:sns:us-west-2:{'2' * 12}:example_project_alerts_dev"), 0),
        ("a_topic_of_another_project", json.dumps("arn:aws:sns:us-west-2:111111111111:other_project_alerts_dev"), 0),
        ("a_topic_of_another_environment", json.dumps("arn:aws:sns:us-west-2:111111111111:example_project_alerts_prod"), 0),
        ("the_budget_topic", json.dumps("arn:aws:sns:us-west-2:111111111111:example_project_budget_alerts_dev"), 0),
        ("a_subscription_arn", json.dumps(TOPIC + ":01234567-89ab-cdef-0123-456789abcdef"), 0),
    ):
        answer(json.dumps("10.40.0.20"), 0, text, code)
        for label, variables in (("an_override_free_render", document["variables"]), ("the_metadata_host_job", metadata)):
            result = invoke(variables)
            checks[f"alerts_topic_refuses_{name}_for_{label}"] = (
                result.returncode == 2 and "alerts_topic_arn" in result.stderr and output.read_bytes() == rendered
            )
    checks["alerts_topic_refusal_asks_for_a_platform_apply"] = "apply the platform stack" in result.stderr.lower()
    answer(json.dumps("10.40.0.20"), 0)
    for name, text, code in (
        ("a_failed_platform_output", "", 1),
        ("a_null_address", "null", 0),
        ("an_address_that_is_not_text", "20", 0),
        ("a_public_address", json.dumps("203.0.113.20"), 0),
        ("a_loopback_address", json.dumps("127.0.0.1"), 0),
        ("a_link_local_address", json.dumps("169.254.169.254"), 0),
        ("an_address_with_a_leading_zero", json.dumps("10.0.01.20"), 0),
        ("a_host_name", json.dumps("host.example.invalid"), 0),
        ("an_address_with_a_port", json.dumps("10.40.0.20:8585"), 0),
    ):
        answer(text, code)
        for label, variables in (("an_override_free_render", document["variables"]), ("the_metadata_host_job", metadata)):
            result = invoke(variables)
            checks[f"host_address_refuses_{name}_for_{label}"] = (
                result.returncode == 2 and "host_private_ip" in result.stderr and output.read_bytes() == rendered
            )
    checks["host_address_refusal_asks_for_a_platform_apply"] = "apply the platform stack" in result.stderr.lower()
    answer(json.dumps("10.40.0.20"), 0)
    # The committed host_jobs default is the live family list, so an override that turns host jobs on needs no list of its own.
    result = invoke(document["variables"] | {"host_jobs_enabled": True})
    checks["host_jobs_switched_on_without_a_list_renders_and_keeps_the_committed_families"] = (
        result.returncode == 0 and json.loads(output.read_text()) == values | {"host_jobs_enabled": True} | document["variables"]
    )
    rendered = output.read_bytes()
    for name, variables in (
        ("a_host_family_nobody_registered", metadata | {"host_jobs": ["fargate"]}),
        ("a_repeated_host_family", metadata | {"host_jobs": ["silver", "silver"]}),
        ("a_host_jobs_switch_that_is_not_a_boolean", document["variables"] | {"host_jobs_enabled": "true", "host_jobs": ["metadata"]}),
    ):
        calls.unlink(missing_ok=True)
        result = invoke(variables)
        checks[f"{name}_is_refused_before_any_terraform_call"] = result.returncode == 2 and not calls.exists() and output.read_bytes() == rendered
    # The schema refuses every key it does not list, so a made-up key proves the refusal without naming any removed input.
    for label, key, typed in (
        ("host_private_ip", "host_private_ip", "10.40.0.20"),
        ("alerts_topic_arn", "alerts_topic_arn", TOPIC),
        ("any_unknown_key", "zz_unregistered_probe_key", "any value"),
    ):
        result = invoke(metadata | {key: typed})
        checks[f"the_overrides_file_may_not_type_{label}"] = (
            result.returncode == 2 and "preparation contract is invalid" in result.stderr and output.read_bytes() == rendered
        )
    checks["the_jobs_stack_files_hold_no_hand_written_host_address"] = _hand_written_host_addresses() == []
    checks["the_address_scan_flags_a_planted_address_in_every_jobs_stack_file"] = _scan_flags_every_stack_file(workspace / "address_scan")
    return checks


def _wrapper_result(workspace: Path, environment: dict[str, str], arguments: list[str]) -> tuple[Any, list[dict[str, Any]]]:
    calls_file = workspace / "calls.json"
    calls_file.write_text("")
    result = run_command("bash", arguments, cwd=workspace, env=environment, timeout=15)
    return result, [json.loads(line) for line in calls_file.read_text().splitlines()]


def _wrapper_cases(folder: Path) -> dict[str, bool]:
    workspace = (folder / "wrapper").resolve()
    script = workspace / "scripts/infrastructure/terraform.sh"
    script.parent.mkdir(parents=True)
    shutil.copy2(ROOT / "scripts/infrastructure/terraform.sh", script)
    binary = workspace / "bin"
    binary.mkdir()
    for name in ("python_capture", "terraform"):
        executable = binary / name
        executable.write_text(
            f"#!{sys.executable}\nimport json, os, sys\nfrom pathlib import Path\n"
            "with (Path(__file__).resolve().parents[1] / 'calls.json').open('a') as handle:\n"
            "    handle.write(json.dumps({'program': Path(__file__).name, 'args': sys.argv[1:]}) + '\\n')\n"
            "sys.exit(int(os.environ.get('SYNTHETIC_RENDER_EXIT', '0')) if Path(__file__).name == 'python_capture' else 0)\n"
        )
        executable.chmod(0o700)
    env = _environment() | {"PATH": str(binary) + os.pathsep + os.environ.get("PATH", ""), "PYTHON_BIN": str(binary / "python_capture")}
    sha256 = "a" * 64
    reviewed = ["--preparation-inputs", "reviewed_inputs.json", "--preparation-sha256", sha256]

    checks = {}
    cases = (
        (
            "jobs",
            "plan",
            "jobs",
            _wrapper_result(
                workspace,
                env,
                [
                    "scripts/infrastructure/terraform.sh",
                    "--jobs",
                    "--preparation-inputs",
                    "reviewed_inputs.json",
                    "--preparation-sha256",
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "plan",
                ],
            ),
        ),
        (
            "jobs",
            "apply",
            "jobs",
            _wrapper_result(
                workspace,
                env,
                [
                    "scripts/infrastructure/terraform.sh",
                    "--jobs",
                    "--preparation-inputs",
                    "reviewed_inputs.json",
                    "--preparation-sha256",
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "apply",
                    "reviewed.tfplan",
                ],
            ),
        ),
        (
            "jobs",
            "validate",
            "jobs",
            _wrapper_result(
                workspace,
                env,
                [
                    "scripts/infrastructure/terraform.sh",
                    "--jobs",
                    "--preparation-inputs",
                    "reviewed_inputs.json",
                    "--preparation-sha256",
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "validate",
                ],
            ),
        ),
    )
    (folder / "wrapper_observed.json").write_text(
        json.dumps(
            [
                {"case": name + "_" + action, "exit_code": result.returncode, "stderr": result.stderr, "calls": calls}
                for name, action, directory, (result, calls) in cases
            ],
            indent=2,
        )
    )
    for name, action, directory, (result, calls) in cases:
        renderer_args = ["-m", "scripts.infrastructure.render_jobs_config"]
        renderer_args += reviewed
        if action == "apply":
            renderer_args += ["--check-plan", f"{workspace}/infra/{directory}/reviewed.tfplan"]
        if action != "validate":
            renderer_args += ["--verify-identity"]
        checks[f"{name}_{action}_passes_reviewed_inputs"] = (
            result.returncode == 0
            and len(calls) == 2
            and calls[0]["args"] == renderer_args
            and calls[1]["args"][0] == f"-chdir={workspace}/infra/{directory}"
            and calls[1]["args"][1] == action
        )
    refusals = (
        (
            "preparation_requires_digest",
            "Supply both",
            _wrapper_result(workspace, env | {}, ["scripts/infrastructure/terraform.sh", "--jobs", "--preparation-inputs", "x.json", "plan"]),
        ),
        (
            "digest_requires_preparation",
            "Supply both",
            _wrapper_result(
                workspace,
                env | {},
                [
                    "scripts/infrastructure/terraform.sh",
                    "--jobs",
                    "--preparation-sha256",
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "plan",
                ],
            ),
        ),
        (
            "preparation_is_not_for_the_iam_stack",
            "only with --jobs",
            _wrapper_result(
                workspace,
                env | {},
                [
                    "scripts/infrastructure/terraform.sh",
                    "--iam",
                    "--admin-profile",
                    "example_admin",
                    "--preparation-inputs",
                    "reviewed_inputs.json",
                    "--preparation-sha256",
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "plan",
                ],
            ),
        ),
        (
            "direct_variables_stay_refused",
            "Set project inputs only",
            _wrapper_result(workspace, env | {}, ["scripts/infrastructure/terraform.sh", "--jobs", "plan", "-var=layer_jobs_enabled=true"]),
        ),
        (
            "platform_environment_variables_are_refused",
            "Unset TF_VAR_project_name",
            _wrapper_result(workspace, env | {"TF_VAR_project_name": "other_project"}, ["scripts/infrastructure/terraform.sh", "--platform", "plan"]),
        ),
        (
            "environment_variables_are_refused",
            "Unset TF_VAR_layer_jobs_enabled",
            _wrapper_result(workspace, env | {"TF_VAR_layer_jobs_enabled": "true"}, ["scripts/infrastructure/terraform.sh", "--jobs", "plan"]),
        ),
    )
    for name, cause, (result, calls) in refusals:
        checks[name] = result.returncode == 2 and cause in result.stderr and not calls
    result, calls = _wrapper_result(workspace, env | {"SYNTHETIC_RENDER_EXIT": "9"}, ["scripts/infrastructure/terraform.sh", "--jobs", "plan"])
    checks["renderer_failure_stops_dispatch"] = result.returncode == 9 and len(calls) == 1
    return checks


def main() -> int:
    """Run real commands against synthetic inputs and save their bounded verification result."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    before = {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in SOURCES if (ROOT / name).exists()}
    args.output.mkdir(parents=True, exist_ok=False)
    with tempfile.TemporaryDirectory(prefix="jobs_preparation_") as temporary:
        folder = Path(temporary)
        checks = _wrapper_cases(folder) | _renderer_cases(folder)
        for name in ("renderer_failure.txt", "wrapper_observed.json"):
            record = folder / name
            if record.exists():
                shutil.copy2(record, args.output / name)
    checks["production_sources_unchanged"] = all(hashlib.sha256((ROOT / name).read_bytes()).hexdigest() == digest for name, digest in before.items())
    report = {
        "timestamp_utc": datetime.now(UTC).isoformat(),
        "status": "passed" if all(checks.values()) else "failed",
        "checks": checks,
        "source_sha256": before,
        "limits": ["Synthetic command boundary only. No live Terraform, AWS, deployment inputs or data."],
        "cleanup": "done",
    }
    (args.output / "report.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    sys.stdout.write(f"Jobs preparation checks: {sum(checks.values())}/{len(checks)} passed.\n")
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    sys.exit(main())
