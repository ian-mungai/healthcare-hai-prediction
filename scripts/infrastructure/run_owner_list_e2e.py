"""E2E of the owner's first layer run in config/aws/owner_command_lists.json: the order of the steps, the flags each step names and every printed command
against its parser.

    .venv/bin/python -m scripts.infrastructure.run_owner_list_e2e --out <new folder>

The commands come from the "Silver first run", "Gold after silver" and "Features after gold" lists of that file (the text moved out of the tracked
documents) and from the hand-started model lists "Training after features", "Promotion after training", "Serving after promotion" and "Drift after
serving". Each runs through the parser of its real program with synthetic values (``scripts.infrastructure.owner_commands``), so a typo, a missing
argument or a wrong option fails here and not on the owner's first run. The reviewed jobs file that each list describes passes the real renderer
through the real wrapper. The training, promotion and serving parameters that the model lists print pass their real contracts. Nothing calls AWS or
Docker."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import shlex
import sys
import tempfile
from pathlib import Path
from typing import Any

from orchestration import serving_contract, training_contract
from scripts.infrastructure import owner_commands
from scripts.infrastructure.run_activation_inputs_e2e import (
    ADMIN,
    PLATFORM_DEFAULT_LOG_GROUPS,
    PLATFORM_DEFAULT_REPOSITORIES,
    ROOT,
    ROWS,
    STACKS,
    Workspace,
    variables_of,
)
from scripts.process import run_command

OWNER_LISTS = ROOT / "config/aws/owner_command_lists.json"
SILVER, GOLD, FEATURES = "Silver first run", "Gold after silver", "Features after gold"
TRAIN, PROMOTE, SERVE, DRIFT = "Training after features", "Promotion after training", "Serving after promotion", "Drift after serving"
HEADINGS = (SILVER, GOLD, FEATURES, TRAIN, PROMOTE, SERVE, DRIFT)
IAM_FLAGS = {"job_writer_layers_prepared": True, "job_layer_runs_prepared": True, "job_endpoints_enabled": True, "job_features_image_prepared": True}
STACK_OPTIONS = ("--iam", "--platform", "--services", "--jobs")


def steps_of(body: str) -> list[str]:
    return [step for step in re.split(r"(?m)^(?=\d+\. )", body) if re.match(r"\d+\. ", step)]


def gold_sample(silver: dict[str, Any]) -> dict[str, Any]:
    """The reviewed file of the gold step: the silver file with the gold role and pair added."""
    variables = json.loads(json.dumps(silver["variables"]))
    variables["layer_jobs"] = ["silver", "gold"]
    variables["job_role_arns"]["write_gold_task"] = variables["job_role_arns"]["write_silver_task"].replace("silver", "gold")
    variables["writer_parameter_names"]["write_gold_task"] = [
        name.replace("silver", "gold") for name in variables["writer_parameter_names"]["write_silver_task"]
    ]
    return silver | {"variables": variables}


def platform_sample(*added: str) -> dict[str, Any]:
    """The reviewed file of an image repository step: the default repositories and log groups and the added repositories."""
    return {
        "schema_version": 1,
        "root": "platform",
        "variables": {"ecr_repositories": [*PLATFORM_DEFAULT_REPOSITORIES, "feature_store", *added], "log_groups": list(PLATFORM_DEFAULT_LOG_GROUPS)},
    }


def model_samples(train: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """The reviewed jobs files of the promotion, serving and drift lists: each is the file before it with its own task added."""
    if not train:
        return {}
    variables = json.loads(json.dumps(train["variables"]))
    promote = variables | {"promote_enabled": True}
    serve = promote | {
        "serve_enabled": True,
        "serve_roles": {"serve_task": variables["train_roles"]["train_task"].replace("train_task", "serve_task")},
        "serve_manifest_prefix": "serving/deployments",
    }
    images = variables["train_images"] | {"drift": variables["train_images"]["training"].replace("_training_", "_drift_")}
    drift = serve | {
        "drift_enabled": True,
        "drift_images": images,
        "drift_roles": {
            "execution": variables["train_roles"]["execution"],
            "drift_task": variables["train_roles"]["train_task"].replace("train_task", "drift_task"),
        },
    }
    return {f"jobs_{name}.json": train | {"variables": value} for name, value in (("promote", promote), ("serve", serve), ("drift", drift))}


def make_wrapper(workspace: Workspace, samples: dict[str, dict[str, Any]]) -> Any:
    """The wrapper runner of the command checks: the documented options in plan mode through the stand-in stack, and the apply grammar by its own rule."""

    def run(arguments: list[str]) -> tuple[int, str]:
        stack = next((item[2:] for item in arguments if item in STACK_OPTIONS), "storage")
        action = next((index for index, item in enumerate(arguments) if item in ("plan", "apply", "validate")), -1)
        if action < 0:
            return 2, "the wrapper needs plan, apply or validate"
        if arguments[action] == "apply":
            rest = arguments[action + 1 :]
            return (0, "") if len(rest) == 1 and not rest[0].startswith("-") else (2, "apply takes one saved plan file")
        options = arguments[:action]
        if stack == "iam":
            workspace.command("iam", IAM_FLAGS)
        if "--preparation-inputs" in options:
            path = Path(options[options.index("--preparation-inputs") + 1])
            path = path if path.is_absolute() else workspace.root / path
            options[options.index("--preparation-inputs") + 1] = str(path)
            sample = samples.get(path.name)
            if sample is None:
                return 2, f"no reviewed sample for {path.name}"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(sample, sort_keys=True) + "\n")
            options[options.index("--preparation-sha256") + 1] = hashlib.sha256(path.read_bytes()).hexdigest()
        (workspace.root / ".env").write_text(workspace.environment_file.read_text())
        line = " ".join(shlex.quote(item) for item in ["scripts/infrastructure/terraform.sh", *options, "plan", "-out", "step.tfplan"])
        (workspace.root / "call_wrapper.sh").write_text(f"#!/bin/bash\nexec bash {line}\n")
        result = run_command("bash", ["call_wrapper.sh"], cwd=workspace.root, env=workspace.environment() | {"PYTHON_BIN": sys.executable}, timeout=60)
        return result.returncode, result.stderr + result.stdout

    return run


def structure_checks(text: str) -> dict[str, bool]:
    body = owner_commands.section(text, SILVER)
    steps = steps_of(body)
    numbers = [int(step.split(".", 1)[0]) for step in steps]
    checks = {"the silver section is one numbered list": bool(body) and numbers == list(range(1, len(numbers) + 1)) and len(numbers) >= 12}

    def index(*needles: str) -> int:
        return next((position for position, step in enumerate(steps) if all(needle in step for needle in needles)), -1)

    iam = index("job_writer_layers_prepared", "job_layer_runs_prepared", "job_endpoints_enabled")
    checks["one IAM step sets the two layer flags and the endpoints flag together"] = iam >= 0
    checks["the IAM step reads job_network and stops unless endpoints_enabled is true"] = (
        iam >= 0 and "endpoints_enabled" in steps[iam] and "job_network" in steps[iam]
    )
    checks["the IAM step states that the plan may replace the host launch template"] = (
        iam >= 0 and "launch template" in steps[iam] and "Stop on any other change" in steps[iam]
    )
    order = [
        ("platform values", index("output -raw cluster_name")),
        ("IAM apply", iam),
        ("policies", index("manage_policies", "CONFIRM_IAM_POLICIES")),
        ("test host start", index("host_session start", "describe-instances")),
        ("catalog identities", index("aws_catalog_setup", "--identity silver")),
        ("images", index("push_images plan", "--repository-urls", "--only spark analytics quality")),
        ("jobs file", index("--jobs", "--preparation-sha256", "layer_jobs")),
        ("staging and approval", index("code_archive", "layer_acceptance approve")),
        ("IAM preflight", index("iam_preflight", "--scope silver")),
        ("dry run", index("run-task", "--dry-run")),
        ("dry run result", index("layer_acceptance task", "layer_acceptance plan", "get-log-events", "silver/publish/")),
        ("real run", index("before.json", "overrides_real.json")),
        ("evidence", index("after.json", "--include '*.json'", "list-tasks")),
        ("host stop and report", index("host_session stop", "layer_acceptance report", "layer_acceptance sign")),
        ("endpoints off", index("job_endpoints_enabled", "`false`")),
    ]
    positions = [position for _, position in order]
    checks[f"the steps come in the order the stacks depend on each other ({positions})"] = all(position >= 0 for position in positions) and positions == sorted(
        positions
    )
    jobs = index("--jobs", "--preparation-sha256", "layer_jobs")
    checks["the jobs step takes its roles, pair names and log group from the outputs of the earlier steps"] = jobs >= 0 and all(
        word in steps[jobs] for word in ("job_roles", "job_writer_layer_contract", "log_group_names", "images")
    )
    preflight = index("iam_preflight", "--scope silver")
    checks["the preflight step stops the list on a FAIL line and names the call that is denied"] = preflight >= 0 and all(
        word in steps[preflight] for word in ("`FAIL`", "Stop the list", "the role, the action and the resource")
    )
    dry = index("run-task", "--dry-run")
    checks["the dry run step requires an empty failures list and settles a lost reply"] = (
        dry >= 0 and "`failures` is empty" in steps[dry] and "lost reply" in steps[dry]
    )
    result = index("layer_acceptance task", "layer_acceptance plan")
    checks["the result step requires every container to exit 0 and reads this task's stream"] = result >= 0 and all(
        word in steps[result]
        for word in ("every container exited 0", "`init`", "`upload`", "silver/publish/<TASK_ID>", "--start-from-head", "stop-task --profile")
    )
    checks["the stop is bounded: the wait ends after 10 minutes and a late task is stopped"] = (
        result >= 0 and "10 minutes" in steps[result] and "6.5 hours" in steps[result]
    )
    gold = owner_commands.section(text, GOLD)
    checks["the gold section is a numbered list and takes the signed silver run as input"] = bool(steps_of(gold)) and "--input-run <SILVER_RUN_ID>" in gold
    features = owner_commands.section(text, FEATURES)
    checks |= features_checks(features)
    checks |= model_checks(text)
    checks |= prerequisite_checks(text)
    variables = {name for names in (variables_of(directory) for directory in STACKS.values()) for name in names}
    named = {name for name in re.findall(r"`([a-z][a-z0-9_]+)`", body + gold + features) if name in variables}
    rows = {row.name for row in ROWS}
    checks[f"every input the lists name has a row ({sorted(named - rows)})"] = named <= rows
    return checks


def features_checks(body: str) -> dict[str, bool]:
    """The features list is complete: every step of the first features run, in the order the stacks and the run depend on."""
    steps = steps_of(body)
    numbers = [int(step.split(".", 1)[0]) for step in steps]
    checks = {"the features section is one numbered list": bool(body) and numbers == list(range(1, len(numbers) + 1)) and len(numbers) >= 12}

    def index(*needles: str) -> int:
        return next((position for position, step in enumerate(steps) if all(needle in step for needle in needles)), -1)

    order = [
        ("repository", index("--platform", "ecr_repositories", "feature_store")),
        ("IAM activation", index("--iam", "job_features_image_prepared")),
        ("feature store image", index("push_images plan", "--only feature_store")),
        ("host start", index("host_session start", "describe-instances")),
        ("features catalog identity", index("aws_catalog_setup principal", "--identity features", "--namespace-approval <APPROVAL_ID>")),
        ("jobs file", index("--jobs", "train_images")),
        ("approval", index("code_archive", "layer_acceptance approve --layer features")),
        ("IAM preflight", index("iam_preflight --scope features")),
        ("dry run", index("run-task", "--input-run <GOLD_RUN_ID> --dry-run")),
        ("dry run result", index("layer_acceptance task --layer features", "layer_acceptance plan --layer features", "features/publish/<TASK_ID>")),
        ("real run", index("before_features.json", "--input-run <GOLD_RUN_ID> >")),
        ("evidence", index("after_features.json", "lakehouse/layer_runs/features/<RUN_ID>/", "list-tasks")),
        ("report", index("host_session stop", "layer_acceptance report --layer features", "--input-evidence", "--input-report", "layer_acceptance sign")),
        ("endpoints off", index("job_endpoints_enabled", "`false`")),
    ]
    positions = [position for _, position in order]
    checks[f"the features steps come in dependency order ({positions})"] = all(position >= 0 for position in positions) and positions == sorted(positions)
    pair = index("aws_catalog_setup principal", "--identity features")
    checks["the features catalog identity is set up under the project profile like silver and gold"] = (
        pair >= 0 and "(project profile)" in steps[pair] and "--admin-profile" not in steps[pair] and "administrator" not in steps[pair]
    )
    jobs = index("--jobs", "train_images")
    checks["the jobs step names the task role, the pair and only the features task definition"] = jobs >= 0 and all(
        word in steps[jobs] for word in ("write_features_task", "_job_features", "and nothing else")
    )
    iam = index("--iam", "job_features_image_prepared")
    checks["the IAM step says what the plan may add"] = iam >= 0 and "two policies" in steps[iam] and "Stop on any other change" in steps[iam]
    result = index("layer_acceptance task --layer features", "layer_acceptance plan --layer features")
    checks["the dry run step explains the online store limit"] = result >= 0 and "online store" in steps[result]
    checks["the list says how to recover from a failed Feast phase"] = "Feast phase fails" in body and "new run ID" in body
    return checks


# The steps of each hand-started model list, in the order the stacks and the runs depend on, each found by the words only that step holds.
MODEL_ORDER = {
    TRAIN: (
        ("repository", ("--platform", "platform_training.json", "`training`")),
        ("IAM endpoints and outputs", ("--iam", "output -json training_network", "Stop on any other change")),
        ("training image", ("push_images plan", "--only training")),
        ("host start", ("host_session start", "describe-instances", "5100")),
        ("jobs file", ("jobs_train.json", "train_roles", "_job_train", "and nothing else", "output -json train_run_task")),
        ("staging", ("git cat-file -t <CODE_REVISION>", "code_archive build --revision <CODE_REVISION>", "<FEATURES_RUN_ID>", "`mlflow`")),
        ("task start", ("--job train", "--training-parameters", "run-task", "`failures` is empty", "lost reply")),
        ("result", ("no `guard` container", "10 minutes", "6 hours", "train/train/<TASK_ID>", "stop-task --profile", "list-tasks")),
        ("registry", ("promotion_verdict", "`challenger`", "<MODEL_VERSION>")),
        ("host stop", ("host_session stop",)),
    ),
    PROMOTE: (
        ("jobs file", ("jobs_promote.json", "promote_enabled", "_job_promote", "and nothing else")),
        ("host start", ("Start the host", "MLflow")),
        (
            "approval",
            (
                "code_archive build --revision <CODE_REVISION>",
                "promotion_approval.json",
                "model_promotion_dag",
                "lakehouse/run_stage/<RUN_ID>/inputs/promotion_approval.json",
            ),
        ),
        ("task start", ("--job promote", "--training-parameters", "run-task", "`failures` is empty", "lost reply")),
        ("result", ("promote/promote/<TASK_ID>", "1 hour", "list-tasks")),
        ("registry", ("`production`", "promoted_run")),
    ),
    SERVE: (
        ("IAM role", ("--iam", "serve_prepared", "model_api_manifest_prefix", "Stop on any other change")),
        (
            "project IAM policy",
            ("manage_policies plan --policy iam", "PassTrainingRoles", "job_serve_task", "job_drift_task", "overlay_sha256.txt", "git checkout --"),
        ),
        ("read grant", ("ReadModelRunRecords", "manage_policies plan --policy s3", "s3:GetObject", "git checkout --")),
        ("jobs file", ("jobs_serve.json", "serve_manifest_prefix", "_job_serve", "and nothing else")),
        ("host start", ("Start the host", "MLflow")),
        ("request", ("code_archive build --revision <CODE_REVISION>", "serving_request.json", "lakehouse/run_stage/<RUN_ID>/inputs/serving_request.json")),
        ("task start", ("--job serve", "--serving-request", "run-task", "`failures` is empty", "lost reply")),
        ("result", ("serve/serve/<TASK_ID>", "15 minutes", "active/<HAI_TYPE>.json", "history/<HAI_TYPE>/<RUN_ID>.json", "list-tasks")),
        ("records", ("sequence 1",)),
    ),
    DRIFT: (
        ("repository", ("--platform", "platform_drift.json", "`drift`")),
        ("IAM role", ("--iam", "drift_prepared", "output -json drift_roles", "Stop on any other change")),
        ("project IAM policy", ("manage_policies plan --policy iam", "PassTrainingRoles", "job_drift_task", "overlay_sha256.txt", "git checkout --")),
        ("read grant", ("ReadModelRunRecords", "manage_policies plan --policy s3")),
        ("drift image", ("git apply --check", "gap2_drift_image.patch", "push_images plan", "--only drift", "git apply -R")),
        ("jobs file", ("jobs_drift.json", "drift_roles", "_job_drift", "and nothing else")),
        ("host start and staging", ("Start the host", "code_archive build --revision <CODE_REVISION>")),
        ("task start", ("--job drift", "run-task", "`failures` is empty", "lost reply")),
        ("result", ("_complete.json", "drift/<RUN_ID>/", "2 hours", "list-tasks")),
        ("records", ("`skipped`", "`triggered`", "host_session stop")),
    ),
}


def model_checks(text: str) -> dict[str, bool]:
    """The model lists are complete and in dependency order, start every task in the training network and print parameters that their contracts accept."""
    checks: dict[str, bool] = {}
    for heading, order in MODEL_ORDER.items():
        body = owner_commands.section(text, heading)
        steps = steps_of(body)
        numbers = [int(step.split(".", 1)[0]) for step in steps]
        checks[f"{heading}: one numbered list"] = bool(body) and numbers == list(range(1, len(numbers) + 1)) and len(numbers) >= len(order)
        positions = [next((index for index, step in enumerate(steps) if all(needle in step for needle in needles)), -1) for _, needles in order]
        missing = [label for (label, _), position in zip(order, positions, strict=True) if position < 0]
        checks[f"{heading}: the steps come in dependency order ({positions}, missing {missing})"] = not missing and positions == sorted(positions)
        starts = [line for line in owner_commands.commands(body) if "ecs run-task" in line]
        checks[f"{heading}: every task starts once on Fargate in the training network with the Project tag and no public address"] = len(starts) == 1 and all(
            all(word in line for word in ("<TRAINING_SUBNET_ID>", "<TRAINING_SECURITY_GROUP>", '"FARGATE"', '"key":"Project"', '"assignPublicIp":"DISABLED"'))
            and "LAKEHOUSE" not in line
            for line in starts
        )
        checks[f"{heading}: the list stops on any failure and the owner applies each plan"] = (
            "Stop the list on any failure" in body and "Each apply is the owner's" in body
        )
    table = owner_commands.values(Path("record"))
    contracts = (
        (TRAIN, "--training-parameters", lambda raw: training_contract.parameters_for("training_dag", raw, ("aws",))),
        (PROMOTE, "--training-parameters", lambda raw: training_contract.parameters_for("model_promotion_dag", raw, ("aws",))),
        (SERVE, "--serving-request", lambda raw: serving_contract.parameters_for("serving_deploy_dag", raw)),
    )
    for heading, option, contract in contracts:
        words = [shlex.split(owner_commands.filled(line, table)) for line in owner_commands.commands(owner_commands.section(text, heading)) if option in line]
        values = [line[line.index(option) + 1] for line in words]
        try:
            parsed = [contract(json.loads(value)) for value in values]
        except (ValueError, KeyError) as error:
            parsed = []
            checks[f"{heading}: the {option} value passes its contract ({error})"] = False
        else:
            checks[f"{heading}: the {option} value passes its contract"] = len(parsed) == 1
        if heading == SERVE:
            first = {"candidate_version": None, "percent": 0, "deploy_mode": "rollback"}
            staged = re.findall(r"printf '(\{.*?\})\\n'", owner_commands.section(text, heading))
            checks[f"{heading}: the first deploy is a rollback with no candidate and percent 0, staged with the same request"] = (
                len(parsed) == 1
                and all(parsed[0][name] == value for name, value in first.items())
                and len(staged) == 1
                and json.loads(staged[0].replace("%s", parsed[0]["hai_type"])) == parsed[0]
            )
    return checks


IAM_OVERLAY = "data/local_keep/iam_combined_20261107/"
PROJECT_IAM_POLICY = "infra/iam/policies/healthcare_hai_prediction_iam_policy.json"
DRIFT_IMAGE_PATCH = "data/local_keep/serving_gaps_20261107/gap2_drift_image.patch"
READ_GRANT = "ReadModelRunRecords"


def step_with(steps: list[str], *needles: str) -> int:
    return next((index for index, step in enumerate(steps) if all(needle in step for needle in needles)), -1)


def prerequisite_checks(text: str) -> dict[str, bool]:
    """The prerequisites of the hand-started model lists: the reviewed code revision, the IAM overlay, the project IAM policy, the owner read grant and the
    drift image patch, each stated and each placed before the step that needs it."""
    checks: dict[str, bool] = {}
    train = owner_commands.section(text, TRAIN)
    intro = re.split(r"(?m)^1\. ", train, maxsplit=1)[0]
    checks["the training list states the code revision prerequisite and names the three reviewed fixes"] = all(
        word in intro for word in ("`<CODE_REVISION>`", "process listing fix", "Feast branch", "Feast registry reader fix", "HEAD of the main checkout")
    )
    checks["the training list states that every IAM plan runs on the combined overlay"] = IAM_OVERLAY in intro and "from `main` would remove" in intro
    for heading in MODEL_ORDER:
        body = owner_commands.section(text, heading)
        lines = owner_commands.commands(body)
        builds = [index for index, line in enumerate(lines) if "code_archive build" in line]
        checks[f"{heading}: every code archive is built from the reviewed revision, after the revision is checked"] = bool(builds) and all(
            "--revision <CODE_REVISION>" in lines[index] and any(line == "git cat-file -t <CODE_REVISION>" for line in lines[:index]) for index in builds
        )
        iam_steps = [step for step in steps_of(body) if "terraform.sh --iam" in step]
        checks[f"{heading}: every infra/iam plan names the IAM overlay"] = all("IAM overlay" in step for step in iam_steps)
    for heading, role in ((SERVE, "job_serve_task"), (DRIFT, "job_drift_task")):
        body = owner_commands.section(text, heading)
        steps = steps_of(body)
        policy = step_with(steps, "manage_policies plan --policy iam", "PassTrainingRoles", role, "Prerequisite")
        start = step_with(steps, "ecs run-task")
        checks[f"{heading}: the hand apply of the project IAM policy with {role} in PassTrainingRoles comes before the task start"] = (
            0 <= policy < start
            and all(
                word in steps[policy]
                for word in (
                    f"shasum -a 256 {IAM_OVERLAY}overlay/{PROJECT_IAM_POLICY}",
                    f"{IAM_OVERLAY}overlay_sha256.txt",
                    f"cp {IAM_OVERLAY}overlay/{PROJECT_IAM_POLICY} {PROJECT_IAM_POLICY}",
                    "manage_policies apply --policy iam",
                    f"git checkout -- {PROJECT_IAM_POLICY}",
                    "Stop on any other change",
                )
            )
        )
        grant = step_with(steps, READ_GRANT, "manage_policies plan --policy s3", "Prerequisite")
        reads = [index for index, step in enumerate(steps) if re.search(r"aws s3 cp s3://<DATA_BUCKET>/(<MANIFEST_PREFIX>|drift)/", step)]
        checks[f"{heading}: the owner read grant comes before every read of the serving or drift records"] = grant >= 0 and bool(reads) and grant < min(reads)
    serve = owner_commands.section(text, SERVE)
    found = re.search(r"```json\n(.*?)```", serve, re.S)
    try:
        statement = json.loads(found.group(1)) if found else {}
    except ValueError:
        statement = {}
    checks["the read grant is one statement that allows only s3:GetObject on the record prefix and drift/"] = (
        statement
        == {
            "Sid": READ_GRANT,
            "Effect": "Allow",
            "Action": "s3:GetObject",
            "Resource": ["arn:aws:s3:::${DATA_BUCKET_NAME}/<MANIFEST_PREFIX>/*", "arn:aws:s3:::${DATA_BUCKET_NAME}/drift/*"],
        }
        and "infra/iam/policies/healthcare_hai_prediction_s3_policy.json" in serve
    )
    drift_lines = owner_commands.commands(owner_commands.section(text, DRIFT))
    order = [
        next((index for index, line in enumerate(drift_lines) if line.startswith(prefix)), -1)
        for prefix in (
            f"git apply --check {DRIFT_IMAGE_PATCH}",
            f"git apply {DRIFT_IMAGE_PATCH}",
            ".venv/bin/python -m scripts.infrastructure.push_images plan",
            f"git apply -R {DRIFT_IMAGE_PATCH}",
        )
    ]
    checks[f"the drift image is planned only with the drift image patch applied, then the patch is removed ({order})"] = -1 not in order and order == sorted(
        order
    )
    return checks


# Each prerequisite removed or widened in one list must fail a prerequisite check.
PREREQUISITE_MUTANTS = (
    ("an archive of HEAD", DRIFT, "build --revision <CODE_REVISION> --output", "build --output"),
    ("a revision that is not checked first", PROMOTE, "git cat-file -t <CODE_REVISION>\n", ""),
    ("no project IAM policy apply before the serve start", SERVE, "manage_policies plan --policy iam", "manage_policies plan"),
    ("a project IAM policy without the drift role", DRIFT, "`job_drift_task` to ECS tasks", "the role to ECS tasks"),
    ("a read grant with every S3 action", SERVE, '"Action": "s3:GetObject"', '"Action": "s3:*"'),
    ("a read grant on the whole bucket", SERVE, "${DATA_BUCKET_NAME}/drift/*", "${DATA_BUCKET_NAME}/*"),
    ("a drift read without the read grant", DRIFT, READ_GRANT, "ReadDriftRecords"),
    ("a drift image plan without the patch", DRIFT, f"git apply --check {DRIFT_IMAGE_PATCH}\n", ""),
    ("an IAM plan without the overlay", TRAIN, "(administrator profile), with the IAM overlay copied in", "(administrator profile)"),
)


def prerequisite_mutant_checks(text: str) -> dict[str, bool]:
    checks: dict[str, bool] = {}
    for label, heading, old, new in PREREQUISITE_MUTANTS:
        body = owner_commands.section(text, heading)
        if old not in body:
            checks[f"prerequisite mutant {label}: the text exists in {heading}"] = False
            continue
        mutated = text.replace(body, body.replace(old, new, 1), 1)
        checks[f"prerequisite mutant {label} is refused"] = not all(prerequisite_checks(mutated).values())
    return checks


def command_checks(text: str, folder: Path) -> dict[str, bool]:
    workspace = Workspace(folder / "workspace")
    silver_body = owner_commands.section(text, SILVER)
    sample = owner_commands.example_json(silver_body) or {}
    features_sample = owner_commands.example_json(owner_commands.section(text, FEATURES)) or {}
    train_sample = owner_commands.example_json(owner_commands.section(text, TRAIN)) or {}
    samples = {
        "jobs_silver.json": sample,
        "jobs_gold.json": gold_sample(sample) if sample else {},
        "jobs_features.json": features_sample,
        "platform_features.json": platform_sample(),
        "jobs_train.json": train_sample,
        **model_samples(train_sample),
        "platform_training.json": platform_sample("training"),
        "platform_drift.json": platform_sample("training", "drift"),
    }
    wrapper = make_wrapper(workspace, samples)
    checks: dict[str, bool] = {
        "the reviewed jobs file of the list is valid JSON with a root and variables": sample.get("root") == "jobs" and "variables" in sample,
        "the reviewed training jobs file is valid JSON and extends the features file": train_sample.get("root") == "jobs"
        and bool(features_sample.get("variables"))
        and all(train_sample["variables"].get(key) == value for key, value in features_sample["variables"].items() if key != "train_images")
        and train_sample["variables"].get("train_enabled") is True
        and features_sample["variables"].get("train_images", {}).items() <= train_sample["variables"].get("train_images", {}).items(),
    }
    for heading in HEADINGS:
        problems = owner_commands.check_section(owner_commands.section(text, heading), folder / heading.split()[0].lower(), wrapper, {"ADMIN_PROFILE": ADMIN})
        count = len(problems)
        checks[f"{heading}: {count} printed commands were found"] = count >= 5
        for number, (line, found) in enumerate(problems.items(), 1):
            checks[f"{heading}: command {number} parses: {line[:110]}" + (f" -> {found[0][:150]}" if found else "")] = not found
    return checks


MUTANTS = (
    ("terraform output with three names", "output -raw cluster_name", "output -raw cluster_name project_tag log_group_names"),
    ("manage_policies without the administrator profile", "manage_policies plan --admin-profile <ADMIN_PROFILE>", "manage_policies plan"),
    ("push_images without the repository file", "--repository-urls <RECORD_DIR>/repository_urls.json ", ""),
    (
        "describe-tasks without the region",
        "describe-tasks --profile <PROJECT_PROFILE> --region <REGION> --cluster",
        "describe-tasks --profile <PROJECT_PROFILE> --cluster",
    ),
    (
        "a mistyped aws option",
        "--cluster <CLUSTER_NAME> --tasks <TASK_ARN> --query '{failures",
        "--clustr <CLUSTER_NAME> --tasks <TASK_ARN> --query '{failures",
    ),
    ("a mistyped module", "scripts.lakehouse.layer_acceptance task", "scripts.lakehouse.layer_acceptanc task"),
    ("an unknown placeholder", "--layer silver --describe <RECORD_DIR>/describe_dry.json", "--layer silver --describe <RECORD_DIR>/<DESCRIBE_FILE>"),
    ("a broken JSON argument", '\'[{"capacityProvider":"FARGATE","weight":1}]\'', '\'[{"capacityProvider":"FARGATE","weight":1}\''),
    ("an unknown preflight scope", "iam_preflight --scope silver", "iam_preflight --scope bronze"),
    ("a report without its output path", " --out <RECORD_DIR>/qualification_silver.json", ""),
    ("an unknown layer", "layer_acceptance plan --layer silver", "layer_acceptance plan --layer bronze"),
    ("an unknown preflight scope in the features list", "iam_preflight --scope features", "iam_preflight --scope feature"),
    ("an unknown identity in the features list", "--identity features --instance", "--identity feature --instance"),
    ("a mistyped image option in the features list", "--only feature_store", "--omly feature_store"),
    ("a features report without its output path", " --out <RECORD_DIR>/qualification_features.json", ""),
    (
        "features overrides without the digest",
        "--job features --run <RUN_ID> --code-run <RUN_ID> --code-sha256 <CODE_SHA256> ",
        "--job features --run <RUN_ID> --code-run <RUN_ID> ",
    ),
    ("training overrides without the MLflow address", "--mlflow-uri http://<HOST_PRIVATE_IP>:5100 --training-parameters", "--training-parameters"),
    ("an unknown training network output", "output -json training_network", "output -json training_net"),
    ("promotion overrides for an unknown job", "--job promote ", "--job promotion "),
    (
        "serve overrides without the request",
        ' --serving-request \'{"hai_type":"<HAI_TYPE>","candidate_version":null,"percent":0,"deploy_mode":"rollback"}\'',
        "",
    ),
    ("a drift start without its task definition", "--task-definition <PROJECT_NAME>_<ENVIRONMENT>_job_drift ", "--task-definition "),
)


def mutant_checks(text: str, folder: Path) -> dict[str, bool]:
    """Each planted typo in one printed command must fail the parser check of that line."""
    lines = [line for heading in HEADINGS for line in owner_commands.commands(owner_commands.section(text, heading))]
    wrapper = make_wrapper(Workspace(folder / "mutant_workspace"), {})
    checks: dict[str, bool] = {}
    for label, old, new in MUTANTS:
        line = next((item for item in lines if old in item), None)
        if line is None:
            checks[f"mutant {label}: the command exists in the lists"] = False
            continue
        table = owner_commands.values(folder / "mutant" / "record", {"ADMIN_PROFILE": ADMIN})
        (folder / "mutant" / "record").mkdir(parents=True, exist_ok=True)
        try:
            found = owner_commands.check_line(owner_commands.filled(line.replace(old, new, 1), table), folder / "mutant", wrapper)
        except (KeyError, ValueError) as error:
            found = [str(error)]
        checks[f"mutant {label} is refused"] = bool(found)
    return checks | prerequisite_mutant_checks(text)


def run(output: Path) -> dict[str, dict[str, bool]]:
    text = "".join(f"## {heading}\n{body}" for heading, body in json.loads(OWNER_LISTS.read_text()).items())
    with tempfile.TemporaryDirectory(prefix="owner_list_") as folder:
        results = {"structure": structure_checks(text), "commands": command_checks(text, Path(folder)), "mutants": mutant_checks(text, Path(folder))}
    output.mkdir(parents=True)
    (output / "report.json").write_text(json.dumps(results, indent=2, sort_keys=True) + "\n")
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--out", type=Path, required=True, help="a new folder for the report")
    arguments = parser.parse_args()
    results = run(arguments.out)
    failed = [f"{group}: {name}" for group, checks in results.items() for name, ok in checks.items() if not ok]
    total = sum(len(checks) for checks in results.values())
    for line in failed:
        sys.stdout.write(f"FAIL {line}\n")
    sys.stdout.write(f"owner list E2E: {total - len(failed)} of {total} checks passed\n")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
