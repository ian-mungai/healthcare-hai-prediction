"""Check the override generator against the variables that the real tasks need.

A hand-started task leaves its variables empty until the run-task call fills them. These cases build the overrides of every job of the job catalog with
sample values and require that they fill exactly the variables that the table gives each container, and that the generator refuses a missing value by name.
"""

from __future__ import annotations

import json
import sys
from collections.abc import Callable
from pathlib import Path

from scripts.orchestration import job_overrides
from scripts.orchestration.ecs_adapter_cases import require
from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]
RUN = "silver_abcdef01"
VALUES = {
    "RUN_ID": RUN,
    "CODE_RUN_ID": RUN,
    "INPUT_RUN_ID": "silver_0badcafe",
    "FEATURES_RUN": "features_feed0001",
    "FEATURE_SET": "base",
    "CODE_SHA256": "ab" * 32,
    "POLARIS_URI": "http://10.0.0.5:8181/api/catalog",
    "MLFLOW_TRACKING_URI": "http://10.0.0.5:5100",
    "TRAINING_PARAMETERS": '{"mlflow": "aws"}',
    "BRONZE_DRY_RUN": "1",
    "BRONZE_INVENTORY_SHA256": "cd" * 32,
    "BRONZE_REQUEST_SHA256": "ef" * 32,
    "BRONZE_TABLES": '["synthetic_table"]',
    "BRONZE_TARGET": "bronze_e2e",
    "HAI_DEADLINE_UTC": "2026-10-30T12:00:00Z",
    "PHASE": "promote",
    "LAYER_PHASE": "release",
    "RELEASE_MODEL": "m2",
}


def job_table() -> dict:
    return json.loads((ROOT / "config/orchestration/ecs_broker_jobs.json").read_text())["jobs"]


def the_generator_fills_every_variable_of_every_job() -> None:
    check_overrides(job_table())


def a_job_variable_without_a_sample_fails_the_check() -> None:
    table = job_table()
    container = next(iter(table["silver"]["containers"]))
    table["silver"]["containers"][container] = [*table["silver"]["containers"][container], "REQUIRED_NEW_VALUE"]
    try:
        check_overrides(table)
    except AssertionError as error:
        require("REQUIRED_NEW_VALUE" in str(error), f"the refusal does not name the variable: {error}")
        return
    require(False, "a job that needs a variable without a sample value was skipped instead of failing the check")


def check_overrides(table: dict) -> None:
    for job, row in table.items():
        if row.get("needs_serving_parameters"):
            continue
        needed = {variable for variables in row["containers"].values() for variable in variables}
        unknown = needed - set(VALUES) - {job_overrides.DRY_RUN_VARIABLE}
        require(not unknown, f"the {job} job needs {sorted(unknown)}, which have no sample value here: add one so its overrides are checked")
        for dry_run in (False, True) if job in job_overrides.LAYERS else (False,):
            document = job_overrides.build(job, {name: VALUES[name] for name in needed if name in VALUES}, dry_run)
            built = {item["name"]: [pair["name"] for pair in item["environment"]] for item in document["containerOverrides"]}
            wanted = {container: [*variables, *([job_overrides.DRY_RUN_VARIABLE] if dry_run else [])] for container, variables in row["containers"].items()}
            require(built == wanted, f"the {job} overrides fill {built}, the task needs {wanted}")


def a_features_build_leaves_the_release_variables_empty() -> None:
    """A features task that is not a release gets the three release variables empty, so a hand-started build needs no value for them: its feast container
    applies Feast and every build phase runs. The metadata job, which reads FEATURES_RUN too, still refuses it when it is missing."""
    build_values = {name: value for name, value in VALUES.items() if name not in ("LAYER_PHASE", "RELEASE_MODEL", "FEATURES_RUN")}
    document = job_overrides.build("features", build_values)
    held = {item["name"]: {pair["name"]: pair["value"] for pair in item["environment"]} for item in document["containerOverrides"]}
    require(
        held["feast"]
        == {
            **{name: build_values[name] for name in ("RUN_ID", "CODE_RUN_ID", "POLARIS_URI", "FEATURE_SET")},
            **dict.fromkeys(("FEATURES_RUN", "RELEASE_MODEL", "LAYER_PHASE"), ""),
        },
        f"{held['feast']}",
    )
    require("release" not in held, f"the features task has a release container: {sorted(held)}")
    require(all(container.get("LAYER_PHASE", "") == "" for container in held.values()), f"a build was told a phase: {held}")
    try:
        job_overrides.build("metadata", {name: value for name, value in VALUES.items() if name != "FEATURES_RUN"})
    except job_overrides.OverrideError as error:
        require("FEATURES_RUN" in str(error), f"the refusal does not name the variable: {error}")
    else:
        require(False, "the metadata job was built without the features run it ingests")


def a_release_names_its_model_and_run() -> None:
    """A hand-started release (LAYER_PHASE release) needs the model set and the features run it releases, by name, and fills its feast container."""
    for missing in ("RELEASE_MODEL", "FEATURES_RUN"):
        try:
            job_overrides.build("features", {name: value for name, value in VALUES.items() if name != missing})
        except job_overrides.OverrideError as error:
            require(missing in str(error), f"the refusal does not name {missing}: {error}")
        else:
            require(False, f"a release without {missing} was built")
    done = run_command(
        sys.executable,
        ["-m", "scripts.orchestration.job_overrides", "--job", "features", "--run", RUN, "--code-run", RUN, "--code-sha256", "ab" * 32]
        + [
            "--polaris-uri",
            VALUES["POLARIS_URI"],
            "--input-run",
            VALUES["INPUT_RUN_ID"],
            "--layer-phase",
            "release",
            "--release-model",
            "m2",
            "--features-run",
            VALUES["FEATURES_RUN"],
        ],
        cwd=ROOT,
        timeout=60,
    )
    require(done.returncode == 0, f"{done.returncode}: {done.stderr.strip()[-200:]}")
    feast = next(item for item in json.loads(done.stdout)["containerOverrides"] if item["name"] == "feast")
    held = {pair["name"]: pair["value"] for pair in feast["environment"]}
    require(
        (held["LAYER_PHASE"], held["RELEASE_MODEL"], held["FEATURES_RUN"]) == ("release", "m2", VALUES["FEATURES_RUN"]) and len(feast["environment"]) == 7,
        f"the feast container: {feast}",
    )


def a_reload_names_its_model_and_no_features_run() -> None:
    """A reload launch (LAYER_PHASE reload, what ``host_session start`` sends) needs the model set and no features run: the reload picks its own run."""
    reload_values = {name: value for name, value in VALUES.items() if name != "FEATURES_RUN"} | {"LAYER_PHASE": "reload"}
    document = job_overrides.build("features", reload_values)
    held = {item["name"]: {pair["name"]: pair["value"] for pair in item["environment"]} for item in document["containerOverrides"]}
    require(
        held["feast"]
        == {
            **{name: reload_values[name] for name in ("RUN_ID", "CODE_RUN_ID", "POLARIS_URI", "RELEASE_MODEL", "FEATURE_SET")},
            "FEATURES_RUN": "",
            "LAYER_PHASE": "reload",
        },
        f"{held['feast']}",
    )
    require(
        all(container.get("LAYER_PHASE") == "reload" for container in held.values() if "LAYER_PHASE" in container),
        f"a build container was told another phase: {held}",
    )
    try:
        job_overrides.build("features", {name: value for name, value in reload_values.items() if name != "RELEASE_MODEL"})
    except job_overrides.OverrideError as error:
        require("RELEASE_MODEL" in str(error), f"the refusal does not name the variable: {error}")
    else:
        require(False, "a reload without its model set was built")


def the_session_reload_launch_equals_the_generator_output() -> None:
    """``host_session start`` builds the overrides of a reload launch from the variable names of the jobs stack. For the containers of the job catalog it fills
    the same variables with the same values as the hand-started generator does, so a reload launch is the launch that a runbook would build."""
    from scripts.infrastructure import host_session_cases

    host, session, _ = host_session_cases.build()
    code = (RUN, "ab" * 32)
    ours = session.reload_overrides("m2", code)["containerOverrides"]
    run = ours[0]["environment"][0]["value"]
    given = {
        **VALUES,
        "RUN_ID": run,
        "CODE_RUN_ID": code[0],
        "CODE_SHA256": code[1],
        "POLARIS_URI": f"http://{host_session_cases.HOST_ADDRESS}:8181/api/catalog",
    }
    theirs = job_overrides.build("features", {name: value for name, value in given.items() if name != "FEATURES_RUN"} | {"LAYER_PHASE": "reload"})[
        "containerOverrides"
    ]

    def values(overrides: list[dict]) -> dict[str, dict[str, str]]:
        """The variables of each container, without the two that only the build phases read (the reload ends them at once)."""
        return {
            item["name"]: {pair["name"]: pair["value"] for pair in item["environment"] if pair["name"] not in ("FEATURE_SET", "INPUT_RUN_ID")}
            for item in overrides
        }

    require(values(ours) == values(theirs), f"the session fills {values(ours)}, the generator {values(theirs)}")


def the_generator_builds_no_serving_request() -> None:
    """The serve job's request is derived by the launch from a closed request, so no owner command builds it."""
    serving = [job for job, row in job_table().items() if row.get("needs_serving_parameters")]
    require(serving == ["serve"], f"the jobs that take a serving request are {serving}")
    try:
        job_overrides.build("serve", dict(VALUES))
    except job_overrides.OverrideError as error:
        require("direct launch" in str(error), f"the refusal does not name the launch: {error}")
    else:
        require(False, "the serve job's overrides were built by hand")


def the_generator_refuses_a_missing_value_by_name() -> None:
    done = run_command(
        sys.executable,
        [
            "-m",
            "scripts.orchestration.job_overrides",
            "--job",
            "silver",
            "--run",
            RUN,
            "--code-sha256",
            "ab" * 32,
            "--polaris-uri",
            "http://10.0.0.5:8181/api/catalog",
        ],
        cwd=ROOT,
        timeout=60,
    )
    require(done.returncode == 2 and "CODE_RUN_ID" in done.stderr, f"{done.returncode}: {done.stderr.strip()[-200:]}")
    done = run_command(sys.executable, ["-m", "scripts.orchestration.job_overrides", "--job", "gold", "--run", RUN, "--dry-run"], cwd=ROOT, timeout=60)
    require(done.returncode == 2, "a gold dry run without its input run was accepted")


CASES: dict[str, Callable[[], None]] = {
    "the_generator_fills_every_variable_of_every_job": the_generator_fills_every_variable_of_every_job,
    "the_generator_builds_no_serving_request": the_generator_builds_no_serving_request,
    "the_generator_refuses_a_missing_value_by_name": the_generator_refuses_a_missing_value_by_name,
    "a_job_variable_without_a_sample_fails_the_check": a_job_variable_without_a_sample_fails_the_check,
    "a_features_build_leaves_the_release_variables_empty": a_features_build_leaves_the_release_variables_empty,
    "a_release_names_its_model_and_run": a_release_names_its_model_and_run,
    "a_reload_names_its_model_and_no_features_run": a_reload_names_its_model_and_no_features_run,
    "the_session_reload_launch_equals_the_generator_output": the_session_reload_launch_equals_the_generator_output,
}
