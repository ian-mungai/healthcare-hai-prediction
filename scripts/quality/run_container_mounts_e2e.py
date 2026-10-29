"""Exercise the container import hook on source-only copies, including reconstructed missing mounts.

Run ``.venv/bin/python -m scripts.quality.run_container_mounts_e2e``. Each case invokes the actual pre-commit hook.
No application import, container, install or network operation runs. Evidence stays under ``data/e2e/container_mounts``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sys
import tempfile
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from scripts.process import CompletedProcess, TimeoutExpired, run_command

ROOT = Path(__file__).resolve().parents[2]
CHECKER = "scripts/quality/container_mounts.py"
LAUNCHER = "orchestration/launcher.py"
COMPOSE = "docker-compose.yaml"
QUALITY = "scripts/lakehouse/silver_quality.py"
RUNTIME = "orchestration/dag_runtime.py"
PIPELINE = "orchestration/pipeline_render.py"
RUN_SECONDS = 720
PYTHON = ROOT / ".venv/bin/python"


@dataclass(frozen=True)
class Case:
    """Source replacements and the independently specified hook result for one synthetic defect."""

    name: str
    files: dict[str, str]
    exit_code: int = 1
    messages: tuple[str, ...] = ()
    seconds: float = 30
    counts: tuple[tuple[str, int], ...] = ()


def injected(source: str, statement: str) -> str:
    anchor = "from __future__ import annotations\n"
    if source.count(anchor) != 1:
        raise ValueError("The synthetic import requires one future-import anchor")
    return source.replace(anchor, anchor + "\n" + statement + "\n", 1)


def replaced(source: str, old: str, new: str) -> str:
    if source.count(old) != 1:
        raise ValueError(f"The fixture replacement requires one occurrence: {old!r}")
    return source.replace(old, new, 1)


def scenarios(source: dict[str, str]) -> list[Case]:
    """Define observable import and mount failures before the checker implementation."""
    engine = "scripts/lakehouse/ecs_engine.py"
    engine_code = "from orchestration.pipeline_protocol import PipelineError\n"
    helper = "scripts/lakehouse/mount_helper.py"
    runtime_import = injected(source[RUNTIME], "import orchestration.training_contract")
    quality_import = injected(source[QUALITY], "from scripts.lakehouse import ecs_engine")
    session = "scripts/lakehouse/session.py"
    cases = [
        Case("current_main", {}, 0),
        Case(
            "past_training_contract",
            {RUNTIME: runtime_import, "orchestration/training_contract.py": "VALUE = 'synthetic'\n"},
            messages=(RUNTIME, "orchestration.training_contract", "container services/orchestration/docker-compose.orchestration.yaml:airflow-scheduler"),
        ),
        Case(
            "past_quality_engine",
            {QUALITY: quality_import, engine: engine_code},
            messages=(engine, "orchestration.pipeline_protocol", "container docker-compose.yaml:quality"),
        ),
        Case(
            "past_spark_engine",
            {session: injected(source[session], "from scripts.lakehouse import ecs_engine"), engine: engine_code},
            messages=(engine, "orchestration.pipeline_protocol", "container launcher:spark/"),
        ),
        Case(
            "transitive_relative_import",
            {
                QUALITY: injected(source[QUALITY], "from scripts.lakehouse import mount_helper as helper"),
                helper: "from .mount_bridge import VALUE\n",
                "scripts/lakehouse/mount_bridge.py": "from orchestration.pipeline_protocol import PipelineError\nVALUE = 1\n",
            },
            messages=("scripts/lakehouse/mount_bridge.py", "orchestration.pipeline_protocol", "container docker-compose.yaml:quality"),
        ),
        Case(
            "package_initializer",
            {
                QUALITY: injected(source[QUALITY], "import scripts.lakehouse.mount_fixture.leaf"),
                "scripts/lakehouse/mount_fixture/__init__.py": "import orchestration.pipeline_protocol\n",
                "scripts/lakehouse/mount_fixture/leaf.py": "VALUE = 1\n",
            },
            messages=("scripts/lakehouse/mount_fixture/__init__.py", "orchestration.pipeline_protocol", "container docker-compose.yaml:quality"),
        ),
        Case(
            "individual_airflow_mount",
            {RUNTIME: injected(source[RUNTIME], "from orchestration import graph")},
            messages=(RUNTIME, "orchestration.graph", "container services/orchestration/docker-compose.orchestration.yaml:airflow-scheduler"),
        ),
        Case(
            "literal_dynamic_import_alias",
            {
                QUALITY: injected(source[QUALITY], "from importlib import import_module as load\nhelper = load('scripts.lakehouse.mount_helper')"),
                helper: "import orchestration.pipeline_protocol\n",
            },
            messages=(helper, "orchestration.pipeline_protocol", "container docker-compose.yaml:quality"),
        ),
        Case(
            "referenced_function_import",
            {
                QUALITY: injected(source[QUALITY], "from scripts.lakehouse.mount_helper import invoke\ninvoke()"),
                helper: "def invoke():\n    import orchestration.pipeline_protocol\n",
            },
            messages=(helper, "orchestration.pipeline_protocol", "container docker-compose.yaml:quality"),
        ),
        Case(
            "module_value_alias",
            {
                QUALITY: injected(source[QUALITY], "from scripts.lakehouse import mount_helper\nother = mount_helper\nother.invoke()"),
                helper: "def invoke():\n    import orchestration.pipeline_protocol\n",
            },
            messages=(helper, "orchestration.pipeline_protocol", "container docker-compose.yaml:quality"),
        ),
        Case(
            "module_passed_to_callback",
            {
                QUALITY: injected(
                    source[QUALITY], "from scripts.lakehouse import mount_helper\ndef callback(module):\n    module.invoke()\ncallback(mount_helper)"
                ),
                helper: "def invoke():\n    import orchestration.pipeline_protocol\n",
            },
            messages=(helper, "orchestration.pipeline_protocol", "container docker-compose.yaml:quality"),
        ),
        Case(
            "relative_dynamic_import",
            {
                QUALITY: injected(source[QUALITY], "from scripts.lakehouse import mount_helper"),
                helper: "import importlib\nimportlib.import_module('.mount_bridge', __package__)\n",
                "scripts/lakehouse/mount_bridge.py": "import orchestration.pipeline_protocol\n",
            },
            messages=("scripts/lakehouse/mount_bridge.py", "orchestration.pipeline_protocol", "container docker-compose.yaml:quality"),
        ),
        Case(
            "unused_host_command",
            {
                QUALITY: injected(source[QUALITY], "from scripts.lakehouse.mount_helper import value\nvalue()"),
                helper: "def value():\n    return 1\n\ndef main():\n    import orchestration.pipeline_protocol\n\nif __name__ == '__main__':\n    main()\n",
            },
            0,
        ),
        Case(
            "third_party_is_not_executed",
            {
                QUALITY: injected(source[QUALITY], "from scripts.lakehouse import mount_helper"),
                helper: "import synthetic_uninstalled_package\nraise RuntimeError('Source must never execute')\n",
            },
            0,
        ),
        Case(
            "cycle_still_checks_missing_import",
            {
                QUALITY: injected(source[QUALITY], "from scripts.lakehouse import mount_helper"),
                helper: "from . import mount_bridge\n",
                "scripts/lakehouse/mount_bridge.py": "from . import mount_helper\nimport orchestration.pipeline_protocol\n",
            },
            messages=("scripts/lakehouse/mount_bridge.py", "orchestration.pipeline_protocol", "container docker-compose.yaml:quality"),
        ),
        Case(
            "mount_target_is_not_python_path",
            {
                COMPOSE: replaced(
                    source[COMPOSE],
                    "- ./scripts:/workspace/scripts:ro\n      - ./data/analytics/dbt:/workspace/build:ro",
                    "- ./scripts:/workspace/elsewhere:ro\n      - ./data/analytics/dbt:/workspace/build:ro",
                )
            },
            messages=("scripts.lakehouse.silver_quality", "container docker-compose.yaml:quality"),
        ),
        Case(
            "missing_mount",
            {
                COMPOSE: replaced(
                    source[COMPOSE],
                    "- ./scripts:/workspace/scripts:ro\n      - ./data/analytics/dbt:/workspace/build:ro",
                    "- ./data/analytics/dbt:/workspace/build:ro",
                )
            },
            messages=("scripts.lakehouse.silver_quality", "container docker-compose.yaml:quality"),
        ),
        Case(
            "new_launcher_job",
            {
                LAUNCHER: replaced(
                    source[LAUNCHER],
                    '    "pins": {',
                    '    "synthetic_job": {"template": "spark_e2e", "module": "orchestration.pipeline_protocol", '
                    '"writes": (), "aws": False, "grammar": {}},\n    "pins": {',
                )
            },
            messages=("orchestration.pipeline_protocol", "container launcher:spark/synthetic_job"),
        ),
        Case(
            "renamed_lineage_package",
            {"services/lineage/provider_events.py": injected(source["services/lineage/provider_events.py"], "import orchestration.graph")},
            messages=(
                "services/lineage/provider_events.py",
                "orchestration.graph",
                "container services/orchestration/docker-compose.orchestration.yaml:airflow-scheduler",
            ),
        ),
        Case(
            "baked_ui_entry_point",
            {"services/analytics/ui_relay.py": injected(source["services/analytics/ui_relay.py"], "import orchestration.pipeline_protocol")},
            messages=("services/analytics/ui_relay.py", "orchestration.pipeline_protocol", "container docker-compose.yaml:analytics-ui"),
        ),
        Case(
            "baked_script_missing_sibling",
            {
                "services/analytics/ui_relay.py": injected(source["services/analytics/ui_relay.py"], "import mount_helper"),
                "services/analytics/mount_helper.py": "VALUE = 1\n",
            },
            messages=("services/analytics/ui_relay.py", "mount_helper", "container docker-compose.yaml:analytics-ui"),
        ),
        Case(
            "unparseable_imported_source",
            {QUALITY: injected(source[QUALITY], "from scripts.lakehouse import mount_helper"), helper: "def broken(:\n"},
            messages=(helper, "syntax"),
        ),
        Case(
            "harness_hook_deadline_stops_its_process_group",
            {
                CHECKER: replaced(
                    source[CHECKER],
                    "findings, count = check(args.root)",
                    "import time\n        Path('deadline_started').write_text('started')\n        time.sleep(60)\n        findings, count = check(args.root)",
                )
            },
            124,
            ("deadline exceeded",),
            5,
        ),
    ]

    def service(spec: str) -> str:
        return replaced(source[COMPOSE], "services:\n", "services:\n  synthetic_python:\n" + spec)

    declaration = "container docker-compose.yaml:synthetic_python"
    launch_quality = "container launcher:quality_validate"
    module_service = "    image: synthetic:1\n    command: [python, -m, scripts.lakehouse.silver_quality]\n    volumes: [./scripts:/workspace/scripts:ro]\n"
    cases.extend(
        [
            Case(
                "shell_wrapped_python_command",
                {COMPOSE: service("    image: synthetic:1\n    entrypoint: [sh, -c, 'python -m scripts.lakehouse.silver_quality']\n")},
                messages=("docker-compose.yaml:1:", declaration, "Python entry point cannot be resolved"),
            ),
            Case(
                "compose_working_folder_unresolved",
                {COMPOSE: service(module_service)},
                messages=("docker-compose.yaml:1:", declaration, "working folder cannot be resolved"),
            ),
            Case(
                "explicit_pythonpath_without_working_folder",
                {COMPOSE: service(module_service + "    environment: {PYTHONPATH: /workspace}\n")},
                0,
            ),
            Case(
                "script_does_not_gain_working_folder",
                {
                    COMPOSE: service(
                        "    image: synthetic:1\n    working_dir: /workspace\n    command: [python, scripts/mount_fixture.py]\n"
                        "    volumes: [./scripts:/workspace/scripts:ro, ./orchestration:/workspace/orchestration:ro]\n"
                    ),
                    "scripts/mount_fixture.py": "import orchestration.pipeline_protocol\n",
                },
                messages=("scripts/mount_fixture.py:1:", "orchestration.pipeline_protocol", declaration),
            ),
            Case(
                "script_explicit_pythonpath",
                {
                    COMPOSE: service(
                        "    image: synthetic:1\n    working_dir: /workspace\n    command: [python, scripts/mount_fixture.py]\n"
                        "    environment: {PYTHONPATH: /workspace}\n"
                        "    volumes: [./scripts:/workspace/scripts:ro, ./orchestration:/workspace/orchestration:ro]\n"
                    ),
                    "scripts/mount_fixture.py": "import orchestration.pipeline_protocol\n",
                },
                0,
            ),
            Case(
                "relative_script_working_folder_unresolved",
                {
                    COMPOSE: service(
                        "    image: synthetic:1\n    command: [python, scripts/mount_fixture.py]\n"
                        "    environment: {PYTHONPATH: /workspace}\n    volumes: [./scripts:/workspace/scripts:ro]\n"
                    ),
                    "scripts/mount_fixture.py": "VALUE = 1\n",
                },
                messages=("docker-compose.yaml:1:", declaration, "working folder cannot be resolved"),
            ),
            Case(
                "launcher_changed_working_folder",
                {
                    LAUNCHER: replaced(
                        source[LAUNCHER],
                        '[*mounts, *extra], int(inp.values["JOB_MEMORY_LIMIT"]), "none", workdir="/workspace"\n',
                        '[*mounts, *extra], int(inp.values["JOB_MEMORY_LIMIT"]), "none", workdir="/unmounted"\n',
                    )
                },
                messages=("scripts.lakehouse.silver_quality", launch_quality),
            ),
            Case(
                "launcher_working_folder_unresolved",
                {
                    LAUNCHER: replaced(
                        source[LAUNCHER],
                        '[*mounts, *extra], int(inp.values["JOB_MEMORY_LIMIT"]), "none", workdir="/workspace"\n',
                        '[*mounts, *extra], int(inp.values["JOB_MEMORY_LIMIT"]), "none", workdir=inp.params["folder"]\n',
                    )
                },
                messages=(LAUNCHER + ":", "working folder cannot be resolved"),
            ),
            Case(
                "unclassified_compose_python_mount",
                {
                    COMPOSE: service("    image: synthetic:1\n    volumes: [./plugins:/opt/airflow/plugins:ro]\n"),
                    "plugins/synthetic_plugin.py": "import orchestration.pipeline_protocol\n",
                },
                messages=("docker-compose.yaml:1:", declaration, "unclassified Python mount", "plugins"),
            ),
            Case(
                "unclassified_launcher_python_mount",
                {
                    LAUNCHER: replaced(
                        source[LAUNCHER],
                        '        entrypoint = ["python", "-m", module]\n',
                        '        mounts.append(Mount(str(self.checkout / "plugins"), "/workspace/plugins", True))\n'
                        '        entrypoint = ["python", "-m", module]\n',
                    ),
                    "plugins/synthetic_plugin.py": "VALUE = 1\n",
                },
                messages=(LAUNCHER + ":", launch_quality, "unclassified Python mount", "plugins"),
            ),
            Case(
                "renamed_mount_helper",
                {LAUNCHER: source[LAUNCHER].replace("spark_mounts", "python_source_mounts")},
                0,
            ),
            Case(
                "pipeline_mount_edit",
                {
                    PIPELINE: replaced(
                        source[PIPELINE],
                        '        (str(checkout / "orchestration"), "/workspace/orchestration", True),\n'
                        '        (str(checkout / "data_contracts/lakehouse"), "/workspace/data_contracts/lakehouse", True),\n',
                        '        (str(checkout / "data_contracts/lakehouse"), "/workspace/data_contracts/lakehouse", True),\n',
                    )
                },
                messages=("orchestration.pipeline_protocol", "container launcher:pipeline/probe_spec"),
            ),
            Case(
                "new_gold_job",
                {
                    LAUNCHER: replaced(
                        source[LAUNCHER],
                        "GOLD_JOBS: dict[str, dict[str, Any]] = {\n",
                        "GOLD_JOBS: dict[str, dict[str, Any]] = {\n"
                        '    "synthetic_gold": {"module": "orchestration.pipeline_protocol", "step": None, "principal": "writer", "sealed": False},\n',
                    )
                },
                messages=("orchestration.pipeline_protocol", "container launcher:spark/synthetic_gold"),
            ),
            Case(
                "unreferenced_entry_function",
                {QUALITY: source[QUALITY] + "\ndef synthetic_unused():\n    import orchestration.pipeline_protocol\n"},
                messages=(QUALITY, "orchestration.pipeline_protocol", "container docker-compose.yaml:quality"),
            ),
            Case(
                "class_method_import",
                {
                    QUALITY: injected(source[QUALITY], "from scripts.lakehouse.mount_helper import Runner\nRunner().invoke()"),
                    helper: "class Runner:\n    def invoke(self):\n        import orchestration.pipeline_protocol\n",
                },
                messages=(helper, "orchestration.pipeline_protocol", "container docker-compose.yaml:quality"),
            ),
            Case(
                "type_checking_imports",
                {
                    QUALITY: injected(
                        source[QUALITY],
                        "from typing import TYPE_CHECKING\nimport typing as t\n"
                        "if TYPE_CHECKING:\n    import orchestration.pipeline_protocol\n"
                        "def synthetic_types():\n    if t.TYPE_CHECKING:\n        import orchestration.pipeline_protocol\n",
                    )
                },
                0,
            ),
            Case(
                "type_checking_else_still_runs",
                {
                    QUALITY: injected(
                        source[QUALITY], "from typing import TYPE_CHECKING\nif TYPE_CHECKING:\n    pass\nelse:\n    import orchestration.pipeline_protocol"
                    )
                },
                messages=(QUALITY, "orchestration.pipeline_protocol", "container docker-compose.yaml:quality"),
            ),
            Case(
                "guarded_import_still_fails",
                {QUALITY: injected(source[QUALITY], "try:\n    import orchestration.pipeline_protocol\nexcept ImportError:\n    pass")},
                messages=(QUALITY, "orchestration.pipeline_protocol", "container docker-compose.yaml:quality"),
            ),
            Case(
                "grouped_import_diagnostic",
                {RUNTIME: runtime_import, "orchestration/training_contract.py": "VALUE = 'synthetic'\n"},
                messages=(RUNTIME, "orchestration.training_contract", "airflow-scheduler", "airflow-apiserver"),
                counts=((RUNTIME + ":", 1),),
            ),
            Case(
                "removed_warn_option",
                {
                    ".pre-commit-config.yaml": replaced(
                        source[".pre-commit-config.yaml"],
                        "entry: .venv/bin/python -m scripts.quality.container_mounts\n",
                        "entry: .venv/bin/python -m scripts.quality.container_mounts --warn\n",
                    )
                },
                messages=("unrecognized arguments: --warn",),
            ),
            Case(
                "bronze_writer_missing_acquisition_import",
                {
                    COMPOSE: service(
                        "    image: synthetic:1\n    working_dir: /workspace\n"
                        "    command: [python, -m, scripts.lakehouse.mount_writer]\n"
                        "    volumes: [./scripts/lakehouse:/workspace/scripts/lakehouse:ro]\n"
                    ),
                    "scripts/lakehouse/mount_writer.py": "from scripts.acquisition import synthetic_source\n",
                    "scripts/acquisition/synthetic_source.py": "VALUE = 1\n",
                },
                messages=("scripts/lakehouse/mount_writer.py:1:", "scripts.acquisition", declaration),
            ),
            Case(
                "long_checkout_variable_bind",
                {
                    COMPOSE: replaced(
                        source[COMPOSE],
                        "- ./scripts:/workspace/scripts:ro\n      - ./data/analytics/dbt:/workspace/build:ro",
                        "- type: bind\n        source: ${SYNTHETIC_REPO}/scripts\n        target: /workspace/scripts\n        read_only: true\n"
                        "      - ./data/analytics/dbt:/workspace/build:ro",
                    )
                },
                0,
            ),
        ]
    )
    return cases


def remaining(deadline: float, cap: float) -> float:
    seconds = min(cap, deadline - time.monotonic())
    if seconds <= 0:
        raise TimeoutError(f"The container import run exceeded its {RUN_SECONDS}-second deadline")
    return seconds


def snapshot(deadline: float) -> dict[str, str]:
    listed = run_command("git", ["ls-files", "-z", "--cached", "--others", "--exclude-standard"], cwd=ROOT, timeout=remaining(deadline, 30), check=True).stdout
    files = {}
    for name in sorted(set(listed.split("\0")) - {""}):
        path = Path(name)
        if path.parts[0] in {"data", "plans", ".venv", ".tools", ".git", ".aws", ".codex", ".agents"}:
            continue
        if path.suffix in {".py", ".sh"} or path.name == "Dockerfile" or "compose" in path.name and path.suffix in {".yaml", ".yml"}:
            if (ROOT / path).is_symlink():
                raise ValueError(f"{name}: source symlinks are not supported")
            files[name] = (ROOT / path).read_text(encoding="utf-8")
    files[".pre-commit-config.yaml"] = (ROOT / ".pre-commit-config.yaml").read_text(encoding="utf-8")
    # The terraform_checks body copies these files into every stack it checks.
    for name in ("config/quality/ci_groups.json", "config/lakehouse/bronze_tables.json", "config/lakehouse/aws_bronze_grants.json"):
        files[name] = (ROOT / name).read_text(encoding="utf-8")
    files[".gitignore"] = ".venv/\n.cache/\n"
    return files


def fixture(files: dict[str, str], folder: Path, workspace: Path, deadline: float) -> dict[str, str]:
    folder.mkdir()
    for name, content in files.items():
        path = folder / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
    (folder / ".venv").symlink_to(ROOT / ".venv", target_is_directory=True)
    environment = {key: value for key, value in os.environ.items() if not key.startswith(("GIT_", "AWS_", "PRE_COMMIT_")) and key != "PYTHONPATH"}
    environment.update(PYTHONDONTWRITEBYTECODE="1", PRE_COMMIT_HOME=str(workspace / "pre_commit_cache"))
    run_command("git", ["init", "--quiet", str(folder)], env=environment, timeout=remaining(deadline, 15), check=True)
    run_command("git", ["add", "--", *sorted(files)], cwd=folder, env=environment, timeout=remaining(deadline, 15), check=True)
    return environment


def bounded(program: str, arguments: list[str], folder: Path, environment: dict[str, str], deadline: float, seconds: float = 30) -> CompletedProcess[str]:
    """Run a fixture command under the repository's process-group deadline entry point."""
    seconds = min(seconds, deadline - time.monotonic() - 70)
    if seconds < 4:
        raise TimeoutError(f"The container import cases exhausted their {RUN_SECONDS}-second deadline, including process-group cleanup")
    child_environment = environment | {"HAI_DEADLINE_UTC": str(time.time() + seconds + 2)}
    result = run_command(
        str(PYTHON),
        [str(ROOT / "services/hai/deadline_run.py"), program, *arguments],
        cwd=folder,
        env=child_environment,
        timeout=seconds + 70,
    )
    if result.returncode == 123:
        raise ChildProcessError("The fixture command's process group did not stop: " + result.stderr)
    return result


def run_case(case: Case, source: dict[str, str], workspace: Path, deadline: float) -> dict[str, object]:
    folder = workspace / case.name
    environment = fixture(source | case.files, folder, workspace, deadline)
    arguments = ["-m", "pre_commit", "run", "container-mounts", "--all-files", "--verbose"]
    result = bounded(str(PYTHON), arguments, folder, environment, deadline, case.seconds)
    output = result.stdout + result.stderr
    deadline_started = (folder / "deadline_started").is_file()
    if result.returncode == 124:
        output += "hook deadline exceeded; the deadline wrapper confirmed process-group cleanup\n"
    return {
        "name": case.name,
        "expected_exit": case.exit_code,
        "observed_exit": result.returncode,
        "expected_messages": case.messages,
        "expected_counts": case.counts,
        "kind": "harness" if case.name.startswith("harness_") else "checker",
        "passed": result.returncode == case.exit_code
        and all(message in output for message in case.messages)
        and all(output.count(fragment) == count for fragment, count in case.counts)
        and (case.exit_code != 124 or deadline_started),
        "command": ".venv/bin/python " + " ".join(arguments),
        "output": output,
        "fixture_changes": case.files,
        "process_group_cleanup": "confirmed by the deadline entry point",
        "deadline_command_started": deadline_started,
    }


def ci_case(source: dict[str, str], workspace: Path, deadline: float, name: str = "ci_selection_and_dispatch") -> dict[str, object]:
    folder = workspace / name
    files = dict(source)
    files["scripts/quality/timing.sh"] = (
        'timed() { label="$1"; shift; printf "COMMAND %s: %s\\n" "$label" "$*"; case "$1" in run_item|run_group) "$@" ;; esac; }\ntiming_table() { :; }\n'
    )
    environment = fixture(files, folder, workspace, deadline)
    selected = {}
    paths = [COMPOSE, "services/orchestration/docker-compose.orchestration.yaml", "services/lakehouse/spark/Dockerfile", "config/example.compose.yaml"]
    for path in paths:
        result = run_command(
            str(PYTHON),
            ["-m", "scripts.quality.ci_groups", "plan", "--scope", "paths", "--part", "core", "--path", path],
            cwd=folder,
            env=environment,
            timeout=remaining(deadline, 15),
        )
        selected[path] = result.returncode == 0 and "repository_code_checks" in json.loads(result.stdout)["run"]
    environment.update(CI_PART="core", CI_SCOPE="full", PYTHON_BIN=str(PYTHON))
    result = bounded("/bin/bash", ["scripts/run_ci.sh"], folder, environment, deadline)
    output = result.stdout + result.stderr
    commands = ("-m scripts.quality.container_mounts", "-m scripts.quality.run_container_mounts_e2e")
    dispatched = {command: output.count(command) == 1 for command in commands}
    return {
        "name": name,
        "passed": result.returncode == 0 and all(selected.values()) and all(dispatched.values()),
        "observed_exit": result.returncode,
        "path_selection": selected,
        "dispatched_once": dispatched,
        "output": output,
        "limits": "Real dispatcher and group bodies, with a timing wrapper that records leaf commands instead of running them.",
    }


def checker_mutations() -> list[tuple[str, str, str, str, str]]:
    """Pair each weakened checker with the fixture that must expose its false pass."""
    imports = "scripts/quality/container_imports.py"
    return [
        (
            "pipeline_entries_ignored",
            CHECKER,
            "for method, function in self.functions[PIPELINE].items():",
            "for method, function in ():",
            "pipeline_mount_edit",
        ),
        (
            "gold_jobs_ignored",
            CHECKER,
            '(("SPARK_JOBS", "render_spark", "spark"), ("GOLD_JOBS", "render_gold_publish", "spark"))',
            '(("SPARK_JOBS", "render_spark", "spark"),)',
            "new_gold_job",
        ),
        (
            "no_function_scan_on_entry",
            imports,
            "if entry:\n                self.pending.extend((name, definition, False) for definition in definitions)",
            "if entry:\n                pass",
            "unreferenced_entry_function",
        ),
        ("no_class_scan", imports, "ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef)", "ast.FunctionDef | ast.AsyncFunctionDef)", "class_method_import"),
        ("no_container_identity", CHECKER, "{container.name}", "unrelated", "past_quality_engine"),
    ]


def mutant_cases(source: dict[str, str], workspace: Path, deadline: float, cases: list[Case]) -> list[dict[str, object]]:
    by_name = {case.name: case for case in cases}
    imports = "scripts/quality/container_imports.py"
    changes = [
        ("checker_does_nothing", CHECKER, "findings, count = check(args.root)", "findings, count = [], 0", "past_training_contract"),
        ("no_transitive_imports", imports, "self.scan(*task)", "pass", "past_quality_engine"),
        ("no_package_initializers", imports, "self.pending.append((package, None, False))", "pass", "package_initializer"),
        ("no_referenced_functions", imports, "self.pending.append((module, parts[length], False))", "pass", "referenced_function_import"),
    ]
    results = []
    for name, path, old, new, case_name in changes + checker_mutations():
        baseline = by_name[case_name]
        planted = source | {path: replaced(source[path], old, new)}
        row = run_case(Case(name, baseline.files, baseline.exit_code, baseline.messages), planted, workspace, deadline)
        rejected = not row["passed"] and (
            row["observed_exit"] == 0 or name == "no_container_identity" and row["observed_exit"] == 1 and "container unrelated" in str(row["output"])
        )
        row.update(protected_case=case_name, scenario_rejected_mutant=rejected)
        row["passed"] = row["scenario_rejected_mutant"]
        results.append(row)
    group = "scripts/ci/groups/repository_code_checks.sh"
    old = '  timed "repository_code_checks: container imports" "$python_bin" -m scripts.quality.container_mounts\n'
    row = ci_case(source | {group: replaced(source[group], old, "")}, workspace, deadline, "missing_ci_command")
    row["scenario_rejected_mutant"] = not row["passed"] and row["observed_exit"] == 0
    row["passed"] = row["scenario_rejected_mutant"]
    results.append(row)
    return results


def main() -> int:
    """Run the source-only hook cases and save repeatable pass and failure evidence."""
    deadline = time.monotonic() + RUN_SECONDS
    for variable in tuple(os.environ):
        if variable.startswith("GIT_") or variable in {"PRE_COMMIT_FROM_REF", "PRE_COMMIT_TO_REF"}:
            os.environ.pop(variable, None)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", action="append", help="run only a named case")
    parser.add_argument("--output", type=Path, help="write the run record to this ignored folder")
    parser.add_argument("--mutants", action="store_true", help="also prove the cases reject weakened checker and CI wiring copies")
    args = parser.parse_args()
    started = datetime.now(UTC)
    out = args.output or ROOT / "data/e2e/container_mounts" / started.strftime("%Y%m%dT%H%M%S_%fZ")
    out.mkdir(parents=True, exist_ok=False)
    source = snapshot(deadline)
    selected = scenarios(source)
    all_cases = list(selected)
    if args.case:
        if unknown := set(args.case) - {case.name for case in selected}:
            parser.error("Unknown case: " + ", ".join(sorted(unknown)))
        selected = [case for case in selected if case.name in args.case]
    report: dict[str, object] = {
        "started_at_utc": started.isoformat(),
        "command": ".venv/bin/python -m scripts.quality.run_container_mounts_e2e",
        "revision": run_command("git", ["rev-parse", "HEAD"], cwd=ROOT, timeout=remaining(deadline, 15), check=True).stdout.strip(),
        "source_sha256": {name: hashlib.sha256(text.encode()).hexdigest() for name, text in source.items()},
        "limits": "Static first-party imports and source declarations only. No application code, Docker, AWS, installs or network operations run.",
    }
    results: list[dict[str, object]] = []
    workspace = Path(tempfile.mkdtemp(prefix="container_mounts."))
    cleanup = "done"
    try:
        for case in selected:
            results.append(run_case(case, source, workspace, deadline))
        if not args.case:
            results.append(ci_case(source, workspace, deadline))
        if args.mutants:
            results.extend(mutant_cases(source, workspace, deadline, all_cases))
    except (ChildProcessError, TimeoutExpired) as error:
        cleanup = "unconfirmed"
        report.update(error=f"{type(error).__name__}: {error}", retained_workspace=str(workspace))
    except (OSError, ValueError, TimeoutError) as error:
        report["error"] = f"{type(error).__name__}: {error}"
    finally:
        if cleanup == "done":
            shutil.rmtree(workspace)
    passed = sum(bool(case["passed"]) for case in results)
    total = len(selected) + int(not args.case) + (5 + len(checker_mutations()) if args.mutants else 0)
    try:
        unchanged = snapshot(deadline) == source
    except (OSError, ValueError, TimeoutError, TimeoutExpired) as error:
        unchanged = False
        report["error"] = f"{type(error).__name__}: {error}"
    success = passed == total and unchanged and "error" not in report
    report.update(cases=results, passed=passed, total=total, source_unchanged=unchanged, cleanup=cleanup, status="passed" if success else "failed")
    (out / "report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    (out / "retention.json").write_text(
        json.dumps(
            {
                "class": 5,
                "purpose": "Container import hook regression baseline",
                "created": started.isoformat(),
                "end_condition": "A newer passing run replaces this baseline",
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    sys.stdout.write(f"{passed}/{total} passed. Report: {out / 'report.json'}\n")
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
