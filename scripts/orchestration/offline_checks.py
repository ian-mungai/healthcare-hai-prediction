"""Offline checks of the orchestration code and configuration.

Run ``.venv/bin/python -m scripts.orchestration.offline_checks [--test-registry <folder>/registry] [--no-image]`` from the repository root.
``--no-image`` skips the checks that need the launcher and the built Airflow image. Nothing reaches the network, AWS or the catalog, and the
checks make no Docker call: the image checks are launcher requests (``image``, ``allocate``, an ``orchestration_check`` attempt). Checks:

1. The Compose file pins PostgreSQL by digest and the Airflow image by the ``hai-`` tag of ``services/orchestration/Dockerfile``, publishes only
   loopback ports and mounts no socket, AWS folder or lakehouse secret; the Dockerfile builds on the recorded digests, installs nothing and adds only
   the entrypoint and the two labels; the constraints file has its recorded SHA-256. Each check also refuses a mutated input.
2. The broker templates validate against their schema and the launcher's closed table.
3. The interface-only import check: DAG files import only Airflow, the standard library (never ``subprocess``), ``orchestration.interface`` and
   ``orchestration.client``; the client only the standard library and the interface.
4. The ``transform_dag`` graph validator on plain graphs: the required graph passes and each mutation fails.
5. The Docker-call check over every orchestration source file.
6. With the built image, through the launcher: the image's labels, user and last layers; every DAG file imports in the image with no error or
   cycle; the graph validator on real Airflow DAG objects; every installed distribution equals the constraints file.
7. Ruff over ``dags/``, ``orchestration/`` and ``scripts/orchestration/``; strict MyPy on the orchestration host scope.
   Files needing Airflow or PostgreSQL types are listed as pending in the report and in ``config/quality/mypy_runtimes.json``.
   Check them in their runtime environment with ``scripts.quality.typecheck --runtime orchestration_image`` and both strict flags.

The report is ``data/e2e/orchestration_offline/<UTC>/report.json``.
"""

from __future__ import annotations

import argparse
import ast
import copy
import hashlib
import json
import re
import runpy
import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any
from unittest import mock

from orchestration import launcher
from orchestration.graph import ALL_SUCCESS, TaskNode, reference_graph, validate_transform_graph
from scripts.orchestration.broker import BrokerError, load_templates
from scripts.process import run_command
from scripts.quality.repo_checks import load_yaml

ROOT = Path(__file__).resolve().parents[2]
PLATFORM_FILE = ROOT / "config" / "orchestration" / "platform.json"
CODE = ("dags", "orchestration", "scripts/orchestration")
IN_IMAGE = "scripts/orchestration/in_image_checks.py"
FORBIDDEN_MOUNT_MARKERS = (".aws", "lakehouse/secrets", "${HOME}")
DAG_MODULES = {"orchestration.interface", "orchestration.client"}
NEVER_IN_DAGS = {"subprocess", "docker", "scripts"}
PACKAGE_MANAGERS = re.compile(r"\b(pip|pip3|uv|apt|apt-get|apk|npm|conda|poetry|curl|wget)\b")
A1_FILES = (
    "scripts/orchestration/broker.py",
    "scripts/orchestration/supervisor.py",
    "scripts/orchestration/ledger.py",
    "scripts/orchestration/offline_checks.py",
    "scripts/orchestration/in_image_checks.py",
    "scripts/orchestration/db_dump.py",
    "scripts/orchestration/sentinel.py",
    "scripts/orchestration/fake_engine.py",
    "scripts/orchestration/run_broker_e2e.py",
    "scripts/orchestration/run_stack_e2e.py",
    "scripts/orchestration/run_supervisor_e2e.py",
    "scripts/orchestration/run_real_e2e.py",
    "orchestration/client.py",
    "orchestration/graph.py",
    "dags/broker_probe_dag.py",
)


def compose_problems(text: str, platform: dict[str, Any]) -> list[str]:
    """What the Compose file breaks."""
    data = load_yaml(text)
    if not isinstance(data, dict):
        return ["the Compose file is not a mapping"]
    problems = []
    if data.get("name") != platform["compose"]["project"]:
        problems.append(f"project name {data.get('name')} is not {platform['compose']['project']}")
    services = data.get("services") or {}
    if sorted(services) != sorted([*platform["compose"]["services"], *platform["compose"]["jobs"]]):
        problems.append(f"the services {sorted(services)} are not the recorded ones")
    for name, service in services.items():
        image = str(service.get("image", ""))
        wanted = platform["metadata_database"]["image"] if name == "postgres" else platform["airflow"]["image"]
        if image != wanted:
            problems.append(f"{name}: image {image} is not the pinned {wanted}")
        for volume in service.get("volumes") or []:
            source = str(volume).split(":")[0]
            if source.endswith(".sock") or any(marker in str(volume) for marker in FORBIDDEN_MOUNT_MARKERS):
                problems.append(f"{name}: forbidden mount {volume}")
        for port in service.get("ports") or []:
            if not str(port).startswith("127.0.0.1:"):
                problems.append(f"{name}: port {port} is not bound to loopback")
        if service.get("privileged") or service.get("network_mode") == "host" or service.get("pid") == "host":
            problems.append(f"{name}: privileged, host network or host PID")
        for key in service.get("environment") or {}:
            if str(key).startswith(("AWS_", "POLARIS")):
                problems.append(f"{name}: data credential variable {key}")
        if name != "postgres" and name != "airflow-check" and "ALL" not in (service.get("cap_drop") or []):
            problems.append(f"{name}: capabilities are not dropped")
    return problems


def constraints_problems(data: bytes, platform: dict[str, Any]) -> list[str]:
    """The constraints file must be the recorded release file."""
    found = hashlib.sha256(data).hexdigest()
    wanted = platform["airflow"]["constraints"]["sha256"]
    return [] if found == wanted else [f"constraints SHA-256 {found} is not the recorded {wanted}"]


def dockerfile_problems(text: str, platform: dict[str, Any]) -> list[str]:
    """The thin image is the pinned base plus the entrypoint and the two labels; no package is installed."""
    airflow = platform["airflow"]
    lines = [line.strip() for line in text.splitlines() if line.strip() and not line.strip().startswith("#")]
    froms = [line for line in lines if line.upper().startswith("FROM ")]
    problems = []
    if len(froms) != 2:
        problems.append(f"the Dockerfile has {len(froms)} FROM lines, not 2")
    else:
        if froms[0].split()[1] != airflow["builder_image"]:
            problems.append(f"the builder stage is {froms[0].split()[1]}, not the pinned {airflow['builder_image']}")
        if froms[1].split()[1] != airflow["base_image"]:
            problems.append(f"the final stage is {froms[1].split()[1]}, not the pinned {airflow['base_image']}")
    final = lines[lines.index(froms[-1]) + 1 :] if froms else []
    for line in final:
        keyword = line.split()[0].upper()
        if keyword == "RUN" or PACKAGE_MANAGERS.search(line):
            problems.append(f"the final stage runs or names an installer: {line[:80]}")
        elif keyword not in ("COPY", "LABEL"):
            problems.append(f"the final stage has a {keyword} line; only COPY and LABEL are allowed")
    labels = " ".join(line for line in final if line.upper().startswith("LABEL"))
    for name, value in airflow["labels"].items():
        if f'{name}="{value}"' not in labels:
            problems.append(f"the final stage lacks the label {name}={value}")
    if not any("/opt/hai/deadline_run" in line and line.upper().startswith("COPY") for line in final):
        problems.append("the final stage does not copy the entrypoint to /opt/hai/deadline_run")
    return problems


def imported(source: str) -> set[str]:
    """Every module a Python source imports, absolute names."""
    names = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            names |= {alias.name for alias in node.names}
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            names.add(node.module)
            names |= {f"{node.module}.{alias.name}" for alias in node.names if node.module == "orchestration"}
        elif isinstance(node, ast.ImportFrom):
            names.add("." * node.level + (node.module or ""))
    return names


def dag_import_problems(name: str, source: str) -> list[str]:
    """A DAG file may import Airflow, the standard library and the two interface modules only."""
    problems = []
    for module in sorted(imported(source)):
        root = module.split(".")[0]
        forbidden = module.startswith(".") or root in NEVER_IN_DAGS or (root == "orchestration" and module not in DAG_MODULES | {"orchestration"})
        unknown = root not in sys.stdlib_module_names and root not in ("airflow", "__future__", "orchestration")
        if forbidden or unknown:
            problems.append(f"{name} imports {module}")
    return problems


def interface_problems() -> list[str]:
    """The client imports only the standard library and the interface; the interface only the standard library."""
    problems = []
    for path, allowed in (("orchestration/client.py", {"orchestration.interface"}), ("orchestration/interface.py", set())):
        for module in sorted(imported((ROOT / path).read_text())):
            root = module.split(".")[0]
            if module not in allowed and (root not in sys.stdlib_module_names or root == "subprocess") and root != "__future__":
                problems.append(f"{path} imports {module}")
    return problems


def docker_call_problems() -> list[str]:
    """The repository's Docker-call check over the orchestration source files."""
    present = [name for name in A1_FILES if (ROOT / name).is_file()]
    result = run_command(sys.executable, ["-m", "scripts.quality.docker_calls", *present], cwd=ROOT, timeout=300)
    return [] if result.returncode == 0 else (result.stdout + result.stderr).strip().splitlines()[:10]


def graph_cases() -> dict[str, dict[str, Any]]:
    """Plain-graph cases: the reference passes; each mutation fails with its rule."""
    reference = reference_graph()

    def changed(task: str, **fields: Any) -> dict[str, TaskNode]:
        graph = copy.deepcopy(reference)
        node = graph[task]
        graph[task] = TaskNode(
            **{"upstream": node.upstream, "trigger_rule": node.trigger_rule, "is_setup": node.is_setup, "is_teardown": node.is_teardown, **fields}
        )
        return graph

    def extra(task: str, node: TaskNode) -> dict[str, TaskNode]:
        return {**reference, task: node}

    without = {key: value for key, value in reference.items() if key != "gold_validate"}
    cases: dict[str, tuple[dict[str, TaskNode], str | None]] = {
        "reference": (reference, None),
        "setup task": (changed("acquire_run", is_setup=True), "setup_or_teardown"),
        "teardown task": (changed("release_run", is_teardown=True), "setup_or_teardown"),
        "second leaf": (extra("notify", TaskNode(frozenset({"final_gate"}), "all_done")), "leaf"),
        "cycle": (changed("silver_build", upstream=frozenset({"pin_bronze", "gold_build"})), "cycle"),
        "missing task": (without, "missing_task"),
        "unknown upstream": (changed("gold_build", upstream=frozenset({"silver_validate", "ghost"})), "unknown_upstream"),
        "run_record all_success": (changed("run_record", trigger_rule=ALL_SUCCESS), "trigger_rule"),
        "catalog_release all_success": (changed("catalog_release", trigger_rule=ALL_SUCCESS), "trigger_rule"),
        "final_gate all_success": (changed("final_gate", trigger_rule=ALL_SUCCESS), "trigger_rule"),
        "release_run all_success": (changed("release_run", trigger_rule=ALL_SUCCESS), "trigger_rule"),
        "work task all_done": (changed("gold_build", trigger_rule="all_done"), "trigger_rule"),
        "final_gate missing an edge": (changed("final_gate", upstream=reference["final_gate"].upstream - {"silver_build"}), "upstream"),
        "release_run with an extra upstream": (changed("release_run", upstream=frozenset({"final_gate", "catalog_release"})), "upstream"),
        "work order broken": (changed("gold_build", upstream=frozenset({"silver_build"})), "upstream"),
    }
    results = {}
    for name, (graph, rule) in cases.items():
        rules = sorted({violation.rule for violation in validate_transform_graph(graph)})
        results[name] = {"expected": rule, "rules": rules, "ok": rules == [] if rule is None else rule in rules}
    return results


def mutation_cases(platform: dict[str, Any]) -> dict[str, bool]:
    """Each input check refuses its mutated input."""
    compose = (ROOT / platform["compose"]["file"]).read_text()
    digest = platform["airflow"]["base_image"].split("@sha256:")[1]
    postgres_digest = platform["metadata_database"]["image"].split("@sha256:")[1]
    dockerfile = (ROOT / platform["airflow"]["dockerfile"]).read_text()
    constraints = (ROOT / platform["airflow"]["constraints"]["path"]).read_bytes()
    dag = (ROOT / "dags" / "broker_probe_dag.py").read_text()
    volume = "    - ${HAI_ORCH_REPO}/dags:/opt/airflow/dags:ro\n"
    return {
        "compose with another PostgreSQL digest refused": bool(compose_problems(compose.replace(postgres_digest, "f" * 64), platform)),
        "compose with an unpinned PostgreSQL tag refused": bool(compose_problems(compose.replace(f"@sha256:{postgres_digest}", ""), platform)),
        "compose with another Airflow image refused": bool(
            compose_problems(compose.replace("image: hai-airflow:3.3.2-python3.12", "image: apache/airflow:latest", 1), platform)
        ),
        "compose with a socket mount refused": bool(
            compose_problems(compose.replace(volume, f"{volume}    - /var/run/engine.sock:/var/run/engine.sock\n", 1), platform)
        ),
        "compose with a port on every interface refused": bool(
            compose_problems(compose.replace('"127.0.0.1:${HAI_ORCH_API_PORT', '"${HAI_ORCH_API_PORT'), platform)
        ),
        "compose with a data credential variable refused": bool(
            compose_problems(compose.replace("PYTHONPATH: /opt/hai", "PYTHONPATH: /opt/hai\n    AWS_PROFILE: x", 1), platform)
        ),
        "compose with an AWS folder mount refused": bool(
            compose_problems(compose.replace(volume, f"{volume}    - ${{HAI_ORCH_STATE}}/.aws:/opt/aws:ro\n", 1), platform)
        ),
        "a changed constraints line refused": bool(constraints_problems(constraints.replace(b"jsonschema==4.26.0", b"jsonschema==4.25.0"), platform)),
        "Dockerfile with another base digest refused": bool(dockerfile_problems(dockerfile.replace(digest, "f" * 64), platform)),
        "Dockerfile with a pip install refused": bool(dockerfile_problems(dockerfile + "RUN pip install requests\n", platform)),
        "Dockerfile with an apt install refused": bool(dockerfile_problems(dockerfile + "RUN apt-get install -y curl\n", platform)),
        "Dockerfile without the labels refused": bool(
            dockerfile_problems(dockerfile.replace("LABEL hai.deadline_entrypoint", "# LABEL hai.deadline_entrypoint"), platform)
        ),
        "Dockerfile with a USER line refused": bool(dockerfile_problems(dockerfile + "USER root\n", platform)),
        "DAG importing scripts.process refused": bool(dag_import_problems("x.py", dag + "\nimport scripts.process\n")),
        "DAG importing orchestration.fake refused": bool(dag_import_problems("x.py", dag + "\nfrom orchestration.fake import FakeSubmissions\n")),
        "DAG importing the broker refused": bool(dag_import_problems("x.py", dag + "\nfrom scripts.orchestration import broker\n")),
        "DAG importing the launcher refused": bool(dag_import_problems("x.py", dag + "\nfrom orchestration import launcher\n")),
        "DAG importing docker refused": bool(dag_import_problems("x.py", dag + "\nimport docker\n")),
        "DAG importing subprocess refused": bool(dag_import_problems("x.py", dag + "\nimport subprocess\n")),
    }


def distribution_cases() -> dict[str, dict[str, Any]]:
    """The in-image distribution comparison on synthetic installed metadata: Airflow's own imports are stubbed (they exist only in the image), the
    constraints file and the release pins are temporary files. Every installed distribution must be in the constraints file at its version or in the exact
    release set at its own pin; nothing is exempt by name prefix."""
    stubs: dict[str, ModuleType] = {}
    for name, attribute in (
        ("airflow.dag_processing.dagbag", "DagBag"),
        ("airflow.providers.standard.operators.empty", "EmptyOperator"),
        ("airflow.sdk", "DAG"),
        ("airflow.sdk.bases.operator", "BaseOperator"),
    ):
        module = ModuleType(name)
        setattr(module, attribute, object)
        stubs[name] = module
    with mock.patch.dict(sys.modules, stubs):
        namespace = runpy.run_path(str(ROOT / IN_IMAGE), run_name="distribution_check")
    function = namespace["constraints"]
    release = {"apache-airflow": "3.3.2", "apache-airflow-core": "3.3.2", "apache-airflow-task-sdk": "1.3.2"}
    with tempfile.TemporaryDirectory(prefix="a1_dist_") as folder:
        constraints_file = Path(folder) / "constraints.txt"
        constraints_file.write_text("jsonschema==4.26.0\napache-airflow-providers-standard==1.19.0\n")
        platform_file = Path(folder) / "platform.json"
        platform_file.write_text(json.dumps({"airflow": {"release_packages": release}}))
        function.__globals__["CONSTRAINTS"] = constraints_file
        function.__globals__["RELEASE_FILE"] = platform_file

        def run(installed: dict[str, str]) -> dict[str, Any]:
            records = [SimpleNamespace(metadata={"Name": name}, version=version) for name, version in installed.items()]
            with mock.patch.object(namespace["metadata"], "distributions", return_value=records):
                found: dict[str, Any] = function({})
            return found

        good = {**release, "jsonschema": "4.26.0", "apache-airflow-providers-standard": "1.19.0"}
        control = run(good)
        extra = run({**good, "ordinary-unreviewed": "9.9.9", "apache-airflow-unreviewed": "9.9.9", "apache-airflow-providers-extra": "1.0.0"})
        moved = run({**good, "apache-airflow-core": "3.3.1", "jsonschema": "4.25.0"})
        absent = run({name: version for name, version in good.items() if name != "apache-airflow-task-sdk"})
        renamed = run({**good, "apache-airflow-task-sdk-extra": "1.3.2"})
    cases = {
        "a tree equal to its pins passes (control)": (
            not control.get("unpinned") and not control.get("differing") and not control.get("release_problems"),
            {"unpinned": control.get("unpinned"), "release_problems": control.get("release_problems")},
        ),
        "an unpinned ordinary distribution is reported": ("ordinary-unreviewed" in extra.get("unpinned", []), {"unpinned": extra.get("unpinned")}),
        "an unpinned apache-airflow-* distribution is reported": (
            "apache-airflow-unreviewed" in extra.get("unpinned", []),
            {"unpinned": extra.get("unpinned")},
        ),
        "a provider absent from the constraints file is reported": (
            "apache-airflow-providers-extra" in extra.get("unpinned", []),
            {"unpinned": extra.get("unpinned")},
        ),
        "a name that only starts like a release package is reported": (
            "apache-airflow-task-sdk-extra" in renamed.get("unpinned", []),
            {"unpinned": renamed.get("unpinned")},
        ),
        "a release package at another version than its own pin is reported": (
            any("apache-airflow-core" in problem for problem in moved.get("release_problems", [])),
            {"release_problems": moved.get("release_problems")},
        ),
        "a pinned distribution at another version than the constraints file is reported": (
            "jsonschema" in moved.get("differing", {}),
            {"differing": moved.get("differing")},
        ),
        "a release package missing from the image is reported": (
            any("apache-airflow-task-sdk" in problem for problem in absent.get("release_problems", [])),
            {"release_problems": absent.get("release_problems")},
        ),
    }
    return {name: {"ok": bool(ok), **evidence} for name, (ok, evidence) in cases.items()}


def lint() -> dict[str, dict[str, Any]]:
    """Ruff and MyPy over the orchestration code."""
    runs = {
        "ruff format": [sys.executable, "-m", "ruff", "format", "--check", *CODE],
        "ruff check": [sys.executable, "-m", "ruff", "check", *CODE],
        "mypy orchestration host scope": [
            sys.executable,
            "-m",
            "scripts.quality.typecheck",
            "--runtime",
            "orchestration",
            "--check-untyped-defs",
            "--disallow-untyped-defs",
        ],
    }
    results = {}
    for name, command in runs.items():
        result = run_command(command[0], command[1:], cwd=ROOT, timeout=900)
        results[name] = {"ok": result.returncode == 0, "output": (result.stdout + result.stderr).strip().splitlines()[-5:]}
    return results


def in_image(platform: dict[str, Any], test_registry: str | None) -> dict[str, Any]:
    """The checks that need the built image, as launcher requests: its description, then the finite in-image check on no network."""
    client = launcher.load_client(test_registry)
    try:
        description = client.call("image", {"service": "airflow-check", "compose": "orchestration", "history": True, "checkout": str(ROOT)}, timeout=300)
    except launcher.ClientError as error:
        return {"error": f"the Airflow image is not available: {error}"}
    allocation = client.allocate()
    try:
        principal = client.principal_alloc(allocation)
        result = client.run_attempt(principal, "orchestration_check", "offline-checks", [], {}, checkout=ROOT, seconds=600)
    except launcher.ClientError as error:
        return {"error": f"the in-image check could not run: {error}", "image": description}
    finally:
        client.release_allocation(allocation)
    lines = [line for line in result.stdout.splitlines() if line.startswith("IN_IMAGE ")]
    if not result.ok or not lines:
        return {"error": (result.stderr or result.stdout)[-1000:], "reason": result.reason, "exit_code": result.exit_code, "image": description}
    loaded: dict[str, Any] = json.loads(lines[-1].removeprefix("IN_IMAGE "))
    loaded["image"] = description
    return loaded


def image_problems(description: dict[str, Any], platform: dict[str, Any]) -> list[str]:
    """The built image carries the labels, runs as the official image's user and adds only the entrypoint copy and the labels on top of the base."""
    config = description["inspect"].get("Config") or {}
    problems = []
    for name, value in platform["airflow"]["labels"].items():
        if (config.get("Labels") or {}).get(name) != value:
            problems.append(f"label {name} is {(config.get('Labels') or {}).get(name)!r}, not {value!r}")
    if config.get("User") != platform["airflow"]["user"]:
        problems.append(f"the image user is {config.get('User')!r}, not the official image's {platform['airflow']['user']!r}")
    history = description.get("history") or []
    added = [str(layer.get("CreatedBy", "")) for layer in history[:2]]
    if len(history) < 3 or not any("deadline_run" in entry for entry in added):
        problems.append(f"the top layers are not the entrypoint copy and the labels: {added}")
    for entry in added:
        if PACKAGE_MANAGERS.search(entry):
            problems.append(f"a top layer runs an installer: {entry[:80]}")
    return problems


SECTIONS = ("inputs", "mutations", "graph", "distribution", "image", "lint")


def collect(platform: dict[str, Any], sections: set[str], test_registry: str | None) -> dict[str, Any]:
    """The results of the chosen sections, by check name."""
    results: dict[str, Any] = {}
    if "inputs" in sections:
        compose = compose_problems((ROOT / platform["compose"]["file"]).read_text(), platform)
        results["compose pins images, loopback ports and no forbidden mount"] = {"ok": not compose, "problems": compose}
        docker = dockerfile_problems((ROOT / platform["airflow"]["dockerfile"]).read_text(), platform)
        results["Dockerfile builds the pinned base plus the entrypoint and the labels, nothing installed"] = {"ok": not docker, "problems": docker}
        constraints = constraints_problems((ROOT / platform["airflow"]["constraints"]["path"]).read_bytes(), platform)
        results["constraints file is the recorded release file"] = {"ok": not constraints, "problems": constraints}
        try:
            templates = load_templates()
            results["broker templates validate against their schema and the launcher's table"] = {"ok": True, "templates": sorted(templates["templates"])}
        except BrokerError as error:
            results["broker templates validate against their schema and the launcher's table"] = {"ok": False, "problems": [str(error)]}
        dag_files = sorted((ROOT / "dags").glob("*.py"))
        imports = [problem for path in dag_files for problem in dag_import_problems(path.name, path.read_text())] + interface_problems()
        results["interface-only imports"] = {"ok": not imports, "problems": imports}
        docker_calls = docker_call_problems()
        results["no A1 file calls Docker"] = {"ok": not docker_calls, "problems": docker_calls}
    if "mutations" in sections:
        results.update({f"mutation: {name}": {"ok": ok} for name, ok in mutation_cases(platform).items()})
    if "graph" in sections:
        results.update({f"graph (plain): {name}": case for name, case in graph_cases().items()})
    if "distribution" in sections:
        results.update({f"distribution check: {name}": case for name, case in distribution_cases().items()})
    if "image" in sections:
        image = in_image(platform, test_registry)
        if "error" in image:
            results["in-image checks ran"] = {"ok": False, "error": image["error"]}
        else:
            found = image_problems(image["image"], platform)
            results["image labels, user and top layers"] = {"ok": not found, "problems": found}
            dags = image["dags"]
            results["every DAG file imports in the image with no error or cycle"] = {"ok": not dags["import_errors"] and bool(dags["dag_ids"]), **dags}
            results.update({f"graph (Airflow objects): {name}": case for name, case in image["graph_cases"].items()})
            pins = image["constraints"]
            tools_ok = all(tool["ok"] for tool in pins["build_tools"].values())
            release_ok = not pins.get("release_problems") and set(pins.get("release", {})) == set(platform["airflow"]["release_packages"])
            results["image packages equal the constraints file and the release pins"] = {
                "ok": not pins["differing"] and not pins["unpinned"] and tools_ok and release_ok,
                **pins,
            }
    if "lint" in sections:
        results.update(lint())
    return results


def main() -> int:
    """Run every check and write the report; 1 when any fails."""
    cli = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    cli.add_argument("--test-registry", help="a test registry folder named registry, for the checks that use the launcher")
    cli.add_argument("--no-image", action="store_true", help="skip the checks that need the launcher and the built image")
    cli.add_argument("--only", action="append", choices=SECTIONS, default=[], help="run only these sections (the failing-first logs use it)")
    args = cli.parse_args()
    started = datetime.now(UTC)
    platform = json.loads(PLATFORM_FILE.read_text())
    sections = set(args.only or SECTIONS) - ({"image"} if args.no_image else set())
    results = collect(platform, sections, args.test_registry)
    passed = bool(results) and all(result["ok"] for result in results.values())
    for name, result in results.items():
        sys.stdout.write(f"{'ok' if result['ok'] else 'FAIL':<5} {name}\n")
        if not result["ok"]:
            sys.stdout.write(f"      evidence: {json.dumps({key: value for key, value in result.items() if key != 'ok'}, default=str)[:400]}\n")
    folder = ROOT / "data" / "e2e" / "orchestration_offline" / started.strftime("%Y%m%dT%H%M%SZ")
    folder.mkdir(parents=True)
    report = {
        "feature": "orchestration offline checks",
        "started_utc": started.isoformat(),
        "status": "pass" if passed else "fail",
        "commit": run_command("git", ["rev-parse", "HEAD"], cwd=ROOT).stdout.strip(),
        "command": ".venv/bin/python -m scripts.orchestration.offline_checks"
        + (" --no-image" if args.no_image else "")
        + "".join(f" --only {s}" for s in args.only),
        "platform_sha256": hashlib.sha256(PLATFORM_FILE.read_bytes()).hexdigest(),
        "sections": sorted(sections),
        "results": results,
        "pending_typechecks": {
            path: entry
            for path, entry in json.loads((ROOT / "config/quality/mypy_runtimes.json").read_text())["pending"].items()
            if entry["runtime"] == "orchestration_image"
        },
        "limits": [
            "Static and in-image checks; no DAG run, broker launch or Airflow service starts here (see run_broker_e2e, run_stack_e2e and run_real_e2e)."
        ],
    }
    (folder / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    sys.stdout.write(f"pending: {len(report['pending_typechecks'])} Airflow-runtime typechecks (dependencies and stubs required).\n")
    sys.stdout.write(f"{report['status']}: {(folder / 'report.json').relative_to(ROOT)}\n")
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
