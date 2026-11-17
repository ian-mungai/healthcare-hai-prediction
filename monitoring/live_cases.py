"""E2E cases for the on-demand live drift check on synthetic fixtures: no real patient, hospital or CMS record is read or written.

Every case writes one small baseline matrix (four window years, like the Production model's DVC baseline) with DuckDB from a fixed hash, starts the real
command as a child process and judges its exit code, its message and the files it leaves. The cases that need Evidently run only when ``.tools/evidently``
exists and are reported as blocked otherwise, never as passed. The phase cases run the real ``monitoring.aws_run`` command on a prepared workspace, and the
publish phase gets an in-memory S3 client (a declared substitution). The DAG cases load the real DAG file against the stand-ins of ``dag_cases``.
"""

from __future__ import annotations

import hashlib
import importlib
import json
import re
import shutil
import sys
from collections.abc import Callable
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest import mock

import duckdb

from monitoring import aws_cases, dag_cases, drift_cases
from monitoring.drift_cases import FEATURES, NUMERIC
from monitoring.drift_check import HAI_TYPES
from orchestration.training_contract import MODELS, RUN_ID
from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[1]
PYTHON = str(ROOT / ".venv/bin/python")
EVIDENTLY_PROGRAM = str(ROOT / ".tools/evidently/bin/python")
LIVE_DAG_FILE = ROOT / "dags/drift_live_check_dag.py"
RUN = drift_cases.RUN_ID
OTHER_RUN = "20261117T120000Z-0a1b2c3e"
BUCKET = "bucket-for-tests"
OUTPUTS = ["drift_HAI_1.json", "report_HAI_1.html"]
Problems = list[str]


def write_baseline(
    folder: Path,
    *,
    per_year: int = 500,
    current_rows: int = 300,
    shifted: tuple[str, ...] = (),
    years: tuple[int, ...] = (2021, 2022, 2023),
    newest: int | None = 2024,
    population: str = "primary",
    features: tuple[str, ...] = FEATURES,
) -> dict[str, Path]:
    """A baseline matrix of ``years`` with ``per_year`` rows each plus ``current_rows`` rows of 2024, the manifest that names 2024 as its newest year and the
    feature list. A feature in ``shifted`` moves only in the 2024 rows."""
    reference_rows = per_year * len(years)
    year = f"CASE WHEN i < {reference_rows} THEN ({years[0] if years else 2024} + i // {per_year}) ELSE 2024 END" if years else "2024"
    columns = [
        f"CASE WHEN i >= {reference_rows} THEN {drift_cases.column_sql(name, name in shifted)} ELSE {drift_cases.column_sql(name, False)} END AS {name}"
        for name in FEATURES
    ]
    projection = f"'k' || i::VARCHAR AS spine_key, {year} AS window_year, (hash('label' || i::VARCHAR) % 1000) / 500.0 AS label_value, {', '.join(columns)}"
    matrix = folder / "matrix.parquet"
    duckdb.sql("SELECT range AS i FROM range(?)", params=[reference_rows + current_rows]).project(projection).write_parquet(str(matrix))
    manifest = {"hai_type": "HAI_1", "population": population} | ({} if newest is None else {"newest_window_year": newest})
    paths = {"matrix": matrix, "manifest": folder / "manifest.json", "features": folder / "features.json"}
    paths["manifest"].write_text(json.dumps(manifest) + "\n")
    paths["features"].write_text(json.dumps(list(features)) + "\n")
    return paths


def run_live(
    folder: Path, paths: dict[str, Path], *, evidently: bool = False, model: str = "m2", run: str = RUN, out: Path | None = None, workspace: Path | None = None
) -> tuple[int, str, Path, Path]:
    out, workspace = out or folder / "demo_out", workspace or folder / "evidently_workspace"
    arguments = [f"--{name}={path}" for name, path in paths.items()] + [f"--model={model}", f"--run-id={run}", f"--out={out}", f"--workspace={workspace}"]
    done = run_command(EVIDENTLY_PROGRAM if evidently else PYTHON, ["-m", "monitoring.live_check", *arguments], cwd=ROOT, timeout=drift_cases.TIMEOUT)
    return done.returncode, done.stdout + done.stderr, out, workspace


def projects_of(workspace: Path) -> dict[str, list[str]]:
    """The projects of an Evidently workspace folder (name to the run IDs of its snapshots), read from the files the UI reads."""
    found: dict[str, list[str]] = {}
    for metadata in sorted(workspace.glob("*/metadata.json")):
        runs = [json.loads(path.read_text())["metadata"].get("run_id", "") for path in sorted((metadata.parent / "snapshots").glob("*.json"))]
        found[json.loads(metadata.read_text())["name"]] = sorted(runs)
    return found


def empty(out: Path, workspace: Path) -> Problems:
    left = sorted(path.name for path in out.iterdir()) if out.exists() else []
    return ([f"a refused check left {left} in the output folder"] if left else []) + (
        [f"a refused check left projects {projects_of(workspace)}"] if workspace.exists() and projects_of(workspace) else []
    )


def refused(name: str, build: Callable[[Path], dict[str, Path]], detail: str = "", model: str = "m2") -> Callable[[Path], Problems]:
    def case(folder: Path) -> Problems:
        code, text, out, workspace = run_live(folder, build(folder), model=model)
        problems = [] if code == 2 else [f"expected exit 2, got {code}: {text[:300]}"]
        problems += [f"the message does not name {needle}: {text[:300]}" for needle in (name, detail) if needle not in text]
        return problems + empty(out, workspace)

    return case


def splits_by_year_and_checks(folder: Path) -> Problems:
    """The split of the fixture: 3 x 500 rows of 2021 to 2023 are the reference, the 300 rows of 2024 the current window, and every feature moved."""
    paths = write_baseline(folder, shifted=FEATURES)
    code, text, out, _ = run_live(folder, paths, evidently=drift_cases.need_evidently())
    if code != 0:
        return [f"exit {code}: {text[:400]}"]
    record = drift_cases.read_record(out)
    problems = drift_cases.schema_problems(record)
    expected = {"status": "checked", "triggered": False, "drifted_count": len(FEATURES), "feature_count": len(FEATURES), "run_id": RUN, "hai_type": "HAI_1"}
    problems += [f"{key} is {record[key]!r}, expected {value!r}" for key, value in expected.items() if record[key] != value]
    if record["reference"]["newest_window_year"] != 2023 or record["reference"]["rows"] != 1500:
        problems.append(f"the reference is {record['reference']}, expected 1500 rows up to 2023")
    if record["current"]["window_years"] != [2024] or record["current"]["rows"] != 300:
        problems.append(f"the current window is {record['current']}, expected 300 rows of 2024")
    if not record["evidently_version"] or record["drift_share"] != 1.0:
        problems.append(f"Evidently did not compute the drift: version {record['evidently_version']}, share {record['drift_share']}")
    return problems


def stays_below_the_share_without_a_shift(folder: Path) -> Problems:
    """No feature moves, so the share of drifted features stays far below the configured half (chance may flag one feature of five at p below 0.05)."""
    code, text, out, _ = run_live(folder, write_baseline(folder), evidently=drift_cases.need_evidently())
    if code != 0:
        return [f"exit {code}: {text[:400]}"]
    record = drift_cases.read_record(out)
    return [] if record["status"] == "checked" and record["drift_share"] < 0.5 else [f"expected a share below half, got {record['drift_share']}"]


def writes_the_record_and_the_html_to_the_demo_folder_only(folder: Path) -> Problems:
    """Only the record and Evidently's own HTML report land in the output folder, the HTML is the self-contained report, and the workspace holds one project."""
    paths = write_baseline(folder, shifted=NUMERIC[:2])
    code, text, out, workspace = run_live(folder, paths, evidently=drift_cases.need_evidently())
    if code != 0:
        return [f"exit {code}: {text[:400]}"]
    problems = [] if sorted(path.name for path in out.iterdir()) == OUTPUTS else [f"the output folder holds {sorted(path.name for path in out.iterdir())}"]
    html = (out / "report_HAI_1.html").read_text() if (out / "report_HAI_1.html").is_file() else ""
    if "<html" not in html[:2000].lower() or len(html) < 100_000 or "<script src=" in html:
        problems.append(f"report_HAI_1.html is not a self-contained Evidently report ({len(html)} characters)")
    if projects_of(workspace) != {"m2 Elastic Net HAI_1 CLABSI": [RUN]}:
        problems.append(f"the workspace holds {projects_of(workspace)}, expected one project with this run")
    stray = sorted(path.name for path in folder.iterdir() if path.name not in {"demo_out", "evidently_workspace", *(path.name for path in paths.values())})
    return problems + ([f"the command left {stray} beside its inputs"] if stray else [])


def rerun_adds_nothing(folder: Path) -> Problems:
    """A rerun of the same run writes the same record bytes, keeps one project and adds no second snapshot."""
    paths, evidently = write_baseline(folder, shifted=NUMERIC[:2]), drift_cases.need_evidently()
    code, text, out, workspace = run_live(folder, paths, evidently=evidently)
    if code != 0:
        return [f"exit {code}: {text[:400]}"]
    first = (out / "drift_HAI_1.json").read_bytes()
    code, text, _, _ = run_live(folder, paths, evidently=evidently)
    problems = [] if code == 0 else [f"the rerun exited {code}: {text[:300]}"]
    if (out / "drift_HAI_1.json").read_bytes() != first:
        problems.append("a rerun on the same inputs changed the record")
    if projects_of(workspace) != {"m2 Elastic Net HAI_1 CLABSI": [RUN]}:
        problems.append(f"the rerun changed the workspace: {projects_of(workspace)}")
    return problems


def another_run_adds_a_snapshot_to_the_same_project(folder: Path) -> Problems:
    paths, evidently = write_baseline(folder, shifted=NUMERIC[:2]), drift_cases.need_evidently()
    for run in (RUN, OTHER_RUN):
        code, text, _, workspace = run_live(folder, paths, evidently=evidently, run=run, out=folder / f"out_{run}")
        if code != 0:
            return [f"exit {code}: {text[:400]}"]
    found = projects_of(workspace)
    return [] if found == {"m2 Elastic Net HAI_1 CLABSI": sorted([RUN, OTHER_RUN])} else [f"expected one project with both runs, got {found}"]


def another_model_gets_its_own_project(folder: Path) -> Problems:
    paths, evidently = write_baseline(folder), drift_cases.need_evidently()
    for model in ("m1", "m4"):
        code, text, _, workspace = run_live(folder, paths, evidently=evidently, model=model, out=folder / f"out_{model}")
        if code != 0:
            return [f"exit {code}: {text[:400]}"]
    found = projects_of(workspace)
    return (
        []
        if sorted(found) == ["m1 OLS baseline HAI_1 CLABSI", "m4 Gradient boosting HAI_1 CLABSI"]
        else [f"expected one project per model, got {sorted(found)}"]
    )


def passes_the_record_to_each_sink_once(folder: Path) -> Problems:
    """The one call site the monitoring merge wires: every sink in ``record_sinks`` gets the written record once, after the files exist. Nothing else runs."""
    paths = write_baseline(folder, shifted=FEATURES)
    script = (
        "import json, sys\n"
        "from argparse import Namespace\n"
        "from pathlib import Path\n"
        "from monitoring import live_check\n"
        "seen = []\n"
        "def sink(record):\n"
        "    seen.append({'hai_type': record['hai_type'], 'triggered': record['triggered'], 'html': Path(sys.argv[1], 'report_HAI_1.html').is_file()})\n"
        "live_check.run(Namespace(model='m2', matrix=Path(sys.argv[2]), manifest=Path(sys.argv[3]), features=Path(sys.argv[4]), run_id=sys.argv[5],\n"
        "    out=Path(sys.argv[1]), workspace=Path(sys.argv[6]), config=live_check.drift_config.CONFIG), record_sinks=(sink,))\n"
        "print(json.dumps(seen))\n"
    )
    out, workspace = folder / "demo_out", folder / "evidently_workspace"
    arguments = ["-c", script, str(out), str(paths["matrix"]), str(paths["manifest"]), str(paths["features"]), RUN, str(workspace)]
    done = run_command(EVIDENTLY_PROGRAM if drift_cases.need_evidently() else PYTHON, arguments, cwd=ROOT, timeout=drift_cases.TIMEOUT)
    if done.returncode != 0:
        return [f"exit {done.returncode}: {(done.stdout + done.stderr)[:400]}"]
    seen = json.loads(done.stdout.strip().splitlines()[-1])
    return [] if seen == [{"hai_type": "HAI_1", "triggered": False, "html": True}] else [f"the sink saw {seen}"]


def refuses_an_unknown_model(folder: Path) -> Problems:
    code, text, out, workspace = run_live(folder, write_baseline(folder), model="m9")
    return ([] if code == 2 and "m9" in text else [f"expected exit 2 naming m9, got {code}: {text[:300]}"]) + empty(out, workspace)


def names_the_missing_dependency(folder: Path) -> Problems:
    code, text, out, workspace = run_live(folder, write_baseline(folder, shifted=FEATURES))
    problems = (
        [] if code == 3 and "dependency_missing" in text and "evidently" in text.lower() else [f"expected exit 3 naming Evidently, got {code}: {text[:300]}"]
    )
    return problems + empty(out, workspace)


def workspace_for(
    folder: Path, hai_type: str = "HAI_1", model: str = "m2", *, newest: int = 2024, population: str = "primary", with_current: bool = True
) -> Path:
    """A drift workspace as the baseline phase leaves it for the live check: the baseline matrix, its manifest and feature list, and the baseline record."""
    work = folder / "work"
    paths = write_baseline(folder, shifted=FEATURES, newest=newest, population=population, current_rows=300 if with_current else 0)
    type_folder = work / "drift" / RUN / hai_type
    type_folder.mkdir(parents=True)
    for name, target in (("matrix", "reference.parquet"), ("manifest", "reference_manifest.json"), ("features", "features.json")):
        (type_folder / target).write_bytes(paths[name].read_bytes())
    done = {"phase": "baseline", "run_id": RUN, "monitored": [hai_type], "unmonitored": {}, "model": model}
    (work / "drift" / RUN / "baseline.done.json").write_text(json.dumps(done) + "\n")
    return work


def phase(folder: Path, name: str, *extra: str, evidently: bool = False, live: str = "HAI_1") -> tuple[int, str]:
    work = folder / "work"
    arguments = [
        "-m",
        "monitoring.aws_run",
        name,
        f"--run={RUN}",
        f"--work={work}",
        f"--workspace={folder / 'evidently_workspace'}",
        f"--live-type={live}",
        *extra,
    ]
    done = run_command(EVIDENTLY_PROGRAM if evidently else PYTHON, arguments, cwd=ROOT, timeout=drift_cases.TIMEOUT)
    return done.returncode, done.stdout + done.stderr


def check_phase_runs_the_live_check(folder: Path) -> Problems:
    work = workspace_for(folder)
    code, text = phase(folder, "check", evidently=drift_cases.need_evidently())
    out = work / "drift_live" / RUN
    problems = [] if code == 0 else [f"exit {code}: {text[:400]}"]
    if sorted(path.name for path in out.glob("*")) != OUTPUTS:
        problems.append(f"the live outputs are {sorted(path.name for path in out.glob('*'))}")
    if (work / "drift" / RUN / "records").exists():
        problems.append("the live check wrote into the records folder of a gate run")
    if projects_of(folder / "evidently_workspace") != {"m2 Elastic Net HAI_1 CLABSI": [RUN]}:
        problems.append(f"the workspace holds {projects_of(folder / 'evidently_workspace')}")
    return problems


def check_phase_refuses_an_unmonitored_type(folder: Path) -> Problems:
    work = workspace_for(folder)
    done = work / "drift" / RUN / "baseline.done.json"
    done.write_text(json.dumps({"phase": "baseline", "run_id": RUN, "monitored": [], "unmonitored": {"HAI_1": "no_production_model"}, "model": "m2"}) + "\n")
    code, text = phase(folder, "check")
    return (
        []
        if code == 2 and "no_production_model" in text and not (work / "drift_live").exists()
        else [f"expected exit 2 naming no_production_model, got {code}: {text[:300]}"]
    )


def current_phase_reads_nothing_in_live_mode(folder: Path) -> Problems:
    work = workspace_for(folder)
    code, text = phase(folder, "current")
    lines = [line for line in text.splitlines() if line.startswith('{"phase"')]
    result = json.loads(lines[-1])["result"] if code == 0 and lines else {}
    problems = [] if code == 0 and result.get("types") == [] else [f"expected exit 0 with no types, got {code}: {text[:300]}"]
    if (work / "drift" / RUN / "current.done.json").exists():
        problems.append("a live run recorded a current phase of a gate run")
    return problems


def baseline_phase_covers_only_the_live_type(folder: Path) -> Problems:
    """The baseline phase of a live run fetches the one type it was given, for the model it was given, and the gate task still covers all six. The registry
    and the DVC workspace are declared stand-ins (the real baseline fetch has its own cases): the case judges which types and model the phase asks for."""
    runner, calls = importlib.import_module("monitoring.aws_run"), []

    def recorder(store: Any, work: Path, run: str, **keywords: Any) -> dict[str, Any]:
        calls.append(keywords)
        return {"phase": "baseline", "run_id": run, "monitored": [], "unmonitored": {}}

    with (
        mock.patch.object(runner, "baseline_phase", recorder),
        mock.patch.object(runner, "open_store", lambda: object()),
        mock.patch.object(runner, "model_feature_list_sha", lambda root, model: None),
    ):
        runner.main(["baseline", f"--run={RUN}", f"--work={folder / 'work'}", "--live-type=HAI_3", "--model=m2"])
        runner.main(["baseline", f"--run={RUN}", f"--work={folder / 'work'}", "--live-type=", "--model=m2"])
        refused = runner.main(["baseline", f"--run={RUN}", f"--work={folder / 'work'}", "--live-type=HAI_3"])
    live, gate = calls[0], calls[1]
    problems = [] if live["hai_types"] == ("HAI_3",) and live["model"] == "m2" else [f"the live baseline asked for {live}"]
    if gate["hai_types"] != HAI_TYPES:
        problems.append(f"the gate task's baseline asked for {gate['hai_types']}, not all six types")
    return problems + ([] if refused == 2 and len(calls) == 2 else [f"a live baseline with no model exited {refused} after {len(calls)} fetches"])


def live_type_is_a_hai_type(folder: Path) -> Problems:
    code, text = phase(folder, "current", live="HAI_9")
    return [] if code == 2 and "invalid choice" in text and "HAI_9" in text else [f"expected exit 2 with an invalid choice HAI_9, got {code}: {text[:300]}"]


def live_publish(folder: Path, s3: aws_cases.MemoryS3, *, outputs: bool = True) -> tuple[Path, Any]:
    work = workspace_for(folder)
    out = work / "drift_live" / RUN
    out.mkdir(parents=True)
    if outputs:
        (out / "drift_HAI_1.json").write_text(json.dumps({"run_id": RUN, "hai_type": "HAI_1"}) + "\n")
        (out / "report_HAI_1.html").write_text("<html>report</html>\n")
    return work, importlib.import_module("monitoring.aws_run").publish_live_phase(s3, BUCKET, work, RUN, "HAI_1")


def publish_writes_the_demo_prefix_then_the_marker(folder: Path) -> Problems:
    s3 = aws_cases.MemoryS3()
    work, done = live_publish(folder, s3)
    out = work / "drift_live" / RUN
    expected = [f"drift_demo/{RUN}/drift_HAI_1.json", f"drift_demo/{RUN}/report_HAI_1.html", f"drift_demo/{RUN}/_complete.json"]
    problems = [] if s3.puts == expected else [f"the writes were {s3.puts}, expected {expected}"]
    if any(key.startswith(("drift/", f"drift/{RUN}")) for key in s3.puts):
        problems.append("a live check wrote into the drift/ prefix of the gate runs")
    marker = json.loads(s3.objects[(BUCKET, f"drift_demo/{RUN}/_complete.json")])
    files = {name: hashlib.sha256((out / name).read_bytes()).hexdigest() for name in OUTPUTS}
    if marker != {"kind": "drift_live", "run_id": RUN, "hai_type": "HAI_1", "model": "m2", "files": files}:
        problems.append(f"the marker is {marker}")
    return problems or ([] if done["files"] == OUTPUTS else [f"the phase reports {done}"])


def publish_repeats_to_the_same_result(folder: Path) -> Problems:
    s3 = aws_cases.MemoryS3()
    work, _ = live_publish(folder, s3)
    first = dict(s3.objects)
    importlib.import_module("monitoring.aws_run").publish_live_phase(s3, BUCKET, work, RUN, "HAI_1")
    return [] if s3.objects == first and len(first) == 3 else ["a second publish changed or added objects"]


def publish_refuses_missing_outputs(folder: Path) -> Problems:
    s3 = aws_cases.MemoryS3()
    try:
        live_publish(folder, s3, outputs=False)
    except importlib.import_module("monitoring.drift_check").CheckError as error:
        return [] if "record_missing" in str(error) and not s3.objects else [f"unexpected: {error}, objects {len(s3.objects)}"]
    return ["a live run without its outputs was published"]


def publish_cli_refuses_a_bad_bucket_name(folder: Path) -> Problems:
    work = workspace_for(folder)
    done = run_command(
        PYTHON, ["-m", "monitoring.aws_run", "publish", f"--run={RUN}", f"--work={work}", "--live-type=HAI_1", "--bucket=A_bad_bucket"], cwd=ROOT, timeout=120
    )
    text = done.stdout + done.stderr
    return [] if done.returncode == 2 and "bucket_invalid" in text else [f"expected exit 2 naming bucket_invalid, got {done.returncode}: {text[:300]}"]


@contextmanager
def loaded_live_dag() -> Any:
    """The real live DAG file against the stand-ins of the drift DAG cases."""
    previous = dag_cases.DAG_FILE
    dag_cases.DAG_FILE = LIVE_DAG_FILE
    try:
        with dag_cases.loaded_dag() as module:
            yield module
    finally:
        dag_cases.DAG_FILE = previous


IAM_FOLDER = ROOT / "infra/iam"
GRANT_BLOCK = re.compile(
    r'resource\s+"(aws_iam_role_policy|aws_iam_role_policy_attachment|aws_iam_policy_attachment|aws_iam_role_policies_exclusive)"\s+"[^"]+"\s*\{'
)


def grants_to_the_ui_role(folder: Path) -> list[str]:
    """The policy-attaching IAM resources of the Terraform files in ``folder`` whose body names the Evidently UI task role."""
    found = []
    for path in sorted(folder.glob("*.tf")):
        text = path.read_text()
        for match in GRANT_BLOCK.finditer(text):
            depth, end = 1, match.end()
            while depth and end < len(text):
                depth += {"{": 1, "}": -1}.get(text[end], 0)
                end += 1
            if "evidently_ui_task" in text[match.end() : end]:
                found.append(f"{path.name}: {text[match.start() : match.end()].split(chr(34))[3]}")
    return found


def ui_role_exists_and_holds_no_grant(folder: Path) -> Problems:
    """The Evidently UI task role is defined in the IAM stack and nothing attaches a policy to it: it calls no AWS service, so it needs no grant."""
    role_file = IAM_FOLDER / "evidently_ui.tf"
    problems = (
        []
        if role_file.is_file() and 'resource "aws_iam_role" "evidently_ui_task"' in role_file.read_text()
        else ["infra/iam/evidently_ui.tf does not define the role"]
    )
    found = grants_to_the_ui_role(IAM_FOLDER)
    return problems + ([f"a policy is attached to the UI role: {found}"] if found else [])


def ui_role_check_catches_a_planted_s3_grant(folder: Path) -> Problems:
    """The check above is effective: in a copy of the IAM stack, an inline S3 read policy and a managed attachment on the role are each found."""
    plant = folder / "iam_copy"
    shutil.copytree(IAM_FOLDER, plant, ignore=shutil.ignore_patterns(".terraform", "tests", "*.tfstate*"))
    unplanted = grants_to_the_ui_role(plant)
    inline = [
        'resource "aws_iam_role_policy" "ui_reads_s3" {',
        "  role   = aws_iam_role.evidently_ui_task[0].name",
        '  policy = jsonencode({ Statement = [{ Effect = "Allow", Action = ["s3:GetObject"], Resource = ["arn:aws:s3:::example/*"] }] })',
        "}",
    ]
    managed = [
        'resource "aws_iam_role_policy_attachment" "ui_managed" {',
        "  role       = aws_iam_role.evidently_ui_task[0].name",
        '  policy_arn = "arn:aws:iam::aws:policy/S3ReadOnly"',
        "}",
    ]
    (plant / "planted.tf").write_text("\n".join([*inline, *managed]) + "\n")
    planted = grants_to_the_ui_role(plant)
    problems = [f"the copy already held a grant: {unplanted}"] if unplanted else []
    return problems + ([] if sorted(planted) == ["planted.tf: ui_managed", "planted.tf: ui_reads_s3"] else [f"the planted grants were found as {planted}"])


def dag_declares_one_job_chain_and_triggers_nothing(folder: Path) -> Problems:
    with loaded_live_dag() as module:
        dag = module.dag
        nodes = {name: node.upstream_task_ids for name, node in dag.task_dict.items()}
        expected = {"preflight": set(), "job": {"preflight"}, "evidence": {"job"}, "gate": {"evidence"}}
        problems = [] if nodes == expected else [f"the graph is {nodes}"]
        wanted = {"dag_id": "drift_live_check_dag", "schedule": None, "catchup": False, "max_active_runs": 1, "default_args": {"retries": 0}}
        problems += [f"DAG setting {key} is {dag.settings.get(key)!r}, expected {value!r}" for key, value in wanted.items() if dag.settings.get(key) != value]
        problems += [f"{name} triggers {node.settings['trigger_dag_id']}" for name, node in dag.task_dict.items() if "trigger_dag_id" in node.settings]
        return problems


def dag_form_takes_a_run_a_model_and_a_type(folder: Path) -> Problems:
    with loaded_live_dag() as module:
        params = module.dag.settings["params"]
        problems = [] if sorted(params) == ["hai_type", "mode", "model", "run_id"] else [f"the form holds {sorted(params)}"]
        if params["run_id"].default is not None or params["run_id"].settings.get("pattern") != f"^{RUN_ID}$":
            problems.append("run_id is not a required run ID")
        if params["mode"].default != "real" or params["mode"].settings.get("enum") != ["real"]:
            problems.append("the mode is not real only")
        for name, values in (("model", list(MODELS)), ("hai_type", list(HAI_TYPES))):
            if params[name].default is not None or params[name].settings.get("enum") != values:
                problems.append(f"{name} is not a required choice of {values}")
        return problems


def dag_request_carries_the_model_and_the_type(folder: Path) -> Problems:
    with loaded_live_dag() as module:
        runtime = sys.modules["orchestration.dag_runtime"]
        form = {"run_id": RUN, "mode": "real", "model": "m2", "hai_type": "HAI_3"}
        instance = SimpleNamespace(dag_id="drift_live_check_dag", try_number=1, task_id="job", run_id="manual__test")
        with mock.patch.object(dag_cases, "CONTEXT", {"context": {"params": form, "ti": instance}}):
            request = runtime.step_request("drift_live_run", dag_cases.CONTEXT["context"])
        problems = [] if request.training == {"model": "m2", "hai_type": "HAI_3"} else [f"the request carries {request.training}"]
        if (request.template, request.run_id, request.input_run_id, request.mode) != ("drift_live_run", RUN, None, "real"):
            problems.append(f"the request is {request}")
        return problems or ([] if module.dag.settings["dag_id"] == "drift_live_check_dag" else ["wrong DAG"])


def dag_is_registered_in_the_job_tables(folder: Path) -> Problems:
    steps = json.loads((ROOT / "config/orchestration/ecs_broker_jobs.json").read_text())["steps"]
    host = json.loads((ROOT / "config/orchestration/host_jobs.json").read_text())["dags"]
    problems = [] if steps.get("drift_live_run") == "drift" else [f"drift_live_run maps to {steps.get('drift_live_run')!r}, not the drift job"]
    return problems + ([] if host.get("drift_live_check_dag") == "drift" else [f"the host table maps the DAG to {host.get('drift_live_check_dag')!r}"])


def cases() -> list[tuple[str, Callable[[Path], Problems]]]:
    refusals: list[tuple[str, Callable[[Path], Problems]]] = [
        ("live_refuses_when_no_row_is_of_the_newest_year", refused("no_current_window", lambda f: write_baseline(f, current_rows=0), "2024")),
        ("live_refuses_below_the_minimum_of_100_rows", refused("too_few_current_rows", lambda f: write_baseline(f, current_rows=99), "100")),
        ("live_refuses_when_no_row_is_older", refused("no_reference_rows", lambda f: write_baseline(f, years=()), "2023")),
        ("live_refuses_the_sensitivity_population", refused("population_not_monitored", lambda f: write_baseline(f, population="sensitivity"))),
        ("live_refuses_a_manifest_without_the_newest_year", refused("manifest_invalid", lambda f: write_baseline(f, newest=None), "newest_window_year")),
        (
            "live_refuses_a_feature_missing_from_the_matrix",
            refused("feature_missing", lambda f: write_baseline(f, features=(*FEATURES, "f_absent")), "f_absent"),
        ),
    ]
    return [
        *refusals,
        ("live_refuses_an_unknown_model", refuses_an_unknown_model),
        ("live_names_the_missing_dependency", names_the_missing_dependency),
        ("live_splits_by_year_and_checks", splits_by_year_and_checks),
        ("live_stays_below_the_share_without_a_shift", stays_below_the_share_without_a_shift),
        ("live_writes_the_record_and_html_to_the_demo_folder_only", writes_the_record_and_the_html_to_the_demo_folder_only),
        ("live_rerun_adds_nothing", rerun_adds_nothing),
        ("live_another_run_adds_a_snapshot", another_run_adds_a_snapshot_to_the_same_project),
        ("live_another_model_gets_its_own_project", another_model_gets_its_own_project),
        ("live_passes_the_record_to_each_sink_once", passes_the_record_to_each_sink_once),
        ("live_task_check_runs_the_live_check", check_phase_runs_the_live_check),
        ("live_task_check_refuses_an_unmonitored_type", check_phase_refuses_an_unmonitored_type),
        ("live_task_current_reads_nothing", current_phase_reads_nothing_in_live_mode),
        ("live_task_baseline_covers_only_the_live_type", baseline_phase_covers_only_the_live_type),
        ("live_task_type_is_a_hai_type", live_type_is_a_hai_type),
        ("live_task_publish_writes_the_demo_prefix_then_the_marker", publish_writes_the_demo_prefix_then_the_marker),
        ("live_task_publish_repeats_to_the_same_result", publish_repeats_to_the_same_result),
        ("live_task_publish_refuses_missing_outputs", publish_refuses_missing_outputs),
        ("live_task_publish_cli_refuses_a_bad_bucket_name", publish_cli_refuses_a_bad_bucket_name),
        ("live_ui_role_exists_and_holds_no_grant", ui_role_exists_and_holds_no_grant),
        ("live_ui_role_check_catches_a_planted_s3_grant", ui_role_check_catches_a_planted_s3_grant),
        ("live_dag_declares_one_job_chain_and_triggers_nothing", dag_declares_one_job_chain_and_triggers_nothing),
        ("live_dag_form_takes_a_run_a_model_and_a_type", dag_form_takes_a_run_a_model_and_a_type),
        ("live_dag_request_carries_the_model_and_the_type", dag_request_carries_the_model_and_the_type),
        ("live_dag_is_registered_in_the_job_tables", dag_is_registered_in_the_job_tables),
    ]
