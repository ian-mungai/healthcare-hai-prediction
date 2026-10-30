"""E2E cases for the drift task's phase runner (``monitoring.aws_run``) on synthetic fixtures: no real record, no AWS call.

The phases run on one shared workspace, as the containers of the task do. The baseline phase gets the DVC E2E's run store with a registry stand-in, the
current phase the training extract cases' synthetic feature reader, and the publish phase an in-memory S3 client. These are declared substitutions. The check
phase runs as the real command, since it reads only files. A check that needs Evidently reports blocked, never passed.
"""

from __future__ import annotations

import hashlib
import importlib
import io
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

from monitoring import drift_cases, reader_cases
from monitoring.drift_check import HAI_TYPES, CheckError
from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[1]
PYTHON = str(ROOT / ".venv/bin/python")
RUN = drift_cases.RUN_ID
Problems = list[str]


class Missing(Exception):
    """What boto3 raises for an absent key."""

    response = {"Error": {"Code": "NoSuchKey"}}


class MemoryS3:
    """The two S3 calls the task and the DAG make, on a dictionary."""

    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}
        self.puts: list[str] = []

    def put_object(self, **arguments: Any) -> dict[str, Any]:
        self.objects[(arguments["Bucket"], arguments["Key"])] = bytes(arguments["Body"])
        self.puts.append(arguments["Key"])
        return {}

    def get_object(self, **arguments: Any) -> dict[str, Any]:
        key = (arguments["Bucket"], arguments["Key"])
        if key not in self.objects:
            raise Missing(f"no object {key}")
        return {"Body": io.BytesIO(self.objects[key])}


def aws_run() -> Any:
    return importlib.import_module("monitoring.aws_run")


def workspace_with_two_baselines(folder: Path) -> tuple[reader_cases.Baseline, Path]:
    """A baseline for HAI_1 only: the stand-in registry has no production alias for the other five types."""
    baseline = reader_cases.Baseline(folder, columns=["c001__score__number", "state_code"], manifest=reader_cases.manifest_document(newest_window_year=2024))
    return baseline, folder / "work"


def baseline_phase_fetches_the_monitorable_types(folder: Path) -> Problems:
    baseline, work = workspace_with_two_baselines(folder)
    done = aws_run().baseline_phase(baseline.store, work, RUN, project=baseline.work.project)
    problems = []
    if done["monitored"] != ["HAI_1"] or sorted(done["unmonitored"]) != ["HAI_2", "HAI_3", "HAI_4", "HAI_5", "HAI_6"]:
        problems.append(f"monitored {done['monitored']}, unmonitored {sorted(done['unmonitored'])}")
    if set(done["unmonitored"].values()) != {"no_production_model"}:
        problems.append(f"the reasons are {done['unmonitored']}")
    files = sorted(path.name for path in (work / "drift" / RUN / "HAI_1").iterdir())
    if files != ["features.json", "reference.parquet", "reference_manifest.json"]:
        problems.append(f"HAI_1 holds {files}")
    return problems


def baseline_phase_resumes(folder: Path) -> Problems:
    baseline, work = workspace_with_two_baselines(folder)
    first = aws_run().baseline_phase(baseline.store, work, RUN, project=baseline.work.project)
    baseline.store.aliases.clear()
    again = aws_run().baseline_phase(baseline.store, work, RUN, project=baseline.work.project)
    return [] if again == first else ["a rerun in the same workspace asked the registry again and changed the result"]


def baseline_phase_with_nothing_to_monitor(folder: Path) -> Problems:
    baseline = reader_cases.Baseline(folder, alias=False)
    work = folder / "work"
    done = aws_run().baseline_phase(baseline.store, work, RUN, project=baseline.work.project)
    return [] if done["monitored"] == [] and len(done["unmonitored"]) == len(HAI_TYPES) else [f"expected six unmonitored types, got {done}"]


def baseline_phase_fails_on_a_broken_type(folder: Path) -> Problems:
    """A type that has a Production model but a damaged run is a failure, not an unmonitored type, and leaves no done record."""
    baseline = reader_cases.Baseline(folder, artifacts=False)
    work = folder / "work"
    try:
        aws_run().baseline_phase(baseline.store, work, RUN, project=baseline.work.project)
    except CheckError as error:
        return [] if "artifact_missing" in str(error) and not (work / "drift" / RUN / "baseline.done.json").exists() else [f"unexpected: {error}"]
    return ["a damaged Production run was recorded as unmonitored"]


def current_phase_reads_each_monitored_type(folder: Path) -> Problems:
    baseline, work = workspace_with_two_baselines(folder)
    aws_run().baseline_phase(baseline.store, work, RUN, project=baseline.work.project)
    (folder / "feature").mkdir()
    current = reader_cases.Current(folder / "feature")
    done = aws_run().current_phase(current.root, current.reader, work, RUN)
    manifest = json.loads((work / "drift" / RUN / "HAI_1" / "current_manifest.json").read_text())
    problems = [] if done["types"] == ["HAI_1"] else [f"the phase covered {done['types']}"]
    if (manifest["hai_type"], manifest["population"], manifest["row_count"]) != ("HAI_1", "primary", 4):
        problems.append(f"the current manifest is {manifest}")
    return problems


def run_check(work: Path, extra: list[str] | None = None) -> tuple[int, str]:
    done = run_command(PYTHON, ["-m", "monitoring.aws_run", "check", f"--run={RUN}", f"--work={work}", *(extra or [])], cwd=ROOT, timeout=300)
    return done.returncode, done.stdout + done.stderr


def prepared_workspace(folder: Path, *, population: str = "primary", current_year: int = 2022) -> Path:
    """A workspace as the baseline and current phases leave it, for HAI_1, written from the drift fixtures."""
    work = folder / "work"
    type_folder = work / "drift" / RUN / "HAI_1"
    type_folder.mkdir(parents=True)
    fixture = drift_cases.Fixture(population=population, current_population=population, current_year=current_year)
    paths = drift_cases.write_inputs(folder, fixture)
    for name, target in (
        ("reference-matrix", "reference.parquet"),
        ("reference-manifest", "reference_manifest.json"),
        ("current-matrix", "current.parquet"),
        ("current-manifest", "current_manifest.json"),
        ("features", "features.json"),
    ):
        (type_folder / target).write_bytes(paths[name].read_bytes())
    (work / "drift" / RUN / "baseline.done.json").write_text(json.dumps({"phase": "baseline", "run_id": RUN, "monitored": ["HAI_1"], "unmonitored": {}}) + "\n")
    return work


def check_phase_skips_without_a_new_window(folder: Path) -> Problems:
    work = prepared_workspace(folder, current_year=2021)
    code, text = run_check(work)
    path = work / "drift" / RUN / "records" / "drift_HAI_1.json"
    if code != 0 or not path.is_file():
        return [f"exit {code}, record present {path.is_file()}: {text[:300]}"]
    record = json.loads(path.read_text())
    return [] if (record["status"], record["skip_reason"], record["triggered"]) == ("skipped", "no_new_window", False) else [f"unexpected record {record}"]


def check_phase_skips_the_sensitivity_population(folder: Path) -> Problems:
    work = prepared_workspace(folder, population="sensitivity")
    code, text = run_check(work)
    path = work / "drift" / RUN / "records" / "drift_HAI_1.json"
    return [] if code == 0 and path.is_file() and json.loads(path.read_text())["skip_reason"] == "population_not_monitored" else [f"exit {code}: {text[:300]}"]


def check_phase_names_a_missing_phase(folder: Path) -> Problems:
    work = folder / "work"
    (work / "drift" / RUN).mkdir(parents=True)
    code, text = run_check(work)
    return [] if code == 2 and "baseline_missing" in text else [f"expected exit 2 naming baseline_missing, got {code}: {text[:300]}"]


def check_phase_checks_with_evidently(folder: Path) -> Problems:
    work = prepared_workspace(folder)
    drift_cases.need_evidently()
    code, text = run_check(work)
    path = work / "drift" / RUN / "records" / "drift_HAI_1.json"
    return [] if code == 0 and json.loads(path.read_text())["status"] == "checked" else [f"exit {code}: {text[:300]}"]


def check_phase_without_evidently_names_it(folder: Path) -> Problems:
    work = prepared_workspace(folder)
    code, text = run_check(work)
    problems = [] if code == 3 else [f"expected exit 3, got {code}: {text[:300]}"]
    if "dependency_missing" not in text:
        problems.append(f"the message does not name the missing dependency: {text[:300]}")
    return problems


def publish_records(folder: Path, s3: MemoryS3, *, records: bool = True) -> tuple[Path, Any]:
    work = prepared_workspace(folder, current_year=2021)
    code, text = run_check(work)
    if code != 0:
        raise ValueError(f"the fixture check failed: {text[:300]}")
    if not records:
        for path in (work / "drift" / RUN / "records").glob("*.json"):
            path.rename(path.with_suffix(".moved"))
    return work, aws_run().publish_phase(s3, "bucket-for-tests", work, RUN)


def publish_writes_records_then_the_marker(folder: Path) -> Problems:
    s3 = MemoryS3()
    work, done = publish_records(folder, s3)
    record = (work / "drift" / RUN / "records" / "drift_HAI_1.json").read_bytes()
    marker = json.loads(s3.objects[("bucket-for-tests", f"drift/{RUN}/_complete.json")])
    problems = []
    if s3.puts != [f"drift/{RUN}/drift_HAI_1.json", f"drift/{RUN}/_complete.json"]:
        problems.append(f"the writes were {s3.puts}; the marker must come last")
    if s3.objects[("bucket-for-tests", f"drift/{RUN}/drift_HAI_1.json")] != record:
        problems.append("the uploaded record differs from the file")
    if marker != {"run_id": RUN, "monitored": ["HAI_1"], "unmonitored": {}, "files": {"drift_HAI_1.json": hashlib.sha256(record).hexdigest()}}:
        problems.append(f"the marker is {marker}")
    return problems or ([] if done["files"] == ["drift_HAI_1.json"] else [f"the phase reports {done}"])


def publish_repeats_to_the_same_result(folder: Path) -> Problems:
    s3 = MemoryS3()
    work, _ = publish_records(folder, s3)
    first = dict(s3.objects)
    aws_run().publish_phase(s3, "bucket-for-tests", work, RUN)
    return [] if s3.objects == first and len(s3.objects) == 2 else ["a second publish changed or added objects"]


def publish_with_no_records_still_marks_the_run(folder: Path) -> Problems:
    s3 = MemoryS3()
    work = folder / "work"
    (work / "drift" / RUN).mkdir(parents=True)
    (work / "drift" / RUN / "baseline.done.json").write_text(
        json.dumps({"phase": "baseline", "run_id": RUN, "monitored": [], "unmonitored": {"HAI_1": "no_production_model"}}) + "\n"
    )
    aws_run().publish_phase(s3, "bucket-for-tests", work, RUN)
    marker = json.loads(s3.objects[("bucket-for-tests", f"drift/{RUN}/_complete.json")])
    return (
        [] if marker["files"] == {} and marker["monitored"] == [] and marker["unmonitored"] == {"HAI_1": "no_production_model"} else [f"the marker is {marker}"]
    )


def publish_refuses_a_monitored_type_without_a_record(folder: Path) -> Problems:
    s3 = MemoryS3()
    try:
        publish_records(folder, s3, records=False)
    except CheckError as error:
        return [] if "record_missing" in str(error) and not s3.objects else [f"unexpected: {error}, objects {len(s3.objects)}"]
    return ["a monitored type without a record was published"]


def publish_refuses_a_record_of_another_run(folder: Path) -> Problems:
    s3 = MemoryS3()
    work = prepared_workspace(folder, current_year=2021)
    run_check(work)
    path = work / "drift" / RUN / "records" / "drift_HAI_1.json"
    path.write_text(path.read_text().replace(RUN, "20261029T120000Z-ffffffff"))
    try:
        aws_run().publish_phase(s3, "bucket-for-tests", work, RUN)
    except CheckError as error:
        return [] if "record_stale" in str(error) and not s3.objects else [f"unexpected: {error}, objects {len(s3.objects)}"]
    return ["a record of another run was published"]


def cli_names_missing_clients(phase: str, needle: str) -> Callable[[Path], Problems]:
    def case(folder: Path) -> Problems:
        work = folder / "work"
        (work / "drift" / RUN).mkdir(parents=True)
        (work / "drift" / RUN / "baseline.done.json").write_text(
            json.dumps({"phase": "baseline", "run_id": RUN, "monitored": ["HAI_1"], "unmonitored": {}}) + "\n"
        )
        done = run_command(PYTHON, ["-m", "monitoring.aws_run", phase, f"--run={RUN}", f"--work={work}"], cwd=ROOT, timeout=120)
        text = done.stdout + done.stderr
        problems = [] if done.returncode == 3 else [f"expected exit 3, got {done.returncode}: {text[:300]}"]
        if "dependency_missing" not in text or needle not in text.lower():
            problems.append(f"the message does not name {needle}: {text[:300]}")
        return problems

    return case


def publish_cli_refuses_a_bad_bucket_name(folder: Path) -> Problems:
    """The command refuses before it makes any client, so this case reaches no AWS service."""
    work = prepared_workspace(folder, current_year=2021)
    done = run_command(PYTHON, ["-m", "monitoring.aws_run", "publish", f"--run={RUN}", f"--work={work}", "--bucket=Not_A_Bucket"], cwd=ROOT, timeout=120)
    text = done.stdout + done.stderr
    return [] if done.returncode == 2 and "bucket_invalid" in text else [f"expected exit 2 naming bucket_invalid, got {done.returncode}: {text[:300]}"]


def cases() -> list[tuple[str, Callable[[Path], Problems]]]:
    return [
        ("task_baseline_fetches_the_monitorable_types", baseline_phase_fetches_the_monitorable_types),
        ("task_baseline_resumes", baseline_phase_resumes),
        ("task_baseline_fails_on_a_broken_type", baseline_phase_fails_on_a_broken_type),
        ("task_baseline_with_nothing_to_monitor", baseline_phase_with_nothing_to_monitor),
        ("task_current_reads_each_monitored_type", current_phase_reads_each_monitored_type),
        ("task_check_skips_without_a_new_window", check_phase_skips_without_a_new_window),
        ("task_check_skips_the_sensitivity_population", check_phase_skips_the_sensitivity_population),
        ("task_check_names_a_missing_phase", check_phase_names_a_missing_phase),
        ("task_check_without_evidently_names_it", check_phase_without_evidently_names_it),
        ("task_check_checks_with_evidently", check_phase_checks_with_evidently),
        ("task_publish_writes_records_then_the_marker", publish_writes_records_then_the_marker),
        ("task_publish_repeats_to_the_same_result", publish_repeats_to_the_same_result),
        ("task_publish_with_no_records_still_marks_the_run", publish_with_no_records_still_marks_the_run),
        ("task_publish_refuses_a_monitored_type_without_a_record", publish_refuses_a_monitored_type_without_a_record),
        ("task_publish_refuses_a_record_of_another_run", publish_refuses_a_record_of_another_run),
        ("task_baseline_cli_names_missing_mlflow", cli_names_missing_clients("baseline", "mlflow")),
        ("task_current_cli_names_the_feature_image", cli_names_missing_clients("current", "feature")),
        ("task_publish_cli_refuses_a_bad_bucket_name", publish_cli_refuses_a_bad_bucket_name),
    ]
