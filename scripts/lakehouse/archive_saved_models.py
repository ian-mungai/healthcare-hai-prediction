"""Archive the silver models that leave silver under the October 10 2026 layer design, with their reference outputs.

Run from the repository root:

    .venv/bin/python -m scripts.lakehouse.archive_saved_models write   # copy, export and write features/archive_manifest.json
    .venv/bin/python -m scripts.lakehouse.archive_saved_models check   # every archived file and reference export matches

The 16 saved models (the hospital-window spine, the alignment steps AL1 to AL5, the HAI outcome values, the CMI year
choices and the three SCD2 histories) and the 7 emptied models (4 formula models and 3 control maps) are copied from commit beab859 (the last verified build)
into features/ with their YAML entries, the macros only they use and their singular tests, plus the Great Expectations
suites and baselines (owner decision 3, Oct 10 2026: "keep the files for now. If needed they will be recovered or
reused"). The E2E expectations stay in the runners at beab859 and are recorded as blob pointers with the names of the
functions and constants that hold them. Reference outputs of the 23 models are exported read-only from the real and
fixture builds of that commit as Parquet into the ignored data/features/reference/ (owner decision W0-1, option A: the
data-files check keeps data out of Git); the manifest records each export's SHA-256, rows and content fingerprint.
Nothing is removed from the active project. Plan: plans/parallel_work_20261010/plan.md, wave 0 item 8.
"""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import re
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.lakehouse import package_fixture
from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]
COMMIT = "beab859750f58cea0bbb0297cf0d635be85ed24f"
DBT_TREE_SHA256 = "49cc1f46390973efb0bb60632978dc742a9f886e38c3396e15de615f91caae5a"
FEATURES = ROOT / "features"
MANIFEST = FEATURES / "archive_manifest.json"
REFERENCE = ROOT / "data" / "features" / "reference"
MODEL_FOLDER = "dbt/models/silver/intermediate"
SAVED = (
    "int_hospital_spine",
    "int_spine_hai_outcomes",
    "int_spine_care_compare_measures",
    "int_spine_hospital_measures",
    "int_spine_operations_measures",
    "int_spine_validation_measures",
    "int_spine_county_measures",
    "int_spine_county_context",
    "int_spine_linkage",
    "int_hai_outcome_values",
    "int_cmi_hospital_years",
    "int_cmi_hospital_data_years",
    "int_cmi_holds",
    "int_hospital_pos_history",
    "int_hospital_hgi_history",
    "int_hospital_ownership_history",
)
# The 7 emptied models: the 4 formula models and the 3 control-map models whose casts move into kept models.
FORMULAS = ("int_cost_report_measures", "int_impact_measures", "int_mup_measures", "int_occmix_measures")
FORMULAS += ("int_registry_measure_windows", "int_validation_measure_windows", "int_validation_program_values")
# Macros only the archived models call; shared macros stay in dbt/macros/ and are recorded as blob pointers.
OWN_MACROS = ("care_compare_alignment.sql", "hai_outcome_columns.sql", "hospital_alignment.sql", "scd2.sql", "cmi_years.sql")
SHARED_MACROS = ("geography_columns.sql", "pos_columns.sql", "mup_columns.sql", "ownership_columns.sql", "cost_report_columns.sql")
SINGULAR_TESTS = (
    "assert_care_compare_as_of_start",
    "assert_care_compare_edv_known",
    "assert_county_context_as_of_start",
    "assert_county_measures_as_of_start",
    "assert_hai_outcome_consistent",
    "assert_hai_outcome_values_cast",
    "assert_history_versions_valid",
    "assert_hospital_measures_as_of_start",
    "assert_occmix_holds_exclude_values",
    "assert_occmix_measure_scope",
    "assert_operations_measures_as_of_start",
    "assert_spine_as_of_window_start",
    "assert_validation_measures_overlap",
)
SUITES = "data_contracts/great_expectations"
RUNNERS = ("scripts/lakehouse/run_staging_e2e.py", "scripts/lakehouse/run_silver_e2e.py", "scripts/lakehouse/silver_quality.py")
# Names of the runner functions and constants that hold the archived models' E2E expectations.
EXPECTATION_NAMES = re.compile(
    r"spine|history|outcome|care_compare|hospital_measure|operations_measure|validation_aligned|county_measure|al4b|bridge|cmi", re.I
)
BUILDS = {"real": ROOT / "data/analytics/dbt/staging.duckdb", "fixture": ROOT / "data/analytics/dbt/e2e/base/staging.duckdb"}
FIXTURE_LAKEHOUSE = ROOT / "data/analytics/dbt/e2e/base/fixture_lakehouse.duckdb"
# Model names are the fixed constants above, never input.
EXPORT_SQL = "COPY (SELECT * FROM main.\"{name}\" ORDER BY ALL) TO '/workspace/out/{name}.parquet' (FORMAT parquet, COMPRESSION zstd);"


def at_commit(path: str) -> bytes:
    """A file's bytes at the archived commit."""
    result = run_command("git", ["show", f"{COMMIT}:{path}"], cwd=ROOT)
    if result.returncode:
        raise SystemExit(f"{path} is not in {COMMIT[:7]}: {result.stderr.strip()}")
    return result.stdout.encode()


def blob(path: str) -> str:
    """A file's Git blob ID at the archived commit."""
    return run_command("git", ["rev-parse", f"{COMMIT}:{path}"], cwd=ROOT, check=True).stdout.strip()


def sha256(data: bytes) -> str:
    """SHA-256 of bytes."""
    return hashlib.sha256(data).hexdigest()


def yaml_entries(text: str, names: tuple[str, ...]) -> str:
    """The named models' entries from a properties file, as written (comments and layout kept)."""
    blocks = re.split(r"(?m)^(?=  - name: )", text)
    kept = [block for block in blocks[1:] if block.split("\n", 1)[0].removeprefix("  - name: ").strip() in names]
    found = {block.split("\n", 1)[0].removeprefix("  - name: ").strip() for block in kept}
    if found != set(names):
        raise SystemExit(f"model entries missing from the properties file: {sorted(set(names) - found)}")
    return "# Archived from dbt/models/silver/intermediate/_intermediate__models.yml at beab859; not parsed by dbt.\nversion: 2\n\nmodels:\n" + "".join(kept)


def expectation_symbols(path: str) -> list[dict[str, Any]]:
    """Top-level functions and constants of a runner whose names mark them as the archived models' expectations."""
    tree = ast.parse(at_commit(path).decode())
    symbols = []
    for node in tree.body:
        names = [node.name] if isinstance(node, ast.FunctionDef) else [target.id for target in getattr(node, "targets", []) if isinstance(target, ast.Name)]
        for name in names:
            if EXPECTATION_NAMES.search(name):
                symbols.append({"name": name, "lines": [node.lineno, node.end_lineno]})
    return symbols


def copies() -> dict[str, str]:
    """Destination path in features/ to source path at the archived commit."""
    plan = {f"features/models/{name}.sql": f"{MODEL_FOLDER}/{name}.sql" for name in (*SAVED, *FORMULAS)}
    plan |= {f"features/macros/{name}": f"dbt/macros/{name}" for name in OWN_MACROS}
    plan |= {f"features/tests/{name}.sql": f"dbt/tests/{name}.sql" for name in SINGULAR_TESTS}
    suites = run_command("git", ["ls-tree", "-r", "--name-only", COMMIT, f"{SUITES}/suites"], cwd=ROOT, check=True).stdout.split()
    plan |= {f"features/great_expectations/suites/{Path(path).name}": path for path in suites}
    plan["features/great_expectations/baselines.json"] = f"{SUITES}/baselines.json"
    plan["features/great_expectations/fixture_baselines.json"] = f"{SUITES}/fixture_baselines.json"
    return plan


def export_reference(build: str, plan: dict[str, Any]) -> dict[str, Any]:
    """Export the 23 models of one build to Parquet, read-only, in the pinned image with no network."""
    database = BUILDS[build]
    out = REFERENCE / f"{build}_beab859"
    out.mkdir(parents=True, exist_ok=True)
    mounts = [
        (database.parent.resolve(), "/workspace/in", True),
        (out.resolve(), "/workspace/out", False),
        (ROOT / "services/analytics/resources.sql", "/opt/analytics/resources.sql", True),
    ]
    statements = "\n".join(EXPORT_SQL.format(name=name) for name in (*SAVED, *FORMULAS))
    environment = {key: plan["environment"][key] for key in ("JOB_MEMORY_LIMIT", "DUCKDB_MEMORY_LIMIT", "JOB_THREADS")}
    args = ["-readonly", f"/workspace/in/{database.name}", "-cmd", ".read /opt/analytics/resources.sql", "-c", statements]
    code, _, stderr = package_fixture.docker_run(args, mounts, environment, "duckdb")
    if code:
        raise SystemExit(f"{build} export failed: {stderr.strip()[-500:]}")
    prints = package_fixture.fingerprint_with(database, FIXTURE_LAKEHOUSE if build == "fixture" else None, plan)["relations"]
    files = {}
    for name in (*SAVED, *FORMULAS):
        path = out / f"{name}.parquet"
        files[name] = {
            "path": str(path.relative_to(ROOT)),
            "sha256": sha256(path.read_bytes()),
            "rows": prints[name]["rows"],
            "content": prints[name]["content"],
        }
    return {"build_file": str(database.relative_to(ROOT)), "build_file_bytes": database.stat().st_size, "files": files}


def write() -> dict[str, Any]:
    """Copy the archive from the commit, export the references and write the manifest."""
    tree = run_command(sys.executable, ["-c", "from scripts.lakehouse.run_staging_e2e import dbt_tree_sha256; print(dbt_tree_sha256())"], cwd=ROOT)
    if tree.stdout.strip() != DBT_TREE_SHA256:
        raise SystemExit("dbt/ differs from beab859's tree; the reference builds no longer match the working tree")
    files = {}
    for destination, source in copies().items():
        data = at_commit(source)
        (ROOT / destination).parent.mkdir(parents=True, exist_ok=True)
        (ROOT / destination).write_bytes(data)
        files[destination] = {"source": source, "blob": blob(source), "sha256": sha256(data)}
    properties = yaml_entries(at_commit(f"{MODEL_FOLDER}/_intermediate__models.yml").decode(), (*SAVED, *FORMULAS)).encode()
    (FEATURES / "models" / "_archived_models.yml").write_bytes(properties)
    files["features/models/_archived_models.yml"] = {"source": f"{MODEL_FOLDER}/_intermediate__models.yml (entries)", "sha256": sha256(properties)}
    with package_fixture.reservation() as plan:
        references = {build: export_reference(build, plan) for build in BUILDS}
    manifest = {
        "written_utc": datetime.now(UTC).isoformat(),
        "commit": COMMIT,
        "dbt_tree_sha256": DBT_TREE_SHA256,
        "image_id": package_fixture.image_id(),
        "saved_models": list(SAVED),
        "formula_models": list(FORMULAS),
        "files": files,
        "shared_macros": {name: blob(f"dbt/macros/{name}") for name in SHARED_MACROS},
        "e2e_expectations": {path: {"blob": blob(path), "symbols": expectation_symbols(path)} for path in RUNNERS},
        "references": references,
        "recover": "git show <commit>:<source> restores any file; the runners' symbols are at the recorded lines of their blob.",
    }
    MANIFEST.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return {"manifest": str(MANIFEST.relative_to(ROOT)), "files": len(files), "references": {build: len(ref["files"]) for build, ref in references.items()}}


def check() -> dict[str, Any]:
    """Every archived file and every reference export still has its recorded hash."""
    manifest = json.loads(MANIFEST.read_text())
    problems = [path for path, entry in manifest["files"].items() if not (ROOT / path).is_file() or sha256((ROOT / path).read_bytes()) != entry["sha256"]]
    missing_data = []
    for build in manifest["references"].values():
        for entry in build["files"].values():
            path = ROOT / entry["path"]
            if not path.is_file():
                missing_data.append(entry["path"])
            elif sha256(path.read_bytes()) != entry["sha256"]:
                problems.append(entry["path"])
    if problems:
        raise SystemExit(f"archive differs from its manifest: {problems}")
    return {"files": len(manifest["files"]), "reference_files_missing_locally": missing_data, "status": "pass"}


def main() -> int:
    """Run write or check and print the result."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("command", choices=("write", "check"))
    args = parser.parse_args()
    result = write() if args.command == "write" else check()
    sys.stdout.write(json.dumps(result, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
