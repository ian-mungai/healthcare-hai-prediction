"""Plant semantic defects in owned copies and require the real CLI cases to detect them.

Use ``--scope dataset`` for a partial host run, which exits nonzero with model defects pending.
Only ``--scope all`` in the pinned training image can pass the complete gate.
The production source is never edited. Every copied defect and case result stays in the requested output folder.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

from scripts.process import TimeoutExpired, run_command

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "scripts/training/capstone_replication.py"
DEFECTS = (
    (
        "hai_row_order",
        "dataset",
        "repeat_and_conflict",
        'for hai in sorted(tables["hai"], key=lambda row: facility_order.get(row.get("facility_id"), len(tables["timely_effective_care"]))):',
        'for hai in tables["hai"]:',
        "row_order: timely-care",
    ),
    (
        "left_join",
        "dataset",
        "inner_join",
        "if any(not group for group in matches):\n            continue",
        "matches = [group or [{}] for group in matches]",
        "inner_join: expected one",
    ),
    (
        "all_runs_admitted",
        "dataset",
        "selected_run_and_record",
        'if not Path(name).name.startswith(run_set[table] + "-part-r-"):',
        "if False:",
        "selected_run: command failed:",
    ),
    (
        "run_list_accepted",
        "dataset",
        "mixed_runs",
        "value = json.loads(path.read_text())",
        "value = {table: run[0] if isinstance(run, list) else run for table, run in json.loads(path.read_text()).items()}",
        "mixed_runs: expected refusal",
    ),
    (
        "selected_part_omitted",
        "dataset",
        "selected_run_and_record",
        "selected_files.append(name)",
        'if name.endswith("00001"):\n                continue\n            selected_files.append(name)',
        "selected_run: expected every part",
    ),
    (
        "duplicates_allowed",
        "dataset",
        "duplicate_facilities",
        'if audit["joined_rows"] == 0 or audit["duplicate_facilities"]:',
        'if audit["joined_rows"] == 0:',
        "duplicate_facilities: expected refusal",
    ),
    (
        "imputation_after_split",
        "model",
        "median_population",
        "imputation_source = frame[numerical_cols]",
        'imputation_source = importlib.import_module("sklearn.model_selection").train_test_split(frame[numerical_cols], test_size=0.20, random_state=42)[0]',
        "median_population: median must use every row",
    ),
    ("wrong_seed", "model", "split_membership", "test_size=0.20, random_state=42", "test_size=0.20, random_state=41", "split_membership: seed 42"),
    (
        "missing_clipping",
        "model",
        "clipping",
        "df_model[column] = df_model[column].clip(lower=lower, upper=upper)",
        "df_model[column] = df_model[column]",
        "clipping: outlier",
    ),
    (
        "last_reference",
        "model",
        "reference_levels",
        "encoded = pandas.get_dummies(df_model, columns=list(CATEGORICAL_COLUMNS), drop_first=True, dtype=int)",
        (
            "for column in CATEGORICAL_COLUMNS:\n"
            "        df_model[column] = pandas.Categorical(df_model[column], categories=sorted(df_model[column].dropna().unique(), reverse=True))\n"
            "    encoded = pandas.get_dummies(df_model, columns=list(CATEGORICAL_COLUMNS), drop_first=True, dtype=int)"
        ),
        "reference_levels: wrong omitted indicator",
    ),
)


def save(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, sort_keys=True, indent=2) + "\n")


def main() -> int:
    """Return success only when every defect is caught after a passing baseline and the source is unchanged."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--scope", choices=("dataset", "all"), default="all")
    args = parser.parse_args()
    output = args.output_dir.resolve()
    output.mkdir(parents=True, exist_ok=False)
    save(
        output / "retention.json",
        {
            "class": 3,
            "purpose": "Owned planted-defect copies and case outputs",
            "created": datetime.now(UTC).isoformat(),
            "end_condition": "The replication change is accepted",
        },
    )
    original = SOURCE.read_text()
    digest = hashlib.sha256(original.encode()).hexdigest()
    results = []
    try:
        for defect, scope, case, before, after, cause in DEFECTS:
            if args.scope == "dataset" and scope != "dataset":
                results.append({"defect": defect, "status": "pending", "cause": "requires the training image"})
                continue
            if original.count(before) != 1:
                raise ValueError(f"{defect}: expected exactly one production mutation site")
            baseline = output / (defect + "_baseline")
            prefix = ["-m", "scripts.training.run_capstone_replication_cases", "--scope", scope, "--case", case, "--output-dir"]
            result = run_command(sys.executable, [*prefix, str(baseline)], cwd=ROOT, timeout=180)
            save(output / (defect + "_baseline_command.json"), {"returncode": result.returncode, "stdout": result.stdout, "stderr": result.stderr})
            if result.returncode != 0:
                baseline_report = json.loads((baseline / "report.json").read_text())
                missing = any("training image required" in row.get("cause", "") for row in baseline_report["cases"])
                results.append(
                    {
                        "defect": defect,
                        "status": "blocked" if missing else "failed",
                        "cause": "baseline case did not pass",
                        "report": str(baseline / "report.json"),
                    }
                )
                continue
            mutant = output / (defect + ".py")
            mutant.write_text(original.replace(before, after))
            run = output / (defect + "_case")
            result = run_command(sys.executable, [*prefix, str(run), "--implementation", str(mutant)], cwd=ROOT, timeout=180)
            report = json.loads((run / "report.json").read_text())
            observed = any(row["case"] == case and row["status"] == "failed" and cause in row.get("cause", "") for row in report["cases"])
            save(output / (defect + "_command.json"), {"returncode": result.returncode, "stdout": result.stdout, "stderr": result.stderr})
            results.append(
                {
                    "defect": defect,
                    "status": "caught" if result.returncode != 0 and observed else "missed",
                    "case": case,
                    "expected_cause": cause,
                    "report": str(run / "report.json"),
                    "mutant_sha256": hashlib.sha256(mutant.read_bytes()).hexdigest(),
                }
            )
    except (ValueError, OSError, TimeoutExpired) as error:
        results.append({"defect": "runner", "status": "failed", "cause": str(error)})
    unchanged = hashlib.sha256(SOURCE.read_bytes()).hexdigest() == digest
    complete = unchanged and len(results) == len(DEFECTS) and all(row["status"] == "caught" for row in results)
    report = {
        "complete_gate": complete,
        "source_sha256": digest,
        "source_unchanged": unchanged,
        "scope": args.scope,
        "defects": results,
        "cleanup": "done",
        "limits": "Dataset scope is partial and exits nonzero. Only all caught defects pass. A blocked baseline never counts as a caught defect.",
    }
    save(output / "report.json", report)
    sys.stdout.write(json.dumps(report, sort_keys=True) + "\n")
    return 0 if complete else 1


if __name__ == "__main__":
    sys.exit(main())
