"""Synthetic real-process checks for CSV abstraction; no publisher data or AWS calls."""

from __future__ import annotations

import argparse
import copy
import csv
import hashlib
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

from scripts.acquisition.cli_tools import run_argv
from scripts.acquisition.s3_store import encoded_json, fingerprint, write_once
from scripts.acquisition.source_registry import canonical_hash


def main() -> None:
    """Run the command-line workflow with the supplied arguments and report its outcome."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    root = args.output.resolve()
    root.mkdir(parents=True, exist_ok=False)
    url = "https://example.org/public/business.csv"
    headers = ["record_id", "year", "person_name", "phone", "address", "description", "amount"]
    raw = root / "sample.csv"
    with raw.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.writer(handle)
        writer.writerow(headers)
        writer.writerows(
            [
                ["00001", "2020", "Sample Person Alpha", "555-0100", "10 Sample Lane", "Beds, staffed", "001.20"],
                ["00002", "2021", "", "", "", "Contact SAMPLE  PERSON\nALPHA; 555-0100", "-999"],
                ["00003", "2022", "Sample Person Beta", "", "", "Retain\nsecond line", ""],
            ]
        )
    digest, size = fingerprint(raw)
    policy = {
        "policy_version": 1,
        "approved": True,
        "source_url": url,
        "input_sha256": digest,
        "max_bytes": 1024**2,
        "expected_headers": headers,
        "expected_row_count": 3,
        "redact_fields": ["person_name", "phone", "address"],
        "scrub_text_fields": ["description"],
        "replacement": "[REDACTED]",
        "s3_retention": "abstracted_only",
    }
    cases = []

    def run(name: str, selected: dict, source: Path, source_url: str = url, success: bool = False, output: Path | None = None) -> Path:
        """Exercise one synthetic CLI case and record its exit and disclosure checks."""
        case = root / name
        case.mkdir()
        policy_path = case / "policy.json"
        write_once(policy_path, encoded_json(selected))
        destination = output or case / "result"
        argv = [
            sys.executable,
            "-m",
            "scripts.acquisition.abstract_public_business_csv",
            "--policy",
            str(policy_path),
            "--source-url",
            source_url,
            "--input",
            str(source),
            "--output",
            str(destination),
        ]
        result = run_argv(argv, capture_output=True, text=True, timeout=60)
        passed = (result.returncode == 0) if success else (result.returncode != 0)
        output_text = result.stdout + result.stderr
        passed = passed and all(value not in output_text for value in ["Sample Person Alpha", "Sample Person Beta", "555-0100", "10 Sample Lane"])
        if not success:
            passed = passed and not destination.exists()
        cases.append(
            {
                "case": name,
                "passed": passed,
                "expected_returncode": "zero" if success else "nonzero",
                "returncode": result.returncode,
                "stdout_sha256": hashlib.sha256(result.stdout.encode()).hexdigest(),
                "stderr_sha256": hashlib.sha256(result.stderr.encode()).hexdigest(),
            }
        )
        if not passed:
            raise AssertionError("Synthetic workflow case failed: " + name)
        return destination

    status = "failed"
    try:
        result = run("valid", policy, raw, success=True)
        with (result / "data.csv").open(newline="", encoding="utf-8") as handle:
            rows = list(csv.reader(handle))
        if not (rows[0] == headers and len(rows) == 4):
            raise AssertionError("Synthetic output invariant failed.")
        if not (rows[1] == ["00001", "2020", "[REDACTED]", "[REDACTED]", "[REDACTED]", "Beds, staffed", "001.20"]):
            raise AssertionError("Synthetic output invariant failed.")
        if not (rows[2] == ["00002", "2021", "", "", "", "contact [REDACTED]; [REDACTED]", "-999"]):
            raise AssertionError("Synthetic output invariant failed.")
        if not (rows[3][-2:] == ["Retain\nsecond line", ""]):
            raise AssertionError("Synthetic output invariant failed.")
        manifest = json.loads((result / "ingestion_manifest.json").read_text())
        if not (manifest["raw_sha256"] == digest and manifest["output_sha256"] == fingerprint(result / "data.csv")[0]):
            raise AssertionError("Synthetic output invariant failed.")
        if not (manifest["model_eligible"] is False and manifest["original_unchanged"] is False):
            raise AssertionError("Synthetic output invariant failed.")
        if not (manifest["cross_field_redacted_cells"] == 1 and manifest["unreviewed_free_text_pii_possible"] is True):
            raise AssertionError("Synthetic output invariant failed.")
        before = {p.name: fingerprint(p) for p in result.iterdir()}
        run("replay", policy, raw, success=True, output=result)
        if not (before == {p.name: fingerprint(p) for p in result.iterdir()} and fingerprint(raw) == (digest, size)):
            raise AssertionError("Synthetic output invariant failed.")
        run("wrong_route", policy, raw, source_url="https://example.org/other.csv")
        invalid_policies: list[tuple[str, dict[str, object]]] = [
            ("not_approved", {"approved": False}),
            ("hash_mismatch", {"input_sha256": "0" * 64}),
            ("oversized", {"max_bytes": 1}),
            ("missing_redaction", {"redact_fields": []}),
            ("unknown_redaction_field", {"redact_fields": ["missing"]}),
            ("wrong_count", {"expected_row_count": 2}),
            ("unsafe_retention", {"s3_retention": "raw_public"}),
            ("unsafe_marker", {"replacement": "Sample Person Alpha"}),
        ]
        for name, changes in invalid_policies:
            run(name, {**policy, **changes}, raw)
        for name, payload in [
            ("column_drift", b"record_id,new_person_name\n1,Example\n"),
            ("duplicate_column", b"record_id,record_id\n1,2\n"),
            ("invalid_utf8", b"\xff"),
            ("bad_row", (",".join(headers) + "\n1,2\n").encode()),
            ("malformed_quote", (",".join(headers) + '\n"unfinished').encode()),
        ]:
            source = root / (name + ".csv")
            write_once(source, payload)
            changed = copy.deepcopy(policy)
            changed["input_sha256"] = fingerprint(source)[0]
            run(name, changed, source)
        cases.append({"case": "raw_unchanged_and_retained_tokens_preserved", "passed": fingerprint(raw) == (digest, size)})
        status = "passed"
    finally:
        files = [{"path": str(p.relative_to(root)), "sha256": fingerprint(p)[0]} for p in sorted(root.rglob("*")) if p.is_file()]
        artifact = {
            "kind": "synthetic_csv_ingestion_process_e2e",
            "status": status,
            "completed_at_utc": datetime.now(UTC).isoformat(),
            "python_version": sys.version,
            "platform": sys.platform,
            "requirements_sha256": fingerprint(Path(__file__).resolve().parents[2] / "requirements.txt")[0],
            "project_configuration_sha256": fingerprint(Path(__file__).resolve().parents[2] / "pyproject.toml")[0],
            "runner_sha256": fingerprint(Path(__file__))[0],
            "cases": cases,
            "files": files,
            "fixture_scope": "Generic synthetic data only",
            "network_calls": 0,
            "aws_calls": 0,
            "implementation_sha256": fingerprint(Path(__file__).with_name("abstract_public_business_csv.py"))[0]
            if Path(__file__).with_name("abstract_public_business_csv.py").exists()
            else None,
            "reproduce": [sys.executable, "-m", "scripts.acquisition.run_privacy_e2e", "--output", "<new_directory>"],
            "prerequisites": ["Run from the repository root with the project .venv and pinned requirements.txt dependencies."],
            "reset": "Use a new output directory; existing evidence is preserved.",
            "tested_boundary": "Synthetic CSV files through the actual abstraction CLI subprocess to output files and manifests.",
            "untested_boundaries": ["Publisher retrieval", "AWS identity and S3 storage", "Unknown free-text personal information"],
            "cleanup": "Synthetic fixtures and verification artifacts retained locally; no cloud resources created.",
        }
        write_once(root / "artifact.json", encoded_json(artifact))
        sys.stdout.write(
            str(json.dumps({"status": status, "cases": len(cases), "artifact": str(root / "artifact.json"), "sha256": canonical_hash(artifact)})) + "\n"
        )


if __name__ == "__main__":
    main()
