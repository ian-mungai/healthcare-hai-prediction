"""Exercise deletion CLI orchestration against synthetic files and an in-memory AWS boundary; never call AWS.

Run: PYTHONPATH=. .venv/bin/python scripts/acquisition/one_off/privacy_review/run_deletion_e2e.py
This integration E2E substitutes AWS calls, identity and configuration. The production dry run separately checks AWS.
"""

from __future__ import annotations

import contextlib
import hashlib
import importlib.util
import io
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import patch

from scripts.acquisition.s3_store import encoded_json, fingerprint, write_once
from scripts.process import CompletedProcess

HERE = Path(__file__).resolve().parent
TOOL = HERE / "delete_listed_versions.py"


def main() -> int:
    """Run the real entry point through deletion, replay and fail-closed scenarios on synthetic data."""
    root = Path("data/e2e/privacy_deletion") / datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    root.mkdir(parents=True)
    spec = importlib.util.spec_from_file_location("deletion_test_target", TOOL)
    if spec is None or spec.loader is None:
        raise RuntimeError("Missing deletion module")
    tool = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(tool)
    cases: list[dict[str, Any]] = []
    for scenario in (
        "delete_then_replay",
        "swapped_replacement",
        "wrong_dataset",
        "duplicate_target",
        "retirement_conflict",
        "absent_without_copy",
        "bad_list_hash",
    ):
        folder = root / scenario
        folder.mkdir()
        original_bytes, copy_bytes = b"Synthetic personal field", b"[REDACTED]"
        local = folder / "original.txt"
        local.write_bytes(original_bytes)
        digest, redacted_digest = (hashlib.sha256(body).hexdigest() for body in (original_bytes, copy_bytes))
        prefix = "example_pub/example_collection/datasets/example"
        key = f"{prefix}/capture_id=SYN/{digest}/data.txt"
        redacted_key = f"{prefix}_redacted/capture_id=SYN/{redacted_digest}/data.txt"
        copy = {"key": redacted_key, "version_id": "copy-version", "sha256": redacted_digest, "byte_count": len(copy_bytes)}
        entry: dict[str, Any] = {
            "key": key,
            "version_id": "original-version",
            "sha256": digest,
            "bytes": len(original_bytes),
            "local_path": str(local),
            "replacement": copy.copy(),
        }
        original = {name: entry[name] for name in ("key", "version_id", "sha256")} | {"byte_count": len(original_bytes)}
        replacements = {"replacements": [{"original": original, "redacted": copy.copy()}]}
        entries = [entry]
        retirement = folder / "retired.json"
        if scenario == "swapped_replacement":
            entry["replacement"]["version_id"] = "another-copy"
        if scenario == "wrong_dataset":
            entry["replacement"]["key"] = redacted_key.replace("example_redacted/", "another_redacted/")
            replacements["replacements"][0]["redacted"] = entry["replacement"].copy()
        if scenario == "duplicate_target":
            entries.append(entry.copy())
        if scenario == "retirement_conflict":
            retirement.write_bytes(encoded_json({"objects": []}))
        listing, replacement_file = folder / "list.json", folder / "replacements.json"
        listing.write_bytes(encoded_json({"objects": entries}))
        replacement_file.write_bytes(encoded_json(replacements))
        live = {(key, "original-version"): original_bytes, (redacted_key, "copy-version"): copy_bytes}
        if scenario == "absent_without_copy":
            live.clear()
        calls: dict[str, int] = {"delete": 0, "verify": 0}

        def live_version(_settings: dict[str, str], object_key: str, version: str, state: dict[tuple[str, str], bytes] = live) -> dict[str, int] | None:
            body = state.get((object_key, version))
            return None if body is None else {"ContentLength": len(body)}

        def verify(
            _client: Any, object_key: str, version: str, sha: str, size: int, state: dict[tuple[str, str], bytes] = live, counts: dict[str, int] = calls
        ) -> None:
            counts["verify"] += 1
            body = state.get((object_key, version))
            if body is None or hashlib.sha256(body).hexdigest() != sha or len(body) != size:
                raise tool.Stop("synthetic replacement hash failure")

        def aws(_settings: dict[str, str], *args: str, state: dict[tuple[str, str], bytes] = live, counts: dict[str, int] = calls) -> CompletedProcess[str]:
            if args[:2] != ("s3api", "delete-object"):
                raise RuntimeError("Unexpected AWS boundary call")
            object_key, version = args[args.index("--key") + 1], args[args.index("--version-id") + 1]
            state.pop((object_key, version), None)
            counts["delete"] += 1
            return CompletedProcess(list(args), 0, "{}", "")

        argv = [
            str(TOOL),
            "--execute",
            "--list",
            str(listing),
            "--retirement",
            str(retirement),
            "--require-replacements",
            "--replacements",
            str(replacement_file),
        ]
        argv += ["--list-sha256", "bad" if scenario == "bad_list_hash" else fingerprint(listing)[0], "--replacements-sha256", fingerprint(replacement_file)[0]]
        with (
            patch.object(tool, "HERE", folder),
            patch.object(tool, "load_configuration", return_value=({"data_bucket_name": "synthetic-bucket"}, {})),
            patch.object(tool, "verify_project_identity"),
            patch.object(tool, "approved_prefixes", return_value=[prefix]),
            patch.object(tool, "live_version", live_version),
            patch.object(tool, "verify_version", verify),
            patch.object(tool, "aws", aws),
            patch.object(tool, "remaining", side_effect=lambda _s, k, state=live: sum(item[0] == k for item in state)),
            patch.object(sys, "argv", argv),
            contextlib.redirect_stdout(io.StringIO()),
        ):
            code = tool.main()
            before = fingerprint(retirement) if scenario == "delete_then_replay" and retirement.exists() else None
            repeated = tool.main() if scenario == "delete_then_replay" else None
        success = (
            code == repeated == 0 and calls == {"delete": 1, "verify": 2} and before == fingerprint(retirement)
            if scenario == "delete_then_replay"
            else code == 1 and calls["delete"] == 0
        )
        cases.append({"case": scenario, "passed": success, "exit_code": code, "repeat_exit_code": repeated, "calls": calls})
    artifact = {
        "status": "passed" if all(case["passed"] for case in cases) else "failed",
        "cases": cases,
        "tool_sha256": fingerprint(TOOL)[0],
        "runner_sha256": fingerprint(Path(__file__))[0],
        "tested_boundary": "Deletion entry point, argument parsing, files, binding, iteration and retirement; AWS and identity substituted.",
        "untested": "Live deletion permissions; approved real deletion must be verified by readback.",
        "reproduce": "PYTHONPATH=. .venv/bin/python scripts/acquisition/one_off/privacy_review/run_deletion_e2e.py",
        "aws_calls": 0,
    }
    write_once(root / "artifact.json", encoded_json(artifact))
    sys.stdout.write(json.dumps({"status": artifact["status"], "cases": len(cases), "artifact": str(root / "artifact.json")}) + "\n")
    return 0 if artifact["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
