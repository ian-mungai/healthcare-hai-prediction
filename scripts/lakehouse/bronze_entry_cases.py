"""Bronze cases through the real entry point: ``python -m scripts.lakehouse.bronze <arguments>`` in a child process.

The in-process cases import ``bronze`` as ``scripts.lakehouse.bronze``. The command line runs the same file as ``__main__``, and ``bronze_pin``,
``unit_records`` and ``dictionary`` then import a second copy of it, so ``BronzeError`` is two classes: the one that ``main`` catches and the one that these
modules raise. These cases start the command as it is started and put the service stand-ins under it with ``bronze_entry_stand_in`` (a
``sitecustomize.py`` that replaces only ``boto3`` and the Spark session). The child's exit code and output are the evidence. ``planted_guard`` loads the
file with its old ``__main__`` guard in the child through ``BRONZE_ENTRY_SOURCE``.
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

from scripts.lakehouse.bronze_entry_stand_in import RESULT, SCENARIO, SOURCE
from scripts.process import CompletedProcess, run_command

REPO_ROOT = Path(__file__).resolve().parents[2]
SOURCE_FILE = Path(__file__).with_name("bronze.py")
SITE = "from scripts.lakehouse.bronze_entry_stand_in import install\n\ninstall()\n"
TABLE = {"table": "files", "collection": "cms/synthetic", "roles": ["data"], "format": "csv", "encodings": ["utf-8"], "member_pattern": r"\.csv$"}
MANIFEST_KEY = "cms/synthetic/manifests/first/manifest.json"
GUARD = ("    from scripts.lakehouse.bronze import main as packaged_main\n\n    raise SystemExit(packaged_main())", "    raise SystemExit(main())")
CHILD_SECONDS = 120


def manifest_text() -> str:
    key = "cms/synthetic/datasets/files/release_date=2026-01-01/" + "a" * 64 + "/file.csv"
    stored = {
        "role": "data",
        "dataset_id": "files",
        "member_chain": ["file.csv"],
        "object": {"bucket": "synthetic", "key": key, "version_id": "v1", "sha256": "a" * 64, "byte_count": 17},
    }
    return json.dumps({"snapshot_id": "first", "source_id": "synthetic", "release_id": "2026-01-01", "objects": [stored]})


def entry(inventory: dict[str, Any], source: Path | None = None) -> tuple[CompletedProcess[str], dict[str, Any]]:
    """Run ``python -m scripts.lakehouse.bronze --tables files --inventory <file>`` against the stand-ins; the answer is the finished child and its end
    state. ``source`` is a file that loads in place of ``bronze.py``."""
    with tempfile.TemporaryDirectory(prefix="bronze_entry_") as directory:
        root = Path(directory)
        site, work = root / "site", root / "work"
        site.mkdir()
        (site / "sitecustomize.py").write_text(SITE, encoding="utf-8")
        for name, text in (
            ("config/lakehouse/bronze_tables.json", json.dumps({"namespace": "bronze", "tables": [TABLE]})),
            ("infra/deployment.auto.tfvars.json", json.dumps({"data_bucket_name": "synthetic"})),
            ("inventory.json", json.dumps(inventory)),
        ):
            (work / name).parent.mkdir(parents=True, exist_ok=True)
            (work / name).write_text(text, encoding="utf-8")
        environment = {
            **os.environ,
            "PYTHONPATH": f"{site}{os.pathsep}{REPO_ROOT}",
            "AWS_PROFILE": "stand_in",
            "AWS_REGION": "us-east-1",
            SCENARIO: json.dumps({"objects": [{"key": MANIFEST_KEY, "text": manifest_text()}]}),
            RESULT: str(root / "result.json"),
        }
        if source is not None:
            environment[SOURCE] = str(source)
        arguments = ["-m", "scripts.lakehouse.bronze", "--tables", "files", "--inventory", "inventory.json"]
        finished = run_command(sys.executable, arguments, cwd=work, env=environment, timeout=CHILD_SECONDS)
        state = json.loads((root / "result.json").read_text(encoding="utf-8")) if (root / "result.json").exists() else {}
    return finished, state


def inventory(manifest_sha256: str) -> dict[str, Any]:
    return {"version": 1, "tables": ["files"], "manifests": [[MANIFEST_KEY, "v1", manifest_sha256]], "objects": [], "documents": []}


def refused_by_name(finished: CompletedProcess[str], state: dict[str, Any], reason: str) -> dict[str, bool]:
    return {
        "the command exits 1": finished.returncode == 1,
        "the refusal is a named line": f"bronze: {MANIFEST_KEY}: {reason}" in finished.stderr,
        "no traceback": "Traceback" not in finished.stderr,
        "the Spark session was stopped": state.get("spark_stops") == 1,
    }


def pinned_manifest_differs(source: Path | None = None) -> dict[str, bool]:
    """``PinnedStorage`` (``bronze_pin``) raises its copy's ``BronzeError`` during discovery, inside the ``try`` of ``main``."""
    finished, state = entry(inventory("0" * 64), source)
    return refused_by_name(finished, state, "the manifest bytes differ from the pinned inventory")


def pinned_replay_differs(source: Path | None = None) -> dict[str, bool]:
    """``verify_replay`` (``bronze_pin``) raises its copy's ``BronzeError`` after discovery, inside the ``try`` of ``main``."""
    finished, state = entry(inventory(hashlib.sha256(manifest_text().encode()).hexdigest()), source)
    return {
        "the command exits 1": finished.returncode == 1,
        "the refusal is a named line": "bronze: the replayed discovery differs from the pinned source inventory" in finished.stderr,
        "no traceback": "Traceback" not in finished.stderr,
        "the Spark session was stopped": state.get("spark_stops") == 1,
    }


def planted_guard() -> dict[str, bool]:
    """The file with its old ``__main__`` guard (``raise SystemExit(main())``) fails both cases above."""
    text = SOURCE_FILE.read_text(encoding="utf-8")
    old, new = GUARD
    results = {"the guard to plant is in the file": text.count(old) == 1}
    with tempfile.TemporaryDirectory(prefix="bronze_entry_plant_") as directory:
        planted = Path(directory) / "bronze_planted.py"
        planted.write_text(text.replace(old, new), encoding="utf-8")
        for name, case in (("a manifest that differs from the pin", pinned_manifest_differs), ("a replay that differs from the pin", pinned_replay_differs)):
            results[f"the old guard fails {name}"] = not all(case(planted).values())
    return results


CASES: dict[str, Callable[[], dict[str, bool]]] = {
    "entry_pinned_manifest_differs": pinned_manifest_differs,
    "entry_pinned_replay_differs": pinned_replay_differs,
    "entry_old_guard_is_caught": planted_guard,
}
