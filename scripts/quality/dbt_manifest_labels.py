"""Check the resolved dbt metadata for Confidential labels on research entries (decision 158, rule 9.1 approach).

Raw YAML cannot show what dbt resolves: model SQL ``config(meta=...)``, templated values, version blocks, test metadata and project-level ``+meta`` all
change the effective metadata. This check runs the pinned ``dbt parse`` offline on the reviewed tree and reads ``manifest.json``. It inspects every
``meta`` block of every node, source, seed, snapshot, test, unit test, exposure, metric, semantic model and saved query (and each column, version and
configuration they hold). An entry that is not named in ``outside`` may not resolve to ``classification: Confidential`` or carry confidential keys. Each
finding names the manifest ``unique_id``. ``outside`` lists the complete future sources outside the Public scope by name or ``unique_id``; the model
metadata check enforces those.

Run from the repository root: ``.venv/bin/python -m scripts.quality.dbt_manifest_labels``. It uses ``.tools/sqlfluff/bin/dbt`` (dbt-core 1.11.15 with
dbt-duckdb, the in-memory ``lint`` target) and installs the locked dbt packages into a scratch folder from the verified local archive, so no network,
catalog or credential is used and the repository is not written.
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
import tempfile
import time
from collections.abc import Collection, Iterator
from pathlib import Path
from typing import Any

from scripts.lakehouse import dbt_package_archive
from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]
DBT = ROOT / ".tools" / "sqlfluff" / "bin" / "dbt"
PROJECT = "dbt"
TARGET = "lint"
PARSE_SECONDS = 300
CONFIDENTIAL_KEYS = ("confidential_columns", "confidential_columns_reason", "proposed_confidential_columns")
OUTSIDE_PUBLIC_SCOPE: tuple[str, ...] = ()
NOT_ENTRIES = frozenset({"metadata", "child_map", "parent_map", "group_map", "docs", "selectors", "files", "flat_graph"})


class ManifestError(RuntimeError):
    """dbt could not parse the tree, so no manifest exists to check."""


def confidential_markers(block: dict[str, Any]) -> list[str]:
    """The Confidential label or confidential keys in one metadata block."""
    classification = block.get("classification")
    found = ["classification Confidential"] if isinstance(classification, str) and classification.strip().lower() == "confidential" else []
    return found + [key for key in CONFIDENTIAL_KEYS if key in block]


def meta_findings(node: Any, trail: str = "") -> Iterator[str]:
    """Every Confidential marker in every ``meta`` block below a manifest entry, with the path of the block."""
    if isinstance(node, list):
        for index, item in enumerate(node):
            yield from meta_findings(item, f"{trail}[{index}]")
    elif isinstance(node, dict):
        for key, value in node.items():
            where = f"{trail}.{key}" if trail else key
            if key == "meta" and isinstance(value, dict):
                yield from (f"{where} {marker}" for marker in confidential_markers(value))
            else:
                yield from meta_findings(value, where)


def manifest_entries(manifest: dict[str, Any]) -> Iterator[tuple[str, dict[str, Any]]]:
    """Every resource of the manifest as (unique_id, entry): any top-level section whose values hold a ``unique_id``, disabled resources included."""
    for section, value in manifest.items():
        if section in NOT_ENTRIES or not isinstance(value, dict):
            continue
        for key, entry in value.items():
            for item in entry if isinstance(entry, list) else [entry]:
                if isinstance(item, dict) and "unique_id" in item:
                    yield str(item["unique_id"] or key), item


def names_of(unique_id: str, entry: dict[str, Any]) -> set[str]:
    """The names that can put one entry in the outside list: its unique_id, its name and the dotted source table name."""
    names = {unique_id}
    name = entry.get("name")
    if isinstance(name, str):
        names.add(name)
        if isinstance(entry.get("source_name"), str):
            names.add(f"{entry['source_name']}.{name}")
    return names


def tested_model(entry: dict[str, Any]) -> str | None:
    """The unique_id of the model a unit test tests: the dependency node whose name is the unit test's model, or None for other entries."""
    model = entry.get("model")
    nodes = (entry.get("depends_on") or {}).get("nodes") or []
    matches = [node for node in nodes if isinstance(node, str) and node.startswith("model.") and node.split(".")[-1] == model]
    return matches[0] if model and len(matches) == 1 else None


def manifest_problems(manifest: dict[str, Any], outside: Collection[str] = OUTSIDE_PUBLIC_SCOPE) -> list[str]:
    """One line per manifest entry that resolves a Confidential label or confidential key and is not excepted; each line starts with the unique_id."""
    entries = list(manifest_entries(manifest))
    excepted = {unique_id for unique_id, entry in entries if names_of(unique_id, entry) & set(outside)}
    problems = []
    for unique_id, entry in entries:
        if unique_id in excepted or entry.get("attached_node") in excepted or entry.get("model") in outside or tested_model(entry) in excepted:
            continue
        findings = list(dict.fromkeys(meta_findings(entry)))
        if findings:
            problems.append(f"{unique_id}: {'; '.join(findings)}")
    return sorted(problems)


def parse_environment(work: Path, packages: Path | None, profiles: Path) -> dict[str, str]:
    """The environment of one parse: the host environment without dbt variables, and every dbt output redirected into the scratch folder."""
    environment = {key: value for key, value in os.environ.items() if not key.startswith("DBT_")}
    environment.update(
        DBT_SEND_ANONYMOUS_USAGE_STATS="false", DBT_PROFILES_DIR=str(profiles), DBT_TARGET_PATH=str(work / "target"), DBT_LOG_PATH=str(work / "logs")
    )
    if packages is not None:
        environment["DBT_PACKAGES_INSTALL_PATH"] = str(packages)
    return environment


def parse_manifest(project_dir: Path, profiles: Path, work: Path, packages: Path | None = None) -> dict[str, Any]:
    """Run the pinned ``dbt parse`` on one project and return its manifest; a failed or missing parse raises ManifestError."""
    if not DBT.exists():
        raise ManifestError("dbt is missing: run .venv/bin/python -m scripts.quality.install_sqlfluff")
    arguments = ["parse", "--project-dir", str(project_dir), "--target", TARGET, "--no-partial-parse", "--no-use-colors", "--no-send-anonymous-usage-stats"]
    result = run_command(str(DBT), arguments, cwd=work, env=parse_environment(work, packages, profiles), timeout=PARSE_SECONDS)
    manifest = work / "target" / "manifest.json"
    if result.returncode != 0 or not manifest.exists():
        raise ManifestError(f"dbt parse failed with exit {result.returncode}: {(result.stdout + result.stderr)[-1500:]}")
    return json.loads(manifest.read_text())


def project_problems(files: dict[str, str], outside: Collection[str] = OUTSIDE_PUBLIC_SCOPE) -> list[str]:
    """Write a synthetic dbt project (dbt_project.yml and profiles.yml included), parse it and check its manifest."""
    with tempfile.TemporaryDirectory(prefix="hai_manifest_labels_") as scratch:
        work = Path(scratch)
        project = work / "project"
        for relative, text in files.items():
            path = project / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text)
        return manifest_problems(parse_manifest(project, project, work), outside)


def install_packages(root: Path, destination: Path) -> None:
    """Install the locked dbt packages into a scratch folder from the verified local archive; no network and no write to the repository."""
    record = root / dbt_package_archive.RECORD
    archive = dbt_package_archive.verify(root)
    dbt_package_archive.install(root, root / dbt_package_archive.LOCK, record, archive, destination, hashlib.sha256(record.read_bytes()).hexdigest())


def repository_problems(root: Path = ROOT, outside: Collection[str] = OUTSIDE_PUBLIC_SCOPE) -> tuple[list[str], float]:
    """Parse the repository's own dbt project with its locked packages and check the manifest; return the findings and the parse seconds."""
    with tempfile.TemporaryDirectory(prefix="hai_manifest_labels_") as scratch:
        work = Path(scratch)
        install_packages(root, work / "dbt_packages")
        began = time.monotonic()
        manifest = parse_manifest(root / PROJECT, root / PROJECT, work, work / "dbt_packages")
        seconds = time.monotonic() - began
    return manifest_problems(manifest, outside), seconds


def main() -> int:
    """Check the repository's resolved dbt metadata; exit 1 on a finding or a failed parse."""
    try:
        problems, seconds = repository_problems()
    except (ManifestError, dbt_package_archive.ArchiveError, OSError) as error:
        sys.stderr.write(f"dbt manifest labels: cannot check: {error}\n")
        return 2
    for line in problems:
        sys.stdout.write(f"{line}\n")
    sys.stdout.write(f"dbt manifest labels: {'fail' if problems else 'ok'} ({len(problems)} findings, dbt parse {seconds:.1f} s)\n")
    return int(bool(problems))


if __name__ == "__main__":
    sys.exit(main())
