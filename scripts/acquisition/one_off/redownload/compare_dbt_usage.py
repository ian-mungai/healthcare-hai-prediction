"""Compare dbt's own manifest with the text-based bronze usage that build_download_manifest.py reads from the model files.

The manifest names each bronze source and the models that depend on it (``child_map``, followed through every descendant model). The
text reading names each bronze table and the models that read it. The run 3 removal list rests on one question per table, whether any
dbt model reads it, so the check fails when the two disagree on that set. Reader differences per table are listed for review only: a
model that names a table as a filter value counts for the text reading and not for dbt.

Either give a manifest made elsewhere (``--manifest``) or let this script parse the tree with the project's pinned, offline dbt
(``--parse``: ``.tools/sqlfluff/bin/dbt``, the in-memory ``lint`` target, the locked packages installed from the verified local archive
into a scratch folder, as scripts.quality.dbt_manifest_labels does). No network, catalog or credential is used.

Usage (repository root):
    git archive <commit> dbt config/lakehouse/bronze_tables.json | tar -x -C <new folder>
    .venv/bin/python -m scripts.acquisition.one_off.redownload.compare_dbt_usage --dbt-tree <new folder> --dbt-commit <commit> --parse \
        --report <new report.json>
Exit 0 when the used-table sets match, 1 when they differ, 2 when dbt cannot parse.
"""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
from pathlib import Path

from scripts.acquisition.one_off.redownload.build_download_manifest import METADATA_SOURCES, dbt_usage
from scripts.quality import dbt_manifest_labels as labels

ROOT = Path(__file__).resolve().parents[4]


def parsed_manifest(tree: Path) -> dict:
    """Parse the unpacked tree with the pinned dbt and the locked packages, all in a scratch folder."""
    with tempfile.TemporaryDirectory(prefix="hai_dbt_usage_") as scratch:
        work = Path(scratch)
        labels.install_packages(ROOT, work / "dbt_packages")
        return labels.parse_manifest(tree / "dbt", tree / "dbt", work, work / "dbt_packages")


def manifest_usage(manifest: dict) -> dict[str, list[str]]:
    """Each bronze source table and every model that depends on it, directly or through other models."""
    children = manifest["child_map"]
    usage: dict[str, list[str]] = {}
    for unique_id, source in manifest["sources"].items():
        if source["source_name"] != "bronze" or source["name"] in METADATA_SOURCES:
            continue
        seen: set[str] = set()
        stack = [child for child in children.get(unique_id, []) if child.startswith("model.")]
        while stack:
            node = stack.pop()
            if node in seen:
                continue
            seen.add(node)
            stack.extend(child for child in children.get(node, []) if child.startswith("model.") and child not in seen)
        usage[source["name"]] = sorted(node.rsplit(".", 1)[-1] for node in seen)
    return usage


def compare(manifest: dict, tree: Path, commit: str) -> dict:
    from_dbt = manifest_usage(manifest)
    from_text = {table: entry["readers"] for table, entry in dbt_usage(tree, commit)["tables"].items()}
    used_dbt = {table for table, models in from_dbt.items() if models}
    used_text = {table for table, readers in from_text.items() if readers}
    direct = {table: sorted(set(from_text.get(table, [])) - set(from_dbt.get(table, []))) for table in sorted(used_dbt | used_text)}
    return {
        "dbt_commit": commit,
        "used_tables_match": used_dbt == used_text,
        "used_by_dbt_only": sorted(used_dbt - used_text),
        "used_by_text_only": sorted(used_text - used_dbt),
        "declared_but_unused_in_dbt": sorted(set(from_dbt) - used_dbt),
        "text_readers_dbt_does_not_list": {table: models for table, models in direct.items() if models},
        "counts": {"dbt_sources": len(from_dbt), "used_by_dbt": len(used_dbt), "used_by_text": len(used_text)},
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dbt-tree", type=Path, required=True)
    parser.add_argument("--dbt-commit", required=True)
    chosen = parser.add_mutually_exclusive_group(required=True)
    chosen.add_argument("--manifest", type=Path, help="a manifest.json that dbt wrote for the same tree")
    chosen.add_argument("--parse", action="store_true", help="parse the tree here with the pinned offline dbt")
    parser.add_argument("--report", type=Path, required=True, help="a new JSON file for the comparison")
    args = parser.parse_args()
    if args.report.exists():
        parser.error(f"{args.report} exists; name a new file")
    try:
        manifest = parsed_manifest(args.dbt_tree) if args.parse else json.loads(args.manifest.read_text(encoding="utf-8"))
    except (labels.ManifestError, OSError, ValueError) as error:
        sys.stderr.write(f"dbt usage comparison: cannot parse: {error}\n")
        return 2
    report = compare(manifest, args.dbt_tree, args.dbt_commit)
    args.report.write_text(json.dumps(report, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    sys.stdout.write(json.dumps({"used_tables_match": report["used_tables_match"], **report["counts"]}) + "\n")
    return 0 if report["used_tables_match"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
