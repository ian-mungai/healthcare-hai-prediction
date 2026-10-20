"""Compare saved file metadata with the single-copy reader SQL without reading source data rows."""

from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from jinja2 import Environment, StrictUndefined, select_autoescape

from scripts.lakehouse import bronze_metadata
from scripts.lakehouse.duckdb_cli import duckdb_binary
from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]
MODELS = (
    "stg_bronze__metadata",
    "stg_bronze__release_observations",
    "stg_bronze__physical_object_index",
    "stg_bronze__file_copies",
    "stg_bronze__file_labels",
    "stg_bronze__files",
    "dim_release",
    "dim_source",
)
INPUT_SQL = "metadata_input as (select * from read_json_auto('{path}'))"
LOADED_SQL = "with loaded as (select bronze_table, object_key, checksum_count, row_count from main.stg_bronze__file_copies where is_canonical),\n{sql}"
SELECT_SQL = "select {columns} from {relation}"
DIFFERENCE_SQL = "select '{name}' as check_name, count(*) as observed from (({before} except all {after}) union all ({after} except all {before})) as delta"
METADATA_SQL = "SELECT * FROM stg_bronze__file_copies ORDER BY bronze_table, object_key"


def query(database: Path, sql: str) -> list[dict[str, Any]]:
    """Run one metadata-only query on the retained file with the required read-only DuckDB executable."""
    result = run_command(str(duckdb_binary()), ["-readonly", str(database), "-json", "-init", "/dev/null", "-c", sql], timeout=60, check=True)
    return list(json.loads(result.stdout))


def retained_generation(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Project saved copy metadata into a comparison generation with explicitly synthetic manifest and size fields."""
    observations = []
    for row in rows:
        identifier = hashlib.sha256(json.dumps([row["bronze_table"], row["object_key"]]).encode()).hexdigest()
        observations.append(
            {
                "table": row["bronze_table"],
                "observation_id": identifier,
                "object_key": row["object_key"],
                "sha256": row["member_sha256"],
                "bucket": "comparison-bucket-not-inspected",
                "key": row["s3_key"],
                "version_id": row["s3_version_id"],
                "source_id": "comparison",
                "snapshot_id": row["snapshot_id"],
                "dataset_id": row["dataset_id"],
                "release_id": row["release_id"],
                "release_partition": "comparison",
                "manifest_key": f"comparison/{identifier}",
                "manifest_version_id": "comparison",
                "manifest_sha256": "0" * 64,
                "member_chain": row["member_path"].split("!"),
                "file_name": row["file_name"],
                "byte_count": 0,
                "retired": row["is_retired"],
                "is_canonical": row["is_canonical"],
            }
        )
    return bronze_metadata.build(
        {row["table"] for row in observations},
        [row for row in observations if row["is_canonical"]],
        [{"copy": row} for row in observations if not row["is_canonical"]],
        [],
        {row["table"] for row in observations},
    )


def render_models(metadata: Path) -> tuple[str, dict[str, str]]:
    """Render the changed reader models while replacing data-row aggregation with retained metadata summaries."""
    environment = Environment(undefined=StrictUndefined, extensions=["jinja2.ext.do"], autoescape=select_autoescape(default_for_string=False))
    config = importlib.import_module("yaml").safe_load((ROOT / "dbt/dbt_project.yml").read_text())
    variables = config["vars"]
    context: dict[str, Any] = {
        "config": lambda **_kwargs: "",
        "ref": lambda name: "after_" + name if name in MODELS else f'main."{name}"',
        "var": lambda name: variables[name],
        "source": lambda _schema, _table: "metadata_input",
        "target": SimpleNamespace(name="fixture"),
        "env_var": lambda _name, default="": default,
    }
    context["file_metadata_source"] = lambda: "metadata_input"
    context["assert_file_metadata_generation"] = lambda: ""
    fragments = [INPUT_SQL.format(path=str(metadata).replace("'", "''"))]
    hashes = {}
    for name in MODELS:
        folder = "gold/dimensions" if name.startswith("dim_") else "silver/staging"
        path = ROOT / f"dbt/models/{folder}/{name}.sql"
        text = path.read_text()
        hashes[str(path.relative_to(ROOT))] = hashlib.sha256(text.encode()).hexdigest()
        if name == "stg_bronze__file_copies":
            text = LOADED_SQL.format(sql=text[text.index("copies as (") :])
        rendered = environment.from_string(text).render(**context).strip()
        fragments.append("after_" + name + " as (" + rendered + ")")
    return "with " + ",\n".join(fragments), hashes


def comparison_sql(prefix: str) -> str:
    """Compare observation identities, labels, keys and dates through the actual changed reader SQL."""
    checks = []

    def difference(name: str, before: str, after: str) -> None:
        checks.append(DIFFERENCE_SQL.format(name=name, before=before, after=after))

    identities = (
        "bronze_table, object_key, member_sha256, s3_key, s3_version_id, snapshot_id, dataset_id, release_id, "
        "member_path, file_name, is_retired, publication_release"
    )
    difference(
        "observation_identity_differences",
        SELECT_SQL.format(columns=identities, relation="main.stg_bronze__file_copies"),
        SELECT_SQL.format(columns=identities, relation="after_stg_bronze__release_observations"),
    )
    difference("dataset_identity_differences", "select source_sk, dataset_id from main.dim_source", "select source_sk, dataset_id from after_dim_source")
    difference(
        "canonical_release_differences",
        "select release_sk, member_sha256, object_key, release_date from main.dim_release",
        "select release_sk, member_sha256, object_key, release_date from after_dim_release",
    )
    fields = "bronze_table, member_sha256, canonical_object_key, publication_releases, latest_publication_date, file_names, has_label_conflict, is_label_held"
    difference(
        "file_metadata_differences",
        SELECT_SQL.format(columns=fields, relation="main.stg_bronze__files"),
        SELECT_SQL.format(columns=fields, relation="after_stg_bronze__files"),
    )
    labels = (
        "bronze_table, object_key, member_sha256, file_name, label_source, family, role, rule_fiscal_year, "
        "data_fiscal_year, identity_year, rule_stage, is_label_checksum_match"
    )
    difference(
        "ipps_label_differences",
        SELECT_SQL.format(columns=labels, relation="main.stg_bronze__file_labels"),
        SELECT_SQL.format(columns=labels, relation="after_stg_bronze__file_labels"),
    )
    before_lists = (
        "select member_sha256, list_sort(list_distinct(list(try_cast(publication_release as date)))) as "
        "publication_dates, list_sort(list_distinct(list(case when regexp_full_match(publication_release, "
        "'.+__[0-9]{8}T[0-9]{6}Z__[a-f0-9]{32}') then publication_release end))) as download_run_ids from "
        "main.stg_bronze__file_copies group by member_sha256"
    )
    difference(
        "typed_release_list_differences",
        before_lists,
        "select member_sha256, publication_dates, download_run_ids from after_dim_release where member_sha256 != '<unknown>'",
    )
    checks += [
        "select 'observation_count' as check_name, count(*) as observed from after_stg_bronze__release_observations",
        "select 'physical_copy_count' as check_name, count(*) as observed from after_stg_bronze__file_copies",
        "select 'retired_observation_count' as check_name, count(*) as observed from after_stg_bronze__release_observations where is_retired",
        (
            "select 'latest_dates_lost_by_retirement_filter' as check_name, count(*) as observed from "
            "main.stg_bronze__files as files inner join (select bronze_table, member_sha256, "
            "max(try_cast(publication_release as date)) as latest_date from main.stg_bronze__file_copies where not "
            "is_retired group by bronze_table, member_sha256) as live using (bronze_table, member_sha256) where "
            "files.latest_publication_date is distinct from live.latest_date"
        ),
        (
            "select 'care_compare_latest_date_differences' as check_name, count(*) as observed from main.stg_bronze__files "
            "as before_files inner join after_stg_bronze__files as after_files using (bronze_table, member_sha256) where "
            "before_files.source_family = 'care_compare' and before_files.latest_publication_date is distinct from "
            "after_files.latest_publication_date"
        ),
    ]
    return prefix + "\n" + "\nunion all\n".join(checks)


def main() -> int:
    """Write counts-only comparison evidence and retain its metadata-only input projection for review."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--retained", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--mutant", choices=("drop_retired",))
    args = parser.parse_args()
    args.report.parent.mkdir(parents=True, exist_ok=True)
    rows = query(args.retained, METADATA_SQL)
    metadata = args.report.with_suffix(".metadata.json")
    generation = retained_generation(rows)
    if args.mutant == "drop_retired":
        generation = [row for row in generation if not row["retired"]]
    metadata.write_text(json.dumps(generation, sort_keys=True) + "\n")
    prefix, hashes = render_models(metadata)
    sql = comparison_sql(prefix)
    args.report.with_suffix(".sql").write_text(sql + ";\n")
    metrics = {row["check_name"]: row["observed"] for row in query(args.retained, sql)}
    expected = {key: 0 for key in metrics if key.endswith("differences")}
    expected.update(observation_count=3046, physical_copy_count=2870, retired_observation_count=157, latest_dates_lost_by_retirement_filter=29)
    passed = all(metrics[key] == value for key, value in expected.items())
    report = {
        "retained": str(args.retained),
        "read_only": True,
        "mutant": args.mutant,
        "scope": "file metadata only, no source data rows",
        "models_sha256": hashes,
        "source_sha256": {
            str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest() for path in (Path(__file__).resolve(), Path(bronze_metadata.__file__))
        },
        "metadata_projection_sha256": hashlib.sha256(metadata.read_bytes()).hexdigest(),
        "metrics": metrics,
        "expected": expected,
        "passed": passed,
        "limits": (
            "Manifest identities, bucket and byte counts absent from the retained copy model are synthetic comparison "
            "fields. This is reader preservation evidence, not a live bootstrap or manifest reload."
        ),
    }
    args.report.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    sys.stdout.write(json.dumps({"report": str(args.report), "passed": passed, "metrics": metrics}) + "\n")
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
