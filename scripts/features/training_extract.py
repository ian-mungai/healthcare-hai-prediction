"""Write deterministic training inputs from one consistently read feature publication."""

from __future__ import annotations

import argparse
import contextlib
import csv
import hashlib
import importlib
import json
import math
import os
import re
import tempfile
import time
from collections.abc import Callable, Iterator, Mapping, MutableMapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Protocol

from jsonschema import Draft202012Validator

from orchestration.training_contract import HAI_TYPES, parameters_for
from scripts.training.step_records import file_sha256, verify_done, write_done

ROOT = Path(__file__).resolve().parents[2]
GROUP_COLUMNS = ("control_type_code", "provider_subtype_code", "state_code", "bed_count", "county_fips")
SPINE_FIELDS = ("spine_key", "window_start", "is_primary_population", "is_sensitivity_population")
VALIDATION_FIELDS = (
    "alignment_key",
    "spine_key",
    "measure_control",
    "measure",
    "fact",
    "value_text",
    "value_number",
    "period_start",
    "period_end",
    "fiscal_year",
    "alignment_status",
)
ADJACENCY_FIELDS = ("edge_key", "county_fips", "neighbor_fips", "is_self_link", "is_isolated")
BUILD_FIELDS = ("build_key", "features_run_id", "gold_run_id", "gold_snapshot_set_sha256", "feature_schema_sha256", "hai_outcome_controls_sha256")
SUPPORT_TABLES = ("int_hospital_spine", "int_spine_validation_measures", "int_county_adjacency", "feature_build_inputs")
KEY = re.compile(r"([0-9]{6}):([12][0-9]{3})")
SHA256 = re.compile(r"[a-f0-9]{64}")
SECONDS = 1800.0


class ExtractError(ValueError):
    """A publication or output violates the training extraction contract."""


class Reader(Protocol):
    """The feature-store operations required by extraction, with materialized row results."""

    schema: dict[str, Any]
    schema_sha256: str

    def publication(self, run: str | None) -> Any: ...

    def historical(self, references: Sequence[str], run: str) -> Any: ...

    def consistent(self, publication: Any, operation: Callable[[], Any]) -> Any: ...

    def scan(self, publication: Any, table: str, fields: tuple[str, ...]) -> list[dict[str, Any]]: ...


class Writer(Protocol):
    """The table writer boundary replaced explicitly by local synthetic cases."""

    def write(self, path: Path, rows: Sequence[Mapping[str, Any]], columns: Mapping[str, str]) -> None: ...


def canonical(value: Any) -> bytes:
    """Encode deterministic metadata without a clock or a run-specific serializer."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _value(value: Any) -> Any:
    if isinstance(value, float):
        return {"float64": value.hex()}
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise ExtractError("an extract timestamp has no time zone")
        return {"timestamp_utc": value.astimezone(UTC).isoformat()}
    if isinstance(value, date):
        return {"date": value.isoformat()}
    if value is None or isinstance(value, str | int | bool):
        return value
    raise ExtractError("an extract contains an unsupported scalar type")


def content_sha256(rows: Sequence[Mapping[str, Any]], columns: Mapping[str, str]) -> str:
    """Hash sorted typed values rather than Parquet encoding bytes."""
    names = sorted(columns)
    payload = {"columns": [[name, columns[name]] for name in names], "rows": [[_value(row.get(name)) for name in names] for row in rows]}
    return hashlib.sha256(canonical(payload)).hexdigest()


class ParquetWriter:
    """Write typed Parquet with the feature image and verify the read-back content."""

    def write(self, path: Path, rows: Sequence[Mapping[str, Any]], columns: Mapping[str, str]) -> None:
        """Write a sorted table and refuse a changed schema, row count or typed read-back value."""
        arrow = importlib.import_module("pyarrow")
        parquet = importlib.import_module("pyarrow.parquet")
        types = {
            "varchar": arrow.string(),
            "double": arrow.float64(),
            "bigint": arrow.int64(),
            "boolean": arrow.bool_(),
            "date": arrow.date32(),
            "timestamp with time zone": arrow.timestamp("us", tz="UTC"),
        }
        if any(value not in types for value in columns.values()):
            raise ExtractError("an extract column has an unsupported declared type")
        schema = arrow.schema([(name, types[columns[name]]) for name in sorted(columns)])
        table = arrow.Table.from_pylist([dict(row) for row in rows], schema=schema)
        parquet.write_table(table, path, compression="zstd", version="2.6", use_dictionary=False)
        restored = parquet.read_table(path)
        if not restored.schema.equals(schema) or content_sha256(restored.to_pylist(), columns) != content_sha256(rows, columns):
            raise ExtractError("Parquet read-back differs from the typed extract")
        with path.open("rb") as handle:
            os.fsync(handle.fileno())


@dataclass
class FeatureReader:
    """Adapter to the accepted historical and publication readers, imported only inside the feature image."""

    catalog: Any
    namespace: str
    environ: MutableMapping[str, str]
    schema_path: Path
    reads: Any
    history: Any
    schema: dict[str, Any]
    schema_sha256: str

    def publication(self, run: str | None) -> Any:
        """Select the committed publication once."""
        return self.reads.read_publication(self.catalog, self.namespace, run)

    def historical(self, references: Sequence[str], run: str) -> Any:
        """Call the accepted helper once with the selected publication ID."""
        return self.history.historical_features(
            references, publication_run=run, environ=self.environ, register=False, catalog=self.catalog, schema_path=self.schema_path
        )

    def consistent(self, publication: Any, operation: Callable[[], Any]) -> Any:
        """Materialize all auxiliary reads between the accepted head checks."""
        return self.reads.consistent_read(self.catalog, publication, operation)

    def scan(self, publication: Any, table: str, fields: tuple[str, ...]) -> list[dict[str, Any]]:
        """Scan explicit columns at the publication's pinned snapshot ID."""
        source = self.reads.load_table(self.catalog, publication.namespace, table)
        result: list[dict[str, Any]] = source.scan(selected_fields=fields, snapshot_id=publication.snapshot_set[table]).to_arrow().to_pylist()
        return result


@contextlib.contextmanager
def live_reader(root: Path, environ: MutableMapping[str, str], features_run: str) -> Iterator[FeatureReader]:
    """Open the accepted fixture SQL or authenticated Polaris reader without a second data path."""
    reads = importlib.import_module("scripts.features.publication_reads")
    history = importlib.import_module("scripts.features.historical")
    with contextlib.ExitStack() as stack:
        environ["FEAST_REDIS_HOST"] = "localhost"
        # Feast validates online settings on offline reads. This placeholder grants no Redis access.
        environ["REDIS_PASSWORD"] = hashlib.sha256(b"offline-read-only").hexdigest()
        if environ.get("FEAST_NAMESPACE") == "features":
            stack.enter_context(reads.private_pyiceberg_home(environ["AWS_REGION"], environ))
            token = reads.polaris_token(reads.POLARIS_ENDPOINT, environ.pop("POLARIS_CLIENT_ID"), environ.pop("POLARIS_CLIENT_SECRET"))
            environ["FEAST_POLARIS_TOKEN"] = token
            environ["FEAST_CATALOG_ENDPOINT"] = reads.POLARIS_ENDPOINT
        else:
            settings = reads.catalog_settings(environ, "publish_fixture")
            environ["FEAST_SQL_CATALOG_URI"] = settings.sql_uri
            environ["FEAST_SQL_WAREHOUSE"] = settings.sql_warehouse
            (Path(environ["FEAST_RUN_FOLDER"]) / "feast").mkdir(parents=True, exist_ok=True)
        schema_path = root / "data_contracts/features/feature_schema.json"
        schema, digest = reads.read_schema(schema_path)
        selected = None if features_run == "latest" else features_run
        catalog = history.default_catalog(environ, selected)
        yield FeatureReader(catalog, environ["FEAST_NAMESPACE"], environ, schema_path, reads, history, schema, digest)


def reference_union(root: Path, schema: dict[str, Any]) -> tuple[tuple[str, ...], dict[str, dict[str, Any]], str]:
    """Check every selected predictor against roles and exclusions, then add labels and masked group attributes."""
    columns = {f"{column['view']}:{column['name']}": column for table in schema["tables"].values() for column in table["columns"] if column.get("view")}
    config = root / "config/training"
    targets = json.loads((config / "targets.json").read_bytes())
    excluded = set(json.loads((config / "excluded_controls.json").read_bytes())["controls"])
    excluded.update(row["control"] for family in ("target_candidates", "earlier_outcome_candidates") for row in targets[family])
    references: set[str] = set()
    for path in sorted((config / "feature_lists").glob("*.json")):
        document = json.loads(path.read_bytes())
        references.update(document["references"])
    if not references:
        raise ExtractError("no predictor references were configured")
    for name in GROUP_COLUMNS:
        found = [reference for reference, column in columns.items() if column["name"] == name and column["role"] == "predictor"]
        if len(found) != 1:
            raise ExtractError("a promotion group attribute has no unique registered predictor reference")
        references.add(found[0])
    for reference in sorted(references):
        column = columns.get(reference, {})
        control = str((column.get("pivot") or {}).get("match", {}).get("measure_control", ""))
        if column.get("role") != "predictor" or reference not in schema.get("servable", []) or control in excluded or control.split(".", 1)[0] in excluded:
            raise ExtractError("a selected reference is unregistered, excluded or not a predictor")
    predictors = tuple(sorted(references))
    for hai_type in HAI_TYPES:
        number = hai_type.split("_")[1]
        for field in ("sir__number", "alignment_status__category"):
            reference = f"label_hai_{number}:hai_{number}__{field}"
            column = columns.get(reference, {})
            if column.get("role") != "label":
                raise ExtractError("a published SIR or alignment-status label is not registered")
            references.add(reference)
    if len({reference.split(":", 1)[1] for reference in references}) != len(references):
        raise ExtractError("selected references have ambiguous bare column names")
    return (
        tuple(sorted(references)),
        {reference.split(":", 1)[1]: columns[reference] for reference in predictors},
        hashlib.sha256(canonical(predictors)).hexdigest(),
    )


def _unique(rows: Sequence[dict[str, Any]], key: str, label: str) -> dict[str, dict[str, Any]]:
    indexed: dict[str, dict[str, Any]] = {}
    invalid = 0
    for row in rows:
        value = row.get(key)
        if not isinstance(value, str) or not value or value in indexed:
            invalid += 1
        else:
            indexed[value] = row
    if invalid:
        raise ExtractError(f"{label} has {invalid} missing or repeated keys")
    return indexed


def _identity(root: Path, publication: Any, schema_sha256: str, rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    if len(rows) != 1:
        raise ExtractError("feature_build_inputs must have exactly one row")
    record = rows[0]
    if record.get("build_key") != "current" or record.get("features_run_id") != publication.run_id or record.get("feature_schema_sha256") != schema_sha256:
        raise ExtractError("feature publication input identity or schema differs from the selected publication")
    seed = root / "dbt/seeds/features/frozen_hai_outcome_controls.csv"
    identity = file_sha256(seed)
    if record.get("hai_outcome_controls_sha256") != identity:
        raise ExtractError("publication HAI outcome control identity differs from the training snapshot")
    with seed.open(newline="") as source:
        controls = {(row["measure_control"], row["hai_type"], row["field"]) for row in csv.DictReader(source)}
    targets = json.loads((root / "config/training/targets.json").read_bytes())
    if any((row["control"], row["hai_type"], "sir") not in controls for row in targets["target_candidates"]):
        raise ExtractError("a training target is absent from the publication control identity")
    if not re.fullmatch(r"[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}", str(record.get("gold_run_id", ""))):
        raise ExtractError("feature publication has no valid gold run identity")
    if not SHA256.fullmatch(str(record.get("gold_snapshot_set_sha256", ""))):
        raise ExtractError("feature publication has no valid gold snapshot-set identity")
    return record


def extract(
    root: Path,
    output: Path,
    run_id: str,
    parameters: Mapping[str, Any],
    code_digest: str,
    reader: Reader,
    *,
    writer_lock_sha256: str,
    writer: Writer | None = None,
    seconds: float = SECONDS,
    clock: Callable[[], float] = time.monotonic,
) -> dict[str, Any]:
    """Write verified per-type predictor files and separate auxiliary files, publishing the done mark last."""
    params = parameters_for("training_dag", parameters)
    if not re.fullmatch(r"[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}", run_id) or not SHA256.fullmatch(code_digest) or not SHA256.fullmatch(writer_lock_sha256):
        raise ExtractError("extraction requires valid run, code and writer-lock identities")
    deadline = clock() + seconds

    def check() -> None:
        if clock() >= deadline:
            raise TimeoutError("training extraction exceeded its absolute deadline")

    check()
    references, predictors, feature_list_sha = reference_union(root, reader.schema)
    parameters_sha = hashlib.sha256(canonical(params)).hexdigest()
    output.mkdir(parents=True, exist_ok=True)
    if output.is_symlink():
        raise ExtractError("extraction output folder is linked")
    if (output / "extract.done.json").exists():
        done = verify_done(output, "extract", code_digest)
        manifests = [name for name in done["outputs"] if name.startswith("manifest_") and name.endswith(".json")]
        expected = {f"manifest_{hai}.json" for hai in params["hai_types"]}
        if set(manifests) != expected or any(json.loads((output / name).read_bytes()).get("parameters_sha256") != parameters_sha for name in manifests):
            raise ExtractError("completed extract belongs to different training parameters")
        return {"resumed": True, "outputs": sorted(done["outputs"])}
    selected = None if params["features_run"] == "latest" else params["features_run"]
    publication = reader.publication(selected)
    if selected is not None and publication.run_id != selected:
        raise ExtractError("the feature reader selected another publication")
    if any(name not in publication.snapshot_set for name in SUPPORT_TABLES):
        raise ExtractError("the publication lacks a required training support snapshot")
    check()
    history = reader.historical(references, publication.run_id)
    if history.publication != publication or history.feature_schema_sha256 != reader.schema_sha256:
        raise ExtractError("historical features came from another publication or schema")
    historical_rows = history.frame.to_pylist()
    historical = _unique(historical_rows, "spine_key", "historical features")
    if history.entity_count != len(historical):
        raise ExtractError("historical feature entity count differs from its rows")
    bare = {reference.split(":", 1)[1] for reference in references}
    if any(not bare.issubset(row) or set(row) - bare - {"spine_key", "event_timestamp"} for row in historical_rows):
        raise ExtractError("historical feature result has missing or unexpected columns")

    def read_support() -> dict[str, list[dict[str, Any]]]:
        result = {}
        for table, fields in zip(SUPPORT_TABLES, (SPINE_FIELDS, VALIDATION_FIELDS, ADJACENCY_FIELDS, BUILD_FIELDS), strict=True):
            check()
            result[table] = reader.scan(publication, table, fields)
        return result

    support = reader.consistent(publication, read_support)
    check()
    build = _identity(root, publication, reader.schema_sha256, support["feature_build_inputs"])
    spine = _unique(support["int_hospital_spine"], "spine_key", "population spine")
    if set(spine) != set(historical):
        raise ExtractError("population spine and historical entities have different keys")
    flags = []
    for key, row in sorted(spine.items()):
        check()
        match = KEY.fullmatch(key)
        if match is None or any(type(row.get(name)) is not bool for name in ("is_primary_population", "is_sensitivity_population")):
            raise ExtractError("population spine has malformed hospital-window keys or flags")
        year = int(match[2])
        start = row["window_start"].date() if isinstance(row["window_start"], datetime) else row["window_start"]
        if start != date(year, 1, 1):
            raise ExtractError("a hospital-window key differs from its own window start")
        if row["is_" + params["population"] + "_population"]:
            flags.append(key)
    _unique(support["int_spine_validation_measures"], "alignment_key", "validation measures")
    _unique(support["int_county_adjacency"], "edge_key", "county adjacency")
    validation = support["int_spine_validation_measures"]
    if any(row.get("spine_key") not in spine for row in validation):
        raise ExtractError("validation measures contain an unknown spine key")
    mediator, hac = [], []
    for row in validation:
        if row["spine_key"] not in flags:
            continue
        if row.get("measure") in ("PSI_90", "PSI_13"):
            start = spine[row["spine_key"]]["window_start"]
            end = row.get("period_end")
            if isinstance(start, datetime):
                start = start.date()
            if isinstance(end, datetime):
                end = end.date()
            if row.get("alignment_status") == "aligned" and (not isinstance(end, date) or end >= start):
                raise ExtractError("an aligned mediator period does not end before its HAI window")
            mediator.append(
                {name: row.get(name) for name in ("spine_key", "measure_control", "measure", "value_number", "period_start", "period_end", "alignment_status")}
            )
        if row.get("fact") == "fct_hac_program_year" and row.get("measure") == "payment_reduction":
            hac.append({name: row.get(name) for name in ("spine_key", "measure_control", "value_number", "value_text", "fiscal_year", "alignment_status")})
    mediator.sort(key=lambda row: (row["spine_key"], row["measure"]))
    hac.sort(key=lambda row: (row["spine_key"], row["fiscal_year"] or 0))
    adjacency = sorted(support["int_county_adjacency"], key=lambda row: row["edge_key"])
    shared = {
        "mediator.parquet": (
            mediator,
            {
                "spine_key": "varchar",
                "measure_control": "varchar",
                "measure": "varchar",
                "value_number": "double",
                "period_start": "date",
                "period_end": "date",
                "alignment_status": "varchar",
            },
            "mediator",
        ),
        "hac.parquet": (
            hac,
            {
                "spine_key": "varchar",
                "measure_control": "varchar",
                "value_number": "double",
                "value_text": "varchar",
                "fiscal_year": "bigint",
                "alignment_status": "varchar",
            },
            "hac",
        ),
        "adjacency.parquet": (
            adjacency,
            {"edge_key": "varchar", "county_fips": "varchar", "neighbor_fips": "varchar", "is_self_link": "boolean", "is_isolated": "boolean"},
            "adjacency",
        ),
    }
    table_writer = writer if writer is not None else ParquetWriter()
    manifest_schema = json.loads((root / "data_contracts/training/extract_manifest.schema.json").read_bytes())
    validator = Draft202012Validator(manifest_schema)
    outputs = []
    with tempfile.TemporaryDirectory(prefix=".extract_", dir=output) as temporary:
        stage = Path(temporary)

        def write_table(name: str, rows: Sequence[Mapping[str, Any]], columns: Mapping[str, str]) -> dict[str, Any]:
            check()
            path = stage / name
            table_writer.write(path, rows, dict(sorted(columns.items())))
            check()
            outputs.append(name)
            return {
                "file": name,
                "sha256": file_sha256(path),
                "row_count": len(rows),
                "columns": dict(sorted(columns.items())),
                "value_content_sha256": content_sha256(rows, columns),
            }

        auxiliary = {name: {**write_table(name, rows, columns), "kind": kind} for name, (rows, columns, kind) in shared.items()}
        for hai_type in params["hai_types"]:
            check()
            number = hai_type.split("_")[1]
            label = f"hai_{number}__sir__number"
            status = f"hai_{number}__alignment_status__category"
            rows = []
            for key in flags:
                source = historical[key]
                value = source[label]
                if source[status] != "aligned" or not isinstance(value, int | float) or isinstance(value, bool) or not math.isfinite(value):
                    continue
                match = KEY.fullmatch(key)
                if match is None:
                    raise ExtractError("a hospital-window key changed during extraction")
                rows.append(
                    {**{name: source[name] for name in predictors}, "spine_key": key, "ccn": match[1], "window_year": int(match[2]), label: float(value)}
                )
            columns = {name: column["type"] for name, column in predictors.items()} | {
                "spine_key": "varchar",
                "ccn": "varchar",
                "window_year": "bigint",
                label: "double",
            }
            file_info = write_table(f"extract_{hai_type}.parquet", rows, columns)
            years = [row["window_year"] for row in rows]
            manifest = {
                "schema_version": 1,
                "kind": "training_extract",
                "run_id": run_id,
                "hai_type": hai_type,
                "population": params["population"],
                "parameters_sha256": parameters_sha,
                "code_digest": code_digest,
                "publication_run_id": publication.run_id,
                "features_namespace": publication.namespace,
                "snapshot_set": dict(sorted(publication.snapshot_set.items())),
                "features_snapshot_set_sha256": publication.snapshot_set_sha256,
                "gold_run_id": build["gold_run_id"],
                "gold_snapshot_set_sha256": build["gold_snapshot_set_sha256"],
                "feature_schema_sha256": reader.schema_sha256,
                "hai_outcome_controls_sha256": build["hai_outcome_controls_sha256"],
                "feature_list_sha256": feature_list_sha,
                "writer_lock_sha256": writer_lock_sha256,
                **file_info,
                "label_column": label,
                "oldest_window_year": min(years) if years else None,
                "newest_window_year": max(years) if years else None,
                "counts": {
                    "spine": len(spine),
                    "population_excluded": len(spine) - len(flags),
                    "label_excluded": len(flags) - len(rows),
                    "included": len(rows),
                },
                "auxiliary_files": auxiliary,
            }
            errors = list(validator.iter_errors(manifest))
            if errors:
                raise ExtractError(f"extract manifest violates schema {errors[0].validator}")
            name = f"manifest_{hai_type}.json"
            with (stage / name).open("wb") as handle:
                handle.write(canonical(manifest) + b"\n")
                handle.flush()
                os.fsync(handle.fileno())
            outputs.append(name)
        for name in outputs:
            check()
            if (output / name).is_symlink():
                raise ExtractError("an extraction destination is linked")
            os.replace(stage / name, output / name)
        check()
        done = write_done(output, "extract", code_digest, outputs)
    return {"resumed": False, "publication_run_id": publication.run_id, "outputs": sorted(done["outputs"])}


def main(argv: Sequence[str] | None = None) -> int:
    """Run extraction in the feature image with the fixed launcher arguments and absolute container deadline."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("extract",))
    parser.add_argument("--run", required=True)
    parser.add_argument("--mode", choices=("fixture", "real"), required=True)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--parameters", type=Path, required=True)
    parser.add_argument("--snapshot-record", type=Path, required=True)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--writer-lock", type=Path, default=Path("/opt/feature_store/requirements.txt"))
    args = parser.parse_args(argv)
    try:
        parameters = parameters_for("training_dag", json.loads(args.parameters.read_bytes()))
        snapshot = json.loads(args.snapshot_record.read_bytes())
        if snapshot.get("run_id") != args.run:
            raise ExtractError("snapshot record belongs to another training run")
        environ = os.environ
        expected = "features_e2e" if args.mode == "fixture" else "features"
        if environ.get("FEAST_NAMESPACE") != expected:
            raise ExtractError("extraction namespace differs from the requested run mode")
        started = time.monotonic()
        with live_reader(args.root, environ, parameters["features_run"]) as reader:
            extract(
                args.root,
                args.run_dir,
                args.run,
                parameters,
                snapshot["code_digest"],
                reader,
                writer_lock_sha256=file_sha256(args.writer_lock),
                seconds=SECONDS - (time.monotonic() - started),
            )
    except (ValueError, OSError, ImportError, TimeoutError) as error:
        import sys

        sys.stderr.write(f"training extraction failed: {type(error).__name__}: {error}\n")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
