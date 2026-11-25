"""Materialize one verified features publication into the online store (design p3c section 4.3, the Feast side of ``feast_job materialize``).

``run_materialize`` selects the ``publish_runs`` row R of its own run (never the head: a later publication cannot give the run's record its identity, and a
run with no publication is refused), builds the load plan (entity count from the entity table at R's snapshot, the time window of
the hospital-window table, the predictor column count) and runs ``online_protocol.run_load`` with hooks that call Feast: ``FeatureStore.materialize`` over the
predictor views for the whole window, a parse of every key as a Feast entity key of the project, and a sample comparison of online values with the published
tables (the first and last spine key of each window year). Every Redis wait is bounded (5 s) and the whole job by the 1,800 s deadline of ``feast_job``.
Feast and PyIceberg are imported when needed. Messages hold names, counts and hashes only.
"""

from __future__ import annotations

import argparse
import contextlib
import functools
import importlib
import importlib.metadata
import json
import os
import sys
from collections.abc import Callable, Mapping, MutableMapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.features import feast_job, feast_sets, feature_sets, historical
from scripts.features import online_protocol as online
from scripts.features import publication_reads as reads
from scripts.features.publication_reads import ReadError

PROJECT = "hai_prediction"
KEY_VERSION = 3
SPINE = reads.SPINE_TABLE
WINDOW_TABLE = "feature_matrix_hospital_window"


def sample_keys(rows: Sequence[dict[str, Any]]) -> list[str]:
    """The first and last spine key (sorted) of each window year: the keys whose online values are compared with the published tables."""
    by_year: dict[Any, list[str]] = {}
    for row in rows:
        by_year.setdefault(row["window_year"], []).append(row["spine_key"])
    chosen: list[str] = []
    for year in sorted(by_year):
        keys = sorted(by_year[year])
        chosen.extend(dict.fromkeys([keys[0], keys[-1]]))
    return chosen


def load_plan(catalog: reads.IcebergCatalog, publication: reads.Publication, schema: dict[str, Any], schema_sha256: str, version: str) -> online.LoadPlan:
    """The plan of one load, read at R's snapshots: entity count, predictor columns and the inclusive time window of the hospital-window table."""
    spine = reads.load_table(catalog, publication.namespace, SPINE)
    keys = [row["spine_key"] for row in spine.scan(selected_fields=("spine_key",), snapshot_id=publication.snapshot_set[SPINE]).to_arrow().to_pylist()]
    if not keys or len(set(keys)) != len(keys) or None in keys:
        raise ReadError(f"{SPINE} is not a clean entity table: {len(keys)} rows, {len(set(keys))} distinct keys")
    window = reads.load_table(catalog, publication.namespace, WINDOW_TABLE)
    times = [
        row["event_timestamp"]
        for row in window.scan(selected_fields=("event_timestamp",), snapshot_id=publication.snapshot_set[WINDOW_TABLE]).to_arrow().to_pylist()
    ]
    if len(times) != len(keys) or None in times:
        raise ReadError(f"{WINDOW_TABLE} has {len(times)} rows for {len(keys)} entities")
    predictors, _ = reads.reference_lists(schema)
    columns = {reference.split(":", 1)[1] for reference in predictors}
    return online.LoadPlan(
        publication.run_id,
        publication.snapshot_set_sha256,
        schema_sha256,
        len(keys),
        len(columns),
        min(times).astimezone(UTC),
        max(times).astimezone(UTC),
        version,
    )


class FeastHooks:
    """The Feast side of a load: head verification, ``materialize``, key parsing and the sample comparison."""

    def __init__(self, catalog: reads.IcebergCatalog, publication: reads.Publication, schema: dict[str, Any], store: Any, version: str) -> None:
        self.catalog, self.publication, self.schema, self.store, self.version = catalog, publication, schema, store, version

    def verify_heads(self, moment: str) -> None:
        reads.verify_heads(self.catalog, self.publication, moment)

    def materialize(self, start: datetime, end: datetime) -> None:
        views = sorted({reference.split(":", 1)[0] for reference in reads.reference_lists(self.schema)[0]})
        self.store.materialize(start_date=start, end_date=end, feature_views=views)

    def parse_key(self, key: bytes) -> bool:
        suffix = PROJECT.encode()
        if not key.endswith(suffix):
            return False
        try:
            parsed = importlib.import_module("feast.infra.key_encoding_utils").deserialize_entity_key(key[: -len(suffix)], KEY_VERSION)
        except Exception:
            return False
        return list(parsed.join_keys) == ["spine_key"]

    def verify_samples(self) -> None:
        """Compare online values with the published tables for the sample keys: null for null, every value equal."""
        spine = reads.load_table(self.catalog, self.publication.namespace, SPINE)
        rows = spine.scan(selected_fields=("spine_key", "window_year"), snapshot_id=self.publication.snapshot_set[SPINE]).to_arrow().to_pylist()
        keys = sample_keys(rows)
        predictors, _ = reads.reference_lists(self.schema)
        online_values = self.store.get_online_features(features=predictors, entity_rows=[{"spine_key": key} for key in keys], full_feature_names=True).to_dict()
        wanted = set(keys)
        mismatches = 0
        for view in sorted({reference.split(":", 1)[0] for reference in predictors}):
            table = reads.load_table(self.catalog, self.publication.namespace, view)
            published = {
                row["spine_key"]: row
                for row in table.scan(selected_fields=("*",), snapshot_id=self.publication.snapshot_set[view]).to_arrow().to_pylist()
                if row["spine_key"] in wanted
            }
            for reference in (item for item in predictors if item.startswith(f"{view}:")):
                column = reference.split(":", 1)[1]
                got = dict(zip(online_values["spine_key"], online_values[f"{view}__{column}"], strict=True))
                mismatches += sum(1 for key in keys if got.get(key) != published[key][column])
        if mismatches:
            raise ReadError(f"{mismatches} sampled online values differ from the published tables ({len(keys)} sample keys)")


def run_materialize(
    settings: feast_job.JobSettings,
    environ: MutableMapping[str, str],
    *,
    control: online.RedisClient | None = None,
    features: online.RedisClient | None = None,
    hooks_for: Callable[[reads.IcebergCatalog, reads.Publication, dict[str, Any]], online.LoadHooks] | None = None,
    schema: tuple[dict[str, Any], str] | None = None,
    loader: Callable[..., Any] | None = None,
    exchange: Callable[[str, str, str], str] | None = None,
    register: bool = True,
    session: Callable[[], contextlib.AbstractContextManager[reads.IcebergCatalog]] | None = None,
    settings_file: Path = feast_job.SETTINGS_FILE,
) -> dict[str, Any]:
    """Run the materialize step and write ``feast_materialize.json`` (names, counts and hashes) into the run folder.

    ``session`` replaces ``feast_job.catalog_session`` and ``settings_file`` names the Feast settings of the profile (the AWS profile passes both).
    """
    deadline = reads.monotonic_deadline(feast_job.JOB_SECONDS)
    document, schema_sha256 = schema if schema is not None else reads.read_schema()
    opened = session() if session is not None else feast_job.catalog_session(settings, environ, loader=loader, exchange=exchange, register=register)
    with opened as catalog:
        deadline("selecting the publication")
        publication = reads.read_publication(catalog, reads.NAMESPACES[settings.mode], settings.run)
        version = importlib.metadata.version("feast") if hooks_for is None else "test"
        plan = load_plan(catalog, publication, document, schema_sha256, version)
        if hooks_for is None:
            hooks: online.LoadHooks = FeastHooks(catalog, publication, document, historical.default_store(environ, settings_file), version)
        else:
            hooks = hooks_for(catalog, publication, document)
        # Feast's own Redis client (materialize writes through it) gets the job bounds and zero retries; the cases that pass their own hooks use no Feast.
        with online.feast_job_redis(environ) if hooks_for is None else contextlib.nullcontext():
            result = online.run_load(
                control if control is not None else online.connect(environ, online.CONTROL_DB),
                features if features is not None else online.connect(environ, online.FEATURE_DB, decode=False),
                plan,
                hooks,
                deadline=deadline,
            )
    record = {
        "schema": feast_job.SCHEMA_VERSION,
        "step": "materialize",
        "run_id": settings.run,
        "mode": settings.mode,
        "publication": {"run_id": publication.run_id, "snapshot_set_sha256": publication.snapshot_set_sha256},
        "feature_schema_sha256": schema_sha256,
        "load_id": result.load_id,
        "entities": result.entity_count,
    }
    feast_job.write_record(settings.folder / "feast_materialize.json", record)
    return record


def release_set(
    argv: Sequence[str],
    environ: MutableMapping[str, str] | None = None,
    *,
    schema: tuple[dict[str, Any], str] | None = None,
    holds: feast_sets.HoldStore | None = None,
    loader: feast_sets.OnlineLoader | None = None,
    newest_run: Callable[[str], str] | None = None,
    production: Callable[[], Mapping[str, str | None]] | None = None,
    direct: bool = False,
) -> dict[str, Any]:
    """Release the held online load of a run: ``--release <SET> <RUN_ID> [--mode real|publish_fixture]``. A repeat returns the first release record.

    With ``direct`` a run that has no hold is accepted when its own load record is a successful load of the set in the request's mode (``feast_sets.release``);
    the command line never sets it.

    The release is refused while a Production model of the set is on another schema (``feast_sets.release``); the Production records are read from S3."""
    parser = argparse.ArgumentParser(description="Load a held model set run online and record the release.", prog="feast_materialize")
    parser.add_argument("--release", nargs=2, metavar=("SET", "RUN_ID"), required=True)
    parser.add_argument("--mode", choices=reads.MODES, default="real")
    args = parser.parse_args(list(argv))
    feature_set, run = args.release
    live = os.environ if environ is None else environ
    if feature_set not in feature_sets.MODEL_SETS:
        raise ReadError(f"set {feature_set!r} is not a model set (m1 to m6)")
    document, sha = schema if schema is not None else reads.read_schema()
    if holds is not None and newest_run is not None and production is not None:
        return feast_sets.release(
            feature_set,
            run,
            schema=document,
            schema_sha256=sha,
            holds=holds,
            loader=loader or feast_sets.refusing_loader,
            newest_run=newest_run,
            production=production,
            direct_mode=args.mode if direct else None,
        )
    from scripts.features import feast_aws

    aws = feast_aws.selected_profile(live) == "aws"
    settings = (feast_aws.job_settings if aws else feast_job.job_settings)(live, "materialize", run, args.mode)
    session = (
        functools.partial(feast_aws.catalog_session, settings, live, open_store=functools.partial(feast_aws.open_ssm, settings.region))
        if aws
        else functools.partial(feast_job.catalog_session, settings, live)
    )
    with session() as catalog:
        # The load reuses the open catalog: the AWS session takes the reader pair out of the environment, so a second session could not open.
        profile_file = feast_aws.AWS_SETTINGS_FILE if aws else feast_job.SETTINGS_FILE
        set_loader = feast_sets.online_loader(
            settings, live, schema=(document, sha), session=lambda: contextlib.nullcontext(catalog), settings_file=profile_file
        )
        return feast_sets.release(
            feature_set,
            run,
            schema=document,
            schema_sha256=sha,
            holds=holds if holds is not None else feast_sets.hold_store(settings, live),
            loader=loader if loader is not None else set_loader,
            newest_run=newest_run if newest_run is not None else (lambda name: feast_sets.newest_set_run(catalog, reads.NAMESPACES[args.mode], name, document)),
            production=production if production is not None else feast_sets.production_reader(settings, feature_set),
            direct_mode=args.mode if direct else None,
        )


def main(argv: Sequence[str] | None = None, environ: MutableMapping[str, str] | None = None) -> int:
    """The release entry point: exit 0 when the run is released (or was), 1 when the release is refused or fails (the message names the cause)."""
    try:
        record = release_set(sys.argv[1:] if argv is None else argv, environ)
    except ReadError as error:
        sys.stderr.write(f"feast_materialize release failed: {error}\n")
        return 1
    sys.stdout.write(json.dumps({key: record[key] for key in ("feature_set", "run_id", "base_run", "load_id") if key in record}, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
