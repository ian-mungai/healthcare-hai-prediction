"""Reload the online store of a model set from its stored offline features, called by ``host_session start`` after every start (owner decision A, Nov 16 2026).

    python -m scripts.features.online_reload --model <SET> --run <LAUNCH_RUN_ID> [--mode real|publish_fixture]

The online store (Redis) keeps no snapshot, so a host that stopped and started holds nothing. The reload loads the set again, from the offline table of the
newest committed run that has load evidence, with the load that a release or a Feast phase makes (``feast_sets.online_loader``). A run has load evidence when

- its own ``feast_materialize.json`` is a successful load of the set (``online`` is ``load``, with a load ID), or
- it was held and its ``online_release.json`` is the release of that hold (``feast_sets.bound_release``).

A newer run that is held and not released is skipped and named, because its load was never online; the older run with evidence is what the store served. The
result is one of

- ``loaded``: the run's table was loaded online (the store was empty or held another load);
- ``already_loaded``: the set's marker is ready for this run, schema and publication, so nothing is loaded again (``SetLoader.recover``);
- ``skipped``: no committed run of the set has load evidence, so there is nothing that was ever online (a set that was never loaded, such as a fresh stack).

Every other case is refused by name and loads nothing: a hold of another set or run, a release record or a load record that is not that run's, a set whose
schema changed since the evidence was written, a Production model of the set on another schema, an unresolved promotion (an open lease), a set that is offline
only or not a model set, and a publication whose table heads moved (``verify_heads`` inside the load). A reload writes no hold, release or load record: it
changes the online store only, and the writer lease of the load serializes it with a release or a Feast load of the same set. Records hold names, counts and
hashes only.
"""

from __future__ import annotations

import argparse
import contextlib
import functools
import json
import os
import sys
from collections.abc import Callable, Mapping, MutableMapping, Sequence
from typing import Any

from scripts.features import feast_sets, feature_schema, feature_sets, retired_publications
from scripts.features import publication_reads as reads
from scripts.features.publication_reads import ReadError

LOADED, ALREADY_LOADED, SKIPPED = "loaded", "already_loaded", "skipped"


def committed_runs(catalog: reads.IcebergCatalog, namespace: str, feature_set: str, schema: Mapping[str, Any]) -> list[str]:
    """The committed runs that publish the set's table, newest first (ties by the larger run ID); a retired run is no publication."""
    table = schema["model_sets"][feature_set]["table"]
    rows = reads.load_table(catalog, namespace, reads.RUNS_TABLE).scan(selected_fields=("run_id", "committed_at", "snapshot_set")).to_arrow().to_pylist()
    found = []
    for row in rows:
        run = str(row.get("run_id"))
        if table in reads.parse_snapshot_set(run, row.get("snapshot_set")) and not retired_publications.is_retired(namespace, run):
            found.append((reads.as_utc(row["committed_at"]), run))
    return [run for _, run in sorted(found, reverse=True)]


def evidence_of(model: str, run: str, set_hash: str, mode: str, holds: feast_sets.HoldStore) -> tuple[dict[str, Any] | None, bool]:
    """The load record that proves ``run`` was loaded online and whether the run is held and not released. No record means no evidence (the run was held and
    not released, or it never loaded). A record that exists and does not fit the request is refused."""
    hold = holds.read(feast_sets.HOLD_NAME, run)
    if hold is not None:
        if hold.get("feature_set") != model:
            raise ReadError(f"the hold of run {run} belongs to set {hold.get('feature_set')}, not to set {model}")
        if hold.get("run_id") != run:
            raise ReadError(f"the hold stored for run {run} names run {hold.get('run_id')}")
        released = holds.read(feast_sets.RELEASE_NAME, run)
        if released is None:
            return None, True
        record = feast_sets.bound_release(released, hold, model, run)
        if record["set_schema_sha256"] != set_hash:
            raise ReadError(f"the schema of set {model} changed since run {run} was released; nothing was loaded")
        return record, False
    loaded = holds.read(feast_sets.MATERIALIZE_NAME, run)
    if loaded is None or loaded.get("online") != feature_sets.LOAD:
        return None, False
    return feast_sets.direct_load_record(model, run, mode, set_hash, holds), False


def reload_checked(
    model: str,
    *,
    mode: str,
    schema: tuple[dict[str, Any], str],
    holds: feast_sets.HoldStore,
    loader: feast_sets.OnlineLoader,
    runs: Callable[[str], Sequence[str]],
    production: Callable[[], Mapping[str, str | None]],
) -> dict[str, Any]:
    """Select the newest run with load evidence and load it online, or report that the set has none."""
    document, _ = schema
    set_hash = feature_schema.set_schema_sha256(model, schema=document)
    skipped: list[str] = []
    for run in runs(model):
        record, held = evidence_of(model, run, set_hash, mode, holds)
        if record is None:
            skipped += [run] if held else []
            continue
        feast_sets.check_production(model, run, set_hash, production)
        request = feast_sets.LoadRequest(model, run, str(record["base_run"]), set_hash)
        recovered = loader.recover(request) if isinstance(loader, feast_sets.SetLoader) else None
        load_id = recovered if recovered else loader(request)
        return {
            "outcome": ALREADY_LOADED if recovered else LOADED,
            "feature_set": model,
            "run_id": run,
            "base_run": request.base_run,
            "load_id": load_id,
            "skipped_held": skipped,
        }
    return {"outcome": SKIPPED, "feature_set": model, "reason": f"no load evidence in any committed run of set {model}", "skipped_held": skipped}


def reload_set(
    model: str,
    *,
    mode: str = "real",
    launch_run: str = "",
    environ: MutableMapping[str, str] | None = None,
    schema: tuple[dict[str, Any], str] | None = None,
    holds: feast_sets.HoldStore | None = None,
    loader: feast_sets.OnlineLoader | None = None,
    runs: Callable[[str], Sequence[str]] | None = None,
    production: Callable[[], Mapping[str, str | None]] | None = None,
) -> dict[str, Any]:
    """Reload the online store of ``model``'s set, or report that there is nothing to reload.

    ``schema``, ``holds``, ``loader``, ``runs`` (the committed runs of the set, newest first) and ``production`` are injected by the cases; a real call takes
    them from the environment's profile. ``launch_run`` is the run of the launch: it names the settings of the load and writes no record."""
    live = os.environ if environ is None else environ
    if model not in feature_sets.MODEL_SETS:
        raise ReadError(f"set {model!r} is not a model set (m1 to m6)")
    document, sha = schema if schema is not None else reads.read_schema()
    if not document["model_sets"][model]["online"]:
        raise ReadError(f"set {model} is not an online model set: only m1, m2 and m4 have an online load")
    if holds is not None and loader is not None and runs is not None and production is not None:
        return reload_checked(model, mode=mode, schema=(document, sha), holds=holds, loader=loader, runs=runs, production=production)
    from scripts.features import feast_aws, feast_job

    if not reads.RUN_ID.fullmatch(launch_run):
        raise ReadError(f"{launch_run!r} is not a run ID: the reload names the run of its launch")
    aws = feast_aws.selected_profile(live) == "aws"
    settings = (feast_aws.job_settings if aws else feast_job.job_settings)(live, "materialize", launch_run, mode)
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
        return reload_checked(
            model,
            mode=mode,
            schema=(document, sha),
            holds=holds if holds is not None else feast_sets.hold_store(settings, live),
            loader=loader if loader is not None else set_loader,
            runs=runs if runs is not None else (lambda name: committed_runs(catalog, reads.NAMESPACES[mode], name, document)),
            production=production if production is not None else feast_sets.production_reader(settings, model),
        )


def main(argv: Sequence[str] | None = None, environ: MutableMapping[str, str] | None = None, **injected: Any) -> int:
    """Exit 0 when the set is loaded, was loaded or has nothing to load, 1 when the reload is refused or fails (the message names the cause)."""
    parser = argparse.ArgumentParser(prog="python -m scripts.features.online_reload", description="Reload the online store of a model set.")
    parser.add_argument("--model", required=True)
    parser.add_argument("--run", required=True, help="the run ID of this launch")
    parser.add_argument("--mode", choices=reads.MODES, default="real")
    args = parser.parse_args(list(sys.argv[1:] if argv is None else argv))
    try:
        if not reads.RUN_ID.fullmatch(args.run):
            raise ReadError(f"{args.run!r} is not a run ID: the reload names the run of its launch")
        result = reload_set(args.model, mode=args.mode, launch_run=args.run, environ=environ, **injected)
    except ReadError as error:
        sys.stderr.write(f"online_reload failed: {error}\n")
        return 1
    sys.stdout.write(json.dumps(result, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
