"""The release of a held online load, called by the automatic promotion after the promoted model reached full rollout.

    python -m scripts.features.online_release --model <SET> --features-run <RUN_ID> [--mode real|publish_fixture]

A model set whose schema differs from the Production model's is published offline and trained, and its online load is held (``feast_sets.set_materialize``:
``online_hold.json`` in the run's prefix); the served model keeps serving the last online data. When the promotion of a model trained on that set reaches full
rollout, it calls ``release_promoted`` (or this command) with the model and the run that trained it. The result is one of

- ``released``: the held run was loaded online, once, and the release was recorded beside the hold;
- ``already_released``: an earlier call did it, nothing is loaded again and the first release's load is named;
- ``not_held``: the run was loaded online by its own Feast phase (its schema equalled the Production model's), so there is nothing to release.

Every other case is refused by name and loads nothing: a set that is offline only (m3, m5, m6) or not a model set, a run with neither a hold nor a load record,
a run that is not the set's newest, and a set while any Production model of it is on another schema (``feast_sets.release``). The set decides what is online, so
the caller does not compare schemas itself. Records hold names, counts and hashes only.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Callable, Mapping, MutableMapping, Sequence
from typing import Any

from scripts.features import feast_job, feast_materialize, feast_sets, feature_sets
from scripts.features import publication_reads as reads
from scripts.features.publication_reads import ReadError

MATERIALIZE_RECORD = "feast_materialize.json"
RELEASED, ALREADY_RELEASED, NOT_HELD = "released", "already_released", "not_held"


def default_holds(run: str, mode: str, environ: Mapping[str, str]) -> feast_sets.HoldStore:
    """The store of the run's records for the profile in the environment: the data bucket's run prefix, or the run folder of a fixture run."""
    from scripts.features import feast_aws

    live = dict(environ)
    settings = (feast_aws.job_settings if feast_aws.selected_profile(live) == "aws" else feast_job.job_settings)(live, "materialize", run, mode)
    return feast_sets.hold_store(settings, live)


def release_promoted(
    model: str,
    features_run: str,
    *,
    mode: str = "real",
    environ: MutableMapping[str, str] | None = None,
    schema: tuple[dict[str, Any], str] | None = None,
    holds: feast_sets.HoldStore | None = None,
    loader: feast_sets.OnlineLoader | None = None,
    newest_run: Callable[[str], str] | None = None,
    production: Callable[[], Mapping[str, str | None]] | None = None,
) -> dict[str, Any]:
    """Release the held online load of ``model``'s set for the run that trained the promoted model, or report that there is nothing to release.

    ``schema``, ``holds``, ``loader``, ``newest_run`` and ``production`` are injected by the cases; a real call takes them from the environment's profile."""
    live = os.environ if environ is None else environ
    if model not in feature_sets.MODEL_SETS:
        raise ReadError(f"set {model!r} is not a model set (m1 to m6)")
    if not reads.RUN_ID.fullmatch(features_run):
        raise ReadError(f"{features_run!r} is not a run ID")
    document, sha = schema if schema is not None else reads.read_schema()
    if not document["model_sets"][model]["online"]:
        raise ReadError(f"set {model} is not an online model set: only m1, m2 and m4 have a held online load")
    store = holds if holds is not None else default_holds(features_run, mode, live)
    if store.read(feast_sets.HOLD_NAME, features_run) is None:
        loaded = store.read(MATERIALIZE_RECORD, features_run)
        if loaded is not None and loaded.get("online") == feature_sets.LOAD and loaded.get("feature_set") == model and loaded.get("run_id") == features_run:
            return {"outcome": NOT_HELD, "feature_set": model, "run_id": features_run}
        raise ReadError(f"run {features_run} has no online hold record and did not load set {model} online, so there is nothing to release")
    first = store.read(feast_sets.RELEASE_NAME, features_run) is None
    record = feast_materialize.release_set(
        ["--release", model, features_run, "--mode", mode],
        live,
        schema=(document, sha),
        holds=store,
        loader=loader,
        newest_run=newest_run,
        production=production,
    )
    return {
        "outcome": RELEASED if first else ALREADY_RELEASED,
        **{key: record[key] for key in ("feature_set", "run_id", "base_run", "load_id") if key in record},
    }


def main(argv: Sequence[str] | None = None, environ: MutableMapping[str, str] | None = None, **injected: Any) -> int:
    """Exit 0 when the run is released, was released or needs no release, 1 when the release is refused or fails (the message names the cause)."""
    parser = argparse.ArgumentParser(prog="python -m scripts.features.online_release", description="Release the held online load of a model set run.")
    parser.add_argument("--model", required=True)
    parser.add_argument("--features-run", required=True)
    parser.add_argument("--mode", choices=reads.MODES, default="real")
    args = parser.parse_args(list(sys.argv[1:] if argv is None else argv))
    try:
        result = release_promoted(args.model, args.features_run, mode=args.mode, environ=environ, **injected)
    except ReadError as error:
        sys.stderr.write(f"online_release failed: {error}\n")
        return 1
    sys.stdout.write(json.dumps(result, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
