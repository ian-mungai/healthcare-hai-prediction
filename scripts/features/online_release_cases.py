"""Cases of the held online load and its release by the promotion (``scripts.features.online_release``): no Feast, no AWS, no Docker.

    .venv/bin/python -B -m scripts.features.feast_cases --part local --case E33_held_release --case E33_not_held --case E33_command

A model set whose schema differs from the Production model's is published offline and trained, and its online load is held; the served model keeps serving the
last online data. The promotion releases the hold once the promoted model reaches full rollout. These cases run that sequence over the real hold, release and
decision code with an in-folder record store and stand-in loader and Production reader.
"""

from __future__ import annotations

import contextlib
import importlib
import io
import json
import tempfile
from pathlib import Path
from typing import Any

from scripts.features import feast_job, feast_sets
from scripts.features import publication_reads as reads
from scripts.features.feast_harness import Checker

BASE_RUN, HELD_RUN = "20261110T010000Z-0000b001", "20261110T020000Z-0000a001"
OLD_HASH = "0" * 64


class Online:
    """The online store of one set as the cases see it: what it serves, and every load that was asked of it."""

    def __init__(self) -> None:
        self.loads: list[str] = []
        self.serving = "first load"

    def load(self, request: Any) -> str:
        self.loads.append(f"{request.feature_set}:{request.run}:{request.base_run}")
        self.serving = f"load of {request.run}"
        return f"load-{len(self.loads)}"


def case_e33_held_release(c: Checker) -> None:
    """A differing schema holds the online load and the served model keeps its data; the release loads the held run once, and only after the promotion."""
    online_release: Any = importlib.import_module("scripts.features.online_release")
    schema, sha = reads.read_schema()
    mine = importlib.import_module("scripts.features.feature_schema").set_schema_sha256("m2", schema=schema)
    with tempfile.TemporaryDirectory(prefix="online_release_cases_") as raw:
        holds = feast_sets.FolderHoldStore(Path(raw))
        online = Online()
        production: dict[str, str | None] = {"HAI_2": OLD_HASH, "HAI_3": OLD_HASH}
        settings = feast_job.JobSettings("materialize", HELD_RUN, "publish_fixture", Path(raw), f"{raw}/feast/registry.pb", "")

        def materialize() -> dict[str, Any]:
            return feast_sets.set_materialize(
                settings,
                "m2",
                schema=schema,
                schema_sha256=sha,
                base_run=BASE_RUN,
                production=lambda: list(production.values()),
                loader=online.load,
                holds=holds,
            )

        def release(newest: str = HELD_RUN) -> dict[str, Any]:
            return online_release.release_promoted(
                "m2", HELD_RUN, schema=(schema, sha), holds=holds, loader=online.load, newest_run=lambda name: newest, production=lambda: dict(production)
            )

        record = materialize()
        c.check("the chain holds the online load of a set whose Production models are on another schema", record["online"] == "hold")
        c.check("the held chain loads nothing online and the served model keeps its last data", online.loads == [] and online.serving == "first load")
        c.refused("a release before any promotion is refused by the infection types still on the old schema", release, "HAI_2")
        c.check(
            "the refused release loads nothing, keeps the hold and records no release",
            online.loads == [] and holds.read(feast_sets.RELEASE_NAME, HELD_RUN) is None,
        )
        production["HAI_2"] = mine
        c.refused("a promotion of one of two infection types still leaves the set held", release, "HAI_3")
        c.check("the partial promotion loaded nothing", online.loads == [] and online.serving == "first load")
        production["HAI_3"] = mine
        c.refused("a release of a superseded run is refused after both promotions", lambda: release("20261110T090000Z-0000a002"), "superseded")
        first = release()
        c.check("the release after every promotion loads the held run once, with its base run", online.loads == [f"m2:{HELD_RUN}:{BASE_RUN}"])
        c.check(
            "the release says released and names the set, the run and the load",
            (first["outcome"], first["feature_set"], first["run_id"]) == ("released", "m2", HELD_RUN),
        )
        c.check("the served data is now the held run's", online.serving == f"load of {HELD_RUN}")
        again = release()
        c.check("a repeat release loads nothing, says it was already released and keeps the load id", online.loads == [f"m2:{HELD_RUN}:{BASE_RUN}"])
        c.check("the repeat names the first release's load", again["outcome"] == "already_released" and again["load_id"] == first["load_id"])


def case_e33_not_held(c: Checker) -> None:
    """A promotion of a model whose run was loaded directly releases nothing, and an unknown run or an offline set is refused."""
    online_release: Any = importlib.import_module("scripts.features.online_release")
    schema, sha = reads.read_schema()
    mine = importlib.import_module("scripts.features.feature_schema").set_schema_sha256("m4", schema=schema)
    loaded_run, blank_run = "20261110T030000Z-0000a004", "20261110T040000Z-0000a004"
    with tempfile.TemporaryDirectory(prefix="online_release_cases_") as raw:
        holds = feast_sets.FolderHoldStore(Path(raw))
        online = Online()
        holds.write_new("feast_materialize.json", loaded_run, {"feature_set": "m4", "run_id": loaded_run, "online": "load", "set_schema_sha256": mine})

        def release(model: str, run: str) -> dict[str, Any]:
            return online_release.release_promoted(
                model, run, schema=(schema, sha), holds=holds, loader=online.load, newest_run=lambda name: run, production=lambda: {"HAI_1": mine}
            )

        result = release("m4", loaded_run)
        c.check("a run that loaded directly has nothing to release", result["outcome"] == "not_held" and online.loads == [])
        c.refused("a run with neither a hold nor a load record is refused by name", lambda: release("m4", blank_run), "no online hold")
        holds.write_new("feast_materialize.json", blank_run, {"feature_set": "m4", "run_id": blank_run, "online": "hold"})
        c.refused("a run whose record says hold but has no hold file is refused", lambda: release("m4", blank_run), "no online hold")
        holds.write_new("feast_materialize.json", "20261110T050000Z-0000a002", {"feature_set": "m2", "run_id": "20261110T050000Z-0000a002", "online": "load"})
        c.refused("a load record of another set is refused", lambda: release("m4", "20261110T050000Z-0000a002"), "no online hold")
        for offline in ("m3", "m5", "m6"):
            c.refused(f"{offline} is offline only and is refused", lambda offline=offline: release(offline, loaded_run), "not an online model set")
        c.refused("a name that is not a model set is refused", lambda: release("base", loaded_run), "not a model set")
        c.check("nothing was loaded by any of these", online.loads == [])


def case_e33_command(c: Checker) -> None:
    """``python -m scripts.features.online_release --model <SET> --features-run <RUN_ID>``: its result line, its exit codes and its refusals."""
    online_release: Any = importlib.import_module("scripts.features.online_release")
    schema, sha = reads.read_schema()
    mine = importlib.import_module("scripts.features.feature_schema").set_schema_sha256("m1", schema=schema)
    with tempfile.TemporaryDirectory(prefix="online_release_cases_") as raw:
        holds = feast_sets.FolderHoldStore(Path(raw))
        holds.write_new(
            feast_sets.HOLD_NAME,
            HELD_RUN,
            {"schema": 1, "feature_set": "m1", "run_id": HELD_RUN, "base_run": BASE_RUN, "set_schema_sha256": mine, "production_set_schema_sha256": [OLD_HASH]},
        )
        online = Online()
        wiring: dict[str, Any] = {"schema": (schema, sha), "holds": holds, "loader": online.load, "newest_run": lambda name: HELD_RUN}
        production = {"HAI_1": OLD_HASH}

        def run(argv: list[str]) -> tuple[int, str, str]:
            out, err = io.StringIO(), io.StringIO()
            with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
                code = online_release.main(argv, {}, production=lambda: dict(production), **wiring)
            return code, out.getvalue(), err.getvalue()

        arguments = ["--model", "m1", "--features-run", HELD_RUN, "--mode", "publish_fixture"]
        code, out, err = run(arguments)
        c.check("a release before the promotion exits 1 and names the infection type", code == 1 and "HAI_1" in err and out == "" and online.loads == [])
        production["HAI_1"] = mine
        code, out, err = run(arguments)
        line = json.loads(out) if out.strip() else {}
        c.check("a release after the promotion exits 0 and prints one result line", code == 0 and err == "" and out.count("\n") == 1)
        c.check(
            "the result line names the outcome, the set, the run and the load",
            (line.get("outcome"), line.get("feature_set"), line.get("run_id")) == ("released", "m1", HELD_RUN) and "load_id" in line,
        )
        code, out, _ = run(arguments)
        c.check(
            "a repeat exits 0, says already released and loads nothing again",
            code == 0 and json.loads(out)["outcome"] == "already_released" and len(online.loads) == 1,
        )
        code, _, err = run(["--model", "m9", "--features-run", HELD_RUN])
        c.check("a name that is not a model set exits 1", code == 1 and "not a model set" in err)
        code, _, err = run(["--model", "m1", "--features-run", "latest"])
        c.check("a run that is not a run ID exits 1 and loads nothing", code == 1 and "run ID" in err and len(online.loads) == 1)
