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
import sys
import tempfile
import types
from pathlib import Path
from typing import Any
from unittest import mock

from scripts.features import feast_job, feast_sets
from scripts.features import publication_reads as reads
from scripts.features.feast_harness import Checker
from scripts.features.publication_reads import ReadError

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
        loaded_receipt(holds, schema, sha, "m4", loaded_run)

        def release(model: str, run: str) -> dict[str, Any]:
            return online_release.release_promoted(
                model,
                run,
                mode="publish_fixture",
                schema=(schema, sha),
                holds=holds,
                loader=online.load,
                newest_run=lambda name: run,
                production=lambda: {"HAI_1": mine},
            )

        result = release("m4", loaded_run)
        c.check("a run that loaded directly has nothing to release", result["outcome"] == "not_held" and online.loads == [])
        c.refused("a run with neither a hold nor a load record is refused by name", lambda: release("m4", blank_run), "no online hold")
        holds.write_new("feast_materialize.json", blank_run, {"feature_set": "m4", "run_id": blank_run, "online": "hold"})
        c.refused("a run whose record says hold but has no hold file is refused", lambda: release("m4", blank_run), "no online hold")
        loaded_receipt(holds, schema, sha, "m2", "20261110T050000Z-0000a002")
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


def loaded_receipt(holds: feast_sets.HoldStore, schema: dict[str, Any], sha: str, model: str, run: str, /, **changes: Any) -> dict[str, Any]:
    """The ``feast_materialize.json`` of a loaded run as ``set_materialize`` writes it, with ``changes`` applied (``None`` drops a field)."""
    mine = importlib.import_module("scripts.features.feature_schema").set_schema_sha256(model, schema=schema)
    settings = feast_job.JobSettings("materialize", run, "publish_fixture", Path("/nonexistent"), "/nonexistent/registry.pb", "")
    record = feast_sets.set_materialize(
        settings,
        model,
        schema=schema,
        schema_sha256=sha,
        base_run=BASE_RUN,
        production=lambda: [mine],
        loader=lambda request: "load-direct-1",
        holds=holds,
    )
    for name, value in changes.items():
        if value is None:
            record.pop(name, None)
        else:
            record[name] = value
    holds.write_new(feast_sets.MATERIALIZE_NAME, run, record)
    return record


def case_e33_not_held_checked(c: Checker) -> None:
    """A run that loaded itself is not held only when its load record is complete and its run is current: the release checks apply."""
    online_release: Any = importlib.import_module("scripts.features.online_release")
    schema, sha = reads.read_schema()
    mine = importlib.import_module("scripts.features.feature_schema").set_schema_sha256("m4", schema=schema)
    newer = "20261110T090000Z-0000a005"
    online = Online()
    calls: dict[str, int] = {"newest": 0, "production": 0}

    def unresolved() -> dict[str, str | None]:
        raise ReadError("the promotion of model-a (HAI_1) is unresolved: lease generation 2 is open")

    with tempfile.TemporaryDirectory(prefix="online_release_cases_") as raw:
        counter = [0]

        def attempt(model: str = "m4", newest: str | None = None, production: Any = None, mode: str = "publish_fixture", **changes: Any) -> dict[str, Any]:
            counter[0] += 1
            run = f"20261110T03{counter[0]:02d}00Z-0000a004"
            holds = feast_sets.FolderHoldStore(Path(raw) / str(counter[0]))
            loaded_receipt(holds, schema, sha, "m4", run, **{name: (run if value == "RUN" else value) for name, value in changes.items()})

            def newest_run(name: str) -> str:
                calls["newest"] += 1
                return run if newest is None else newest

            def read_production() -> dict[str, str | None]:
                calls["production"] += 1
                return {"HAI_1": mine} if production is None else production()

            return online_release.release_promoted(
                model, run, mode=mode, schema=(schema, sha), holds=holds, loader=online.load, newest_run=newest_run, production=read_production
            )

        good = attempt()
        c.check(
            "a complete load record of a current run is not held and names its load", good["outcome"] == "not_held" and good.get("load_id") == "load-direct-1"
        )
        c.check("the answer names the set, the run and the base run", good["feature_set"] == "m4" and good.get("base_run") == BASE_RUN and "run_id" in good)
        c.check("the check read the newest run and the Production models once each", calls == {"newest": 1, "production": 1})
        c.refused("a superseded run is refused, as for a held run", lambda: attempt(newest=newer), "superseded")
        c.refused("an unresolved promotion is refused", lambda: attempt(production=unresolved), "unresolved")
        c.refused(
            "a Production model on another schema is refused by infection type", lambda: attempt(production=lambda: {"HAI_1": mine, "HAI_2": OLD_HASH}), "HAI_2"
        )
        c.refused("a load record of another schema is refused", lambda: attempt(set_schema_sha256=OLD_HASH), "set_schema_sha256")
        c.refused("a load record without a load ID is refused", lambda: attempt(load_id=None), "load_id")
        c.refused("a load record with an empty load ID is refused", lambda: attempt(load_id=""), "load_id")
        c.refused("a load record of another mode is refused", lambda: attempt(mode="real"), "mode")
        c.refused("a load record of another run is refused", lambda: attempt(run_id=newer), "run_id")
        c.refused("a load record of another set is refused", lambda: attempt(feature_set="m2"), "feature_set")
        c.refused("a load record that says hold is refused", lambda: attempt(online="hold"), "no online hold")
        c.refused("a load record without a base run is refused", lambda: attempt(base_run=None), "base_run")
        c.refused("a load record of another step is refused", lambda: attempt(step="apply"), "step")
        counter[0] += 1
        run = f"20261110T03{counter[0]:02d}00Z-0000a004"
        holds = feast_sets.FolderHoldStore(Path(raw) / str(counter[0]))
        holds.write_new(feast_sets.MATERIALIZE_NAME, run, {"feature_set": "m4", "run_id": run, "online": "load"})
        c.refused(
            "the three-field record that the old shortcut accepted is refused",
            lambda: online_release.release_promoted(
                "m4",
                run,
                mode="publish_fixture",
                schema=(schema, sha),
                holds=holds,
                loader=online.load,
                newest_run=lambda name: run,
                production=lambda: {"HAI_1": mine},
            ),
            "load_id",
        )
        c.check("no refusal and no answer loaded anything online", online.loads == [])


def case_e33_cached_release(c: Checker) -> None:
    """A release record is returned only when it is the release of this hold: its set, run, base run and schema hash match, and it names a load."""
    online_release: Any = importlib.import_module("scripts.features.online_release")
    schema, sha = reads.read_schema()
    mine = importlib.import_module("scripts.features.feature_schema").set_schema_sha256("m1", schema=schema)
    other_run = "20261110T090000Z-0000a002"
    online = Online()
    held = {"schema": 1, "feature_set": "m1", "run_id": HELD_RUN, "base_run": BASE_RUN, "set_schema_sha256": mine, "production_set_schema_sha256": [OLD_HASH]}
    genuine = {
        "schema": 1,
        "feature_set": "m1",
        "run_id": HELD_RUN,
        "base_run": BASE_RUN,
        "set_schema_sha256": mine,
        "feature_schema_sha256": sha,
        "load_id": "load-first",
    }
    with tempfile.TemporaryDirectory(prefix="online_release_cases_") as raw:
        counter = [0]

        def attempt(release: dict[str, Any] | None, hold: dict[str, Any] | None = None) -> dict[str, Any]:
            counter[0] += 1
            holds = feast_sets.FolderHoldStore(Path(raw) / str(counter[0]))
            holds.write_new(feast_sets.HOLD_NAME, HELD_RUN, held if hold is None else hold)
            if release is not None:
                holds.write_new(feast_sets.RELEASE_NAME, HELD_RUN, release)
            return online_release.release_promoted(
                "m1", HELD_RUN, schema=(schema, sha), holds=holds, loader=online.load, newest_run=lambda name: HELD_RUN, production=lambda: {"HAI_1": mine}
            )

        again = attempt(genuine)
        c.check(
            "the genuine release record answers already released with its load", again["outcome"] == "already_released" and again["load_id"] == "load-first"
        )
        c.refused("a release record of another set is refused without a load", lambda: attempt({**genuine, "feature_set": "m2"}), "release record")
        c.refused("a release record of another run is refused", lambda: attempt({**genuine, "run_id": other_run}), "release record")
        c.refused("a release record of another base run is refused", lambda: attempt({**genuine, "base_run": other_run}), "release record")
        c.refused("a release record of another schema is refused", lambda: attempt({**genuine, "set_schema_sha256": OLD_HASH}), "release record")
        c.refused("an empty release record is refused", lambda: attempt({}), "release record")
        c.refused(
            "a release record without a load ID is refused", lambda: attempt({key: value for key, value in genuine.items() if key != "load_id"}), "load_id"
        )
        c.refused("a release record with an empty load ID is refused", lambda: attempt({**genuine, "load_id": ""}), "load_id")
        c.refused("a release record with a load ID that is not text is refused", lambda: attempt({**genuine, "load_id": 7}), "load_id")
        c.refused("a hold of another run is refused", lambda: attempt(genuine, {**held, "run_id": other_run}), "hold")
        c.check("no refusal loaded anything online", online.loads == [])


class ListedBucket:
    """The data bucket as the features task role sees it (infra/iam job_writer_features_feast.json): it lists a prefix under ``lakehouse/layer_runs/features/``
    and reads the keys it finds, and it may not list the rest of the bucket. So a read of a key that is not there is AccessDenied, as in S3 for a caller without
    the bucket-wide list permission, and only a listing says that a key is absent. A write with ``IfNoneMatch`` is refused when the key exists."""

    class ClientError(Exception):
        def __init__(self, code: str) -> None:
            super().__init__(code)
            self.response = {"Error": {"Code": code}}

    class NoSuchKey(ClientError):
        pass

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.calls: list[str] = []
        self.exceptions = type("Errors", (), {"NoSuchKey": self.NoSuchKey, "ClientError": self.ClientError})

    def list_objects_v2(self, *, Bucket: str, Prefix: str, MaxKeys: int = 1000) -> dict[str, Any]:
        self.calls.append(f"list {Prefix}")
        if not Prefix.startswith(feast_sets.RUN_PREFIX + "/"):
            raise self.ClientError("AccessDenied")
        return {"Contents": [{"Key": key} for key in sorted(self.objects) if key.startswith(Prefix)][:MaxKeys]}

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        self.calls.append(f"get {Key}")
        if Key not in self.objects:
            raise self.ClientError("AccessDenied")
        return {"Body": io.BytesIO(self.objects[Key])}

    def put_object(self, *, Bucket: str, Key: str, Body: str, IfNoneMatch: str) -> None:
        self.calls.append(f"put {Key}")
        if Key in self.objects:
            raise self.ClientError("PreconditionFailed")
        self.objects[Key] = Body.encode()


def case_e33_s3_store_lists_before_it_reads(c: Checker) -> None:
    """The release phase runs as the features task role, which may list only the run records of the features layer, so the store of the run's records asks
    the listing whether a key is there and reads only a key that is: the absence of a hold or of a release (the first release has none) is an answer, not a
    denial."""
    client = ListedBucket()
    store = feast_sets.S3HoldStore(client, "data-bucket")
    run = HELD_RUN
    c.check("a record that is not there reads as absent, not as a denied read", store.read(feast_sets.HOLD_NAME, run) is None)
    c.check("the absence was settled by a listing of that key and no read", client.calls == [f"list {feast_sets.RUN_PREFIX}/{run}/{feast_sets.HOLD_NAME}"])
    c.check(
        "a record is written once",
        store.write_new(feast_sets.HOLD_NAME, run, {"held": True}) and not store.write_new(feast_sets.HOLD_NAME, run, {"held": False}),
    )
    c.check("a record that is there is read back as it was first written", store.read(feast_sets.HOLD_NAME, run) == {"held": True})
    client.objects[f"{feast_sets.RUN_PREFIX}/{run}/{feast_sets.RELEASE_NAME}.old"] = b"{}"
    c.check("a key that only starts with the name is not the record", store.read(feast_sets.RELEASE_NAME, run) is None)
    c.check(
        "every call stayed under the run records of the features layer", all(call.split(" ", 1)[1].startswith(feast_sets.RUN_PREFIX) for call in client.calls)
    )


RELOAD_RUNS = [f"20261111T0{hour}0000Z-0000a00{hour}" for hour in range(1, 7)]  # oldest first; the cases name them from the newest down
LAUNCH_RUN = "20261112T010000Z-0000c001"


class Reloadable:
    """The online store of one set that restarts empty: a loader that marks what it loaded and recovers a finished load, as ``online_loader`` does."""

    def __init__(self) -> None:
        self.loads: list[str] = []
        self.marker: tuple[str, str] | None = None
        self.refusal: str | None = None

    def load(self, request: Any) -> str:
        if self.refusal is not None:
            raise ReadError(self.refusal)
        self.loads.append(f"{request.feature_set}:{request.run}:{request.base_run}")
        self.marker = (request.run, f"load-{len(self.loads)}")
        return self.marker[1]

    def recover(self, request: Any) -> str | None:
        return self.marker[1] if self.marker is not None and self.marker[0] == request.run else None

    def restart(self) -> None:
        self.marker = None

    def loader(self) -> feast_sets.SetLoader:
        return feast_sets.SetLoader(self.load, self.recover)


class CountingStore(feast_sets.FolderHoldStore):
    """A folder store that remembers which runs it was asked about."""

    def __init__(self, root: Path) -> None:
        super().__init__(root)
        self.asked: list[str] = []

    def read(self, name: str, run: str) -> dict[str, Any] | None:
        self.asked.append(run)
        return super().read(name, run)


def hold_of(model: str, run: str, set_hash: str, **changes: Any) -> dict[str, Any]:
    return {
        "schema": 1,
        "feature_set": model,
        "run_id": run,
        "base_run": BASE_RUN,
        "set_schema_sha256": set_hash,
        "production_set_schema_sha256": [OLD_HASH],
        **changes,
    }


def release_of(model: str, run: str, set_hash: str, sha: str, **changes: Any) -> dict[str, Any]:
    return {
        "schema": 1,
        "feature_set": model,
        "run_id": run,
        "base_run": BASE_RUN,
        "set_schema_sha256": set_hash,
        "feature_schema_sha256": sha,
        "load_id": "load-first",
        **changes,
    }


def reload_run(model: str, holds: feast_sets.HoldStore, *, runs: list[str], store: Reloadable, production: Any, **injected: Any) -> dict[str, Any]:
    """One reload of ``model`` with the records in ``holds`` and the committed ``runs`` (newest first), over the stand-in store."""
    online_reload: Any = importlib.import_module("scripts.features.online_reload")
    schema, sha = reads.read_schema()
    wiring: dict[str, Any] = {
        "schema": (schema, sha),
        "holds": holds,
        "loader": store.loader(),
        "runs": lambda name: list(runs),
        "production": production,
        **injected,
    }
    return online_reload.reload_set(model, mode="publish_fixture", **wiring)


def case_e34_reload_picks_the_newest_run_with_load_evidence(c: Checker) -> None:
    """After a restart the store is empty: the reload loads the newest committed run that has load evidence (its own load record, or a release bound to its
    hold) and skips a newer run that is held and not released, because its load was never online."""
    schema, sha = reads.read_schema()
    mine = importlib.import_module("scripts.features.feature_schema").set_schema_sha256("m2", schema=schema)
    held_new, blank, released, direct = RELOAD_RUNS[5], RELOAD_RUNS[4], RELOAD_RUNS[3], RELOAD_RUNS[2]
    with tempfile.TemporaryDirectory(prefix="online_reload_cases_") as raw:
        holds = CountingStore(Path(raw))
        holds.write_new(feast_sets.HOLD_NAME, held_new, hold_of("m2", held_new, mine))
        holds.write_new(feast_sets.MATERIALIZE_NAME, held_new, {"feature_set": "m2", "run_id": held_new, "online": "hold"})
        holds.write_new(feast_sets.MATERIALIZE_NAME, blank, {"feature_set": "m2", "run_id": blank, "online": "offline"})
        holds.write_new(feast_sets.HOLD_NAME, released, hold_of("m2", released, mine))
        holds.write_new(feast_sets.RELEASE_NAME, released, release_of("m2", released, mine, sha))
        loaded_receipt(holds, schema, sha, "m2", direct)
        store, same = Reloadable(), lambda: {"HAI_2": mine, "HAI_3": mine}
        result = reload_run("m2", holds, runs=RELOAD_RUNS[::-1], store=store, production=same)
        c.check("the newest run with evidence is loaded once, with the base run its hold names", store.loads == [f"m2:{released}:{BASE_RUN}"])
        c.check(
            "the result says loaded and names the set, the run, the base run and the load",
            (result["outcome"], result["feature_set"], result["run_id"], result["base_run"]) == ("loaded", "m2", released, BASE_RUN)
            and result["load_id"] == "load-1",
        )
        c.check("the held newer run is named as skipped", result["skipped_held"] == [held_new])
        c.check("the runs older than the loaded one were not read", direct not in holds.asked and RELOAD_RUNS[1] not in holds.asked)
        newest = RELOAD_RUNS[5]
        holds2 = feast_sets.FolderHoldStore(Path(raw) / "second")
        loaded_receipt(holds2, schema, sha, "m2", newest)
        holds2.write_new(feast_sets.HOLD_NAME, released, hold_of("m2", released, mine))
        holds2.write_new(feast_sets.RELEASE_NAME, released, release_of("m2", released, mine, sha))
        other = Reloadable()
        result = reload_run("m2", holds2, runs=RELOAD_RUNS[::-1], store=other, production=same)
        c.check(
            "a newest run that loaded itself is the run to load",
            other.loads == [f"m2:{newest}:{BASE_RUN}"] and result["run_id"] == newest and result["skipped_held"] == [],
        )


def case_e34_reload_skips_a_set_without_load_evidence(c: Checker) -> None:
    """A set whose runs are all held, offline, blank or absent has nothing that was ever online: it is skipped and reported, and loads nothing."""
    schema, sha = reads.read_schema()
    mine = importlib.import_module("scripts.features.feature_schema").set_schema_sha256("m4", schema=schema)
    held, offline = RELOAD_RUNS[5], RELOAD_RUNS[4]
    with tempfile.TemporaryDirectory(prefix="online_reload_cases_") as raw:
        holds = feast_sets.FolderHoldStore(Path(raw))
        holds.write_new(feast_sets.HOLD_NAME, held, hold_of("m4", held, mine))
        holds.write_new(feast_sets.MATERIALIZE_NAME, offline, {"feature_set": "m4", "run_id": offline, "online": "offline"})
        store = Reloadable()
        result = reload_run("m4", holds, runs=[held, offline, RELOAD_RUNS[3]], store=store, production=lambda: {"HAI_1": mine})
        c.check("the set is skipped and loads nothing", result["outcome"] == "skipped" and store.loads == [] and store.marker is None)
        c.check("the report names the reason and the held run", "no load evidence" in result["reason"] and result["skipped_held"] == [held])
        none = reload_run("m4", holds, runs=[], store=store, production=lambda: {"HAI_1": mine})
        c.check("a set with no committed run is skipped too", none["outcome"] == "skipped" and store.loads == [])


def case_e34_reload_skips_a_held_newer_run_on_a_changed_schema(c: Checker) -> None:
    """A newer run that is held and not released, over an older run that was loaded on the schema the set had before, is skipped and named (owner decision B,
    Nov 17 2026): nothing is loaded, because the older run's rows are on another schema than the set has now. Without a held newer run the same schema change
    is still a refusal, and so is every other fault in the older run's record."""
    online_reload: Any = importlib.import_module("scripts.features.online_reload")
    schema, sha = reads.read_schema()
    mine = importlib.import_module("scripts.features.feature_schema").set_schema_sha256("m2", schema=schema)
    held_new, older = RELOAD_RUNS[5], RELOAD_RUNS[4]
    good = {"HAI_2": mine, "HAI_3": mine}
    with tempfile.TemporaryDirectory(prefix="online_reload_cases_") as raw:
        counter = [0]
        store = Reloadable()

        def attempt(*, held: bool = True, receipt: dict[str, Any] | None = None, release: str | None = None) -> dict[str, Any]:
            """One reload with an optional held newer run, an older run that loaded itself (``receipt``) or an older run released on the schema ``release``."""
            counter[0] += 1
            holds = feast_sets.FolderHoldStore(Path(raw) / str(counter[0]))
            runs = [older]
            if held:
                holds.write_new(feast_sets.HOLD_NAME, held_new, hold_of("m2", held_new, mine))
                runs = [held_new, older]
            if receipt is not None:
                loaded_receipt(holds, schema, sha, "m2", older, **receipt)
            if release is not None:
                holds.write_new(feast_sets.HOLD_NAME, older, hold_of("m2", older, release))
                holds.write_new(feast_sets.RELEASE_NAME, older, release_of("m2", older, release, sha))
            return reload_run("m2", holds, runs=runs, store=store, production=lambda: dict(good))

        for kind, change in (("loaded itself", {"receipt": {"set_schema_sha256": OLD_HASH}}), ("was released", {"release": OLD_HASH})):
            result = attempt(**change)
            c.check(f"a held newer run over an older run that {kind} on the old schema is skipped", result["outcome"] == "skipped")
            c.check(f"the skip names the held run and the schema reason ({kind})", result["skipped_held"] == [held_new] and held_new in result["reason"])
            c.check(f"the reason names the schema ({kind})", "another schema" in result["reason"])
        c.check("nothing was loaded and the store holds nothing", store.loads == [] and store.marker is None)
        c.refused(
            "a changed schema without a held newer run is still refused (own load)",
            lambda: attempt(held=False, receipt={"set_schema_sha256": OLD_HASH}),
            "set_schema_sha256",
        )
        c.refused(
            "a changed schema without a held newer run is still refused (release)", lambda: attempt(held=False, release=OLD_HASH), "schema of set m2 changed"
        )
        c.refused("a held newer run does not excuse a load record without a load ID", lambda: attempt(receipt={"load_id": None}), "load_id")
        c.refused("a held newer run does not excuse a load record of another mode", lambda: attempt(receipt={"mode": "real"}), "mode")
        c.refused(
            "a held newer run does not excuse a load record with no schema hash", lambda: attempt(receipt={"set_schema_sha256": None}), "set_schema_sha256"
        )
        c.check("none of the refusals loaded anything", store.loads == [])
        loaded = attempt(receipt={})
        c.check(
            "a held newer run over an older run on the current schema still loads the older run", loaded["outcome"] == "loaded" and loaded["run_id"] == older
        )
        c.check("the one load was the older run", store.loads == [f"m2:{older}:{BASE_RUN}"])
        store.restart()
        holds = feast_sets.FolderHoldStore(Path(raw) / "command")
        holds.write_new(feast_sets.HOLD_NAME, held_new, hold_of("m2", held_new, mine))
        loaded_receipt(holds, schema, sha, "m2", older, set_schema_sha256=OLD_HASH)
        wiring: dict[str, Any] = {
            "schema": (schema, sha),
            "holds": holds,
            "loader": store.loader(),
            "runs": lambda name: [held_new, older],
            "production": lambda: dict(good),
        }
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = online_reload.main(["--model", "m2", "--run", LAUNCH_RUN, "--mode", "publish_fixture"], {}, **wiring)
        line = json.loads(out.getvalue()) if out.getvalue().strip() else {}
        c.check("the command exits 0 with one skipped line and no error text", code == 0 and err.getvalue() == "" and out.getvalue().count("\n") == 1)
        c.check(
            "the line names the outcome, the set, the held run and the reason",
            (line.get("outcome"), line.get("feature_set"), line.get("skipped_held")) == ("skipped", "m2", [held_new]) and held_new in line.get("reason", ""),
        )
        c.check("the command loaded nothing", store.loads == [f"m2:{older}:{BASE_RUN}"] and store.marker is None)


def case_e34_reload_skip_needs_a_genuine_held_run_and_valid_evidence(c: Checker) -> None:
    """The skip excuses only the schema difference of a genuine, well-formed held newer run: an open promotion lease, a hold without its base run or schema
    hash, a hold on the older schema and a load, hold or release record whose schema hash is not a SHA-256 each stay refused, and nothing is loaded."""
    schema, sha = reads.read_schema()
    mine = importlib.import_module("scripts.features.feature_schema").set_schema_sha256("m2", schema=schema)
    held_new, older = RELOAD_RUNS[5], RELOAD_RUNS[4]
    good = {"HAI_2": mine, "HAI_3": mine}

    def unresolved() -> dict[str, str | None]:
        raise ReadError("the promotion of model-b (HAI_2) is unresolved: lease generation 3 is open")

    with tempfile.TemporaryDirectory(prefix="online_reload_cases_") as raw:
        counter = [0]
        store = Reloadable()

        def attempt(
            hold: dict[str, Any] | None = None,
            receipt: dict[str, Any] | None = None,
            release: str | None = None,
            production: Any = None,
            older_base: str = BASE_RUN,
        ) -> dict[str, Any]:
            """One reload with the held newer run ``hold`` (the genuine one by default), an older run that loaded itself on the old schema or was released on
            the schema ``release`` over the base run ``older_base``."""
            counter[0] += 1
            holds = feast_sets.FolderHoldStore(Path(raw) / str(counter[0]))
            holds.write_new(feast_sets.HOLD_NAME, held_new, hold if hold is not None else hold_of("m2", held_new, mine))
            if release is not None:
                holds.write_new(feast_sets.HOLD_NAME, older, hold_of("m2", older, release, base_run=older_base))
                holds.write_new(feast_sets.RELEASE_NAME, older, release_of("m2", older, release, sha, base_run=older_base))
            else:
                loaded_receipt(holds, schema, sha, "m2", older, **({"set_schema_sha256": OLD_HASH} if receipt is None else receipt))
            return reload_run("m2", holds, runs=[held_new, older], store=store, production=production or (lambda: dict(good)))

        genuine = attempt()
        c.check("the genuine held run over an older schema is skipped", genuine["outcome"] == "skipped" and genuine["skipped_held"] == [held_new])
        other_schema = attempt(production=lambda: {"HAI_2": OLD_HASH, "HAI_3": OLD_HASH})
        c.check("Production models on the older schema do not stop the skip (that is the hold's reason)", other_schema["outcome"] == "skipped")
        c.refused("an open promotion lease refuses the skip", lambda: attempt(production=unresolved), "unresolved")
        c.refused("an open promotion lease refuses the skip of a released older run", lambda: attempt(release=OLD_HASH, production=unresolved), "unresolved")
        released_skip = attempt(release=OLD_HASH)
        c.check("a genuine older hold and release on the older schema is skipped", released_skip["outcome"] == "skipped")
        for base in ("run-1", ""):
            c.refused(
                f"an older hold and release with the base run {base!r} are no schema change",
                lambda base=base: attempt(release=OLD_HASH, older_base=base),
                "base_run",
            )
        genuine_hold = hold_of("m2", held_new, mine)
        for field in ("base_run", "set_schema_sha256"):
            c.refused(f"a hold without {field} does not excuse the schema change", lambda field=field: attempt(hold={**genuine_hold, field: None}), field)
        c.refused("a hold with an empty base run does not excuse the schema change", lambda: attempt(hold={**genuine_hold, "base_run": ""}), "base_run")
        c.refused("a hold with a base run that is no run ID does not excuse it", lambda: attempt(hold={**genuine_hold, "base_run": "run-1"}), "base_run")
        c.refused("a hold on the older schema does not excuse it", lambda: attempt(hold=hold_of("m2", held_new, OLD_HASH)), "set_schema_sha256")
        c.refused("a hold of another set does not excuse it", lambda: attempt(hold=hold_of("m4", held_new, mine)), "belongs to set m4")
        c.refused("a hold that names another run does not excuse it", lambda: attempt(hold=hold_of("m2", RELOAD_RUNS[0], mine)), "names run")
        for bad in ("", "not-a-schema-hash", "A" * 64, "0" * 63):
            shown = bad or "empty"
            c.refused(
                f"an older load record with the schema hash {shown!r} is no schema change",
                lambda bad=bad: attempt(receipt={"set_schema_sha256": bad}),
                "set_schema_sha256",
            )
            c.refused(
                f"an older hold and release with the schema hash {shown!r} are no schema change", lambda bad=bad: attempt(release=bad), "set_schema_sha256"
            )
        c.check("none of the refusals loaded anything", store.loads == [] and store.marker is None)


def case_e34_reload_refuses_what_does_not_fit(c: Checker) -> None:
    """A foreign hold, a record without a load, a changed schema, a Production model on another schema, an open promotion lease and a moved head each stop
    the reload by name, and nothing is loaded."""
    schema, sha = reads.read_schema()
    mine = importlib.import_module("scripts.features.feature_schema").set_schema_sha256("m2", schema=schema)
    run = RELOAD_RUNS[5]
    good = {"HAI_2": mine, "HAI_3": mine}

    def unresolved() -> dict[str, str | None]:
        raise ReadError("the promotion of model-b (HAI_2) is unresolved: lease generation 3 is open")

    with tempfile.TemporaryDirectory(prefix="online_reload_cases_") as raw:
        counter = [0]
        store = Reloadable()

        def attempt(
            hold: dict[str, Any] | None = None, release: dict[str, Any] | None = None, receipt: dict[str, Any] | None = None, production: Any = None
        ) -> dict[str, Any]:
            counter[0] += 1
            holds = feast_sets.FolderHoldStore(Path(raw) / str(counter[0]))
            if hold is not None:
                holds.write_new(feast_sets.HOLD_NAME, run, hold)
            if release is not None:
                holds.write_new(feast_sets.RELEASE_NAME, run, release)
            if receipt is not None:
                loaded_receipt(holds, schema, sha, "m2", run, **receipt)
            return reload_run("m2", holds, runs=[run], store=store, production=production or (lambda: dict(good)))

        c.check("a genuine released run loads", attempt(hold_of("m2", run, mine), release_of("m2", run, mine, sha))["outcome"] == "loaded")
        store.restart()
        c.check("the genuine reload asked the store once", len(store.loads) == 1)
        released = release_of("m2", run, mine, sha)
        c.refused("a hold of another set is refused", lambda: attempt(hold_of("m1", run, mine), released), "belongs to set m1")
        c.refused("a hold that names another run is refused", lambda: attempt(hold_of("m2", RELOAD_RUNS[0], mine), released), "names run")
        c.refused(
            "a release record without a load ID is refused",
            lambda: attempt(hold_of("m2", run, mine), {key: value for key, value in released.items() if key != "load_id"}),
            "load_id",
        )
        c.refused(
            "a release record of another base run is refused",
            lambda: attempt(hold_of("m2", run, mine), {**released, "base_run": RELOAD_RUNS[0]}),
            "release record",
        )
        c.refused(
            "a hold and release whose base run is no run ID are refused",
            lambda: attempt(hold_of("m2", run, mine, base_run="run-1"), {**released, "base_run": "run-1"}),
            "base_run",
        )
        c.refused("a direct load record without a load ID is refused", lambda: attempt(receipt={"load_id": None}), "load_id")
        c.refused("a direct load record of another schema is refused", lambda: attempt(receipt={"set_schema_sha256": OLD_HASH}), "set_schema_sha256")
        c.refused(
            "a hold of another schema than the set now has is refused",
            lambda: attempt(hold_of("m2", run, OLD_HASH), release_of("m2", run, OLD_HASH, sha)),
            "schema of set m2 changed",
        )
        c.refused(
            "a Production model on another schema is refused by infection type",
            lambda: attempt(hold_of("m2", run, mine), released, production=lambda: {"HAI_2": mine, "HAI_3": OLD_HASH}),
            "HAI_3",
        )
        c.refused("an unresolved promotion is refused", lambda: attempt(hold_of("m2", run, mine), released, production=unresolved), "unresolved")
        store.refusal = "before the read: 1 of 8 tables are not at the publication (feature_matrix: main is 9, publication names 8)"
        c.refused(
            "a moved head that the load finds is refused and loads nothing", lambda: attempt(hold_of("m2", run, mine), released), "not at the publication"
        )
        c.check("no refusal loaded anything online", len(store.loads) == 1)
        online_reload: Any = importlib.import_module("scripts.features.online_reload")
        for model in ("m3", "m5", "m6"):
            c.refused(
                f"{model} is offline only and is refused",
                lambda model=model: online_reload.reload_set(model, mode="publish_fixture"),
                "not an online model set",
            )
        c.refused("a name that is not a model set is refused", lambda: online_reload.reload_set("base", mode="publish_fixture"), "not a model set")


def case_e34_reload_is_idempotent(c: Checker) -> None:
    """A store that still holds the ready load is left alone; one that restarted empty is loaded again."""
    schema, sha = reads.read_schema()
    mine = importlib.import_module("scripts.features.feature_schema").set_schema_sha256("m1", schema=schema)
    run = RELOAD_RUNS[5]
    with tempfile.TemporaryDirectory(prefix="online_reload_cases_") as raw:
        holds = feast_sets.FolderHoldStore(Path(raw))
        holds.write_new(feast_sets.HOLD_NAME, run, hold_of("m1", run, mine))
        holds.write_new(feast_sets.RELEASE_NAME, run, release_of("m1", run, mine, sha))
        store, records = Reloadable(), sorted(path.name for path in Path(raw).rglob("*"))
        first = reload_run("m1", holds, runs=[run], store=store, production=lambda: {"HAI_1": mine})
        again = reload_run("m1", holds, runs=[run], store=store, production=lambda: {"HAI_1": mine})
        c.check(
            "the first reload loads and the repeat finds the ready load",
            (first["outcome"], again["outcome"]) == ("loaded", "already_loaded") and len(store.loads) == 1,
        )
        c.check("the repeat names the load the store holds", again["load_id"] == first["load_id"] == "load-1")
        store.restart()
        third = reload_run("m1", holds, runs=[run], store=store, production=lambda: {"HAI_1": mine})
        c.check("a store that restarted empty is loaded again", third["outcome"] == "loaded" and len(store.loads) == 2)
        plain = Reloadable()
        loaded = reload_run("m1", holds, runs=[run], store=plain, production=lambda: {"HAI_1": mine}, loader=plain.load)
        c.check("a loader that cannot recover loads", loaded["outcome"] == "loaded" and len(plain.loads) == 1)
        c.check("the reload wrote no hold, release or load record", sorted(path.name for path in Path(raw).rglob("*")) == records)


def case_e34_reload_command(c: Checker) -> None:
    """``python -m scripts.features.online_reload --model <SET> --run <LAUNCH_RUN>``: its result line and exit codes."""
    online_reload: Any = importlib.import_module("scripts.features.online_reload")
    schema, sha = reads.read_schema()
    mine = importlib.import_module("scripts.features.feature_schema").set_schema_sha256("m1", schema=schema)
    run = RELOAD_RUNS[5]
    with tempfile.TemporaryDirectory(prefix="online_reload_cases_") as raw:
        holds = feast_sets.FolderHoldStore(Path(raw))
        holds.write_new(feast_sets.HOLD_NAME, run, hold_of("m1", run, mine))
        holds.write_new(feast_sets.RELEASE_NAME, run, release_of("m1", run, mine, sha))
        store, production = Reloadable(), {"HAI_1": mine}
        wiring: dict[str, Any] = {
            "schema": (schema, sha),
            "holds": holds,
            "loader": store.loader(),
            "runs": lambda name: [run],
            "production": lambda: dict(production),
        }

        def invoke(argv: list[str]) -> tuple[int, str, str]:
            out, err = io.StringIO(), io.StringIO()
            with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
                code = online_reload.main(argv, {}, **wiring)
            return code, out.getvalue(), err.getvalue()

        arguments = ["--model", "m1", "--run", LAUNCH_RUN, "--mode", "publish_fixture"]
        code, out, err = invoke(arguments)
        line = json.loads(out) if out.strip() else {}
        c.check("a reload exits 0 and prints one result line", code == 0 and err == "" and out.count("\n") == 1)
        c.check(
            "the line names the outcome, the set, the run and the load",
            (line.get("outcome"), line.get("feature_set"), line.get("run_id")) == ("loaded", "m1", run) and "load_id" in line,
        )
        code, out, _ = invoke(arguments)
        c.check("a repeat exits 0 and says already loaded", code == 0 and json.loads(out)["outcome"] == "already_loaded" and len(store.loads) == 1)
        production["HAI_1"] = OLD_HASH
        code, out, err = invoke(arguments)
        c.check("a refusal exits 1 with the cause on stderr and no result line", code == 1 and out == "" and "online_reload failed" in err and "HAI_1" in err)
        wiring["runs"] = lambda name: []
        production["HAI_1"] = mine
        code, out, _ = invoke(arguments)
        c.check("a set without evidence exits 0 and says skipped", code == 0 and json.loads(out)["outcome"] == "skipped")
        c.check("a name that is not a model set exits 1", invoke(["--model", "m9", "--run", LAUNCH_RUN])[0] == 1)
        c.check("a launch run that is not a run ID exits 1", invoke(["--model", "m1", "--run", "latest"])[0] == 1)


DATA_BUCKET = "example-data-bucket"
S3_CLIENT = "s3-client-stand-in"


def launch_environment(**changes: str | None) -> dict[str, str]:
    """The environment of the feast container in a release or a reload launch of the features task (synthetic values): the AWS profile and the data bucket."""
    environ: dict[str, str] = {
        "FEAST_PROFILE": "aws",
        "AWS_REGION": "us-east-2",
        "FEAST_REGISTRY_BUCKET": DATA_BUCKET,
        "DATA_BUCKET_NAME": DATA_BUCKET,
        "RELEASE_MODEL": "m1",
    }
    for name, value in changes.items():
        if value is None:
            environ.pop(name, None)
        else:
            environ[name] = value
    return environ


def case_e35_launch_without_the_deployment_file(c: Checker) -> None:
    """Failure modes of the run's record store in a release or a reload launch (first live reload, Nov 16: ``the deployment file is missing or not valid
    JSON``). Only a build launch's steps write ``/workspace/infra/deployment.auto.tfvars.json``; a release or reload launch skips them, so the AWS profile takes
    the data bucket of the record store from ``DATA_BUCKET_NAME``, which every features container has, and never reads that file.

    1. The file is absent and ``DATA_BUCKET_NAME`` is set: the release store and the reload store open on that bucket (the file is never read).
    2. ``DATA_BUCKET_NAME`` is unset or empty: both refuse by that name (not by the file).
    3. ``DATA_BUCKET_NAME`` is not a bucket name (capitals, a space, ``..``, too short): both refuse by that name.
    4. A file that is there does not override the variable (one source), and the local profile keeps reading its file."""
    online_release: Any = importlib.import_module("scripts.features.online_release")
    feast_aws: Any = importlib.import_module("scripts.features.feast_aws")
    boto3 = types.SimpleNamespace(client=lambda service, region_name=None: f"{S3_CLIENT}:{service}:{region_name}")
    absent = mock.patch.object(reads, "deployment_bucket", side_effect=ReadError("the deployment file is missing or not valid JSON"))
    run = HELD_RUN

    def release_store(environ: dict[str, str]) -> feast_sets.HoldStore:
        return online_release.default_holds(run, "real", environ)

    def reload_store(environ: dict[str, str]) -> feast_sets.HoldStore:
        return feast_sets.hold_store(feast_aws.job_settings(environ, "materialize", LAUNCH_RUN, "real"), environ)

    with mock.patch.dict(sys.modules, {"boto3": boto3}), absent as deployment_read:
        for name, make in (("release", release_store), ("reload", reload_store)):
            store = make(launch_environment())
            c.check(
                f"{name}: the store opens on the data bucket with no deployment file", isinstance(store, feast_sets.S3HoldStore) and store.bucket == DATA_BUCKET
            )
            c.check(f"{name}: the S3 client is the one of the task region", getattr(store, "client", "") == f"{S3_CLIENT}:s3:us-east-2")
            c.refused(
                f"{name}: an unset DATA_BUCKET_NAME is refused by name", lambda make=make: make(launch_environment(DATA_BUCKET_NAME=None)), "DATA_BUCKET_NAME"
            )
            c.refused(
                f"{name}: an empty DATA_BUCKET_NAME is refused by name", lambda make=make: make(launch_environment(DATA_BUCKET_NAME="")), "DATA_BUCKET_NAME"
            )
            for bad in ("Bad_Bucket", "a..b", "x", "has space", "-leading"):
                c.refused(
                    f"{name}: DATA_BUCKET_NAME {bad!r} is refused by name",
                    lambda make=make, bad=bad: make(launch_environment(DATA_BUCKET_NAME=bad)),
                    "DATA_BUCKET_NAME",
                )
            other = make(launch_environment(DATA_BUCKET_NAME="another-data-bucket"))
            c.check(f"{name}: the store takes the variable, not the registry bucket", getattr(other, "bucket", "") == "another-data-bucket")
        c.check("no deployment file was read by any of them", deployment_read.call_count == 0)
    with mock.patch.dict(sys.modules, {"boto3": boto3}), mock.patch.object(reads, "deployment_bucket", return_value="file-bucket-name") as deployment_read:
        local = feast_job.JobSettings("materialize", run, "real", Path("/workspace/out"), "s3://file-bucket-name/lakehouse/feast/registry.pb", "us-east-2")
        store = feast_sets.hold_store(local, {"AWS_REGION": "us-east-2", "DATA_BUCKET_NAME": DATA_BUCKET})
        c.check("the local profile still reads its deployment file", getattr(store, "bucket", "") == "file-bucket-name" and deployment_read.call_count == 1)
