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
