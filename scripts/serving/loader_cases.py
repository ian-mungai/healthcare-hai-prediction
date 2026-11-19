"""Cases of the data-only model loader (``scripts/serving/mlflow_loader.py``), run over stand-in MLflow, pandas and skops modules.

The loader downloads four named files through its one ``fetch`` seam into one temporary folder per load, checks them as data, admits a model only when its
closure digest and four library versions equal the image's, loads ``model.skops`` under the image's own trusted types and replays the saved prediction check
through the wrapper the service uses. The stand-ins record every download (run, path, destination), the trusted list skops received and whether
``mlflow.sklearn.load_model`` was ever called. Real skops, scikit-learn and XGBoost run only in the serving image (heavy run in the build report).
The bundle files are written here in the format of the minimal design v2 (canonical JSON, the keys of ``bundle/self_check.json``), not by the bundle writer.
"""

from __future__ import annotations

import contextlib
import copy
import importlib.metadata
import json
import logging
import math
import os
import shutil
import sys
import tempfile
import threading
import time
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass, field
from functools import partial
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest import mock

from scripts import model_code
from scripts import model_data_matrix as data_rule_matrix
from scripts.features.feast_harness import Checker
from scripts.serving import mlflow_loader

ROOT = Path(__file__).resolve().parents[2]
CONTRACT = "data_contracts/training/model_contract.json"
HAI = "HAI_1"
VERSION = "3"
RUN = "run1"
PROXY_SWITCH = "MLFLOW_ENABLE_PROXY_MULTIPART_DOWNLOAD"
ARTIFACT_URI = "mlflow-artifacts:/1/run1/artifacts"
FEATURE_LIST, PROVENANCE, SELF_CHECK, MODEL = "bundle/feature_list.json", "bundle/provenance.json", "bundle/self_check.json", "model/model.skops"
FOUR = [FEATURE_LIST, PROVENANCE, SELF_CHECK, MODEL]
REFERENCES = ["a:1", "b:2", "c:3"]
COLUMNS = ["a", "b", "c"]
# Number, number with a null in row 2, category as text: the three cell kinds of the design.
ROWS: list[list[Any]] = [[1.0, 2.0, "x"], [0.5, None, "y"], [3.25, 1.5, "x"], [0.0, 0.125, "z"]]
LIBRARIES = {"scikit-learn": "1.9.1", "xgboost": "3.4.1", "numpy": "2.5.3", "scipy": "1.18.1"}
MIB = 1024 * 1024
PLANTED = "planted-by-the-artifact"

Edit = Callable[[Any], None]


class UntrustedTypesFoundException(Exception):  # the name skops gives its own error
    pass


def score(row: list[Any]) -> float:
    """The stand-in model: a fixed function of one row (a null is 0, a category adds 1 for ``x``)."""
    a, b, c = row
    return 0.5 * a + 2.0 * (0.0 if b is None or (isinstance(b, float) and math.isnan(b)) else b) + (1.0 if c == "x" else 0.0)


def canonical(document: Any) -> bytes:
    return (json.dumps(document, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode()


def good_documents(closure: str) -> dict[str, Any]:
    """The three good documents, as copies: an edit of one case never reaches the module constants."""
    return copy.deepcopy(_documents(closure))


def _documents(closure: str) -> dict[str, Any]:
    return {
        FEATURE_LIST: {
            "slot": "m1_ols",
            "references": list(REFERENCES),
            "columns": list(COLUMNS),
            "fitted_slot": "m1_ols",
            "kept_references": list(REFERENCES),
            "kept_columns": list(COLUMNS),
        },
        PROVENANCE: {
            "run_id": "train_1",
            "timings": {"fit_seconds": 1.5},
            "try": None,
            "git_commit": "0" * 40,
            "git_dirty": False,
            "code_digest": "1" * 64,
            "model_code_closure_sha256": closure,
            "resolved_versions": {**LIBRARIES, "mlflow": "3.17.2", "mlflow-skinny": "3.17.2", "sqlalchemy": "2.0.0", "psycopg2-binary": None, "libpq": None},
            "inputs": {"snapshot_set": {"feature_matrix_m1": 9, "hospital": 4}, "auxiliary_files": {"adjacency": {"sha256": "2" * 64, "row_count": 120}}},
        },
        SELF_CHECK: {"schema": 1, "slot": "m1_ols", "references": list(REFERENCES), "rows": copy.deepcopy(ROWS), "predictions": [score(row) for row in ROWS]},
    }


@dataclass
class Run:
    """The files of one MLflow run behind the ``fetch`` seam. ``fetch(run, path, local_path, limit)`` writes the file at ``local_path``, which the loader chose,
    and returns it. It records each download; ``other`` replaces what a path returns (a symlink, a folder, another file)."""

    files: dict[str, bytes]
    failing: dict[str, Exception] = field(default_factory=dict)
    other: dict[str, Callable[[str, bytes], str]] = field(default_factory=dict)
    calls: list[tuple[str, str, str]] = field(default_factory=list)
    local_paths: list[str] = field(default_factory=list)
    limits: list[int] = field(default_factory=list)

    def __call__(self, run_id: str, artifact_path: str, local_path: str, limit: int) -> str:
        self.calls.append((run_id, artifact_path, str(Path(local_path).parent)))
        self.local_paths.append(local_path)
        self.limits.append(limit)
        if artifact_path in self.failing:
            raise self.failing[artifact_path]
        if run_id != RUN or artifact_path not in self.files:
            raise FileNotFoundError(f"MlflowException: no artifact {artifact_path!r} in {run_id!r}")
        if artifact_path in self.other:
            return self.other[artifact_path](local_path, self.files[artifact_path])
        Path(local_path).write_bytes(self.files[artifact_path])
        return local_path

    @property
    def paths(self) -> list[str]:
        return [path for _, path, _ in self.calls]

    @property
    def destinations(self) -> set[str]:
        return {destination for _, _, destination in self.calls}


class Skops:
    """A stand-in for ``skops.io``: it records the trusted list and whether the file was there, refuses the planted bytes by skops' error name and returns a
    pipeline whose ``predict`` runs ``predictor`` on the frame the wrapper built."""

    def __init__(self, predictor: Callable[[list[dict[str, Any]]], Any]) -> None:
        self.predictor = predictor
        self.trusted: Any = None
        self.loads: list[str] = []
        self.present: list[bool] = []
        self.frames: list[list[dict[str, Any]]] = []
        self.folders: list[list[str]] = []

    def load(self, file: Any, trusted: Any = None) -> Any:
        self.trusted = trusted
        self.loads.append(str(file))
        self.present.append(Path(file).is_file())
        self.folders.append(sorted(str(item.relative_to(Path(file).parent)) for item in Path(file).parent.rglob("*")))
        if Path(file).read_bytes() == b"UNTRUSTED":
            raise UntrustedTypesFoundException("untrusted types found: ['evil.Thing']")
        return SimpleNamespace(predict=self.predict)

    def predict(self, frame: list[dict[str, Any]]) -> Any:
        self.frames.append(frame)
        return self.predictor(frame)


def default_predictor(frame: list[dict[str, Any]]) -> list[float]:
    row = frame[0]
    return [score([row["a"], row["b"], row["c"]])]


class Clock:
    """A test clock. Only the stand-ins move it, so a deadline of 300 seconds passes in milliseconds and never depends on the speed of the machine."""

    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


class Drip:
    """A response whose body never ends: one byte at a time, each after a test-clock second and a millisecond of real time, so no single read is slow and
    no byte count is near the cap. ``close`` is what a cancel calls, and a read after it fails as a closed connection does. If nothing cuts the drip off
    after ``most`` bytes, the body raises ``AssertionError``, so that a missing deadline fails a case and never hangs it."""

    def __init__(self, clock: Clock, most: int = 2000, stops_after: int | None = None) -> None:
        self.clock = clock
        self.most = most
        self.stops_after = stops_after
        self.pulled = 0
        self.closed = False
        self.status = 200

    def iter_content(self, chunk_size: int) -> Iterator[bytes]:
        while True:
            if self.closed:
                raise OSError("the connection was closed")
            if self.pulled >= self.most:
                raise AssertionError("the drip was never cut off")
            if self.stops_after is not None and self.pulled >= self.stops_after:
                self.hang()
            time.sleep(0.001)
            self.clock.now += 1.0
            self.pulled += 1
            yield b"{"

    def served(self) -> Drip:
        """What the server answers to the GET: this response."""
        return self

    def hang(self) -> None:
        """A read that waits in the socket for a byte that never comes: only closing the connection ends it."""
        give_up = time.monotonic() + 5
        while not self.closed:
            if time.monotonic() > give_up:
                raise AssertionError("the blocked read was never ended by closing the connection")
            self.clock.now += 0.2
            time.sleep(0.001)
        raise OSError("the connection was closed")

    def close(self) -> None:
        self.closed = True


class Server:
    """The artifact store of the tracking server, as MLflow 3.17.2 sees it: files by path, a body that is longer than the file it names (``long``), a listing
    that shows the children of a folder and a streamed GET. It records each GET, the chunks pulled and the most bytes the watched folder held at a pull."""

    def __init__(self, files: dict[str, bytes]) -> None:
        self.files = files
        self.long: dict[str, int] = {}
        self.reads: list[str] = []
        self.pulled: dict[str, int] = {}
        self.peak = 0
        self.watch: Path | None = None
        # The hooks of the deadline cases: a test clock that the stand-in advances while it works, seconds per chunk of a body, a body for a path that never
        # ends (``drips``), gates that stall the listing or the request until the case releases them, and the paths whose response was closed.
        self.clock: Clock | None = None
        self.tick = 0.0
        self.drips: dict[str, Callable[[], Any]] = {}
        self.list_gates: dict[str, threading.Event] = {}
        self.get_gates: dict[str, threading.Event] = {}
        self.closed: list[str] = []

    def wait(self, gate: threading.Event) -> None:
        """Stall as a hung server does: the test clock moves on 0.2 seconds per millisecond until the case releases the gate."""
        give_up = time.monotonic() + 20
        while not gate.is_set():
            if time.monotonic() > give_up:
                raise AssertionError("the stalled call was never cut off")
            if self.clock is not None:
                self.clock.now += 0.2
            time.sleep(0.001)

    def exists(self, path: str) -> bool:
        return path in self.files or path in self.long

    def children(self, path: str) -> list[str]:
        return sorted(name for name in (*self.files, *self.long) if name.startswith(path + "/"))

    def list_artifacts(self, path: str) -> list[Any]:
        """``ArtifactRepository.list_artifacts``: the entries directly below a folder, and an empty list for a file."""
        if path in self.list_gates:
            self.wait(self.list_gates[path])
        prefix = f"{path}/"
        heads: dict[str, bool] = {}
        for name in self.children(path):
            head, _, rest = name[len(prefix) :].partition("/")
            heads[head] = heads.get(head, False) or bool(rest)
        return [SimpleNamespace(path=prefix + head, is_dir=is_dir) for head, is_dir in sorted(heads.items())]

    def held(self) -> int:
        return sum(item.stat().st_size for item in self.watch.rglob("*") if item.is_file()) if self.watch is not None else 0

    def body(self, path: str, chunk_size: int) -> Iterator[bytes]:
        """The chunks of one file. ``iter_content`` of a real response pulls one chunk per step, so the bytes on disk at a pull are the loader's own writes."""
        data = None if path in self.long else self.files[path]
        size = self.long[path] if data is None else len(data)
        for start in range(0, size, chunk_size):
            if self.clock is not None:
                self.clock.now += self.tick
            self.pulled[path] = self.pulled.get(path, 0) + 1
            self.peak = max(self.peak, self.held())
            yield b"{" * min(chunk_size, size - start) if data is None else data[start : start + chunk_size]

    def get(self, path: str) -> Any:
        """``http_request(host_creds, '/' + path, 'GET', stream=True)``: a 404 for no file and a 400 for a folder."""
        self.reads.append(path)
        if path in self.get_gates:
            self.wait(self.get_gates[path])
        if path in self.drips:
            return self.drips[path]()
        status = 200 if self.exists(path) else 400 if self.children(path) else 404
        return SimpleNamespace(status=status, iter_content=lambda chunk_size: self.body(path, chunk_size), close=lambda: self.closed.append(path))

    def download_artifacts(self, artifact_path: str, dst_path: str) -> str:
        """``ArtifactRepository.download_artifacts`` of 3.17.2: a listing decides file or folder, a folder is downloaded with every descendant, and
        ``HttpArtifactRepository._download_file`` writes every chunk with no limit (``artifact_repo.py:346``, ``http_artifact_repo.py:145``)."""
        names = self.children(artifact_path) if self.list_artifacts(artifact_path) else [artifact_path]
        for name in names:
            self.reads.append(name)
            target = Path(dst_path) / name
            target.parent.mkdir(parents=True, exist_ok=True)
            with open(target, "wb") as out:
                for chunk in self.body(name, 1024 * 1024):
                    out.write(chunk)
        return str(Path(dst_path) / artifact_path)


class Registry:
    """A stand-in for the MLflow module: the version and run lookups, the default download call and a ``sklearn.load_model`` that must never run."""

    def __init__(self, tags: dict[str, str], source: str, server: Server) -> None:
        self.tags = tags
        self.source = source
        self.server = server
        self.sklearn_loads = 0
        self.direct_reads = 0
        self.default_calls: list[dict[str, Any]] = []
        self.artifacts = SimpleNamespace(download_artifacts=self.download_artifacts)
        self.sklearn = SimpleNamespace(load_model=self.load_model)
        self.lookup_gate: threading.Event | None = None
        self.MlflowClient = lambda: SimpleNamespace(
            get_model_version=self.model_version,
            get_run=lambda run_id: SimpleNamespace(info=SimpleNamespace(artifact_uri=ARTIFACT_URI)),
        )

    def model_version(self, name: str, version: str) -> Any:
        if self.lookup_gate is not None:
            self.server.wait(self.lookup_gate)
        return SimpleNamespace(run_id=RUN, tags=dict(self.tags), source=self.source)

    def load_model(self, uri: str) -> Any:
        self.sklearn_loads += 1
        raise AssertionError("mlflow.sklearn.load_model must never be called")

    def download_artifacts(self, run_id: str, artifact_path: str, dst_path: str | None = None) -> str:
        """The MLflow 3.17.2 call over an S3 artifact store: with no proxy setting the client reads S3 directly, which the task cannot."""
        if os.environ.get(PROXY_SWITCH, "").lower() != "false":
            self.direct_reads += 1
            raise RuntimeError(f"MlflowException: Failed to download artifacts from path {artifact_path!r}, please ensure that the path is correct.")
        self.default_calls.append({"run_id": run_id, "artifact_path": artifact_path, "dst_path": dst_path})
        return self.server.download_artifacts(artifact_path, str(dst_path or tempfile.mkdtemp()))

    def modules(self) -> dict[str, Any]:
        """The MLflow modules the default fetch imports, over the same server: the repository of the run, the credentials and the streamed request."""

        class HttpArtifactRepository:
            artifact_uri = ARTIFACT_URI
            list_artifacts = staticmethod(self.server.list_artifacts)

        def http_request(creds: Any, endpoint: str, method: str, **options: Any) -> Any:
            if method != "GET" or options != {"stream": True}:
                raise AssertionError(f"the download is a streamed GET: {method} {options}")
            return self.server.get(endpoint.removeprefix("/"))

        def raise_for_status(response: Any) -> None:
            if response.status != 200:
                raise RuntimeError(f"MlflowException: HTTP {response.status}")

        return {
            "mlflow": self,
            "mlflow.store": SimpleNamespace(),
            "mlflow.store.artifact": SimpleNamespace(),
            "mlflow.store.artifact.artifact_repository_registry": SimpleNamespace(get_artifact_repository=lambda uri: HttpArtifactRepository()),
            "mlflow.store.artifact.http_artifact_repo": SimpleNamespace(HttpArtifactRepository=HttpArtifactRepository),
            "mlflow.utils": SimpleNamespace(),
            "mlflow.utils.credentials": SimpleNamespace(get_default_host_creds=lambda uri: SimpleNamespace(host=uri)),
            "mlflow.utils.rest_utils": SimpleNamespace(http_request=http_request, augmented_raise_for_status=raise_for_status),
        }


@dataclass
class World:
    root: Path
    tmp: Path
    run: Run
    skops: Skops
    registry: Registry
    closure: str
    server: Server

    def attempt(self, use_default_fetch: bool = False) -> tuple[Any, Exception | None]:
        try:
            loader = mlflow_loader.MlflowLoader(root=self.root) if use_default_fetch else mlflow_loader.MlflowLoader(fetch=self.run, root=self.root)
            return loader.load(HAI, VERSION), None
        except Exception as error:
            return None, error

    def leftover(self) -> list[str]:
        return sorted(os.listdir(self.tmp))


def copy_image_files(target: Path) -> Path:
    """The files the image holds for this check: the closure, the image-provided files, the record and the contract."""
    record = json.loads((ROOT / model_code.RECORD).read_text())
    names = [item["path"] for item in record["files"]] + list(record["image_provided"]) + [model_code.RECORD, CONTRACT]
    for name in names:
        (target / name).parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / name, target / name)
    return target


@contextlib.contextmanager
def world(
    *,
    feature: Edit | None = None,
    provenance: Edit | None = None,
    self_check: Edit | None = None,
    raw: Mapping[str, bytes] | None = None,
    drop: tuple[str, ...] = (),
    failing: Mapping[str, Exception] | None = None,
    installed: Mapping[str, str] | None = None,
    tags: dict[str, str] | None = None,
    source: str | None = None,
    predictor: Callable[[list[dict[str, Any]]], Any] = default_predictor,
    model_bytes: bytes = b"SKOPS-STAND-IN",
    long: Mapping[str, int] | None = None,
    other: Mapping[str, Callable[[str, bytes], str]] | None = None,
) -> Iterator[World]:
    """One serving image over stand-in libraries. Edits change the good documents; ``raw`` replaces whole files; every temporary file lands in ``tmp``."""
    with tempfile.TemporaryDirectory() as base, contextlib.ExitStack() as stack:
        folder = Path(base)
        tmp = folder / "tmp"
        tmp.mkdir()
        root = copy_image_files(folder / "image")
        closure = model_code.closure_digest(root)
        documents = good_documents(closure)
        for path, edit in ((FEATURE_LIST, feature), (PROVENANCE, provenance), (SELF_CHECK, self_check)):
            if edit is not None:
                edit(documents[path])
        files = {path: canonical(document) for path, document in documents.items()} | {MODEL: model_bytes}
        files |= {"model/MLmodel": b"trusted_types: [evil.Thing]", "model/code/evil.py": f"open({str(tmp / PLANTED)!r}, 'w')".encode()}
        files |= dict(raw or {})
        for path in drop:
            files.pop(path, None)
        run = Run(files, dict(failing or {}), dict(other or {}))
        server = Server(files)
        server.watch = tmp
        server.long = dict(long or {})
        registry = Registry({"feature_schema_sha256": "hash1"} if tags is None else tags, f"runs:/{RUN}/model" if source is None else source, server)
        skops = Skops(predictor)
        versions = dict(LIBRARIES) if installed is None else dict(installed)

        def installed_version(name: str) -> str:
            if name not in versions:
                raise importlib.metadata.PackageNotFoundError(name)
            return versions[name]

        stack.enter_context(
            mock.patch.dict(sys.modules, {**registry.modules(), "pandas": SimpleNamespace(DataFrame=lambda rows: rows), "skops": skops, "skops.io": skops})
        )
        stack.enter_context(mock.patch.dict(os.environ, {key: value for key, value in os.environ.items() if key != PROXY_SWITCH}, clear=True))
        stack.enter_context(mock.patch.object(tempfile, "tempdir", str(tmp)))
        stack.enter_context(mock.patch.object(mlflow_loader, "installed_version", installed_version))
        yield World(root, tmp, run, skops, registry, closure, server)


def refused(c: Checker, label: str, expected: str, **options: Any) -> None:
    """A load with these edits is refused as ``expected`` (by class name) and leaves no downloaded file and no download folder."""
    with world(**options) as w:
        model, error = w.attempt()
        c.check(
            f"{label}: refused as {expected} (got {type(error).__name__ if error else 'a loaded model'})", model is None and type(error).__name__ == expected
        )
        c.check(f"{label}: no downloaded file or folder remains", w.leftover() == [] and not any(Path(d).exists() for d in w.run.destinations))
        c.check(f"{label}: nothing outside the four paths was asked for", set(w.run.paths) <= set(FOUR))


def empty_references(document: dict[str, Any]) -> None:
    document.update(references=[], columns=[])


def wrong_closure_and_numpy(document: dict[str, Any]) -> None:
    document["model_code_closure_sha256"] = "0" * 64
    document["resolved_versions"]["numpy"] = "0"


def null_row_zero_for_near_zero(document: dict[str, Any]) -> None:
    document["rows"][0] = [0.0, 0.0, "z"]
    document["predictions"][0] = 1e-13


def too_many(document: dict[str, Any]) -> None:
    count = mlflow_loader.MAX_FEATURES + 1
    document.update(references=[f"r:{n}" for n in range(count)], columns=[f"c{n}" for n in range(count)])


def scale_prediction(index: int, factor: float, document: dict[str, Any]) -> None:
    document["predictions"][index] *= factor


def drop_key(key: str) -> Edit:
    return lambda document: document.pop(key)


def set_key(key: str, value: Any) -> Edit:
    return lambda document: document.__setitem__(key, value)


def case_loader_asks_for_exactly_the_four_paths_and_loads_data_only(c: Checker) -> None:
    """A good version loads from exactly the four named files. The planted MLmodel and code folder are never asked for, so nothing of them runs, and the
    loader never calls ``mlflow.sklearn.load_model``. skops gets the image's own contract list."""
    contract = json.loads((ROOT / CONTRACT).read_text())["serialization"]["trusted_types"]
    with world() as w:
        model, error = w.attempt()
        replayed = list(w.skops.frames)
        c.check(f"a good version loads (error: {error!r})", model is not None and error is None)
        c.check("the loader asked for exactly the four paths, once each", sorted(w.run.paths) == sorted(FOUR) and len(w.run.paths) == 4)
        c.check(
            "none of them is the MLmodel or anything under code/", not any(path.startswith("model/code") or path.endswith("MLmodel") for path in w.run.paths)
        )
        c.check("every download names the run of the registry's source", {run for run, _, _ in w.run.calls} == {RUN})
        c.check("the planted code file never appeared on disk", not (w.tmp / PLANTED).exists())
        c.check("mlflow.sklearn.load_model was never called", w.registry.sklearn_loads == 0)
        try:
            w.registry.sklearn.load_model("models:/probe")
            armed = False
        except AssertionError:
            armed = True
        c.check("the stand-in's load_model is armed: a call would fail the load", armed and w.registry.sklearn_loads == 1)
        c.check("skops was called once", len(w.skops.loads) == 1)
        c.check(
            "skops got exactly the image's contract list, not the model's MLmodel list",
            w.skops.trusted is not None and list(w.skops.trusted) == contract and "evil.Thing" not in w.skops.trusted,
        )
        c.check("the model file was on disk while skops read it", w.skops.present == [True])
        c.check(
            "the loaded version carries its references, schema hash and an empty feature set",
            model.references == ("a:1", "b:2", "c:3") and model.schema_hash == "hash1" and model.feature_set == "",
        )
        c.check("the wrapper predicts the stand-in score", model.predict({"a:1": 1.0, "b:2": 2.0, "c:3": "x"}) == score([1.0, 2.0, "x"]))
        c.check("the wrapper turns a null into NaN, in the self-check too", math.isnan(replayed[1][0]["b"]))
        c.check("the self-check ran every saved row through the wrapper, once each", len(replayed) == len(ROWS) and replayed[0][0]["c"] == "x")
        c.check("the proxy setting is false for every later download in the process", os.environ.get(PROXY_SWITCH) == "false")
    c.check("the caps are 1 MiB for each JSON file and 64 MiB for the model", (mlflow_loader.JSON_CAP, mlflow_loader.MODEL_CAP) == (MIB, 64 * MIB))


def case_loader_downloads_into_one_temporary_folder_removed_every_time(c: Checker) -> None:
    """One ``TemporaryDirectory`` per load is the destination of all four downloads. It is gone after a success and every refusal, also over repeated loads."""
    with world() as w:
        model, error = w.attempt()
        c.check(f"the load succeeds ({error!r})", model is not None)
        c.check("all four downloads share one destination", len(w.run.destinations) == 1 and len(w.run.calls) == 4)
        destination = next(iter(w.run.destinations))
        c.check("the destination was made inside the temporary folder of the process", Path(destination).parent == w.tmp)
        c.check("after the load, no file and no folder remain", w.leftover() == [] and not Path(destination).exists())
        for number in range(3):
            w.attempt()
            c.check(f"repeated load {number + 2} leaves nothing", w.leftover() == [])
        w.run.calls.clear()
        w.run.files[PROVENANCE] = b"{"
        for number in range(3):
            _, error = w.attempt()
            c.check(f"repeated refusal {number + 1} leaves nothing", type(error).__name__ == "ArtifactRefused" and w.leftover() == [])
        c.check("each load made its own folder", len(w.run.destinations) == 3)
    with world(model_bytes=b"UNTRUSTED") as w:
        _, error = w.attempt()
        c.check("skops' untrusted-types error leaves nothing", type(error).__name__ == "UntrustedTypesFoundException" and w.leftover() == [])
    with world(failing={MODEL: RuntimeError("MlflowException: 403")}) as w:
        _, error = w.attempt()
        c.check("a model download that fails leaves nothing and keeps its own error", type(error).__name__ == "RuntimeError" and w.leftover() == [])
    with world(drop=(FEATURE_LIST,)) as w:
        _, error = w.attempt()
        c.check("a missing feature list leaves nothing and keeps the download error", type(error).__name__ == "FileNotFoundError" and w.leftover() == [])


def case_loader_refuses_the_closure_and_the_libraries_by_name(c: Checker) -> None:
    """The exact-equality admission: the model's closure digest equals the image's and the four library versions equal the installed ones."""
    refused(c, "a closure that differs", "ModelCodeMismatch", provenance=set_key("model_code_closure_sha256", "0" * 64))
    refused(c, "no closure key", "ModelCodeMissing", provenance=drop_key("model_code_closure_sha256"))
    refused(c, "a null closure", "ModelCodeMissing", provenance=set_key("model_code_closure_sha256", None))
    refused(c, "no provenance file", "ModelCodeMissing", drop=(PROVENANCE,))
    refused(c, "a closure that is not 64 lowercase hex", "ArtifactRefused", provenance=set_key("model_code_closure_sha256", "abc"))
    refused(
        c,
        "an uppercase closure",
        "ArtifactRefused",
        provenance=lambda document: document.__setitem__("model_code_closure_sha256", document["model_code_closure_sha256"].upper()),
    )
    refused(c, "a closure that is a number", "ArtifactRefused", provenance=set_key("model_code_closure_sha256", 7))
    refused(c, "a closure and a library that both differ: the closure is named first", "ModelCodeMismatch", provenance=wrong_closure_and_numpy)
    for library in LIBRARIES:
        refused(c, f"{library} at another version", "LibraryMismatch", provenance=lambda d, name=library: d["resolved_versions"].__setitem__(name, "0.0.1"))
        refused(c, f"{library} recorded as null", "LibraryMismatch", provenance=lambda d, name=library: d["resolved_versions"].__setitem__(name, None))
        refused(c, f"{library} not recorded", "LibraryMismatch", provenance=lambda d, name=library: d["resolved_versions"].pop(name))
        refused(c, f"{library} not installed in the image", "LibraryMismatch", installed={name: value for name, value in LIBRARIES.items() if name != library})
    refused(c, "resolved_versions that is not an object", "ArtifactRefused", provenance=set_key("resolved_versions", ["numpy"]))
    refused(c, "a library version that is a number", "ArtifactRefused", provenance=lambda d: d["resolved_versions"].__setitem__("numpy", 2))
    refused(c, "provenance that is not an object", "ArtifactRefused", raw={PROVENANCE: b"[]"})
    with world(provenance=lambda d: d["resolved_versions"].__setitem__("mlflow", "0.0.1")) as w:
        model, _ = w.attempt()
        c.check("a version of a library outside the four does not matter", model is not None)
    with world(provenance=set_key("model_code_closure_sha256", "0" * 64)) as w:
        w.attempt()
        c.check("a closure refusal comes before the model download and the self-check", MODEL not in w.run.paths and SELF_CHECK not in w.run.paths)


def case_loader_refuses_wrong_source_size_and_bad_json_by_name(c: Checker) -> None:
    """The registry source form, the size caps and the shape of the two bundle files the loader reads."""
    for source in (
        "runs:/other/model",
        f"runs:/{RUN}/model/",
        f"runs:/{RUN}/other",
        f"models:/hai_model_{HAI}/{VERSION}",
        "s3://bucket/run1/model",
        "",
        f"runs:/{RUN}/model/code",
    ):
        refused(c, f"source {source!r}", "ArtifactRefused", source=source)
    with world(source="runs:/other/model") as w:
        w.attempt()
        c.check("a wrong source stops before any download", w.run.calls == [])
    padding = b" " * MIB
    refused(c, "a feature list over 1 MiB", "ArtifactRefused", raw={FEATURE_LIST: canonical(good_documents("0" * 64)[FEATURE_LIST]) + padding})
    refused(c, "a provenance file over 1 MiB", "ArtifactRefused", raw={PROVENANCE: canonical(good_documents("0" * 64)[PROVENANCE]) + padding})
    with mock.patch.object(mlflow_loader, "MODEL_CAP", 1024):
        refused(c, "a model file over its cap", "ArtifactRefused", model_bytes=b"x" * 1025)
        with world(model_bytes=b"x" * 1024) as w:
            model, error = w.attempt()
            c.check(f"a model file exactly at its cap is read ({error!r})", model is not None)
    for label, text in (
        ("text that is not JSON", b"{"),
        ("an empty file", b""),
        ("a NaN token", b'{"references":["a:1"],"columns":[NaN]}'),
        ("an Infinity token", b'{"references":["a:1"],"columns":[Infinity]}'),
        ("a duplicate key", b'{"references":["a:1"],"columns":["a"],"columns":["a"]}'),
        ("a list instead of an object", b"[]"),
        ("bytes that are not UTF-8", b"\xff\xfe"),
    ):
        refused(c, f"a feature list that is {label}", "ArtifactRefused", raw={FEATURE_LIST: text})
    refused(c, "no references key", "ArtifactRefused", feature=drop_key("references"))
    refused(c, "no columns key", "ArtifactRefused", feature=drop_key("columns"))
    refused(c, "references that is a string", "ArtifactRefused", feature=set_key("references", "a:1"))
    refused(c, "a reference that is a number", "ArtifactRefused", feature=lambda d: d["references"].__setitem__(0, 1))
    refused(c, "a column that is null", "ArtifactRefused", feature=lambda d: d["columns"].__setitem__(0, None))
    refused(c, "references and columns of unequal length", "ArtifactRefused", feature=lambda d: d["columns"].pop())
    refused(c, "too many references", "ArtifactRefused", feature=too_many)
    refused(c, "a reference that is too long", "ArtifactRefused", feature=lambda d: d["references"].__setitem__(0, "r" * (mlflow_loader.MAX_TEXT + 1)))
    refused(c, "a column that is too long", "ArtifactRefused", feature=lambda d: d["columns"].__setitem__(0, "c" * (mlflow_loader.MAX_TEXT + 1)))
    # A feature list for a set model has to name references.
    with world(tags={"feature_schema_sha256": "whole", "feature_set": "m2", "feature_set_schema_sha256": "setdigest"}, feature=empty_references) as w:
        _, error = w.attempt()
        c.check("a set model whose feature list names no references does not load and leaves nothing", isinstance(error, ValueError) and w.leftover() == [])


def case_loader_passes_the_image_trust_list_and_never_the_models(c: Checker) -> None:
    """A model that lists extra trusted types in its own MLmodel, or holds a class outside the image's list, never widens what skops trusts."""
    contract = json.loads((ROOT / CONTRACT).read_text())["serialization"]["trusted_types"]
    with world() as w:
        w.attempt()
        c.check("the planted MLmodel was never downloaded", "model/MLmodel" not in w.run.paths)
        c.check("the trusted list is the contract's list of the image, in order", w.skops.trusted is not None and list(w.skops.trusted) == contract)
    with world(model_bytes=b"UNTRUSTED") as w:
        model, error = w.attempt()
        c.check("a class outside the list is refused by skops' own error name", model is None and type(error).__name__ == "UntrustedTypesFoundException")
        c.check("that refusal comes after the admission and before any prediction", len(w.skops.frames) == 0)


def case_loader_self_check_is_missing_or_malformed_by_name(c: Checker) -> None:
    """The saved prediction check is required (``SelfCheckMissing``), and its file is checked as data (``ArtifactRefused``)."""
    refused(c, "no self-check file in the run", "SelfCheckMissing", drop=(SELF_CHECK,))
    refused(c, "a self-check download that fails", "SelfCheckMissing", failing={SELF_CHECK: RuntimeError("MlflowException: 404")})
    refused(c, "a self-check file of 1 MiB and one byte", "ArtifactRefused", raw={SELF_CHECK: canonical(good_documents("0" * 64)[SELF_CHECK]) + b" " * MIB})
    for label, text in (
        ("text that is not JSON", b"{"),
        ("a list", b"[]"),
        ("a NaN token", b'{"schema":1,"slot":"m1_ols","references":["a:1","b:2","c:3"],"rows":[[NaN,1.0,"x"]],"predictions":[1.0]}'),
        ("an Infinity prediction", b'{"schema":1,"slot":"m1_ols","references":["a:1","b:2","c:3"],"rows":[[1.0,1.0,"x"]],"predictions":[Infinity]}'),
        ("a number too large for a float", b'{"schema":1,"slot":"m1_ols","references":["a:1","b:2","c:3"],"rows":[[1.0,1.0,"x"]],"predictions":[1e999]}'),
        ("a duplicate key", b'{"schema":1,"schema":1,"slot":"m1_ols","references":["a:1","b:2","c:3"],"rows":[[1.0,1.0,"x"]],"predictions":[1.0]}'),
    ):
        refused(c, f"a self-check file with {label}", "ArtifactRefused", raw={SELF_CHECK: text})
    refused(c, "an extra key", "ArtifactRefused", self_check=set_key("extra", 1))
    for key in ("schema", "slot", "references", "rows", "predictions"):
        refused(c, f"no {key} key", "ArtifactRefused", self_check=drop_key(key))
    refused(c, "schema 2", "ArtifactRefused", self_check=set_key("schema", 2))
    refused(c, "schema true", "ArtifactRefused", self_check=set_key("schema", True))
    refused(c, "a slot that is not text", "ArtifactRefused", self_check=set_key("slot", 1))
    refused(c, "references that differ from feature_list.json", "ArtifactRefused", self_check=lambda d: d["references"].reverse())
    refused(c, "one reference fewer", "ArtifactRefused", self_check=lambda d: d.update(references=d["references"][:2], rows=[row[:2] for row in d["rows"]]))
    refused(c, "no rows", "ArtifactRefused", self_check=lambda d: d.update(rows=[], predictions=[]))
    refused(c, "9 rows", "ArtifactRefused", self_check=lambda d: d.update(rows=[d["rows"][0]] * 9, predictions=[d["predictions"][0]] * 9))
    with world(self_check=lambda d: d.update(rows=[d["rows"][0]] * 8, predictions=[d["predictions"][0]] * 8)) as w:
        model, error = w.attempt()
        c.check(f"8 rows is the largest allowed size ({error!r})", model is not None)
    refused(c, "a row one cell short", "ArtifactRefused", self_check=lambda d: d["rows"][1].pop())
    refused(c, "a row one cell long", "ArtifactRefused", self_check=lambda d: d["rows"][1].append(1.0))
    refused(c, "a row that is not a list", "ArtifactRefused", self_check=lambda d: d["rows"].__setitem__(0, "x"))
    refused(c, "one prediction fewer than rows", "ArtifactRefused", self_check=lambda d: d["predictions"].pop())
    refused(c, "one prediction more than rows", "ArtifactRefused", self_check=lambda d: d["predictions"].append(1.0))
    refused(c, "a boolean cell", "ArtifactRefused", self_check=lambda d: d["rows"][0].__setitem__(0, True))
    refused(c, "an object cell", "ArtifactRefused", self_check=lambda d: d["rows"][0].__setitem__(0, {"a": 1}))
    refused(c, "a list cell", "ArtifactRefused", self_check=lambda d: d["rows"][0].__setitem__(0, [1]))
    refused(c, "a text cell over the length bound", "ArtifactRefused", self_check=lambda d: d["rows"][0].__setitem__(2, "x" * (mlflow_loader.MAX_TEXT + 1)))
    refused(c, "a cell that is an integer too large for a float", "ArtifactRefused", self_check=lambda d: d["rows"][0].__setitem__(0, 10**400))
    refused(c, "a prediction that is an integer too large for a float", "ArtifactRefused", self_check=lambda d: d["predictions"].__setitem__(0, 10**400))
    refused(c, "a prediction that is text", "ArtifactRefused", self_check=lambda d: d["predictions"].__setitem__(0, "1.0"))
    refused(c, "a prediction that is a boolean", "ArtifactRefused", self_check=lambda d: d["predictions"].__setitem__(0, True))
    refused(c, "a prediction that is null", "ArtifactRefused", self_check=lambda d: d["predictions"].__setitem__(0, None))
    with world() as w:
        w.attempt()
        c.check("the self-check was downloaded before the model", w.run.paths.index(SELF_CHECK) < w.run.paths.index(MODEL))


def case_loader_self_check_refuses_a_model_that_does_not_reproduce_its_predictions(c: Checker) -> None:
    """Every saved row goes through the wrapper; each result must be a finite number within ``math.isclose(rel_tol=1e-9, abs_tol=1e-12)``."""
    refused(
        c,
        "one saved prediction 1e-6 higher (relative)",
        "PredictionMismatch",
        self_check=lambda d: d["predictions"].__setitem__(2, d["predictions"][2] * (1 + 1e-6)),
    )
    refused(
        c,
        "one saved prediction 1e-6 lower (relative)",
        "PredictionMismatch",
        self_check=lambda d: d["predictions"].__setitem__(0, d["predictions"][0] * (1 - 1e-6)),
    )
    refused(c, "the last saved prediction different", "PredictionMismatch", self_check=lambda d: d["predictions"].__setitem__(3, d["predictions"][3] + 1.0))
    for label, predictor in (
        ("a model that raises", lambda frame: (_ for _ in ()).throw(ValueError("boom"))),
        ("a model that returns NaN", lambda frame: [math.nan]),
        ("a model that returns infinity", lambda frame: [math.inf]),
        ("a model that returns nothing", lambda frame: []),
        ("a model that returns a row of numbers", lambda frame: [[1.0, 2.0]]),
        ("a model that returns text", lambda frame: ["x"]),
        ("a model that returns None", lambda frame: [None]),
    ):
        refused(c, label, "PredictionMismatch", predictor=predictor)
    refused(
        c,
        "a model that fails only on the row with a null",
        "PredictionMismatch",
        predictor=lambda frame: [score([frame[0]["a"], frame[0]["b"], frame[0]["c"]]) if not math.isnan(frame[0]["b"]) else math.nan],
    )
    for scale, label, expected in ((1 + 1e-10, "1e-10 relative is within the tolerance", True), (1 + 5e-9, "5e-9 relative is outside the tolerance", False)):
        with world(self_check=partial(scale_prediction, 2, scale)) as w:
            model, error = w.attempt()
            c.check(
                f"{label} ({type(error).__name__ if error else 'loaded'})",
                (model is not None) is expected and (expected or type(error).__name__ == "PredictionMismatch"),
            )
    with world(self_check=lambda d: d["predictions"].__setitem__(1, d["predictions"][1] + 5e-13)) as w:
        model, error = w.attempt()
        c.check(f"an absolute difference of 5e-13 is within the 1e-12 floor ({error!r})", model is not None)
    with world(self_check=lambda d: d["predictions"].__setitem__(1, d["predictions"][1] + 1e-8)) as w:
        _, error = w.attempt()
        c.check("an absolute difference of 1e-8 on a value of 0.25 is outside the relative tolerance", type(error).__name__ == "PredictionMismatch")
    with world(self_check=null_row_zero_for_near_zero) as w:
        model, error = w.attempt()
        c.check(f"a prediction near zero is compared with the absolute floor ({error!r})", model is not None)
    with world(self_check=lambda d: d["predictions"].__setitem__(2, d["predictions"][2] * (1 + 1e-6))) as w:
        _, error = w.attempt()
        c.check("the message names the row", "row 2" in str(error))
        c.check("the message does not hold the saved or computed prediction", all(token not in str(error) for token in ("3.25", "5.625", "1.5")))


def case_loader_logs_one_line_with_the_version_and_the_peak_memory(c: Checker) -> None:
    """One log line per load names the version and the process peak memory (the 1280 MiB limit is unmeasured)."""
    records: list[str] = []
    files: dict[str, bytes] = {}

    class Capture(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(record.getMessage())

    handler = Capture()
    logger = logging.getLogger(mlflow_loader.__name__)
    logger.addHandler(handler)
    previous = logger.level
    logger.setLevel(logging.INFO)
    try:
        with world() as w:
            w.attempt()
            files = dict(w.run.files)
    finally:
        logger.removeHandler(handler)
        logger.setLevel(previous)
    lines = [line for line in records if f"version {VERSION}" in line and "peak" in line]
    c.check("one line names the version and the peak memory", len(lines) == 1 and "MiB" in lines[0])
    sizes = [f"'{path}': {len(data)}" for path, data in files.items() if path in FOUR]
    c.check(
        "the same line holds the real size in bytes of each of the four files", len(lines) == 1 and all(size in lines[0] for size in sizes) and len(sizes) == 4
    )


def only_references_and_columns(document: dict[str, Any]) -> None:
    for key in ("slot", "fitted_slot", "kept_references", "kept_columns"):
        document.pop(key)


def nested(depth: int) -> Any:
    value: Any = 1
    for _ in range(depth):
        value = [value]
    return value


def case_loader_default_fetch_stops_a_download_at_its_cap(c: Checker) -> None:
    """The default fetch counts the bytes it writes and stops the transport at the path's cap: a response of 5 MiB for a 1 MiB cap is refused as
    ``ArtifactRefused`` after the first chunk, the temporary folder never holds more than the cap plus one chunk (real MLflow wrote 2 MiB for the 1 MiB
    JSON cap before the refusal) and nothing remains."""
    size = 5 * MIB + 2
    for path in FOUR:
        with mock.patch.object(mlflow_loader, "MODEL_CAP", MIB), world(long={path: size}) as w:
            model, error = w.attempt(use_default_fetch=True)
            c.check(
                f"{path}: a response of {size} bytes is refused as ArtifactRefused (got {type(error).__name__ if error else 'a loaded model'})",
                model is None and type(error).__name__ == "ArtifactRefused",
            )
            c.check(f"{path}: the folder held at most the cap plus one chunk ({w.server.peak} bytes)", w.server.peak <= 2 * MIB)
            c.check(f"{path}: the transport was stopped, {w.server.pulled.get(path)} chunks pulled", 1 <= w.server.pulled.get(path, 0) <= 2)
            c.check(f"{path}: no downloaded file or folder remains", w.leftover() == [])
    padded = canonical(good_documents("0" * 64)[FEATURE_LIST])
    with world(raw={FEATURE_LIST: padded + b" " * (MIB - len(padded))}) as w:
        model, error = w.attempt(use_default_fetch=True)
        c.check(f"a feature list of exactly 1 MiB is read ({error!r})", model is not None)
    with world(raw={FEATURE_LIST: padded + b" " * (MIB + 1 - len(padded))}) as w:
        _, error = w.attempt(use_default_fetch=True)
        c.check("a feature list of 1 MiB and one byte is refused as ArtifactRefused", type(error).__name__ == "ArtifactRefused" and w.leftover() == [])


def case_loader_default_fetch_stops_the_model_at_its_own_cap(c: Checker) -> None:
    """The model file has its own cap, 64 MiB, checked while it is written: a response one byte over it and one of 300 MiB (more than the 256 MiB temporary
    filesystem) are both refused as ``ArtifactRefused``, and the folder never holds more than the cap plus one chunk. A file of exactly the cap is read."""
    cap = mlflow_loader.MODEL_CAP
    c.check("the model cap is 64 MiB", cap == 64 * MIB)
    for label, size in (("one byte over the cap", cap + 1), ("300 MiB", 300 * MIB)):
        with world(long={MODEL: size}) as w:
            model, error = w.attempt(use_default_fetch=True)
            c.check(
                f"a model response of {label} is refused as ArtifactRefused (got {type(error).__name__ if error else 'a loaded model'})",
                model is None and type(error).__name__ == "ArtifactRefused",
            )
            c.check(f"{label}: the folder held at most the cap plus one chunk ({w.server.peak} bytes)", w.server.peak <= cap + MIB)
            c.check(f"{label}: at most {cap // MIB + 1} chunks were pulled ({w.server.pulled[MODEL]})", w.server.pulled[MODEL] <= cap // MIB + 1)
            c.check(
                f"{label}: the three small files were read first and nothing remains",
                w.server.reads[:3] == [FEATURE_LIST, PROVENANCE, SELF_CHECK] and w.leftover() == [],
            )
    with world(long={MODEL: cap}) as w:
        model, error = w.attempt(use_default_fetch=True)
        c.check(f"a model response of exactly the cap is read ({error!r})", model is not None and w.leftover() == [])


def case_loader_default_fetch_refuses_a_folder_without_fetching_a_descendant(c: Checker) -> None:
    """A named path that is a folder is refused as ``ArtifactRefused`` for each of the four files, and no file below it is fetched (real MLflow downloaded
    ``bundle/feature_list.json/unexpected.py`` when the named path was a folder)."""
    for path in FOUR:
        with world(raw={f"{path}/unexpected.py": b"print(1)"}, drop=(path,)) as w:
            model, error = w.attempt(use_default_fetch=True)
            c.check(
                f"{path} as a folder: refused as ArtifactRefused (got {type(error).__name__ if error else 'a loaded model'})",
                model is None and type(error).__name__ == "ArtifactRefused",
            )
            c.check(f"{path} as a folder: nothing below it was fetched ({w.server.reads})", not any(read.startswith(path + "/") for read in w.server.reads))
            c.check(f"{path} as a folder: no downloaded file or folder remains", w.leftover() == [])


def case_loader_default_fetch_writes_four_fixed_names_into_one_folder(c: Checker) -> None:
    """A good load GETs exactly the four paths once each and writes them under the loader's four fixed names in one flat folder: the artifact never
    chooses a local name or a subfolder."""
    with world() as w:
        model, error = w.attempt(use_default_fetch=True)
        c.check(f"the load succeeds ({error!r})", model is not None)
        c.check(f"the four paths were fetched once each ({w.server.reads})", sorted(w.server.reads) == sorted(FOUR))
        c.check(
            f"the folder holds the four fixed names and no subfolder ({w.skops.folders})",
            w.skops.folders == [["feature_list.json", "model.skops", "provenance.json", "self_check.json"]],
        )
        c.check("the proxy setting is false before the first download", os.environ.get(PROXY_SWITCH) == "false" and w.registry.direct_reads == 0)
        c.check("mlflow's recursive download call was never used", w.registry.default_calls == [])
        c.check("nothing remains after the load", w.leftover() == [])


def symlink_to_the_content(local: str, data: bytes) -> str:
    real = Path(local).with_name("real_content")
    real.write_bytes(data)
    os.symlink(real, local)
    return local


def folder_instead_of_a_file(local: str, data: bytes) -> str:
    os.mkdir(local)
    return local


def another_file(local: str, data: bytes) -> str:
    other = Path(local).with_name("other_name")
    other.write_bytes(data)
    return str(other)


def fetch_workers() -> list[threading.Thread]:
    return [thread for thread in threading.enumerate() if thread.name.startswith("model-fetch")]


@contextlib.contextmanager
def test_clock(w: World) -> Iterator[Clock]:
    """The default fetch reads this clock, checks it every 5 milliseconds and waits 0.2 seconds at most for a worker it has cancelled."""
    clock = Clock()
    w.server.clock = clock
    with (
        mock.patch.object(model_code, "monotonic", clock, create=True),
        mock.patch.object(model_code, "POLL_SECONDS", 0.005, create=True),
        mock.patch.object(model_code, "GRACE_SECONDS", 0.2, create=True),
    ):
        yield clock


def case_loader_default_fetch_is_cut_off_at_its_deadline(c: Checker) -> None:
    """One fetch has one total deadline for its run lookup, its listing, its request with every retry and its body, however slowly the server answers.
    A drip (a byte every second of the test clock, each read short, so no socket timeout trips), a read blocked in the socket, a stalled listing, a
    stalled request and a stalled lookup are each refused as ``ArtifactRefused`` by the deadline of the file kind (30 seconds for the JSON files, 300 for the
    model), the transport is stopped and closed, nothing remains on disk and no worker thread outlives its release. A fetch well inside its deadline is not
    affected."""
    json_seconds, model_seconds = model_code.JSON_FETCH_SECONDS, model_code.MODEL_FETCH_SECONDS
    c.check("the model file may take longer than a JSON file", 0 < json_seconds < model_seconds)
    c.check("64 MiB passes at 1 MiB/s with a margin of at least four", model_seconds * MIB >= 4 * mlflow_loader.MODEL_CAP and json_seconds * MIB >= 4 * MIB)

    def cut_off(label: str, path: str, seconds: float, w: World, clock: Clock, error: Exception | None, model: Any) -> None:
        c.check(
            f"{label}: refused as ArtifactRefused naming the deadline (got {type(error).__name__ if error else 'a loaded model'}: {error})",
            model is None and type(error).__name__ == "ArtifactRefused" and "deadline" in str(error),
        )
        c.check(
            f"{label}: cut off at {seconds} seconds of the test clock, and not long after ({clock.now})",
            seconds <= clock.now < seconds + 100,
        )
        c.check(f"{label}: no downloaded file or folder remains", w.leftover() == [])

    for path, seconds in ((FEATURE_LIST, json_seconds), (PROVENANCE, json_seconds), (SELF_CHECK, json_seconds), (MODEL, model_seconds)):
        with world() as w, test_clock(w) as clock:
            drip = Drip(clock)
            w.server.drips[path] = drip.served
            model, error = w.attempt(use_default_fetch=True)
            cut_off(f"{path}: a drip", path, seconds, w, clock, error, model)
            c.check(f"{path}: the body was stopped after {drip.pulled} bytes, and its connection closed", drip.closed and drip.pulled < 1000)
            c.check(f"{path}: no worker thread remains", fetch_workers() == [])
    for path, seconds in ((FEATURE_LIST, json_seconds), (MODEL, model_seconds)):
        with world() as w, test_clock(w) as clock:
            blocked = Drip(clock, stops_after=5)
            w.server.drips[path] = blocked.served
            model, error = w.attempt(use_default_fetch=True)
            cut_off(f"{path}: a read blocked in the socket", path, seconds, w, clock, error, model)
            c.check(f"{path}: closing the connection ended the blocked read ({blocked.pulled} bytes came)", blocked.closed and blocked.pulled == 5)
            c.check(f"{path}: no worker thread remains after a blocked read", fetch_workers() == [])
    for path in (FEATURE_LIST, MODEL):
        with world() as w, test_clock(w) as clock:
            drip = Drip(clock)
            w.server.drips[path] = drip.served
            local = str(w.tmp / "fetched")
            try:
                model_code.mlflow_fetch(RUN, path, local, mlflow_loader.MODEL_CAP)
                direct: Exception | None = None
            except Exception as caught:
                direct = caught
            c.check(f"{path}: the fetch itself raises the rule 'deadline' ({direct!r})", getattr(direct, "rule", "") == "deadline")
            c.check(
                f"{path}: the file the drip had begun to write was removed by the fetch ({drip.pulled} bytes came)",
                drip.pulled > 0 and not os.path.exists(local),
            )
    with world() as w, test_clock(w) as clock:
        drip = Drip(clock)
        w.server.drips["extract/other.parquet"] = drip.served
        try:
            model_code.mlflow_fetch(RUN, "extract/other.parquet", str(w.tmp / "other"), 10 * MIB, seconds=5.0)
            other: Exception | None = None
        except Exception as caught:
            other = caught
        c.check(
            f"another reader's path, cap and time (5 seconds) go through the same fetch and are cut off at its own time ({other!r}, {clock.now})",
            getattr(other, "rule", "") == "deadline" and 5.0 <= clock.now < 105.0 and not os.path.exists(w.tmp / "other"),
        )
    for stall, label in (("list_gates", "a stalled listing"), ("get_gates", "a stalled request with its retries")):
        for path, seconds in ((FEATURE_LIST, json_seconds), (MODEL, model_seconds)):
            with world() as w, test_clock(w) as clock:
                gate = threading.Event()
                getattr(w.server, stall)[path] = gate
                model, error = w.attempt(use_default_fetch=True)
                cut_off(f"{path}: {label}", path, seconds, w, clock, error, model)
                reads_at_expiry = list(w.server.reads)
                gate.set()
                for worker in fetch_workers():
                    worker.join(5)
                c.check(f"{path}: {label}: the stalled call ended after its release and no worker remains", fetch_workers() == [])
                c.check(f"{path}: {label}: nothing was requested or left on disk after the cut-off", w.leftover() == [] and w.server.reads == reads_at_expiry)
                c.check(f"{path}: {label}: no byte of the late response was read", w.server.pulled.get(path, 0) == 0)
                if stall == "get_gates":
                    c.check(f"{path}: {label}: the response that arrived after the cut-off was closed", path in w.server.closed)
    with world() as w, test_clock(w) as clock:
        gate = threading.Event()
        w.registry.lookup_gate = gate
        model, error = w.attempt(use_default_fetch=True)
        cut_off("a stalled lookup of the model version", "", json_seconds, w, clock, error, model)
        gate.set()
        c.check("a stalled lookup: nothing was downloaded", w.server.reads == [] and w.leftover() == [])
    with world() as w, test_clock(w) as clock:
        w.server.tick = 1.0
        model, error = w.attempt(use_default_fetch=True)
        c.check(f"a model of 1 MiB per second of the test clock loads, well inside its deadline ({error!r}, {clock.now} seconds)", model is not None)
        c.check("the clock stood well inside the deadline", clock.now < model_seconds / 2 and w.leftover() == [])
    with world(long={MODEL: mlflow_loader.MODEL_CAP}) as w, test_clock(w) as clock:
        w.server.tick = 1.0
        model, error = w.attempt(use_default_fetch=True)
        c.check(
            f"a model of exactly 64 MiB at 1 MiB per second of the test clock loads ({error!r}, {clock.now} seconds)",
            model is not None and clock.now < model_seconds,
        )


def case_loader_refuses_a_fetch_that_returns_anything_but_the_expected_file(c: Checker) -> None:
    """Whatever a fetch returns must be the regular file at the path the loader chose (no symlink, no folder, no other file), whatever its content."""
    for path in FOUR:
        for label, other in (("a symlink", symlink_to_the_content), ("a folder", folder_instead_of_a_file), ("another file", another_file)):
            refused(c, f"{path} returned as {label}", "ArtifactRefused", other={path: other})
    with world() as w:
        w.attempt()
        c.check(
            "each fetch got its path-specific cap: 1 MiB for the three JSON files, 64 MiB for the model",
            dict(zip(w.run.paths, w.run.limits, strict=True)) == {FEATURE_LIST: MIB, PROVENANCE: MIB, SELF_CHECK: MIB, MODEL: 64 * MIB},
        )
        c.check(
            "each fetch got a local path with a fixed name in one folder",
            sorted(Path(local).name for local in w.run.local_paths) == ["feature_list.json", "model.skops", "provenance.json", "self_check.json"],
        )


def case_loader_refuses_unknown_deep_or_oversized_fields_of_the_feature_list_and_provenance(c: Checker) -> None:
    """The two bundle documents have closed shapes. Every case is refused as ``ArtifactRefused`` before admission and leaves no file. The values are the
    reviewer's (a list nested 600 deep, a 300,000 character text, an array of 50,000) and the same under a permitted key."""
    big_text, big_list, deep = "x" * 300_000, list(range(50_000)), nested(600)
    for name, option in (("feature_list.json", "feature"), ("provenance.json", "provenance")):
        for label, value in (("a small value", 1), ("a list nested 600 deep", deep), ("a 300,000 character text", big_text), ("an array of 50,000", big_list)):
            refused(c, f"{name}: unknown key with {label}", "ArtifactRefused", **{option: set_key("unknown", value)})
    for label, edit in (
        ("slot that is a nested list", set_key("slot", nested(3))),
        ("slot that is a number", set_key("slot", 1)),
        ("slot of 300,000 characters", set_key("slot", "x" * 300_000)),
        ("fitted_slot of 257 characters", set_key("fitted_slot", "x" * (mlflow_loader.MAX_TEXT + 1))),
        ("fitted_slot that is a list", set_key("fitted_slot", ["m1"])),
        ("kept_references of 50,000 elements", set_key("kept_references", ["r"] * 50_000)),
        ("kept_references with a list inside", set_key("kept_references", [["a:1"]])),
        ("kept_references nested 600 deep", set_key("kept_references", deep)),
        ("kept_columns that is text", set_key("kept_columns", "a")),
        ("kept_references and kept_columns of unequal length", lambda d: d.update(kept_references=["a:1"], kept_columns=[])),
    ):
        refused(c, f"feature_list.json: {label}", "ArtifactRefused", feature=edit)
    for label, edit in (
        ("inputs nested 600 deep", set_key("inputs", {"a": deep})),
        ("inputs holding a 300,000 character text", set_key("inputs", {"a": "x" * 300_000})),
        ("inputs that is a list", set_key("inputs", [1])),
        ("timings holding 50,000 numbers", set_key("timings", {"a": big_list})),
        ("timings with 50,000 keys", set_key("timings", {f"k{n}": n for n in range(50_000)})),
        ("a key of 257 characters", set_key("timings", {"k" * 257: 1})),
        ("run_id of 300,000 characters", set_key("run_id", "x" * 300_000)),
        ("run_id that is a list", set_key("run_id", ["a"])),
        ("git_dirty that is text", set_key("git_dirty", "no")),
        ("try that is an object", set_key("try", {"a": 1})),
        ("code_digest that is a number", set_key("code_digest", 5)),
        ("a version text of 300,000 characters", lambda d: d["resolved_versions"].__setitem__("mlflow", "x" * 300_000)),
        ("resolved_versions with an object value", lambda d: d["resolved_versions"].__setitem__("mlflow", {"a": 1})),
    ):
        refused(c, f"provenance.json: {label}", "ArtifactRefused", provenance=edit)
    refused(
        c,
        "provenance.json: unknown key and a wrong closure: the shape comes before admission",
        "ArtifactRefused",
        provenance=lambda d: d.update(unknown=1, model_code_closure_sha256="0" * 64),
    )
    with world(feature=only_references_and_columns) as w:
        model, error = w.attempt()
        c.check(f"a feature list with only references and columns still loads ({error!r})", model is not None)
    with world(feature=lambda d: d.update(slot=None, fitted_slot=None, kept_references=[], kept_columns=[])) as w:
        model, error = w.attempt()
        c.check(f"a feature list of a run that fitted no slot (null slots, empty kept lists) loads ({error!r})", model is not None)
    with world(provenance=lambda d: d.update(git_commit=None, git_dirty=None, run_id=None, **{"try": 2})) as w:
        model, error = w.attempt()
        c.check(f"a provenance with null commit, dirty flag and run_id and an integer try loads ({error!r})", model is not None)


def case_loader_agrees_with_the_shared_matrix_of_bundle_documents(c: Checker) -> None:
    """Every document of the shared matrix (``scripts/quality/data_rule_matrix.py``) is refused as ``ArtifactRefused`` by the loader, or loads when the matrix
    names no rule. The promote action and the promotion check run the same list, so the readers cannot drift apart: the reviewer's probes (a repeated schema
    key, a UTF-16 self-check, a valid provenance with an unused NaN field) and the slot and feature-list bounds are in it."""
    paths = {data_rule_matrix.FEATURE_LIST: FEATURE_LIST, data_rule_matrix.PROVENANCE: PROVENANCE, data_rule_matrix.SELF_CHECK: SELF_CHECK}
    good = good_documents(model_code.closure_digest(ROOT))
    for entry in data_rule_matrix.entries():
        path = paths[entry.file]
        built = data_rule_matrix.build(entry, good[path])
        raw = {path: built if isinstance(built, bytes) else canonical(built)}
        if entry.rule:
            refused(c, f"matrix {entry.name}", "ArtifactRefused", raw=raw)
        else:
            with world(raw=raw) as w:
                model, error = w.attempt()
                c.check(f"matrix {entry.name}: loads (error: {error!r})", model is not None)


def case_loader_reads_the_feature_set_from_the_tags(c: Checker) -> None:
    """The loader reads the model's feature set and the set's schema hash from the version tags; a set model with no set hash does not load."""

    def loaded(tags: dict[str, str]) -> Any:
        with world(tags=tags) as w:
            model, _ = w.attempt()
            return model

    both = {"feature_schema_sha256": "whole", "feature_set": "m2", "feature_set_schema_sha256": "setdigest"}
    model = loaded(both)
    c.check("a version tagged with a set loads for that set", model is not None and model.feature_set == "m2")
    c.check("its schema hash is the set's, which the set's marker holds", model is not None and model.schema_hash == "setdigest")
    c.check("its references stay the ones of its feature list", model is not None and model.references == ("a:1", "b:2", "c:3"))
    old = loaded({"feature_schema_sha256": "whole"})
    c.check(
        "a version with no set tag loads for the single path with the whole schema hash",
        old is not None and (old.feature_set, old.schema_hash) == ("", "whole"),
    )
    c.check("a version tagged with a set but no set hash does not load", loaded({"feature_schema_sha256": "whole", "feature_set": "m2"}) is None)
    c.check(
        "a version tagged with an empty set loads for the single path", (lambda m: m is not None and m.feature_set == "")(loaded({**both, "feature_set": ""}))
    )
    c.check("a version with a set tag that is not text of a set name does not load", loaded({**both, "feature_set": "M 2/x"}) is None)
    with world(tags={}) as w:
        _, error = w.attempt()
        c.check("a version with no schema hash tag at all does not load, and leaves no download", error is not None and w.run.calls == [])
    with world(tags={"feature_schema_sha256": "whole", "feature_set": "M 2/x"}) as w:
        w.attempt()
        c.check("a refused tag stops before any download", w.run.calls == [])


def case_image_start_refuses_files_that_differ_from_their_record(c: Checker) -> None:
    """The image hashes its own closure files at start (``model_code.closure_digest``) and does not build the loader when the value differs from its record."""
    c.check(
        "the repository's own files match their record, so the loader is built",
        mlflow_loader.MlflowLoader().closure == json.loads((ROOT / model_code.RECORD).read_text())["closure_sha256"],
    )
    with world() as w:
        c.check("a copy of the files builds, and its closure is the record's", mlflow_loader.MlflowLoader(root=w.root).closure == w.closure)
        changed = w.root / "scripts/training/models/linear.py"
        changed.write_bytes(changed.read_bytes() + b"\n# a build from a dirty tree\n")
        try:
            mlflow_loader.MlflowLoader(root=w.root)
            name = ""
        except Exception as error:
            name = type(error).__name__
        c.check(f"one changed byte of a closure file refuses the start as ImageCodeMismatch (got {name!r})", name == "ImageCodeMismatch")
    with world() as w:
        record = w.root / model_code.RECORD
        document = json.loads(record.read_text())
        document["closure_sha256"] = "0" * 64
        record.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n")
        try:
            mlflow_loader.MlflowLoader(root=w.root)
            name = ""
        except Exception as error:
            name = type(error).__name__
        c.check(f"a record whose digest is not the files' digest refuses the start as ImageCodeMismatch (got {name!r})", name == "ImageCodeMismatch")
    with world() as w:
        (w.root / "scripts/training/models/trees.py").unlink()
        try:
            mlflow_loader.MlflowLoader(root=w.root)
            name = message = ""
        except Exception as error:
            name, message = type(error).__name__, str(error)
        c.check(
            f"a missing closure file refuses the start by name and path (got {name!r})",
            name == "RecordError" and "file_missing" in message and "trees.py" in message,
        )
    with world() as w:
        (w.root / model_code.RECORD).write_text("{")
        try:
            mlflow_loader.MlflowLoader(root=w.root)
            name = ""
        except Exception as error:
            name = type(error).__name__
        c.check(f"an unreadable record refuses the start (got {name!r})", name == "RecordError")


CASES: dict[str, tuple[Callable[[Checker], None], str]] = {
    "loader_asks_for_exactly_the_four_paths_and_loads_data_only": (case_loader_asks_for_exactly_the_four_paths_and_loads_data_only, "mlflow loader"),
    "loader_downloads_into_one_temporary_folder_removed_every_time": (case_loader_downloads_into_one_temporary_folder_removed_every_time, "mlflow loader"),
    "loader_refuses_the_closure_and_the_libraries_by_name": (case_loader_refuses_the_closure_and_the_libraries_by_name, "mlflow loader"),
    "loader_refuses_wrong_source_size_and_bad_json_by_name": (case_loader_refuses_wrong_source_size_and_bad_json_by_name, "mlflow loader"),
    "loader_passes_the_image_trust_list_and_never_the_models": (case_loader_passes_the_image_trust_list_and_never_the_models, "mlflow loader"),
    "loader_self_check_is_missing_or_malformed_by_name": (case_loader_self_check_is_missing_or_malformed_by_name, "mlflow loader"),
    "loader_self_check_refuses_a_model_that_does_not_reproduce_its_predictions": (
        case_loader_self_check_refuses_a_model_that_does_not_reproduce_its_predictions,
        "mlflow loader",
    ),
    "loader_logs_one_line_with_the_version_and_the_peak_memory": (case_loader_logs_one_line_with_the_version_and_the_peak_memory, "mlflow loader"),
    "loader_default_fetch_stops_a_download_at_its_cap": (case_loader_default_fetch_stops_a_download_at_its_cap, "mlflow loader"),
    "loader_default_fetch_stops_the_model_at_its_own_cap": (case_loader_default_fetch_stops_the_model_at_its_own_cap, "mlflow loader"),
    "loader_default_fetch_refuses_a_folder_without_fetching_a_descendant": (
        case_loader_default_fetch_refuses_a_folder_without_fetching_a_descendant,
        "mlflow loader",
    ),
    "loader_default_fetch_writes_four_fixed_names_into_one_folder": (case_loader_default_fetch_writes_four_fixed_names_into_one_folder, "mlflow loader"),
    "loader_default_fetch_is_cut_off_at_its_deadline": (case_loader_default_fetch_is_cut_off_at_its_deadline, "mlflow loader"),
    "loader_refuses_a_fetch_that_returns_anything_but_the_expected_file": (
        case_loader_refuses_a_fetch_that_returns_anything_but_the_expected_file,
        "mlflow loader",
    ),
    "loader_refuses_unknown_deep_or_oversized_fields": (
        case_loader_refuses_unknown_deep_or_oversized_fields_of_the_feature_list_and_provenance,
        "mlflow loader",
    ),
    "loader_agrees_with_the_shared_matrix_of_bundle_documents": (case_loader_agrees_with_the_shared_matrix_of_bundle_documents, "mlflow loader"),
    "loader_reads_the_feature_set": (case_loader_reads_the_feature_set_from_the_tags, "mlflow loader feast"),
    "image_start_refuses_files_that_differ_from_their_record": (case_image_start_refuses_files_that_differ_from_their_record, "mlflow loader"),
}
