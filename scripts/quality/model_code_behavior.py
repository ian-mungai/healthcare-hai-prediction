"""Behaviour check of the model code closure: the declared files alone fit, save, load and predict every model family.

    <python with the model_api libraries> -m scripts.quality.model_code_behavior [--require-libraries]

The import rule in ``scripts/model_code.py`` reads source text, so it cannot prove that no code path reaches a file outside the closure. This check runs
the code instead. It copies only the recorded files into a scratch folder, adds a stand-in for each file the model API image provides, and starts a clean
interpreter (``-I -B``: no PYTHONPATH, no working directory on the path, no bytecode). That interpreter builds the pipeline of each family the closure
serves (m1 ordinary least squares, m2 elastic net, m4 boosting and its forest check), fits it on a tiny synthetic frame and predicts. The three registry
candidates are also saved with skops, loaded with the trusted types of ``data_contracts/training/model_contract.json`` and predicted again (the forest
check is never saved by the training path, so it is not saved here). A path that needs a file outside the closure fails with ``ModuleNotFoundError`` or a
changed prediction, and each failure names its family and step. It needs numpy, pandas, scikit-learn, statsmodels, xgboost and skops (the model_api group
of uv.lock). Without them the check reports ``skipped`` with the missing library, and ``--require-libraries`` turns that into a failure. The host ``.venv``
has none of them except numpy: run it in the training or model API environment. It uses only the standard library itself, so a bare interpreter with those
libraries is enough."""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import NamedTuple

from scripts import model_code
from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]
# What the model API image provides: the stand-in defines exactly the names the closure takes from orchestration/training_contract.py.
STAND_IN = {
    "orchestration/__init__.py": "",
    "orchestration/training_contract.py": 'MODEL_LISTS = {"m1": ("m1_ols",), "m2": ("m2_elastic_net",), "m4": ("m4_boosting",)}\n',
}
# Literal code for the clean interpreter. Its arguments are the scratch folder and the trusted type names (JSON). It prints one JSON line.
EXERCISE = """
import json, sys, tempfile
scratch, trusted = sys.argv[1], json.loads(sys.argv[2])
sys.path.insert(0, scratch)
try:
    import numpy as np
    import pandas
    import skops.io
    import statsmodels.api
    import xgboost
    from sklearn.pipeline import Pipeline
except ImportError as error:
    print(json.dumps({"skipped": str(error)}))
    raise SystemExit(0)
report = {"families": {}, "failures": []}
try:
    from scripts.training.models.common import Feature
    from scripts.training.models.elastic import ElasticRegressor
    from scripts.training.models.linear import OLSRegressor
    from scripts.training.models.preprocessing import FoldPreprocessor
    from scripts.training.models.trees import TreeRegressor
except Exception as error:
    report["failures"].append({"family": "closure", "step": "import", "error": type(error).__name__ + ": " + " ".join(str(error).split())[:300]})
    print(json.dumps(report))
    raise SystemExit(0)

rng = np.random.default_rng(7)
count = 60
x1, x2, cmi = rng.normal(size=count), rng.normal(size=count), rng.uniform(0.5, 1.5, size=count)
x2[::9] = np.nan
group = rng.choice(["a", "b", "c"], size=count)
y = 1.0 + 0.5 * x1 - 0.25 * np.nan_to_num(x2) + cmi + 0.1 * rng.normal(size=count)
rows = tuple({"x1": float(a), "x2": float(b), "cmi": float(c), "grp": str(g)} for a, b, c, g in zip(x1, x2, cmi, group))
features = (Feature("test:x1", "number"), Feature("test:x2", "number"), Feature("test:cmi", "number"), Feature("test:grp", "category"))
forced = ("test:cmi",)
frame = pandas.DataFrame(list(rows))
# (name, pipeline, saved). The registry candidates (m1, m2, m4 boosting) are saved and loaded as the training bundle does. The forest is the training
# path's check fit: it is fitted and predicted but never saved, so it is not saved here either.
families = (
    ("m1_ols", Pipeline([("prepare", FoldPreprocessor(features, drop_dependent=True)), ("model", OLSRegressor())]), True),
    (
        "m2_elastic_net",
        Pipeline([("prepare", FoldPreprocessor(features, forced=forced, winsorize=True)), ("model", ElasticRegressor(0.01, 0.5, forced, 1))]),
        True,
    ),
    (
        "m4_boosting",
        Pipeline(
            [
                ("prepare", FoldPreprocessor(features, forced=forced, native_missing=True)),
                ("model", TreeRegressor("xgboost", 1, 1, {"n_estimators": 5, "max_depth": 2})),
            ]
        ),
        True,
    ),
    (
        "m4_forest",
        Pipeline(
            [
                ("prepare", FoldPreprocessor(features, forced=forced, native_missing=True)),
                ("model", TreeRegressor("random_forest", 1, 1, {"n_estimators": 5})),
            ]
        ),
        False,
    ),
)
with tempfile.TemporaryDirectory() as folder:
    for name, pipeline, saved in families:
        step, steps, path = "fit", [], folder + "/" + name + ".skops"
        try:
            pipeline.fit(rows, y)
            steps.append("fit")
            step = "predict"
            before = np.asarray(pipeline.predict(rows), dtype=np.float64)
            if before.shape != (count,) or not np.isfinite(before).all():
                raise ValueError("predictions are not finite")
            steps.append("predict")
            difference = 0.0
            if saved:
                step = "save"
                skops.io.dump(pipeline, path)
                steps.append("save")
                step = "load"
                loaded = skops.io.load(path, trusted=trusted)
                steps.append("load")
                step = "predict_loaded"
                after = np.asarray(loaded.predict(rows), dtype=np.float64)
                from_frame = np.asarray(loaded.predict(frame), dtype=np.float64)
                for array in (after, from_frame):
                    if array.shape != (count,) or not np.isfinite(array).all():
                        raise ValueError("loaded predictions are not finite")
                difference = float(max(np.abs(before - after).max(), np.abs(before - from_frame).max()))
                steps.append("predict_loaded")
            report["families"][name] = {"steps": steps, "difference": difference}
        except Exception as error:
            detail = type(error).__name__ + ": " + " ".join(str(error).split())[:300]
            if step == "load":
                lacking = [kind for kind in skops.io.get_untrusted_types(file=path) if kind not in trusted]
                detail += " | types in the file that the trusted list lacks: " + str(lacking)
            report["failures"].append({"family": name, "step": step, "error": detail})
            report["families"][name] = {"steps": steps, "difference": None}
print(json.dumps(report))
"""
TOLERANCE = 1e-9


class Outcome(NamedTuple):
    problems: list[str]
    skipped: str
    families: dict[str, list[str]]  # the steps each family completed


def scratch_folder(root: Path, base: Path) -> Path:
    """The declared files of the record plus the stand-ins, in a folder of their own."""
    record = model_code.read_record(root / model_code.RECORD)
    for item in record["files"]:
        target = base / item["path"]
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(root / item["path"], target)
    for name, text in STAND_IN.items():
        (base / name).parent.mkdir(parents=True, exist_ok=True)
        (base / name).write_text(text)
    return base


def run(root: Path = ROOT, edit: Callable[[Path], None] | None = None) -> Outcome:
    """Run the exercise in a clean interpreter over a scratch copy; ``edit`` may change the copy first (the cases use it to plant a defect)."""
    trusted = json.loads((root / model_code.CONTRACT).read_text())["serialization"]["trusted_types"]
    with tempfile.TemporaryDirectory() as folder:
        scratch = scratch_folder(root, Path(folder).resolve())
        if edit is not None:
            edit(scratch)
        result = run_command(sys.executable, ["-I", "-B", "-c", EXERCISE, str(scratch), json.dumps(trusted)], cwd=scratch, timeout=300)
    last = result.stdout.strip().splitlines()[-1:] or [""]
    if result.returncode != 0:
        tail = " | ".join((result.stderr.strip().splitlines() or ["no message"])[-3:])
        return Outcome([f"the clean interpreter stopped outside any family: {tail}"], "", {})
    try:
        report = json.loads(last[0])
    except ValueError:
        return Outcome([f"the clean interpreter printed no report: {last[0][:200]}"], "", {})
    if "skipped" in report:
        return Outcome([], str(report["skipped"]), {})
    problems = [f"{item['family']}: {item['step']} failed: {item['error']}" for item in report["failures"]]
    for name, family in report["families"].items():
        if family["difference"] is not None and family["difference"] > TOLERANCE:
            problems.append(f"{name}: predict_loaded failed: predictions differ after the skops round trip by {family['difference']}")
    return Outcome(problems, "", {name: family["steps"] for name, family in report["families"].items()})


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Fit, save, load and predict every model family with only the declared closure files.")
    parser.add_argument("--require-libraries", action="store_true", help="fail, instead of skip, when a model_api library is missing")
    arguments = parser.parse_args(argv)
    outcome = run()
    if outcome.skipped:
        sys.stdout.write(f"model code behaviour: skipped, {outcome.skipped}\n")
        return 1 if arguments.require_libraries else 0
    for problem in outcome.problems:
        sys.stdout.write(f"{problem}\n")
    for name, steps in outcome.families.items():
        sys.stdout.write(f"  {name}: {', '.join(steps) or 'nothing completed'}\n")
    failed = sorted({text.split(":")[0] for text in outcome.problems})
    sys.stdout.write(
        f"model code behaviour: {len(outcome.families) - len([n for n in failed if n in outcome.families])} of {len(outcome.families)} families passed"
    )
    sys.stdout.write(f"; failed: {failed}\n" if outcome.problems else "\n")
    return 1 if outcome.problems else 0


if __name__ == "__main__":
    sys.exit(main())
