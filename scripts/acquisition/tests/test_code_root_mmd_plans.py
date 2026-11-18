"""The publisher's startup file check over the code root that the committed checkout really produces: build the code archive, unpack it, stage the plan inputs.

The container's code root is the archive plus the plan inputs that ``host_inputs.prepare`` copies out of the tracked bundle. A plan file that only a local,
untracked folder holds never reaches it, and the publisher's startup check refuses the run. This case builds the archive from ``HEAD`` with the real builder,
unpacks it into a new folder and runs the check in a child process there, so no file of the working tree can stand in for a missing one.
"""

from __future__ import annotations

import json
import os
import shutil
import sys
import tarfile
from pathlib import Path

import pytest

from scripts.process import run_command
from tests.support import check

ROOT = Path(__file__).resolve().parents[3]
TIMEOUT_SECONDS = 600
SAVED_PAGE = "data/e2e/mmd_api_comparison/20260925/references/map.html"
# The child runs inside the unpacked code root and prints one JSON line: the failed startup checks, the plans that cannot load and the data files that a
# plan load opens and the startup check does not name.
CHILD = """
import json, os, sys
from pathlib import Path

root = Path(sys.argv[1])
sys.path.insert(0, str(root))
os.chdir(root)
opened = []


def record_open(event, arguments):
    if event == "open" and arguments and isinstance(arguments[0], (str, os.PathLike)):
        opened.append(os.path.abspath(arguments[0]))


sys.addaudithook(record_open)
from scripts.acquisition import host_inputs, mmd_api_contract
from scripts.infrastructure import startup_check

bundle = host_inputs.prepare(root)
if len(sys.argv) > 2:
    os.remove(root / sys.argv[2])
roots = {"code": str(root)}
queue = str(bundle / "queue.json")
queue_request = {"kind": "queue", "job": "publisher", "cwd": str(root), "roots": roots, "queue": queue, "controls": "", "keys": True, "listing": False}
failed = {}
failed["queue"] = [r for r in startup_check.execute(queue_request) if r.get("k") == "f" and not r["o"]]
mmd = startup_check.mmd_needs()
failed["mmd_needs"] = [r for r in startup_check.records_of(startup_check.file_findings(mmd, {}, root)) if r.get("k") == "f" and not r["o"]]
named = {os.path.abspath(need.path) for need in mmd}
unloadable = []
unnamed = set()
for plan_sha256 in mmd_api_contract.PLANS:
    opened.clear()
    try:
        mmd_api_contract.load_plan(plan_sha256)
    except (OSError, ValueError, KeyError) as error:
        unloadable.append([plan_sha256, type(error).__name__])
    reads = {path for path in opened if path.startswith(str(root / "data") + os.sep) and os.path.isfile(path)}
    unnamed |= {os.path.relpath(path, root) for path in reads - named}
counts = {name: len(rows) for name, rows in failed.items()}
print(json.dumps({"failed": counts, "needs": len(mmd), "plans": len(mmd_api_contract.PLANS), "unloadable": unloadable, "unnamed": sorted(unnamed)}))
"""


@pytest.fixture(scope="module")
def unpacked(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """The code archive of ``HEAD`` unpacked into a new folder."""
    base = tmp_path_factory.mktemp("code_root")
    built = run_command(
        sys.executable,
        ["-m", "scripts.lakehouse.code_archive", "build", "--output", str(base / "archive"), "--revision", "HEAD"],
        cwd=ROOT,
        timeout=TIMEOUT_SECONDS,
    )
    check(built.returncode == 0, f"the code archive did not build: {built.stderr[-600:]}")
    root = base / "root"
    root.mkdir()
    with tarfile.open(base / "archive" / "code.tar.gz") as archive:
        archive.extractall(root, filter="data")
    return root


def run_child(root: Path, *removed: str) -> dict:
    done = run_command(
        sys.executable, ["-I", "-c", CHILD, str(root), *removed], cwd=root, env={**os.environ, "AWS_CONFIG_FILE": "/dev/null"}, timeout=TIMEOUT_SECONDS
    )
    check(done.returncode == 0, f"the startup check child failed to run: {done.stderr[-600:]}")
    return json.loads(done.stdout.strip().splitlines()[-1])


def test_the_committed_checkout_brings_every_mmd_plan_file_to_the_code_root(unpacked: Path) -> None:
    result = run_child(unpacked)
    check(result["plans"] == 2, f"the contract holds the two current plans: {result['plans']}")
    check(result["failed"] == {"queue": 0, "mmd_needs": 0}, f"startup file checks that failed at the unpacked code root: {result['failed']}")
    check(result["unloadable"] == [], f"plans that cannot load at the unpacked code root: {result['unloadable']}")
    check(result["unnamed"] == [], f"files that a plan load opens and the startup check does not name: {result['unnamed']}")


def test_a_missing_saved_page_is_refused_at_startup_before_a_plan_load_needs_it(unpacked: Path, tmp_path: Path) -> None:
    root = tmp_path / "root"
    shutil.copytree(unpacked, root)
    result = run_child(root, SAVED_PAGE)
    check(result["failed"]["queue"] >= 1 and result["failed"]["mmd_needs"] >= 1, f"the startup check names the missing page: {result['failed']}")
    check(result["unloadable"] != [], f"the plan load needs the page too: {result['unloadable']}")
