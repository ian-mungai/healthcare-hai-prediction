"""Freeze the run 3 execution controls for the publisher task, offline: the code, the operational inputs and the state root the task computes.

The task checks its controls inside its own layout: the code archive is the code root, the unpacked mirror is the stored data root and its state root is
``run`` under its state parent. This script computes the same controls on the owner's Mac and writes them beside the queue as ``controls.json``. It must run
from the code tree to bind, which is a clean export of the revision that goes into the code archive: tracked files only, no ``.env`` and no untracked
configuration. The independent review then binds the SHA-256 of these controls (``independent_review.json``, ``reviewed_controls_sha256``).

Usage, from a clean export of the revision (PYTHONPATH names that export):
    python -m scripts.acquisition.one_off.redownload.freeze_controls_run3 --queue <data root>/data/redownload_checks/20261111_bls2006/queue.json \\
        --data-root <data root> --state-parent <new empty folder> --code-root <the export>
No network or AWS request is made. Controls already written are never overwritten.
"""

import argparse
import sys
from pathlib import Path

from scripts.acquisition import redownload
from scripts.acquisition.s3_store import encoded_json, write_once
from scripts.acquisition.source_registry import REPO_ROOT
from scripts.acquisition.transport import CaptureError


def freeze(queue: Path, data_root: Path, state_parent: Path) -> str:
    """Write ``controls.json`` beside the queue and return the SHA-256 the review binds."""
    run = redownload.Run(queue_path=queue, state_root=state_parent / "run", state_parent=state_parent, stored_root=data_root)
    controls = redownload.runtime_controls(run)
    try:
        write_once(queue.with_name("controls.json"), encoded_json(controls))
    except CaptureError:
        sys.exit("Different controls already exist beside this queue; do not overwrite them.")
    return redownload.digest(encoded_json(controls))


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--queue", required=True, type=Path)
    parser.add_argument("--data-root", required=True, type=Path)
    parser.add_argument("--state-parent", required=True, type=Path)
    parser.add_argument("--code-root", type=Path, help="the code tree these controls bind; it must be the tree this script runs from")
    args = parser.parse_args(argv)
    if args.code_root is not None and args.code_root.resolve() != REPO_ROOT.resolve():
        sys.exit(f"The code root {args.code_root.resolve()} is not the tree this script runs from ({REPO_ROOT.resolve()}).")
    sys.stdout.write(f"controls_sha256 {freeze(args.queue, args.data_root, args.state_parent)}\n")


if __name__ == "__main__":
    main()
