"""Code one-shot of the container host: show the reviewed code archive of one run as the checkout /data/repo (host path /host/repo here).

The services stack embeds this file as the container command (``python -I -c``) in the training image. ``host_session start --code-run <RUN>
--code-sha256 <SHA>`` passes CODE_RUN_ID and CODE_SHA256 as RunTask overrides. The image's own /opt/hai/fetch_code.py downloads
lakehouse/run_stage/<RUN>/inputs/code.tar.gz, checks it against CODE_SHA256 and unpacks it, here into repo_releases/<SHA>/code. Only then does
repo become a relative link to that release, swapped in one rename, so a refused or failed fetch leaves the previous checkout in place. A rerun for
the same digest finds the archive and the release and changes nothing. Without both values the step keeps the existing checkout and refuses when
there is none, because Airflow and the catalog bootstrap mount it. Releases are kept; the previous one stays available for a link back.
"""

from __future__ import annotations

import importlib
import os
import re
import sys
from pathlib import Path
from typing import Any

SHA256 = re.compile(r"[a-f0-9]{64}")
RELEASES = "repo_releases"
LINK = "repo"
# Airflow mounts this folder of the checkout, so a checkout without it is not one.
CHECKOUT_FOLDER = "dags"


def run(host: Path, fetch_dir: Path, client: Any = None) -> int:
    """Place the code of the task environment under ``host`` and return the exit code; ``client`` replaces the S3 client of the task role."""
    run_id, digest = os.environ.get("CODE_RUN_ID", ""), os.environ.get("CODE_SHA256", "")
    link = host / LINK
    if not run_id and not digest:
        if (link / CHECKOUT_FOLDER).is_dir():
            sys.stdout.write(f"code_fetch: no code given; kept the checkout {os.readlink(link) if link.is_symlink() else LINK}\n")
            return 0
        sys.stderr.write("code_fetch refused: the host has no checkout; run host_session start with --code-run and --code-sha256\n")
        return 1
    if not run_id or not SHA256.fullmatch(digest):
        sys.stderr.write("code_fetch refused: give both a run ID and a 64-character lowercase hex code digest\n")
        return 1
    if link.exists() and not link.is_symlink() and any(link.iterdir()):
        sys.stderr.write(f"code_fetch refused: {LINK} is a folder with content, not a link to a release; move it aside first\n")
        return 1
    release = host / RELEASES / digest
    release.mkdir(parents=True, exist_ok=True)
    # The image's own fetch script, found once its folder is on the path. The tools folder does not exist, so it links no DVC environment.
    sys.path.insert(0, str(fetch_dir))
    code = importlib.import_module("fetch_code").main(client, release, release / "no_tools")
    if code != 0:
        if not any(release.iterdir()):
            release.rmdir()
        return code
    target = f"{RELEASES}/{digest}/code"
    if link.is_symlink() and os.readlink(link) == target:
        sys.stdout.write(f"code_fetch: {LINK} already shows run {run_id} code {digest[:12]}; nothing changed\n")
        return 0
    if link.is_dir() and not link.is_symlink():
        link.rmdir()
    staged = host / f"{LINK}.next"
    if staged.is_symlink() or staged.exists():
        staged.unlink()
    staged.symlink_to(target)
    os.replace(staged, link)
    sys.stdout.write(f"code_fetch: {LINK} now shows run {run_id} code {digest[:12]}\n")
    return 0


if __name__ == "__main__":
    sys.exit(run(Path("/host"), Path("/opt/hai")))
