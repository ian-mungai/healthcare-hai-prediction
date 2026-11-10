"""Build what the publisher job takes as staged inputs: one deterministic archive and the request that binds it.

Run it on the owner's Mac after ``stored_mirror`` and the approvals. It writes ``publisher_inputs.tar.gz`` and ``publisher_request.json``. The owner copies both
beside the run's code archive. The archive holds the stored mirror, its index, the route files and a manifest that names the queue. It refuses an original, a
settings file or anything that is not a plain file, because everything in it reaches the task.

    .venv/bin/python -m scripts.acquisition.stage_publisher --mirror-output <folder> --queue <path inside the mirror> --removal-plan <file> \\
        --removal-confirmation <file> --hosts <file> --hosts-approval <file> --output <new folder> [--resume-source <source>]
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import sys
import tarfile
from collections.abc import Sequence
from pathlib import Path, PurePosixPath

from scripts.acquisition.s3_store import encoded_json, write_once

INPUTS_NAME = "publisher_inputs.tar.gz"
REQUEST_NAME = "publisher_request.json"
MANIFEST_NAME = "publisher_manifest.json"


class StageError(ValueError):
    """The stage cannot be built. The message holds fixed text only."""


def refused(relative: str) -> bool:
    parts = PurePosixPath(relative).parts
    name = parts[-1]
    return "private_original" in parts or name == ".env" or name.startswith(".env.") or name.endswith(".tfvars.json") or name.startswith("terraform.tfstate")


def archive_bytes(entries: dict[str, bytes]) -> bytes:
    """Fixed order, owners, modes and times, and a gzip header with no time, so the same inputs give the same bytes."""
    buffer = io.BytesIO()
    with (
        gzip.GzipFile(fileobj=buffer, mode="wb", mtime=0, filename="") as compressed,
        tarfile.open(fileobj=compressed, mode="w", format=tarfile.PAX_FORMAT) as archive,
    ):
        for name in sorted(entries):
            info = tarfile.TarInfo(name)
            info.size, info.mode, info.mtime = len(entries[name]), 0o644, 0
            archive.addfile(info, io.BytesIO(entries[name]))
    return buffer.getvalue()


def build(mirror_output: Path, queue: str, plan: Path, confirmation: Path, hosts: Path, hosts_approval: Path, output: Path, resume: Sequence[str]) -> Path:
    """Write the archive and the request into a new folder and return it."""
    relative = PurePosixPath(queue)
    if relative.is_absolute() or ".." in relative.parts or not (mirror_output / "mirror" / relative).is_file():
        raise StageError("The queue must be a file inside the mirror.")
    entries: dict[str, bytes] = {}
    for path in sorted(mirror_output.rglob("*")):
        name = path.relative_to(mirror_output).as_posix()
        if path.is_symlink() or (not path.is_file() and not path.is_dir()):
            raise StageError("The mirror output holds a file that is not plain, so the stage is refused.")
        if path.is_file():
            if refused(name):
                raise StageError("The mirror output holds an original or a settings file, so the stage is refused.")
            entries[name] = path.read_bytes()
    if MANIFEST_NAME in entries:
        raise StageError("The mirror output already holds a manifest, so the stage is refused.")
    routed = {"removal_plan.json": plan, "removal_confirmation.json": confirmation, "publisher_hosts.json": hosts, "hosts_approval.json": hosts_approval}
    entries |= {name: path.read_bytes() for name, path in routed.items()}
    entries[MANIFEST_NAME] = encoded_json({"queue": f"mirror/{relative.as_posix()}"})
    body = archive_bytes(entries)
    request = {
        "inputs_sha256": hashlib.sha256(body).hexdigest(),
        "queue_sha256": hashlib.sha256((mirror_output / "mirror" / relative).read_bytes()).hexdigest(),
        "resume_sources": list(resume),
    }
    write_once(output / INPUTS_NAME, body)
    write_once(output / REQUEST_NAME, encoded_json(request))
    return output


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--mirror-output", required=True, type=Path)
    parser.add_argument("--queue", required=True)
    for name in ("removal-plan", "removal-confirmation", "hosts", "hosts-approval", "output"):
        parser.add_argument(f"--{name}", required=True, type=Path)
    parser.add_argument("--resume-source", action="append", default=[])
    args = parser.parse_args(argv)
    try:
        folder = build(
            args.mirror_output, args.queue, args.removal_plan, args.removal_confirmation, args.hosts, args.hosts_approval, args.output, args.resume_source
        )
    except (StageError, OSError) as error:
        parser.error(str(error))
    sys.stdout.write(json.dumps({"stage": str(folder)}) + "\n")


if __name__ == "__main__":
    main()
