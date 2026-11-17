"""Ship and restore real Prometheus blocks through a folder that stands in for the S3 bucket, and write a report of what came back.

    python -m scripts.infrastructure.prometheus_history_real_blocks --blocks <folder of finished blocks> --work <empty folder> [--report <file>]

``--blocks`` is the data folder of a Prometheus of the pinned image (or the output of ``promtool tsdb create-blocks-from openmetrics``). The run ships it
with ``monitoring.prometheus_history.ship_once`` to ``<work>/bucket`` (a stand-in with the create-only refusal of S3), restores into ``<work>/restored`` with
a scratch folder on the same volume, restores a second time, and compares every restored file byte for byte with its source. The report lists the blocks and
the checks; the heavy run (data/local_keep/prometheus_history/heavy_run.sh) then starts the pinned image on ``<work>/restored`` and queries the samples.
No AWS service is contacted.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from botocore.exceptions import ClientError

from monitoring import prometheus_history as history

BUCKET = "stand-in-bucket"


class FolderBucket:
    """The calls of the shipper and the restore over a folder: a create-only put refuses an existing key as S3 does."""

    def __init__(self, root: Path) -> None:
        self.root = root

    def path(self, key: str) -> Path:
        return self.root / key

    def put_object(self, **call: Any) -> dict[str, Any]:
        target = self.path(call["Key"])
        if call.get("IfNoneMatch") != "*":
            raise AssertionError(f"{call['Key']} was put without the create-only condition")
        if target.exists():
            raise ClientError({"Error": {"Code": "PreconditionFailed", "Message": "exists"}}, "PutObject")
        target.parent.mkdir(parents=True, exist_ok=True)
        body = call["Body"]
        target.write_bytes(body.read() if hasattr(body, "read") else bytes(body))
        return {}

    def get_object(self, **call: Any) -> dict[str, Any]:
        return {"Body": self.path(call["Key"]).open("rb")}

    def list_objects_v2(self, **call: Any) -> dict[str, Any]:
        found = sorted(path for path in self.root.rglob("*") if path.is_file() and path.relative_to(self.root).as_posix().startswith(call["Prefix"]))
        return {"Contents": [{"Key": path.relative_to(self.root).as_posix(), "Size": path.stat().st_size} for path in found], "IsTruncated": False}


def tree(root: Path) -> dict[str, bytes]:
    return {path.relative_to(root).as_posix(): path.read_bytes() for path in sorted(root.rglob("*")) if path.is_file()}


def run(blocks: Path, work: Path) -> dict[str, Any]:
    bucket = FolderBucket(work / "bucket")
    restored, scratch = work / "restored", work / "scratch"
    first = history.ship_once(bucket, BUCKET, blocks)
    second = history.ship_once(bucket, BUCKET, blocks)
    back = history.restore(bucket, BUCKET, restored, scratch)
    again = history.restore(bucket, BUCKET, restored, scratch)
    names = [path.name for path in history.finished_blocks(blocks)]
    source = {key: body for key, body in tree(blocks).items() if key.split("/", 1)[0] in names}
    checks = {
        "at least one finished block was shipped": bool(first.shipped),
        "every finished block was shipped": first.shipped == names,
        "the second shipper pass uploaded nothing": second.shipped == [] and second.present == names,
        "every block was restored": back.restored == names,
        "the second restore changed nothing": again.restored == [] and again.present == names,
        "every restored file equals its source byte for byte": tree(restored) == source,
        "the scratch folder is empty": not any(scratch.rglob("*")) if scratch.exists() else True,
    }
    return {"blocks": names, "files": len(source), "checks": checks, "passed": all(checks.values())}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    parser.add_argument("--blocks", type=Path, required=True)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--report", type=Path)
    arguments = parser.parse_args(argv)
    if arguments.work.exists() and any(arguments.work.iterdir()):
        sys.stderr.write(f"{arguments.work} is not empty; give a new folder\n")
        return 2
    arguments.work.mkdir(parents=True, exist_ok=True)
    report = run(arguments.blocks, arguments.work)
    text = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if arguments.report is not None:
        arguments.report.write_text(text, encoding="utf-8")
    sys.stdout.write(text)
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
