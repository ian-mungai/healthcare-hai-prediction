"""Check that ECR still holds the recorded digest of each named image, with one bounded read-only call per image.

Run from the repository root: ``.venv/bin/python -m scripts.infrastructure.image_digest_check --repository-urls <file> --profile <profile> --region <region>
--only <image> [<image>...]``. ``push_images require`` proves that a digest is recorded. This command proves that the registry still has it: another push can
make the ECR lifecycle policy expire an older image, and a record of an image that is gone must not go on to staging and a host start.

Each image gets one ``aws ecr describe-images`` call by digest, under the project profile and region and a time limit (``--seconds``, 30 by default).
Exit 0 means ECR holds every digest. Exit 1 means a digest is not recorded or ECR does not hold it: the plan of the images that are missing is printed, with the
steps to publish and record them. Exit 2 means ECR could not be asked (a failed call, a refusal or the time limit): nothing is guessed and nothing is changed.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from scripts.infrastructure import push_images
from scripts.process import TimeoutExpired, run_command

DEFAULT_SECONDS = 30.0
NOT_FOUND = ("ImageNotFoundException", "RepositoryNotFoundException")


def ecr_status(repository: str, digest: str, profile: str, region: str, seconds: float) -> str:
    """Whether ECR holds the digest: ``present``, ``missing`` (no image or no repository) or ``failed`` (the call failed or was refused) or ``timeout``."""
    arguments = ["ecr", "describe-images", "--repository-name", repository, "--image-ids", f"imageDigest={digest}", "--profile", profile, "--region", region]
    try:
        result = run_command("aws", [*arguments, "--output", "json"], timeout=seconds)
    except TimeoutExpired:
        return "timeout"
    except OSError:
        return "failed"
    if result.returncode != 0:
        return "missing" if any(word in result.stderr for word in NOT_FOUND) else "failed"
    try:
        details: Any = json.loads(result.stdout).get("imageDetails", [])
    except (ValueError, AttributeError):
        return "failed"
    return "present" if any(isinstance(item, dict) and item.get("imageDigest") == digest for item in details) else "missing"


def main(argv: list[str] | None = None) -> int:
    """Check the named images and return the exit code."""
    cli = argparse.ArgumentParser(description=__doc__.split("\n", maxsplit=1)[0] if __doc__ else None)
    cli.add_argument("--root", type=Path, default=push_images.ROOT, help="the repository folder that holds the image files (a test passes a scratch copy)")
    cli.add_argument("--repository-urls", type=Path, required=True)
    cli.add_argument("--profile", required=True)
    cli.add_argument("--region", required=True)
    cli.add_argument("--only", nargs="+", required=True)
    cli.add_argument("--seconds", type=float, default=DEFAULT_SECONDS)
    args = cli.parse_args(argv)
    try:
        config = push_images.load_config(args.root / push_images.CONFIG_FILE)
        unknown = [name for name in args.only if name not in config["images"]]
        if unknown:
            raise push_images.ImagesError(f"unknown image: {', '.join(unknown)}")
        urls = push_images.repository_urls(args.repository_urls)
        path = args.root / push_images.DIGESTS_FILE
        recorded = dict(json.loads(path.read_text(encoding="utf-8")).get("image_digests", {})) if path.exists() else {}
    except (push_images.ImagesError, OSError, ValueError) as error:
        sys.stderr.write(f"{error}\n")
        return 1
    status = {
        name: ecr_status(urls[name].partition("/")[2], recorded[name], args.profile, args.region, args.seconds) if name in recorded else "missing"
        for name in args.only
    }
    for name, answer in status.items():
        sys.stdout.write(f"{name}: {answer}\n")
    missing = [name for name, answer in status.items() if answer == "missing"]
    failed = [name for name, answer in status.items() if answer == "failed"]
    slow = [name for name, answer in status.items() if answer == "timeout"]
    if missing:
        sys.stdout.write("\n".join(push_images.plan(config, urls, args.profile, args.region, missing)) + "\n")
        sys.stderr.write(
            f"ECR does not hold the recorded digest of {', '.join(missing)}: another push can make its lifecycle policy expire an older image. "
            "Run the commands above, record the new digests, then run the script again.\n"
        )
        return 1
    if failed or slow:
        reasons = [f"a failed or refused call for {', '.join(failed)}"] * bool(failed)
        reasons += [f"ECR did not answer within {args.seconds:g} s for {', '.join(slow)}"] * bool(slow)
        sys.stderr.write(
            f"ECR could not be checked ({'; '.join(reasons)}). Nothing was guessed and nothing was changed. "
            "Check the profile and the network, then run the script again.\n"
        )
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
