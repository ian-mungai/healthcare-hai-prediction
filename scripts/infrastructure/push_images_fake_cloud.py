"""A stand-in for the ``aws ecr`` and ``docker`` commands that a printed ``push_images`` plan runs; test-only, offline, used by ``push_images_cases``.

It keeps the images of one fake registry in ``ecr.json`` under the folder named by ``PUSH_IMAGES_FAKE_STATE`` and appends every command to
``commands.log``. ``aws ecr describe-images`` answers from that list by tag or by digest and fails with ``ImageNotFoundException`` (exit 254) when
nothing matches. Only a successful ``docker push`` changes the registry: it adds the pushed tag with the digest in ``PUSH_IMAGES_FAKE_DIGEST`` and
replaces ``ecr.json`` by rename, so a concurrent reader never sees a partial file. A push is refused for a tag that exists, as an immutable
repository does, and for an image that was not tagged first, as docker does. ``docker login --password-stdin`` reads its input to the end, as docker
does. ``docker image inspect`` prints ``PUSH_IMAGES_FAKE_PLATFORM`` (default linux/arm64). Every other docker command succeeds.
It decides nothing that the cases prove: whether a plan skips, pushes or stops comes only from
the shell lines of the plan. It imports nothing from the repository, so it runs with ``python -I push_images_fake_cloud.py aws|docker <arguments>``.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path


def option(arguments: list[str], name: str) -> str:
    """The value that follows ``name`` on the command line."""
    return arguments[arguments.index(name) + 1]


def aws(arguments: list[str], images: list[dict[str, str]]) -> int:
    """Answer ``aws ecr`` commands from the fake registry."""
    if arguments[:2] == ["ecr", "get-login-password"]:
        sys.stdout.write("stand-in-token\n")
        return 0
    if arguments[:2] != ["ecr", "describe-images"]:
        sys.stderr.write("unknown aws command\n")
        return 252
    repository = option(arguments, "--repository-name")
    kind, _, value = option(arguments, "--image-ids").partition("=")
    key = "digest" if kind == "imageDigest" else "tag"
    found = [item for item in images if item["repository"] == repository and item[key] == value]
    if not found:
        sys.stderr.write("An error occurred (ImageNotFoundException) when calling the DescribeImages operation\n")
        return 254
    if "--query" in arguments:
        sys.stdout.write(found[0]["digest"] + "\n")
    else:
        sys.stdout.write(json.dumps({"imageDetails": [{"imageDigest": found[0]["digest"]}]}))
    return 0


def docker(arguments: list[str], images: list[dict[str, str]]) -> int:
    """Answer ``docker`` commands: a push adds an image, an inspect names the platform and everything else succeeds."""
    if arguments[:1] == ["push"]:
        target = arguments[1]
        log = (Path(os.environ["PUSH_IMAGES_FAKE_STATE"]) / "commands.log").read_text(encoding="utf-8").splitlines()
        if not any(line.startswith("docker tag ") and line.endswith(" " + target) for line in log):
            sys.stderr.write("An image does not exist locally with the tag: " + target + "\n")
            return 1
        host_and_repository, _, tag = target.rpartition(":")
        repository = host_and_repository.partition("/")[2]
        if any(item["repository"] == repository and item["tag"] == tag for item in images):
            sys.stderr.write("tag invalid: the tag already exists in the repository and the repository is immutable\n")
            return 1
        images.append({"repository": repository, "tag": tag, "digest": os.environ["PUSH_IMAGES_FAKE_DIGEST"]})
        save(images)
    elif arguments[:1] == ["login"] and "--password-stdin" in arguments:
        sys.stdin.read()
    elif arguments[:2] == ["image", "inspect"]:
        sys.stdout.write(os.environ.get("PUSH_IMAGES_FAKE_PLATFORM", "linux/arm64") + "\n")
    return 0


def save(images: list[dict[str, str]]) -> None:
    """Replace the registry file by rename, so that a process that reads it at the same time sees the old or the new file and never a partial one."""
    registry = Path(os.environ["PUSH_IMAGES_FAKE_STATE"]) / "ecr.json"
    handle, temporary = tempfile.mkstemp(dir=registry.parent, prefix="ecr.json.", suffix=".tmp")
    try:
        with os.fdopen(handle, "w", encoding="utf-8") as stream:
            stream.write(json.dumps(images))
        os.replace(temporary, registry)
    except BaseException:
        Path(temporary).unlink(missing_ok=True)
        raise


def main(argv: list[str]) -> int:
    """Run one stand-in command: ``argv[0]`` is ``aws`` or ``docker``."""
    state = Path(os.environ["PUSH_IMAGES_FAKE_STATE"])
    with (state / "commands.log").open("a", encoding="utf-8") as log:
        log.write(" ".join(argv) + "\n")
    registry = state / "ecr.json"
    images: list[dict[str, str]] = json.loads(registry.read_text(encoding="utf-8"))
    return aws(argv[1:], images) if argv[0] == "aws" else docker(argv[1:], images)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
