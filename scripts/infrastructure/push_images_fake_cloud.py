"""A stand-in for the ``aws ecr`` and ``docker`` commands that a printed ``push_images`` plan runs; test-only, offline, used by ``push_images_cases``.

It keeps the images of one fake registry in ``ecr.json`` under the folder named by ``PUSH_IMAGES_FAKE_STATE`` and appends every command to
``commands.log``. ``aws ecr describe-images`` answers from that list by tag or by digest and fails with ``ImageNotFoundException`` (exit 254) when
nothing matches. Only a successful ``docker push`` changes the registry: it adds the pushed tag with the digest in ``PUSH_IMAGES_FAKE_DIGEST`` and
replaces ``ecr.json`` by rename, so a concurrent reader never sees a partial file. A push is refused for a tag that exists, as an immutable
repository does, and for an image that was not tagged first, as docker does. ``docker login --password-stdin`` reads its input to the end, as docker
does. ``docker image inspect`` prints ``PUSH_IMAGES_FAKE_PLATFORM`` (default linux/arm64).

``docker run`` follows ``PUSH_IMAGES_FAKE_RUN``. ``ok`` (the default) stands for an image whose files are the synthetic folder ``entry_root`` of the state
folder: a run with ``--entrypoint python`` executes the exact program of the plan in that folder (the working folder of the image, with the folder on the
import path; an absolute ``file:`` name is read below it) and so prints ``ok`` only when every named module and file can be read. Any other run prints the
text of the state file ``entry_output``. ``closed`` fails with a permission error and exit 1, ``exit23`` prints ``ok`` and exits 23, and ``hang`` ends only
when ``docker rm -f`` or ``docker kill`` names its container (a run without ``--name`` never ends before 120 s). ``stuck`` ignores the TERM signal and
cannot be removed or killed by name. A run with ``--name`` keeps a file in the state folder ``containers`` while it lives, which ``docker container inspect``,
``docker rm`` and ``docker kill`` read. Every other docker command succeeds.
It decides nothing that the cases prove: whether a plan skips, pushes or stops comes only from
the shell lines of the plan. It imports nothing from the repository, so it runs with ``python -I push_images_fake_cloud.py aws|docker <arguments>``.
"""

from __future__ import annotations

import json
import os
import runpy
import signal
import sys
import tempfile
import time
import traceback
from pathlib import Path

HANG_SECONDS = 120


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
    elif arguments[:1] == ["run"]:
        return run(arguments[1:])
    elif arguments[:1] in (["kill"], ["rm"]) or arguments[:2] == ["container", "inspect"]:
        return container_command(arguments)
    elif arguments[:2] == ["image", "inspect"]:
        sys.stdout.write(os.environ.get("PUSH_IMAGES_FAKE_PLATFORM", "linux/arm64") + "\n")
    return 0


def containers() -> Path:
    """The folder that holds one file per live container."""
    folder = Path(os.environ["PUSH_IMAGES_FAKE_STATE"]) / "containers"
    folder.mkdir(exist_ok=True)
    return folder


def container_command(arguments: list[str]) -> int:
    """``docker container inspect NAME`` fails for an unknown container; ``docker rm -f NAME`` and ``docker kill NAME`` end the container that has the name."""
    name = arguments[-1]
    live = containers() / name
    if os.environ.get("PUSH_IMAGES_FAKE_RUN") == "stuck" and arguments[0] in ("rm", "kill"):
        sys.stderr.write(f"Error: cannot remove container {name}: it does not stop\n")
        return 1
    if arguments[0] == "container":
        return 0 if live.exists() else 1
    if not live.exists():
        sys.stderr.write(f"Error: No such container: {name}\n")
        return 1
    live.unlink()
    return 0


def read_entries(names: list[str], root: Path, program: str) -> int:
    """Run the program of the plan on the synthetic image folder: the exit code it would have inside the image."""
    mapped = [f"file:{root}{name[5:]}" if name.startswith("file:/") else name for name in names]
    (root.parent / "entry_program.py").write_text(program, encoding="utf-8")
    previous = (os.getcwd(), list(sys.argv), list(sys.path))
    os.chdir(root)
    sys.argv = ["entry", *mapped]
    sys.path.insert(0, str(root))
    try:
        runpy.run_path(str(root.parent / "entry_program.py"), run_name="__main__")
    except Exception:
        traceback.print_exc()
        return 1
    finally:
        os.chdir(previous[0])
        sys.argv, sys.path[:] = previous[1], previous[2]
    return 0


def run(arguments: list[str]) -> int:
    """Answer ``docker run`` as ``PUSH_IMAGES_FAKE_RUN`` says."""
    mode = os.environ.get("PUSH_IMAGES_FAKE_RUN", "ok")
    state = Path(os.environ["PUSH_IMAGES_FAKE_STATE"])
    name = option(arguments, "--name") if "--name" in arguments else ""
    live = containers() / name if name else None
    if live is not None:
        live.write_text("running\n", encoding="utf-8")
    try:
        if mode == "closed":
            sys.stderr.write("PermissionError: [Errno 13] Permission denied: '/app/scripts/serving'\n")
            return 1
        if mode == "exit23":
            sys.stdout.write("ok\n")
            return 23
        if mode == "stuck":
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
            time.sleep(HANG_SECONDS)
            return 137
        if mode == "hang":
            deadline = time.monotonic() + HANG_SECONDS
            while (live is None or live.exists()) and time.monotonic() < deadline:
                time.sleep(0.1)
            return 137
        tail = arguments[arguments.index("--entrypoint") + 2 :]
        if arguments[arguments.index("--entrypoint") + 1] == "python" and tail[1:3] == ["-B", "-c"]:
            return read_entries(tail[4:], state / "entry_root", tail[3])
        sys.stdout.write((state / "entry_output").read_text(encoding="utf-8"))
        return 0
    finally:
        if live is not None:
            live.unlink(missing_ok=True)


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
