"""Plan the arm64 image build and ECR push for the AWS container host, and record the pushed digests; this script starts no program.

The coordinating agent runs the printed Docker and AWS commands, because only the launcher may call Docker. Three commands, run from the repository root:

    .venv/bin/python -m scripts.infrastructure.push_images check [--require-resolved]
    .venv/bin/python -m scripts.infrastructure.push_images plan --repository-urls urls.json --profile <PROFILE> --region <REGION> [--only NAME ...]
    .venv/bin/python -m scripts.infrastructure.push_images record NAME=sha256:<DIGEST> [NAME=sha256:<DIGEST> ...]

``check`` validates ``infra/services/images.json`` offline: every mirror pinned by digest, every build context present, every base image of a
built Dockerfile pinned by digest, every source build pinned to a commit and base digests, and every mirrored reference equal to the pin the
local stack uses. ``plan`` prints the commands for each image. ``urls.json`` is the foundation output ``ecr_repository_urls`` (``terraform output
-json`` form or a plain object). The ECR repositories are immutable, so every push uses a unique tag: the tag in the list plus twelve hex digits of
the pinned source digest (mirror), the local image ID (build) or the source commit (source build). The printed push is skipped when ECR already
holds that tag, and the digest to record is read back from ECR. ``record`` merges those digests into ``infra/services/images.auto.tfvars.json``, the
variable file the services stack reads. Images run by digest, never by tag, so a rerun that finds the same tag records the same digest.

The three OpenMetadata images (``openmetadata_server``, ``openmetadata_ingestion`` and ``openmetadata_search``) are mirrors pinned to the linux/arm64
digest of the selected upstream release, and each names its entry in ``config/metadata/images.json`` (``metadata_image``). Their plan skips the pull and
the push when ECR already holds the digest under any tag, and asks ECR for the pinned digest and stops when it is absent (``read_back``). ``record`` takes
only a digest equal to the pin for these images and writes it to the variable file and to ``config/metadata/images.json`` together. ``check
--require-resolved`` is the activation check: it fails while a digest in that file is still ``TO_RESOLVE`` or the two files disagree. The finite catalog
task's database image is not part of this list.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import sys
import tempfile
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[2]
CONFIG_FILE = "infra/services/images.json"
DIGESTS_FILE = "infra/services/images.auto.tfvars.json"
METADATA_FILE = "config/metadata/images.json"
CONFIG = ROOT / CONFIG_FILE
DIGESTS = ROOT / DIGESTS_FILE
METADATA = ROOT / METADATA_FILE
OPENMETADATA = {"openmetadata_server": "server", "openmetadata_ingestion": "ingestion", "openmetadata_search": "search"}
PLATFORM = "linux/arm64"
KINDS = ("mirror", "build", "source", "blocked")
SERVICE_IMAGES = ("postgres", "redis", "polaris", "polaris_admin", "airflow")
LOCAL_PIN_FILES = ("docker-compose.yaml", "services/orchestration/egress_proxy.json", "services/lineage/docker-compose.lineage.yaml")
PINNED = re.compile(r"@sha256:[0-9a-f]{64}$")
DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
NAME = re.compile(r"^[a-z][a-z0-9_]*$")
URL = re.compile(r"^[0-9]{12}\.dkr\.ecr\.[a-z0-9-]+\.amazonaws\.com/[a-z0-9._/-]+$")
COMMIT = re.compile(r"^[0-9a-f]{40}$")
FROM_LINE = re.compile(r"^FROM\s+(?:--platform=\S+\s+)?(\S+)(?:\s+AS\s+(\S+))?\s*$", re.IGNORECASE)


class ImagesError(ValueError):
    """The image list, a repository URL or a digest is invalid; the message names it."""


def load_config(path: Path = CONFIG) -> dict[str, Any]:
    """The image list, read as JSON."""
    config = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(config, dict) or config.get("platform") != PLATFORM or not isinstance(config.get("images"), dict):
        raise ImagesError(f"{path.name} must be an object with platform {PLATFORM} and an images object")
    return config


def dockerfile_problems(context: Path, root: Path, name: str = "Dockerfile") -> list[str]:
    """Problems with the Dockerfile of a build context: it must exist and pin every base image by digest. ``name`` is its path inside the context."""
    dockerfile = context / name
    if not dockerfile.is_file():
        return [f"{dockerfile.relative_to(root)} does not exist"]
    problems: list[str] = []
    stages: set[str] = set()
    for line in dockerfile.read_text(encoding="utf-8").splitlines():
        match = FROM_LINE.match(line.strip())
        if match is None:
            continue
        base, stage = match.groups()
        if base not in stages and not PINNED.search(base):
            problems.append(f"{dockerfile.relative_to(root)} base image {base} is not pinned by digest")
        if stage:
            stages.add(stage)
    return problems


def source_problems(name: str, image: dict[str, Any]) -> list[str]:
    """Problems with a source build: a public repository, a full commit, digest-pinned base images, a local tag and well-formed edits."""
    problems: list[str] = []
    if not str(image.get("repository", "")).startswith("https://"):
        problems.append(f"{name}: repository must be an https URL")
    if not COMMIT.match(str(image.get("commit", ""))):
        problems.append(f"{name}: commit must be a full 40-digit commit hash, not a tag or branch")
    if not isinstance(image.get("context"), str) or not image["context"]:
        problems.append(f"{name}: context is required")
    if not isinstance(image.get("local_tag"), str) or not image["local_tag"]:
        problems.append(f"{name}: local_tag is required")
    if not isinstance(image.get("from_lines"), int) or image["from_lines"] < 1:
        problems.append(f"{name}: from_lines must count the Dockerfile lines that name an outside base image")
    base_pins = image.get("base_pins")
    if not isinstance(base_pins, dict) or not base_pins:
        problems.append(f"{name}: base_pins must pin every base image of the Dockerfile")
    else:
        problems.extend(f"{name}: base image {base} is not pinned to a sha256 digest" for base, digest in base_pins.items() if not DIGEST.match(str(digest)))
    for edit in image.get("edits", []):
        if not all(isinstance(edit.get(field), str) and edit[field] for field in ("file", "expression", "expect")):
            problems.append(f"{name}: every edit needs file, expression and expect")
    return problems


def openmetadata_problems(images: dict[str, dict[str, Any]], metadata: dict[str, Any]) -> list[str]:
    """Problems with the three OpenMetadata mirrors: present, pinned to the upstream name and release of the metadata image file and read back from ECR."""
    problems: list[str] = []
    for name, key in OPENMETADATA.items():
        image = images.get(name)
        if image is None:
            problems.append(f"the OpenMetadata mirror needs an entry for {name}")
            continue
        entry = metadata.get("images", {}).get(key, {})
        if image.get("kind") != "mirror":
            problems.append(f"{name}: an OpenMetadata image must be a mirror of the upstream image")
            continue
        if image.get("metadata_image") != key:
            problems.append(f"{name}: metadata_image must be {key}, the entry of {METADATA_FILE}")
        if image.get("read_back") != "equal":
            problems.append(f"{name}: read_back must be equal, so that the ECR digest is compared with the pinned upstream digest")
        source = str(image.get("source", ""))
        if not source.startswith(f"{entry.get('upstream')}@sha256:"):
            problems.append(f"{name}: source must be the upstream {entry.get('upstream')} pinned by digest")
        if image.get("tag") != f"{entry.get('release')}-arm64":
            problems.append(f"{name}: the tag must be the release {entry.get('release')} followed by -arm64")
        if image.get("local_reference", True) is not False:
            problems.append(f"{name}: local_reference must be false, because the local stack does not run this image")
    return problems


def unresolved_problems(config: dict[str, Any], root: Path) -> list[str]:
    """Problems for the activation check: a digest still ``TO_RESOLVE``, a missing variable file or entry and files that disagree with each other or the pin."""
    metadata = json.loads((root / METADATA_FILE).read_text(encoding="utf-8"))["images"]
    path = root / DIGESTS_FILE
    recorded: dict[str, str] = dict(json.loads(path.read_text(encoding="utf-8")).get("image_digests", {})) if path.exists() else {}
    problems = [] if path.exists() else [f"{DIGESTS_FILE} does not exist; run record after the push"]
    for name, key in OPENMETADATA.items():
        digest = str(metadata.get(key, {}).get("digest", ""))
        if not DIGEST.match(digest):
            problems.append(f"{name}: the digest in {METADATA_FILE} is still {digest.split(':')[0] or 'missing'}; run record after the push")
        if path.exists() and name not in recorded:
            problems.append(f"{name}: missing from {DIGESTS_FILE}")
        elif path.exists() and DIGEST.match(digest) and recorded[name] != digest:
            problems.append(f"{name}: {DIGESTS_FILE} differs from {METADATA_FILE}")
        pin = str(config["images"].get(name, {}).get("source", "")).rpartition("@")[2]
        if DIGEST.match(digest) and digest != pin:
            problems.append(f"{name}: the digest in {METADATA_FILE} differs from the pinned upstream digest")
    return problems


def check(config: dict[str, Any], root: Path = ROOT) -> list[str]:
    """Every problem of the image list, as text lines; an empty list means the list is valid."""
    problems: list[str] = []
    metadata_file = root / METADATA_FILE
    problems.extend(openmetadata_problems(config["images"], json.loads(metadata_file.read_text(encoding="utf-8")) if metadata_file.exists() else {}))
    images: dict[str, dict[str, Any]] = config["images"]
    problems.extend(f"the services stack needs an entry for {name}" for name in SERVICE_IMAGES if name not in images)
    local_references = "\n".join((root / name).read_text(encoding="utf-8") for name in LOCAL_PIN_FILES if (root / name).is_file())
    seen_tags: dict[str, str] = {}
    for name, image in images.items():
        if not NAME.match(name):
            problems.append(f"{name}: use lowercase letters, digits and underscores")
        if image.get("kind") not in KINDS:
            problems.append(f"{name}: kind must be one of {', '.join(KINDS)}")
            continue
        for field in ("tag", "arm64", "runs_as"):
            if not isinstance(image.get(field), str) or not image[field]:
                problems.append(f"{name}: {field} is required")
        if image.get("kind") in ("mirror", "blocked"):
            source = str(image.get("source", ""))
            if not PINNED.search(source):
                problems.append(f"{name}: source {source or '(missing)'} is not pinned by digest")
            elif image.get("local_reference", True) and source not in local_references:
                problems.append(f"{name}: source differs from the pin in the local stack")
        if image.get("kind") == "build":
            context = root / str(image.get("context", ""))
            problems.extend(
                dockerfile_problems(context, root, str(image.get("dockerfile", "Dockerfile")))
                if context.is_dir()
                else [f"{name}: build context {image.get('context')} does not exist"]
            )
            for build_name, path in dict(image.get("build_contexts", {})).items():
                if not (root / str(path)).is_dir():
                    problems.append(f"{name}: build context {build_name} points to {path}, which does not exist")
            if not isinstance(image.get("local_tag"), str) or not image["local_tag"]:
                problems.append(f"{name}: local_tag is required")
        if image.get("kind") == "source":
            problems.extend(source_problems(name, image))
        owner = seen_tags.setdefault(f"{name}:{image.get('tag')}", name)
        if owner != name:
            problems.append(f"{name}: repeats a tag of {owner}")
    return problems


def repository_urls(path: Path) -> dict[str, str]:
    """The ECR repository URL of each image, from ``terraform output -json`` or a plain object."""
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, dict) and isinstance(data.get("value"), dict):
        data = data["value"]
    if not isinstance(data, dict):
        raise ImagesError(f"{path.name} must hold an object of image name to repository URL")
    urls = {str(key): str(value) for key, value in data.items()}
    for name, url in urls.items():
        if not URL.match(url):
            raise ImagesError(f"repository URL of {name} is not an ECR repository URL without a tag")
    return urls


def sed_pattern(text: str) -> str:
    """Escape the characters that an extended regular expression reads as operators."""
    return re.sub(r"([.\\[\]{}()*+?^$|])", r"\\\1", text)


def source_prepare(name: str, image: dict[str, Any], clones: dict[str, str]) -> list[str]:
    """Commands that check out the pinned commit, pin the base images by digest and apply the listed edits to a scratch copy."""
    lines: list[str] = []
    repository = str(image["repository"])
    new_clone = repository not in clones
    clone_variable = clones.setdefault(repository, f"SOURCE_{len(clones) + 1}")
    if new_clone:
        lines.extend(
            [
                f'{clone_variable}="$(mktemp -d)/source"   # scratch folder outside this repository',
                f'git clone --no-checkout {shlex.quote(repository)} "${clone_variable}"',
                f'git -C "${clone_variable}" checkout --detach {image["commit"]}',
                f'git -C "${clone_variable}" rev-parse HEAD   # must print {image["commit"]}',
            ]
        )
    dockerfile = "/".join(part for part in (str(image["context"]).strip("."), "Dockerfile") if part)
    for base, digest in dict(image["base_pins"]).items():
        expression = f"s#^FROM {sed_pattern(base)}( |$)#FROM {base}@{digest}\\1#"
        lines.append(f'sed -i.orig -E {shlex.quote(expression)} "${clone_variable}/{dockerfile}"')
    lines.append(f'grep -c "^FROM .*@sha256:" "${clone_variable}/{dockerfile}"   # must print {image["from_lines"]}')
    for edit in image.get("edits", []):
        lines.append(f'sed -i.orig -E {shlex.quote(str(edit["expression"]))} "${clone_variable}/{edit["file"]}"')
        lines.append(f'grep -c {shlex.quote(str(edit["expect"]))} "${clone_variable}/{edit["file"]}"   # must print 1')
    return lines


def image_lines(name: str, image: dict[str, Any], url: str, profile: str, region: str, clones: dict[str, str]) -> list[str]:
    """The commands that obtain, check, tag, push and read back one image."""
    repository = urlsplit("//" + url).path.lstrip("/")
    tag_variable = f"TAG_{name.upper()}"
    lines: list[str] = []
    if image.get("read_back") == "equal":
        return mirror_lines(name, image, url, profile, region)
    if image["kind"] == "mirror":
        local = str(image["source"])
        suffix = local.rsplit("@sha256:", maxsplit=1)[1][:12]
        lines.append(f"docker pull --platform {PLATFORM} {local}")
        tag = f"{image['tag']}-{suffix}"
    elif image["kind"] == "source":
        local = str(image["local_tag"])
        lines.extend(source_prepare(name, image, clones))
        clone_variable = clones[str(image["repository"])]
        arguments = " ".join(f"--build-arg {key}={value}" for key, value in sorted(dict(image.get("build_args", {})).items()))
        lines.append(
            f'docker build --platform {PLATFORM} --tag {local} {arguments} "${clone_variable}/{str(image["context"]).strip(".") or "."}"'.replace("  ", " ")
        )
        tag = f"{image['tag']}-{str(image['commit'])[:12]}"
    else:
        local = str(image["local_tag"])
        contexts = " ".join(f"--build-context {key}={path}" for key, path in sorted(dict(image.get("build_contexts", {})).items()))
        dockerfile = f"-f {str(Path(str(image['context'])) / str(image['dockerfile']))} " if "dockerfile" in image else ""
        lines.append(f"docker build --platform {PLATFORM} --tag {local} {dockerfile}{contexts} {image['context']}".replace("  ", " "))
        tag = ""
    lines.append(f"docker image inspect --format '{{{{.Os}}}}/{{{{.Architecture}}}}' {local}   # must print {PLATFORM}")
    if tag:
        lines.append(f"{tag_variable}={tag}")
    else:
        lines.append(f"{tag_variable}={image['tag']}-$(docker image inspect --format '{{{{.Id}}}}' {local} | sed 's/^sha256://' | cut -c1-12)")
    describe = f"aws ecr describe-images --profile {profile} --region {region} --repository-name {repository} --image-ids imageTag=${tag_variable}"
    target = f"{url}:${tag_variable}"
    lines.append(
        f"{describe} >/dev/null 2>&1 || {{ docker tag {local} {target} && docker push {target}; }}   # the repository is immutable: a tag is pushed once"
    )
    lines.append(f"{describe} --query 'imageDetails[0].imageDigest' --output text   # the digest to record for {name}")
    return lines


def mirror_lines(name: str, image: dict[str, Any], url: str, profile: str, region: str) -> list[str]:
    """The commands that mirror one pinned upstream image: skip it when ECR holds the digest, push it once and stop when ECR lacks the pinned digest.

    The digest is known, so every check reads the image by digest (``imageDigest=``). A tag only names the push, and an image that ECR holds under another
    tag counts as present.
    """
    repository = urlsplit("//" + url).path.lstrip("/")
    source = str(image["source"])
    digest = source.rpartition("@")[2]
    tag_variable = f"TAG_{name.upper()}"
    tag = f"{image['tag']}-{digest.removeprefix('sha256:')[:12]}"
    target = f"{url}:${tag_variable}"
    describe = f"aws ecr describe-images --profile {profile} --region {region} --repository-name {repository} --image-ids imageDigest={digest}"
    inspect = f"docker image inspect --format '{{{{.Os}}}}/{{{{.Architecture}}}}' {source}"
    return [
        f"{tag_variable}={tag}",
        f"{describe} >/dev/null 2>&1 || {{ docker pull --platform {PLATFORM} {source} "
        f'&& [ "$({inspect})" = {PLATFORM} ] && docker tag {source} {target} && docker push {target}; }}   # skipped when ECR holds the digest under any tag',
        f"[ \"$({describe} --query 'imageDetails[0].imageDigest' --output text 2>/dev/null)\" = {digest} ] "
        f'|| {{ echo "{name}: ECR does not hold the pinned upstream digest" >&2; exit 1; }}   # the digest to record for {name} is {digest}',
    ]


def plan(config: dict[str, Any], urls: dict[str, str], profile: str, region: str, only: list[str]) -> list[str]:
    """The commands for the chosen images, in order, as text lines; blocked images appear as comments."""
    images: dict[str, dict[str, Any]] = config["images"]
    unknown = [name for name in only if name not in images]
    if unknown:
        raise ImagesError(f"unknown image: {', '.join(unknown)}")
    chosen = {name: image for name, image in images.items() if not only or name in only}
    missing = [name for name, image in chosen.items() if image["kind"] != "blocked" and name not in urls]
    if missing:
        raise ImagesError(f"no repository URL for {', '.join(missing)}")
    registries = {urlsplit("//" + urls[name]).netloc for name, image in chosen.items() if image["kind"] != "blocked"}
    lines = [f"# Every image is {PLATFORM}. Run these from the repository root; the profile is named on each AWS command."]
    for registry in sorted(registries):
        lines.append(f"aws ecr get-login-password --profile {profile} --region {region} | docker login --username AWS --password-stdin {registry}")
    clones: dict[str, str] = {}
    for name, image in chosen.items():
        lines.append("")
        if image["kind"] == "blocked":
            lines.append(f"# {name}: blocked. {image['arm64']}. Build an arm64 image from source or choose another route before this image is pushed.")
            continue
        lines.extend(image_lines(name, image, urls[name], profile, region, clones))
    names = " ".join(f"{name}=sha256:<DIGEST>" for name, image in chosen.items() if image["kind"] != "blocked")
    lines.extend(["", f"# Then record the digests: .venv/bin/python -m scripts.infrastructure.push_images record {names}"])
    if clones:
        folders = " ".join(f'"$(dirname "${variable}")"' for variable in clones.values())
        lines.append(f"# Then remove the scratch source folders: rm -rf {folders}")
    return lines


def write_atomically(path: Path, text: str) -> None:
    """Replace a file with the text through a temporary file in its folder."""
    handle, temporary = tempfile.mkstemp(dir=path.parent, prefix=".images_", suffix=".tmp")
    try:
        with os.fdopen(handle, "w", encoding="utf-8") as stream:
            stream.write(text)
        os.replace(temporary, path)
    except BaseException:
        Path(temporary).unlink(missing_ok=True)
        raise


def record(config: dict[str, Any], assignments: list[str], path: Path = DIGESTS, metadata_path: Path = METADATA) -> dict[str, str]:
    """Merge ``name=digest`` pairs into the variable file and the OpenMetadata ones also into the metadata image file; return the variable file's digests.

    An OpenMetadata image takes only the digest equal to its pin. Everything is checked before the first write, and a rerun writes the same bytes.
    """
    known: set[str] = {name for name, image in config["images"].items() if image["kind"] != "blocked"}
    current: dict[str, str] = {}
    if path.exists():
        current = dict(json.loads(path.read_text(encoding="utf-8")).get("image_digests", {}))
    for assignment in assignments:
        name, separator, digest = assignment.partition("=")
        if not separator or name not in known or not DIGEST.match(digest):
            raise ImagesError(f"{name or assignment}: use NAME=sha256:<64 hex digits> with an image that is not blocked")
        pin = str(config["images"][name].get("source", "")).rpartition("@")[2]
        if name in OPENMETADATA and digest != pin:
            raise ImagesError(f"{name}: the ECR digest {digest} differs from the pinned upstream digest {pin}; nothing was recorded")
        current[name] = digest
    texts = {path: json.dumps({"image_digests": dict(sorted(current.items()))}, indent=2) + "\n"}
    chosen = {name: current[name] for name in OPENMETADATA if name in current}
    if chosen and metadata_path.exists():
        document = json.loads(metadata_path.read_text(encoding="utf-8"))
        for name, digest in chosen.items():
            document["images"][OPENMETADATA[name]]["digest"] = digest
        texts[metadata_path] = json.dumps(document, indent=2, ensure_ascii=False) + "\n"
    for target, text in texts.items():
        write_atomically(target, text)
    return dict(sorted(current.items()))


def main(argv: list[str] | None = None) -> int:
    """Run one command and return the exit code."""
    cli = argparse.ArgumentParser(description=__doc__.split("\n", maxsplit=1)[0] if __doc__ else None)
    cli.add_argument("--root", type=Path, default=ROOT, help="the repository folder that holds the image files (a test passes a scratch copy)")
    commands = cli.add_subparsers(dest="command", required=True)
    checker = commands.add_parser("check", help="validate the image list offline")
    checker.add_argument("--require-resolved", action="store_true", help="also fail while an OpenMetadata digest is still TO_RESOLVE or the files disagree")
    planner = commands.add_parser("plan", help="print the build, push and digest commands")
    planner.add_argument("--repository-urls", type=Path, required=True)
    planner.add_argument("--profile", required=True)
    planner.add_argument("--region", required=True)
    planner.add_argument("--only", nargs="+", default=[])
    recorder = commands.add_parser("record", help="merge pushed digests into the services variable file")
    recorder.add_argument("assignments", nargs="+")
    args = cli.parse_args(argv)
    root: Path = args.root
    try:
        config = load_config(root / CONFIG_FILE)
        if args.command == "check":
            problems = check(config, root)
            if args.require_resolved:
                problems += unresolved_problems(config, root)
            sys.stdout.write("\n".join(problems or ["The image list is valid."]) + "\n")
            return 1 if problems else 0
        if args.command == "plan":
            problems = check(config, root)
            if problems:
                sys.stderr.write("\n".join(problems) + "\n")
                return 1
            sys.stdout.write("\n".join(plan(config, repository_urls(args.repository_urls), args.profile, args.region, args.only)) + "\n")
            return 0
        digests = record(config, args.assignments, root / DIGESTS_FILE, root / METADATA_FILE)
        sys.stdout.write(f"Recorded {len(digests)} digests in {DIGESTS_FILE}.\n")
        return 0
    except (ImagesError, OSError, ValueError) as error:
        sys.stderr.write(f"{error}\n")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
