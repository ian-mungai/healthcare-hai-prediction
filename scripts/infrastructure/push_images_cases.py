"""Offline cases for the OpenMetadata image mirror and the drift image of ``push_images``; no real registry, AWS service or Docker is contacted.

Run from the repository root: ``.venv/bin/python -m scripts.infrastructure.push_images_cases``. ``scripts/infrastructure/run_push_images_e2e.py`` runs
them and keeps the report. The cases call the command line of ``push_images`` (``check``, ``plan`` and ``record``) on a scratch copy of the two image
files, and the list and plan functions on the real image list. The upstream digests below are the linux/arm64 entries of the selected releases that the
owner read from the Docker Hub registry on Nov 3 2026; they are the oracle for the pins in ``infra/services/images.json``.

The command line of ``push_images`` starts no program (the ``no_program`` case watches for one). Two cases do start local programs: ``plan_execution`` and
``registry_file`` run the printed plan and small scripts with ``sh`` or ``bash`` in a scratch folder, against stand-in ``aws`` and ``docker`` commands
(``push_images_fake_cloud.py``, run with the Python interpreter in isolated mode). Their PATH holds only the stand-ins, ``/usr/bin`` and ``/bin``, so no real
``aws`` or ``docker`` can run.
"""

from __future__ import annotations

import contextlib
import copy
import io
import json
import shutil
import sys
import tempfile
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

from scripts.infrastructure import push_images
from scripts.infrastructure import push_images_fake_cloud as fake_cloud
from scripts.process import run_command
from scripts.quality import lock_env

REPO_ROOT = Path(__file__).resolve().parents[2]
UPSTREAM = {
    "openmetadata_server": "sha256:010d63887c5b428d877a84e7edd65a152894145e0bb0a90aabf334cd70815a0c",
    "openmetadata_ingestion": "sha256:cd28c0b1f64b9c3581badf996f7fef6ffea3918754d588095b2f04a8c9f3dca4",
    "openmetadata_search": "sha256:6836708871685e272ad9042285e1c680790fe4315b047279ad16f761da2f4b70",
}
METADATA_KEY = {"openmetadata_server": "server", "openmetadata_ingestion": "ingestion", "openmetadata_search": "search"}
UPSTREAM_NAME = {
    "openmetadata_server": "docker.io/openmetadata/server",
    "openmetadata_ingestion": "docker.io/openmetadata/ingestion",
    "openmetadata_search": "docker.io/opensearchproject/opensearch",
}
RELEASE = {"openmetadata_server": "2.0.7", "openmetadata_ingestion": "2.0.7", "openmetadata_search": "3.3.2"}
ACCOUNT = str(2 * 10**11 + 7)
OTHER = "sha256:" + "ab" * 32
SERVICES_FILE = "infra/services/images.json"
METADATA_FILE = "config/metadata/images.json"
DIGEST_FILE = "infra/services/images.auto.tfvars.json"

Case = Callable[[], dict[str, bool]]


def scratch_root(directory: Path) -> Path:
    """A scratch repository root: links to everything in the repository except the folders of the two image files, which hold copies.

    The check reads build contexts and local pins through the root, so the links keep it valid. Only the two copies and the variable file are written.
    """
    for entry in REPO_ROOT.iterdir():
        if entry.name not in ("infra", "config", ".git"):
            (directory / entry.name).symlink_to(entry)
    for top, copied in (("infra", "services"), ("config", "metadata")):
        (directory / top).mkdir()
        for entry in (REPO_ROOT / top).iterdir():
            if entry.name != copied:
                (directory / top / entry.name).symlink_to(entry)
        (directory / top / copied).mkdir()
        for entry in (REPO_ROOT / top / copied).iterdir():
            if entry.name == "images.json":
                shutil.copy(entry, directory / top / copied / entry.name)
            elif entry.name != "images.auto.tfvars.json":
                (directory / top / copied / entry.name).symlink_to(entry)
    return directory


def said(result: tuple[int, str, str]) -> str:
    """Everything a command line printed, standard output and standard error."""
    return result[1] + result[2]


def run(root: Path, *arguments: str) -> tuple[int, str, str]:
    """One command line of ``push_images`` on a scratch root: the exit code, the standard output and the standard error."""
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        try:
            code = push_images.main(["--root", str(root), *arguments])
        except SystemExit as stop:
            code = int(stop.code) if isinstance(stop.code, int) else 1
    return code, out.getvalue(), err.getvalue()


def read(root: Path, relative: str) -> Any:
    """A JSON file of the scratch root, or ``None`` when it does not exist."""
    path = root / relative
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else None


def digests_of(root: Path) -> dict[str, str]:
    """The digests of the services variable file, or an empty map."""
    return dict((read(root, DIGEST_FILE) or {}).get("image_digests", {}))


def metadata_digests(root: Path) -> dict[str, str]:
    """The ``digest`` field of the server, ingestion and search images of the scratch metadata image file."""
    images = read(root, METADATA_FILE)["images"]
    return {name: images[key]["digest"] for name, key in METADATA_KEY.items()}


def assignments(digests: dict[str, str]) -> list[str]:
    return [f"{name}={digest}" for name, digest in digests.items()]


def repository_urls(directory: Path) -> Path:
    """A repository URL file for every image of the list, in the plain object form."""
    config = push_images.load_config(REPO_ROOT / SERVICES_FILE)
    path = directory / "urls.json"
    path.write_text(json.dumps({name: f"{ACCOUNT}.dkr.ecr.us-west-2.amazonaws.com/example_project_{name}_dev" for name in config["images"]}), encoding="utf-8")
    return path


def real_config() -> dict[str, Any]:
    return push_images.load_config(REPO_ROOT / SERVICES_FILE)


def list_pins() -> dict[str, bool]:
    """The list holds the three images as mirrors of the selected upstream arm64 digests, and the database image stays out."""
    config = real_config()
    images = config["images"]
    metadata = json.loads((REPO_ROOT / METADATA_FILE).read_text(encoding="utf-8"))["images"]
    pins = {
        name: str(images.get(name, {}).get("source")) == f"{UPSTREAM_NAME[name]}@{UPSTREAM[name]}" and images.get(name, {}).get("kind") == "mirror"
        for name in UPSTREAM
    }
    return {
        "the list is valid with the three images": push_images.check(config) == [],
        "each image is a mirror of its selected upstream release pinned by the linux/arm64 digest": all(pins.values()),
        "each tag follows the release": all(images.get(name, {}).get("tag") == f"{RELEASE[name]}-arm64" for name in UPSTREAM),
        "each image names its entry of the metadata image file, whose upstream and release match": all(
            images.get(name, {}).get("metadata_image") == key and metadata[key]["upstream"] == UPSTREAM_NAME[name] and metadata[key]["release"] == RELEASE[name]
            for name, key in METADATA_KEY.items()
        ),
        "the database image is not in the list": not any("database" in name for name in images),
    }


def list_refusals() -> dict[str, bool]:
    """A pin that is not the selected digest, a missing entry, a build, an unpinned source or another upstream name is refused with the cause."""
    base = real_config()

    def problems(mutate: Callable[[dict[str, Any]], None]) -> str:
        config = copy.deepcopy(base)
        mutate(config["images"])
        return "\n".join(push_images.check(config))

    def unpinned(images: dict[str, Any]) -> None:
        images["openmetadata_server"]["source"] = "docker.io/openmetadata/server:2.0.7"

    def other_name(images: dict[str, Any]) -> None:
        images["openmetadata_ingestion"]["source"] = "docker.io/someone/ingestion@" + UPSTREAM["openmetadata_ingestion"]

    def wrong_tag(images: dict[str, Any]) -> None:
        images["openmetadata_search"]["tag"] = "latest"

    def as_build(images: dict[str, Any]) -> None:
        images["openmetadata_server"] = {"kind": "build", "context": ".", "local_tag": "x", "tag": "x", "arm64": "x", "runs_as": "x"}

    def no_link(images: dict[str, Any]) -> None:
        del images["openmetadata_search"]["metadata_image"]

    def no_check(images: dict[str, Any]) -> None:
        del images["openmetadata_server"]["read_back"]

    def removed(images: dict[str, Any]) -> None:
        del images["openmetadata_ingestion"]

    return {
        "an unpinned source is refused": "openmetadata_server" in problems(unpinned) and "not pinned by digest" in problems(unpinned),
        "a source from another upstream name is refused": "openmetadata_ingestion" in problems(other_name) and "upstream" in problems(other_name),
        "a tag that is not the release is refused": "openmetadata_search" in problems(wrong_tag) and "tag" in problems(wrong_tag),
        "a build instead of a mirror is refused": "openmetadata_server" in problems(as_build) and "mirror" in problems(as_build),
        "a missing metadata link is refused": "openmetadata_search" in problems(no_link) and "metadata_image" in problems(no_link),
        "a missing digest read-back is refused": "openmetadata_server" in problems(no_check) and "read_back" in problems(no_check),
        "a removed image is refused": "openmetadata_ingestion" in problems(removed),
    }


def plan_lines() -> dict[str, bool]:
    """The plan mirrors each image by its digest, skips one that ECR holds with that digest and reads the digest back."""
    config = real_config()
    with tempfile.TemporaryDirectory(prefix="push_images_plan_") as directory:
        urls = push_images.repository_urls(repository_urls(Path(directory)))
    lines = push_images.plan(config, urls, "example_profile", "us-west-2", list(UPSTREAM))
    text = "\n".join(lines)
    return {
        "each image is pulled for linux/arm64 by its upstream digest": all(
            f"docker pull --platform linux/arm64 {UPSTREAM_NAME[n]}@{UPSTREAM[n]}" in text for n in UPSTREAM
        ),
        "the pull and push run only when ECR does not hold the digest": all(
            any(f"imageDigest={UPSTREAM[n]}" in line and "||" in line and "docker pull" in line for line in lines) for n in UPSTREAM
        ),
        "the ECR digest is read back and compared with the upstream digest": all(
            any(f"= {UPSTREAM[n]}" in line and "exit 1" in line for line in lines) for n in UPSTREAM
        ),
        "no image is pulled by a tag alone": not any(line.startswith("docker pull") and "@sha256:" not in line for line in lines),
        "the plan names the record command with the three names": "push_images record" in text and all(f"{n}=sha256:<DIGEST>" in text for n in UPSTREAM),
        "the database image is not planned": "database" not in text,
    }


def drift_image() -> dict[str, bool]:
    """The drift task's own image is a build of ``services/drift`` that the plan accepts by name, beside the training and feature store images it also runs."""
    config = real_config()
    images = config["images"]
    drift = images.get("drift", {})
    with tempfile.TemporaryDirectory(prefix="push_images_drift_") as directory:
        urls = push_images.repository_urls(repository_urls(Path(directory)))
    try:
        lines = push_images.plan(config, urls, "example_profile", "us-west-2", ["drift"])
    except push_images.ImagesError:
        lines = []
    text = "\n".join(lines)
    return {
        "the list is valid with the drift image": push_images.check(config) == [],
        "the drift image builds the drift context and its Dockerfile": drift.get("kind") == "build"
        and drift.get("context") == "services/drift"
        and "dockerfile" not in drift,
        "the drift image needs no shared build context, because its Dockerfile copies only the lock's requirement file": "build_contexts" not in drift,
        "the drift tags follow the Evidently release of the lock": drift.get("local_tag") == "hai-drift:evidently0.7.23"
        and drift.get("tag") == "evidently0.7.23-arm64",
        "the requirement file the lock writes for the drift group is in that context": lock_env.IMAGE_FILES["drift"]
        == {"requirements.txt": "services/drift/requirements.txt"},
        "every image the drift task runs is in the list": all(images.get(name, {}).get("kind") == "build" for name in ("training", "feature_store", "drift")),
        "the plan for the drift image alone writes the requirement files and builds it for linux/arm64": (
            "scripts.quality.lock_env images" in text and "docker build --platform linux/arm64 --tag hai-drift:evidently0.7.23 services/drift" in text
        ),
        "the plan names the drift record command": "push_images record drift=sha256:<DIGEST>" in text,
    }


@contextlib.contextmanager
def sandbox(images: list[dict[str, str]], pushed_digest: str, platform: str = "linux/arm64") -> Iterator[tuple[Path, dict[str, str]]]:
    """A scratch folder with the stand-in ``aws`` and ``docker`` first on a PATH that holds only ``/usr/bin`` and ``/bin`` besides, and a registry."""
    with tempfile.TemporaryDirectory(prefix="push_images_shell_") as directory:
        folder = Path(directory)
        bin_directory = folder / "bin"
        bin_directory.mkdir()
        for command in ("aws", "docker"):
            shim = bin_directory / command
            shim.write_text(f'#!/bin/sh\nexec "{sys.executable}" -I "{Path(fake_cloud.__file__).resolve()}" {command} "$@"\n', encoding="utf-8")
            shim.chmod(0o755)
        (folder / "ecr.json").write_text(json.dumps(images), encoding="utf-8")
        environment = {
            "PATH": f"{bin_directory}:/usr/bin:/bin",
            "PUSH_IMAGES_FAKE_STATE": str(folder),
            "PUSH_IMAGES_FAKE_DIGEST": pushed_digest,
            "PUSH_IMAGES_FAKE_PLATFORM": platform,
        }
        yield folder, environment


def run_plan(
    images: list[dict[str, str]], pushed_digest: str, name: str = "openmetadata_server", platform: str = "linux/arm64"
) -> tuple[int, list[str], list[dict[str, str]]]:
    """Run the printed plan of one image as shell against the fake registry: the exit code, the commands the stand-in saw and the registry afterwards."""
    with sandbox(images, pushed_digest, platform) as (folder, environment):
        urls = push_images.repository_urls(repository_urls(folder))
        (folder / "plan.sh").write_text("\n".join(push_images.plan(real_config(), urls, "example_profile", "us-west-2", [name])) + "\n", encoding="utf-8")
        result = run_command("sh", ["-e", "plan.sh"], cwd=folder, env=environment, timeout=60)
        commands = (folder / "commands.log").read_text(encoding="utf-8").splitlines()
        registry = json.loads((folder / "ecr.json").read_text(encoding="utf-8"))
    return result.returncode, commands, registry


def held(name: str, tag: str, digest: str | None = None) -> dict[str, str]:
    """An image of the fake registry in the repository of ``name``."""
    return {"repository": f"example_project_{name}_dev", "tag": tag, "digest": digest or UPSTREAM[name]}


def plan_execution() -> dict[str, bool]:
    """The printed plan runs as shell against a fake registry: a held image is skipped, a push is verified and a differing digest stops."""
    name = "openmetadata_server"
    pinned = UPSTREAM[name]
    same_tag = f"2.0.7-arm64-{pinned.removeprefix('sha256:')[:12]}"
    held_same = run_plan([held(name, same_tag)], pinned)
    held_other = run_plan([held(name, "2.0.7-hand-pushed")], pinned)
    empty = run_plan([], pinned)
    differs = run_plan([], OTHER)
    wrong_held = run_plan([held(name, same_tag, OTHER)], pinned)
    wrong_platform = run_plan([], pinned, platform="linux/amd64")

    def docker_ran(commands: list[str], word: str) -> bool:
        return any(line.startswith(f"docker {word}") for line in commands)

    def tag_lookups(commands: list[str]) -> list[str]:
        return [line for line in commands if "imageTag=" in line]

    return {
        "an image held under the synthesized tag is skipped and verified": held_same[0] == 0
        and not docker_ran(held_same[1], "pull")
        and not docker_ran(held_same[1], "push"),
        "an image held under another tag is skipped and verified": held_other[0] == 0
        and not docker_ran(held_other[1], "pull")
        and not docker_ran(held_other[1], "push"),
        "an image that ECR does not hold is pulled and pushed once, then verified": empty[0] == 0
        and sum(line.startswith("docker pull") for line in empty[1]) == 1
        and sum(line.startswith("docker push") for line in empty[1]) == 1
        and len(empty[2]) == 1,
        "a push that lands under a different digest stops with the digest message": differs[0] == 1 and docker_ran(differs[1], "push"),
        "a registry that holds only other digests is not taken for a match": wrong_held[0] == 1 and docker_ran(wrong_held[1], "push"),
        "an image that is not linux/arm64 is not pushed": wrong_platform[0] != 0 and not docker_ran(wrong_platform[1], "push") and wrong_platform[2] == [],
        "no verification reads an image by tag": not any(tag_lookups(result[1]) for result in (held_same, held_other, empty, differs, wrong_held)),
    }


def fingerprint(path: Path) -> tuple[int, int, bytes]:
    """The inode, the modification time in nanoseconds and the bytes of a file: any rewrite, even with the same content, changes the first two."""
    status = path.stat()
    return status.st_ino, status.st_mtime_ns, path.read_bytes()


READ_ONLY = """aws ecr get-login-password --profile p --region r | docker login --username AWS --password-stdin registry.example
aws ecr describe-images --profile p --region r --repository-name {repository} --image-ids imageDigest={digest} >/dev/null
aws ecr describe-images --profile p --region r --repository-name {repository} --image-ids imageTag=absent >/dev/null 2>&1 || true
docker pull --platform linux/arm64 {source}
docker tag {source} registry.example/{repository}:t
docker image inspect --format x {source} >/dev/null
"""
STRESS = """pids=""
for attempt in $(seq 1 {count}); do
  (aws ecr get-login-password --profile p --region r | docker login --username AWS --password-stdin registry.example) &
  pids="$pids $!"
  (aws ecr describe-images --profile p --region r --repository-name {repository} --image-ids imageDigest={digest} >/dev/null) &
  pids="$pids $!"
done
status=0
for pid in $pids; do
  wait "$pid" || status=1
done
exit $status
"""
STRESS_COUNT = 20


def registry_file() -> dict[str, bool]:
    """The stand-in rewrites the registry file only for a successful push, and then by rename; concurrent logins and lookups never see a partial file."""
    name = "openmetadata_server"
    values = {"repository": f"example_project_{name}_dev", "digest": UPSTREAM[name], "source": f"{UPSTREAM_NAME[name]}@{UPSTREAM[name]}"}
    with sandbox([held(name, "kept")], UPSTREAM[name]) as (folder, environment):
        registry = folder / "ecr.json"
        before = fingerprint(registry)
        (folder / "reads.sh").write_text(READ_ONLY.format(**values), encoding="utf-8")
        reads = run_command("bash", ["-e", "-o", "pipefail", "reads.sh"], cwd=folder, env=environment, timeout=60)
        after_reads = fingerprint(registry)
        (folder / "stress.sh").write_text(STRESS.format(count=STRESS_COUNT, **values), encoding="utf-8")
        stress = run_command("bash", ["-e", "-o", "pipefail", "stress.sh"], cwd=folder, env=environment, timeout=120)
        after_stress = fingerprint(registry)
        logged = (folder / "commands.log").read_text(encoding="utf-8").splitlines()
    with sandbox([], UPSTREAM[name]) as (folder, environment):
        registry = folder / "ecr.json"
        before_push = fingerprint(registry)
        urls = push_images.repository_urls(repository_urls(folder))
        (folder / "plan.sh").write_text("\n".join(push_images.plan(real_config(), urls, "example_profile", "us-west-2", [name])) + "\n", encoding="utf-8")
        pushed = run_command("sh", ["-e", "plan.sh"], cwd=folder, env=environment, timeout=60)
        after_push = fingerprint(registry)
        leftovers = sorted(item.name for item in folder.iterdir() if item.name.startswith("ecr.json") and item.name != "ecr.json")
    with sandbox([held(name, "kept")], UPSTREAM[name]) as (folder, environment):
        registry = folder / "ecr.json"
        before_refusal = fingerprint(registry)
        target = f"registry.example/{values['repository']}:kept"
        (folder / "refused.sh").write_text(f"docker tag {values['source']} {target}\ndocker push {target}\n", encoding="utf-8")
        refused = run_command("sh", ["-e", "refused.sh"], cwd=folder, env=environment, timeout=60)
        after_refusal = fingerprint(registry)
    return {
        "a push that ECR refuses for an existing tag fails and leaves the registry file untouched": refused.returncode != 0 and after_refusal == before_refusal,
        "login, lookup, pull, tag and inspect commands leave the registry file untouched": reads.returncode == 0 and after_reads == before,
        f"{STRESS_COUNT} concurrent login pipelines and lookups all succeed and leave the registry file untouched": stress.returncode == 0
        and after_stress == before
        and sum(line.startswith("aws ecr get-login-password") for line in logged) >= STRESS_COUNT,
        "a successful push replaces the registry file by rename with the new image in it": pushed.returncode == 0
        and after_push[0] != before_push[0]
        and json.loads(after_push[2]) != json.loads(before_push[2])
        and len(json.loads(after_push[2])) == 1,
        "a push leaves no temporary file beside the registry file": leftovers == [],
    }


def record_flow() -> dict[str, bool]:
    """Matching digests are written to both files, a rerun changes nothing and a partial record leaves the other images unresolved."""
    with tempfile.TemporaryDirectory(prefix="push_images_record_") as directory:
        root = scratch_root(Path(directory))
        before_services = (root / SERVICES_FILE).read_bytes()
        unresolved_before = metadata_digests(root)
        partial_code, _, _ = run(root, "record", f"openmetadata_server={UPSTREAM['openmetadata_server']}")
        partial_metadata = metadata_digests(root)
        partial_tfvars = digests_of(root)
        code, out, err = run(root, "record", *assignments(UPSTREAM))
        files = {relative: (root / relative).read_bytes() for relative in (METADATA_FILE, DIGEST_FILE)}
        again_code, _, _ = run(root, "record", *assignments(UPSTREAM))
        again = {relative: (root / relative).read_bytes() for relative in (METADATA_FILE, DIGEST_FILE)}
        recorded_metadata = metadata_digests(root)
        recorded_tfvars = digests_of(root)
        database = read(root, METADATA_FILE)["images"]["database"]
        services_after = (root / SERVICES_FILE).read_bytes()
    return {
        "the metadata file starts with all three digests unresolved": all(value.startswith("TO_RESOLVE") for value in unresolved_before.values()),
        "a partial record writes only that digest to both files": partial_code == 0
        and partial_metadata["openmetadata_server"] == UPSTREAM["openmetadata_server"]
        and partial_metadata["openmetadata_ingestion"].startswith("TO_RESOLVE")
        and partial_tfvars == {"openmetadata_server": UPSTREAM["openmetadata_server"]},
        "a full record writes the three digests to the metadata file and the services variable file": code == 0
        and recorded_metadata == UPSTREAM
        and recorded_tfvars == UPSTREAM,
        "a rerun with the same digests exits zero and changes neither file": again_code == 0 and again == files,
        "the database image gets no digest and the image list itself is untouched": "digest" not in database and services_after == before_services,
        "the output names the recorded count and the variable file": f"Recorded 3 digests in {DIGEST_FILE}" in out and not err,
    }


def record_refusals() -> dict[str, bool]:
    """An ECR digest that differs from the upstream digest, the database image, an unknown name and a malformed digest change nothing and say why."""
    with tempfile.TemporaryDirectory(prefix="push_images_refuse_") as directory:
        root = scratch_root(Path(directory))
        reference = {relative: (root / relative).read_bytes() for relative in (METADATA_FILE,)}
        mismatch = run(root, "record", f"openmetadata_server={OTHER}")
        mixed = run(root, "record", f"openmetadata_ingestion={UPSTREAM['openmetadata_ingestion']}", f"openmetadata_search={OTHER}")
        database = run(root, "record", f"openmetadata_database={UPSTREAM['openmetadata_server']}")
        unknown = run(root, "record", f"openmetadata_other={UPSTREAM['openmetadata_server']}")
        malformed = run(root, "record", "openmetadata_server=sha256:1234")
        unchanged = {relative: (root / relative).read_bytes() for relative in reference} == reference and not (root / DIGEST_FILE).exists()
    return {
        "an ECR digest that differs from the upstream digest exits one and names the image and both digests": mismatch[0] == 1
        and "openmetadata_server" in mismatch[2]
        and OTHER in mismatch[2]
        and UPSTREAM["openmetadata_server"] in mismatch[2]
        and "differs" in mismatch[2],
        "one differing digest among valid ones fails the whole record": mixed[0] == 1 and "openmetadata_search" in mixed[2],
        "the database image cannot be recorded": database[0] == 1 and "openmetadata_database" in database[2],
        "an unknown name cannot be recorded": unknown[0] == 1 and "openmetadata_other" in unknown[2],
        "a malformed digest cannot be recorded": malformed[0] == 1 and "sha256" in malformed[2],
        "after every refusal neither file has changed and no variable file exists": unchanged,
    }


def require_resolved() -> dict[str, bool]:
    """The plain check passes with unresolved digests; the activation check fails on each cause and passes once both files agree."""
    with tempfile.TemporaryDirectory(prefix="push_images_resolved_") as directory:
        root = scratch_root(Path(directory))
        plain = run(root, "check")
        fresh = run(root, "check", "--require-resolved")
        run(root, "record", *assignments(UPSTREAM))
        resolved = run(root, "check", "--require-resolved")
        tfvars = root / DIGEST_FILE
        saved = tfvars.read_text(encoding="utf-8")
        tfvars.write_text(json.dumps({"image_digests": {k: v for k, v in digests_of(root).items() if k != "openmetadata_search"}}), encoding="utf-8")
        missing = run(root, "check", "--require-resolved")
        tfvars.write_text(json.dumps({"image_digests": {**digests_of(root), "openmetadata_ingestion": OTHER}}), encoding="utf-8")
        differs = run(root, "check", "--require-resolved")
        tfvars.write_text(saved, encoding="utf-8")
        tfvars.unlink()
        no_file = run(root, "check", "--require-resolved")
    return {
        "the plain check passes while the digests are unresolved": plain[0] == 0,
        "the activation check fails while the digests are unresolved and names each image": fresh[0] == 1
        and all(name in said(fresh) for name in UPSTREAM)
        and "TO_RESOLVE" in said(fresh),
        "the activation check passes once the digests are recorded": resolved[0] == 0,
        "the activation check fails when the variable file lacks an image": missing[0] == 1 and "openmetadata_search" in said(missing),
        "the activation check fails when the two files differ": differs[0] == 1 and "openmetadata_ingestion" in said(differs) and "differs" in said(differs),
        "the activation check fails when the variable file does not exist": no_file[0] == 1 and DIGEST_FILE in said(no_file),
    }


WATCHED = ("subprocess.Popen", "os.system", "os.exec", "os.posix_spawn", "os.spawn", "os.fork", "pty.spawn")
STARTS: list[str] = []
WATCHING: list[bool] = []


def watch(event: str, arguments: tuple[object, ...]) -> None:
    """An audit hook that records a program start while a case is watching; a hook cannot be removed, so it only reads the flag."""
    if WATCHING and event in WATCHED:
        STARTS.append(event)


def no_program() -> dict[str, bool]:
    """The command line starts no program: the audit events of every way to start one are recorded, and every command still finishes."""
    if watch not in AUDITED:
        sys.addaudithook(watch)
        AUDITED.append(watch)
    STARTS.clear()
    WATCHING.append(True)
    try:
        with tempfile.TemporaryDirectory(prefix="push_images_quiet_") as directory:
            root = scratch_root(Path(directory))
            urls = repository_urls(Path(directory))
            codes = [
                run(root, "check")[0],
                run(root, "plan", "--repository-urls", str(urls), "--profile", "p", "--region", "us-west-2", "--only", *UPSTREAM)[0],
                run(root, "record", *assignments(UPSTREAM))[0],
            ]
    finally:
        WATCHING.clear()
    return {"check, plan and record all exit zero without starting a program": codes == [0, 0, 0] and not STARTS}


AUDITED: list[object] = []


CASES: dict[str, Case] = {
    "list_pins": list_pins,
    "list_refusals": list_refusals,
    "plan_lines": plan_lines,
    "drift_image": drift_image,
    "plan_execution": plan_execution,
    "registry_file": registry_file,
    "record_flow": record_flow,
    "record_refusals": record_refusals,
    "require_resolved": require_resolved,
    "no_program": no_program,
}


def main() -> int:
    """Run every case and print one line per check; exit one when a check fails or a case raises."""
    failed = False
    for name, function in CASES.items():
        try:
            checks = function()
        except Exception as error:
            checks = {f"the case ended with {type(error).__name__}": False}
        for check, ok in checks.items():
            sys.stdout.write(f"{'PASS' if ok else 'FAIL'} {name}: {check}\n")
            failed = failed or not ok
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
