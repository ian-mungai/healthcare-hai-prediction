"""Build and check the code archive that a layer task fetches: one deterministic tarball of a Git revision plus the verified dbt packages.

    .venv/bin/python -m scripts.lakehouse.code_archive build --output <new folder> [--revision HEAD]
    .venv/bin/python -m scripts.lakehouse.code_archive check <code.tar.gz> <code.sha256>
    .venv/bin/python -m scripts.lakehouse.code_archive cutover --old-revision <OLD> --new-revision <NEW>

``git archive`` alone cannot pass the first layer pin: the dbt packages are installed, not tracked, and the task has no network to install them. The builder
exports the revision, installs the packages of that revision's lock from the pinned local archive (``dbt_package_archive``, no network) and writes both
into one tar under ``dbt_packages/``. Names, order, owners, times and the gzip header are fixed, so the same revision and the same pinned packages always give
the same bytes. The digest file holds the SHA-256 of the tarball, which is the value the launch passes to the task as the code digest.
``check`` applies the layout that the layer driver's pin phase needs, before anything is staged or claimed.

A host runs the Airflow services from a checkout that their task definitions mount file by file. When a revision renames such a file, no order of two steps
(new checkout, then new definitions, or the reverse) leaves every running service its files. ``build --overlap-from <OLD>`` writes the transition release:
the revision plus the mounted files of the old revision that the new one lacks, taken from the old revision. With it the order is safe: the transition
release first, the new definitions next, the plain release last. ``cutover`` lists, for each step and for the two wrong orders, the mounted paths that
step's release lacks.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import re
import sys
import tarfile
import tempfile
from pathlib import Path

from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]
REVISION = re.compile(r"[0-9a-f]{40}")
SHA256 = re.compile(r"[0-9a-f]{64}")
PROJECT = "dbt"
PACKAGES = "dbt_packages"
LOCK = "dbt/package-lock.yml"
RECORD = "config/lakehouse/dbt_package_archive.json"
ARCHIVE_FOLDER = "data/analytics/dbt/package_archive"
LIMIT = 512 * 1024 * 1024


class ArchiveError(ValueError):
    """The code archive cannot be built, or it lacks what the layer pin needs."""


def git(arguments: list[str], root: Path, strip: bool = True) -> str:
    done = run_command("git", arguments, cwd=root, timeout=300)
    if done.returncode:
        raise ArchiveError(f"git {arguments[0]} failed: {done.stderr.strip()[-300:]}")
    return done.stdout.strip() if strip else done.stdout


SERVICES_FOLDER = "infra/services"
CHECKOUT_PATH = re.compile(r'"\$\{local\.code_path\}/([A-Za-z0-9_./-]+)"')
LAUNCH_FILES = re.compile(r"airflow_launch_files = \{\n(.*?)\n  \}", re.DOTALL)
LAUNCH_FILE = re.compile(r'= "([A-Za-z0-9_./-]+)"')


def mounted_paths(services_text: str) -> set[str]:
    """The repository paths (files or folders) that the services stack mounts from the host checkout: the checkout volumes and the launch files."""
    paths = set(CHECKOUT_PATH.findall(services_text))
    for block in LAUNCH_FILES.findall(services_text):
        paths.update(LAUNCH_FILE.findall(block))
    return paths


def tree_files(root: Path, commit: str) -> set[str]:
    return {line for line in git(["ls-tree", "-r", "--name-only", commit], root).splitlines() if line}


def services_text(root: Path, commit: str) -> str:
    """The Terraform files of the services stack at ``commit``."""
    names = [name for name in tree_files(root, commit) if name.startswith(f"{SERVICES_FOLDER}/") and name.endswith(".tf") and name.count("/") == 2]
    return "\n".join(git(["show", f"{commit}:{name}"], root) for name in sorted(names))


def present(path: str, files: set[str]) -> bool:
    """A mounted path exists in a release as a file or as a folder with files in it."""
    return path in files or any(name.startswith(f"{path}/") for name in files)


def overlap_members(root: Path, old: str, new: str) -> dict[str, bytes]:
    """The files that the old revision's definitions mount and the new revision lacks, with their content at the old revision."""
    old_files, new_files = tree_files(root, old), tree_files(root, new)
    gone = sorted(path for path in mounted_paths(services_text(root, old)) if path in old_files and not present(path, new_files))
    return {path: git(["show", f"{old}:{path}"], root, strip=False).encode() for path in gone}


def cutover_report(root: Path, old: str, new: str) -> dict[str, object]:
    """For every step of the rollout and for the two wrong orders, the mounted paths the release of that step lacks. ``safe`` is true when no step of the right
    order lacks one."""
    old_files, new_files = tree_files(root, old), tree_files(root, new)
    old_mounts, new_mounts = mounted_paths(services_text(root, old)), mounted_paths(services_text(root, new))
    transition = new_files | set(overlap_members(root, old, new))

    def lacking(mounts: set[str], files: set[str]) -> list[str]:
        return sorted(path for path in mounts if not present(path, files))

    states = {
        "old definitions on the old release": lacking(old_mounts, old_files),
        "old definitions on the transition release": lacking(old_mounts, transition),
        "both generations on the transition release": lacking(old_mounts | new_mounts, transition),
        "new definitions on the transition release": lacking(new_mounts, transition),
        "new definitions on the final release": lacking(new_mounts, new_files),
        "old definitions on the final release": lacking(old_mounts, new_files),
        "new definitions on the old release": lacking(new_mounts, old_files),
    }
    wrong = ("old definitions on the final release", "new definitions on the old release")
    return {"safe": not any(paths for name, paths in states.items() if name not in wrong), "states": states}


def compose(git_tar: Path, packages: Path, output: Path, extras: dict[str, bytes] | None = None) -> str:
    """Write the deterministic ``output`` tarball from a ``git archive`` tar and an installed package folder; return the commit ID it carries.

    ``extras`` are added after the revision's files, in name order, with the revision's newest time: the old-revision files of a transition release."""
    with tarfile.open(git_tar) as source:
        revision = str(source.pax_headers.get("comment", ""))
        if not REVISION.fullmatch(revision):
            raise ArchiveError("the Git tar carries no commit ID (it must come from git archive)")
        members = source.getmembers()
        if any(Path(member.name).parts[:1] == (PACKAGES,) for member in members):
            raise ArchiveError(f"the revision tracks a {PACKAGES} folder: installed packages must not be committed")
        mtime = max((member.mtime for member in members), default=0)
        with (
            output.open("wb") as raw,
            gzip.GzipFile(filename="", fileobj=raw, mode="wb", mtime=0, compresslevel=9) as compressed,
            tarfile.open(fileobj=compressed, mode="w", format=tarfile.PAX_FORMAT, pax_headers={"comment": revision}) as target,
        ):
            for member in members:
                target.addfile(member, source.extractfile(member) if member.isfile() else None)
            taken = {member.name.rstrip("/") for member in members}
            for name, data in sorted((extras or {}).items()):
                parts = Path(name).parts
                if name.startswith("/") or ".." in parts or parts[:1] == (PACKAGES,) or name in taken:
                    raise ArchiveError(f"an extra file may not shadow a file of the revision or enter {PACKAGES}: {name}")
                info = tarfile.TarInfo(name)
                info.size, info.mode, info.mtime, info.uid, info.gid, info.uname, info.gname = len(data), 0o644, mtime, 0, 0, "", ""
                target.addfile(info, io.BytesIO(data))
            for path in sorted(packages.rglob("*")):
                info = tarfile.TarInfo(f"{PACKAGES}/{path.relative_to(packages).as_posix()}")
                info.mtime, info.uid, info.gid, info.uname, info.gname = mtime, 0, 0, "", ""
                if path.is_symlink():
                    raise ArchiveError(f"the package folder holds a link: {path.name}")
                if path.is_dir():
                    info.type, info.mode = tarfile.DIRTYPE, 0o755
                    target.addfile(info)
                else:
                    data = path.read_bytes()
                    info.size, info.mode = len(data), 0o755 if path.stat().st_mode & 0o100 else 0o644
                    target.addfile(info, io.BytesIO(data))
    return revision


def build(output: Path, revision: str = "HEAD", root: Path = ROOT, overlap_from: str = "") -> dict[str, str]:
    """Export ``revision``, install its locked dbt packages from the pinned local archive and write ``code.tar.gz`` and ``code.sha256`` to a new folder.

    With ``overlap_from`` (an older revision) the archive is the transition release: it also holds the old revision's mounted files that ``revision`` lacks."""
    from scripts.lakehouse import dbt_package_archive  # needs PyYAML, which the task images do not carry, so only the host build imports it

    if output.exists():
        raise ArchiveError(f"{output} exists: pass a new folder")
    commit = git(["rev-parse", "--verify", f"{revision}^{{commit}}"], root)
    with tempfile.TemporaryDirectory(prefix="code_archive_") as scratch:
        folder = Path(scratch)
        git_tar = folder / "git.tar"
        git(["archive", "--format=tar", f"--output={git_tar}", commit], root)
        with tarfile.open(git_tar) as source:
            for name in (LOCK, RECORD):
                member = source.extractfile(name)
                if member is None:
                    raise ArchiveError(f"the revision has no {name}")
                (folder / Path(name).name).write_bytes(member.read())
        record = folder / Path(RECORD).name
        packages = folder / PACKAGES
        archive = root / json.loads(record.read_text())["archive"]["path"]
        dbt_package_archive.install(root, folder / Path(LOCK).name, record, archive, packages, hashlib.sha256(record.read_bytes()).hexdigest())
        output.mkdir(parents=True)
        extras = overlap_members(root, git(["rev-parse", "--verify", f"{overlap_from}^{{commit}}"], root), commit) if overlap_from else {}
        revision_id = compose(git_tar, packages, output / "code.tar.gz", extras)
    digest = hashlib.sha256((output / "code.tar.gz").read_bytes()).hexdigest()
    (output / "code.sha256").write_text(digest + "\n")
    check(output / "code.tar.gz", output / "code.sha256")
    return {"revision": revision_id, "sha256": digest, "folder": str(output), "overlap": sorted(extras)}


def check(archive: Path, digest_file: Path | None = None, expected: str = "") -> str:
    """Refuse an archive the layer pin cannot use; return its SHA-256. The tarball must match the digest, carry the commit ID, hold the dbt project and hold
    every package of its own lock under ``dbt_packages/``."""
    data = archive.read_bytes()
    if len(data) > LIMIT:
        raise ArchiveError("the archive is larger than the bound")
    digest = hashlib.sha256(data).hexdigest()
    wanted = expected or (digest_file.read_text().strip() if digest_file else digest)
    if not SHA256.fullmatch(wanted) or wanted != digest:
        raise ArchiveError("the archive does not match its SHA-256")
    with tarfile.open(fileobj=io.BytesIO(data)) as tar:
        if not REVISION.fullmatch(str(tar.pax_headers.get("comment", ""))):
            raise ArchiveError("the archive carries no commit ID (it must come from git archive)")
        names = {member.name.rstrip("/") for member in tar.getmembers()}
        if f"{PROJECT}/dbt_project.yml" not in names:
            raise ArchiveError(f"the archive has no {PROJECT}/dbt_project.yml")
        lock = tar.extractfile(LOCK)
        if lock is None:
            raise ArchiveError(f"the archive has no {LOCK}")
        wanted_packages = re.findall(r"^  - name: ([A-Za-z0-9_]+)$", lock.read().decode(), re.MULTILINE)
    if not wanted_packages:
        raise ArchiveError(f"{LOCK} names no package")
    missing = [name for name in wanted_packages if f"{PACKAGES}/{name}/dbt_project.yml" not in names]
    if missing:
        raise ArchiveError(f"the archive lacks the locked dbt packages {missing}: build it with scripts.lakehouse.code_archive, not git archive alone")
    return digest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    built = commands.add_parser("build", help="write code.tar.gz and code.sha256 for a revision into a new folder")
    built.add_argument("--output", type=Path, required=True)
    built.add_argument("--revision", default="HEAD")
    built.add_argument("--overlap-from", default="", help="an older revision: also carry its mounted files that --revision lacks (the transition release)")
    cutover = commands.add_parser("cutover", help="list the mounted paths each rollout step lacks; exit 1 unless the right order is safe")
    cutover.add_argument("--old-revision", required=True)
    cutover.add_argument("--new-revision", default="HEAD")
    checked = commands.add_parser("check", help="refuse an archive the layer pin cannot use")
    checked.add_argument("archive", type=Path)
    checked.add_argument("digest", type=Path)
    args = parser.parse_args()
    try:
        if args.command == "cutover":
            report = cutover_report(
                ROOT,
                git(["rev-parse", "--verify", f"{args.old_revision}^{{commit}}"], ROOT),
                git(["rev-parse", "--verify", f"{args.new_revision}^{{commit}}"], ROOT),
            )
            sys.stdout.write(json.dumps(report, indent=2) + "\n")
            return 0 if report["safe"] else 1
        result = build(args.output, args.revision, overlap_from=args.overlap_from) if args.command == "build" else {"sha256": check(args.archive, args.digest)}
    except (ArchiveError, OSError, KeyError, ValueError, tarfile.TarError) as error:
        sys.stderr.write(f"code archive refused: {error}\n")
        return 1
    sys.stdout.write(json.dumps(result, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
