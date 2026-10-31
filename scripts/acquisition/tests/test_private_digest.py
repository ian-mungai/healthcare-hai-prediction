from __future__ import annotations

import ast
import hashlib
import json
import os
import stat
import sys
from pathlib import Path

from scripts.acquisition import private_digest, unit_classification
from scripts.acquisition.cli_tools import run_argv
from scripts.acquisition.tests.held import classification_body
from scripts.process import CompletedProcess
from tests.support import check

NAMES = ("Jane Q. Example", "Ozzie Sample-Rivera", "Dr. Pat Fictional", "Nell Folder-Name")
PRIVATE = {"snapshot_id": "OWNERS__1", "source_id": "OWNERS", "disposition": "direct_download", "mode": "cms_owners_org", "flags": [], "route": "publisher_url"}
FLAGGED = PRIVATE | {"snapshot_id": "FLAG__2", "mode": "file", "flags": []}
PUBLIC = PRIVATE | {"snapshot_id": "PUBLIC__3", "mode": "file"}


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def setup(tmp_path: Path) -> tuple[Path, Path, Path]:
    queue = tmp_path / "queue.json"
    units = [PRIVATE, FLAGGED, PUBLIC]
    queue.write_text(json.dumps({"run": 3, "queue": units}))
    classes = {"OWNERS__1": "personal", "FLAG__2": "personal"}
    queue.with_name("unit_classification.json").write_bytes(classification_body(queue.read_bytes(), units, classes, unreviewed=["FLAG__2"]))
    folder = tmp_path / "originals"
    (folder / "OWNERS__1" / NAMES[3]).mkdir(parents=True)
    (folder / "OWNERS__1" / f"{NAMES[0]}.csv").write_bytes(f"name\n{NAMES[0]}\n".encode())
    (folder / "OWNERS__1" / NAMES[3] / "inner.csv").write_bytes(f"name\n{NAMES[2]}\n".encode())
    (folder / "FLAG__2").mkdir()
    (folder / "FLAG__2" / "one.bin").write_bytes(b"\x00" + NAMES[1].encode())
    (folder / "PUBLIC__3").mkdir()
    (folder / "PUBLIC__3" / "public.csv").write_bytes(b"not private")
    (folder / f"{NAMES[1]} not in the queue").mkdir()
    (folder / f"{NAMES[1]} not in the queue" / "x.csv").write_bytes(NAMES[1].encode())
    output = tmp_path / "owner_only"
    output.mkdir(mode=0o700)
    return queue, folder, output


def run(queue: Path, folder: Path, output: Path) -> CompletedProcess[str]:
    environment = {**os.environ, "PYTHONPATH": str(Path(private_digest.__file__).resolve().parents[2])}
    arguments = ["--queue", str(queue), "--folder", str(folder), "--output", str(output / "digests.json")]
    return run_argv(
        [sys.executable, "-B", "-m", "scripts.acquisition.private_digest", *arguments],
        cwd=Path(private_digest.__file__).resolve().parents[2],
        env=environment,
        timeout=120,
    )


def test_it_records_digest_size_and_file_count_by_unit_id_and_nothing_else(tmp_path: Path) -> None:
    queue, folder, output = setup(tmp_path)
    done = run(queue, folder, output)
    record = json.loads((output / "digests.json").read_text())
    first = [sha(f"name\n{NAMES[0]}\n".encode()), sha(f"name\n{NAMES[2]}\n".encode())]
    expected = {
        "sha256": sha("\n".join(sorted(first)).encode()),
        "bytes": len(f"name\n{NAMES[0]}\n") + len(f"name\n{NAMES[2]}\n"),
        "files": 2,
        "status": "read",
    }
    check(record["units"]["OWNERS__1"] == expected, f"digest, size and count for the collector unit: {record['units']}")
    check(
        record["units"]["FLAG__2"] == {"sha256": sha(sha(b"\x00" + NAMES[1].encode()).encode()), "bytes": 1 + len(NAMES[1]), "files": 1, "status": "read"},
        "the flagged file unit",
    )
    check(set(record["units"]) == {"OWNERS__1", "FLAG__2"}, "only private units of the queue are read, never the public one or a folder outside the queue")
    check(done.returncode == 0, f"exit {done.returncode}")


def test_nothing_about_content_or_file_names_reaches_the_output_streams_or_the_file(tmp_path: Path) -> None:
    queue, folder, output = setup(tmp_path)
    done = run(queue, folder, output)
    text = done.stdout + "\n" + done.stderr + "\n" + (output / "digests.json").read_text()
    check([name for name in NAMES if name in text] == [], "no name, file name or folder name appears")
    check("inner.csv" not in text and "one.bin" not in text and "csv" not in text.replace("cms_owners_org", ""), "no file name appears")
    check(done.stdout.strip() == "2 private units: 2 read, 0 missing, 0 unreadable", f"it prints counts only: {done.stdout!r}")
    check(stat.S_IMODE((output / "digests.json").stat().st_mode) == 0o600, "the file is owner-only")


def test_an_unreadable_file_a_missing_unit_and_a_link_are_recorded_by_unit_id_only(tmp_path: Path) -> None:
    queue, folder, output = setup(tmp_path)
    locked = folder / "OWNERS__1" / f"{NAMES[0]}.csv"
    locked.chmod(0)
    (folder / "FLAG__2" / "link").symlink_to(folder / f"{NAMES[1]} not in the queue" / "x.csv")
    try:
        done = run(queue, folder, output)
    finally:
        locked.chmod(0o600)
    record = json.loads((output / "digests.json").read_text())
    check(record["units"]["OWNERS__1"] == {"status": "unreadable"} and record["units"]["FLAG__2"] == {"status": "refused"}, f"statuses only: {record['units']}")
    text = done.stdout + done.stderr + (output / "digests.json").read_text()
    check([name for name in NAMES if name in text] == [], "no name reached anything on failure")
    check(done.returncode == 0, "the run itself succeeds")


def test_a_missing_unit_is_recorded_as_missing(tmp_path: Path) -> None:
    queue, folder, output = setup(tmp_path)
    (folder / "OWNERS__1").rename(folder / "moved")
    for child in sorted((folder / "moved").rglob("*"), reverse=True):
        child.chmod(0o700)
    done = run(queue, folder, output)
    check(json.loads((output / "digests.json").read_text())["units"]["OWNERS__1"] == {"status": "missing"}, "missing")
    check(done.stdout.strip() == "2 private units: 1 read, 1 missing, 0 unreadable", done.stdout)


def test_an_output_folder_that_others_can_read_is_refused_before_anything_is_read(tmp_path: Path) -> None:
    queue, folder, output = setup(tmp_path)
    output.chmod(0o755)
    done = run(queue, folder, output)
    check(done.returncode == 2 and not (output / "digests.json").exists(), f"refused: {done.returncode}")
    check("owner-only" in done.stderr and not any(name in done.stderr for name in NAMES), "a fixed message")


def test_a_rerun_with_the_same_bytes_changes_nothing_and_changed_bytes_do_not_overwrite(tmp_path: Path) -> None:
    queue, folder, output = setup(tmp_path)
    run(queue, folder, output)
    before = (output / "digests.json").read_bytes()
    run(queue, folder, output)
    check((output / "digests.json").read_bytes() == before, "same bytes, same file")
    (folder / "FLAG__2" / "one.bin").write_bytes(b"changed")
    done = run(queue, folder, output)
    check(done.returncode == 3 and (output / "digests.json").read_bytes() == before, "a changed result is refused, never overwritten")


def test_the_script_parses_nothing_and_prints_nothing_itself() -> None:
    source = Path(private_digest.__file__).read_text()
    tree = ast.parse(source)
    imported = {alias.name.split(".")[0] for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names}
    imported |= {node.module.split(".")[0] for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) and node.module}
    check(
        imported <= {"__future__", "argparse", "hashlib", "json", "os", "stat", "sys", "pathlib", "collections", "typing", "scripts"},
        f"imports: {sorted(imported)}",
    )
    check(not any(isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "print" for node in ast.walk(tree)), "no print call")
    check("csv" not in imported and "zipfile" not in imported and "openpyxl" not in imported, "no parser is imported")


def fixed_failure(done: CompletedProcess[str], code: int) -> None:
    check(done.returncode == code, f"exit {done.returncode}, wanted {code}")
    lines = done.stderr.strip().splitlines()
    check(len(lines) == 1 and lines[0].startswith("private_digest: "), f"one fixed line, no traceback: {done.stderr!r}")
    check("Traceback" not in done.stderr and "Error" not in done.stderr and "/" not in done.stderr, "no traceback, exception text or path")
    check([name for name in NAMES if name in done.stdout + done.stderr] == [], "no synthetic name appears")


def test_a_missing_queue_whose_name_holds_a_person_gives_a_fixed_message(tmp_path: Path) -> None:
    queue, folder, output = setup(tmp_path)
    done = run(tmp_path / f"{NAMES[0]} queue.json", folder, output)
    fixed_failure(done, 4)
    check(not (output / "digests.json").exists(), "no output was written")


def test_a_missing_output_folder_below_a_person_named_folder_gives_a_fixed_message(tmp_path: Path) -> None:
    queue, folder, output = setup(tmp_path)
    done = run(queue, folder, tmp_path / NAMES[1] / "owner_only")
    fixed_failure(done, 5)


def test_a_missing_or_stale_classification_gives_a_fixed_message(tmp_path: Path) -> None:
    queue, folder, output = setup(tmp_path)
    queue.with_name("unit_classification.json").rename(tmp_path / f"{NAMES[2]}.json")
    fixed_failure(run(queue, folder, output), 4)
    (tmp_path / f"{NAMES[2]}.json").rename(queue.with_name("unit_classification.json"))
    queue.write_text(json.dumps({"run": 3, "queue": []}))
    fixed_failure(run(queue, folder, output), 4)


def test_an_unreadable_queue_or_output_gives_a_fixed_message(tmp_path: Path) -> None:
    queue, folder, output = setup(tmp_path)
    queue.chmod(0)
    try:
        fixed_failure(run(queue, folder, output), 4)
    finally:
        queue.chmod(0o600)
    (output / "digests.json").write_text("x")
    (output / "digests.json").chmod(0)
    try:
        fixed_failure(run(queue, folder, output), 5)
    finally:
        (output / "digests.json").chmod(0o600)


def test_a_proposed_personal_unit_is_read_and_a_unit_not_listed_as_personal_never_is(tmp_path: Path) -> None:
    queue, folder, output = setup(tmp_path)
    run(queue, folder, output)
    check(set(json.loads((output / "digests.json").read_text())["units"]) == {"OWNERS__1", "FLAG__2"}, "the proposed and the reviewed personal unit are read")


def test_a_sealed_extra_key_outside_the_queue_reads_nothing_and_leaves_no_name(tmp_path: Path) -> None:
    queue, folder, output = setup(tmp_path)
    (tmp_path / f"{NAMES[0]}.csv").write_bytes(NAMES[0].encode())
    path = queue.with_name("unit_classification.json")
    document = json.loads(path.read_text())
    document["units"][f"../{NAMES[0]}.csv"] = {"classification": "personal", "status": "proposed", "source": "synthetic"}
    path.write_text(json.dumps(document))
    unit_classification.seal(path)
    done = run(queue, folder, output)
    fixed_failure(done, 4)
    check(not (output / "digests.json").exists(), "nothing was written")
