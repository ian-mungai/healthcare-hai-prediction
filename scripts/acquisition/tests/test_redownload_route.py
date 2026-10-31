from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from scripts.acquisition import redownload, unused_files
from scripts.acquisition import redownload_route as route
from scripts.acquisition.s3_store import encoded_json
from scripts.acquisition.tests.held import classification_body
from tests.support import check


def unit(snapshot: str, disposition: str = "direct_download", host: str = "data.example.gov") -> dict:
    return {"snapshot_id": snapshot, "disposition": disposition, "url": f"https://{host}/{snapshot}.csv"}


def write_state(folder: Path, units: list[dict], removed: list[str]) -> dict[str, Path]:
    queue = folder / "queue.json"
    body = json.dumps({"queue": units}).encode()
    queue.write_bytes(body)
    lock = folder / "queue.lock.json"
    lock.write_text(json.dumps({"queue_sha256": hashlib.sha256(body).hexdigest()}))
    folder.joinpath("unit_classification.json").write_bytes(classification_body(body, units))
    plan_body = {
        "kind": "unused_file_removal_plan",
        "bucket": "example-bucket",
        "items": [{"snapshot_id": name, "s3_key": f"raw/{name}", "version_id": f"v-{name}"} for name in removed],
    }
    plan = folder / "removal_plan.json"
    plan.write_bytes(encoded_json({**plan_body, "plan_sha256": hashlib.sha256(encoded_json(plan_body)).hexdigest()}))
    confirmation = folder / "removal_confirmation.json"
    unused_files.write_confirmation_template(plan, confirmation)
    document = json.loads(confirmation.read_text())
    document["confirmed_by"] = "owner"
    confirmation.write_text(json.dumps(document))
    hosts = folder / "publisher_hosts.json"
    route.derive_hosts(queue, hosts)
    hosts_approval = folder / "hosts_approval.json"
    hosts_approval.write_text(json.dumps({"hosts_sha256": json.loads(hosts.read_text())["hosts_sha256"], "approved_by": "owner"}))
    return {"queue": queue, "plan": plan, "confirmation": confirmation, "hosts": hosts, "hosts_approval": hosts_approval}


def problems(paths: dict[str, Path], root: str = "raw/") -> list[str]:
    return route.check(paths["queue"], paths["plan"], paths["confirmation"], root, paths["hosts"], paths["hosts_approval"])


def test_a_ready_route_has_no_problem(tmp_path: Path) -> None:
    paths = write_state(tmp_path, [unit("S__1"), unit("S__2", "not_used_by_bronze")], ["S__9"])
    check(problems(paths) == [], "ready route has no problem")


def test_removal_must_be_confirmed_before_run_3(tmp_path: Path) -> None:
    paths = write_state(tmp_path, [unit("S__1")], ["S__9"])
    paths["confirmation"].unlink()
    check(any("removal confirmation" in item for item in problems(paths)), "a missing confirmation blocks run 3")


def test_the_confirmation_must_belong_to_the_plan(tmp_path: Path) -> None:
    paths = write_state(tmp_path, [unit("S__1")], ["S__9"])
    rewrite(paths, {"plan_sha256": "0" * 64})
    check(any("another plan" in item for item in problems(paths)), "a confirmation of another plan blocks run 3")


def test_a_removed_file_cannot_still_be_a_run_3_unit(tmp_path: Path) -> None:
    paths = write_state(tmp_path, [unit("S__1"), unit("S__9")], ["S__9"])
    check(any("S__9" in item and "queue" in item for item in problems(paths)), "a unit the plan removes blocks run 3")


def test_the_capture_root_must_be_raw(tmp_path: Path) -> None:
    paths = write_state(tmp_path, [unit("S__1")], [])
    check(any("raw/" in item for item in problems(paths, root="")), "root layout blocks run 3")


def test_a_queue_that_differs_from_its_lock_blocks_run_3(tmp_path: Path) -> None:
    paths = write_state(tmp_path, [unit("S__1")], [])
    paths["queue"].write_text(json.dumps({"queue": [unit("S__1"), unit("S__2")]}))
    check(any("lock" in item for item in problems(paths)), "queue changed after its lock blocks run 3")


def test_the_command_exits_with_the_problems(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    paths = write_state(tmp_path, [unit("S__1")], [])
    paths["confirmation"].unlink()
    arguments = ["check"]
    for name in ("queue", "plan", "confirmation", "hosts", "hosts_approval"):
        arguments += [f"--{name.replace('_', '-')}", str(paths[name])]
    with pytest.raises(SystemExit):
        route.main(arguments, root="raw/")
    check("removal confirmation" in capsys.readouterr().err, "problem is printed")


def test_the_hosts_file_lists_every_active_queue_host_and_the_signed_redirect_host(tmp_path: Path) -> None:
    redirect_source = next(iter(route.SIGNED_REDIRECT_ROUTES))
    units = [unit("S__1", host="a.example.gov"), unit("S__2", host="b.example.gov"), unit("S__3", "not_used_by_bronze", host="skipped.example.gov")]
    units.append({"snapshot_id": "S__4", "disposition": "api", "url": redirect_source})
    paths = write_state(tmp_path, units, [])
    hosts = json.loads(paths["hosts"].read_text())["hosts"]
    check("skipped.example.gov" not in hosts, "an inactive unit adds no host")
    check({"a.example.gov", "b.example.gov", "s3.amazonaws.com"} <= set(hosts), "queue hosts and the signed redirect host are listed")


def test_a_queue_host_missing_from_the_approved_file_blocks_run_3(tmp_path: Path) -> None:
    paths = write_state(tmp_path, [unit("S__1", host="a.example.gov")], [])
    document = json.loads(paths["hosts"].read_text())
    document["hosts"] = ["other.example.gov"]
    document["hosts_sha256"] = route.hosts_digest(document["hosts"])
    paths["hosts"].write_text(json.dumps(document))
    paths["hosts_approval"].write_text(json.dumps({"hosts_sha256": document["hosts_sha256"], "approved_by": "owner"}))
    check(any("a.example.gov" in item and "approved hosts" in item for item in problems(paths)), "a host off the file blocks run 3")


@pytest.mark.parametrize("damage", ["not json", "missing hosts", "changed list", "no approval", "other approval"])
def test_a_damaged_hosts_file_or_approval_blocks_run_3(tmp_path: Path, damage: str) -> None:
    paths = write_state(tmp_path, [unit("S__1")], [])
    document = json.loads(paths["hosts"].read_text())
    if damage == "not json":
        paths["hosts"].write_text("{")
    elif damage == "missing hosts":
        paths["hosts"].write_text(json.dumps({"queue_sha256": document["queue_sha256"]}))
    elif damage == "changed list":
        paths["hosts"].write_text(json.dumps({**document, "hosts": [*document["hosts"], "new.example.gov"]}))
    elif damage == "no approval":
        paths["hosts_approval"].unlink()
    else:
        paths["hosts_approval"].write_text(json.dumps({"hosts_sha256": "0" * 64, "approved_by": "owner"}))
    check(any("hosts" in item for item in problems(paths)), f"{damage} blocks run 3")


def test_a_hosts_file_made_for_another_queue_blocks_run_3(tmp_path: Path) -> None:
    paths = write_state(tmp_path, [unit("S__1")], [])
    document = json.loads(paths["hosts"].read_text())
    paths["hosts"].write_text(json.dumps({**document, "queue_sha256": "0" * 64}))
    check(any("another queue" in item for item in problems(paths)), "a hosts file of another queue blocks run 3")


def rewrite(paths: dict[str, Path], change: dict) -> None:
    document = json.loads(paths["confirmation"].read_text())
    paths["confirmation"].write_text(json.dumps({**document, **change}))


def test_an_unsigned_template_is_not_a_confirmation(tmp_path: Path) -> None:
    paths = write_state(tmp_path, [unit("S__1")], ["S__9"])
    unused_files.write_confirmation_template(paths["plan"], tmp_path / "template.json")
    paths["confirmation"].write_text((tmp_path / "template.json").read_text())
    check(any("confirming person" in item for item in problems(paths)), "a template with no confirmer blocks run 3")


def test_a_planned_key_that_is_not_confirmed_blocks_run_3(tmp_path: Path) -> None:
    paths = write_state(tmp_path, [unit("S__1")], ["S__8", "S__9"])
    document = json.loads(paths["confirmation"].read_text())
    rewrite(paths, {"keys": document["keys"][:1]})
    found = problems(paths)
    check(any("does not confirm 1 planned key" in item for item in found), f"a missing key blocks run 3: {found}")
    rewrite(paths, {"keys": []})
    check(any("does not confirm 2 planned key" in item for item in problems(paths)), "an empty list blocks run 3")


def test_a_confirmation_that_names_a_key_the_plan_does_not_hold_or_repeats_one_blocks_run_3(tmp_path: Path) -> None:
    paths = write_state(tmp_path, [unit("S__1")], ["S__9"])
    document = json.loads(paths["confirmation"].read_text())
    rewrite(paths, {"keys": [*document["keys"], "raw/S__other"]})
    check(any("does not hold" in item for item in problems(paths)), "a key outside the plan blocks run 3")
    rewrite(paths, {"keys": [*document["keys"], *document["keys"]]})
    check(any("repeats" in item for item in problems(paths)), "a repeated key blocks run 3")
    rewrite(paths, {"keys": "raw/S__9"})
    check(any("unusable" in item for item in problems(paths)), "a malformed list blocks run 3")


def test_a_confirmation_for_another_bucket_blocks_run_3(tmp_path: Path) -> None:
    paths = write_state(tmp_path, [unit("S__1")], ["S__9"])
    rewrite(paths, {"bucket": "another-bucket"})
    check(any("bucket" in item for item in problems(paths)), "another bucket blocks run 3")


def test_a_plan_changed_after_it_was_written_blocks_run_3(tmp_path: Path) -> None:
    paths = write_state(tmp_path, [unit("S__1")], ["S__9"])
    document = json.loads(paths["plan"].read_text())
    document["items"] = []
    paths["plan"].write_text(json.dumps(document))
    check(any("changed after it was written" in item for item in problems(paths)), "an edited plan blocks run 3")


def test_a_plan_with_nothing_to_remove_needs_a_confirmation_of_no_keys(tmp_path: Path) -> None:
    paths = write_state(tmp_path, [unit("S__1")], [])
    check(problems(paths) == [], "an empty plan and its empty confirmation pass")
    paths["confirmation"].unlink()
    check(any("removal confirmation" in item for item in problems(paths)), "even an empty plan needs its confirmation record")


def execution_run(folder: Path, number: int, files: route.RouteFiles | None = None) -> redownload.Run:
    queue = folder / "queue.json"
    body = json.dumps({"run": number, "queue": []}).encode()
    queue.write_bytes(body)
    queue.with_name("queue.lock.json").write_text(json.dumps({"queue_sha256": hashlib.sha256(body).hexdigest(), "entries": 0}))
    queue.with_name("unit_classification.json").write_bytes(classification_body(body, []))
    return redownload.Run(
        queue_path=queue, state_root=folder / "run", base=folder / "base", route=files, stored_root=folder / "mirror" if number == 3 else None
    )


def test_executing_run_3_without_route_files_is_refused_before_the_review_gate(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="route files"):
        redownload.execute(execution_run(tmp_path, 3), allow_network=True)


def test_executing_run_3_with_an_incomplete_route_is_refused_with_the_gate_problems(tmp_path: Path) -> None:
    (tmp_path / "route").mkdir()
    paths = write_state(tmp_path / "route", [unit("S__1")], ["S__9"])
    paths["confirmation"].unlink()
    files = route.RouteFiles(paths["plan"], paths["confirmation"], paths["hosts"], paths["hosts_approval"])
    with pytest.raises(ValueError, match="removal confirmation"):
        redownload.execute(execution_run(tmp_path, 3, files), allow_network=True)


def test_other_runs_do_not_need_the_route_gate(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="independent review"):
        redownload.execute(execution_run(tmp_path, 2), allow_network=True)
