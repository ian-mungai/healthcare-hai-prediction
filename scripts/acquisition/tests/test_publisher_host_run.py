"""The publisher job's entry point on the host: it builds its workspace from the tracked inputs in the code archive, checks them and runs capture mode.

Every case runs ``publisher_run.main`` over a code root that holds a synthetic ``config/acquisition/baseline_run/`` bundle with its lock and a synthetic
bootstrap seed folder, with the bucket, the identity call and the publisher replaced by the stand-ins of the capture mode cases. No AWS call and no request
leaves the machine.
"""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any

import pytest

from scripts.acquisition import capture_mode, check_baseline_run, collection_layout, publisher_run, s3_store, transport
from scripts.acquisition import completion_record as record
from scripts.acquisition.tests.test_capture_mode import Bucket, World
from scripts.acquisition.tests.test_task_keys import ACCOUNT, ROLE, TASK_IDENTITY
from scripts.acquisition.tests.test_task_owners import BUCKET, opener
from tests.support import check

RUN_ID = "20261113T100000Z-0a1b2c3d"
CODE_SHA = "c" * 64
TRACKED_INPUT = "data/acquisition_planning/tracked_plan_input.json"
SEED_FILES = {
    "mmd_browser_history/mmd_data.csv": ("data/datasets/historical_acquisition/mmd_browser_history/d1/captures/MMD/c1/raw/mmd_data.csv", b"id,value\n1,2\n"),
    "mmd_references/countynames.tsv": ("data/e2e/mmd_api_comparison/20260925/references/countynames.tsv", b"fips\tname\n01001\tAutauga\n"),
    "structure_history/county_adjacency2010.txt": (
        "data/datasets/historical_acquisition/structure_history/jobs/a/captures/ADJ/b/raw/county_adjacency2010.txt",
        b"\xe9\n",
    ),
}


@pytest.fixture(autouse=True)
def isolated_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """The capture mode cases' isolation: the real collection routes, no approved hosts file left in the environment and the raw root."""
    monkeypatch.setattr(s3_store, "load_routes", collection_layout.load_routes)
    for name in (transport.APPROVED_HOSTS_VARIABLE, transport.APPROVED_HOSTS_APPROVAL_VARIABLE):
        monkeypatch.setenv(name, "placeholder")
        monkeypatch.delenv(name)
    monkeypatch.setenv("HAI_CAPTURE_ROOT", "raw/")


POINTER = b"version https://git-lfs.github.com/spec/v1\noid sha256:" + b"a" * 64 + b"\nsize 14\n"


class Cloud:
    """The identity call and the bucket behind one client factory. ``calls`` names every service a client was made for."""

    def __init__(self, bucket: Bucket) -> None:
        self.bucket = bucket
        self.calls: list[str] = []

    def factory(self, service: str, _region: str) -> Any:
        self.calls.append(service)
        return self if service == "sts" else self.bucket

    def get_caller_identity(self) -> dict:
        return TASK_IDENTITY


class Host:
    """A code root with the tracked bundle, its lock and the seed folder, a bucket, a publisher stand-in and the task environment."""

    def __init__(self, tmp_path: Path, names: list[str]) -> None:
        self.code = tmp_path / "code"
        self.bundle = self.code / "config/acquisition/baseline_run"
        self.seed = self.code / "config/acquisition/bootstrap_seed"
        self.bundle.mkdir(parents=True)
        self.seed.mkdir(parents=True)
        self.workspace = tmp_path / "workspace"
        self.world = World(self.bundle, names)
        (self.bundle / "hosts.json").rename(self.bundle / "publisher_hosts_scope.json")
        tracked = self.bundle / "plan_inputs" / TRACKED_INPUT
        tracked.parent.mkdir(parents=True)
        tracked.write_bytes(b'{"tracked": true}\n')
        expected = sorted([TRACKED_INPUT, *(plan for plan, _ in SEED_FILES.values())])
        (self.bundle / "expected_plan_inputs.json").write_text(json.dumps({"expected_plan_inputs": expected}))
        for path, (_, body) in SEED_FILES.items():
            (self.seed / path).parent.mkdir(parents=True, exist_ok=True)
            (self.seed / path).write_bytes(body)
        self.write_manifest()
        self.relock()
        self.cloud = Cloud(self.world.bucket)

    def write_manifest(self, **changes: Any) -> None:
        queue = hashlib.sha256((self.bundle / "queue.json").read_bytes()).hexdigest()
        files = [{"path": path, "plan_input": plan, "sha256": hashlib.sha256(body).hexdigest()} for path, (plan, body) in SEED_FILES.items()] + [
            {"path": "manual_units/HUD_XLSX__x/crosswalk.csv", "unit": "HUD_XLSX__x", "sha256": None}
        ]
        document = {"kind": "bootstrap_seed_manifest", "queue_sha256": queue, "files": files} | changes
        (self.seed / "manifest.json").write_text(json.dumps(document))

    def relock(self) -> None:
        (self.bundle / "inputs.lock.json").write_bytes(check_baseline_run.build_lock(self.bundle))

    def environment(self, **changes: str) -> dict[str, str]:
        return {
            "RUN_ID": RUN_ID,
            "CODE_SHA256": CODE_SHA,
            "PUBLISHER_RUN_KIND": "bootstrap",
            "HAI_CAPTURE_ROOT": "raw/",
            "AWS_REGION": "us-west-2",
            "EXPECTED_ACCOUNT_ID": ACCOUNT,
            "TASK_ROLE_NAME": ROLE,
            "DATA_BUCKET_NAME": BUCKET,
            "PUBLISHER_WORKSPACE": str(self.workspace),
        } | changes

    def run(self, monkeypatch: pytest.MonkeyPatch, **changes: str) -> None:
        monkeypatch.chdir(self.code)
        self.world.site.scratch = self.workspace / "scratch"
        publisher_run.main(self.environment(**changes), self.cloud.factory, opener(self.world.site))

    def untouched(self) -> bool:
        return self.cloud.calls == [] and not self.world.bucket.versions and not self.world.site.asked


@pytest.fixture
def host(tmp_path: Path) -> Host:
    return Host(tmp_path, ["a.csv", "b.csv"])


def refused(monkeypatch: pytest.MonkeyPatch, host: Host, match: str, **changes: str) -> str:
    with pytest.raises(publisher_run.PublisherError, match=match) as caught:
        host.run(monkeypatch, **changes)
    check(host.untouched(), "the refusal came before any cloud call, any write and any request")
    return str(caught.value)


def test_the_host_run_captures_from_the_tracked_bundle_and_a_second_run_sends_nothing(host: Host, monkeypatch: pytest.MonkeyPatch) -> None:
    host.run(monkeypatch)
    asked = len(host.world.site.asked)
    check(asked == 2, f"each of the two units is requested once: {asked}")
    first = json.loads(host.world.bucket.objects[record.run_record_key("raw/", RUN_ID)][1])
    check(first["code_sha256"] == CODE_SHA and first["recheck"] is False and first["root"] == "raw/", f"the run record names the code and the kind: {first}")
    check(first["bundle_sha256"] == json.loads((host.bundle / "inputs.lock.json").read_text())["bundle_sha256"], "the run record names the inputs bundle")
    check(len(first["units"]["completed"]) == 2, "both units completed")
    host.run(monkeypatch, RUN_ID="20261113T110000Z-0a1b2c3e")
    check(len(host.world.site.asked) == asked, "a second bootstrap run skips every unit with a completion record and sends no request")


def test_a_monthly_run_compares_every_unit_again(host: Host, monkeypatch: pytest.MonkeyPatch) -> None:
    host.run(monkeypatch)
    asked = len(host.world.site.asked)
    host.run(monkeypatch, RUN_ID="20261113T110000Z-0a1b2c3e", PUBLISHER_RUN_KIND="monthly")
    check(len(host.world.site.asked) == asked + 2, "a monthly run requests each unit again")
    second = json.loads(host.world.bucket.objects[record.run_record_key("raw/", "20261113T110000Z-0a1b2c3e")][1])
    check(second["recheck"] is True and len(second["units"]["unchanged"]) == 2, f"it finds both payloads unchanged: {second['units']}")


def test_the_tracked_plan_inputs_and_the_seed_files_are_installed_as_real_files_in_the_code_root(host: Host, monkeypatch: pytest.MonkeyPatch) -> None:
    host.run(monkeypatch)
    check((host.code / TRACKED_INPUT).read_bytes() == b'{"tracked": true}\n', "a tracked plan input is copied to its path")
    for path, (plan, body) in SEED_FILES.items():
        target = host.code / plan
        check(target.is_file() and not target.is_symlink() and target.read_bytes() == body, f"{path} sits at its plan input path")
    check(
        (host.seed / "mmd_data.csv").exists() is False and (host.seed / "mmd_browser_history/mmd_data.csv").is_file(), "the seed folder itself is not changed"
    )


@pytest.mark.parametrize(
    "name", ["PUBLISHER_INPUTS_SHA256", "PUBLISHER_QUEUE_SHA256", "PUBLISHER_PRIOR_STATE", "PUBLISHER_RESUME_SOURCES", "PUBLISHER_REQUEST_SHA256"]
)
def test_a_launch_that_names_an_inputs_archive_or_a_prior_state_is_refused(host: Host, monkeypatch: pytest.MonkeyPatch, name: str) -> None:
    text = refused(monkeypatch, host, "inputs archive or a prior state", **{name: "a" * 64})
    check(name not in text and "a" * 64 not in text, f"the refusal names neither the variable's value nor a path: {text}")


@pytest.mark.parametrize("missing", ["RUN_ID", "CODE_SHA256", "PUBLISHER_RUN_KIND", "HAI_CAPTURE_ROOT", "DATA_BUCKET_NAME", "TASK_ROLE_NAME"])
def test_a_missing_variable_stops_the_job_before_any_call(host: Host, monkeypatch: pytest.MonkeyPatch, missing: str) -> None:
    monkeypatch.chdir(host.code)
    environment = host.environment()
    del environment[missing]
    with pytest.raises(publisher_run.PublisherError, match=missing):
        publisher_run.main(environment, host.cloud.factory, opener(host.world.site))
    check(host.untouched(), "nothing was called")


@pytest.mark.parametrize(
    ("changes", "match"),
    [
        ({"PUBLISHER_RUN_KIND": "retry"}, "run kind"),
        ({"PUBLISHER_RUN_KIND": ""}, "PUBLISHER_RUN_KIND"),
        ({"HAI_CAPTURE_ROOT": "raw"}, "capture root"),
        ({"HAI_CAPTURE_ROOT": "lakehouse/"}, "capture root"),
        ({"HAI_CAPTURE_ROOT": "ingestion_test/t1/"}, "capture root"),
        ({"HAI_CAPTURE_ROOT": "raw/\n"}, "capture root"),
    ],
)
def test_a_run_kind_or_capture_root_the_launch_does_not_list_is_refused(
    host: Host, monkeypatch: pytest.MonkeyPatch, changes: dict[str, str], match: str
) -> None:
    refused(monkeypatch, host, match, **changes)


def test_a_changed_tracked_file_stops_the_run_before_any_request(host: Host, monkeypatch: pytest.MonkeyPatch) -> None:
    (host.bundle / "plan_inputs" / TRACKED_INPUT).write_bytes(b'{"tracked": false}\n')
    text = refused(monkeypatch, host, "tracked inputs differ from their lock")
    check("plan_inputs/" + TRACKED_INPUT in text, f"the changed file is named: {text}")


def test_a_file_the_lock_does_not_list_or_that_is_gone_stops_the_run(host: Host, monkeypatch: pytest.MonkeyPatch) -> None:
    (host.bundle / "extra.json").write_text("{}")
    refused(monkeypatch, host, "extra.json is not in the lock")
    (host.bundle / "extra.json").unlink()
    (host.bundle / "required_secrets.json").write_text("{}")
    host.relock()
    (host.bundle / "required_secrets.json").unlink()
    refused(monkeypatch, host, "required_secrets.json is missing")


def test_a_missing_or_unreadable_lock_stops_the_run(host: Host, monkeypatch: pytest.MonkeyPatch) -> None:
    (host.bundle / "inputs.lock.json").write_text("{not json")
    refused(monkeypatch, host, "tracked inputs differ from their lock")
    (host.bundle / "inputs.lock.json").unlink()
    refused(monkeypatch, host, "tracked inputs differ from their lock")


def test_a_lock_that_agrees_with_changed_files_but_not_with_its_own_digest_stops_the_run(host: Host, monkeypatch: pytest.MonkeyPatch) -> None:
    lock = json.loads((host.bundle / "inputs.lock.json").read_text())
    lock["bundle_sha256"] = "0" * 64
    (host.bundle / "inputs.lock.json").write_text(json.dumps(lock))
    refused(monkeypatch, host, "bundle digest differs")


def test_a_missing_seed_file_stops_the_run_and_names_it(host: Host, monkeypatch: pytest.MonkeyPatch) -> None:
    (host.seed / "mmd_references/countynames.tsv").unlink()
    text = refused(monkeypatch, host, "mmd_references/countynames.tsv is missing")
    check("mmd_data.csv" not in text, f"only the missing file is named: {text}")


def test_an_unresolved_lfs_pointer_stops_the_run_and_names_the_file(host: Host, monkeypatch: pytest.MonkeyPatch) -> None:
    (host.seed / "mmd_browser_history/mmd_data.csv").write_bytes(POINTER)
    refused(monkeypatch, host, "mmd_browser_history/mmd_data.csv is an unresolved LFS pointer")


def test_a_seed_file_that_differs_from_its_manifest_digest_stops_the_run(host: Host, monkeypatch: pytest.MonkeyPatch) -> None:
    (host.seed / "structure_history/county_adjacency2010.txt").write_bytes(b"\xe9 changed\n")
    refused(monkeypatch, host, "structure_history/county_adjacency2010.txt differs from the manifest digest")


def test_a_seed_entry_without_a_digest_or_with_another_size_stops_the_run(host: Host, monkeypatch: pytest.MonkeyPatch) -> None:
    document = json.loads((host.seed / "manifest.json").read_text())
    document["files"][2]["sha256"] = None
    (host.seed / "manifest.json").write_text(json.dumps(document))
    refused(monkeypatch, host, "county_adjacency2010.txt has no recorded digest")
    host.write_manifest()
    document = json.loads((host.seed / "manifest.json").read_text())
    document["files"][0]["bytes"] = 5
    (host.seed / "manifest.json").write_text(json.dumps(document))
    refused(monkeypatch, host, "mmd_data.csv differs from the manifest size")
    document["files"][0]["bytes"] = len(SEED_FILES["mmd_browser_history/mmd_data.csv"][1])
    (host.seed / "manifest.json").write_text(json.dumps(document))
    host.run(monkeypatch)


def test_every_problem_of_the_seed_is_named_at_once(host: Host, monkeypatch: pytest.MonkeyPatch) -> None:
    (host.seed / "mmd_references/countynames.tsv").unlink()
    (host.seed / "mmd_browser_history/mmd_data.csv").write_bytes(POINTER)
    (host.seed / "structure_history/county_adjacency2010.txt").write_bytes(b"other")
    text = refused(monkeypatch, host, "countynames.tsv is missing")
    check("mmd_data.csv is an unresolved LFS pointer" in text and "county_adjacency2010.txt differs" in text, f"all three files are named: {text}")


def test_a_seed_manifest_of_another_queue_or_another_kind_is_refused(host: Host, monkeypatch: pytest.MonkeyPatch) -> None:
    host.write_manifest(queue_sha256="0" * 64)
    refused(monkeypatch, host, "seed manifest belongs to another queue")
    host.write_manifest(kind="other")
    refused(monkeypatch, host, "not a bootstrap seed manifest")
    (host.seed / "manifest.json").write_text("{not json")
    refused(monkeypatch, host, "seed manifest is not usable")
    (host.seed / "manifest.json").unlink()
    refused(monkeypatch, host, "seed manifest is not usable")


@pytest.mark.parametrize("path", ["../escape.csv", "/absolute.csv", "a/../../b.csv", ""])
def test_a_seed_path_that_leaves_the_seed_folder_is_refused(host: Host, monkeypatch: pytest.MonkeyPatch, path: str) -> None:
    document = json.loads((host.seed / "manifest.json").read_text())
    document["files"][0]["path"] = path
    (host.seed / "manifest.json").write_text(json.dumps(document))
    refused(monkeypatch, host, "not a relative path inside the seed folder")


@pytest.mark.parametrize("plan_input", ["../outside.csv", "/etc/x", "notdata/x.csv", "data/../x.csv"])
def test_a_plan_input_path_that_leaves_the_data_folder_is_refused(host: Host, monkeypatch: pytest.MonkeyPatch, plan_input: str) -> None:
    document = json.loads((host.seed / "manifest.json").read_text())
    document["files"][0]["plan_input"] = plan_input
    (host.seed / "manifest.json").write_text(json.dumps(document))
    refused(monkeypatch, host, "plan input path")


def test_a_file_already_at_a_plan_input_path_with_other_bytes_is_refused(host: Host, monkeypatch: pytest.MonkeyPatch) -> None:
    plan = SEED_FILES["mmd_references/countynames.tsv"][0]
    (host.code / plan).parent.mkdir(parents=True)
    (host.code / plan).write_bytes(b"other")
    refused(monkeypatch, host, "already in the code root differs")


def test_an_expected_plan_input_with_no_home_stops_the_run_and_names_it(host: Host, monkeypatch: pytest.MonkeyPatch) -> None:
    document = json.loads((host.bundle / "expected_plan_inputs.json").read_text())
    document["expected_plan_inputs"].append("data/nowhere/missing.json")
    (host.bundle / "expected_plan_inputs.json").write_text(json.dumps(document))
    host.relock()
    refused(monkeypatch, host, "data/nowhere/missing.json is not in the code root")


def test_a_run_that_fails_the_evidence_check_stops_with_its_problems_counted(host: Host, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(capture_mode, "check_run", lambda *_arguments: ["a stand-in problem", "another"])
    with pytest.raises(publisher_run.PublisherError, match="evidence check found 2 problems"):
        host.run(monkeypatch)
    check(
        record.run_record_key("raw/", RUN_ID) in host.world.bucket.objects, "the run record is stored before the evidence check, so the failure stays on record"
    )


def test_a_run_with_a_failed_unit_ends_the_job_with_a_failure_through_the_evidence_check_and_keeps_its_record(
    host: Host, monkeypatch: pytest.MonkeyPatch
) -> None:
    host.world.site.bodies.pop(host.world.units[1]["url"])
    with pytest.raises(publisher_run.PublisherError, match="evidence check found 1 problem "):
        host.run(monkeypatch)
    run = json.loads(host.world.bucket.objects[record.run_record_key("raw/", RUN_ID)][1])
    check(len(run["units"]["completed"]) == 1 and len(run["units"]["failed"]) == 1, f"one unit completed and one failed: {run['units']}")


def test_the_job_reads_no_staged_archive_and_saves_no_state(host: Host, monkeypatch: pytest.MonkeyPatch) -> None:
    host.run(monkeypatch)
    keys = host.world.bucket.names()
    check(
        not any("run3_state" in key or "run3_runs" in key or "run_stage" in key for key in keys),
        f"no state, no earlier run record and no staged key: {sorted(keys)}",
    )
    check(not (host.workspace / "inputs").exists(), "no inputs archive is unpacked under the workspace")


def test_the_publisher_entry_point_runs_through_the_resource_peaks_recorder() -> None:
    """Like every job entry point, the publisher's ``__main__`` runs ``main`` through ``resource_peaks.recorded``, so its first host run logs its peaks."""
    import ast

    tree = ast.parse(Path(publisher_run.__file__).read_text(encoding="utf-8"))
    guards = [node for node in tree.body if isinstance(node, ast.If) and ast.unparse(node.test) == "__name__ == '__main__'"]
    check(len(guards) == 1, "one __main__ block")
    check([ast.unparse(statement) for statement in guards[0].body] == ["resource_peaks.recorded(main)"], "the __main__ block runs main through the recorder")


def test_a_failed_publisher_start_still_logs_one_resource_peaks_line(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from scripts import resource_peaks

    for name in publisher_run.REQUIRED:
        monkeypatch.delenv(name, raising=False)
    with pytest.raises(publisher_run.PublisherError):
        resource_peaks.recorded(publisher_run.main, scratch=(tmp_path,), interval=0.02)
    lines = [line for line in capsys.readouterr().err.splitlines() if line.startswith("event=resource_peaks ")]
    check(len(lines) == 1, f"one resource_peaks line: {lines}")
    check(json.loads(lines[0].removeprefix("event=resource_peaks "))["exit_code"] == 1, "the failed start is recorded with exit code 1")


def test_no_message_of_the_job_holds_a_digest_a_key_or_a_value(host: Host, monkeypatch: pytest.MonkeyPatch) -> None:
    (host.seed / "mmd_browser_history/mmd_data.csv").write_bytes(b"changed")
    with pytest.raises(publisher_run.PublisherError) as caught:
        host.run(monkeypatch)
    check(re.search(r"[0-9a-f]{40,}", str(caught.value)) is None, f"no digest in the message: {caught.value}")
