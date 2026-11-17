"""The publisher job's entry point on the host: it builds its workspace from the tracked inputs in the code archive, checks them and runs capture mode.

Every case runs ``publisher_run.main`` over a code root that holds a synthetic ``config/acquisition/baseline_run/`` bundle with its lock and a synthetic
bootstrap seed folder, with the bucket, the identity call and the publisher replaced by the stand-ins of the capture mode cases. No AWS call and no request
leaves the machine.
"""

from __future__ import annotations

import hashlib
import json
import re
import urllib.parse
from pathlib import Path
from typing import Any

import pytest

from scripts.acquisition import capture_mode, check_baseline_run, collection_layout, publisher_run, s3_store, transport
from scripts.acquisition import completion_record as record
from scripts.acquisition.tests.test_capture_mode import Bucket, World, csv_body
from scripts.acquisition.tests.test_task_keys import ACCOUNT, ROLE, TASK_IDENTITY
from scripts.acquisition.tests.test_task_owners import BUCKET, opener
from tests.support import check

RUN_ID = "ingestion_0a1b2c3d"
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
        self.links_of: Any = None

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
        publisher_run.main(self.environment(**changes), self.cloud.factory, opener(self.world.site), links_of=self.links_of)

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
    host.run(monkeypatch, RUN_ID="ingestion_0a1b2c3e")
    check(len(host.world.site.asked) == asked, "a second bootstrap run skips every unit with a completion record and sends no request")


def test_a_run_under_a_test_folder_writes_only_there_and_a_second_run_sends_nothing(host: Host, monkeypatch: pytest.MonkeyPatch) -> None:
    root = "ingestion_test/ingestion_0a1b2c3d/"
    monkeypatch.setenv("HAI_CAPTURE_ROOT", root)
    host.run(monkeypatch, HAI_CAPTURE_ROOT=root)
    keys = host.world.bucket.names()
    check(keys and all(key.startswith(root) for key in keys), f"every object sits under the test folder: {sorted(keys)[:5]}")
    first = json.loads(host.world.bucket.objects[record.run_record_key(root, RUN_ID)][1])
    check(first["root"] == root and len(first["units"]["completed"]) == 2, f"the run record sits under the test folder and names its root: {first['root']}")
    asked = len(host.world.site.asked)
    host.run(monkeypatch, HAI_CAPTURE_ROOT=root, RUN_ID="ingestion_0a1b2c3e")
    check(len(host.world.site.asked) == asked, "a second run under the same test folder skips every unit")


def test_a_launch_root_that_differs_from_the_storage_path_root_is_refused_before_any_request(host: Host, monkeypatch: pytest.MonkeyPatch) -> None:
    root = "ingestion_test/ingestion_0a1b2c3d/"
    with pytest.raises(Exception, match="differs from the capture root"):
        host.run(monkeypatch, HAI_CAPTURE_ROOT=root)
    check(not host.world.site.asked and not host.world.bucket.versions, "no request and no write happened")


def test_a_monthly_run_compares_every_unit_again(host: Host, monkeypatch: pytest.MonkeyPatch) -> None:
    host.run(monkeypatch)
    asked = len(host.world.site.asked)
    host.run(monkeypatch, RUN_ID="ingestion_0a1b2c3e", PUBLISHER_RUN_KIND="monthly")
    check(len(host.world.site.asked) == asked + 2, "a monthly run requests each unit again")
    second = json.loads(host.world.bucket.objects[record.run_record_key("raw/", "ingestion_0a1b2c3e")][1])
    check(second["recheck"] is True and len(second["units"]["unchanged"]) == 2, f"it finds both payloads unchanged: {second['units']}")


BOOTSTRAP_RUN = "ingestion_00000001"
MONTHLY_RUN = "ingestion_0a0a0a0a"
HTML = b"<html><body>Unavailable</body></html>"


def retry_changes(day: int, original: str = MONTHLY_RUN) -> dict[str, str]:
    """The environment of the retry of ``original`` on ``day``: its own run id, its kind, its original run and its day."""
    return {
        "RUN_ID": publisher_run.retry_run_id(original, day),
        "PUBLISHER_RUN_KIND": "retry",
        "PUBLISHER_ORIGINAL_RUN": original,
        "PUBLISHER_RETRY_DAY": str(day),
    }


def run_record_of(host: Host, run_id: str) -> dict[str, Any]:
    return dict(json.loads(host.world.bucket.objects[record.run_record_key("raw/", run_id)][1]))


def failed_month(host: Host, monkeypatch: pytest.MonkeyPatch, which: int = 1) -> str:
    """A bootstrap that captured every unit, then a monthly refresh in which one unit's publisher returns a page instead of data. Both units belong to one
    source, so a failure of the first pauses the second. Returns the URL of the failing unit."""
    host.run(monkeypatch, RUN_ID=BOOTSTRAP_RUN)
    url = str(host.world.units[which]["url"])
    host.world.bodies[url] = HTML
    with pytest.raises(publisher_run.PublisherError):
        host.run(monkeypatch, RUN_ID=MONTHLY_RUN, PUBLISHER_RUN_KIND="monthly")
    return url


def test_a_retry_recaptures_a_unit_whose_monthly_refresh_failed_over_an_older_capture(host: Host, monkeypatch: pytest.MonkeyPatch) -> None:
    url = failed_month(host, monkeypatch)
    failed = run_record_of(host, MONTHLY_RUN)["units"]["failed"]
    check(len(failed) == 1 and failed[0]["reason"] == "html_instead_of_data", f"the monthly refresh of the second unit failed: {failed}")
    asked = len(host.world.site.asked)
    host.world.bodies[url] = csv_body(999)
    host.run(monkeypatch, **retry_changes(1))
    check(len(host.world.site.asked) == asked + 1, "the retry asked for the failed unit only")
    units = run_record_of(host, retry_changes(1)["RUN_ID"])["units"]
    check([item["snapshot_id"] for item in units["changed"]] == [failed[0]["snapshot_id"]], f"the retry recaptured the failed unit: {units}")
    check(len(units["skipped_complete"]) == 1 and not units["failed"], f"the retry skipped the unit the monthly run completed: {units}")


def test_a_retry_also_takes_the_units_a_failure_paused_in_the_monthly_run(host: Host, monkeypatch: pytest.MonkeyPatch) -> None:
    url = failed_month(host, monkeypatch, which=0)
    paused = run_record_of(host, MONTHLY_RUN)["units"]["paused_not_attempted"]
    check(len(paused) == 1, f"the failure of the first unit paused the second: {paused}")
    asked = len(host.world.site.asked)
    host.world.bodies[url] = csv_body(999)
    host.run(monkeypatch, **retry_changes(1))
    check(len(host.world.site.asked) == asked + 2, "the retry did not ask for both the failed unit and the paused one")
    units = run_record_of(host, retry_changes(1)["RUN_ID"])["units"]
    check(len(units["changed"]) + len(units["unchanged"]) == 2 and not units["skipped_complete"], f"the retry took both units: {units}")


def test_the_next_retry_day_skips_the_unit_the_earlier_day_recovered(host: Host, monkeypatch: pytest.MonkeyPatch) -> None:
    url = failed_month(host, monkeypatch)
    host.world.bodies[url] = csv_body(999)
    host.run(monkeypatch, **retry_changes(1))
    asked = len(host.world.site.asked)
    host.run(monkeypatch, **retry_changes(2))
    check(len(host.world.site.asked) == asked, "day 2 asked for a unit that day 1 recovered")
    check(len(run_record_of(host, retry_changes(2)["RUN_ID"])["units"]["skipped_complete"]) == 2, "day 2 skipped both units")


def test_a_retry_that_fails_again_leaves_the_unit_for_the_next_day(host: Host, monkeypatch: pytest.MonkeyPatch) -> None:
    url = failed_month(host, monkeypatch)
    with pytest.raises(publisher_run.PublisherError):
        host.run(monkeypatch, **retry_changes(1))
    asked = len(host.world.site.asked)
    host.world.bodies[url] = csv_body(999)
    host.run(monkeypatch, **retry_changes(2))
    check(len(host.world.site.asked) == asked + 1, "day 2 did not ask again for the unit that failed on day 1")


def test_a_retry_after_a_clean_month_skips_every_unit_and_compares_nothing(host: Host, monkeypatch: pytest.MonkeyPatch) -> None:
    host.run(monkeypatch, RUN_ID=BOOTSTRAP_RUN)
    host.run(monkeypatch, RUN_ID=MONTHLY_RUN, PUBLISHER_RUN_KIND="monthly")
    asked = len(host.world.site.asked)
    host.run(monkeypatch, **retry_changes(1))
    check(len(host.world.site.asked) == asked, "a retry requested a unit that the monthly run refreshed")
    second = run_record_of(host, retry_changes(1)["RUN_ID"])
    check(second["recheck"] is False and len(second["units"]["skipped_complete"]) == 2, f"it skips both units: {second['units']}")


def test_a_retry_whose_original_run_has_no_record_is_refused_before_any_request(host: Host, monkeypatch: pytest.MonkeyPatch) -> None:
    host.run(monkeypatch, RUN_ID=BOOTSTRAP_RUN)
    asked = len(host.world.site.asked)
    with pytest.raises(publisher_run.PublisherError, match="original run"):
        host.run(monkeypatch, **retry_changes(1))
    check(len(host.world.site.asked) == asked, "a retry with no original run record asked for a unit")


@pytest.mark.parametrize(
    ("changes", "match"),
    [
        ({"PUBLISHER_ORIGINAL_RUN": ""}, "PUBLISHER_ORIGINAL_RUN"),
        ({"PUBLISHER_RETRY_DAY": ""}, "PUBLISHER_RETRY_DAY"),
        ({"PUBLISHER_RETRY_DAY": "0"}, "retry day"),
        ({"PUBLISHER_RETRY_DAY": "8"}, "retry day"),
        ({"PUBLISHER_RETRY_DAY": "1.5"}, "retry day"),
        ({"PUBLISHER_RETRY_DAY": "1\n"}, "retry day"),
        ({"PUBLISHER_ORIGINAL_RUN": "latest"}, "original run"),
        ({"RUN_ID": "ingestion_0a1b2c3e"}, "run ID of that day"),
    ],
)
def test_a_retry_that_does_not_name_a_valid_original_run_and_day_is_refused_before_any_call(
    host: Host, monkeypatch: pytest.MonkeyPatch, changes: dict[str, str], match: str
) -> None:
    refused(monkeypatch, host, match, **(retry_changes(1) | changes))


@pytest.mark.parametrize("kind", ["bootstrap", "monthly"])
@pytest.mark.parametrize("name", ["PUBLISHER_ORIGINAL_RUN", "PUBLISHER_RETRY_DAY"])
def test_a_run_that_is_not_a_retry_refuses_the_retry_variables_before_any_call(host: Host, monkeypatch: pytest.MonkeyPatch, kind: str, name: str) -> None:
    value = MONTHLY_RUN if name == "PUBLISHER_ORIGINAL_RUN" else "1"
    refused(monkeypatch, host, name, PUBLISHER_RUN_KIND=kind, **{name: value})


def listing_host(host: Host, pages: dict[str, Any]) -> str:
    """Give the code root a reviewed listing source on the host of the first unit, and the job a stand-in reader of its index pages. Returns the host name."""
    name = urllib.parse.urlsplit(str(host.world.units[0]["url"])).hostname or ""
    row = {
        "source_id": "NEWSRC",
        "index_urls": [f"https://{name}/index/"],
        "file_pattern": r"new_\d{4}\.csv",
        "role": "data",
        "mode": "file",
        "path_prefixes": ["/index/"],
    }
    (host.code / "config/acquisition/listing_sources.json").write_text(json.dumps({"sources": [row]}))

    def links_of(url: str) -> list[dict[str, str]]:
        found = pages[url]
        if isinstance(found, Exception):
            raise found
        return list(found)

    host.links_of = links_of
    return name


def report_of(host: Host, run_id: str = MONTHLY_RUN) -> dict[str, Any]:
    key = f"raw/ingestion_runs/{run_id}/plan_rebuild/report.json"
    return dict(json.loads(host.world.bucket.objects[key][1]))


def test_a_monthly_run_writes_a_hold_report_of_the_new_files_and_adds_nothing(host: Host, monkeypatch: pytest.MonkeyPatch) -> None:
    host.run(monkeypatch, RUN_ID=BOOTSTRAP_RUN)
    name = urllib.parse.urlsplit(str(host.world.units[0]["url"])).hostname
    new = f"https://{name}/index/new_2026.csv"
    listing_host(host, {f"https://{name}/index/": [{"url": new, "label": "new"}]})
    queue = (host.bundle / "queue.json").read_bytes()
    host.run(monkeypatch, RUN_ID=MONTHLY_RUN, PUBLISHER_RUN_KIND="monthly")
    found = report_of(host)
    check([item["url"] for item in found["held"]] == [new] and found["known"] == 0, f"the report names the new file: {found}")
    check(f"raw/ingestion_runs/{MONTHLY_RUN}/plan_rebuild/report.txt" in host.world.bucket.objects, "the report is also stored as text for the owner")
    check(new not in [str(item) for item in host.world.site.asked], "the new file was requested")
    check((host.bundle / "queue.json").read_bytes() == queue, "the tracked queue changed")
    check(len(run_record_of(host, MONTHLY_RUN)["units"]["unchanged"]) == 2, "the monthly run refreshed only the units of the queue")


def test_only_a_monthly_run_writes_the_hold_report(host: Host, monkeypatch: pytest.MonkeyPatch) -> None:
    name = urllib.parse.urlsplit(str(host.world.units[0]["url"])).hostname
    listing_host(host, {f"https://{name}/index/": [{"url": f"https://{name}/index/new_2026.csv", "label": "new"}]})
    host.run(monkeypatch, RUN_ID=BOOTSTRAP_RUN)
    check(not [key for key in host.world.bucket.objects if "plan_rebuild" in key], "a bootstrap run wrote a hold report")
    host.run(monkeypatch, RUN_ID=MONTHLY_RUN, PUBLISHER_RUN_KIND="monthly")
    host.run(monkeypatch, **retry_changes(1))
    reports = sorted(key for key in host.world.bucket.objects if "plan_rebuild" in key)
    check(all(f"/{MONTHLY_RUN}/" in key for key in reports) and len(reports) == 2, f"only the monthly run wrote its report: {reports}")


def test_an_index_page_that_cannot_be_read_is_named_in_the_report_and_never_fails_the_run(host: Host, monkeypatch: pytest.MonkeyPatch) -> None:
    host.run(monkeypatch, RUN_ID=BOOTSTRAP_RUN)
    name = urllib.parse.urlsplit(str(host.world.units[0]["url"])).hostname
    listing_host(host, {f"https://{name}/index/": OSError("down")})
    host.run(monkeypatch, RUN_ID=MONTHLY_RUN, PUBLISHER_RUN_KIND="monthly")
    check(report_of(host)["unreached"] == [{"source_id": "NEWSRC", "url": f"https://{name}/index/", "reason": "index_unreadable"}], f"{report_of(host)}")


def test_a_code_root_without_the_reviewed_sources_still_runs_and_says_no_report_was_written(
    host: Host, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    host.run(monkeypatch, RUN_ID=BOOTSTRAP_RUN)
    host.run(monkeypatch, RUN_ID=MONTHLY_RUN, PUBLISHER_RUN_KIND="monthly")
    check('"plan_rebuild": "not_written"' in capsys.readouterr().out, "the run did not say that no report was written")
    check(not [key for key in host.world.bucket.objects if "plan_rebuild" in key], "a report was written without sources")


def bootstrap_marker(root: str = "raw/", run_id: str = RUN_ID) -> str:
    return f"{root}ingestion_runs/{run_id}/bootstrap.json"


def test_a_bootstrap_run_that_passes_leaves_its_bootstrap_record(host: Host, monkeypatch: pytest.MonkeyPatch) -> None:
    host.run(monkeypatch)
    body = json.loads(host.world.bucket.objects[bootstrap_marker()][1])
    check(body == {"kind": "bootstrap_run", "run_id": RUN_ID, "code_sha256": CODE_SHA}, f"the bootstrap record names its run and code: {body}")


@pytest.mark.parametrize("kind", ["monthly", "retry"])
def test_a_monthly_or_retry_run_on_a_root_with_no_bootstrap_record_is_refused_before_any_request(
    host: Host, monkeypatch: pytest.MonkeyPatch, kind: str
) -> None:
    changes = retry_changes(1) if kind == "retry" else {"PUBLISHER_RUN_KIND": kind}
    with pytest.raises(publisher_run.PublisherError, match="no bootstrap run record"):
        host.run(monkeypatch, **changes)
    check(not host.world.site.asked and not host.world.bucket.versions, f"a {kind} run asked for a unit or wrote an object")


def test_a_bootstrap_record_of_another_root_does_not_open_a_test_root(host: Host, monkeypatch: pytest.MonkeyPatch) -> None:
    host.run(monkeypatch)
    root = "ingestion_test/ingestion_0a1b2c3d/"
    asked = len(host.world.site.asked)
    with pytest.raises(publisher_run.PublisherError, match="no bootstrap run record"):
        host.run(monkeypatch, HAI_CAPTURE_ROOT=root, RUN_ID="ingestion_0a1b2c3e", PUBLISHER_RUN_KIND="monthly")
    check(len(host.world.site.asked) == asked, "a monthly run under a root with no bootstrap record asked for a unit")


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
        ({"PUBLISHER_RUN_KIND": "weekly"}, "run kind"),
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


def test_a_refused_run_writes_nothing_into_the_code_root(host: Host, monkeypatch: pytest.MonkeyPatch) -> None:
    (host.seed / "mmd_references/countynames.tsv").unlink()
    refused(monkeypatch, host, "countynames.tsv is missing")
    check(not (host.code / "data").exists(), "neither a tracked plan input nor a seed file was copied before the check passed")
    document = json.loads((host.bundle / "expected_plan_inputs.json").read_text())
    document["expected_plan_inputs"].append("data/nowhere/missing.json")
    (host.bundle / "expected_plan_inputs.json").write_text(json.dumps(document))
    host.relock()
    (host.seed / "mmd_references/countynames.tsv").write_bytes(SEED_FILES["mmd_references/countynames.tsv"][1])
    refused(monkeypatch, host, "missing.json is not in the code root")
    check(not (host.code / "data").exists(), "an expected input without a home also stops the run before any copy")


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


def test_every_one_of_sixteen_unusable_seed_files_is_named(host: Host, monkeypatch: pytest.MonkeyPatch) -> None:
    document = json.loads((host.seed / "manifest.json").read_text())
    paths = [f"mmd_browser_history/seed{number:02d}.csv" for number in range(16)]
    document["files"] = [{"path": path, "plan_input": f"data/seeds/{number}.csv", "sha256": "a" * 64} for number, path in enumerate(paths)]
    (host.seed / "manifest.json").write_text(json.dumps(document))
    text = refused(monkeypatch, host, "seed00.csv is missing")
    check(all(f"{path} is missing" in text for path in paths), f"each of the sixteen files is named: {text}")
    check("more" not in text, f"the message does not cut the list: {text}")


def test_every_missing_expected_plan_input_is_named(host: Host, monkeypatch: pytest.MonkeyPatch) -> None:
    document = json.loads((host.bundle / "expected_plan_inputs.json").read_text())
    names = [f"data/nowhere/missing{number:02d}.json" for number in range(16)]
    document["expected_plan_inputs"] += names
    (host.bundle / "expected_plan_inputs.json").write_text(json.dumps(document))
    host.relock()
    text = refused(monkeypatch, host, "missing00.json is not in the code root")
    check(all(f"{name} is not in the code root" in text for name in names), f"each expected input is named: {text}")


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
