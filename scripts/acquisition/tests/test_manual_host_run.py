"""Manual units on the host run: the seed's release assets are downloaded and checked before any capture request, and every manual unit goes through the normal
capture path.

Every case runs ``publisher_run.main`` over a code root with a synthetic bundle and seed folder (the cases of ``test_publisher_host_run``), with a stand-in for
the GitHub release (the download address answers with one redirect to the release asset host), a bucket stand-in and a publisher stand-in. No request leaves the
machine and the real archives are never downloaded.
"""

from __future__ import annotations

import errno
import hashlib
import io
import json
import shutil
import socket
import ssl
import threading
import time
import urllib.request
import urllib.response
import zipfile
from email.message import Message
from pathlib import Path
from typing import Any, NamedTuple

import pytest

from scripts.acquisition import capture_mode, collection_layout, host_inputs, manual_content, publisher_run, run_manual_units_e2e, s3_store, transport
from scripts.acquisition import completion_record as record
from scripts.acquisition import redownload_route as route
from scripts.acquisition.tests.test_ahrq_manual_units import BASE, BODIES, XLSX_NAME, plan_of
from scripts.acquisition.tests.test_publisher_host_run import RUN_ID, Host
from scripts.acquisition.tests.test_task_owners import opener
from tests.support import check

COMMITTED = Path("config/acquisition/baseline_run")


@pytest.fixture(autouse=True)
def isolated_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """The host run cases' isolation: the real collection routes, no approved hosts file left in the environment and the raw root."""
    monkeypatch.setattr(s3_store, "load_routes", collection_layout.load_routes)
    for name in (transport.APPROVED_HOSTS_VARIABLE, transport.APPROVED_HOSTS_APPROVAL_VARIABLE):
        monkeypatch.setenv(name, "placeholder")
        monkeypatch.delenv(name)
    monkeypatch.setenv("HAI_CAPTURE_ROOT", "raw/")
    # The disk admission reads the free space of the real disk. A case that is not about disk space reports a roomy one, so the host's own free space
    # does not decide it.
    free_space(monkeypatch, 2**40)


TAG = "seed-test-20261121"
ASSET_HOST_URL = "https://release-assets.githubusercontent.com/github-production-release-asset/1/"
MEMBERS = 3


def committed_unit() -> tuple[dict, dict]:
    """The first committed CMS hospital archive unit and its specification, whose members, template and saved record the synthetic units copy."""
    queue = json.loads((COMMITTED / "queue.json").read_bytes())["queue"]
    specs = json.loads((COMMITTED / "capture_specs.json").read_text(encoding="utf-8"))["specs"]
    unit = next(item for item in queue if item["mode"] == "hospital_archive")
    return unit, specs[unit["snapshot_id"]]


def nested_zip(inner: dict[str, str | bytes], name: str = "inner.zip", outer: dict[str, str | bytes] | None = None) -> bytes:
    """A ZIP archive that holds one nested ZIP archive (and any plain members), as the republished annual archive holds its five quarterly archives."""
    inside = io.BytesIO()
    with zipfile.ZipFile(inside, "w", zipfile.ZIP_DEFLATED) as archive:
        for member, body in inner.items():
            archive.writestr(member, body)
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(name, inside.getvalue())
        for member, body in (outer or {}).items():
            archive.writestr(member, body)
    return buffer.getvalue()


def zip_of(members: dict[str, str | bytes]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for member, body in members.items():
            archive.writestr(member, body)
    return buffer.getvalue()


def declaring(body: bytes, size: int) -> bytes:
    """The same ZIP bytes with every member's declared expanded size in the central directory set to ``size``. The directory is all that a size check reads,
    so a few bytes stand for an archive of any declared size without writing it."""
    data = bytearray(body)
    at = data.find(b"PK\x01\x02")
    while at != -1:
        data[at + 24 : at + 28] = size.to_bytes(4, "little")
        at = data.find(b"PK\x01\x02", at + 4)
    return bytes(data)


def archive_of(number: int, padding: int = 0, nested: bool = False) -> tuple[str, bytes, list[dict]]:
    """A small ZIP archive named like a committed one, the members its specification lists, and the members' reviewed map.

    ``padding`` makes the first member that many bytes of one repeated character, which a deflated archive holds in a few bytes: its declared expanded size is
    large while the archive stays small, as the republished annual archive's is.
    """
    _, spec = committed_unit()
    members = spec["archive_members"][:MEMBERS]
    name = f"hospitals_2099-01-{number:02}.zip"
    if nested:
        first = spec["archive_members"][0]
        leaf = first["member_chain"][-1].rsplit("/", 1)[-1]
        return name, nested_zip({leaf: f"id,value\n{number},1\n"}, f"q{number:02}/inner.zip"), [first | {"member_chain": [f"q{number:02}/inner.zip", leaf]}]
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for index, item in enumerate(members):
            text = f"id,value\n{number},{len(item['dataset_id'])}\n"
            archive.writestr("/".join(item["member_chain"]), "0" * padding if padding and index == 0 else text)
    return name, buffer.getvalue(), members


def unit_and_spec(number: int, padding: int = 0, nested: bool = False) -> tuple[dict, dict, str, bytes]:
    unit, spec = committed_unit()
    name, body, members = archive_of(number, padding, nested)
    snapshot = f"main-hai-pdc__20990101T0000{number:02}Z__{number:032x}"
    url = unit["url"].rsplit("/", 1)[0] + "/" + name
    template = spec["receipt_template"] | {"expected_sha256": hashlib.sha256(body).hexdigest()}
    made = {
        "expected_format": "zip",
        "mode": "hospital_archive",
        "receipt_template": template,
        "saved": spec["saved"] | {"origin_url": url},
        "source_id": "main-hai-pdc",
        "stored_data_files": [name],
        "archive_members": members,
    }
    return unit | {"snapshot_id": snapshot, "url": url, "bytes": len(body)}, made, name, body


class Answer(urllib.response.addinfourl):
    msg = "OK"


class Release(urllib.request.HTTPSHandler):
    """The release stand-in: the download address answers with a redirect to the asset host, which serves the bytes. ``counts`` names each address asked."""

    def __init__(self, assets: dict[str, bytes], *, redirect_to: str | None = None) -> None:
        super().__init__()
        self.assets, self.redirect_to = dict(assets), redirect_to
        self.asked: list[str] = []

    def reply(self, request: Any, status: int, body: bytes = b"", **headers: str) -> Any:
        message = Message()
        message["Content-Type"] = "application/octet-stream"
        message["Content-Length"] = str(len(body))
        for name, value in headers.items():
            message[name.replace("_", "-")] = value
        return Answer(io.BytesIO(body), message, request.full_url, status)

    def https_open(self, request: Any) -> Any:
        url = request.full_url
        if url.startswith(transport.RELEASE_DOWNLOAD_PREFIX):
            self.asked.append(url)
            name = url.rsplit("/", 1)[-1]
            if name not in self.assets:
                return self.reply(request, 404)
            return self.reply(request, 302, Location=(self.redirect_to or ASSET_HOST_URL + name) + "?sp=r&sig=SECRET&jwt=SECRET")
        if url.startswith(ASSET_HOST_URL):
            self.asked.append(url.split("?")[0])
            return self.reply(request, 200, self.assets[url.split("?")[0].rsplit("/", 1)[-1]])
        return None


class ManualHost(Host):
    """The host of ``test_publisher_host_run`` with CMS hospital archive units added: their files are in no code archive, only in the stand-in release."""

    def __init__(self, tmp_path: Path, numbers: list[int], padding: int = 0, nested: bool = False) -> None:
        self.units = [unit_and_spec(number, padding, nested) for number in numbers]
        super().__init__(tmp_path, ["a.csv"])
        self.release = Release({name: body for _, _, name, body in self.units})
        specs = json.loads((self.bundle / "capture_specs.json").read_text(encoding="utf-8"))["specs"]
        self.world.units += [unit for unit, _, _, _ in self.units]
        self.world.write_inputs(specs | {unit["snapshot_id"]: spec for unit, spec, _, _ in self.units})
        (self.bundle / "hosts.json").rename(self.bundle / "publisher_hosts_scope.json")
        self.write_manifest()
        self.write_release()
        self.relock()

    def write_manifest(self, **changes: Any) -> None:
        super().write_manifest()
        manifest = json.loads((self.seed / "manifest.json").read_text(encoding="utf-8"))
        manifest["files"] += [
            {"path": f"manual_units/{unit['snapshot_id']}/{name}", "unit": unit["snapshot_id"], "sha256": hashlib.sha256(body).hexdigest()}
            for unit, _, name, body in self.units
        ]
        (self.seed / "manifest.json").write_text(json.dumps(manifest | changes))

    def write_release(self, **changes: Any) -> None:
        queue = hashlib.sha256((self.bundle / "queue.json").read_bytes()).hexdigest()
        assets = [
            {
                "name": name,
                "path": f"manual_units/{unit['snapshot_id']}/{name}",
                "bytes": len(body),
                "sha256": hashlib.sha256(body).hexdigest(),
                "url": f"{transport.RELEASE_DOWNLOAD_PREFIX}{TAG}/{name}",
            }
            for unit, _, name, body in self.units
        ]
        document = {"kind": host_inputs.RELEASE_KIND, "queue_sha256": queue, "tag": TAG, "assets": assets} | changes
        (self.seed / host_inputs.RELEASE_MANIFEST).write_text(json.dumps(document))

    def run(self, monkeypatch: pytest.MonkeyPatch, **changes: str) -> None:
        monkeypatch.chdir(self.code)
        self.world.site.scratch = self.workspace / "scratch"
        base = opener(self.world.site)
        stand_in = transport.ReviewedTransport(*base.handlers[:1], self.release, *base.handlers[1:])
        publisher_run.main(self.environment(**changes), self.cloud.factory, stand_in, links_of=self.links_of)

    def seeded(self) -> list[Path]:
        return sorted(self.seed.glob("manual_units/*/*.zip"))

    def run_record(self, run_id: str = RUN_ID) -> dict[str, Any]:
        return dict(json.loads(self.world.bucket.objects[record.run_record_key("raw/", run_id)][1]))


@pytest.fixture
def manual(tmp_path: Path) -> ManualHost:
    return ManualHost(tmp_path, [1, 2])


def test_the_hosts_file_of_a_queue_with_release_seeded_units_names_the_release_download_host_and_its_one_redirect_host(manual: ManualHost) -> None:
    document = json.loads((manual.bundle / "publisher_hosts_scope.json").read_text(encoding="utf-8"))
    check({"github.com", "release-assets.githubusercontent.com"} <= set(document["hosts"]), f"the release hosts are on the hosts file: {document['hosts']}")
    check("data.cms.gov" in document["hosts"], "the direct CMS units keep their host")
    only_manual = {"queue": [unit for unit, _, _, _ in manual.units]}
    check(route.queue_hosts(only_manual) == {"github.com", "release-assets.githubusercontent.com"}, "the seed-only archive units add only the release hosts")
    check(
        route.host_problems((manual.bundle / "queue.json").read_bytes(), manual.bundle / "publisher_hosts_scope.json", manual.bundle / "hosts_approval.json")
        == [],
        "approved",
    )


def evidence_problems(capsys: pytest.CaptureFixture[str]) -> int:
    """The number of problems the run's own evidence step reported on its counters line."""
    lines = [json.loads(line) for line in capsys.readouterr().out.splitlines() if line.startswith("{")]
    return int(next(line for line in lines if "evidence_problems" in line)["evidence_problems"])


def test_the_host_downloads_each_release_asset_checks_it_and_captures_every_manual_unit_with_a_completion_record(
    manual: ManualHost, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    manual.run(monkeypatch)
    check(evidence_problems(capsys) == 0, "the evidence check passed on a run with manual units")
    check(len(manual.seeded()) == 2 and not (manual.seed / host_inputs.DOWNLOAD_FOLDER).exists(), f"both assets are in the seed folder: {manual.seeded()}")
    check(len(manual.release.asked) == 4, f"each asset is asked at its release address and at the asset host: {manual.release.asked}")
    run = manual.run_record()
    done = {item["snapshot_id"] for item in run["units"]["completed"]}
    check({unit["snapshot_id"] for unit, _, _, _ in manual.units} <= done, f"every manual unit completed: {run['units']}")
    check(
        run["units"]["failed"] == [] and run["units"]["refused"] == [] and run["units"]["manual_not_attempted"] == [],
        f"nothing failed or was left out: {run['units']}",
    )
    for unit, spec, name, _body in manual.units:
        key = capture_mode.unit_key_of(unit, spec)
        stored = [key_ for key_ in manual.world.bucket.names(f"raw/ingestion_units/{key}/")]
        check(len(stored) == 1, f"{name} has one completion record: {stored}")
        written = json.loads(manual.world.bucket.objects[stored[0]][1])
        stored_names = {item["stored_file_name"] for item in written["data_objects"]}
        check(
            stored_names == {item["member_chain"][-1].rsplit("/", 1)[-1] for item in spec["archive_members"] if item["role"] == "data"},
            f"{name}: {stored_names}",
        )
        check(all(item["key"].startswith("raw/") for item in written["data_objects"]), "every data object sits under the raw root")
    check(run["manual_counts"]["captured"] == 2 and run["manual_http_requests"] == 0, f"the handler counted its two captures and sent no request: {run}")


def test_a_second_run_downloads_nothing_and_writes_only_its_run_record(manual: ManualHost, monkeypatch: pytest.MonkeyPatch) -> None:
    manual.run(monkeypatch)
    asked, written = len(manual.release.asked), len(manual.world.bucket.versions)
    manual.run(monkeypatch, RUN_ID="ingestion_0a1b2c3e")
    check(len(manual.release.asked) == asked, "the seed files in place are checked and not downloaded again")
    second = manual.run_record("ingestion_0a1b2c3e")
    check(
        len(second["units"]["skipped_complete"]) == 3 and second["counters"]["objects_created"] == 0, f"every unit is skipped as complete: {second['counters']}"
    )
    added = {item["Key"] for item in manual.world.bucket.versions[written:]}
    check(
        added == {record.run_record_key("raw/", "ingestion_0a1b2c3e"), publisher_run.bootstrap_key("raw/", "ingestion_0a1b2c3e")}, f"only run objects: {added}"
    )


def test_a_release_asset_with_another_digest_is_refused_before_any_cloud_call_or_capture_request(manual: ManualHost, monkeypatch: pytest.MonkeyPatch) -> None:
    name = manual.units[0][2]
    manual.release.assets[name] = manual.release.assets[name][:-1] + b"X"
    with pytest.raises(publisher_run.PublisherError, match=f"{name} differs from the release manifest") as caught:
        manual.run(monkeypatch)
    check("SECRET" not in str(caught.value), "the refusal holds no signed query")
    check(manual.untouched(), "no cloud call, no write and no publisher request came before the refusal")
    check(
        manual.seeded() == [] and not (manual.seed / host_inputs.DOWNLOAD_FOLDER).exists(),
        "no asset is placed when one fails, and the download folder is removed",
    )


def test_a_release_asset_of_another_size_is_refused(manual: ManualHost, monkeypatch: pytest.MonkeyPatch) -> None:
    name = manual.units[1][2]
    manual.release.assets[name] += b"\0"
    with pytest.raises(publisher_run.PublisherError, match=f"{name} differs from the release manifest size"):
        manual.run(monkeypatch)
    check(manual.untouched() and manual.seeded() == [], "nothing was written or placed")


def test_a_missing_release_asset_stops_the_run_before_any_capture_request(manual: ManualHost, monkeypatch: pytest.MonkeyPatch) -> None:
    name = manual.units[0][2]
    del manual.release.assets[name]
    with pytest.raises(publisher_run.PublisherError, match=f"{name} is not served by the release") as caught:
        manual.run(monkeypatch)
    check("http_404" in str(caught.value), "the refusal names the status code")
    check(manual.untouched() and manual.seeded() == [], "no cloud call, no write, no publisher request and no placed asset")


def test_a_seed_file_in_place_with_another_digest_is_refused_before_any_request(manual: ManualHost, monkeypatch: pytest.MonkeyPatch) -> None:
    unit, _, name, _ = manual.units[0]
    placed = manual.seed / "manual_units" / unit["snapshot_id"] / name
    placed.parent.mkdir(parents=True)
    placed.write_bytes(b"another file")
    with pytest.raises(publisher_run.PublisherError, match=f"{name} in the seed folder differs from the release manifest"):
        manual.run(monkeypatch)
    check(manual.release.asked == [] and manual.untouched(), "nothing was requested")


def test_a_release_redirect_to_another_host_is_refused(manual: ManualHost, monkeypatch: pytest.MonkeyPatch) -> None:
    manual.release.redirect_to = "https://objects.example.org/x"
    with pytest.raises(publisher_run.PublisherError, match="was refused|is not served"):
        manual.run(monkeypatch)
    check(manual.untouched() and manual.seeded() == [], "nothing was stored or placed")


def test_a_release_manifest_that_names_another_address_or_a_digest_the_seed_manifest_lacks_is_refused_before_any_request(
    manual: ManualHost, monkeypatch: pytest.MonkeyPatch
) -> None:
    manual.write_release(tag="seed-test-20261121")
    document = json.loads((manual.seed / host_inputs.RELEASE_MANIFEST).read_text(encoding="utf-8"))
    document["assets"][0]["url"] = "https://example.org/download/" + document["assets"][0]["name"]
    document["assets"][1]["sha256"] = "0" * 64
    (manual.seed / host_inputs.RELEASE_MANIFEST).write_text(json.dumps(document))
    with pytest.raises(publisher_run.PublisherError, match="release download address") as caught:
        manual.run(monkeypatch)
    check("not listed in the seed manifest with its digest" in str(caught.value), f"both problems are named: {caught.value}")
    check(manual.release.asked == [] and manual.untouched(), "nothing was requested")


def withhold(manual: ManualHost, index: int) -> dict:
    """Take the seed file of one archive away: it leaves the release manifest and the seed folder, so the host neither downloads nor finds it."""
    unit, _, name, _ = manual.units[index]
    document = json.loads((manual.seed / host_inputs.RELEASE_MANIFEST).read_text(encoding="utf-8"))
    document["assets"] = [item for item in document["assets"] if item["name"] != name]
    (manual.seed / host_inputs.RELEASE_MANIFEST).write_text(json.dumps(document))
    (manual.seed / "manual_units" / unit["snapshot_id"] / name).unlink(missing_ok=True)
    return unit


def bootstrap_marker(manual: ManualHost) -> list[str]:
    return sorted(manual.world.bucket.names("raw/ingestion_runs/"))


def test_a_bootstrap_fails_when_a_manual_unit_has_no_seed_file_and_no_verified_capture_and_leaves_no_bootstrap_marker(
    manual: ManualHost, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The unit has no earlier capture to fall back on: it is stale and missing. A bootstrap that lists it must fail like one with a paused or keyed unit."""
    unit = withhold(manual, 1)
    with pytest.raises(publisher_run.PublisherError, match="evidence check found"):
        manual.run(monkeypatch)
    run = manual.run_record()
    check([item["snapshot_id"] for item in run["missing_units"]] == [unit["snapshot_id"]], f"the unit is listed as missing: {run['missing_units']}")
    check(run["units"]["failed"] == [] and run["units"]["refused"] == [], "no unit failed or was refused: only the missing capture fails the run")
    check(not any(key.endswith("/bootstrap.json") for key in bootstrap_marker(manual)), "no bootstrap success marker was written")


def test_a_run_stays_successful_when_a_manual_unit_without_a_seed_file_has_a_verified_earlier_capture(
    manual: ManualHost, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The bootstrap captured both archives. A later monthly run lacks one seed file: the unit keeps its verified capture, is marked stale and listed."""
    manual.run(monkeypatch)
    unit = withhold(manual, 1)
    manual.run(monkeypatch, RUN_ID="ingestion_0a1b2c3e", PUBLISHER_RUN_KIND="monthly")
    run = manual.run_record("ingestion_0a1b2c3e")
    check(
        [item["snapshot_id"] for item in run["stale_units"]] == [unit["snapshot_id"]] and run["missing_units"] == [],
        f"a verified stale unit: {run['stale_units']}",
    )
    check(run["stale_units"][0]["reason"] == "seed_missing", "the reason is the missing seed file")


def test_the_release_route_follows_one_redirect_to_the_asset_host_and_saves_no_signed_query(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    name, body, _ = archive_of(1)
    monkeypatch.delenv(transport.APPROVED_HOSTS_VARIABLE, raising=False)
    monkeypatch.delenv(transport.APPROVED_HOSTS_APPROVAL_VARIABLE, raising=False)
    monkeypatch.setenv("HAI_CAPTURE_ROOT", "data/")
    release = Release({name: body})
    reviewed = transport.ReviewedTransport(urllib.request.ProxyHandler({}), release)
    url = f"{transport.RELEASE_DOWNLOAD_PREFIX}{TAG}/{name}"
    result = transport.download(url, tmp_path / "ok", name, "zip", "data", transport.Limits(attempts=1), opener=reviewed)
    check(result.complete and result.sha256 == hashlib.sha256(body).hexdigest(), f"the asset arrived: {result.failure}")
    check(
        result.resolved_url_redacted and result.resolved_url is not None and "?" not in result.resolved_url,
        f"the signed query is not saved: {result.resolved_url}",
    )
    check((tmp_path / "ok" / "attempt_01" / "transport.json").read_text().count("SECRET") == 0, "no signed value reached the saved metadata")
    other = Release({name: body}, redirect_to="https://objects.githubusercontent.com/x")
    refused = transport.download(url, tmp_path / "other", name, "zip", "data", transport.Limits(attempts=1), opener=transport.ReviewedTransport(other))
    check(not refused.complete and refused.failure == "CaptureError", f"a redirect to another host is refused: {refused.failure}")
    check(other.asked == [url], "the other host was never asked")


def test_a_seeded_ahrq_original_bound_to_its_plan_is_captured_through_the_normal_path_with_no_request(
    manual: ManualHost, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """The AHRQ file is in the seed folder only (its host is on no approved list), bound to the plan by SHA-256 and size. It is a held source, so the receipt
    carries the binding of its access release record, and the unit completes with the file stored as it is."""
    body = BODIES[XLSX_NAME]
    plan = plan_of({XLSX_NAME: body})
    locked = manual_content.locked_plan
    monkeypatch.setattr(manual_content, "locked_plan", lambda path: plan if "ahrq" in str(path) else locked(path))
    unit = {
        "snapshot_id": "AHRQ_SYSTEM__20990101T000000Z__" + "a" * 32,
        "source_id": "AHRQ_SYSTEM",
        "url": BASE + XLSX_NAME,
        "mode": "ahrq_compendium",
        "bytes": len(body),
        "pages": 1,
        "disposition": "approved_manual",
        "route": "manual_browser_download",
        "flags": [],
        "receipt": "unused",
        "receipt_sha256": "0" * 64,
    }
    spec = {
        "mode": "ahrq_compendium",
        "release_binding": "access_release",
        "source_id": "AHRQ_SYSTEM",
        "stored_data_files": [XLSX_NAME],
        "saved": {"vintage": "2018"},
    }
    specs = json.loads((manual.bundle / "capture_specs.json").read_text(encoding="utf-8"))["specs"]
    manual.world.units.append(unit)
    manual.world.write_inputs(specs | {unit["snapshot_id"]: spec})
    (manual.bundle / "hosts.json").rename(manual.bundle / "publisher_hosts_scope.json")
    manual.write_manifest()
    manual.write_release()
    manual.relock()
    original = manual.seed / "manual_units" / str(unit["snapshot_id"]) / XLSX_NAME
    original.parent.mkdir(parents=True)
    original.write_bytes(body)
    manual.run(monkeypatch)
    check(evidence_problems(capsys) == 0, "the evidence check passed")
    run = manual.run_record()
    check(unit["snapshot_id"] in {item["snapshot_id"] for item in run["units"]["completed"]}, f"the AHRQ unit completed: {run['units']}")
    [stored] = manual.world.bucket.names(f"raw/ingestion_units/{capture_mode.unit_key_of(unit, spec)}/")
    data = json.loads(manual.world.bucket.objects[stored][1])["data_objects"]
    check(
        [item["stored_file_name"] for item in data] == [XLSX_NAME] and data[0]["sha256"] == hashlib.sha256(body).hexdigest(),
        f"the file is stored as it is: {data}",
    )
    check(not any("ahrq.gov" in asked for asked in manual.world.site.asked), "no request went to the publisher")


def free_space(monkeypatch: pytest.MonkeyPatch, free: int) -> list[str]:
    """Report ``free`` bytes of disk space to every admission check. Returns the paths asked about."""
    asked: list[str] = []

    class Usage(NamedTuple):
        total: int
        used: int
        free: int

    def usage(path: Any) -> Usage:
        asked.append(str(path))
        return Usage(10 * 2**40, 0, free)

    monkeypatch.setattr(shutil, "disk_usage", usage)
    return asked


def admission_of_one_archive(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> tuple[ManualHost, int, int]:
    """A host with one archive that is small and declares a large expanded size, and the free space that the old admission (twice the file and 1 GiB) allows."""
    manual = ManualHost(tmp_path, [1], padding=PADDING)
    size = len(manual.units[0][3])
    return manual, size, 2 * size + 2**30


PADDING = 8_000_000


def test_the_host_admits_the_expanded_size_of_an_archive_before_it_copies_or_expands_it(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    manual, _, old_threshold = admission_of_one_archive(monkeypatch, tmp_path)
    free_space(monkeypatch, old_threshold + 2**20)
    monkeypatch.setattr(manual_content, "expand_tree", lambda *_a, **_k: (_ for _ in ()).throw(AssertionError("the archive was expanded before admission")))
    copied: list[tuple[Any, ...]] = []
    real_copy = shutil.copyfile

    def noting_copy(*arguments: Any, **options: Any) -> Any:
        copied.append(arguments)
        return real_copy(*arguments, **options)

    monkeypatch.setattr(shutil, "copyfile", noting_copy)
    with pytest.raises(publisher_run.PublisherError, match="evidence check found"):
        manual.run(monkeypatch)
    run = manual.run_record()
    check([item["reason"] for item in run["missing_units"]] == ["not_enough_disk_space"], f"a recorded unit outcome, not an escape: {run['missing_units']}")
    check(not [pair for pair in copied if str(manual.units[0][2]) in str(pair)], f"the seed file was not copied: {copied}")


def test_the_capture_path_admits_the_expansion_of_an_archive_it_stores(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Enough room for the staged copy and its expansion, not for the expansion that the capture path makes again beside the stored files."""
    manual, size, _ = admission_of_one_archive(monkeypatch, tmp_path)
    free_space(monkeypatch, 2 * size + PADDING + 2**30 + 2**20)
    with pytest.raises(publisher_run.PublisherError, match="evidence check found"):
        manual.run(monkeypatch)
    run = manual.run_record()
    check([item["reason"] for item in run["units"]["failed"]] == ["not_enough_disk_space"], f"the unit failed with a fixed reason: {run['units']['failed']}")


def test_a_disk_that_fills_while_a_seed_file_is_staged_is_a_recorded_unit_outcome_and_not_an_escape(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    manual = ManualHost(tmp_path, [1, 2])

    def full(*_arguments: Any, **_options: Any) -> None:
        raise OSError(errno.ENOSPC, "No space left on device")

    monkeypatch.setattr(shutil, "copyfile", full)
    with pytest.raises(publisher_run.PublisherError, match="evidence check found"):
        manual.run(monkeypatch)
    run = manual.run_record()
    check({item["reason"] for item in run["missing_units"]} == {"not_enough_disk_space"} and len(run["missing_units"]) == 2, f"both units are recorded: {run}")
    check(not any(key.endswith("/bootstrap.json") for key in bootstrap_marker(manual)), "no bootstrap marker")


TRICKLE_HEADERS = b"HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: 1000000\r\n\r\n"
TRICKLE_DEADLINE = 1.0
TRICKLE_ALLOWED = 4.0
TRICKLE_SENDER = 9.0


class Trickle:
    """A publisher on a real local connection that answers with quick headers and then sends one byte every 0.1 s, below any socket timeout. It stops after
    ``TRICKLE_SENDER`` seconds, so a download with no total bound ends the case instead of hanging it."""

    def __init__(self) -> None:
        self.listener = socket.create_server(("127.0.0.1", 0))
        self.port = self.listener.getsockname()[1]
        self.thread = threading.Thread(target=self.serve, name="trickle_publisher", daemon=True)
        self.thread.start()

    def serve(self) -> None:
        try:
            connection, _ = self.listener.accept()
        except OSError:
            return
        with connection:
            try:
                connection.recv(65536)
                connection.sendall(TRICKLE_HEADERS)
                end = time.monotonic() + TRICKLE_SENDER
                while time.monotonic() < end:
                    connection.sendall(b"0")
                    time.sleep(0.1)
            except OSError:
                return

    def close(self) -> None:
        self.listener.close()
        self.thread.join(timeout=TRICKLE_SENDER + 5)


def test_a_release_download_that_trickles_below_the_socket_timeout_stops_at_the_deadline(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    manual = ManualHost(tmp_path, [1])
    publisher = Trickle()
    monkeypatch.setattr(socket, "getaddrinfo", lambda *_a, **_k: [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 443))])

    def to_the_publisher(address: tuple[str, int], timeout: float | None = None, source_address: Any = None) -> socket.socket:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        sock.connect(("127.0.0.1", publisher.port))
        return sock

    class PassThrough:
        def wrap_socket(self, sock: socket.socket, **_options: Any) -> socket.socket:
            return sock

    monkeypatch.setattr(socket, "create_connection", to_the_publisher)
    monkeypatch.setattr(ssl, "create_default_context", PassThrough)
    monkeypatch.setattr(capture_mode, "download_seconds", lambda _bytes: TRICKLE_DEADLINE)
    monkeypatch.chdir(manual.code)
    started = time.monotonic()
    try:
        with pytest.raises(host_inputs.InputsError, match="TimeoutError"):
            host_inputs.fetch_release_assets(manual.code, transport.ReviewedTransport(transport.PublicHTTPSHandler()))
    finally:
        publisher.close()
    elapsed = time.monotonic() - started
    check(elapsed < TRICKLE_ALLOWED, f"the download ran {elapsed:.1f} s past a {TRICKLE_DEADLINE} s deadline")
    check(manual.seeded() == [] and not (manual.seed / host_inputs.DOWNLOAD_FOLDER).exists(), "no asset was placed and the download folder is gone")


def test_the_preparation_docstring_says_that_it_downloads_the_seed_assets() -> None:
    text = " ".join((host_inputs.__doc__ or "").split())
    check("makes no request" not in text, "the module says it makes no request, but it downloads the release assets")
    check("downloads" in text, "the module names its download")


REVIEWED_FREE = 1_395_864_371
THREE_GIB = 3 * 2**30


def test_the_declared_sizes_of_nested_archives_are_summed_from_their_central_directories_without_expanding_anything(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def refuse(*_arguments: Any, **_options: Any) -> None:
        raise AssertionError("an archive was expanded to read its declared sizes")

    monkeypatch.setattr(zipfile.ZipFile, "extract", refuse)
    monkeypatch.setattr(zipfile.ZipFile, "extractall", refuse)
    plain = tmp_path / "plain.zip"
    plain.write_bytes(declaring(zip_of({"a.csv": "1,2\n"}), 5_000_000))
    check(manual_content.declared_expansion(plain, "hospital_archive") == 5_000_000, "a plain archive reserves its own declared size")
    small = tmp_path / "small.zip"
    small.write_bytes(nested_zip({"a.csv": "1,2\n"}))
    check(manual_content.declared_expansion(small, "hospital_archive") == manual_content.TREE_BYTES, "a nested archive reserves at least the shared bound")
    big = tmp_path / "big.zip"
    big.write_bytes(zip_of({"inner.zip": declaring(zip_of({"a.csv": "1,2\n"}), THREE_GIB)}))
    check(
        manual_content.declared_expansion(big, "hospital_archive") >= THREE_GIB, "a nested archive that declares more than the bound reserves what it declares"
    )
    twice = tmp_path / "twice.zip"
    twice.write_bytes(zip_of({"mid.zip": zip_of({"innermost.zip": declaring(zip_of({"a.csv": "1,2\n"}), THREE_GIB)})}))
    check(manual_content.declared_expansion(twice, "hospital_archive") >= THREE_GIB, "the sizes are summed through two levels of nesting")


def test_a_nested_archive_is_refused_before_it_is_copied_when_the_free_space_is_what_the_reviewer_measured(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manual = ManualHost(tmp_path, [1], nested=True)
    free_space(monkeypatch, REVIEWED_FREE)
    monkeypatch.setattr(manual_content, "expand_tree", lambda *_a, **_k: (_ for _ in ()).throw(AssertionError("the archive was expanded before admission")))
    with pytest.raises(publisher_run.PublisherError, match="evidence check found"):
        manual.run(monkeypatch)
    run = manual.run_record()
    check([item["reason"] for item in run["missing_units"]] == ["not_enough_disk_space"], f"a stale unit with a fixed reason: {run['missing_units']}")


def test_a_nested_archive_with_the_space_the_shared_bound_asks_is_captured(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    manual = ManualHost(tmp_path, [1], nested=True)
    size = len(manual.units[0][3])
    free_space(monkeypatch, 2 * size + manual_content.TREE_BYTES + manual_content.ADMISSION_HEADROOM + 2**20)
    manual.run(monkeypatch)
    check(evidence_problems(capsys) == 0, "the evidence check passed")
    check(len(manual.run_record()["units"]["completed"]) == 2, "the direct unit and the nested archive completed")


def test_the_download_deadline_bounds_the_name_lookup(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A lookup that stalls for 0.8 s must end the download at its 0.1 s deadline, before any connection is made."""
    manual = ManualHost(tmp_path, [1])
    connected: list[object] = []

    def refused(*_arguments: Any, **_options: Any) -> socket.socket:
        connected.append(1)
        raise OSError("no network")

    def stalled(*_arguments: Any, **_options: Any) -> list[tuple[Any, ...]]:
        time.sleep(0.8)
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 443))]

    monkeypatch.setattr(socket, "getaddrinfo", stalled)
    monkeypatch.setattr(socket, "create_connection", refused)
    monkeypatch.setattr(capture_mode, "download_seconds", lambda _bytes: 0.1)
    monkeypatch.chdir(manual.code)
    started = time.monotonic()
    with pytest.raises(host_inputs.InputsError, match="TimeoutError"):
        host_inputs.fetch_release_assets(manual.code, transport.ReviewedTransport(transport.PublicHTTPSHandler()))
    elapsed = time.monotonic() - started
    check(elapsed < 0.5, f"the download ran {elapsed:.2f} s against a 0.1 s deadline")
    check(connected == [], "no connection was made after the deadline")


def test_the_module_texts_say_before_any_capture_request_and_describe_the_trickle_case() -> None:
    first = " ".join((host_inputs.__doc__ or "").split("\n\n")[0].split())
    check("before any capture request" in first, f"the first paragraph says: {first}")
    runner = " ".join((run_manual_units_e2e.__doc__ or "").split())
    check("trickle" in runner and "loopback" in runner, "the runner's text describes the loopback trickle case")


class Silent:
    """A publisher on a real local connection that accepts every connection and never answers. ``accepted`` counts the connections."""

    def __init__(self) -> None:
        self.listener = socket.create_server(("127.0.0.1", 0))
        self.listener.settimeout(0.2)
        self.port = self.listener.getsockname()[1]
        self.accepted = 0
        self.held: list[socket.socket] = []
        self.running = True
        self.thread = threading.Thread(target=self.serve, name="silent_publisher", daemon=True)
        self.thread.start()

    def serve(self) -> None:
        while self.running:
            try:
                connection, _ = self.listener.accept()
            except TimeoutError:
                continue
            except OSError:
                return
            self.accepted += 1
            self.held.append(connection)

    def close(self) -> None:
        self.running = False
        self.thread.join(timeout=5)
        for connection in self.held:
            connection.close()
        self.listener.close()


def test_a_silent_publisher_costs_one_socket_timeout_for_each_attempt_and_not_the_whole_download_budget(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A 0.1 s socket timeout inside a 0.8 s download budget and two attempts: each attempt ends at the socket timeout, so both run in about 0.2 s."""
    publisher = Silent()
    monkeypatch.setattr(socket, "getaddrinfo", lambda *_a, **_k: [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 443))])

    def to_the_publisher(address: tuple[str, int], timeout: float | None = None, source_address: Any = None) -> socket.socket:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        sock.connect(("127.0.0.1", publisher.port))
        return sock

    class PassThrough:
        def wrap_socket(self, sock: socket.socket, **_options: Any) -> socket.socket:
            return sock

    monkeypatch.setattr(socket, "create_connection", to_the_publisher)
    monkeypatch.setattr(ssl, "create_default_context", PassThrough)
    monkeypatch.delenv(transport.APPROVED_HOSTS_VARIABLE, raising=False)
    monkeypatch.delenv(transport.CAPTURE_ROOT_VARIABLE, raising=False)
    limits = transport.Limits(attempts=2, timeout_seconds=0.1, max_seconds=0.8, max_retry_delay=0)
    started = time.monotonic()
    try:
        result = transport.download("https://data.example.gov/file.json", tmp_path, "file.json", "json", "data", limits)
    finally:
        publisher.close()
    elapsed = time.monotonic() - started
    check(result.attempt == 2 and not result.complete and result.failure == "TimeoutError", f"attempt {result.attempt}, failure {result.failure}")
    check(elapsed < 0.6, f"the two attempts ran {elapsed:.2f} s, not the 0.8 s budget")
    check(publisher.accepted == 2, f"the publisher saw {publisher.accepted} connections")
