"""The publisher's startup file check per collector mode: the code root and the mirror root, and the guard on the staged plan input listing."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

from scripts.acquisition import collect_mmd_api, mmd_api_contract, publisher_run
from scripts.acquisition import redownload as harness
from scripts.acquisition.tests.test_publisher_run import Cloud, environment, staged
from scripts.infrastructure import startup_check
from tests.startup_check_support import Network, host_environment, install_network
from tests.support import check

ROOT = Path(__file__).resolve().parents[3]
PLAN_FILES = {
    "cms_owners_org": "cms_owners_plan",
    "onc_mu_hospital": "onc_mu_hospital_plan",
    "bls_api_v2": "bls_api_plan",
    "hud_api": "hud_api_plan",
}
CONFIG_KEYS = ("config/acquisition/example_plan.json", "config/acquisition/example_code_versions.json")
OTHER_KEY = "data/redownload_checks/example/job.json"


class Reached(Exception):
    """The harness started, so the startup check passed."""


def unit(mode: str) -> dict:
    return {"disposition": "api", "mode": mode, "flags": []}


def plan_of(mode: str) -> dict:
    return json.loads((ROOT / f"config/acquisition/{PLAN_FILES[mode]}.json").read_text())


def legacy_paths(mode: str) -> list[str]:
    """The legacy registry files of the mode's plan, read from the committed version catalog."""
    catalog = json.loads((ROOT / "config/acquisition/registry_versions.json").read_text())
    versions = [item for item in catalog["legacy_versions"] if item["registry_sha256"] == plan_of(mode)["registry_sha256"]]
    return [entry["path"] for version in versions for entry in version["files"].values()]


def code_paths(mode: str) -> list[str]:
    """Every file the mode reads at the code root: the legacy registry and, for BLS, the county adjacency references of the plan."""
    references = [item["path"] for item in plan_of(mode).get("references") or []]
    return [*legacy_paths(mode), *references]


class World:
    """A code root and a mirror root holding every file one mode reads. Tests remove one file and see it named."""

    def __init__(self, base: Path, mode: str) -> None:
        self.mode = mode
        self.code = base / "code"
        self.mirror = base / "mirror"
        self.controls = self.mirror / "data/redownload_checks/example/controls.json"
        for name in code_paths(mode):
            self.write(self.code / name)
        for name in CONFIG_KEYS:
            self.write(self.mirror / name)
        self.controls.parent.mkdir(parents=True, exist_ok=True)
        self.controls.write_text(json.dumps({"operational_sha256": {key: "0" * 64 for key in (*CONFIG_KEYS, OTHER_KEY)}}))
        self.listing = self.mirror / "plan_inputs.json"
        self.listing.write_text(json.dumps({code_paths(mode)[0]: "0" * 64}))

    @staticmethod
    def write(path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("x")

    def check(self, keys: bool = True) -> None:
        document = {"queue": [unit(self.mode)]}
        needs = startup_check.queue_files(document, controls=self.controls, keys=keys)
        startup_check.check_files("publisher", needs, {"code": self.code, "mirror": self.mirror})


@pytest.fixture
def on_host(monkeypatch: pytest.MonkeyPatch) -> None:
    host_environment(monkeypatch)
    install_network(monkeypatch, Network())


@pytest.mark.parametrize("mode", PLAN_FILES)
def test_a_plan_mode_with_both_roots_complete_starts(on_host: None, tmp_path: Path, mode: str) -> None:
    World(tmp_path, mode).check()


@pytest.mark.parametrize("mode", PLAN_FILES)
def test_a_legacy_registry_file_missing_from_the_code_root_is_named(on_host: None, tmp_path: Path, mode: str) -> None:
    world = World(tmp_path, mode)
    missing = legacy_paths(mode)[1]
    (world.code / missing).unlink()
    with pytest.raises(startup_check.StartupCheckError, match="input_file_missing") as caught:
        world.check()
    check(f"{mode} code root: {missing}" in str(caught.value), f"the mode, root and file are named: {caught.value}")


@pytest.mark.parametrize("mode", PLAN_FILES)
def test_an_operational_input_missing_from_the_mirror_root_is_named(on_host: None, tmp_path: Path, mode: str) -> None:
    world = World(tmp_path, mode)
    (world.mirror / CONFIG_KEYS[1]).unlink()
    with pytest.raises(startup_check.StartupCheckError, match="input_file_missing") as caught:
        world.check()
    check(f"{mode} mirror root: {CONFIG_KEYS[1]}" in str(caught.value), f"the mirror file is named with its root: {caught.value}")


def test_the_mirror_list_is_the_config_keys_of_the_controls_and_nothing_else(tmp_path: Path) -> None:
    world = World(tmp_path, "cms_owners_org")
    needs = startup_check.queue_files({"queue": [unit("cms_owners_org")]}, controls=world.controls, keys=True)
    mirror = sorted(need.path for need in needs if need.root == "mirror" and not need.nonempty)
    check(mirror == sorted(CONFIG_KEYS), f"only the config/acquisition keys of the controls are required: {mirror}")


def test_a_file_under_the_wrong_root_does_not_count(on_host: None, tmp_path: Path) -> None:
    world = World(tmp_path, "hud_api")
    (world.mirror / CONFIG_KEYS[0]).unlink()
    world.write(world.code / CONFIG_KEYS[0])
    with pytest.raises(startup_check.StartupCheckError, match="mirror root"):
        world.check()


def test_bls_names_each_county_adjacency_file(on_host: None, tmp_path: Path) -> None:
    world = World(tmp_path, "bls_api_v2")
    adjacency = [item["path"] for item in plan_of("bls_api_v2")["references"]]
    check(len(adjacency) == 5, f"the plan names five adjacency files: {len(adjacency)}")
    (world.code / adjacency[3]).unlink()
    with pytest.raises(startup_check.StartupCheckError, match="county_adjacency") as caught:
        world.check()
    check(adjacency[3] in str(caught.value), "the removed file is named")


@pytest.mark.parametrize("mode", ["bls_api_v2", "hud_api"])
def test_keyed_modes_name_files_only_when_the_task_has_keys(tmp_path: Path, mode: str) -> None:
    world = World(tmp_path, mode)
    check(startup_check.queue_files({"queue": [unit(mode)]}, controls=world.controls, keys=False) == [], "a task without keys does not run the mode")
    check(startup_check.queue_files({"queue": [unit(mode)]}, controls=world.controls, keys=True) != [], "a task with keys does")


@pytest.mark.parametrize(
    ("content", "cause"), [("{}", "plan_inputs_empty"), ("[]", "plan_inputs_empty"), ("", "plan_inputs_empty"), (None, "plan_inputs_missing")]
)
@pytest.mark.parametrize("mode", [*PLAN_FILES, "mmd_api"])
def test_a_plan_mode_with_an_empty_or_missing_plan_listing_fails_with_that_cause(
    on_host: None, tmp_path: Path, mode: str, content: str | None, cause: str
) -> None:
    world = World(tmp_path, "cms_owners_org")
    if content is None:
        world.listing.unlink()
    else:
        world.listing.write_text(content)
    needs = startup_check.queue_files({"queue": [unit(mode)]}, controls=world.controls, keys=True)
    with pytest.raises(startup_check.StartupCheckError, match=cause):
        startup_check.check_files("publisher", needs, {"code": world.code, "mirror": world.mirror})


def test_a_queue_without_a_plan_mode_needs_no_listing(on_host: None, tmp_path: Path) -> None:
    world = World(tmp_path, "cms_owners_org")
    world.listing.unlink()
    needs = startup_check.queue_files({"queue": [unit("file")]}, controls=world.controls, keys=True)
    check(needs == [], f"file units read neither plans nor listing: {needs}")
    startup_check.check_files("publisher", needs, {"code": world.code, "mirror": world.mirror})


def test_mmd_names_the_legacy_registry_and_the_template_at_the_code_root(on_host: None, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    world = World(tmp_path, "cms_owners_org")
    monkeypatch.chdir(world.code)
    plan, template = Path("data/mmd/plan.json"), Path("data/mmd/template_plan.json")
    registry = plan_of("cms_owners_org")["registry_sha256"]
    world.write(world.code / plan)
    (world.code / plan).write_text(json.dumps({"comparison_report": "data/mmd/report.json", "references": {}, "registry_sha256": registry}))
    world.write(world.code / "data/mmd/report.json")
    (world.code / "data/mmd/report.json").write_text(json.dumps({"years": []}))
    world.write(world.code / template)
    monkeypatch.setattr(mmd_api_contract, "PLANS", {mmd_api_contract.PLAN_SHA256: plan})
    monkeypatch.setattr(collect_mmd_api, "TEMPLATE", template)
    needs = startup_check.queue_files({"queue": [unit("mmd_api")]}, controls=world.controls, keys=True)
    named = {need.path for need in needs if need.root == "code"}
    check(set(legacy_paths("cms_owners_org")) <= named and str(template) in named, f"legacy registry and template are code root files: {sorted(named)}")
    startup_check.check_files("publisher", needs, {"code": world.code, "mirror": world.mirror})
    (world.code / template).unlink()
    with pytest.raises(startup_check.StartupCheckError, match="template_plan.json"):
        startup_check.check_files("publisher", needs, {"code": world.code, "mirror": world.mirror})


def staged_world(world: World, listing: dict[str, str]) -> tuple[dict[str, bytes], str, bytes]:
    """The staged inputs of the entry point test: the mirror tree of the world inside the inputs archive."""
    queue = json.dumps({"queue": [unit("cms_owners_org")]}).encode()
    files = {"mirror/data/redownload_checks/example/queue.json": queue, "publisher_hosts.json": b"{}"}
    files["publisher_manifest.json"] = b'{"queue": "mirror/data/redownload_checks/example/queue.json"}'
    files["mirror/data/redownload_checks/example/controls.json"] = world.controls.read_bytes()
    files["mirror/plan_inputs.json"] = json.dumps(listing).encode()
    for name in CONFIG_KEYS:
        files[f"mirror/{name}"] = b"x"
    stage, digest = staged(files)
    return stage, digest, queue


def run_entry(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, listing: dict[str, str]) -> Cloud:
    world = World(tmp_path, "cms_owners_org")
    monkeypatch.chdir(world.code)
    stage, digest, queue = staged_world(world, listing)
    cloud = Cloud(stage)

    def started(*_arguments: Any) -> None:
        raise Reached

    monkeypatch.setattr(harness, "run_from", started)
    monkeypatch.setattr(publisher_run, "install_plan_inputs", lambda *_arguments: None)
    monkeypatch.setenv("HAI_TASK_KEYS", "1")
    environ = environment(tmp_path / "workspace", digest) | {"PUBLISHER_QUEUE_SHA256": hashlib.sha256(queue).hexdigest()}
    publisher_run.main(environ, cloud.factory)
    return cloud


def test_the_entry_point_stops_on_an_empty_plan_listing_before_it_restores_state(on_host: None, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    with pytest.raises(startup_check.StartupCheckError, match="plan_inputs_empty"):
        run_entry(monkeypatch, tmp_path, {})


def test_the_entry_point_starts_with_a_listing_and_both_roots(on_host: None, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    with pytest.raises(Reached):
        run_entry(monkeypatch, tmp_path, {"data/example.json": "0" * 64})
