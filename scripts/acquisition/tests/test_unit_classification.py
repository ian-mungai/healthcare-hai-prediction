from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.acquisition import unit_classification as classification
from scripts.acquisition.source_registry import RegistryError
from tests.support import check


def unit(identity: str, **changes: object) -> dict:
    base = {"snapshot_id": identity, "source_id": "ALPHA", "disposition": "direct_download", "mode": "file", "flags": [], "url": "https://example.gov/a.csv"}
    return base | changes


UNITS = [
    unit("PLAIN__1"),
    unit("FLAGGED__2", flags=["original_contains_personal_details:private_owner_only"]),
    unit("COLLECTOR__3", mode="hcai_util_workbook"),
    unit("CONTACTS__4", source_id="main-hai-pdc"),
    unit("MANUAL__5", disposition="approved_manual", mode="hud_xlsx"),
    unit("SKIPPED__6", disposition="not_used_by_bronze"),
]


def queue_in(folder: Path) -> Path:
    queue = folder / "queue.json"
    queue.write_text(json.dumps({"run": 3, "queue": UNITS}))
    return queue


def test_the_proposal_marks_every_active_unit_proposed_and_names_where_each_class_comes_from(tmp_path: Path) -> None:
    document = classification.propose(queue_in(tmp_path).read_bytes())
    found = {identity: (entry["classification"], entry["status"], entry["source"]) for identity, entry in document["units"].items()}
    check(set(found) == {"PLAIN__1", "FLAGGED__2", "COLLECTOR__3", "CONTACTS__4", "MANUAL__5"}, "only active units are classified")
    check({status for _c, status, _s in found.values()} == {"proposed"} and document["reviewed_by"] is None, "nothing is reviewed")
    check(found["FLAGGED__2"] == ("personal", "proposed", "queue_flag:original_contains_personal_details"), "the queue flag is named")
    check(found["COLLECTOR__3"][0::2] == ("personal", "collector_mode:hcai_util_workbook"), "the collector mode is named")
    check(found["CONTACTS__4"][0::2] == ("personal", "contact_table_source:main-hai-pdc"), "the known contacts source is named even with no flag")
    check(found["PLAIN__1"][0::2] == ("non_personal", classification.NO_MARKER), "a unit with no marker is proposed non personal on the absence of a marker")


def test_the_proposal_reads_the_queue_and_nothing_else(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    queue = queue_in(tmp_path)
    opened: list[Path] = []
    real = Path.read_bytes

    def record(self: Path) -> bytes:
        opened.append(self)
        return real(self)

    monkeypatch.setattr(Path, "read_bytes", record)
    monkeypatch.setattr(Path, "read_text", lambda self, *a, **k: pytest.fail(f"a text read of {self.name}"))
    classification.main(["propose", "--queue", str(queue)])
    check(opened == [queue], f"only the queue file was read: {opened}")
    check(sorted(path.name for path in tmp_path.iterdir()) == ["queue.json", "unit_classification.json"], "no receipt or stored folder exists or was needed")


def test_a_proposal_alone_makes_no_unit_runnable(tmp_path: Path) -> None:
    body = queue_in(tmp_path).read_bytes()
    document = classification.propose(body)
    path = tmp_path / "unit_classification.json"
    path.write_bytes(json.dumps(document).encode())
    check(dict(classification.reviewed(path.read_bytes(), body)) == {}, "no unit is reviewed")


def test_the_owner_review_and_the_seal_make_units_runnable_and_change_nothing_else(tmp_path: Path) -> None:
    queue = queue_in(tmp_path)
    classification.main(["propose", "--queue", str(queue)])
    path = tmp_path / "unit_classification.json"
    document = json.loads(path.read_text())
    for identity in ("PLAIN__1", "FLAGGED__2"):
        document["units"][identity]["status"] = "reviewed"
    document["reviewed_by"] = "the owner"
    path.write_text(json.dumps(document))
    with pytest.raises(RegistryError, match="digest"):
        classification.reviewed(path.read_bytes(), queue.read_bytes())
    classification.main(["seal", "--file", str(path)])
    sealed = json.loads(path.read_text())
    check(sealed["units"] == document["units"] and sealed["reviewed_by"] == "the owner", "the seal changed no entry")
    held = classification.reviewed(path.read_bytes(), queue.read_bytes())
    check(dict(held) == {"PLAIN__1": "non_personal", "FLAGGED__2": "personal"}, f"only the reviewed units count: {dict(held)}")


def test_reviewed_entries_without_a_named_reviewer_are_refused(tmp_path: Path) -> None:
    queue = queue_in(tmp_path)
    document = classification.propose(queue.read_bytes())
    document["units"]["PLAIN__1"]["status"] = "reviewed"
    document["classification_sha256"] = classification.body_digest(document)
    with pytest.raises(RegistryError, match="reviewer"):
        classification.reviewed(json.dumps(document).encode(), queue.read_bytes())


def test_a_classification_made_for_another_queue_is_refused(tmp_path: Path) -> None:
    document = classification.propose(queue_in(tmp_path).read_bytes())
    with pytest.raises(RegistryError, match="another queue"):
        classification.reviewed(json.dumps(document).encode(), b'{"queue": []}')


def test_the_command_reports_counts_by_status_class_and_source_and_a_second_run_changes_nothing(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    queue = queue_in(tmp_path)
    classification.main(["propose", "--queue", str(queue)])
    counts = json.loads(capsys.readouterr().out)
    check(counts["units"] == 5 and counts["by_status"] == {"proposed": 5}, f"counts: {counts}")
    check(counts["by_classification"] == {"non_personal": 2, "personal": 3}, "two non personal and three personal")
    check(counts["by_source"][f"non_personal:{classification.NO_MARKER}"] == 2, "each source is counted")
    before = (tmp_path / "unit_classification.json").read_bytes()
    classification.main(["propose", "--queue", str(queue)])
    check((tmp_path / "unit_classification.json").read_bytes() == before, "the same proposal changes nothing")


def sealed_with(tmp_path: Path, key: str) -> tuple[bytes, bytes]:
    queue = queue_in(tmp_path)
    document = classification.propose(queue.read_bytes())
    document["units"][key] = {"classification": "personal", "status": "proposed", "source": "synthetic"}
    document["classification_sha256"] = classification.body_digest(document)
    return json.dumps(document).encode(), queue.read_bytes()


@pytest.mark.parametrize("key", ["../Jane Q. Example.csv", "SKIPPED__6", "NOT__IN_QUEUE"])
def test_a_classification_naming_a_unit_that_is_not_an_active_queue_unit_is_refused(tmp_path: Path, key: str) -> None:
    body, queue_body = sealed_with(tmp_path, key)
    with pytest.raises(RegistryError, match="outside the queue"):
        classification.reviewed(body, queue_body)
    with pytest.raises(RegistryError, match="outside the queue"):
        classification.personal_units(body, queue_body)


def test_an_unsafe_active_unit_id_is_refused_before_any_path_is_built(tmp_path: Path) -> None:
    queue = tmp_path / "queue.json"
    queue.write_text(json.dumps({"queue": [unit("../Jane Q. Example")]}))
    with pytest.raises(RegistryError, match="unsafe"):
        classification.propose(queue.read_bytes())
