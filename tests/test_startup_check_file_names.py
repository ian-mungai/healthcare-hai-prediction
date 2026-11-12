"""A file finding of the files check names the file by its repository-relative path when that is safe to show, and otherwise by its kind and a digest.

The path comes from the parent's own needs list, never from the child. It shows only when it is under the code root and passes the privacy scan's home-path
rule. Every path here is synthetic.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from scripts.infrastructure import startup_check
from tests.startup_check_support import host_environment
from tests.support import check

# The privacy scan flags a home-directory path with a user name. The pieces are joined here so that this file holds no such path itself.
HOME_PATH = "data/" + "ho" + "me/synthetic_name/plan.json"


def shown_for(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], code: Path, need: startup_check.Need) -> str:
    host_environment(monkeypatch)
    monkeypatch.chdir(code)
    with pytest.raises(startup_check.StartupCheckError):
        startup_check.check_files("publisher", [need], {"code": code, "mirror": code})
    return capsys.readouterr().err


def test_a_needed_file_under_the_code_root_prints_its_relative_path(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    relative = "config/acquisition/synthetic_plan.json"
    shown = shown_for(monkeypatch, capsys, tmp_path, startup_check.Need("hcai_util_workbook", "code", relative, kind="plan"))
    check(f"FAILED input_file_missing hcai_util_workbook code root: plan {relative}" in shown, f"the relative path is named: {shown}")
    check(startup_check.file_digest(relative) not in shown, f"no digest stands in its place: {shown}")


def test_an_absolute_path_under_the_code_root_prints_the_relative_path_only(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    absolute = tmp_path / "config" / "synthetic_plan.json"
    shown = shown_for(monkeypatch, capsys, tmp_path, startup_check.Need("hcai_util_workbook", "code", str(absolute), kind="plan"))
    check("plan config/synthetic_plan.json" in shown and str(tmp_path) not in shown, f"only the path below the code root is shown: {shown}")


@pytest.mark.parametrize("kind", ["outside", "parent"])
def test_a_path_outside_the_code_root_prints_its_kind_and_digest(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path, kind: str
) -> None:
    code = tmp_path / "code"
    code.mkdir()
    path = str(tmp_path / "elsewhere" / "synthetic_plan.json") if kind == "outside" else "../synthetic_plan.json"
    shown = shown_for(monkeypatch, capsys, code, startup_check.Need("hcai_util_workbook", "code", path, kind="plan"))
    check(f"code root: plan {startup_check.file_digest(path)}" in shown, f"the kind and the digest are named: {shown}")
    check("synthetic_plan" not in shown, f"the path is not shown: {shown}")


def test_a_path_that_the_home_path_rule_flags_prints_its_kind_and_digest(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    shown = shown_for(monkeypatch, capsys, tmp_path, startup_check.Need("hcai_util_workbook", "code", HOME_PATH, kind="plan"))
    check(f"code root: plan {startup_check.file_digest(HOME_PATH)}" in shown, f"the kind and the digest are named: {shown}")
    check("synthetic_name" not in shown, f"the flagged path is not shown: {shown}")


def test_a_path_with_a_control_character_prints_its_kind_and_digest(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    path = "config/synthetic\nstartup_check: ok forged.json"
    shown = shown_for(monkeypatch, capsys, tmp_path, startup_check.Need("hcai_util_workbook", "code", path, kind="plan"))
    check(f"code root: plan {startup_check.file_digest(path)}" in shown and "forged" not in shown, f"the line cannot be forged: {shown}")


def test_a_path_under_the_mirror_root_prints_its_kind_and_digest(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path) -> None:
    path = "config/acquisition/synthetic_input.json"
    shown = shown_for(monkeypatch, capsys, tmp_path, startup_check.Need("hcai_util_workbook", "mirror", path, kind="config_input"))
    check(f"mirror root: config_input {startup_check.file_digest(path)}" in shown and "synthetic_input" not in shown, f"{shown}")


def test_a_present_file_under_the_code_root_prints_its_relative_path_too(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    host_environment(monkeypatch)
    monkeypatch.chdir(tmp_path)
    (tmp_path / "synthetic_present.json").write_text("{}")
    startup_check.check_files("publisher", [startup_check.Need("hcai_util_workbook", "code", "synthetic_present.json")], {"code": tmp_path})
    check("ok input_file hcai_util_workbook code root: input synthetic_present.json" in capsys.readouterr().err, "the passing file is named by its path")
