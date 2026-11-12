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
# Other findings of the privacy scan, joined from pieces for the same reason: an account ID, a machine temporary path and a phone number.
SCANNER_PATHS = {
    "an account ID": "config/" + "account" + "_id=" + "123456789012" + ".json",
    "a machine temporary path": "data/" + "va" + "r/folders/ab/cd_synthetic/plan.json",
    "a phone number": "data/" + "555" + "-123-" + "4567" + "/plan.json",
}
# A credential-bearing address, with a user, a secret, a host and a token, as a configured path may be written.
CREDENTIAL_URI = "https://SYNTHETIC_USER:SYNTHETIC_PASSWORD@SYNTHETIC_HOST/SYNTHETIC_PATH?token=SYNTHETIC_TOKEN"
SECRETS = ("SYNTHETIC_USER", "SYNTHETIC_PASSWORD", "SYNTHETIC_HOST", "SYNTHETIC_PATH", "SYNTHETIC_TOKEN")


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


def test_a_credential_bearing_address_under_the_code_root_prints_its_kind_and_digest_and_no_part_of_it(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    host_environment(monkeypatch)
    monkeypatch.chdir(tmp_path)
    with pytest.raises(startup_check.StartupCheckError) as caught:
        startup_check.check_files("publisher", [startup_check.Need("hcai_util_workbook", "code", CREDENTIAL_URI, kind="plan")], {"code": tmp_path})
    shown = capsys.readouterr().err + str(caught.value)
    check(f"code root: plan {startup_check.file_digest(CREDENTIAL_URI)}" in shown, f"the kind and the digest are named: {shown}")
    leaked = [secret for secret in SECRETS if secret in shown]
    check(not leaked, f"the output carries {leaked}: {shown}")


@pytest.mark.parametrize("finding", list(SCANNER_PATHS))
def test_a_path_with_a_privacy_scan_finding_other_than_a_home_path_prints_its_kind_and_digest(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path, finding: str
) -> None:
    path = SCANNER_PATHS[finding]
    shown = shown_for(monkeypatch, capsys, tmp_path, startup_check.Need("hcai_util_workbook", "code", path, kind="plan"))
    check(f"code root: plan {startup_check.file_digest(path)}" in shown, f"{finding}: the kind and the digest are named: {shown}")
    check(path.rsplit("/", 1)[-1] not in shown, f"{finding}: the path is not shown: {shown}")


# Forms of a credential that the file-name syntax must keep out of the output, each joined from pieces. A query token, a password query, a percent-encoded
# address, an escaped query key, a backslash address and a look-alike of the address punctuation (full width colon, slash and at sign).
FULL_WIDTH = {":": chr(0xFF1A), "/": chr(0xFF0F), "@": chr(0xFF20)}
FORBIDDEN_FORMS = {
    "a query token": "config/plan.json" + "?" + "token=SYNTHETIC_TOKEN",
    "a password query": "config/plan.json" + "?" + "password=SYNTHETIC_PASSWORD",
    "a percent-encoded address": "https%3A%2F%2FSYNTHETIC_USER%3ASYNTHETIC_PASSWORD%40SYNTHETIC_HOST%2Fplan%3Ftoken%3DSYNTHETIC_TOKEN",
    "an escaped query key": "config/plan.json" + "?" + "%74oken=SYNTHETIC_TOKEN",
    "a backslash address": "https:" + chr(92) * 2 + "SYNTHETIC_HOST" + chr(92) + "plan" + "?" + "token=SYNTHETIC_TOKEN",
    "look-alike address punctuation": "https"
    + FULL_WIDTH[":"]
    + FULL_WIDTH["/"] * 2
    + "SYNTHETIC_USER"
    + FULL_WIDTH[":"]
    + "SYNTHETIC_PASSWORD"
    + FULL_WIDTH["@"]
    + "SYNTHETIC_HOST/plan",
    "a percent-encoded account ID": "config/" + "account" + "_id%3D" + "123456789012" + ".json",
    "a space": "config/synthetic plan.json",
    "an empty segment": "config//plan.json",
    "a dot segment": "config/./plan.json",
    "a parent segment": "config/../config/plan.json",
    "a trailing slash": "config/plan/",
    "a non-ASCII letter": "config/pl" + chr(0xE4) + "n.json",
}


@pytest.mark.parametrize("form", list(FORBIDDEN_FORMS))
def test_a_code_root_path_outside_the_repository_filename_syntax_prints_its_kind_and_digest(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path, form: str
) -> None:
    path = FORBIDDEN_FORMS[form]
    host_environment(monkeypatch)
    monkeypatch.chdir(tmp_path)
    with pytest.raises(startup_check.StartupCheckError) as caught:
        startup_check.check_files("publisher", [startup_check.Need("hcai_util_workbook", "code", path, kind="plan")], {"code": tmp_path})
    shown = capsys.readouterr().err + str(caught.value)
    check(f"code root: plan {startup_check.file_digest(path)}" in shown, f"{form}: the kind and the digest are named: {shown}")
    leaked = [secret for secret in (*SECRETS, "plan.json", "token", "password") if secret in shown]
    check(not leaked, f"{form}: the output carries {leaked}: {shown}")


ORDINARY_PATHS = ("config/acquisition/x.json", "data/reference/county_adjacency_2020.txt", "dbt/models/silver/stg-a_b.sql", "README.md", "a/b/c.d.e/f-g_h.json")


@pytest.mark.parametrize("path", ORDINARY_PATHS)
def test_an_ordinary_repository_path_under_the_code_root_still_prints(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path, path: str
) -> None:
    shown = shown_for(monkeypatch, capsys, tmp_path, startup_check.Need("hcai_util_workbook", "code", path, kind="plan"))
    check(f"code root: plan {path}" in shown, f"the ordinary path is named: {shown}")
