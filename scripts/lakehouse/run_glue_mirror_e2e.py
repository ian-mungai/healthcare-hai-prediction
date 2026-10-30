"""Check the Glue mirror of published tables with stand-ins only: a fake Glue client and a fake Polaris pointer source.

Run from the repository root: ``.venv/bin/python -m scripts.lakehouse.run_glue_mirror_e2e --out <NEW_DIRECTORY>``. No AWS service is contacted, no
container starts and no Spark runs. Each case states one failure mode of the mirror and its required behavior. The report holds names and outcomes only.
"""

from __future__ import annotations

import argparse
import contextlib
import io
import json
import sys
import tempfile
from collections.abc import Callable, Sequence
from dataclasses import replace
from pathlib import Path
from typing import Any

import yaml
from botocore.exceptions import ClientError

from scripts.lakehouse import glue_mirror, gold_publish
from scripts.lakehouse.gold_publish_record import PublishError

REPO_ROOT = Path(__file__).resolve().parents[2]
BUCKET = "example-lakehouse-bucket"
PILOT = "bridge_county_neighbor"
CONFIDENTIAL = "dim_state"
RUN_ID = "20261030T120000Z-0123abcd"
DIGEST = "a" * 64
FIRST = f"s3://{BUCKET}/lakehouse/gold/{PILOT}/metadata/00001-aaaa.metadata.json"
SECOND = f"s3://{BUCKET}/lakehouse/gold/{PILOT}/metadata/00002-bbbb.metadata.json"
COLUMNS = [("county_sk", "string"), ("neighbor_sk", "string"), ("loaded_at", "timestamp")]
WRITES = ("create_table", "update_table")
Check = dict[str, bool]


class FakeGlue:
    """The Glue calls the mirror may use. ``fail`` names calls that raise a service error; every call is logged."""

    def __init__(self, databases: Sequence[str] = ("hai_mirror_gold", "hai_mirror_silver"), fail: dict[str, str] | None = None) -> None:
        self.databases = set(databases)
        self.tables: dict[tuple[str, str], dict[str, Any]] = {}
        self.calls: list[tuple[str, str, str]] = []
        self.fail = fail or {}
        self.updates: list[dict[str, Any]] = []

    def _error(self, operation: str, code: str) -> ClientError:
        return ClientError({"Error": {"Code": code, "Message": "stand-in"}}, operation)

    def _enter(self, operation: str, database: str, name: str) -> None:
        self.calls.append((operation, database, name))
        if operation in self.fail:
            raise self._error(operation, self.fail[operation])

    def get_table(self, DatabaseName: str, Name: str) -> dict[str, Any]:
        self._enter("get_table", DatabaseName, Name)
        if (DatabaseName, Name) not in self.tables:
            raise self._error("get_table", "EntityNotFoundException")
        return {"Table": self.tables[(DatabaseName, Name)]}

    def create_table(self, DatabaseName: str, TableInput: dict[str, Any]) -> dict[str, Any]:
        self._enter("create_table", DatabaseName, TableInput["Name"])
        if DatabaseName not in self.databases:
            raise self._error("create_table", "EntityNotFoundException")
        self.tables[(DatabaseName, TableInput["Name"])] = {**TableInput, "VersionId": "1"}
        return {}

    def update_table(self, DatabaseName: str, TableInput: dict[str, Any], VersionId: str) -> dict[str, Any]:
        self._enter("update_table", DatabaseName, TableInput["Name"])
        current = self.tables[(DatabaseName, TableInput["Name"])]
        if current["VersionId"] != VersionId:
            raise self._error("update_table", "ConcurrentModificationException")
        self.updates.append(TableInput)
        self.tables[(DatabaseName, TableInput["Name"])] = {**TableInput, "VersionId": str(int(VersionId) + 1)}
        return {}

    def writes(self) -> list[tuple[str, str, str]]:
        return [call for call in self.calls if call[0] in WRITES]


class FakeSource:
    """The Polaris side: the current metadata file and the columns of each table."""

    def __init__(self, pointers: dict[str, str | None], error: Exception | None = None) -> None:
        self.pointers = pointers
        self.error = error

    def pointer(self, table: str) -> str | None:
        if self.error is not None:
            raise self.error
        return self.pointers.get(table)

    def columns(self, table: str) -> list[tuple[str, str]]:
        return list(COLUMNS)


def enabled() -> glue_mirror.MirrorConfig:
    return replace(glue_mirror.load_config(), enabled=True)


def table_of(glue: FakeGlue, name: str = PILOT) -> dict[str, Any]:
    return glue.tables[("hai_mirror_gold", name)]


def case_first_publish_registers() -> Check:
    glue = FakeGlue()
    report = glue_mirror.mirror(enabled(), "gold", "real", [PILOT, CONFIDENTIAL], FakeSource({PILOT: FIRST}), glue)
    table = glue.tables.get(("hai_mirror_gold", PILOT), {})
    parameters = table.get("Parameters", {})
    return {
        "one_table_created": glue.writes() == [("create_table", "hai_mirror_gold", PILOT)],
        "reported_ok": report.get("status") == "ok" and report.get("tables") == {PILOT: "created"},
        "points_at_polaris_file": parameters.get("metadata_location") == FIRST,
        "marked_iceberg_and_mirror": parameters.get("table_type") == "ICEBERG" and parameters.get("hai_mirror") == "true",
        "location_is_table_folder": table.get("StorageDescriptor", {}).get("Location") == f"s3://{BUCKET}/lakehouse/gold/{PILOT}",
        "columns_listed": [(c["Name"], c["Type"]) for c in table.get("StorageDescriptor", {}).get("Columns", [])] == COLUMNS,
        "external_table": table.get("TableType") == "EXTERNAL_TABLE",
    }


def case_repeat_is_a_no_op() -> Check:
    glue = FakeGlue()
    source = FakeSource({PILOT: FIRST})
    glue_mirror.mirror(enabled(), "gold", "real", [PILOT], source, glue)
    before = repr(sorted(glue.tables.items()))
    glue.calls.clear()
    report = glue_mirror.mirror(enabled(), "gold", "real", [PILOT], source, glue)
    return {
        "no_write_call": glue.writes() == [],
        "read_once": glue.calls == [("get_table", "hai_mirror_gold", PILOT)],
        "reported_unchanged": report.get("status") == "unchanged" and report.get("tables") == {PILOT: "unchanged"},
        "table_identical": repr(sorted(glue.tables.items())) == before,
    }


def case_new_commit_moves_the_pointer() -> Check:
    glue = FakeGlue()
    glue_mirror.mirror(enabled(), "gold", "real", [PILOT], FakeSource({PILOT: FIRST}), glue)
    glue.calls.clear()
    report = glue_mirror.mirror(enabled(), "gold", "real", [PILOT], FakeSource({PILOT: SECOND}), glue)
    parameters = table_of(glue).get("Parameters", {})
    return {
        "one_update": glue.writes() == [("update_table", "hai_mirror_gold", PILOT)],
        "reported_updated": report.get("tables") == {PILOT: "updated"},
        "new_pointer": parameters.get("metadata_location") == SECOND,
        "previous_pointer_kept": parameters.get("previous_metadata_location") == FIRST,
        "still_marked": parameters.get("hai_mirror") == "true",
    }


def case_glue_error_is_reported_not_raised() -> Check:
    glue = FakeGlue(fail={"create_table": "ThrottlingException"})
    try:
        report = glue_mirror.mirror(enabled(), "gold", "real", [PILOT], FakeSource({PILOT: FIRST}), glue)
        raised = False
    except Exception:
        report, raised = {}, True
    reason = json.dumps(report)
    return {
        "did_not_raise": not raised,
        "status_failed": report.get("status") == "failed",
        "names_table_and_cause": PILOT in reason and "ThrottlingException" in reason,
        "nothing_registered": glue.tables == {},
    }


def case_missing_database_says_so() -> Check:
    glue = FakeGlue(databases=())
    report = glue_mirror.mirror(enabled(), "gold", "real", [PILOT], FakeSource({PILOT: FIRST}), glue)
    return {"status_failed": report.get("status") == "failed", "names_database": "hai_mirror_gold" in json.dumps(report)}


def context(directory: Path, namespace: str = "gold", mode: str = "real") -> gold_publish.Context:
    plan = replace(
        gold_publish.load_plan(),
        tables={PILOT: ("county_sk", "neighbor_sk"), CONFIDENTIAL: ("state_sk",)},
        namespaces={"publish_fixture": f"{namespace}_e2e", "real": namespace},
    )
    return gold_publish.Context(None, plan, RUN_ID, mode, directory, DIGEST, accepted_result_sha256=DIGEST, needs_sealed=True)


def case_hook_never_fails_the_publish() -> Check:
    with tempfile.TemporaryDirectory(prefix="glue_mirror_") as directory:
        folder = Path(directory)
        ctx = context(folder)
        outcomes: dict[str, Any] = {}
        for label, source, glue in (
            ("glue_down", FakeSource({PILOT: FIRST}), FakeGlue(fail={"get_table": "InternalServiceException"})),
            ("polaris_down", FakeSource({}, error=RuntimeError("catalog unreachable")), FakeGlue()),
        ):
            try:
                outcomes[label] = gold_publish.mirror_after_commit(ctx, source=source, glue=glue, config=enabled())
            except Exception as error:
                outcomes[label] = {"raised": repr(error)}
        record = json.loads((folder / "mirror.json").read_text()) if (folder / "mirror.json").is_file() else {}
        return {
            "glue_down_reported": outcomes["glue_down"].get("status") == "failed",
            "polaris_down_reported": outcomes["polaris_down"].get("status") == "failed" and "catalog unreachable" in json.dumps(outcomes["polaris_down"]),
            "record_written_for_the_run": record.get("run_id") == RUN_ID and record.get("status") == "failed",
        }


def case_hook_repeat_is_a_no_op() -> Check:
    with tempfile.TemporaryDirectory(prefix="glue_mirror_") as directory:
        ctx, glue, source = context(Path(directory)), FakeGlue(), FakeSource({PILOT: FIRST})
        first = gold_publish.mirror_after_commit(ctx, source=source, glue=glue, config=enabled())
        glue.calls.clear()
        again = gold_publish.mirror_after_commit(ctx, source=source, glue=glue, config=enabled())
        return {
            "first_created": first.get("tables") == {PILOT: "created"},
            "repeat_writes_nothing": glue.writes() == [],
            "repeat_unchanged": again.get("status") == "unchanged",
        }


def case_disabled_touches_nothing() -> Check:
    glue = FakeGlue()
    report = glue_mirror.mirror(glue_mirror.load_config(), "gold", "real", [PILOT], FakeSource({PILOT: FIRST}), glue)
    return {"tracked_default_is_off": glue_mirror.load_config().enabled is False, "no_call": glue.calls == [], "skipped": report.get("status") == "skipped"}


def case_test_namespace_touches_nothing() -> Check:
    glue = FakeGlue()
    report = glue_mirror.mirror(enabled(), "gold", "publish_fixture", [PILOT], FakeSource({PILOT: FIRST}), glue)
    with tempfile.TemporaryDirectory(prefix="glue_mirror_") as directory:
        hooked = gold_publish.mirror_after_commit(
            context(Path(directory), mode="publish_fixture"), source=FakeSource({PILOT: FIRST}), glue=glue, config=enabled()
        )
    return {"no_call": glue.calls == [], "skipped": report.get("status") == "skipped" and hooked.get("status") == "skipped"}


def case_bronze_and_other_layers_touch_nothing() -> Check:
    glue = FakeGlue()
    direct = glue_mirror.mirror(enabled(), "bronze", "real", ["any_table"], FakeSource({"any_table": FIRST}), glue)
    with tempfile.TemporaryDirectory(prefix="glue_mirror_") as directory:
        hooked = gold_publish.mirror_after_commit(context(Path(directory), namespace="bronze"), source=FakeSource({PILOT: FIRST}), glue=glue, config=enabled())
        runs = gold_publish.mirror_after_commit(context(Path(directory)), source=FakeSource({"publish_runs": FIRST}), glue=glue, config=enabled())
    return {
        "no_call": glue.calls == [],
        "bronze_skipped": direct.get("status") == "skipped" and hooked.get("status") == "skipped",
        "runs_table_never_listed": "publish_runs" not in json.dumps(runs) and ("create_table", "hai_mirror_gold", "publish_runs") not in glue.calls,
    }


def case_unlisted_tables_are_ignored() -> Check:
    glue = FakeGlue()
    glue_mirror.mirror(
        enabled(), "gold", "real", [PILOT, CONFIDENTIAL, "dim_hospital"], FakeSource({PILOT: FIRST, CONFIDENTIAL: FIRST, "dim_hospital": FIRST}), glue
    )
    silver = glue_mirror.mirror(enabled(), "silver", "real", ["int_acs_county_values"], FakeSource({"int_acs_county_values": FIRST}), glue)
    return {
        "only_the_listed_table": sorted({call[2] for call in glue.calls}) == [PILOT],
        "silver_lists_none": silver.get("status") == "skipped" and not any(call[1] == "hai_mirror_silver" for call in glue.calls),
    }


def case_foreign_table_is_refused() -> Check:
    glue = FakeGlue()
    glue.tables[("hai_mirror_gold", PILOT)] = {"Name": PILOT, "Parameters": {"owner": "someone"}, "VersionId": "1"}
    report = glue_mirror.mirror(enabled(), "gold", "real", [PILOT], FakeSource({PILOT: FIRST}), glue)
    return {"no_write": glue.writes() == [], "failed_and_says_why": report.get("status") == "failed" and "not a mirror" in json.dumps(report)}


def case_other_pointers_are_refused() -> Check:
    checks: Check = {}
    for label, pointer in (
        ("local_file", "file:///tmp/x/metadata/00001.metadata.json"),
        ("s3a_scheme", f"s3a://{BUCKET}/t/metadata/00001.metadata.json"),
        ("not_metadata", f"s3://{BUCKET}/t/data/a.parquet"),
        ("absent", None),
    ):
        glue = FakeGlue()
        report = glue_mirror.mirror(enabled(), "gold", "real", [PILOT], FakeSource({PILOT: pointer}), glue)
        checks[f"{label}_refused"] = glue.writes() == [] and report.get("status") == "failed"
    return checks


def dbt_classes() -> dict[str, str]:
    classes: dict[str, str] = {}
    for path in sorted((REPO_ROOT / "dbt/models").rglob("*.yml")):
        for model in (yaml.safe_load(path.read_text()) or {}).get("models", []):
            classes[model["name"]] = str((model.get("meta") or {}).get("classification"))
    return classes


def case_config_lists_only_public_contract_tables() -> Check:
    config, classes = glue_mirror.load_config(), dbt_classes()
    contract = json.loads((REPO_ROOT / "data_contracts/lakehouse/gold_contract.json").read_text())["publish"]["tables"]
    silver = json.loads((REPO_ROOT / "data_contracts/lakehouse/silver_publish.json").read_text())["publish"]["tables"]
    planted = replace(config, layers={"gold": replace(config.layers["gold"], tables=(PILOT, CONFIDENTIAL))})
    return {
        "the_pilot_is_listed": config.layers["gold"].tables == (PILOT,),
        "real_config_has_no_violation": glue_mirror.violations(config, classes, {"gold": set(contract), "silver": set(silver)}) == [],
        "confidential_table_is_flagged": any(CONFIDENTIAL in line for line in glue_mirror.violations(planted, classes, {"gold": set(contract)})),
        "unknown_table_is_flagged": any(
            "no_such_table" in line
            for line in glue_mirror.violations(
                replace(config, layers={"gold": replace(config.layers["gold"], tables=("no_such_table",))}), classes, {"gold": set(contract)}
            )
        ),
        "bronze_layer_is_flagged": any(
            "bronze" in line for line in glue_mirror.violations(replace(config, layers={"bronze": config.layers["gold"]}), classes, {})
        ),
    }


def run_check(arguments: Sequence[str], source: FakeSource, glue: FakeGlue) -> tuple[int, str]:
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        code = glue_mirror.main(arguments, source=source, glue=glue, config=enabled())
    return code, out.getvalue() + err.getvalue()


def case_check_compares_the_two_pointers() -> Check:
    glue = FakeGlue()
    glue_mirror.mirror(enabled(), "gold", "real", [PILOT], FakeSource({PILOT: FIRST}), glue)
    glue.calls.clear()
    same_code, same_text = run_check(["check", "--layer", "gold"], FakeSource({PILOT: FIRST}), glue)
    drift_code, drift_text = run_check(["check", "--layer", "gold"], FakeSource({PILOT: SECOND}), glue)
    missing_code, missing_text = run_check(["check", "--layer", "gold"], FakeSource({PILOT: FIRST}), FakeGlue())
    return {
        "equal_pointers_pass": same_code == 0 and PILOT in same_text,
        "drift_fails_and_names_both": drift_code == 1 and PILOT in drift_text and "00001-aaaa" in drift_text and "00002-bbbb" in drift_text,
        "missing_registration_fails": missing_code == 1 and "missing" in missing_text,
        "check_writes_nothing": glue.writes() == [],
    }


CASES: dict[str, Callable[[], Check]] = {
    "first_publish_registers": case_first_publish_registers,
    "repeat_is_a_no_op": case_repeat_is_a_no_op,
    "new_commit_moves_the_pointer": case_new_commit_moves_the_pointer,
    "glue_error_is_reported_not_raised": case_glue_error_is_reported_not_raised,
    "missing_database_says_so": case_missing_database_says_so,
    "hook_never_fails_the_publish": case_hook_never_fails_the_publish,
    "hook_repeat_is_a_no_op": case_hook_repeat_is_a_no_op,
    "disabled_touches_nothing": case_disabled_touches_nothing,
    "test_namespace_touches_nothing": case_test_namespace_touches_nothing,
    "bronze_and_other_layers_touch_nothing": case_bronze_and_other_layers_touch_nothing,
    "unlisted_tables_are_ignored": case_unlisted_tables_are_ignored,
    "foreign_table_is_refused": case_foreign_table_is_refused,
    "other_pointers_are_refused": case_other_pointers_are_refused,
    "config_lists_only_public_contract_tables": case_config_lists_only_public_contract_tables,
    "check_compares_the_two_pointers": case_check_compares_the_two_pointers,
}


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path, required=True, help="a new folder for cases.json")
    args = parser.parse_args(argv)
    if args.out.exists():
        sys.stderr.write(f"glue mirror E2E: {args.out} exists; give a new folder\n")
        return 1
    results: dict[str, Any] = {}
    for name, case in CASES.items():
        try:
            checks = case()
        except (PublishError, Exception) as error:
            checks = {f"case_ran ({type(error).__name__}: {error})": False}
        results[name] = {"pass": all(checks.values()) and bool(checks), "checks": checks}
        sys.stdout.write(f"{'PASS' if results[name]['pass'] else 'FAIL'} {name}\n")
        for label, ok in checks.items():
            if not ok:
                sys.stdout.write(f"     failed: {label}\n")
    args.out.mkdir(parents=True)
    (args.out / "cases.json").write_text(json.dumps(results, indent=2, sort_keys=True) + "\n")
    failed = [name for name, item in results.items() if not item["pass"]]
    sys.stdout.write(f"{len(results) - len(failed)} of {len(results)} cases passed\n")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
