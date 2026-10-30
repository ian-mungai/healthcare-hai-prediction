"""Check the Glue mirror of published tables with stand-ins only: a fake Glue client and a fake Polaris pointer source.

Run from the repository root: ``.venv/bin/python -m scripts.lakehouse.run_glue_mirror_e2e --out <NEW_DIRECTORY>``. No AWS service is contacted, no
container starts and no Spark runs. Each case states one failure mode of the mirror and its required behavior. The report holds names and outcomes only.
"""

from __future__ import annotations

import argparse
import contextlib
import io
import json
import re
import sys
import tempfile
from collections.abc import Callable, Sequence
from dataclasses import replace
from pathlib import Path
from typing import Any
from unittest import mock

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
    """The Polaris side. ``pointers`` is the live main. ``by_snapshot`` maps (table, snapshot ID) to the metadata file of that snapshot."""

    def __init__(self, pointers: dict[str, str | None], error: Exception | None = None, by_snapshot: dict[tuple[str, int], str] | None = None) -> None:
        self.pointers = pointers
        self.error = error
        self.by_snapshot = by_snapshot
        self.asked: list[tuple[str, int | None]] = []

    def pointer(self, table: str, snapshot_id: int | None = None) -> str | None:
        self.asked.append((table, snapshot_id))
        if self.error is not None:
            raise self.error
        if self.by_snapshot is not None and snapshot_id is not None:
            return self.by_snapshot.get((table, snapshot_id))
        return self.pointers.get(table)

    def columns(self, table: str) -> list[tuple[str, str]]:
        return list(COLUMNS)


def ids(names: Sequence[str], snapshot: int = 10) -> dict[str, int]:
    """A committed snapshot set: one snapshot ID for each table."""
    return dict.fromkeys(names, snapshot)


def hook(
    ctx: gold_publish.Context, source: Any, glue: Any, config: Any = None, names: Sequence[str] = (PILOT, CONFIDENTIAL), snapshot: int = 10
) -> dict[str, Any]:
    """The post-commit hook with the newest committed set stood in: the set the publish_runs table would name."""
    head = {"run_id": RUN_ID, "snapshot_set": ids(names, snapshot)}
    with mock.patch.object(gold_publish, "published_head", return_value=head):
        return gold_publish.mirror_after_commit(ctx, source=source, glue=glue, config=config or enabled())


def enabled() -> glue_mirror.MirrorConfig:
    return replace(glue_mirror.load_config(), enabled=True)


def table_of(glue: FakeGlue, name: str = PILOT) -> dict[str, Any]:
    return glue.tables[("hai_mirror_gold", name)]


def case_first_publish_registers() -> Check:
    glue = FakeGlue()
    report = glue_mirror.mirror(enabled(), "gold", "real", ids([PILOT, CONFIDENTIAL]), FakeSource({PILOT: FIRST, CONFIDENTIAL: FIRST}), glue)
    table = glue.tables.get(("hai_mirror_gold", PILOT), {})
    parameters = table.get("Parameters", {})
    return {
        "every_published_table_created": glue.writes() == [("create_table", "hai_mirror_gold", PILOT), ("create_table", "hai_mirror_gold", CONFIDENTIAL)],
        "reported_ok": report.get("status") == "ok" and report.get("tables") == {PILOT: "created", CONFIDENTIAL: "created"},
        "points_at_polaris_file": parameters.get("metadata_location") == FIRST,
        "marked_iceberg_and_mirror": parameters.get("table_type") == "ICEBERG" and parameters.get("hai_mirror") == "true",
        "location_is_table_folder": table.get("StorageDescriptor", {}).get("Location") == f"s3://{BUCKET}/lakehouse/gold/{PILOT}",
        "columns_listed": [(c["Name"], c["Type"]) for c in table.get("StorageDescriptor", {}).get("Columns", [])] == COLUMNS,
        "external_table": table.get("TableType") == "EXTERNAL_TABLE",
    }


def case_repeat_is_a_no_op() -> Check:
    glue = FakeGlue()
    source = FakeSource({PILOT: FIRST})
    glue_mirror.mirror(enabled(), "gold", "real", ids([PILOT]), source, glue)
    before = repr(sorted(glue.tables.items()))
    glue.calls.clear()
    report = glue_mirror.mirror(enabled(), "gold", "real", ids([PILOT]), source, glue)
    return {
        "no_write_call": glue.writes() == [],
        "read_once": glue.calls == [("get_table", "hai_mirror_gold", PILOT)],
        "reported_unchanged": report.get("status") == "unchanged" and report.get("tables") == {PILOT: "unchanged"},
        "table_identical": repr(sorted(glue.tables.items())) == before,
    }


def case_new_commit_moves_the_pointer() -> Check:
    glue = FakeGlue()
    glue_mirror.mirror(enabled(), "gold", "real", ids([PILOT]), FakeSource({PILOT: FIRST}), glue)
    glue.calls.clear()
    report = glue_mirror.mirror(enabled(), "gold", "real", ids([PILOT]), FakeSource({PILOT: SECOND}), glue)
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
        report = glue_mirror.mirror(enabled(), "gold", "real", ids([PILOT]), FakeSource({PILOT: FIRST}), glue)
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
    report = glue_mirror.mirror(enabled(), "gold", "real", ids([PILOT]), FakeSource({PILOT: FIRST}), glue)
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
                outcomes[label] = hook(ctx, source, glue)
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
        ctx, glue, source = context(Path(directory)), FakeGlue(), FakeSource({PILOT: FIRST, CONFIDENTIAL: FIRST})
        first = hook(ctx, source, glue)
        glue.calls.clear()
        again = hook(ctx, source, glue)
        return {
            "first_created": first.get("tables") == {PILOT: "created", CONFIDENTIAL: "created"},
            "repeat_writes_nothing": glue.writes() == [],
            "repeat_unchanged": again.get("status") == "unchanged",
        }


SECOND_OF = {name: FIRST.replace(PILOT, name).replace("00001-aaaa", "00002-bbbb") for name in (PILOT, CONFIDENTIAL)}
FIRST_OF = {name: FIRST.replace(PILOT, name) for name in (PILOT, CONFIDENTIAL)}


def later_failed_publish_source() -> FakeSource:
    """Snapshot 10 is the committed set. A later publish failed after moving main to snapshot 20, so the live pointers are ahead of the committed set."""
    by_snapshot = {(name, 10): FIRST_OF[name] for name in FIRST_OF} | {(name, 20): SECOND_OF[name] for name in SECOND_OF}
    return FakeSource(dict(SECOND_OF), by_snapshot=by_snapshot)


def case_committed_retry_mirrors_the_committed_set() -> Check:
    glue = FakeGlue()
    source = later_failed_publish_source()
    with tempfile.TemporaryDirectory(prefix="glue_mirror_") as directory:
        report = hook(context(Path(directory)), source, glue)
    held = {name: glue.tables[("hai_mirror_gold", name)]["Parameters"] for name in FIRST_OF}
    return {
        "mirrored_ok": report.get("status") == "ok",
        "points_at_the_committed_snapshot_not_the_live_main": all(held[name]["metadata_location"] == FIRST_OF[name] for name in held),
        "asked_polaris_for_the_committed_snapshots": sorted(source.asked) == sorted((name, 10) for name in FIRST_OF),
        "records_the_snapshot_and_the_run": all(held[name].get("hai_snapshot_id") == "10" and held[name].get("hai_run_id") == RUN_ID for name in held),
    }


def case_check_is_bound_to_the_committed_set() -> Check:
    source = later_failed_publish_source()
    glue = FakeGlue()
    glue_mirror.mirror(enabled(), "gold", "real", ids(list(FIRST_OF)), source, glue, RUN_ID)
    committed = glue_mirror.check(enabled(), "gold", ids(list(FIRST_OF)), source, glue)
    for name in SECOND_OF:
        glue.tables[("hai_mirror_gold", name)]["Parameters"]["metadata_location"] = SECOND_OF[name]
    ahead = glue_mirror.check(enabled(), "gold", ids(list(FIRST_OF)), source, glue)
    return {
        "mirror_at_the_committed_set_passes": committed["ok"] is True,
        "mirror_at_the_uncommitted_main_fails": ahead["ok"] is False and all("differs" in outcome for outcome in ahead["tables"].values()),
    }


def case_missing_committed_snapshot_is_refused() -> Check:
    glue = FakeGlue()
    source = FakeSource(dict(SECOND_OF), by_snapshot={(PILOT, 20): SECOND_OF[PILOT]})
    report = glue_mirror.mirror(enabled(), "gold", "real", ids([PILOT]), source, glue, RUN_ID)
    return {
        "failed_and_says_why": report.get("status") == "failed" and "committed snapshot" in json.dumps(report),
        "did_not_fall_back_to_the_live_main": glue.writes() == [],
    }


def case_mirror_stops_at_its_deadline() -> Check:
    glue = FakeGlue()
    names = [PILOT, CONFIDENTIAL, "dim_hospital"]
    ticks = iter(range(0, 10_000, 100))
    config = replace(enabled(), mirror_seconds=150)
    report = glue_mirror.mirror(config, "gold", "real", ids(names), FakeSource({name: FIRST for name in names}), glue, RUN_ID, clock=lambda: float(next(ticks)))
    return {
        "reported_failed": report.get("status") == "failed" and "deadline" in report.get("reason", ""),
        "later_tables_not_attempted": ("create_table", "hai_mirror_gold", "dim_hospital") not in glue.calls,
        "earlier_tables_kept": ("hai_mirror_gold", PILOT) in glue.tables,
        "no_exception": True,
    }


def case_glue_client_has_short_timeouts() -> Check:
    settings = glue_mirror.client_config()
    return {
        "connect_timeout_is_short": settings.connect_timeout <= 10,
        "read_timeout_is_short": settings.read_timeout <= 30,
        "retries_are_few": settings.retries["max_attempts"] <= 3,
        "config_has_an_overall_deadline": 0 < glue_mirror.load_config().mirror_seconds <= 600,
    }


class BrokenStream:
    def write(self, text: str) -> int:
        raise BrokenPipeError("diagnostic pipe closed")

    def flush(self) -> None:
        return None


def case_closed_diagnostic_pipe_does_not_escape() -> Check:
    glue = FakeGlue(fail={"get_table": "InternalServiceException"})
    raised = False
    with tempfile.TemporaryDirectory(prefix="glue_mirror_") as directory, mock.patch.object(gold_publish.sys, "stderr", BrokenStream()):
        try:
            report = hook(context(Path(directory)), FakeSource(dict(FIRST_OF)), glue)
        except Exception:
            report, raised = {}, True
        record = json.loads((Path(directory) / "mirror.json").read_text()) if (Path(directory) / "mirror.json").is_file() else {}
    return {
        "did_not_raise": not raised,
        "outcome_kept_in_the_result": report.get("status") == "failed",
        "outcome_kept_in_the_record": record.get("status") == "failed",
    }


def case_disabled_touches_nothing() -> Check:
    glue = FakeGlue()
    report = glue_mirror.mirror(glue_mirror.load_config(), "gold", "real", ids([PILOT]), FakeSource({PILOT: FIRST}), glue)
    return {"tracked_default_is_off": glue_mirror.load_config().enabled is False, "no_call": glue.calls == [], "skipped": report.get("status") == "skipped"}


def case_test_namespace_touches_nothing() -> Check:
    glue = FakeGlue()
    report = glue_mirror.mirror(enabled(), "gold", "publish_fixture", ids([PILOT]), FakeSource({PILOT: FIRST}), glue)
    with tempfile.TemporaryDirectory(prefix="glue_mirror_") as directory:
        hooked = hook(context(Path(directory), mode="publish_fixture"), FakeSource({PILOT: FIRST}), glue)
    return {"no_call": glue.calls == [], "skipped": report.get("status") == "skipped" and hooked.get("status") == "skipped"}


def case_bronze_and_other_layers_touch_nothing() -> Check:
    glue = FakeGlue()
    direct = glue_mirror.mirror(enabled(), "bronze", "real", ids(["any_table"]), FakeSource({"any_table": FIRST}), glue)
    with tempfile.TemporaryDirectory(prefix="glue_mirror_") as directory:
        hooked = hook(context(Path(directory), namespace="bronze"), FakeSource({PILOT: FIRST}), glue)
        runs = hook(context(Path(directory)), FakeSource({"publish_runs": FIRST}), glue)
    return {
        "no_call": glue.calls == [],
        "bronze_skipped": direct.get("status") == "skipped" and hooked.get("status") == "skipped",
        "runs_table_never_listed": "publish_runs" not in json.dumps(runs) and ("create_table", "hai_mirror_gold", "publish_runs") not in glue.calls,
    }


def case_confidential_gold_table_is_mirrored() -> Check:
    glue = FakeGlue()
    published = [PILOT, CONFIDENTIAL, "dim_hospital"]
    report = glue_mirror.mirror(enabled(), "gold", "real", ids(published), FakeSource({name: FIRST for name in published}), glue)
    return {
        "all_three_created": sorted(table for (_, table) in glue.tables) == sorted(published),
        "confidential_dimension_created": ("create_table", "hai_mirror_gold", CONFIDENTIAL) in glue.calls,
        "reported_ok": report.get("status") == "ok",
    }


def case_other_layers_touch_nothing() -> Check:
    glue = FakeGlue()
    reports = {
        layer: glue_mirror.mirror(enabled(), layer, "real", ids(["int_acs_county_values"]), FakeSource({"int_acs_county_values": FIRST}), glue)
        for layer in ("silver", "features", "bronze")
    }
    return {
        "no_call": glue.calls == [],
        "every_layer_skipped": all(report.get("status") == "skipped" for report in reports.values()),
        "no_silver_database_in_config": "silver" not in glue_mirror.load_config().layers,
    }


def case_foreign_table_is_refused() -> Check:
    glue = FakeGlue()
    glue.tables[("hai_mirror_gold", PILOT)] = {"Name": PILOT, "Parameters": {"owner": "someone"}, "VersionId": "1"}
    report = glue_mirror.mirror(enabled(), "gold", "real", ids([PILOT]), FakeSource({PILOT: FIRST}), glue)
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
        report = glue_mirror.mirror(enabled(), "gold", "real", ids([PILOT]), FakeSource({PILOT: pointer}), glue)
        checks[f"{label}_refused"] = glue.writes() == [] and report.get("status") == "failed"
    return checks


def case_config_scope_is_gold_only() -> Check:
    config = glue_mirror.load_config()
    gold = config.layers["gold"]
    return {
        "only_gold_is_configured": list(config.layers) == ["gold"] and gold.namespace == "gold",
        "real_config_has_no_violation": glue_mirror.violations(config) == [],
        "silver_is_flagged": any(
            "silver" in line for line in glue_mirror.violations(replace(config, layers={**config.layers, "silver": replace(gold, namespace="silver")}))
        ),
        "features_is_flagged": any(
            "features" in line for line in glue_mirror.violations(replace(config, layers={"features": replace(gold, namespace="features")}))
        ),
        "test_namespace_is_flagged": any(
            "gold_e2e" in line for line in glue_mirror.violations(replace(config, layers={"gold": replace(gold, namespace="gold_e2e")}))
        ),
        "no_class_filter_left": not hasattr(glue_mirror, "PUBLIC") and "tables" not in json.loads(glue_mirror.CONFIG.read_text())["layers"]["gold"],
    }


def source_files(pattern: str) -> dict[str, str]:
    return {str(path.relative_to(REPO_ROOT)): path.read_text() for path in sorted((REPO_ROOT / "infra").rglob(pattern)) if ".terraform" not in path.parts}


def case_reader_access_is_one_role_for_one_user() -> Check:
    template = (REPO_ROOT / "infra/iam/owner_policies/athena_reader.json").read_text()
    stack = (REPO_ROOT / "infra/iam/athena_mirror.tf").read_text()
    terraform = source_files("*.tf")
    users = [name for name, text in terraform.items() if "aws_iam_role_policy.athena_reader" in text or "aws_iam_role.athena_reader" in text]
    attachments = re.findall(r'resource "(aws_iam_[a-z_]*attachment|aws_iam_policy)" "(\w+)"', stack)
    roles = re.findall(r'resource "aws_iam_role" "(\w+)"', stack)
    return {
        "reader_policy_names_no_principal": not re.search(r'"(Principal|NotPrincipal|NotAction|NotResource)"', template),
        "reader_policy_only_allows": template.count('"Effect": "Allow"') == template.count('"Effect"'),
        "reader_role_used_only_by_its_stack_file": users == ["infra/iam/athena_mirror.tf"],
        "no_attachment_or_managed_policy": attachments == [],
        "one_role_reaches_the_reader_policy": roles == ["athena_reader"],
        "no_resource_policy_or_public_path": not re.search(
            r'resource "(aws_glue_resource_policy|aws_s3_bucket_policy|aws_s3_bucket_public_access_block|aws_lakeformation_\w+)"', stack
        )
        and '"*"' not in template,
    }


def run_check(arguments: Sequence[str], source: FakeSource, glue: FakeGlue) -> tuple[int, str]:
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        code = glue_mirror.main(arguments, source=source, glue=glue, config=enabled(), snapshots=ids([PILOT]))
    return code, out.getvalue() + err.getvalue()


def case_check_compares_the_two_pointers() -> Check:
    glue = FakeGlue()
    glue_mirror.mirror(enabled(), "gold", "real", ids([PILOT]), FakeSource({PILOT: FIRST}), glue)
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
    "confidential_gold_table_is_mirrored": case_confidential_gold_table_is_mirrored,
    "other_layers_touch_nothing": case_other_layers_touch_nothing,
    "foreign_table_is_refused": case_foreign_table_is_refused,
    "other_pointers_are_refused": case_other_pointers_are_refused,
    "committed_retry_mirrors_the_committed_set": case_committed_retry_mirrors_the_committed_set,
    "check_is_bound_to_the_committed_set": case_check_is_bound_to_the_committed_set,
    "missing_committed_snapshot_is_refused": case_missing_committed_snapshot_is_refused,
    "mirror_stops_at_its_deadline": case_mirror_stops_at_its_deadline,
    "glue_client_has_short_timeouts": case_glue_client_has_short_timeouts,
    "closed_diagnostic_pipe_does_not_escape": case_closed_diagnostic_pipe_does_not_escape,
    "config_scope_is_gold_only": case_config_scope_is_gold_only,
    "reader_access_is_one_role_for_one_user": case_reader_access_is_one_role_for_one_user,
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
