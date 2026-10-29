"""End-to-end cases of the feature store package, lane A: the schema and generated files, the publish contract, the gold pin, the build, the
validation and the export. Each case names the failure modes it proves (``plans/full_scope_20261022/p3c/failure_modes_lane_a.md`` and
``failure_modes_design.md``).

Run from the repository root:

    .venv/bin/python -m scripts.features.run_feature_store_e2e --mode static
    .tools/sqlfluff/bin/python -m scripts.features.run_feature_store_e2e --mode build_local
    .venv/bin/python -m scripts.features.run_feature_store_e2e --mode publish_offline        # launcher and Docker: run outside the agent sandbox

Modes:

- ``static``: pure Python, no dbt, no DuckDB, no Docker (CI group ``lakehouse_features``). The schema generator and its checks with planted defects,
  the publish contract, the gold pin with a stand-in Spark, the pin and plan refusals.
- ``build_local``: the host half of the offline cases. It builds the synthetic fixture in the SQLFluff tool environment (dbt and DuckDB, no Docker),
  runs ``features_build`` and ``feature_validation`` exactly as the containers do, plants the defects of design 1.3 and requires each to fail its own
  check, mutates the validation to prove each case would notice a weakened check, and exports the build through the shared consumer. About three
  minutes.
- ``masks_local``: the three generated matrix queries and their mask validation on synthetic rows using the accepted spine source labels. It needs
  DuckDB from the SQLFluff tool environment, no producer build, launcher or Docker. ``build_local`` includes the same projection case.
- ``publish_offline``: three fixture builds exported through the shared consumer and published to a local Iceberg catalog in the Spark image by the
  Spark case suite (``features_publish_cases``). It needs the launcher job ``features_publish_e2e`` (template ``spark_e2e``); without it the mode
  reports the case as blocked and fails. Run it under ``.tools/sqlfluff/bin/python`` (it builds the three exports first).

Report: ``data/e2e/feature_store/<UTC stamp>/report.json`` with case ID, failure-mode IDs, verdict (``pass``, ``fail`` or ``pending`` for a case whose
code lives in another lane), counts, hashes and timings; never a row value.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import importlib
import importlib.util
import json
import shutil
import sys
import tempfile
import time
import traceback
from collections.abc import Callable
from datetime import UTC, datetime
from functools import partial
from pathlib import Path
from typing import Any
from unittest import mock

from scripts.features import feature_schema as schema_module
from scripts.features import features_input, features_publish
from scripts.lakehouse import feature_columns as columns
from scripts.lakehouse import gold_publish
from scripts.lakehouse.gold_publish_record import PublishError
from scripts.process import TimeoutExpired, run_command

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "data" / "e2e" / "feature_store"
MODES = ("static", "build_local", "masks_local", "publish_offline")
RUN = "20261023T000000Z-00000001"
RUN_B = "20261023T000100Z-00000002"
RUN_2 = "20261023T000200Z-00000003"
GOLD_RUN = "20261014T040000Z-0000ab01"
SUITE_FOLDER = "data_contracts/great_expectations/features"
# The launcher jobs the publish_offline mode needs from the launcher lane (see the report): none of these exist before lane B adds them.
Step = dict[str, Any]
ZERO_COUNT = 'select count(*) from feature_matrix_county_measures where "{column}" = 0'
TABLE_HASH = 'select count(*), sum(hash(t)::hugeint) from "{table}" as t'
PARQUET_VALUE = "select c001__value__number from read_parquet('{file}') where c001__value__number is not null order by 1 limit 1"
PARQUET_SCHEMA = "select name, logical_type from parquet_schema('{file}')"


class Report:
    """The steps of one run, with the verdict of each."""

    def __init__(self) -> None:
        self.steps: list[Step] = []

    def case(self, case: str, modes: str, name: str, ok: bool, **evidence: Any) -> bool:
        """Record one check: ``ok`` is the verdict; the evidence holds names, counts and hashes only."""
        self.steps.append({"case": case, "failure_modes": modes, "name": name, "verdict": "pass" if ok else "fail", **evidence})
        return ok

    def pending(self, case: str, modes: str, name: str, owner: str, reason: str) -> None:
        """A case whose code is in another lane: reported as pending, never as passed."""
        self.steps.append({"case": case, "failure_modes": modes, "name": name, "verdict": "pending", "owner": owner, "reason": reason})

    @property
    def failed(self) -> list[str]:
        return [f"{step['case']}: {step['name']}" for step in self.steps if step["verdict"] == "fail"]


def refuses(action: Callable[[], Any], *words: str) -> bool:
    """True when the action raises a refusal whose text holds every word (a refusal for the right reason)."""
    try:
        action()
    except (PublishError, schema_module.SchemaError, features_input.PipelineError) as error:
        return all(word in str(error) for word in words)
    except Exception:
        return False
    return False


def scratch_root(folder: Path) -> Path:
    """A copy of the files the generator reads and writes, to plant a defect in."""
    root = folder / "root"
    for name in (schema_module.SEEDS, "data_contracts/features", "data_contracts/lakehouse", schema_module.MATRIX_FOLDER, "feature_store"):
        shutil.copytree(ROOT / name, root / name)
    return root


# ----------------------------------------------------------------------------------------------------
# static


class FakeSpark:
    """The few Spark calls the pin makes, answered from lists: the committed runs and the snapshots the catalog holds."""

    def __init__(self, runs: list[tuple[Any, ...]], snapshots: set[int], tables: bool = True) -> None:
        self.runs, self.snapshots, self.tables = runs, snapshots, tables
        self.catalog = self

    def tableExists(self, name: str) -> bool:
        return self.tables

    def sql(self, text: str) -> Any:
        class Frame:
            def __init__(self, rows: list[Any]) -> None:
                self.rows = rows

            def collect(self) -> list[Any]:
                return self.rows

        if ".snapshots WHERE snapshot_id =" in text:
            snapshot = int(text.rsplit("=", 1)[1])
            return Frame([(snapshot,)] if snapshot in self.snapshots else [])
        if "ORDER BY committed_at DESC" in text:
            return Frame(self.runs[-1:])
        return Frame(list(self.runs))


def gold_run_row(
    plan: gold_publish.Plan, mode: str = "publish_fixture", run: str = GOLD_RUN, tables: list[str] | None = None, ids: Any = None
) -> tuple[Any, ...]:
    """A ``publish_runs`` row of the gold namespace."""
    names = tables if tables is not None else sorted(plan.tables)
    snapshot_set = ids if ids is not None else {name: 1000 + index for index, name in enumerate(names)}
    return (run, mode, datetime(2026, 10, 14, 4, 0, tzinfo=UTC), "a" * 64, "b" * 64, "catalog_backup_x.json", "c" * 64, json.dumps(snapshot_set))


def pin_cases(report: Report) -> None:
    """E02: the gold pin accepts a complete committed set once and refuses every incomplete, foreign or absent set."""
    plan = features_input.gold_plan()
    names = sorted(plan.tables)
    every = {1000 + index for index in range(len(names))}

    def context(spark: FakeSpark, mode: str = "publish_fixture") -> gold_publish.Context:
        return gold_publish.Context(spark, plan, "20261023T000000Z-00000001", mode, Path("/nonexistent"), "", needs_sealed=False)

    with tempfile.TemporaryDirectory(prefix="pin_cases_") as folder:
        path = Path(folder) / features_input.PIN_NAME
        good = FakeSpark([gold_run_row(plan)], every)
        record = features_input.pin(context(good), path)
        again = FakeSpark([gold_run_row(plan), gold_run_row(plan, run="20261015T040000Z-0000ab02")], every)
        retried = features_input.pin(context(again), path)
        features_input.check_gold_pin(record, "20261023T000000Z-00000001", "publish_fixture", set(names))
        report.case(
            "E02",
            "A1",
            "the pin of the head gold set names every gold table and is immutable on a retry after the head moved",
            retried == record and len(record["snapshot_set"]) == 70,
        )
        report.case(
            "E02",
            "A1",
            "a retry that asks for another gold run than the pin is refused",
            refuses(lambda: features_input.pin(context(again), path, "20261015T040000Z-0000ab02"), "gold", "immutable"),
        )

        def pin_with(spark: FakeSpark, mode: str = "publish_fixture", source: str | None = None) -> Any:
            return features_input.pin(context(spark, mode), Path(folder) / f"{len(list(Path(folder).iterdir()))}.json", source)

        report.case(
            "E02", "A1", "no committed gold set is refused", refuses(lambda: pin_with(FakeSpark([], every, tables=False)), "no matching committed gold")
        )
        report.case(
            "E02", "A1", "a set of the other mode is refused", refuses(lambda: pin_with(FakeSpark([gold_run_row(plan, mode="real")], every)), "gold", "mode")
        )
        report.case(
            "E02",
            "A1",
            "a partial set (one table missing) is refused",
            refuses(lambda: pin_with(FakeSpark([gold_run_row(plan, tables=names[:-1])], every)), "gold", "exactly"),
        )
        report.case(
            "E02",
            "A1",
            "a snapshot absent from the catalog is refused",
            refuses(lambda: pin_with(FakeSpark([gold_run_row(plan)], every - {1003})), "gold", "absent"),
        )
        zero = {name: (0 if index == 0 else 1000 + index) for index, name in enumerate(names)}
        report.case(
            "E02", "A1", "an invalid snapshot ID is refused", refuses(lambda: pin_with(FakeSpark([gold_run_row(plan, ids=zero)], every)), "gold", "invalid")
        )
        report.case(
            "E02",
            "A1",
            "a run named with a bad ID is refused",
            refuses(lambda: pin_with(good, source="not-a-run"), "gold", "invalid"),
        )
        bad_records = {
            "wrong run": {**record, "run_id": "20261023T000000Z-00000009"},
            "wrong namespace": {**record, "namespace": "silver_e2e"},
            "extra key": {**record, "extra": 1},
            "missing table": {**record, "snapshot_set": dict(list(record["snapshot_set"].items())[:-1])},
            "boolean snapshot": {**record, "snapshot_set": {**record["snapshot_set"], names[0]: True}},
        }
        refused = {
            label: refuses(partial(features_input.check_gold_pin, document, "20261023T000000Z-00000001", "publish_fixture", set(names)), "gold pin")
            for label, document in bad_records.items()
        }
        report.case("E02", "A1", "a pin record of another shape, run, namespace or table set is refused", all(refused.values()), refused=refused)


def plan_cases(report: Report) -> None:
    """E05: the write guard of the plan, and the writer's grants."""
    fixture, real = features_publish.publish_plan("publish_fixture"), features_publish.publish_plan("real")
    report.case(
        "E05",
        "A7",
        "the plan maps the fixture to features_e2e and real to features, each with its own backup prefix",
        fixture.namespaces == {"publish_fixture": "features_e2e", "real": "features"}
        and fixture.backup_prefix == "lakehouse/features_e2e/catalog_backups/"
        and real.backup_prefix == "lakehouse/features/catalog_backups/",
    )
    report.case("E05", "A7", "any other mode is refused", refuses(lambda: features_publish.publish_plan("test"), "invalid"))
    with tempfile.TemporaryDirectory(prefix="plan_cases_") as folder:
        document = json.loads(features_publish.CONTRACT.read_text())
        swapped = json.loads(json.dumps(document))
        swapped["publish"]["namespaces"] = {"publish_fixture": "features", "real": "features_e2e"}
        other_writer = json.loads(json.dumps(document))
        other_writer["publish"]["writer_principal"] = "gold_writer"
        for label, changed in (("a swapped namespace mapping", swapped), ("another writer principal", other_writer)):
            path = Path(folder) / f"{label.replace(' ', '_')}.json"
            path.write_text(json.dumps(changed))
            report.case("E05", "A7", f"a contract with {label} is refused", refuses(partial(features_publish.publish_plan, "real", path), "invalid"))
    from scripts.features import features_writer_setup
    from scripts.lakehouse import polaris_writer

    grants = polaris_writer.grants(features_writer_setup.FEATURES)
    namespaces = {grant["grant"]["namespace"][0] for grant in grants if grant["grant"]["type"] == "namespace"}
    privileges = {grant["grant"]["privilege"] for grant in grants}
    report.case(
        "E05",
        "H14",
        "the features writer may write the two features namespaces and holds none of the forbidden privileges",
        namespaces == {"features", "features_e2e"} and not privileges & set(polaris_writer.FORBIDDEN_PRIVILEGES) and "NAMESPACE_CREATE" not in privileges,
    )
    report.pending(
        "E05",
        "A7, A17",
        "the broker refuses a real features run without a matching approval record; fixture renders carry no AWS profile",
        "lane B",
        "launcher files",
    )


def contract_cases(report: Report) -> None:
    """E02: the publish contract against the schema, the models and their tests."""
    schema = schema_module.load_schema()
    contract = json.loads((ROOT / "data_contracts/lakehouse/features_publish.json").read_text())["publish"]["tables"]
    wide = set(schema["tables"])
    longs = {table["long_table"] for table in schema["tables"].values()}
    support = {"int_spine_validation_measures", "int_county_adjacency"}
    expected = wide | longs | {"int_spine_linkage", "feature_build_inputs"} | support
    report.case(
        "E02",
        "H1",
        "the contract names the wide, long, linkage, inputs and two non-Feast support tables",
        set(contract) == expected,
    )
    report.case(
        "E02",
        "DT2",
        "neither support table is a Feast schema table or reference",
        not (support & wide) and not any(ref.split(":")[0] in support for ref in schema["servable"]),
    )
    models = {path.stem for path in (ROOT / columns.MODELS).rglob("*.sql")}
    report.case("E02", "H1", "every contract table is a model except the inputs table", (set(contract) - {"feature_build_inputs"}) <= models)
    yaml: Any = importlib.import_module("yaml")
    tested: dict[str, dict[str, list[str]]] = {}
    for path in columns.yaml_files(ROOT):
        for model in yaml.safe_load(path.read_text())["models"]:
            tested[model["name"]] = {column["name"]: column.get("data_tests") or [] for column in model.get("columns") or []}
    missing = [
        name
        for name, entry in contract.items()
        if name != "feature_build_inputs" and any({"unique", "not_null"} - set(tested[name].get(key, [])) for key in entry["merge_key"])
    ]
    report.case("E02", "H1", "every merge key has a unique and a not_null test in the model YAML", not missing, tables_without_tests=missing)
    suite = json.loads((ROOT / SUITE_FOLDER / "features_keys.json").read_text())
    covered = {entry["table"] for entry in suite["expectations"]}
    report.case(
        "E02",
        "H1",
        "the Great Expectations suite folder has a grain expectation for every contract table",
        covered == set(contract),
        uncovered=sorted(set(contract) - covered),
    )


def generator_cases(report: Report) -> None:
    """E04s and E11s: the generator and its checks, each defect planted in a scratch copy."""
    problems = schema_module.check(ROOT)
    report.case("E04s", "G2", "the committed schema, models, YAML and definitions are what the generator writes", not problems, problems=problems[:3])
    schema = schema_module.load_schema()
    inventory: dict[str, set[Any]] = {
        "care_compare": {row["measure_control"] for row in schema_module.read_seed(ROOT, "frozen_care_compare_controls")},
        "hospital": {(row["measure_source"], row["measure_control"], row["field"]) for row in schema_module.read_seed(ROOT, "frozen_hospital_controls")},
        "operations": {
            (row["measure_source"], row["measure_control"])
            for row in schema_module.read_seed(ROOT, "frozen_operations_controls")
            if row["is_carried"] == "true"
        },
    }
    rules = json.loads((ROOT / schema_module.RULES).read_text())
    extra = {"care_compare": 0, "hospital": 0, "operations": len(rules["both_kinds"])}
    counts = {}
    for family, keys in inventory.items():
        table = schema["tables"][f"feature_matrix_{family}"]
        counts[family] = (len(keys), sum(column["role"] == "predictor" for column in table["columns"]))
    report.case(
        "E04s",
        "G4",
        "predictor columns equal the frozen controls of the family (plus one per both-kinds key)",
        all(wide == keys + extra[name] for name, (keys, wide) in counts.items()),
        counts=counts,
    )
    with tempfile.TemporaryDirectory(prefix="generator_cases_") as folder:
        base = Path(folder)

        def planted(name: str, change: Callable[[Path], None]) -> Path:
            root = scratch_root(base / name)
            change(root)
            return root

        def append_row(root: Path, seed: str, row: str) -> None:
            with (root / schema_module.SEEDS / f"{seed}.csv").open("a") as handle:
                handle.write(row + "\n")

        clash = planted(
            "clash",
            lambda root: append_row(
                root,
                "frozen_hospital_controls",
                "cost_report,C001,v.1,value,x,,,cost_report_measures,90\ncost_report,C001,v-1,value,x,,,cost_report_measures,91",
            ),
        )
        report.case(
            "E04s",
            "G1",
            "two keys that fold to one column name stop the generator",
            refuses(lambda: schema_module.generated_files(clash), "fold to the column name"),
        )
        long = planted("long", lambda root: append_row(root, "frozen_hospital_controls", f"cost_report,C001,{'f' * 130},value,x,,,cost_report_measures,90"))
        report.case("E04s", "G1", "a column name over 128 characters stops the generator", refuses(lambda: schema_module.generated_files(long), "limit is 128"))
        leaked = planted("leak", lambda root: append_row(root, "frozen_care_compare_controls", "C269,fct_x,m,v,registry_measure_sources,99"))
        report.case(
            "E04s",
            "G10",
            "an outcome control among the predictors stops the generator",
            refuses(lambda: schema_module.generated_files(leaked), "excluded from predictors"),
        )

        def unknown_rule(root: Path) -> None:
            path = root / schema_module.RULES
            document = json.loads(path.read_text())
            document["categories"].append({"family": "hospital", "source": "x", "control": "C999", "field": "y", "reason": "planted"})
            path.write_text(json.dumps(document))

        report.case(
            "E04s",
            "G4",
            "a reviewed rule that names no control stops the generator",
            refuses(lambda: schema_module.generated_files(planted("rule", unknown_rule)), "no control seed gives"),
        )

        def drop_code_rule(root: Path) -> None:
            path = root / schema_module.RULES
            document = json.loads(path.read_text())
            document["categories"] = [entry for entry in document["categories"] if entry["control"] != "C002"]
            path.write_text(json.dumps(document))

        report.case(
            "E04s",
            "G5",
            "a rule-code hospital control that is not classed as a category stops the generator",
            refuses(lambda: schema_module.generated_files(planted("code", drop_code_rule)), "not a category"),
        )
        stale = {
            "stale_schema": schema_module.SCHEMA,
            "stale_model": f"{schema_module.MATRIX_FOLDER}/feature_matrix_care_compare.sql",
            "stale_yaml": schema_module.MATRIX_YAML,
            "stale_definitions": schema_module.DEFINITIONS,
        }
        for name, path in stale.items():
            root = planted(name, partial(hand_edit, path))
            found = schema_module.check(root)
            report.case(
                "E04s", "G2", f"{name}: a hand-edited generated file is named", len(found) == 1 and path in found[0], found=[line[:80] for line in found]
            )

        def add_model(root: Path) -> None:
            (root / schema_module.MATRIX_FOLDER / "feature_matrix_extra.sql").write_text("select 1 as spine_key\n")

        found = schema_module.check(planted("unregistered", add_model))
        report.case("E04s", "G3", "a feature_matrix model without a schema entry is named", len(found) == 1 and "feature_matrix_extra" in found[0], found=found)
        missing = planted("missing", lambda root: (root / schema_module.DEFINITIONS).unlink())
        report.case("E04s", "G2", "a missing generated file is named", any("missing" in line for line in schema_module.check(missing)))
    broken = copy.deepcopy(schema)
    for column in broken["tables"][schema_module.WINDOW_TABLE]["columns"]:
        if column["name"] == "ccn":
            column.update({"role": "predictor", "view": schema_module.WINDOW_TABLE, "feast_type": "String"})
    broken["servable"].append(f"{schema_module.WINDOW_TABLE}:ccn")
    report.case("E04s", "G10", "an identifier registered as a predictor is refused", bool(schema_module.role_problems(broken)))
    planted_columns = tuple(
        (name, kind, "predictor" if name == "ccn" else role, source, mask) for name, kind, role, source, mask in schema_module.WINDOW_COLUMNS
    )
    with mock.patch.object(schema_module, "WINDOW_COLUMNS", planted_columns):
        report.case(
            "E04s", "G10", "the generator itself stops when an identifier is made a predictor", refuses(lambda: schema_module.build_schema(ROOT), "identifier")
        )
    served = {item.split(":", 1)[1] for item in schema["servable"]}
    forbidden = {
        column["name"] for table in schema["tables"].values() for column in table["columns"] if column["role"] in ("key", "event_time", "identifier", "label")
    }
    report.case(
        "E04s",
        "G10",
        "no key, event time, identifier or label is served; no excluded control is a predictor column",
        not served & forbidden and not schema_module.role_problems(schema),
    )
    masks = {column["name"]: column["mask"] for column in schema["tables"][schema_module.WINDOW_TABLE]["columns"]}
    families = {name: {column["mask"] for column in table["columns"] if column["pivot"] is not None} for name, table in schema["tables"].items()}
    report.case(
        "E04s",
        "G8",
        "the mask rules are in the schema: attributes from the own snapshot, county families from a clean own county, RUCA from the own ZIP",
        masks["state_code"] == "own_snapshot_state"
        and masks["county_fips"] == "own_snapshot_county"
        and masks["control_type_code"] == "own_snapshot_classification"
        and families["feature_matrix_county_measures"] == {"county_family"}
        and families["feature_matrix_county_context"] == {"county_family", "ruca_zip"}
        and families["feature_matrix_hospital"] == {"none"}
        and set(schema["masks"]) >= {"county_family", "ruca_zip", "own_snapshot_state"},
        masks=sorted(set(masks.values())),
    )
    types = {column["feast_type"] for table in schema["tables"].values() for column in table["columns"] if column["role"] in ("predictor", "label")}
    report.case("E11s", "G9", "only Float64, Int64, String and Bool reach Feast", types <= {"Float64", "Int64", "String", "Bool"}, types=sorted(types))
    definitions = (ROOT / schema_module.DEFINITIONS).read_text()
    registered = schema_module.servable_in_definitions(definitions)
    report.case(
        "E11s",
        "G11",
        "the servable list equals the predictor features the definitions register",
        registered == schema["servable"] and len(registered) == len(set(registered)),
        registered=len(registered),
    )
    cut = definitions.replace('        ("c119__value__number", "Float64"),\n', "", 1)
    report.case("E11s", "G11", "a predictor missing from the definitions is noticed", schema_module.servable_in_definitions(cut) != schema["servable"])
    label_views = [name for name in schema_module.servable_in_definitions(definitions) if name.startswith("label_")]
    report.case(
        "E11s", "G10", "no label view is online", not label_views and "online = False" not in definitions and 'view("label_hai_1", False)' in definitions
    )
    sql_text = "\n".join(path.read_text() for path in (ROOT / schema_module.MATRIX_FOLDER).glob("*.sql"))
    report.case("E04s", "G6", "no generated model fills a missing value (no coalesce, no ifnull, no zero literal in an aggregate)", not zero_fill(sql_text))
    report.case(
        "E04s",
        "G6",
        "the zero-fill detector finds a planted coalesce",
        zero_fill(
            sql_text.replace("max(value_number) filter", "coalesce(max(value_number) filter", 1).replace(
                " as c119__value__number", ", 0) as c119__value__number", 1
            )
        ),
    )


def hand_edit(path: str, root: Path) -> None:
    """Append a line to a generated file of a scratch root."""
    (root / path).write_text((root / path).read_text() + "\n-- hand edit\n")


def zero_fill(sql: str) -> bool:
    """Whether generated model SQL replaces a missing value with a default."""
    lowered = sql.lower()
    return "coalesce(" in lowered or "ifnull(" in lowered or "nvl(" in lowered or ", 0)" in lowered


def settings_cases(report: Report) -> None:
    """E30: no teardown, closed settings, database numbers."""
    files = [*sorted((ROOT / "feature_store").glob("*.py")), *sorted((ROOT / "scripts/features").glob("*.py"))]
    needle = ".teardown("
    hits = [path.name for path in files if needle in path.read_text() and path.name != Path(__file__).name]
    report.case("E30", "B7", "no code calls Feast teardown", not hits, files=len(files), hits=hits)
    report.case("E30", "B7", "the teardown scan finds a planted call", needle in "store.teardown()")
    definitions = (ROOT / schema_module.DEFINITIONS).read_text()
    sources = definitions.count("IcebergSource(")
    report.case(
        "E30",
        "B8",
        "every IcebergSource in the definitions sets credential_vending=False (Polaris cannot vend credentials)",
        sources == 2 and definitions.count("credential_vending=False,") == sources,
        sources=sources,
    )
    report.case(
        "E30",
        "B8",
        "the vending detector finds a source without the option",
        definitions.replace("        credential_vending=False,\n", "", 1).count("credential_vending=False,") != sources,
    )
    report.case(
        "E30",
        "G13",
        "the definitions close the namespace and catalog type, and allow the SQL catalog only for features_e2e inside the run folder",
        'NAMESPACES = ("features", "features_e2e")' in definitions
        and 'CATALOG_TYPES = ("rest", "sql")' in definitions
        and 'namespace != "features_e2e"' in definitions
        and 'startswith(f"sqlite:///{folder}/")' in definitions,
    )
    yaml_path = ROOT / "feature_store/feature_store.yaml"
    if yaml_path.is_file():
        text = yaml_path.read_text()
        report.case(
            "E30",
            "D13",
            "feature_store.yaml keeps the entity hashes in database 1, skips deduplication and holds no literal password",
            "db=1" in text and "skip_dedup: true" in text and "password=${REDIS_PASSWORD}" in text,
        )
    else:
        report.pending(
            "E30",
            "D13, B9",
            "feature_store.yaml database numbers and unset-variable refusal",
            "lane C",
            "feature_store/feature_store.yaml is written by lane C",
        )


def pipeline_quality_case(report: Report) -> None:
    """E04: ``pipeline_quality --kind features`` hands the launcher's arguments to the features validation."""
    from scripts.lakehouse import pipeline_quality

    seen: list[list[str]] = []

    def fake_main() -> int:
        seen.append(list(sys.argv[1:]))
        return 0

    from scripts.features import feature_validation

    arguments = ["--kind", "features", "--database", "/x/staging.duckdb", "--suites", "/s", "--baselines", "/b.json", "--output", "/o/result.json"]
    with (
        mock.patch.object(feature_validation, "main", fake_main),
        mock.patch.object(pipeline_quality, "read_record", lambda *_: ({"counts": {}}, "d" * 64)),
        mock.patch.object(sys, "argv", ["pipeline_quality", *arguments]),
        mock.patch.object(sys, "stdout", new=mock.MagicMock()),
    ):
        code = pipeline_quality.main()
    report.case(
        "E04",
        "H8",
        "--kind features calls the features validation with the launcher's arguments minus --kind",
        code == 0 and seen == [arguments[2:]],
        seen=len(seen),
    )


def static(report: Report) -> None:
    """Every static case."""
    generator_cases(report)
    contract_cases(report)
    pin_cases(report)
    plan_cases(report)
    settings_cases(report)
    pipeline_quality_case(report)
    report.pending("E01", "A1, A3, A21", "the pinned build receipt passes in the launcher (publish_offline)", "lead", "needs the launcher; see publish_offline")


# ----------------------------------------------------------------------------------------------------
# build_local (runs under .tools/sqlfluff/bin/python)


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run_tool(arguments: list[str], seconds: int) -> Any:
    """Run this Python interpreter (the tool environment's) with the arguments in the repository root, with a deadline."""
    return run_command(sys.executable, arguments, cwd=ROOT, env=columns.tool_environment(ROOT), timeout=seconds)


class LocalBuild:
    """The synthetic fixture built by the real ``features_build`` in child processes, in one folder."""

    def __init__(self, base: Path) -> None:
        self.base = base
        self.shell: Path | None = None
        self.failures: dict[str, str] = {}

    def make_shell(self) -> Path:
        if self.shell is None:
            self.shell = columns.build_shell(ROOT, self.base / "shell_build")
        return self.shell

    def run_folder(self, name: str, run: str, version: int, source: Path | None = None) -> Path:
        """Make a run folder with the gold extract of one fixture version (or a copy of another run's extract) and build it."""
        folder = self.base / name
        folder.mkdir()
        if source is None:
            tables = sorted(gold_publish.load_plan().tables)
            shell_copy = self.base / f"shell_{name}.duckdb"
            shutil.copy2(self.make_shell(), shell_copy)
            made = run_tool(
                ["-m", "scripts.features.feature_fixture", "make-input", str(shell_copy), str(folder), run, "publish_fixture", str(version), *tables], 600
            )
            if made.returncode != 0:
                raise RuntimeError(f"fixture exited {made.returncode}: {(made.stdout + made.stderr)[-800:]}")
            shell_copy.unlink()
        else:
            shutil.copytree(source / "gold_input", folder / "gold_input")
            pin = json.loads((source / "gold_pins.json").read_text())
            pin["run_id"] = run
            text = json.dumps(pin, indent=2, sort_keys=True) + "\n"
            (folder / "gold_pins.json").write_text(text)
            manifest = json.loads((source / "gold_input/manifest.json").read_text())
            manifest.update({"run_id": run, "pins_sha256": hashlib.sha256(text.encode()).hexdigest()})
            (folder / "gold_input/manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
        result = self.build(folder, run)
        if result.returncode != 0:
            self.failures[name] = (result.stdout + result.stderr)[-600:]
        return folder

    def build(self, folder: Path, run: str, extra: list[str] | None = None) -> Any:
        arguments = [
            "-m",
            "scripts.features.features_build",
            "--mode",
            "publish_fixture",
            "--run",
            run,
            "--folder",
            str(folder),
            "--project",
            str(ROOT / "dbt"),
            "--source-sha256",
            "a" * 64,
            "--input-sha256",
            sha256(folder / "gold_pins.json"),
            "--extract-sha256",
            sha256(folder / "gold_input/manifest.json"),
            *(extra or []),
        ]
        return run_tool(arguments, 1500)


def rows(connection: Any, sql: str, parameters: list[Any] | None = None) -> list[tuple[Any, ...]]:
    return [tuple(row) for row in connection.execute(sql, parameters or []).fetchall()]


def open_db(path: Path, read_only: bool = True) -> Any:
    duckdb: Any = importlib.import_module("duckdb")
    return duckdb.connect(str(path), read_only=read_only)


def validate_file(database: Path) -> list[dict[str, Any]]:
    """The validation's expectations on a build file."""
    from scripts.features import feature_validation

    return feature_validation.validate(database, ROOT / schema_module.SCHEMA, ROOT / "data_contracts/lakehouse/features_publish.json", [ROOT / SUITE_FOLDER])


def failed_checks(expectations: list[dict[str, Any]]) -> set[tuple[str, str]]:
    return {(item["id"], item["table"]) for item in expectations if item["level"] == "fail"}


DAMAGE: dict[str, tuple[str, set[tuple[str, str]]]] = {
    "zero_fill": (
        "update feature_matrix_care_compare set c119__value__number = 0 where c119__value__number is null",
        {("pivot_round_trip", "feature_matrix_care_compare")},
    ),
    "dropped_control": (
        "delete from int_spine_care_compare_measures where measure_control = 'C119'",
        {("long_key_set_equals_schema", "feature_matrix_care_compare")},
    ),
    "wrong_column": (
        "update feature_matrix_care_compare set c119__value__number = c284__value__number",
        {("pivot_round_trip", "feature_matrix_care_compare")},
    ),
    "duplicate_key": (
        "insert into feature_matrix_operations select * from feature_matrix_operations limit 1",
        {
            ("one_row_per_spine_key", "feature_matrix_operations"),
            ("merge_key", "feature_matrix_operations"),
            ("suite:feature_matrix_operations.grain", "feature_matrix_operations"),
        },
    ),
    "missing_key_row": ("delete from feature_matrix_operations where spine_key = '990005:2022'", {("one_row_per_spine_key", "feature_matrix_operations")}),
    "extra_key_row": (
        "insert into feature_matrix_operations select 'zzz:2099', * exclude (spine_key) from feature_matrix_operations limit 1",
        {("one_row_per_spine_key", "feature_matrix_operations")},
    ),
    "null_key": (
        "update feature_matrix_operations set spine_key = null where spine_key = '990005:2022'",
        {("one_row_per_spine_key", "feature_matrix_operations"), ("merge_key", "feature_matrix_operations")},
    ),
    "shifted_event_time": (
        "update feature_matrix_care_compare set event_timestamp = event_timestamp + interval 1 day",
        {("event_time_equals_window_start", "feature_matrix_care_compare")},
    ),
    "null_event_time": (
        "update feature_matrix_county_context set event_timestamp = null where spine_key = '990001:2022'",
        {("event_time_equals_window_start", "feature_matrix_county_context")},
    ),
    "wrong_type": ("alter table feature_matrix_hospital alter c001__value__number type float", {("columns_equal_schema", "feature_matrix_hospital")}),
    "date_column": (
        "alter table feature_matrix_operations add column extra_date date",
        {("no_date_or_32bit_integer", "feature_matrix_operations"), ("columns_equal_schema", "feature_matrix_operations")},
    ),
    "int32_column": (
        "alter table feature_matrix_hospital_window alter bed_count type integer",
        {("no_date_or_32bit_integer", "feature_matrix_hospital_window"), ("columns_equal_schema", "feature_matrix_hospital_window")},
    ),
    "unmasked_future_attribute": (
        "update feature_matrix_hospital_window set state_code = 'TX' where spine_key = '990002:2023'",
        {("masks_have_own_snapshot_provenance", "feature_matrix_hospital_window"), ("pivot_round_trip", "feature_matrix_hospital_window")},
    ),
    "unmasked_connecticut_region": (
        "update feature_matrix_county_measures set c203__diabetes_ageadjprv__number = 9.5 where spine_key = '990004:2022'",
        {("masks_have_own_snapshot_provenance", "feature_matrix_county_measures"), ("pivot_round_trip", "feature_matrix_county_measures")},
    ),
    "unmasked_zip_from_later_snapshot": (
        "update feature_matrix_county_context set c199__primary_ruca__number = 1 where spine_key = '990005:2022'",
        {("masks_have_own_snapshot_provenance", "feature_matrix_county_context"), ("pivot_round_trip", "feature_matrix_county_context")},
    ),
    "category_has_number": (
        "update int_spine_care_compare_measures set value_number = 1.0 where measure_control = 'C141'",
        {("category_has_no_number", "feature_matrix_care_compare")},
    ),
    "receipt_node_failed": (
        "update feature_build_receipt set status = 'error' where node like '%int_hospital_spine'",
        {("receipt", "feature_build_receipt")},
    ),
    "inputs_schema_digest": (
        "update feature_build_inputs set feature_schema_sha256 = repeat('0', 64)",
        {("inputs_one_row_with_schema_digest", "feature_build_inputs")},
    ),
    "inputs_outcome_digest_null": (
        "update feature_build_inputs set hai_outcome_controls_sha256 = null",
        {("inputs_hai_outcome_digest", "feature_build_inputs")},
    ),
    "inputs_outcome_digest_malformed": (
        "update feature_build_inputs set hai_outcome_controls_sha256 = 'not-a-sha256'",
        {("inputs_hai_outcome_digest", "feature_build_inputs")},
    ),
    "inputs_outcome_digest_missing": (
        "alter table feature_build_inputs drop column hai_outcome_controls_sha256",
        {("inputs_columns", "feature_build_inputs"), ("inputs_hai_outcome_digest", "feature_build_inputs")},
    ),
    "adjacency_duplicate_key": (
        "insert into int_county_adjacency select * from int_county_adjacency limit 1",
        {("merge_key", "int_county_adjacency"), ("suite:int_county_adjacency.grain", "int_county_adjacency")},
    ),
    "validation_duplicate_key": (
        "insert into int_spine_validation_measures select * from int_spine_validation_measures limit 1",
        {("merge_key", "int_spine_validation_measures"), ("suite:int_spine_validation_measures.grain", "int_spine_validation_measures")},
    ),
    "adjacency_null_key": (
        "update int_county_adjacency set edge_key = null where edge_key = (select min(edge_key) from int_county_adjacency)",
        {("merge_key", "int_county_adjacency"), ("suite:int_county_adjacency.grain", "int_county_adjacency")},
    ),
    "validation_null_key": (
        "update int_spine_validation_measures set alignment_key = null where alignment_key = (select min(alignment_key) from int_spine_validation_measures)",
        {("merge_key", "int_spine_validation_measures"), ("suite:int_spine_validation_measures.grain", "int_spine_validation_measures")},
    ),
}


def damaged_copy(source: Path, target: Path, sql: str) -> Path:
    shutil.copy2(source, target)
    connection = open_db(target, read_only=False)
    try:
        connection.execute(sql)
    finally:
        connection.close()
    return target


def mutated_validation(folder: Path, old: str, new: str) -> Any:
    """The validation module with one code change, loaded from its own scratch file (one file name per change, and no bytecode written, so no change reuses
    the cached bytecode of another)."""
    source = (ROOT / "scripts/features/feature_validation.py").read_text()
    if old not in source:
        raise RuntimeError(f"the mutation target {old!r} is not in feature_validation.py")
    changed = source.replace(old, new)
    path = folder / f"feature_validation_mutant_{hashlib.sha256(changed.encode()).hexdigest()[:12]}.py"
    path.write_text(changed)
    spec = importlib.util.spec_from_file_location(path.stem, path)
    if spec is None or spec.loader is None:
        raise RuntimeError("the mutant module cannot be loaded")
    module = importlib.util.module_from_spec(spec)
    saved, sys.dont_write_bytecode = sys.dont_write_bytecode, True
    try:
        spec.loader.exec_module(module)
    finally:
        sys.dont_write_bytecode = saved
    return module


def build_cases(report: Report, build: LocalBuild, base: Path) -> None:
    """The cases that need the fixture build."""
    started = time.monotonic()
    first = build.run_folder("run_a", RUN, 1)
    receipt = json.loads((first / "build_receipt.json").read_text()) if (first / "build_receipt.json").is_file() else {}
    staging = first / "staging.duckdb"
    report.case(
        "E01",
        "A1, A21, H5",
        "the fixture builds with every model and test node passing (contracts enforced)",
        bool(receipt) and receipt.get("nodes", 0) > 150,
        nodes=receipt.get("nodes"),
        seconds=round(time.monotonic() - started),
        failure=build.failures.get("run_a", ""),
    )
    if not staging.is_file():
        report.case("E01", "H5", "the build wrote staging.duckdb", False)
        return
    from scripts.features.delta_cases import built_support

    built_support(report, staging, ROOT, first)
    connection = open_db(staging)
    try:
        tables = {row[0] for row in rows(connection, "select table_name from information_schema.tables where table_schema = 'main'")}
        contract = set(json.loads((ROOT / "data_contracts/lakehouse/features_publish.json").read_text())["publish"]["tables"])
        report.case(
            "E01",
            "H1",
            "staging.duckdb holds every contract table as a plain table",
            contract <= tables and not {row[0] for row in rows(connection, "select table_name from information_schema.tables where table_type = 'VIEW'")},
            extra=sorted(tables - contract),
        )
        inputs = rows(connection, "select * from feature_build_inputs")
        report.case(
            "E06a",
            "H7",
            "feature_build_inputs holds one row of identifiers and digests, no time and no invocation ID",
            len(inputs) == 1 and RUN in inputs[0] and GOLD_RUN in inputs[0] and not any(isinstance(value, datetime) for value in inputs[0]),
        )
        spine = rows(connection, "select spine_key, state_code, county_fips, provider_subtype_code, bed_count from feature_matrix_hospital_window order by 1")
        by_key = {row[0]: row for row in spine}
        masked = [by_key[key][1:4] for key in ("990002:2023", "990003:2023")]
        report.case(
            "E14",
            "A19",
            "windows whose state, county and classification came from a later snapshot, an enrollment row or a mid-window period are null",
            masked == [(None, None, None)] * 2,
        )
        report.case(
            "E14",
            "A19",
            "windows with an own snapshot keep state, county and classification",
            by_key["990001:2022"][1:4] == ("AL", "01001", "01") and by_key["990005:2022"][1] == "FL",
        )
        county = dict(rows(connection, "select spine_key, c203__diabetes_ageadjprv__number from feature_matrix_county_measures"))
        context = {
            row[0]: row[1:]
            for row in rows(connection, "select spine_key, c175__s1701_c01_001e__number, c199__primary_ruca__number from feature_matrix_county_context")
        }
        report.case(
            "E14",
            "A20",
            "a Connecticut window with a complete own snapshot and a HUD planning region has no county-family value, and keeps its RUCA",
            county["990004:2022"] is None and context["990004:2022"] == (None, 1.0),
        )
        report.case(
            "E14",
            "A20",
            "a window whose ZIP only a later snapshot gives has no RUCA value but keeps its own county's values",
            context["990005:2022"] == (12.5, None) and county["990005:2022"] == 9.5,
        )
        report.case(
            "E14",
            "C1",
            "a source period that ends on the window start is absent from that window and present in the next",
            county["990001:2022"] == 9.5 and county["990001:2023"] == 11.25,
        )
        hospital = dict(rows(connection, "select spine_key, c001__value__number from feature_matrix_hospital"))
        report.case(
            "E16",
            "B2",
            "a 17-significant-digit double passes through the wide table exactly",
            hospital["990001:2022"] == 120.12345678901234567 and hospital["990001:2023"] == 999.0,
        )
        report.case("E14", "C1", "a cost report that ends on the window start is not used for that window", hospital["990001:2022"] != 999.0)
        zeros = sum(
            rows(connection, ZERO_COUNT.format(column=column))[0][0]
            for column in [
                row[0]
                for row in rows(
                    connection,
                    "select column_name from information_schema.columns where table_name = 'feature_matrix_county_measures' and data_type = 'DOUBLE'",
                )
            ]
        )
        report.case("E04", "G6", "no wide county measure holds a zero on the fixture (nulls are never zero)", zeros == 0)
        timestamps = rows(
            connection,
            "select spine_key, epoch(event_timestamp) from feature_matrix_hospital_window where spine_key in ('990001:2022', '990001:2024') order by 1",
        )
        report.case(
            "E12",
            "B3",
            "event_timestamp is the window start at 00:00 UTC",
            timestamps == [("990001:2022", 1640995200.0), ("990001:2024", 1704067200.0)],
            timestamps=len(timestamps),
        )
    finally:
        connection.close()
    result_path = base / "result_a.json"
    try:
        validation = run_tool(
            [
                "-m",
                "scripts.features.feature_validation",
                "--database",
                str(staging),
                "--output",
                str(result_path),
                "--suites",
                str(ROOT / SUITE_FOLDER),
                "--schema",
                str(ROOT / schema_module.SCHEMA),
                "--contract",
                str(ROOT / "data_contracts/lakehouse/features_publish.json"),
            ],
            600,
        )
    except TimeoutExpired:
        validation = None
    record = json.loads(result_path.read_text()) if result_path.is_file() else {}
    counts = record.get("counts", {})
    report.case(
        "E04",
        "H8",
        "validation of the clean build passes every expectation and carries the build file digest",
        validation is not None
        and validation.returncode == 0
        and counts.get("fail") == 0
        and counts.get("pass", 0) > 60
        and record.get("build_file_sha256") == sha256(staging),
        counts=counts,
    )
    text = result_path.read_text() if result_path.is_file() else ""
    report.case(
        "E19",
        "H9",
        "the validation record holds no ccn, no vendor name and no value of the fixture",
        not any(token in text for token in ("990001", "Vendor A", "CEHRT1", "35005")),
    )
    clean = validate_file(staging)
    report.case("E04", "H8", "validation of the clean build in process has no failing expectation", not failed_checks(clean), expectations=len(clean))
    work = base / "damage"
    work.mkdir()
    for name, (sql, expected) in DAMAGE.items():
        found = failed_checks(validate_file(damaged_copy(staging, work / f"{name}.duckdb", sql)))
        report.case(
            "E04",
            "A4, A5, A15, A18, A19, A20, G9",
            f"planted {name}: validation fails the expected checks",
            expected <= found,
            expected=sorted(f"{a}:{b}" for a, b in expected),
            found=sorted(f"{a}:{b}" for a, b in found)[:8],
        )
    mutants = work / "mutants"
    mutants.mkdir()
    zero = damaged_copy(staging, work / "mutant_zero.duckdb", DAMAGE["zero_fill"][0])
    shifted = damaged_copy(staging, work / "mutant_shifted.duckdb", DAMAGE["shifted_event_time"][0])
    numbered = damaged_copy(staging, work / "mutant_numbered.duckdb", DAMAGE["category_has_number"][0])
    future = damaged_copy(staging, work / "mutant_future.duckdb", DAMAGE["unmasked_connecticut_region"][0])
    connection_group = {
        "null-safe compare weakened to <>": (
            "e.value is distinct from a.value",
            "e.value <> a.value",
            zero,
            ("pivot_round_trip", "feature_matrix_care_compare"),
        ),
        "event-time check removed": (
            'self.expect("event_time_equals_window_start", name, wrong == 0,',
            'self.expect("event_time_equals_window_start", name, True,',
            shifted,
            ("event_time_equals_window_start", "feature_matrix_care_compare"),
        ),
        "category-with-number check removed": (
            'failed += kinds == {"category"} and has_number',
            "failed += 0",
            numbered,
            ("category_has_no_number", "feature_matrix_care_compare"),
        ),
        "own-county provenance check removed": (
            "o.county_fips is not null and s.county_fips = o.county_fips and s.planning_region_fips is null",
            "true",
            future,
            ("masks_have_own_snapshot_provenance", "feature_matrix_county_measures"),
        ),
    }
    for label, (old, new, database, key) in connection_group.items():
        module = mutated_validation(mutants, old, new)
        missed = key not in failed_checks(
            module.validate(database, ROOT / schema_module.SCHEMA, ROOT / "data_contracts/lakehouse/features_publish.json", [ROOT / SUITE_FOLDER])
        )
        report.case("E04", "G6, G8", f"mutant ({label}): the planted defect is missed, so its case depends on that part of the validation", missed)
    malformed = damaged_copy(staging, work / "mutant_control_hash.duckdb", DAMAGE["inputs_outcome_digest_malformed"][0])
    weakened = mutated_validation(mutants, "regexp_full_match(hai_outcome_controls_sha256, '[a-f0-9]{64}')", "hai_outcome_controls_sha256 is not null")
    missing_check = mutated_validation(mutants, "rows == 1 and controls == 1", "True")
    for label, module in (("outcome digest rule widened", weakened), ("outcome digest check removed", missing_check)):
        found = failed_checks(
            module.validate(malformed, ROOT / schema_module.SCHEMA, ROOT / "data_contracts/lakehouse/features_publish.json", [ROOT / SUITE_FOLDER])
        )
        report.case(
            "E04",
            "DT8",
            f"mutant ({label}): malformed digest case detects the missing protection",
            ("inputs_hai_outcome_digest", "feature_build_inputs") not in found,
        )
    from scripts.features.delta_cases import planted_support_rules

    planted_support_rules(report, staging, ROOT, first, work)
    second = build.run_folder("run_b", RUN_B, 1, source=first)
    other = open_db(second / "staging.duckdb")
    this = open_db(staging)
    try:
        differing = rows(other, "select * from feature_build_inputs")[0], rows(this, "select * from feature_build_inputs")[0]
        names = [
            row[0]
            for row in rows(this, "select column_name from information_schema.columns where table_name = 'feature_build_inputs' order by ordinal_position")
        ]
        changed = [name for name, a, b in zip(names, *differing, strict=True) if a != b]
        report.case(
            "E06b",
            "A8, H7",
            "a new run ID with the same pin gives a second build whose provenance differs only in features_run_id",
            changed == ["features_run_id"],
            changed=changed,
        )
        same = all(
            rows(other, TABLE_HASH.format(table=table)) == rows(this, TABLE_HASH.format(table=table)) for table in sorted(contract - {"feature_build_inputs"})
        )
        report.case("E06b", "A8", "the data tables of the second build have the same row counts and content hashes", same)
    finally:
        other.close()
        this.close()
    third = build.run_folder("run_v2", RUN_2, 2)
    after = open_db(third / "staging.duckdb")
    try:
        keys = {row[0] for row in rows(after, "select spine_key from feature_matrix_hospital_window")}
        beds = rows(after, "select bed_count from feature_matrix_hospital_window where spine_key = '990001:2022'")
        report.case(
            "E07",
            "A9",
            "version 2 drops the removed hospital, adds a new one and changes a value",
            "990003:2023" not in keys and "990006:2023" in keys and beds == [(110,)],
            keys=len(keys),
        )
    finally:
        after.close()
    export_cases(report, staging, first, base)
    guard_cases(report, build, first, base)


def export_cases(report: Report, staging: Path, folder: Path, base: Path) -> None:
    """E09 and E10: the shared consumer exports the build; a replaced copy and a repeated merge key are refused; types survive the Parquet files."""
    from scripts.lakehouse import gold_export

    plan = features_publish.publish_plan("publish_fixture")
    publish = base / "export"
    publish.mkdir()
    digest = sha256(staging)
    result = gold_export.export_sealed(staging, digest, publish, plan, RUN)
    manifest = json.loads((publish / "export/manifest.json").read_text())
    report.case(
        "E10",
        "H10",
        "the consumer exports exactly the contract's tables",
        result["exported"] is True and set(manifest["tables"]) == set(plan.tables),
        tables=len(manifest["tables"]),
    )
    types = {name: dict(entry["columns"]) for name, entry in manifest["tables"].items()}
    schema = schema_module.load_schema()
    expected_types = {name: {column["name"]: column["type"].upper() for column in table["columns"]} for name, table in schema["tables"].items()}
    report.case(
        "E10", "H10, G9", "the exported types of every wide table equal the schema's", all(types[name] == wanted for name, wanted in expected_types.items())
    )
    connection = open_db(staging)
    parquet = importlib.import_module("duckdb").connect()
    try:
        file = publish / "export" / manifest["tables"]["feature_matrix_hospital"]["file"]
        read = parquet.execute(PARQUET_VALUE.format(file=file)).fetchone()
        read_types = dict(parquet.execute(PARQUET_SCHEMA.format(file=file)).fetchall())
        report.case(
            "E10",
            "B2",
            "the exported Parquet keeps the 17-digit double and the timestamp with time zone",
            read is not None and read[0] == 120.12345678901234567 and "isAdjustedToUTC=1" in str(read_types.get("event_timestamp")),
            exact=read is not None and read[0] == 120.12345678901234567,
        )
    finally:
        connection.close()
        parquet.close()
    replaced = base / "replaced.duckdb"
    shutil.copy2(staging, replaced)
    with replaced.open("ab") as handle:
        handle.write(b"\0replaced")
    refused_dir = base / "export_refused"
    refused_dir.mkdir()
    report.case(
        "E09",
        "H11",
        "a build file replaced after validation is refused and nothing is exported",
        refuses(lambda: gold_export.export_sealed(replaced, digest, refused_dir, plan, RUN), "differs") and not (refused_dir / "export").exists(),
    )
    repeated = damaged_copy(staging, base / "repeated.duckdb", DAMAGE["duplicate_key"][0])
    repeated_dir = base / "export_repeated"
    repeated_dir.mkdir()
    report.case(
        "E09",
        "H10",
        "a repeated merge key is refused by the export",
        refuses(lambda: gold_export.export_sealed(repeated, sha256(repeated), repeated_dir, plan, RUN), "repeat the merge key"),
    )
    del folder


def guard_cases(report: Report, build: LocalBuild, first: Path, base: Path) -> None:
    """E03 and the bounded waits: the build refuses a changed input, a changed pin, and a late step."""
    from orchestration.pipeline_protocol import PipelineError
    from scripts.features import features_build

    duckdb: Any = importlib.import_module("duckdb")
    manifest_sha = sha256(first / "gold_input/manifest.json")
    pin_sha = sha256(first / "gold_pins.json")
    scratch = base / "guard"
    shutil.copytree(first, scratch, ignore=shutil.ignore_patterns("features_work", "staging.duckdb", "layer_sealed"))
    connection = duckdb.connect()
    try:
        report.case(
            "E03",
            "H3",
            "an extract whose manifest or pin differs from the accepted digest is refused",
            refuses(lambda: features_build.import_gold(scratch, "0" * 64, pin_sha, RUN, connection), "differs")
            and refuses(lambda: features_build.import_gold(scratch, manifest_sha, "0" * 64, RUN, connection), "differs"),
        )
        victim = next((scratch / "gold_input").glob("*/part-00000.parquet"))
        victim.chmod(0o644)
        victim.write_bytes(victim.read_bytes() + b"x")
        report.case(
            "E03",
            "H3",
            "an exported file changed after the manifest is refused",
            refuses(lambda: features_build.import_gold(scratch, manifest_sha, pin_sha, RUN, connection), "changed before import"),
        )
        features_build.COPY_SECONDS, saved = -1, features_build.COPY_SECONDS
        try:
            report.case(
                "E03",
                "H15",
                "the import copy loop stops at its deadline",
                refuses(lambda: features_build.import_gold(scratch, manifest_sha, pin_sha, RUN, connection), "exceeded"),
            )
        finally:
            features_build.COPY_SECONDS = saved
    finally:
        connection.close()
    try:
        features_build.run_child(["-c", "import time; time.sleep(30)"], ROOT, "sleeper", 1)
        late = False
    except PipelineError as error:
        late = "longer than 1 seconds" in str(error)
    report.case("E03", "H15", "a child step that runs past its deadline is stopped and reported", late)
    results = scratch / "results" / "target"
    results.mkdir(parents=True)
    models = [f"model.hai.{name}" for name in sorted(path.stem for path in (ROOT / columns.MODELS).rglob("*.sql")) if not name.startswith("feature_bridge_")]

    def write_results(statuses: dict[str, str], skip: str = "") -> None:
        items = [{"unique_id": node, "status": statuses.get(node, "success")} for node in models if not node.endswith(skip or "\0")]
        (results / "run_results.json").write_text(json.dumps({"metadata": {"invocation_id": "x"}, "results": items}))

    write_results({})
    passes = features_build.check_results(scratch / "results", ROOT)[1]
    write_results({models[0]: "error"})
    failed_node = refuses(lambda: features_build.check_results(scratch / "results", ROOT), "nonpassing")
    write_results({}, skip="feature_matrix_operations")
    missing_model = refuses(lambda: features_build.check_results(scratch / "results", ROOT), "did not build feature_matrix_operations")
    report.case(
        "E01",
        "H5",
        "the build receipt accepts all-passing nodes and refuses a failed node and a feature model that was not built",
        len(passes) == len(models) and failed_node and missing_model,
    )
    stopped = build.build(scratch, RUN)
    report.case(
        "E03",
        "H3",
        "the build refuses the tampered extract and leaves no staging file",
        stopped.returncode == 1 and "changed before import" in stopped.stderr and not (scratch / "staging.duckdb").exists(),
    )
    wrong = run_tool(
        [
            "-m",
            "scripts.features.features_build",
            "--mode",
            "publish_fixture",
            "--run",
            RUN,
            "--folder",
            str(scratch),
            "--project",
            str(ROOT / "dbt"),
            "--source-sha256",
            "a" * 64,
            "--input-sha256",
            "f" * 64,
            "--extract-sha256",
            manifest_sha,
        ],
        300,
    )
    report.case("E03", "H3", "the build refuses an input digest that is not the pin's", wrong.returncode == 1 and "accepted input digest" in wrong.stderr)

    from scripts.features.delta_cases import snapshot_identity

    snapshot_identity(report, build, first, base)


def build_local(report: Report) -> None:
    """Run the build_local cases under the tool environment."""
    if importlib.util.find_spec("duckdb") is None:
        report.case("E01", "H5", "build_local runs under .tools/sqlfluff/bin/python (DuckDB and dbt are not in .venv)", False)
        return
    masks_local(report)
    with tempfile.TemporaryDirectory(prefix="feature_store_build_local_") as folder:
        base = Path(folder)
        build_cases(report, LocalBuild(base), base)


def masks_local(report: Report) -> None:
    """Run generated projections against the accepted producer's source-label contract without changing the producer."""
    from scripts.features.mask_cases import run

    run(report, ROOT)


# ----------------------------------------------------------------------------------------------------
# publish_offline


def publish_exports(base: Path, report_folder: Path) -> dict[str, Any]:
    """Build runs A, B (the same pin and data as A) and V2 (changed data), export each through the shared consumer and write ``inputs.json``."""
    from scripts.lakehouse import gold_export

    build = LocalBuild(base)
    first = build.run_folder("run_a", RUN, 1)
    second = build.run_folder("run_b", RUN_B, 1, source=first)
    third = build.run_folder("run_v2", RUN_2, 2)
    plan = features_publish.publish_plan("publish_fixture")
    container = f"/workspace/data/e2e/feature_store/{report_folder.name}"
    runs = {}
    for name, folder, run in (("a", first, RUN), ("b", second, RUN_B), ("v2", third, RUN_2)):
        staging = folder / "staging.duckdb"
        publish_dir = report_folder / "exports" / name
        publish_dir.mkdir(parents=True)
        digest = sha256(staging)
        gold_export.export_sealed(staging, digest, publish_dir, plan, run)
        runs[name] = {"publish_dir": f"{container}/exports/{name}", "run_id": run, "sealed_sha256": digest}
    document = {"runs": runs}
    (report_folder / "inputs.json").write_text(json.dumps(document, indent=2) + "\n")
    return document


def publish_offline(report: Report, report_folder: Path) -> None:
    """The Spark case suite on the exports of runs A, B and V2. The job ``features_publish_e2e`` belongs to the launcher lane; without it the mode fails."""
    from orchestration import launcher

    if "features_publish_e2e" not in getattr(launcher, "SPARK_JOBS", {}):
        report.case(
            "E06a",
            "A8, A10, A13",
            "publish_offline needs the launcher job features_publish_e2e (template spark_e2e, module scripts.features.features_publish_cases): not defined",
            False,
            blocked_on=["features_publish_e2e"],
        )
        return
    from scripts.lakehouse.run_gold_publish_e2e import SUITE_SECONDS, Session

    with tempfile.TemporaryDirectory(prefix="feature_store_publish_offline_") as folder:
        publish_exports(Path(folder), report_folder)
    session = Session()
    try:
        session.start()
        ran = session.job("features_publish_e2e", "suite", ["--stamp", report_folder.name], SUITE_SECONDS)
        cases = json.loads((report_folder / "cases_all.json").read_text()) if (report_folder / "cases_all.json").is_file() else []
        report.case("E06a", "A8", "the case suite ran in the Spark image", ran.ok, exit=ran.exit_code)
        for item in cases:
            report.case(
                "E06a, E06b, E07, E08, E09, E10",
                "A8, A9, A10, A11, A12",
                f"case {item['case']}",
                item["status"] == "pass",
                checks=item["checks"],
                error=item["error"],
            )
    finally:
        cleanup = session.close()
    report.case(
        "E06a",
        "RS4",
        "the session left no launcher and no container",
        not (cleanup.get("launcher_left") or cleanup.get("containers_left") or cleanup.get("errors")),
        cleanup=cleanup,
    )


# ----------------------------------------------------------------------------------------------------


def main() -> int:
    """Run one mode and write the report."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--mode", required=True, choices=MODES)
    parser.add_argument("--output-directory", type=Path)
    args = parser.parse_args()
    report = Report()
    folder_for_modes = args.output_directory or OUT / datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    folder_for_modes.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    error = ""
    try:
        if args.mode == "publish_offline":
            publish_offline(report, folder_for_modes)
        else:
            {"static": static, "build_local": build_local, "masks_local": masks_local}[args.mode](report)
    except Exception as exception:
        error = f"{type(exception).__name__}: {exception}\n{traceback.format_exc()[-1500:]}"
    folder = folder_for_modes
    verdicts = [step["verdict"] for step in report.steps]
    status = "fail" if error or "fail" in verdicts or not report.steps else "pass"
    summary = {
        "mode": args.mode,
        "command": f"{Path(sys.executable).name} -m scripts.features.run_feature_store_e2e --mode {args.mode}",
        "status": status,
        "counts": {verdict: verdicts.count(verdict) for verdict in ("pass", "fail", "pending")},
        "seconds": round(time.monotonic() - started),
        "schema_sha256": schema_module.schema_sha256(),
        "script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "error": error,
        "steps": report.steps,
    }
    (folder / "report.json").write_text(json.dumps(summary, indent=2) + "\n")
    sys.stdout.write(f"feature store e2e {args.mode}: {status}; {summary['counts']}; {folder / 'report.json'}\n")
    for failure in report.failed[:20]:
        sys.stderr.write(f"failed: {failure}\n")
    if error:
        sys.stderr.write(error.splitlines()[0] + "\n")
    return 0 if status == "pass" else 1


if __name__ == "__main__":
    sys.exit(main())
